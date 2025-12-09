#!/usr/bin/env python3
"""
Combined utilities for crawling Wikipedia (Nobel laureates) and cleaning/preprocessing
for NER/RE finetuning datasets.

Provides two subcommands:
 - crawl : run the crawler (from original `crawl_wiki_nobel_laureates_content.py`)
 - clean : run the cleaner (from original `clean_nobel_content.py`)

This file preserves the original logic from both source files and exposes them
via a small CLI with consistent arguments.
"""
import chardet
import os
import time
import json
import argparse
import requests
import re
import csv
from bs4 import BeautifulSoup
from tqdm import tqdm
from urllib.parse import urlparse, unquote

# NLTK sentence tokenizer
try:
    import nltk
    nltk.data.find('tokenizers/punkt')
except Exception:
    try:
        import nltk
        nltk.download('punkt')
    except Exception:
        pass
from nltk.tokenize import sent_tokenize

# Optional HuggingFace tokenizer
USE_HF_TOKENIZER = False
HF_MODEL_NAME = "Qwen/Qwen3-0.6B"

if USE_HF_TOKENIZER:
    from transformers import AutoTokenizer
    hf_tokenizer = AutoTokenizer.from_pretrained(HF_MODEL_NAME, trust_remote_code=True)

WIKI_API = "https://en.wikipedia.org/w/api.php"
WIKIDATA_API = "https://www.wikidata.org/w/api.php"

# ---------------- Helpers: Wikidata & Wikipedia ----------------

# đặt ở global (đảm bảo session header được dùng trong main)
DEFAULT_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; DataCrawler/1.0; +https://example.org/bot)"}

def extract_title_from_wiki_url(url: str):
    """
    Nếu url là Wikipedia enwiki full URL, trả về page title.
    Ví dụ:
      https://en.wikipedia.org/wiki/Marie_Curie  -> "Marie_Curie"
      /wiki/Marie_Curie -> "Marie_Curie"
    """
    if not url:
        return None
    try:
        u = url.strip()
        # accept both full url and path like /wiki/Name
        if u.startswith("http"):
            parsed = urlparse(u)
            # ensure domain contains wikipedia.org and language en
            host = parsed.netloc.lower()
            if "wikipedia.org" not in host:
                return None
            # path like /wiki/Marie_Curie
            path = parsed.path
        else:
            # sometimes input is just /wiki/Name
            path = u
        if not path:
            return None
        # split path and find last segment
        parts = path.split("/")
        if len(parts) == 0:
            return None
        last = parts[-1] or parts[-2] if len(parts)>1 else parts[-1]
        # decode URL-encoding
        title = unquote(last)
        # Wikipedia titles use underscores as spaces — keep as-is for API
        return title
    except Exception:
        return None

def qid_to_enwiki_title(qid, session=None, retries=3, backoff=1.0, verbose=False):
    """
    Map Wikidata QID -> English Wikipedia title via wbgetentities sitelinks.
    Fallback: if no enwiki sitelink, use labels[*] to search enwiki.
    Returns title string or None.
    """
    if session is None:
        session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    params = {
        "action": "wbgetentities",
        "ids": qid,
        "props": "sitelinks|labels",
        "format": "json",
        "languages": "en"  # ask for en label if exists
    }

    for attempt in range(1, retries + 1):
        try:
            r = session.get(WIKIDATA_API, params=params, timeout=30)
            if verbose:
                print(f"[WIKIDATA] Q={qid} status={r.status_code} attempt={attempt}")
            r.raise_for_status()
            data = r.json()
            ent = data.get("entities", {}).get(qid)
            if not ent:
                if verbose:
                    print(f"[WIKIDATA] no entity for {qid} in response")
                return None

            sitelinks = ent.get("sitelinks", {}) or {}
            en = sitelinks.get("enwiki")
            if en:
                title = en.get("title")
                if verbose:
                    print(f"[WIKIDATA] Q={qid} -> enwiki title: {title}")
                return title

            # No enwiki sitelink — try use labels to search enwiki
            labels = ent.get("labels", {}) or {}
            # prefer English label
            label_en = labels.get("en", {}).get("value") if labels.get("en") else None
            # if no en label, pick any available label (first)
            if not label_en and labels:
                # pick first label value
                label_en = next(iter(labels.values())).get("value")

            if label_en:
                if verbose:
                    print(f"[WIKIDATA] Q={qid} no enwiki sitelink; fallback search label='{label_en}'")
                # use Wikipedia search to find best matching page title
                title = search_wikipedia_title(label_en, session=session, retries=2, backoff=1.0)
                if title:
                    if verbose:
                        print(f"[FALLBACK] Found enwiki via search: {title}")
                    return title

            # nothing found
            return None

        except requests.HTTPError as he:
            if verbose:
                print(f"[WIKIDATA] HTTPError for {qid}: {he}. Retrying...")
            time.sleep(backoff * attempt)
            continue
        except Exception as e:
            if verbose:
                print(f"[WIKIDATA] Exception for {qid}: {e}. Retrying...")
            time.sleep(backoff * attempt)
            continue

    # after retries
    if verbose:
        print(f"[WIKIDATA] Failed to map {qid} after {retries} retries.")
    return None


def search_wikipedia_title(name, session=None, retries=2, backoff=0.8, verbose=False):
    """
    Search Wikipedia for best match title for given name.
    Returns title or None.
    """
    if session is None:
        session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    params = {
        "action": "query",
        "list": "search",
        "srsearch": name,
        "format": "json",
        "srlimit": 1
    }
    for attempt in range(1, retries + 1):
        try:
            r = session.get(WIKI_API, params=params, timeout=30)
            if verbose:
                print(f"[WIKI SEARCH] '{name}' status={r.status_code} attempt={attempt}")
            r.raise_for_status()
            data = r.json()
            hits = data.get("query", {}).get("search")
            if hits:
                return hits[0]["title"]
            return None
        except Exception as e:
            if verbose:
                print(f"[WIKI SEARCH] Error searching '{name}': {e}. Retrying...")
            time.sleep(backoff * attempt)
            continue
    return None


def fetch_page_parse(title, section=None, session=None, retries=2, backoff=1.0):
    """
    Fetch parse output for page title. If section specified (int or str), pass it.
    Returns dict with 'html' and optionally 'sections' list or {'error':...}
    """
    if session is None:
        session = requests.Session()
    params = {
        "action": "parse",
        "page": title,
        "prop": "text|sections",
        "format": "json"
    }
    if section is not None:
        params["section"] = str(section)
    for attempt in range(retries):
        try:
            r = session.get(WIKI_API, params=params, timeout=30)
            if r.status_code != 200:
                time.sleep(backoff * (attempt + 1))
                continue
            data = r.json()
            if "error" in data:
                return {"html": None, "error": data["error"]}
            return {"html": data.get("parse", {}).get("text", {}).get("*"), "sections": data.get("parse", {}).get("sections")}
        except Exception as e:
            time.sleep(backoff * (attempt + 1))
            continue
    return {"html": None, "error": "fetch_failed"}

def html_to_paragraphs(html):
    """Extract non-empty paragraph strings from page HTML"""
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    paras = []
    for p in soup.find_all("p"):
        txt = p.get_text().strip()
        if txt:
            paras.append(txt)
    return paras

# ---------------- Sentence splitting with protected dots ----------------

_PROTECT_CHAR = "∯"  # unlikely char used as placeholder for protected dots

def protect_dots(text: str) -> str:
    """
    Replace dots that should NOT be treated as sentence boundaries with a placeholder.
    """
    t = text

    # 1) Protect repeated initials/acronyms: (A.){2,} e.g. U.S., U.S.A., Ph.D.
    t = re.sub(r'\b((?:[A-Z]\.){2,})', lambda m: m.group(1).replace('.', _PROTECT_CHAR), t)

    # Also catch patterns like "U. S." with spaces
    t = re.sub(r'\b((?:[A-Z]\.\s){1,}[A-Z]\.)', lambda m: m.group(1).replace('.', _PROTECT_CHAR), t)

    # 2) Protect initials sequences before a capitalized word
    t = re.sub(r'\b((?:[A-Z]\.){1,})(?=\s+[A-Z][a-z])', lambda m: m.group(1).replace('.', _PROTECT_CHAR), t)

    # 3) Protect single initials when followed by another initial/name with dot-space pattern
    t = re.sub(r'\b([A-Z]\.)\s+(?=[A-Z]\.)', lambda m: m.group(1).replace('.', _PROTECT_CHAR) + ' ', t)

    # 4) Protect common name suffixes (Jr., Sr., Jr, Sr)
    t = re.sub(r'\b(Jr|Sr|Jr\.|Sr\.)\b', lambda m: m.group(0).replace('.', _PROTECT_CHAR), t)

    return t

def restore_dots(text: str) -> str:
    return text.replace(_PROTECT_CHAR, '.')

def sentence_split(text):
    """Sentence split using nltk with fallback regex, but protect initials/acronyms from being split as sentence end."""
    try:
        protected = protect_dots(text)
        sents = sent_tokenize(protected)
        sents = [restore_dots(s).strip() for s in sents if s.strip()]
        return sents
    except Exception:
        pieces = re.split(r'(?<=[.!?])\s+(?=[A-Z0-9"\'\(\[])', text)
        return [p.strip() for p in pieces if p.strip()]

# ---------------- Counting tokens ----------------

def count_word_tokens(sentences):
    return sum(len(s.split()) for s in sentences)

def count_hf_tokens(sentences):
    if not USE_HF_TOKENIZER:
        raise RuntimeError("HF tokenizer disabled")
    total = 0
    for s in sentences:
        total += len(hf_tokenizer.encode(s, add_special_tokens=False))
    return total

# ---------------- Gather sentences per title ----------------

def gather_sentences_for_title(title, need_sentences=10, session=None, pause=0.3):
    """
    Return list of dicts: {'sentence':..., 'source_section': section_title_or_index}
    """
    if session is None:
        session = requests.Session()
    collected = []

    # initial parse (lead + sections list)
    res = fetch_page_parse(title, section=None, session=session)
    if res.get("error"):
        return collected, res.get("error")

    # lead paragraphs
    lead_html = res.get("html")
    paras = html_to_paragraphs(lead_html)
    for p in paras:
        for s in sentence_split(p):
            if len(collected) < need_sentences:
                collected.append({"sentence": s, "source_section": "Lead"})
            else:
                break
        if len(collected) >= need_sentences:
            break
    if len(collected) >= need_sentences:
        return collected, None

    sections_info = res.get("sections") or []
    skip_lower = {"references", "external links", "further reading", "notes", "bibliography", "sources", "see also", "footnotes"}
    for sec in sections_info:
        try:
            sec_index = int(sec.get("index"))
        except:
            continue
        if sec_index == 0:
            continue
        sec_title = sec.get("line", "").strip()
        if sec_title and sec_title.lower() in skip_lower:
            continue
        sec_res = fetch_page_parse(title, section=sec_index, session=session)
        if sec_res.get("error"):
            time.sleep(pause)
            continue
        paras = html_to_paragraphs(sec_res.get("html"))
        for p in paras:
            for s in sentence_split(p):
                if len(collected) < need_sentences:
                    collected.append({"sentence": s, "source_section": sec_title or f"section_{sec_index}"})
                else:
                    break
            if len(collected) >= need_sentences:
                break
        if len(collected) >= need_sentences:
            break
        time.sleep(pause)
    return collected, None

# ---------------- Main processing single person ----------------

def process_person_input(entry, need_sentences=10, session=None, pause=0.3):
    """
    entry: dict possibly containing keys 'url', 'qid' and/or 'name'
    returns: result dict for JSONL (includes name, qid, title, sentences, token counts)
    """
    if session is None:
        session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    qid = entry.get("qid")
    name = entry.get("name") or entry.get("label")
    url = entry.get("url")
    title = None
    used_qid = None

    # Priority 1: if url provided, extract title from url and use it directly
    if url:
        title = extract_title_from_wiki_url(url)
        if title is None:
            return {"input": entry, "error": "invalid_wikipedia_url"}
    else:
        if qid:
            title = qid_to_enwiki_title(qid, session=session)
            used_qid = qid
        if not title and name:
            title = search_wikipedia_title(name, session=session)

    if not title:
        return {"input": entry, "error": "no_wikipedia_title_found"}

    sentences, err = gather_sentences_for_title(title, need_sentences=need_sentences, session=session, pause=pause)
    if err:
        return {"input": entry, "title": title, "error": err}

    texts = [s["sentence"] for s in sentences]
    word_tokens = count_word_tokens(texts)
    hf_tokens = None
    if USE_HF_TOKENIZER:
        hf_tokens = count_hf_tokens(texts)

    result = {
        "input": entry,
        "qid": used_qid,
        "title": title,
        "n_sentences": len(texts),
        "sentences": sentences,
        "word_token_count": word_tokens,
        "hf_token_count": hf_tokens
    }
    return result

# ---------------- IO (crawler) ----------------

def load_input_list(path):
    """
    Accept CSV with columns 'url' or 'qid' and/or 'name' OR newline file.
    Always return list of dicts: {'url':..., 'qid':..., 'name':...}
    """
    items = []

    # detect encoding
    with open(path, "rb") as f:
        raw = f.read(65536)
        enc = chardet.detect(raw).get("encoding") or "utf-8"

    # CSV case
    if path.lower().endswith(".csv"):
        with open(path, newline='', encoding=enc, errors="replace") as cf:
            reader = csv.DictReader(cf)
            if not reader.fieldnames:
                cf.seek(0)
                for line in cf:
                    v = line.strip()
                    if not v:
                        continue
                    if re.match(r'^https?://', v, re.IGNORECASE):
                        items.append({"url": v})
                    elif re.match(r'^Q\d+$', v):
                        items.append({"qid": v})
                    else:
                        items.append({"name": v})
            else:
                for row in reader:
                    entry = {}
                    for h, v in row.items():
                        if v is None:
                            continue
                        key = h.strip().lower()
                        val = v.strip()
                        if not val:
                            continue
                        if key == "url":
                            entry["url"] = val
                        elif key == "qid":
                            entry["qid"] = val
                        elif key == "name":
                            entry["name"] = val
                    if not entry:
                        for v in row.values():
                            if v and v.strip():
                                vv = v.strip()
                                if re.match(r'^https?://', vv, re.IGNORECASE):
                                    entry["url"] = vv
                                elif re.match(r'^Q\d+$', vv):
                                    entry["qid"] = vv
                                else:
                                    entry["name"] = vv
                                break
                    if entry:
                        items.append(entry)
    else:
        with open(path, "r", encoding=enc, errors="replace") as f:
            for line in f:
                v = line.strip()
                if not v:
                    continue
                if re.match(r'^https?://', v, re.IGNORECASE):
                    items.append({"url": v})
                elif re.match(r'^Q\d+$', v):
                    items.append({"qid": v})
                else:
                    items.append({"name": v})
    return items

def save_jsonl_line(filepath, obj):
    with open(filepath, "a", encoding='utf-8') as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def crawl_main(args):
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    os.makedirs(os.path.dirname(args.outfile) or ".", exist_ok=True)

    items = load_input_list(args.input)
    total = len(items)
    print(f"Loaded {total} input items.")

    for entry in tqdm(items, desc="Crawling"):
        key = entry.get("qid") or entry.get("name") or entry.get("url")

        res = process_person_input(
            entry,
            need_sentences=args.n_sentences,
            session=session,
            pause=args.pause
        )

        if res.get("error"):
            name_in = entry.get("name") or entry.get("label") or entry.get("qid") or entry.get("url")
            if isinstance(name_in, str) and "_" in name_in and " " not in name_in:
                name_in = name_in.replace("_", " ")
            out_obj = {
                "name": name_in,
                "text": None,
                "error": res.get("error")
            }
            save_jsonl_line(args.outfile, out_obj)

        else:
            sentences = res.get("sentences", [])
            texts = [s.get("sentence", "").strip() for s in sentences]

            joined_text = " ".join(texts).strip()

            name_in = entry.get("name") or entry.get("label") or res.get("title") or entry.get("qid") or entry.get("url")
            if isinstance(name_in, str) and "_" in name_in and " " not in name_in:
                name_in = name_in.replace("_", " ")

            out_obj = {
                "name": name_in,
                "text": joined_text
            }
            save_jsonl_line(args.outfile, out_obj)

# ------------------ Cleaner code (from clean_nobel_content.py) ------------------

def clean_text(text):
    """
    Xóa:
    - Phần metadata/navigation ở đầu (thường không có dấu câu)
    - Tất cả các đoạn nằm trong dấu ngoặc vuông []
    - Các ký tự đặc biệt như ⓘ
    """
    match = re.search(r'\b[A-Z][a-z]+\s+[A-Z][a-z]+.*?[.!?]', text)
    if match:
        text = text[match.start():]

    text = re.sub(r'\[[^\]]*\]', '', text)

    text = text.replace('ⓘ', '')

    text = re.sub(r'\s+', ' ', text)

    return text.strip()

def split_sentences(text):
    """
    Tách văn bản thành các câu dựa trên dấu câu kết thúc (. ! ?)
    Xử lý cẩn thận các trường hợp viết tắt và số thập phân.
    """
    text = re.sub(r'\bDr\.', 'Dr<DOT>', text)
    text = re.sub(r'\bMr\.', 'Mr<DOT>', text)
    text = re.sub(r'\bMrs\.', 'Mrs<DOT>', text)
    text = re.sub(r'\bMs\.', 'Ms<DOT>', text)
    text = re.sub(r'\bProf\.', 'Prof<DOT>', text)
    text = re.sub(r'\bSr\.', 'Sr<DOT>', text)
    text = re.sub(r'\bJr\.', 'Jr<DOT>', text)
    text = re.sub(r'\bInc\.', 'Inc<DOT>', text)
    text = re.sub(r'\bCo\.', 'Co<DOT>', text)
    text = re.sub(r'\bLtd\.', 'Ltd<DOT>', text)
    text = re.sub(r'\b([A-Z])\.', r'\1<DOT>', text)
    text = re.sub(r'(\d+)\.(\d+)', r'\1<DOT>\2', text)

    sentences = re.split(r'([.!?])\s+(?=[A-Z])|([.!?])$', text)

    result = []
    current = ""

    for part in sentences:
        if part is None:
            continue
        if part in '.!?':
            current += part
            if current.strip():
                current = current.replace('<DOT>', '.')
                result.append(current.strip())
            current = ""
        else:
            current += part

    if current.strip():
        current = current.replace('<DOT>', '.')
        result.append(current.strip())

    return [s for s in result if s]

def truncate_to_10_sentences(text):
    """
    Nếu văn bản có > 10 câu, chỉ giữ lại 10 câu đầu tiên.
    Trả về tuple (text_mới, số_câu_thực_tế)
    """
    sentences = split_sentences(text)
    actual_count = len(sentences)

    if actual_count > 10:
        truncated = ' '.join(sentences[:10])
        return truncated, 10

    return text, actual_count

def process_jsonl(input_path, output_path):
    """
    Đọc file jsonl đầu vào, làm sạch trường 'text' (xóa nội dung trong []),
    cắt giảm xuống tối đa 10 câu,
    rồi ghi ra file jsonl mới.
    """
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Không tìm thấy file đầu vào: {input_path}")

    total_records = 0
    truncated_records = 0

    with open(input_path, 'r', encoding='utf-8') as infile, \
         open(output_path, 'w', encoding='utf-8') as outfile:

        for line_num, line in enumerate(infile, 1):
            line = line.strip()
            if not line:
                continue

            try:
                data = json.loads(line)
                total_records += 1

                if 'text' in data:
                    data['text'] = clean_text(data['text'])

                    truncated_text, sentence_count = truncate_to_10_sentences(data['text'])

                    if sentence_count == 10 and len(split_sentences(data['text'])) > 10:
                        truncated_records += 1
                        if line_num <= 5:
                            print(f"Dòng {line_num}: Cắt từ {len(split_sentences(data['text']))} câu xuống 10 câu")

                    data['text'] = truncated_text

                    if 'n_sentences' in data:
                        data['n_sentences'] = sentence_count

                json.dump(data, outfile, ensure_ascii=False)
                outfile.write('\n')

            except json.JSONDecodeError as e:
                print(f"Cảnh báo: Lỗi JSON ở dòng {line_num}: {e}")
                continue
            except Exception as e:
                print(f"Cảnh báo: Lỗi xử lý ở dòng {line_num}: {e}")
                continue

    print(f"\n{'='*50}")
    print(f"Đã xử lý xong! File output: {output_path}")
    print(f"Tổng số bản ghi: {total_records}")
    print(f"Số bản ghi bị cắt giảm xuống 10 câu: {truncated_records}")
    print(f"{'='*50}")

def build_cli_and_run():
    parser = argparse.ArgumentParser(description="Preprocess utilities: crawl and clean for NER/RE finetune data")
    sub = parser.add_subparsers(dest='cmd', required=True)

    # Crawl subcommand (keeps original args)
    p_crawl = sub.add_parser('crawl', help='Crawl Wikipedia pages and produce JSONL of texts')
    p_crawl.add_argument("--input", required=True, help="CSV file with column 'url' or 'qid' or 'name', or newline file")
    p_crawl.add_argument("--outfile", required=True, help="Output JSONL file (one JSON object per line)")
    p_crawl.add_argument("--n_sentences", type=int, default=10, help="Number of sentences to collect per person")
    p_crawl.add_argument("--pause", type=float, default=0.5, help="Seconds pause between requests")

    # Clean subcommand (keeps original args)
    p_clean = sub.add_parser('clean', help='Clean JSONL texts and truncate to max 10 sentences')
    p_clean.add_argument("-i", "--input", required=True, help="Path to input JSONL")
    p_clean.add_argument("-o", "--output", required=True, help="Path to output JSONL")

    args = parser.parse_args()

    if args.cmd == 'crawl':
        crawl_main(args)
    elif args.cmd == 'clean':
        process_jsonl(args.input, args.output)

if __name__ == '__main__':
    build_cli_and_run()
