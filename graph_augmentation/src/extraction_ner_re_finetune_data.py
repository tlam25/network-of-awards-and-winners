#!/usr/bin/env python3
# ===================================================================
# Gemini 2.5 Flash — NER + Relation Extraction Pipeline (IMPROVED)
# ===================================================================

import os
import json
import re
import difflib
import argparse
import time
import random
import google.generativeai as genai
from typing import List, Set
from dotenv import load_dotenv

# -----------------------------
# CONFIG
# -----------------------------
load_dotenv()

GEMINI_API_KEYS = os.getenv("GEMINI_API_KEYS", "").split(",")
GEMINI_API_KEYS = [k.strip() for k in GEMINI_API_KEYS if k.strip()]

if not GEMINI_API_KEYS:
    raise ValueError("No valid API keys found! Set GEMINI_API_KEYS in .env file")

MODEL_NAME = "models/gemini-2.5-flash"
MAX_RETRIES_PER_RECORD = 10  # Số lần retry tối đa cho 1 record

# ---------------------------------------------------------
# JSON Extraction & Parsing (UNCHANGED)
# ---------------------------------------------------------
def extract_first_json(s: str):
    if not s:
        return None
    json_match = re.search(r'```json\s*(\{.*?\})\s*```', s, re.DOTALL)
    if json_match:
        return json_match.group(1)
    code_match = re.search(r'```\s*(\{.*?\})\s*```', s, re.DOTALL)
    if code_match:
        return code_match.group(1)
    start = s.find("{")
    if start == -1:
        return None
    stack = 0
    in_string = False
    esc = False
    for i in range(start, len(s)):
        ch = s[i]
        if ch == '"' and not esc:
            in_string = not in_string
        if ch == "\\" and not esc:
            esc = True
            continue
        esc = False
        if in_string:
            continue
        if ch == "{":
            stack += 1
        elif ch == "}":
            stack -= 1
            if stack == 0:
                return s[start:i+1]
    json_pattern = r'\{\s*"name"\s*:\s*"[^"]*"\s*,\s*"text"\s*:\s*"[^"]*".*?\}'
    partial_match = re.search(json_pattern, s, re.DOTALL)
    if partial_match:
        return partial_match.group(0)
    return None


def parse_json_response(text: str):
    if not text:
        return None
    try:
        return json.loads(text)
    except:
        pass
    json_str = extract_first_json(text)
    if json_str:
        try:
            return json.loads(json_str, strict=False)
        except:
            pass
        try:
            fixed_str = json_str.replace('\n', '\\n').replace('\r', '')
            return json.loads(fixed_str, strict=False)
        except:
            pass
    try:
        fixed = re.sub(r',(\s*[}\]])', r'\1', text)
        return json.loads(fixed, strict=False)
    except:
        pass
    try:
        name_match = re.search(r'"name"\s*:\s*"([^"]*)"', text)
        text_match = re.search(r'"text"\s*:\s*"((?:[^"\\]|\\.)*)"', text, re.DOTALL)
        entities_match = re.search(r'"entities"\s*:\s*(\[.*?\])', text, re.DOTALL)
        relations_match = re.search(r'"relations"\s*:\s*(\[.*?\])', text, re.DOTALL)
        if name_match and text_match:
            result = {
                "name": name_match.group(1),
                "text": text_match.group(1),
                "entities": [],
                "relations": []
            }
            if entities_match:
                try:
                    result["entities"] = json.loads(entities_match.group(1))
                except:
                    pass
            if relations_match:
                try:
                    result["relations"] = json.loads(relations_match.group(1))
                except:
                    pass
            return result
    except:
        pass
    return None


# ---------------------------------------------------------
# Country normalization (GIỮ LẠI - vẫn hữu ích)
# ---------------------------------------------------------
IRREGULAR_COUNTRIES = {
    "French": "France", "Dutch": "Netherlands", "Greek": "Greece",
    "Thai": "Thailand", "Swiss": "Switzerland", "Czech": "Czech Republic",
    "Danish": "Denmark", "British": "United Kingdom", "English": "United Kingdom",
    "Scottish": "United Kingdom", "Welsh": "United Kingdom", "Irish": "Ireland",
    "Spanish": "Spain", "Portuguese": "Portugal", "American": "United States",
    "US": "United States", "U.S.": "United States"
}

def normalize_country(text: str):
    if not text:
        return text
    t = text.strip()
    if t in IRREGULAR_COUNTRIES:
        return IRREGULAR_COUNTRIES[t]
    lower = t.lower()
    if lower.endswith("ish"):
        base = t[:-3]
        if base.lower() in ("engl", "eng", "brit"):
            return "United Kingdom"
        return base.capitalize() + "land"
    if lower.endswith("ese"):
        base = t[:-3]
        if base.lower() in ("chin", "china"):
            return "China"
        if base.lower() in ("japan",):
            return "Japan"
        return base.capitalize()
    if lower.endswith("ian"):
        base = t[:-3]
        if base.lower() in ("hungar",):
            return "Hungary"
        return base.capitalize() + "a"
    if lower.endswith("i"):
        base = t[:-1]
        return base.capitalize()
    if lower.endswith("ic"):
        base = t[:-2]
        return base.capitalize() + "ia"
    return " ".join([p.capitalize() for p in t.split()])


# ---------------------------------------------------------
# Merge entities (GIỮ LẠI - cần thiết)
# ---------------------------------------------------------
def merge_similar_by_name(entities: List[dict], threshold=0.85):
    merged = []
    name_map = {}
    for e in entities:
        name = (e.get("name") or "").strip()
        label = e.get("label") or e.get("type")
        if not name:
            continue
        found = False
        for m in merged:
            sim = difflib.SequenceMatcher(None, name.lower(), m["name"].lower()).ratio()
            if sim >= threshold:
                if not m.get("label") and label:
                    m["label"] = label
                name_map[name] = m["name"]
                found = True
                break
        if not found:
            merged.append({"name": name, "label": label})
            name_map[name] = name
    return merged, name_map


# ---------------------------------------------------------
# Remap relations (GIỮ LẠI - cần thiết)
# ---------------------------------------------------------
def remap_relations(relations, id_to_name=None, name_map=None):
    out = []
    for r in relations or []:
        h = r.get("head")
        t = r.get("tail")
        typ = r.get("type") or r.get("relation") or r.get("label")
        if h is None or t is None:
            continue
        h_name = id_to_name.get(h, h) if id_to_name else h
        t_name = id_to_name.get(t, t) if id_to_name else t
        if name_map:
            h_name = name_map.get(h_name, h_name)
            t_name = name_map.get(t_name, t_name)
        if h_name.lower() == t_name.lower():
            continue
        out.append({"head": h_name, "tail": t_name, "type": typ})
    seen = set()
    dedup = []
    for r in out:
        key = (r["head"].lower(), r["tail"].lower(), r["type"])
        if key not in seen:
            dedup.append(r)
            seen.add(key)
    return dedup


# ---------------------------------------------------------
# Remove orphan entities (GIỮ LẠI - cần thiết)
# ---------------------------------------------------------
def remove_orphan_entities(entities, relations):
    linked = set()
    for r in relations:
        linked.add(r["head"])
        linked.add(r["tail"])
    return [e for e in entities if e["name"] in linked]


# ---------------------------------------------------------
# Load laureates names (MỚI)
# ---------------------------------------------------------
def load_laureates_names(path: str) -> Set[str]:
    """Load tất cả tên người đoạt giải từ input file"""
    laureates = set()
    with open(path, 'r', encoding='utf8') as f:
        for line in f:
            try:
                record = json.loads(line)
                name = record.get("name", "").strip()
                if name:
                    laureates.add(name.lower())
            except:
                continue
    return laureates


# ---------------------------------------------------------
# IMPROVED PROMPT với Person_Non_Laureate và Notable_Work
# ---------------------------------------------------------
def build_prompt(text: str, laureate_name: str) -> str:
    return f"""You are an expert information extraction agent for building a knowledge graph about Nobel Prize laureates.

**CRITICAL INSTRUCTIONS:**

1. **EXTRACT ONLY DIRECT INFORMATION ABOUT THE LAUREATE: "{laureate_name}"**
   - This person is the Nobel Prize winner we're focusing on
   - Extract their awards, positions, affiliations, discoveries, works, relationships

2. **DISTINGUISH PERSON TYPES:**
   - **Person (Laureate)**: The main laureate "{laureate_name}" - ALWAYS label as "Person"
   - **Person_Non_Laureate**: Other individuals mentioned (family, colleagues, co-workers who are NOT Nobel laureates themselves)
   - If unsure whether someone is a laureate, label as "Person_Non_Laureate"

3. **ENTITY TYPES** (extract if directly related to laureate):
   - **Person**: The main laureate ONLY
   - **Person_Non_Laureate**: Other individuals mentioned (not Nobel laureates)
   - **Award**: Nobel Prize, other prizes
   - **Organization**: Universities, companies, institutions
   - **Position**: Job titles, roles
   - **Field**: Scientific/academic fields
   - **Occupation**: Professions
   - **Country**: Nationalities, places of citizenship
   - **Event**: Conferences, wars, historical events
   - **Location**: Cities, regions, specific places
   - **Notable_Work**: Discoveries, inventions, theories, scientific findings, biological structures, techniques

4. **RELATION TYPES** (laureate must be head or tail):
   - **RECEIVED**: Person → Award (won prize)
   - **IS_CITIZEN_OF**: Person → Country (nationality)
   - **WORKS_AS**: Person → Occupation (job type)
   - **WORKS_IN_FIELD**: Person → Field (research area)
   - **EDUCATED_AT**: Person → Organization (studied)
   - **STUDIED_AT**: Person → Organization (same as EDUCATED_AT)
   - **EMPLOYED_BY**: Person → Organization (worked for)
   - **IS_MEMBER_OF**: Person → Organization (membership)
   - **HOLDS_POSITION**: Person → Position (current role)
   - **IS_SPOUSE_OF**: Person ↔ Person (married, ONLY if both are laureates)
   - **AUTHOR_OF**: Person → Work (wrote book/paper)
   - **PARTICIPATED_IN**: Person → Event (attended)
   - **DISCOVERED**: Person → Notable_Work (made discovery)
   - **INVENTED**: Person → Notable_Work (created invention)
   - **DEVELOPED**: Person → Notable_Work (developed theory/model/technique)
   - **FOUNDED**: Person → Organization (established organization/school)
   - **CO_FOUNDED**: Person → Organization (co-established with others)
   - **CO_DISCOVERED_WITH**: Person → Person_Non_Laureate (collaborated on discovery)
   - **IS_A**: Entity → Category (type relationship, e.g., "DNA structure" IS_A "Notable_Work")

5. **DO NOT USE THESE RELATIONS:**
   - IS_PARENT_OF, IS_FATHER_OF, IS_MOTHER_OF, IS_SIBLING_OF, IS_RELATED_TO
   - WORKED_ON (use WORKS_IN_FIELD or DISCOVERED/DEVELOPED instead)

6. **OUTPUT FORMAT:**
   Return STRICTLY VALID JSON (no markdown, no code blocks):
   {{"name": "{laureate_name}", "text": "original_text", "entities": [{{"name": "...", "label": "..."}}], "relations": [{{"head": "...", "tail": "...", "type": "..."}}]}}

**EXAMPLES:**

Example 1:
Text: "Marie Curie won the Nobel Prize in Physics in 1903 for her work on radioactivity. She was French."
Output: {{"name": "Marie Curie", "text": "Marie Curie won the Nobel Prize in Physics in 1903 for her work on radioactivity. She was French.", "entities": [{{"name": "Marie Curie", "label": "Person"}}, {{"name": "Nobel Prize in Physics", "label": "Award"}}, {{"name": "radioactivity", "label": "Field"}}, {{"name": "France", "label": "Country"}}], "relations": [{{"head": "Marie Curie", "tail": "Nobel Prize in Physics", "type": "RECEIVED"}}, {{"head": "Marie Curie", "tail": "radioactivity", "type": "WORKS_IN_FIELD"}}, {{"head": "Marie Curie", "tail": "France", "type": "IS_CITIZEN_OF"}}]}}

Example 2:
Text: "Peter Agre discovered aquaporin water channels. He worked with his colleague John Smith at Johns Hopkins."
Output: {{"name": "Peter Agre", "text": "Peter Agre discovered aquaporin water channels. He worked with his colleague John Smith at Johns Hopkins.", "entities": [{{"name": "Peter Agre", "label": "Person"}}, {{"name": "aquaporin water channels", "label": "Notable_Work"}}, {{"name": "John Smith", "label": "Person_Non_Laureate"}}, {{"name": "Johns Hopkins", "label": "Organization"}}], "relations": [{{"head": "Peter Agre", "tail": "aquaporin water channels", "type": "DISCOVERED"}}, {{"head": "Peter Agre", "tail": "John Smith", "type": "CO_DISCOVERED_WITH"}}, {{"head": "Peter Agre", "tail": "Johns Hopkins", "type": "EMPLOYED_BY"}}]}}

**NOW EXTRACT FROM THIS TEXT:**

-----
{text}
-----

Return ONLY the JSON object, nothing else:"""


# ---------------------------------------------------------
# Call Gemini (GIỮ NGUYÊN)
# ---------------------------------------------------------
def call_gemini_for_extraction(prompt: str, api_keys):
    base_wait = 2
    attempt = 0
    while True:
        try:
            key = random.choice(api_keys)
            genai.configure(api_key=key)
            model = genai.GenerativeModel(MODEL_NAME)
            safety_settings = [
                {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"}
            ]
            response = model.generate_content(
                prompt,
                generation_config={"temperature": 0.0, "max_output_tokens": 8192},
                safety_settings=safety_settings
            )
            raw = response.text if hasattr(response, "text") else str(response)
            return raw
        except Exception as e:
            error_msg = str(e)
            attempt += 1
            if "429" in error_msg or "Resource has been exhausted" in error_msg or "500" in error_msg:
                wait_time = (base_wait * (2 ** min(attempt, 6))) + random.uniform(0, 3)
                wait_time = min(wait_time, 60)
                print(f"   Rate limit/quota (attempt {attempt}), waiting {wait_time:.1f}s...")
                time.sleep(wait_time)
            else:
                print(f"   API error (attempt {attempt}): {type(e).__name__}")
                time.sleep(2)


# ---------------------------------------------------------
# Validate result (MỚI)
# ---------------------------------------------------------
def is_valid_result(data: dict) -> bool:
    """Kiểm tra xem result có entities VÀ relations không rỗng"""
    if not data:
        return False
    entities = data.get("entities", [])
    relations = data.get("relations", [])

    # Yêu cầu phải có ít nhất 1 entity VÀ 1 relation
    return len(entities) > 0 and len(relations) > 0


# ---------------------------------------------------------
# Map Notable_Work types (MỚI)
# ---------------------------------------------------------
def normalize_entity_types(entities: List[dict]) -> List[dict]:
    """Chuyển Discovery, ScientificFinding, etc. thành Notable_Work"""
    notable_work_types = {"Discovery", "ScientificFinding", "BiologicalStructure", "Invention"}

    for e in entities:
        if e.get("label") in notable_work_types:
            e["label"] = "Notable_Work"

    return entities


# ---------------------------------------------------------
# Main pipeline với validation (IMPROVED)
# ---------------------------------------------------------
def run_pipeline(text: str, laureate_name: str, api_keys, laureates_set: Set[str]):
    """
    Pipeline với retry limit và validation
    Trả về: (result_dict, success_flag)
    """
    attempt = 0

    while attempt < MAX_RETRIES_PER_RECORD:
        attempt += 1

        # Build prompt with laureate name
        prompt = build_prompt(text, laureate_name)

        # Call API
        raw = call_gemini_for_extraction(prompt, api_keys)

        # Parse JSON
        data = parse_json_response(raw)

        if data is None:
            print(f"   No valid JSON (attempt {attempt}/{MAX_RETRIES_PER_RECORD})")
            time.sleep(2)
            continue

        # Validate structure
        entities_input = data.get("entities", [])
        relations_input = data.get("relations", [])

        if not isinstance(entities_input, list) or not isinstance(relations_input, list):
            print(f"   Invalid structure (attempt {attempt}/{MAX_RETRIES_PER_RECORD})")
            time.sleep(2)
            continue

        # VALIDATION: Kiểm tra không rỗng
        if not is_valid_result(data):
            print(f"   Empty entities or relations (attempt {attempt}/{MAX_RETRIES_PER_RECORD})")
            time.sleep(2)
            continue

        # Process entities
        entities_normal = []
        for e in entities_input:
            if not isinstance(e, dict):
                continue
            name = (e.get("name") or "").strip()
            typ = e.get("label") or e.get("type")
            if not name:
                continue

            # Normalize country
            if typ in ("Country", "Nationality"):
                name = normalize_country(name)

            # Phân biệt Person vs Person_Non_Laureate
            if typ == "Person" and name.lower() != laureate_name.lower():
                # Kiểm tra xem có phải laureate khác không
                if name.lower() not in laureates_set:
                    typ = "Person_Non_Laureate"

            entities_normal.append({"name": name, "label": typ})

        # Normalize entity types (Notable_Work)
        entities_normal = normalize_entity_types(entities_normal)

        # Process relations
        relations_normalized = []
        for r in relations_input:
            if not isinstance(r, dict):
                continue
            head = (r.get("head") or "").strip()
            tail = (r.get("tail") or "").strip()
            typ = r.get("type") or r.get("relation") or r.get("label")

            if not head or not tail or not typ:
                continue

            # Skip WORKED_ON relations
            if typ == "WORKED_ON":
                continue

            # Normalize countries in relations
            head = normalize_country(head) if head in IRREGULAR_COUNTRIES or any(head.lower().endswith(suffix) for suffix in ["ish", "ese", "ian", "i", "ic"]) else head
            tail = normalize_country(tail) if tail in IRREGULAR_COUNTRIES or any(tail.lower().endswith(suffix) for suffix in ["ish", "ese", "ian", "i", "ic"]) else tail

            relations_normalized.append({"head": head, "tail": tail, "type": typ})

        # Merge and deduplicate
        merged_entities, name_map = merge_similar_by_name(entities_normal)
        final_rel = remap_relations(relations_normalized, id_to_name=None, name_map=name_map)

        # Remove orphans
        merged_entities = remove_orphan_entities(merged_entities, final_rel)

        # Final deduplication
        final_entities = []
        seen = set()
        for e in merged_entities:
            if e["name"] not in seen:
                final_entities.append(e)
                seen.add(e["name"])

        # Primary name
        primary = laureate_name
        for e in final_entities:
            if e["label"] == "Person" and e["name"].lower() == laureate_name.lower():
                primary = e["name"]
                break

        # Final validation
        result = {
            "name": primary,
            "text": data.get("text") or text,
            "entities": final_entities,
            "relations": final_rel
        }

        if is_valid_result(result):
            return result, True
        else:
            print(f"   Result still empty after processing (attempt {attempt}/{MAX_RETRIES_PER_RECORD})")
            time.sleep(2)

    # After MAX_RETRIES attempts, return empty result
    print(f"   Failed after {MAX_RETRIES_PER_RECORD} attempts, returning empty")
    return {
        "name": laureate_name,
        "text": text,
        "entities": [],
        "relations": []
    }, False


# ---------------------------------------------------------
# Load JSONL
# ---------------------------------------------------------
def load_jsonl(path):
    print(f"Loading JSONL: {path}")
    with open(path, "r", encoding="utf8") as f:
        return [json.loads(line) for line in f]


# ---------------------------------------------------------
# Save JSONL
# ---------------------------------------------------------
def save_jsonl(path, records):
    with open(path, "w", encoding="utf8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"Saved: {path}")


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------
if __name__ == "__main__":
    from tqdm import tqdm

    parser = argparse.ArgumentParser()
    parser.add_argument("--input_jsonl", required=True, help="Input JSONL file with laureate texts")
    parser.add_argument("--output_jsonl", required=True, help="Output JSONL file for successful extractions")
    parser.add_argument("--failed_jsonl", default="failed_records.jsonl", help="Output file for failed records")
    args = parser.parse_args()

    print(f"Loaded {len(GEMINI_API_KEYS)} API keys")
    print(f"Max retries per record: {MAX_RETRIES_PER_RECORD}")

    # Load input records
    records = load_jsonl(args.input_jsonl)
    total_records = len(records)

    # Load laureates names
    print(f"Loading laureates names from input...")
    laureates_set = load_laureates_names(args.input_jsonl)
    print(f"   Found {len(laureates_set)} unique laureates")

    # Check processed records
    processed_count = 0
    if os.path.exists(args.output_jsonl):
        print(f"Output file exists, counting processed records...")
        with open(args.output_jsonl, 'r', encoding='utf8') as f:
            processed_count = sum(1 for _ in f)
        print(f"   Found {processed_count} already processed records")

    remaining = total_records - processed_count

    if remaining <= 0:
        print(f"All {total_records} records already processed!")
        print("DONE.")
        exit(0)

    print(f"Processing {remaining} remaining records")

    # Open output files
    failed_records = []

    with open(args.output_jsonl, 'a', encoding='utf8') as output_file:
        with tqdm(total=remaining, desc="Processing", unit="record") as pbar:
            for idx in range(processed_count, total_records):
                item = records[idx]
                text = item.get("text", "")
                laureate_name = item.get("name", "")

                # Run pipeline with validation
                result, success = run_pipeline(text, laureate_name, GEMINI_API_KEYS, laureates_set)

                # Write to output
                output_file.write(json.dumps(result, ensure_ascii=False) + "\n")
                output_file.flush()

                # Track failures
                if not success:
                    failed_records.append({
                        "index": idx,
                        "name": laureate_name,
                        "text": text[:200] + "..."  # First 200 chars for reference
                    })

                pbar.update(1)
                pbar.set_postfix({
                    'done': processed_count + pbar.n,
                    'failed': len(failed_records)
                })

    # Save failed records
    if failed_records:
        save_jsonl(args.failed_jsonl, failed_records)
        print(f"\n{len(failed_records)} records failed after {MAX_RETRIES_PER_RECORD} retries")
        print(f"   Failed records saved to: {args.failed_jsonl}")

    print(f"\nSummary:")
    print(f"   Successfully processed: {total_records - len(failed_records)}")
    print(f"   Failed: {len(failed_records)}")
    print(f"   Output: {args.output_jsonl}")
    print("DONE.")
