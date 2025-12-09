import re
import unicodedata
from pathlib import Path

try:
    import jsonlines
except Exception:
    raise SystemExit("Please install jsonlines (pip install jsonlines)")


INPUT = Path("outputs/nobel_laureates_extraction.jsonl")
OUTPUT = Path("outputs/nobel_laureates_extraction_refined.jsonl")


def normalize_text(s: str) -> str:
    if not s:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"[^0-9a-z]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_rel_type(s: str) -> str:
    if not s:
        return ""
    s = str(s).strip().upper()
    s = re.sub(r"[^0-9A-Z]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def build_name_label_map(records):
    m = {}
    for r in records:
        for ent in r.get("entities", []) or []:
            name = ent.get("name")
            if not name:
                continue
            lbl = ent.get("label", "").strip()
            if lbl.lower() in ("work", "notable_work"):
                lbl = "Notable_Work"
            m.setdefault(name, set()).add(lbl)
    return m


def has_label(name, label, name_label_map):
    if not name:
        return False
    labels = name_label_map.get(name)
    if not labels:
        return False
    return label in labels


def valid_relation(rtype, head_name, tail_name, name_label_map):
    if rtype == "RECEIVED":
        return has_label(head_name, "Person", name_label_map) and has_label(tail_name, "Award", name_label_map)
    if rtype == "IS_CITIZEN_OF":
        return has_label(head_name, "Person", name_label_map) and has_label(tail_name, "Country", name_label_map)
    if rtype == "WORKS_AS":
        return has_label(head_name, "Person", name_label_map) and has_label(tail_name, "Occupation", name_label_map)
    if rtype == "WORKS_IN_FIELD":
        return has_label(head_name, "Person", name_label_map) and has_label(tail_name, "Field", name_label_map)
    if rtype == "EDUCATED_AT":
        return has_label(head_name, "Person", name_label_map) and has_label(tail_name, "Organization", name_label_map)
    if rtype == "EMPLOYED_BY":
        return has_label(head_name, "Person", name_label_map) and has_label(tail_name, "Organization", name_label_map)
    if rtype == "IS_MEMBER_OF":
        return has_label(head_name, "Person", name_label_map) and has_label(tail_name, "Organization", name_label_map)
    if rtype == "HOLDS_POSITION":
        return has_label(head_name, "Person", name_label_map) and has_label(tail_name, "Position", name_label_map)
    if rtype == "IS_SPOUSE_OF":
        cond1 = has_label(head_name, "Person", name_label_map) and has_label(tail_name, "Person", name_label_map)
        cond2 = has_label(head_name, "Person", name_label_map) and has_label(tail_name, "Person_Non_Laureate", name_label_map)
        cond3 = has_label(tail_name, "Person", name_label_map) and has_label(head_name, "Person_Non_Laureate", name_label_map)
        return cond1 or cond2 or cond3
    if rtype == "PARTICIPATED_IN":
        return has_label(head_name, "Person", name_label_map) and has_label(tail_name, "Event", name_label_map)
    if rtype == "FOUNDED":
        return has_label(head_name, "Person", name_label_map) and has_label(tail_name, "Organization", name_label_map)
    if rtype == "CO_FOUNDED":
        return has_label(head_name, "Person", name_label_map) and has_label(tail_name, "Organization", name_label_map)
    if rtype == "CO_DISCOVERED_WITH":
        cond1 = has_label(head_name, "Person", name_label_map) and has_label(tail_name, "Person", name_label_map)
        cond2 = has_label(head_name, "Person", name_label_map) and has_label(tail_name, "Person_Non_Laureate", name_label_map)
        cond3 = has_label(tail_name, "Person", name_label_map) and has_label(head_name, "Person_Non_Laureate", name_label_map)
        return cond1 or cond2 or cond3
    if rtype == "DEVELOPED":
        return has_label(head_name, "Person", name_label_map) and has_label(tail_name, "Notable_Work", name_label_map)
    return False


REL_MAP = {"STUDIED_AT": "EDUCATED_AT"}

ALLOWED_TYPES = {
    "RECEIVED",
    "IS_CITIZEN_OF",
    "WORKS_AS",
    "WORKS_IN_FIELD",
    "EDUCATED_AT",
    "EMPLOYED_BY",
    "IS_MEMBER_OF",
    "HOLDS_POSITION",
    "IS_SPOUSE_OF",
    "PARTICIPATED_IN",
    "FOUNDED",
    "CO_FOUNDED",
    "CO_DISCOVERED_WITH",
    "DEVELOPED",
}


def is_name_match(name_norm, record_name_norm):
    """Check if names match (exact, substring, or word-level)"""
    if not name_norm or not record_name_norm:
        return False

    # Exact match
    if name_norm == record_name_norm:
        return True

    # Substring match
    if name_norm in record_name_norm or record_name_norm in name_norm:
        return True

    # Word-level match: check if all words from shorter name appear in longer name
    name_words = set(name_norm.split())
    record_words = set(record_name_norm.split())

    if len(name_words) > 0 and len(record_words) > 0:
        # If one is subset of other, they match
        if name_words.issubset(record_words) or record_words.issubset(name_words):
            return True

    return False
def refine_records(records):
    name_label_map = build_name_label_map(records)

    # Build list of normalized record names for checking PNL match
    record_names_norm = [normalize_text(r.get("name", "")) for r in records]

    refined = []
    total_entities_removed = 0
    total_rels_removed = 0

    for r in records:
        record_name = r.get("name", "")
        record_name_norm = normalize_text(record_name)

        new_r = {k: v for k, v in r.items() if k not in ("entities", "relations")}

        # entities
        new_entities = []
        removed_entity_names = set()
        for ent in r.get("entities", []) or []:
            name = ent.get("name")
            if not name:
                continue
            lbl = ent.get("label", "").strip()
            if lbl.lower() in ("work", "notable_work"):
                lbl = "Notable_Work"

            # remove PNL if its normalized name matches any record name (exact or substring)
            if lbl.lower() == "person_non_laureate":
                name_norm = normalize_text(name)
                should_remove = False
                for rec_name_norm in record_names_norm:
                    if is_name_match(name_norm, rec_name_norm):
                        should_remove = True
                        break

                if should_remove:
                    total_entities_removed += 1
                    removed_entity_names.add(name)
                    continue

            new_entities.append({"name": name, "label": lbl})

            if name in name_label_map:
                name_label_map[name].add(lbl)
            else:
                name_label_map[name] = {lbl}

        new_r["entities"] = new_entities

        # relations
        new_relations = []
        for rel in r.get("relations", []) or []:
            head = rel.get("head", "")
            tail = rel.get("tail", "")
            raw_type = rel.get("type", "")

            rtype = normalize_rel_type(raw_type)
            rtype = REL_MAP.get(rtype, rtype)

            # drop AUTHOR_OF
            if rtype == "AUTHOR_OF":
                total_rels_removed += 1
                continue

            # drop if head/tail removed due to record-name match
            if head in removed_entity_names or tail in removed_entity_names:
                total_rels_removed += 1
                continue

            # Person <-> Notable_Work => DEVELOPED (Person -> Notable_Work)
            if has_label(head, "Person", name_label_map) and has_label(tail, "Notable_Work", name_label_map):
                rtype = "DEVELOPED"
            elif has_label(tail, "Person", name_label_map) and has_label(head, "Notable_Work", name_label_map):
                head, tail = tail, head
                rtype = "DEVELOPED"
            # If rtype is DEVELOPED but doesn't match Person->Notable_Work, drop it
            elif rtype == "DEVELOPED":
                total_rels_removed += 1
                continue

            if rtype not in ALLOWED_TYPES:
                total_rels_removed += 1
                continue

            # drop relations between two Person_Non_Laureate
            if has_label(head, "Person_Non_Laureate", name_label_map) and has_label(tail, "Person_Non_Laureate", name_label_map):
                total_rels_removed += 1
                continue

            if not valid_relation(rtype, head, tail, name_label_map):
                total_rels_removed += 1
                continue

            new_relations.append({"head": head, "tail": tail, "type": rtype})

        new_r["relations"] = new_relations
        refined.append(new_r)

    return refined, total_entities_removed, total_rels_removed


def main():
    if not INPUT.exists():
        print(f"Input not found: {INPUT}")
        return

    print(f"Reading input {INPUT}...")
    records = []
    with jsonlines.open(INPUT) as reader:
        for obj in reader:
            records.append(obj)

    print(f"Loaded {len(records)} records")

    refined, ents_removed, rels_removed = refine_records(records)

    print(f"Writing refined output to {OUTPUT}...")
    with jsonlines.open(OUTPUT, mode="w") as writer:
        for obj in refined:
            writer.write(obj)

    print("Done.")
    print(f"Entities removed: {ents_removed}")
    print(f"Relations removed: {rels_removed}")


if __name__ == "__main__":
    main()
