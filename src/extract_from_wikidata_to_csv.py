import csv
import os
from pathlib import Path

INPUT_CSV = "network-of-awards-and-winners/wikidata/allNobel.csv"
OUT_DIR = Path("network-of-awards-and-winners/neo4j_import")
OUT_DIR.mkdir(exist_ok=True)


# --- Helper to write unique nodes ---
def add_node(node_dict, node_type, name, extra=None):
    if not name or name.strip() == "":
        return
    name = name.strip()
    if node_type not in node_dict:
        node_dict[node_type] = {}
    if name not in node_dict[node_type]:
        node_dict[node_type][name] = {"name": name}
    if extra:
        node_dict[node_type][name].update({k: v for k, v in extra.items() if v})


# --- Write node CSVs ---
def write_nodes(node_dict):
    for node_type, entries in node_dict.items():
        if not entries:
            continue
        path = OUT_DIR / f"{node_type}.csv"

        # ✅ Lấy toàn bộ key từ tất cả các node (không bỏ sót trường nào)
        all_fields = set()
        for e in entries.values():
            all_fields.update(e.keys())
        fieldnames = sorted(all_fields)

        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in entries.values():
                writer.writerow(row)



# --- Write relations CSV ---
def write_rels(rels):
    path = OUT_DIR / "Relations.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["start_id", "relation", "end_name"])
        writer.writeheader()
        writer.writerows(rels)


def main():
    nodes = {}
    rels = []

    with open(INPUT_CSV, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # --- Extract basic info ---
            person_id = row["laureate"].split("/")[-1]
            person_name = row["laureateName"].strip()
            award_id = row["award"].split("/")[-1]
            award_name = row["awardName"].strip()
            year = row["year"].strip()
            motivation = (row.get("has_motivation") or "").replace("\n", " ").strip()

            # --- Person attributes ---
            person_attrs = {
                "id": person_id,
                "family_name": row.get("has_family_name", "").strip(),
                "gender": row.get("has_gender", "").strip(),
                "born_on_date": row.get("born_on_date", "").strip(),
                "died_on_date": row.get("died_on_date", "").strip(),
                "notable_work": row.get("has_notable_work", "").strip(),
            }

            # --- Add nodes ---
            add_node(nodes, "Person", person_name, person_attrs)
            add_node(nodes, "Award", award_name, {"id": award_id})
            add_node(nodes, "AwardStatement", f"{person_id}_{year}", {"year": year, "motivation": motivation})

            # --- Relationships ---
            rels.append({"start_id": person_name, "relation": "RECEIVED", "end_name": f"{person_id}_{year}"})
            rels.append({"start_id": f"{person_id}_{year}", "relation": "IS_INSTANCE_OF", "end_name": award_name})

            # --- Helper for multi-value fields ---
            def handle_list(field, label, rel):
                val = row.get(field)
                if not val:
                    return
                values = [v.strip() for v in val.split(",") if v.strip()]
                for v in values:
                    add_node(nodes, label, v)
                    rels.append({"start_id": person_name, "relation": rel, "end_name": v})

            # --- Map list fields to relationships ---
            handle_list("is_citizen_of", "Country", "IS_CITIZEN_OF")
            handle_list("works_as", "Occupation", "WORKS_AS")
            handle_list("works_in_field", "Field", "WORKS_IN_FIELD")
            handle_list("educated_at", "Organization", "EDUCATED_AT")
            handle_list("employed_by", "Organization", "EMPLOYED_BY")
            handle_list("is_member_of", "Organization", "IS_MEMBER_OF")
            handle_list("holds_position", "Position", "HOLDS_POSITION")



    # --- Write outputs ---
    write_nodes(nodes)
    write_rels(rels)
    print(f"✅ Exported {len(rels)} relations and {sum(len(v) for v in nodes.values())} nodes to {OUT_DIR}/")


if __name__ == "__main__":
    main()
