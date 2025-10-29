import csv
import os
from pathlib import Path

INPUT_CSV = "wikidata/allNobel.csv"
OUT_DIR = Path("neo4j_import")
OUT_DIR.mkdir(exist_ok=True)


# Helper to write unique nodes
def add_node(node_dict, node_type, name, extra=None):
    if not name or name.strip() == "":
        return
    name = name.strip()
    if node_type not in node_dict:
        node_dict[node_type] = {}
    if name not in node_dict[node_type]:
        node_dict[node_type][name] = {"name": name}
        if extra:
            node_dict[node_type][name].update(extra)


def write_nodes(node_dict):
    for node_type, entries in node_dict.items():
        if not entries:
            continue
        path = OUT_DIR / f"{node_type}.csv"
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=sorted(entries[next(iter(entries))].keys()))
            writer.writeheader()
            writer.writerows(entries.values())


def write_rels(rels):
    path = OUT_DIR / "Relations.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["start_id", "relation", "end_name"])
        writer.writeheader()
        writer.writerows(rels)


def main():
    nodes = {}
    rels = []

    with open(INPUT_CSV, newline='', encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        for row in reader:
            # Parse core person and award info
            person_id = row["laureate"].split("/")[-1]
            person_name = row["laureateLabel"].strip()
            award_id = row["award"].split("/")[-1]
            award_name = row["awardLabel"].strip()
            year = row["year"].strip()
            motivation = row["has_motivation_en"].replace("\n", " ").strip()

            # --- Add core nodes ---
            add_node(nodes, "Person", person_name, {"id": person_id})
            add_node(nodes, "Award", award_name, {"id": award_id})
            add_node(nodes, "AwardStatement", f"{person_id}_{year}", {"year": year, "motivation": motivation})

            # --- Relationships ---
            rels.append({"start_id": person_name, "relation": "RECEIVED", "end_name": f"{person_id}_{year}"})
            rels.append({"start_id": f"{person_id}_{year}", "relation": "INSTANCE_OF", "end_name": award_name})

            # --- Helper for list-type fields ---
            def handle_list(field, label, rel):
                if not row.get(field):
                    return
                values = [v.strip() for v in row[field].split(",") if v.strip()]
                for v in values:
                    add_node(nodes, label, v)
                    rels.append({"start_id": person_name, "relation": rel, "end_name": v})

            # --- Map fields to labels ---
            handle_list("is_citizen_of", "Country", "CITIZEN_OF")
            handle_list("works_as", "Occupation", "OCCUPATION")
            handle_list("works_in_field", "Field", "FIELD_OF_WORK")
            handle_list("educated_at", "Organization", "EDUCATED_AT")
            handle_list("employed_by", "Organization", "EMPLOYED_AT")
            handle_list("is_member_of", "MemberOrg", "MEMBER_OF")

    # --- Write output ---
    write_nodes(nodes)
    write_rels(rels)
    print(f"✅ Exported {len(rels)} relations and {sum(len(v) for v in nodes.values())} nodes to {OUT_DIR}/")


if __name__ == "__main__":
    main()
