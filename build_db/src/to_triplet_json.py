import csv
import json
from pathlib import Path

input_file = Path(__file__).parent.parent / "neo4j_import" / "Relations.csv"
output_file = Path(__file__).parent.parent / "triplets.json"

triplets = []

with open(str(input_file), newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        triplets.append({
            "start": row["start_id"],
            "relation": row["relation"],
            "end": row["end_name"]
        })

with open(str(output_file), "w", encoding="utf-8") as f:
    json.dump(triplets, f, ensure_ascii=False, indent=2)

print(f"Saved {len(triplets)} triplets to {output_file}")
