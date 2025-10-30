import csv
import json

# Đường dẫn đến file CSV
input_file = "neo4j_import/Relations.csv"
output_file = "triplets.json"

triplets = []

# Đọc file CSV
with open(input_file, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        triplets.append({
            "start": row["start_id"],
            "relation": row["relation"],
            "end": row["end_name"]
        })

# Ghi ra file JSON
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(triplets, f, ensure_ascii=False, indent=2)

print(f"✅ Đã lưu {len(triplets)} triplets vào {output_file}")
