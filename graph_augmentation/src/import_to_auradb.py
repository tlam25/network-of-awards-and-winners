import pandas as pd
from neo4j import GraphDatabase
from pathlib import Path
from tqdm import tqdm
import json
import time
import jsonlines
from dotenv import load_dotenv
import os

load_dotenv()

URI = os.getenv("NEO4J_URI")
USER = os.getenv("NEO4J_USER")
PASSWORD = os.getenv("NEO4J_PASSWORD")

JSONL_FILE = Path(__file__).parent.parent / "outputs" / "nobel_laureates_extraction_refined.jsonl"

BATCH_SIZE = 100

driver = GraphDatabase.driver(
    URI, 
    auth=(USER, PASSWORD),
    notifications_min_severity="OFF"
)


# --- Tạo constraint mới (Neo4j 5+) nếu chưa có ---
def create_constraints():
    print("Creating constraints (indexes)...")
    with driver.session() as session:
        session.run("CREATE CONSTRAINT person_name_unique IF NOT EXISTS FOR (n:Person) REQUIRE n.name IS UNIQUE")
        session.run("CREATE CONSTRAINT award_name_unique IF NOT EXISTS FOR (n:Award) REQUIRE n.name IS UNIQUE")
        session.run("CREATE CONSTRAINT awardstatement_name_unique IF NOT EXISTS FOR (n:AwardStatement) REQUIRE n.name IS UNIQUE")
        session.run("CREATE CONSTRAINT country_name_unique IF NOT EXISTS FOR (n:Country) REQUIRE n.name IS UNIQUE")
        session.run("CREATE CONSTRAINT occupation_name_unique IF NOT EXISTS FOR (n:Occupation) REQUIRE n.name IS UNIQUE")
        session.run("CREATE CONSTRAINT field_name_unique IF NOT EXISTS FOR (n:Field) REQUIRE n.name IS UNIQUE")
        session.run("CREATE CONSTRAINT organization_name_unique IF NOT EXISTS FOR (n:Organization) REQUIRE n.name IS UNIQUE")
        session.run("CREATE CONSTRAINT position_name_unique IF NOT EXISTS FOR (n:Position) REQUIRE n.name IS UNIQUE")
    print("Constraints created successfully")


# --- Import từ JSONL ---
def import_from_jsonl():
    """
    Import dữ liệu từ file JSONL.
    Mỗi record có: name, text, entities[], relations[]
    """
    if not JSONL_FILE.exists():
        print(f"File not found: {JSONL_FILE}")
        return

    print(f"Reading JSONL file: {JSONL_FILE}")

    # Đọc toàn bộ dữ liệu vào memory
    records = []
    with jsonlines.open(JSONL_FILE) as reader:
        for obj in reader:
            records.append(obj)

    if not records:
        print("JSONL file is empty")
        return

    print(f"Total records: {len(records)}")

    # Import nodes từ entities
    print("\n--- Importing Nodes ---")
    import_nodes_from_entities(records)

    # Import relationships
    print("\n--- Importing Relationships ---")
    import_relationships_from_jsonl(records)

    print("All data from JSONL imported successfully")


def import_nodes_from_entities(records):
    """
    Tạo nodes từ các entities trong JSONL.
    Mỗi entity có: name, label (loại node)
    """
    # Thu thập tất cả unique entities
    entities_by_label = {}

    for record in records:
        if 'entities' not in record:
            continue
        for entity in record['entities']:
            label = entity.get('label', 'Unknown')
            name = entity.get('name', '')
            if not name:
                continue

            if label not in entities_by_label:
                entities_by_label[label] = set()
            entities_by_label[label].add(name)

    # Import từng loại node
    with driver.session() as session:
        for label, names in entities_by_label.items():
            # Chuẩn hóa tên label cho Neo4j
            neo4j_label = label.replace(' ', '_').replace('-', '_')

            print(f"Importing {label} nodes ({len(names)} unique)...")

            batch = [{"name": name} for name in names]

            # Batch import
            for i in tqdm(range(0, len(batch), BATCH_SIZE), desc=f"Importing {label}"):
                batch_slice = batch[i:i + BATCH_SIZE]
                query = f"""
                UNWIND $batch AS row
                MERGE (n:{neo4j_label} {{name: row.name}})
                """
                session.run(query, batch=batch_slice)

            print(f"Imported {label}: {len(names)} nodes")


def import_relationships_from_jsonl(records):
    """
    Tạo relationships từ relations trong JSONL.
    Mỗi relation có: head, tail, type
    """
    # Thu thập tất cả relationships
    relations_by_type = {}

    for record in records:
        if 'relations' not in record:
            continue
        for rel in record['relations']:
            rel_type = rel.get('type', 'UNKNOWN')
            head = rel.get('head', '')
            tail = rel.get('tail', '')
            if not head or not tail:
                continue

            if rel_type not in relations_by_type:
                relations_by_type[rel_type] = []
            relations_by_type[rel_type].append({"head": head, "tail": tail})

    print(f"Found {len(relations_by_type)} relation types")

    # Import từng loại relationship
    with driver.session() as session:
        for rel_type, rels in relations_by_type.items():
            print(f"Importing {rel_type} relationships ({len(rels)} edges)...")

            # Batch import - try to match nodes by name
            for i in tqdm(range(0, len(rels), BATCH_SIZE), desc=f"Importing {rel_type}"):
                batch = rels[i:i + BATCH_SIZE]
                query = f"""
                UNWIND $batch AS row
                MATCH (start {{name: row.head}})
                MATCH (end {{name: row.tail}})
                MERGE (start)-[:{rel_type}]->(end)
                """
                try:
                    session.run(query, batch=batch)
                except Exception as e:
                    print(f"Error importing {rel_type}: {e}")
                    # Continue with next batch

            print(f"Imported {rel_type}: {len(rels)} relationships")



def export_graph_to_json(local_filename):
    """
    Xuất toàn bộ graph ra file JSON local bằng APOC.
    Neo4j Aura trả về NDJSON (nhiều dòng JSON nối nhau), ta cần xử lý thủ công.
    """
    print(f"\nExporting graph to {local_filename}...")

    query = "CALL apoc.export.json.all(null, {stream: true}) YIELD data"

    try:
        with driver.session() as session:
            result = session.run(query)

            all_records = []
            for record in result:
                data_str = record["data"]
                # Mỗi record có thể chứa nhiều dòng NDJSON → tách theo newline
                for line in data_str.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        all_records.append(obj)
                    except json.JSONDecodeError as e:
                        print(f"Skip invalid JSON line: {e}")

            # Lưu toàn bộ vào file JSON
            print(f"Writing {len(all_records)} records to {local_filename} ...")
            with open(local_filename, "w", encoding="utf-8") as f:
                json.dump(all_records, f, indent=2, ensure_ascii=False)

        print(f"Exported graph to {local_filename}")

    except Exception as e:
        print(f"Error during JSON export: {e}")
        print("Note: APOC plugin must be enabled (AuraDB usually has it pre-installed).")



def main():
    start_time = time.time()

    create_constraints()

    print("\n--- Importing from JSONL ---")
    import_from_jsonl()

    export_graph_to_json("outputs/nobel_network_local.json")

    driver.close()
    total_time = time.time() - start_time
    print(f"\nDone! Total time: {total_time:.2f} seconds")


if __name__ == "__main__":
    main()
