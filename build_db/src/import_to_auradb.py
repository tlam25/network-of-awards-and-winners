import pandas as pd
from neo4j import GraphDatabase
from pathlib import Path
from tqdm import tqdm
import json
import time
from dotenv import load_dotenv
import os

load_dotenv()

URI = os.getenv("NEO4J_URI")
USER = os.getenv("NEO4J_USER")
PASSWORD = os.getenv("NEO4J_PASSWORD")

DATA_DIR = Path(__file__).parent.parent / "neo4j_import"

BATCH_SIZE = 1000

driver = GraphDatabase.driver(
    URI, 
    auth=(USER, PASSWORD),
    notifications_min_severity="OFF"
)


def clear_database():
    with driver.session() as session:
        print("Clearing all nodes and relationships...")
        session.run("MATCH (n) DETACH DELETE n")

        print("Clearing old constraints...")
        constraints = session.run("SHOW CONSTRAINTS")
        for c in constraints:
            session.run(f"DROP CONSTRAINT {c['name']}")
    print("Cleared old data and constraints")


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


def import_nodes(label, filename, fields):
    path = DATA_DIR / filename
    if not path.exists():
        print(f"File not found: {path}")
        return
    
    df = pd.read_csv(path)
    if df.empty:
        print(f"Skipping {label}: CSV is empty")
        return

    print(f"Importing {label} ({len(df)} rows) in batches of {BATCH_SIZE}...")

    # Tạo phần SET (bỏ 'name' vì dùng trong MERGE)
    set_fields = [f"n.{k} = row.{k}" for k in fields if k != "name"]
    set_clause = f"SET {', '.join(set_fields)}" if set_fields else ""

    query = f"""
    UNWIND $batch AS row
    MERGE (n:{label} {{name: row.name}})
    {set_clause}
    """

    # Chuẩn bị dữ liệu batch
    records = []
    for _, row in df.iterrows():
        props = {f: (str(row.get(f)) if pd.notna(row.get(f)) else None) for f in fields}
        props = {k: v for k, v in props.items() if v}
        if 'name' in props:
            records.append(props)

    if not records:
        print(f"No valid records found for {label}")
        return

    with driver.session() as session:
        for i in tqdm(range(0, len(records), BATCH_SIZE), desc=f"Importing {label}"):
            batch = records[i:i + BATCH_SIZE]
            session.run(query, batch=batch)

    print(f"Imported {label} ({len(records)} nodes)")


def import_relationships():
    rels_path = DATA_DIR / "Relations.csv"
    if not rels_path.exists():
        print("Relations.csv not found")
        return

    df = pd.read_csv(rels_path)
    if df.empty:
        print("Relations.csv is empty")
        return

    print(f"Importing {len(df)} relationships...")

    df_groups = df.groupby('relation')

    with driver.session() as session:
        for rel, group_df in df_groups:
            if rel == "RECEIVED":
                q = """
                UNWIND $batch AS row
                MATCH (p:Person {name: row.start_id})
                MATCH (a:AwardStatement {name: row.end_name})
                MERGE (p)-[:RECEIVED]->(a)
                """
            elif rel == "IS_INSTANCE_OF":
                q = """
                UNWIND $batch AS row
                MATCH (as:AwardStatement {name: row.start_id})
                MATCH (aw:Award {name: row.end_name})
                MERGE (as)-[:IS_INSTANCE_OF]->(aw)
                """
            elif rel == "IS_CITIZEN_OF":
                q = """
                UNWIND $batch AS row
                MATCH (p:Person {name: row.start_id})
                MATCH (c:Country {name: row.end_name})
                MERGE (p)-[:IS_CITIZEN_OF]->(c)
                """
            elif rel == "WORKS_AS":
                q = """
                UNWIND $batch AS row
                MATCH (p:Person {name: row.start_id})
                MATCH (o:Occupation {name: row.end_name})
                MERGE (p)-[:WORKS_AS]->(o)
                """
            elif rel == "WORKS_IN_FIELD":
                q = """
                UNWIND $batch AS row
                MATCH (p:Person {name: row.start_id})
                MATCH (f:Field {name: row.end_name})
                MERGE (p)-[:WORKS_IN_FIELD]->(f)
                """
            elif rel == "EDUCATED_AT":
                q = """
                UNWIND $batch AS row
                MATCH (p:Person {name: row.start_id})
                MATCH (org:Organization {name: row.end_name})
                MERGE (p)-[:EDUCATED_AT]->(org)
                """
            elif rel == "EMPLOYED_BY":
                q = """
                UNWIND $batch AS row
                MATCH (p:Person {name: row.start_id})
                MATCH (org:Organization {name: row.end_name})
                MERGE (p)-[:EMPLOYED_BY]->(org)
                """
            elif rel == "IS_MEMBER_OF":
                q = """
                UNWIND $batch AS row
                MATCH (p:Person {name: row.start_id})
                MATCH (org:Organization {name: row.end_name})
                MERGE (p)-[:IS_MEMBER_OF]->(org)
                """
            elif rel == "HOLDS_POSITION":
                q = """
                UNWIND $batch AS row
                MATCH (p:Person {name: row.start_id})
                MATCH (pos:Position {name: row.end_name})
                MERGE (p)-[:HOLDS_POSITION]->(pos)
                """
            else:
                print(f"Unknown relation type skipped: {rel}")
                continue

            records = group_df.to_dict('records')
            for i in tqdm(range(0, len(records), BATCH_SIZE), desc=f"Importing {rel}"):
                batch = records[i:i + BATCH_SIZE]
                session.run(q, batch=batch)

    print("All relationships imported")


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

    clear_database()
    create_constraints()

    print("\n--- Importing Nodes ---")
    import_nodes("Person", "Person.csv", ["name", "id", "family_name", "gender", "born_on_date", "died_on_date", "notable_work"])
    import_nodes("Award", "Award.csv", ["name"])
    import_nodes("AwardStatement", "AwardStatement.csv", ["name", "year", "motivation"])
    import_nodes("Country", "Country.csv", ["name"])
    import_nodes("Occupation", "Occupation.csv", ["name"])
    import_nodes("Field", "Field.csv", ["name"])
    import_nodes("Organization", "Organization.csv", ["name"])
    import_nodes("Position", "Position.csv", ["name"])

    print("\n--- Importing Relationships ---")
    import_relationships()

    output_file = Path(__file__).parent.parent / "nobel_network_local.json"
    export_graph_to_json(str(output_file))

    driver.close()
    total_time = time.time() - start_time
    print(f"\nDone! Total time: {total_time:.2f} seconds")


if __name__ == "__main__":
    main()
