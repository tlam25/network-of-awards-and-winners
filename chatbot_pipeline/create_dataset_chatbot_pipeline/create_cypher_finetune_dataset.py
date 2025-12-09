import os
import random
import time
import json
import re
import ast
import csv
import pandas as pd
from dotenv import load_dotenv
from neo4j import GraphDatabase
import google.generativeai as genai
from tqdm import tqdm
import multiprocessing
from multiprocessing import Manager, Process
import warnings

warnings.filterwarnings("ignore", category=FutureWarning, module="google.api_core")
warnings.filterwarnings("ignore", message="You are using a Python version")

# --- CONFIGURATION ---
load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
GEMINI_API_KEYS = os.getenv("GEMINI_API_KEYS", "").split(",")

# Lọc key rỗng
GEMINI_API_KEYS = [k.strip() for k in GEMINI_API_KEYS if k.strip()]

# OLD ENTITIES/RELATIONSHIPS
OLD_NODES = {
    "Person",
    "Award", 
    "AwardStatement",
    "Country",
    "Occupation",
    "Field",
    "Organization",
    "Position"
}

OLD_RELATIONSHIPS = {
    "RECEIVED",
    "IS_INSTANCE_OF",
    "IS_CITIZEN_OF",
    "WORKS_AS",
    "WORKS_IN_FIELD",
    "EDUCATED_AT",
    "EMPLOYED_BY",
    "IS_MEMBER_OF",
    "HOLDS_POSITION"
}

# NEW ENTITIES (augmented data)
NEW_NODES = {
    "Person_Non_Laureate",
    "Notable_Work",
    "Event",
    "Location"
}

# NEW RELATIONSHIPS (augmented data)
NEW_RELATIONSHIPS = {
    "DEVELOPED",           
    "CO_DISCOVERED_WITH",  
    "FOUNDED",             
    "CO_FOUNDED",          
    "PARTICIPATED_IN",     
    "IS_SPOUSE_OF"         
}

# Schema Definition - Bao gồm cũ và mới
SCHEMA_NODES = {
    # Old nodes
    "Person": '"Person": properties: ["id", "name", "family_name", "gender", "born_on_date", "died_on_date", "notable_work"]',
    "Award": '"Award": properties: ["name"]',
    "AwardStatement": '"AwardStatement": properties: ["name", "year", "motivation"]',
    "Country": '"Country": properties: ["name"]',
    "Occupation": '"Occupation": properties: ["name"]',
    "Field": '"Field": properties: ["name"]',
    "Organization": '"Organization": properties: ["name"]',
    "Position": '"Position": properties: ["name"]',
    # New nodes
    "Person_Non_Laureate": '"Person_Non_Laureate": properties: ["id", "name", "family_name", "gender", "born_on_date", "died_on_date"]',
    "Notable_Work": '"Notable_Work": properties: ["name", "description"]',
    "Event": '"Event": properties: ["name", "date"]',
    "Location": '"Location": properties: ["name"]'
}

SCHEMA_RELS = {
    # Old relationships
    "RECEIVED": '(:Person)-[:RECEIVED]->(:AwardStatement)',
    "IS_INSTANCE_OF": '(:AwardStatement)-[:IS_INSTANCE_OF]->(:Award)',
    "IS_CITIZEN_OF": '(:Person)-[:IS_CITIZEN_OF]->(:Country)',
    "WORKS_AS": '(:Person)-[:WORKS_AS]->(:Occupation)',
    "WORKS_IN_FIELD": '(:Person)-[:WORKS_IN_FIELD]->(:Field)',
    "EDUCATED_AT": '(:Person)-[:EDUCATED_AT]->(:Organization)',
    "EMPLOYED_BY": '(:Person)-[:EMPLOYED_BY]->(:Organization)',
    "IS_MEMBER_OF": '(:Person)-[:IS_MEMBER_OF]->(:Organization)',
    "HOLDS_POSITION": '(:Person)-[:HOLDS_POSITION]->(:Position)',
    # New relationships
    "CO_FOUNDED": '(:Person)-[:CO_FOUNDED]->(:Organization)',
    "IS_SPOUSE_OF": '(:Person)-[:IS_SPOUSE_OF]->(:Person)',
    "DEVELOPED": '(:Person)-[:DEVELOPED]->(:Notable_Work)',
    "CO_DISCOVERED_WITH": '(:Person)-[:CO_DISCOVERED_WITH]->(:Person)',
    "FOUNDED": '(:Person)-[:FOUNDED]->(:Organization)',
    "PARTICIPATED_IN": '(:Person)-[:PARTICIPATED_IN]->(:Event)'
}

# --- HELPER FUNCTIONS ---

def extract_relevant_schema(cypher_query: str) -> str:
    """Trích xuất schema nodes và relationships liên quan từ Cypher query"""
    tokens = set(re.findall(r':(\w+)', cypher_query))
    found_nodes = []
    found_rels = []

    for token in tokens:
        if token in SCHEMA_NODES:
            found_nodes.append(SCHEMA_NODES[token])
        if token in SCHEMA_RELS:
            found_rels.append(SCHEMA_RELS[token])

    context_parts = []
    if found_nodes:
        context_parts.append("Relevant Nodes:\n" + "\n".join(sorted(found_nodes)))
    if found_rels:
        context_parts.append("Relevant Relationships:\n" + "\n".join(sorted(found_rels)))

    return "\n\n".join(context_parts)

def uses_new_entities_or_relations(cypher_query: str) -> bool:
    """
    Kiểm tra xem Cypher query có sử dụng entity hoặc relationship mới không.
    """
    # Extract nodes và relationships từ query
    nodes = set(re.findall(r':(\w+)', cypher_query))
    rels = set(re.findall(r'\[:(\w+)\]', cypher_query))
    
    # Check nếu có bất kỳ node hoặc relationship mới nào
    has_new_node = bool(nodes & NEW_NODES)
    has_new_rel = bool(rels & NEW_RELATIONSHIPS)
    
    return has_new_node or has_new_rel

def uses_only_old_entities_and_relations(cypher_query: str) -> bool:
    """
    Kiểm tra xem Cypher query CHỈ sử dụng entity và relationship cũ (không có new entities).
    """
    return not uses_new_entities_or_relations(cypher_query)

def clean_and_parse_json(text: str):
    """Parse JSON từ response của Gemini với nhiều fallback strategies"""
    # Regex tìm JSON object
    match = re.search(r'\{.*\}', text, re.DOTALL)
    json_str = match.group(0) if match else text
    json_str = json_str.strip()

    try:
        return json.loads(json_str, strict=False)
    except:
        pass

    try:
        # fix lỗi xuống dòng trong string json
        fixed_str = json_str.replace('\n', '\\n').replace('\r', '')
        return json.loads(fixed_str, strict=False)
    except:
        pass

    try:
        return ast.literal_eval(json_str)
    except:
        pass

    # Fallback: Regex extract từng phần
    try:
        q_match = re.search(r'"question"\s*:\s*"(.*?)(?<!\\)"', json_str, re.DOTALL)
        t_match = re.search(r'"thinking"\s*:\s*"(.*?)(?<!\\)"', json_str, re.DOTALL)
        if q_match and t_match:
            return {
                "question": q_match.group(1).replace('\\"', '"'),
                "thinking": t_match.group(1).replace('\\"', '"')
            }
    except:
        pass

    return None

# --- NEO4J HANDLER ---
class Neo4jHandler:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def get_all_node_names(self, label, property_name="name"):
        """Lấy toàn bộ danh sách tên để cache, tránh query random liên tục"""
        query = f"MATCH (n:{label}) WHERE n.{property_name} IS NOT NULL RETURN n.{property_name} as value"
        with self.driver.session() as session:
            try:
                result = session.run(query)
                return [record["value"] for record in result]
            except:
                return []

    def check_path_exists(self, query):
        """Kiểm tra xem path sinh ra có dữ liệu thật không"""
        with self.driver.session() as session:
            try:
                result = session.run(query)
                return result.peek() is not None
            except:
                return False

# --- GEMINI API HANDLER ---

def call_gemini_retry(prompt, api_keys):
    """Gọi Gemini API với retry logic và exponential backoff"""
    max_retries = 10
    base_wait = 2  # giây

    for attempt in range(max_retries):
        try:
            # Chọn key ngẫu nhiên
            key = random.choice(api_keys)
            genai.configure(api_key=key)
            model = genai.GenerativeModel('gemini-2.5-flash-lite')

            # Cấu hình safety để tránh bị block vô lý
            safety_settings = [
                {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"}
            ]

            response = model.generate_content(prompt, safety_settings=safety_settings)

            result = clean_and_parse_json(response.text)
            if result and "question" in result and "thinking" in result:
                return result
            else:
                raise ValueError("JSON Parse Fail or Empty")

        except Exception as e:
            error_msg = str(e)
            # Nếu hết quota (429) hoặc lỗi server (5xx)
            if "429" in error_msg or "Resource has been exhausted" in error_msg or "500" in error_msg:
                # Exponential Backoff: Chờ 2, 4, 8, 16... giây + jitter ngẫu nhiên
                wait_time = (base_wait * (2 ** attempt)) + random.uniform(0, 3)
                # Cap thời gian chờ tối đa 60s
                wait_time = min(wait_time, 60)
                time.sleep(wait_time)
            else:
                # Lỗi khác thì chờ ít hơn
                time.sleep(2)

    return None

# --- CYPHER GENERATION LOGIC ---

def generate_cypher_old_entities(hops, entity_cache):
    """
    Tạo Cypher query CHỈ SỬ DỤNG các entity/relationship CŨ.
    Dùng cho dataset gốc (nobel_graph_raw_full.csv).
    """
    # Lấy ngẫu nhiên từ cache
    person_name = random.choice(entity_cache.get('Person', ['Marie Curie']))
    award_name = random.choice(entity_cache.get('Award', ['Physics']))
    country_name = random.choice(entity_cache.get('Country', ['USA']))
    organization_name = random.choice(entity_cache.get('Organization', ['MIT']))
    
    templates = []
    
    if hops == 1:
        templates = [
            f'MATCH (p:Person {{name: "{person_name}"}}) RETURN p.born_on_date',
            f'MATCH (p:Person {{name: "{person_name}"}})-[:IS_CITIZEN_OF]->(c:Country) RETURN c.name',
            f'MATCH (p:Person {{name: "{person_name}"}})-[:WORKS_AS]->(o:Occupation) RETURN o.name',
            f'MATCH (p:Person {{name: "{person_name}"}})-[:WORKS_IN_FIELD]->(f:Field) RETURN f.name'
        ]
    elif hops == 2:
        templates = [
            f'MATCH (p1:Person {{name: "{person_name}"}})-[:IS_CITIZEN_OF]->(c:Country)<-[:IS_CITIZEN_OF]-(p2:Person) RETURN p2.name',
            f'MATCH (p:Person)-[:RECEIVED]->(ast:AwardStatement)-[:IS_INSTANCE_OF]->(a:Award {{name: "{award_name}"}}) RETURN p.name',
            f'MATCH (p:Person {{name: "{person_name}"}})-[:WORKS_IN_FIELD]->(f:Field)<-[:WORKS_IN_FIELD]-(p2:Person) RETURN p2.name'
        ]
    elif hops == 3:
        templates = [
            f'MATCH (a:Award {{name: "{award_name}"}})<-[:IS_INSTANCE_OF]-(ast:AwardStatement)<-[:RECEIVED]-(p:Person)-[:IS_CITIZEN_OF]->(c:Country) RETURN c.name',
            f'MATCH (p:Person {{name: "{person_name}"}})-[:EDUCATED_AT]->(o:Organization)<-[:EDUCATED_AT]-(p2:Person) RETURN p2.name, o.name'
        ]
    elif hops == 4:
        templates = [
            f'MATCH (a:Award {{name: "{award_name}"}})<-[:IS_INSTANCE_OF]-(ast:AwardStatement)<-[:RECEIVED]-(p1:Person)-[:WORKS_IN_FIELD]->(f:Field)<-[:WORKS_IN_FIELD]-(p2:Person) RETURN p2.name',
            f'MATCH (c:Country {{name: "{country_name}"}})<-[:IS_CITIZEN_OF]-(p:Person)-[:EMPLOYED_BY]->(o:Organization)<-[:EMPLOYED_BY]-(p2:Person) RETURN p2.name, o.name'
        ]
    
    # Shuffle và return
    random.shuffle(templates)
    return templates[0] if templates else None

def generate_cypher_new_entities(hops, entity_cache):
    """
    Tạo Cypher query SỬ DỤNG các entity/relationship MỚI.
    Đảm bảo mỗi query có ít nhất 1 NEW node hoặc NEW relationship.
    Dùng cho dataset augmented (nobel_graph_augment_raw.csv).
    """
    # Lấy ngẫu nhiên entities từ cache
    person = random.choice(entity_cache.get('Person', ['Marie Curie']))
    person_nl = random.choice(entity_cache.get('Person_Non_Laureate', [])) if entity_cache.get('Person_Non_Laureate') else None
    notable_work = random.choice(entity_cache.get('Notable_Work', [])) if entity_cache.get('Notable_Work') else None
    event = random.choice(entity_cache.get('Event', [])) if entity_cache.get('Event') else None
    organization = random.choice(entity_cache.get('Organization', ['MIT']))
    
    templates = []
    
    if hops == 1:
        templates = [
            # NEW: Notable_Work → DEVELOPED → Person
            f'MATCH (nw:Notable_Work {{name: "{notable_work}"}})<-[:DEVELOPED]-(p:Person) RETURN p.name' if notable_work else None,
            # NEW: Event → PARTICIPATED_IN → Person
            f'MATCH (e:Event {{name: "{event}"}})<-[:PARTICIPATED_IN]-(p:Person) RETURN p.name' if event else None,
            # NEW: Person_Non_Laureate → CO_DISCOVERED_WITH → Person
            f'MATCH (pnl:Person_Non_Laureate {{name: "{person_nl}"}})<-[:CO_DISCOVERED_WITH]-(p:Person) RETURN p.name' if person_nl else None,
            # NEW: Person_Non_Laureate → IS_SPOUSE_OF → Person
            f'MATCH (pnl:Person_Non_Laureate {{name: "{person_nl}"}})<-[:IS_SPOUSE_OF]-(p:Person) RETURN p.name' if person_nl else None,
            # NEW: Organization → FOUNDED → Person
            f'MATCH (o:Organization {{name: "{organization}"}})<-[:FOUNDED]-(p:Person) RETURN p.name',
            # NEW: Organization → CO_FOUNDED → Person
            f'MATCH (o:Organization {{name: "{organization}"}})<-[:CO_FOUNDED]-(p:Person) RETURN p.name',
        ]
    
    elif hops == 2:
        templates = [
            # NEW: Notable_Work → DEVELOPED → Person
            f'MATCH (nw:Notable_Work {{name: "{notable_work}"}})<-[:DEVELOPED]-(p:Person) RETURN p.name, p.gender' if notable_work else None,
            # NEW: Event → PARTICIPATED_IN → Person
            f'MATCH (e:Event {{name: "{event}"}})<-[:PARTICIPATED_IN]-(p:Person) RETURN p.name, p.born_on_date' if event else None,
            # NEW: Person_Non_Laureate → CO_DISCOVERED_WITH → Person
            f'MATCH (pnl:Person_Non_Laureate {{name: "{person_nl}"}})<-[:CO_DISCOVERED_WITH]-(p:Person) RETURN p.name' if person_nl else None,
            # NEW: Organization → FOUNDED → Person
            f'MATCH (o:Organization {{name: "{organization}"}})<-[:FOUNDED]-(p:Person) RETURN p.name',
            # NEW: Notable_Work → DEVELOPED → Person → DEVELOPED → Notable_Work khác
            f'MATCH (nw:Notable_Work {{name: "{notable_work}"}})<-[:DEVELOPED]-(p:Person)-[:DEVELOPED]->(nw2:Notable_Work) WHERE nw.name <> nw2.name RETURN p.name, nw2.name' if notable_work else None,
            # NEW: Event → PARTICIPATED_IN → Person → DEVELOPED → Notable_Work
            f'MATCH (e:Event)<-[:PARTICIPATED_IN]-(p:Person)-[:DEVELOPED]->(nw:Notable_Work) RETURN p.name, nw.name',
        ]
    
    elif hops == 3:
        templates = [
            # NEW: Notable_Work → DEVELOPED → Person → DEVELOPED → Notable_Work khác
            f'MATCH (nw1:Notable_Work {{name: "{notable_work}"}})<-[:DEVELOPED]-(p:Person)-[:DEVELOPED]->(nw2:Notable_Work) WHERE nw1.name <> nw2.name RETURN p.name, nw2.name' if notable_work else None,
            # NEW: Event → PARTICIPATED_IN → Person → DEVELOPED → Notable_Work
            f'MATCH (e:Event {{name: "{event}"}})<-[:PARTICIPATED_IN]-(p:Person)-[:DEVELOPED]->(nw:Notable_Work) RETURN p.name, nw.name' if event else None,
            # NEW: Organization → FOUNDED → Person → CO_DISCOVERED_WITH → Person_Non_Laureate
            f'MATCH (o:Organization {{name: "{organization}"}})<-[:FOUNDED]-(p:Person)-[:CO_DISCOVERED_WITH]->(pnl:Person_Non_Laureate) RETURN p.name, pnl.name',
            # NEW: Notable_Work → DEVELOPED → Person → PARTICIPATED_IN → Event
            f'MATCH (nw:Notable_Work {{name: "{notable_work}"}})<-[:DEVELOPED]-(p:Person), (p)-[:PARTICIPATED_IN]->(e:Event) RETURN p.name, e.name' if notable_work else None,
            # NEW: Organization → CO_FOUNDED → Person → IS_SPOUSE_OF → Person_Non_Laureate
            f'MATCH (o:Organization {{name: "{organization}"}})<-[:CO_FOUNDED]-(p:Person)-[:IS_SPOUSE_OF]->(pnl:Person_Non_Laureate) RETURN p.name, pnl.name',
            # NEW: Event → PARTICIPATED_IN → Person → FOUNDED → Organization
            f'MATCH (e:Event)<-[:PARTICIPATED_IN]-(p:Person), (o:Organization)<-[:FOUNDED]-(p) RETURN p.name, o.name',
        ]
    
    elif hops == 4:
        templates = [
            # NEW: Notable_Work → DEVELOPED → Person → CO_DISCOVERED_WITH → Person_Non_Laureate
            f'MATCH (nw:Notable_Work {{name: "{notable_work}"}})<-[:DEVELOPED]-(p:Person)-[:CO_DISCOVERED_WITH]->(pnl:Person_Non_Laureate) RETURN p.name, pnl.name' if notable_work else None,
            # NEW: Event → PARTICIPATED_IN → Person → DEVELOPED → Notable_Work ← DEVELOPED ← Person khác
            f'MATCH (e:Event {{name: "{event}"}})<-[:PARTICIPATED_IN]-(p1:Person)-[:DEVELOPED]->(nw:Notable_Work)<-[:DEVELOPED]-(p2:Person) WHERE p1.name <> p2.name RETURN p1.name, p2.name, nw.name' if event else None,
            # NEW: Organization → FOUNDED → Person → DEVELOPED → Notable_Work, Person → PARTICIPATED_IN → Event
            f'MATCH (o:Organization {{name: "{organization}"}})<-[:FOUNDED]-(p:Person)-[:DEVELOPED]->(nw:Notable_Work), (p)-[:PARTICIPATED_IN]->(e:Event) RETURN p.name, nw.name, e.name',
            # NEW: Notable_Work → DEVELOPED → Person → IS_SPOUSE_OF → Person_Non_Laureate, Person ← CO_FOUNDED ← Organization
            f'MATCH (nw:Notable_Work {{name: "{notable_work}"}})<-[:DEVELOPED]-(p:Person)-[:IS_SPOUSE_OF]->(pnl:Person_Non_Laureate), (o:Organization)<-[:CO_FOUNDED]-(p) RETURN p.name, pnl.name, o.name' if notable_work else None,
            # NEW: Event → PARTICIPATED_IN → Person ← FOUNDED ← Organization, Person → DEVELOPED → Notable_Work
            f'MATCH (e:Event)<-[:PARTICIPATED_IN]-(p:Person), (o:Organization)<-[:FOUNDED]-(p), (p)-[:DEVELOPED]->(nw:Notable_Work) RETURN p.name, o.name, nw.name',
        ]
    
    # Lọc None và shuffle
    templates = [t for t in templates if t is not None]
    if not templates:
        return None
        
    random.shuffle(templates)
    
    # Return template đầu tiên có NEW entities/relationships
    for temp in templates:
        if uses_new_entities_or_relations(temp):
            return temp
    
    return None

# --- CREATE PROMPT ---

def create_prompt_old_entities(cypher, hops, relevant_context):
    """Prompt cho dataset gốc (chỉ dùng old entities/relationships)"""
    return f"""
You are an expert creating a Text-to-Cypher dataset.

Context:
- English Schema: {relevant_context}
- Cypher Query: `{cypher}`
- Hops: {hops}

Task:
1. "question": Generate a natural language question in **VIETNAMESE**. Paraphrase English relationships naturally (e.g., `IS_CITIZEN_OF` -> "quê ở đâu", "người nước nào").

2. "thinking": Generate reasoning steps strictly in **ENGLISH**.
   - CRITICAL: Start by translating the question to English to bridge the language gap.
   - INSTRUCTION: You MUST use the literal string '\\n' to separate each step clearly.
   - Step 1: Translate the Vietnamese question into English.
   - Step 2: Identify entities and intent from the translated English question.
   - Step 3: Map keywords to the English Schema provided above.
   - Step 4: Construct the MATCH and RETURN clauses logic.

OUTPUT FORMAT REQUIREMENTS (CRITICAL):
- Return a **SINGLE LINE** JSON object.
- STRICTLY ESCAPE all double quotes inside strings with backslash (e.g., \"quoted text\").
- Insert '\\n' between steps in the thinking field.
- Do NOT use trailing commas.
- No markdown code blocks. Just the raw JSON string.

Example Output:
{{"question": "Marie Curie sinh năm bao nhiêu?", "thinking": "Step 1: Translate to English: 'What year was Marie Curie born?'\\nStep 2: Identify entity 'Marie Curie' and intent 'birth year'.\\nStep 3: Map 'birth year' to property 'born_on_date'.\\nStep 4: Construct MATCH (p:Person {{name: 'Marie Curie'}}) RETURN p.born_on_date."}}
"""

def create_prompt_new_entities(cypher, hops, relevant_context):
    """Prompt cho dataset augmented (dùng new entities/relationships)"""
    return f"""
You are an expert creating a Text-to-Cypher dataset.

Context:
- English Schema: {relevant_context}
- Cypher Query: `{cypher}`
- Hops: {hops}

Task:
1. "question": Generate a natural language question in **VIETNAMESE**. Paraphrase English relationships naturally:
   - `DEVELOPED` -> "phát triển", "tạo ra", "nghiên cứu" (Note: Notable_Work = "công trình", NOT "công việc")
   - `CO_DISCOVERED_WITH` -> "cùng khám phá với", "cộng tác với"
   - `FOUNDED` -> "thành lập", "sáng lập"
   - `CO_FOUNDED` -> "đồng sáng lập", "cùng thành lập"
   - `PARTICIPATED_IN` -> "tham gia", "tham dự"
   - `IS_SPOUSE_OF` -> "vợ/chồng của", "bạn đời"

2. "thinking": Generate reasoning steps strictly in **ENGLISH**.
   - CRITICAL: Start by translating the question to English to bridge the language gap.
   - INSTRUCTION: You MUST use the literal string '\\n' to separate each step clearly.
   - Step 1: Translate the Vietnamese question into English.
   - Step 2: Identify entities and intent from the translated English question.
   - Step 3: Map keywords to the English Schema provided above.
   - Step 4: Construct the MATCH and RETURN clauses logic.

OUTPUT FORMAT REQUIREMENTS (CRITICAL):
- Return a **SINGLE LINE** JSON object.
- STRICTLY ESCAPE all double quotes inside strings with backslash (e.g., \"quoted text\").
- Insert '\\n' between steps in the thinking field.
- Do NOT use trailing commas.
- No markdown code blocks. Just the raw JSON string.

Example Output:
{{"question": "Ai đã phát triển công trình Radioactivity?", "thinking": "Step 1: Translate to English: 'Who developed the work Radioactivity?'\\nStep 2: Identify entity 'Radioactivity' (Notable_Work) and intent 'who developed'.\\nStep 3: Map 'developed' to relationship 'DEVELOPED' and 'who' to Person node.\\nStep 4: Construct MATCH (nw:Notable_Work {{name: 'Radioactivity'}})<-[:DEVELOPED]-(p:Person) RETURN p.name."}}
"""

# --- WORKER LOGIC ---

def worker_task(hops_list, neo4j_config, api_keys, entity_cache, result_queue, use_new_entities=False):
    """
    Worker process:
    1. Nhận danh sách số hop cần làm.
    2. Sinh Cypher từ cache (offline) hoặc query Neo4j nhẹ.
    3. Gọi Gemini.
    4. Đẩy kết quả vào Queue.
    
    Args:
        use_new_entities: True = augmented dataset (new entities), False = original dataset (old entities only)
    """
    # Khởi tạo connection Neo4j riêng cho process này
    db = Neo4jHandler(neo4j_config['uri'], neo4j_config['user'], neo4j_config['pwd'])

    for hops in hops_list:
        # Sinh Cypher dựa trên mode
        cypher = None
        retry_count = 0
        max_retries = 100
        
        while not cypher and retry_count < max_retries:
            if use_new_entities:
                candidate_cypher = generate_cypher_new_entities(hops, entity_cache)
            else:
                candidate_cypher = generate_cypher_old_entities(hops, entity_cache)
            
            if candidate_cypher:
                # Kiểm tra xem query có trả về kết quả không
                if db.check_path_exists(candidate_cypher):
                    cypher = candidate_cypher
                    break
            retry_count += 1
        
        if not cypher:
            result_queue.put(None)  # Báo fail sau max_retries lần thử
            continue

        relevant_context = extract_relevant_schema(cypher)

        # Tạo prompt dựa trên mode
        if use_new_entities:
            prompt = create_prompt_new_entities(cypher, hops, relevant_context)
        else:
            prompt = create_prompt_old_entities(cypher, hops, relevant_context)

        data = call_gemini_retry(prompt, api_keys)

        if data:
            result_queue.put({
                "question": data.get("question", ""),
                "hops": hops,
                "cypher_query": cypher,
                "thinking": data.get("thinking", ""),
                "context": relevant_context
            })
        else:
            result_queue.put(None)  # Signal failure

    db.close()

# --- MAIN FUNCTION ---

def main(mode="old"):
    """
    Main function cho unified dataset generation.
    
    Args:
        mode: "old" = original dataset (old entities only), 
              "new" = augmented dataset (new entities)
    """
    use_new_entities = (mode == "new")
    
    if use_new_entities:
        print("--- Starting NEW ENTITIES Dataset Generation ---")
    else:
        print("--- Starting ORIGINAL Dataset Generation ---")

    if not GEMINI_API_KEYS:
        print("ERROR: Missing API Keys.")
        return

    # Sử dụng đường dẫn tuyệt đối dựa trên vị trí của script
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    
    if use_new_entities:
        RAW_FILE = os.path.join(SCRIPT_DIR, "../data/nobel_graph_augment_raw.csv")
    else:
        RAW_FILE = os.path.join(SCRIPT_DIR, "nobel_graph_raw_full.csv")
    
    HEADERS = ["question", "hops", "cypher_query", "thinking", "context"]

    print(f"\nWorking with file: {RAW_FILE}")

    # 1. Checkpoint Loading - Đếm chi tiết từng hop
    existing_counts = {1: 0, 2: 0, 3: 0, 4: 0}
    existing_total = 0

    if os.path.exists(RAW_FILE):
        try:
            print(f"Reading existing file (size: {os.path.getsize(RAW_FILE) / (1024*1024):.2f} MB)...")
            
            # Đọc file với các tùy chọn tối ưu cho file lớn
            df = pd.read_csv(RAW_FILE, encoding='utf-8-sig', on_bad_lines='skip')
            existing_total = len(df)
            
            print(f"Successfully loaded {existing_total} records")
            
            # Đếm chi tiết từng hop
            for hop in [1, 2, 3, 4]:
                existing_counts[hop] = len(df[df['hops'] == hop])
            
            print(f"\nCheckpoint Status:")
            print(f"   Total existing records: {existing_total}")
            print(f"   Hop 1: {existing_counts[1]} samples")
            print(f"   Hop 2: {existing_counts[2]} samples")
            print(f"   Hop 3: {existing_counts[3]} samples")
            print(f"   Hop 4: {existing_counts[4]} samples")
        except Exception as e:
            print(f" Warning: Could not read existing data!")
            print(f"   Error: {type(e).__name__}: {e}")
            print(f"   Starting fresh...")
            with open(RAW_FILE, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=HEADERS)
                writer.writeheader()
    else:
        print("No existing file found. Creating new file...")
        # Tạo thư mục nếu chưa tồn tại
        os.makedirs(os.path.dirname(RAW_FILE), exist_ok=True)
        with open(RAW_FILE, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=HEADERS)
            writer.writeheader()

    # 2. Calculate Needs - Chi tiết cho từng hop
    if use_new_entities:
        TOTAL_SAMPLES = 1500  # Giảm xuống vì chỉ focus vào new entities
        TARGET_LOW = int(TOTAL_SAMPLES * 0.6)  # 900 samples (hop 1-2)
        TARGET_HIGH = TOTAL_SAMPLES - TARGET_LOW  # 600 samples (hop 3-4)
    else:
        TOTAL_SAMPLES = 12000
        TARGET_LOW = int(TOTAL_SAMPLES * 0.6)  # 7200 samples (hop 1-2)
        TARGET_HIGH = TOTAL_SAMPLES - TARGET_LOW  # 4800 samples (hop 3-4)
    
    # Phân bổ chi tiết
    TARGET_HOP1 = TARGET_LOW // 2
    TARGET_HOP2 = TARGET_LOW - TARGET_HOP1
    TARGET_HOP3 = TARGET_HIGH // 2
    TARGET_HOP4 = TARGET_HIGH - TARGET_HOP3
    
    # Tính số lượng cần thêm
    needed_counts = {
        1: max(0, TARGET_HOP1 - existing_counts[1]),
        2: max(0, TARGET_HOP2 - existing_counts[2]),
        3: max(0, TARGET_HOP3 - existing_counts[3]),
        4: max(0, TARGET_HOP4 - existing_counts[4])
    }
    
    total_needed = sum(needed_counts.values())
    
    print(f"\nTarget Distribution:")
    print(f"   Hop 1: {TARGET_HOP1} (need {needed_counts[1]} more)")
    print(f"   Hop 2: {TARGET_HOP2} (need {needed_counts[2]} more)")
    print(f"   Hop 3: {TARGET_HOP3} (need {needed_counts[3]} more)")
    print(f"   Hop 4: {TARGET_HOP4} (need {needed_counts[4]} more)")
    print(f"   Total needed: {total_needed} samples\n")

    if total_needed == 0:
        print("Target reached!")
        return

    # 3. Cache & Setup
    print("Pre-fetching entity names from Neo4j...")
    db_main = Neo4jHandler(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    
    # Load entities dựa trên mode
    entity_cache = {
        "Person": db_main.get_all_node_names("Person"),
        "Award": db_main.get_all_node_names("Award"),
        "Country": db_main.get_all_node_names("Country"),
        "Organization": db_main.get_all_node_names("Organization")
    }
    
    if use_new_entities:
        # Load thêm new entities
        entity_cache.update({
            "Person_Non_Laureate": db_main.get_all_node_names("Person_Non_Laureate"),
            "Position": db_main.get_all_node_names("Position"),
            "Notable_Work": db_main.get_all_node_names("Notable_Work"),
            "Event": db_main.get_all_node_names("Event"),
            "Location": db_main.get_all_node_names("Location")
        })
    
    db_main.close()
    
    print("Entity cache loaded:")
    for entity_type, names in entity_cache.items():
        print(f"   {entity_type}: {len(names)} entities")

    # Tạo task list dựa trên số lượng cần thiết cho từng hop
    tasks = (
        [1] * needed_counts[1] +
        [2] * needed_counts[2] +
        [3] * needed_counts[3] +
        [4] * needed_counts[4]
    )
    random.shuffle(tasks)
    
    print(f"\nGenerated {len(tasks)} tasks to process")

    # 4. Multiprocessing Setup
    manager = Manager()
    result_queue = manager.Queue()

    # Chia task cho worker
    NUM_WORKERS = 8
    chunk_size = len(tasks) // NUM_WORKERS
    task_chunks = [tasks[i:i + chunk_size] for i in range(0, len(tasks), chunk_size)]
    if len(tasks) % NUM_WORKERS != 0:
        task_chunks[-1].extend(tasks[NUM_WORKERS * chunk_size:])

    neo4j_config = {'uri': NEO4J_URI, 'user': NEO4J_USER, 'pwd': NEO4J_PASSWORD}

    workers = []
    for i in range(len(task_chunks)):
        if not task_chunks[i]: continue
        p = Process(target=worker_task,
                    args=(task_chunks[i], neo4j_config, GEMINI_API_KEYS, entity_cache, result_queue, use_new_entities))
        workers.append(p)
        p.start()

    # --- MAIN PROCESS LOOP: NHẬN KẾT QUẢ VÀ UPDATE PROGRESS BAR ---
    # Mở file ghi trực tiếp (append mode để tiếp tục từ checkpoint)
    with open(RAW_FILE, 'a', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)

        # Pbar chạy đúng số lượng task cần làm (total_needed)
        pbar = tqdm(
            total=total_needed, 
            desc="Processing", 
            unit="sample",
            initial=0,
            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
        )

        completed_count = 0
        success_count = 0
        fail_count = 0
        
        while completed_count < total_needed:
            # Lấy kết quả từ queue
            item = result_queue.get()

            if item is not None:
                writer.writerow(item)
                f.flush()  # Lưu ngay lập tức
                success_count += 1
            else:
                fail_count += 1

            # Update progress bar
            pbar.update(1)
            completed_count += 1
            
            # Update description với thống kê
            pbar.set_postfix({
                'success': success_count,
                'fail': fail_count,
                'total_saved': existing_total + success_count
            })

        pbar.close()

    # Chờ worker dọn dẹp
    for p in workers:
        p.join()
    
    print(f"\nGeneration completed!")
    print(f"   Success: {success_count}")
    print(f"   Failed: {fail_count}")
    print(f"   Total in file: {existing_total + success_count}")

if __name__ == "__main__":
    import sys
    multiprocessing.freeze_support()
    
    # Chọn mode từ command line argument
    # Usage: python create_cypher_finetune_dataset_unified.py [old|new]
    mode = sys.argv[1] if len(sys.argv) > 1 else "old"
    
    if mode not in ["old", "new"]:
        print("ERROR: Invalid mode. Use 'old' or 'new'")
        print("Usage: python create_cypher_finetune_dataset_unified.py [old|new]")
        sys.exit(1)
    
    main(mode=mode)
