import os
import sys
import random
import time
import json
import re
import csv
import pandas as pd
from dotenv import load_dotenv
from neo4j import GraphDatabase
import google.generativeai as genai
from tqdm import tqdm

# --- CONFIGURATION ---
load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
GEMINI_API_KEYS = os.getenv("GEMINI_API_KEYS", "").split(",")
GEMINI_API_KEYS = [k.strip() for k in GEMINI_API_KEYS if k.strip()]

# NEW entities and relationships
NEW_NODES = {
    "Person_Non_Laureate",
    "Notable_Work",
    "Event",
    "Location"
}

NEW_RELATIONSHIPS = {
    "DEVELOPED",
    "CO_DISCOVERED_WITH",
    "FOUNDED",
    "CO_FOUNDED",
    "PARTICIPATED_IN",
    "IS_SPOUSE_OF"
}

ANSWER_MAP = {"A": 0, "B": 1, "C": 2, "D": 3}

# --- HELPER FUNCTIONS ---

def uses_new_entities_or_relations(cypher_query: str) -> bool:
    """Check if Cypher query uses NEW entities or relationships"""
    nodes = set(re.findall(r':(\w+)', cypher_query))
    rels = set(re.findall(r'\[:(\w+)\]', cypher_query))
    
    has_new_node = bool(nodes & NEW_NODES)
    has_new_rel = bool(rels & NEW_RELATIONSHIPS)
    
    return has_new_node or has_new_rel

# --- NEO4J CONNECTION ---

class Neo4jConnection:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password), max_connection_lifetime=3600)

    def close(self):
        if self.driver:
            self.driver.close()

    def query(self, cypher_query, max_retries=3):
        for attempt in range(max_retries):
            try:
                with self.driver.session() as session:
                    result = session.run(cypher_query)
                    return [record for record in result]
            except Exception as e:
                if "ServiceUnavailable" in str(e) or "defunct connection" in str(e):
                    if attempt < max_retries - 1:
                        time.sleep(2)
                        self.driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD), max_connection_lifetime=3600)
                    else:
                        raise
                else:
                    raise
        return []

# --- PROMPT CREATION ---

def create_prompt_original(question_text, cypher_query, correct_answer, result_values):
    """
    Prompt cho dataset gốc (OLD entities only).
    Validation nghiêm ngặt: distractor KHÔNG ĐƯỢC có trong DB results.
    """
    prompt = f"""Bạn đang tạo câu hỏi trắc nghiệm tiếng Việt cho bộ dữ liệu tri thức về Giải Nobel.

DỮ LIỆU ĐẦU VÀO:
- Câu hỏi gốc: {question_text}
- Truy vấn Cypher: {cypher_query}
- ĐÁP ÁN ĐÚNG (từ cơ sở dữ liệu): {correct_answer}
- TẤT CẢ kết quả từ database (CẤM dùng làm đáp án nhiễu): {', '.join([f'"{v}"' for v in result_values])}

QUY TẮC BẮT BUỘC:
1. **DIỄN ĐẠT LẠI câu hỏi gốc**:
   - Dựa trên câu hỏi gốc: "{question_text}"
   - Và ngữ cảnh từ truy vấn Cypher: {cypher_query}
   - Tạo một câu hỏi tiếng Việt MỚI hỏi CÙNG NỘI DUNG nhưng với CÁCH DIỄN ĐẠT KHÁC
   - Giữ nguyên ý nghĩa và mục đích
   - Câu hỏi phải tự nhiên và trôi chảy trong tiếng Việt
   - Phải là câu hỏi SỐ ÍT hỏi về MỘT đáp án (không phải số nhiều)
   - Ví dụ về diễn đạt lại:
     * "Michael Levitt là người nước nào?" → "Quốc tịch của Michael Levitt là gì?"
     * "Đất nước nào có công dân đã nhận giải Nobel Văn học?" → "Quốc gia nào có người đoạt giải Nobel Văn học?"
     * "Aziz Sancar làm nghề gì?" → "Nghề nghiệp của Aziz Sancar là gì?"

2. **Phân tích truy vấn Cypher để hiểu LOẠI dữ liệu đang được hỏi**:
   - `RETURN p.name` → Tên người
   - `RETURN c.name` với c:Country → Tên quốc gia
   - `RETURN o.name` với o:Occupation → Nghề nghiệp
   - `RETURN f.name` với f:Field → Lĩnh vực nghiên cứu
   - `RETURN p.born_on_date` → Ngày tháng
   - `RETURN org.name` với org:Organization → Tên tổ chức

3. **LOẠI ĐÁP ÁN PHẢI KHỚP VỚI LOẠI TRUY VẤN**:
   - Nếu truy vấn hỏi về quốc gia → đáp án phải là quốc gia (KHÔNG phải người)
   - Nếu truy vấn hỏi về người → đáp án phải là người (KHÔNG phải quốc gia/tổ chức)
   - Nếu truy vấn hỏi về nghề nghiệp → đáp án phải là nghề nghiệp (KHÔNG phải tên người)
   - KHÔNG ĐƯỢC TỰ Ý BỊA ĐẶT! Tuân thủ đúng loại dữ liệu mà truy vấn trả về!

4. **TẤT CẢ CÁC LỰA CHỌN PHẢI BẰNG TIẾNG VIỆT**:
   - Tên người: Giữ nguyên (ví dụ: "Albert Einstein")
   - Tên quốc gia: Dịch sang tiếng Việt (ví dụ: "USA" → "Hoa Kỳ", "Germany" → "Đức")
   - Nghề nghiệp: Dịch sang tiếng Việt (ví dụ: "engineer" → "Kỹ sư", "physicist" → "Nhà vật lý")
   - Lĩnh vực: Dịch sang tiếng Việt (ví dụ: "Physics" → "Vật lý", "Chemistry" → "Hóa học")
   - Ngày tháng: Giữ nguyên định dạng (ví dụ: "1950-01-15")

5. **Đối với ĐÁP ÁN ĐÚNG**:
   - Sử dụng chính xác: "{correct_answer}"
   - Dịch sang tiếng Việt nếu cần (theo quy tắc 4)
   - PHẢI khớp với loại dữ liệu mà truy vấn Cypher yêu cầu

6. **Tạo 3 đáp án nhiễu**:
   - CÙNG LOẠI với đáp án đúng (tất cả đều là quốc gia, hoặc tất cả đều là người, hoặc tất cả đều là nghề nghiệp, v.v.)
   - Ít nổi tiếng hơn hoặc là các lựa chọn hợp lý
   - **TUYỆT ĐỐI CẤM**: KHÔNG được lấy bất kỳ đáp án nhiễu nào từ danh sách kết quả database ở trên
   - Kiểm tra kỹ từng đáp án nhiễu KHÔNG có trong danh sách cấm

ĐẦU RA JSON:
{{
  "question": "Câu hỏi tiếng Việt đã được DIỄN ĐẠT LẠI (cách diễn đạt khác, ý nghĩa giống câu gốc)",
  "option_a": "",
  "option_b": "",
  "option_c": "",
  "option_d": "",
  "correct_answer": "A hoặc B hoặc C hoặc D",
  "thinking": "Giải thích ngắn gọn bằng tiếng Anh: câu hỏi đã diễn đạt lại hỏi về loại X, đáp án là Y (tiếng Việt), các đáp án nhiễu cùng loại và KHÔNG có trong database"
}}"""
    return prompt

def create_prompt_augmented(question_text, cypher_query, correct_answer, result_values):
    """
    Prompt cho dataset augmented (NEW entities).
    Validation nghiêm ngặt: distractor KHÔNG ĐƯỢC có trong DB results.
    """
    db_results_str = ', '.join([f'"{v}"' for v in result_values]) if result_values else "Không có kết quả từ database - hãy tự tạo đáp án hợp lý"
    
    prompt = f"""Bạn đang tạo câu hỏi trắc nghiệm tiếng Việt cho bộ dữ liệu tri thức về Giải Nobel.

DỮ LIỆU ĐẦU VÀO:
- Câu hỏi gốc: {question_text}
- Truy vấn Cypher: {cypher_query}
- ĐÁP ÁN ĐÚNG (từ cơ sở dữ liệu): {correct_answer}
- TẤT CẢ kết quả từ database (CẤM dùng làm đáp án nhiễu): {db_results_str}

QUY TẮC BẮT BUỘC:
1. **DIỄN ĐẠT LẠI câu hỏi gốc**:
   - Dựa trên câu hỏi gốc: "{question_text}"
   - Và ngữ cảnh từ truy vấn Cypher: {cypher_query}
   - Tạo một câu hỏi tiếng Việt MỚI hỏi CÙNG NỘI DUNG nhưng với CÁCH DIỄN ĐẠT KHÁC
   - Giữ nguyên ý nghĩa và mục đích
   - Câu hỏi phải tự nhiên và trôi chảy trong tiếng Việt
   - Phải là câu hỏi SỐ ÍT hỏi về MỘT đáp án (không phải số nhiều)
   - Ví dụ về diễn đạt lại:
     * "Ai phát triển công trình X?" → "Người phát triển công trình X là ai?"
     * "Ai cộng tác với Marie Curie?" → "Người cùng nghiên cứu với Marie Curie là ai?"
     * "Ai thành lập tổ chức Y?" → "Người sáng lập tổ chức Y là ai?"
     * "Ai tham gia sự kiện Z?" → "Người tham dự sự kiện Z là ai?"

2. **Phân tích truy vấn Cypher để hiểu LOẠI dữ liệu đang được hỏi**:
   - `RETURN p.name` với p:Person hoặc p:Person_Non_Laureate → Tên người
   - `RETURN nw.name` với nw:Notable_Work → Tên công trình (giữ nguyên tiếng Anh)
   - `RETURN e.name` với e:Event → Tên sự kiện (giữ nguyên tiếng Anh)
   - `RETURN loc.name` với loc:Location → Tên địa điểm (giữ nguyên tiếng Anh)
   - `RETURN org.name` với org:Organization → Tên tổ chức (giữ nguyên tiếng Anh)
   - `RETURN c.name` với c:Country → Tên quốc gia
   - `RETURN o.name` với o:Occupation → Nghề nghiệp
   - `RETURN f.name` với f:Field → Lĩnh vực nghiên cứu

3. **LOẠI ĐÁP ÁN PHẢI KHỚP VỚI LOẠI TRUY VẤN**:
   - Nếu truy vấn hỏi về người (Person/Person_Non_Laureate) → đáp án phải là tên người
   - Nếu truy vấn hỏi về công trình (Notable_Work) → đáp án phải là tên công trình
   - Nếu truy vấn hỏi về sự kiện (Event) → đáp án phải là tên sự kiện
   - Nếu truy vấn hỏi về địa điểm (Location) → đáp án phải là tên địa điểm
   - Nếu truy vấn hỏi về tổ chức (Organization) → đáp án phải là tên tổ chức
   - Nếu truy vấn hỏi về quốc gia → đáp án phải là quốc gia
   - Nếu truy vấn hỏi về nghề nghiệp → đáp án phải là nghề nghiệp
   - KHÔNG ĐƯỢC TỰ Ý BỊA ĐẶT! Tuân thủ đúng loại dữ liệu mà truy vấn trả về!

4. **TẤT CẢ CÁC LỰA CHỌN PHẢI BẰNG TIẾNG VIỆT**:
   - Tên người (Person, Person_Non_Laureate): Giữ nguyên (ví dụ: "Marie Curie", "Albert Einstein")
   - Tên công trình (Notable_Work): Giữ nguyên tiếng Anh (ví dụ: "Theory of Relativity", "Radium Research")
   - Tên sự kiện (Event): Giữ nguyên tiếng Anh (ví dụ: "Solvay Conference", "Nobel Prize Ceremony")
   - Tên địa điểm (Location): Giữ nguyên tiếng Anh (ví dụ: "Paris", "Stockholm", "MIT Laboratory")
   - Tên tổ chức (Organization): Giữ nguyên tiếng Anh (ví dụ: "Curie Institute", "MIT", "CERN")
   - Tên quốc gia: Dịch sang tiếng Việt (ví dụ: "USA" → "Hoa Kỳ", "France" → "Pháp")
   - Nghề nghiệp: Dịch sang tiếng Việt (ví dụ: "scientist" → "Nhà khoa học", "physicist" → "Nhà vật lý")
   - Lĩnh vực: Dịch sang tiếng Việt (ví dụ: "Physics" → "Vật lý", "Chemistry" → "Hóa học")

5. **Đối với ĐÁP ÁN ĐÚNG**:
   - Sử dụng chính xác: "{correct_answer}"
   - Dịch sang tiếng Việt nếu cần (theo quy tắc 4)
   - PHẢI khớp với loại dữ liệu mà truy vấn Cypher yêu cầu

6. **Tạo 3 đáp án nhiễu**:
   - CÙNG LOẠI với đáp án đúng:
     * Nếu đáp án đúng là TÊN NGƯỜI → 3 đáp án nhiễu cũng là TÊN NGƯỜI (nhà khoa học khác)
     * Nếu đáp án đúng là CÔNG TRÌNH → 3 đáp án nhiễu cũng là TÊN CÔNG TRÌNH khoa học
     * Nếu đáp án đúng là SỰ KIỆN → 3 đáp án nhiễu cũng là TÊN SỰ KIỆN khoa học
     * Nếu đáp án đúng là TỔ CHỨC → 3 đáp án nhiễu cũng là TÊN TỔ CHỨC
     * Nếu đáp án đúng là ĐỊA ĐIỂM → 3 đáp án nhiễu cũng là TÊN ĐỊA ĐIỂM
   - Ít nổi tiếng hơn hoặc là các lựa chọn hợp lý liên quan đến Nobel Prize
   - **TUYỆT ĐỐI CẤM**: KHÔNG được lấy bất kỳ đáp án nhiễu nào từ danh sách kết quả database ở trên
   - Kiểm tra kỹ từng đáp án nhiễu KHÔNG có trong danh sách cấm

ĐẦU RA JSON:
{{
  "question": "Câu hỏi tiếng Việt đã được DIỄN ĐẠT LẠI (cách diễn đạt khác, ý nghĩa giống câu gốc)",
  "option_a": "",
  "option_b": "",
  "option_c": "",
  "option_d": "",
  "correct_answer": "A hoặc B hoặc C hoặc D",
  "thinking": "Giải thích ngắn gọn bằng tiếng Anh: câu hỏi đã diễn đạt lại hỏi về loại X, đáp án là Y (tiếng Việt), các đáp án nhiễu cùng loại và KHÔNG có trong database"
}}"""
    return prompt

def create_prompt_hop1_flexible(question_text, cypher_query, correct_answer, result_values):
    """
    Prompt cho hop 1 (relaxed validation).
    Cho phép Gemini tự do tạo đáp án khi DB không có đủ dữ liệu.
    """
    db_results_str = ', '.join([f'"{v}"' for v in result_values]) if result_values else "Không có kết quả từ database - hãy tự tạo đáp án hợp lý"
    correct_answer_str = correct_answer if correct_answer != "Unknown" else "Tự tạo đáp án đúng hợp lý dựa trên Cypher query"
    
    prompt = f"""Bạn đang tạo câu hỏi trắc nghiệm tiếng Việt cho bộ dữ liệu tri thức về Giải Nobel.

DỮ LIỆU ĐẦU VÀO:
- Câu hỏi gốc: {question_text}
- Truy vấn Cypher: {cypher_query}
- ĐÁP ÁN ĐÚNG (từ cơ sở dữ liệu hoặc tự tạo): {correct_answer_str}
- Kết quả từ database (tham khảo): {db_results_str}

QUY TẮC BẮT BUỘC:
1. **DIỄN ĐẠT LẠI câu hỏi gốc**:
   - Dựa trên câu hỏi gốc: "{question_text}"
   - Và ngữ cảnh từ truy vấn Cypher: {cypher_query}
   - Tạo một câu hỏi tiếng Việt MỚI hỏi CÙNG NỘI DUNG nhưng với CÁCH DIỄN ĐẠT KHÁC
   - Giữ nguyên ý nghĩa và mục đích
   - Câu hỏi phải tự nhiên và trôi chảy trong tiếng Việt
   - Phải là câu hỏi SỐ ÍT hỏi về MỘT đáp án (không phải số nhiều)
   - Ví dụ về diễn đạt lại:
     * "Ai phát triển công trình X?" → "Người phát triển công trình X là ai?"
     * "Ai cộng tác với Marie Curie?" → "Người cùng nghiên cứu với Marie Curie là ai?"
     * "Ai thành lập tổ chức Y?" → "Người sáng lập tổ chức Y là ai?"
     * "Ai tham gia sự kiện Z?" → "Người tham dự sự kiện Z là ai?"

2. **Phân tích truy vấn Cypher để hiểu LOẠI dữ liệu đang được hỏi**:
   - `RETURN p.name` với p:Person hoặc p:Person_Non_Laureate → Tên người
   - `RETURN nw.name` với nw:Notable_Work → Tên công trình (giữ nguyên tiếng Anh)
   - `RETURN e.name` với e:Event → Tên sự kiện (giữ nguyên tiếng Anh)
   - `RETURN loc.name` với loc:Location → Tên địa điểm (giữ nguyên tiếng Anh)
   - `RETURN org.name` với org:Organization → Tên tổ chức (giữ nguyên tiếng Anh)
   - `RETURN c.name` với c:Country → Tên quốc gia
   - `RETURN o.name` với o:Occupation → Nghề nghiệp
   - `RETURN f.name` với f:Field → Lĩnh vực nghiên cứu

3. **LOẠI ĐÁP ÁN PHẢI KHỚP VỚI LOẠI TRUY VẤN**:
   - Nếu truy vấn hỏi về người (Person/Person_Non_Laureate) → đáp án phải là tên người
   - Nếu truy vấn hỏi về công trình (Notable_Work) → đáp án phải là tên công trình
   - Nếu truy vấn hỏi về sự kiện (Event) → đáp án phải là tên sự kiện
   - Nếu truy vấn hỏi về địa điểm (Location) → đáp án phải là tên địa điểm
   - Nếu truy vấn hỏi về tổ chức (Organization) → đáp án phải là tên tổ chức
   - Nếu truy vấn hỏi về quốc gia → đáp án phải là quốc gia
   - Nếu truy vấn hỏi về nghề nghiệp → đáp án phải là nghề nghiệp
   - KHÔNG ĐƯỢC TỰ Ý BỊA ĐẶT! Tuân thủ đúng loại dữ liệu mà truy vấn trả về!

4. **TẤT CẢ CÁC LỰA CHỌN PHẢI BẰNG TIẾNG VIỆT**:
   - Tên người (Person, Person_Non_Laureate): Giữ nguyên (ví dụ: "Marie Curie", "Albert Einstein")
   - Tên công trình (Notable_Work): Giữ nguyên tiếng Anh (ví dụ: "Theory of Relativity", "Radium Research")
   - Tên sự kiện (Event): Giữ nguyên tiếng Anh (ví dụ: "Solvay Conference", "Nobel Prize Ceremony")
   - Tên địa điểm (Location): Giữ nguyên tiếng Anh (ví dụ: "Paris", "Stockholm", "MIT Laboratory")
   - Tên tổ chức (Organization): Giữ nguyên tiếng Anh (ví dụ: "Curie Institute", "MIT", "CERN")
   - Tên quốc gia: Dịch sang tiếng Việt (ví dụ: "USA" → "Hoa Kỳ", "France" → "Pháp")
   - Nghề nghiệp: Dịch sang tiếng Việt (ví dụ: "scientist" → "Nhà khoa học", "physicist" → "Nhà vật lý")
   - Lĩnh vực: Dịch sang tiếng Việt (ví dụ: "Physics" → "Vật lý", "Chemistry" → "Hóa học")

5. **Đối với ĐÁP ÁN ĐÚNG**:
   - Nếu có kết quả từ database: Sử dụng "{correct_answer_str}"
   - Nếu không có kết quả: Tự tạo đáp án đúng HỢP LÝ dựa trên Cypher query và kiến thức về Nobel Prize
   - Dịch sang tiếng Việt nếu cần (theo quy tắc 4)
   - PHẢI khớp với loại dữ liệu mà truy vấn Cypher yêu cầu

6. **Tạo 3 đáp án nhiễu**:
   - **Độ nổi tiếng**: Chọn các tên CỰC KỲ ÍT NỔI TIẾNG, ít được biết đến trong lịch sử Nobel Prize
   - **Linh hoạt về loại**: 
     * Ưu tiên CÙNG LOẠI với đáp án đúng (tên người, công trình, sự kiện, etc.)
     * Nhưng có thể KHÁC LOẠI nếu vẫn hợp lý với ngữ cảnh (ví dụ: hỏi về người nhưng đáp án nhiễu có thể là tổ chức/địa điểm nếu phù hợp)
   - **Ví dụ tên kém nổi tiếng**:
     * Người: "Karl Landsteiner", "Johannes Fibiger", "Charles Richet" (các nhà khoa học ít ai biết)
     * Công trình: "Bacteriophage Research", "Insulin Purification Method", "Cryogenic Studies"
     * Tổ chức: "Lesser-known research institutes", "Obscure scientific societies"
     * Địa điểm: "Small laboratories", "Regional research centers"
   - **TỰ DO TẠO**: Tự tạo hoàn toàn các đáp án nhiễu hợp lý nhưng KÉM NỔI TIẾNG
   - Đảm bảo đáp án nhiễu nghe có vẻ hợp lý nhưng không phải là đáp án đúng rõ ràng

ĐẦU RA JSON:
{{
  "question": "Câu hỏi tiếng Việt đã được DIỄN ĐẠT LẠI (cách diễn đạt khác, ý nghĩa giống câu gốc)",
  "option_a": "",
  "option_b": "",
  "option_c": "",
  "option_d": "",
  "correct_answer": "A hoặc B hoặc C hoặc D",
  "thinking": "Giải thích ngắn gọn bằng tiếng Anh: câu hỏi đã diễn đạt lại hỏi về loại X, đáp án là Y (tiếng Việt), các đáp án nhiễu cùng loại và KHÔNG có trong database"
}}"""
    return prompt

# --- GEMINI API CALL ---

def call_gemini_for_mc(question_text, cypher_query, result_values, api_keys, mode="original", strict_validation=True):
    """
    Gọi Gemini để tạo multiple choice question.
    
    Args:
        question_text: Câu hỏi gốc (Vietnamese)
        cypher_query: Query Cypher
        result_values: List các giá trị kết quả từ DB
        api_keys: List API keys
        mode: "original" | "augmented" | "hop1_flexible"
        strict_validation: True = validate distractor không được trong DB, False = skip validation
    
    Returns:
        dict với keys: question, option_a, option_b, option_c, option_d, correct_answer, thinking
        hoặc None nếu fail
    """
    max_retries = 10
    base_wait = 2
    used_keys = set()
    
    # Pick random 1 answer từ result_values (hoặc "Unknown" nếu không có)
    correct_answer = random.choice(result_values) if result_values else "Unknown"
    
    # Tạo list FULL result_values để check distractor nghiêm ngặt
    all_db_values_lower = [str(v).lower().strip() for v in result_values]
    
    # Chọn prompt function dựa trên mode
    if mode == "hop1_flexible":
        prompt = create_prompt_hop1_flexible(question_text, cypher_query, correct_answer, result_values)
    elif mode == "augmented":
        prompt = create_prompt_augmented(question_text, cypher_query, correct_answer, result_values)
    else:  # original
        prompt = create_prompt_original(question_text, cypher_query, correct_answer, result_values)
    
    for attempt in range(max_retries):
        try:
            # Smart key rotation
            available_keys = [k for k in api_keys if k not in used_keys]
            if not available_keys:
                used_keys.clear()
                available_keys = api_keys
            key = random.choice(available_keys)
            used_keys.add(key)
            
            genai.configure(api_key=key)
            model = genai.GenerativeModel('gemini-2.5-flash')
            
            safety_settings = [
                {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"}
            ]
            
            response = model.generate_content(prompt, safety_settings=safety_settings, request_options={"timeout": 60})
            result_text = response.text.strip()
            
            # Parse JSON
            match = re.search(r'\{.*\}', result_text, re.DOTALL)
            if not match:
                raise ValueError("No JSON found")
            
            json_str = match.group(0)
            result = json.loads(json_str, strict=False)
            
            # Validate required fields
            required = ["question", "option_a", "option_b", "option_c", "option_d", "correct_answer", "thinking"]
            if not all(k in result for k in required):
                raise ValueError("Missing required fields")
            
            if result["correct_answer"] not in ["A", "B", "C", "D"]:
                raise ValueError("Invalid correct_answer")
            
            answer_idx = ANSWER_MAP[result["correct_answer"]]
            options = [result["option_a"], result["option_b"], result["option_c"], result["option_d"]]
            gemini_answer = options[answer_idx]
            
            # Check options không rỗng
            if not all(options):
                raise ValueError("Some options are empty")
            
            # Validate correct answer - VERY flexible to accept translations
            if strict_validation and correct_answer != "Unknown":
                gemini_lower = gemini_answer.lower().strip()
                correct_lower = str(correct_answer).lower().strip()
                
                # Direct match or substring
                is_match = (gemini_lower == correct_lower or 
                           correct_lower in gemini_lower or 
                           gemini_lower in correct_lower)
                
                # If no match, be VERY lenient for common terms/translations
                if not is_match:
                    # Trust Gemini's translation if both are < 50 chars
                    if len(gemini_answer) < 50 and len(str(correct_answer)) < 50:
                        is_match = True
                    # For longer text, check word overlap
                    elif any(word in gemini_lower for word in correct_lower.split()) or any(word in correct_lower for word in gemini_lower.split()):
                        is_match = True
                
                if not is_match:
                    raise ValueError(f"Mismatch: '{gemini_answer}' vs '{correct_answer}'")
            
            # Check: distractors không được nằm trong result_values (chỉ khi strict_validation=True)
            if strict_validation and result_values:
                distractors = [opt for opt in options if opt != gemini_answer]
                for dist in distractors:
                    dist_lower = dist.lower().strip()
                    # So sánh với TẤT CẢ result_values
                    for db_val in result_values:
                        db_lower = str(db_val).lower().strip()
                        # Flexible: chỉ check exact match hoặc substring rõ ràng
                        if dist_lower == db_lower or (len(dist_lower) > 10 and db_lower in dist_lower):
                            raise ValueError(f"Distractor '{dist}' found in DB results (matches '{db_val}')")
            
            # Check: question không được có pattern số nhiều
            question_lower = result["question"].lower()
            plural_patterns = ["những người nào", "những quốc gia nào", "những nghề nghiệp nào", 
                             "những ai", "những gì", "các người", "các quốc gia"]
            if any(pattern in question_lower for pattern in plural_patterns):
                raise ValueError(f"Question has plural form: {result['question'][:100]}")
            
            return result
            
        except Exception as e:
            error_msg = str(e)
            if "429" in error_msg or "Resource has been exhausted" in error_msg or "500" in error_msg:
                wait_time = min((base_wait * (2 ** attempt)) + random.uniform(0, 3), 60)
                time.sleep(wait_time)
            else:
                time.sleep(2)
    
    return None

# --- MAIN FUNCTION ---

def main(mode="original"):
    """
    Main function cho unified MC question generation.
    
    Args:
        mode: 
            - "original": Dataset gốc (OLD entities only), strict validation
            - "augmented": Dataset augmented (NEW entities), strict validation  
            - "hop1": Chỉ hop 1 với NEW entities, relaxed validation
    """
    
    # Configure based on mode
    if mode == "original":
        INPUT_FILE = "../data/nobel_graph_augment_raw.csv"
        OUTPUT_FILE = "../data/nobel_mc_questions_vi_prompt.csv"
        TARGET_SAMPLES = 1000
        filter_new_entities = False
        filter_hop = None
        strict_validation = True
        hop_targets = {
            1: int(TARGET_SAMPLES * 0.3),   # 30%
            2: int(TARGET_SAMPLES * 0.3),   # 30%
            3: int(TARGET_SAMPLES * 0.2),   # 20%
            4: int(TARGET_SAMPLES * 0.2)    # 20%
        }
        max_reuse_per_query = {1: 2, 2: 3, 3: 3, 4: 3}
        
    elif mode == "augmented":
        INPUT_FILE = "../data/nobel_graph_augment_final.csv"
        OUTPUT_FILE = "../data/nobel_mc_questions_augment_vi_prompt.csv"
        TARGET_SAMPLES = 1000
        filter_new_entities = True
        filter_hop = None
        strict_validation = True
        hop_targets = {
            1: int(TARGET_SAMPLES * 0.5),   # 50%
            2: int(TARGET_SAMPLES * 0.3),   # 30%
            3: int(TARGET_SAMPLES * 0.1),   # 10%
            4: int(TARGET_SAMPLES * 0.1)    # 10%
        }
        max_reuse_per_query = {1: 2, 2: 3, 3: 3, 4: 3}
        
    elif mode == "hop1":
        INPUT_FILE = "../data/nobel_graph_augment_final.csv"
        OUTPUT_FILE = "../data/nobel_mc_questions_augment_vi_prompt.csv"
        TARGET_SAMPLES = 500  # Chỉ focus vào hop 1
        filter_new_entities = True
        filter_hop = 1
        strict_validation = False  # Relaxed validation cho hop 1
        hop_targets = {1: 500, 2: 0, 3: 0, 4: 0}
        max_reuse_per_query = {1: 2, 2: 3, 3: 3, 4: 3}
        
    else:
        print(f"ERROR: Invalid mode '{mode}'. Use 'original', 'augmented', or 'hop1'")
        return
    
    print(f"--- Starting MC Question Generation (Mode: {mode}) ---")
    print(f"Input: {INPUT_FILE}")
    print(f"Output: {OUTPUT_FILE}")
    print(f"Target: {TARGET_SAMPLES} samples")
    
    # Load input with proper CSV parsing for multiline fields
    df = pd.read_csv(
        INPUT_FILE, 
        encoding='utf-8-sig', 
        engine='python', 
        quotechar='"', 
        doublequote=True,
        on_bad_lines='skip',
        skipinitialspace=True
    )
    
    # Ensure hops column is integer
    df['hops'] = pd.to_numeric(df['hops'], errors='coerce')
    df = df.dropna(subset=['hops'])
    df['hops'] = df['hops'].astype(int)

    
    # Apply filters
    if filter_new_entities:
        df['has_new_rel'] = df['cypher_query'].apply(uses_new_entities_or_relations)
        df = df[df['has_new_rel']].copy()
        df = df.drop(columns=['has_new_rel'])
    
    if filter_hop is not None:
        df = df[df['hops'] == filter_hop].copy()
    
    if len(df) == 0:
        print("ERROR: No rows after filtering!")
        return
    
    # Shuffle for random sampling
    df = df.sample(frac=1).reset_index(drop=True)
    
    # Check existing
    existing_count = 0
    existing_hop_counts = {1: 0, 2: 0, 3: 0, 4: 0}
    
    if os.path.exists(OUTPUT_FILE):
        try:
            existing_df = pd.read_csv(OUTPUT_FILE, encoding='utf-8-sig', engine='python', on_bad_lines='skip')
            existing_count = len(existing_df)
            for hop in [1, 2, 3, 4]:
                existing_hop_counts[hop] = len(existing_df[existing_df['hops'] == hop])
            print(f"\nCheckpoint: {existing_count} existing questions")
            for hop in [1, 2, 3, 4]:
                print(f"   Hop {hop}: {existing_hop_counts[hop]}")
        except:
            print("Warning: Could not read existing file")
    
    if existing_count >= TARGET_SAMPLES:
        print(f"\nTarget reached: {existing_count}/{TARGET_SAMPLES}")
        return
    
    # Setup output
    file_exists = os.path.exists(OUTPUT_FILE)
    file_mode = 'a' if file_exists else 'w'
    headers = ['question', 'answer', 'answer_index', 'hops', 'thinking', 'cypher_query']
    
    # Calculate needs
    needed = TARGET_SAMPLES - existing_count
    hop_needed = {hop: max(0, hop_targets[hop] - existing_hop_counts[hop]) for hop in [1, 2, 3, 4]}
    hop_counts = existing_hop_counts.copy()
    
    print(f"\nTarget: {TARGET_SAMPLES}, Need: {needed}")
    for hop in [1, 2, 3, 4]:
        if hop_targets[hop] > 0:
            print(f"   Hop {hop}: {hop_counts[hop]}/{hop_targets[hop]} (need {hop_needed[hop]})")
    
    # Check if need newline before append
    needs_newline = False
    if file_exists:
        try:
            with open(OUTPUT_FILE, 'rb') as f:
                f.seek(-1, 2)
                last_char = f.read(1)
                if last_char != b'\n':
                    needs_newline = True
        except:
            pass
    
    # Neo4j
    neo4j_conn = Neo4jConnection(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    
    # Processing
    success_count = 0
    attempts = 0
    max_attempts = needed * 100
    cypher_usage_count = {}  # Track Cypher query usage
    
    with open(OUTPUT_FILE, file_mode, encoding='utf-8-sig', newline='') as f:
        # Add newline if appending and file doesn't end with one
        if file_exists and needs_newline:
            f.write('\n')
        
        writer = csv.DictWriter(f, fieldnames=headers, quoting=csv.QUOTE_ALL, lineterminator='\n')
        if not file_exists:
            writer.writeheader()
        
        pbar = tqdm(total=needed, desc="Generating", unit="q")
        
        # Debug counters
        skip_counters = {
            'hop_full': 0,
            'no_result': 0,
            'too_many_results': 0,
            'cypher_overused': 0,
            'few_values': 0,
            'gemini_fail': 0,
            'exception': 0
        }
        
        while success_count < needed and attempts < max_attempts:
            attempts += 1
            
            # Weighted sampling: ưu tiên hop còn thiếu nhiều
            hop_needed_current = {hop: max(0, hop_targets[hop] - hop_counts[hop]) for hop in [1, 2, 3, 4]}
            total_needed = sum(hop_needed_current.values())
            
            # Filter 1: Loại bỏ các hop đã đạt quota
            df_available = df.copy()
            for hop in [1, 2, 3, 4]:
                if hop_counts[hop] >= hop_targets[hop]:
                    df_available = df_available[df_available['hops'] != hop]
            
            if len(df_available) == 0:
                break
            
            # Filter 2: Loại bỏ những Cypher query đã đạt max reuse
            for cypher_query, count in cypher_usage_count.items():
                cypher_rows = df[df['cypher_query'] == cypher_query]
                if len(cypher_rows) > 0:
                    query_hop = cypher_rows.iloc[0]['hops']
                    max_reuse = max_reuse_per_query.get(query_hop, 3)
                    if count >= max_reuse:
                        df_available = df_available[df_available['cypher_query'] != cypher_query]
            
            if len(df_available) == 0:
                skip_counters['cypher_overused'] += 1
                break
            
            # Shuffle to avoid repetition
            df_available = df_available.sample(frac=1.0).reset_index(drop=True)
            
            # Weighted sampling by hop deficit
            if total_needed > 0:
                weights = df_available['hops'].map(lambda h: max(hop_needed_current.get(h, 0), 0.1))
                idx = df_available.sample(n=1, weights=weights).index[0]
            else:
                idx = df_available.sample(n=1).index[0]
            
            row = df_available.loc[idx]
            question_text = row['question']
            cypher_query = row['cypher_query']
            hop = row['hops']
            
            # Query Neo4j
            try:
                results = neo4j_conn.query(cypher_query)
            except Exception as e:
                skip_counters['exception'] += 1
                continue
            
            if not results:
                skip_counters['no_result'] += 1
                if mode == "hop1":
                    # For hop1 mode, allow empty results
                    results = []
                else:
                    continue
            
            if len(results) > 1000:
                skip_counters['too_many_results'] += 1
                continue
            
            # Extract result values
            result_values = []
            for record in results:
                for key in record.keys():
                    val = record[key]
                    if val and str(val).strip():
                        result_values.append(str(val))
            
            # Unique values
            result_values = list(set(result_values))
            
            # For strict modes, require at least 4 unique values
            if strict_validation and len(result_values) < 4:
                skip_counters['few_values'] += 1
                continue
            
            # Call Gemini
            gemini_mode = "hop1_flexible" if mode == "hop1" else mode
            mc_data = call_gemini_for_mc(question_text, cypher_query, result_values, GEMINI_API_KEYS, 
                                        mode=gemini_mode, strict_validation=strict_validation)
            
            if not mc_data:
                skip_counters['gemini_fail'] += 1
                continue
            
            # Format output (match file gốc structure)
            full_question = f"{mc_data['question']}\nA. {mc_data['option_a']}\nB. {mc_data['option_b']}\nC. {mc_data['option_c']}\nD. {mc_data['option_d']}"
            
            writer.writerow({
                'question': full_question,
                'answer': mc_data['correct_answer'],
                'answer_index': ANSWER_MAP[mc_data['correct_answer']],
                'hops': hop,
                'thinking': mc_data['thinking'],
                'cypher_query': cypher_query
            })
            f.flush()
            
            # Update counters
            success_count += 1
            hop_counts[hop] += 1
            cypher_usage_count[cypher_query] = cypher_usage_count.get(cypher_query, 0) + 1
            
            pbar.update(1)
            pbar.set_postfix({
                'success': success_count,
                'attempts': attempts,
                'hop1': hop_counts[1],
                'hop2': hop_counts[2],
                'hop3': hop_counts[3],
                'hop4': hop_counts[4]
            })
        
        pbar.close()
    
    neo4j_conn.close()
    
    # Print statistics
    print(f"\n=== Generation Complete ===")
    print(f"Success: {success_count}/{needed}")
    print(f"Total attempts: {attempts}")
    print(f"\nHop distribution:")
    for hop in [1, 2, 3, 4]:
        if hop_targets[hop] > 0:
            print(f"   Hop {hop}: {hop_counts[hop]}/{hop_targets[hop]}")
    
    print(f"\nSkip counters:")
    for reason, count in skip_counters.items():
        if count > 0:
            print(f"   {reason}: {count}")

if __name__ == "__main__":
    # Usage: python create_mcquestion_dataset_unified.py [original|augmented|hop1]
    mode = sys.argv[1] if len(sys.argv) > 1 else "original"
    
    if mode not in ["original", "augmented", "hop1"]:
        print("ERROR: Invalid mode. Use 'original', 'augmented', or 'hop1'")
        print("Usage: python create_mcquestion_dataset_unified.py [original|augmented|hop1]")
        sys.exit(1)
    
    main(mode=mode)
