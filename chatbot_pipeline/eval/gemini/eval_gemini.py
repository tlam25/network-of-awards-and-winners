import pandas as pd
import google.generativeai as genai
from google.api_core import exceptions
import json
import time
import re
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
GEMINI_API_KEYS = [k.strip() for k in GEMINI_API_KEYS if k.strip()]

BASE_DIR = Path(__file__).parent.parent.parent
INPUT_CSV = BASE_DIR / 'data' / 'mcq' / 'nobel_mc_questions_vi_prompt_2000_samples_0312.csv'
OUTPUT_CSV = Path(__file__).parent / 'result_gemini.csv'

MAX_RETRIES = 100000  
RETRY_DELAY = 2 
REQUEST_DELAY = 1

current_key_index = 0

# Cấu hình Model
MODEL_NAME = "gemini-2.5-flash" 
generation_config = {
    "temperature": 0.0,
    "response_mime_type": "application/json",
}

SYSTEM_INSTRUCTION = """
Bạn là một trợ lý AI đánh giá câu hỏi trắc nghiệm. 
Hãy trả lời câu hỏi bằng cách chọn đáp án đúng nhất (A, B, C, hoặc D).
Bạn BẮT BUỘC phải trả về kết quả dưới định dạng JSON như sau:
{ "answer": "Đáp án bạn chọn" }
Ví dụ: { "answer": "A" }
"""

# Hàm lấy key tiếp theo và cấu hình lại client
def rotate_key():
    global current_key_index
    current_key_index = (current_key_index + 1) % len(GEMINI_API_KEYS)
    new_key = GEMINI_API_KEYS[current_key_index]
    # Cấu hình lại thư viện với key mới
    genai.configure(api_key=new_key)
    return new_key

# Khởi tạo key đầu tiên
genai.configure(api_key=GEMINI_API_KEYS[0])

def get_model():
    # Cần tạo lại object model để đảm bảo nó nhận config mới nhất (đôi khi client cache lại)
    return genai.GenerativeModel(
        model_name=MODEL_NAME,
        generation_config=generation_config,
        system_instruction=SYSTEM_INSTRUCTION
    )

def extract_json_answer(text):
    try:
        data = json.loads(text)
        return data.get("answer", "").strip()
    except:
        match = re.search(r'answer["\s]*:[\s"]*([A-D])', text, re.IGNORECASE)
        if match:
            return match.group(1).upper()
        return text.strip() 

def call_gemini(question, retry_count=0):
    global current_key_index
    start_time = time.time()
    
    try:
        # Lấy model instance
        model = get_model()
        
        # Gọi API
        response = model.generate_content(question)
        latency = time.time() - start_time
        
        raw_answer = response.text
        parsed_answer = extract_json_answer(raw_answer)
        
        # Sau mỗi lần gọi thành công, tự động chuyển sang key tiếp theo để chia tải đều (Round-Robin)
        rotate_key()
        
        return parsed_answer, raw_answer, latency, False

    except exceptions.ResourceExhausted:
        print(f"Key ...{GEMINI_API_KEYS[current_key_index][-6:]} rate limited (429). Switching key...")
        rotate_key() 
        
        if retry_count < MAX_RETRIES:
            time.sleep(1)
            return call_gemini(question, retry_count + 1)
        return "ERROR", "Rate Limit All Keys", 0, True

    except Exception as e:
        error_msg = str(e)
        print(f"\nException: {error_msg}. Switching key and retry {retry_count + 1}...")
        rotate_key()
        
        if retry_count < MAX_RETRIES:
            time.sleep(RETRY_DELAY)
            return call_gemini(question, retry_count + 1)
        return "ERROR", error_msg, 0, True

print(f"Loaded {len(GEMINI_API_KEYS)} API Keys.")
print("Reading CSV file...")
df = pd.read_csv(str(INPUT_CSV))

start_index = 0
if os.path.exists(str(OUTPUT_CSV)):
    print(f"Found existing result file: {OUTPUT_CSV}")
    existing_df = pd.read_csv(str(OUTPUT_CSV))
    processed_mask = (
        existing_df['actual_answer'].notna() & 
        (existing_df['actual_answer'] != '') & 
        (existing_df['actual_answer'] != 'ERROR')
    )
    if processed_mask.any():
        start_index = processed_mask.sum()
        print(f"Tiếp tục từ câu hỏi số {start_index + 1}...")
        df = existing_df
    else:
        df['actual_answer'] = ""
        df['raw_response'] = ""
        df['latency'] = 0.0
        df['is_correct'] = False
else:
    df['actual_answer'] = ""
    df['raw_response'] = ""
    df['latency'] = 0.0
    df['is_correct'] = False

print(f"Đánh giá {len(df) - start_index} câu hỏi còn lại...")

correct_count = df['is_correct'].sum() if start_index > 0 else 0

for index in range(start_index, len(df)):
    row = df.iloc[index]
    question = row['question']
    expected_answer = str(row['answer']).strip().upper()
    
    # Hiển thị key đang dùng (ẩn bớt ký tự để gọn)
    current_key_short = GEMINI_API_KEYS[current_key_index][-4:]
    print(f"#{index + 1} [Key:..{current_key_short}]: {question[:30]}...", end=" ")
    
    parsed_ans, raw_ans, lat, has_error = call_gemini(question)
    
    is_correct = (parsed_ans == expected_answer) and not has_error
    if is_correct:
        correct_count += 1
        print(f"[Correct] ({parsed_ans}|{expected_answer}) - {lat:.2f}s")
    else:
        status = "Error" if has_error else "Wrong"
        print(f"[{status}] ({parsed_ans}|{expected_answer}) - {lat:.2f}s")

    df.at[index, 'actual_answer'] = parsed_ans
    df.at[index, 'raw_response'] = raw_ans
    df.at[index, 'latency'] = lat
    df.at[index, 'is_correct'] = is_correct
    
    df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
    
        if has_error:
            print(f"\nStopped at question #{index + 1} after {MAX_RETRIES} failed attempts.")
            break    time.sleep(REQUEST_DELAY)

# --- TỔNG KẾT ---
accuracy = (correct_count / len(df)) * 100 if len(df) > 0 else 0
print("\n" + "="*30)
print(f"ĐÃ XỬ LÝ: {start_index + (index - start_index + 1 if 'index' in locals() else 0)}/{len(df)} câu hỏi")
print(f"Độ chính xác: {accuracy:.2f}% ({correct_count}/{len(df)})")
print(f"Kết quả lưu tại: {OUTPUT_CSV}")
