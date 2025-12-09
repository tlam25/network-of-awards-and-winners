import pandas as pd
import requests
import json
import time
import re
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

GEMINI_API_KEYS = os.getenv("GEMINI_API_KEYS", "").split(",")

# Lọc key rỗng
GEMINI_API_KEYS = [k.strip() for k in GEMINI_API_KEYS if k.strip()]

API_KEY = GEMINI_API_KEYS[0] if GEMINI_API_KEYS else None
API_URL = 'http://localhost/v1/chat-messages' 
USER_ID = 'evaluate-bot'
BASE_DIR = Path(__file__).parent.parent.parent
INPUT_CSV = BASE_DIR / 'data' / 'mcq' / 'nobel_mc_questions_augment_vi_prompt.csv'
OUTPUT_CSV = Path(__file__).parent / 'result_aug.csv'    
MAX_RETRIES = 10  
RETRY_DELAY = 10  
REQUEST_DELAY = 2  


def extract_json_answer(text):
    """
    Trích đáp án A/B/C/D từ bất kỳ dạng output nào:
    - JSON đúng
    - JSON lỗi
    - Text thuần
    - 'C. Đức', 'Đáp án: B', 'Option A', 'answer: D'
    """

    if not text:
        return ""

    # 1. Thử parse JSON (tránh lỗi format)
    try:
        data = json.loads(text)
        if isinstance(data, dict):
            ans = data.get("answer", "").strip()
            # Nếu JSON có answer hợp lệ → dùng
            if re.fullmatch(r"[A-Da-d]", ans):
                return ans.upper()
    except:
        pass  # bỏ qua và fallback sang regex

    # 2. Regex tìm "answer": "A" hoặc answer: B
    match = re.search(r'answer["\s:]*["\s]*([A-Da-d])', text)
    if match:
        return match.group(1).upper()

    # 3. Bắt pattern dạng "A.", "B.", "C.", "D." — thường là output "C. Đức"
    match = re.search(r'\b([A-Da-d])\s*[\.\)]', text)
    if match:
        return match.group(1).upper()

    # 4. Bắt pattern nếu model trả: "Option C", "Chọn B", "Đáp án là D"
    match = re.search(
        r'( Câu trả lời đúng:|Option|Đáp án|Answer|Chọn|=>|→)\s*[:\-]?\s*([A-Da-d])',
        text,
        re.IGNORECASE
    )
    if match:
        return match.group(2).upper()

    # 5. Nếu output chỉ là một chữ cái A/B/C/D đứng riêng lẻ
    match = re.fullmatch(r'\s*([A-Da-d])\s*', text.strip())
    if match:
        return match.group(1).upper()

    # 6. Fallback cuối cùng: tìm chữ cái A/B/C/D đầu tiên trong text
    match = re.search(r'\b([A-Da-d])\b', text)
    if match:
        return match.group(1).upper()

    # 7. Nếu không tìm ra — trả về text để debug
    return text.strip()


def call_dify(question, retry_count=0):
    headers = {
        'Authorization': f'Bearer {API_KEY}',
        'Content-Type': 'application/json'
    }
    
    # Dùng mode 'blocking' để nhận kết quả ngay, không cần xử lý stream
    payload = {
        "inputs": {},
        "query": question,
        "response_mode": "blocking", 
        "conversation_id": "",
        "user": USER_ID
    }
    
    start_time = time.time()
    try:
        response = requests.post(API_URL, headers=headers, json=payload, timeout=30)
        latency = time.time() - start_time
        
        if response.status_code == 200:
            result = response.json()
            # Dify trả về câu trả lời trong key 'answer'
            raw_answer = result.get('answer', '')
            parsed_answer = extract_json_answer(raw_answer)
            return parsed_answer, raw_answer, latency, False  # False = không lỗi
        else:
            error_msg = f"HTTP {response.status_code}: {response.text}"
            if retry_count < MAX_RETRIES:
                print(f"\nError: {error_msg}. Retry {retry_count + 1}/{MAX_RETRIES}...")
                time.sleep(RETRY_DELAY)
                return call_dify(question, retry_count + 1)
            return "ERROR", error_msg, latency, True
    except Exception as e:
        error_msg = str(e)
        if retry_count < MAX_RETRIES:
            print(f"\nException: {error_msg}. Retry {retry_count + 1}/{MAX_RETRIES}...")
            time.sleep(RETRY_DELAY)
            return call_dify(question, retry_count + 1)
        return "ERROR", error_msg, 0, True

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
        print(f"Resuming from question {start_index + 1}...")
        df = existing_df
    else:
        print("Result file is empty, starting from beginning...")
        df['actual_answer'] = ""
        df['raw_response'] = ""
        df['latency'] = 0.0
        df['is_correct'] = False
else:
    print("No result file found, starting from beginning...")
    df['actual_answer'] = ""
    df['raw_response'] = ""
    df['latency'] = 0.0
    df['is_correct'] = False

print(f"Evaluating {len(df) - start_index} remaining questions...")

correct_count = df['is_correct'].sum() if start_index > 0 else 0

for index in range(start_index, len(df)):
    row = df.iloc[index]
    question = row['question']
    expected_answer = str(row['answer']).strip().upper()
    
    print(f"Processing #{index + 1}/{len(df)}: {question[:50]}...", end=" ")
    
    parsed_ans, raw_ans, lat, has_error = call_dify(question)
    
    is_correct = (parsed_ans == expected_answer) and not has_error
    if is_correct:
        correct_count += 1
        print(f"[Correct] (Bot: {parsed_ans} | Expected: {expected_answer}) - {lat:.2f}s")
    else:
        status = "Error" if has_error else "Wrong"
        print(f"[{status}] (Bot: {parsed_ans} | Expected: {expected_answer}) - {lat:.2f}s")

    # Lưu vào dataframe
    df.at[index, 'actual_answer'] = parsed_ans
    df.at[index, 'raw_response'] = raw_ans
    df.at[index, 'latency'] = lat
    df.at[index, 'is_correct'] = is_correct
    
    df.to_csv(str(OUTPUT_CSV), index=False, encoding='utf-8-sig')
    
    if has_error:
        print(f"\nStopped at question #{index + 1} due to error after {MAX_RETRIES} retries.")
        print(f"Check error and re-run script to continue.")
        break
    
    time.sleep(REQUEST_DELAY)

accuracy = (correct_count / len(df)) * 100 if len(df) > 0 else 0
print("\n" + "="*30)
print(f"PROCESSED: {start_index + (index - start_index + 1 if 'index' in locals() else 0)}/{len(df)} questions")
print(f"Accuracy: {accuracy:.2f}% ({correct_count}/{len(df)})")
print(f"Detailed results saved to: {OUTPUT_CSV}")
