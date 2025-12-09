# Network of Awards and Winners - Nobel Prize Knowledge Graph

## Giới thiệu
Dự án xây dựng đồ thị tri thức (Knowledge Graph) về giải Nobel, bao gồm thông tin về người đoạt giải, tổ chức, quốc gia, lĩnh vực và mối quan hệ giữa các thực thể. Hệ thống sử dụng Neo4j làm cơ sở dữ liệu đồ thị và tích hợp các pipeline chatbot với khả năng trả lời câu hỏi dựa trên tri thức.

## Cấu trúc dự án

### 1. Build Database (`build_db/`)
Xây dựng cơ sở dữ liệu đồ thị từ Wikidata.

#### Các bước thực hiện:
1. **Trích xuất dữ liệu từ Wikidata** (`extract_from_wikidata_to_csv.py`)
   - Sử dụng SPARQL query (`queryWikiData.sparql`) để lấy thông tin về người đoạt giải Nobel
   - Lưu kết quả vào file `allNobel.csv`

2. **Chuyển đổi sang định dạng triplet JSON** (`to_triplet_json.py`)
   - Chuyển đổi dữ liệu CSV thành định dạng JSON với cấu trúc triplet (subject-predicate-object)
   - Output: `nobel_network_local.json`

3. **Import vào Neo4j** (`import_to_auradb.py`)
   - Tạo các node: Person, Award, Country, Field, Occupation, Organization, Position
   - Tạo các relationship giữa các node
   - Sử dụng Neo4j CQL script (`import_data_neo4j.cql`) để định nghĩa schema

#### Dữ liệu đầu ra:
- `neo4j_import/`: Chứa các file CSV cho từng loại entity và relationship
  - `Person.csv`, `Award.csv`, `Country.csv`, `Field.csv`, etc.
  - `Relations.csv`: Các mối quan hệ giữa entities

### 2. Chatbot Pipeline (`chatbot_pipeline/`)
Tạo dataset và đánh giá pipeline chatbot.

#### 2.1 Tạo Dataset (`create_dataset_chatbot_pipeline/`)

**Dataset Cypher Finetuning:**
- File: `create_cypher_finetune_dataset.py`
- Mục đích: Tạo dataset để finetune model chuyển đổi câu hỏi tự nhiên sang Cypher query
- Output: `nobel_cypher_finetune_data_with_splits.csv`
- Bao gồm: train/validation/test splits

**Dataset Multiple Choice Questions:**
- File: `create_mcquestion_dataset.py`
- Mục đích: Tạo câu hỏi trắc nghiệm từ Knowledge Graph
- Output: 
  - `nobel_mc_questions_vi_prompt.csv`: Câu hỏi tiếng Việt
  - `nobel_mc_questions_augment_vi_prompt.csv`: Câu hỏi augmented

**Xử lý Dataset:**
- File: `dataset_processing.py`
- Làm sạch và chuẩn hóa dữ liệu

#### 2.2 Đánh giá Model (`eval/`)

**Đánh giá Dify:**
- `eval_dify.py`: Đánh giá trên dataset gốc
- `eval_dify_aug.py`: Đánh giá trên dataset augmented

**Đánh giá Gemini:**
- `eval_gemini.py`: Đánh giá performance của Gemini model

### 3. Graph Augmentation (`graph_augmentation/`)
Bổ sung thông tin vào Knowledge Graph sử dụng NER và Relation Extraction.

#### Các bước:
1. **Tiền xử lý** (`preprocess_ner_re_finetune_data.py`)
   - Chuẩn bị dữ liệu cho việc finetune model NER/RE
   - Input: `nobel_meta.csv`

2. **Trích xuất entities và relations** (`extraction_ner_re_finetune_data.py`)
   - Sử dụng model đã finetune để trích xuất thông tin
   - Notebook: `ptich-mxh-finetuned-nobel-kg-extraction.ipynb`

3. **Hậu xử lý** (`postprocess.py`)
   - Làm sạch và chuẩn hóa kết quả trích xuất
   - Output: `nobel_laureates_extraction_refined.jsonl`

4. **Import vào AuraDB** (`import_to_auradb.py`)
   - Cập nhật Knowledge Graph với thông tin mới

### 4. Analysis Algorithm (`analysis_algorithm/`)
Phân tích cấu trúc và đặc tính của Knowledge Graph sử dụng các thuật toán graph phổ biến.

#### Các thuật toán:

**1. Small World Analysis** (`small_world.py`)
- Tính toán Average Shortest Path Length
- Tính toán Average Clustering Coefficient
- So sánh với random graph để xác định tính chất small-world
- Output: `small_world_result.txt`

**2. Node Ranking** (`ranking.py`)
- PageRank: Đánh giá tầm quan trọng dựa trên cấu trúc liên kết
- Degree Centrality: Đếm số lượng kết nối trực tiếp
- Betweenness Centrality: Đo lường vị trí cầu nối trong network
- Visualization: Top N nodes quan trọng nhất
- Output: `ranking_result.txt`

**3. Community Detection** (`community_detection.py`)
- Sử dụng Louvain algorithm để phát hiện cộng đồng
- Tính modularity score
- Phân tích phân bố kích thước cộng đồng
- Liệt kê thành viên trong các cộng đồng lớn nhất
- Visualization: Network với các cộng đồng được tô màu
- Output: `community_detection_result.txt`

#### Module hỗ trợ:
- `neo4j_connector.py`: Kết nối và trích xuất dữ liệu từ Neo4j
- `graph_projection.py`: Tạo projection graph chỉ với Person nodes và relationships
- `main.py`: Script chính để chạy các thuật toán

#### Cách chạy:
```bash
cd analysis_algorithm/src

# Small world analysis
python main.py --algo small_world

# Node ranking (top 20 mặc định)
python main.py --algo ranking --top 30

# Community detection
python main.py --algo community
```

### 5. Shortest Path Analysis (`shortest-path-nobel-network/`)
Tìm đường đi ngắn nhất giữa các node trong đồ thị.

#### Tính năng:
- File: `find-shortest-path.py`
- Tìm mối liên hệ giữa các người đoạt giải Nobel
- Phân tích network structure

## Yêu cầu hệ thống

```bash
# Python dependencies
pip install -r shortest-path-nobel-network/src/requirements.txt
pip install -r analysis_algorithm/src/requirements.txt
```

### Các thư viện chính:
- neo4j-driver: Kết nối với Neo4j database
- pandas: Xử lý dữ liệu
- SPARQLWrapper: Query Wikidata
- openai/gemini-api: Tích hợp LLM
- networkx: Phân tích và xử lý graph
- matplotlib: Visualization
- python-louvain: Community detection algorithm

## Hướng dẫn sử dụng

### 1. Build Knowledge Graph
```bash
cd build_db/src

# Bước 1: Trích xuất từ Wikidata
python extract_from_wikidata_to_csv.py

# Bước 2: Chuyển sang JSON triplet
python to_triplet_json.py

# Bước 3: Import vào Neo4j
python import_to_auradb.py
```

### 2. Tạo Dataset cho Chatbot
```bash
cd chatbot_pipeline/create_dataset_chatbot_pipeline

# Tạo Cypher dataset
python create_cypher_finetune_dataset.py

# Tạo MC questions
python create_mcquestion_dataset.py

# Xử lý dataset
python dataset_processing.py
```

### 3. Đánh giá Model
```bash
cd chatbot_pipeline/eval

# Đánh giá Dify
python dify/eval_dify.py
python dify/eval_dify_aug.py

# Đánh giá Gemini
python gemini/eval_gemini.py
```

### 4. Augment Graph
```bash
cd graph_augmentation/src

# Tiền xử lý
python preprocess_ner_re_finetune_data.py

# Trích xuất
python extraction_ner_re_finetune_data.py

# Hậu xử lý
python postprocess.py

# Import
python import_to_auradb.py
```

### 5. Phân tích thuật toán Graph
```bash
cd analysis_algorithm/src

# Phân tích Small World
python main.py --algo small_world

# Ranking các node quan trọng
python main.py --algo ranking --top 30

# Phát hiện cộng đồng
python main.py --algo community
```

### 6. Phân tích Shortest Path
```bash
cd shortest-path-nobel-network/src
python find-shortest-path.py
```