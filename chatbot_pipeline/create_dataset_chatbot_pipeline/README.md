# Nobel Prize Dataset Generation - Scripts Guide

This folder contains scripts that consolidate multiple dataset generation workflows into single, configurable modules. These scripts streamline the process of creating Text-to-Cypher and Multiple Choice Question datasets for Nobel Prize knowledge graphs.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Environment Setup](#environment-setup)
3. [Script Overview](#script-overview)
4. [Detailed Usage Guides](#detailed-usage-guides)
   - [Cypher Finetune Dataset Generation](#1-cypher-finetune-dataset-generation)
   - [Multiple Choice Question Generation](#2-multiple-choice-question-generation)
   - [Dataset Processing (Deduplication & Split)](#3-dataset-processing-deduplication--split)
5. [Workflow Examples](#workflow-examples)
6. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Required Software
- **Python 3.11+**
- **Neo4j Database** (cloud or local instance)
- **Google Gemini API Keys** (multiple keys recommended for rate limit handling)

### Required Python Packages
Install all dependencies from the root `requirements.txt`:

```bash
pip install -r src/requirements.txt
```

**Dependencies:**
- `neo4j` - Neo4j database driver
- `google-generativeai` - Google Gemini API client
- `python-dotenv` - Environment variable management
- `pandas` - Data manipulation
- `tqdm` - Progress bars

---

## Detailed Usage Guides

---

## 1. Cypher Finetune Dataset Generation

**Script:** `create_cypher_finetune_dataset.py`

### Purpose
Generates Vietnamese question + Cypher query pairs for training Text-to-Cypher models. Supports two modes:
- **`old`**: Uses only original entities/relationships (Person, Award, Country, etc.)
- **`new`**: Uses augmented entities/relationships (Notable_Work, Event, Person_Non_Laureate, etc.)

### Command Structure

```bash
python create_cypher_finetune_dataset.py [old|new]
```

### Modes

#### **Mode: `old` (Original Entities)**

Generates queries using **only original entities** from the base Nobel Prize graph.

**Command:**
```bash
python create_cypher_finetune_dataset.py old
```

#### **Mode: `new` (Augmented Entities)**

Generates queries using **new augmented entities** to create more diverse training data.

**Command:**
```bash
python create_cypher_finetune_dataset.py new
```

### Features

✅ **Multiprocessing** - Uses 8 worker processes for parallel generation  
✅ **Checkpoint/Resume** - Automatically resumes from existing file  
✅ **Entity Caching** - Pre-fetches entity names to avoid repeated Neo4j queries  
✅ **Weighted Sampling** - Prioritizes underrepresented hop levels  
✅ **Path Validation** - Only generates queries that return actual data  
✅ **Exponential Backoff** - Smart retry logic for API rate limits  

---

## 2. Multiple Choice Question Generation

**Script:** `create_mcquestion_dataset.py`

### Purpose
Converts Text-to-Cypher datasets into Multiple Choice Questions (MCQ) with 4 options (A/B/C/D). Supports three modes with different validation strictness levels.

### Command Structure

```bash
python create_mcquestion_dataset.py [original|augmented|hop1]
```

### Modes

#### **Mode: `original` (Original Dataset)**

**Command:**
```bash
python create_mcquestion_dataset.py original
```

**Features:**
- Only uses OLD entities/relationships
- Strict validation ensures high-quality distractors
- Prevents distractor overlap with database results

#### **Mode: `augmented` (Augmented Dataset)**

**Command:**
```bash
python create_mcquestion_dataset.py augmented
```

**Features:**
- Focuses on NEW entities/relationships
- Higher hop1 percentage to balance dataset
- Weighted sampling prioritizes underrepresented hops

#### **Mode: `hop1` (Hop 1 Focus - Relaxed Validation)**

**Command:**
```bash
python create_mcquestion_dataset.py hop1
```

**Features:**
- Designed to fill hop1 gap in augmented dataset
- Allows Gemini creative freedom for obscure entities
- Relaxed validation enables generation even with sparse DB results
- Automatically appends to existing output file

### Output Format

All modes produce CSV files with the following structure:

**Columns:**
- `question` - Full question text with options (A/B/C/D format)
- `answer` - Correct answer letter (A/B/C/D)
- `answer_index` - Numeric index of correct answer (0/1/2/3)
- `hops` - Number of hops in the Cypher query
- `thinking` - English explanation of reasoning
- `cypher_query` - Original Cypher query

### Features

✅ **Smart Key Rotation** - Cycles through API keys to avoid rate limits  
✅ **Weighted Sampling** - Prioritizes hops with deficits  
✅ **Query Reuse Tracking** - Prevents overuse of same Cypher queries  
✅ **Checkpoint/Resume** - Appends to existing files safely  
✅ **Validation Modes** - Strict vs. relaxed validation based on use case  
✅ **Progress Tracking** - Real-time statistics (hop distribution, success rate)  

---

## 3. Dataset Processing (Deduplication & Split)

**Script:** `dataset_processing.py`

### Purpose
Post-processes generated datasets by:
1. **Filtering duplicates** - Removes duplicate questions (keeps first occurrence)
2. **Splitting data** - Divides into train/validation/test sets

### Command Structure

The script supports three sub-commands:

```bash
# Full pipeline (filter + split)
python dataset_processing.py full --input <file> --output <dir> [options]

# Only filter duplicates
python dataset_processing.py filter --input <file> --output <file> [options]

# Only split dataset
python dataset_processing.py split --input <file> --output <dir> [options]
```

### Sub-commands

#### **Command: `full` (Complete Pipeline)**

Runs duplicate filtering followed by train/val/test split.

**Usage:**
```bash
cd src/merge
python dataset_processing.py full \
    --input ../../data/nobel_graph_augment_raw.csv \
    --output ../../data/ \
    --train 0.8 \
    --val 0.1 \
    --test 0.1 \
    --seed 42
```

**Arguments:**
- `--input` (required) - Input CSV file (raw/unprocessed)
- `--output` (required) - Output directory
- `--train` (optional) - Train ratio (default: 0.8)
- `--val` (optional) - Validation ratio (default: 0.1)
- `--test` (optional) - Test ratio (default: 0.1)
- `--no-log` (optional) - Skip saving duplicates log
- `--seed` (optional) - Random seed for reproducibility (default: 42)

**Output Files:**
```
data/
├── dataset_dup_filtered.csv          # Deduplicated data
├── train.csv                          # Training set (80%)
├── val.csv                            # Validation set (10%)
├── test.csv                           # Test set (10%)
├── dataset_final.csv                  # All data with split labels
└── duplicate_questions.txt            # Log of duplicates (if not --no-log)
```

#### **Command: `filter` (Deduplication Only)**

Only removes duplicate questions from the dataset.

**Usage:**
```bash
cd src/merge
python dataset_processing.py filter \
    --input ../../data/nobel_graph_augment_raw.csv \
    --output ../../data/nobel_graph_dup_filtered.csv \
    --log ../../data/duplicates.txt
```

**Arguments:**
- `--input` (required) - Input CSV file
- `--output` (required) - Output CSV file (deduplicated)
- `--log` (optional) - Path to save duplicates log

#### **Command: `split` (Train/Val/Test Split Only)**

Splits a dataset into train/validation/test sets (assumes data is already deduplicated).

**Usage:**
```bash
cd src/merge
python dataset_processing.py split \
    --input ../../data/nobel_graph_dup_filtered.csv \
    --output ../../data/ \
    --train 0.7 \
    --val 0.2 \
    --test 0.1 \
    --seed 42
```

**Arguments:**
- `--input` (required) - Input CSV file (already filtered)
- `--output` (required) - Output directory
- `--train` (optional) - Train ratio (default: 0.8)
- `--val` (optional) - Validation ratio (default: 0.1)
- `--test` (optional) - Test ratio (default: 0.1)
- `--seed` (optional) - Random seed (default: 42)

**Output Files:**
```
data/
├── train.csv          # Training set
├── val.csv            # Validation set
├── test.csv           # Test set
└── dataset_final.csv  # All data with 'split' column
```

### Features

✅ **Duplicate Detection** - Identifies and removes duplicate questions  
✅ **Duplicate Logging** - Optional detailed log of all duplicates  
✅ **Flexible Ratios** - Customizable train/val/test splits  
✅ **Reproducible Splits** - Uses random seed for consistency  
✅ **Split Labels** - Final CSV includes 'split' column for tracking  
✅ **Multiline Support** - Properly handles CSV files with multiline fields  

---

## Summary

This script collection provides a complete pipeline for Nobel Prize dataset generation:

1. **`create_cypher_finetune_dataset.py`** - Generate Text-to-Cypher data
2. **`create_mcquestion_dataset.py`** - Convert to Multiple Choice Questions
3. **`dataset_processing.py`** - Deduplicate and split for training

All scripts support flexible configuration via command-line arguments, making it easy to customize for different use cases and dataset sizes.
