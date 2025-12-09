import os
import sys
import argparse
import pandas as pd
from pathlib import Path

# --- DUPLICATE FILTERING ---

def filter_duplicate_questions(input_file: str, output_file: str, duplicates_txt: str = None):
    """
    Lọc các câu hỏi trùng lặp dựa trên column 'question'.
    Giữ lại duplicate đầu tiên (keep='first').
    
    Args:
        input_file: Path đến file CSV input
        output_file: Path đến file CSV output (đã lọc)
        duplicates_txt: (Optional) Path đến file txt lưu danh sách duplicate
    
    Returns:
        DataFrame đã được lọc
    """
    print(f"\n=== FILTERING DUPLICATES ===")
    print(f"Input file: {input_file}")
    
    # Load CSV với settings phù hợp cho multiline fields
    df = pd.read_csv(
        input_file,
        encoding='utf-8-sig',
        engine='python',
        quotechar='"',
        doublequote=True,
        skipinitialspace=True,
        on_bad_lines='skip'
    )
    
    print(f"\nStats before filtering:")
    print(f"   - Total rows: {len(df)}")
    print(f"   - Unique questions: {df['question'].nunique()}")
    print(f"   - Duplicate questions: {len(df) - df['question'].nunique()}")
    
    # Tìm các duplicate questions
    duplicate_questions = df[df.duplicated(subset=['question'], keep='first')]['question'].unique()
    
    if len(duplicate_questions) > 0:
        print(f"\nFound {len(duplicate_questions)} duplicate questions")
        
        # Save duplicate list nếu được chỉ định
        if duplicates_txt:
            Path(duplicates_txt).parent.mkdir(parents=True, exist_ok=True)
            with open(duplicates_txt, 'w', encoding='utf-8') as f:
                f.write(f"Total duplicate questions: {len(duplicate_questions)}\n")
                f.write("=" * 80 + "\n\n")
                for q in duplicate_questions:
                    count = len(df[df['question'] == q])
                    f.write(f"Question: {q[:200]}...\n")  # Truncate long questions
                    f.write(f"Number of occurrences: {count}\n")
                    f.write("-" * 80 + "\n")
            print(f"   Saved duplicate questions list to: {duplicates_txt}")
    else:
        print(f"\nNo duplicate questions found!")
    
    # Drop duplicates - giữ lại occurrence đầu tiên
    df_filtered = df.drop_duplicates(subset=['question'], keep='first')
    
    print(f"\nAfter filtering:")
    print(f"   - Total remaining rows: {len(df_filtered)}")
    print(f"   - Removed: {len(df) - len(df_filtered)} duplicate rows")
    
    # Tạo thư mục output nếu chưa tồn tại
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    
    # Save filtered data
    df_filtered.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\nSaved filtered data to: {output_file}")
    
    return df_filtered

# --- DATASET SPLIT ---

def split_dataset(input_file: str, output_dir: str, 
                 train_ratio: float = 0.8, 
                 val_ratio: float = 0.1, 
                 test_ratio: float = 0.1, 
                 random_seed: int = 42):
    """
    Chia dataset thành train/val/test theo tỉ lệ chỉ định.
    
    Args:
        input_file: Path đến file CSV input
        output_dir: Thư mục output để lưu train.csv, val.csv, test.csv
        train_ratio: Tỉ lệ train (default: 0.8)
        val_ratio: Tỉ lệ validation (default: 0.1)
        test_ratio: Tỉ lệ test (default: 0.1)
        random_seed: Random seed cho shuffle (default: 42)
    
    Returns:
        Dict chứa các DataFrame: {'train': df_train, 'val': df_val, 'test': df_test, 'final': df_all}
    """
    print(f"\n=== SPLITTING DATASET ===")
    print(f"Input file: {input_file}")
    
    # Validate ratios
    total_ratio = train_ratio + val_ratio + test_ratio
    if abs(total_ratio - 1.0) > 0.001:
        raise ValueError(f"Total ratios must sum to 1.0, currently: {total_ratio}")
    
    # Load CSV
    df = pd.read_csv(
        input_file,
        encoding='utf-8-sig',
        engine='python',
        quotechar='"',
        doublequote=True,
        skipinitialspace=True,
        on_bad_lines='skip'
    )

    if len(df) == 0:
        print("ERROR: No valid data to split!")
        return None
    
    # Shuffle data
    df = df.sample(frac=1, random_state=random_seed).reset_index(drop=True)
    
    # Calculate split sizes
    total_size = len(df)
    train_size = int(train_ratio * total_size)
    val_size = int(val_ratio * total_size)
    test_size = total_size - train_size - val_size
    
    print(f"\nSplit statistics:")
    print(f"   - Total: {total_size} samples")
    print(f"   - Train: {train_size} samples ({train_ratio*100:.1f}%)")
    print(f"   - Val:   {val_size} samples ({val_ratio*100:.1f}%)")
    print(f"   - Test:  {test_size} samples ({test_ratio*100:.1f}%)")
    
    # Split data
    train_df = df.iloc[:train_size].copy()
    val_df = df.iloc[train_size:train_size+val_size].copy()
    test_df = df.iloc[train_size+val_size:].copy()
    
    # Add split column
    train_df['split'] = 'train'
    val_df['split'] = 'validation'
    test_df['split'] = 'test'
    
    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Define output files
    train_file = os.path.join(output_dir, 'train.csv')
    val_file = os.path.join(output_dir, 'val.csv')
    test_file = os.path.join(output_dir, 'test.csv')
    
    # Save splits
    train_df.to_csv(train_file, index=False, encoding='utf-8-sig')
    val_df.to_csv(val_file, index=False, encoding='utf-8-sig')
    test_df.to_csv(test_file, index=False, encoding='utf-8-sig')
    
    print(f"\nSaved split files:")
    print(f"   - Train: {train_file}")
    print(f"   - Val:   {val_file}")
    print(f"   - Test:  {test_file}")
    
    # Combine all with split labels
    final_df = pd.concat([train_df, val_df, test_df])
    final_file = os.path.join(output_dir, 'dataset_final.csv')
    final_df.to_csv(final_file, index=False, encoding='utf-8-sig')
    
    print(f"   - Final (all with split labels): {final_file}")
    print(f"{'='*80}\n")
    
    return {
        'train': train_df,
        'val': val_df,
        'test': test_df,
        'final': final_df
    }

# --- COMBINED PIPELINE ---

def run_full_pipeline(input_file: str, output_dir: str, 
                     train_ratio: float = 0.8, 
                     val_ratio: float = 0.1, 
                     test_ratio: float = 0.1,
                     save_duplicates_log: bool = True,
                     random_seed: int = 42):
    """
    Chạy toàn bộ pipeline: duplicate filtering → dataset split.
    
    Args:
        input_file: Path đến file CSV input (raw)
        output_dir: Thư mục output
        train_ratio: Tỉ lệ train
        val_ratio: Tỉ lệ validation
        test_ratio: Tỉ lệ test
        save_duplicates_log: Có lưu log các duplicate không
        random_seed: Random seed
    """
    print(f"\n{'='*80}")
    print(f"DATASET PROCESSING PIPELINE")
    print(f"{'='*80}")
    
    # Define intermediate and output files
    filtered_file = os.path.join(output_dir, 'dataset_dup_filtered.csv')
    duplicates_log = os.path.join(output_dir, 'duplicate_questions.txt') if save_duplicates_log else None
    
    # Step 1: Filter duplicates
    try:
        df_filtered = filter_duplicate_questions(
            input_file=input_file,
            output_file=filtered_file,
            duplicates_txt=duplicates_log
        )
    except Exception as e:
        print(f"\nERROR while filtering duplicates: {e}")
        sys.exit(1)
    
    # Step 2: Split dataset
    try:
        result = split_dataset(
            input_file=filtered_file,
            output_dir=output_dir,
            train_ratio=train_ratio,
            val_ratio=val_ratio,
            test_ratio=test_ratio,
            random_seed=random_seed
        )
        
        if result is None:
            sys.exit(1)
            
    except Exception as e:
        print(f"\nERROR while splitting dataset: {e}")
        sys.exit(1)
    
    # Summary
    print(f"\n{'='*80}")
    print(f"PIPELINE COMPLETED SUCCESSFULLY")
    print(f"{'='*80}")
    print(f"\nOutput files:")
    print(f"   - Filtered: {filtered_file}")
    print(f"   - Train:    {os.path.join(output_dir, 'train.csv')}")
    print(f"   - Val:      {os.path.join(output_dir, 'val.csv')}")
    print(f"   - Test:     {os.path.join(output_dir, 'test.csv')}")
    print(f"   - Final:    {os.path.join(output_dir, 'dataset_final.csv')}")
    if save_duplicates_log:
        print(f"   - Log:      {duplicates_log}")
    print()

# --- MAIN ---

def main():
    parser = argparse.ArgumentParser(
        description='Unified dataset processing: duplicate filtering + train/val/test split',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline (filter duplicates + split)
  python dataset_processing_unified.py full --input data/raw.csv --output data/

  # Only filter duplicates
  python dataset_processing_unified.py filter --input data/raw.csv --output data/filtered.csv

  # Only split dataset (assume already filtered)
  python dataset_processing_unified.py split --input data/filtered.csv --output data/

  # Custom split ratios
  python dataset_processing_unified.py full --input data/raw.csv --output data/ --train 0.7 --val 0.2 --test 0.1
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # --- FULL PIPELINE ---
    parser_full = subparsers.add_parser('full', help='Run full pipeline: filter duplicates + split dataset')
    parser_full.add_argument('--input', required=True, help='Input CSV file (raw)')
    parser_full.add_argument('--output', required=True, help='Output directory')
    parser_full.add_argument('--train', type=float, default=0.8, help='Train ratio (default: 0.8)')
    parser_full.add_argument('--val', type=float, default=0.1, help='Validation ratio (default: 0.1)')
    parser_full.add_argument('--test', type=float, default=0.1, help='Test ratio (default: 0.1)')
    parser_full.add_argument('--no-log', action='store_true', help='Do not save duplicates log')
    parser_full.add_argument('--seed', type=int, default=42, help='Random seed (default: 42)')
    
    # --- FILTER ONLY ---
    parser_filter = subparsers.add_parser('filter', help='Only filter duplicates')
    parser_filter.add_argument('--input', required=True, help='Input CSV file')
    parser_filter.add_argument('--output', required=True, help='Output CSV file (filtered)')
    parser_filter.add_argument('--log', help='Path to save duplicates log (optional)')
    
    # --- SPLIT ONLY ---
    parser_split = subparsers.add_parser('split', help='Only split dataset into train/val/test')
    parser_split.add_argument('--input', required=True, help='Input CSV file (already filtered)')
    parser_split.add_argument('--output', required=True, help='Output directory')
    parser_split.add_argument('--train', type=float, default=0.8, help='Train ratio (default: 0.8)')
    parser_split.add_argument('--val', type=float, default=0.1, help='Validation ratio (default: 0.1)')
    parser_split.add_argument('--test', type=float, default=0.1, help='Test ratio (default: 0.1)')
    parser_split.add_argument('--seed', type=int, default=42, help='Random seed (default: 42)')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Execute command
    if args.command == 'full':
        run_full_pipeline(
            input_file=args.input,
            output_dir=args.output,
            train_ratio=args.train,
            val_ratio=args.val,
            test_ratio=args.test,
            save_duplicates_log=not args.no_log,
            random_seed=args.seed
        )
    
    elif args.command == 'filter':
        filter_duplicate_questions(
            input_file=args.input,
            output_file=args.output,
            duplicates_txt=args.log
        )
    
    elif args.command == 'split':
        split_dataset(
            input_file=args.input,
            output_dir=args.output,
            train_ratio=args.train,
            val_ratio=args.val,
            test_ratio=args.test,
            random_seed=args.seed
        )

if __name__ == "__main__":
    main()
