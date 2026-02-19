import os
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
DATA_DIR = os.path.join(PROJECT_ROOT, "Data")
os.makedirs(DATA_DIR, exist_ok=True)

COL_MAP = {
    "Q2": "Age",
    " ": "Age",  # The Age column is actually labeled as a space in the CSV
    "Q1": "Gender",
    "Q8": "shopping_freq",
    "Q13": "cookie",
    "Q15": "cup",
    "Q17": "sneakers",
    "Q21": "headphones",
    "Q19": "perfume"
}

def _find(path_candidates):
    for p in path_candidates:
        if p and os.path.exists(p):
            return p
    return None

def load_and_prepare(path, treated_flag):
    # Read the file with header row 0
    df = pd.read_csv(path, header=0, dtype=str, keep_default_na=False)
    
    # Remove rows 0 (description) and 1 (ImportId junk)
    df = df.iloc[2:].reset_index(drop=True)
    
    print(f"Initial shape: {df.shape}")
    print(f"All columns: {list(df.columns)}")
    
    # Keep only ResponseId and Q columns (including the " " column which is Age/Q2)
    cols_to_keep = ['ResponseId']
    for col in df.columns:
        if col.startswith('Q') or col.strip() == '':  # Check for space or empty column name
            cols_to_keep.append(col)
    
    df = df[cols_to_keep]
    
    print(f"Kept columns: {list(df.columns)}")
    
    # Rename columns according to COL_MAP
    df = df.rename(columns={k: v for k, v in COL_MAP.items() if k in df.columns})
    
    print(f"After renaming: {list(df.columns)}")
    
    # Add treated column (0 control, 1 treatment)
    df["treated"] = 1 if treated_flag else 0
    
    # Reset index
    df = df.reset_index(drop=True)
    
    return df

def run(save_dir=None):
    save_dir = save_dir or DATA_DIR
    
    # prefer standard filenames in Data/
    control_candidates = [
        os.path.join(DATA_DIR, "control.csv"),
        os.path.join(DATA_DIR, "control_responses.csv"),
        os.path.join(PROJECT_ROOT, "control.csv"),
    ]
    treatment_candidates = [
        os.path.join(DATA_DIR, "treatment.csv"),
        os.path.join(DATA_DIR, "treatment_responses.csv"),
        os.path.join(PROJECT_ROOT, "treatment.csv"),
    ]
    
    control_path = _find(control_candidates)
    treatment_path = _find(treatment_candidates)
    
    if not control_path or not treatment_path:
        raise FileNotFoundError("control.csv or treatment.csv not found in Data/ or project root.")
    
    print(f"\n{'='*60}")
    print("PROCESSING CONTROL")
    print(f"{'='*60}")
    ctrl = load_and_prepare(control_path, treated_flag=0)
    
    print(f"\n{'='*60}")
    print("PROCESSING TREATMENT")
    print(f"{'='*60}")
    treat = load_and_prepare(treatment_path, treated_flag=1)
    
    # save mapped individual files
    control_out = os.path.join(save_dir, "control_mapped.csv")
    treatment_out = os.path.join(save_dir, "treatment_mapped.csv")
    ctrl.to_csv(control_out, index=False)
    treat.to_csv(treatment_out, index=False)
    
    # combine by stacking rows
    combined = pd.concat([ctrl, treat], ignore_index=True)
    
    # Save combined
    combined_out = os.path.join(save_dir, "combined.csv")
    combined.to_csv(combined_out, index=False)
    
    print(f"\n{'='*60}")
    print("FINAL RESULTS")
    print(f"{'='*60}")
    print(f"Control rows: {len(ctrl)}")
    print(f"Treatment rows: {len(treat)}")
    print(f"Combined rows: {len(combined)}")
    print(f"\nFinal columns: {list(combined.columns)}")
    print(f"\nSample data (first row):")
    print(combined.iloc[0].to_dict())
    print(f"\nSaved files:")
    print(f"  ✓ {control_out}")
    print(f"  ✓ {treatment_out}")
    print(f"  ✓ {combined_out}")
    
    return control_out, treatment_out, combined_out

if __name__ == "__main__":
    run()