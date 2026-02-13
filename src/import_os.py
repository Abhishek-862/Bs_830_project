import os
import time
import zipfile
import io
import requests
import pandas as pd
from scipy import stats

# ==========================
# 1. CONFIGURATION
# ==========================
# Fill these in from your Qualtrics account settings and survey properties.
# Account details
API_TOKEN = "P6hsoujpI98WQhpFL5EMzd7T6ltN1FfTErmktksH"     
DATA_CENTER = "bostonu"  

# Survey IDs for the two conditions (from Qualtrics survey IDs)
CONTROL_SURVEY_ID = "SV_cMglwtaf5SPhX8i"
TREATMENT_SURVEY_ID = "SV_73vQkXAB4mbbDg2"

# Main outcome variable name (question export tag / column name in the CSV)
# Example: "Q3" for willingness-to-pay slider, or "Q5" for purchase intent rating.
OUTCOME_COL = "Q3"

# Output: path where you want to save the raw CSVs
HERE = os.path.dirname(os.path.abspath(__file__))

# Project root = parent of src
PROJECT_ROOT = os.path.dirname(HERE)

# Data folder inside project root
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data")

os.makedirs(OUTPUT_DIR, exist_ok=True)
output_path = os.path.join(OUTPUT_DIR, "results.csv")




# ==========================
# 2. HELPER FUNCTIONS
# ==========================

def get_headers():
    """Create HTTP headers for Qualtrics API."""
    return {
        "x-api-token": API_TOKEN,
        "content-type": "application/json"
    }

def export_survey_responses(survey_id, file_label):
    """
    Export responses for a given survey_id to a pandas DataFrame.
    Uses the Qualtrics 'export-responses' endpoint. [web:19]
    """
    base_url = f"https://{DATA_CENTER}.qualtrics.com/API/v3/surveys/{survey_id}/export-responses"
    print(f"Starting export for survey {survey_id} ({file_label})...")

    # 1) Start export job
    payload = {
        "format": "csv"
    }
    response = requests.post(base_url, headers=get_headers(), json=payload)
    response.raise_for_status()
    progress_id = response.json()["result"]["progressId"]
    print(f"  Export started. progressId = {progress_id}")

    # 2) Poll until export is complete
    progress_status = "inProgress"
    while progress_status not in ("complete", "failed"):
        check_url = f"{base_url}/{progress_id}"
        check_resp = requests.get(check_url, headers=get_headers())
        check_resp.raise_for_status()
        result = check_resp.json()["result"]
        progress_status = result["status"]
        percent = result.get("percentComplete", 0)
        print(f"  Status: {progress_status}, {percent}% complete")
        if progress_status in ("inProgress", "queued"):
            time.sleep(2)

    if progress_status == "failed":
        raise RuntimeError(f"Export failed for survey {survey_id}")

    file_id = result["fileId"]
    print(f"  Export complete. fileId = {file_id}")

    # 3) Download the file (ZIP containing CSV)
    download_url = f"{base_url}/{file_id}/file"
    download_resp = requests.get(download_url, headers=get_headers(), stream=True)
    download_resp.raise_for_status()
    print("  Downloading export file...")

    # Read ZIP into memory
    z = zipfile.ZipFile(io.BytesIO(download_resp.content))
    # There is usually only one file; take the first one
    csv_name = z.namelist()[0]
    with z.open(csv_name) as f:
        df = pd.read_csv(f)

    # Optionally save CSV
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, f"{file_label}.csv")
    df.to_csv(output_path, index=False)
    print(f"  Saved CSV to {output_path}")

    return df

# ==========================
# 3. MAIN ANALYSIS
# ==========================

def run_experiment_analysis():
    # Export each survey
    control_df = export_survey_responses(CONTROL_SURVEY_ID, "control")
    treatment_df = export_survey_responses(TREATMENT_SURVEY_ID, "treatment")

    # Add treatment indicator: 0 = control, 1 = limited edition
    control_df["treatment"] = 0
    treatment_df["treatment"] = 1

    # Combine into one DataFrame
    combined = pd.concat([control_df, treatment_df], ignore_index=True)

    # Keep only complete cases for the main outcome
    combined_clean = combined.dropna(subset=[OUTCOME_COL])

    # Split into groups
    control_vals = combined_clean.loc[combined_clean["treatment"] == 0, OUTCOME_COL]
    treatment_vals = combined_clean.loc[combined_clean["treatment"] == 1, OUTCOME_COL]

    # Basic descriptive stats
    print("\n=== Descriptive statistics ===")
    print(f"Outcome column: {OUTCOME_COL}")
    print(f"N control:   {len(control_vals)}")
    print(f"N treatment: {len(treatment_vals)}")
    print(f"Mean control:   {control_vals.mean():.3f}")
    print(f"Mean treatment: {treatment_vals.mean():.3f}")

    # Two-sample t-test (does limited-edition tag change the mean?) [web:9]
    t_stat, p_val = stats.ttest_ind(treatment_vals, control_vals, equal_var=False, nan_policy="omit")

    print("\n=== Difference in means test (Welch t-test) ===")
    print(f"t-statistic: {t_stat:.3f}")
    print(f"p-value:     {p_val:.4f}")
    if p_val < 0.05:
        print("Conclusion: Statistically significant difference at the 5% level.")
    else:
        print("Conclusion: No statistically significant difference at the 5% level.")

    # Optional: save combined data
    combined_path = os.path.join(OUTPUT_DIR, "combined_with_treatment_flag.csv")
    combined.to_csv(combined_path, index=False)
    print(f"\nCombined dataset saved to: {combined_path}")

if __name__ == "__main__":
    run_experiment_analysis()
