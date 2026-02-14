import os
import time
import io
import zipfile
import requests

# Optional .env loader
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# --- PATHS ---
HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
DATA_DIR = os.path.join(PROJECT_ROOT, "Data")   # raw exports saved here
os.makedirs(DATA_DIR, exist_ok=True)

# --- CONFIG (read from environment) ---
API_TOKEN = os.environ.get("QUALTRICS_API_TOKEN", "")
DATA_CENTER = os.environ.get("QUALTRICS_DATA_CENTER", "")
CONTROL_SURVEY_ID = os.environ.get("CONTROL_SURVEY_ID", "") or "SV_cMglwtaf5SPhX8i"
TREATMENT_SURVEY_ID = os.environ.get("TREATMENT_SURVEY_ID", "") or "SV_73vQkXAB4mbbDg2"

# --- HELPERS ---
def get_headers():
    if not API_TOKEN:
        raise RuntimeError("QUALTRICS_API_TOKEN not set")
    if not DATA_CENTER:
        raise RuntimeError("QUALTRICS_DATA_CENTER not set")
    return {"x-api-token": API_TOKEN, "content-type": "application/json", "accept": "application/json"}

def export_survey_responses(survey_id, out_filename):
    """
    Export responses via Qualtrics API and save extracted CSV to Data/<out_filename>.
    out_filename should include .csv (e.g. 'control.csv').
    """
    base = f"https://{DATA_CENTER}.qualtrics.com/API/v3/surveys/{survey_id}/export-responses"
    resp = requests.post(base, headers=get_headers(), json={"format": "csv"}, timeout=30)
    resp.raise_for_status()
    progress_id = resp.json()["result"]["progressId"]
    check_url = f"{base}/{progress_id}"
    status = ""
    while status not in ("complete", "failed"):
        r = requests.get(check_url, headers=get_headers(), timeout=30)
        r.raise_for_status()
        result = r.json()["result"]
        status = result.get("status", "")
        if status in ("inProgress", "queued"):
            time.sleep(2)
    if status == "failed":
        raise RuntimeError(f"Export failed for survey {survey_id}")
    file_id = result["fileId"]
    dl_url = f"{base}/{file_id}/file"
    dl = requests.get(dl_url, headers=get_headers(), stream=True, timeout=60)
    dl.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(dl.content))
    csv_name_in_zip = z.namelist()[0]
    out_path = os.path.join(DATA_DIR, out_filename)
    with z.open(csv_name_in_zip) as fin, open(out_path, "wb") as fout:
        fout.write(fin.read())
    print(f"Saved CSV to {out_path}")
    return out_path

# --- MAIN: import only (no cleaning / mapping) ---
def import_only():
    if not API_TOKEN or not DATA_CENTER:
        raise RuntimeError("QUALTRICS_API_TOKEN and QUALTRICS_DATA_CENTER must be set in environment to import from API.")
    ctrl_path = export_survey_responses(CONTROL_SURVEY_ID, "control.csv")
    treat_path = export_survey_responses(TREATMENT_SURVEY_ID, "treatment.csv")
    return ctrl_path, treat_path

if __name__ == "__main__":
    try:
        ctrl, treat = import_only()
        print("Import complete.")
        print("Control file:", ctrl)
        print("Treatment file:", treat)
    except Exception as e:
        print("ERROR during import:", e)
        raise