from pathlib import Path
import subprocess
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
PYTHON = sys.executable

COMMANDS = [
    [PYTHON, "ingest_adea.py"],
    [PYTHON, "ingest_caras.py"],
    [PYTHON, "ingest_craigslist.py"],
    [PYTHON, "ingest_mha_vacancy_pdf.py"],
    [PYTHON, "ingest_mha_properties.py"],
    [PYTHON, "ingest_mpm_listings.py"],
    [PYTHON, "ingest_plum.py"],
    [PYTHON, "normalize_raw_to_stg.py"],
    [PYTHON, "validate_pipeline.py", "--apply-dashboard-view"],
]

print("Starting daily pipeline...")

for cmd in COMMANDS:
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=SCRIPTS_DIR)
    if result.returncode != 0:
        print(f"Failed: {' '.join(cmd)}")
        sys.exit(result.returncode)

print("Pipeline finished successfully.")