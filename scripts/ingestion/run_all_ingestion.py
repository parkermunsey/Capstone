import subprocess
import sys
from pathlib import Path

INGESTION_SCRIPTS = [
    "ingest_adea.py",
    "ingest_caras.py",
    "ingest_mpm_listings.py",
    "ingest_plum.py",
    "ingest_mha_properties.py",
    "ingest_mha_vacancy_pdf.py",
    "ingest_craigslist.py",
]

BASE_DIR = Path(__file__).resolve().parent


def run_script(script_name):
    script_path = BASE_DIR / script_name

    print(f"\n--- Running {script_name} ---")

    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True
    )

    if result.stdout:
        print(result.stdout)

    if result.returncode != 0:
        print(f"ERROR in {script_name}")
        if result.stderr:
            print(result.stderr)
    else:
        print(f"SUCCESS: {script_name}")


def main():
    for script in INGESTION_SCRIPTS:
        run_script(script)

    print("\nAll ingestion scripts completed.")


if __name__ == "__main__":
    main()