import subprocess
import sys

PYTHON = sys.executable  # guarantees venv Python is used

STEPS = [
    ("Create/verify tables", [PYTHON, "create_mha_tables.py"]),
    ("Ingest properties page", [PYTHON, "ingest_mha_properties.py"]),
    ("Ingest vacancy board PDF", [PYTHON, "ingest_mha_vacancy_pdf.py"]),
    ("Show vacancy results", [PYTHON, "demo_vacancies.py"]),
]

def run_step(name, cmd):
    print("\n" + "=" * 70)
    print(f"STEP: {name}")
    print("COMMAND:", " ".join(cmd))
    print("=" * 70)

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.stdout:
        print(result.stdout.strip())
    if result.stderr:
        print(result.stderr.strip())

    if result.returncode != 0:
        print(f"\n❌ Step failed: {name}")
        sys.exit(result.returncode)

def main():
    print("\nMHA Affordable Housing Demo Runner")
    print("This script will create tables, ingest data, and print demo queries.\n")

    for name, cmd in STEPS:
        run_step(name, cmd)

    print("\nDemo complete. You can now open Supabase Table Editor to show results.")

if __name__ == "__main__":
    main()
