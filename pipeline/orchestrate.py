"""
pipeline/orchestrate.py
========================
"""
import sys
import time
from datetime import datetime
import ingest
import transform

def step(name: str, fn):
    print(f"\n{'='*50}")
    print(f" STEP: {name}")
    print(f"{'='*50}")
    t0 = time.time()
    try:
        fn()
        elapsed = round(time.time() - t0, 2)
        print(f" ✓ {name} completed in {elapsed}s")
        return True
    except Exception as e:
        print(f" ✗ {name} FAILED: {e}")
        return False

def validate_dwh():
    import sqlite3, os
    dwh = os.path.join(os.path.dirname(__file__), "../data/warehouse.db")
    con = sqlite3.connect(dwh)
    tables = ["fact_properties", "agg_by_arrondissement", "agg_by_type",
              "agg_by_condition", "agg_by_decade", "agg_price_distribution"]
    for t in tables:
        count = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        assert count > 0, f"Table {t} is empty!"
        print(f" ✓ {t}: {count} rows")
    con.close()

def run_pipeline():
    print(f"\nParis Housing Pipeline")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    steps = [
        ("Ingest (CSV → Parquet lake)", ingest.run),
        ("Transform (Parquet → SQLite DWH)", transform.run),
        ("Validate DWH tables", validate_dwh),
    ]
    results = []
    for name, fn in steps:
        ok = step(name, fn)
        results.append((name, ok))
        if not ok:
            print("\nPipeline aborted due to step failure.")
            sys.exit(1)
    print("\n" + "="*50)
    print(" PIPELINE SUMMARY")
    print("="*50)
    for name, ok in results:
        status = "✓" if ok else "✗"
        print(f" {status} {name}")
    print("\nAll steps completed successfully.")

if __name__ == "__main__":
    run_pipeline()