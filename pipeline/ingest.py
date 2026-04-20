"""
pipeline/ingest.py
==================
Batch ingestion: читает CSV → валидирует → сохраняет в data lake (Parquet).
"""
import os
import pandas as pd
from datetime import datetime

RAW_CSV = os.path.join(os.path.dirname(__file__), "../data/paris_housing_prices_dataset.csv")
LAKE_DIR = os.path.join(os.path.dirname(__file__), "../data/lake")
LOG_FILE = os.path.join(os.path.dirname(__file__), "../data/ingest_log.txt")

def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    # ← ИСПРАВЛЕНИЕ: явно utf-8 + безопасные стрелки
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def load_raw(path: str) -> pd.DataFrame:
    log(f"Loading raw CSV: {path}")
    df = pd.read_csv(path)
    log(f" rows={len(df)}, cols={list(df.columns)}")
    return df

def validate(df: pd.DataFrame) -> pd.DataFrame:
    log("Validating data...")
    required = ["Property_ID", "Arrondissement", "Property_Type", "Size_sqm",
                "Rooms", "Floor", "Year_Built", "Condition",
                "Distance_to_Center_km", "Price_EUR"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    before = len(df)
    df = df.dropna(subset=required)
    df = df[df["Price_EUR"] > 0]
    df = df[df["Size_sqm"] > 0]
    df = df[df["Year_Built"].between(1800, 2025)]
    after = len(df)
    log(f" Dropped {before - after} invalid rows. Kept {after}.")
    return df

def save_to_lake(df: pd.DataFrame):
    os.makedirs(LAKE_DIR, exist_ok=True)
    partition = datetime.now().strftime("%Y%m%d")
    out_path = os.path.join(LAKE_DIR, f"paris_housing_{partition}.parquet")
    df.to_parquet(out_path, index=False)
    log(f"Saved to data lake: {out_path}")
    return out_path

def run():
    log("=== INGEST START ===")
    df = load_raw(RAW_CSV)
    df = validate(df)
    path = save_to_lake(df)
    log(f"=== INGEST DONE -> {path} ===")   # ← стрелка заменена на ->
    return path

if __name__ == "__main__":
    run()