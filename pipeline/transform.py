import os
import glob
import sqlite3
import pandas as pd
from datetime import datetime

LAKE_DIR = os.path.join(os.path.dirname(__file__), "../data/lake")
DWH_PATH = os.path.join(os.path.dirname(__file__), "../data/warehouse.db")
LOG_FILE = os.path.join(os.path.dirname(__file__), "../data/transform_log.txt")

def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:   # ← ИСПРАВЛЕНИЕ
        f.write(line + "\n")

def load_latest_parquet() -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(LAKE_DIR, "*.parquet")))
    if not files:
        raise FileNotFoundError(f"No parquet files in {LAKE_DIR}. Run ingest.py first.")
    latest = files[-1]
    log(f"Loading from lake: {latest}")
    df = pd.read_parquet(latest)
    log(f" Loaded {len(df)} rows.")
    return df

def enrich(df: pd.DataFrame) -> pd.DataFrame:
    """Feature engineering before loading to DWH."""
    df = df.copy()
    df["Decade_Built"] = (df["Year_Built"] // 10) * 10
    df["Price_per_sqm"] = (df["Price_EUR"] / df["Size_sqm"]).round(2)
    df["Price_Bracket"] = pd.cut(
        df["Price_EUR"],
        bins=[0, 500_000, 1_000_000, 1_500_000, 2_000_000, 2_500_000, float("inf")],
        labels=["<500k", "500k-1M", "1M-1.5M", "1.5M-2M", "2M-2.5M", "2.5M+"]
    )
    df["Arrondissement_str"] = df["Arrondissement"].apply(lambda x: f"{x}e")
    log(f" Enrichment done. New columns: Decade_Built, Price_per_sqm, Price_Bracket, Arrondissement_str")
    return df

def build_aggregates(df: pd.DataFrame) -> dict:
    """Pre-aggregate tables for fast dashboard queries."""
    agg = {}
    # 1. Summary by arrondissement
    agg["agg_by_arrondissement"] = (
        df.groupby("Arrondissement")
        .agg(
            count=("Property_ID", "count"),
            avg_price=("Price_EUR", "mean"),
            median_price=("Price_EUR", "median"),
            min_price=("Price_EUR", "min"),
            max_price=("Price_EUR", "max"),
            avg_size=("Size_sqm", "mean"),
            avg_price_per_sqm=("Price_per_sqm", "mean"),
        )
        .round(2)
        .reset_index()
    )
    # 2. Summary by property type
    agg["agg_by_type"] = (
        df.groupby("Property_Type")
        .agg(
            count=("Property_ID", "count"),
            avg_price=("Price_EUR", "mean"),
            avg_size=("Size_sqm", "mean"),
        )
        .round(2)
        .reset_index()
    )
    # 3. Summary by condition
    agg["agg_by_condition"] = (
        df.groupby("Condition")
        .agg(
            count=("Property_ID", "count"),
            avg_price=("Price_EUR", "mean"),
        )
        .round(2)
        .reset_index()
    )
    # 4. Temporal: avg price by decade built
    agg["agg_by_decade"] = (
        df.groupby("Decade_Built")
        .agg(
            count=("Property_ID", "count"),
            avg_price=("Price_EUR", "mean"),
        )
        .round(2)
        .reset_index()
    )
    # 5. Price distribution
    agg["agg_price_distribution"] = (
        df.groupby("Price_Bracket", observed=True)
        .agg(count=("Property_ID", "count"))
        .reset_index()
    )
    # 6. Raw fact table
    agg["fact_properties"] = df[[
        "Property_ID", "Arrondissement", "Arrondissement_str",
        "Property_Type", "Size_sqm", "Rooms", "Floor",
        "Year_Built", "Decade_Built", "Condition",
        "Distance_to_Center_km", "Price_EUR",
        "Price_per_sqm", "Price_Bracket"
    ]].copy()

    for name, table in agg.items():
        log(f" Built '{name}': {len(table)} rows")
    return agg

def write_to_dwh(agg: dict):
    log(f"Writing to warehouse: {DWH_PATH}")
    con = sqlite3.connect(DWH_PATH)
    for table_name, df_table in agg.items():
        df_table.to_sql(table_name, con, if_exists="replace", index=False)
        log(f" → table '{table_name}' written ({len(df_table)} rows)")
    # Indexes
    con.execute("CREATE INDEX IF NOT EXISTS idx_arr ON fact_properties(Arrondissement)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_type ON fact_properties(Property_Type)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_price ON fact_properties(Price_EUR)")
    con.commit()
    con.close()
    log(" Indexes created.")

def run():
    log("=== TRANSFORM START ===")
    df = load_latest_parquet()
    df = enrich(df)
    agg = build_aggregates(df)
    write_to_dwh(agg)
    log("=== TRANSFORM DONE ===")

if __name__ == "__main__":
    run()