import pandas as pd
from pathlib import Path

SILVER_PATH = Path("data/silver")
GOLD_PATH = Path("data/gold")
GOLD_PATH.mkdir(parents=True, exist_ok=True)

txns = pd.read_parquet(SILVER_PATH / "transactions.parquet")
custs = pd.read_parquet(SILVER_PATH / "customers.parquet")

fact = txns.merge(custs, on="cust_id", how="left")

country_kpi = (
    fact
    .groupby("country")
    .agg(
        total_transactions=("txn_id", "count"),
        total_amount=("amount", "sum"),
        fraud_rate=("fraud", "mean")
    )
    .reset_index()
)

country_kpi["fraud_rate"] = country_kpi["fraud_rate"].round(4)

risk_level_kpi = (
    fact
    .groupby("risk_level")
    .agg(
        total_transactions=("txn_id", "count"),
        avg_transaction_amount=("amount", "mean"),
        fraud_rate=("fraud", "mean")
    )
    .reset_index()
)

risk_level_kpi["fraud_rate"] = risk_level_kpi["fraud_rate"].round(4)

customer_summary = (
    fact
    .groupby("cust_id")
    .agg(
        total_transactions=("txn_id", "count"),
        total_amount=("amount", "sum"),
        fraud_rate=("fraud", "mean")
    )
    .reset_index()
)

country_kpi.to_parquet(GOLD_PATH / "country_kpi.parquet", index=False)
risk_level_kpi.to_parquet(GOLD_PATH / "risk_level_kpi.parquet", index=False)
customer_summary.to_parquet(GOLD_PATH / "customer_summary.parquet", index=False)

print("Gold layer saved")
