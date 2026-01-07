import pandas as pd
from pathlib import Path

SILVER_PATH = Path("data/silver")
FEATURE_PATH = Path("data/feature")
FEATURE_PATH.mkdir(parents=True, exist_ok=True)

txns = pd.read_parquet(SILVER_PATH / "transactions.parquet")
custs = pd.read_parquet(SILVER_PATH / "customers.parquet")

df = txns.merge(custs, on="cust_id", how="left")

df = pd.get_dummies(
    df,
    columns=["country", "risk_level"],
    drop_first=True
)

features = df[
    [c for c in df.columns if c not in ["txn_id", "cust_id"]]
]

features.to_parquet(
    FEATURE_PATH / "features.parquet",
    index=False
)

print("Feature layer saved")

