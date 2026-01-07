import pandas as pd
import numpy as np
import os
df = pd.read_csv("data/raw/creditcard.csv")
print(df.head())
print(df.shape)




# 建 customer id
df["cust_id"] = np.random.randint(1, 1001, size=len(df))
df["txn_id"] = np.arange(len(df))

# 建交易表
transactions = df[["txn_id", "cust_id", "Time", "Amount", "Class"]]
transactions.columns = ["txn_id", "cust_id", "time", "amount", "fraud"]

# 建客戶表
customers = pd.DataFrame({
    "cust_id": range(1, 1001),
    "country": np.random.choice(["US", "JP", "TW", "DE"], 1000),
    "income": np.random.randint(30000, 150000, 1000),
    "credit_limit": np.random.randint(5000, 100000, 1000),
    "risk_level": np.random.choice(["low", "medium", "high"], 1000)
})

print(transactions.head())
print(customers.head())



os.makedirs("data/silver", exist_ok=True)

transactions.to_parquet("data/silver/transactions.parquet", index=False)
customers.to_parquet("data/silver/customers.parquet", index=False)

print("Saved silver layer parquet files")

