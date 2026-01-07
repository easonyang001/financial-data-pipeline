import duckdb

con = duckdb.connect()

con.execute("""
CREATE VIEW country_kpi AS
SELECT * FROM 'data/gold/country_kpi.parquet';
""")

con.execute("""
CREATE VIEW risk_level_kpi AS
SELECT * FROM 'data/gold/risk_level_kpi.parquet';
""")

con.execute("""
CREATE VIEW customer_summary AS
SELECT * FROM 'data/gold/customer_summary.parquet';
""")

print("=== Fraud rate by country ===")
print(
    con.execute("""
    SELECT
        country,
        total_transactions,
        total_amount,
        fraud_rate
    FROM country_kpi
    ORDER BY fraud_rate DESC;
    """).fetchdf()
)

print("\n=== Fraud rate by risk level ===")
print(
    con.execute("""
    SELECT
        risk_level,
        total_transactions,
        avg_transaction_amount,
        fraud_rate
    FROM risk_level_kpi
    ORDER BY fraud_rate DESC;
    """).fetchdf()
)

print("\n=== High-risk customers ===")
print(
    con.execute("""
    SELECT
        cust_id,
        total_transactions,
        total_amount,
        fraud_rate
    FROM customer_summary
    WHERE fraud_rate > 0.2
    ORDER BY fraud_rate DESC
    LIMIT 10;
    """).fetchdf()
)
