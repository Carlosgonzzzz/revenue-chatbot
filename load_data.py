import mysql.connector
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Read CSV
print("Reading sales_pipeline.csv...")
df = pd.read_csv('sales_pipeline.csv')
print(f"Loaded {len(df)} records from CSV")

# Connect to MySQL
print("Connecting to MySQL...")
conn = mysql.connector.connect(
    host=os.getenv('MYSQL_HOST', 'localhost'),
    port=int(os.getenv('MYSQL_PORT', 3306)),
    user=os.getenv('MYSQL_USER', 'root'),
    password=os.getenv('MYSQL_PASSWORD'),
    database=os.getenv('MYSQL_DATABASE', 'revenue_ops')
)
cursor = conn.cursor()

# Drop and recreate table
print("Creating table...")
cursor.execute("DROP TABLE IF EXISTS sales_pipeline")
cursor.execute("""
CREATE TABLE sales_pipeline (
    opportunity_id VARCHAR(50) PRIMARY KEY,
    sales_agent VARCHAR(100),
    product VARCHAR(100),
    account VARCHAR(200),
    deal_stage VARCHAR(50),
    engage_date DATE,
    close_date DATE,
    close_value INT
)
""")

# Insert data
print("Inserting records...")
insert_query = """
INSERT INTO sales_pipeline 
(opportunity_id, sales_agent, product, account, deal_stage, engage_date, close_date, close_value)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

records = 0
for _, row in df.iterrows():
    values = (
        row['opportunity_id'],
        row['sales_agent'],
        row['product'],
        row['account'] if pd.notna(row['account']) else None,
        row['deal_stage'],
        row['engage_date'] if pd.notna(row['engage_date']) else None,
        row['close_date'] if pd.notna(row['close_date']) else None,
        int(row['close_value']) if pd.notna(row['close_value']) else 0
    )
    cursor.execute(insert_query, values)
    records += 1
    if records % 1000 == 0:
        print(f"Inserted {records} records...")

conn.commit()

# Verify
cursor.execute("SELECT COUNT(*) FROM sales_pipeline")
count = cursor.fetchone()[0]
print(f"\n✅ SUCCESS! Loaded {count} records into MySQL")

# Show sample
cursor.execute("SELECT * FROM sales_pipeline LIMIT 5")
print("\nSample data:")
for row in cursor.fetchall():
    print(row)

cursor.close()
conn.close()
