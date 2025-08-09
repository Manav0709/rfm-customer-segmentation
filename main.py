import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv() 
# Step 1: Load and clean CSV
df = pd.read_csv("online_retail.csv", encoding='ISO-8859-1')
df.dropna(subset=['CustomerID', 'Description'], inplace=True)
df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
df['TotalPrice'] = df['Quantity'] * df['UnitPrice']

# DB connection info


conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port = int(os.getenv("DB_PORT")),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)
cur = conn.cursor()
print("Connected to PostgreSQL.")

# Step 2: Drop tables if re-running
cur.execute("""
DROP TABLE IF EXISTS invoice_items;
DROP TABLE IF EXISTS invoices;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;
""")

# Step 3: Create schema
cur.execute("""
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    country TEXT
);

CREATE TABLE products (
    stock_code TEXT PRIMARY KEY,
    description TEXT,
    unit_price FLOAT
);
            
CREATE TABLE invoices (
    invoice_no TEXT PRIMARY KEY,
    invoice_date TIMESTAMP NOT NULL,
    customer_id INTEGER REFERENCES customers(customer_id)
);

CREATE TABLE invoice_items (
    invoice_no TEXT REFERENCES invoices(invoice_no),
    stock_code TEXT REFERENCES products(stock_code),
    quantity INT
);
""")
conn.commit()
print("Tables created.")

# Step 4: Insert customers
customers = df[['CustomerID', 'Country']].drop_duplicates()
for _, row in customers.iterrows():
    cur.execute("""
        INSERT INTO customers (customer_id, country)
        VALUES (%s, %s)
        ON CONFLICT (customer_id) DO NOTHING
    """, (int(row['CustomerID']), row['Country']))

# Step 5: Insert products
products = df[['StockCode', 'Description', 'UnitPrice']].drop_duplicates()
for _, row in products.iterrows():
    cur.execute("""
        INSERT INTO products (stock_code, description, unit_price)
        VALUES (%s, %s, %s)
        ON CONFLICT (stock_code) DO NOTHING
    """, (row['StockCode'], row['Description'], row['UnitPrice']))

# Step 6: Insert invoices
invoices = df[['InvoiceNo', 'InvoiceDate', 'CustomerID']].drop_duplicates()
for _, row in invoices.iterrows():
    cur.execute("""
        INSERT INTO invoices (invoice_no, invoice_date, customer_id)
        VALUES (%s, %s, %s)
        ON CONFLICT (invoice_no) DO NOTHING
    """, (row['InvoiceNo'], row['InvoiceDate'], row['CustomerID']))

# Step 7: Insert invoice items
invoice_items = df[['InvoiceNo', 'StockCode', 'Quantity']]
for _, row in invoice_items.iterrows():
    cur.execute("""
        INSERT INTO invoice_items (invoice_no, stock_code, quantity)
        VALUES (%s, %s, %s)
    """, (row['InvoiceNo'], row['StockCode'], row['Quantity']))

conn.commit()
print("Data inserted.")

# Step 8: Perform RFM entirely in SQL
print("Calculating RFM in SQL...")

cur.execute("DROP TABLE IF EXISTS rfm_segmentation")

cur.execute("""
CREATE TABLE rfm_segmentation AS
WITH rfm AS (
  SELECT
    c.customer_id,
    DATE_PART('day', CURRENT_DATE - MAX(i.invoice_date)) AS recency,
    COUNT(DISTINCT i.invoice_no) AS frequency,
    SUM(ii.quantity * p.unit_price) AS monetary
  FROM customers c
  JOIN invoices i ON c.customer_id = i.customer_id
  JOIN invoice_items ii ON i.invoice_no = ii.invoice_no
  JOIN products p ON ii.stock_code = p.stock_code
  GROUP BY c.customer_id
),
rfm_ntile AS (
  SELECT
    customer_id,
    recency,
    frequency,
    monetary,
    NTILE(5) OVER (ORDER BY recency ASC) AS r_ntile,
    NTILE(5) OVER (ORDER BY frequency DESC) AS f_ntile, 
    NTILE(5) OVER (ORDER BY monetary DESC) AS m_ntile
  FROM rfm
),
rfm_scores AS (
  SELECT
    customer_id,
    recency,
    frequency,
    monetary,
    (6 - r_ntile) AS r_score,   -- flip so 5 = best (most recent)
    (6 - f_ntile) AS f_score,   -- flip so 5 = best (most frequent)
    (6 - m_ntile) AS m_score    -- flip so 5 = best (highest monetary)
  FROM rfm_ntile
)
SELECT
  customer_id,
  recency,
  frequency,
  monetary,
  r_score,
  f_score,
  m_score,
  concat(r_score, f_score, m_score) AS rfm_score,
  CASE
    WHEN r_score = 5 AND f_score = 5 AND m_score = 5 THEN 'Champions'
    WHEN r_score >= 4 AND f_score >= 4 THEN 'Loyal Customers'
    WHEN m_score = 5 THEN 'Big Spenders'
    WHEN r_score = 1 THEN 'Lost'
    ELSE 'Others'
  END AS segment
FROM rfm_scores;

""")
conn.commit()

print("RFM segmentation created in database.")

rfm_df = pd.read_sql("""
    SELECT customer_id, recency AS Recency, frequency AS Frequency, 
           monetary AS Monetary, r_score, f_score, m_score, 
           rfm_score, segment
    FROM rfm_segmentation
""", conn)

cur.close()
conn.close()
print("PostgreSQL connection closed.")

rfm_df.to_csv("rfm_results.csv", index=False)
print("RFM results exported successfully!")



