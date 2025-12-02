"""
Reload Table to Superset
This script loads entry-dropoff mapping data from CSV and creates/populates
a PostgreSQL table for use in Superset visualizations.
"""

import pandas as pd
from sqlalchemy import create_engine, text

# Configuration - Update these values
DB_URL = "postgresql://postgres:<password>@34.93.93.62:5432/matomo_analytics"
CSV_PATH = "/home/user/Downloads/entry_dropoff_mapping.csv"  # Update if your file is elsewhere
TABLE_NAME = "entry_dropoff_mapping"

# 1. Load your entry drop-off mapping data (CSV with columns: from_page, dropoff_page, dropoff_count)
data = pd.read_csv(CSV_PATH)

# 2. Connect to PostgreSQL
en = create_engine(DB_URL)

with en.connect() as conn:
    # 3. Create table (drop if exists for clean reload)
    conn.execute(text(f"DROP TABLE IF EXISTS {TABLE_NAME}"))
    conn.execute(text(f"""
        CREATE TABLE {TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            from_page VARCHAR(1000) NOT NULL,
            dropoff_page VARCHAR(1000) NOT NULL,
            dropoff_count INTEGER NOT NULL
        )
    """))
    conn.commit()
    print(f"Table '{TABLE_NAME}' created")

# 4. Insert data
insert_data = data[['from_page', 'dropoff_page', 'dropoff_count']].copy()
insert_data.to_sql(TABLE_NAME, en, if_exists='append', index=False, method='multi')
print(f"Inserted {len(insert_data)} rows into '{TABLE_NAME}'")

en.dispose()
print("Done! Table ready for Superset")
