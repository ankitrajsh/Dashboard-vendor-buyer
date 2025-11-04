#!/usr/bin/env python3
"""
PostgreSQL Database Table Loader
This script connects to a PostgreSQL database and loads all tables.
"""

import psycopg2
import pandas as pd
from psycopg2.extras import RealDictCursor
import sys
from typing import Dict, List, Any

# ---------- PostgreSQL Connection ----------
def create_connection():
    """Create and return a PostgreSQL connection"""
    try:
        pg_conn = psycopg2.connect(
            host="34.93.93.62",
            user="postgres",
            password="Y5mnshpDFF44",
            dbname="matomo_analytics",
            port="5432"
        )
        return pg_conn
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
        sys.exit(1)

def get_all_tables(cursor) -> List[str]:
    """Get list of all tables in the database"""
    query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
    AND table_type = 'BASE TABLE'
    ORDER BY table_name;
    """
    cursor.execute(query)
    tables = [row[0] for row in cursor.fetchall()]
    return tables

def get_table_info(cursor, table_name: str) -> Dict[str, Any]:
    """Get detailed information about a table"""
    # Get column information
    column_query = """
    SELECT column_name, data_type, is_nullable, column_default
    FROM information_schema.columns
    WHERE table_name = %s AND table_schema = 'public'
    ORDER BY ordinal_position;
    """
    cursor.execute(column_query, (table_name,))
    columns = cursor.fetchall()
    
    # Get row count
    count_query = f"SELECT COUNT(*) FROM {table_name};"
    cursor.execute(count_query)
    row_count = cursor.fetchone()[0]
    
    return {
        'columns': columns,
        'row_count': row_count
    }

def load_table_data(cursor, table_name: str, limit: int = None) -> pd.DataFrame:
    """Load data from a specific table"""
    if limit:
        query = f"SELECT * FROM {table_name} LIMIT {limit};"
    else:
        query = f"SELECT * FROM {table_name};"
    
    try:
        return pd.read_sql_query(query, cursor.connection)
    except Exception as e:
        print(f"Error loading data from table {table_name}: {e}")
        return pd.DataFrame()

def main():
    """Main function to load all tables"""
    print("Connecting to PostgreSQL database...")
    pg_conn = create_connection()
    pg_cur = pg_conn.cursor()
    
    try:
        # Get all tables
        print("Fetching list of all tables...")
        tables = get_all_tables(pg_cur)
        
        if not tables:
            print("No tables found in the database.")
            return
        
        print(f"Found {len(tables)} tables:")
        for i, table in enumerate(tables, 1):
            print(f"{i}. {table}")
        
        print("\n" + "="*50)
        print("TABLE ANALYSIS")
        print("="*50)
        
        # Dictionary to store all loaded data
        all_tables_data = {}
        
        # Analyze each table
        for table_name in tables:
            print(f"\nAnalyzing table: {table_name}")
            print("-" * 40)
            
            # Get table info
            table_info = get_table_info(pg_cur, table_name)
            
            print(f"Row count: {table_info['row_count']:,}")
            print(f"Columns ({len(table_info['columns'])}):")
            
            for col_name, data_type, is_nullable, default_val in table_info['columns']:
                nullable = "NULL" if is_nullable == "YES" else "NOT NULL"
                default = f" DEFAULT {default_val}" if default_val else ""
                print(f"  - {col_name}: {data_type} {nullable}{default}")
            
            # Load sample data (first 5 rows) for preview
            if table_info['row_count'] > 0:
                print(f"\nSample data (first 5 rows):")
                sample_data = load_table_data(pg_cur, table_name, limit=5)
                if not sample_data.empty:
                    print(sample_data.to_string(index=False, max_cols=10))
                    
                    # Store full data (you might want to limit this for large tables)
                    if table_info['row_count'] <= 10000:  # Only load full data for smaller tables
                        print(f"Loading full data for {table_name}...")
                        all_tables_data[table_name] = load_table_data(pg_cur, table_name)
                    else:
                        print(f"Table {table_name} is large ({table_info['row_count']} rows). Loading first 1000 rows...")
                        all_tables_data[table_name] = load_table_data(pg_cur, table_name, limit=1000)
                else:
                    print("No sample data available.")
            else:
                print("Table is empty.")
        
        print("\n" + "="*50)
        print("SUMMARY")
        print("="*50)
        print(f"Total tables processed: {len(tables)}")
        print(f"Tables with data loaded: {len(all_tables_data)}")
        
        # You can now work with all_tables_data dictionary
        # Example: Access data from a specific table
        # df = all_tables_data['your_table_name']
        
        # Optionally, save data to files
        save_to_files = input("\nDo you want to save table data to CSV files? (y/n): ").lower().strip()
        if save_to_files == 'y':
            import os
            output_dir = "exported_tables"
            os.makedirs(output_dir, exist_ok=True)
            
            for table_name, df in all_tables_data.items():
                if not df.empty:
                    filename = f"{output_dir}/{table_name}.csv"
                    df.to_csv(filename, index=False)
                    print(f"Saved {table_name} to {filename}")
            
            print(f"\nAll table data saved to '{output_dir}' directory.")
        
        return all_tables_data
        
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
    finally:
        pg_cur.close()
        pg_conn.close()
        print("\nDatabase connection closed.")

if __name__ == "__main__":
    loaded_data = main()
