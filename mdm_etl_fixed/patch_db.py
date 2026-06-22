import os
import psycopg2
from dotenv import load_dotenv

# Load the Aiven database URL from your .env file
load_dotenv()
db_url = os.getenv("ETL_DATABASE_URL")

print("Connecting to the database...")

try:
    # Connect directly to PostgreSQL
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    cursor = conn.cursor()

    # Force add the missing column
    print("Patching use_case_definitions table...")
    cursor.execute("""
        ALTER TABLE use_case_definitions 
        ADD COLUMN IF NOT EXISTS mapping_by_file JSONB DEFAULT '{}'::jsonb NOT NULL;
    """)

    print("✅ Success! The 'mapping_by_file' column has been permanently added.")
    
except Exception as e:
    print(f"❌ Error: {e}")
finally:
    if 'conn' in locals():
        conn.close()