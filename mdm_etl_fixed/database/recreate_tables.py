import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from models import Base

# 1. Load your database URL from the .env file
load_dotenv()
database_url = os.getenv("ETL_DATABASE_URL")

if not database_url:
    print("❌ Error: ETL_DATABASE_URL not found in .env file.")
    exit(1)

print(f"Connecting to database and scanning models.py...")

# 2. Create a direct engine connection
engine = create_engine(database_url)

# 3. Create only the missing tables (this will NOT overwrite existing data)
Base.metadata.create_all(bind=engine)

print("✅ Missing tables have been successfully recreated!")