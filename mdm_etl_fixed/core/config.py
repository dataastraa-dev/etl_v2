# core/config.py
import os
from dotenv import load_dotenv

# Load local .env file
load_dotenv()

class Config:
    """Central configuration object for the ETL Engine."""
    
    # Database
    DATABASE_URL = os.getenv("ETL_DATABASE_URL")
    
    # Processing
    DEFAULT_CHUNK_SIZE_MB = int(os.getenv("CHUNK_SIZE_MB", 64))
    MAX_WORKERS = int(os.getenv("MAX_WORKERS", 4))
    
    # System
    ENVIRONMENT = os.getenv("ENVIRONMENT", "DEVELOPMENT").upper()

    @classmethod
    def validate(cls):
        if not cls.DATABASE_URL:
            raise ValueError("CRITICAL: ETL_DATABASE_URL environment variable is missing.")