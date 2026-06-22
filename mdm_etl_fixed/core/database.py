import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
from dotenv import load_dotenv

load_dotenv()

database_url = os.getenv("ETL_DATABASE_URL", "postgresql://user:password@host:port/dbname")

engine = create_engine(
    database_url,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=1800,
)

# scoped_session: one session per thread, auto-created on first use
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))


def get_conn():
    """
    Return the raw psycopg2 connection that backs the current SQLAlchemy session.

    WHY THIS EXISTS
    ---------------
    The repositories use psycopg2-style cursor() calls and
    psycopg2.extras.execute_values — they need a raw DBAPI connection.
    SQLAlchemy's scoped_session.connection() returns a SQLAlchemy Connection
    object, which does NOT have .cursor().

    get_conn() drills through the SQLAlchemy layer to the actual psycopg2
    connection. This keeps the repositories working with raw SQL while still
    benefiting from SQLAlchemy's connection pool and session lifecycle.
    """
    # get_bind() returns the Engine; connect() checks out a pooled connection.
    # .connection is the raw DBAPI (psycopg2) connection underneath.
    return db_session.get_bind().connect().connection


def init_app(app):
    """Register database teardown with the Flask app."""
    @app.teardown_appcontext
    def shutdown_session(exception=None):
        db_session.remove()