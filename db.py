# db.py
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

load_dotenv()

DB_URL = os.getenv("DB_URL", "sqlite:///otms.db")
connect_args = {"check_same_thread": False} if DB_URL.startswith("sqlite") else {}

engine = create_engine(DB_URL, echo=False, future=True, connect_args=connect_args)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

# Import AFTER engine so models bind to this MetaData one time
from models import Base  # noqa: E402

def get_session():
    return SessionLocal()

def init_db():
    Base.metadata.create_all(bind=engine)
    _ensure_schema_updates()


def _ensure_schema_updates():
    """Apply lightweight schema updates without full migrations."""
    try:
        backend = engine.url.get_backend_name()
    except Exception:
        backend = "sqlite"

    if backend != "sqlite":
        return

    inspector_stmt = text("PRAGMA table_info('users')")
    try:
        with engine.begin() as conn:
            existing_cols = {row[1] for row in conn.execute(inspector_stmt).fetchall()}
            if "supervisor_code_hash" not in existing_cols:
                conn.execute(text("ALTER TABLE users ADD COLUMN supervisor_code_hash VARCHAR(255)"))
            if "supervisor_code_set_at" not in existing_cols:
                conn.execute(text("ALTER TABLE users ADD COLUMN supervisor_code_set_at DATETIME"))
    except Exception:
        pass
