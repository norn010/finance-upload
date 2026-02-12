from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings


def _quote_identifier(name: str) -> str:
    return f"[{name.replace(']', ']]')}]"


def _ensure_sqlserver_database_exists(connection_string: str) -> None:
    url = make_url(connection_string)
    if not url.drivername.startswith("mssql"):
        return
    database_name = url.database
    if not database_name:
        return

    admin_engine = create_engine(
        url.set(database="master"),
        pool_pre_ping=True,
        isolation_level="AUTOCOMMIT",
    )
    try:
        with admin_engine.connect() as conn:
            exists = conn.execute(text("SELECT DB_ID(:db_name)"), {"db_name": database_name}).scalar()
            if exists is None:
                conn.exec_driver_sql(f"CREATE DATABASE {_quote_identifier(database_name)}")
    finally:
        admin_engine.dispose()


settings = get_settings()
_ensure_sqlserver_database_exists(settings.sqlserver_connection_string)
engine = create_engine(
    settings.sqlserver_connection_string,
    pool_pre_ping=True,
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
