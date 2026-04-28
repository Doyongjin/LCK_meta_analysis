"""공용 DB 연결"""
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

load_dotenv()

_engine = None


def _get_secret(key: str, default: str = "") -> str:
    """Streamlit Secrets → 환경변수 순으로 조회"""
    try:
        import streamlit as st
        return st.secrets[key]
    except Exception:
        return os.getenv(key, default)


def get_engine():
    global _engine
    if _engine is None:
        url = URL.create(
            drivername="postgresql+psycopg2",
            username=_get_secret("DB_USER"),
            password=_get_secret("DB_PASSWORD"),
            host=_get_secret("DB_HOST"),
            port=int(_get_secret("DB_PORT", "5432")),
            database=_get_secret("DB_NAME"),
        )
        _engine = create_engine(url)
    return _engine
