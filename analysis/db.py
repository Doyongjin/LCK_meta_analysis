"""공용 DB 연결"""
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

_engine = None


def get_engine():
    global _engine
    if _engine is None:
        from sqlalchemy.engine import URL
        url = URL.create(
            drivername="postgresql+psycopg2",
            username=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT", 5432)),
            database=os.getenv("DB_NAME"),
        )
        _engine = create_engine(url)
    return _engine
