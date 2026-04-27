"""공용 DB 연결"""
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

_engine = None


def get_engine():
    global _engine
    if _engine is None:
        url = (
            f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
            f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        )
        _engine = create_engine(url)
    return _engine
