"""공용 DB 연결 및 공통 필터 유틸리티"""
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


def build_game_filter(season_id=None, patch_id=None, alias="gp") -> tuple[str, dict]:
    """
    season_id / patch_id 조합으로 game_id IN (...) 필터 생성.
    alias: game_id를 참조하는 테이블 별칭 (gp, gt, pb 등)
    반환: (filter_str, params_dict)
    """
    conditions = []
    params: dict = {}
    if season_id:
        conditions.append("s.season_id = :sid")
        params["sid"] = season_id
    if patch_id:
        conditions.append("g.patch_id = :patch_id")
        params["patch_id"] = patch_id
    if not conditions:
        return "", {}
    where_clause = " AND ".join(conditions)
    filter_str = f"""AND {alias}.game_id IN (
        SELECT g.game_id FROM games g
        JOIN series s ON s.series_id = g.series_id
        WHERE {where_clause}
    )"""
    return filter_str, params
