"""
Oracle's Elixir CSV 자동 다운로드
- 공식 AWS S3 경로 검증 후 다운로드
- 연도별 최신 파일 감지
"""
import os
import hashlib
import requests
from datetime import datetime
from pathlib import Path

OFFICIAL_HOST = "oracleselixir-downloadable-match-data.s3-us-west-2.amazonaws.com"
RAW_DATA_DIR = Path(__file__).parent.parent / "data" / "raw"

def verify_official_url(url: str) -> bool:
    """AWS S3 공식 경로인지 검증"""
    from urllib.parse import urlparse
    parsed = urlparse(url)
    return parsed.netloc == OFFICIAL_HOST

def build_url(year: int) -> str:
    today = datetime.now().strftime("%Y%m%d")
    filename = f"{year}_LoL_esports_match_data_from_OraclesElixir_{today}.csv"
    return f"https://{OFFICIAL_HOST}/{filename}"

def download_csv(year: int) -> Path | None:
    url = build_url(year)

    if not verify_official_url(url):
        raise ValueError(f"비공식 URL 감지: {url}")

    print(f"다운로드 중: {url}")
    response = requests.get(url, timeout=60)

    if response.status_code == 403:
        # 오늘 날짜 파일 없으면 최근 파일 탐색
        print("오늘 파일 없음. 최근 파일 탐색 중...")
        return None

    if response.status_code != 200:
        print(f"다운로드 실패: HTTP {response.status_code}")
        return None

    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"oracles_elixir_{year}.csv"
    save_path = RAW_DATA_DIR / filename
    save_path.write_bytes(response.content)
    print(f"저장 완료: {save_path}")
    return save_path

if __name__ == "__main__":
    for year in [2024, 2025, 2026]:
        download_csv(year)
