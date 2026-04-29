"""
Oracle's Elixir CSV 자동 다운로드
- 공식 AWS S3 경로 검증 후 다운로드
- 오늘 날짜 파일 없으면 최근 30일까지 역순 탐색
"""
import requests
from datetime import datetime, timedelta
from pathlib import Path

OFFICIAL_HOST = "oracleselixir-downloadable-match-data.s3-us-west-2.amazonaws.com"
_DEFAULT_RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
_TMP_RAW_DIR = Path("/tmp/lck_raw")


def _raw_dir() -> Path:
    if _DEFAULT_RAW_DIR.exists():
        return _DEFAULT_RAW_DIR
    _TMP_RAW_DIR.mkdir(parents=True, exist_ok=True)
    return _TMP_RAW_DIR


def _verify_official_url(url: str) -> bool:
    from urllib.parse import urlparse
    return urlparse(url).netloc == OFFICIAL_HOST


def _build_url(year: int, date: datetime) -> str:
    filename = f"{year}_LoL_esports_match_data_from_OraclesElixir_{date.strftime('%Y%m%d')}.csv"
    return f"https://{OFFICIAL_HOST}/{filename}"


def _save_filename(year: int) -> str:
    """ETL이 찾는 파일명과 동일하게 저장"""
    return f"{year}_LoL_esports_match_data_from_OraclesElixir.csv"


def download_csv(year: int, lookback_days: int = 30) -> Path | None:
    """
    최신 Oracle's Elixir CSV 다운로드.
    오늘 날짜부터 역순으로 lookback_days일까지 탐색.
    """
    raw_dir = _raw_dir()
    raw_dir.mkdir(parents=True, exist_ok=True)
    save_path = raw_dir / _save_filename(year)

    for days_ago in range(lookback_days):
        date = datetime.now() - timedelta(days=days_ago)
        url = _build_url(year, date)

        if not _verify_official_url(url):
            raise ValueError(f"비공식 URL 감지: {url}")

        print(f"다운로드 시도: {url}")
        try:
            response = requests.get(url, timeout=60)
        except requests.RequestException as e:
            print(f"  요청 오류: {e}")
            continue

        if response.status_code == 200:
            save_path.write_bytes(response.content)
            print(f"저장 완료: {save_path} ({date.strftime('%Y-%m-%d')} 기준)")
            return save_path

        if response.status_code not in (403, 404):
            print(f"  HTTP {response.status_code} — 중단")
            break

    print(f"{year}년 파일을 찾지 못했습니다 (최근 {lookback_days}일 탐색)")
    return None


if __name__ == "__main__":
    for year in [2024, 2025, 2026]:
        download_csv(year)
