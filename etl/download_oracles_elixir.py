"""
Oracle's Elixir CSV 다운로드 — Google Drive 버전
공식 Google Drive 폴더에서 연도별 CSV 다운로드
폴더: https://drive.google.com/drive/folders/1gLSw0RLjBbtaNy0dgnGQDAZOHIgCe-HH
"""
import shutil
import tempfile
from pathlib import Path

GDRIVE_FOLDER_ID = "1gLSw0RLjBbtaNy0dgnGQDAZOHIgCe-HH"

_DEFAULT_RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
_TMP_RAW_DIR = Path("/tmp/lck_raw")


def _raw_dir() -> Path:
    if _DEFAULT_RAW_DIR.exists():
        return _DEFAULT_RAW_DIR
    _TMP_RAW_DIR.mkdir(parents=True, exist_ok=True)
    return _TMP_RAW_DIR


def _save_filename(year: int) -> str:
    return f"{year}_LoL_esports_match_data_from_OraclesElixir.csv"


def download_csv(year: int) -> Path | None:
    """
    Google Drive 공개 폴더에서 해당 연도 CSV 다운로드.
    폴더 내 파일을 임시 디렉토리에 받은 뒤 연도에 맞는 파일만 저장.
    """
    import gdown

    raw_dir = _raw_dir()
    raw_dir.mkdir(parents=True, exist_ok=True)
    save_path = raw_dir / _save_filename(year)
    target_name = _save_filename(year)

    folder_url = f"https://drive.google.com/drive/folders/{GDRIVE_FOLDER_ID}"
    print(f"Google Drive 폴더 스캔 중: {folder_url}")

    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            gdown.download_folder(
                folder_url,
                output=tmpdir,
                quiet=False,
                use_cookies=False,
            )
        except Exception as e:
            print(f"Google Drive 다운로드 실패: {e}")
            return None

        # 연도 파일 찾기 (파일명 정확히 일치 우선, 없으면 연도 포함 파일)
        found = None
        for f in Path(tmpdir).rglob("*.csv"):
            if f.name == target_name:
                found = f
                break
        if found is None:
            for f in Path(tmpdir).rglob("*.csv"):
                if str(year) in f.name:
                    found = f
                    break

        if found:
            shutil.copy2(found, save_path)
            print(f"저장 완료: {save_path}")
            return save_path

    print(f"{year}년 파일을 찾지 못했습니다")
    return None


if __name__ == "__main__":
    for year in [2024, 2025, 2026]:
        download_csv(year)
