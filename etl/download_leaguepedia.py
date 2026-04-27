"""
Leaguepedia 위키 API 수집
- 패치별 버프/너프 챔피언 목록 (시나리오 C: 패치 적응 속도 분석용)
- cargo API 사용
"""
import os
import time
import json
import requests
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

WIKI_API = "https://lol.fandom.com/api.php"
DATA_DIR = Path(__file__).parent.parent / "data" / "raw"

# 패치 노트 테이블이 Leaguepedia에 구조화되어 있지 않으므로
# Riot 공식 패치 노트 페이지를 파싱하는 대신
# community-maintained 데이터를 cargo로 조회

HEADERS = {"User-Agent": "LCK-Analysis-Tool/1.0 (hmnbuild@gmail.com)"}


def get_engine():
    url = (
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    return create_engine(url)


def cargo_query(tables: str, fields: str, where: str = "", limit: int = 500, offset: int = 0) -> list:
    """Leaguepedia Cargo API 쿼리"""
    params = {
        "action": "cargoquery",
        "format": "json",
        "tables": tables,
        "fields": fields,
        "limit": str(limit),
        "offset": str(offset),
    }
    if where:
        params["where"] = where

    try:
        resp = requests.get(WIKI_API, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return [item["title"] for item in data.get("cargoquery", [])]
    except Exception as e:
        print(f"  [오류] cargo_query: {e}")
        return []


def fetch_lck_games(year: int) -> list:
    """
    LCK 경기 ScoreboardGames 테이블 수집
    pick_order 보완용 (Oracle's Elixir에 firstPick 컬럼 있으므로 보조적으로 사용)
    """
    print(f"LCK {year} 경기 데이터 수집 중...")
    results = []
    offset = 0
    while True:
        rows = cargo_query(
            tables="ScoreboardGames",
            fields="GameId,Patch,Team1,Team2,WinTeam,Team1Picks,Team2Picks,Team1Bans,Team2Bans,DateTime_UTC",
            where=f"Tournament LIKE 'LCK {year}%'",
            limit=500,
            offset=offset,
        )
        if not rows:
            break
        results.extend(rows)
        if len(rows) < 500:
            break
        offset += 500
        time.sleep(0.5)

    print(f"  {year}년 경기: {len(results)}개")
    return results


def fetch_patch_changes() -> dict:
    """
    패치별 버프/너프 챔피언 목록
    Leaguepedia PatchNotes 테이블 조회
    반환: {patch_id: {"buffed": [...], "nerfed": [...], "adjusted": [...]}}
    """
    print("패치 변경 데이터 수집 중...")
    results = {}
    offset = 0

    while True:
        rows = cargo_query(
            tables="PatchNotes",
            fields="Patch,Champion,ChangeType",
            where="Champion IS NOT NULL AND ChangeType IS NOT NULL",
            limit=500,
            offset=offset,
        )
        if not rows:
            break
        for row in rows:
            patch = row.get("Patch", "").strip()
            champ = row.get("Champion", "").strip()
            change = row.get("ChangeType", "").strip().lower()
            if not patch or not champ:
                continue
            if patch not in results:
                results[patch] = {"buffed": [], "nerfed": [], "adjusted": []}
            if "buff" in change:
                results[patch]["buffed"].append(champ)
            elif "nerf" in change:
                results[patch]["nerfed"].append(champ)
            else:
                results[patch]["adjusted"].append(champ)
        if len(rows) < 500:
            break
        offset += 500
        time.sleep(0.5)

    if not results:
        print("  PatchNotes 테이블 없음. 로컬 캐시 사용.")
        return _load_patch_changes_cache()

    print(f"  패치 {len(results)}개 변경 데이터 수집 완료")
    return results


def _load_patch_changes_cache() -> dict:
    """
    Leaguepedia API 실패 시 로컬 캐시 반환
    캐시 파일이 없으면 빈 dict 반환
    """
    cache_path = DATA_DIR / "patch_changes_cache.json"
    if cache_path.exists():
        data = json.loads(cache_path.read_text(encoding="utf-8"))
        print(f"  캐시 로드: {len(data)}개 패치")
        return data
    return {}


def save_patch_changes_cache(data: dict):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = DATA_DIR / "patch_changes_cache.json"
    cache_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  캐시 저장: {cache_path}")


def upsert_patch_changes_to_db(conn, patch_changes: dict):
    """
    패치 변경 데이터를 DB에 저장
    champion_meta 테이블에 해당 패치의 챔피언을 pre-populate
    (픽률/밴률/승률은 ETL 이후 계산)
    """
    count = 0
    for patch_id, changes in patch_changes.items():
        for change_type, champs in changes.items():
            for champ in champs:
                # champions 테이블에 없으면 먼저 삽입
                conn.execute(text("""
                    INSERT INTO champions (champion_id, name)
                    VALUES (:cid, :name)
                    ON CONFLICT (champion_id) DO NOTHING
                """), {"cid": champ, "name": champ})

                # patch_versions 테이블에 없으면 삽입
                conn.execute(text("""
                    INSERT INTO patch_versions (patch_id, version)
                    VALUES (:pid, :pid)
                    ON CONFLICT (patch_id) DO NOTHING
                """), {"pid": patch_id})

                # champion_meta에 change_type 컬럼이 없으므로
                # 여기서는 pre-populate만 (승률 등은 나중에 계산)
                conn.execute(text("""
                    INSERT INTO champion_meta (champion_id, patch_id)
                    VALUES (:cid, :pid)
                    ON CONFLICT (champion_id, patch_id) DO NOTHING
                """), {"cid": champ, "pid": patch_id})
                count += 1

    print(f"  champion_meta pre-populate: {count}개")


def run():
    patch_changes = fetch_patch_changes()
    save_patch_changes_cache(patch_changes)

    if patch_changes:
        engine = get_engine()
        with engine.begin() as conn:
            upsert_patch_changes_to_db(conn, patch_changes)

    print("\nLeaguepedia 수집 완료")


if __name__ == "__main__":
    run()
