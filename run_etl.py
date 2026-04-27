"""
전체 ETL 실행 스크립트
순서: Oracle's Elixir → Community Dragon → Leaguepedia
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

def main():
    print("=" * 50)
    print("1단계: Oracle's Elixir CSV → DB 로드")
    print("=" * 50)
    from etl.load_to_db import run_etl
    for year in [2024, 2025, 2026]:
        run_etl(year)

    print("\n" + "=" * 50)
    print("2단계: Community Dragon (아이콘 + 패치 날짜)")
    print("=" * 50)
    from etl.download_community_dragon import run as run_cdragon
    run_cdragon()

    print("\n" + "=" * 50)
    print("3단계: Leaguepedia (패치 변경 데이터)")
    print("=" * 50)
    from etl.download_leaguepedia import run as run_wiki
    run_wiki()

    print("\n" + "=" * 50)
    print("4단계: PBI 계산 (champion_meta 업데이트)")
    print("=" * 50)
    from analysis.scenario_c import calculate_patch_pbi
    calculate_patch_pbi()

    print("\n전체 ETL 완료.")


if __name__ == "__main__":
    main()
