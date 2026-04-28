"""
Oracle's Elixir CSV -> PostgreSQL ETL
2024/2025/2026 LCK 경기 데이터를 DB 스키마에 맞게 재구성해서 삽입
"""
import sys
import math
import pandas as pd
from pathlib import Path
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).parent.parent))

_DEFAULT_RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
_TMP_RAW_DIR = Path("/tmp/lck_raw")

def _raw_dir() -> Path:
    """Streamlit Cloud는 /tmp 사용, 로컬은 data/raw 사용"""
    if _DEFAULT_RAW_DIR.exists():
        return _DEFAULT_RAW_DIR
    _TMP_RAW_DIR.mkdir(parents=True, exist_ok=True)
    return _TMP_RAW_DIR

def _csv_path(year: int) -> Path:
    for d in [_DEFAULT_RAW_DIR, _TMP_RAW_DIR]:
        p = d / f"{year}_LoL_esports_match_data_from_OraclesElixir.csv"
        if p.exists():
            return p
    return _raw_dir() / f"{year}_LoL_esports_match_data_from_OraclesElixir.csv"

# 픽 순서 매핑 (글로벌 픽 슬롯 1~10)
# 선픽팀 pick1~5 -> 글로벌 슬롯
FIRST_PICK_MAP  = {"pick1": 1, "pick2": 4, "pick3": 5, "pick4": 8, "pick5": 9}
SECOND_PICK_MAP = {"pick1": 2, "pick2": 3, "pick3": 6, "pick4": 7, "pick5": 10}

# 밴 순서 매핑 (글로벌 밴 슬롯 1~10)
# 블루팀: 1페이즈 선밴 / 레드팀: 2페이즈 선밴
BLUE_BAN_MAP = {"ban1": 1, "ban2": 3, "ban3": 5, "ban4": 8, "ban5": 10}
RED_BAN_MAP  = {"ban1": 2, "ban2": 4, "ban3": 6, "ban4": 7, "ban5": 9}


def get_engine():
    from analysis.db import get_engine as _get_engine
    return _get_engine()


def val(v):
    """NaN -> None 변환, numpy 스칼라 -> Python 기본형 변환"""
    if v is None:
        return None
    try:
        if math.isnan(float(v)):
            return None
    except (TypeError, ValueError):
        pass
    # numpy 스칼라를 psycopg2가 인식하는 Python 기본형으로 변환
    if hasattr(v, 'item'):
        return v.item()
    return v


def insert_seasons(conn, df):
    """seasons 테이블 삽입"""
    seen = set()
    for _, row in df[['year', 'split', 'league']].drop_duplicates().iterrows():
        year = int(row['year'])
        split = str(row['split'])
        rule_era = 'first_selection' if year >= 2026 else 'pre_2026'
        season_id = f"LCK_{year}_{split}"
        if season_id in seen:
            continue
        seen.add(season_id)
        conn.execute(text("""
            INSERT INTO seasons (season_id, year, split, rule_era)
            VALUES (:sid, :year, :split, :era)
            ON CONFLICT (season_id) DO NOTHING
        """), {"sid": season_id, "year": year, "split": split, "era": rule_era})
    print(f"  seasons: {len(seen)}개")


def insert_patches(conn, df):
    """patch_versions 테이블 삽입"""
    patches = df[['patch', 'year', 'split']].drop_duplicates()
    count = 0
    for _, row in patches.iterrows():
        patch_id = str(row['patch'])
        season_id = f"LCK_{int(row['year'])}_{row['split']}"
        conn.execute(text("""
            INSERT INTO patch_versions (patch_id, version, season_id)
            VALUES (:pid, :ver, :sid)
            ON CONFLICT (patch_id) DO NOTHING
        """), {"pid": patch_id, "ver": patch_id, "sid": season_id})
        count += 1
    print(f"  patch_versions: {count}개")


def insert_champions(conn, df):
    """champions 테이블 삽입 (밴픽에 등장한 모든 챔피언)"""
    champ_cols = ['ban1','ban2','ban3','ban4','ban5','pick1','pick2','pick3','pick4','pick5','champion']
    champs = set()
    for col in champ_cols:
        if col in df.columns:
            champs.update(df[col].dropna().unique())
    count = 0
    for c in champs:
        c = str(c).strip()
        if not c or c == 'nan':
            continue
        conn.execute(text("""
            INSERT INTO champions (champion_id, name)
            VALUES (:cid, :name)
            ON CONFLICT (champion_id) DO NOTHING
        """), {"cid": c, "name": c})
        count += 1
    print(f"  champions: {count}개")


def insert_teams(conn, df):
    """teams 테이블 삽입, team_id 맵 반환"""
    teams = df['teamname'].dropna().unique()
    team_map = {}
    for name in teams:
        name = str(name).strip()
        if not name or name == 'nan':
            continue
        result = conn.execute(text("""
            INSERT INTO teams (name, acronym, region)
            VALUES (:name, :acronym, 'LCK')
            ON CONFLICT (name) DO NOTHING
            RETURNING team_id
        """), {"name": name, "acronym": name[:10]})
        row = result.fetchone()
        if row:
            team_map[name] = row[0]
        else:
            row = conn.execute(text("SELECT team_id FROM teams WHERE name=:n"), {"n": name}).fetchone()
            if row:
                team_map[name] = row[0]
    print(f"  teams: {len(team_map)}개")
    return team_map


def insert_players(conn, df):
    """players 테이블 삽입, player_id 맵 반환"""
    player_rows = df[df['position'] != 'team'][['playername']].dropna().drop_duplicates()
    player_map = {}
    for _, row in player_rows.iterrows():
        name = str(row['playername']).strip()
        if not name or name == 'nan':
            continue
        result = conn.execute(text("""
            INSERT INTO players (summoner_name)
            VALUES (:name)
            ON CONFLICT (summoner_name) DO NOTHING
            RETURNING player_id
        """), {"name": name})
        r = result.fetchone()
        if r:
            player_map[name] = r[0]
        else:
            r = conn.execute(text("SELECT player_id FROM players WHERE summoner_name=:n"), {"n": name}).fetchone()
            if r:
                player_map[name] = r[0]
    print(f"  players: {len(player_map)}개")
    return player_map


def insert_series_and_games(conn, df, team_map):
    """
    series, games, game_teams, game_participants, picks_bans 삽입
    game_id 맵 반환
    """
    team_rows = df[df['position'] == 'team'].copy()
    player_rows = df[df['position'] != 'team'].copy()

    # gameid 기준으로 묶기
    game_ids_in_csv = team_rows['gameid'].unique()
    game_id_map = {}  # csv gameid -> db game_id

    # 시리즈 구성: 같은 날짜, 같은 두 팀의 연속 게임을 하나의 시리즈로 묶음
    # game 컬럼(1,2,3...)을 기준으로 game=1인 행마다 새 시리즈 시작
    team_rows = team_rows.sort_values(['date', 'gameid', 'game'])
    blue_rows = team_rows[team_rows['side'] == 'Blue'].copy()

    series_cache = {}  # (date, team1, team2) -> series_id

    total_games = 0
    for _, blue in blue_rows.iterrows():
        gameid = blue['gameid']
        if gameid in game_id_map:
            continue

        # 같은 게임의 레드팀 행 찾기
        red = team_rows[(team_rows['gameid'] == gameid) & (team_rows['side'] == 'Red')]
        if red.empty:
            continue
        red = red.iloc[0]

        blue_team = str(blue['teamname'])
        red_team = str(red['teamname'])
        date_str = str(blue['date'])[:10]
        game_num = int(blue['game']) if not pd.isna(blue['game']) else 1
        year = int(blue['year'])
        split = str(blue['split'])
        season_id = f"LCK_{year}_{split}"
        patch_id = str(blue['patch'])

        # 시리즈 키: 날짜 + 두 팀 이름 (순서 무관)
        series_key = (date_str, tuple(sorted([blue_team, red_team])))

        if series_key not in series_cache:
            # 드래프트 타입 결정
            draft_type = 'fearless' if (year > 2025 or (year == 2025 and split != 'Spring')) else 'standard'
            # 정확한 피어리스 시점: 2025 LCK Cup부터 → split 이름 확인 필요
            # 일단 2025 Spring = standard, 이후 = fearless 로 처리
            t1_id = team_map.get(blue_team)
            t2_id = team_map.get(red_team)
            if not t1_id or not t2_id:
                continue
            r = conn.execute(text("""
                INSERT INTO series (season_id, team1_id, team2_id, format, draft_type, date)
                VALUES (:sid, :t1, :t2, 'BO3', :dt, :date)
                RETURNING series_id
            """), {"sid": season_id, "t1": t1_id, "t2": t2_id, "dt": draft_type, "date": date_str})
            series_id = r.fetchone()[0]
            series_cache[series_key] = series_id
        else:
            series_id = series_cache[series_key]

        # games 삽입
        r = conn.execute(text("""
            INSERT INTO games (series_id, patch_id, game_number, date, length_seconds)
            VALUES (:sid, :pid, :gnum, :date, :length)
            RETURNING game_id
        """), {
            "sid": series_id, "pid": patch_id, "gnum": game_num,
            "date": date_str, "length": val(blue['gamelength'])
        })
        db_game_id = r.fetchone()[0]
        game_id_map[gameid] = db_game_id
        total_games += 1

        # game_teams 삽입 (블루/레드)
        blue_first = int(val(blue['firstPick']) or 1)
        for side_row, side_name in [(blue, 'blue'), (red, 'red')]:
            team_id = team_map.get(str(side_row['teamname']))
            if not team_id:
                continue
            is_first = (side_name == 'blue' and blue_first == 1) or \
                       (side_name == 'red' and blue_first == 0)
            conn.execute(text("""
                INSERT INTO game_teams
                  (game_id, team_id, side, pick_order, first_selection_choice, result,
                   gold_at_15, first_dragon, first_herald, first_tower)
                VALUES (:gid, :tid, :side, :po, :fsc, :res, :g15, :fd, :fh, :ft)
            """), {
                "gid": db_game_id, "tid": team_id,
                "side": side_name,
                "po": 'first' if is_first else 'second',
                "fsc": None,  # first_selection_choice: 별도 데이터 필요
                "res": bool(int(val(side_row['result']) or 0)),
                "g15": val(side_row['goldat15']),
                "fd": bool(int(val(side_row['firstdragon']) or 0)),
                "fh": bool(int(val(side_row['firstherald']) or 0)),
                "ft": bool(int(val(side_row['firsttower']) or 0)),
            })

        # picks_bans 삽입
        _insert_picks_bans(conn, db_game_id, blue, red, blue_first, team_map)

    print(f"  games: {total_games}개, series: {len(series_cache)}개")

    # game_participants 삽입
    _insert_participants(conn, player_rows, game_id_map, team_map)
    return game_id_map


def _insert_picks_bans(conn, game_id, blue, red, blue_first, team_map):
    """picks_bans 삽입 (밴 10개 + 픽 10개)"""
    blue_id = team_map.get(str(blue['teamname']))
    red_id = team_map.get(str(red['teamname']))
    if not blue_id or not red_id:
        return

    # 밴 삽입
    for col, slot in BLUE_BAN_MAP.items():
        champ = val(blue[col]) if col in blue.index else None
        if champ:
            conn.execute(text("""
                INSERT INTO picks_bans (game_id, team_id, champion_id, phase, global_order)
                VALUES (:gid, :tid, :cid, 'ban', :go)
            """), {"gid": game_id, "tid": blue_id, "cid": str(champ), "go": slot})

    for col, slot in RED_BAN_MAP.items():
        champ = val(red[col]) if col in red.index else None
        if champ:
            conn.execute(text("""
                INSERT INTO picks_bans (game_id, team_id, champion_id, phase, global_order)
                VALUES (:gid, :tid, :cid, 'ban', :go)
            """), {"gid": game_id, "tid": red_id, "cid": str(champ), "go": slot})

    # 픽 삽입 (선픽/후픽에 따라 글로벌 슬롯 결정)
    if blue_first == 1:
        blue_pick_map, red_pick_map = FIRST_PICK_MAP, SECOND_PICK_MAP
    else:
        blue_pick_map, red_pick_map = SECOND_PICK_MAP, FIRST_PICK_MAP

    for i, (col, slot) in enumerate(blue_pick_map.items(), 1):
        champ = val(blue[col]) if col in blue.index else None
        if champ:
            conn.execute(text("""
                INSERT INTO picks_bans (game_id, team_id, champion_id, phase, global_order, team_pick_order)
                VALUES (:gid, :tid, :cid, 'pick', :go, :tpo)
            """), {"gid": game_id, "tid": blue_id, "cid": str(champ), "go": slot, "tpo": i})

    for i, (col, slot) in enumerate(red_pick_map.items(), 1):
        champ = val(red[col]) if col in red.index else None
        if champ:
            conn.execute(text("""
                INSERT INTO picks_bans (game_id, team_id, champion_id, phase, global_order, team_pick_order)
                VALUES (:gid, :tid, :cid, 'pick', :go, :tpo)
            """), {"gid": game_id, "tid": red_id, "cid": str(champ), "go": slot, "tpo": i})


def _insert_participants(conn, player_rows, game_id_map, team_map):
    """game_participants 삽입"""
    count = 0
    for _, row in player_rows.iterrows():
        gameid = row['gameid']
        if gameid not in game_id_map:
            continue
        db_game_id = game_id_map[gameid]
        player_name = str(val(row['playername']) or '').strip()
        team_name = str(val(row['teamname']) or '').strip()
        champion = str(val(row['champion']) or '').strip()
        if not player_name or player_name == 'nan':
            continue

        player_r = conn.execute(text(
            "SELECT player_id FROM players WHERE summoner_name=:n"
        ), {"n": player_name}).fetchone()
        team_id = team_map.get(team_name)
        if not player_r or not team_id:
            continue

        conn.execute(text("""
            INSERT INTO game_participants
              (game_id, player_id, team_id, champion_id, position,
               gold_at_15, cs_diff_at_15, xp_diff_at_15, kills, deaths, assists)
            VALUES (:gid, :pid, :tid, :cid, :pos, :g15, :cs15, :xp15, :k, :d, :a)
        """), {
            "gid": db_game_id,
            "pid": player_r[0],
            "tid": team_id,
            "cid": champion if champion and champion != 'nan' else None,
            "pos": str(row['position']),
            "g15": val(row['goldat15']),
            "cs15": val(row['csdiffat15']),
            "xp15": val(row['xpdiffat15']),
            "k": val(row.get('kills')),
            "d": val(row.get('deaths')),
            "a": val(row.get('assists')),
        })
        count += 1
    print(f"  game_participants: {count}개")


def insert_player_team_history(conn, df, team_map):
    """player_team_history 삽입 (선수-팀-시즌 소속 이력)"""
    player_rows = df[df['position'] != 'team'][
        ['playername', 'teamname', 'year', 'split', 'position']
    ].drop_duplicates()

    count = 0
    for _, row in player_rows.iterrows():
        player_name = str(val(row['playername']) or '').strip()
        team_name = str(val(row['teamname']) or '').strip()
        if not player_name or player_name == 'nan':
            continue

        player_r = conn.execute(text(
            "SELECT player_id FROM players WHERE summoner_name=:n"
        ), {"n": player_name}).fetchone()
        team_id = team_map.get(team_name)
        season_id = f"LCK_{int(row['year'])}_{row['split']}"
        if not player_r or not team_id:
            continue

        conn.execute(text("""
            INSERT INTO player_team_history (player_id, team_id, season_id, role)
            VALUES (:pid, :tid, :sid, :role)
            ON CONFLICT DO NOTHING
        """), {
            "pid": player_r[0], "tid": team_id,
            "sid": season_id, "role": str(row['position'])
        })
        count += 1
    print(f"  player_team_history: {count}개")


def run_etl(year: int):
    csv_path = _csv_path(year)
    if not csv_path.exists():
        print(f"[건너뜀] 파일 없음: {csv_path}")
        return

    print(f"\n{'='*40}")
    print(f"{year}년 ETL 시작")
    print(f"{'='*40}")

    df = pd.read_csv(csv_path, low_memory=False)
    df = df[df['league'] == 'LCK'].copy()
    print(f"LCK 행 수: {len(df)}")

    engine = get_engine()
    with engine.begin() as conn:
        insert_seasons(conn, df)
        insert_patches(conn, df)
        insert_champions(conn, df)
        team_map = insert_teams(conn, df)
        insert_players(conn, df)
        game_id_map = insert_series_and_games(conn, df, team_map)
        insert_player_team_history(conn, df, team_map)

    print(f"\n{year}년 ETL 완료")


if __name__ == "__main__":
    for year in [2024, 2025, 2026]:
        run_etl(year)
