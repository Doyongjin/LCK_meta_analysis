"""
시나리오 H — 스페셜리스트 챔피언 판별
LCK 동일 라인 평균 대비 이 선수가 유독 잘하는 챔피언 식별
기준: 최소 플레이 횟수 + 초과 승률 + 15분 지표 우위 + Gold@15 안정성(낮은 표준편차)

피어리스 보정: 챔피언별로 "피어리스 시리즈 후반(2/3경기) 픽 비율" 표시
  - 후반 경기 픽이 높으면 강제 픽일 가능성 → 스페셜리스트 신뢰도 하락
"""
from sqlalchemy import text
from .db import get_engine

POS_ORDER = ["top", "jng", "mid", "bot", "sup"]


def get_team_roster_by_games(team_name: str, season_id: str | None = None) -> dict:
    """
    실제 출전 경기 기반 팀 로스터 반환
    포지션별 선수 목록 + 경기수, 주전/콜업 구분
    콜업 기준: 포지션 내 최다 출전자 경기수의 50% 미만
    """
    engine = get_engine()
    with engine.connect() as conn:
        team = conn.execute(
            text("SELECT team_id FROM teams WHERE name = :n OR acronym = :n"),
            {"n": team_name}
        ).fetchone()
        if not team:
            return {"error": f"팀 없음: {team_name}"}
        tid = team[0]

        s_filter = ""
        params: dict = {"tid": tid}
        if season_id:
            s_filter = """AND gp.game_id IN (
                SELECT g.game_id FROM games g
                JOIN series s ON s.series_id = g.series_id
                WHERE s.season_id = :sid
            )"""
            params["sid"] = season_id

        rows = conn.execute(text(f"""
            SELECT p.summoner_name, gp.position, COUNT(*) AS games
            FROM game_participants gp
            JOIN players p ON p.player_id = gp.player_id
            JOIN game_teams gt ON gt.game_id = gp.game_id AND gt.team_id = gp.team_id
            WHERE gt.team_id = :tid
              AND gp.position IS NOT NULL
              {s_filter}
            GROUP BY p.summoner_name, gp.position
            ORDER BY gp.position, games DESC
        """), params).fetchall()

    roster: dict = {}
    for name, pos, games in rows:
        if pos not in roster:
            roster[pos] = []
        roster[pos].append({"player": name, "games": int(games)})

    # 주전/콜업 구분
    result: dict = {}
    for pos in POS_ORDER:
        players = roster.get(pos, [])
        if not players:
            continue
        max_games = players[0]["games"]
        starters, callups = [], []
        for p in players:
            if p["games"] >= max_games * 0.5:
                starters.append(p)
            else:
                callups.append(p)
        result[pos] = {"starters": starters, "callups": callups}

    return {"team": team_name, "roster": result}

MIN_GAMES = 1                    # 표시 최소 경기수 (1경기부터 표 노출)
SPECIALIST_MIN_GAMES = 3         # 스페셜리스트 판정에 필요한 최소 표본
JOKER_MIN_GAMES = 1          # 조커 픽 본인 최소 경기 (1경기면 승률 100% 필수)
JOKER_MIN_LCK_GAMES = 1      # LCK 그 라인 총 사용량 최소
JOKER_MIN_SHARE = 0.70       # 본인 점유율 최소
JOKER_MIN_WR_MULTI = 0.50    # 2경기+ 본인 승률 최소
JOKER_MIN_WR_SOLO = 1.00     # 1경기일 때 본인 승률 최소 (= 100%)


def get_specialist_champions(player_name: str,
                              season_id: str | None = None) -> dict:
    """
    선수의 스페셜리스트 챔피언 목록
    season_id 지정 시 해당 시즌 게임만 집계 (LCK 평균도 동일 시즌 기준)
    피어리스 후반 경기 픽 비율을 보조 지표로 제공
    """
    engine = get_engine()
    with engine.connect() as conn:
        player = conn.execute(
            text("SELECT player_id FROM players WHERE summoner_name = :n"),
            {"n": player_name}
        ).fetchone()
        if not player:
            return {"error": f"선수 없음: {player_name}"}
        pid = player[0]

        pos_row = conn.execute(text("""
            SELECT role FROM player_team_history
            WHERE player_id = :pid
            ORDER BY id DESC LIMIT 1
        """), {"pid": pid}).fetchone()
        position = pos_row[0] if pos_row else "mid"

        s_filter = ""
        s_params_base: dict = {}
        if season_id:
            s_filter = """AND gp.game_id IN (
                SELECT g.game_id FROM games g
                JOIN series s ON s.series_id = g.series_id
                WHERE s.season_id = :sid
            )"""
            s_params_base["sid"] = season_id

        # 선수 챔피언별 통계 + 피어리스 후반(>=2경기) 픽 카운트
        player_stats = conn.execute(text(f"""
            SELECT
                gp.champion_id,
                c.icon_url,
                COUNT(*) AS games,
                AVG(gt.result::int) AS player_wr,
                AVG(gp.gold_at_15) AS player_gold15,
                STDDEV(gp.gold_at_15) AS player_gold15_stddev,
                SUM(CASE
                      WHEN ser.draft_type = 'fearless' AND g.game_number >= 2 THEN 1
                      ELSE 0
                    END) AS fearless_late_games
            FROM game_participants gp
            JOIN game_teams gt  ON gt.game_id = gp.game_id AND gt.team_id = gp.team_id
            JOIN games g        ON g.game_id = gp.game_id
            JOIN series ser     ON ser.series_id = g.series_id
            LEFT JOIN champions c ON c.champion_id = gp.champion_id
            WHERE gp.player_id = :pid
              AND gp.champion_id IS NOT NULL
              {s_filter}
            GROUP BY gp.champion_id, c.icon_url
            HAVING COUNT(*) >= :min_games
            ORDER BY games DESC
        """), {"pid": pid, "min_games": MIN_GAMES, **s_params_base}).fetchall()

        lck_avg = conn.execute(text(f"""
            SELECT
                gp.champion_id,
                AVG(gt.result::int) AS lck_wr,
                AVG(gp.gold_at_15) AS lck_gold15,
                STDDEV(gp.gold_at_15) AS lck_gold15_stddev,
                COUNT(*) AS lck_total_games
            FROM game_participants gp
            JOIN game_teams gt ON gt.game_id = gp.game_id AND gt.team_id = gp.team_id
            WHERE gp.position = :pos
              AND gp.champion_id IS NOT NULL
              {s_filter}
            GROUP BY gp.champion_id
        """), {"pos": position, **s_params_base}).fetchall()

    lck_map = {
        r[0]: {
            "wr": float(r[1] or 0.5),
            "gold15": float(r[2] or 0),
            "gold15_stddev": float(r[3] or 0),
            "total_games": int(r[4] or 0),
        }
        for r in lck_avg
    }

    all_champions = []
    for row in player_stats:
        cid, icon, games, p_wr, p_g15, p_g15_std, fearless_late = row
        lck = lck_map.get(cid, {"wr": 0.5, "gold15": 0.0, "gold15_stddev": 0.0, "total_games": 0})

        excess_wr = float(p_wr or 0) - lck["wr"]
        g15_adv   = float(p_g15 or 0) - lck["gold15"]
        stab_adv  = lck["gold15_stddev"] - float(p_g15_std or 0)

        score = (excess_wr * 50) + (g15_adv / 200 * 30) + (stab_adv / 100 * 20)

        fl_late = int(fearless_late or 0)
        forced_ratio = round(fl_late / games, 3) if games > 0 else 0.0
        likely_forced = forced_ratio >= 0.5
        is_specialist = (
            games >= SPECIALIST_MIN_GAMES
            and (excess_wr > 0 or g15_adv > 0 or stab_adv > 0)
        )

        # 조커 픽 판정: 본인이 LCK 같은 라인에서 그 챔피언을 거의 독점 사용
        # 1경기면 승률 100% 필수, 2경기 이상이면 승률 50%+ 필요
        lck_total = lck["total_games"]
        joker_share = round(games / lck_total, 3) if lck_total > 0 else 0.0
        wr_threshold = JOKER_MIN_WR_SOLO if games == 1 else JOKER_MIN_WR_MULTI
        is_joker_pick = (
            games >= JOKER_MIN_GAMES
            and lck_total >= JOKER_MIN_LCK_GAMES
            and joker_share >= JOKER_MIN_SHARE
            and float(p_wr or 0) >= wr_threshold
        )

        all_champions.append({
            "champion": cid,
            "icon_url": icon,
            "games": int(games),
            "player_wr": round(float(p_wr or 0), 3),
            "lck_avg_wr": round(lck["wr"], 3),
            "excess_wr": round(excess_wr, 3),
            "player_gold15": round(float(p_g15 or 0), 1),
            "lck_avg_gold15": round(lck["gold15"], 1),
            "gold15_advantage": round(g15_adv, 1),
            "player_gold15_stddev": round(float(p_g15_std or 0), 1),
            "lck_avg_gold15_stddev": round(lck["gold15_stddev"], 1),
            "gold15_stability_advantage": round(stab_adv, 1),
            "specialist_score": round(float(score), 2),
            "is_specialist": is_specialist,
            "fearless_late_games": fl_late,
            "forced_pick_ratio": forced_ratio,
            "likely_forced_pick": likely_forced,
            # 조커 픽
            "lck_total_games": lck_total,
            "joker_share": joker_share,
            "is_joker_pick": is_joker_pick,
        })

    all_champions.sort(key=lambda x: x["specialist_score"], reverse=True)
    specialists = [c for c in all_champions if c["is_specialist"]]

    return {
        "player": player_name,
        "position": position,
        "specialists": specialists,
        "all_champions": all_champions,
    }
