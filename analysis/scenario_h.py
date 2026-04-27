"""
시나리오 H — 스페셜리스트 챔피언 판별
LCK 동일 라인 평균 대비 이 선수가 유독 잘하는 챔피언 식별
기준: 최소 플레이 횟수 + 초과 승률 + 15분 지표 우위 + Gold@15 안정성(낮은 표준편차)
"""
from sqlalchemy import text
from .db import get_engine

MIN_GAMES = 3


def get_specialist_champions(player_name: str,
                              season_id: str | None = None) -> dict:
    """
    선수의 스페셜리스트 챔피언 목록
    season_id 지정 시 해당 시즌 게임만 집계 (LCK 평균도 동일 시즌 기준)
    반환:
    {
      "player": str,
      "position": str,
      "specialists": [...]
    }
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

        player_stats = conn.execute(text(f"""
            SELECT
                gp.champion_id,
                c.icon_url,
                COUNT(*) AS games,
                AVG(gt.result::int) AS player_wr,
                AVG(gp.gold_at_15) AS player_gold15,
                STDDEV(gp.gold_at_15) AS player_gold15_stddev
            FROM game_participants gp
            JOIN game_teams gt ON gt.game_id = gp.game_id AND gt.team_id = gp.team_id
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
                STDDEV(gp.gold_at_15) AS lck_gold15_stddev
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
        }
        for r in lck_avg
    }

    specialists = []
    for row in player_stats:
        cid, icon, games, p_wr, p_g15, p_g15_std = row
        lck = lck_map.get(cid, {"wr": 0.5, "gold15": 0.0, "gold15_stddev": 0.0})

        excess_wr = float(p_wr or 0) - lck["wr"]
        g15_adv   = float(p_g15 or 0) - lck["gold15"]
        stab_adv  = lck["gold15_stddev"] - float(p_g15_std or 0)

        score = (excess_wr * 50) + (g15_adv / 200 * 30) + (stab_adv / 100 * 20)

        if excess_wr > 0 or g15_adv > 0 or stab_adv > 0:
            specialists.append({
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
            })

    specialists.sort(key=lambda x: x["specialist_score"], reverse=True)

    return {
        "player": player_name,
        "position": position,
        "specialists": specialists,
    }
