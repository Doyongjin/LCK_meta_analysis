"""
시나리오 A — 핵심 챔피언 밴 시 승률 영향
선수 주력 챔피언이 밴됐을 때 팀 승률과 15분 지표 변화 측정
"""
from sqlalchemy import text
from .db import get_engine


def get_ban_impact(player_name: str, top_n: int = 3,
                   season_id: str | None = None) -> dict:
    """
    선수의 주력 챔피언 밴 시 vs 밴 안 됐을 때 승률/지표 비교
    season_id 지정 시 해당 시즌 게임만 집계
    반환:
    {
      "player": str,
      "champions": [
        {
          "champion": str,
          "icon_url": str,
          "total_games": int,
          "banned_games": int,
          "win_rate_normal": float,
          "win_rate_banned": float,
          "gold15_normal": float,
          "gold15_banned": float,
        }
      ]
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

        s_filter = ""
        base_params: dict = {"pid": pid}
        if season_id:
            s_filter = """AND gp.game_id IN (
                SELECT g.game_id FROM games g
                JOIN series s ON s.series_id = g.series_id
                WHERE s.season_id = :sid
            )"""
            base_params["sid"] = season_id

        top_champs = conn.execute(text(f"""
            SELECT gp.champion_id, COUNT(*) AS cnt
            FROM game_participants gp
            WHERE gp.player_id = :pid AND gp.champion_id IS NOT NULL
              {s_filter}
            GROUP BY gp.champion_id
            ORDER BY cnt DESC
            LIMIT :n
        """), {**base_params, "n": top_n}).fetchall()

        results = []
        for champ_row in top_champs:
            champ_id = champ_row[0]

            stats = conn.execute(text(f"""
                WITH player_games AS (
                    SELECT gp.game_id, gp.team_id, gp.champion_id,
                           gt.result,
                           gt.gold_at_15
                    FROM game_participants gp
                    JOIN game_teams gt
                      ON gt.game_id = gp.game_id AND gt.team_id = gp.team_id
                    WHERE gp.player_id = :pid
                      {s_filter}
                ),
                banned_games AS (
                    SELECT pb.game_id
                    FROM picks_bans pb
                    JOIN player_games pg ON pb.game_id = pg.game_id
                    WHERE pb.champion_id = :cid
                      AND pb.phase = 'ban'
                      AND pb.team_id != pg.team_id
                )
                SELECT
                    pg.champion_id,
                    c.icon_url,
                    COUNT(*) AS total,
                    SUM(CASE WHEN pg.game_id IN (SELECT game_id FROM banned_games) THEN 1 ELSE 0 END) AS banned_cnt,
                    AVG(CASE WHEN pg.champion_id = :cid
                              AND pg.game_id NOT IN (SELECT game_id FROM banned_games)
                              THEN pg.result::int END) AS wr_normal,
                    AVG(CASE WHEN pg.game_id IN (SELECT game_id FROM banned_games)
                              THEN pg.result::int END) AS wr_banned,
                    AVG(CASE WHEN pg.champion_id = :cid
                              AND pg.game_id NOT IN (SELECT game_id FROM banned_games)
                              THEN pg.gold_at_15 END) AS g15_normal,
                    AVG(CASE WHEN pg.game_id IN (SELECT game_id FROM banned_games)
                              THEN pg.gold_at_15 END) AS g15_banned
                FROM player_games pg
                LEFT JOIN champions c ON c.champion_id = :cid
                GROUP BY pg.champion_id, c.icon_url
            """), {**base_params, "cid": champ_id}).fetchone()

            if not stats:
                continue

            results.append({
                "champion": champ_id,
                "icon_url": stats[1],
                "total_games": stats[2],
                "banned_games": stats[3],
                "win_rate_normal": round(float(stats[4] or 0), 3),
                "win_rate_banned": round(float(stats[5] or 0), 3),
                "gold15_normal": round(float(stats[6] or 0), 1),
                "gold15_banned": round(float(stats[7] or 0), 1),
            })

    return {"player": player_name, "champions": results}
