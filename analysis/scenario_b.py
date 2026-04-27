"""
시나리오 B — 진영별 챔피언 선택 변화
같은 선수가 블루/레드 진영에서 고르는 챔피언 목록 분석
"""
from sqlalchemy import text
from .db import get_engine


def get_side_champion_preference(player_name: str,
                                  season_id: str | None = None) -> dict:
    """
    선수의 블루/레드 진영별 챔피언 사용 현황
    season_id 지정 시 해당 시즌 게임만 집계
    반환:
    {
      "player": str,
      "blue_only": [...],
      "red_only": [...],
      "both": [...],
      "detail": [
        {
          "champion": str,
          "icon_url": str,
          "blue_games": int, "blue_wr": float,
          "red_games": int,  "red_wr": float,
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
        params: dict = {"pid": pid}
        if season_id:
            s_filter = """AND gp.game_id IN (
                SELECT g.game_id FROM games g
                JOIN series s ON s.series_id = g.series_id
                WHERE s.season_id = :sid
            )"""
            params["sid"] = season_id

        rows = conn.execute(text(f"""
            SELECT
                gp.champion_id,
                c.icon_url,
                gt.side,
                COUNT(*) AS games,
                AVG(gt.result::int) AS win_rate
            FROM game_participants gp
            JOIN game_teams gt
              ON gt.game_id = gp.game_id AND gt.team_id = gp.team_id
            LEFT JOIN champions c ON c.champion_id = gp.champion_id
            WHERE gp.player_id = :pid AND gp.champion_id IS NOT NULL
              {s_filter}
            GROUP BY gp.champion_id, c.icon_url, gt.side
            ORDER BY gp.champion_id, gt.side
        """), params).fetchall()

    champ_data: dict[str, dict] = {}
    for row in rows:
        cid, icon_url, side, games, wr = row
        if cid not in champ_data:
            champ_data[cid] = {"champion": cid, "icon_url": icon_url,
                               "blue_games": 0, "blue_wr": 0.0,
                               "red_games": 0, "red_wr": 0.0}
        if side == "blue":
            champ_data[cid]["blue_games"] = games
            champ_data[cid]["blue_wr"] = round(float(wr or 0), 3)
        else:
            champ_data[cid]["red_games"] = games
            champ_data[cid]["red_wr"] = round(float(wr or 0), 3)

    detail = list(champ_data.values())
    blue_only = [d["champion"] for d in detail if d["blue_games"] > 0 and d["red_games"] == 0]
    red_only  = [d["champion"] for d in detail if d["red_games"] > 0 and d["blue_games"] == 0]
    both      = [d["champion"] for d in detail if d["blue_games"] > 0 and d["red_games"] > 0]

    return {
        "player": player_name,
        "blue_only": blue_only,
        "red_only": red_only,
        "both": both,
        "detail": detail,
    }
