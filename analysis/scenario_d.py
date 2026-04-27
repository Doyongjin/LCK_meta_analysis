"""
시나리오 D — 저격 밴 패턴
특정 팀을 상대할 때 상대방이 밴 1~3순위에 우리 팀 선수 주력 챔피언을 집중하는 패턴
"""
from sqlalchemy import text
from .db import get_engine


def get_snipe_ban_matrix(team_name: str, season_id: str | None = None) -> dict:
    """
    팀 내 선수별 저격 밴 현황
    season_id 지정 시 해당 시즌 게임만 집계 (선수 목록 + 챔피언 통계 모두)
    반환:
    {
      "team": str,
      "players": [
        {
          "player": str,
          "position": str,
          "top_champions": [
            {
              "champion": str,
              "icon_url": str,
              "total_games_available": int,
              "snipe_ban_count": int,
              "snipe_ban_rate": float,
              "opponents": [{"team": str, "count": int}]
            }
          ]
        }
      ]
    }
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

        # 선수 목록 (시즌 지정 시 해당 시즌, 없으면 가장 최신 시즌)
        if season_id:
            season_filter = "AND pth.season_id = :sid"
            season_params = {"tid": tid, "sid": season_id}
        else:
            season_filter = """
                AND pth.season_id = (
                    SELECT season_id FROM player_team_history
                    WHERE team_id = :tid
                    ORDER BY season_id DESC LIMIT 1
                )"""
            season_params = {"tid": tid}

        players = conn.execute(text(f"""
            SELECT DISTINCT p.summoner_name, pth.role
            FROM player_team_history pth
            JOIN players p ON p.player_id = pth.player_id
            WHERE pth.team_id = :tid
              {season_filter}
            ORDER BY pth.role
        """), season_params).fetchall()

        # 시즌 필터용 game_id 서브쿼리
        s_gp_filter = ""
        s_game_filter = ""
        if season_id:
            s_gp_filter = """AND gp.game_id IN (
                SELECT g.game_id FROM games g
                JOIN series s ON s.series_id = g.series_id
                WHERE s.season_id = :sid
            )"""
            s_game_filter = """AND g.game_id IN (
                SELECT g2.game_id FROM games g2
                JOIN series s2 ON s2.series_id = g2.series_id
                WHERE s2.season_id = :sid
            )"""

        result_players = []
        for player_name, position in players:
            player = conn.execute(
                text("SELECT player_id FROM players WHERE summoner_name = :n"),
                {"n": player_name}
            ).fetchone()
            if not player:
                continue
            pid = player[0]

            base = {"pid": pid, "tid": tid}
            if season_id:
                base["sid"] = season_id

            # 선수 주력 챔피언 top3 (해당 시즌 기준)
            top_champs = conn.execute(text(f"""
                SELECT gp.champion_id, COUNT(*) AS cnt
                FROM game_participants gp
                JOIN game_teams gt ON gt.game_id = gp.game_id AND gt.team_id = gp.team_id
                WHERE gp.player_id = :pid AND gp.champion_id IS NOT NULL
                  AND gt.team_id = :tid
                  {s_gp_filter}
                GROUP BY gp.champion_id
                ORDER BY cnt DESC
                LIMIT 3
            """), base).fetchall()

            # 팀 총 경기 수 (해당 시즌)
            total_available = conn.execute(text(f"""
                SELECT COUNT(DISTINCT g.game_id)
                FROM games g
                JOIN game_teams gt ON gt.game_id = g.game_id AND gt.team_id = :tid
                {s_game_filter}
            """), base).scalar() or 0

            champ_results = []
            for champ_row in top_champs:
                champ_id = champ_row[0]

                snipe_params = {**base, "cid": champ_id}

                # 시즌 필터를 our_games CTE에 적용
                season_our_games_filter = ""
                if season_id:
                    season_our_games_filter = "AND g.game_id IN (SELECT g2.game_id FROM games g2 JOIN series s2 ON s2.series_id = g2.series_id WHERE s2.season_id = :sid)"

                snipe_data = conn.execute(text(f"""
                    WITH our_games AS (
                        SELECT DISTINCT g.game_id, ser.team1_id, ser.team2_id,
                               gt.team_id AS our_team_id
                        FROM games g
                        JOIN series ser ON ser.series_id = g.series_id
                        JOIN game_teams gt ON gt.game_id = g.game_id AND gt.team_id = :tid
                        {season_our_games_filter}
                    ),
                    opponent_early_bans AS (
                        SELECT pb.game_id, pb.champion_id, pb.team_id AS opp_team_id,
                               pb.global_order
                        FROM picks_bans pb
                        JOIN our_games og ON og.game_id = pb.game_id
                        WHERE pb.team_id != :tid
                          AND pb.phase = 'ban'
                          AND pb.global_order <= 6
                    )
                    SELECT
                        oeb.opp_team_id,
                        t.name AS opp_team,
                        COUNT(*) AS snipe_count,
                        (SELECT COUNT(DISTINCT game_id) FROM our_games) AS total_games
                    FROM opponent_early_bans oeb
                    JOIN teams t ON t.team_id = oeb.opp_team_id
                    WHERE oeb.champion_id = :cid
                    GROUP BY oeb.opp_team_id, t.name
                    ORDER BY snipe_count DESC
                """), snipe_params).fetchall()

                icon = conn.execute(
                    text("SELECT icon_url FROM champions WHERE champion_id = :cid"),
                    {"cid": champ_id}
                ).scalar()

                snipe_total = sum(r[2] for r in snipe_data)
                opponents = [{"team": r[1], "count": r[2]} for r in snipe_data]

                champ_results.append({
                    "champion": champ_id,
                    "icon_url": icon,
                    "total_games_available": int(total_available),
                    "snipe_ban_count": snipe_total,
                    "snipe_ban_rate": round(snipe_total / total_available, 3) if total_available > 0 else 0.0,
                    "opponents": opponents,
                })

            result_players.append({
                "player": player_name,
                "position": position,
                "top_champions": champ_results,
            })

    return {"team": team_name, "players": result_players}
