"""
시나리오 D — 저격 밴 패턴
특정 팀을 상대할 때 상대방이 밴 1~3순위에 우리 팀 선수 주력 챔피언을 집중하는 패턴
피어리스 드래프트의 경우 게임 번호별(1/2/3경기) 밴 패턴을 따로 집계
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
              "total_games_available": int,        # 전체 (피어리스+표준)
              "snipe_ban_count": int,               # 전체 저격 밴 횟수
              "snipe_ban_rate": float,              # 전체 저격 밴율
              "opponents": [{"team": str, "count": int}],
              "by_game": {                          # 피어리스만 게임 번호별 분리
                "1": {"count": int, "available": int, "rate": float},
                "2": {"count": int, "available": int, "rate": float},
                "3": {"count": int, "available": int, "rate": float},
                ...
              },
              "fearless_games_available": int       # 피어리스 시리즈 총 경기 수
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

        # 우리 팀의 게임 번호별 경기 수 (피어리스만)
        team_params: dict = {"tid": tid}
        if season_id:
            team_params["sid"] = season_id

        fearless_games_by_num = conn.execute(text(f"""
            SELECT g.game_number, COUNT(DISTINCT g.game_id) AS cnt
            FROM games g
            JOIN series ser ON ser.series_id = g.series_id
            JOIN game_teams gt ON gt.game_id = g.game_id AND gt.team_id = :tid
            WHERE ser.draft_type = 'fearless'
            {s_game_filter}
            GROUP BY g.game_number
            ORDER BY g.game_number
        """), team_params).fetchall()

        fearless_available_by_num = {int(r[0]): int(r[1]) for r in fearless_games_by_num}
        fearless_total = sum(fearless_available_by_num.values())

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

            # 팀 총 경기 수 (전체, 해당 시즌)
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
                    season_our_games_filter = """AND g.game_id IN (
                        SELECT g2.game_id FROM games g2
                        JOIN series s2 ON s2.series_id = g2.series_id
                        WHERE s2.season_id = :sid
                    )"""

                # 게임 번호 + 드래프트 타입까지 함께 가져옴
                snipe_rows = conn.execute(text(f"""
                    WITH our_games AS (
                        SELECT DISTINCT g.game_id, g.game_number, ser.draft_type,
                               gt.team_id AS our_team_id
                        FROM games g
                        JOIN series ser ON ser.series_id = g.series_id
                        JOIN game_teams gt ON gt.game_id = g.game_id AND gt.team_id = :tid
                        {season_our_games_filter}
                    )
                    SELECT
                        pb.team_id        AS opp_team_id,
                        t.name            AS opp_team,
                        og.game_number,
                        og.draft_type
                    FROM picks_bans pb
                    JOIN our_games og  ON og.game_id = pb.game_id
                    JOIN teams t       ON t.team_id = pb.team_id
                    WHERE pb.team_id != :tid
                      AND pb.phase = 'ban'
                      AND pb.global_order <= 6
                      AND pb.champion_id = :cid
                """), snipe_params).fetchall()

                # 파이썬에서 집계
                opp_count: dict = {}
                game_num_count: dict = {}  # 피어리스만
                snipe_total = 0

                for row in snipe_rows:
                    opp_id, opp_name, game_num, draft_type = row
                    snipe_total += 1
                    opp_count[opp_name] = opp_count.get(opp_name, 0) + 1
                    if draft_type == "fearless" and game_num is not None:
                        gn = int(game_num)
                        game_num_count[gn] = game_num_count.get(gn, 0) + 1

                opponents = [
                    {"team": name, "count": cnt}
                    for name, cnt in sorted(opp_count.items(), key=lambda x: -x[1])
                ]

                # 게임 번호별 breakdown (피어리스 한정)
                by_game = {}
                for gn, available in fearless_available_by_num.items():
                    cnt = game_num_count.get(gn, 0)
                    by_game[str(gn)] = {
                        "count": cnt,
                        "available": available,
                        "rate": round(cnt / available, 3) if available > 0 else 0.0,
                    }

                icon = conn.execute(
                    text("SELECT icon_url FROM champions WHERE champion_id = :cid"),
                    {"cid": champ_id}
                ).scalar()

                champ_results.append({
                    "champion": champ_id,
                    "icon_url": icon,
                    "total_games_available": int(total_available),
                    "snipe_ban_count": snipe_total,
                    "snipe_ban_rate": round(snipe_total / total_available, 3) if total_available > 0 else 0.0,
                    "opponents": opponents,
                    "by_game": by_game,
                    "fearless_games_available": fearless_total,
                })

            result_players.append({
                "player": player_name,
                "position": position,
                "top_champions": champ_results,
            })

    return {"team": team_name, "players": result_players}
