"""
시나리오 I — 저격 밴 실효성 검증 (D × A 교차 분석)
"상대가 우리 선수의 주력 챔피언을 밴했을 때 실제로 우리 팀이 졌는가?"

분석 단위:
  - 챔피언 단위: 특정 챔피언이 밴된 경기/시리즈 승률 vs 밴 안 된 경기/시리즈 승률
  - 시리즈 단위: 저격 밴 1회 이상 / 0회 시리즈 승률 비교
1
반환 지표:
  - 밴 시 팀 게임 승률, 밴 안 됐을 때 팀 게임 승률, 승률 차이
  - 시리즈 단위 동일 지표
  - 밴 횟수, 샘플 신뢰 경고 (N < 10)
"""
from sqlalchemy import text
from .db import get_engine


def get_snipe_effectiveness(team_name: str,
                            season_id: str | None = None) -> dict:
    """
    팀 내 선수별 · 챔피언별 저격 밴 실효성
    반환:
    {
      "team": str,
      "players": [
        {
          "player": str,
          "position": str,
          "champions": [
            {
              "champion": str,
              "icon_url": str,
              "total_games": int,
              # 게임 단위
              "banned_games":       int,    # 상대가 early ban한 경기 수
              "banned_game_wr":     float,  # 밴된 경기 팀 게임 승률
              "normal_game_wr":     float,  # 밴 안 된 경기 팀 게임 승률
              "game_wr_delta":      float,  # normal - banned (양수 = 밴이 효과적)
              # 시리즈 단위
              "snipe_series":       int,    # 해당 챔피언 1회 이상 밴된 시리즈 수
              "total_series":       int,    # 전체 시리즈 수
              "snipe_series_wr":    float,  # 저격 밴 시리즈 팀 승률
              "normal_series_wr":   float,  # 저격 없는 시리즈 팀 승률
              "series_wr_delta":    float,  # normal - snipe (양수 = 밴이 효과적)
              "low_sample":         bool,   # banned_games < 10
            }
          ]
        }
      ],
      # 팀 전체 요약 (가장 효과적인 저격 대상 순 정렬)
      "summary": [
        {
          "player": str, "champion": str, "icon_url": str,
          "game_wr_delta": float, "series_wr_delta": float,
          "banned_games": int, "snipe_series": int,
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

        # 시즌 필터
        s_filter_game = ""
        s_filter_series = ""
        params_base: dict = {"tid": tid}
        if season_id:
            s_filter_game = """AND gt.game_id IN (
                SELECT g.game_id FROM games g
                JOIN series s ON s.series_id = g.series_id
                WHERE s.season_id = :sid
            )"""
            s_filter_series = "AND ser.season_id = :sid"
            params_base["sid"] = season_id

        # 선수 목록
        if season_id:
            roster = conn.execute(text("""
                SELECT DISTINCT p.summoner_name, pth.role
                FROM player_team_history pth
                JOIN players p ON p.player_id = pth.player_id
                WHERE pth.team_id = :tid AND pth.season_id = :sid
                ORDER BY pth.role
            """), params_base).fetchall()
        else:
            roster = conn.execute(text("""
                SELECT DISTINCT p.summoner_name, pth.role
                FROM player_team_history pth
                JOIN players p ON p.player_id = pth.player_id
                WHERE pth.team_id = :tid
                  AND pth.season_id = (
                      SELECT season_id FROM player_team_history
                      WHERE team_id = :tid ORDER BY season_id DESC LIMIT 1
                  )
                ORDER BY pth.role
            """), params_base).fetchall()

        # 팀 전체 시리즈 목록
        all_series = conn.execute(text(f"""
            SELECT DISTINCT ser.series_id, ser.winner_team_id
            FROM series ser
            JOIN games g ON g.series_id = ser.series_id
            JOIN game_teams gt ON gt.game_id = g.game_id AND gt.team_id = :tid
            WHERE 1=1 {s_filter_series}
        """), params_base).fetchall()

        total_series = len(all_series)
        all_series_ids = [r[0] for r in all_series]
        # series_id → 우리팀 승리 여부
        series_win_map = {
            r[0]: (r[1] == tid) for r in all_series
        }

        result_players = []
        summary_rows = []

        for player_name, position in roster:
            pid_row = conn.execute(
                text("SELECT player_id FROM players WHERE summoner_name = :n"),
                {"n": player_name}
            ).fetchone()
            if not pid_row:
                continue
            pid = pid_row[0]

            # 선수 주력 챔피언 top3
            top_champs = conn.execute(text(f"""
                SELECT gp.champion_id, c.icon_url, COUNT(*) AS games
                FROM game_participants gp
                JOIN game_teams gt ON gt.game_id = gp.game_id AND gt.team_id = gp.team_id
                LEFT JOIN champions c ON c.champion_id = gp.champion_id
                WHERE gp.player_id = :pid AND gt.team_id = :tid
                  AND gp.champion_id IS NOT NULL
                  {s_filter_game}
                GROUP BY gp.champion_id, c.icon_url
                ORDER BY games DESC
                LIMIT 3
            """), {**params_base, "pid": pid}).fetchall()

            champ_results = []
            for cid, icon, total_games in top_champs:
                # ── 게임 단위 ──────────────────────────────────────
                # global_order는 전체 공통 순서(1~10, 두 팀 번갈아).
                # 1페이즈 밴 = order 1~6, 2페이즈 = 7~10
                # 페이즈 무관 전체 밴으로 집계 (표본 확보 우선)
                banned_game_rows = conn.execute(text(f"""
                    SELECT gt.game_id, gt.result::int AS win
                    FROM game_teams gt
                    JOIN games g ON g.game_id = gt.game_id
                    WHERE gt.team_id = :tid
                      {s_filter_game}
                      AND EXISTS (
                          SELECT 1 FROM picks_bans pb
                          WHERE pb.game_id = gt.game_id
                            AND pb.champion_id = :cid
                            AND pb.phase = 'ban'
                            AND pb.team_id != :tid
                      )
                """), {**params_base, "cid": cid}).fetchall()

                # 해당 챔피언이 밴되지 않은 경기 전체 팀 승률
                # (픽 여부 무관 — "밴 안 됐을 때 팀이 어땠나"가 핵심)
                normal_game_rows = conn.execute(text(f"""
                    SELECT gt.game_id, gt.result::int AS win
                    FROM game_teams gt
                    JOIN games g ON g.game_id = gt.game_id
                    WHERE gt.team_id = :tid
                      {s_filter_game}
                      AND NOT EXISTS (
                          SELECT 1 FROM picks_bans pb
                          WHERE pb.game_id = gt.game_id
                            AND pb.champion_id = :cid
                            AND pb.phase = 'ban'
                            AND pb.team_id != :tid
                      )
                """), {**params_base, "cid": cid}).fetchall()

                banned_games = len(banned_game_rows)
                banned_game_wr = (
                    sum(r[1] for r in banned_game_rows) / banned_games
                    if banned_games > 0 else None
                )
                normal_games = len(normal_game_rows)
                normal_game_wr = (
                    sum(r[1] for r in normal_game_rows) / normal_games
                    if normal_games > 0 else None
                )
                game_wr_delta = (
                    round(float(normal_game_wr) - float(banned_game_wr), 3)
                    if banned_game_wr is not None and normal_game_wr is not None
                    else None
                )

                # ── 시리즈 단위 ────────────────────────────────────
                # 해당 챔피언이 1회 이상 밴(페이즈 무관)된 시리즈
                snipe_series_rows = conn.execute(text(f"""
                    SELECT DISTINCT g.series_id
                    FROM picks_bans pb
                    JOIN games g ON g.game_id = pb.game_id
                    JOIN game_teams gt ON gt.game_id = g.game_id AND gt.team_id = :tid
                    WHERE pb.champion_id = :cid
                      AND pb.phase = 'ban'
                      AND pb.team_id != :tid
                      {s_filter_game}
                """), {**params_base, "cid": cid}).fetchall()

                snipe_series_ids = {r[0] for r in snipe_series_rows}
                normal_series_ids = [
                    sid for sid in all_series_ids
                    if sid not in snipe_series_ids
                ]

                snipe_count = len(snipe_series_ids)
                snipe_wins = sum(
                    1 for sid in snipe_series_ids
                    if series_win_map.get(sid, False)
                )
                snipe_series_wr = (
                    snipe_wins / snipe_count if snipe_count > 0 else None
                )

                normal_count = len(normal_series_ids)
                normal_wins = sum(
                    1 for sid in normal_series_ids
                    if series_win_map.get(sid, False)
                )
                normal_series_wr = (
                    normal_wins / normal_count if normal_count > 0 else None
                )

                series_wr_delta = (
                    round(float(normal_series_wr) - float(snipe_series_wr), 3)
                    if snipe_series_wr is not None and normal_series_wr is not None
                    else None
                )

                row = {
                    "champion": cid,
                    "icon_url": icon,
                    "total_games": int(total_games),
                    "banned_games": banned_games,
                    "normal_games": normal_games,
                    "banned_game_wr": round(float(banned_game_wr), 3) if banned_game_wr is not None else None,
                    "normal_game_wr": round(float(normal_game_wr), 3) if normal_game_wr is not None else None,
                    "game_wr_delta": game_wr_delta,
                    "snipe_series": snipe_count,
                    "total_series": total_series,
                    "snipe_series_wr": round(float(snipe_series_wr), 3) if snipe_series_wr is not None else None,
                    "normal_series_wr": round(float(normal_series_wr), 3) if normal_series_wr is not None else None,
                    "series_wr_delta": series_wr_delta,
                    "low_sample": banned_games < 10,
                }
                champ_results.append(row)

                if game_wr_delta is not None:
                    summary_rows.append({
                        "player": player_name,
                        "champion": cid,
                        "icon_url": icon,
                        "game_wr_delta": game_wr_delta,
                        "series_wr_delta": series_wr_delta,
                        "banned_games": banned_games,
                        "snipe_series": snipe_count,
                        "low_sample": banned_games < 10,
                    })

            result_players.append({
                "player": player_name,
                "position": position,
                "champions": champ_results,
            })

    summary_rows.sort(key=lambda x: x["game_wr_delta"], reverse=True)

    return {
        "team": team_name,
        "players": result_players,
        "summary": summary_rows,
    }
