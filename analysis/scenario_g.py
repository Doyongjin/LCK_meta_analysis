"""
시나리오 G — 팀 색깔 × 승률 상관관계
F 결과를 팀 단위로 합산해 팀 색깔 분류 + 진영/패치/상대팀 조합별 승률 교차 분석
"""
import numpy as np
from sqlalchemy import text
from .db import get_engine
from .scenario_f import get_ban_resistance


TEAM_COLOR_LABELS = {
    "carry_dependent": "캐리 의존형",
    "system":          "시스템형",
    "early_aggression": "초반 압박형",
    "late_comeback":   "후반 역전형",
}


def classify_team(ban_resistance_avg: float, gold15_avg: float,
                  object_rate_avg: float, late_wr: float) -> str:
    """
    팀 색깔 분류 기준:
    - 밴 내성 낮고 특정 선수 의존 → 캐리 의존형
    - 밴 내성 높음 → 시스템형
    - 15분 골드 우위 크고 오브젝트 높음 → 초반 압박형
    - 나머지 → 후반 역전형
    """
    if ban_resistance_avg < 40:
        return "carry_dependent"
    if ban_resistance_avg >= 65:
        return "system"
    if gold15_avg > 500 and object_rate_avg > 0.55:
        return "early_aggression"
    return "late_comeback"


def get_team_profile(team_name: str, season_id: str | None = None) -> dict:
    """
    팀 색깔 프로파일
    반환:
    {
      "team": str,
      "color": str,
      "color_label": str,
      "ban_resistance_avg": float,
      "gold15_avg": float,
      "first_object_rate": float,
      "blue_win_rate": float,
      "red_win_rate": float,
      "first_pick_wr": float,
      "second_pick_wr": float,
      "players": [{"player": str, "position": str, "ban_resistance": float}]
    }
    """
    engine = get_engine()
    with engine.connect() as conn:
        team = conn.execute(
            text("SELECT team_id, name FROM teams WHERE name = :n OR acronym = :n"),
            {"n": team_name}
        ).fetchone()
        if not team:
            return {"error": f"팀 없음: {team_name}"}
        tid, tname = team

        # 시즌 필터 구성 (없으면 가장 최신 시즌)
        # season_id는 games → series 경유 (games 테이블에 season_id 없음)
        if season_id:
            season_join_gt    = "JOIN series s ON s.series_id = g.series_id"
            season_filter_gt  = "AND s.season_id = :sid"
            season_filter_pth = "AND pth.season_id = :sid"
            season_params_gt  = {"tid": tid, "sid": season_id}
            season_params_pth = {"tid": tid, "sid": season_id}
        else:
            season_join_gt    = ""
            season_filter_gt  = ""
            latest_subq = "(SELECT season_id FROM player_team_history WHERE team_id = :tid ORDER BY season_id DESC LIMIT 1)"
            season_filter_pth = f"AND pth.season_id = {latest_subq}"
            season_params_gt  = {"tid": tid}
            season_params_pth = {"tid": tid}

        # 팀 경기 통계
        game_stats = conn.execute(text(f"""
            SELECT
                AVG(CASE WHEN gt.side = 'blue' THEN gt.result::int END) AS blue_wr,
                AVG(CASE WHEN gt.side = 'red'  THEN gt.result::int END) AS red_wr,
                AVG(CASE WHEN gt.pick_order = 'first'  THEN gt.result::int END) AS first_pick_wr,
                AVG(CASE WHEN gt.pick_order = 'second' THEN gt.result::int END) AS second_pick_wr,
                AVG(gt.gold_at_15) AS gold15_avg,
                AVG(gt.first_dragon::int) AS dragon_rate,
                AVG(gt.first_herald::int) AS herald_rate,
                AVG(gt.first_tower::int) AS tower_rate
            FROM game_teams gt
            JOIN games g ON g.game_id = gt.game_id
            {season_join_gt}
            WHERE gt.team_id = :tid
              {season_filter_gt}
        """), season_params_gt).fetchone()

        if not game_stats:
            return {"error": "경기 데이터 없음"}

        blue_wr, red_wr, fp_wr, sp_wr, g15, dr, hr, tr = game_stats
        object_rate = float(np.nanmean([float(dr or 0), float(hr or 0), float(tr or 0)]))

        # 선수 목록 (지정 시즌 기준)
        players_rows = conn.execute(text(f"""
            SELECT DISTINCT p.summoner_name, pth.role
            FROM player_team_history pth
            JOIN players p ON p.player_id = pth.player_id
            WHERE pth.team_id = :tid
              {season_filter_pth}
            ORDER BY pth.role
        """), season_params_pth).fetchall()

    # 선수별 밴 내성 (연결 재사용)
    from .scenario_f import _get_player_raw_stats, _compute_ban_resistance, get_position_averages
    players_data = []
    br_scores = []
    with engine.connect() as conn2:
        for pname, pos in players_rows:
            pid_row = conn2.execute(
                text("SELECT player_id FROM players WHERE summoner_name = :n"),
                {"n": pname}
            ).fetchone()
            if not pid_row:
                continue
            raw = _get_player_raw_stats(pid_row[0], tid, conn2, season_id)
            pos_avg = get_position_averages(pos, conn2)
            score, _ = _compute_ban_resistance(raw, pos_avg)
            br_scores.append(score)
            players_data.append({"player": pname, "position": pos, "ban_resistance": score})

    br_avg = float(np.mean(br_scores)) if br_scores else 50.0
    color = classify_team(br_avg, float(g15 or 0), object_rate, float(red_wr or 0))

    return {
        "team": tname,
        "color": color,
        "color_label": TEAM_COLOR_LABELS.get(color, color),
        "ban_resistance_avg": round(br_avg, 1),
        "gold15_avg": round(float(g15 or 0), 0),
        "first_object_rate": round(object_rate, 3),
        "blue_win_rate": round(float(blue_wr or 0), 3),
        "red_win_rate": round(float(red_wr or 0), 3),
        "first_pick_wr": round(float(fp_wr or 0), 3),
        "second_pick_wr": round(float(sp_wr or 0), 3),
        "players": players_data,
    }


def get_role_priority(team_name: str, season_id: str | None = None) -> dict:
    """
    팀의 포지션별 평균 픽 순서 (1=가장 먼저, 5=가장 나중)
    반환:
    {
      "team": str,
      "roles": {
        "top": {"avg_pick_order": float, "pick_counts": {1:int, 2:int, ...}},
        "jng": {...}, "mid": {...}, "bot": {...}, "sup": {...}
      }
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

        s_filter = ""
        params: dict = {"tid": tid}
        if season_id:
            s_filter = """AND pb.game_id IN (
                SELECT g.game_id FROM games g
                JOIN series s ON s.series_id = g.series_id
                WHERE s.season_id = :sid
            )"""
            params["sid"] = season_id

        rows = conn.execute(text(f"""
            SELECT gp.position, pb.team_pick_order, COUNT(*) AS cnt
            FROM picks_bans pb
            JOIN game_participants gp
              ON gp.game_id = pb.game_id
             AND gp.team_id = pb.team_id
             AND gp.champion_id = pb.champion_id
            WHERE pb.phase = 'pick'
              AND pb.team_id = :tid
              AND pb.team_pick_order IS NOT NULL
              AND gp.position IS NOT NULL
              {s_filter}
            GROUP BY gp.position, pb.team_pick_order
            ORDER BY gp.position, pb.team_pick_order
        """), params).fetchall()

    POS_ORDER = ["top", "jng", "mid", "bot", "sup"]
    roles: dict = {p: {"pick_counts": {}, "total": 0, "weighted_sum": 0} for p in POS_ORDER}

    for pos, order, cnt in rows:
        if pos not in roles:
            continue
        roles[pos]["pick_counts"][int(order)] = int(cnt)
        roles[pos]["total"] += int(cnt)
        roles[pos]["weighted_sum"] += int(order) * int(cnt)

    result_roles = {}
    for pos in POS_ORDER:
        d = roles[pos]
        avg = round(d["weighted_sum"] / d["total"], 2) if d["total"] > 0 else None
        result_roles[pos] = {
            "avg_pick_order": avg,
            "pick_counts": d["pick_counts"],
            "total": d["total"],
        }

    return {"team": team_name, "roles": result_roles}


def get_all_team_profiles(season_id: str | None = None) -> list:
    """LCK 전 팀 프로파일 목록 — 해당 시즌에 실제 경기가 있는 팀만"""
    engine = get_engine()
    with engine.connect() as conn:
        if season_id:
            teams = conn.execute(text("""
                SELECT DISTINCT t.name FROM teams t
                JOIN game_teams gt ON gt.team_id = t.team_id
                JOIN games g ON g.game_id = gt.game_id
                JOIN series s ON s.series_id = g.series_id
                WHERE t.region = 'LCK' AND s.season_id = :sid
                ORDER BY t.name
            """), {"sid": season_id}).fetchall()
        else:
            # 전체 선택 시 가장 최신 시즌 기준 활성 팀만
            teams = conn.execute(text("""
                SELECT DISTINCT t.name FROM teams t
                JOIN game_teams gt ON gt.team_id = t.team_id
                JOIN games g ON g.game_id = gt.game_id
                JOIN series s ON s.series_id = g.series_id
                WHERE t.region = 'LCK'
                  AND s.season_id = (
                      SELECT season_id FROM series
                      ORDER BY season_id DESC LIMIT 1
                  )
                ORDER BY t.name
            """)).fetchall()

    return [get_team_profile(t[0], season_id) for t in teams]
