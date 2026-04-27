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

    # 선수별 밴 내성
    players_data = []
    br_scores = []
    for pname, pos in players_rows:
        br = get_ban_resistance(pname, team_name)
        score = br.get("ban_resistance_score", 50.0)
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


def get_all_team_profiles(season_id: str | None = None) -> list:
    """LCK 전 팀 프로파일 목록"""
    engine = get_engine()
    with engine.connect() as conn:
        teams = conn.execute(
            text("SELECT name FROM teams WHERE region = 'LCK' ORDER BY name")
        ).fetchall()

    return [get_team_profile(t[0], season_id) for t in teams]
