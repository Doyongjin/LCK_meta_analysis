"""
시나리오 F — 선수별 밴 내성 지수 (라인별 기준 분리)
챔프폭 + 주력 의존도 + 밴 시 승률 하락폭 + Gold@15 변동성을 종합해 0~100 점수 산출
"""
import numpy as np
from sqlalchemy import text
from .db import get_engine


def _get_player_raw_stats(pid: int, tid: int | None, conn,
                          season_id: str | None = None) -> dict:
    """선수의 원시 통계 수집"""
    params: dict = {"pid": pid}
    team_filter     = "AND gt.team_id = :tid" if tid else ""
    cte_team_filter = "AND gp.team_id = :tid" if tid else ""
    if tid:
        params["tid"] = tid

    s_filter = ""
    if season_id:
        s_filter = """AND gp.game_id IN (
            SELECT g.game_id FROM games g
            JOIN series s ON s.series_id = g.series_id
            WHERE s.season_id = :sid
        )"""
        params["sid"] = season_id

    champ_stats = conn.execute(text(f"""
        SELECT
            gp.champion_id,
            COUNT(*) AS games,
            AVG(gt.result::int) AS win_rate
        FROM game_participants gp
        JOIN game_teams gt ON gt.game_id = gp.game_id AND gt.team_id = gp.team_id
        WHERE gp.player_id = :pid
          AND gp.champion_id IS NOT NULL
          {team_filter}
          {s_filter}
        GROUP BY gp.champion_id
        ORDER BY games DESC
    """), params).fetchall()

    if not champ_stats:
        return {}

    total_games = sum(r[1] for r in champ_stats)
    top_champ_games = champ_stats[0][1] if champ_stats else 0

    primary_dependency = top_champ_games / total_games if total_games > 0 else 0
    champ_pool_size = sum(1 for r in champ_stats if r[1] >= 2)

    primary_champ = champ_stats[0][0] if champ_stats else None
    wr_drop = 0.0
    gold15_stddev_normal = 0.0
    gold15_stddev_banned = 0.0

    if primary_champ:
        banned_stats = conn.execute(text(f"""
            WITH player_games AS (
                SELECT gp.game_id, gp.team_id
                FROM game_participants gp
                WHERE gp.player_id = :pid {cte_team_filter}
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
                AVG(gt.result::int)   AS banned_wr,
                STDDEV(gt.gold_at_15) AS banned_gold15_stddev
            FROM game_teams gt
            JOIN player_games pg ON pg.game_id = gt.game_id AND pg.team_id = gt.team_id
            WHERE gt.game_id IN (SELECT game_id FROM banned_games)
        """), {**params, "cid": primary_champ}).fetchone()

        normal_stddev_row = conn.execute(text(f"""
            SELECT STDDEV(gt.gold_at_15)
            FROM game_participants gp
            JOIN game_teams gt ON gt.game_id = gp.game_id AND gt.team_id = gp.team_id
            WHERE gp.player_id = :pid
              AND gp.champion_id = :cid
              {team_filter}
              {s_filter}
        """), {**params, "cid": primary_champ}).scalar()

        normal_wr = float(champ_stats[0][2] or 0)
        banned_wr_val = float((banned_stats[0] if banned_stats else None) or normal_wr)
        wr_drop = max(0.0, normal_wr - banned_wr_val)

        gold15_stddev_normal = float(normal_stddev_row or 0)
        gold15_stddev_banned = float((banned_stats[1] if banned_stats else None) or gold15_stddev_normal)

    gold15_volatility_delta = max(0.0, gold15_stddev_banned - gold15_stddev_normal)

    return {
        "total_games": total_games,
        "champ_pool_size": champ_pool_size,
        "primary_dependency": primary_dependency,
        "wr_drop_when_banned": wr_drop,
        "primary_champ": primary_champ,
        "gold15_stddev_normal": gold15_stddev_normal,
        "gold15_stddev_banned": gold15_stddev_banned,
        "gold15_volatility_delta": gold15_volatility_delta,
    }


def _compute_ban_resistance(stats: dict, position_avg: dict) -> float:
    """
    밴 내성 점수 0~100 계산
    - 챔프폭 넓을수록 +          (가중치 0.25)
    - 주력 의존도 낮을수록 +      (가중치 0.28)
    - 밴 시 승률 하락폭 낮을수록 + (가중치 0.27)
    - 밴 시 Gold@15 변동성 낮을수록 + (가중치 0.20)
    """
    if not stats:
        return 50.0

    avg_pool    = position_avg.get("champ_pool_size", 5)
    avg_dep     = position_avg.get("primary_dependency", 0.35)
    avg_drop    = position_avg.get("wr_drop_when_banned", 0.1)
    avg_vdelta  = position_avg.get("gold15_volatility_delta", 300.0)

    pool_score  = min(100, (stats["champ_pool_size"] / max(avg_pool, 1)) * 50)
    dep_score   = max(0, (1 - stats["primary_dependency"] / max(avg_dep * 2, 0.01)) * 50 + 50)
    drop_score  = max(0, (1 - stats["wr_drop_when_banned"] / max(avg_drop * 2, 0.01)) * 50 + 50)
    stab_score  = max(0, (1 - stats["gold15_volatility_delta"] / max(avg_vdelta * 2, 1)) * 50 + 50)

    raw = (pool_score * 0.25 + dep_score * 0.28 + drop_score * 0.27 + stab_score * 0.20)
    return round(min(100, max(0, raw)), 1)


def get_position_averages(position: str, conn) -> dict:
    """라인별 LCK 전체 평균 통계"""
    rows = conn.execute(text("""
        SELECT pp.champion_dependency, pp.ban_resistance_score
        FROM player_profiles pp
        WHERE pp.position = :pos
    """), {"pos": position}).fetchall()

    if rows:
        avg_dep = float(np.mean([r[0] for r in rows if r[0] is not None] or [0.35]))
        return {"primary_dependency": avg_dep, "champ_pool_size": 5,
                "wr_drop_when_banned": 0.1, "gold15_volatility_delta": 300.0}

    defaults = {
        "top": {"champ_pool_size": 5, "primary_dependency": 0.35, "wr_drop_when_banned": 0.10, "gold15_volatility_delta": 350.0},
        "jng": {"champ_pool_size": 6, "primary_dependency": 0.30, "wr_drop_when_banned": 0.08, "gold15_volatility_delta": 280.0},
        "mid": {"champ_pool_size": 5, "primary_dependency": 0.38, "wr_drop_when_banned": 0.12, "gold15_volatility_delta": 320.0},
        "bot": {"champ_pool_size": 4, "primary_dependency": 0.42, "wr_drop_when_banned": 0.15, "gold15_volatility_delta": 400.0},
        "sup": {"champ_pool_size": 7, "primary_dependency": 0.28, "wr_drop_when_banned": 0.07, "gold15_volatility_delta": 220.0},
    }
    return defaults.get(position, defaults["mid"])


def get_ban_resistance(player_name: str, team_name: str | None = None,
                       season_id: str | None = None) -> dict:
    """
    선수 밴 내성 지수 반환
    season_id 지정 시 해당 시즌 게임만 집계
    반환:
    {
      "player": str,
      "position": str,
      "ban_resistance_score": float,
      "champ_pool_size": int,
      "primary_dependency": float,
      "wr_drop_when_banned": float,
      "primary_champion": str,
      "gold15_stddev_normal": float,
      "gold15_stddev_banned": float,
      "gold15_volatility_delta": float,
      "lck_avg_benchmark": dict,
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

        team_id = None
        if team_name:
            t = conn.execute(
                text("SELECT team_id FROM teams WHERE name = :n OR acronym = :n"),
                {"n": team_name}
            ).fetchone()
            if t:
                team_id = t[0]

        pos_row = conn.execute(text("""
            SELECT role FROM player_team_history
            WHERE player_id = :pid
            ORDER BY id DESC LIMIT 1
        """), {"pid": pid}).fetchone()
        position = pos_row[0] if pos_row else "mid"

        raw = _get_player_raw_stats(pid, team_id, conn, season_id)
        pos_avg = get_position_averages(position, conn)
        score = _compute_ban_resistance(raw, pos_avg)

        conn.execute(text("""
            INSERT INTO player_profiles
              (player_id, position, ban_resistance_score, champion_dependency, based_on_seasons)
            VALUES (:pid, :pos, :score, :dep, '2024_2026')
            ON CONFLICT (player_id, position, based_on_seasons)
            DO UPDATE SET ban_resistance_score = EXCLUDED.ban_resistance_score,
                          champion_dependency = EXCLUDED.champion_dependency,
                          calculated_at = NOW()
        """), {
            "pid": pid, "pos": position,
            "score": score,
            "dep": round(raw.get("primary_dependency", 0), 4),
        })
        conn.commit()

    return {
        "player": player_name,
        "position": position,
        "ban_resistance_score": score,
        "champ_pool_size": raw.get("champ_pool_size", 0),
        "primary_dependency": round(raw.get("primary_dependency", 0), 3),
        "wr_drop_when_banned": round(raw.get("wr_drop_when_banned", 0), 3),
        "gold15_stddev_normal": round(raw.get("gold15_stddev_normal", 0), 1),
        "gold15_stddev_banned": round(raw.get("gold15_stddev_banned", 0), 1),
        "gold15_volatility_delta": round(raw.get("gold15_volatility_delta", 0), 1),
        "primary_champion": raw.get("primary_champ"),
        "lck_avg_benchmark": pos_avg,
    }
