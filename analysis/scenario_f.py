"""
시나리오 F — 선수별 밴 내성 지수 (라인별 기준 분리)
챔프폭 + 주력 의존도 + 주력 챔피언 사용 불가 시 승률 하락폭 + Gold@15 변동성을 종합해 0~100 점수 산출

가중치 객관화:
  - 4개 지표 모두 LCK 실제 분포(percentile) 기반으로 점수화
  - wr_drop / gold15_volatility 기준값도 DB에서 실제 계산 (하드코딩 제거)

주력 챔피언 확장:
  - 1개 → top3 챔피언 기준 (하나라도 막혔을 때 평균 영향 측정)

피어리스 보정: "주력 챔피언 사용 불가" 정의를 확장
  - 상대팀 밴 (기존)
  - 같은 시리즈 앞 경기에서 우리 팀이 이미 픽 → 피어리스 규정상 재픽 불가 (추가)
"""
from sqlalchemy import text
from .db import get_engine


def _get_player_raw_stats(pid: int, tid: int | None, conn,
                          season_id: str | None = None) -> dict:
    """선수의 원시 통계 수집 (피어리스 보정 + top3 주력 챔피언 기준)"""
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
    champ_pool_size = len(champ_stats)

    # 주력 챔피언: 2경기 이상 상위 3개
    champs_2plus = [r for r in champ_stats if r[1] >= 2]
    primary_champs = [r[0] for r in champs_2plus[:3]]
    primary_champ = primary_champs[0] if primary_champs else None

    wr_drops = []
    gold15_stddev_normals = []
    gold15_stddev_banneds = []
    banned_count = 0
    prev_used_count = 0
    blocked_count = 0

    for cid in primary_champs:
        blocked_stats = conn.execute(text(f"""
            WITH player_games AS (
                SELECT gp.game_id, gp.team_id, g.game_number,
                       ser.series_id, ser.draft_type
                FROM game_participants gp
                JOIN games g   ON g.game_id = gp.game_id
                JOIN series ser ON ser.series_id = g.series_id
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
            ),
            prev_used_games AS (
                SELECT DISTINCT pg.game_id
                FROM player_games pg
                WHERE pg.draft_type = 'fearless'
                  AND EXISTS (
                      SELECT 1 FROM picks_bans pb2
                      JOIN games g2 ON g2.game_id = pb2.game_id
                      WHERE g2.series_id = pg.series_id
                        AND g2.game_number < pg.game_number
                        AND pb2.champion_id = :cid
                        AND pb2.phase = 'pick'
                        AND pb2.team_id = pg.team_id
                  )
            ),
            blocked_games AS (
                SELECT game_id FROM banned_games
                UNION
                SELECT game_id FROM prev_used_games
            )
            SELECT
                AVG(gt.result::int)   AS blocked_wr,
                STDDEV(gt.gold_at_15) AS blocked_gold15_stddev,
                (SELECT COUNT(*) FROM banned_games)                    AS banned_count,
                (SELECT COUNT(*) FROM prev_used_games)                 AS prev_used_count,
                (SELECT COUNT(DISTINCT game_id) FROM blocked_games)    AS blocked_count
            FROM game_teams gt
            JOIN player_games pg ON pg.game_id = gt.game_id AND pg.team_id = gt.team_id
            WHERE gt.game_id IN (SELECT game_id FROM blocked_games)
        """), {**params, "cid": cid}).fetchone()

        normal_stddev_row = conn.execute(text(f"""
            SELECT STDDEV(gt.gold_at_15)
            FROM game_participants gp
            JOIN game_teams gt ON gt.game_id = gp.game_id AND gt.team_id = gp.team_id
            WHERE gp.player_id = :pid
              AND gp.champion_id = :cid
              {team_filter}
              {s_filter}
        """), {**params, "cid": cid}).scalar()

        normal_wr = float(next(r[2] for r in champ_stats if r[0] == cid) or 0)
        normal_std = float(normal_stddev_row or 0)

        if blocked_stats and blocked_stats[4] and int(blocked_stats[4]) > 0:
            blocked_wr_val = float(blocked_stats[0]) if blocked_stats[0] is not None else normal_wr
            wr_drops.append(max(0.0, normal_wr - blocked_wr_val))
            blocked_std = float(blocked_stats[1] or normal_std)
            gold15_stddev_banneds.append(blocked_std)
            if cid == primary_champ:
                banned_count    = int(blocked_stats[2] or 0)
                prev_used_count = int(blocked_stats[3] or 0)
                blocked_count   = int(blocked_stats[4] or 0)
        else:
            wr_drops.append(0.0)
            gold15_stddev_banneds.append(normal_std)

        gold15_stddev_normals.append(normal_std)

    wr_drop = sum(wr_drops) / len(wr_drops) if wr_drops else 0.0
    gold15_stddev_normal = gold15_stddev_normals[0] if gold15_stddev_normals else 0.0
    gold15_stddev_banned = gold15_stddev_banneds[0] if gold15_stddev_banneds else 0.0
    gold15_volatility_delta = max(0.0, gold15_stddev_banned - gold15_stddev_normal)

    return {
        "total_games": total_games,
        "champ_pool_size": champ_pool_size,
        "primary_dependency": primary_dependency,
        "wr_drop_when_banned": wr_drop,
        "primary_champ": primary_champ,
        "primary_champs_top3": primary_champs,
        "gold15_stddev_normal": gold15_stddev_normal,
        "gold15_stddev_banned": gold15_stddev_banned,
        "gold15_volatility_delta": gold15_volatility_delta,
        "banned_games_count": banned_count,
        "prev_used_games_count": prev_used_count,
        "blocked_games_count": blocked_count,
    }


def get_position_averages(position: str, conn) -> dict:
    """
    라인별 LCK 분포 통계 (percentile 기준)
    분포 기준은 항상 전체 데이터 사용 — 짧은 시즌에서도 안정적인 기준값 유지
    """
    params: dict = {"pos": position}

    # 챔프풀 + 주력 의존도 (전체 데이터 기준)
    pool_rows = conn.execute(text("""
        WITH player_champ AS (
            SELECT gp.player_id, gp.champion_id, COUNT(*) AS games
            FROM game_participants gp
            WHERE gp.position = :pos
              AND gp.champion_id IS NOT NULL
            GROUP BY gp.player_id, gp.champion_id
        ),
        player_agg AS (
            SELECT player_id,
                   COUNT(*) AS pool,
                   SUM(games) AS total,
                   MAX(games) AS top_games
            FROM player_champ
            GROUP BY player_id
            HAVING SUM(games) >= 5
        )
        SELECT
            AVG(pool::float)                  AS avg_pool,
            STDDEV(pool::float)               AS std_pool,
            AVG(top_games::float / total)     AS avg_dep,
            STDDEV(top_games::float / total)  AS std_dep,
            PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY pool::float) AS p90_pool,
            PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY pool::float) AS p10_pool,
            PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY top_games::float / total) AS p10_dep,
            PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY top_games::float / total) AS p90_dep
        FROM player_agg
    """), params).fetchone()

    avg_pool  = float(pool_rows[0]) if pool_rows and pool_rows[0] else 5.0
    p90_pool  = float(pool_rows[4]) if pool_rows and pool_rows[4] else avg_pool * 1.5
    p10_pool  = float(pool_rows[5]) if pool_rows and pool_rows[5] else 1.0
    avg_dep   = float(pool_rows[2]) if pool_rows and pool_rows[2] else 0.35
    p10_dep   = float(pool_rows[6]) if pool_rows and pool_rows[6] else 0.15
    p90_dep   = float(pool_rows[7]) if pool_rows and pool_rows[7] else 0.60

    # wr_drop / gold15_volatility: 선수별 실제 분포 (전체 데이터 기준)
    drop_rows = conn.execute(text("""
        WITH pos_players AS (
            SELECT DISTINCT gp.player_id
            FROM game_participants gp
            WHERE gp.position = :pos
              AND gp.champion_id IS NOT NULL
            GROUP BY gp.player_id
            HAVING COUNT(*) >= 5
        ),
        top_champs AS (
            SELECT gp.player_id, gp.champion_id, COUNT(*) AS games,
                   AVG(gt.result::int) AS normal_wr,
                   STDDEV(gt.gold_at_15) AS normal_std,
                   ROW_NUMBER() OVER (PARTITION BY gp.player_id ORDER BY COUNT(*) DESC) AS rn
            FROM game_participants gp
            JOIN game_teams gt ON gt.game_id = gp.game_id AND gt.team_id = gp.team_id
            JOIN pos_players pp ON pp.player_id = gp.player_id
            WHERE gp.champion_id IS NOT NULL
            GROUP BY gp.player_id, gp.champion_id
            HAVING COUNT(*) >= 2
        ),
        blocked AS (
            SELECT tc.player_id,
                   AVG(gt.result::int) AS blocked_wr,
                   STDDEV(gt.gold_at_15) AS blocked_std,
                   COUNT(*) AS cnt
            FROM top_champs tc
            JOIN picks_bans pb ON pb.champion_id = tc.champion_id
              AND pb.phase = 'ban'
            JOIN game_participants gp2 ON gp2.game_id = pb.game_id
              AND gp2.player_id = tc.player_id
            JOIN game_teams gt ON gt.game_id = pb.game_id AND gt.team_id = gp2.team_id
            WHERE tc.rn = 1
              AND pb.team_id != gp2.team_id
            GROUP BY tc.player_id
            HAVING COUNT(*) >= 1
        ),
        player_drops AS (
            SELECT tc.player_id,
                   GREATEST(0, tc.normal_wr - COALESCE(b.blocked_wr, tc.normal_wr)) AS wr_drop,
                   GREATEST(0, COALESCE(b.blocked_std, tc.normal_std) - tc.normal_std) AS vdelta
            FROM top_champs tc
            LEFT JOIN blocked b ON b.player_id = tc.player_id
            WHERE tc.rn = 1
        )
        SELECT
            AVG(wr_drop)                                                    AS avg_drop,
            PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY wr_drop)           AS p90_drop,
            PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY wr_drop)           AS p10_drop,
            AVG(vdelta)                                                     AS avg_vdelta,
            PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY vdelta)            AS p90_vdelta,
            PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY vdelta)            AS p10_vdelta
        FROM player_drops
    """), params).fetchone()

    avg_drop   = float(drop_rows[0]) if drop_rows and drop_rows[0] else 0.10
    p90_drop   = float(drop_rows[1]) if drop_rows and drop_rows[1] else avg_drop * 2
    p10_drop   = float(drop_rows[2]) if drop_rows and drop_rows[2] else 0.0
    avg_vdelta = float(drop_rows[3]) if drop_rows and drop_rows[3] else 300.0
    p90_vdelta = float(drop_rows[4]) if drop_rows and drop_rows[4] else avg_vdelta * 2
    p10_vdelta = float(drop_rows[5]) if drop_rows and drop_rows[5] else 0.0

    return {
        "champ_pool_size":         avg_pool,
        "p10_pool": p10_pool, "p90_pool": p90_pool,
        "primary_dependency":      avg_dep,
        "p10_dep":  p10_dep,  "p90_dep":  p90_dep,
        "wr_drop_when_banned":     avg_drop,
        "p10_drop": p10_drop, "p90_drop": p90_drop,
        "gold15_volatility_delta": avg_vdelta,
        "p10_vdelta": p10_vdelta, "p90_vdelta": p90_vdelta,
    }


def _percentile_score(val: float, p10_best: float, p90_worst: float) -> float:
    """
    실제 LCK 분포 기반 0~100 점수화
    p10_best: 분포 상위 10% (좋은 방향)
    p90_worst: 분포 하위 10% (나쁜 방향)
    """
    if p90_worst == p10_best:
        return 50.0
    score = (p90_worst - val) / (p90_worst - p10_best) * 100
    return round(max(0.0, min(100.0, score)), 1)


def _compute_ban_resistance(stats: dict, position_avg: dict) -> tuple[float, dict]:
    """
    밴 내성 점수 0~100 계산 (LCK 분포 percentile 기반, 동등 가중치 25%씩)
    반환: (종합점수, 지표별 점수 dict)
    """
    if not stats:
        return 50.0, {}

    # 각 지표를 LCK 분포 기반으로 0~100 점수화
    # 챔프풀: 높을수록 좋음 → p90이 best, p10이 worst
    pool_score = _percentile_score(
        stats["champ_pool_size"],
        p10_best=position_avg.get("p90_pool", 10.0),
        p90_worst=position_avg.get("p10_pool", 1.0),
    )
    # 주력 의존도: 낮을수록 좋음 → p10이 best, p90이 worst
    dep_score = _percentile_score(
        stats["primary_dependency"],
        p10_best=position_avg.get("p10_dep", 0.15),
        p90_worst=position_avg.get("p90_dep", 0.60),
    )
    # WR 하락폭: 낮을수록 좋음 → p10이 best, p90이 worst
    drop_score = _percentile_score(
        stats["wr_drop_when_banned"],
        p10_best=position_avg.get("p10_drop", 0.0),
        p90_worst=position_avg.get("p90_drop", 0.30),
    )
    # Gold 변동성: 낮을수록 좋음 → p10이 best, p90이 worst
    stab_score = _percentile_score(
        stats["gold15_volatility_delta"],
        p10_best=position_avg.get("p10_vdelta", 0.0),
        p90_worst=position_avg.get("p90_vdelta", 600.0),
    )

    raw = (pool_score + dep_score + drop_score + stab_score) / 4
    score = round(min(100.0, max(0.0, raw)), 1)

    breakdown = {
        "pool_score":  pool_score,
        "dep_score":   dep_score,
        "drop_score":  drop_score,
        "stab_score":  stab_score,
    }
    return score, breakdown


def get_ban_resistance(player_name: str, team_name: str | None = None,
                       season_id: str | None = None) -> dict:
    """
    선수 밴 내성 지수 반환 (피어리스 보정 + top3 주력 챔피언 + percentile 기반 점수)
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
        score, breakdown = _compute_ban_resistance(raw, pos_avg)

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
        "score_breakdown": breakdown,
        "champ_pool_size": raw.get("champ_pool_size", 0),
        "primary_dependency": round(raw.get("primary_dependency", 0), 3),
        "wr_drop_when_banned": round(raw.get("wr_drop_when_banned", 0), 3),
        "gold15_stddev_normal": round(raw.get("gold15_stddev_normal", 0), 1),
        "gold15_stddev_banned": round(raw.get("gold15_stddev_banned", 0), 1),
        "gold15_volatility_delta": round(raw.get("gold15_volatility_delta", 0), 1),
        "primary_champion": raw.get("primary_champ"),
        "primary_champions_top3": raw.get("primary_champs_top3", []),
        "banned_games_count": raw.get("banned_games_count", 0),
        "prev_used_games_count": raw.get("prev_used_games_count", 0),
        "blocked_games_count": raw.get("blocked_games_count", 0),
        "lck_avg_benchmark": pos_avg,
    }
