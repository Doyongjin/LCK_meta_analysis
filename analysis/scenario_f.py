"""
시나리오 F — 선수별 밴 내성 지수 (라인별 기준 분리)
챔프폭 + 주력 의존도 + 주력 챔피언 사용 불가 시 승률 하락폭 + Gold@15 변동성을 종합해 0~100 점수 산출

피어리스 보정: "주력 챔피언 사용 불가" 정의를 확장
  - 상대팀 밴 (기존)
  - 같은 시리즈 앞 경기에서 우리 팀이 이미 픽 → 피어리스 규정상 재픽 불가 (추가)
"""
from sqlalchemy import text
from .db import get_engine


def _get_player_raw_stats(pid: int, tid: int | None, conn,
                          season_id: str | None = None) -> dict:
    """선수의 원시 통계 수집 (피어리스 보정 포함)"""
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
    champ_pool_size = len(champ_stats)  # 1경기 이상 모든 챔피언

    # 주력 챔피언: 2경기 이상 플레이한 챔피언 중 상위
    champs_2plus = [r for r in champ_stats if r[1] >= 2]
    primary_champ = champs_2plus[0][0] if champs_2plus else None
    wr_drop = 0.0
    gold15_stddev_normal = 0.0
    gold15_stddev_banned = 0.0
    banned_count = 0
    prev_used_count = 0
    blocked_count = 0

    if primary_champ:
        # 1) 상대팀이 주력 챔피언을 밴한 경기 + 2) 피어리스에서 같은 시리즈 앞 경기에 픽한 경기 통합
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
                -- 피어리스 시리즈에서 같은 시리즈 앞 경기에 우리 팀이 이미 해당 챔피언을 픽한 경우
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
                (SELECT COUNT(*) FROM banned_games)            AS banned_count,
                (SELECT COUNT(*) FROM prev_used_games)         AS prev_used_count,
                (SELECT COUNT(DISTINCT game_id) FROM blocked_games) AS blocked_count
            FROM game_teams gt
            JOIN player_games pg ON pg.game_id = gt.game_id AND pg.team_id = gt.team_id
            WHERE gt.game_id IN (SELECT game_id FROM blocked_games)
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
        if blocked_stats:
            blocked_wr_val = float(blocked_stats[0]) if blocked_stats[0] is not None else normal_wr
            wr_drop = max(0.0, normal_wr - blocked_wr_val)
            gold15_stddev_banned = float(blocked_stats[1] or 0)
            banned_count   = int(blocked_stats[2] or 0)
            prev_used_count = int(blocked_stats[3] or 0)
            blocked_count  = int(blocked_stats[4] or 0)

        gold15_stddev_normal = float(normal_stddev_row or 0)
        if gold15_stddev_banned == 0:
            gold15_stddev_banned = gold15_stddev_normal

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
        "banned_games_count": banned_count,
        "prev_used_games_count": prev_used_count,
        "blocked_games_count": blocked_count,
    }


def _compute_ban_resistance(stats: dict, position_avg: dict) -> float:
    """
    밴 내성 점수 0~100 계산
    - 챔프폭 넓을수록 +          (가중치 0.25)
    - 주력 의존도 낮을수록 +      (가중치 0.28)
    - 주력 사용 불가 시 승률 하락폭 낮을수록 + (가중치 0.27)
    - 주력 사용 불가 시 Gold@15 변동성 낮을수록 + (가중치 0.20)
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


def get_position_averages(position: str, conn, season_id: str | None = None) -> dict:
    """라인별 LCK 평균 통계 (DB에서 실시간 계산, 시즌 필터 적용)"""
    params: dict = {"pos": position}
    s_filter = ""
    if season_id:
        s_filter = """AND gp.game_id IN (
            SELECT g.game_id FROM games g
            JOIN series s ON s.series_id = g.series_id
            WHERE s.season_id = :sid
        )"""
        params["sid"] = season_id

    # 선수별 챔프풀(1경기+) 및 주력 의존도 계산
    rows = conn.execute(text(f"""
        WITH player_champ AS (
            SELECT gp.player_id, gp.champion_id, COUNT(*) AS games
            FROM game_participants gp
            WHERE gp.position = :pos
              AND gp.champion_id IS NOT NULL
              {s_filter}
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
        SELECT AVG(pool::float), AVG(top_games::float / total)
        FROM player_agg
    """), params).fetchone()

    avg_pool = float(rows[0]) if rows and rows[0] is not None else 5.0
    avg_dep  = float(rows[1]) if rows and rows[1] is not None else 0.35

    # wr_drop / gold15_volatility는 표본 부족 시 휴리스틱 유지
    drop_defaults = {"top": 0.10, "jng": 0.08, "mid": 0.12, "bot": 0.15, "sup": 0.07}
    vdelta_defaults = {"top": 350.0, "jng": 280.0, "mid": 320.0, "bot": 400.0, "sup": 220.0}

    return {
        "champ_pool_size":          avg_pool,
        "primary_dependency":       avg_dep,
        "wr_drop_when_banned":      drop_defaults.get(position, 0.10),
        "gold15_volatility_delta":  vdelta_defaults.get(position, 300.0),
    }


def get_ban_resistance(player_name: str, team_name: str | None = None,
                       season_id: str | None = None) -> dict:
    """
    선수 밴 내성 지수 반환 (피어리스 보정 적용)
    "주력 챔피언 사용 불가" = 상대 밴 OR 피어리스 앞 경기 픽
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
        pos_avg = get_position_averages(position, conn, season_id)
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
        # 피어리스 보정 상세
        "banned_games_count": raw.get("banned_games_count", 0),       # 상대 밴
        "prev_used_games_count": raw.get("prev_used_games_count", 0), # 피어리스 앞경기 픽
        "blocked_games_count": raw.get("blocked_games_count", 0),     # 합산 (사용 불가 경기 총합)
        "lck_avg_benchmark": pos_avg,
    }
