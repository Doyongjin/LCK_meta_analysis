"""
시나리오 C — 패치 적응 속도
패치 출시 후 팀별로 강챔(높은 PBI)을 얼마나 빠르게 픽에 반영하는지 측정
"""
from sqlalchemy import text
from .db import get_engine


def get_meta_adaptation_speed(season_id: str | None = None) -> list:
    """
    팀별 패치 적응 속도 지수
    - 패치 출시 후 PBI 상위 챔피언을 첫 픽까지 걸린 경기 수 평균
    반환:
    [
      {
        "team": str,
        "avg_adaptation_games": float,  # 낮을수록 빠른 적응
        "patches_analyzed": int,
      },
      ...
    ]
    """
    engine = get_engine()
    with engine.connect() as conn:
        # PBI 상위 챔피언 목록 (패치별 상위 5개)
        meta_champs = conn.execute(text("""
            SELECT cm.patch_id, cm.champion_id, cm.pbi_score,
                   pv.release_date
            FROM champion_meta cm
            JOIN patch_versions pv ON pv.patch_id = cm.patch_id
            WHERE cm.pbi_score IS NOT NULL
              AND pv.release_date IS NOT NULL
            ORDER BY cm.patch_id, cm.pbi_score DESC
        """)).fetchall()

        if not meta_champs:
            return []

        # 패치별 상위 5 챔피언만
        patch_top: dict[str, list] = {}
        for row in meta_champs:
            patch_id, champ_id, pbi, release_date = row
            if patch_id not in patch_top:
                patch_top[patch_id] = []
            if len(patch_top[patch_id]) < 5:
                patch_top[patch_id].append((champ_id, release_date))

        # 팀별 첫 채택 경기 수 계산
        where_clause = f"AND g.season_id = :sid" if season_id else ""
        team_scores: dict[str, list] = {}

        for patch_id, champ_list in patch_top.items():
            for champ_id, release_date in champ_list:
                if not release_date:
                    continue

                rows = conn.execute(text(f"""
                    SELECT gt.team_id, t.name,
                           MIN(g.date) AS first_pick_date,
                           (
                               SELECT COUNT(*) FROM games g2
                               WHERE g2.patch_id = :pid
                                 AND g2.date >= :rd
                                 AND g2.date < MIN(g.date)
                                 AND g2.series_id IN (
                                     SELECT series_id FROM series
                                     WHERE team1_id = gt.team_id
                                        OR team2_id = gt.team_id
                                 )
                           ) AS games_before_adoption
                    FROM picks_bans pb
                    JOIN games g ON g.game_id = pb.game_id
                    JOIN game_teams gt ON gt.game_id = g.game_id AND gt.team_id = pb.team_id
                    JOIN teams t ON t.team_id = gt.team_id
                    WHERE pb.champion_id = :cid
                      AND pb.phase = 'pick'
                      AND g.date >= :rd
                    GROUP BY gt.team_id, t.name
                """), {"pid": patch_id, "cid": champ_id, "rd": str(release_date)}).fetchall()

                for row in rows:
                    team_id, team_name, first_date, games_before = row
                    if team_name not in team_scores:
                        team_scores[team_name] = []
                    team_scores[team_name].append(int(games_before or 0))

        result = []
        for team_name, scores in team_scores.items():
            result.append({
                "team": team_name,
                "avg_adaptation_games": round(sum(scores) / len(scores), 2),
                "patches_analyzed": len(scores),
            })

        result.sort(key=lambda x: x["avg_adaptation_games"])
    return result


def calculate_patch_pbi(season_id: str | None = None):
    """
    picks_bans + game_teams 데이터로 champion_meta PBI 계산 후 저장
    PBI = (win_rate - avg_win_rate) × pick_rate / (1 - ban_rate)
    """
    engine = get_engine()
    with engine.connect() as conn:
        season_filter = "AND g.season_id = :sid" if season_id else ""

        # 패치별 전체 경기 수
        total_games_per_patch = conn.execute(text(f"""
            SELECT g.patch_id, COUNT(DISTINCT g.game_id) AS total
            FROM games g
            GROUP BY g.patch_id
        """)).fetchall()
        total_map = {r[0]: r[1] for r in total_games_per_patch}

        # 챔피언별 패치별 픽/밴/승 집계
        stats = conn.execute(text(f"""
            SELECT
                g.patch_id,
                pb.champion_id,
                SUM(CASE WHEN pb.phase = 'pick' THEN 1 ELSE 0 END) AS picks,
                SUM(CASE WHEN pb.phase = 'ban'  THEN 1 ELSE 0 END) AS bans,
                SUM(CASE WHEN pb.phase = 'pick' AND gt.result = true THEN 1 ELSE 0 END) AS wins
            FROM picks_bans pb
            JOIN games g ON g.game_id = pb.game_id
            JOIN game_teams gt ON gt.game_id = pb.game_id AND gt.team_id = pb.team_id
            WHERE pb.champion_id IS NOT NULL
            GROUP BY g.patch_id, pb.champion_id
        """)).fetchall()

        updated = 0
        for row in stats:
            patch_id, champ_id, picks, bans, wins = row
            total = total_map.get(patch_id, 1)
            pick_rate = picks / total if total > 0 else 0
            ban_rate  = bans  / total if total > 0 else 0
            win_rate  = wins  / picks if picks > 0 else 0
            presence  = pick_rate + ban_rate
            # 평균 승률 0.5 가정
            pbi = (win_rate - 0.5) * pick_rate / max(1 - ban_rate, 0.01)

            conn.execute(text("""
                INSERT INTO champion_meta (champion_id, patch_id, pick_rate, ban_rate, win_rate, presence_rate, pbi_score)
                VALUES (:cid, :pid, :pr, :br, :wr, :presence, :pbi)
                ON CONFLICT (champion_id, patch_id) DO UPDATE
                  SET pick_rate = EXCLUDED.pick_rate,
                      ban_rate  = EXCLUDED.ban_rate,
                      win_rate  = EXCLUDED.win_rate,
                      presence_rate = EXCLUDED.presence_rate,
                      pbi_score = EXCLUDED.pbi_score,
                      calculated_at = NOW()
            """), {
                "cid": champ_id, "pid": patch_id,
                "pr": round(pick_rate, 4), "br": round(ban_rate, 4),
                "wr": round(win_rate, 4), "presence": round(presence, 4),
                "pbi": round(pbi, 4),
            })
            updated += 1

        conn.commit()
    print(f"champion_meta PBI 계산 완료: {updated}개")
