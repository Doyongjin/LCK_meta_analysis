"""
시나리오 J — 챔피언 메타 분석
시즌/패치 기준 챔피언별 픽률·승률·밴율 + 세트별 분리 + 선수/팀 통계
"""
from sqlalchemy import text
from .db import get_engine


def get_champion_meta(season_id: str | None = None,
                      patch_id: str | None = None,
                      position: str | None = None) -> dict:
    """
    반환:
    {
      "total_games": int,
      "champions": [
        {
          "champion": str,
          "icon_url": str,
          "position": str | None,      # 포지션 필터 시 해당 포지션, 전체 시 최다 포지션
          "picks": int,
          "pick_rate": float,
          "win_rate": float | None,
          "bans": int,
          "ban_rate": float,
          "presence_rate": float,
          "by_game": {
            "1": {"picks": int, "pick_rate": float, "win_rate": float|None,
                  "bans": int, "ban_rate": float},
            ...
          },
          "top_pickers":  [{"player": str, "team": str, "picks": int, "win_rate": float}],
          "top_winners":  [{"player": str, "team": str, "picks": int, "win_rate": float}],
          "top_banners":  [{"team": str, "bans": int}],
        }
      ]
    }
    """
    engine = get_engine()

    # 게임 ID 서브쿼리 조건 구성
    conds = []
    params: dict = {}
    if season_id:
        conds.append("s.season_id = :sid")
        params["sid"] = season_id
    if patch_id:
        conds.append("g.patch_id = :patch_id")
        params["patch_id"] = patch_id
    where_clause = ("WHERE " + " AND ".join(conds)) if conds else ""
    game_ids_subq = f"""
        SELECT g.game_id FROM games g
        JOIN series s ON s.series_id = g.series_id
        {where_clause}
    """

    pos_filter = "AND gp.position = :pos" if position else ""
    if position:
        params["pos"] = position

    with engine.connect() as conn:
        # ── 총 게임 수 ─────────────────────────────────
        total_games = conn.execute(
            text(f"SELECT COUNT(*) FROM ({game_ids_subq}) _t"),
            params
        ).scalar() or 1

        # ── 세트별 총 게임 수 ──────────────────────────
        game_num_totals = {
            int(r[0]): int(r[1])
            for r in conn.execute(text(f"""
                SELECT g.game_number, COUNT(DISTINCT g.game_id)
                FROM games g
                WHERE g.game_id IN ({game_ids_subq})
                GROUP BY g.game_number
            """), params).fetchall()
        }

        # ── 픽 통계 (전체) ─────────────────────────────
        pick_rows = conn.execute(text(f"""
            SELECT
                gp.champion_id,
                c.icon_url,
                gp.position,
                COUNT(*) AS picks,
                SUM(gt.result::int) AS wins
            FROM game_participants gp
            JOIN game_teams gt ON gt.game_id = gp.game_id AND gt.team_id = gp.team_id
            LEFT JOIN champions c ON c.champion_id = gp.champion_id
            WHERE gp.champion_id IS NOT NULL
              AND gp.game_id IN ({game_ids_subq})
              {pos_filter}
            GROUP BY gp.champion_id, c.icon_url, gp.position
        """), params).fetchall()

        # ── 밴 통계 (전체) ─────────────────────────────
        ban_rows = conn.execute(text(f"""
            SELECT pb.champion_id, COUNT(*) AS bans
            FROM picks_bans pb
            WHERE pb.phase = 'ban'
              AND pb.game_id IN ({game_ids_subq})
            GROUP BY pb.champion_id
        """), params).fetchall()

        # ── 세트별 픽 통계 ─────────────────────────────
        game_pick_rows = conn.execute(text(f"""
            SELECT gp.champion_id, g.game_number,
                   COUNT(*) AS picks, SUM(gt.result::int) AS wins
            FROM game_participants gp
            JOIN games g ON g.game_id = gp.game_id
            JOIN game_teams gt ON gt.game_id = gp.game_id AND gt.team_id = gp.team_id
            WHERE gp.champion_id IS NOT NULL
              AND gp.game_id IN ({game_ids_subq})
              {pos_filter}
            GROUP BY gp.champion_id, g.game_number
        """), params).fetchall()

        # ── 세트별 밴 통계 ─────────────────────────────
        game_ban_rows = conn.execute(text(f"""
            SELECT pb.champion_id, g.game_number, COUNT(*) AS bans
            FROM picks_bans pb
            JOIN games g ON g.game_id = pb.game_id
            WHERE pb.phase = 'ban'
              AND pb.game_id IN ({game_ids_subq})
            GROUP BY pb.champion_id, g.game_number
        """), params).fetchall()

        # ── 선수별 픽 통계 ─────────────────────────────
        player_rows = conn.execute(text(f"""
            SELECT gp.champion_id, p.summoner_name, t.name,
                   COUNT(*) AS picks, SUM(gt.result::int) AS wins
            FROM game_participants gp
            JOIN players p ON p.player_id = gp.player_id
            JOIN game_teams gt ON gt.game_id = gp.game_id AND gt.team_id = gp.team_id
            JOIN teams t ON t.team_id = gp.team_id
            WHERE gp.champion_id IS NOT NULL
              AND gp.game_id IN ({game_ids_subq})
              {pos_filter}
            GROUP BY gp.champion_id, p.summoner_name, t.name
        """), params).fetchall()

        # ── 팀별 밴 통계 ───────────────────────────────
        team_ban_rows = conn.execute(text(f"""
            SELECT pb.champion_id, t.name, COUNT(*) AS bans
            FROM picks_bans pb
            JOIN teams t ON t.team_id = pb.team_id
            WHERE pb.phase = 'ban'
              AND pb.game_id IN ({game_ids_subq})
            GROUP BY pb.champion_id, t.name
        """), params).fetchall()

        # ── 밴 전용 챔피언 아이콘 ──────────────────────
        icon_map = {
            r[0]: r[1]
            for r in conn.execute(text(
                "SELECT champion_id, icon_url FROM champions"
            )).fetchall()
        }

    # ── Python 집계 ────────────────────────────────────
    ban_map: dict[str, int] = {}
    for r in ban_rows:
        ban_map[r[0]] = int(r[1])

    game_pick_map: dict[str, dict] = {}
    for r in game_pick_rows:
        cid, gn, picks, wins = r[0], int(r[1]), int(r[2]), int(r[3] or 0)
        game_pick_map.setdefault(cid, {})[gn] = {"picks": picks, "wins": wins}

    game_ban_map: dict[str, dict] = {}
    for r in game_ban_rows:
        cid, gn, bans = r[0], int(r[1]), int(r[2])
        game_ban_map.setdefault(cid, {})[gn] = bans

    player_map: dict[str, list] = {}
    for r in player_rows:
        cid, pname, tname, picks, wins = r[0], r[1], r[2], int(r[3]), int(r[4] or 0)
        player_map.setdefault(cid, []).append({
            "player": pname, "team": tname, "picks": picks, "wins": wins,
        })

    team_ban_map: dict[str, list] = {}
    for r in team_ban_rows:
        cid, tname, bans = r[0], r[1], int(r[2])
        team_ban_map.setdefault(cid, []).append({"team": tname, "bans": bans})

    # 픽 행 집계 (포지션 없을 때 champion_id 기준 합산)
    champ_agg: dict[str, dict] = {}
    for r in pick_rows:
        cid, icon, pos, picks, wins = r[0], r[1], r[2], int(r[3]), int(r[4] or 0)
        if cid not in champ_agg:
            champ_agg[cid] = {"icon_url": icon, "pos_counts": {}, "picks": 0, "wins": 0}
        champ_agg[cid]["picks"] += picks
        champ_agg[cid]["wins"] += wins
        if pos:
            champ_agg[cid]["pos_counts"][pos] = champ_agg[cid]["pos_counts"].get(pos, 0) + picks

    champions = []
    processed = set()

    for cid, agg in champ_agg.items():
        processed.add(cid)
        picks = agg["picks"]
        wins = agg["wins"]
        bans = ban_map.get(cid, 0)
        top_pos = max(agg["pos_counts"], key=agg["pos_counts"].get) if agg["pos_counts"] else None

        pick_rate = round(picks / total_games, 3)
        pick_wr = round(wins / picks, 3) if picks > 0 else None
        ban_rate = round(bans / total_games, 3)
        presence_rate = round(min((picks + bans) / total_games, 1.0), 3)

        # 세트별
        all_gns = sorted(set(
            list(game_pick_map.get(cid, {}).keys()) +
            list(game_ban_map.get(cid, {}).keys())
        ))
        by_game = {}
        for gn in all_gns:
            gt = game_num_totals.get(gn, 1)
            gp = game_pick_map.get(cid, {}).get(gn, {"picks": 0, "wins": 0})
            gb = game_ban_map.get(cid, {}).get(gn, 0)
            by_game[str(gn)] = {
                "picks": gp["picks"],
                "pick_rate": round(gp["picks"] / gt, 3) if gt > 0 else 0.0,
                "win_rate": round(gp["wins"] / gp["picks"], 3) if gp["picks"] > 0 else None,
                "bans": gb,
                "ban_rate": round(gb / gt, 3) if gt > 0 else 0.0,
            }

        # 선수 통계
        pstats = player_map.get(cid, [])
        top_pickers = sorted(pstats, key=lambda x: -x["picks"])[:3]
        top_winners = sorted(
            [p for p in pstats if p["picks"] >= 2],
            key=lambda x: -(x["wins"] / x["picks"])
        )[:3]
        for p in top_pickers + top_winners:
            p["win_rate"] = round(p["wins"] / p["picks"], 3) if p["picks"] > 0 else 0.0

        top_banners = sorted(team_ban_map.get(cid, []), key=lambda x: -x["bans"])[:3]

        champions.append({
            "champion": cid,
            "icon_url": agg["icon_url"],
            "position": top_pos,
            "picks": picks,
            "pick_rate": pick_rate,
            "win_rate": pick_wr,
            "bans": bans,
            "ban_rate": ban_rate,
            "presence_rate": presence_rate,
            "by_game": by_game,
            "top_pickers": top_pickers,
            "top_winners": top_winners,
            "top_banners": top_banners,
        })

    # 밴만 된 챔피언 추가 (포지션 필터 없을 때만 — 밴에는 포지션 정보가 없음)
    if position:
        champions.sort(key=lambda x: -x["presence_rate"])
        return {"total_games": total_games, "champions": champions}

    for cid, bans in ban_map.items():
        if cid in processed:
            continue
        ban_rate = round(bans / total_games, 3)
        by_game = {}
        for gn, gt in game_num_totals.items():
            gb = game_ban_map.get(cid, {}).get(gn, 0)
            by_game[str(gn)] = {
                "picks": 0, "pick_rate": 0.0, "win_rate": None,
                "bans": gb,
                "ban_rate": round(gb / gt, 3) if gt > 0 else 0.0,
            }
        champions.append({
            "champion": cid,
            "icon_url": icon_map.get(cid),
            "position": None,
            "picks": 0,
            "pick_rate": 0.0,
            "win_rate": None,
            "bans": bans,
            "ban_rate": ban_rate,
            "presence_rate": ban_rate,
            "by_game": by_game,
            "top_pickers": [],
            "top_winners": [],
            "top_banners": sorted(team_ban_map.get(cid, []), key=lambda x: -x["bans"])[:3],
        })

    champions.sort(key=lambda x: -x["presence_rate"])
    return {"total_games": total_games, "champions": champions}
