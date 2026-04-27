"""
LCK Victory Formula API
FastAPI 백엔드 — 분석 시나리오 A~H 엔드포인트 제공
"""
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from analysis.scenario_a import get_ban_impact
from analysis.scenario_b import get_side_champion_preference
from analysis.scenario_c import get_meta_adaptation_speed, calculate_patch_pbi
from analysis.scenario_d import get_snipe_ban_matrix
from analysis.scenario_e import get_win_formula, get_win_formula_by_patch
from analysis.scenario_f import get_ban_resistance
from analysis.scenario_g import get_team_profile, get_all_team_profiles
from analysis.scenario_h import get_specialist_champions

app = FastAPI(
    title="LCK Victory Formula API",
    description="LCK 경기 데이터 분석 — 메타/팀/선수 레벨",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# =============================================
# 시나리오 A — 핵심 챔피언 밴 시 승률 영향
# =============================================
@app.get("/analysis/ban-impact/{player_name}")
def ban_impact(player_name: str, top_n: int = Query(default=3, ge=1, le=10),
               season_id: str | None = Query(default=None)):
    result = get_ban_impact(player_name, top_n=top_n, season_id=season_id)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


# =============================================
# 시나리오 B — 진영별 챔피언 선택 변화
# =============================================
@app.get("/analysis/side-preference/{player_name}")
def side_preference(player_name: str, season_id: str | None = Query(default=None)):
    result = get_side_champion_preference(player_name, season_id=season_id)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


# =============================================
# 시나리오 C — 패치 적응 속도
# =============================================
@app.get("/analysis/meta-adaptation")
def meta_adaptation(season_id: str | None = Query(default=None)):
    return get_meta_adaptation_speed(season_id)


@app.post("/analysis/calculate-pbi")
def recalculate_pbi(season_id: str | None = Query(default=None)):
    calculate_patch_pbi(season_id)
    return {"status": "ok", "message": "PBI 계산 완료"}


# =============================================
# 시나리오 D — 저격 밴 패턴
# =============================================
@app.get("/analysis/snipe-ban/{team_name}")
def snipe_ban(team_name: str, season_id: str | None = Query(default=None)):
    result = get_snipe_ban_matrix(team_name, season_id)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


# =============================================
# 시나리오 E — 패치별 승리 공식
# =============================================
@app.get("/analysis/win-formula")
def win_formula(patch_id: str | None = Query(default=None)):
    result = get_win_formula(patch_id)
    if "error" in result:
        raise HTTPException(status_code=422, detail=result["error"])
    return result


@app.get("/analysis/win-formula/all")
def win_formula_all():
    return get_win_formula_by_patch()


# =============================================
# 시나리오 F — 선수별 밴 내성 지수
# =============================================
@app.get("/analysis/ban-resistance/{player_name}")
def ban_resistance(player_name: str, team_name: str | None = Query(default=None),
                   season_id: str | None = Query(default=None)):
    result = get_ban_resistance(player_name, team_name, season_id=season_id)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


# =============================================
# 시나리오 G — 팀 색깔 프로파일
# =============================================
@app.get("/analysis/team-profile/{team_name}")
def team_profile(team_name: str, season_id: str | None = Query(default=None)):
    result = get_team_profile(team_name, season_id)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@app.get("/analysis/team-profiles")
def all_team_profiles(season_id: str | None = Query(default=None)):
    return get_all_team_profiles(season_id)


# =============================================
# 시나리오 H — 스페셜리스트 챔피언
# =============================================
@app.get("/analysis/specialist/{player_name}")
def specialist(player_name: str, season_id: str | None = Query(default=None)):
    result = get_specialist_champions(player_name, season_id=season_id)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


# =============================================
# 기타 — 기준 데이터 조회
# =============================================
@app.get("/meta/patches")
def list_patches():
    from analysis.db import get_engine
    from sqlalchemy import text
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT patch_id, version, release_date FROM patch_versions ORDER BY patch_id"
        )).fetchall()
    return [{"patch_id": r[0], "version": r[1], "release_date": str(r[2])} for r in rows]


@app.get("/meta/teams")
def list_teams():
    from analysis.db import get_engine
    from sqlalchemy import text
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT team_id, name, acronym FROM teams WHERE region='LCK' ORDER BY name"
        )).fetchall()
    return [{"team_id": r[0], "name": r[1], "acronym": r[2]} for r in rows]


@app.get("/meta/players")
def list_players(team_name: str | None = Query(default=None)):
    from analysis.db import get_engine
    from sqlalchemy import text
    engine = get_engine()
    with engine.connect() as conn:
        if team_name:
            rows = conn.execute(text("""
                SELECT DISTINCT p.player_id, p.summoner_name, pth.role
                FROM players p
                JOIN player_team_history pth ON pth.player_id = p.player_id
                JOIN teams t ON t.team_id = pth.team_id
                WHERE t.name = :n OR t.acronym = :n
                ORDER BY pth.role, p.summoner_name
            """), {"n": team_name}).fetchall()
        else:
            rows = conn.execute(text(
                "SELECT player_id, summoner_name, NULL FROM players ORDER BY summoner_name"
            )).fetchall()
    return [{"player_id": r[0], "name": r[1], "position": r[2]} for r in rows]
