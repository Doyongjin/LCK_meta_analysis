"""
LCK Victory Formula — Streamlit 대시보드
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st

st.set_page_config(
    page_title="LCK Victory Formula",
    page_icon="⚔️",
    layout="wide",
    initial_sidebar_state="expanded",
)

with st.sidebar:
    st.title("⚔️ LCK Victory Formula")
    st.caption("메타 · 팀 · 선수 분석")
    st.divider()

    # DB 새로고침 버튼 (2시간 쿨다운)
    import time as _time
    _COOLDOWN = 2 * 3600  # 7200초

    if "last_db_refresh" not in st.session_state:
        st.session_state["last_db_refresh"] = 0.0

    _now = _time.time()
    _elapsed = _now - st.session_state["last_db_refresh"]
    _remaining = _COOLDOWN - _elapsed

    st.caption("⚠️ 데이터 다운로드 및 DB 새로고침은 2시간에 1번만 가능합니다.")

    if _remaining <= 0:
        _pw_input = st.text_input("관리자 비밀번호", type="password", key="admin_pw")
        if st.button("⬇️ 데이터 다운로드 + DB 로드", use_container_width=True):
            _correct_pw = st.secrets.get("ADMIN_PASSWORD", "")
            if not _pw_input or _pw_input != _correct_pw:
                st.error("비밀번호가 틀렸습니다.")
            else:
                dl_errors = []
                with st.spinner("Oracle's Elixir 데이터 다운로드 중..."):
                    try:
                        from etl.download_oracles_elixir import download_csv
                        for y in [2024, 2025, 2026]:
                            download_csv(y)
                    except Exception as e:
                        dl_errors.append(str(e))
                if dl_errors:
                    st.warning(f"다운로드 일부 실패 (오늘 파일 없을 수 있음): {dl_errors[0][:200]}")

                with st.spinner("DB에 데이터 로드 중... (수분 소요)"):
                    try:
                        from etl.load_to_db import run_etl
                        for y in [2024, 2025, 2026]:
                            run_etl(y)
                        st.success("✅ 데이터 로드 완료!")
                    except Exception as e:
                        st.error(f"ETL 오류: {str(e)[:500]}")

                st.cache_data.clear()
                st.cache_resource.clear()
                st.session_state["last_db_refresh"] = _time.time()
                st.rerun()
    else:
        _rem_h = int(_remaining // 3600)
        _rem_m = int((_remaining % 3600) // 60)
        _rem_s = int(_remaining % 60)
        if _rem_h > 0:
            _rem_str = f"{_rem_h}시간 {_rem_m}분 후"
        else:
            _rem_str = f"{_rem_m}분 {_rem_s}초 후"
        st.button(f"⬇️ 데이터 다운로드 + DB 로드 ({_rem_str})", disabled=True, use_container_width=True)

    st.divider()

    from analysis.db import get_engine as _get_engine
    from sqlalchemy import text as _text
    _eng = _get_engine()
    with _eng.connect() as _c:
        _seasons = [r[0] for r in _c.execute(
            _text("SELECT season_id FROM seasons ORDER BY year DESC, season_id DESC")
        ).fetchall()]

    selected_season = st.selectbox(
        "시즌",
        options=["전체"] + _seasons,
        index=1 if _seasons else 0,
    )
    season_id = None if selected_season == "전체" else selected_season
    st.caption(f"선택: {selected_season}")
    st.divider()

    page = st.radio(
        "분석 메뉴",
        options=[
            "🏠 홈",
            "A. 밴 시 승률 영향",
            "B. 진영별 챔피언 성향",
            "C. 패치 적응 속도",
            "D. 저격 밴 패턴",
            "E. 패치별 승리 공식",
            "F. 밴 내성 지수",
            "G. 팀 색깔 프로파일",
            "H. 스페셜리스트 챔피언",
        ],
        label_visibility="collapsed",
    )


@st.cache_resource
def load_fns():
    from analysis.scenario_a import get_ban_impact
    from analysis.scenario_b import get_side_champion_preference
    from analysis.scenario_c import get_meta_adaptation_speed
    from analysis.scenario_d import get_snipe_ban_matrix
    from analysis.scenario_e import get_win_formula
    from analysis.scenario_f import get_ban_resistance
    from analysis.scenario_g import get_team_profile, get_all_team_profiles
    from analysis.scenario_h import get_specialist_champions, get_team_roster_by_games
    return {
        "ban_impact": get_ban_impact,
        "side_pref": get_side_champion_preference,
        "meta_adapt": get_meta_adaptation_speed,
        "snipe_ban": get_snipe_ban_matrix,
        "win_formula": get_win_formula,
        "ban_resistance": get_ban_resistance,
        "team_profile": get_team_profile,
        "all_team_profiles": get_all_team_profiles,
        "specialist": get_specialist_champions,
        "team_roster": get_team_roster_by_games,
    }


@st.cache_data(ttl=60)
def load_lists(season_id: str | None):
    """시즌별 팀·선수·패치·포지션 목록"""
    from analysis.db import get_engine
    from sqlalchemy import text
    engine = get_engine()
    with engine.connect() as conn:
        if season_id:
            teams = [r[0] for r in conn.execute(text("""
                SELECT DISTINCT t.name FROM teams t
                JOIN player_team_history pth ON pth.team_id = t.team_id
                WHERE pth.season_id = :sid AND t.region = 'LCK'
                ORDER BY t.name
            """), {"sid": season_id}).fetchall()]
            players = [r[0] for r in conn.execute(text("""
                SELECT DISTINCT p.summoner_name FROM players p
                JOIN player_team_history pth ON pth.player_id = p.player_id
                WHERE pth.season_id = :sid
                ORDER BY p.summoner_name
            """), {"sid": season_id}).fetchall()]
            pos_rows = conn.execute(text("""
                SELECT DISTINCT p.summoner_name, pth.role
                FROM players p
                JOIN player_team_history pth ON pth.player_id = p.player_id
                WHERE pth.season_id = :sid
            """), {"sid": season_id}).fetchall()
        else:
            teams = [r[0] for r in conn.execute(
                text("SELECT name FROM teams WHERE region='LCK' ORDER BY name")
            ).fetchall()]
            players = [r[0] for r in conn.execute(
                text("SELECT summoner_name FROM players ORDER BY summoner_name")
            ).fetchall()]
            pos_rows = conn.execute(text("""
                WITH ranked AS (
                    SELECT p.summoner_name, pth.role,
                           ROW_NUMBER() OVER (
                               PARTITION BY p.summoner_name
                               ORDER BY pth.season_id DESC
                           ) AS rn
                    FROM players p
                    JOIN player_team_history pth ON pth.player_id = p.player_id
                )
                SELECT summoner_name, role FROM ranked WHERE rn = 1
                ORDER BY summoner_name
            """)).fetchall()

        patches = [r[0] for r in conn.execute(
            text("SELECT patch_id FROM patch_versions ORDER BY patch_id")
        ).fetchall()]

    player_positions = {r[0]: r[1] for r in pos_rows}
    return {"teams": teams, "players": players, "patches": patches,
            "player_positions": player_positions}


# ─────────────────────────────────────────────
# 공통 헬퍼
# ─────────────────────────────────────────────
def _patch_public(patch_id: str) -> str:
    """Oracle's Elixir 내부 패치 ID → Riot 공개 명칭 (e.g. '16.08' → '26.8')"""
    try:
        major, minor = patch_id.split(".", 1)
        return f"{int(major) + 10}.{int(minor)}"
    except (ValueError, AttributeError):
        return patch_id


def _same_pos_options(player: str, players: list, positions: dict) -> list[str]:
    """같은 포지션 선수 목록 (본인 제외)"""
    pos = positions.get(player, "")
    return [p for p in players if positions.get(p) == pos and p != player]


def _player_compare_ui(players: list, positions: dict, key_prefix: str):
    """선수 + 비교 선수 selectbox 반환 (col1, col2)"""
    col1, col2 = st.columns(2)
    with col1:
        player = st.selectbox("선수 선택", players, key=f"{key_prefix}_p1")
    with col2:
        pos = positions.get(player, "")
        same = _same_pos_options(player, players, positions)
        compare = st.selectbox(
            f"비교 선수 ({pos} 포지션)",
            ["(없음)"] + same,
            key=f"{key_prefix}_p2",
        )
    return player, (None if compare == "(없음)" else compare)


# ─────────────────────────────────────────────
# 홈
# ─────────────────────────────────────────────
@st.cache_data(ttl=3600)
def _load_last_game():
    from analysis.db import get_engine
    from sqlalchemy import text
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT
                g.date,
                t1.name AS team1,
                t2.name AS team2,
                tw.name AS winner,
                s.stage,
                s.format
            FROM games g
            JOIN series s ON g.series_id = s.series_id
            JOIN teams t1 ON s.team1_id = t1.team_id
            JOIN teams t2 ON s.team2_id = t2.team_id
            LEFT JOIN teams tw ON s.winner_team_id = tw.team_id
            ORDER BY g.date DESC, g.game_id DESC
            LIMIT 1
        """)).fetchone()
    return dict(row._mapping) if row else None


def show_home():
    st.title("LCK Victory Formula & Meta Analysis")

    last = _load_last_game()
    if last:
        st.info(
            f"📅 **DB 최신 경기**: {last['date']}  |  "
            f"**{last['team1']} vs {last['team2']}**"
            + (f"  |  승리: **{last['winner']}**" if last['winner'] else "")
            + (f"  |  {last['stage']}" if last['stage'] else "")
        )

    st.markdown("""
**핵심 질문: "누가 이겼나"가 아닌 "어떤 메타적 선택이 승리를 이끌었나"**

| 시나리오 | 설명 |
|---------|------|
| A | 핵심 챔피언 밴 시 승률 영향 |
| B | 진영별 챔피언 선택 변화 |
| C | 패치 적응 속도 |
| D | 저격 밴 패턴 |
| E | 패치별 승리 공식 (Logistic Regression) |
| F | 선수별 밴 내성 지수 |
| G | 팀 색깔 × 승률 상관관계 |
| H | 스페셜리스트 챔피언 판별 |
    """)


# ─────────────────────────────────────────────
# A. 밴 시 승률 영향
# ─────────────────────────────────────────────
def show_scenario_a(fn, players, player_positions, season_id=None):
    st.title("A. 핵심 챔피언 밴 시 승률 영향")

    with st.expander("📖 해석 가이드", expanded=False):
        st.markdown("""
- **표시 지표**: 선수의 주력 챔피언이 밴당했을 때의 승률 vs 밴 안 됐을 때 승률
- **양수(빨강 막대)**: 주력 챔피언이 밴당하면 승률이 떨어진다 → **저격 밴이 유효한 선수**
- **음수(파랑 막대)**: 밴당해도 오히려 승률이 올라간다 → 챔프폭이 넓거나 대체 챔이 더 강함
- **0 근처**: 밴 영향 없음 → 안정적
- **주의**: 밴당한 경기 수가 적으면 극단값(0%, 100%)이 나오므로 **표본 수**를 함께 봐야 함
""")

    col1, col2, col3 = st.columns([3, 3, 1])
    with col1:
        player = st.selectbox("선수 선택", players, key="a_p1")
    with col2:
        pos = player_positions.get(player, "")
        same = _same_pos_options(player, players, player_positions)
        compare_raw = st.selectbox(f"비교 선수 ({pos} 포지션)",
                                   ["(없음)"] + same, key="a_p2")
        compare = None if compare_raw == "(없음)" else compare_raw
    with col3:
        top_n = st.slider("챔피언 수", 1, 5, 3, key="a_topn")

    if not st.button("분석", key="a_run"):
        return

    import plotly.graph_objects as go
    import pandas as pd

    def _run(name):
        return fn(name, top_n=top_n, season_id=season_id)

    def _render(result, title):
        if "error" in result:
            st.error(result["error"])
            return
        rows = result["champions"]
        if not rows:
            st.info("데이터 없음")
            return
        df = pd.DataFrame(rows)
        fig = go.Figure()
        for _, r in df.iterrows():
            fig.add_trace(go.Bar(
                name=r["champion"],
                x=["밴 안 됐을 때", "밴됐을 때"],
                y=[r["win_rate_normal"], r["win_rate_banned"]],
                text=[f"{r['win_rate_normal']:.1%}", f"{r['win_rate_banned']:.1%}"],
                textposition="auto",
            ))
        fig.update_layout(
            title=title, yaxis_title="팀 승률",
            yaxis_tickformat=".0%", barmode="group",
        )
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df[["champion", "total_games", "banned_games",
                          "win_rate_normal", "win_rate_banned",
                          "gold15_normal", "gold15_banned"]],
                     hide_index=True)

    with st.spinner("분석 중..."):
        r1 = _run(player)
        r2 = _run(compare) if compare else None

    if r2:
        c1, c2 = st.columns(2)
        with c1:
            st.subheader(player)
            _render(r1, f"{player} — 밴 시 승률 변화")
        with c2:
            st.subheader(compare)
            _render(r2, f"{compare} — 밴 시 승률 변화")
    else:
        _render(r1, f"{player} — 주력 챔피언 밴 시 승률 변화")


# ─────────────────────────────────────────────
# B. 진영별 챔피언 성향
# ─────────────────────────────────────────────
def show_scenario_b(fn, players, player_positions, season_id=None):
    st.title("B. 진영별 챔피언 선택 변화")

    with st.expander("📖 해석 가이드", expanded=False):
        st.markdown("""
- **표시 지표**: 블루(선픽) 진영 vs 레드(후픽) 진영에서 선수가 자주 픽하는 챔피언
- **블루-레드 챔피언이 다름**: 선수가 진영에 따라 픽 전략을 바꾼다 (예: 블루는 카운터당하기 쉬운 챔, 레드는 카운터픽)
- **블루-레드 챔피언이 비슷함**: 진영과 무관하게 챔프풀이 일관됨
- **활용**: 상대팀 분석 시 우리 진영에 따라 어떤 챔피언이 풀리지 예측
""")

    player, compare = _player_compare_ui(players, player_positions, "b")

    if not st.button("분석", key="b_run"):
        return

    import pandas as pd

    def _render(result):
        if "error" in result:
            st.error(result["error"])
            return
        c1, c2, c3 = st.columns(3)
        with c1:
            st.subheader("🔵 블루 전용")
            for c in result["blue_only"]:
                st.write(f"• {c}")
        with c2:
            st.subheader("🔴 레드 전용")
            for c in result["red_only"]:
                st.write(f"• {c}")
        with c3:
            st.subheader("⚪ 공통")
            for c in result["both"]:
                st.write(f"• {c}")
        df = pd.DataFrame(result["detail"])
        if not df.empty:
            st.dataframe(df[["champion", "blue_games", "blue_wr",
                              "red_games", "red_wr"]], hide_index=True)

    with st.spinner("분석 중..."):
        r1 = fn(player, season_id=season_id)
        r2 = fn(compare, season_id=season_id) if compare else None

    if r2:
        c1, c2 = st.columns(2)
        with c1:
            st.subheader(player)
            _render(r1)
        with c2:
            st.subheader(compare)
            _render(r2)
    else:
        _render(r1)


# ─────────────────────────────────────────────
# C. 패치 적응 속도
# ─────────────────────────────────────────────
def show_scenario_c(fn):
    st.title("C. 패치 적응 속도")
    st.caption("패치 출시 후 강챔을 얼마나 빠르게 픽에 반영하는지 (낮을수록 빠름)")

    with st.expander("📖 해석 가이드", expanded=False):
        st.markdown("""
- **표시 지표**: 새 패치의 메타 챔피언을 팀이 처음 픽하기까지 걸린 평균 경기 수
- **낮을수록(빠를수록) 좋음**: 메타 변화에 빠르게 적응 → 코칭 스태프의 분석력
- **0~3경기**: 매우 빠름 (선제 대응)
- **4~7경기**: 평균
- **8경기 이상**: 느림 (다른 팀 따라가기)
- **활용**: 신규 패치 직후 어느 팀이 메타 우위를 점할지 예측
""")

    if not st.button("분석", key="c_run"):
        return

    with st.spinner("분석 중..."):
        result = fn()
    if not result:
        st.info("데이터 부족 (PBI 계산 먼저 실행 필요)")
        return

    import pandas as pd
    import plotly.express as px

    df = pd.DataFrame(result)
    fig = px.bar(df, x="team", y="avg_adaptation_games",
                 title="팀별 패치 적응 속도 (경기 수)",
                 labels={"avg_adaptation_games": "평균 첫 채택까지 경기 수"},
                 color="avg_adaptation_games",
                 color_continuous_scale="RdYlGn_r")
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df)


# ─────────────────────────────────────────────
# D. 저격 밴 패턴
# ─────────────────────────────────────────────
def show_scenario_d(fn, teams, season_id=None):
    st.title("D. 저격 밴 패턴")

    with st.expander("📖 해석 가이드", expanded=False):
        st.markdown("""
- **표시 지표**: 상대팀이 우리 선수의 주력 챔피언을 1~3순위 밴(global_order ≤ 6)에 얼마나 자주 사용했는지
- **저격 밴율 높음(40%+)**: 상대팀이 의식적으로 견제 → **위협적인 선수**
- **저격 밴율 낮음**: 상대팀이 위협으로 보지 않거나, 챔프폭이 넓어서 견제 의미 없음
- **피어리스 게임별 분리**: 1경기 vs 3경기 밴 패턴이 구조적으로 다름
  - 3경기 밴은 1-2경기에서 픽된 챔피언이 자동 제외된 상태에서 이뤄짐
- **활용**: 어떤 선수/챔피언이 메타에서 OP로 인식되는지 파악
""")

    col1, col2 = st.columns(2)
    with col1:
        team1 = st.selectbox("팀 선택", teams, key="d_t1")
    with col2:
        compare_raw = st.selectbox("비교 팀 (선택)", ["(없음)"] + teams, key="d_t2")
        compare_team = None if compare_raw == "(없음)" else compare_raw

    if not st.button("분석", key="d_run"):
        return

    def _render(result):
        if "error" in result:
            st.error(result["error"])
            return
        for player_data in result["players"]:
            st.subheader(f"{player_data['player']} ({player_data['position']})")
            for champ in player_data["top_champions"]:
                ca, cb = st.columns([1, 3])
                with ca:
                    if champ["icon_url"]:
                        st.image(champ["icon_url"], width=50)
                    st.write(champ["champion"])
                with cb:
                    st.metric(
                        "저격 밴율 (전체)",
                        f"{champ['snipe_ban_rate']:.1%}",
                        f"{champ['snipe_ban_count']}/{champ['total_games_available']}경기"
                    )
                    if champ["opponents"]:
                        opps = ", ".join(
                            [f"{o['team']}({o['count']})" for o in champ["opponents"][:3]]
                        )
                        st.caption(f"주요 저격팀: {opps}")

                    # 피어리스 게임 번호별 분리 표시
                    by_game = champ.get("by_game", {})
                    fearless_total = champ.get("fearless_games_available", 0)
                    if by_game and fearless_total > 0:
                        with st.expander(f"피어리스 게임별 밴 패턴 (총 {fearless_total}경기)"):
                            cols = st.columns(len(by_game))
                            for idx, (gn, data) in enumerate(sorted(by_game.items(), key=lambda x: int(x[0]))):
                                with cols[idx]:
                                    st.metric(
                                        f"{gn}경기",
                                        f"{data['rate']:.1%}",
                                        f"{data['count']}/{data['available']}",
                                    )
                            st.caption("3경기 밴율은 1-2경기 픽으로 자동 제외된 챔피언 풀에서 계산됨")
            st.divider()

    with st.spinner("분석 중..."):
        r1 = fn(team1, season_id)
        r2 = fn(compare_team, season_id) if compare_team else None

    if r2:
        c1, c2 = st.columns(2)
        with c1:
            st.subheader(team1)
            _render(r1)
        with c2:
            st.subheader(compare_team)
            _render(r2)
    else:
        _render(r1)


# ─────────────────────────────────────────────
# E. 패치별 승리 공식
# ─────────────────────────────────────────────
def show_scenario_e(fn, patches):
    st.title("E. 패치별 승리 공식")

    with st.expander("📖 해석 가이드", expanded=False):
        st.markdown("""
- **표시 지표**: 로지스틱 회귀로 학습한 "어떤 지표가 승리에 기여하는가"의 가중치(coefficient)
- **양수 가중치**: 그 지표가 높을수록 **승리 확률 상승** (예: 첫 드래곤 +0.8 → 첫 드래곤 잡으면 승리 가능성↑)
- **음수 가중치**: 그 지표가 높을수록 패배 확률 (해당 패치에서는 의미 없는 지표)
- **절댓값이 클수록**: 승패에 미치는 영향이 큼
- **패치별 차이**: 패치마다 메타가 달라져서 "이번 패치는 첫 타워가 중요" 등 변화
- **활용**: 현재 패치에서 우선순위로 챙겨야 할 오브젝트/지표 파악
""")

    # 내부 ID → 공개 명칭 매핑 (e.g. "16.08" → "26.8")
    patch_display_map = {p: _patch_public(p) for p in patches}
    patch_internal_map = {v: k for k, v in patch_display_map.items()}
    display_options = ["전체"] + [patch_display_map[p] for p in patches]

    patch_display = st.selectbox("패치 선택 (비워두면 전체)", display_options)
    patch_id = None if patch_display == "전체" else patch_internal_map.get(patch_display, patch_display)

    if not st.button("분석", key="e_run"):
        return

    with st.spinner("분석 중..."):
        result = fn(patch_id)
    if "error" in result:
        st.error(result["error"])
        return

    import plotly.express as px
    import pandas as pd

    st.metric("분석 경기 수", result["n_games"])
    st.metric("모델 정확도", f"{result['accuracy']:.1%}")

    df = pd.DataFrame(result["features"])
    df["color"] = df["coefficient"].apply(lambda x: "승리 기여" if x > 0 else "패배 요인")
    fig = px.bar(df, x="coefficient", y="name", orientation="h",
                 color="color",
                 color_discrete_map={"승리 기여": "#2196F3", "패배 요인": "#F44336"},
                 title=f"패치 {patch_display} 승리 공식 — 요소별 기여도")
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df[["name", "coefficient", "odds_ratio"]])


# ─────────────────────────────────────────────
# F. 밴 내성 지수
# ─────────────────────────────────────────────
def show_scenario_f(fn, players, player_positions, season_id=None):
    st.title("F. 밴 내성 지수")

    with st.expander("📖 해석 가이드", expanded=False):
        st.markdown("""
**밴 내성 지수란?** 0~100점. 주력 챔피언을 밴당해도 선수가 얼마나 잘 버틸 수 있는지를 측정.

#### 점수 구성 — LCK 실제 분포 기반 (각 25%)
| 요소 | 의미 | 좋은 방향 |
|---|---|---|
| 챔피언 풀 | 시즌 내 사용한 챔피언 종류 (1경기 이상) | 많을수록 ↑ |
| 주력 의존도 | 가장 많이 쓴 챔피언이 전체 경기에서 차지하는 비율 | 낮을수록 ↑ |
| 밴 시 승률 하락 | 주력 top3 사용 불가 시 평균 승률 하락폭 | 낮을수록 ↑ |
| 골드 변동성 증가 | 주력 사용 불가 시 15분 골드 표준편차 증가폭 | 낮을수록 ↑ |

> 각 지표는 LCK 동일 포지션 선수들의 실제 분포(상위 10% ~ 하위 10%)를 기준으로 점수화.
> 임의 가중치 없이 4개 지표 동등 적용.

#### 점수 해석
- **70점 이상**: 밴 내성 매우 높음 → LCK 상위 30% 수준, 저격 밴 효과 낮음
- **50점 근처**: LCK 평균 수준
- **30점 이하**: LCK 하위 30% → 특정 챔피언 의존, 저격 밴이 효과적

#### 지표별 점수 해석 (레이더 차트)
- **100점**: LCK 상위 10% (해당 포지션 최상위)
- **50점**: LCK 평균
- **0점**: LCK 하위 10%

#### 주력 챔피언 기준
- 2경기 이상 플레이한 챔피언 **top3** 기준으로 평균 영향 측정
- 1개 챔피언만 보는 것보다 실제 밴 상황을 더 정확하게 반영

#### 주의사항
⚠️ **표본 부족 시 극단값**: 밴당한 경기가 3개 미만이면 신뢰도 낮음.

#### 피어리스 보정
주력 챔피언 "사용 불가"는 두 가지를 합산:
- 상대팀의 밴
- 같은 시리즈 앞 경기에서 우리 팀이 이미 픽 (피어리스 규정)
""")

    col1, col2 = st.columns(2)
    with col1:
        player = st.selectbox("선수 선택", players, key="f_p1")
    with col2:
        pos = player_positions.get(player, "")
        same = _same_pos_options(player, players, player_positions)
        compare_raw = st.selectbox(f"비교 선수 ({pos} 포지션)",
                                   ["(없음)"] + same, key="f_p2")
        compare = None if compare_raw == "(없음)" else compare_raw

    if not st.button("분석", key="f_run"):
        return

    import plotly.graph_objects as go

    def _radar_vals(result):
        bd = result.get("score_breakdown", {})
        return [
            bd.get("pool_score", 50),
            bd.get("dep_score", 50),
            bd.get("drop_score", 50),
            bd.get("stab_score", 50),
            bd.get("pool_score", 50),  # 닫힘
        ]

    def _pct_label(score):
        if score >= 90: return "상위 10%"
        if score >= 70: return "상위 30%"
        if score >= 50: return "평균 수준"
        if score >= 30: return "하위 30%"
        return "하위 10%"

    def _metrics(result, label):
        score = result["ban_resistance_score"]
        top3 = result.get("primary_champions_top3", [result.get("primary_champion") or "-"])
        top3_str = " / ".join(top3) if top3 else "-"
        st.caption(f"**{label}** — 밴 내성: **{score:.1f}/100** ({_pct_label(score)})")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("챔피언 풀", f"{result['champ_pool_size']}개")
        m2.metric("밴 시 승률 하락", f"{result['wr_drop_when_banned']:.1%}")
        delta_str = f"+{result['gold15_volatility_delta']:.0f}" if result["gold15_volatility_delta"] > 0 else "0"
        m3.metric("골드 변동성 증가", f"{result['gold15_volatility_delta']:.0f}골드",
                  delta=delta_str, delta_color="inverse")
        m4.metric("주력 챔피언 (top3)", top3_str)

        bd = result.get("score_breakdown", {})
        if bd:
            b1, b2, b3, b4 = st.columns(4)
            b1.caption(f"풀 점수: **{bd.get('pool_score', 0):.0f}**")
            b2.caption(f"의존도 점수: **{bd.get('dep_score', 0):.0f}**")
            b3.caption(f"밴 시 점수: **{bd.get('drop_score', 0):.0f}**")
            b4.caption(f"안정성 점수: **{bd.get('stab_score', 0):.0f}**")

    with st.spinner("분석 중..."):
        r1 = fn(player, None, season_id=season_id)
        r2 = fn(compare, None, season_id=season_id) if compare else None

    if "error" in r1:
        st.error(r1["error"])
        return

    categories = ["챔피언 풀", "주력 의존도↓", "밴 시 승률 유지", "골드 안정성", "챔피언 풀"]

    _metrics(r1, player)
    if r2 and "error" not in r2:
        _metrics(r2, compare)

    with st.expander("Gold@15 변동성 상세"):
        cx1, cx2 = st.columns(2)
        top3_str = " / ".join(r1.get("primary_champions_top3", [])) or r1.get("primary_champion") or "-"
        cx1.metric(f"{top3_str} 픽 시 팀 골드 표준편차",
                   f"{r1['gold15_stddev_normal']:.0f}골드")
        cx2.metric("주력 사용 불가 시 팀 골드 표준편차",
                   f"{r1['gold15_stddev_banned']:.0f}골드",
                   delta=f"+{r1['gold15_volatility_delta']:.0f}" if r1["gold15_volatility_delta"] > 0 else "변화 없음",
                   delta_color="inverse")
        st.caption("표준편차가 높을수록 팀 전술이 흔들리는 신호")

    with st.expander("주력 챔피언 사용 불가 사유 (피어리스 보정)"):
        bc1, bc2, bc3 = st.columns(3)
        bc1.metric("상대 밴", f"{r1.get('banned_games_count', 0)}경기")
        bc2.metric("피어리스 앞 경기 픽", f"{r1.get('prev_used_games_count', 0)}경기")
        bc3.metric("총 사용 불가", f"{r1.get('blocked_games_count', 0)}경기")
        st.caption("피어리스 시리즈에서 같은 챔피언을 앞 경기에 이미 썼다면 후속 경기에선 강제로 다른 챔피언을 픽해야 합니다.")

    # 레이더 차트
    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=_radar_vals(r1), theta=categories,
        fill="toself", name=player,
        line=dict(color="#2196F3"), fillcolor="rgba(33,150,243,0.15)",
    ))
    if r2 and "error" not in r2:
        fig.add_trace(go.Scatterpolar(
            r=_radar_vals(r2), theta=categories,
            fill="toself", name=compare,
            line=dict(color="#FF9800"), fillcolor="rgba(255,152,0,0.15)",
        ))
    fig.add_trace(go.Scatterpolar(
        r=[50, 50, 50, 50, 50], theta=categories,
        fill="toself", name="LCK 평균",
        line=dict(dash="dash", color="rgba(150,150,150,0.6)"),
        fillcolor="rgba(200,200,200,0.15)",
    ))
    fig.update_layout(
        polar=dict(radialaxis=dict(range=[0, 100], tickvals=[25, 50, 75],
                                   ticktext=["하위30%", "평균", "상위30%"])),
        legend=dict(x=0.85, y=1.1),
    )
    st.plotly_chart(fig, use_container_width=True)


# ─────────────────────────────────────────────
# G. 팀 색깔 프로파일
# ─────────────────────────────────────────────
@st.cache_data(ttl=3600)
def _cached_team_profile(team_name, season_id):
    from analysis.scenario_g import get_team_profile
    return get_team_profile(team_name, season_id)

@st.cache_data(ttl=3600)
def _cached_all_team_profiles(season_id):
    from analysis.scenario_g import get_all_team_profiles
    return get_all_team_profiles(season_id)

def show_scenario_g(fn_single, fn_all, teams, season_id=None):
    st.title("G. 팀 색깔 프로파일")

    with st.expander("📖 해석 가이드", expanded=False):
        st.markdown("""
**팀 색깔이란?** 팀의 경기 스타일을 4가지 유형으로 분류.

#### 팀 색깔 분류
| 색깔 | 조건 | 의미 |
|---|---|---|
| **시스템형** | 밴 내성 평균 65+ | 선수 전원의 챔프폭이 넓어 어떤 픽이든 운영 가능 |
| **초반 압박형** | 15분 골드 우위 +500↑ & 오브젝트율 55%+ | 빠른 라인전 우위로 스노우볼 |
| **캐리 의존형** | 밴 내성 평균 40↓ | 특정 선수의 주력 챔에 의존 → 저격 밴에 약함 |
| **후반 역전형** | 그 외 | 초반은 안정, 후반 한타로 게임 결정 |

#### 레이더 차트 해석
- **블루/레드 WR**: 진영 편향 (한쪽이 너무 강하면 진영 의존적)
- **선픽/후픽 WR**: 픽 순서 편향
- **밴 내성**: 팀 평균 (선수들의 적응력)
- **오브젝트율**: 첫 드래곤/전령/타워 선점 비율

#### 활용
- **상대팀 분석**: 캐리 의존형 → 저격 밴 효과적 / 시스템형 → 다른 전략 필요
- **자기팀 진단**: 약점 지표 확인
- **주의**: 밴 내성은 개인 역량 합산이라 팀 전체적으로 높게 나오기 쉬움 (절대값보다 팀 간 상대 비교가 의미 있음)

#### 밴 내성 점수 해석 주의사항
밴 내성 점수가 **낮다고 해서 선수가 못하는 것이 아닙니다.**
오히려 **"상대가 챙겨 밴할 만큼 위협적인 선수"** 일 가능성이 높습니다.

> **예시**: 정글 선수의 밴 내성이 낮게 나온 경우
> → 해당 선수의 주력 챔피언이 차단됐을 때 팀 승률이 LCK 정글 평균보다 크게 떨어진다는 뜻
> → 상대 입장에서는 그 선수의 주력을 밴하는 것이 가장 효과적인 드래프트 전략
> → 팀이 "캐리 의존형"으로 분류된다면, 그 근거의 상당 부분이 이 선수에게서 나오는 것

**밴 내성이 낮은 선수가 있는 팀을 상대할 때의 드래프트 전략:**
1. 해당 선수 주력 챔피언을 1~3순위 밴에 배치 (D. 저격 밴 패턴 탭 참조)
2. 해당 선수가 비주력으로 올 경우 초반 라인전 압박으로 우위 선점

**밴 내성이 낮은 팀의 자가 진단:**
- 특정 선수 의존도를 줄이기 위해 챔프폭 확장 훈련 필요
- 피어리스 드래프트에서는 특히 주력 챔피언 소진 후 대안 챔피언 준비 필수
""")

    mode = st.radio("모드", ["두 팀 비교", "전체 팀 분포"])

    if mode == "두 팀 비교":
        col1, col2 = st.columns(2)
        with col1:
            team1 = st.selectbox("팀 1", teams, key="g_t1")
        with col2:
            teams2_options = [t for t in teams if t != team1]
            team2 = st.selectbox("팀 2", teams2_options, key="g_t2")

        if not st.button("분석", key="g_run"):
            return

        with st.spinner("분석 중..."):
            r1 = _cached_team_profile(team1, season_id)
            r2 = _cached_team_profile(team2, season_id)

        for r in (r1, r2):
            if "error" in r:
                st.error(r["error"])
                return

        # 팀 색깔 레이더 (오버레이)
        import plotly.graph_objects as go
        import plotly.express as px
        import pandas as pd

        cats = ["블루 WR", "레드 WR", "선픽 WR", "후픽 WR", "밴 내성", "오브젝트율", "블루 WR"]

        def _t_vals(r):
            return [
                r["blue_win_rate"] * 100,
                r["red_win_rate"] * 100,
                r["first_pick_wr"] * 100,
                r["second_pick_wr"] * 100,
                r["ban_resistance_avg"],
                r["first_object_rate"] * 100,
                r["blue_win_rate"] * 100,
            ]

        fig_radar = go.Figure()
        fig_radar.add_trace(go.Scatterpolar(
            r=_t_vals(r1), theta=cats, fill="toself", name=team1,
            line=dict(color="#2196F3"), fillcolor="rgba(33,150,243,0.15)",
        ))
        fig_radar.add_trace(go.Scatterpolar(
            r=_t_vals(r2), theta=cats, fill="toself", name=team2,
            line=dict(color="#FF9800"), fillcolor="rgba(255,152,0,0.15)",
        ))
        fig_radar.update_layout(
            polar=dict(radialaxis=dict(range=[0, 100], tickvals=[25, 50, 75])),
            legend=dict(x=0.85, y=1.1),
        )
        st.plotly_chart(fig_radar, use_container_width=True)

        # 지표 비교
        metrics = [
            ("팀 색깔", "color_label", "color_label"),
            ("밴 내성", "ban_resistance_avg", "ban_resistance_avg"),
            ("15분 골드 우위", "gold15_avg", "gold15_avg"),
            ("블루 승률", "blue_win_rate", "blue_win_rate"),
            ("레드 승률", "red_win_rate", "red_win_rate"),
            ("선픽 승률", "first_pick_wr", "first_pick_wr"),
            ("후픽 승률", "second_pick_wr", "second_pick_wr"),
            ("오브젝트율", "first_object_rate", "first_object_rate"),
        ]

        header_cols = st.columns([2, 2, 2])
        header_cols[0].write("**지표**")
        header_cols[1].write(f"**{team1}**")
        header_cols[2].write(f"**{team2}**")
        st.divider()

        def _fmt(key, val):
            if key == "color_label":
                return str(val)
            if key == "gold15_avg":
                return f"{val:,.0f}"
            if isinstance(val, float):
                if 0 <= val <= 1:
                    return f"{val:.1%}"
                return f"{val:.1f}"
            return str(val)

        for label, k1, k2 in metrics:
            row = st.columns([2, 2, 2])
            row[0].write(label)
            row[1].write(_fmt(k1, r1.get(k1, "-")))
            row[2].write(_fmt(k2, r2.get(k2, "-")))

        # 선수 밴 내성 비교
        st.divider()
        st.subheader("선수별 밴 내성 비교")
        df1 = pd.DataFrame(r1["players"]).assign(team=team1) if r1["players"] else pd.DataFrame()
        df2 = pd.DataFrame(r2["players"]).assign(team=team2) if r2["players"] else pd.DataFrame()
        if not df1.empty or not df2.empty:
            df_all = pd.concat([df1, df2], ignore_index=True)
            pos_order = {"top": 0, "jng": 1, "mid": 2, "bot": 3, "sup": 4}
            df_all["pos_rank"] = df_all["position"].map(pos_order)
            df_all = df_all.sort_values(["pos_rank", "player"]).drop("pos_rank", axis=1)
            fig_bar = px.bar(
                df_all, x="player", y="ban_resistance", color="team",
                barmode="group", title="선수별 밴 내성 점수",
                color_discrete_map={team1: "#2196F3", team2: "#FF9800"},
                labels={"ban_resistance": "밴 내성 점수"},
            )
            st.plotly_chart(fig_bar, use_container_width=True)

    else:
        if not st.button("전체 분석", key="g_all_run"):
            return
        with st.spinner("전체 팀 분석 중..."):
            results = _cached_all_team_profiles(season_id)

        import plotly.express as px
        import pandas as pd

        valid = [r for r in results if "error" not in r]
        if not valid:
            st.info("데이터 없음")
            return

        df = pd.DataFrame([{
            "팀": r["team"], "색깔": r["color_label"],
            "밴내성": r["ban_resistance_avg"],
            "15분골드": r["gold15_avg"],
            "오브젝트율": r["first_object_rate"],
        } for r in valid])

        fig = px.scatter(
            df, x="15분골드", y="밴내성",
            color="색깔", text="팀",
            title="팀 색깔 맵 (초반 압박 강도 × 밴 내성)",
            size="오브젝트율", size_max=30,
        )
        fig.update_traces(textposition="top center")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df)


# ─────────────────────────────────────────────
# H. 스페셜리스트 챔피언
# ─────────────────────────────────────────────
def _show_scenario_h_team(roster_fn, specialist_fn, teams, season_id):
    """팀 비교 모드 — 팀 선택 → 포지션별 행 (콜업 있는 포지션만 드롭다운) → 분석"""
    import pandas as pd
    import plotly.graph_objects as go

    POS_ORDER = ["top", "jng", "mid", "bot", "sup"]
    POS_LABEL = {"top": "TOP", "jng": "JNG", "mid": "MID", "bot": "BOT", "sup": "SUP"}

    # ── 팀 선택 ─────────────────────────────────────
    col1, col2 = st.columns(2)
    with col1:
        team_a = st.selectbox("팀 A", teams, key="h_team_a")
    with col2:
        team_b = st.selectbox("팀 B", [t for t in teams if t != team_a], key="h_team_b")

    # ── 로스터 로딩 ──────────────────────────────────
    with st.spinner("로스터 로딩 중..."):
        roster_a = roster_fn(team_a, season_id=season_id)
        roster_b = roster_fn(team_b, season_id=season_id)

    if "error" in roster_a:
        st.error(roster_a["error"]); return
    if "error" in roster_b:
        st.error(roster_b["error"]); return

    # ── 포지션별 선수 선택 UI ─────────────────────────
    # 콜업이 있는 포지션만 드롭다운, 없으면 텍스트
    st.divider()
    st.caption(f"**{team_a}** vs **{team_b}** — 포지션별 선수 선택 (콜업 있는 라인만 변경 가능)")

    selected: dict = {}  # pos -> (player_a, player_b)

    for pos in POS_ORDER:
        data_a = roster_a["roster"].get(pos, {})
        data_b = roster_b["roster"].get(pos, {})
        if not data_a and not data_b:
            continue

        all_a = data_a.get("starters", []) + data_a.get("callups", [])
        all_b = data_b.get("starters", []) + data_b.get("callups", [])

        col_pos, col_a, col_b = st.columns([1, 3, 3])
        col_pos.markdown(f"**{POS_LABEL[pos]}**")

        with col_a:
            if len(all_a) > 1:
                opts = [(p["player"], f"{p['player']} ({p['games']}경기)" +
                         (" · 콜업" if p in data_a.get("callups", []) else ""))
                        for p in all_a]
                sel_a = st.selectbox(
                    f"{pos}_a", [o[0] for o in opts],
                    format_func=lambda v, o=opts: next(lb for nm, lb in o if nm == v),
                    key=f"h_sel_{pos}_a", label_visibility="collapsed",
                )
            else:
                sel_a = all_a[0]["player"] if all_a else None
                st.write(sel_a or "없음")

        with col_b:
            if len(all_b) > 1:
                opts = [(p["player"], f"{p['player']} ({p['games']}경기)" +
                         (" · 콜업" if p in data_b.get("callups", []) else ""))
                        for p in all_b]
                sel_b = st.selectbox(
                    f"{pos}_b", [o[0] for o in opts],
                    format_func=lambda v, o=opts: next(lb for nm, lb in o if nm == v),
                    key=f"h_sel_{pos}_b", label_visibility="collapsed",
                )
            else:
                sel_b = all_b[0]["player"] if all_b else None
                st.write(sel_b or "없음")

        selected[pos] = (sel_a, sel_b)

    if not st.button("팀 비교 분석", key="h_team_run"):
        return

    # ── 포지션별 산포도 + 표 ──────────────────────────
    def _make_scatter(champs_a, champs_b, name_a, name_b, pos_label):
        color_a, color_b = "#2196F3", "#FF9800"
        fig = go.Figure()
        for champs, name, color in [(champs_a, name_a, color_a), (champs_b, name_b, color_b)]:
            if not champs:
                continue
            df = pd.DataFrame(champs)
            for is_spec, suffix, filled in [(True, "스페셜리스트", True), (False, "일반", False)]:
                sub = df[df["is_specialist"] == is_spec]
                if sub.empty:
                    continue
                fig.add_trace(go.Scatter(
                    x=sub["excess_wr"], y=sub["gold15_advantage"],
                    mode="markers+text",
                    text=sub["champion"], textposition="top center",
                    name=f"{name} ({suffix})",
                    marker=dict(
                        size=sub["games"] * 4, sizemode="diameter", sizemin=8,
                        color=color if filled else "rgba(0,0,0,0)",
                        line=dict(width=2, color=color),
                    ),
                ))
        fig.add_vline(x=0, line_dash="dash", line_color="gray", opacity=0.4)
        fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.4)
        fig.update_layout(
            title=f"{pos_label} — {name_a} vs {name_b}",
            xaxis_title="초과 승률 (LCK 평균 대비)",
            yaxis_title="15분 골드 우위 (골드)",
            height=450,
        )
        return fig

    def _make_table(champs, name):
        if not champs:
            st.caption("데이터 없음")
            return
        df = pd.DataFrame(champs)
        def _label(r):
            marks = ("⭐" if r["is_specialist"] else "") + ("🃏" if r.get("is_joker_pick") else "")
            return f"{marks} {r['champion']}".strip() if marks else r["champion"]
        df["챔피언"] = df.apply(_label, axis=1)
        df["LCK 점유율"] = df["joker_share"].apply(lambda x: f"{x:.0%}")
        df["강제픽"] = df["likely_forced_pick"].apply(lambda x: "⚠️" if x else "")
        disp = df[["챔피언", "games", "LCK 점유율", "player_wr", "excess_wr",
                    "gold15_advantage", "specialist_score", "강제픽"]].copy()
        disp.columns = ["챔피언", "경기수", "LCK점유율", "WR", "초과WR", "골드우위", "점수", "강제픽"]
        st.subheader(name)
        st.dataframe(disp, hide_index=True, use_container_width=True)

    for pos, (sel_a, sel_b) in selected.items():
        if not sel_a and not sel_b:
            continue
        st.divider()
        st.markdown(f"### {POS_LABEL[pos]}")
        with st.spinner(f"{POS_LABEL[pos]} 분석 중..."):
            r_a = specialist_fn(sel_a, season_id=season_id) if sel_a else {}
            r_b = specialist_fn(sel_b, season_id=season_id) if sel_b else {}
        champs_a = r_a.get("all_champions", []) if "error" not in r_a else []
        champs_b = r_b.get("all_champions", []) if "error" not in r_b else []

        st.plotly_chart(
            _make_scatter(champs_a, champs_b, sel_a or "없음", sel_b or "없음", POS_LABEL[pos]),
            use_container_width=True,
        )
        col5, col6 = st.columns(2)
        with col5:
            _make_table(champs_a, f"{sel_a} ({team_a})" if sel_a else team_a)
        with col6:
            _make_table(champs_b, f"{sel_b} ({team_b})" if sel_b else team_b)

    st.caption("⭐ = 스페셜리스트 / 🃏 = 조커 픽 / ⚠️ = 피어리스 강제픽 의심 / 채움 = 스페셜리스트, 테두리 = 일반")


def show_scenario_h(fn, players, player_positions, season_id=None, roster_fn=None, teams=None):
    st.title("H. 스페셜리스트 챔피언")

    with st.expander("📖 해석 가이드", expanded=False):
        st.markdown("""
**스페셜리스트란?** LCK 같은 라인 평균 대비 이 선수가 유독 잘하는 챔피언.

#### 산정 기준 (최소 3경기)
- **초과 승률 (excess_wr × 50)**: 선수 승률 - 라인 평균 승률
- **15분 골드 우위 (gold_adv / 200 × 30)**: 평균보다 골드를 더 잘 벌어옴
- **골드 안정성 (stab_adv / 100 × 20)**: 표준편차가 낮을수록 안정적

#### 점수 해석
- **specialist_score 60+**: 명확한 스페셜리스트 (의도적 픽 가치 있음)
- **30~60**: 평균 이상 (선택지 중 하나)
- **30 이하**: 큰 차이 없음

#### 차트 해석 (산점도)
- **X축(초과 승률)**: 오른쪽일수록 평균보다 잘함
- **Y축(15분 골드 우위)**: 위쪽일수록 라인전 우위
- **버블 크기**: 경기 수 (크면 표본 신뢰도↑)
- **오른쪽 위 영역**: 가장 이상적인 스페셜리스트 챔피언

#### 두 가지 스페셜리스트 유형
- **⭐ 수행 스페셜리스트**: LCK 같은 라인 평균 대비 잘함 (승률·골드·안정성 우위)
- **🃏 조커 픽**: 본인이 LCK에서 그 챔피언을 거의 독점 사용한 깜짝 카드
  - 조건: LCK 점유율 70%+ AND 표본별 승률 충족
    - 본인 1경기 → 승률 100% 필수 ("한 번 꺼냈는데 통한 깜짝 픽")
    - 본인 2경기+ → 승률 50%+ ("꾸준히 본인만 쓰는 픽")
  - 예: Faker 스몰더, Canyon 스카너 — 평소엔 안 쓰다가 결정적 순간에 꺼내는 픽
  - **메타 외 발굴자** — 다른 선수는 안 쓰지만 본인은 통하는 무기
- **⭐🃏 둘 다**: 희귀하면서 잘하기까지 — 강력한 깜짝 카드

#### 피어리스 보정 (⚠️ 강제픽 의심)
피어리스 시리즈 후반 경기(2/3경기)에서 픽한 비율이 50% 이상이면 **강제픽 가능성**:
- 1-2경기에서 주력 챔이 이미 사용됨 → 어쩔 수 없이 픽
- "스페셜리스트"가 아니라 "어쩔 수 없는 선택"일 수 있음
- 표에 ⚠️ 아이콘 표시

#### 활용
- 메타 챔피언 외에도 이 선수만의 무기 발견
- 상대 분석 시 "이 선수의 의외 픽" 대비
""")

    mode = st.radio("모드", ["개인 분석", "팀 비교"], horizontal=True, key="h_mode")

    if mode == "팀 비교":
        _show_scenario_h_team(roster_fn, fn, teams, season_id)
        return

    player, compare = _player_compare_ui(players, player_positions, "h")

    if not st.button("분석", key="h_run"):
        return

    with st.spinner("분석 중..."):
        r1 = fn(player, season_id=season_id)
        r2 = fn(compare, season_id=season_id) if compare else None

    if "error" in r1:
        st.error(r1["error"])
        return

    import plotly.express as px
    import plotly.graph_objects as go
    import pandas as pd

    all1 = r1.get("all_champions", r1["specialists"])
    if not all1:
        st.info("사용한 챔피언 없음 (최소 3게임 조건 미충족)")
        return

    st.caption(f"포지션: {r1['position']}")

    df1 = pd.DataFrame(all1)
    df1["선수"] = player

    if r2 and "error" not in r2 and r2.get("all_champions"):
        df2 = pd.DataFrame(r2["all_champions"])
        df2["선수"] = compare

        # 스페셜리스트(채움) vs 일반(테두리만) 분리해서 4개 trace로 그리기
        color_map = {player: "#2196F3", compare: "#FF9800"}
        fig = go.Figure()

        for name, df_p in [(player, df1), (compare, df2)]:
            for is_spec, suffix, fill_mode in [(True, "스페셜리스트", "fill"), (False, "일반", "open")]:
                sub = df_p[df_p["is_specialist"] == is_spec]
                if sub.empty:
                    continue
                marker_kwargs = dict(
                    size=sub["games"] * 4,
                    sizemode="diameter",
                    sizemin=8,
                    color=color_map[name],
                    line=dict(width=2, color=color_map[name]),
                )
                if fill_mode == "open":
                    marker_kwargs["color"] = "rgba(0,0,0,0)"
                fig.add_trace(go.Scatter(
                    x=sub["excess_wr"], y=sub["gold15_advantage"],
                    mode="markers+text",
                    text=sub["champion"], textposition="top center",
                    marker=marker_kwargs,
                    name=f"{name} ({suffix})",
                ))

        fig.add_vline(x=0, line_dash="dash", line_color="gray", opacity=0.4)
        fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.4)
        fig.update_layout(
            title=f"{player} vs {compare} — 챔피언 비교 (채움=스페셜리스트, 테두리=일반)",
            xaxis_title="초과 승률 (LCK 평균 대비)",
            yaxis_title="15분 골드 우위 (골드)",
        )
        st.plotly_chart(fig, use_container_width=True)

        st.divider()
        col1, col2 = st.columns(2)

        def _table(df, name):
            tmp = df.copy()
            def _champ_label(r):
                marks = ""
                if r["is_specialist"]: marks += "⭐"
                if r.get("is_joker_pick"): marks += "🃏"
                return f"{marks} {r['champion']}".strip() if marks else r["champion"]
            tmp["챔피언"] = tmp.apply(_champ_label, axis=1)
            tmp["강제픽 의심"] = tmp["likely_forced_pick"].apply(lambda x: "⚠️" if x else "")
            tmp["LCK 점유율"] = tmp["joker_share"].apply(lambda x: f"{x:.0%}")
            display = tmp[[
                "챔피언", "games", "lck_total_games", "LCK 점유율",
                "player_wr", "lck_avg_wr", "excess_wr",
                "gold15_advantage", "gold15_stability_advantage", "specialist_score",
                "fearless_late_games", "강제픽 의심",
            ]].copy()
            display.columns = [
                "챔피언", "게임수", "LCK 총사용", "LCK 점유율",
                "선수 WR", "LCK 평균 WR", "초과 WR",
                "골드 우위", "안정성 우위", "점수",
                "피어리스 후반픽", "강제픽 의심",
            ]
            st.subheader(name)
            st.dataframe(display, hide_index=True)

        with col1:
            _table(df1, player)
        with col2:
            _table(df2, compare)
        st.caption("⭐ = 수행 스페셜리스트 (평균 대비 잘함) / 🃏 = 조커 픽 (LCK 70%+ 독점, 1경기는 승률 100%·2경기+는 50%+) / ⚠️ = 피어리스 후반 픽 50%+ (강제 픽 의심)")

    else:
        # 단일 선수
        fig = px.scatter(
            df1, x="excess_wr", y="gold15_advantage",
            size="games", text="champion",
            color="gold15_stability_advantage",
            color_continuous_scale="RdYlGn",
            title=f"{player} — 스페셜리스트 챔피언",
            labels={
                "excess_wr": "초과 승률 (LCK 평균 대비)",
                "gold15_advantage": "15분 골드 우위 (골드)",
                "gold15_stability_advantage": "골드 안정성 우위",
            },
        )
        fig.add_vline(x=0, line_dash="dash", line_color="gray", opacity=0.4)
        fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.4)
        fig.update_traces(textposition="top center")
        fig.update_layout(coloraxis_colorbar=dict(title="골드 안정성<br>우위 (골드)"))
        st.plotly_chart(fig, use_container_width=True)
        st.caption("색이 초록일수록 평균보다 골드 변동이 적음 / 버블 크기 = 플레이 횟수")

        df1_disp = df1.copy()
        def _champ_label(r):
            marks = ""
            if r["is_specialist"]: marks += "⭐"
            if r.get("is_joker_pick"): marks += "🃏"
            return f"{marks} {r['champion']}".strip() if marks else r["champion"]
        df1_disp["챔피언"] = df1_disp.apply(_champ_label, axis=1)
        df1_disp["강제픽 의심"] = df1_disp["likely_forced_pick"].apply(lambda x: "⚠️" if x else "")
        df1_disp["LCK 점유율"] = df1_disp["joker_share"].apply(lambda x: f"{x:.0%}")
        display_df = df1_disp[[
            "챔피언", "games", "lck_total_games", "LCK 점유율",
            "player_wr", "lck_avg_wr", "excess_wr",
            "player_gold15", "lck_avg_gold15", "gold15_advantage",
            "player_gold15_stddev", "lck_avg_gold15_stddev", "gold15_stability_advantage",
            "specialist_score",
            "fearless_late_games", "강제픽 의심",
        ]].copy()
        display_df.columns = [
            "챔피언", "게임수", "LCK 총사용", "LCK 점유율",
            "선수 승률", "LCK 평균 승률", "초과 승률",
            "선수 골드15", "LCK 평균 골드15", "골드 우위",
            "선수 골드 표준편차", "LCK 평균 표준편차", "안정성 우위", "스페셜리스트 점수",
            "피어리스 후반픽", "강제픽 의심",
        ]
        st.dataframe(display_df, hide_index=True)
        st.caption("⭐ = 수행 스페셜리스트 / 🃏 = 조커 픽 (LCK 70%+ 독점, 1경기는 승률 100%·2경기+는 50%+) / ⚠️ = 피어리스 후반 픽 50%+ (강제 픽 의심)")


# ─────────────────────────────────────────────
# 메인 렌더링
# ─────────────────────────────────────────────
try:
    fns   = load_fns()
    lists = load_lists(season_id)

    if page == "🏠 홈":
        show_home()
    elif page == "A. 밴 시 승률 영향":
        show_scenario_a(fns["ban_impact"], lists["players"],
                        lists["player_positions"], season_id)
    elif page == "B. 진영별 챔피언 성향":
        show_scenario_b(fns["side_pref"], lists["players"],
                        lists["player_positions"], season_id)
    elif page == "C. 패치 적응 속도":
        show_scenario_c(fns["meta_adapt"])
    elif page == "D. 저격 밴 패턴":
        show_scenario_d(fns["snipe_ban"], lists["teams"], season_id)
    elif page == "E. 패치별 승리 공식":
        show_scenario_e(fns["win_formula"], lists["patches"])
    elif page == "F. 밴 내성 지수":
        show_scenario_f(fns["ban_resistance"], lists["players"],
                        lists["player_positions"], season_id)
    elif page == "G. 팀 색깔 프로파일":
        show_scenario_g(fns["team_profile"], fns["all_team_profiles"],
                        lists["teams"], season_id)
    elif page == "H. 스페셜리스트 챔피언":
        show_scenario_h(fns["specialist"], lists["players"],
                        lists["player_positions"], season_id,
                        roster_fn=fns["team_roster"], teams=lists["teams"])

except Exception as e:
    st.error(f"DB 연결 실패: {e}")
    st.info("`.env` 파일에 DB 접속 정보를 확인하고 ETL을 먼저 실행하세요.")
    st.code("""
# .env 예시
DB_HOST=localhost
DB_PORT=5432
DB_NAME=lck
DB_USER=postgres
DB_PASSWORD=yourpassword
    """)
