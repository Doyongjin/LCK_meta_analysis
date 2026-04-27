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
    from analysis.scenario_h import get_specialist_champions
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
def show_home():
    st.title("LCK Victory Formula & Meta Analysis")
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
                        "저격 밴율",
                        f"{champ['snipe_ban_rate']:.1%}",
                        f"{champ['snipe_ban_count']}/{champ['total_games_available']}경기"
                    )
                    if champ["opponents"]:
                        opps = ", ".join(
                            [f"{o['team']}({o['count']})" for o in champ["opponents"][:3]]
                        )
                        st.caption(f"주요 저격팀: {opps}")
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
def show_scenario_f(fn, players, teams, player_positions, season_id=None):
    st.title("F. 밴 내성 지수")

    col1, col2 = st.columns(2)
    with col1:
        player = st.selectbox("선수 선택", players, key="f_p1")
        team1 = st.selectbox("소속 팀 (선택)", ["(없음)"] + teams, key="f_t1")
    with col2:
        pos = player_positions.get(player, "")
        same = _same_pos_options(player, players, player_positions)
        compare_raw = st.selectbox(f"비교 선수 ({pos} 포지션)",
                                   ["(없음)"] + same, key="f_p2")
        compare = None if compare_raw == "(없음)" else compare_raw
        team2 = st.selectbox("비교 선수 팀 (선택)", ["(없음)"] + teams, key="f_t2")

    team_name1 = None if team1 == "(없음)" else team1
    team_name2 = None if team2 == "(없음)" else team2

    if not st.button("분석", key="f_run"):
        return

    import plotly.graph_objects as go

    def _norm(val, best, worst):
        if best == worst:
            return 50.0
        return max(0.0, min(100.0, (val - worst) / (best - worst) * 100))

    def _radar_vals(result, avg):
        avg_vdelta = avg.get("gold15_volatility_delta", 300.0)
        return [
            _norm(result["champ_pool_size"], avg.get("champ_pool_size", 5) * 2, 0),
            _norm(result["primary_dependency"], 0, avg.get("primary_dependency", 0.35) * 2),
            _norm(result["wr_drop_when_banned"], 0, avg.get("wr_drop_when_banned", 0.1) * 2),
            _norm(result["gold15_volatility_delta"], 0, avg_vdelta * 2),
            _norm(result["champ_pool_size"], avg.get("champ_pool_size", 5) * 2, 0),  # 닫힘
        ]

    def _metrics(result, label):
        st.caption(f"**{label}** — 밴 내성: {result['ban_resistance_score']:.1f}/100")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("챔피언 풀", f"{result['champ_pool_size']}개")
        m2.metric("밴 시 승률 하락", f"{result['wr_drop_when_banned']:.1%}")
        delta_str = f"+{result['gold15_volatility_delta']:.0f}" if result["gold15_volatility_delta"] > 0 else "0"
        m3.metric("골드 변동성 증가", f"{result['gold15_volatility_delta']:.0f}골드",
                  delta=delta_str, delta_color="inverse")
        m4.metric("주력 챔피언", result["primary_champion"] or "-")

    with st.spinner("분석 중..."):
        r1 = fn(player, team_name1, season_id=season_id)
        r2 = fn(compare, team_name2, season_id=season_id) if compare else None

    if "error" in r1:
        st.error(r1["error"])
        return

    categories = ["챔피언 풀", "주력 의존도↓", "밴 시 승률 유지", "골드 안정성", "챔피언 풀"]
    avg = r1["lck_avg_benchmark"]

    _metrics(r1, player)
    if r2 and "error" not in r2:
        _metrics(r2, compare)

    with st.expander("Gold@15 변동성 상세"):
        cx1, cx2 = st.columns(2)
        cx1.metric(f"{r1['primary_champion']} 픽 시 팀 골드 표준편차",
                   f"{r1['gold15_stddev_normal']:.0f}골드")
        cx2.metric("밴됐을 때 팀 골드 표준편차",
                   f"{r1['gold15_stddev_banned']:.0f}골드",
                   delta=f"+{r1['gold15_volatility_delta']:.0f}" if r1["gold15_volatility_delta"] > 0 else "변화 없음",
                   delta_color="inverse")
        st.caption("표준편차가 높을수록 팀 전술이 흔들리는 신호")

    # 레이더 차트 (비교 시 오버레이)
    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=_radar_vals(r1, avg), theta=categories,
        fill="toself", name=player, line=dict(color="#2196F3"),
    ))
    if r2 and "error" not in r2:
        fig.add_trace(go.Scatterpolar(
            r=_radar_vals(r2, r2["lck_avg_benchmark"]), theta=categories,
            fill="toself", name=compare, line=dict(color="#FF9800"),
            fillcolor="rgba(255,152,0,0.15)",
        ))
    avg_vdelta = avg.get("gold15_volatility_delta", 300.0)
    fig.add_trace(go.Scatterpolar(
        r=[50, 50, 50, _norm(avg_vdelta, 0, avg_vdelta * 2), 50],
        theta=categories, fill="toself", name="LCK 평균",
        line=dict(dash="dash", color="rgba(150,150,150,0.6)"),
        fillcolor="rgba(200,200,200,0.2)",
    ))
    fig.update_layout(
        polar=dict(radialaxis=dict(range=[0, 100], showticklabels=False)),
        legend=dict(x=0.85, y=1.1),
    )
    st.plotly_chart(fig, use_container_width=True)


# ─────────────────────────────────────────────
# G. 팀 색깔 프로파일
# ─────────────────────────────────────────────
def show_scenario_g(fn_single, fn_all, teams, season_id=None):
    st.title("G. 팀 색깔 프로파일")
    mode = st.radio("모드", ["두 팀 비교", "전체 팀 분포"])

    if mode == "두 팀 비교":
        col1, col2 = st.columns(2)
        with col1:
            team1 = st.selectbox("팀 1", teams, key="g_t1")
        with col2:
            team2 = st.selectbox("팀 2", teams, key="g_t2",
                                 index=min(1, len(teams) - 1))

        if not st.button("분석", key="g_run"):
            return

        with st.spinner("분석 중..."):
            r1 = fn_single(team1, season_id)
            r2 = fn_single(team2, season_id)

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
            results = fn_all(season_id)

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
def show_scenario_h(fn, players, player_positions, season_id=None):
    st.title("H. 스페셜리스트 챔피언")

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

    specialists1 = r1["specialists"]
    if not specialists1:
        st.info("스페셜리스트 챔피언 없음 (최소 3게임 + LCK 평균 초과 조건 미충족)")
        return

    st.caption(f"포지션: {r1['position']}")

    df1 = pd.DataFrame(specialists1)
    df1["선수"] = player

    if r2 and "error" not in r2 and r2["specialists"]:
        df2 = pd.DataFrame(r2["specialists"])
        df2["선수"] = compare
        df_all = pd.concat([df1, df2], ignore_index=True)

        # 두 선수 오버레이 버블 차트
        fig = px.scatter(
            df_all, x="excess_wr", y="gold15_advantage",
            size="games", text="champion",
            color="선수",
            color_discrete_map={player: "#2196F3", compare: "#FF9800"},
            symbol="선수",
            title=f"{player} vs {compare} — 스페셜리스트 챔피언 비교",
            labels={
                "excess_wr": "초과 승률 (LCK 평균 대비)",
                "gold15_advantage": "15분 골드 우위 (골드)",
            },
            size_max=40,
        )
        fig.add_vline(x=0, line_dash="dash", line_color="gray", opacity=0.4)
        fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.4)
        fig.update_traces(textposition="top center")
        st.plotly_chart(fig, use_container_width=True)

        st.divider()
        col1, col2 = st.columns(2)

        def _table(df, name):
            display = df[[
                "champion", "games", "player_wr", "lck_avg_wr", "excess_wr",
                "gold15_advantage", "gold15_stability_advantage", "specialist_score",
            ]].copy()
            display.columns = [
                "챔피언", "게임수", "선수 WR", "LCK 평균 WR", "초과 WR",
                "골드 우위", "안정성 우위", "점수",
            ]
            st.subheader(name)
            st.dataframe(display, hide_index=True)

        with col1:
            _table(df1, player)
        with col2:
            _table(df2, compare)

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

        display_df = df1[[
            "champion", "games", "player_wr", "lck_avg_wr", "excess_wr",
            "player_gold15", "lck_avg_gold15", "gold15_advantage",
            "player_gold15_stddev", "lck_avg_gold15_stddev", "gold15_stability_advantage",
            "specialist_score",
        ]].copy()
        display_df.columns = [
            "챔피언", "게임수", "선수 승률", "LCK 평균 승률", "초과 승률",
            "선수 골드15", "LCK 평균 골드15", "골드 우위",
            "선수 골드 표준편차", "LCK 평균 표준편차", "안정성 우위", "스페셜리스트 점수",
        ]
        st.dataframe(display_df, hide_index=True)


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
        show_scenario_f(fns["ban_resistance"], lists["players"], lists["teams"],
                        lists["player_positions"], season_id)
    elif page == "G. 팀 색깔 프로파일":
        show_scenario_g(fns["team_profile"], fns["all_team_profiles"],
                        lists["teams"], season_id)
    elif page == "H. 스페셜리스트 챔피언":
        show_scenario_h(fns["specialist"], lists["players"],
                        lists["player_positions"], season_id)

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
