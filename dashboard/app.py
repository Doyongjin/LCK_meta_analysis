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
            "I. 저격 밴 실효성",
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
    from analysis.scenario_i import get_snipe_effectiveness
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
        "snipe_effectiveness": get_snipe_effectiveness,
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
| I | 저격 밴 실효성 (D × A 교차 검증) |

---

## 목적별 추천 조합

### 상대 팀 드래프트 준비
> D → I → F 순서로 확인

1. **D** 어떤 선수의 어떤 챔피언이 자주 밴되는가
2. **I** 그 챔피언을 실제로 밴했을 때 상대 팀이 졌는가
3. **F** 해당 선수의 밴 내성이 낮은가 — 낮으면 효과가 팀 전체로 전파

세 지표가 모두 같은 방향 → **밴 우선순위 1순위**

---

### 상대 선수 개인 분석
> H → D → A 순서로 확인

1. **H** 어떤 챔피언에서 LCK 평균 대비 특출난가 (스페셜리스트 · 조커픽)
2. **D** 다른 팀들이 이미 그 챔피언을 밴하고 있는가
3. **A** 실제로 밴됐을 때 해당 선수 팀 승률이 떨어지는가

---

### 우리 팀 자가 진단
> F → G → B 순서로 확인

1. **F** 선수별 밴 압박 취약도 서열 파악
2. **G** 팀 색깔 — 캐리 의존형이면 저격 밴에 구조적으로 약함
3. **B** 레드 진영에서 챔피언 풀이 좁은 선수 파악 → 진영 선택 시 참고

---

### 현재 패치 메타 파악
> E → C → H 순서로 확인

1. **E** 이번 패치에서 오브젝트·골드 중 무엇이 승리와 더 연관되는가
2. **C** 상대 팀의 메타 챔피언 채택 속도
3. **H** 조커픽(LCK 점유율 70%+) — 메타와 무관하게 특정 선수가 독점하는 챔피언

---

## 지표 신뢰도 순서

```
D (단순 카운팅) > B > H (N≥10) > I 게임WR > F 세부지표 > G 레이더 > I 시리즈WR
```

수치가 복잡할수록 표본 의존도가 높아집니다.
**여러 지표가 같은 결론을 가리킬 때** 행동 근거로 삼는 것이 가장 안전합니다.
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

        # Role Priority
        st.divider()
        st.subheader("포지션별 픽 우선순위")
        st.caption("평균 픽 순서 (1 = 가장 먼저, 5 = 가장 나중) — 낮을수록 해당 포지션을 드래프트 축으로 삼는 경향")

        from analysis.scenario_g import get_role_priority
        rp1 = get_role_priority(team1, season_id)
        rp2 = get_role_priority(team2, season_id)

        POS_KR = {"top": "탑", "jng": "정글", "mid": "미드", "bot": "원딜", "sup": "서폿"}
        POS_ORDER_G = ["top", "jng", "mid", "bot", "sup"]

        if "error" not in rp1 and "error" not in rp2:
            # 막대 차트 — 두 팀 나란히
            rp_data = []
            for pos in POS_ORDER_G:
                v1 = rp1["roles"][pos]["avg_pick_order"]
                v2 = rp2["roles"][pos]["avg_pick_order"]
                if v1 is not None:
                    rp_data.append({"포지션": POS_KR[pos], "평균 픽 순서": v1, "팀": team1})
                if v2 is not None:
                    rp_data.append({"포지션": POS_KR[pos], "평균 픽 순서": v2, "팀": team2})

            if rp_data:
                fig_rp = px.bar(
                    pd.DataFrame(rp_data),
                    x="포지션", y="평균 픽 순서", color="팀",
                    barmode="group",
                    color_discrete_map={team1: "#2196F3", team2: "#FF9800"},
                    category_orders={"포지션": [POS_KR[p] for p in POS_ORDER_G]},
                    text="평균 픽 순서",
                )
                fig_rp.update_traces(texttemplate="%{text:.1f}", textposition="outside")
                fig_rp.update_layout(
                    yaxis=dict(range=[0, 6], title="평균 픽 순서"),
                    height=320,
                    margin=dict(t=20, b=20),
                )
                st.plotly_chart(fig_rp, use_container_width=True, key="g_rp_bar")

            # 픽 순서 분포 테이블 — 팀1 / 팀2 나란히
            tc1, tc2 = st.columns(2)
            for col, rp, tname in [(tc1, rp1, team1), (tc2, rp2, team2)]:
                with col:
                    st.caption(tname)
                    tbl = {"포지션": [], "평균": [], "1픽": [], "2픽": [], "3픽": [], "4픽": [], "5픽": []}
                    for pos in POS_ORDER_G:
                        d = rp["roles"][pos]
                        tbl["포지션"].append(POS_KR[pos])
                        tbl["평균"].append(f"{d['avg_pick_order']:.1f}" if d["avg_pick_order"] else "-")
                        for i in range(1, 6):
                            cnt = d["pick_counts"].get(i, 0)
                            pct = cnt / d["total"] if d["total"] > 0 else 0
                            tbl[f"{i}픽"].append(f"{pct:.0%}" if cnt > 0 else "-")
                    st.dataframe(pd.DataFrame(tbl), hide_index=True)

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


def _generate_h_scenario_pdf(player_name, position, results_df):
    """H 시나리오 분석 결과를 PDF로 생성. 테이블 중심 레이아웃."""
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import cm
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from io import BytesIO
    from datetime import datetime

    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=landscape(A4),
        rightMargin=1*cm, leftMargin=1*cm,
        topMargin=1*cm, bottomMargin=1*cm,
    )

    story = []
    styles = getSampleStyleSheet()

    # 제목
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=18,
        textColor=colors.HexColor('#1f77b4'),
        spaceAfter=6,
    )
    story.append(Paragraph(f"H. 스페셜리스트 챔피언 분석 — {player_name} ({position})", title_style))
    story.append(Paragraph(f"생성일: {datetime.now().strftime('%Y-%m-%d %H:%M')}", styles['Normal']))
    story.append(Spacer(1, 0.3*cm))

    # 테이블 데이터 준비
    if len(results_df) > 0:
        # 각 컬럼을 문자열로 변환하고 지표 포맷팅
        table_data = [
            ["챔피언", "경기", "LCK총", "점유율", "선수WR", "LCK평균", "초과WR", "골드우위", "안정성", "점수", "강제픽"],
        ]

        for _, row in results_df.iterrows():
            champ = row.get('champion', '')
            marks = ""
            if row.get('is_specialist'):
                marks += "⭐"
            if row.get('is_joker_pick'):
                marks += "🃏"
            if row.get('likely_forced_pick'):
                marks += "⚠️"
            champ_label = f"{marks} {champ}".strip() if marks else str(champ)

            table_data.append([
                champ_label,
                str(int(row.get('games', 0))),
                str(int(row.get('lck_total_games', 0))),
                f"{row.get('joker_share', 0):.0%}",
                f"{row.get('player_wr', 0):.1%}",
                f"{row.get('lck_avg_wr', 0):.1%}",
                f"{row.get('excess_wr', 0):.1%}",
                f"{row.get('gold15_advantage', 0):.0f}",
                f"{row.get('gold15_stability_advantage', 0):.0f}",
                f"{row.get('specialist_score', 0):.1f}",
                "⚠️" if row.get('likely_forced_pick') else "",
            ])

        # 테이블 스타일
        table = Table(table_data, colWidths=[2.2*cm, 0.9*cm, 1.1*cm, 1.2*cm, 1.1*cm, 1.2*cm, 1.2*cm, 1.2*cm, 1.1*cm, 1.1*cm, 0.9*cm])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#e6f2ff')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('ALIGN', (0, 0), (0, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 9),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f9f9f9')]),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ]))
        story.append(table)
    else:
        story.append(Paragraph("사용한 챔피언 없음 (최소 3게임 조건 미충족)", styles['Normal']))

    story.append(Spacer(1, 0.5*cm))

    # 범례
    legend = Paragraph(
        "⭐ = 수행 스페셜리스트 (평균 대비 잘함) | "
        "🃏 = 조커 픽 (LCK 70%+ 독점, 1경기는 승률 100%, 2경기+는 50%+) | "
        "⚠️ = 피어리스 후반 픽 의심 (50%+ 강제픽)",
        ParagraphStyle('Legend', parent=styles['Normal'], fontSize=8, textColor=colors.grey)
    )
    story.append(legend)

    doc.build(story)
    buffer.seek(0)
    return buffer.getvalue()


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

        # PDF 다운로드 (비교 모드)
        col_pdf1, col_pdf2 = st.columns(2)
        with col_pdf1:
            pdf_data1 = _generate_h_scenario_pdf(player, r1["position"], df1)
            st.download_button(
                label="📥 PDF 다운로드",
                data=pdf_data1,
                file_name=f"H_스페셜리스트_{player}_{r1['position']}.pdf",
                mime="application/pdf",
                key="pdf_p1"
            )
        with col_pdf2:
            pdf_data2 = _generate_h_scenario_pdf(compare, r2["position"], df2)
            st.download_button(
                label="📥 PDF 다운로드",
                data=pdf_data2,
                file_name=f"H_스페셜리스트_{compare}_{r2['position']}.pdf",
                mime="application/pdf",
                key="pdf_p2"
            )

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

        # PDF 다운로드 버튼
        pdf_data = _generate_h_scenario_pdf(player, r1["position"], df1)
        st.download_button(
            label="📥 PDF 다운로드",
            data=pdf_data,
            file_name=f"H_스페셜리스트_{player}_{r1['position']}.pdf",
            mime="application/pdf"
        )


# ─────────────────────────────────────────────
# I. 저격 밴 실효성
# ─────────────────────────────────────────────
@st.cache_data(ttl=3600)
def _cached_snipe_effectiveness(team_name, season_id):
    from analysis.scenario_i import get_snipe_effectiveness
    return get_snipe_effectiveness(team_name, season_id)


def show_scenario_i(_fn, teams, season_id=None):
    st.title("I. 저격 밴 실효성")

    with st.expander("📖 해석 가이드", expanded=False):
        st.markdown("""
**이 분석이 보여주는 것**: "상대가 우리 선수의 주력 챔피언을 밴했을 때 실제로 우리 팀이 졌는가?"

상관관계(저격 밴이 많다)에서 한 단계 나아가, **밴의 실제 효과**를 게임·시리즈 단위로 측정합니다.

#### 지표 해석
| 지표 | 의미 |
|------|------|
| **게임 승률 차이** | 밴 안 됐을 때 승률 − 밴됐을 때 승률. **양수가 클수록** 그 챔피언 밴이 효과적 |
| **시리즈 승률 차이** | 저격 밴이 1회 이상 있었던 시리즈 vs 없었던 시리즈의 팀 승률 차이 |
| **⚠️ 샘플 부족** | 밴된 경기가 10경기 미만 — 방향성만 참고, 수치 신뢰 금지 |

#### 해석 시 주의사항
- **양수 = 밴이 효과적**: 해당 챔피언이 없을 때 팀 승률이 낮다 → 상대 입장에서 밴할 가치 있음
- **음수 = 밴이 역효과**: 해당 챔피언이 없어도 팀이 잘 이김 → 해당 챔피언은 저격 가치가 낮음
- **게임 WR 차이 vs 시리즈 WR 차이**: 게임 단위는 개별 경기 성과, 시리즈 단위는 실제 승강 결과와 직결
- **Selection bias 주의**: 강팀 상대할 때 밴이 더 집중되는 경향 → 밴됐을 때 승률이 낮은 이유가 상대팀 강도일 수 있음

#### 시즌 필터 선택에 따른 한계

현재 사이드바에서 **한 시즌** 또는 **전체**만 선택할 수 있어 다음 문제가 생깁니다.

| 선택 | 문제 |
|------|------|
| **한 시즌** | LCK 한 스플릿은 팀당 14~18시리즈. 특정 챔피언이 저격 밴된 시리즈는 많아야 3~8개 → **시리즈 WR 차이가 0% 또는 극단값으로 나올 가능성 높음** |
| **전체** | 시즌이 섞여 메타·선수 이적이 혼재 → **과거 데이터가 현재 판단을 왜곡할 수 있음** |

**실용적 권장 사용법**:
- **게임 WR 차이** → 한 시즌 필터로 봐도 어느 정도 표본 확보 가능 (경기 수 > 시리즈 수)
- **시리즈 WR 차이** → 전체 또는 2~3시즌을 묶어야 패턴이 나타남. 한 시즌 결과가 0%나 100%면 표본 부족 신호
- 두 지표가 **같은 방향**을 가리킬 때만 신뢰도 있는 패턴으로 판단
""")

    col1, col2 = st.columns(2)
    with col1:
        team = st.selectbox("팀 선택", teams, key="i_team")
    with col2:
        compare_raw = st.selectbox("비교 팀 (선택)", ["(없음)"] + [t for t in teams if t != team], key="i_compare")
        compare_team = None if compare_raw == "(없음)" else compare_raw

    if not st.button("분석", key="i_run"):
        return

    _POS_ORDER_I = ["top", "jng", "mid", "bot", "sup"]
    _POS_LABEL_I = {"top": "탑", "jng": "정글", "mid": "미드", "bot": "원딜", "sup": "서폿"}

    def _render_champ(champ, key_prefix=""):
        import plotly.graph_objects as go
        c_icon, c_info = st.columns([1, 5])
        with c_icon:
            if champ["icon_url"]:
                st.image(champ["icon_url"], width=48)
            st.caption(champ["champion"])
        with c_info:
            if champ["low_sample"]:
                st.warning(f"⚠️ 밴된 경기 {champ['banned_games']}경기 — 샘플 부족, 방향성만 참고")

            bwr = champ["banned_game_wr"]
            nwr = champ["normal_game_wr"]
            delta = champ["game_wr_delta"]

            if bwr is not None and nwr is not None:
                fig = go.Figure()
                fig.add_bar(
                    x=["밴됐을 때", "픽했을 때"],
                    y=[bwr * 100, nwr * 100],
                    marker_color=["#EF553B", "#00CC96"],
                    text=[f"{bwr:.1%}<br>({champ['banned_games']}경기)",
                          f"{nwr:.1%}<br>({champ['normal_games']}경기)"],
                    textposition="outside",
                )
                delta_sign = "+" if delta > 0 else ""
                fig.update_layout(
                    title=f"게임 승률 비교 | 차이: {delta_sign}{delta:.1%}",
                    yaxis=dict(range=[0, 110], ticksuffix="%"),
                    height=240,
                    margin=dict(t=40, b=10, l=20, r=20),
                    showlegend=False,
                )
                st.plotly_chart(fig, use_container_width=True,
                                key=f"i_chart_{key_prefix}_{champ['champion']}")
            else:
                st.caption("게임 승률 비교 데이터 부족")

            swr_snipe = champ["snipe_series_wr"]
            swr_normal = champ["normal_series_wr"]
            s_delta = champ["series_wr_delta"]
            sc1, sc2, sc3 = st.columns(3)
            sc1.metric(
                "저격 시리즈 승률",
                f"{swr_snipe:.1%}" if swr_snipe is not None else "N/A",
                f"{champ['snipe_series']}시리즈",
            )
            sc2.metric(
                "비저격 시리즈 승률",
                f"{swr_normal:.1%}" if swr_normal is not None else "N/A",
                f"{champ['total_series'] - champ['snipe_series']}시리즈",
            )
            sc3.metric(
                "시리즈 WR 차이",
                f"+{s_delta:.1%}" if s_delta and s_delta > 0
                else (f"{s_delta:.1%}" if s_delta is not None else "N/A"),
                "양수 = 밴이 효과적",
                delta_color="normal" if s_delta and s_delta > 0 else "inverse",
            )

    def _render_team(result):
        if "error" in result:
            st.error(result["error"])
            return

        import pandas as pd

        # 포지션 순서로 정렬
        pos_order_map = {p: i for i, p in enumerate(_POS_ORDER_I)}
        players_sorted = sorted(
            result["players"],
            key=lambda x: pos_order_map.get(x["position"], 99)
        )

        # 요약 테이블
        summary = result.get("summary", [])
        if summary:
            df_sum = pd.DataFrame(summary)
            df_sum["게임 WR 차이"] = df_sum["game_wr_delta"].apply(
                lambda x: f"+{x:.1%}" if x > 0 else f"{x:.1%}"
            )
            df_sum["시리즈 WR 차이"] = df_sum["series_wr_delta"].apply(
                lambda x: f"+{x:.1%}" if x is not None and x > 0
                else (f"{x:.1%}" if x is not None else "N/A")
            )
            df_sum["샘플"] = df_sum["low_sample"].apply(lambda x: "⚠️" if x else "")
            st.subheader("요약 — 밴 효과 순위")
            st.caption("게임 WR 차이 기준 내림차순 (양수 = 밴이 효과적)")
            st.dataframe(
                df_sum[["player", "champion", "banned_games", "snipe_series",
                         "게임 WR 차이", "시리즈 WR 차이", "샘플"]].rename(columns={
                    "player": "선수", "champion": "챔피언",
                    "banned_games": "밴된 게임", "snipe_series": "저격 시리즈",
                }),
                hide_index=True,
            )

        # 포지션별 섹션
        for player_data in players_sorted:
            pos = player_data["position"]
            pos_label = _POS_LABEL_I.get(pos, pos)
            st.markdown(f"### {pos_label} · {player_data['player']}")
            champs = player_data["champions"]
            if not champs:
                st.caption("데이터 없음")
                st.divider()
                continue
            for idx, champ in enumerate(champs):
                _render_champ(champ, key_prefix=f"single_{pos}_{idx}")
            st.divider()

    def _render_compare(r1, r2, name1, name2):
        """두 팀을 포지션 단위로 행 분할해서 정렬 유지"""
        import pandas as pd

        # 요약 테이블 — 좌우 나란히
        st.subheader("요약 — 밴 효과 순위")
        st.caption("게임 WR 차이 기준 내림차순 (양수 = 밴이 효과적)")
        ca, cb = st.columns(2)

        def _summary_df(result):
            summary = result.get("summary", [])
            if not summary:
                return None
            df = pd.DataFrame(summary)
            df["게임 WR 차이"] = df["game_wr_delta"].apply(
                lambda x: f"+{x:.1%}" if x > 0 else f"{x:.1%}"
            )
            df["시리즈 WR 차이"] = df["series_wr_delta"].apply(
                lambda x: f"+{x:.1%}" if x is not None and x > 0
                else (f"{x:.1%}" if x is not None else "N/A")
            )
            df["샘플"] = df["low_sample"].apply(lambda x: "⚠️" if x else "")
            return df[["player", "champion", "banned_games", "snipe_series",
                        "게임 WR 차이", "시리즈 WR 차이", "샘플"]].rename(columns={
                "player": "선수", "champion": "챔피언",
                "banned_games": "밴된 게임", "snipe_series": "저격 시리즈",
            })

        with ca:
            st.caption(name1)
            df1 = _summary_df(r1)
            if df1 is not None:
                st.dataframe(df1, hide_index=True)
            else:
                st.caption("데이터 없음")
        with cb:
            st.caption(name2)
            df2 = _summary_df(r2)
            if df2 is not None:
                st.dataframe(df2, hide_index=True)
            else:
                st.caption("데이터 없음")

        st.divider()

        # 포지션별로 행 단위 렌더링 — 같은 포지션이 항상 같은 높이에서 시작
        pos_order_map = {p: i for i, p in enumerate(_POS_ORDER_I)}
        players1 = sorted(r1["players"], key=lambda x: pos_order_map.get(x["position"], 99))
        players2 = sorted(r2["players"], key=lambda x: pos_order_map.get(x["position"], 99))
        p1_map = {p["position"]: p for p in players1}
        p2_map = {p["position"]: p for p in players2}

        all_positions = sorted(
            set(p1_map) | set(p2_map),
            key=lambda x: pos_order_map.get(x, 99)
        )

        for pos in all_positions:
            pos_label = _POS_LABEL_I.get(pos, pos)
            pd1 = p1_map.get(pos)
            pd2 = p2_map.get(pos)

            # 포지션 행 헤더
            hc1, hc2 = st.columns(2)
            with hc1:
                name_str = f" · {pd1['player']}" if pd1 else ""
                st.markdown(f"### {pos_label}{name_str}")
            with hc2:
                name_str = f" · {pd2['player']}" if pd2 else ""
                st.markdown(f"### {pos_label}{name_str}")

            # 챔피언별 — 같은 인덱스끼리 같은 행에
            champs1 = pd1["champions"] if pd1 else []
            champs2 = pd2["champions"] if pd2 else []
            max_champs = max(len(champs1), len(champs2))

            for i in range(max_champs):
                cc1, cc2 = st.columns(2)
                with cc1:
                    if i < len(champs1):
                        _render_champ(champs1[i], key_prefix=f"cmp1_{pos}_{i}")
                with cc2:
                    if i < len(champs2):
                        _render_champ(champs2[i], key_prefix=f"cmp2_{pos}_{i}")

            st.divider()

    with st.spinner("분석 중..."):
        r1 = _cached_snipe_effectiveness(team, season_id)
        r2 = _cached_snipe_effectiveness(compare_team, season_id) if compare_team else None

    if r2:
        if "error" in r1:
            st.error(r1["error"])
        elif "error" in r2:
            st.error(r2["error"])
        else:
            _render_compare(r1, r2, team, compare_team)
    else:
        _render_team(r1)


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
    elif page == "I. 저격 밴 실효성":
        show_scenario_i(fns["snipe_effectiveness"], lists["teams"], season_id)

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
