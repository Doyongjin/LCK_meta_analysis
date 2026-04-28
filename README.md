# LCK Victory Formula & Meta Analysis Tool

> "누가 이겼나"가 아닌 **"어떤 메타적 선택이 승리를 이끌었나"**

LCK 경기 데이터를 기반으로 패치별 메타, 팀 전략, 선수 성향을 분석하는 대시보드입니다.

**라이브 대시보드**: [lckmetaanalysis.streamlit.app](https://lckmetaanalysis-b7qzcpdqzyxqmtrukcbqwn.streamlit.app)

---

## 분석 시나리오

| 시나리오 | 설명 |
|---------|------|
| A. 밴 시 승률 영향 | 선수 주력 챔피언이 밴됐을 때 팀 승률·골드 변화 |
| B. 진영별 챔피언 성향 | 블루/레드 진영별 선수 픽 패턴 차이 |
| C. 패치 적응 속도 | 새 패치 강챔을 팀별로 얼마나 빠르게 채택하는지 |
| D. 저격 밴 패턴 | 상대팀이 우리 선수 주력 챔피언을 얼마나 집중 밴하는지 |
| E. 패치별 승리 공식 | Logistic Regression으로 패치별 승패 기여 요소 역추적 |
| F. 밴 내성 지수 | 챔프폭·주력 의존도·밴 시 승률 하락을 종합한 0~100 점수 |
| G. 팀 색깔 프로파일 | 팀을 시스템형·캐리 의존형·초반 압박형·후반 역전형으로 분류 |
| H. 스페셜리스트 챔피언 | LCK 동라인 평균 대비 선수가 유독 잘하는 챔피언 식별 |

---

## 기술 스택

| 구분 | 기술 |
|------|------|
| 데이터베이스 | PostgreSQL (Supabase) |
| 백엔드 분석 | Python, SQLAlchemy, scikit-learn |
| 대시보드 | Streamlit, Plotly |
| 데이터 소스 | Oracle's Elixir (AWS S3) |
| 배포 | Streamlit Community Cloud |

---

## 데이터 범위

- **기간**: 2024 LCK Spring ~ 2026 LCK Rounds 1-2 (ETL 실행 시점 기준)
- **지역**: LCK 정규 시즌 (MSI, Worlds 제외)
- **경기 수**: 약 2,500경기 (게임 단위)

---

## 프로젝트 구조

```
LCK/
├── analysis/           # 분석 모듈 (시나리오 A~H)
│   ├── db.py           # DB 연결 (Supabase/로컬 공용)
│   ├── scenario_a.py ~ scenario_h.py
├── api/                # FastAPI 백엔드
├── dashboard/          # Streamlit 대시보드
│   └── app.py
├── db/
│   └── schema.sql      # PostgreSQL 스키마
├── etl/                # 데이터 수집 및 로드
│   ├── download_oracles_elixir.py
│   └── load_to_db.py
├── run_etl.py          # ETL 전체 실행
├── requirements.txt
└── .env                # DB 접속 정보 (git 제외)
```

---

## 로컬 실행

### 1. 환경 설정

```bash
pip install -r requirements.txt
```

### 2. `.env` 파일 생성

```env
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=lck_analysis
```

### 3. DB 스키마 생성

```bash
psql -U postgres -d lck_analysis -f db/schema.sql
```

### 4. 데이터 수집 및 로드

Oracle's Elixir에서 CSV를 다운로드하고 DB에 로드합니다.

```bash
python run_etl.py
```

> CSV 파일은 `data/raw/` 폴더에 저장됩니다.
> 다운로드 링크: [oracleselixir.com/tools/downloads](https://oracleselixir.com/tools/downloads)

### 5. 대시보드 실행

```bash
streamlit run dashboard/app.py
```

---

## 배포 구조

```
Oracle's Elixir (AWS S3)
        ↓ ETL
   Supabase DB (PostgreSQL)
        ↓
Streamlit Community Cloud → 공개 URL
```

대시보드 사이드바의 **⬇️ 데이터 다운로드 + DB 로드** 버튼으로 최신 데이터를 반영할 수 있습니다. (관리자 비밀번호 필요, 2시간 쿨다운)

---

## 규정 변경 처리

### 피어리스 밴픽
시리즈 내 한 번 픽된 챔피언은 나머지 경기에서 재픽 불가.
- 2024: 일반 밴픽
- 2025 LCK Cup~: 피어리스 전면 적용

### First Selection (2026년~)
블루 진영과 선픽 순서가 분리. 패배팀이 진영 또는 픽 순서 중 하나를 먼저 선택.

---

## 주의사항

- 분석 결과는 상관관계이며 인과관계가 아닙니다.
- 샘플 수(N)가 10 미만인 경우 수치보다 방향성만 참고하세요.
- 시나리오별 상세 한계는 [LIMITATIONS.md](LIMITATIONS.md)를 참고하세요.
