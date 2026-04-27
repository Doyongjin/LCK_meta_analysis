-- LCK Victory Formula & Meta Analysis Tool
-- DB Schema

-- 확장 기능
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =============================================
-- 그룹 1: 기준 데이터
-- =============================================

CREATE TABLE seasons (
    season_id       VARCHAR(30) PRIMARY KEY,  -- 'LCK_2024_Spring'
    year            INT NOT NULL,
    split           VARCHAR(20) NOT NULL,     -- 'Spring' | 'Summer' | 'Cup'
    start_date      DATE,
    end_date        DATE,
    rule_era        VARCHAR(20) NOT NULL DEFAULT 'pre_2026'
                    CHECK (rule_era IN ('pre_2026', 'first_selection'))
);

CREATE TABLE patch_versions (
    patch_id        VARCHAR(20) PRIMARY KEY,  -- '14.1', '25.S1.3'
    version         VARCHAR(20) NOT NULL,
    release_date    DATE,
    season_id       VARCHAR(30) REFERENCES seasons(season_id)
);

CREATE TABLE champions (
    champion_id     VARCHAR(30) PRIMARY KEY,  -- 'Ahri', 'Jinx' (Riot 키값)
    name            VARCHAR(50) NOT NULL,
    icon_url        VARCHAR(255)
);

CREATE TABLE teams (
    team_id         SERIAL PRIMARY KEY,
    name            VARCHAR(100) NOT NULL,
    acronym         VARCHAR(10) NOT NULL,     -- 'T1', 'GEN', 'HLE'
    region          VARCHAR(10) NOT NULL DEFAULT 'LCK'
);

CREATE TABLE players (
    player_id       SERIAL PRIMARY KEY,
    summoner_name   VARCHAR(50) NOT NULL,
    real_name       VARCHAR(50),
    nationality     VARCHAR(30)
);

-- =============================================
-- 그룹 2: 이력 데이터
-- =============================================

CREATE TABLE player_team_history (
    id              SERIAL PRIMARY KEY,
    player_id       INT NOT NULL REFERENCES players(player_id),
    team_id         INT NOT NULL REFERENCES teams(team_id),
    season_id       VARCHAR(30) REFERENCES seasons(season_id),
    role            VARCHAR(10) NOT NULL
                    CHECK (role IN ('top', 'jng', 'mid', 'bot', 'sup')),
    joined_date     DATE,
    left_date       DATE  -- NULL = 현재 재직
);

-- =============================================
-- 그룹 3: 경기 데이터
-- =============================================

CREATE TABLE series (
    series_id       SERIAL PRIMARY KEY,
    season_id       VARCHAR(30) REFERENCES seasons(season_id),
    team1_id        INT NOT NULL REFERENCES teams(team_id),
    team2_id        INT NOT NULL REFERENCES teams(team_id),
    format          VARCHAR(5) NOT NULL CHECK (format IN ('BO1', 'BO3', 'BO5')),
    draft_type      VARCHAR(10) NOT NULL DEFAULT 'standard'
                    CHECK (draft_type IN ('standard', 'fearless')),
    stage           VARCHAR(20),             -- 'regular' | 'playoff' | 'final'
    date            DATE,
    winner_team_id  INT REFERENCES teams(team_id)
);

CREATE TABLE games (
    game_id         SERIAL PRIMARY KEY,
    series_id       INT NOT NULL REFERENCES series(series_id),
    patch_id        VARCHAR(20) REFERENCES patch_versions(patch_id),
    game_number     INT NOT NULL,            -- 시리즈 내 몇 번째 경기
    date            DATE,
    length_seconds  INT
);

-- 팀 단위 경기 기록 (경기당 2행)
CREATE TABLE game_teams (
    id                      SERIAL PRIMARY KEY,
    game_id                 INT NOT NULL REFERENCES games(game_id),
    team_id                 INT NOT NULL REFERENCES teams(team_id),
    side                    VARCHAR(5) NOT NULL CHECK (side IN ('blue', 'red')),
    pick_order              VARCHAR(10) NOT NULL CHECK (pick_order IN ('first', 'second')),
    first_selection_choice  VARCHAR(15)
                            CHECK (first_selection_choice IN ('side', 'pick_order')),
                            -- NULL = 2025 이전 (first_selection 규정 없음)
    result                  BOOLEAN NOT NULL,
    gold_at_15              INT,
    first_dragon            BOOLEAN,
    first_herald            BOOLEAN,
    first_tower             BOOLEAN
);

-- 선수 단위 경기 기록 (경기당 10행)
CREATE TABLE game_participants (
    id              SERIAL PRIMARY KEY,
    game_id         INT NOT NULL REFERENCES games(game_id),
    player_id       INT NOT NULL REFERENCES players(player_id),
    team_id         INT NOT NULL REFERENCES teams(team_id),
    champion_id     VARCHAR(30) REFERENCES champions(champion_id),
    position        VARCHAR(5) NOT NULL CHECK (position IN ('top', 'jng', 'mid', 'bot', 'sup')),
    gold_at_15      INT,
    cs_diff_at_15   INT,
    xp_diff_at_15   INT,
    kills           INT,
    deaths          INT,
    assists         INT
);

-- 밴픽 순서 (경기당 최대 20행)
CREATE TABLE picks_bans (
    id              SERIAL PRIMARY KEY,
    game_id         INT NOT NULL REFERENCES games(game_id),
    team_id         INT NOT NULL REFERENCES teams(team_id),
    champion_id     VARCHAR(30) REFERENCES champions(champion_id),
    phase           VARCHAR(5) NOT NULL CHECK (phase IN ('ban', 'pick')),
    global_order    INT NOT NULL,            -- 전체 순서 1~20
    team_pick_order INT                      -- 팀 내 순서 1~5 (픽만 해당)
);

-- =============================================
-- 그룹 4: 분석 파생 데이터
-- =============================================

-- 패치별 챔피언 메타 강도
CREATE TABLE champion_meta (
    id              SERIAL PRIMARY KEY,
    champion_id     VARCHAR(30) NOT NULL REFERENCES champions(champion_id),
    patch_id        VARCHAR(20) NOT NULL REFERENCES patch_versions(patch_id),
    pick_rate       DECIMAL(5,4),
    ban_rate        DECIMAL(5,4),
    win_rate        DECIMAL(5,4),
    presence_rate   DECIMAL(5,4),            -- pick_rate + ban_rate
    pbi_score       DECIMAL(8,4),            -- (win_rate - avg) × pick / (1 - ban_rate)
    calculated_at   TIMESTAMP DEFAULT NOW(),
    UNIQUE (champion_id, patch_id)
);

-- 선수 성향 프로파일 (2024~2025 기반 누적)
CREATE TABLE player_profiles (
    id                  SERIAL PRIMARY KEY,
    player_id           INT NOT NULL REFERENCES players(player_id),
    position            VARCHAR(5) NOT NULL CHECK (position IN ('top', 'jng', 'mid', 'bot', 'sup')),
    based_on_seasons    VARCHAR(50),          -- '2024_2025'
    calculated_at       TIMESTAMP DEFAULT NOW(),
    blue_side_wr        DECIMAL(5,4),
    first_pick_wr       DECIMAL(5,4),
    avg_gold_at_15      DECIMAL(10,2),
    avg_cs_diff_at_15   DECIMAL(8,2),
    top_champion_ids    VARCHAR(30)[],        -- 주로 사용한 챔피언 배열
    champion_dependency DECIMAL(5,4),        -- 핵심 챔피언 밴 시 팀 성과 하락률
    ban_resistance_score DECIMAL(5,2),       -- 밴 내성 지수 0~100 (시나리오 F)
    UNIQUE (player_id, position, based_on_seasons)
);

-- =============================================
-- 인덱스
-- =============================================

CREATE INDEX idx_games_series ON games(series_id);
CREATE INDEX idx_games_patch ON games(patch_id);
CREATE INDEX idx_game_teams_game ON game_teams(game_id);
CREATE INDEX idx_game_teams_team ON game_teams(team_id);
CREATE INDEX idx_game_participants_game ON game_participants(game_id);
CREATE INDEX idx_game_participants_player ON game_participants(player_id);
CREATE INDEX idx_game_participants_champion ON game_participants(champion_id);
CREATE INDEX idx_picks_bans_game ON picks_bans(game_id);
CREATE INDEX idx_picks_bans_champion ON picks_bans(champion_id);
CREATE INDEX idx_player_team_history_player ON player_team_history(player_id);
CREATE INDEX idx_champion_meta_patch ON champion_meta(patch_id);
