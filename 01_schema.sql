-- ============================================================
-- Colorado School Accountability Framework
-- Schema: Dimension & Fact Tables
-- Author: Srikanth Chandesure
-- ============================================================

-- ── Dimension: District ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_district (
    district_code       TEXT PRIMARY KEY,
    district_name       TEXT NOT NULL,
    county              TEXT,
    locale_type         TEXT   -- Urban, Suburban, Rural, Town
);

-- ── Dimension: School ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_school (
    school_code         TEXT PRIMARY KEY,
    school_name         TEXT NOT NULL,
    district_code       TEXT NOT NULL REFERENCES dim_district(district_code),
    school_level        TEXT,  -- Elementary, Middle, High, K-12
    school_type         TEXT,  -- Traditional, Charter, AEC
    is_active           INTEGER DEFAULT 1
);

-- ── Fact: Performance Indicators (annual, per school) ────────
-- One row per school per year with all weighted indicator scores
CREATE TABLE IF NOT EXISTS fact_performance (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    school_code             TEXT NOT NULL REFERENCES dim_school(school_code),
    district_code           TEXT NOT NULL REFERENCES dim_district(district_code),
    academic_year           INTEGER NOT NULL,  -- e.g. 2024

    -- Raw indicator values (as published by CDE)
    achievement_pts         REAL,  -- CMAS proficiency points
    growth_mgp              REAL,  -- Median Growth Percentile
    graduation_rate         REAL,  -- 4-year cohort grad rate (%)
    dropout_rate            REAL,  -- Annual dropout rate (%)
    postsecondary_rate      REAL,  -- Matriculation / CTE / military (%)

    -- Weighted scores (calculated fields)
    achievement_weighted    REAL,  -- achievement_pts * 0.30
    growth_weighted         REAL,  -- growth_mgp     * 0.30
    graduation_weighted     REAL,  -- graduation_rate * 0.20
    dropout_weighted        REAL,  -- (1 - dropout_rate) * 0.10
    postsecondary_weighted  REAL,  -- postsecondary_rate * 0.10

    -- Composite
    composite_score         REAL,  -- sum of all weighted scores
    performance_tier        TEXT,  -- Accredited / Accredited w/ Distinction / Performance / Improvement / Priority Improvement / Turnaround

    -- Data quality flags
    achievement_missing     INTEGER DEFAULT 0,
    growth_missing          INTEGER DEFAULT 0,
    graduation_missing      INTEGER DEFAULT 0,
    n_size_flag             INTEGER DEFAULT 0,  -- 1 if below n-size threshold
    data_complete           INTEGER DEFAULT 1,

    UNIQUE(school_code, academic_year)
);

-- ── Fact: District-level rollup ──────────────────────────────
CREATE TABLE IF NOT EXISTS fact_district_performance (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    district_code           TEXT NOT NULL REFERENCES dim_district(district_code),
    academic_year           INTEGER NOT NULL,

    avg_achievement_pts     REAL,
    avg_growth_mgp          REAL,
    avg_graduation_rate     REAL,
    avg_dropout_rate        REAL,
    avg_postsecondary_rate  REAL,
    composite_score         REAL,
    accreditation_rating    TEXT,  -- Accredited / Priority Improvement / Turnaround etc.
    n_schools               INTEGER,
    n_schools_priority      INTEGER,  -- # schools in Priority Improvement or Turnaround
    pct_schools_meeting     REAL,     -- % schools at or above Performance

    UNIQUE(district_code, academic_year)
);

-- ── Fact: Subgroup equity (for anomaly/equity analysis) ──────
CREATE TABLE IF NOT EXISTS fact_subgroup (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    school_code         TEXT NOT NULL REFERENCES dim_school(school_code),
    academic_year       INTEGER NOT NULL,
    subgroup            TEXT NOT NULL,  -- All, ELL, FRL, IEP, White, Hispanic, Black, Asian
    achievement_pts     REAL,
    growth_mgp          REAL,
    n_students          INTEGER,
    n_size_flag         INTEGER DEFAULT 0,

    UNIQUE(school_code, academic_year, subgroup)
);

-- ── Fact: Anomaly flags (output of Python analysis) ──────────
CREATE TABLE IF NOT EXISTS fact_anomaly (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    school_code         TEXT NOT NULL REFERENCES dim_school(school_code),
    academic_year       INTEGER NOT NULL,
    indicator           TEXT NOT NULL,  -- composite, achievement, growth, etc.
    prior_value         REAL,
    current_value       REAL,
    yoy_change          REAL,
    zscore              REAL,
    is_anomaly          INTEGER DEFAULT 0,  -- 1 = flagged
    anomaly_direction   TEXT,  -- 'spike' or 'drop'
    notes               TEXT,

    UNIQUE(school_code, academic_year, indicator)
);

-- ── Indexes for dashboard query performance ──────────────────
CREATE INDEX IF NOT EXISTS idx_perf_year        ON fact_performance(academic_year);
CREATE INDEX IF NOT EXISTS idx_perf_school      ON fact_performance(school_code);
CREATE INDEX IF NOT EXISTS idx_perf_district    ON fact_performance(district_code);
CREATE INDEX IF NOT EXISTS idx_perf_tier        ON fact_performance(performance_tier);
CREATE INDEX IF NOT EXISTS idx_dist_year        ON fact_district_performance(academic_year);
CREATE INDEX IF NOT EXISTS idx_subgroup_school  ON fact_subgroup(school_code, academic_year);
CREATE INDEX IF NOT EXISTS idx_anomaly_school   ON fact_anomaly(school_code, is_anomaly);
