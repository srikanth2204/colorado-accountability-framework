-- ============================================================
-- CDE Accountability Framework — Business Rule Calculations
-- Author: Srikanth Chandesure
--
-- Implements:
--   1. Weighted indicator scoring (replicates CDE methodology)
--   2. Performance tier assignment
--   3. Data quality flags
--   4. District rollup aggregations
--   5. Year-over-year change calculations
-- ============================================================

-- ── 1. WEIGHTED COMPOSITE SCORE ──────────────────────────────
-- CDE uses percent of points earned (weighted) as the composite.
-- Tiers are assigned based on pct_pts_earned_wtd thresholds.
-- Source: CDE Accountability Handbook 2024

-- View: school performance with derived tier
DROP VIEW IF EXISTS v_school_performance;
CREATE VIEW v_school_performance AS
SELECT
    fp.school_code,
    fp.district_code,
    ds.school_name,
    dd.district_name,
    ds.emh_type,
    ds.charter_yn,
    fp.academic_year,
    fp.ach_pct_pts_earned,
    fp.ach_mean_ss,
    fp.ach_percentile,
    fp.ach_n_valid,
    fp.gro_median_sgp,
    fp.gro_n_valid,
    fp.grad_rate_4yr,
    fp.pwr_rate,
    -- CDE published composite
    fp.pct_pts_earned,
    fp.pct_pts_earned_wtd,
    fp.pts_earned_wtd,
    fp.pts_eligible_wtd,
    fp.rating_final,
    -- Derived performance tier (mirrors CDE cut points)
    CASE
        WHEN fp.rating_final LIKE '%Turnaround%'          THEN 'Turnaround'
        WHEN fp.rating_final LIKE '%Priority Improvement%' THEN 'Priority Improvement'
        WHEN fp.rating_final LIKE '%Improvement%'          THEN 'Improvement'
        WHEN fp.rating_final LIKE '%Performance%'          THEN 'Performance'
        WHEN fp.rating_final LIKE '%Insufficient%'         THEN 'Insufficient Data'
        ELSE 'Other'
    END AS performance_tier,
    -- Tier rank for sorting (1=worst, 5=best)
    CASE
        WHEN fp.rating_final LIKE '%Turnaround%'           THEN 1
        WHEN fp.rating_final LIKE '%Priority Improvement%' THEN 2
        WHEN fp.rating_final LIKE '%Improvement%'          THEN 3
        WHEN fp.rating_final LIKE '%Performance%'          THEN 4
        ELSE 0
    END AS tier_rank,
    fp.n_size_flag
FROM fact_performance fp
JOIN dim_school ds ON fp.school_code = ds.school_code
JOIN dim_district dd ON fp.district_code = dd.district_code;


-- ── 2. INDICATOR BREAKDOWN VIEW ──────────────────────────────
-- Shows each school's individual indicator scores side by side
DROP VIEW IF EXISTS v_indicator_breakdown;
CREATE VIEW v_indicator_breakdown AS
SELECT
    fp.school_code,
    fp.district_code,
    ds.school_name,
    dd.district_name,
    ds.emh_type,
    fp.academic_year,
    -- Achievement indicator
    fp.ach_pct_pts_earned                           AS ach_score,
    fp.ach_mean_ss                                  AS ach_mean_scale_score,
    fp.ach_percentile                               AS ach_percentile,
    fp.ach_n_valid                                  AS ach_students_tested,
    -- Growth indicator (MGP)
    fp.gro_median_sgp                               AS growth_mgp,
    fp.gro_n_valid                                  AS growth_n_valid,
    -- Post-secondary readiness (PWR)
    fp.grad_rate_4yr                                AS graduation_rate_4yr,
    fp.grad_rate_5yr                                AS graduation_rate_5yr,
    fp.pwr_rate                                     AS postsecondary_rate,
    -- Overall
    fp.pct_pts_earned_wtd                           AS composite_pct_wtd,
    fp.rating_final,
    -- Flag schools where growth > achievement (growth bright spots)
    CASE WHEN fp.gro_median_sgp > 55
          AND fp.ach_percentile < 40
         THEN 1 ELSE 0
    END AS growth_bright_spot,
    -- Flag schools where achievement high but growth low
    CASE WHEN fp.ach_percentile > 60
          AND fp.gro_median_sgp < 40
         THEN 1 ELSE 0
    END AS achievement_without_growth
FROM fact_performance fp
JOIN dim_school ds ON fp.school_code = ds.school_code
JOIN dim_district dd ON fp.district_code = dd.district_code;


-- ── 3. DATA QUALITY FLAGS VIEW ───────────────────────────────
DROP VIEW IF EXISTS v_data_quality;
CREATE VIEW v_data_quality AS
SELECT
    fp.school_code,
    ds.school_name,
    dd.district_name,
    fp.academic_year,
    -- Completeness checks
    CASE WHEN fp.ach_pct_pts_earned IS NULL THEN 1 ELSE 0 END AS ach_missing,
    CASE WHEN fp.gro_median_sgp     IS NULL THEN 1 ELSE 0 END AS growth_missing,
    CASE WHEN fp.grad_rate_4yr      IS NULL THEN 1 ELSE 0 END AS grad_missing,
    CASE WHEN fp.pct_pts_earned_wtd IS NULL THEN 1 ELSE 0 END AS composite_missing,
    -- N-size flag (< 20 students tested)
    fp.n_size_flag,
    -- Total missing indicators
    (CASE WHEN fp.ach_pct_pts_earned IS NULL THEN 1 ELSE 0 END +
     CASE WHEN fp.gro_median_sgp     IS NULL THEN 1 ELSE 0 END +
     CASE WHEN fp.grad_rate_4yr      IS NULL THEN 1 ELSE 0 END) AS total_missing_indicators,
    -- Overall data completeness
    CASE
        WHEN fp.pct_pts_earned_wtd IS NULL THEN 'Incomplete - No Composite'
        WHEN fp.n_size_flag = 1            THEN 'Flag - Small N'
        ELSE 'Complete'
    END AS data_status
FROM fact_performance fp
JOIN dim_school ds ON fp.school_code = ds.school_code
JOIN dim_district dd ON fp.district_code = dd.district_code;


-- ── 4. DISTRICT ROLLUP VIEW ──────────────────────────────────
DROP VIEW IF EXISTS v_district_summary;
CREATE VIEW v_district_summary AS
SELECT
    dp.district_code,
    dd.district_name,
    dp.academic_year,
    dp.rating_final                         AS district_rating,
    dp.pct_pts_earned_wtd                   AS composite_pct_wtd,
    dp.enrollment,
    ROUND(dp.pct_frl    * 100, 1)           AS pct_frl,
    ROUND(dp.pct_minority * 100, 1)         AS pct_minority,
    ROUND(dp.pct_el     * 100, 1)           AS pct_el,
    ROUND(dp.pct_iep    * 100, 1)           AS pct_iep,
    -- School counts by tier
    COUNT(fp.school_code)                   AS total_schools,
    SUM(CASE WHEN fp.rating_final LIKE '%Turnaround%'           THEN 1 ELSE 0 END) AS n_turnaround,
    SUM(CASE WHEN fp.rating_final LIKE '%Priority Improvement%' THEN 1 ELSE 0 END) AS n_priority_improvement,
    SUM(CASE WHEN fp.rating_final LIKE '%Improvement%'
              AND fp.rating_final NOT LIKE '%Priority%'         THEN 1 ELSE 0 END) AS n_improvement,
    SUM(CASE WHEN fp.rating_final LIKE '%Performance%'          THEN 1 ELSE 0 END) AS n_performance,
    -- Avg school scores
    ROUND(AVG(fp.ach_pct_pts_earned), 1)    AS avg_ach_pct,
    ROUND(AVG(fp.gro_median_sgp), 1)        AS avg_mgp,
    ROUND(AVG(fp.grad_rate_4yr), 1)         AS avg_grad_rate,
    ROUND(AVG(fp.pct_pts_earned_wtd), 1)    AS avg_school_composite,
    -- % schools at Performance or above
    ROUND(100.0 * SUM(CASE WHEN fp.rating_final LIKE '%Performance%' THEN 1 ELSE 0 END)
          / NULLIF(COUNT(fp.school_code), 0), 1) AS pct_schools_at_performance
FROM fact_district_performance dp
JOIN dim_district dd ON dp.district_code = dd.district_code
LEFT JOIN fact_performance fp
    ON fp.district_code = dp.district_code
    AND fp.academic_year = dp.academic_year
    AND fp.n_size_flag = 0
GROUP BY dp.district_code, dp.academic_year;


-- ── 5. LONGITUDINAL TREND VIEW ───────────────────────────────
DROP VIEW IF EXISTS v_school_trend;
CREATE VIEW v_school_trend AS
SELECT
    r.school_code,
    r.entity_name                           AS school_name,
    r.district_code,
    r.academic_year,
    r.final_rating,
    r.pct_pts_earned,
    -- Tier numeric for trending
    CASE
        WHEN r.final_rating LIKE '%Turnaround%'           THEN 1
        WHEN r.final_rating LIKE '%Priority Improvement%' THEN 2
        WHEN r.final_rating LIKE '%Improvement%'          THEN 3
        WHEN r.final_rating LIKE '%Performance%'          THEN 4
        WHEN r.final_rating LIKE '%Insufficient%'         THEN 0
        ELSE NULL
    END AS tier_numeric,
    -- YOY change in pct points
    r.pct_pts_earned - LAG(r.pct_pts_earned)
        OVER (PARTITION BY r.school_code ORDER BY r.academic_year) AS yoy_pct_change,
    -- Rating change
    LAG(r.final_rating)
        OVER (PARTITION BY r.school_code ORDER BY r.academic_year) AS prior_rating
FROM fact_ratings_over_time r
WHERE r.entity_type = 'SCHOOL';


-- ── 6. EQUITY GAP VIEW ───────────────────────────────────────
DROP VIEW IF EXISTS v_equity_gaps;
CREATE VIEW v_equity_gaps AS
SELECT
    s_all.school_code,
    ds.school_name,
    dd.district_name,
    s_all.academic_year,
    -- All students baseline
    s_all.ach_pct_pts                       AS ach_all,
    s_all.gro_median_sgp                    AS mgp_all,
    -- FRL gap
    s_frl.ach_pct_pts                       AS ach_frl,
    s_all.ach_pct_pts - s_frl.ach_pct_pts   AS ach_gap_frl,
    -- ELL gap
    s_ell.ach_pct_pts                       AS ach_ell,
    s_all.ach_pct_pts - s_ell.ach_pct_pts   AS ach_gap_ell,
    -- IEP gap
    s_iep.ach_pct_pts                       AS ach_iep,
    s_all.ach_pct_pts - s_iep.ach_pct_pts   AS ach_gap_iep,
    -- Flag large gaps
    CASE WHEN (s_all.ach_pct_pts - s_frl.ach_pct_pts) > 20 THEN 1 ELSE 0 END AS large_gap_frl,
    CASE WHEN (s_all.ach_pct_pts - s_ell.ach_pct_pts) > 20 THEN 1 ELSE 0 END AS large_gap_ell,
    CASE WHEN (s_all.ach_pct_pts - s_iep.ach_pct_pts) > 25 THEN 1 ELSE 0 END AS large_gap_iep
FROM fact_subgroup s_all
JOIN dim_school ds ON s_all.school_code = ds.school_code
JOIN dim_district dd ON s_all.district_code = dd.district_code
LEFT JOIN fact_subgroup s_frl
    ON  s_frl.school_code   = s_all.school_code
    AND s_frl.academic_year = s_all.academic_year
    AND s_frl.indicator     = 'ACH'
    AND s_frl.subgroup      = 'Free/Reduced-Price Lunch Eligible'
LEFT JOIN fact_subgroup s_ell
    ON  s_ell.school_code   = s_all.school_code
    AND s_ell.academic_year = s_all.academic_year
    AND s_ell.indicator     = 'ACH'
    AND s_ell.subgroup      = 'Multilingual Learners'
LEFT JOIN fact_subgroup s_iep
    ON  s_iep.school_code   = s_all.school_code
    AND s_iep.academic_year = s_all.academic_year
    AND s_iep.indicator     = 'ACH'
    AND s_iep.subgroup      = 'Students with Disabilities'
WHERE s_all.indicator = 'ACH'
  AND s_all.subgroup  = 'All Students';
