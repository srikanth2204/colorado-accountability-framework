"""
CDE Accountability Framework — Analysis & Anomaly Detection
Author: Srikanth Chandesure

Produces:
  1. Longitudinal trend analysis (2019-2024)
  2. Anomaly detection (z-score on YOY change)
  3. Equity gap analysis by subgroup
  4. CSV exports for Power BI
  5. PNG charts for methodology doc / GitHub
"""

import pandas as pd
import numpy as np
import sqlite3
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
from scipy import stats
import warnings, os
warnings.filterwarnings('ignore')

DB_PATH    = "/home/claude/cde_project/data/clean/cde_accountability.db"
EXPORT_DIR = "/home/claude/cde_project/exports"
CHART_DIR  = "/home/claude/cde_project/exports/charts"
os.makedirs(EXPORT_DIR, exist_ok=True)
os.makedirs(CHART_DIR,  exist_ok=True)

conn = sqlite3.connect(DB_PATH)

# Style
plt.rcParams.update({
    'font.family': 'DejaVu Sans', 'font.size': 11,
    'axes.spines.top': False, 'axes.spines.right': False,
    'axes.grid': True, 'grid.alpha': 0.3, 'figure.dpi': 120
})
COLORS = {'Performance':'#2E86AB','Improvement':'#F6AE2D',
          'Priority Improvement':'#F26419','Turnaround':'#D62246',
          'Insufficient Data':'#999999','Other':'#CCCCCC'}

print("="*60)
print("CDE Accountability Analysis Pipeline")
print("="*60)

# ═══════════════════════════════════════════════════════════════
# 1. LONGITUDINAL TREND — Rating distribution 2019-2024
# ═══════════════════════════════════════════════════════════════
print("\n[1/5] Longitudinal trend analysis...")

trend_df = pd.read_sql("""
    SELECT academic_year, final_rating,
           CASE WHEN final_rating LIKE '%Turnaround%'           THEN 'Turnaround'
                WHEN final_rating LIKE '%Priority Improvement%' THEN 'Priority Improvement'
                WHEN final_rating LIKE '%Improvement%'          THEN 'Improvement'
                WHEN final_rating LIKE '%Performance%'          THEN 'Performance'
                ELSE 'Other/Insufficient'
           END AS tier
    FROM fact_ratings_over_time
    WHERE entity_type='SCHOOL' AND academic_year >= 2019
""", conn)

tier_by_year = trend_df.groupby(['academic_year','tier']).size().reset_index(name='n')
tier_pivot   = tier_by_year.pivot(index='academic_year', columns='tier', values='n').fillna(0)
# As % of total
tier_pct = tier_pivot.div(tier_pivot.sum(axis=1), axis=0) * 100

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# Stacked bar — count
tier_order = ['Performance','Improvement','Priority Improvement','Turnaround','Other/Insufficient']
plot_cols   = [c for c in tier_order if c in tier_pct.columns]
colors      = [COLORS.get(c,'#CCCCCC') for c in plot_cols]
tier_pct[plot_cols].plot(kind='bar', stacked=True, ax=axes[0],
    color=colors, width=0.6, edgecolor='white', linewidth=0.5)
axes[0].set_title('School rating distribution by year (%)', fontweight='bold', pad=12)
axes[0].set_xlabel('Academic year')
axes[0].set_ylabel('% of schools')
axes[0].set_xticklabels(tier_pct.index, rotation=0)
axes[0].legend(loc='upper left', fontsize=9, framealpha=0.9)
axes[0].yaxis.set_major_formatter(plt.FuncFormatter(lambda x,_: f'{x:.0f}%'))

# Line — Turnaround + Priority Improvement trend
concern_tiers = [c for c in ['Turnaround','Priority Improvement'] if c in tier_pivot.columns]
for tier in concern_tiers:
    axes[1].plot(tier_pivot.index, tier_pivot[tier],
                 marker='o', linewidth=2.5, markersize=6,
                 color=COLORS[tier], label=tier)
axes[1].set_title('Schools in Turnaround or Priority Improvement', fontweight='bold', pad=12)
axes[1].set_xlabel('Academic year')
axes[1].set_ylabel('Number of schools')
axes[1].legend(fontsize=10)
axes[1].xaxis.set_major_locator(plt.MaxNLocator(integer=True))

plt.tight_layout()
plt.savefig(f"{CHART_DIR}/01_rating_trends.png", bbox_inches='tight')
plt.close()
print(f"   Chart saved: 01_rating_trends.png")

# Export
tier_by_year.to_csv(f"{EXPORT_DIR}/trend_rating_distribution.csv", index=False)
print(f"   Export: trend_rating_distribution.csv  ({len(tier_by_year)} rows)")

# ═══════════════════════════════════════════════════════════════
# 2. ANOMALY DETECTION — YOY score change z-score
# ═══════════════════════════════════════════════════════════════
print("\n[2/5] Anomaly detection (z-score on YOY pct-points change)...")

yoy_df = pd.read_sql("""
    SELECT school_code, school_name, district_code, academic_year,
           pct_pts_earned, yoy_pct_change, final_rating, tier_numeric
    FROM v_school_trend
    WHERE pct_pts_earned IS NOT NULL
      AND yoy_pct_change IS NOT NULL
      AND academic_year >= 2022
""", conn)

# Z-score within each year
yoy_df['zscore'] = yoy_df.groupby('academic_year')['yoy_pct_change'].transform(
    lambda x: stats.zscore(x, nan_policy='omit'))

ANOMALY_THRESHOLD = 2.0
yoy_df['is_anomaly']        = (yoy_df['zscore'].abs() >= ANOMALY_THRESHOLD).astype(int)
yoy_df['anomaly_direction'] = np.where(yoy_df['yoy_pct_change'] > 0, 'spike', 'drop')
yoy_df['anomaly_direction'] = np.where(yoy_df['is_anomaly']==1, yoy_df['anomaly_direction'], '')

anomalies = yoy_df[yoy_df['is_anomaly']==1].copy()
print(f"   Total anomalies flagged: {len(anomalies)}")
print(f"   Spikes (large improvement): {(anomalies['anomaly_direction']=='spike').sum()}")
print(f"   Drops  (large decline):     {(anomalies['anomaly_direction']=='drop').sum()}")

# Save anomalies to DB
cur = conn.cursor()
cur.execute("DELETE FROM fact_anomaly")
for _, r in yoy_df.iterrows():
    try:
        cur.execute("""INSERT OR REPLACE INTO fact_anomaly
            (school_code,district_code,entity_name,entity_type,indicator,
             year_to,value_to,yoy_change,zscore,is_anomaly,anomaly_direction)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (str(r['school_code']),str(r['district_code']),str(r['school_name']),
             'SCHOOL','composite',int(r['academic_year']),
             float(r['pct_pts_earned']) if pd.notna(r['pct_pts_earned']) else None,
             float(r['yoy_pct_change']) if pd.notna(r['yoy_pct_change']) else None,
             float(r['zscore']) if pd.notna(r['zscore']) else None,
             int(r['is_anomaly']),str(r['anomaly_direction'])))
    except: pass
conn.commit()

# Chart: scatter YOY change vs score, highlight anomalies
fig, axes = plt.subplots(1, 2, figsize=(14, 5))

yr2024 = yoy_df[yoy_df['academic_year']==2024].copy()
normal   = yr2024[yr2024['is_anomaly']==0]
drops    = yr2024[(yr2024['is_anomaly']==1) & (yr2024['anomaly_direction']=='drop')]
spikes   = yr2024[(yr2024['is_anomaly']==1) & (yr2024['anomaly_direction']=='spike')]

axes[0].scatter(normal['pct_pts_earned'],   normal['yoy_pct_change'],   alpha=0.3, s=18, color='#AAAAAA', label='Normal')
axes[0].scatter(drops['pct_pts_earned'],    drops['yoy_pct_change'],    alpha=0.8, s=40, color=COLORS['Turnaround'], label=f'Anomaly drop (n={len(drops)})', zorder=3)
axes[0].scatter(spikes['pct_pts_earned'],   spikes['yoy_pct_change'],   alpha=0.8, s=40, color=COLORS['Performance'], label=f'Anomaly spike (n={len(spikes)})', zorder=3)
axes[0].axhline(0, color='black', linewidth=0.8, linestyle='--', alpha=0.5)
axes[0].set_xlabel('2024 composite score (% pts earned)')
axes[0].set_ylabel('YOY change (pct pts)')
axes[0].set_title('2024 anomaly detection — score vs YOY change', fontweight='bold', pad=12)
axes[0].legend(fontsize=9)

# Histogram of YOY changes
axes[1].hist(yoy_df[yoy_df['academic_year']==2024]['yoy_pct_change'].dropna(),
             bins=40, color='#2E86AB', alpha=0.7, edgecolor='white')
lo = yoy_df[yoy_df['academic_year']==2024]['yoy_pct_change'].mean() - ANOMALY_THRESHOLD * yoy_df[yoy_df['academic_year']==2024]['yoy_pct_change'].std()
hi = yoy_df[yoy_df['academic_year']==2024]['yoy_pct_change'].mean() + ANOMALY_THRESHOLD * yoy_df[yoy_df['academic_year']==2024]['yoy_pct_change'].std()
axes[1].axvline(lo, color=COLORS['Turnaround'],  linewidth=2, linestyle='--', label=f'−2σ threshold ({lo:.1f})')
axes[1].axvline(hi, color=COLORS['Performance'], linewidth=2, linestyle='--', label=f'+2σ threshold ({hi:.1f})')
axes[1].set_xlabel('YOY change (pct pts)')
axes[1].set_ylabel('Number of schools')
axes[1].set_title('Distribution of YOY score changes — 2024', fontweight='bold', pad=12)
axes[1].legend(fontsize=9)

plt.tight_layout()
plt.savefig(f"{CHART_DIR}/02_anomaly_detection.png", bbox_inches='tight')
plt.close()
print(f"   Chart saved: 02_anomaly_detection.png")

yoy_df.to_csv(f"{EXPORT_DIR}/anomaly_flags.csv", index=False)
print(f"   Export: anomaly_flags.csv  ({len(yoy_df)} rows)")

# ═══════════════════════════════════════════════════════════════
# 3. EQUITY GAP ANALYSIS
# ═══════════════════════════════════════════════════════════════
print("\n[3/5] Equity gap analysis...")

equity_df = pd.read_sql("""
    SELECT school_name, district_name, ach_all, ach_frl, ach_ell, ach_iep,
           ach_gap_frl, ach_gap_ell, ach_gap_iep,
           large_gap_frl, large_gap_ell, large_gap_iep
    FROM v_equity_gaps
    WHERE ach_all IS NOT NULL
""", conn)

print(f"   Schools with data: {len(equity_df)}")
print(f"   Large FRL gap (>20 pts): {equity_df['large_gap_frl'].sum()}")
print(f"   Large ELL gap (>20 pts): {equity_df['large_gap_ell'].sum()}")
print(f"   Large IEP gap (>25 pts): {equity_df['large_gap_iep'].sum()}")

# Subgroup avg achievement
subgroup_avgs = {
    'All students':    equity_df['ach_all'].mean(),
    'FRL eligible':    equity_df['ach_frl'].dropna().mean(),
    'Multilingual':    equity_df['ach_ell'].dropna().mean(),
    'Students w/ IEP': equity_df['ach_iep'].dropna().mean(),
}

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# Bar chart — avg by subgroup
sg_names = list(subgroup_avgs.keys())
sg_vals  = list(subgroup_avgs.values())
bar_colors = ['#2E86AB','#F6AE2D','#F26419','#D62246']
bars = axes[0].bar(sg_names, sg_vals, color=bar_colors, width=0.5, edgecolor='white')
axes[0].set_title('Average achievement score by subgroup (2024)', fontweight='bold', pad=12)
axes[0].set_ylabel('Avg % points earned')
axes[0].set_ylim(0, 100)
for bar, val in zip(bars, sg_vals):
    axes[0].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                 f'{val:.1f}%', ha='center', va='bottom', fontsize=10, fontweight='bold')

# Scatter — FRL gap vs overall achievement
valid = equity_df[equity_df['ach_gap_frl'].notna() & equity_df['ach_all'].notna()]
sc = axes[1].scatter(valid['ach_all'], valid['ach_gap_frl'],
    c=valid['large_gap_frl'].map({1: COLORS['Turnaround'], 0: '#AAAAAA'}),
    alpha=0.5, s=20)
axes[1].axhline(20, color=COLORS['Turnaround'], linewidth=1.5, linestyle='--', alpha=0.7, label='Large gap threshold (20 pts)')
axes[1].set_xlabel('Overall school achievement (%)')
axes[1].set_ylabel('FRL achievement gap (pct pts)')
axes[1].set_title('FRL equity gap vs school achievement level', fontweight='bold', pad=12)
large_patch = mpatches.Patch(color=COLORS['Turnaround'], label=f'Large gap (n={valid["large_gap_frl"].sum():.0f})')
norm_patch  = mpatches.Patch(color='#AAAAAA', label='Normal range')
axes[1].legend(handles=[large_patch, norm_patch], fontsize=9)

plt.tight_layout()
plt.savefig(f"{CHART_DIR}/03_equity_gaps.png", bbox_inches='tight')
plt.close()
print(f"   Chart saved: 03_equity_gaps.png")

equity_df.to_csv(f"{EXPORT_DIR}/equity_gaps.csv", index=False)
print(f"   Export: equity_gaps.csv  ({len(equity_df)} rows)")

# ═══════════════════════════════════════════════════════════════
# 4. FULL EXPORTS FOR POWER BI
# ═══════════════════════════════════════════════════════════════
print("\n[4/5] Exporting all Power BI tables...")

exports = {
    "schools_2024": """
        SELECT sp.school_code, sp.school_name, sp.district_code, sp.district_name,
               sp.emh_type, sp.charter_yn,
               sp.ach_pct_pts_earned, sp.ach_mean_ss, sp.ach_percentile, sp.ach_n_valid,
               sp.gro_median_sgp, sp.grad_rate_4yr, sp.pwr_rate,
               sp.pct_pts_earned_wtd AS composite_score,
               sp.final_rating, sp.performance_tier, sp.tier_rank, sp.n_size_flag
        FROM v_school_performance sp
        WHERE sp.academic_year = 2024
    """,
    "districts_2024": """
        SELECT dp.district_code, dd.district_name,
               rot.final_rating AS district_rating,
               rot.pct_pts_earned AS composite_score,
               dp.enrollment, ROUND(dp.pct_frl*100,1) AS pct_frl,
               ROUND(dp.pct_minority*100,1) AS pct_minority,
               ROUND(dp.pct_el*100,1) AS pct_el,
               ROUND(dp.pct_iep*100,1) AS pct_iep
        FROM fact_district_performance dp
        JOIN dim_district dd ON dp.district_code = dd.district_code
        LEFT JOIN fact_ratings_over_time rot
            ON rot.district_code=dp.district_code
            AND rot.academic_year=dp.academic_year
            AND rot.entity_type='DISTRICT'
        WHERE dp.academic_year = 2024
    """,
    "ratings_over_time": """
        SELECT entity_type, district_code, school_code, entity_name,
               academic_year, final_rating, pct_pts_earned,
               CASE WHEN final_rating LIKE '%Turnaround%'           THEN 'Turnaround'
                    WHEN final_rating LIKE '%Priority Improvement%' THEN 'Priority Improvement'
                    WHEN final_rating LIKE '%Improvement%'          THEN 'Improvement'
                    WHEN final_rating LIKE '%Performance%'          THEN 'Performance'
                    ELSE 'Other/Insufficient' END AS tier
        FROM fact_ratings_over_time
        WHERE academic_year >= 2019
    """,
    "anomaly_flags": """
        SELECT a.school_code, a.entity_name AS school_name, a.district_code,
               a.year_to AS academic_year, a.value_to AS composite_score,
               a.yoy_change, a.zscore, a.is_anomaly, a.anomaly_direction
        FROM fact_anomaly a
        WHERE a.is_anomaly = 1
    """,
    "equity_gaps_export": """
        SELECT school_code, school_name, district_name, academic_year,
               ach_all, ach_frl, ach_ell, ach_iep,
               ach_gap_frl, ach_gap_ell, ach_gap_iep,
               large_gap_frl, large_gap_ell, large_gap_iep
        FROM v_equity_gaps
    """,
    "data_quality": """
        SELECT school_code, school_name, district_name, academic_year,
               ach_missing, growth_missing, grad_missing, n_size_flag, data_status
        FROM v_data_quality
    """,
}

for name, query in exports.items():
    df = pd.read_sql(query, conn)
    df.to_csv(f"{EXPORT_DIR}/{name}.csv", index=False)
    print(f"   {name}.csv  ({len(df):,} rows)")

# ═══════════════════════════════════════════════════════════════
# 5. SUMMARY STATS (for methodology doc)
# ═══════════════════════════════════════════════════════════════
print("\n[5/5] Summary statistics...")

schools_df  = pd.read_csv(f"{EXPORT_DIR}/schools_2024.csv")
anomaly_df  = pd.read_csv(f"{EXPORT_DIR}/anomaly_flags.csv")
equity_df2  = pd.read_csv(f"{EXPORT_DIR}/equity_gaps_export.csv")

summary = {
    "Total schools (2024)":          len(schools_df),
    "Total districts (2024)":        schools_df['district_code'].nunique(),
    "Schools - Performance Plan":    (schools_df['performance_tier']=='Performance').sum(),
    "Schools - Improvement Plan":    (schools_df['performance_tier']=='Improvement').sum(),
    "Schools - Priority Improvement":(schools_df['performance_tier']=='Priority Improvement').sum(),
    "Schools - Turnaround":          (schools_df['performance_tier']=='Turnaround').sum(),
    "Avg composite score":           f"{schools_df['composite_score'].mean():.1f}%",
    "Avg MGP (growth)":              f"{schools_df['gro_median_sgp'].mean():.1f}",
    "Anomalies flagged (2022-2024)": len(anomaly_df),
    "Schools w/ large FRL gap":      equity_df2['large_gap_frl'].sum(),
    "Schools w/ large ELL gap":      equity_df2['large_gap_ell'].sum(),
    "Schools w/ large IEP gap":      equity_df2['large_gap_iep'].sum(),
}

print("\n   KEY FINDINGS:")
for k, v in summary.items():
    print(f"   {k:<40} {v}")

# Save summary
pd.DataFrame(list(summary.items()), columns=['Metric','Value']).to_csv(
    f"{EXPORT_DIR}/summary_stats.csv", index=False)

conn.close()
print(f"\nAll exports saved to: {EXPORT_DIR}")
print("="*60)
