import pandas as pd, sqlite3, numpy as np, re, os

DATA_DIR  = "/mnt/user-data/uploads"
DB_PATH   = "/home/claude/cde_project/data/clean/cde_accountability.db"
CALC_FILE = f"{DATA_DIR}/2024_DPF_SPF_FINAL_PUBLIC_DATA_FILE_12_09_24.xlsx"
SPF_FILE  = f"{DATA_DIR}/SPF2024FinalRatingsOverTime.xlsx"
DPF_FILE  = f"{DATA_DIR}/DPF2024FinalRatingsOverTime.xlsx"

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
conn = sqlite3.connect(DB_PATH)
cur  = conn.cursor()

def clean_pct(val):
    if pd.isna(val) or val == '-' or val == '': return None
    if isinstance(val, str):
        val = val.strip().replace('%','')
        try:
            v = float(val); return v if v > 1 else v * 100
        except: return None
    if isinstance(val, (int, float)): return float(val) if val <= 1 else float(val)
    return None

def clean_float(val):
    if pd.isna(val) or val == '-': return None
    try: return float(val)
    except: return None

cur.executescript("""
DROP TABLE IF EXISTS dim_district; DROP TABLE IF EXISTS dim_school;
DROP TABLE IF EXISTS fact_performance; DROP TABLE IF EXISTS fact_district_performance;
DROP TABLE IF EXISTS fact_subgroup; DROP TABLE IF EXISTS fact_ratings_over_time;
DROP TABLE IF EXISTS fact_anomaly;

CREATE TABLE dim_district (district_code TEXT PRIMARY KEY, district_name TEXT NOT NULL, setting TEXT, region TEXT);
CREATE TABLE dim_school (school_code TEXT PRIMARY KEY, school_name TEXT NOT NULL, district_code TEXT NOT NULL, emh_type TEXT, charter_yn TEXT, online_yn TEXT, aec_yn TEXT);
CREATE TABLE fact_performance (id INTEGER PRIMARY KEY AUTOINCREMENT, school_code TEXT, district_code TEXT, academic_year INTEGER, ach_pct_pts_earned REAL, ach_mean_ss REAL, ach_percentile REAL, ach_n_valid INTEGER, gro_median_sgp REAL, gro_n_valid INTEGER, grad_rate_4yr REAL, grad_rate_5yr REAL, pwr_rate REAL, pct_pts_earned REAL, pct_pts_earned_wtd REAL, pts_earned_wtd REAL, pts_eligible_wtd REAL, rating_final TEXT, n_size_flag INTEGER DEFAULT 0, UNIQUE(school_code, academic_year));
CREATE TABLE fact_district_performance (id INTEGER PRIMARY KEY AUTOINCREMENT, district_code TEXT, academic_year INTEGER, pct_pts_earned REAL, pct_pts_earned_wtd REAL, pts_earned_wtd REAL, pts_eligible_wtd REAL, rating_final TEXT, enrollment INTEGER, pct_frl REAL, pct_minority REAL, pct_el REAL, pct_iep REAL, UNIQUE(district_code, academic_year));
CREATE TABLE fact_subgroup (id INTEGER PRIMARY KEY AUTOINCREMENT, school_code TEXT, district_code TEXT, academic_year INTEGER, indicator TEXT, subgroup TEXT, ach_pct_pts REAL, ach_mean_ss REAL, ach_percentile REAL, gro_median_sgp REAL, n_valid INTEGER, rating_final TEXT, n_size_flag INTEGER DEFAULT 0, UNIQUE(school_code, academic_year, indicator, subgroup));
CREATE TABLE fact_ratings_over_time (id INTEGER PRIMARY KEY AUTOINCREMENT, entity_type TEXT, district_code TEXT, school_code TEXT, entity_name TEXT, academic_year INTEGER, final_rating TEXT, pct_pts_earned REAL, UNIQUE(entity_type, school_code, district_code, academic_year));
CREATE TABLE fact_anomaly (id INTEGER PRIMARY KEY AUTOINCREMENT, school_code TEXT, district_code TEXT, entity_name TEXT, entity_type TEXT, indicator TEXT, year_from INTEGER, year_to INTEGER, value_from REAL, value_to REAL, yoy_change REAL, zscore REAL, is_anomaly INTEGER DEFAULT 0, anomaly_direction TEXT, UNIQUE(school_code, district_code, indicator, year_to));
CREATE INDEX IF NOT EXISTS idx_perf_year ON fact_performance(academic_year);
CREATE INDEX IF NOT EXISTS idx_perf_sch  ON fact_performance(school_code);
CREATE INDEX IF NOT EXISTS idx_rot_year  ON fact_ratings_over_time(academic_year);
""")
conn.commit()

print("Loading data...")
raw      = pd.read_excel(CALC_FILE, sheet_name="INDICATOR_DETAIL")
sch_raw  = raw[raw['SCH_NUMBER'] != 'ALL'].copy()
dist_raw = raw[raw['SCH_NUMBER'] == 'ALL'].copy()

for _, r in sch_raw[['DIST_NUMBER','DIST_NAME']].drop_duplicates().iterrows():
    cur.execute("INSERT OR IGNORE INTO dim_district(district_code,district_name) VALUES (?,?)", (str(r['DIST_NUMBER']),str(r['DIST_NAME'])))
for _, r in sch_raw[['SCH_NUMBER','SCH_NAME','DIST_NUMBER','EMH_TYPE','CHARTER_YN','ONLINE_YN','AEC_YN']].drop_duplicates(subset='SCH_NUMBER').iterrows():
    cur.execute("INSERT OR IGNORE INTO dim_school VALUES (?,?,?,?,?,?,?)", (str(r['SCH_NUMBER']),str(r['SCH_NAME']),str(r['DIST_NUMBER']),str(r.get('EMH_TYPE','')),str(r.get('CHARTER_YN','')),str(r.get('ONLINE_YN','')),str(r.get('AEC_YN',''))))

all_stu = sch_raw[sch_raw['SUBCATEGORY']=='All Students'].copy()
ach = all_stu[all_stu['INDICATOR']=='ACH'][['SCH_NUMBER','DIST_NUMBER','ACH_N_VALID','ACH_MEAN_SS','ACH_PERCENTILE','PCT_PTS_EARN','K12_ENROLLMENT.2024']].copy()
ach.columns = ['school_code','district_code','ach_n_valid','ach_mean_ss','ach_percentile','ach_pct_pts','enrollment']
gro = all_stu[all_stu['INDICATOR']=='GRO'][['SCH_NUMBER','GRO_N_VALID','GRO_MEDIAN_SGP']].copy()
gro.columns = ['school_code','gro_n_valid','gro_median_sgp']
pwr = all_stu[all_stu['INDICATOR']=='PWR'].copy()
pwr_4yr = pwr[pwr['SUBCATEGORY']=='4 YEAR'][['SCH_NUMBER','PWR_GRAD_RATE_4YR','PWR_GRAD_RATE_5YR','PWR_RATE']].copy()
pwr_4yr.columns = ['school_code','grad_rate_4yr','grad_rate_5yr','pwr_rate']
ind_ttl = pd.read_excel(CALC_FILE, sheet_name="INDICATOR_TTLS_ALL_LEVELS")
ind_sch = ind_ttl[ind_ttl['SCH_NUMBER'].notna() & (ind_ttl['SCH_NUMBER']!='ALL')].copy()
overall = ind_sch[ind_sch['SUBINDICATOR']=='TOTAL'].groupby('SCH_NUMBER').agg(pct_pts_earned=('PCT_PTS_EARN','first'),pct_pts_earned_wtd=('PCT_PTS_EARN_WEIGHTED','first'),pts_earned_wtd=('PTS_EARN_WEIGHTED','sum'),pts_eligible_wtd=('PTS_ELIG_WEIGHTED','sum')).reset_index()
overall.columns = ['school_code','pct_pts_earned','pct_pts_earned_wtd','pts_earned_wtd','pts_eligible_wtd']
merged = ach.merge(gro,on='school_code',how='left').merge(pwr_4yr,on='school_code',how='left').merge(overall,on='school_code',how='left')
for _, r in merged.iterrows():
    try:
        cur.execute("INSERT OR REPLACE INTO fact_performance(school_code,district_code,academic_year,ach_pct_pts_earned,ach_mean_ss,ach_percentile,ach_n_valid,gro_median_sgp,gro_n_valid,grad_rate_4yr,grad_rate_5yr,pwr_rate,pct_pts_earned,pct_pts_earned_wtd,pts_earned_wtd,pts_eligible_wtd,n_size_flag) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (str(r['school_code']),str(r['district_code']),2024,clean_pct(r.get('ach_pct_pts')),clean_float(r.get('ach_mean_ss')),clean_pct(r.get('ach_percentile')),clean_float(r.get('ach_n_valid')),clean_float(r.get('gro_median_sgp')),clean_float(r.get('gro_n_valid')),clean_pct(r.get('grad_rate_4yr')),clean_pct(r.get('grad_rate_5yr')),clean_pct(r.get('pwr_rate')),clean_pct(r.get('pct_pts_earned')),clean_pct(r.get('pct_pts_earned_wtd')),clean_float(r.get('pts_earned_wtd')),clean_float(r.get('pts_eligible_wtd')),1 if (clean_float(r.get('ach_n_valid')) or 0)<20 else 0))
    except: pass

dist_ind = pd.read_excel(CALC_FILE, sheet_name="INDICATOR_TTLS_ALL_LEVELS")
dist_ind = dist_ind[dist_ind['SCH_NUMBER']=='ALL'].copy()
dist_overall = dist_ind[dist_ind['SUBINDICATOR']=='TOTAL'].groupby('DIST_NUMBER').agg(pct_pts_earned=('PCT_PTS_EARN','first'),pct_pts_earned_wtd=('PCT_PTS_EARN_WEIGHTED','first'),pts_earned_wtd=('PTS_EARN_WEIGHTED','sum'),pts_eligible_wtd=('PTS_ELIG_WEIGHTED','sum'),enrollment=('K12_ENROLLMENT.2024','first'),pct_frl=('PCT_FRL.2024','first'),pct_minority=('PCT_MINORITY.2024','first'),pct_el=('PCT_EL.2024','first'),pct_iep=('PCT_IEP.2024','first')).reset_index()
for _, r in dist_overall.iterrows():
    try:
        cur.execute("INSERT OR REPLACE INTO fact_district_performance(district_code,academic_year,pct_pts_earned,pct_pts_earned_wtd,pts_earned_wtd,pts_eligible_wtd,enrollment,pct_frl,pct_minority,pct_el,pct_iep) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (str(r['DIST_NUMBER']),2024,clean_pct(r['pct_pts_earned']),clean_pct(r['pct_pts_earned_wtd']),clean_float(r['pts_earned_wtd']),clean_float(r['pts_eligible_wtd']),clean_float(r['enrollment']),clean_float(r['pct_frl']),clean_float(r['pct_minority']),clean_float(r['pct_el']),clean_float(r['pct_iep'])))
    except: pass

target_subgroups = ['Free/Reduced-Price Lunch Eligible','Multilingual Learners','Students with Disabilities','Minority Students','All Students']
sub_df = sch_raw[sch_raw['SUBCATEGORY'].isin(target_subgroups) & sch_raw['INDICATOR'].isin(['ACH','GRO'])].copy()
for _, r in sub_df.iterrows():
    try:
        cur.execute("INSERT OR IGNORE INTO fact_subgroup(school_code,district_code,academic_year,indicator,subgroup,ach_pct_pts,ach_mean_ss,ach_percentile,gro_median_sgp,n_valid,rating_final,n_size_flag) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (str(r['SCH_NUMBER']),str(r['DIST_NUMBER']),2024,str(r['INDICATOR']),str(r['SUBCATEGORY']),clean_pct(r.get('PCT_PTS_EARN')),clean_float(r.get('ACH_MEAN_SS')),clean_pct(r.get('ACH_PERCENTILE')),clean_float(r.get('GRO_MEDIAN_SGP')),clean_float(r.get('ACH_N_VALID') if r['INDICATOR']=='ACH' else r.get('GRO_N_VALID')),str(r['RATING_FINAL']) if pd.notna(r.get('RATING_FINAL')) else None,1 if (clean_float(r.get('ACH_N_VALID')) or 0)<20 else 0))
    except: pass

spf = pd.read_excel(SPF_FILE, sheet_name="SPF Ratings 2019-2024")
year_cols = {2019:('2019_FINAL_RATING',None),2020:('2020_FINAL_RATING',None),2021:('2021_FINAL_RATING',None),2022:('2022_FINAL_RATING','2022_PERCENT_POINTS_EARNED'),2023:('2023_FINAL_RATING','2023_PERCENT_POINTS_EARNED'),2024:('2024_FINAL_RATING','2024_PERCENT_POINTS_EARNED')}
for yr,(rating_col,pct_col) in year_cols.items():
    for _, r in spf.iterrows():
        rating = r.get(rating_col)
        if pd.isna(rating): continue
        pct = r.get(pct_col) if pct_col else None
        try:
            cur.execute("INSERT OR IGNORE INTO fact_ratings_over_time(entity_type,district_code,school_code,entity_name,academic_year,final_rating,pct_pts_earned) VALUES (?,?,?,?,?,?,?)",
                ('SCHOOL',str(r.get('DISTRICT_NUMBER','')),str(r.get('SCHOOL_NUMBER','')),str(r.get('SCHOOL_NAME','')),yr,str(rating),clean_float(pct)*100 if isinstance(pct,float) and pct<=1 else clean_float(pct)))
        except: pass

dpf = pd.read_excel(DPF_FILE, sheet_name="DPF Ratings 2019-2024", header=1)
for col in [c for c in dpf.columns if re.match(r'20\d\d_FINAL_RATING',str(c))]:
    yr = int(col[:4])
    for _, r in dpf.iterrows():
        rating = r.get(col)
        if pd.isna(rating): continue
        try:
            cur.execute("INSERT OR IGNORE INTO fact_ratings_over_time(entity_type,district_code,school_code,entity_name,academic_year,final_rating) VALUES (?,?,?,?,?,?)",
                ('DISTRICT',str(r.get('DISTRICT_NUMBER','')),None,str(r.get('DISTRICT_NAME','')),yr,str(rating)))
        except: pass

conn.commit()
conn.executescript("""
DROP VIEW IF EXISTS v_school_performance;
CREATE VIEW v_school_performance AS
SELECT fp.school_code,fp.district_code,ds.school_name,dd.district_name,ds.emh_type,ds.charter_yn,fp.academic_year,fp.ach_pct_pts_earned,fp.ach_mean_ss,fp.ach_percentile,fp.ach_n_valid,fp.gro_median_sgp,fp.gro_n_valid,fp.grad_rate_4yr,fp.pwr_rate,fp.pct_pts_earned,fp.pct_pts_earned_wtd,fp.pts_earned_wtd,fp.pts_eligible_wtd,rot.final_rating,rot.pct_pts_earned AS rot_pct_pts_earned,
CASE WHEN rot.final_rating LIKE '%Turnaround%' THEN 'Turnaround' WHEN rot.final_rating LIKE '%Priority Improvement%' THEN 'Priority Improvement' WHEN rot.final_rating LIKE '%Improvement%' THEN 'Improvement' WHEN rot.final_rating LIKE '%Performance%' THEN 'Performance' WHEN rot.final_rating LIKE '%Insufficient%' THEN 'Insufficient Data' ELSE 'Other' END AS performance_tier,
CASE WHEN rot.final_rating LIKE '%Turnaround%' THEN 1 WHEN rot.final_rating LIKE '%Priority Improvement%' THEN 2 WHEN rot.final_rating LIKE '%Improvement%' THEN 3 WHEN rot.final_rating LIKE '%Performance%' THEN 4 ELSE 0 END AS tier_rank, fp.n_size_flag
FROM fact_performance fp JOIN dim_school ds ON fp.school_code=ds.school_code JOIN dim_district dd ON fp.district_code=dd.district_code LEFT JOIN fact_ratings_over_time rot ON rot.school_code=fp.school_code AND rot.academic_year=fp.academic_year AND rot.entity_type='SCHOOL';

DROP VIEW IF EXISTS v_school_trend;
CREATE VIEW v_school_trend AS
SELECT r.school_code,r.entity_name AS school_name,r.district_code,r.academic_year,r.final_rating,r.pct_pts_earned,
CASE WHEN r.final_rating LIKE '%Turnaround%' THEN 1 WHEN r.final_rating LIKE '%Priority Improvement%' THEN 2 WHEN r.final_rating LIKE '%Improvement%' THEN 3 WHEN r.final_rating LIKE '%Performance%' THEN 4 ELSE 0 END AS tier_numeric,
r.pct_pts_earned - LAG(r.pct_pts_earned) OVER (PARTITION BY r.school_code ORDER BY r.academic_year) AS yoy_pct_change,
LAG(r.final_rating) OVER (PARTITION BY r.school_code ORDER BY r.academic_year) AS prior_rating
FROM fact_ratings_over_time r WHERE r.entity_type='SCHOOL';

DROP VIEW IF EXISTS v_equity_gaps;
CREATE VIEW v_equity_gaps AS
SELECT s_all.school_code,ds.school_name,dd.district_name,s_all.academic_year,s_all.ach_pct_pts AS ach_all,s_all.gro_median_sgp AS mgp_all,s_frl.ach_pct_pts AS ach_frl,s_all.ach_pct_pts-s_frl.ach_pct_pts AS ach_gap_frl,s_ell.ach_pct_pts AS ach_ell,s_all.ach_pct_pts-s_ell.ach_pct_pts AS ach_gap_ell,s_iep.ach_pct_pts AS ach_iep,s_all.ach_pct_pts-s_iep.ach_pct_pts AS ach_gap_iep,
CASE WHEN (s_all.ach_pct_pts-s_frl.ach_pct_pts)>20 THEN 1 ELSE 0 END AS large_gap_frl,
CASE WHEN (s_all.ach_pct_pts-s_ell.ach_pct_pts)>20 THEN 1 ELSE 0 END AS large_gap_ell,
CASE WHEN (s_all.ach_pct_pts-s_iep.ach_pct_pts)>25 THEN 1 ELSE 0 END AS large_gap_iep
FROM fact_subgroup s_all JOIN dim_school ds ON s_all.school_code=ds.school_code JOIN dim_district dd ON s_all.district_code=dd.district_code
LEFT JOIN fact_subgroup s_frl ON s_frl.school_code=s_all.school_code AND s_frl.academic_year=s_all.academic_year AND s_frl.indicator='ACH' AND s_frl.subgroup='Free/Reduced-Price Lunch Eligible'
LEFT JOIN fact_subgroup s_ell ON s_ell.school_code=s_all.school_code AND s_ell.academic_year=s_all.academic_year AND s_ell.indicator='ACH' AND s_ell.subgroup='Multilingual Learners'
LEFT JOIN fact_subgroup s_iep ON s_iep.school_code=s_all.school_code AND s_iep.academic_year=s_all.academic_year AND s_iep.indicator='ACH' AND s_iep.subgroup='Students with Disabilities'
WHERE s_all.indicator='ACH' AND s_all.subgroup='All Students';

DROP VIEW IF EXISTS v_data_quality;
CREATE VIEW v_data_quality AS
SELECT fp.school_code,ds.school_name,dd.district_name,fp.academic_year,
CASE WHEN fp.ach_pct_pts_earned IS NULL THEN 1 ELSE 0 END AS ach_missing,
CASE WHEN fp.gro_median_sgp IS NULL THEN 1 ELSE 0 END AS growth_missing,
CASE WHEN fp.grad_rate_4yr IS NULL THEN 1 ELSE 0 END AS grad_missing,
fp.n_size_flag,
CASE WHEN fp.pct_pts_earned_wtd IS NULL THEN 'Incomplete - No Composite' WHEN fp.n_size_flag=1 THEN 'Flag - Small N' ELSE 'Complete' END AS data_status
FROM fact_performance fp JOIN dim_school ds ON fp.school_code=ds.school_code JOIN dim_district dd ON fp.district_code=dd.district_code;
""")
conn.commit()
conn.close()

# Quick validation
conn = sqlite3.connect(DB_PATH)
for tbl in ['dim_district','dim_school','fact_performance','fact_subgroup','fact_ratings_over_time']:
    n = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
    print(f"  {tbl:<35} {n:>8,}")
conn.close()
print("Database ready.")
