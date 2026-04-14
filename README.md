# Colorado School & District Accountability Performance Framework

A end-to-end data analytics project replicating Colorado's school accountability framework using publicly available CDE data. Built as a portfolio project demonstrating SQL pipeline design, Python analysis, and Power BI dashboard development aligned with the Colorado Department of Education's published methodology.

---

## Project Overview

| Item | Detail |
|------|--------|
| Data source | CDE Published Performance Framework Flat Files (ed.cde.state.co.us) |
| Schools covered | 1,841 schools across 185 Colorado districts |
| Years | 2019–2024 (longitudinal) |
| Tools | Python, SQL (SQLite), Power BI, DAX |

---

## Key Findings

- **58.2%** average composite score statewide (2024)
- **1,074 schools** (58.3%) rated at Performance Plan
- **32 schools** in Turnaround Plan — highest accountability concern tier
- **214 anomalies** flagged via z-score method (2022–2024)
- **692 schools** with large FRL achievement gaps (>20 percentage points)

---

## Project Structure

```
cde_project/
├── python/
│   ├── 01_ingest.py          # Data ingestion pipeline — reads CDE Excel files → SQLite DB
│   └── 02_analysis.py        # Anomaly detection, equity analysis, Power BI CSV exports
├── sql/
│   ├── 01_schema.sql         # Database schema — dimension and fact tables
│   └── 02_calculations.sql   # Business rule views — tier assignment, equity gaps, trends
├── exports/
│   ├── schools_2024.csv          # School-level performance data
│   ├── districts_2024.csv        # District-level summary
│   ├── ratings_over_time.csv     # Longitudinal ratings 2019–2024
│   ├── anomaly_flags.csv         # Schools flagged for unusual YOY changes
│   └── equity_gaps_export.csv    # Subgroup achievement gap analysis
├── exports/charts/
│   ├── 01_rating_trends.png      # Rating distribution trends 2019–2024
│   ├── 02_anomaly_detection.png  # Anomaly scatter + distribution charts
│   └── 03_equity_gaps.png        # Subgroup equity gap analysis
└── docs/
    └── CDE_Accountability_Methodology.docx  # Full technical methodology document
```

---

## How to Reproduce

**1. Download source data** from [CDE Accountability Data Files](https://ed.cde.state.co.us/accountability/data-files):
- 2024 DPF/SPF Final Public Data File (XLSX)
- SPF 2024 Final Ratings Over Time (XLSX)
- DPF 2024 Final Ratings Over Time (XLSX)

**2. Install dependencies:**
```bash
pip install pandas openpyxl sqlalchemy matplotlib seaborn scipy
```

**3. Run ingestion pipeline:**
```bash
python python/01_ingest.py
```
Builds SQLite database with 7 tables, 6 SQL views, and all business rule logic.

**4. Run analysis:**
```bash
python python/02_analysis.py
```
Generates anomaly flags, equity gap analysis, 3 charts, and 6 CSV exports for Power BI.

**5. Open Power BI dashboard** and connect to the CSV files in `/exports/`.

---

## Methodology Summary

### Performance Tier Assignment
Tiers are assigned based on CDE's published final ratings:

| Tier | CDE Rating |
|------|-----------|
| Performance | Performance Plan |
| Improvement | Improvement Plan |
| Priority Improvement | Priority Improvement Plan |
| Turnaround | Turnaround Plan |

### Anomaly Detection
Z-score method on year-over-year composite score changes. Schools with |z| ≥ 2.0 flagged as anomalies (~top/bottom 5% of changers per year). COVID years (2020, 2021) excluded.

### Equity Gap Analysis
Achievement gaps calculated as: `All Students Score − Subgroup Score`. Large gap thresholds: FRL >20 pts, ELL >20 pts, IEP >25 pts.

See [full methodology document](docs/CDE_Accountability_Methodology.docx) for complete technical details.

---

## Power BI Dashboard Pages

1. **District Overview** — KPI cards, rating tier donut chart, district ratings table, year slicer
2. **School Drilldown** — District slicer, school performance table, high-risk school bar chart
3. **Anomaly & Equity** — Flagged anomalies table, FRL equity gap chart, anomaly count KPI

---

## Data Source

All data is publicly available from the Colorado Department of Education:
- [CDE Accountability Data Files](https://ed.cde.state.co.us/accountability/data-files)
- [CDE Accountability Resources](https://cde.state.co.us/accountability/accountability-resources)

No proprietary or non-public data was used in this analysis.
