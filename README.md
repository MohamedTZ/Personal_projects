# 📊 Donation Analytics – SQL Project

> **Industry-level SQL analytics** across three relational tables (donors, donations, assignments) covering data cleaning, EDA, dimensional modelling, advanced window functions, CTEs, and business intelligence views.

---

## 🗂 Project Structure

```
donation-analytics/
├── data/
│   ├── assignments.csv       # 5,000 assignments with budget, region, performance score
│   ├── donations.csv         # 5,000 donation transactions
│   └── donors.csv            # 5,000 donors (Individual / Organization / Corporate)
├── donation_analytics.sql    # Full SQL analysis script (all sections)
├── donation_analytics.pptx   # Microsoft PowerPoint – insights & recommendations
└── README.md
```

---

## 🗃 Schema Overview

```sql
donors         (donor_id PK, donor_name, donor_type)
                    ↑
donations      (donation_id PK, donor_id FK, amount, donation_date, assignment_id FK)
                                                                          ↓
assignments    (assignment_id PK, assignment_name, start_date, end_date, budget, region, performance_score)
```

| Table       | Rows  | Key Fields                                            |
|-------------|-------|-------------------------------------------------------|
| donors      | 5,000 | donor_type: Individual, Organization, Corporate       |
| donations   | 5,000 | amount ($10–$1,000), donation_date (2020–2022)        |
| assignments | 5,000 | budget (can be negative), region, performance_score 1–10 |

---

## 📋 SQL Sections

### Section 0 – DDL
Full `CREATE TABLE` definitions with primary keys and foreign key constraints.

### Section 1 – Data Cleaning & Quality Checks
| Query | Purpose |
|-------|---------|
| 1.1 Null audit | Count nulls across all three tables |
| 1.2 Duplicate PKs | Detect repeated IDs |
| 1.3 Negative budgets | Flag 1,730 assignments with budget < 0 |
| 1.4 Invalid date ranges | end_date < start_date |
| 1.5 Outlier donations | Z-score > 3 |
| 1.6 Referential integrity | Orphaned FK references |
| 1.7 Type standardisation | Donor type casing inconsistencies |

### Section 2 – Dimensions & Measures
Documents the classification of all fields into **dimensions** (categorical/descriptive) and **measures** (quantitative/aggregatable), including derived fields.

### Section 3 – EDA
- Overall summary statistics (total raised, avg, min, max, stddev)
- Year-over-year donation trend
- Breakdown by donor type
- Performance score distribution (banded)
- Regional analysis

### Section 4 – Advanced Business Questions (12 queries)

| # | Business Question | Technique |
|---|-------------------|-----------|
| 4.1 | Top 10 donors by contribution | CTE + `RANK()` window |
| 4.2 | Best assignments by fundraising vs. budget | CTE + multi-join + `NTILE` |
| 4.3 | Donor donation trend over time | `SUM()` running total + `AVG()` rolling 3-window |
| 4.4 | Month-over-month growth rate | CTE + `LAG()` |
| 4.5 | Cross-regional donors | CTE + `GROUP_CONCAT` + `HAVING` |
| 4.6 | Donor retention classification (New/Recurring/Lapsed) | Chained CTEs + `CASE` |
| 4.7 | Pareto / concentration by region | Cumulative `SUM() OVER` |
| 4.8 | High-value donor segments (quartile banding) | `NTILE(4)` + `CASE` labels |
| 4.9 | Underperforming assignment risk flags | CTE + `PERCENTILE_CONT` + `CASE` |
| 4.10 | Repeat donor frequency ranking | Multi-level CTE + `DENSE_RANK()` |
| 4.11 | Assignment duration vs. fundraising | Duration buckets + correlation |
| 4.12 | Donor 360° single-row summary view | CTE fan-out + aggregated LEFT JOINs |

### Section 5 – Reporting Views
Three reusable SQL views for BI/dashboard consumption:

| View | Description |
|------|-------------|
| `vw_donor_leaderboard` | Ranked donors by total donated (overall + per type) |
| `vw_assignment_dashboard` | Assignment KPIs: duration, raised, budget coverage % |
| `vw_monthly_kpi` | Monthly totals, unique donors, avg donation, MoM growth % |

---

## 🔍 Key Findings

| Metric | Value |
|--------|-------|
| Total donations raised | **$2,526,579** |
| Average donation | **$505** |
| Average performance score | **5.44 / 10** |
| Assignments with negative budget | **34.6%** (1,730 / 5,000) ⚠️ |
| Year-over-year revenue trend | **Declining** (2020 → 2022) |
| Top donor type by revenue | Organisation ($860K) — all types nearly equal |
| Top region | South |

---

## 📌 Strategic Recommendations

1. **Reverse declining revenue** – Targeted year-end campaigns + recurring giving programmes
2. **Nurture Platinum donors** – Top 25% contribute 44% of revenue; invest in high-touch stewardship
3. **Fix data quality** – 1,730 negative-budget records distort ROI; enforce validation at source
4. **Prioritise South & West** – Replicate South's campaign structure to lift North performance
5. **Deploy donor retention model** – CTE-based New/Recurring/Lapsed classification → automated re-engagement
6. **Leverage multi-region donors** – Active in 2+ regions; create VIP ambassador tier

---

## 🛠 SQL Techniques Used

- **CTEs** – Modular, readable multi-step transformations
- **Window Functions** – `RANK`, `DENSE_RANK`, `NTILE(4)`, `LAG`, running totals, rolling averages, cumulative distributions
- **Aggregate Functions** – `GROUP BY`, `HAVING`, `COALESCE`, `NULLIF` for null-safe arithmetic
- **Joins** – `INNER`, `LEFT OUTER`, `EXISTS`/`NOT EXISTS`, `CROSS JOIN`
- **Date Functions** – `YEAR()`, `DATE_FORMAT()`, `DATEDIFF()`
- **CASE Expressions** – Performance bands, donor tiers, risk flags
- **Reporting Views** – `CREATE OR REPLACE VIEW` for reusable BI layers

---

## ▶️ How to Run

1. **Set up the database** (MySQL 8+ or compatible):

```sql
CREATE DATABASE donation_analytics;
USE donation_analytics;
```

2. **Load the CSVs** into the tables defined in Section 0 of the SQL script.

```bash
# Example using MySQL CLI
mysqlimport --local --fields-terminated-by=',' --lines-terminated-by='\n' \
  --columns='donor_id,donor_name,donor_type' \
  donation_analytics donors.csv
```

3. **Run the full script**:

```bash
mysql -u root -p donation_analytics < donation_analytics.sql
```

4. **Explore individual sections** by running queries from `donation_analytics.sql` in your SQL client (DBeaver, DataGrip, MySQL Workbench, etc.).

---

## 📁 Files

| File | Description |
|------|-------------|
| `data/assignments.csv` | Raw assignment data |
| `data/donations.csv` | Raw donation transactions |
| `data/donors.csv` | Raw donor registry |
| `donation_analytics.sql` | Complete SQL analysis (DDL + 5 sections, 20+ queries) |
| `donation_analytics.pptx` | 10-slide PowerPoint presentation with charts and recommendations |

---

## 🧠 Skills Demonstrated

`SQL` `CTEs` `Window Functions` `Data Cleaning` `EDA` `Dimensional Modelling` `Business Intelligence` `Fundraising Analytics` `Donor Segmentation` `Cohort Analysis`

---

## 📬 Contact

Feel free to open an issue or reach out via GitHub if you have questions or suggestions for extending the analysis.
