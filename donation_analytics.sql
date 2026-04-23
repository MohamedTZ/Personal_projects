-- =============================================================================
-- DONATION ANALYTICS PROJECT
-- Tables: assignments, donations, donors
-- Author: Data Analytics Team
-- Description: Industry-level SQL analysis covering data cleaning, EDA,
--              dimensional modelling, and advanced business intelligence.
-- =============================================================================

-- =============================================================================
-- SECTION 0: TABLE DEFINITIONS (DDL)
-- =============================================================================

CREATE TABLE donors (
    donor_id        INT           PRIMARY KEY,
    donor_name      VARCHAR(100)  NOT NULL,
    donor_type      VARCHAR(50)   NOT NULL   -- 'Individual', 'Organization', 'Corporate'
);

CREATE TABLE assignments (
    assignment_id       INT             PRIMARY KEY,
    assignment_name     VARCHAR(100)    NOT NULL,
    start_date          DATE            NOT NULL,
    end_date            DATE            NOT NULL,
    budget              DECIMAL(15,2),           -- Can be negative (data quality flag)
    region              VARCHAR(50)     NOT NULL, -- 'North','South','East','West'
    performance_score   DECIMAL(4,2)             -- 1.00 – 10.00
);

CREATE TABLE donations (
    donation_id     INT             PRIMARY KEY,
    donor_id        INT             NOT NULL REFERENCES donors(donor_id),
    amount          DECIMAL(10,2)   NOT NULL,
    donation_date   DATE            NOT NULL,
    assignment_id   INT             NOT NULL REFERENCES assignments(assignment_id)
);


-- =============================================================================
-- SECTION 1: DATA CLEANING & QUALITY CHECKS
-- =============================================================================

-- 1.1  Null / missing value audit across all three tables
SELECT 'donors'      AS table_name, COUNT(*) AS total_rows,
       SUM(CASE WHEN donor_id   IS NULL THEN 1 ELSE 0 END) AS null_donor_id,
       SUM(CASE WHEN donor_name IS NULL THEN 1 ELSE 0 END) AS null_donor_name,
       SUM(CASE WHEN donor_type IS NULL THEN 1 ELSE 0 END) AS null_donor_type
FROM donors
UNION ALL
SELECT 'assignments', COUNT(*),
       SUM(CASE WHEN assignment_id    IS NULL THEN 1 ELSE 0 END),
       SUM(CASE WHEN assignment_name  IS NULL THEN 1 ELSE 0 END),
       SUM(CASE WHEN budget           IS NULL THEN 1 ELSE 0 END)
FROM assignments
UNION ALL
SELECT 'donations', COUNT(*),
       SUM(CASE WHEN donation_id   IS NULL THEN 1 ELSE 0 END),
       SUM(CASE WHEN donor_id      IS NULL THEN 1 ELSE 0 END),
       SUM(CASE WHEN amount        IS NULL THEN 1 ELSE 0 END)
FROM donations;


-- 1.2  Detect duplicate primary keys
SELECT 'donors'      AS source, donor_id      AS id, COUNT(*) AS occurrences FROM donors      GROUP BY donor_id      HAVING COUNT(*) > 1
UNION ALL
SELECT 'assignments',            assignment_id,         COUNT(*)              FROM assignments GROUP BY assignment_id HAVING COUNT(*) > 1
UNION ALL
SELECT 'donations',              donation_id,           COUNT(*)              FROM donations   GROUP BY donation_id   HAVING COUNT(*) > 1;


-- 1.3  Assignments with negative budget (data anomaly)
SELECT assignment_id, assignment_name, region, budget,
       ROUND(ABS(budget), 2) AS abs_budget
FROM   assignments
WHERE  budget < 0
ORDER  BY budget ASC;


-- 1.4  Assignments where end_date precedes start_date (logical error)
SELECT assignment_id, assignment_name, start_date, end_date,
       DATEDIFF(end_date, start_date) AS duration_days
FROM   assignments
WHERE  end_date < start_date;


-- 1.5  Donations with suspiciously low or high amounts (outlier detection)
WITH stats AS (
    SELECT AVG(amount)                          AS avg_amt,
           STDDEV(amount)                       AS std_amt
    FROM   donations
)
SELECT d.donation_id, d.amount, d.donor_id, d.assignment_id,
       ROUND((d.amount - s.avg_amt) / NULLIF(s.std_amt, 0), 2) AS z_score
FROM   donations d
CROSS  JOIN stats s
WHERE  ABS((d.amount - s.avg_amt) / NULLIF(s.std_amt, 0)) > 3
ORDER  BY z_score DESC;


-- 1.6  Referential integrity: donations referencing non-existent donors / assignments
SELECT 'orphan_donor_in_donations' AS issue,
       COUNT(*)                    AS count
FROM   donations d
WHERE  NOT EXISTS (SELECT 1 FROM donors dn WHERE dn.donor_id = d.donor_id)
UNION ALL
SELECT 'orphan_assignment_in_donations',
       COUNT(*)
FROM   donations d
WHERE  NOT EXISTS (SELECT 1 FROM assignments a WHERE a.assignment_id = d.assignment_id);


-- 1.7  Standardise donor_type capitalisation (inspection query)
SELECT donor_type,
       COUNT(*) AS cnt
FROM   donors
GROUP  BY donor_type
ORDER  BY cnt DESC;


-- =============================================================================
-- SECTION 2: DIMENSIONS & MEASURES CLASSIFICATION
-- =============================================================================
/*
  DIMENSIONS (descriptive / categorical context):
    donors      : donor_id, donor_name, donor_type
    assignments : assignment_id, assignment_name, region, start_date, end_date
    (derived)   : donation_year, donation_month, donation_quarter

  MEASURES (quantitative / aggregatable):
    donations   : amount
    assignments : budget, performance_score
    (derived)   : total_donated, avg_donation, budget_utilisation_rate,
                  assignment_duration_days, roi_score
*/


-- =============================================================================
-- SECTION 3: EXPLORATORY DATA ANALYSIS (EDA)
-- =============================================================================

-- 3.1  Overall summary statistics
SELECT
    COUNT(DISTINCT dn.donor_id)       AS total_donors,
    COUNT(DISTINCT a.assignment_id)   AS total_assignments,
    COUNT(d.donation_id)              AS total_donations,
    ROUND(SUM(d.amount), 2)           AS total_amount_raised,
    ROUND(AVG(d.amount), 2)           AS avg_donation_amount,
    ROUND(MIN(d.amount), 2)           AS min_donation,
    ROUND(MAX(d.amount), 2)           AS max_donation,
    ROUND(STDDEV(d.amount), 2)        AS stddev_donation
FROM   donations d
JOIN   donors      dn ON dn.donor_id      = d.donor_id
JOIN   assignments a  ON a.assignment_id  = d.assignment_id;


-- 3.2  Donation volume & revenue by year
SELECT
    YEAR(donation_date)           AS donation_year,
    COUNT(*)                      AS num_donations,
    ROUND(SUM(amount), 2)         AS total_amount,
    ROUND(AVG(amount), 2)         AS avg_amount,
    ROUND(SUM(amount)
          / SUM(SUM(amount)) OVER () * 100, 2) AS pct_of_total
FROM   donations
GROUP  BY YEAR(donation_date)
ORDER  BY donation_year;


-- 3.3  Donation distribution by donor type
SELECT
    dn.donor_type,
    COUNT(DISTINCT dn.donor_id)   AS unique_donors,
    COUNT(d.donation_id)          AS total_donations,
    ROUND(SUM(d.amount), 2)       AS total_amount,
    ROUND(AVG(d.amount), 2)       AS avg_amount,
    ROUND(MAX(d.amount), 2)       AS max_amount
FROM   donations d
JOIN   donors dn ON dn.donor_id = d.donor_id
GROUP  BY dn.donor_type
ORDER  BY total_amount DESC;


-- 3.4  Assignment performance distribution (bucketed)
SELECT
    CASE
        WHEN performance_score >= 8  THEN 'High (8-10)'
        WHEN performance_score >= 5  THEN 'Medium (5-7.99)'
        ELSE                              'Low (1-4.99)'
    END AS performance_band,
    COUNT(*)                                        AS num_assignments,
    ROUND(AVG(budget), 2)                           AS avg_budget,
    ROUND(SUM(CASE WHEN budget < 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_negative_budget
FROM   assignments
GROUP  BY 1
ORDER  BY MIN(performance_score) DESC;


-- 3.5  Regional breakdown
SELECT
    a.region,
    COUNT(DISTINCT a.assignment_id)   AS num_assignments,
    COUNT(d.donation_id)              AS num_donations,
    ROUND(SUM(d.amount), 2)           AS total_raised,
    ROUND(AVG(a.performance_score), 2) AS avg_performance_score,
    ROUND(AVG(a.budget), 2)           AS avg_budget
FROM   assignments a
LEFT   JOIN donations d ON d.assignment_id = a.assignment_id
GROUP  BY a.region
ORDER  BY total_raised DESC;


-- =============================================================================
-- SECTION 4: ADVANCED BUSINESS QUESTIONS
-- =============================================================================

-- 4.1  QUESTION: Who are the top 10 donors by total contribution (with ranking)?
-- Technique: CTE + Window Function (RANK)
WITH donor_totals AS (
    SELECT
        dn.donor_id,
        dn.donor_name,
        dn.donor_type,
        ROUND(SUM(d.amount), 2)   AS total_donated,
        COUNT(d.donation_id)      AS num_donations,
        ROUND(AVG(d.amount), 2)   AS avg_donation
    FROM   donations d
    JOIN   donors dn ON dn.donor_id = d.donor_id
    GROUP  BY dn.donor_id, dn.donor_name, dn.donor_type
)
SELECT
    RANK() OVER (ORDER BY total_donated DESC) AS rank_overall,
    donor_id,
    donor_name,
    donor_type,
    total_donated,
    num_donations,
    avg_donation,
    ROUND(total_donated / SUM(total_donated) OVER () * 100, 2) AS pct_of_total
FROM   donor_totals
ORDER  BY rank_overall
LIMIT  10;


-- 4.2  QUESTION: Which assignments received the most donations and are they
--               performing well relative to their budget?
-- Technique: CTE + Multi-join + Window Function
WITH assignment_stats AS (
    SELECT
        a.assignment_id,
        a.assignment_name,
        a.region,
        a.budget,
        a.performance_score,
        DATEDIFF(a.end_date, a.start_date)  AS duration_days,
        COUNT(d.donation_id)                 AS num_donations,
        ROUND(SUM(d.amount), 2)              AS total_raised,
        ROUND(AVG(d.amount), 2)              AS avg_donation
    FROM   assignments a
    LEFT   JOIN donations d ON d.assignment_id = a.assignment_id
    GROUP  BY a.assignment_id, a.assignment_name, a.region,
              a.budget, a.performance_score, a.start_date, a.end_date
)
SELECT
    assignment_id,
    assignment_name,
    region,
    ROUND(budget, 2)                                               AS budget,
    total_raised,
    ROUND(total_raised / NULLIF(ABS(budget), 0) * 100, 2)         AS budget_coverage_pct,
    performance_score,
    num_donations,
    avg_donation,
    RANK() OVER (PARTITION BY region ORDER BY total_raised DESC)   AS region_rank,
    NTILE(4) OVER (ORDER BY total_raised DESC)                     AS quartile_raised
FROM   assignment_stats
ORDER  BY total_raised DESC
LIMIT  20;


-- 4.3  QUESTION: What is each donor's donation trend (running total) over time?
-- Technique: Window Function (SUM … OVER with ORDER BY)
SELECT
    dn.donor_id,
    dn.donor_name,
    dn.donor_type,
    d.donation_date,
    d.amount,
    ROUND(SUM(d.amount) OVER (
        PARTITION BY dn.donor_id
        ORDER BY d.donation_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2)                           AS running_total,
    ROUND(AVG(d.amount) OVER (
        PARTITION BY dn.donor_id
        ORDER BY d.donation_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2)                           AS rolling_3_avg
FROM   donations d
JOIN   donors dn ON dn.donor_id = d.donor_id
ORDER  BY dn.donor_id, d.donation_date;


-- 4.4  QUESTION: Month-over-month donation growth rate
-- Technique: CTE + LAG window function
WITH monthly AS (
    SELECT
        DATE_FORMAT(donation_date, '%Y-%m')   AS month,
        ROUND(SUM(amount), 2)                 AS total_amount,
        COUNT(*)                              AS num_donations
    FROM   donations
    GROUP  BY DATE_FORMAT(donation_date, '%Y-%m')
)
SELECT
    month,
    total_amount,
    num_donations,
    LAG(total_amount) OVER (ORDER BY month)  AS prev_month_amount,
    ROUND(
        (total_amount - LAG(total_amount) OVER (ORDER BY month))
        / NULLIF(LAG(total_amount) OVER (ORDER BY month), 0) * 100, 2
    )                                         AS mom_growth_pct
FROM   monthly
ORDER  BY month;


-- 4.5  QUESTION: Which donors have donated to multiple regions (cross-regional donors)?
-- Technique: CTE + HAVING + aggregation
WITH donor_regions AS (
    SELECT
        d.donor_id,
        COUNT(DISTINCT a.region)  AS regions_covered,
        GROUP_CONCAT(DISTINCT a.region ORDER BY a.region SEPARATOR ', ') AS regions_list,
        ROUND(SUM(d.amount), 2)   AS total_donated
    FROM   donations d
    JOIN   assignments a ON a.assignment_id = d.assignment_id
    GROUP  BY d.donor_id
    HAVING COUNT(DISTINCT a.region) > 1
)
SELECT
    dn.donor_name,
    dn.donor_type,
    dr.regions_covered,
    dr.regions_list,
    dr.total_donated
FROM   donor_regions dr
JOIN   donors dn ON dn.donor_id = dr.donor_id
ORDER  BY dr.regions_covered DESC, dr.total_donated DESC;


-- 4.6  QUESTION: Donor retention – classify donors as New, Recurring, or Lapsed
--               based on their donation activity across years.
-- Technique: CTE chaining + CASE classification
WITH donor_years AS (
    SELECT
        donor_id,
        MIN(YEAR(donation_date)) AS first_year,
        MAX(YEAR(donation_date)) AS last_year,
        COUNT(DISTINCT YEAR(donation_date)) AS active_years
    FROM   donations
    GROUP  BY donor_id
),
classified AS (
    SELECT
        donor_id,
        first_year,
        last_year,
        active_years,
        CASE
            WHEN active_years = 1 AND first_year = (SELECT MAX(YEAR(donation_date)) FROM donations)
                THEN 'New'
            WHEN active_years > 1
                THEN 'Recurring'
            ELSE 'Lapsed'
        END AS retention_status
    FROM   donor_years
)
SELECT
    c.retention_status,
    COUNT(c.donor_id)                               AS num_donors,
    ROUND(AVG(t.total_donated), 2)                  AS avg_lifetime_value,
    ROUND(SUM(t.total_donated), 2)                  AS total_contribution
FROM   classified c
JOIN (
    SELECT donor_id, SUM(amount) AS total_donated
    FROM   donations
    GROUP  BY donor_id
) t ON t.donor_id = c.donor_id
JOIN donors dn ON dn.donor_id = c.donor_id
GROUP  BY c.retention_status
ORDER  BY total_contribution DESC;


-- 4.7  QUESTION: For each region, what percentage of total fundraising does each
--               assignment contribute? (Pareto / concentration analysis)
-- Technique: CTE + Window Function (cumulative distribution)
WITH region_totals AS (
    SELECT
        a.region,
        a.assignment_id,
        a.assignment_name,
        a.performance_score,
        ROUND(SUM(d.amount), 2) AS raised
    FROM   assignments a
    JOIN   donations d ON d.assignment_id = a.assignment_id
    GROUP  BY a.region, a.assignment_id, a.assignment_name, a.performance_score
)
SELECT
    region,
    assignment_id,
    assignment_name,
    raised,
    performance_score,
    ROUND(raised / SUM(raised) OVER (PARTITION BY region) * 100, 2)  AS pct_of_region,
    ROUND(SUM(raised) OVER (
        PARTITION BY region
        ORDER BY raised DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) / SUM(raised) OVER (PARTITION BY region) * 100, 2)             AS cumulative_pct
FROM   region_totals
ORDER  BY region, raised DESC;


-- 4.8  QUESTION: Identify high-value donor segments using quartile banding
-- Technique: Window Function NTILE + CTE
WITH donor_value AS (
    SELECT
        d.donor_id,
        dn.donor_name,
        dn.donor_type,
        ROUND(SUM(d.amount), 2)  AS total_donated,
        COUNT(d.donation_id)     AS donation_frequency
    FROM   donations d
    JOIN   donors dn ON dn.donor_id = d.donor_id
    GROUP  BY d.donor_id, dn.donor_name, dn.donor_type
),
quartiled AS (
    SELECT *,
        NTILE(4) OVER (ORDER BY total_donated DESC) AS value_quartile
    FROM donor_value
)
SELECT
    value_quartile,
    CASE value_quartile
        WHEN 1 THEN 'Platinum (Top 25%)'
        WHEN 2 THEN 'Gold (25-50%)'
        WHEN 3 THEN 'Silver (50-75%)'
        WHEN 4 THEN 'Bronze (Bottom 25%)'
    END                                    AS segment_label,
    COUNT(*)                               AS num_donors,
    ROUND(AVG(total_donated), 2)           AS avg_donated,
    ROUND(SUM(total_donated), 2)           AS segment_total,
    ROUND(AVG(donation_frequency), 1)      AS avg_frequency
FROM   quartiled
GROUP  BY value_quartile
ORDER  BY value_quartile;


-- 4.9  QUESTION: Which assignments are underperforming — low performance score
--               AND low fundraising relative to their budget?
-- Technique: CTE + multi-condition filter + CASE flag
WITH assignment_perf AS (
    SELECT
        a.assignment_id,
        a.assignment_name,
        a.region,
        a.budget,
        a.performance_score,
        ROUND(COALESCE(SUM(d.amount), 0), 2) AS total_raised,
        COUNT(d.donation_id)                  AS num_donations
    FROM   assignments a
    LEFT   JOIN donations d ON d.assignment_id = a.assignment_id
    GROUP  BY a.assignment_id, a.assignment_name, a.region,
              a.budget, a.performance_score
)
SELECT
    assignment_id,
    assignment_name,
    region,
    budget,
    total_raised,
    num_donations,
    performance_score,
    ROUND(total_raised / NULLIF(ABS(budget), 0) * 100, 2) AS coverage_ratio_pct,
    CASE
        WHEN performance_score < 4
         AND total_raised < (SELECT PERCENTILE_CONT(0.25)
                             WITHIN GROUP (ORDER BY total_raised)
                             FROM assignment_perf) THEN 'Critical Underperformer'
        WHEN performance_score < 5 THEN 'Watch List'
        ELSE 'Acceptable'
    END AS risk_flag
FROM   assignment_perf
WHERE  performance_score < 5
ORDER  BY performance_score ASC, total_raised ASC;


-- 4.10 QUESTION: Repeat donor behaviour — how many times does the average donor give
--               per year, and who are the most frequent contributors?
-- Technique: Multi-level CTE + dense ranking
WITH yearly_frequency AS (
    SELECT
        donor_id,
        YEAR(donation_date)  AS yr,
        COUNT(*)             AS donations_that_year,
        SUM(amount)          AS amount_that_year
    FROM   donations
    GROUP  BY donor_id, YEAR(donation_date)
),
donor_summary AS (
    SELECT
        yf.donor_id,
        dn.donor_name,
        dn.donor_type,
        ROUND(AVG(yf.donations_that_year), 2)  AS avg_donations_per_year,
        ROUND(SUM(yf.amount_that_year), 2)     AS lifetime_value,
        COUNT(DISTINCT yf.yr)                   AS years_active
    FROM   yearly_frequency yf
    JOIN   donors dn ON dn.donor_id = yf.donor_id
    GROUP  BY yf.donor_id, dn.donor_name, dn.donor_type
)
SELECT
    DENSE_RANK() OVER (ORDER BY avg_donations_per_year DESC) AS freq_rank,
    donor_id,
    donor_name,
    donor_type,
    avg_donations_per_year,
    lifetime_value,
    years_active
FROM   donor_summary
ORDER  BY freq_rank
LIMIT  20;


-- 4.11 QUESTION: Assignment duration vs. fundraising effectiveness — do longer
--               assignments raise more money?
-- Technique: CTE + correlated aggregation + duration buckets
WITH assignment_enriched AS (
    SELECT
        a.assignment_id,
        a.assignment_name,
        a.region,
        a.performance_score,
        DATEDIFF(a.end_date, a.start_date)   AS duration_days,
        ROUND(SUM(d.amount), 2)              AS total_raised,
        COUNT(d.donation_id)                 AS num_donations
    FROM   assignments a
    LEFT   JOIN donations d ON d.assignment_id = a.assignment_id
    GROUP  BY a.assignment_id, a.assignment_name, a.region,
              a.performance_score, a.start_date, a.end_date
)
SELECT
    CASE
        WHEN duration_days <= 30  THEN '0-30 days'
        WHEN duration_days <= 90  THEN '31-90 days'
        WHEN duration_days <= 180 THEN '91-180 days'
        ELSE                           '180+ days'
    END                                 AS duration_bucket,
    COUNT(*)                            AS num_assignments,
    ROUND(AVG(total_raised), 2)         AS avg_raised,
    ROUND(AVG(num_donations), 1)        AS avg_donations,
    ROUND(AVG(performance_score), 2)    AS avg_performance,
    ROUND(SUM(total_raised), 2)         AS total_raised
FROM   assignment_enriched
GROUP  BY 1
ORDER  BY MIN(duration_days);


-- 4.12 QUESTION: Build a comprehensive donor 360° view (single-row summary per donor)
-- Technique: CTE fan-out + LEFT JOIN aggregation
WITH donation_agg AS (
    SELECT
        donor_id,
        COUNT(*)                                  AS total_donations,
        ROUND(SUM(amount), 2)                     AS lifetime_value,
        ROUND(AVG(amount), 2)                     AS avg_donation,
        MIN(donation_date)                        AS first_donation_date,
        MAX(donation_date)                        AS last_donation_date,
        DATEDIFF(MAX(donation_date), MIN(donation_date)) AS tenure_days,
        COUNT(DISTINCT assignment_id)             AS assignments_supported,
        ROUND(MAX(amount), 2)                     AS largest_gift
    FROM   donations
    GROUP  BY donor_id
),
region_agg AS (
    SELECT
        d.donor_id,
        COUNT(DISTINCT a.region)                         AS regions_supported,
        GROUP_CONCAT(DISTINCT a.region ORDER BY a.region) AS supported_regions
    FROM   donations d
    JOIN   assignments a ON a.assignment_id = d.assignment_id
    GROUP  BY d.donor_id
)
SELECT
    dn.donor_id,
    dn.donor_name,
    dn.donor_type,
    da.total_donations,
    da.lifetime_value,
    da.avg_donation,
    da.first_donation_date,
    da.last_donation_date,
    da.tenure_days,
    da.assignments_supported,
    da.largest_gift,
    ra.regions_supported,
    ra.supported_regions,
    NTILE(4) OVER (ORDER BY da.lifetime_value DESC) AS value_tier
FROM   donors dn
LEFT   JOIN donation_agg da  ON da.donor_id  = dn.donor_id
LEFT   JOIN region_agg   ra  ON ra.donor_id  = dn.donor_id
ORDER  BY da.lifetime_value DESC NULLS LAST;


-- =============================================================================
-- SECTION 5: REPORTING VIEWS (reusable for dashboards / BI tools)
-- =============================================================================

CREATE OR REPLACE VIEW vw_donor_leaderboard AS
WITH ranked AS (
    SELECT
        dn.donor_id,
        dn.donor_name,
        dn.donor_type,
        ROUND(SUM(d.amount), 2)  AS total_donated,
        COUNT(d.donation_id)     AS donations_made,
        RANK() OVER (ORDER BY SUM(d.amount) DESC) AS overall_rank,
        RANK() OVER (PARTITION BY dn.donor_type ORDER BY SUM(d.amount) DESC) AS type_rank
    FROM   donations d
    JOIN   donors dn ON dn.donor_id = d.donor_id
    GROUP  BY dn.donor_id, dn.donor_name, dn.donor_type
)
SELECT * FROM ranked;


CREATE OR REPLACE VIEW vw_assignment_dashboard AS
SELECT
    a.assignment_id,
    a.assignment_name,
    a.region,
    a.start_date,
    a.end_date,
    DATEDIFF(a.end_date, a.start_date)    AS duration_days,
    a.budget,
    a.performance_score,
    COALESCE(agg.total_raised, 0)         AS total_raised,
    COALESCE(agg.num_donations, 0)        AS num_donations,
    ROUND(COALESCE(agg.total_raised, 0)
          / NULLIF(ABS(a.budget), 0) * 100, 2) AS budget_coverage_pct
FROM   assignments a
LEFT JOIN (
    SELECT assignment_id,
           ROUND(SUM(amount), 2) AS total_raised,
           COUNT(*)              AS num_donations
    FROM   donations
    GROUP  BY assignment_id
) agg ON agg.assignment_id = a.assignment_id;


CREATE OR REPLACE VIEW vw_monthly_kpi AS
SELECT
    DATE_FORMAT(donation_date, '%Y-%m')          AS month,
    COUNT(*)                                      AS total_donations,
    COUNT(DISTINCT donor_id)                      AS unique_donors,
    ROUND(SUM(amount), 2)                         AS total_raised,
    ROUND(AVG(amount), 2)                         AS avg_donation,
    ROUND(SUM(amount)
          / LAG(SUM(amount)) OVER (ORDER BY DATE_FORMAT(donation_date, '%Y-%m')) * 100 - 100
        , 2)                                      AS mom_growth_pct
FROM   donations
GROUP  BY DATE_FORMAT(donation_date, '%Y-%m');

-- =============================================================================
-- END OF SCRIPT
-- =============================================================================
