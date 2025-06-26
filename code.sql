-- Create Schema & Tables

-- Customers Table
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(25),
    gender VARCHAR(10),
    city VARCHAR(25),
    age INT,
    income INT,
    signup_date DATE
);
SELECT *
FROM customers;

-- Cards table
CREATE TABLE cards (
    customer_id INT,
    card_type VARCHAR(15),
    credit_limit INT,
    current_balance DECIMAL(10,2),
    credit_score INT,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
SELECT *
FROM cards;

-- Transactions Table
CREATE TABLE transactions (
    transaction_id INT PRIMARY KEY,
    customer_id INT,
    transaction_date DATE,
    amount DECIMAL(10,2),
    merchant_category VARCHAR(15),
    channel VARCHAR(10),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
SELECT *
FROM transactions;

-- KPIs per customer

-- 1. Monthly Spend & Standard Deviation
WITH monthly_spend AS (
  SELECT
    customer_id,
    DATE_TRUNC('month', transaction_date) AS txn_month,
    SUM(amount) AS total_spend
  FROM transactions
  WHERE transaction_date >= CURRENT_DATE - INTERVAL '12 months'
  GROUP BY customer_id, txn_month
),

monthly_stats AS (
  SELECT
    customer_id,
    ROUND(AVG(total_spend), 2) AS avg_monthly_spend,
    ROUND(STDDEV(total_spend), 2) AS spend_std_dev,
    COUNT(*) AS months_active
  FROM monthly_spend
  GROUP BY customer_id
),

-- 2. Last Transaction Gap (Recency/ days since last transaction)
last_txn AS (
  SELECT
    customer_id,
    MAX(transaction_date) AS last_txn_date,
    (CURRENT_DATE - MAX(transaction_date))::INTEGER AS last_txn_gap
  FROM transactions
  GROUP BY customer_id
),

-- 3. Category Diversity
category_count AS (
  SELECT
    customer_id,
    COUNT(DISTINCT merchant_category) AS category_diversity
  FROM transactions
  GROUP BY customer_id
),

-- 4. Channel Usage Ratio
channel_usage AS (
  SELECT
    customer_id,
    COUNT(*) FILTER (WHERE channel = 'Online') AS online_txns,
    COUNT(*) FILTER (WHERE channel = 'Offline') AS offline_txns
  FROM transactions
  GROUP BY customer_id
),
channel_ratio AS (
  SELECT
    customer_id,
    ROUND(
      CASE 
        WHEN offline_txns = 0 THEN 1
        ELSE online_txns / (online_txns + offline_txns) 
      END, 2
    ) AS online_ratio
  FROM channel_usage
),

-- 5. Credit Utilization & Score Band
credit_metrics AS (
  SELECT
    customer_id,
    ROUND(current_balance / credit_limit, 2) AS credit_util_ratio,
    credit_score,
    CASE 
      WHEN credit_score < 580 THEN 'Low'
      WHEN credit_score BETWEEN 580 AND 720 THEN 'Medium'
      ELSE 'High'
    END AS credit_score_band
  FROM cards
)

-- 6. Combine All KPIs
SELECT
	c.customer_id,
  	c.name,
  	ms.avg_monthly_spend,
  	ms.spend_std_dev,
  	ms.months_active,
  	l.last_txn_gap,
  	cd.category_diversity,
  	cr.online_ratio,
  	cm.credit_util_ratio,
  	cm.credit_score,
  	cm.credit_score_band
FROM customers c
LEFT JOIN monthly_stats ms ON c.customer_id = ms.customer_id
LEFT JOIN last_txn l ON c.customer_id = l.customer_id
LEFT JOIN category_count cd ON c.customer_id = cd.customer_id
LEFT JOIN channel_ratio cr ON c.customer_id = cr.customer_id
LEFT JOIN credit_metrics cm ON c.customer_id = cm.customer_id;

-- Output: This query will return one row per customer with all KPIs ready for scoring.

-- Step 1: Create a customer-level metrics summary
-- This aggregates all the data you need per customer

CREATE MATERIALIZED VIEW customer_summary AS
SELECT
c.customer_id,
MAX(c.gender) AS gender,
MAX(c.city) AS city,
MAX(c.age) AS age,
MAX(c.income) AS income,
COUNT(DISTINCT DATE_TRUNC('month', t.transaction_date)) AS active_months,
COUNT(t.transaction_id) AS txn_count,
SUM(t.amount) AS total_spend,
AVG(t.amount) AS avg_spend_per_txn,
COUNT(DISTINCT t.merchant_category) AS category_count,
MAX(t.transaction_date) AS last_txn_date,
MAX(cd.credit_score) AS credit_score,
MAX(cd.current_balance / NULLIF(cd.credit_limit, 0)) AS credit_util_ratio
FROM customers c
LEFT JOIN transactions t ON c.customer_id = t.customer_id
LEFT JOIN cards cd ON c.customer_id = cd.customer_id
GROUP BY c.customer_id;

SELECT * 
FROM customer_summary;

-- Step 2: Add churn label (for validation) — if customer hasn’t transacted in last 90 days

CREATE MATERIALIZED VIEW customer_churn_flag AS
SELECT
cs.*,
CASE 
  WHEN CURRENT_DATE - cs.last_txn_date > 90 THEN 1
  ELSE 0
END AS churn_flag
FROM customer_summary cs;

SELECT *
FROM customer_churn_flag;

--Step 3: Create percentile ranks for each scoring variable
--We’ll use the following fields:
--active_months
--category_count
--credit_util_ratio
--total_spend
--credit_score
--avg_spend_per_txn
CREATE MATERIALIZED VIEW customer_percentile_scores AS
SELECT
*,
NTILE(100) OVER (ORDER BY active_months) AS pct_active_months,
NTILE(100) OVER (ORDER BY category_count) AS pct_category_count,
NTILE(100) OVER (ORDER BY credit_util_ratio DESC) AS pct_utilization,
NTILE(100) OVER (ORDER BY total_spend) AS pct_total_spend,
NTILE(100) OVER (ORDER BY credit_score) AS pct_credit_score,
NTILE(100) OVER (ORDER BY avg_spend_per_txn) AS pct_avg_spend
FROM customer_churn_flag;

SELECT * 
FROM customer_percentile_scores;

-- Step 4: Assign churn risk scores based on bad percentiles
-- You give points if a customer is in a risky zone 
-- Say bottom 25th percentile for some metrics or top 25% for credit utilization
CREATE MATERIALIZED VIEW customer_churn_risk_score AS
SELECT
customer_id,
pct_active_months,
pct_category_count,
pct_utilization,
pct_total_spend,
pct_credit_score,
pct_avg_spend,
churn_flag,
-- Assign rule-based points
CASE WHEN pct_active_months < 25 THEN 2 ELSE 0 END +
CASE WHEN pct_category_count < 25 THEN 2 ELSE 0 END +
CASE WHEN pct_utilization > 75 THEN 2 ELSE 0 END +
CASE WHEN pct_total_spend < 25 THEN 1 ELSE 0 END +
CASE WHEN pct_credit_score < 25 THEN 1 ELSE 0 END +
CASE WHEN pct_avg_spend < 25 THEN 1 ELSE 0 END
AS risk_score
FROM customer_percentile_scores;

SELECT *
FROM customer_churn_risk_score;

-- Step 5: Classify risk buckets
SELECT
*,
CASE
WHEN risk_score >= 6 THEN 'High Risk'
WHEN risk_score BETWEEN 3 AND 5 THEN 'Medium Risk'
ELSE 'Low Risk'
END AS risk_segment
FROM customer_churn_risk_score;

-- Step 6: Validate — what % of each segment actually churned
SELECT
risk_segment,
COUNT(*) AS customers,
SUM(churn_flag) AS churned,
ROUND(100.0 * SUM(churn_flag)::numeric / COUNT(*), 2) AS churn_rate
FROM (
SELECT
*,
CASE
WHEN risk_score >= 6 THEN 'High Risk'
WHEN risk_score BETWEEN 3 AND 5 THEN 'Medium Risk'
ELSE 'Low Risk'
END AS risk_segment
FROM customer_churn_risk_score
) sub
GROUP BY risk_segment
ORDER BY risk_segment;