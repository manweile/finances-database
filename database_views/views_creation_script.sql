-- Shows total spent per month, increase from prior month, and the running total for the year the month is in
CREATE VIEW monthly_spend_summary AS
-- CTE to perform initial aggregation
WITH cte AS (
    SELECT
        d.month_number,
        d.month_name AS month,
        d.year,
        -- Take absolute value since transaction_amount is negative for purchases
        SUM(ABS(tf.transaction_amount)) AS month_amount_spent
    FROM transaction_facts AS tf
    JOIN transaction_type AS t 
        USING (transaction_type_id)
    -- Inner join with category filters out all transactions that are not purchases
    JOIN category AS c
        USING (category_id)
    JOIN date AS d 
        USING (short_date)
    -- Not interested in 2022
    WHERE d.year != '2022'
    GROUP BY month, d.month_number, d.year
    ORDER BY d.year, d.month_number
)
SELECT
    *,
    -- Calculate dollar increase from prior month as decimal
    ROUND((month_amount_spent - LAG(month_amount_spent)
                                    OVER (ORDER BY year, month_number))
        / LAG(month_amount_spent) OVER (ORDER BY year, month_number),
          2) AS prior_month_change,
    -- Calculate yearly running total spent
    SUM(month_amount_spent)
        OVER (
            PARTITION BY year
            ORDER BY year, month_number
        ) AS yearly_running_total
FROM cte;
--------------------------------------------------------------------------------------------------------------------------------------------------------
-- Shows and ranks amount spent per category per month
CREATE VIEW monthly_spend_category AS
SELECT
    d.year,
    d.month_number,
    d.month_name,
    c.category_description,
    -- Take absolute value since transaction_amount is negative for purchases
    SUM(ABS(tf.transaction_amount)) AS monthly_spend,
    -- Rank spending per category by month and year
    RANK()
        OVER(
        PARTITION BY month_number, month_name, year 
            ORDER BY ABS(SUM(tf.transaction_amount)) DESC
        ) AS month_ranking
FROM transaction_facts AS tf
-- Inner join with category filters out all non-purchase transactions
JOIN category AS c
    USING (category_id)
JOIN account AS a 
    USING (account_id)
JOIN transaction_type AS t 
    USING (transaction_type_id)
JOIN date AS d 
    USING (short_date)
-- Not interested in 2022
WHERE d.year != '2022'
GROUP BY month_number, month_name, year, category_description
ORDER BY year, month_number, month_ranking;
--------------------------------------------------------------------------------------------------------------------------------------------------------
-- Shows all transactions and the yearly running total spent
CREATE VIEW daily_spend AS
SELECT
    d.short_date,
    -- Get daily spend amount. Take abs since transaction_amount is negative for purchases 
    -- Assign 0 for days with no purchases.
    COALESCE(ABS(SUM(tf.transaction_amount)), 0) AS date_spend,
    -- Get 7 day running average spent. Assign 0 to days with no transactions.    
    ROUND(AVG(ABS(SUM(COALESCE(tf.transaction_amount, 0))))
        OVER (
            ORDER BY short_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2) AS 7_day_avg_spend,
    -- Sum total spent by year up to the current date
    SUM(ABS(SUM(tf.transaction_amount)))
        OVER (
            PARTITION BY d.year 
            ORDER BY short_date
        ) AS running_yearly_spend
-- Select from date column to keep all dates, not just ones with purchases
FROM date AS d
-- Use left join to keep dates mentioned above. Also need to filter out non-purchases since left join was used
LEFT JOIN transaction_facts AS tf
    ON d.short_date = tf.short_date
    AND tf.transaction_type_id IN (1,2)
WHERE d.short_date <= CURDATE()
    -- Not interested in 2022
    AND year != '2022'
GROUP BY d.short_date
ORDER BY short_date;
--------------------------------------------------------------------------------------------------------------------------------------------------------
-- View created to run visual on Power BI dashboard. Calculates running total spent for each category (daily level of granularity) 
CREATE VIEW daily_category_balance AS
-- CTE to generate all possible days and categories
WITH date_category AS (
    SELECT 
        d.short_date,
        c.category_id
    FROM date AS d
    CROSS JOIN (
        SELECT category_id
        FROM category
    ) AS c
    WHERE d.year != '2022'
        AND d.short_date <= CURDATE()
    ORDER BY short_date, category_id
)
SELECT 
	dc.short_date,
    dc.category_id,
    -- Categorical spending each day
    COALESCE(SUM(ABS(tf.transaction_amount)), 0) AS day_sum
-- Select from cte and left join other tables to keep all dates and categories from cte
FROM date_category AS dc
LEFT JOIN transaction_facts AS tf
    ON dc.short_date = tf.short_date
    AND dc.category_id = tf.category_id
LEFT JOIN transaction_type AS t
    ON tf.transaction_type_id = t.transaction_type_id
WHERE EXTRACT(year FROM dc.short_date) != '2022'
    AND dc.short_date <= CURDATE()
    AND (t.transaction_type_id IN (1, 2) OR t.transaction_type_id IS NULL)
GROUP BY dc.short_date, dc.category_id
ORDER BY dc.short_date, dc.category_id;
--------------------------------------------------------------------------------------------------------------------------------------------------------
-- Calculates balances of all accounts at the end of each month
CREATE VIEW monthly_account_balances AS
SELECT
    -- Combine month and year to get date period
    CONCAT(year, '-' , LPAD(month_number, 2, '0')) AS end_date_period,
    tf.account_id,
    account_type,
    -- Running total represents account balances
    SUM(SUM(transaction_amount))
    OVER(
    PARTITION BY tf.account_id 
            ORDER BY CONCAT(year, '-' , LPAD(month_number, 2, '0'))
    ) AS balance
FROM transaction_facts AS tf
JOIN account 
    USING (account_id)
JOIN date 
    USING (short_date)
GROUP BY end_date_period, account_id, account_type
ORDER BY end_date_period;
