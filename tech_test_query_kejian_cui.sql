-- generate a date range
WITH date_range AS (
    SELECT generate_series(
        '2020-06-01'::date,
        '2020-09-30'::date,
        interval '1 day'
    ) AS dt_report
),

-- Filter trades
filtered_trades AS (
    SELECT 
        *
    FROM 
        trades t
    JOIN
        users u ON t.login_hash = u.login_hash
    WHERE 
        u.enable = 1
        AND t.contractsize IS NOT NULL
),

-- rolling for 7 days
rolling_7_day_metrics AS (
    SELECT
        dr.dt_report,
        t.login_hash,
        t.server_hash,
        t.symbol,
        t.currency,
        
        -- Calculate sum of volume in the last 7 days for each `dt_report`
        COALESCE(SUM(t.volume) OVER (
            PARTITION BY t.login_hash, t.server_hash, t.symbol
            ORDER BY t.close_time
            RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW
        ), 0) AS sum_volume_prev_7d,
        
        -- Calculate rank of volume in the last 7 days for each `dt_report`
        DENSE_RANK() OVER (
            PARTITION BY t.login_hash, t.symbol 
            ORDER BY SUM(t.volume) DESC
        ) AS rank_volume_symbol_prev_7d,
        
        -- Calculate rank of trade count in the last 7 days for each `dt_report`
        DENSE_RANK() OVER (
            PARTITION BY t.login_hash 
            ORDER BY COUNT(t.ticket_hash) DESC
        ) AS rank_count_prev_7d
    FROM
        date_range dr
    LEFT JOIN 
        filtered_trades t ON dr.dt_report = DATE(t.close_time)
    WHERE
        t.close_time BETWEEN dr.dt_report - INTERVAL '6 days' AND dr.dt_report
),

-- cumulative sum for all volumn
cumulative_metrics AS (
    SELECT
        dr.dt_report,
        t.login_hash,
        t.server_hash,
        t.symbol,
        t.currency,
        
        COALESCE(SUM(t.volume) OVER (
            PARTITION BY t.login_hash, t.server_hash, t.symbol
            ORDER BY dr.dt_report
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 0) AS sum_volume_prev_all,
        
        COALESCE(SUM(CASE WHEN t.close_time BETWEEN '2020-08-01' AND '2020-08-31' THEN t.volume ELSE 0 END), 0) AS sum_volume_2020_08,
        
        MIN(t.open_time) OVER (
            PARTITION BY t.login_hash, t.server_hash, t.symbol
            ORDER BY dr.dt_report
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS date_first_trade
    FROM
        date_range dr
    LEFT JOIN
        filtered_trades t ON t.close_time <= dr.dt_report
)

-- Combine the results
SELECT
    r7.dt_report,
    r7.login_hash,
    r7.server_hash,
    r7.symbol,
    r7.currency,
    r7.sum_volume_prev_7d,
    c.sum_volume_prev_all,
    r7.rank_volume_symbol_prev_7d,
    r7.rank_count_prev_7d,
    c.sum_volume_2020_08,
    c.date_first_trade,
    
    -- Add row number
    ROW_NUMBER() OVER (ORDER BY r7.dt_report DESC, r7.login_hash, r7.server_hash, r7.symbol, r7.currency) AS row_number
FROM
    rolling_7_day_metrics r7
JOIN
    cumulative_metrics c ON r7.dt_report = c.dt_report
    AND r7.login_hash = c.login_hash
    AND r7.server_hash = c.server_hash
    AND r7.symbol = c.symbol
    AND r7.currency = c.currency
ORDER BY
    row_number DESC;