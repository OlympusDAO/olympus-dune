CREATE OR REPLACE VIEW dune_user_generated.ohm_bond_daily_metrics AS

WITH market_time_series AS (
    SELECT markets.id as market_id, day
    FROM (
        SELECT generate_series('2022-01-08 00:00', '2032-01-08 00:00', '1 day'::interval) as day
    ) AS t
    JOIN dune_user_generated.ohm_bond_markets as markets
        ON TRUE
    LEFT JOIN olympus_v2."OlympusBondDepositoryV2_evt_CloseMarket" as market_closed
        ON market_closed.id = markets.id
    WHERE day <= COALESCE(market_closed.evt_block_time, markets.conclusion_date)
        AND day >= date_trunc('day', markets.market_create_time)
),
bcvs as (
    SELECT  market_id
        ,   block_time
        ,   date_trunc('day', block_time) as day
        ,   bcv
    FROM dune_user_generated.ohm_bond_bcvs
),
grouped_bcvs AS (
    SELECT  market_time_series.market_id
        ,   market_time_series.day
        ,   AVG(bcvs.bcv) AS bcv_avg
        ,   MAX(bcvs.block_time) AS latest_tune_date
    FROM market_time_series
    LEFT JOIN bcvs
        ON bcvs.market_id = market_time_series.market_id
        AND bcvs.day = market_time_series.day
    GROUP BY market_time_series.market_id, market_time_series.day
)
, daily_bcvs AS (
   SELECT   present_day.day
        ,   present_day.market_id
        ,   COALESCE(present_day.bcv_avg, past_day.bcv_avg) AS bcv
        ,   COALESCE(present_day.latest_tune_date, past_day.latest_tune_date) as latest_tune_date
    FROM grouped_bcvs AS present_day
    LEFT JOIN LATERAL(
        SELECT  past_day.market_id
            ,   past_day.bcv_avg
            ,   past_day.latest_tune_date
        FROM grouped_bcvs AS past_day
        WHERE past_day.day < present_day.day
            AND past_day.market_id = present_day.market_id
            AND present_day.bcv_avg IS NULL
            AND past_day.bcv_avg IS NOT NULL
        ORDER BY past_day.day DESC
        FETCH FIRST 1 ROW ONLY
    ) AS past_day
        ON TRUE
)
,grouped_bond_prices as (
    SELECT  market_time_series.market_id
        ,   market_time_series.day
        ,   avg(bd.bond_price) as bond_price_avg
    FROM market_time_series
    LEFT JOIN (
        SELECT date_trunc('day', market_create_time) as day, id as market_id, initial_price as bond_price
        FROM  dune_user_generated.ohm_bond_markets
        UNION
        SELECT date_trunc('day', block_time) as day, market_id, bond_price
        FROM dune_user_generated.ohm_bond_deposits
    ) AS bd
        ON bd.market_id = market_time_series.market_id
        AND bd.day = market_time_series.day
    GROUP BY market_time_series.market_id, market_time_series.day
    )
,daily_bond_prices as (
   SELECT   present_day.day
        ,   present_day.market_id
        ,   COALESCE(present_day.bond_price_avg, past_day.bond_price_avg) AS price
    FROM grouped_bond_prices AS present_day
    LEFT JOIN LATERAL(
        SELECT  past_day.market_id
            ,   past_day.bond_price_avg
        FROM grouped_bond_prices AS past_day
        WHERE past_day.day < present_day.day
            AND past_day.market_id = present_day.market_id
            AND present_day.bond_price_avg IS NULL
            AND past_day.bond_price_avg IS NOT NULL
        ORDER BY past_day.day DESC
        FETCH FIRST 1 ROW ONLY
    ) AS past_day
        ON TRUE
)
,grouped_deposits AS (
    SELECT market_id
        ,   day
        ,   COALESCE(bonded_ohm_qty, 0) AS bonded_ohm_qty
        ,   COALESCE(bonded_token_qty, 0) AS bonded_token_qty
        ,   SUM(bonded_ohm_qty) OVER(PARTITION BY market_id ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS bonded_ohm_qty_running_sum
        ,   SUM(bonded_token_qty) OVER(PARTITION BY market_id ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS bonded_token_qty_running_sum
    FROM (
        SELECT  market_time_series.market_id
            ,   market_time_series.day
            ,   SUM(bonded_ohm_qty*1e9) AS bonded_ohm_qty
            ,   SUM(bonded_token_qty) AS bonded_token_qty
        FROM market_time_series
        LEFT JOIN dune_user_generated.ohm_bond_deposits as bd
            ON date_trunc('day', block_time) = market_time_series.day
            AND bd.market_id = market_time_series.market_id
        LEFT JOIN dune_user_generated.ohm_bond_markets as markets
            ON markets.id = bd.market_id
        LEFT JOIN dune_user_generated.ohm_bond_tokens as tokens
            ON tokens.address = markets.quoted_token
        GROUP BY 1, 2
    ) as sx
)
SELECT  day
    ,   market_id
    ,   price
    ,   bcv
    ,   ((running_capacity * deposit_interval) / latest_tune_seconds_to_conclusion)/1e9 AS max_payout --correct
    ,   CASE WHEN remaining_seconds_to_conclusion = 0
            THEN 0
            ELSE ((running_capacity * seconds_in_a_day) / remaining_seconds_to_conclusion)/1e9 
        END AS capacity --correct
    ,   (running_capacity)/1e9 AS running_total_capacity
    ,   bonded_ohm_qty
    ,   bonded_ohm_qty_running_sum
    ,   remaining_seconds_to_conclusion
    ,   running_capacity_in_token
    ,   (target_debt * seconds_in_a_day) / seconds_to_conclusion as daily_bcv_capacity
FROM (
    SELECT  prices.day
        ,   markets.id as market_id
        ,   prices.price
        ,   bcvs.bcv
        ,   (supply.total_supply * 1e9) AS total_supply
        ,   extract(epoch from markets.conclusion_date - (prices.day + markets.conclusion_date::time)::timestamptz) AS remaining_seconds_to_conclusion
        ,   extract(epoch from markets.conclusion_date - bcvs.latest_tune_date) AS latest_tune_seconds_to_conclusion
        ,   86400 AS seconds_in_a_day
        ,   markets.deposit_interval
        ,   (price * supply.total_supply) / bcv AS target_debt --CORRECT
        --,   (price * (supply.total_supply * 1e9) / bcv) * extract(epoch from markets.conclusion_date - (prices.day + markets.conclusion_date::time)::timestamptz) / markets.seconds_to_conclusion AS calc_capacity --Close to correct
        ,   CASE WHEN markets.capacity_in_quote
                THEN ((markets.capacity * 1e18) / COALESCE(prices.price, markets.initial_price) / (10 ^ COALESCE(tokens.decimals, 18))) - COALESCE(grouped_deposits.bonded_ohm_qty_running_sum, 0)
                ELSE
                    markets.capacity - COALESCE(grouped_deposits.bonded_ohm_qty_running_sum, 0) --CORRECT
            END AS running_capacity 
        ,   COALESCE(grouped_deposits.bonded_ohm_qty_running_sum, 0)/1e9 AS bonded_ohm_qty_running_sum
        ,   grouped_deposits.bonded_ohm_qty/1e9 AS bonded_ohm_qty
        ,   CASE WHEN markets.capacity_in_quote
                THEN (markets.capacity/ (10 ^ tokens.decimals)) - COALESCE(grouped_deposits.bonded_token_qty_running_sum, 0)
                ELSE 0 --later i could convert ohm capacity into token equivalent?
            END AS running_capacity_in_token
        ,   markets.seconds_to_conclusion
    FROM dune_user_generated.ohm_bond_markets as markets
    JOIN daily_bond_prices as prices
        ON prices.market_id = markets.id
    JOIN daily_bcvs as bcvs
        ON bcvs.market_id = markets.id
        AND bcvs.day = prices.day
    JOIN dune_user_generated.olydao_ohm_circ_supply as supply
        ON supply."date" = prices.day
    LEFT JOIN grouped_deposits
        ON grouped_deposits.day = prices.day-- - INTERVAL '1 DAY'
        AND grouped_deposits.market_id = prices.market_id
    LEFT JOIN dune_user_generated.ohm_bond_tokens as tokens
        ON tokens.address = markets.quoted_token
) as daily_bond_metrics

