--DROP VIEW dune_user_generated.olympus_bonds_status;

CREATE OR REPLACE VIEW dune_user_generated.olympus_bonds_status AS
WITH reformatted_capacities as (
    SELECT ohm_bond_markets.id as market_id
        , quoted_token_name
        , CASE WHEN capacity_in_quote THEN '' || TO_CHAR(capacity_in_quoted_token::NUMERIC, 'FM9,999,999,999') || ' ' || quoted_token_name ELSE '' || TO_CHAR(capacity_in_ohm::NUMERIC, 'FM9,999,999') || ' OHM' END as initial_capacity
        , CASE WHEN capacity_in_quote THEN COALESCE((total_token_qty / capacity_in_quoted_token), 0) ELSE COALESCE((total_ohm_bonded / capacity_in_ohm), 0) END as perc_bonded
        , market_create_time
        , conclusion_date
        , tune_interval_hours / 24 as tune_interval_days
        , max_payout
        , vesting_days
    FROM dune_user_generated.ohm_bond_markets
    LEFT JOIN (
        select market_id, sum(bonded_ohm_qty) as total_ohm_bonded, sum(bonded_token_qty) as total_token_qty
        FROM dune_user_generated.ohm_bond_deposits
        group by market_id
    ) as d
    on id = d.market_id
),

last_metrics as (
    SELECT DISTINCT ON (market_id) market_id, day, daily_bcv_capacity
    FROM dune_user_generated.ohm_bond_daily_metrics
    order by market_id, day desc
),

latest_price as (
    SELECT
        ("amount1In" + "amount1Out")/1e18 / (("amount0In" + "amount0Out")/1e9) AS price
    FROM sushi."Pair_evt_Swap" sw
    WHERE contract_address = '\x055475920a8c93cffb64d039a8205f7acc7722d3'
    ORDER BY sw."evt_block_time" desc
    LIMIT 1
),

final as (
    SELECT f.market_id
        , CASE WHEN f.perc_bonded > 0.999 OR NOW() > f.conclusion_date OR f.market_id in (SELECT id FROM olympus_v2."OlympusBondDepositoryV2_evt_CloseMarket") THEN 'CLOSE' ELSE 'OPEN' END AS status
        , f.quoted_token_name
        , f.initial_capacity
        , f.perc_bonded
        , f.market_create_time
        , f.conclusion_date
        , f.vesting_days
        , f.tune_interval_days
        , f.max_payout
    FROM reformatted_capacities as f
)

SELECT final.market_id
    , final.status
    , final.quoted_token_name
    , final.initial_capacity
    , final.perc_bonded
    , final.market_create_time
    , final.conclusion_date
    , final.vesting_days
    , final.tune_interval_days
    , max_payout * latest_price.price as max_payout_usd
    , CASE WHEN status = 'OPEN' THEN last_metrics.daily_bcv_capacity ELSE 0 END AS daily_bcv_capacity
    , CASE WHEN status = 'OPEN' THEN last_metrics.daily_bcv_capacity * latest_price.price ELSE 0 END AS daily_usd_capacity
FROM latest_price, final
LEFT JOIN
last_metrics
ON final.market_id = last_metrics.market_id
ORDER BY status DESC , final.market_id desc

