WITH  market_time_series AS (
    SELECT markets.id as market_id, minute
    FROM (
        SELECT generate_series('2022-01-08 00:00', now(), '1 minute'::interval) as minute
    ) AS t
    JOIN dune_user_generated.ohm_inverse_bond_markets as markets
        ON TRUE
    LEFT JOIN olympus_v2."OlympusProV2_evt_CloseMarket" as market_closed
        ON market_closed.id = markets.id
    WHERE minute <= COALESCE(market_closed.evt_block_time, markets.conclusion_date)
        AND minute >= date_trunc('minute', markets.market_create_time)
)
, minute_series AS (
    SELECT generate_series(t.start_date, now(), '1 minute'::interval) as minute
    FROM (
        SELECT MIN(minute) as start_date
        FROM market_time_series
    ) as t
)
, swap AS ( 
        SELECT
            date_trunc('minute', sw."evt_block_time") as minute,
            ("amount0In" + "amount0Out")/1e9 AS a0_amt, 
            ("amount1In" + "amount1Out")/1e18 AS a1_amt
            
        FROM sushi."Pair_evt_Swap" sw
        WHERE evt_block_time >= (SELECT MIN(minute) FROM market_time_series)
            AND contract_address = '\x055475920a8c93cffb64d039a8205f7acc7722d3' 
)
, price as (
    select  swap."minute"
        ,   AVG((a1_amt/a0_amt)) as price
    from swap
    GROUP BY 1
)
, prices_per_minute as (
    SELECT series.minute, COALESCE(present_minute.price, past_minute.price) as price
    FROM minute_series as series
    LEFT JOIN price as present_minute
        ON present_minute.minute = series.minute
    LEFT JOIN LATERAL(
        SELECT past_minute.price
        FROM price AS past_minute
        WHERE past_minute.minute < series.minute
            AND present_minute IS NULL
            AND past_minute.price IS NOT NULL
        ORDER BY past_minute.minute DESC
        FETCH FIRST 1 ROW ONLY
    ) AS past_minute
        ON TRUE
)

/******TEST DATA***********
, trans as (
    SELECT quote_token
        ,   POSITION(quote_token in "data")+20 as bond_param_data_position
        ,   "data" as tran_data
    FROM (
        SELECT '\xfabcbb7b0000000000000000000000005f98805a4e8be255a32880fdec7f6728c6568ba000000000000000000000000064aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d500000000000000000000000000000000000000000000be951906eba2aa8000000000000000000000000000000000000000000000000000000072ea6cf30b82000000000000000000000000000000000000000000000000000088a490a601540000000000000000000000000000000000000000000000000000000000000186a00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000062213a60000000000000000000000000000000000000000000000000000000000000546000000000000000000000000000000000000000000000000000000000006ddd00'::bytea AS "data"
        ,   '\x64aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d5'::bytea as quote_token
    ) as a
)
, markets AS (
    SELECT      24 AS id
            ,   '\x5f98805a4e8be255a32880fdec7f6728c6568ba0'::bytea AS base_token
            ,   2 as vesting_days
            ,   BYTEA2NUMERIC(SUBSTRING(tran_data from  bond_param_data_position for 32)) AS Capacity
            ,   BYTEA2NUMERIC(SUBSTRING(tran_data from  bond_param_data_position + (32 * 1) for 32)) AS Initial_Price
            ,   BYTEA2NUMERIC(SUBSTRING(tran_data from  bond_param_data_position + (32 * 2) for 32)) AS minimum_price
            ,   BYTEA2NUMERIC(SUBSTRING(tran_data from  bond_param_data_position + (32 * 3) for 32)) AS Debt_Buffer
            ,   BYTEA2NUMERIC(SUBSTRING(tran_data from  bond_param_data_position + (32 * 4) for 32)) AS CapacityInQuote_Bool
            ,   BYTEA2NUMERIC(SUBSTRING(tran_data from  bond_param_data_position + (32 * 5) for 32)) AS FixedTerm_Bool
            ,   BYTEA2NUMERIC(SUBSTRING(tran_data from  bond_param_data_position + (32 * 6) for 32)) AS Vesting
            ,   BYTEA2NUMERIC(SUBSTRING(tran_data from  bond_param_data_position + (32 * 7) for 32)) AS Conclusion
            ,   BYTEA2NUMERIC(SUBSTRING(tran_data from  bond_param_data_position + (32 * 8) for 32)) AS Deposit_Interval
            ,   BYTEA2NUMERIC(SUBSTRING(tran_data from  bond_param_data_position + (32 * 9) for 32)) AS Tune_Interval
    FROM trans
)
, deposits AS (
    SELECT  markets.id AS market_id
        ,   '2022-03-08 07:32'::timestamp AS block_time
        ,   24896.2 AS payout_token_qty
        ,   778 AS bonded_ohm_qty
        ,   31250000000000000 AS bond_price
        ,   '\x902669f80014369a180f3474b8cb2cee5e07b8631ee7278f8c5942ca6610a095'::bytea as evt_tx_hash
    FROM markets
    UNION
    SELECT  markets.id AS market_id
        ,   '2022-03-08 08:00'::timestamp AS block_time
        ,   100000 AS payout_token_qty
        ,   3081.66 AS bonded_ohm_qty
        ,   30816640986132500 AS bond_price
        ,   '\xd929a5aaad8e19b34bdde3a96ce5edfc19d8fcf5759704ef6ddf6ad0d9f6d1a8'::bytea as evt_tx_hash
    FROM markets
)
*/
, bond_market_deposits AS (
    SELECT  date_trunc('minute', deposits.block_time) AS minute
        ,   market_id
        ,   markets.vesting_days
        ,   tokens.name as token_name
        ,   CASE 
                WHEN bond_type = 'LP' 
                    THEN (lp_prices.slp_price/deposits.bond_price) * (10 ^ tokens.decimals) 
                ELSE (token_prices.price/deposits.bond_price) * (10 ^ tokens.decimals) 
            END AS bond_price_usd
        ,   CASE 
                WHEN bond_type = 'LP' 
                    THEN lp_prices.slp_price * deposits.payout_token_qty
                ELSE token_prices.price * deposits.payout_token_qty
            END AS usd_payout
        ,   ohm_prices.price as ohm_price
        ,   deposits.bonded_ohm_qty AS bonded_ohm
        ,   deposits.payout_token_qty
        ,   deposits.evt_tx_hash
    FROM dune_user_generated.ohm_inverse_bond_deposits AS deposits
    JOIN dune_user_generated.ohm_inverse_bond_markets AS markets
        ON markets.id = deposits.market_id
    JOIN dune_user_generated.ohm_bond_tokens AS tokens
        ON tokens.address = markets.base_token
    LEFT JOIN prices_per_minute as ohm_prices 
        ON ohm_prices.minute = date_trunc('minute', deposits.block_time) 
    LEFT JOIN dune_user_generated.olympus_pools as lp_prices
        ON lp_prices.pool = markets.base_token
        AND lp_prices.Date = date_trunc('hour', deposits.block_time)
    LEFT JOIN prices.usd as token_prices 
        ON token_prices.minute = date_trunc('minute', deposits.block_time) 
        AND token_prices.contract_address = markets.base_token
)    

SELECT  minute
    ,   token_name as bond
    ,   vesting_days
    ,   usd_payout
    ,   payout_token_qty
    ,   bonded_ohm as purchased_ohm
    ,   (bond_price_usd - ohm_price) / bond_price_usd as roi
    ,   evt_tx_hash
FROM bond_market_deposits
ORDER BY 1;

        