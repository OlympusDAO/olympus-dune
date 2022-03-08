CREATE OR REPLACE VIEW dune_user_generated.ohm_inverse_bond_markets AS
/*
--Testing before contract deploy. Used remix to grab data for CreateMarket
WITH trans as (
    SELECT quote_token
        ,   POSITION(quote_token in "data")+20 as bond_param_data_position
        ,   "data" as tran_data
    FROM (
        SELECT '\xfabcbb7b0000000000000000000000005f98805a4e8be255a32880fdec7f6728c6568ba000000000000000000000000064aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d500000000000000000000000000000000000000000000be951906eba2aa8000000000000000000000000000000000000000000000000000000072ea6cf30b82000000000000000000000000000000000000000000000000000088a490a601540000000000000000000000000000000000000000000000000000000000000186a00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000062213a60000000000000000000000000000000000000000000000000000000000000546000000000000000000000000000000000000000000000000000000000006ddd00'::bytea AS "data"
        ,   '\x64aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d5'::bytea as quote_token
    ) as a
)
SELECT 
            BYTEA2NUMERIC(SUBSTRING(tran_data from  bond_param_data_position for 32)) AS Capacity
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
*/

WITH create_market_trans AS (
    select  BYTEA2NUMERIC(logs."topic2") AS market_id
        ,   COALESCE(create_market."baseToken", SUBSTRING(logs."topic3" from 13 for 21)::bytea) as base_token
        ,   COALESCE(create_market."quoteToken", SUBSTRING(logs."topic4" from 13 for 21)::bytea) AS quoted_token
        ,   (POSITION(COALESCE(create_market."quoteToken", SUBSTRING(logs."topic4" from 13 for 21)) in trans."data"))+20 as bond_param_data_position
        ,   trans.block_time AS market_create_time
        ,   trans.hash
        ,   trans."data" AS tran_data
    from ethereum."transactions" as trans
    JOIN ethereum."logs" AS logs
        ON logs.tx_hash = trans.hash
        AND logs.contract_address = '\x22AE99D07584A2AE1af748De573c83f1B9Cdb4c0' --BondDepo
        AND topic1 = '\xc983f5286f433c36d6f24f4fda9749b373987ca1e2826ab14e6add4d36406cc0' --CreateMarket Event
    LEFT JOIN  olympus_v2."OlympusProV2_evt_CreateMarket" AS create_market
        ON create_market.evt_tx_hash = trans.hash
        AND create_market.evt_index = logs.index
    WHERE trans."to" = '\x0cf30dc0d48604a301df8010cdc028c055336b2e' --Policy MultiSig
)
, CreateMarket AS (
    SELECT market_id
        ,   base_token
        ,   quoted_token
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
        ,   market_create_time
        ,   hash
    FROM create_market_trans
)
,parsed as (
    SELECT  createmarket.market_id
        ,   tokens.name AS base_token_name
        ,   CreateMarket.base_token
        ,   extract(epoch from TO_TIMESTAMP(conclusion) - CreateMarket.market_create_time) AS seconds_to_conclusion
        ,   CASE createmarket.capacityinquote_bool
                WHEN 1
                THEN 
                    CreateMarket.capacity * (1 * (10 ^ 18)) / CreateMarket.initial_price / (10 ^ COALESCE(tokens.decimals, 18))
                ELSE CreateMarket.capacity
            END AS target_debt
        ,   COALESCE(tokens.decimals, 18) as decimals
        ,   capacity/1e9 AS capacity_in_ohm
        ,   capacity / (1 * (10 ^ COALESCE(tokens.decimals, 18))) AS capacity_in_quoted_token
        ,   DATE_PART('day', TO_TIMESTAMP(vesting) - '1970-01-01 00:00:00'::timestamp) AS Vesting_Days
        ,   TO_TIMESTAMP(vesting) AS fixed_vested_date
        ,   TO_TIMESTAMP(conclusion) AS conclusion_date
        ,   conclusion
        ,   CASE createmarket.capacityinquote_bool
                WHEN 1
                THEN true
                ELSE false
            END as capacity_in_quote
        ,   CASE createmarket.fixedterm_bool
                WHEN 1
                THEN true
                ELSE false
            END as fixed_term
        ,   extract(epoch from TO_TIMESTAMP(deposit_interval) - '1970-01-01 00:00:00'::timestamp)/3600 AS deposit_interval_hours
        ,   extract(epoch from TO_TIMESTAMP(tune_interval) - '1970-01-01 00:00:00'::timestamp)/3600 AS tune_interval_hours
        ,   CreateMarket.capacity
        ,   CreateMarket.deposit_interval
        ,   CreateMarket.tune_interval
        ,   CreateMarket.initial_price
        ,   CreateMarket.minimum_price
        ,   CreateMarket.debt_buffer / 1e5 as debt_buffer
        ,   CreateMarket.market_create_time
    FROM CreateMarket
    LEFT JOIN dune_user_generated.ohm_bond_tokens as tokens
        ON tokens.address = createmarket.base_token
)
SELECT  market_id AS id
    ,   market_create_time
    ,   base_token_name
    ,   base_token
    ,   capacity_in_ohm
    ,   capacity_in_quoted_token
    ,   vesting_days
    ,   fixed_vested_date
    ,   conclusion_date
    ,   capacity_in_quote
    ,   fixed_term
    ,   deposit_interval_hours
    ,   tune_interval_hours
    ,   CAST(TRUNC(initial_price * (10 ^ decimals) / target_debt) AS NUMERIC) AS initial_bcv
    ,   target_debt * deposit_interval / seconds_to_conclusion AS max_payout
    ,   (target_debt + (target_debt * debt_buffer)) / (1 * (10 ^ decimals)) AS max_debt_quoted
    ,   capacity
    ,   deposit_interval
    ,   tune_interval
    ,   debt_buffer
    ,   initial_price
    ,   minimum_price
    ,   seconds_to_conclusion
    ,   target_debt
FROM parsed
JOIN dune_user_generated.olydao_ohm_circ_supply as supply
    ON supply."date" = date_trunc('day', parsed.market_create_time);
    
    
    