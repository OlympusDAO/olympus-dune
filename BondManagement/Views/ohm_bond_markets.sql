CREATE OR REPLACE VIEW dune_user_generated.ohm_bond_markets AS

WITH create_market_trans AS (
    select  BYTEA2NUMERIC(logs."topic2") AS market_id
        ,   create_market."quoteToken" AS quoted_token
        ,   (POSITION(create_market."quoteToken" in trans."data"))+20 as bond_param_data_position
        ,   trans.block_time AS market_create_time
        ,   trans.hash
        ,   trans."data" AS tran_data
    from ethereum."transactions" as trans
    JOIN ethereum."logs" AS logs
        ON logs.tx_hash = trans.hash
        AND logs.contract_address = '\x9025046c6fb25Fb39e720d97a8FD881ED69a1Ef6' --BondDepo
        AND topic1 = '\x2f6ff727bd580b1d1b8332e28aa93ed4ec9d8b08d6e30d6b4c9f7aa63ca17f63' --CreateMarket Event
    JOIN  olympus_v2."OlympusBondDepositoryV2_evt_CreateMarket" AS create_market
        ON create_market.evt_tx_hash = trans.hash
        AND create_market.evt_index = logs.index
    WHERE trans."to" = '\x0cf30dc0d48604a301df8010cdc028c055336b2e' --Policy MultiSig
)
, CreateMarket AS (
    SELECT market_id
        ,   quoted_token
        ,   BYTEA2NUMERIC(SUBSTRING(tran_data from  bond_param_data_position for 32)) AS Capacity
        ,   BYTEA2NUMERIC(SUBSTRING(tran_data from  bond_param_data_position + (32 * 1) for 32)) AS Initial_Price
        ,   BYTEA2NUMERIC(SUBSTRING(tran_data from  bond_param_data_position + (32 * 2) for 32)) AS Debt_Buffer
        ,   BYTEA2NUMERIC(SUBSTRING(tran_data from  bond_param_data_position + (32 * 3) for 32)) AS CapacityInQuote_Bool
        ,   BYTEA2NUMERIC(SUBSTRING(tran_data from  bond_param_data_position + (32 * 4) for 32)) AS FixedTerm_Bool
        ,   BYTEA2NUMERIC(SUBSTRING(tran_data from  bond_param_data_position + (32 * 5) for 32)) AS Vesting
        ,   BYTEA2NUMERIC(SUBSTRING(tran_data from  bond_param_data_position + (32 * 6) for 32)) AS Conclusion
        ,   BYTEA2NUMERIC(SUBSTRING(tran_data from  bond_param_data_position + (32 * 7) for 32)) AS Deposit_Interval
        ,   BYTEA2NUMERIC(SUBSTRING(tran_data from  bond_param_data_position + (32 * 8) for 32)) AS Tune_Interval
        ,   market_create_time
        ,   hash
    FROM create_market_trans
)
,parsed as (
    SELECT  createmarket.market_id
        ,   tokens.name AS Quoted_Token_name
        ,   CreateMarket.quoted_token
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
        ,   CreateMarket.debt_buffer / 1e5 as debt_buffer
        ,   CreateMarket.market_create_time
    FROM CreateMarket
    LEFT JOIN dune_user_generated.ohm_bond_tokens as tokens
        ON tokens.address = createmarket.quoted_token
    LEFT JOIN  ethereum.contracts as eth_con
        ON eth_con.address = createmarket.quoted_token
)
SELECT  market_id AS id
    ,   market_create_time
    ,   quoted_token_name
    ,   quoted_token
    ,   capacity_in_ohm
    ,   capacity_in_quoted_token
    ,   vesting_days
    ,   fixed_vested_date
    ,   conclusion_date
    ,   capacity_in_quote
    ,   fixed_term
    ,   deposit_interval_hours
    ,   tune_interval_hours
    ,   CAST(TRUNC(initial_price * (supply.total_supply*1e9) / target_debt) AS NUMERIC) AS initial_bcv
    ,   target_debt * deposit_interval / seconds_to_conclusion / 1e9 AS max_payout
    ,   (target_debt + (target_debt * debt_buffer)) / 1e9 AS max_debt_ohm
    ,   (target_debt + (target_debt * debt_buffer)) / (1 * (10 ^ decimals)) AS max_debt_quoted
    ,   capacity
    ,   deposit_interval
    ,   tune_interval
    ,   debt_buffer
    ,   initial_price
    ,   seconds_to_conclusion
    ,   target_debt
FROM parsed
JOIN dune_user_generated.olydao_ohm_circ_supply as supply
    ON supply."date" = date_trunc('day', parsed.market_create_time);
    
    
    