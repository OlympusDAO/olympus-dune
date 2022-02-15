WITH new_bond AS (
    SELECT  token_address::bytea
        ,   create_date
        ,   conclusion_date
        ,   ROUND(extract(epoch from conclusion_date)) as conclusion_epoch
        ,   extract(epoch from conclusion_date - create_date) AS seconds_market_is_live
        ,   vesting_length * 86400 as vesting_length_seconds
        ,   tune_interval * 3600 as tune_interval
        ,   deposit_interval * 3600 as deposit_interval
        ,   CASE 
                WHEN capacity_in_quote_token = 'true'
                    then true
                ELSE false
            END as capacity_in_quote_token
        ,   daily_capacity
        ,   initial_price
        ,   ip_discount
    FROM (
        SELECT  '{{A_Token_Address}}' as token_address
            ,   '{{B_Bond_Create_Date}}'::timestamp as create_date
            ,   '{{C_Conclusion_Date}}'::timestamp as conclusion_date
            ,   '{{D_Vest_Term_Days}}'::FLOAT as vesting_length
            ,   '{{E_Tune_Interval_Hrs}}'::FLOAT as tune_interval
            ,   '{{F_Deposit_Interval_Hrs}}'::FLOAT as deposit_interval
            ,   '{{G_Is_Capacity_in_Token}}' as capacity_in_quote_token
            ,   '{{H_Daily_Capacity_decimal}}'::FLOAT as daily_capacity
            ,   '{{I_Initial_Price_decimal}}'::FLOAT as initial_price
            ,   '{{J_Bond_Price_Initial_Discount}}':: FLOAT as ip_discount
        ) as input
)
, calc AS (
    SELECT tokens.name
        ,   new_bond.*
        ,   seconds_market_is_live / 86400 AS days_market_is_live
        ,   ohm_price.price_usd / COALESCE(prices.price_usd, initial_price) as ohm_token_price
        ,   ohm_price.price_usd AS ohm_price
        ,   prices.price_usd AS token_price
        ,   tokens.decimals
    FROM new_bond
    JOIN dune_user_generated.ohm_bond_tokens as tokens
        ON tokens.address = new_bond.token_address
    JOIN dune_user_generated.fluidsonic_ohm_token_prices_daily as prices
        ON prices.address = new_bond.token_address
        AND prices.day = date_trunc('day', now())
    JOIN dune_user_generated.fluidsonic_ohm_token_prices_daily as ohm_price
        ON ohm_price.address = '\x64aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d5'
        AND ohm_price.day = date_trunc('day', now())
)
SELECT *
        ,   CONCAT(
                 '_quoteToken: ', token_address
                ,' _market: [',param_market_total_capacity,',',param_market_price,',',param_market_debt_buffer,']'
                ,' _booleans: [',param_bool_cap_in_quote_token,',',param_bool_fixed_term,']' 
                ,' _terms: [',param_terms_vest,',',param_terms_conclusion,']'
                ,' _intervals: [',param_interval_deposit,',',param_interval_tune,']' 
            ) AS params
FROM (
    SELECT  name
        ,   replace(token_address::text, '\', '0') as token_address
        ,   create_date
        ,   conclusion_date
        ,   days_market_is_live
        ,   daily_capacity
        ,   ROUND(((daily_capacity * days_market_is_live * deposit_interval / seconds_market_is_live) * ohm_price)::numeric, 0) as max_payout_usd
        ,   ROUND((((daily_capacity * days_market_is_live)) *  deposit_interval / seconds_market_is_live)::numeric, 0) as max_payout_ohm
        ,   ROUND(daily_capacity * days_market_is_live) AS total_capacity 
        ,   CONCAT(ROUND(daily_capacity * days_market_is_live), REPEAT('0',9)) AS param_market_total_capacity
        ,   ROUND(ohm_token_price * 1e9 * (1-ip_discount)) as param_market_price
        ,   100000 as param_market_debt_buffer
        ,   'false' as param_bool_cap_in_quote_token
        ,   'true' as param_bool_fixed_term
        ,   vesting_length_seconds as param_terms_vest
        ,   conclusion_epoch as param_terms_conclusion
        ,   deposit_interval as param_interval_deposit
        ,   tune_interval as param_interval_tune
    FROM calc   
    WHERE capacity_in_quote_token = false
    ) AS A
UNION
SELECT *
        ,   CONCAT(
                 '_quoteToken: ', token_address
                ,' _market: [',param_market_total_capacity,',',param_market_price,',',param_market_debt_buffer,']'
                ,' _booleans: [',param_bool_cap_in_quote_token,',',param_bool_fixed_term,']' 
                ,' _terms: [',param_terms_vest,',',param_terms_conclusion,']'
                ,' _intervals: [',param_interval_deposit,',',param_interval_tune,']' 
            ) AS params
FROM (
    SELECT  name
        ,   replace(token_address::text, '\', '0') as token_address
        ,   create_date
        ,   conclusion_date
        ,   days_market_is_live
        ,   daily_capacity
        ,   ROUND((daily_capacity * days_market_is_live * token_price * deposit_interval / seconds_market_is_live)::numeric,0) as max_payout_usd
        ,   ROUND((daily_capacity * days_market_is_live * deposit_interval / seconds_market_is_live / ohm_token_price)::numeric,0) as max_payout_ohm
        ,   ROUND(daily_capacity * days_market_is_live) AS total_capacity 
        ,   CONCAT(ROUND(daily_capacity * days_market_is_live), REPEAT('0',decimals)) AS param_market_total_capacity
        ,   ROUND(ohm_token_price * (1-ip_discount)) * 1e9 as param_market_price
        ,   100000 as param_market_debt_buffer
        ,   'true' as param_bool_cap_in_quote_token
        ,   'true' as param_bool_fixed_term
        ,   vesting_length_seconds as param_terms_vest
        ,   conclusion_epoch as param_terms_conclusion
        ,   deposit_interval as param_interval_deposit
        ,   tune_interval as param_interval_tune
    FROM calc   
    WHERE capacity_in_quote_token = true
) AS B