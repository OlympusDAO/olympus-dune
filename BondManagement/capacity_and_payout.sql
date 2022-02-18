
SELECT  day
    ,   id
    ,   quoted_token_name
    ,   CONCAT(id,': ',quoted_token_name) AS market_token
    ,   bonded_ohm_qty
    ,   capacity
    ,   daily_bcv_capacity
    ,   running_total_capacity
    ,   CASE WHEN capacity_in_quote
            THEN ((initial_target_debt - running_capacity_in_token) / initial_target_debt)
            ELSE ((initial_target_debt - running_total_capacity) / initial_target_debt)
        END * 100 AS accumulated_debt_target_percent
    ,   max_payout
    ,   bonded_capacity_ratio
FROM (
    SELECT  metrics.day
        ,   markets.id
        ,   markets.quoted_token_name
        ,   metrics.bonded_ohm_qty
        ,   metrics.capacity
        ,   metrics.daily_bcv_capacity
        ,   metrics.running_total_capacity
        ,   markets.capacity_in_quote
        ,   (
                CASE WHEN markets.capacity_in_quote
                    THEN markets.capacity / (10 ^ tokens.decimals)
                    ELSE markets.target_debt / 1e9
                END
            ) AS initial_target_debt
        ,   metrics.max_payout
        ,   CASE WHEN metrics.capacity = 0
                THEN 0
                ELSE metrics.bonded_ohm_qty / metrics.capacity 
            END as bonded_capacity_ratio
        ,   metrics.running_capacity_in_token
    FROM dune_user_generated.ohm_bond_daily_metrics as metrics
    JOIN dune_user_generated.ohm_bond_markets as markets
        ON markets.id = metrics.market_id
    JOIN dune_user_generated.ohm_bond_tokens as tokens
        ON tokens.address = markets.quoted_token
) AS daily
