
SELECT  metrics.day
    ,   markets.quoted_token_name
    ,   SUM(metrics.bonded_ohm_qty) AS bonded_ohm_qty
FROM dune_user_generated.ohm_bond_daily_metrics as metrics
JOIN dune_user_generated.ohm_bond_markets as markets
    ON markets.id = metrics.market_id
JOIN dune_user_generated.ohm_bond_tokens as tokens
    ON tokens.address = markets.quoted_token
GROUP BY day, quoted_token_name
