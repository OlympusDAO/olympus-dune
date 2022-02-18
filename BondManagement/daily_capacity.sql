SELECT  day
    ,   SUM(daily_bcv_capacity) AS daily_capacity
    ,   SUM(bonded_ohm_qty) AS bonded_ohm_qty
FROM (
    SELECT  metrics.day
        ,   metrics.daily_bcv_capacity
        ,   metrics.bonded_ohm_qty
    FROM dune_user_generated.ohm_bond_daily_metrics as metrics
) as daily_bond_metrics
GROUP BY 1