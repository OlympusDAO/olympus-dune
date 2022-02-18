

;WITH ohm_supply AS (
    SELECT date_trunc('day', date) as day
        ,   total_supply
        ,   circ_supply
    FROM dune_user_generated.ohm_circ_supply
)


, swap AS ( 
    SELECT
        date_trunc('day', sw."evt_block_time") AS day,
        ("amount0In" + "amount0Out")/1e9 AS a0_amt, 
        ("amount1In" + "amount1Out")/1e18 AS a1_amt
    FROM sushi."Pair_evt_Swap" sw
    WHERE contract_address = '\x34d7d7Aaf50AD4944B70B320aCB24C95fa2def7c'
        AND sw.evt_block_time >= '2021-01-01 00:00'
        AND sw.evt_block_time < '2021-12-18 3:14:11'
    UNION
    SELECT
        date_trunc('day', sw."evt_block_time") AS day,
        ("amount0In" + "amount0Out")/1e9 AS a0_amt, 
        ("amount1In" + "amount1Out")/1e18 AS a1_amt
    FROM sushi."Pair_evt_Swap" sw
    WHERE contract_address = '\x055475920a8c93cffb64d039a8205f7acc7722d3' -- OHM-DAI2
        AND sw.evt_block_time >= '2021-01-01 00:00'
        AND sw.evt_block_time >= '2021-12-18 3:14:11'
)
	
, a1_prcs AS (
    SELECT avg(price) a1_prc, date_trunc('day', minute) AS day
    FROM prices.usd
    WHERE minute >= '2021-01-01 00:00'
        and contract_address ='\x6b175474e89094c44da98b954eedeac495271d0f'
    group by 2
)

, price as ( 
    SELECT
        a1_prcs."day" AS "date", 
        (AVG((a1_amt/a0_amt)*a1_prc)) AS price
    FROM swap 
    JOIN a1_prcs ON swap."day" = a1_prcs."day"
    GROUP BY 1
)
, mcap AS (
    select price."date",(circ_supply * price) as market_cap, price, circ_supply, total_supply
    from ohm_supply
    join price 
        on ohm_supply.day = price."date"
)

, v2_capacity AS (
    SELECT  day
        ,   SUM(daily_bcv_capacity) as daily_bcv_capacity
    FROM dune_user_generated.ohm_bond_daily_metrics
    GROUP BY day
)
, v1_capacity AS (
    SELECT day, total_daily_capacity
    FROM dune_user_generated.ohm_v1_bond_capacity as v1
    WHERE v1.day < '2021-12-11 00:00' 
        or v1.day > '2021-12-18 00:00' --migration
)

, capacity AS (
    SELECT  day
        ,   price
        ,   market_cap
        ,   capacity
        ,   CASE 
                WHEN capacity = 0
                    THEN 0
                ELSE market_cap / capacity
            END AS mcap_capacity_ratio
    FROM (
        SELECT  mcap."date" as day
            ,   mcap.market_cap
            ,   mcap.price
            --,   v1.total_daily_capacity as v1_capacity
            --,   v2.capacity * mcap.price as v2_capacity
            ,   COALESCE(v1.total_daily_capacity, 0) + (COALESCE(v2.daily_bcv_capacity, 0) * mcap.price) AS capacity
        FROM mcap
        LEFT JOIN v1_capacity as v1
            ON v1.day = mcap."date"
        LEFT JOIN v2_capacity as v2
            ON v2.day = mcap."date"
        WHERE mcap."date" > '2021-06-28 00:00'
    )  daily_cap
)

SELECT  *
    ,   AVG(mcap_capacity_ratio) OVER(ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS "7 Moving Average"
FROM capacity