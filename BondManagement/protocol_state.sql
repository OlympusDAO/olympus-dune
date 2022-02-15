DROP TABLE IF EXISTS ohm_supply;
DROP TABLE IF EXISTS olympus_general_stats;
DROP TABLE IF EXISTS reformated_table;

CREATE TEMP TABLE ohm_supply AS
SELECT date_trunc('day', date) as day
    ,   total_supply
    ,   circ_supply
FROM dune_user_generated.ohm_circ_supply;

CREATE TEMP TABLE olympus_general_stats AS
WITH swap AS (
    SELECT
        date_trunc('day', sw."evt_block_time") AS day,
        ("amount0In" + "amount0Out")/1e9 AS a0_amt,
        ("amount1In" + "amount1Out")/1e18 AS a1_amt
    FROM sushi."Pair_evt_Swap" sw
        WHERE CASE WHEN evt_block_time >= '2021-12-18' THEN contract_address = '\x055475920a8c93cffb64d039a8205f7acc7722d3' ELSE contract_address = '\x34d7d7Aaf50AD4944B70B320aCB24C95fa2def7c' END -- liq pair address I am searching the price for --> previously OHMv1-DAI now OHMv2-DAI
    ),

a1_prcs AS (
    SELECT avg(price) a1_prc, date_trunc('day', minute) AS day
    FROM prices.usd
    WHERE minute >= '2021-03-22'
        and contract_address ='\x6b175474e89094c44da98b954eedeac495271d0f'
    group by 2
),

temp_price as (
SELECT
    a1_prcs."day" AS "date"
    , (AVG((a1_amt/a0_amt)*a1_prc)) AS price
FROM swap
JOIN a1_prcs ON swap."day" = a1_prcs."day"
GROUP BY 1
),

price as (
SELECT "date"
    , price
    , case when row_number() over () > 7 then AVG(price) over(order by date rows between 7 preceding and current row) else null end as price_7d_ma
    , case when row_number() over () > 15 then AVG(price) over(order by date rows between 30 preceding and current row) else null end as price_30d_ma
FROM temp_price
order by date desc
),

time as
(
SELECT
generate_series('2021-12-14', NOW(), '1 hour'::interval) as Date
),


-- Taken from sh4dow to compute the staked % of the supply
time_staked as
(
SELECT NOW() as Date
),

staking_address_v1 AS
(
    SELECT
    date_trunc('day', evt_block_time) as Date,
    COALESCE(-sum(e.value/1e9), 0) as staked_amount
    FROM erc20."ERC20_evt_Transfer" e
    WHERE "contract_address" in ('\x383518188c0c6d7730d91b2c03a03c837814a899', '\x64aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d5') -- OHM contract address
    and e."from" in ('\xFd31c7d00Ca47653c6Ce64Af53c1571f9C36566a', '\x0822F3C03dcc24d200AFF33493Dc08d0e1f274A2')
    GROUP BY 1
UNION ALL
    SELECT
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(e.value/1e9), 0) as staked_amount
    FROM erc20."ERC20_evt_Transfer" e
    WHERE "contract_address" in ('\x383518188c0c6d7730d91b2c03a03c837814a899', '\x64aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d5') -- OHM contract address
    and e."to" in ('\xFd31c7d00Ca47653c6Ce64Af53c1571f9C36566a', '\x0822F3C03dcc24d200AFF33493Dc08d0e1f274A2')
    GROUP BY 1
),

final_staked_v1 as
(
SELECT
Date,
sum(sum(staked_amount)) over (order by Date) as OHM_staked
FROM
(
SELECT Date, staking_address_v1.staked_amount as staked_amount FROM staking_address_v1 UNION ALL
SELECT Date, 0 as staked_amount FROM time_staked
) t
GROUP BY 1
ORDER BY Date DESC
),

staking_address_v2 AS
(
    SELECT
    date_trunc('day', evt_block_time) as Date,
    COALESCE(-sum(e.value/1e9), 0) as staked_amount
    FROM erc20."ERC20_evt_Transfer" e
    WHERE "contract_address" in ('\x383518188c0c6d7730d91b2c03a03c837814a899', '\x64aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d5') -- OHM contract address
    and e."from" = '\xB63cac384247597756545b500253ff8E607a8020'
    GROUP BY 1
UNION ALL
    SELECT
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(e.value/1e9), 0) as staked_amount
    FROM erc20."ERC20_evt_Transfer" e
    WHERE "contract_address" in ('\x383518188c0c6d7730d91b2c03a03c837814a899', '\x64aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d5') -- OHM contract address
    and e."to" = '\xB63cac384247597756545b500253ff8E607a8020'
    GROUP BY 1
),

final_staked_v2 as
(
SELECT
Date,
sum(sum(staked_amount)) over (order by Date) as OHM_staked
FROM
(
SELECT Date, staking_address_v2.staked_amount as staked_amount FROM staking_address_v2 UNION ALL
SELECT Date, 0 as staked_amount FROM time_staked
) t
GROUP BY 1
ORDER BY Date DESC
),

staked_perc as (
select dune_user_generated.ohm_circ_supply.date, ohm_staked / circ_supply as ohm_staked_perc
from dune_user_generated.ohm_circ_supply
left join final_staked_v1 on dune_user_generated.ohm_circ_supply."date" = final_staked_v1."date"
where dune_user_generated.ohm_circ_supply.date <= '2021-12-14'
UNION ALL
select dune_user_generated.ohm_circ_supply.date, ohm_staked / circ_supply as ohm_staked_perc
from dune_user_generated.ohm_circ_supply
left join final_staked_v2 on dune_user_generated.ohm_circ_supply."date" = final_staked_v2."date"
where dune_user_generated.ohm_circ_supply.date > '2021-12-14'
ORDER BY date desc
LIMIT 1
),

OHM AS
(
    SELECT
    evt_block_time as Date,
    COALESCE(e.value/1e9, 0) as supply
    FROM erc20."ERC20_evt_Transfer" e
    WHERE "contract_address" = '\x64aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d5' -- OHM contract address
    and e."from" = '\x0000000000000000000000000000000000000000'
UNION ALL
    SELECT
    evt_block_time as Date,
    COALESCE(-e.value/1e9, 0) as supply
    FROM erc20."ERC20_evt_Transfer" e
    WHERE "contract_address" = '\x64aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d5' -- OHM contract address
    and (e."to" = '\x0000000000000000000000000000000000000000')
),

temp_supply as
(
SELECT
Date,
sum(sum(supply)) over (order by Date) as total_supply
FROM ohm
group by 1
),

staking_tx as (
select evt_block_time, (value/1e9) as ohm_transferred, evt_tx_hash
from erc20."ERC20_evt_Transfer" e
where e."from" = '\x0000000000000000000000000000000000000000'
and e."to"  = '\xb63cac384247597756545b500253ff8e607a8020' and value != 0
and evt_block_time > '2021-12-14 19:19'
),

supply_and_fee as (
select day, circ_supply, total_supply, daoFeePerc
    , (SELECT SUM(daily_bcv_capacity) FROM dune_user_generated.olympus_bonds_status) as ohm_sold_bonds_daily
    , (select ohm_transferred/(total_supply-ohm_transferred) as reward_rate
            from staking_tx
            left join temp_supply on date = evt_block_time
            order by evt_block_time desc
        LIMIT 1)
    , (select ohm_staked_perc FROM staked_perc LIMIT 1)
from ohm_supply, (
                    SELECT "OlympusBondDepositoryV2_call_setRewards"."_toDAO" / 100  as daoFeePerc FROM olympus_v2."OlympusBondDepositoryV2_call_setRewards"
                    WHERE call_success = 'true'
                    ORDER BY call_block_time desc
                    LIMIT 1
                ) as bondRewards
LIMIT 1
)

select price."date", (circ_supply * price.price) as market_cap
    , price.price, price.price_7d_ma, price.price_30d_ma
    , circ_supply, total_supply
    , mv.treasury_mv
    , mv.treasury_backing
    , mv.treasury_rfv
    , daoFeePerc
    , ohm_sold_bonds_daily
    , mv.treasury_mv / circ_supply as mv_per_ohm
    , mv.treasury_backing / circ_supply as backing_per_ohm
    , mv.treasury_rfv / circ_supply as rfv_per_ohm
    , reward_rate
    , (3 * reward_rate + 3 * POWER(reward_rate, 2) + POWER(reward_rate, 3)) as daily_reward_rate
    , ohm_staked_perc
from supply_and_fee
join price
on day = price."date"
join (
        select day
            , (treasury_mv - (liquidity_mv/2)) as treasury_backing
            , treasury_mv
            , treasury_rfv
        from dune_user_generated.olympustreasury t
        ORDER BY day desc
        LIMIT 1
     ) as mv
ON supply_and_fee.day = mv.day
JOIN dune_user_generated.treasury_rfv as rfv
ON supply_and_fee.day = rfv.date
ORDER BY date desc;


CREATE TEMP TABLE reformated_table (
variable VARCHAR(100) PRIMARY KEY,
value VARCHAR(200),
index INT
);

INSERT INTO reformated_table(variable, value, index)
(SELECT 'Total supply' as variable, TO_CHAR(total_supply::NUMERIC, 'FM9,999,999,999 OHM'), 1 as index FROM olympus_general_stats)
UNION
(SELECT 'Price' as variable, TO_CHAR(price::NUMERIC, 'FM9,999,999.00 $'), 2 as index FROM olympus_general_stats)
UNION
(SELECT 'Reward rate' as variable, TO_CHAR(reward_rate::NUMERIC * 100, 'FM99.0000 %'), 3 as index FROM olympus_general_stats)
UNION
(SELECT 'Daily reward rate' as variable, TO_CHAR(daily_reward_rate::NUMERIC * 100, 'FM99.0000 %'), 3 as index FROM olympus_general_stats)
UNION
(SELECT 'DAO fee' as variable, TO_CHAR(daoFeePerc::NUMERIC, 'FM999.00 %'), 4 as index FROM olympus_general_stats)
UNION
(SELECT 'Treasury MV' as variable, TO_CHAR(treasury_mv::NUMERIC, 'FM9,999,999,999 $'), 5 as index FROM olympus_general_stats)
UNION
(SELECT 'Treasury RFV' as variable, TO_CHAR(treasury_rfv::NUMERIC, 'FM9,999,999,999 $'), 6 as index FROM olympus_general_stats)
UNION
(SELECT 'MV/OHM' as variable, TO_CHAR(mv_per_ohm::NUMERIC, 'FM9,999,999.00 $') || '/OHM', 7 as index FROM olympus_general_stats)
UNION
(SELECT 'BACKING/OHM' as variable, TO_CHAR(backing_per_ohm::NUMERIC, 'FM9,999,999.00 $') || '/OHM', 8 as index FROM olympus_general_stats)
UNION
(SELECT 'RFV/OHM' as variable, TO_CHAR(rfv_per_ohm::NUMERIC, 'FM9,999,999.00 $') || '/OHM', 9 as index FROM olympus_general_stats)
UNION
(SELECT '$ Capacity to maintain BACKING' as variable, TO_CHAR(backing_per_ohm::NUMERIC * (ohm_sold_bonds_daily + total_supply * daily_reward_rate), 'FM9,999,999 $') || '/day', 10 as index FROM olympus_general_stats)
UNION
(SELECT 'RFV Capacity to maintain RFV' as variable, TO_CHAR(rfv_per_ohm::NUMERIC * (ohm_sold_bonds_daily + total_supply * daily_reward_rate), 'FM9,999,999 $') || '/day', 11 as index FROM olympus_general_stats)
UNION
(SELECT 'Current $ bond capacity' as variable, TO_CHAR(price::NUMERIC * ohm_sold_bonds_daily, 'FM9,999,999,999 $') || '/day', 12 as index FROM olympus_general_stats)
UNION
(SELECT 'Current $ bond revenues' as variable, TO_CHAR((price::NUMERIC - mv_per_ohm) * ohm_sold_bonds_daily, 'FM9,999,999,999 $') || '/day', 13 as index FROM olympus_general_stats)
UNION
(SELECT 'Current OHM bond capacity' as variable, TO_CHAR(ohm_sold_bonds_daily::NUMERIC, 'FM9,999,999 OHM') || '/day', 14 as index FROM olympus_general_stats)
UNION
(SELECT 'OHM distributed to stakers' as variable, TO_CHAR(total_supply::NUMERIC * daily_reward_rate, 'FM9,999,999 OHM') || '/day', 15 as index FROM olympus_general_stats)
UNION
(SELECT 'OHM to sell for 0 dilution' as variable, TO_CHAR(total_supply::NUMERIC * daily_reward_rate * ( total_supply / (ohm_staked_perc * circ_supply) - 1), 'FM9,999,999 OHM') || '/day', 16 as index FROM olympus_general_stats)
UNION
(SELECT 'Premium' as variable, TO_CHAR((price::NUMERIC - mv_per_ohm) / mv_per_ohm * 100, 'FM999.00%'), 17 as index FROM olympus_general_stats)
UNION
(SELECT 'Price change vs 7dMA' as variable, TO_CHAR((price::NUMERIC - price_7d_ma) / price_7d_ma * 100, 'FM999.00%'), 18 as index FROM olympus_general_stats)
UNION
(SELECT 'Price change vs 30dMA' as variable, TO_CHAR((price::NUMERIC - price_30d_ma) / price_30d_ma * 100, 'FM999.00%'), 19 as index FROM olympus_general_stats)
;

SELECT variable, value FROM reformated_table
ORDER BY index


