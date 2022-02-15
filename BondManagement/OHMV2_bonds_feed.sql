CREATE OR REPLACE VIEW dune_user_generated.OHMV2_bonds_feed AS

with swap AS ( 
        SELECT
            sw."evt_block_time" as minute,
            ("amount0In" + "amount0Out")/1e9 AS a0_amt, 
            ("amount1In" + "amount1Out")/1e18 AS a1_amt
            
        FROM sushi."Pair_evt_Swap" sw
        WHERE CASE WHEN evt_block_time >= '2021-12-18' THEN contract_address = '\x055475920a8c93cffb64d039a8205f7acc7722d3' ELSE contract_address = '\x34d7d7Aaf50AD4944B70B320aCB24C95fa2def7c' END -- liq pair address I am searching the price for --> previously OHMv1-DAI now OHMv2-DAI
        ), 

   price as (select swap."minute", 
    (a1_amt/a0_amt) as price
from swap
order by 1 desc),

rebases AS NOT MATERIALIZED (
SELECT evt_block_time, index/1e9 as index FROM olympus."sOlympus_evt_LogRebase"
WHERE contract_address = '\x04906695d6d12cf5459975d7c3c03356e4ccd460'
ORDER BY evt_block_time DESC
),

bond_price as (
SELECT b.evt_block_time AS minute, t.name, vesting_days, amount/10^t.decimals as amount, gohm.value / 1e18 * rebases.index as ohm_payout,
CASE WHEN bond_type = 'LP' THEN (b.price/1e9) * slp_price 
WHEN bond_type = 'Reserve' THEN (b.price/1e9)
ELSE (b.price/1e9) * pr.price
END AS bond_price, e.contract_address, b.evt_tx_hash
FROM olympus_v2."OlympusBondDepositoryV2_evt_Bond" b
LEFT JOIN erc20."ERC20_evt_Transfer" e ON e.evt_tx_hash = b.evt_tx_hash AND e."to" = '\x9a315bdf513367c0377fb36545857d12e85813ef'
LEFT JOIN dune_user_generated.ohm_bond_tokens t ON e.contract_address = t.address
LEFT JOIN dune_user_generated.olympus_pools p ON pool = e.contract_address AND p.Date = date_trunc('hour', b.evt_block_time)
LEFT JOIN prices.usd pr ON pr.minute = date_trunc('minute', b.evt_block_time) AND e.contract_address = pr.contract_address
LEFT JOIN erc20."ERC20_evt_Transfer" as gohm on gohm.evt_tx_hash = b.evt_tx_hash and gohm."to" = '\x9025046c6fb25fb39e720d97a8fd881ed69a1ef6'  and gohm.contract_address = '\x0ab87046fbb341d058f17cbc4c1133f25a20a52f'
LEFT JOIN rebases on rebases.evt_block_time = (SELECT evt_block_time from rebases where rebases.evt_block_time < b.evt_block_time LIMIT 1)
LEFT JOIN 
    (SELECT *, DATE_PART('day', TO_TIMESTAMP("_terms"[1]) - '1970-01-01 00:00:00'::timestamp) AS Vesting_Days
    FROM olympus_v2."OlympusBondDepositoryV2_call_create") as v ON v."output_id_" = b.id
)

SELECT minute, 
    (select price from price where price.minute <= bond_price.minute order by bond_price.minute limit 1) as price,  
    bond_price, 
    ((select price from price where price.minute <= bond_price.minute order by bond_price.minute limit 1)-bond_price)/bond_price as roi, 
    name, vesting_days, evt_tx_hash, ohm_payout
FROM bond_price 
order by minute desc
   
