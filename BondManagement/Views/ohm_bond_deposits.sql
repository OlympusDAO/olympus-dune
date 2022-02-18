CREATE OR REPLACE VIEW dune_user_generated.ohm_bond_deposits AS

SELECT  evt.id AS market_id
    ,   evt.evt_block_time AS block_time
    ,   ohm_staked.value/1e9 AS bonded_ohm_qty
    ,   evt.amount/(10 ^ COALESCE(tokens.decimals, 18)) AS bonded_token_qty
    ,   evt.price AS bond_price
FROM olympus_v2."OlympusBondDepositoryV2_evt_Bond" AS evt
JOIN dune_user_generated.ohm_bond_markets as markets
    ON markets.id = evt.id
JOIN dune_user_generated.ohm_bond_tokens as tokens
    ON tokens.address = markets.quoted_token
JOIN erc20."ERC20_evt_Transfer" as ohm_staked
    ON ohm_staked.evt_tx_hash = evt.evt_tx_hash
    AND ohm_staked."from" = '\x9025046c6fb25Fb39e720d97a8FD881ED69a1Ef6'--Bond Depo
    AND ohm_staked."to" = '\xB63cac384247597756545b500253ff8E607a8020' --Staking

