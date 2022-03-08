CREATE OR REPLACE VIEW dune_user_generated.ohm_inverse_bond_deposits AS

SELECT  evt.id AS market_id
    ,   evt.evt_block_time AS block_time
    ,   payout.value/(10 ^ COALESCE(tokens.decimals, 18)) AS payout_token_qty
    ,   evt.amount/1e9 AS bonded_ohm_qty
    ,   evt.price AS bond_price
    ,   evt.evt_tx_hash
FROM olympus_v2."OlympusProV2_evt_Bond" AS evt
JOIN dune_user_generated.ohm_inverse_bond_markets as markets
    ON markets.id = evt.id
JOIN dune_user_generated.ohm_bond_tokens as tokens
    ON tokens.address = markets.base_token
JOIN erc20."ERC20_evt_Transfer" as payout
    ON payout.evt_tx_hash = evt.evt_tx_hash
    AND payout."from" = '\xBA42BE149e5260EbA4B82418A6306f55D532eA47' --InverseBondCreator
    AND payout."to" = '\x9025046c6fb25Fb39e720d97a8FD881ED69a1Ef6'--Bond Depo

