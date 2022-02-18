CREATE OR REPLACE VIEW dune_user_generated.ohm_bond_bcvs AS

    SELECT  id as market_id
        ,   market_create_time as block_time
        ,   initial_bcv AS bcv
    FROM dune_user_generated.ohm_bond_markets
    UNION
    SELECT  BYTEA2NUMERIC(logs."topic2") AS market_id
        ,   trans.block_time
        ,   BYTEA2NUMERIC(SUBSTRING(logs."data" from  33 for 64)) as bcv
    FROM ethereum."transactions" as trans
    JOIN ethereum."logs" AS logs
        ON logs.tx_hash = trans.hash
        AND logs.contract_address = '\x9025046c6fb25Fb39e720d97a8FD881ED69a1Ef6' --BondDepo
        AND topic1 = '\x3070b0e3e52b8713c7489d32604ea4b0970024f74c6e05319269a19bc1e3a9d9' --Tune
    WHERE trans."to" = '\x9025046c6fb25Fb39e720d97a8FD881ED69a1Ef6' --BondDepo
