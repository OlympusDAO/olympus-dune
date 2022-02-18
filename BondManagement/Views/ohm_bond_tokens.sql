
CREATE OR REPLACE VIEW dune_user_generated.ohm_bond_tokens AS

SELECT '\x853d955acef822db058eb8505911ed77f175b99e'::bytea AS address,'FRAX' AS name, 18 AS decimals, 'Reserve' AS bond_type
UNION SELECT '\x6b175474e89094c44da98b954eedeac495271d0f' AS address,'DAI' AS name, 18 AS decimals, 'Reserve' AS bond_type
UNION SELECT '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599' AS address,'WBTC' AS name, 8 AS decimals, 'Strategic' AS bond_type
UNION SELECT '\x69b81152c5a8d35a67b32a4d3772795d96cae4da' AS address,'OHM-WETH' AS name, 18 AS decimals, 'LP' AS bond_type
UNION SELECT '\x055475920a8c93cffb64d039a8205f7acc7722d3' AS address,'OHM-DAI' AS name, 18 AS decimals, 'LP' AS bond_type
UNION SELECT '\xa693b19d2931d498c5b318df961919bb4aee87a5' AS address,'UST' AS name, 6 AS decimals, 'Reserve' AS bond_type
UNION SELECT '\x5f98805A4E8be255a32880FDeC7F6728C6568bA0' AS address,'LUSD' AS name, 18 AS decimals, 'Reserve' AS bond_type
UNION SELECT '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2' AS address,'WETH' AS name, 18 AS decimals, 'Strategic' AS bond_type
UNION SELECT '\xb612c37688861f1f90761dc7f382c2af3a50cc39' AS address,'OHM-FRAX' AS name, 18 AS decimals, 'LP' AS bond_type