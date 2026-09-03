SELECT trade_date, last_trading_price, highest_price, lowest_price, opening_price, trading_volume, sma_50, sma_200, volatility_30d, 'CL=F' AS ticker
FROM {{ ref('stg_wti') }}
UNION ALL
SELECT trade_date, last_trading_price, highest_price, lowest_price, opening_price, trading_volume, sma_50, sma_200, volatility_30d, 'NG=F' AS ticker
FROM {{ ref('stg_ng') }}
UNION ALL
SELECT trade_date, last_trading_price, highest_price, lowest_price, opening_price, trading_volume, sma_50, sma_200, volatility_30d, 'XOM' AS ticker
FROM {{ ref('stg_xom') }}