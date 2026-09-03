SELECT
    "Date"::date AS "trade_date",
    "Close"::numeric AS "last_trading_price",
    "High"::numeric AS "highest_price",
    "Low"::numeric AS "lowest_price",
    "Open"::numeric AS "opening_price",
    "Volume"::numeric AS "trading_volume",
    "SMA_50"::numeric AS "sma_50",
    "SMA_200"::numeric AS "sma_200",
    "Volatility_30d"::numeric AS "volatility_30d"
FROM {{ source('raw', 'og_sac_data') }}
WHERE "Ticker" = 'XOM'