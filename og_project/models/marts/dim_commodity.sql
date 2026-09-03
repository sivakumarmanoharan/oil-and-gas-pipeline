SELECT DISTINCT 
    "Asset_Name"::TEXT AS asset_name,
    "Ticker"::TEXT AS ticker,
    "Asset_Type"::TEXT AS asset_type
FROM {{ source('raw', 'og_sac_data') }}