WITH ga4_data AS (
    SELECT *, 
    (select value.int_value from unnest(event_params) where key = 'ga_session_number') AS domain_sessionidx,
    (select value.string_value from unnest(event_params) where key = 'page_location') AS page_url,
    (select value.string_value from unnest(event_params) where key = 'page_title') AS page_title,
    (select value.string_value from unnest(event_params) where key = 'page_referrer') AS page_referrer,
    (select value.string_value from unnest(event_params) where key = 'medium') as mkt_medium,
    (select value.string_value from unnest(event_params) where key = 'source') AS mkt_source,
    (select value.string_value from unnest(event_params) where key = 'term') AS mkt_term,
    (select value.string_value from unnest(event_params) where key = 'content') AS mkt_content,
    (select value.string_value from unnest(event_params) where key = 'campaign') AS mkt_campaign,
    (select affiliation from unnest(items)) AS tr_affiliation,
    (select item_id from unnest(items)) AS ti_sku,
    (select item_name from unnest(items)) AS ti_name,
    (select item_category from unnest(items)) AS ti_category,
    (select price from unnest(items)) AS ti_price,
    (select quantity from unnest(items)) AS ti_quantity,
    (select value.int_value from unnest(event_params) where key = 'ga_session_id') AS domain_sessionid,
    geo.country AS geo_country_name,
    geo.region AS geo_region_name,
    FROM `{project_id}.{dataset}.{table}`
)
SELECT  
    domain_sessionidx,
    page_url,
    page_title,
    page_referrer,
    mkt_medium,
    mkt_source,
    mkt_term,
    mkt_content,
    mkt_campaign,
    tr_affiliation,
    ti_sku,
    ti_name,
    ti_category,
    ti_price,
    ti_quantity,
    domain_sessionid,
    CASE WHEN stream_id = '4271243942'
        THEN ""
        ELSE NULL
    END AS app_id,
    platform,
    TIMESTAMP_MICROS(event_timestamp) AS etl_tstamp,
    TIMESTAMP_MICROS(event_timestamp) AS collector_tstamp,
    TIMESTAMP_MICROS(event_timestamp) AS dvce_created_tstamp,
    CASE WHEN event_name = 'page_view' THEN 'pageview'
    END AS event,
    md5(TO_JSON_STRING(ga4_data)) AS event_id,
    "google-analytics-4" AS name_tracker,
    "1.0.0" AS v_tracker,
    "collector-version" AS v_collector,
    "enrich-version" AS v_etl,
    user_id,
    md5(user_pseudo_id) AS domain_userid,
    c.alpha2_code AS geo_country,
    SPLIT(r.code, '-')[SAFE_OFFSET(1)] AS geo_region,
    geo.city AS geo_city,
    geo_region_name,
    regexp_extract(page_url, r'(.*):') AS page_urlscheme,
    regexp_extract(page_url, r'.*:\/\/(.*?)\/') AS page_urlhost,
    regexp_extract(page_url, r'^(?:(?:[^:\/?#]+):)?(?:\/\/(?:[^\/?#]*))?([^?#]*)') AS page_urlpath,
    regexp_extract(page_url, r'(?:.*)\?(.*)') AS page_urlquery,
    regexp_extract(page_url, r'(?:.*)\#(.*)') AS page_urlfragment,
    regexp_extract(page_referrer, r'(.*):') AS refr_urlscheme,
    regexp_extract(page_referrer, r'.*:\/\/(.*?)\/') AS refr_urlhost,
    regexp_extract(page_referrer, r'^(?:(?:[^:\/?#]+):)?(?:\/\/(?:[^\/?#]*))?([^?#]*)') AS refr_urlpath,
    regexp_extract(page_referrer, r'(?:.*)\?(.*)') AS refr_urlquery,
    regexp_extract(page_referrer, r'(?:.*)\#(.*)') AS refr_urlfragment,
    traffic_source.medium AS refr_medium,
    traffic_source.source AS refr_source,
    regexp_extract(page_referrer, r'utm_term=([^&]*)') AS refr_term,
    ecommerce.transaction_id AS tr_orderid,
    ecommerce.purchase_revenue AS tr_total,
    ecommerce.tax_value AS tr_tax,
    ecommerce.shipping_value AS tr_shipping,
    device.browser AS br_name,
    NULL AS br_family,
    device.browser_version AS br_version,
    IF(user_pseudo_id is not NULL, true, false) AS br_cookies,
    NULL AS br_viewwidth,
    NULL AS br_viewheight,
    device.operating_system AS os_name,
    NULL AS os_family,
    NULL AS os_manufacturer,
    TIMESTAMP_MICROS(event_timestamp) AS os_timezone,
    COALESCE(
        regexp_extract(page_url, r'gclid=([^&]*)'),
        regexp_extract(page_url, r'msclkid=([^&]*)'),
        regexp_extract(page_url, r'dclid=([^&]*)')
    ) AS mkt_clickid,
    device.category AS dvce_type,
    IF(device.category = 'mobile', 1, 0) AS dvce_ismobile,
    TIMESTAMP_MICROS(event_timestamp) AS dvce_sent_tstamp,
    TIMESTAMP_MICROS(event_timestamp) AS derived_tstamp,
    "com.googleanalytics" AS event_vendor,
    event_name,
    "jsonschema" AS event_format,
    "1-0-0" AS event_version,
    MD5(TO_JSON_STRING(ga4_data)) AS event_fingerprint,
    current_timestamp() AS load_tstamp,
    CASE WHEN regexp_extract(page_url, r'(.*):') = 'https'
        THEN '443'
        WHEN regexp_extract(page_url, r'(.*):') = 'http'
        THEN '80'
        ELSE NULL
    END AS refr_urlport,
    CASE WHEN regexp_extract(page_referrer, r'(.*):') = 'https'
        THEN '443'
        WHEN regexp_extract(page_referrer, r'(.*):') = 'http'
        THEN '80'
        ELSE NULL
    END AS page_urlport,
    CASE WHEN page_url LIKE '%gclid=%'
        THEN 'Google'
        WHEN page_url LIKE '%msclkid=%'
        THEN 'Microsoft'
        WHEN page_url LIKE '%dclid=%'
        THEN 'DoubleClick'
    END AS mkt_network

FROM ga4_data g
    LEFT JOIN ga4_test.countries_0 c ON g.geo_country_name=c.name
    LEFT JOIN ga4_test.regions r ON g.geo_region_name=r.name AND SPLIT(r.code, '-')[SAFE_OFFSET(0)]=c.alpha2_code
