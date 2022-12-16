fields_1 = [
    {
        "source": "",
        "destination": "domain_sessionidx",
        "transform": "JSON_QUERY(sensible(event_params), '$.ga_session_number')",
    },
    {
        "source": "",
        "destination": "page_url",
        "transform": "JSON_QUERY(sensible(event_params), '$.page_location')",
    },
    {
        "source": "",
        "destination": "page_title",
        "transform": "JSON_QUERY(sensible(event_params), '$.page_title')",
    },
    {
        "source": "",
        "destination": "page_referrer",
        "transform": "JSON_QUERY(sensible(event_params), '$.page_referrer')",
    },
    {
        "source": "",
        "destination": "mkt_medium",
        "transform": "JSON_QUERY(sensible(event_params), '$.medium')",
    },
    {
        "source": "",
        "destination": "mkt_source",
        "transform": "JSON_QUERY(sensible(event_params), '$.source')",
    },
    {
        "source": "",
        "destination": "mkt_term",
        "transform": "JSON_QUERY(sensible(event_params), '$.term')",
    },
    {
        "source": "",
        "destination": "mkt_content",
        "transform": "JSON_QUERY(sensible(event_params), '$.content')",
    },
    {
        "source": "",
        "destination": "mkt_campaign",
        "transform": "JSON_QUERY(sensible(event_params), '$.campaign')",
    },
    {
        "source": "",
        "destination": "domain_sessionid",
        "transform": "JSON_QUERY(sensible(event_params), '$.ga_session_id')",
    },
    {
        "source": "geo.country",
        "destination": "geo_country_name",
        "transform": "",
    },
    {
        "source": "geo.region",
        "destination": "geo_region_name",
        "transform": "",
    }
]

fields_2 = [
    {
        "source": "items",
        "destination": "items",
        "transform": ""
    },
    {
        "source": "",
        "destination": "app_id",
        "transform": "CASE WHEN stream_id = '4271243942' THEN 'Poplin' ELSE NULL END"
    },
    {
        "source": "platform",
        "destination": "platform",
        "transform": ""
    },
    {
        "source": "",
        "destination": "etl_tstamp",
        "transform": "TIMESTAMP_MICROS(event_timestamp)"
    },
    {
        "source": "",
        "destination": "collector_tstamp",
        "transform": "TIMESTAMP_MICROS(event_timestamp)",
    },
    {
        "source": "",
        "destination": "dvce_created_tstamp",
        "transform": "TIMESTAMP_MICROS(event_timestamp)",
    },
    {
        "source": "",
        "destination": "event",
        "transform": "CASE WHEN event_name = 'page_view' THEN 'pageview' END",
    },
    {
        "source": "",
        "destination": "event_id",
        "transform": "md5(TO_JSON_STRING(ga4_data))",
    },
    {
        "source": "",
        "destination": "name_tracker",
        "transform": "'google-analytics-4'",
    },
    {
        "source": "",
        "destination": "v_tracker",
        "transform": "'1.0.0'",
    },
    {
        "source": "",
        "destination": "v_collector",
        "transform": "'collector-version'",
    },
    {
        "source": "",
        "destination": "v_etl",
        "transform": "'enrich-version'",
    },
    {
        "source": "user_id",
        "destination": "user_id",
        "transform": "",
    },
    {
        "source": "",
        "destination": "domain_userid",
        "transform": "md5(user_pseudo_id)",
    },
    {
        "source": "",
        "destination": "geo_country",
        "transform": "c.alpha2_code",
    },
    {
        "source": "",
        "destination": "geo_region",
        "transform": "SPLIT(r.code, '-')[SAFE_OFFSET(1)]",
    },
    {
        "source": "geo.city",
        "destination": "geo_city",
        "transform": "",
    },
    {
        "source": "",
        "destination": "page_urlscheme",
        "transform": "regexp_extract(page_url, r'(.*):')",
    },
    {
        "source": "",
        "destination": "page_urlhost",
        "transform": "regexp_extract(page_url, r'.*:\/\/(.*?)\/')",
    },
    {
        "source": "",
        "destination": "page_urlport",
        "transform": "CASE WHEN regexp_extract(page_referrer, r'(.*):') = 'https' THEN '443' WHEN regexp_extract(page_referrer, r'(.*):') = 'http' THEN '80' ELSE NULL END",
    },
    {
        "source": "",
        "destination": "page_urlpath",
        "transform": "regexp_extract(page_url, r'^(?:(?:[^:\/?#]+):)?(?:\/\/(?:[^\/?#]*))?([^?#]*)')",
    },
    {
        "source": "",
        "destination": "page_urlquery",
        "transform": "regexp_extract(page_url, r'(?:.*)\?(.*)')",
    },
    {
        "source": "",
        "destination": "page_urlfragment",
        "transform": "regexp_extract(page_url, r'(?:.*)\#(.*)')",
    },
    {
        "source": "",
        "destination": "refr_urlscheme",
        "transform": "regexp_extract(page_referrer, r'(.*):')",
    },
    {
        "source": "",
        "destination": "refr_urlhost",
        "transform": "regexp_extract(page_referrer, r'.*:\/\/(.*?)\/')",
    },
    {
        "source": "",
        "destination": "refr_urlport",
        "transform": "CASE WHEN regexp_extract(page_url, r'(.*):') = 'https' THEN '443' WHEN regexp_extract(page_url, r'(.*):') = 'http' THEN '80' ELSE NULL END",
    },
    {
        "source": "",
        "destination": "refr_urlpath",
        "transform": "regexp_extract(page_referrer, r'^(?:(?:[^:\/?#]+):)?(?:\/\/(?:[^\/?#]*))?([^?#]*)')",
    },
    {
        "source": "",
        "destination": "refr_urlquery",
        "transform": "regexp_extract(page_referrer, r'(?:.*)\?(.*)')",
    },
    {
        "source": "",
        "destination": "refr_urlfragment",
        "transform": "regexp_extract(page_referrer, r'(?:.*)\#(.*)')",
    },
    {
        "source": "traffic_source.medium",
        "destination": "refr_medium",
        "transform": "",
    },
    {
        "source": "traffic_source.source",
        "destination": "refr_source",
        "transform": "",
    },
    {
        "source": "",
        "destination": "refr_term",
        "transform": "regexp_extract(page_referrer, r'utm_term=([^&]*)')",
    },
    {
        "source": "ecommerce.transaction_id",
        "destination": "tr_orderid",
        "transform": "",
    },
    {
        "source": "ecommerce.purchase_revenue",
        "destination": "tr_total",
        "transform": "",
    },
    {
        "source": "ecommerce.tax_value",
        "destination": "tr_tax",
        "transform": "",
    },
    {
        "source": "ecommerce.shipping_value",
        "destination": "tr_shipping",
        "transform": "",
    },
    {
        "source": "device.browser",
        "destination": "br_name",
        "transform": "",
    },
    {
        "source": "",
        "destination": "br_family",
        "transform": "",
    },
    {
        "source": "device.browser_version",
        "destination": "br_version",
        "transform": "",
    },
    {
        "source": "",
        "destination": "br_cookies",
        "transform": "IF(user_pseudo_id is not NULL, true, false)",
    },
    {
        "source": "",
        "destination": "br_viewwidth",
        "transform": "",
    },
    {
        "source": "",
        "destination": "br_viewheight",
        "transform": "",
    },
    {
        "source": "device.operating_system",
        "destination": "os_name",
        "transform": "",
    },
    {
        "source": "",
        "destination": "os_family",
        "transform": "",
    },
    {
        "source": "",
        "destination": "os_manufacturer",
        "transform": "",
    },
    {
        "source": "",
        "destination": "os_timezone",
        "transform": "TIMESTAMP_MICROS(event_timestamp)",
    },
    {
        "source": "",
        "destination": "mkt_clickid",
        "transform": "COALESCE(regexp_extract(page_url, r'gclid=([^&]*)'), regexp_extract(page_url, r'msclkid=([^&]*)'), regexp_extract(page_url, r'dclid=([^&]*)'))",
    },
    {
        "source": "device.category",
        "destination": "dvce_type",
        "transform": "",
    },
    {
        "source": "",
        "destination": "dvce_ismobile",
        "transform": "IF(device.category = 'mobile', 1, 0)",
    },
    {
        "source": "",
        "destination": "dvce_sent_tstamp",
        "transform": "TIMESTAMP_MICROS(event_timestamp)",
    },
    {
        "source": "",
        "destination": "derived_tstamp",
        "transform": "TIMESTAMP_MICROS(event_timestamp)",
    },
    {
        "source": "",
        "destination": "event_vendor",
        "transform": "'com.googleanalytics'",
    },
    {
        "source": "event_name",
        "destination": "event_name",
        "transform": "",
    },
    {
        "source": "",
        "destination": "event_format",
        "transform": "'jsonschema'",
    },
    {
        "source": "",
        "destination": "event_version",
        "transform": "'1-0-0'",
    },
    {
        "source": "",
        "destination": "event_fingerprint",
        "transform": "MD5(TO_JSON_STRING(ga4_data))",
    },
    {
        "source": "",
        "destination": "load_tstamp",
        "transform": "current_timestamp()",
    }, 
    {
        "source": "",
        "destination": "mkt_network",
        "transform": "CASE WHEN page_url LIKE '%gclid=%' THEN 'Google' WHEN page_url LIKE '%msclkid=%' THEN 'Microsoft' WHEN page_url LIKE '%dclid=%' THEN 'DoubleClick' END",
    }
]