fields_1 = [
    {
        "source": "",
        "destination": "domain_sessionidx",
        "transform": "(select value.int_value from unnest(event_params) where key = 'ga_session_number')",
    },
    {
        "source": "",
        "destination": "page_url",
        "transform": "(select value.string_value from unnest(event_params) where key = 'page_location')",
    },
    {
        "source": "",
        "destination": "page_title",
        "transform": "(select value.string_value from unnest(event_params) where key = 'page_title')",
    },
    {
        "source": "",
        "destination": "page_referrer",
        "transform": "(select value.string_value from unnest(event_params) where key = 'page_referrer')",
    },
    {
        "source": "",
        "destination": "mkt_medium",
        "transform": "(select value.string_value from unnest(event_params) where key = 'medium')",
    },
    {
        "source": "",
        "destination": "mkt_source",
        "transform": "(select value.string_value from unnest(event_params) where key = 'source')",
    },
    {
        "source": "",
        "destination": "mkt_term",
        "transform": "(select value.string_value from unnest(event_params) where key = 'term')",
    },
    {
        "source": "",
        "destination": "mkt_content",
        "transform": "(select value.string_value from unnest(event_params) where key = 'content')",
    },
    {
        "source": "",
        "destination": "mkt_campaign",
        "transform": "(select value.string_value from unnest(event_params) where key = 'campaign')",
    },
    {
        "source": "",
        "destination": "tr_affiliation",
        "transform": "(select affiliation from unnest(items))",
    },
    {
        "source": "",
        "destination": "ti_sku",
        "transform": "(select item_id from unnest(items))",
    },
    {
        "source": "",
        "destination": "ti_name",
        "transform": "(select item_name from unnest(items))",
    },
    {
        "source": "",
        "destination": "ti_category",
        "transform": "(select item_category from unnest(items))",
    },
    {
        "source": "",
        "destination": "ti_price",
        "transform": "(select price from unnest(items))",
    },
    {
        "source": "",
        "destination": "ti_quantity",
        "transform": "(select quantity from unnest(items))",
    },
    {
        "source": "",
        "destination": "domain_sessionid",
        "transform": "(select value.int_value from unnest(event_params) where key = 'ga_session_id')",
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
        "source": "null",
        "destination": "app_id",
        "transform": "CASE WHEN stream_id = '4271243942' THEN 'Poplin' ELSE NULL END"
    },
    {
        "source": "platform",
        "destination": "platform",
        "transform": "null"
    },
    {
        "source": "null",
        "destination": "etl_tstamp",
        "transform": "TIMESTAMP_MICROS(event_timestamp)"
    },
    {
        "source": "null",
        "destination": "collector_tstamp",
        "transform": "TIMESTAMP_MICROS(event_timestamp)",
    },
    {
        "source": "null",
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
        "transform": "google-analytics-4",
    },
    {
        "source": "",
        "destination": "v_tracker",
        "transform": "1.0.0",
    },
    {
        "source": "",
        "destination": "v_collector",
        "transform": "collector-version",
    },
    {
        "source": "",
        "destination": "v_etl",
        "transform": "enrich-version",
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
        "source": "geo_region_name",
        "destination": "geo_region_name",
        "transform": "",
    },
    {
        "source": "page_url",
        "destination": "page_url",
        "transform": "",
    },
    {
        "source": "page_title",
        "destination": "page_title",
        "transform": "",
    },
    {
        "source": "page_referrer",
        "destination": "page_referrer",
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
        "source": "mkt_medium",
        "destination": "mkt_medium",
        "transform": "",
    },
    {
        "source": "mkt_source",
        "destination": "mkt_source",
        "transform": "",
    },
    {
        "source": "mkt_term",
        "destination": "mkt_term",
        "transform": "",
    },
    {
        "source": "mkt_content",
        "destination": "mkt_content",
        "transform": "",
    },
    {
        "source": "mkt_campaign",
        "destination": "mkt_campaign",
        "transform": "",
    },
    {
        "source": "ecommerce.transaction_id",
        "destination": "tr_orderid",
        "transform": "",
    },
    {
        "source": "tr_affiliation",
        "destination": "tr_affiliation",
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
        "source": "ti_sku",
        "destination": "ti_sku",
        "transform": "",
    },
    {
        "source": "ti_name",
        "destination": "ti_name",
        "transform": "",
    },
    {
        "source": "ti_category",
        "destination": "ti_category",
        "transform": "",
    },
    {
        "source": "ti_price",
        "destination": "ti_price",
        "transform": "",
    },
    {
        "source": "ti_quantity",
        "destination": "ti_quantity",
        "transform": "",
    },
    {
        "source": "device.browser",
        "destination": "br_name",
        "transform": "",
    },
    {
        "source": "null",
        "destination": "br_family",
        "transform": "null",
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
        "destination": "",
        "transform": "",
    },
    {
        "source": "",
        "destination": "",
        "transform": "",
    },
    {
        "source": "",
        "destination": "",
        "transform": "",
    },
    {
        "source": "",
        "destination": "",
        "transform": "",
    },


]