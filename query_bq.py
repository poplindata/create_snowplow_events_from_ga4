from flask import Flask, render_template
import pandas_gbq
import os


app = Flask(__name__, template_folder='templates', static_folder='static_files')
app.config["DEBUG"] = True

project_id = 'snowflake-snowplow-217500'
dataset = os.environ.get('ga4_dataset')
table = os.environ.get('ga4_table')
destination_table = os.environ.get('destination_dataset_and_table')

sql = f"""WITH ga4_data AS (
    SELECT * FROM `{project_id}.{dataset}.{table}`
),
ga4 AS (
SELECT  
    CASE WHEN stream_id = '4271243942' -- TODO
        THEN ""
        ELSE NULL
    END AS app_id,
    platform,
    TIMESTAMP_MICROS(event_timestamp) AS etl_tstamp,
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
    (select value.int_value from unnest(event_params) where key = 'ga_session_number') AS domain_sessionidx,
    geo.country AS geo_country_name,
    geo.city AS geo_city,
    geo.region AS geo_region_name,
    (select value.string_value from unnest(event_params) where key = 'page_location') AS page_url,
    (select value.string_value from unnest(event_params) where key = 'page_title') AS page_title,
    (select value.string_value from unnest(event_params) where key = 'page_referrer') AS page_referrer,
    traffic_source.medium AS refr_medium,
    traffic_source.source AS refr_source,
    (select value.string_value from unnest(event_params) where key = 'medium') as mkt_medium,
    (select value.string_value from unnest(event_params) where key = 'source') AS mkt_source,
    (select value.string_value from unnest(event_params) where key = 'term') AS mkt_term,
    (select value.string_value from unnest(event_params) where key = 'content') AS mkt_content,
    (select value.string_value from unnest(event_params) where key = 'campaign') AS mkt_campaign,
    ecommerce.transaction_id AS tr_orderid,
    (select affiliation from unnest(items)) AS tr_affiliation,
    ecommerce.purchase_revenue AS tr_total,
    ecommerce.tax_value AS tr_tax,
    ecommerce.shipping_value AS tr_shipping,
    (select item_id from unnest(items)) AS ti_sku,
    (select item_name from unnest(items)) AS ti_name,
    (select item_category from unnest(items)) AS ti_category,
    (select price from unnest(items)) AS ti_price,
    (select quantity from unnest(items)) AS ti_quantity,
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
    device.category AS dvce_type,
    IF(device.category = 'mobile', 1, 0) AS dvce_ismobile,
    TIMESTAMP_MICROS(event_timestamp) AS dvce_sent_tstamp,
    (select value.int_value from unnest(event_params) where key = 'ga_session_id') AS domain_sessionid,
    TIMESTAMP_MICROS(event_timestamp) AS derived_tstamp,
    "com.googleanalytics" AS event_vendor,
    event_name,
    "jsonschema" AS event_format,
    "1-0-0" AS event_version,
    MD5(TO_JSON_STRING(ga4_data)) AS event_fingerprint,
    current_timestamp() AS load_tstamp

FROM ga4_data
),
extract_fields AS (
    SELECT
        g.*except(geo_country_name),
        regexp_extract(page_referrer, r'utm_term=([^&]*)') AS refr_term,
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
        c.alpha2_code AS geo_country,
        SPLIT(r.code, '-')[SAFE_OFFSET(1)] AS geo_region,
        COALESCE(
        regexp_extract(page_url, r'gclid=([^&]*)'),
        regexp_extract(page_url, r'msclkid=([^&]*)'),
        regexp_extract(page_url, r'dclid=([^&]*)')
        ) AS mkt_clickid,

    FROM ga4 g
    LEFT JOIN ga4_test.countries_0 c ON g.geo_country_name=c.name
    LEFT JOIN ga4_test.regions r ON g.geo_region_name=r.name AND SPLIT(r.code, '-')[SAFE_OFFSET(0)]=c.alpha2_code
),
create_fields as (
    select 
        *,
        CASE WHEN page_urlscheme = 'https'
            THEN '443'
            WHEN page_urlscheme = 'http'
            THEN '80'
            ELSE NULL
        END AS refr_urlport,
        CASE WHEN refr_urlscheme = 'https'
            THEN '443'
            WHEN refr_urlscheme = 'http'
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
    from extract_fields
)
select * from create_fields
"""

@app.route('/', methods=['GET'])
def greet():
    return render_template('welcome.html')


@app.route('/translate_ga4_to_snowplow', methods=['GET'])
def ga4_to_sp_events():
    try:
        df = pandas_gbq.read_gbq(sql, project_id=project_id)
        pandas_gbq.to_gbq(dataframe=df, destination_table=destination_table, project_id=project_id, if_exists='replace')
        
        return render_template('conversion_page.html', dataset=dataset, table=table, rows=df.shape[0], dest=destination_table)

    except Exception as e:
        return f"Whoops! ERROR: {e}"
        

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5050))
    app.run(debug=True, host='0.0.0.0', port=port)