from flask import Flask
import pandas_gbq

app = Flask(__name__)
app.config["DEBUG"] = True #If the code is malformed, there will be an error shown when visit app

project_id = 'snowflake-snowplow-217500'
dataset = 'analytics_341844832'
table = 'events_*'


sql = f"""
    SELECT 
    TIMESTAMP_MICROS(event_Timestamp) as event_timestamp,
    event_name,
    max(case when event_params.key = 'page_referrer'
        then event_params.value.string_value else null
    end) as page_referrer,
    max(case when event_params.key = 'page_title'
        then event_params.value.string_value else null
    end) as page_title,
    max(case when event_params.key = 'ga_session_id'
        then event_params.value.int_value else null
    end) as ga_session_id,
    FROM `{project_id}.{dataset}.{table}`, unnest(event_params) as event_params
    group by event_timestamp, event_name
"""

@app.route('/translate_ga4_to_snowplow', methods=['GET'])
def ga4_to_sp_events():
    df = pandas_gbq.read_gbq(sql, project_id=project_id)
    pandas_gbq.to_gbq(dataframe=df, destination_table='ga4_test.ga4_events', project_id=project_id)
    return f"""
        Queried table row count:\n 
        {df.shape[0]}
        New table has been created
    """
    
