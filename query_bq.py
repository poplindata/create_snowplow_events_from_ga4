from flask import Flask, render_template
import pandas_gbq
import os

app = Flask(__name__, template_folder='templates', static_folder='static_files')
app.config["DEBUG"] = True

project_id = 'snowflake-snowplow-217500'
dataset = os.environ.get('ga4_dataset')
table = os.environ.get('ga4_table')
destination_table = os.environ.get('destination_dataset_and_table')

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

