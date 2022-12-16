from flask import Flask, render_template
from build_sql_query import create_sql_query
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
import os

project_id = 'snowflake-snowplow-217500'
dataset = os.environ.get('ga4_dataset')
start_date = os.environ.get('start_date')
end_date = os.environ.get('end_date')
destination_table = os.environ.get('destination_dataset_and_table')
sql_query_output = "built_sql_query.sql"


app = Flask(__name__, template_folder='templates', static_folder='static_files')
app.config["DEBUG"] = True


@app.route('/', methods=['GET'])
def greet():
    return render_template('welcome.html')


@app.route('/translate_ga4_to_snowplow', methods=['GET'])
def ga4_to_sp_events():
    try:
        client = bigquery.Client()
        create_sql_query(ga4_table=f"{project_id}.{dataset}.events_*", 
            output_filename=sql_query_output,
            output_table=destination_table,
            start_date=start_date,
            end_date=end_date)
        try:
            with open(sql_query_output, 'r') as f:
                sql = f.read()
                print(sql)
            query_job = client.query(sql)
            
        except BadRequest as br:
            for br in query_job.errors:
                print(br['message'])
        except Exception as f:
            print(f)

        return render_template('conversion_page.html', dataset=dataset, start_date=start_date, end_date=end_date, dest=destination_table)


    except Exception as e:
        return f"Whoops! ERROR: {e}"
        

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5050))
    app.run(debug=True, host='0.0.0.0', port=port)