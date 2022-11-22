from flask import Flask
import pandas_gbq

app = Flask(__name__)
app.config["DEBUG"] = True #If the code is malformed, there will be an error shown when visit app

project_id = 'snowflake-snowplow-217500'
dataset = 'scratch'
table = 'customer_test'


sql = f"""
    SELECT *
    FROM `{project_id}.{dataset}.{table}`
"""

@app.route('/', methods=['GET'])
def ga4_to_sp_events():
    print("Hi")
    df = pandas_gbq.read_gbq(sql, project_id=project_id)
    return f"Queried table! {print(df.head())}"