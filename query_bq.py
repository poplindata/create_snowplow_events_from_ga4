
import pandas_gbq

project_id = 'snowflake-snowplow-217500'
dataset = 'scratch'
table = 'customer_test'


sql = f"""
    SELECT *
    FROM `{project_id}.{dataset}.{table}`
"""

df = pandas_gbq.read_gbq(sql, project_id=project_id)
print(df.head())