from map import fields_1, fields_2


def extract_fields(item: dict):
    sql_fields = []
    if item['source']:
        sql_fields.append(f"{item['source']} AS {item['destination']},")
    elif item['transform']:
        sql_fields.append(f"{item['transform']} AS {item['destination']},")
    else:
        sql_fields.append(f"NULL AS {item['destination']},")
    return sql_fields

def create_sql_query(ga4_table,output_filename, output_table, start_date=None, end_date=None ):
    cte_1 = []
    cte_2 = []
    for item in fields_1:
        cte_1.extend(extract_fields(item))

    for item in fields_2:
        cte_2.extend(extract_fields(item))
        
    with open(f"{output_filename}", 'w') as f:
        f.write(f"CREATE OR REPLACE TABLE {output_table} AS (")
        f.write("WITH ga4_data AS (\n")
        f.write("SELECT *,\n")
        for field in cte_1:
            f.writelines({f"{field}\n"})
        f.writelines(f"""
    FROM `{ga4_table}`
    """)
        if start_date and end_date:
            f.writelines(f"""
            WHERE _TABLE_SUFFIX BETWEEN '{start_date}' AND '{end_date}'
            """)
        f.writelines(f"""
        )
        SELECT\n""")
        for field in cte_2:
            f.writelines({f"{field}\n"})
        f.writelines(f"""
    FROM ga4_data
    LEFT JOIN ga4_test.countries_0 c ON ga4_data.geo_country_name=c.name
    LEFT JOIN ga4_test.regions r ON ga4_data.geo_region_name=r.name AND SPLIT(r.code, '-')[SAFE_OFFSET(0)]=c.alpha2_code
    )    """)

if __name__ == "__main__":
    create_sql_query("snowflake-snowplow-217500.analytics_341844832.events_*", "built_sql_query.sql", "ga4_to_snowplow.ga4_events", start_date='20221201', end_date='20221213')