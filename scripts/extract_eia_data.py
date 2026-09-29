# IMPORTS
import requests
import json
from datetime import datetime, timezone, timedelta
import time
from pyspark.sql.functions import col, parse_json
import uuid
import hashlib
from delta.tables import DeltaTable

# Define namespace
catalog_name = "the_data_masons"
schema_name = "landing"

# Set global API details
api_key = dbutils.secrets.get(scope = "the-data-masons", key = "eia_api_key")
period_start_date = "2026-07-01T00"
time_period = "2026-09-01T00"
# time_period = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H")
LENGTH = 5000

# Rate Limiting Configuration
# 0.45 seconds = ~2.22 req/sec (max 5) & ~8,000 req/hr (max 9,000)
MIN_REQUEST_INTERVAL = 0.45  
LAST_REQUEST_TIMESTAMP = 0.0

def enforce_rate_limit():
    # Enforces minimum interval between API calls to honor rate caps.
    global LAST_REQUEST_TIMESTAMP
    elapsed = time.time() - LAST_REQUEST_TIMESTAMP
    if elapsed < MIN_REQUEST_INTERVAL:
        time.sleep(MIN_REQUEST_INTERVAL - elapsed)
    LAST_REQUEST_TIMESTAMP = time.time()


def build_Xparams(tracker):
    print("Building Xparams")
    return {
        "frequency":"hourly",
        "data": ["value"],
        "start":tracker['target_dt'],
        "end":tracker['target_dt'],
        "facets": tracker.get("facets", {}),
        "sort": tracker.get("sort", []),
        "offset": tracker['offset'],
        "length": LENGTH
    }

def fetch_response(url, params, headers):
    print("Fetching data")
    return requests.get(url, params= params, headers= headers)

def construct_record(tracker, response):
    print("Constructing record")

    record_buffer = {
        'run_id' : tracker['run_id'],
        "request_timestamp": tracker['timestamp'],
        'target_dt':tracker['target_dt'],
        'page': tracker['page'],
        'offset_start':tracker['offset'],
        "status_code": response.status_code,
        "status": response.reason
    }

    if record_buffer["status_code"] == 200:
        print("Status code is 200")
        data = response.json()
        raw_response = json.dumps(data)
        records = data.get("response",{}).get("data",[])
        record_count = len(records)
        error_msg = 'None'
    else:
        print(f"Status code is {record_buffer["status_code"]}")
        error_msg = response.text if hasattr(response, 'text') else str(response.get("error"))
        raw_response = "{}"
        record_count = 0

    response_hash = hashlib.md5(raw_response.encode('utf-8')).hexdigest()

    record_buffer.update({
        "raw_response": raw_response,
        'response_hash' : response_hash,
        "error_msg": error_msg,
        "record_count": record_count
    })

    return record_buffer

def create_dataframe(buffer):
    print("Creating dataframe from buffer")
    return (spark.createDataFrame(buffer)
    .withColumn("raw_response", parse_json(col("raw_response"))))

def target_hours(period_start_date):
    print("Calculating target hours")
    start_dt = datetime.strptime(period_start_date, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(time_period, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)

    hours = []
    curr = start_dt
    while curr <= end_dt:
        hours.append(curr.strftime("%Y-%m-%dT%H"))
        curr += timedelta(hours=1)

    return hours
    # print(f"Target hours: {hours}")

def extract_endpoint(endpoint, dt_list):

    # create a Delta Table buffer array
    landing_buffer = []
    tracker = {
        'url' : endpoint['url'],
        'target_table' : endpoint['table'],
        'facets' : endpoint.get('facets', {}),
        'sort' : endpoint.get('sort', []),
        "run_id" : "",
        'timestamp': datetime.now(timezone.utc),
        'target_dt' : '',
        "offset" : 0,
        "page" : 0
    }

    for i, hour in enumerate(dt_list):

        # generate run id
        tracker["run_id"] = str(uuid.uuid4())
        tracker['offset'] = 0
        tracker['target_dt'] = hour
        has_more_data = True

        # loop and append response record to the buffer
        while has_more_data:

            # set page number
            tracker["page"] = (tracker["offset"] // LENGTH) + 1

            # build request params
            headers = {"X-Params" : json.dumps(build_Xparams(tracker))}
            params = {"api_key": api_key}

            # get request time
            enforce_rate_limit()
            tracker['timestamp'] = datetime.now(timezone.utc)

            print(f'header: {headers}, url: {tracker["url"]}, params: {params}')

            # get request resonse
            # fetch_response() returns a response object
            response = fetch_response(tracker['url'], params, headers)

            # no errors, then Construct record
            # construct_record() returns 
            landing_record = construct_record(tracker, response)

            # update tracker
            if landing_record["status_code"] != 200:
                print(f"Failed to fetch data, status code: {landing_record['status_code']}")
                has_more_data = False
            elif landing_record["record_count"] < LENGTH:
                has_more_data = False
            else:
                tracker["offset"] += LENGTH
            
            

            # append record to buffer
            print(f"Appending record {i} to buffer")
            print(landing_record)   
            landing_buffer.append(landing_record)
        
    # create a dataframe from the buffer array
    df_landing = create_dataframe(landing_buffer)
                                                
    return df_landing

if __name__ == '__main__':
    print(time_period)
    dt_list = target_hours(period_start_date)

    ENDPOINTS = [
        {
            "url": "https://api.eia.gov/v2/electricity/rto/region-data/data/",
            "table": "RAW_REGIONAL_OPERATING_METRICS",
            "facets": {
                "respondent": [
                    "NYIS"
                ]
            },
            "sort": [
                {
                    "column": "period",
                    "direction": "asc"
                },
                {
                    "column": "respondent",
                    "direction": "asc"
                },
                {
                    "column": "type",
                    "direction": "asc"
                }
            ],
        },
        {
            "url": "https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/",
            "table": "RAW_GENERATION_ENERGY_SOURCE",
            "facets": {
                "respondent": [
                    "NYIS"
                ]
            },
            "sort": [
                {
                    "column": "period",
                    "direction": "asc"
                },
                {
                    "column": "respondent",
                    "direction": "asc"
                }
            ], 
        },
        {
            "url": "https://api.eia.gov/v2/electricity/rto/region-sub-ba-data/data/",
            "table": "RAW_SUBREGIONAL_DEMAND",
           "facets": {
                "parent": [
                    "NYIS"
                ]
            },
            "sort": [
                {
                    "column": "period",
                    "direction": "asc"
                },
                {
                    "column": "subba",
                    "direction": "asc"
                },
                {
                    "column": "parent",
                    "direction": "asc"
                }
            ],
        }
    ]

    for endpoint in ENDPOINTS:
        full_table_path = f"{catalog_name}.{schema_name}.{endpoint['table']}"
        df = extract_endpoint(endpoint, dt_list)
        
        # Table existence check for the first run
        if spark.catalog.tableExists(full_table_path):
            target_table = DeltaTable.forName(spark, full_table_path)
            
            target_table.alias("t") \
                .merge(
                    source = df.alias("s"),
                    condition="t.target_dt = s.target_dt AND t.page = s.page"
                ) \
                .whenMatchedUpdateAll(
                    condition='t.response_hash != s.response_hash'
                ) \
                .whenNotMatchedInsertAll() \
                .execute()
        else:
            # First run: create the table directly from the dataframe
            print(f"Creating new table: {full_table_path}")
            df.write \
              .format("delta") \
              .mode("overwrite") \
              .saveAsTable(full_table_path)

            # Enable row tracking so DLT can read it incrementally
            print(f"Enabling row tracking for: {full_table_path}")
            spark.sql(f"ALTER TABLE {full_table_path} SET TBLPROPERTIES ('delta.enableRowTracking' = 'true')")