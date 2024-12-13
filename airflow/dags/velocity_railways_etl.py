import os
import logging
import json
import time
import dotenv
import requests
import pandas as pd
import psycopg2
#import great_expectations as gx
#import great_expectations.expectations as gxe
from datetime import datetime, timedelta
#from dotenv import load_dotenv
#from pyflink.table import (DataTypes, TableEnvironment, EnvironmentSettings)
#from pyflink.table.expressions import col
#from great_expectations.expectations import expectation_configuration
#from great_expectations.core import expectation_suite

#velocity_railways_etl = (__name__)

# DATA EXTRACTION LAYER
def data_extraction(**kwargs):
    load_dotenv()
    APP_KEY = os.getenv("APP_KEY")
    APP_ID = os.getenv("APP_ID")
    id_code = ["RMD", "HBD", "WAT"]
#url = "https://transportapi.com/v3/uk/train/station_timetables/crs:RMD.json"

    BASE_URL = [
        "https://transportapi.com/v3/uk/train/station_timetables/crs:RMD.json",
        "https://transportapi.com/v3/uk/train/station_timetables/crs:HBD.json",
        "https://transportapi.com/v3/uk/train/station_timetables/crs:WAT.json"
    ]

    transport_data = []

    # Current time in isoformat
    t = datetime.now().strftime("%Y-%m-%dT%H:%M:%S+01:00")

    for url in BASE_URL:
    
        params = { 
        #"id": id,
        "app_id": APP_ID,
        "app_key": APP_KEY,
        #"darwin": "false",
        "datetime": t,
        "from_offset": "PT00:30:00",
        "to_offset": "PT02:30:00",
        "limit": "20",
        "live": "true",
        "train_status": "passenger",
        "station_detail": "origin",
        "type": "departure"
        #"source_config": "dws_staff"
        }

        headers = {
        'accept': 'application/json'
        }

        response = requests.get(url, params = params, headers = headers)

        time.sleep(10)
    
        # Check the response status and print the response data
        if response.status_code == 200:
            #print(response.json())
            transport_data.append(response.json())
            print(transport_data)
        else:
            print(f"Error: {response.status_code}, {response.text}")

    # DATA NORMALIZATION AND CLEANING
    ## Use pd.json_normalize to convert the JSON to a DataFrame
    df_railways = pd.json_normalize(transport_data, ['departures', 'all'], \
                                    ['date', 'time_of_day', 'request_time', 'station_code', 'station_name'],)
                                 
    df_railways_data = pd.DataFrame(df_railways)

    # Let's remove null values in our data
    df_railways_data['best_arrival_estimate_mins'].fillna(0, inplace=True,)

    df_railways_data.fillna('None', inplace=True)

    # Let's check if there are still null values in our data
    for data in df_railways_data.iterrows():
        if data == 'null':
            print(f'data cleaning was not successful {data}')
        else:
            print(f"Normalization and cleaning was successful; great job! {df_railways_data}") 
    
    #print(f'Your data is successfully normalized and cleaaned {df_railways}')
    ti = kwargs['ti']
    ti.xcom_push(key='railways_data', value=df_railways_data)


# DATA TRANSFORMATION WITH FLINK
from pyflink.table import (DataTypes, TableEnvironment, EnvironmentSettings)
from pyflink.table.expressions import col

def data_transformation(**kwargs):
    ti = kwargs['ti']
    df_railways = ti.xcom_pull(key='railways_data',) #task_id='data_extraction')

    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    t_env.get_config().set("parallelism.default", "1")

# Data Transformation and conversion to pandas dataframe
    table_temp = t_env.from_pandas(df_railways)

    table_temp1 = table_temp.drop_columns(
        col('aimed_arrival_time'),
        col('expected_departure_time'),
        col('expected_arrival_time'),
        col('source'),
        col('category'),
        col('best_arrival_estimate_mins'),
        col('station_detail.origin.station_name'),
        col('request_time'),
        col('station_detail.origin.station_name'),
        col('station_detail.origin.platform'), 
        col('station_detail.origin.station_code'),
        col('station_detail.origin.tiploc_code'),
        col('station_detail.origin.aimed_arrival_time'),
        col('station_detail.origin.aimed_departure_time'),
        col('station_detail.origin.aimed_pass_time'),
        col('aimed_pass_time')
    )

#table_temp1.limit(5).execute().print()

    table_temp2 = table_temp1.rename_columns(
        col('service_timetable.id').alias('timetable_id'),
        col('aimed_departure_time').alias('departure_time')
    )

#table_temp2.limit(5).execute().print()
    table_df = table_temp2.to_pandas()

    #table_df
    ti = kwargs['ti']
    ti.xcom_push(key='transformed_data', value=table_df)


# DATA VALIDATION LAYER
import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.expectations import expectation_configuration
from great_expectations.core import expectation_suite

def data_validation(**kwargs):
    ti = kwargs['ti']
    table_df = ti.xcom_pull(key='transformed_data',) #task_id='data_transformation')
    # create a context
    context = gx.get_context()

# Define the data source
    data_source_name = "table_df"

# Add the data source to the context
    data_source = context.data_sources.add_pandas(name=data_source_name)

# Retrieve the data source from the context
    data_source = context.data_sources.get(data_source_name)

# Define the data asset name
    data_asset_name = "table_df_asset"

# Add a data asset to the data source
    data_asset = data_source.add_dataframe_asset(name=data_asset_name)

# Retrieve the data asset from the context
    data_asset = context.data_sources.get(data_source_name).get_asset(data_asset_name)

# Define a batch
    batch_definition_name = 'table_df_batch_definition'

# Add the batch definition to the data asset
    batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_definition_name)

# Define the batch parameters and batch definition parameters
    batch_parameters = {"dataframe":table_df}

    batch_definition = (
        context.data_sources.get(data_source_name)
        .get_asset(data_asset_name)
        .get_batch_definition(batch_definition_name)
    )

# Define an expectation suite
    suite_name = "train_expectation_suite"
    suite = gx.ExpectationSuite(name=suite_name)

# Add expectation suite to the context
    suite = context.suites.add(suite)

# Create a List of expectations for each table column, add to the expectation suite and save.
    expectation1 = gxe.ExpectColumnValuesToNotBeNull(column="mode")
    suite.add_expectation(expectation1)
    expectation1.save()

    expectation2 = gxe.ExpectColumnValuesToNotBeNull(column="service")
    suite.add_expectation(expectation2)
    expectation2.save()

    expectation3 = gxe.ExpectColumnValuesToNotBeNull(column="train_uid")
    suite.add_expectation(expectation3)
    expectation3.save()

    expectation4 = gxe.ExpectColumnValuesToNotBeNull(column="platform")
    suite.add_expectation(expectation4)
    expectation4.save()

    expectation5 = gxe.ExpectColumnValuesToNotBeNull(column="operator")
    suite.add_expectation(expectation5)
    expectation5.save()

    expectation6 = gxe.ExpectColumnValuesToNotBeNull(column="operator_name")
    suite.add_expectation(expectation6)
    expectation6.save()

    expectation7 = gxe.ExpectColumnValuesToNotBeNull(column="departure_time")
    suite.add_expectation(expectation7)
    expectation7.save()

    expectation9 = gxe.ExpectColumnValuesToNotBeNull(column="origin_name")
    suite.add_expectation(expectation9)
    expectation9.save()

    expectation10 = gxe.ExpectColumnValuesToNotBeNull(column="destination_name")
    suite.add_expectation(expectation10)
    expectation10.save()

    expectation11 = gxe.ExpectColumnValuesToNotBeNull(column="status")
    suite.add_expectation(expectation11)
    expectation11.save()

    expectation12 = gxe.ExpectColumnValuesToNotBeNull(column="best_departure_estimate_mins")
    suite.add_expectation(expectation12)
    expectation12.save()

    expectation13 = gxe.ExpectColumnValuesToNotBeNull(column="timetable_id")
    suite.add_expectation(expectation13)
    expectation13.save()

    expectation14 = gxe.ExpectColumnValuesToNotBeNull(column="date")
    suite.add_expectation(expectation14)
    expectation14.save()

    expectation15 = gxe.ExpectColumnValuesToNotBeNull(column="time_of_day")
    suite.add_expectation(expectation15)
    expectation15.save()

    expectation16 = gxe.ExpectColumnValuesToNotBeNull(column="station_code")
    suite.add_expectation(expectation16)
    expectation16.save()

    expectation17 = gxe.ExpectColumnValuesToNotBeNull(column="station_name")
    suite.add_expectation(expectation17)
    expectation17.save()

    batch = batch_definition.get_batch(batch_parameters=batch_parameters)

    validation_results = batch.validate(suite)

# Take action based on the results
    if validation_results["success"]:
        print ("Validation was Successful!")
    else:
        print("Validation Failed.")

    #kwargs['ti'].xcom_push(key='validated_data', value=table_df)


# DATA LOADING LAYER (to POSTGRES AND GCP)
def data_loading(**kwargs):

    ti = kwargs['ti']
    table_df = ti.xcom_pull(key='transformed_data',) #task_id='data_transformation')

    def get_db_connection():
        connection = psycopg2.connect(
        host = os.getenv("DB_HOST"),
        user = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD"),
        dbname = os.getenv("DB_NAME"),
    #database = 'postgres'
         )
        return connection

# Load data into the velocity_railways_db database
# Load the data into the 'train' table of the velocity railways database
    def load_to_postgres():
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            cursor.execute("""
                    CREATE TABLE IF NOT EXISTS train_schedule(
                        id SERIAL PRIMARY KEY, 
                        mode VARCHAR(50), 
                        service VARCHAR(50), 
                        train_uid VARCHAR(50), 
                        platform VARCHAR(50), 
                        operator VARCHAR(50),
                        operator_name VARCHAR(255), 
                        departure_time VARCHAR(25), 
                        origin_name VARCHAR(255),
                        destination_name VARCHAR(255), 
                        status VARCHAR(50), 
                        best_departure_estimate_mins INT,
                        timetable_id VARCHAR(255),
                        date VARCHAR(50), 
                        time_of_day VARCHAR(25), 
                        station_code VARCHAR(50), 
                        station_name VARCHAR(255)
                    )"""
            )
        
            for idx, row in table_df.iterrows():
                cursor.execute(
                    """INSERT INTO train_schedule(mode, service, train_uid, platform, operator, \
                    operator_name, departure_time, origin_name, \
                    destination_name, status, best_departure_estimate_mins, timetable_id, \
                    date, time_of_day, station_code, station_name)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
                    (row['mode'], row['service'], row['train_uid'], row['platform'], \
                    row['operator'], row['operator_name'], row['departure_time'], \
                    row['origin_name'], row['destination_name'], \
                    row['status'], row['best_departure_estimate_mins'], row['timetable_id'], \
                    row['date'], row['time_of_day'], row['station_code'], row['station_name'])
                )

            conn.commit()
        except Exception as e:
            print(f"An error occurred: {e}")
            logging.error(f"An error occurred: {e}")
            conn.rollback()
        finally:
            if conn:
                print("Data loaded successfully")
                logging.info("Data loaded successfully")
            else:
                print("Data not loaded")
                logging.info("Data not loaded")
            
        cursor.close()
        conn.close()

    def load_to_gcp():
        # 1. Import modules and packages
        import os
        import pandas as pd
        from google.cloud import storage

        # 2. Set JSON key as environment variable
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= os.getenv("GCP_PARAMS")

        # 3. Specify a bucket name and other details
        bucket_name = 'railways_train_schedule'
        contents = table_df
        destination_blob_name = 'train_schedule'

        # 4. Define a special function
        def upload_to_storage(bucket_name, contents, destination_blob_name):
            """Uploads a file to the bucket."""
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            train_csv = contents.to_csv(index=False).encode()
            blob.upload_from_string(train_csv)
            print(f'The file {contents} is uploaded to GCP bucket path: {destination_blob_name}')
            return None

        # 5. Run the function!
        upload_to_storage(bucket_name, contents, destination_blob_name)



if __name__ == "__main__":
    data_extraction
    data_transformation
    data_validation
    data_loading