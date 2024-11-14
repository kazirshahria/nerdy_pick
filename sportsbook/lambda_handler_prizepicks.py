import logging, requests
import datetime as dt
import json, pickle, boto3, os
from utils import prizepicks_lines

def invoke_prizepicks_lambda():
    client = boto3.client(service_name="lambda")
    return client.invoke(FunctionName = "private-rds-db", InvocationType = "Event")

def upload_pickle_to_s3(data, bucket_name, file_key):
    # Serialize the data to a pickle format
    pickle_data = pickle.dumps(data)
    # Initialize S3 client
    s3_client = boto3.client(
        service_name = 's3', 
        region_name = "us-east-2"
        )
    # Upload the pickled data to S3
    try:
        s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=pickle_data)
        print(f"File uploaded successfully to s3://{bucket_name}/{file_key}")
    except Exception as e:
        print(f"Error uploading file: {e}")

def lambda_handler(event, content):
    players_data_nfl, lines_data_nfl = prizepicks_lines("NFL")
    players_data_nba, lines_data_nba = prizepicks_lines("NBA")
    all_players_data = players_data_nba + players_data_nfl
    all_props_data = lines_data_nba + lines_data_nfl
    print(f"{len(all_players_data)} players and {len(all_props_data)} prop datas found on Prizepicks")
    upload_pickle_to_s3(data=all_players_data, bucket_name="prizepicks-bucket", file_key="players.pickle")
    upload_pickle_to_s3(data=all_props_data, bucket_name="prizepicks-bucket", file_key="props.pickle")
    response = invoke_prizepicks_lambda()
    if response["StatusCode"] == 202:
        print("PostgreSQL RDS database was invoked and run")
    return {
        'statusCode': 200,
        'body': json.dumps('Lambda function successfully ran with no errors'),
    }
