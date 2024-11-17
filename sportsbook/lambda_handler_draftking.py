import requests
import json, pickle, boto3, os
from utils import draftking_lines, draftking_odds

def invoke_draftking_lambda():
    client = boto3.client(service_name="lambda")
    return client.invoke(FunctionName = "private-rds-db-draftking", InvocationType = "Event")

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

def get_response(url: str) -> json:
    """
    Takes in a URL and resturns a JSON response

    Params:
        url (str): Takes in a Draftking API Url
    
    Returns:
        response (json): Response in JSON
    """
    headers = {'Origin': os.environ["ORIGIN"], 'User-Agent': os.environ["USER_AGENT"]}
    while True:
        response = requests.request("GET", url, headers=headers, timeout=120)
        if response.status_code == 200:
            return response.json()

def lambda_handler(event, content):
    nfl_prop_data = draftking_lines("88808")
    nba_prop_data = draftking_lines("42648")
    nfl_odd_data = draftking_odds("88808")
    nba_odd_data = draftking_odds("42648")
    all_prop_data = nfl_prop_data + nba_prop_data
    all_odd_data = nfl_odd_data + nba_odd_data
    print(f"{len(all_odd_data)} odds and {len(all_prop_data)} prop datas found on Draftking")
    upload_pickle_to_s3(data=all_odd_data, bucket_name="draftking-bucket", file_key="odds.pickle")
    upload_pickle_to_s3(data=all_prop_data, bucket_name="draftking-bucket", file_key="props.pickle")
    response = invoke_draftking_lambda()
    if response["StatusCode"] == 202:
        print("PostgreSQL RDS database was invoked and run")
    return {
        'statusCode': 200,
        'body': json.dumps('Lambda function successfully ran with no errors'),
    }

