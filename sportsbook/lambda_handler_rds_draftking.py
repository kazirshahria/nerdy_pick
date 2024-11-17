import os
import json
import pickle
import boto3
import psycopg2
from psycopg2.extras import execute_values

def db_connection(db_name: str):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host=os.environ["DB_ENDPOINT"],
        port=5432,
        user=os.environ["DB_USERNAME"],
        password=os.environ["DB_PASSWORD"],
        dbname=db_name
    )
    return conn

def sql_query(query_name: str) -> str:
    # Queries to import data into the database
    draftking_odd_query = """
        INSERT INTO draftkind_odd (odd_id, home_team_id, away_team_id, game_date, game_time, prop_league, home_team, away_team, home_spread, away_spread, home_spread_odd, away_spread_odd, home_spread_true_odd, away_spread_true_odd, home_moneyline, away_moneyline, home_moneyline_true_odd, away_moneyline_true_odd, over_total, under_total, over_total_moneyline, under_total_moneyline, over_total_true_odd, under_total_true_odd)
        VALUES %s
        ON CONFLICT (odd_id) DO NOTHING;
    """
    draftking_prop_query = """
        INSERT INTO draftking_prop (prop_id, player_id, game_date, game_time, prop_desc, prop_league, home_team, away_team, player_name, prop_line_over, prop_line_under, over_odd, under_odd, over_true_odd, under_true_odd)
        VALUES %s
        ON CONFLICT (prop_id) DO NOTHING;
    """
    if query_name == "odd":
        return draftking_odd_query
    return draftking_prop_query

def import_data_into_db(connection: psycopg2.connect, query: str, query_type: str, data: list):
    """
    Handles appending new data into the PostgreSQL database and commiting the changes

    Params:
        connection (psycopg2.connect): Database connection
        query (str): SQL query to insert data into a database
        query_type (str): SQL query type 
        data (list): A list of tuple values
    """
    cursor = connection.cursor()
    execute_values(cur=cursor, sql=query, argslist=data)
    print(f"Successfully ran the SQL {query_type} query to import data into the database")
    connection.commit()
    print(f"Commited the changes into the PostgreSQL database")

def s3_bucket_data(file_name: str):
    # Initialize S3 client
    s3_client = boto3.client('s3')

    # Replace with your S3 bucket name
    bucket_name = 'draftking-bucket'

    # Retrieve the file from S3
    try:
        s3_object = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        # Read the file content and load with pickle
        file_content = s3_object['Body'].read()
        data = pickle.loads(file_content)
        return data
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error retrieving or loading file: {str(e)}")
        }

def lambda_handler(event, context):
    # Create a database connection
    db_name = "sportsbookdb"
    conn = db_connection(db_name)
    print("Connected to PostgreSQL")

    # Get the data for the S3 data
    all_odds_data = s3_bucket_data("odds.pickle")
    all_props_data = s3_bucket_data("props.pickle")
    print(f"{len(all_odds_data)} odds and {len(all_props_data)} prop datas found on S3 bucket")
    
    # Player and prop datas to append into the database
    import_data_into_db(connection=conn, query=sql_query("odd"), query_type="odd", data=all_odds_data)
    import_data_into_db(connection=conn, query=sql_query("prop"), query_type="prop", data=all_props_data)
    
    # Close the database connection
    conn.close()
    print("Closed and disconnected the database connection")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Lambda function successfully ran with no errors')
    }
