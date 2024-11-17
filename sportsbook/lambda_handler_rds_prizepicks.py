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
    prizepicks_player_query = """
        INSERT INTO prizepicks_player (player_id, player_name, player_pos, player_team_abbr, player_team, player_combo, player_league)
        VALUES %s
        ON CONFLICT (player_id) DO NOTHING;
    """
    prizepicks_prop_query = """
        INSERT INTO prizepicks_prop (prop_id, player_id, game_date, game_time, prop_desc, prop_type, prop_combo, prop_name, prop_league, player_pos, player_team, opponent_team, prop_line, prop_adjusted, prop_rank, board_date)
        VALUES %s
        ON CONFLICT (prop_id) DO NOTHING;
    """
    if query_name == "player":
        return prizepicks_player_query
    return prizepicks_prop_query

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
    bucket_name = 'prizepicks-bucket'

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
    all_players_data = s3_bucket_data("players.pickle")
    all_props_data = s3_bucket_data("props.pickle")
    print(f"{len(all_players_data)} players and {len(all_props_data)} prop datas found on S3 bucket")
    
    # Player and prop datas to append into the database
    import_data_into_db(connection=conn, query=sql_query("player"), query_type="player", data=all_players_data)
    import_data_into_db(connection=conn, query=sql_query("prop"), query_type="prop", data=all_props_data)
    
    # Close the database connection
    conn.close()
    print("Closed and disconnected the database connection")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Lambda function successfully ran with no errors')
    }
