 
from .bronze_function import *
from .silver_function import *
import os
from prefect import task, flow
from prefect.tasks import task_input_hash
from datetime import timedelta

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')

# *************** Prefect Task ***************
@task(name="Get raw games data from Gamalytic API")
def task_get_games_from_api(from_page,to_page,game_list_params,game_filter_params):
    games = get_game_list(from_page,to_page,game_list_params,game_filter_params)
    return games

@task(name="Upload data to S3",
      task_run_name="Upload {layer} data to S3")
def task_upload_to_s3(data, filename, bucket_name, metadata,aws_access_key_id,aws_secret_access_key,layer):
    s3_client = boto3.client('s3',
                            aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key)

    data_json = data.to_json(orient='records')

    s3_client.put_object(
        Bucket=bucket_name,
        Key=filename,
        Body=data_json,
        Metadata=metadata
    )


@task(name="Transform raw data to silver") 
def task_transform_to_silver(games):
    df_silver = silver_transformation(games)
    return df_silver


@task(name="Insert silver data to Postgres")
def task_upsert_data(df_silver,db_config:dict):
    conn = connect_to_postgres(**db_config)
    create_steam_game_table(conn)
    insert_data_to_postgres(conn, df_silver)


# *************** Prefect Flow ***************

@flow(name="Gamalytic Game Data Pipeline")
def gamalytic_etl(from_page, to_page,game_list_params,game_filter_params):
    # Read params from env
    db_config = {
        "host": os.environ.get('host'),
        "dbname": os.environ.get('dbname'),
        "user": os.environ.get('user'),
        "password": os.environ.get('password'),
        "port": int(os.environ.get('port'))
    }
    bucket_name = os.environ.get('bucket_name')

    # Task 1
    raw_data=task_get_games_from_api(from_page,to_page,game_list_params,game_filter_params)
    
    # Task 2
    raw_filename = create_filename_raw()
    metadata = create_metadata(from_page, to_page, game_list_params, game_filter_params)
    task_upload_to_s3(raw_data, raw_filename, bucket_name, metadata,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,layer="bronze")


    # Task 3
    df_silver = task_transform_to_silver(raw_data)

    # Task 4
    silver_filename = create_filename_silver()
    task_upload_to_s3(df_silver, silver_filename, bucket_name, metadata,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,layer="silver")

    # Task 5
    task_upsert_data(df_silver,db_config)




# --- Running the Flow ---
def lambda_handler(event, context):
    # API Documentation: https://gamalytic.com/api-reference.txt
    # List of accepted value for parameters:  https://gamalytic.com/game-list

    # Default value for params
    # Leave dict empty if you dont want to pass any params
    from_page=0
    to_page=2

    game_list_params={
        "limit": 100, 
        "sort_mode": "desc"
    }
    game_filter_params={
        "genres":"Action,RPG",
        "tags":"2D,2D Platformer"
    }

    # Get params from lambda event
    from_page = event.get('from_page', from_page)  
    to_page = event.get('to_page', to_page) 
    game_list_params = event.get('game_list_params', game_list_params) 
    game_filter_params = event.get('game_filter_params', game_filter_params)

    print("********* List of params *********")
    print("from_page:",from_page)
    print("to_page:",to_page)
    print("game_list_params:",game_list_params)
    print("game_filter_params:",game_filter_params)

    try:
        gamalytic_etl(from_page,to_page,game_list_params,game_filter_params)
        return {
            'statusCode': 200,
            'body': 'Get data from Gamalytic API successfully!'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error processing Gamalytic ETL: {str(e)}'
        }