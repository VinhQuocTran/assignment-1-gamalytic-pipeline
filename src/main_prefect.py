from bronze_function import *
from silver_function import *
from prefect import task, flow
from prefect.tasks import task_input_hash
from datetime import timedelta

AWS_ACCESS_KEY_ID = "AKIAYPRKYK336NVWQY25"
AWS_SECRET_ACCESS_KEY = "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"

# *************** Prefect Task ***************
@task(name="Get raw games data from Gamalytic API")
def get_game_data(from_page,to_page,game_list_params,game_filter_params):
    games = get_game_list(from_page,to_page,game_list_params,game_filter_params)
    return games

@task(name="Upload data to S3",
      task_run_name="Upload {layer} data to S3")
def upload_to_s3(data, filename, bucket_name, metadata,aws_access_key_id,aws_secret_access_key,layer):
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
def transform_to_silver(games):
    df_silver = silver_transformation(games)
    return df_silver


@task(name="Insert silver data to Postgres")
def upsert_data(df_silver,db_config:dict):
    conn = connect_to_postgres(**db_config)
    create_steam_game_table(conn)
    insert_data_to_postgres(conn, df_silver)


# *************** Prefect Flow ***************

@flow(name="Gamalytic Game Data Pipeline")
def game_data_pipeline(from_page, to_page,game_list_params,game_filter_params):
    # Task 1
    raw_data=get_game_data(from_page,to_page,game_list_params,game_filter_params)
    
    # Task 2
    raw_filename = create_filename_raw()
    metadata = create_metadata(from_page, to_page, game_list_params, game_filter_params)
    upload_to_s3(raw_data, raw_filename, 'test-bucket-alextran', metadata,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,layer="bronze")


    # Task 3
    df_silver = transform_to_silver(raw_data)

    # Task 4
    silver_filename = create_filename_silver()
    upload_to_s3(df_silver, silver_filename, 'test-bucket-alextran', metadata,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,layer="silver")

    # Task 5
    db_config={
        "host":"localhost",
        "dbname":"postgres",
        "user":"postgres",
        "password":"6102002a",
        "port":5440
    }
    upsert_data(df_silver,db_config)




# --- Running the Flow ---
def main():
    from_page=0
    to_page=1

    # Leave dict empty if you dont want to pass any params
    # API Documentation: https://gamalytic.com/api-reference.txt
    # List of accepted value for parameters:  https://gamalytic.com/game-list
    game_list_params={
        "limit": 100, 
        "sort_mode": "desc"
    }
    game_filter_params={
        "genres":"Action,RPG",
        "tags":"2D,2D Platformer"
    }
    game_data_pipeline(from_page,to_page,game_list_params,game_filter_params)

main()