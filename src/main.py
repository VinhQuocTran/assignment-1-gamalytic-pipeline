from bronze_function import *
from silver_function import *
# from prefect import task, flow
# from prefect.tasks import task_input_hash

AWS_ACCESS_KEY_ID = "AKIAYPRKYK336NVWQY25"
AWS_SECRET_ACCESS_KEY = "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"

def main():
    from_page=0
    to_page=5

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


    games=get_game_list(from_page,to_page,game_list_params,game_filter_params)
    print("Step 1: Get data from Gamalytic API is done")

    ##### Upload raw data to S3
    s3_client = boto3.client('s3',
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    bucket_name = 'test-bucket-alextran'
    raw_filename=create_filename_raw()
    metadata = create_metadata(from_page, to_page, game_list_params, game_filter_params)
    games_json = games.to_json(orient='records')


    request=s3_client.put_object(Bucket=bucket_name, 
                        Key=raw_filename, 
                        Body=games_json,
                        Metadata=metadata)

    print("Step 2:Upload raw data to s3 successfully")

    ##### Transform raw data to silver data
    df_silver=silver_transformation(games)
    print("Step 3: Transform raw data to silver data successfully")

    ##### Upload silver data to S3
    bucket_name = 'test-bucket-alextran'
    silver_filename=create_filename_silver()
    metadata = create_metadata(from_page, to_page, game_list_params, game_filter_params)
    games_json = df_silver.to_json(orient='records', lines=True)


    request=s3_client.put_object(Bucket=bucket_name, 
                        Key=silver_filename, 
                        Body=games_json,
                        Metadata=metadata)

    print("Step 4: Upload silver data to s3 successfully")


main()
