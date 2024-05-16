from datetime import datetime
import numpy as np
import pandas as pd
from psycopg2 import connect, extras
import psycopg2
import json

def silver_transformation(df_games):
    df_games=df_games.copy()
    
    # Rename 
    df_games=df_games.rename(columns={'steamId': 'steam_id'})
    
    # Add and remove columns
    df_games = df_games.drop('id', axis=1)
    df_games['ingestion_timestamp'] = datetime.now()
    df_games['is_free'] = (df_games['price'] == 0).astype(int)
    df_games['releaseDate'] = pd.to_datetime(df_games['releaseDate']/1000, unit='s', errors='coerce')
    df_games['EAReleaseDate'] = pd.to_datetime(df_games['EAReleaseDate']/1000, unit='s', errors='coerce')
    
    # Standarize data type for insert json postgres
    df_games['releaseDate'] = df_games['releaseDate'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_games['EAReleaseDate'] = df_games['EAReleaseDate'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_games['ingestion_timestamp'] = df_games['ingestion_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_games['avgPlaytime'] = df_games['avgPlaytime'].round().astype(int)
    df_games = df_games.replace({np.nan: None})
    
    # reviewScoreRange
    bins = [0, 20, 40, 60, 80, 100]
    labels = ['0-20', '20-40', '40-60', '60-80', '80-100']
    df_games['reviewScoreRange'] = pd.cut(df_games['reviewScore'], bins=bins, labels=labels, include_lowest=True)
    
    return df_games

def create_filename_silver():
    current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")  # Include seconds for better uniqueness
    return f"gamalytic_api_{current_datetime}_silver.json"


def connect_to_postgres(host,dbname,user,password,port):
    try:
      # Connect to the database
        conn = psycopg2.connect(
          dbname=dbname, 
          user=user, 
          password=password, 
          host=host, 
          port=port)

        # Create a cursor object
        cur = conn.cursor()
        print(f"Connected to Postgres succesfully!\n")
        return conn

    except Exception as e:
        print(f"Error connecting to Postgres: {e}")

def create_steam_game_table(conn):
    try:
        table_ddl_sql="""
            CREATE TABLE IF NOT EXISTS game_data (
            steam_id INT PRIMARY KEY,
            game_info JSONB NOT NULL 
            );
        """
        cursor = conn.cursor()
        cursor.execute(table_ddl_sql)
        conn.commit()
    except Exception as e:
        print(f"Error creating table: {e}")

def insert_data_to_postgres(conn,df_silver):
    cursor = conn.cursor()
    batch_insert_sql_query = """
        INSERT INTO game_data (steam_id, game_info)
        VALUES (%s, %s)
        ON CONFLICT (steam_id) DO UPDATE SET game_info = excluded.game_info;
    """

    try:
        list_game_info = df_silver.drop(['steam_id'], axis=1).to_dict('records')
        # Execute upsert in batches (adjust page_size as needed)
        extras.execute_batch(
            cursor, 
            batch_insert_sql_query, 
            [(row['steam_id'], json.dumps(game_info)) for row, game_info in zip(df_silver.to_dict('records'), list_game_info)], page_size=100)

        conn.commit()
        conn.close()
        print("Batch insert executed successfully")
    except Exception as e:
        print(f"Error inserting data: {e}")