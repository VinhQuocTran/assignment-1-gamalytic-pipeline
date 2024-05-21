import requests 
import pandas as pd
import numpy as np
import json
import boto3
from datetime import datetime
import urllib3
import itertools


def get_game_list(from_page,to_page,game_list_params,game_filter_params):
    base_url = "https://api.gamalytic.com/steam-games/list"
    list_game_json=[]
    print_flag=0

    for page_idx in range(from_page,to_page+1):
        game_list_params['page']=page_idx
        api_params={**game_list_params, **game_filter_params}
        
        response = requests.get(base_url, params=api_params)
        game_json = response.json()
        
        if("next" not in game_json or page_idx>to_page or response.status_code!=200):
            break

        if(print_flag==0):
            print("Total games found:",game_json['total'])
            print("Total pages found:",game_json['pages'],'\n\n')
            print_flag=1

        list_game_json.append(game_json['result'])
        print(f"Get page {page_idx} successfully")
        print(f"Request URL:",response.request.url,"\n")

    flatten_games=list(itertools.chain(*list_game_json))
    df_game=pd.DataFrame(flatten_games)
    print("Get data from Gamalytic API is done")
    return df_game


def create_metadata(from_page, to_page, game_list_params, game_filter_params):
    metadata = {
        'from_page': str(from_page),
        'to_page': str(to_page),
        'game_list_params': "_".join([f"{k}={v}" for k, v in game_list_params.items()]),
        'game_filter_params': "_".join([f"{k}={v}" for k, v in game_filter_params.items()])
      }

    return metadata

def create_filename_raw():
    current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")  # Include seconds for better uniqueness
    return f"gamalytic_api_{current_datetime}.json"