# Databricks notebook source

chess_com_endpoints={
'username':f'https://api.chess.com/pub/titled/',
'userprofile':f'https://api.chess.com/pub/player/',
'streamers':f'https://api.chess.com/pub/streamers',
'leaderboard':f'https://api.chess.com/pub/leaderboards'
}

# COMMAND ----------

import requests
def getData(url,endpoint,param=None):
    headers = {
        "User-Agent": "ChessAnalytics/1.0",
    }
    if endpoint == 'username' or endpoint == 'userprofile' :
        url=url+param
    elif endpoint == 'userstat':
        url=url+param+'/stats'
    try:
        response = requests.get(url, headers=headers)
        data = response.json()
    except requests.exceptions.RequestException as e:
        data = {"error": str(e)}
    return data

# COMMAND ----------

import json
def json_to_str(json_arr):
    data=[]
    for ele in json_arr:
        try:
            json_str=json.dumps(ele)
            data=data.append(json_str)
        except:
            print("Json is not imported in current session")
    return data

