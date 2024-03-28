# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.16.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

import pandas as pd
import numpy as np
import pymongo                                                  
from dotenv import load_dotenv
import os
import time
from datetime import datetime
import nba_api_functions_hist as hist
import nba_prop_functions_hist as prop_hist
import random
import uuid


# +
def fetch_data_from_database():
    # connecting to mongodb
    load_dotenv()
    uri = os.getenv('URI')
    client = pymongo.MongoClient(uri)
    db = client['nba_stats']
    
    # creating pandas df for historical players logs
    hist_player_stats = db['team_gamelogs']
    player_cursor = hist_player_stats.find()
    hist_player_stats_df = pd.DataFrame(list(player_cursor))
    hist_player_stats_df = hist_player_stats_df.drop(columns='_id')

    return hist_player_stats_df

hist_df = fetch_data_from_database()
# -

df_2016 = pd.read_csv('player_gamelogs_2016.csv',index_col=None)
df_2017 = pd.read_csv('player_gamelogs_2017.csv',index_col=None)
df_2018 = pd.read_csv('player_gamelogs_2018.csv',index_col=None)
df_2019 = pd.read_csv('player_gamelogs_2019.csv',index_col=None)
df_2020 = pd.read_csv('player_gamelogs_2020.csv',index_col=None)
df_2021 = pd.read_csv('player_gamelogs_2021.csv',index_col=None)
df_2022 = pd.read_csv('player_gamelogs_2022.csv',index_col=None)

hist_df = pd.concat([df_2016,df_2017,df_2018,df_2019,df_2020,df_2021,df_2022], axis=0)

hist_df.reset_index(drop=True, inplace=True)
hist_df

# +
# filtering out only what we need and changing the names to match
hist_df = hist_df[['SEASON_ID','Game_ID','GAME_DATE','Player_ID','MIN','PTS','REB','AST','STL','BLK','FG3M']]
hist_df.rename(columns={'SEASON_ID':'season_id','Game_ID':'game_id','GAME_DATE':'game_date','Player_ID':'player_id', 'MIN':'min', 
                        'PTS':'pts','REB':'reb','AST':'ast','STL':'stl','BLK':'blk', 'FG3M':'fg3m'}, inplace=True)

# updating game_date to something sortable
# ******* not sure if we need to flip it back at somepoint
hist_df['game_date'] = pd.to_datetime(hist_df['game_date'], format='%b %d, %Y')
# # ***** returns it back to a more readable format ********
# hist_df['game_date'] = hist_df['game_date'].dt.strftime('%b %d, %Y')

# sorting by playerid, seasonid, gamedate
hist_df = hist_df.sort_values(by=['player_id', 'season_id', 'game_date'], ascending=True).reset_index(drop=True)

# grouping by playerid and seasonid to add player_game_no
hist_df['player_game_no'] = hist_df.groupby(['season_id', 'player_id']).cumcount() + 1

# adding unique id to each row
hist_df['unique_id'] = hist_df.index.map(lambda _:uuid.uuid4())
# -

# ******** creating season rolling averages ********
hist_df['min_season_avg'] = hist_df.groupby(['season_id', 'player_id'])['min'].transform(lambda x: x.expanding().mean())
hist_df['pts_season_avg'] = hist_df.groupby(['season_id', 'player_id'])['pts'].transform(lambda x: x.expanding().mean())
hist_df['reb_season_avg'] = hist_df.groupby(['season_id', 'player_id'])['reb'].transform(lambda x: x.expanding().mean())
hist_df['ast_season_avg'] = hist_df.groupby(['season_id', 'player_id'])['ast'].transform(lambda x: x.expanding().mean())
hist_df['stl_season_avg'] = hist_df.groupby(['season_id', 'player_id'])['stl'].transform(lambda x: x.expanding().mean())
hist_df['blk_season_avg'] = hist_df.groupby(['season_id', 'player_id'])['blk'].transform(lambda x: x.expanding().mean())
hist_df['fg3m_season_avg'] = hist_df.groupby(['season_id', 'player_id'])['fg3m'].transform(lambda x: x.expanding().mean())


# +
avg_cols = ['min_season_avg','pts_season_avg','reb_season_avg','ast_season_avg','stl_season_avg','blk_season_avg','fg3m_season_avg']

hist_df[avg_cols] = hist_df[avg_cols].round(2)

hist_df.insert(0, 'unique_id', hist_df.pop('unique_id'))
hist_df
# -

memory_usage = hist_df.memory_usage(deep=True).sum()
memory_usage = memory_usage / (1024**2)
memory_usage

hist_df.to_csv('hist_nba_player_gamelogs_all.csv',index=False)
