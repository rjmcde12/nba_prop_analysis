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
import nba_api
from nba_api.stats.static import players
from nba_api.stats.static import teams
from nba_api.stats.endpoints import commonteamroster as rosters
from nba_api.stats.endpoints import playergamelog
from nba_api.stats.endpoints import playbyplayv2 as pbp
from nba_api.stats.endpoints import commonallplayers
from nba_api.stats.endpoints import teamgamelog
from nba_api.stats.endpoints import winprobabilitypbp as winprob
from nba_api.stats.endpoints import BoxScoreAdvancedV3 as box
import time
from datetime import datetime

# +
# CommonAllPlayers - TO_YEAR is the best measure. means they have at some point played in a game this year.
# players.get_active_players - this is players currently on an active roster. 
    # can show up on this list and not the above (based on this year TO YEAR) if on roster but have not appeared in a game yet


# -

current_rosters_df = rosters.CommonTeamRoster(1610612749, 2018).get_data_frames()[0]
# current_rosters_df = pd.DataFrame(current_rosters_obj.get_data_frames()[0])
# current_rosters_df

# returns full list of all players who played in the nba
# ROSTERSTATUS = 1 seems to be is_active
all_players = commonallplayers.CommonAllPlayers().get_data_frames()[0]
all_players['TO_YEAR'] = all_players['TO_YEAR'].apply(lambda x: int(x))
all_players['FROM_YEAR'] = all_players['FROM_YEAR'].apply(lambda x: int(x))
hist_players = all_players[all_players['TO_YEAR']>= 2016]

hist_players = hist_players[['PERSON_ID', 'DISPLAY_FIRST_LAST', 'FROM_YEAR', 'TO_YEAR']]
hist_players.loc[:, 'total_seasons'] = hist_players['TO_YEAR'] - hist_players['FROM_YEAR'] + 1
hist_players.loc[:, 'total_seasons'] = hist_players['total_seasons'].clip(upper=7)

# +
hist_players.loc[:,'seasons_played'] = hist_players['total_seasons'].apply(lambda x: [year for season in range(2023-x, 2023) for year in [season]])

id_season_df = hist_players[['PERSON_ID','seasons_played']]
# -

id_season_pair_list = []
for index, row in id_season_df.iterrows():
    player_id = row['PERSON_ID']
    seasons_played = row['seasons_played']
    player_entry = {'PERSON_ID': player_id, 'seasons_played': seasons_played}
    id_season_pair_list.append(player_entry)

# +
id_season_pairs = []

for pair in id_season_pair_list:
    player_id = pair['PERSON_ID']
    player_pairs = [{'player_id': player_id, 'season_year': season} for season in pair['seasons_played']]
    id_season_pairs.extend(player_pairs)

all_ids_all_years = pd.DataFrame(id_season_pairs)

# +
games_2016 = all_ids_all_years[all_ids_all_years['season_year']==2016]
games_2017 = all_ids_all_years[all_ids_all_years['season_year']==2017]
games_2018 = all_ids_all_years[all_ids_all_years['season_year']==2018]
games_2019 = all_ids_all_years[all_ids_all_years['season_year']==2019]
games_2020 = all_ids_all_years[all_ids_all_years['season_year']==2020]
games_2021 = all_ids_all_years[all_ids_all_years['season_year']==2021]
games_2022 = all_ids_all_years[all_ids_all_years['season_year']==2022]

df_list = [games_2020, games_2022]

# +
for df in df_list:
    gamelog_df_list = []
    start_time = time.time()
    formatted_start = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))
    for index, row in df.iterrows():
        player_id = row['player_id'] 
        season = row['season_year']
        try:
            player_gamelog = playergamelog.PlayerGameLog(player_id, season)
            player_gamelog_df = player_gamelog.get_data_frames()[0]
            print(f'\rIndex: {player_id, season}', end='', flush=True)
            gamelog_df_list.append(player_gamelog_df)
            time.sleep(.1)
        except:
            continue
    
    player_gamelogs_df = pd.concat(gamelog_df_list, ignore_index=True)
    player_gamelogs_df.to_csv(f'player_gamelogs_{str(season)}.csv', index=False)
    
    end_time = time.time()
    duration = round((end_time - start_time)/60, 2)
    formatted_end = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))
    print(formatted_start)
    print(formatted_end)
    print(str(duration) + ' minutes')
    print(f'{str(season)} is complete. {str(len(player_gamelogs_df))} total player gamelogs.')
    

# -

player_gamelogs_df = pd.concat(gamelog_df_list, ignore_index=True)


