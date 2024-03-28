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
# Returns list of all team_ids
teams_df = pd.DataFrame(teams.get_teams())
teams_id_list = teams_df['id'].tolist()

seasons_list = [2016,2017,2018,2019,2020,2021,2022]
# -

start_time = time.time()
formatted_start = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))
print(formatted_start)

# +
all_team_gamelogs = []
for team_id in teams_id_list:
    for season in seasons_list:
        team_gamelogs_obj = teamgamelog.TeamGameLog(team_id, season)
        time.sleep(.05)
        team_gamelogs_df = pd.DataFrame(team_gamelogs_obj.get_data_frames()[0])
        all_team_gamelogs.append(team_gamelogs_df)
        print(f'\rExtracted team_id:{str(team_id)} season:{str(season)}.', end='', flush=True)

hist_team_gamelogs_df = pd.concat(all_team_gamelogs, ignore_index=True)
hist_team_gamelogs_df.dropna(inplace=True)
hist_team_gamelogs_df = hist_team_gamelogs_df.reset_index(drop=True)
# -

end_time = time.time()
duration = round((end_time - start_time)/60, 2)
formatted_end = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))
print(formatted_start)
print(formatted_end)
print(str(duration) + ' minutes')

# +
hist_team_gamelogs_df['game_no'] = hist_team_gamelogs_df['W'] + hist_team_gamelogs_df['L']

# creates df with game/team level data for all games
team_gamelogs_filtered_df = hist_team_gamelogs_df[[
    'Team_ID','Game_ID', 'game_no', 'MATCHUP','GAME_DATE', 'WL','PTS', 'REB', 'AST', 'STL', 'BLK', 'FGM','FGA','FG3M', 'FG3A', 'FTM','FTA','OREB','DREB','TOV'
]]
team_match = team_gamelogs_filtered_df['MATCHUP'].tolist()
match_loc = [match for match in team_match if 'vs.' in match]
team_gamelogs_df = team_gamelogs_filtered_df.copy()
team_gamelogs_df['IS_HOME'] = team_gamelogs_df['MATCHUP'].isin(match_loc).astype(int)
team_gamelogs_df[['team_short', 'opp_short']] = team_gamelogs_df['MATCHUP'].str.split(' vs. | @ ', expand=True)

# splits them as home and away to rename the columns then merge back
home_gamelogs_df = team_gamelogs_df[team_gamelogs_df['IS_HOME'] == 1]
home_gamelogs_df = home_gamelogs_df.copy()
home_gamelogs_df.rename(columns={'Team_ID':'team_id','Game_ID':'game_id', 'MATCHUP':'matchup','GAME_DATE':'game_date', 
                                 'WL':'team_wl','PTS':'team_pts', 'REB':'team_reb', 'AST':'team_ast', 'STL':'team_stl', 
                                 'BLK':'team_blk', 'FGM':'team_fgm','FGA':'team_fga','FG3M':'team_fg3m', 'FG3A':'team_fg3a', 
                                 'FTM':'team_ftm','FTA':'team_fta','OREB':'team_oreb',
                                 'DREB':'team_dreb','TOV':'team_tov', 'IS_HOME':'team_home', 'game_no':'team_game_no'}, inplace=True)
away_gamelogs_df = team_gamelogs_df[team_gamelogs_df['IS_HOME'] == 0]
away_gamelogs_df = away_gamelogs_df.copy()
away_gamelogs_df.rename(columns={'Team_ID':'opp_id','Game_ID':'game_id', 'MATCHUP':'matchup','GAME_DATE':'game_date', 
                                 'WL':'opp_wl','PTS':'opp_pts', 'REB':'opp_reb', 'AST':'opp_ast', 'STL':'opp_stl', 
                                 'BLK':'opp_blk', 'FGM':'opp_fgm','FGA':'opp_fga','FG3M':'opp_fg3m', 'FG3A':'opp_fg3a', 
                                 'FTM':'opp_ftm','FTA':'opp_fta','OREB':'opp_oreb',
                                 'DREB':'opp_dreb','TOV':'opp_tov', 'IS_HOME':'opp_home', 'game_no':'opp_game_no'}, inplace=True)
away_gamelogs_df.drop(columns=['matchup','team_short','opp_short', 'game_date'], inplace=True)

game_gamelogs_df = pd.merge(home_gamelogs_df, away_gamelogs_df, on='game_id')

# creates team_poss stat for games
game_gamelogs_df['team_poss'] = 0.5 * (
        (game_gamelogs_df['team_fga'] + 0.4 * game_gamelogs_df['team_fta'] - 1.07 * (game_gamelogs_df['team_oreb'] / (game_gamelogs_df['team_oreb'] + game_gamelogs_df['opp_dreb'])) * (game_gamelogs_df['team_fga'] - game_gamelogs_df['team_fgm']) + game_gamelogs_df['team_tov']) +
        (game_gamelogs_df['opp_fga'] + 0.4 * game_gamelogs_df['opp_fta'] - 1.07 * (game_gamelogs_df['opp_oreb'] / (game_gamelogs_df['opp_oreb'] + game_gamelogs_df['team_dreb'])) * (game_gamelogs_df['opp_fga'] - game_gamelogs_df['opp_fgm']) + game_gamelogs_df['opp_tov'])
    )
game_gamelogs_df['opp_poss'] = 0.5 * (
        (game_gamelogs_df['opp_fga'] + 0.4 * game_gamelogs_df['opp_fta'] - 1.07 * (game_gamelogs_df['opp_oreb'] / (game_gamelogs_df['opp_oreb'] + game_gamelogs_df['team_dreb'])) * (game_gamelogs_df['opp_fga'] - game_gamelogs_df['opp_fgm']) + game_gamelogs_df['opp_tov']) +
        (game_gamelogs_df['team_fga'] + 0.4 * game_gamelogs_df['team_fta'] - 1.07 * (game_gamelogs_df['team_oreb'] / (game_gamelogs_df['team_oreb'] + game_gamelogs_df['opp_dreb'])) * (game_gamelogs_df['team_fga'] - game_gamelogs_df['team_fgm']) + game_gamelogs_df['team_tov'])
    )
    
home_game_gamelogs_df = game_gamelogs_df.copy()
away_game_gamelogs_df = game_gamelogs_df.copy()

# renames away games df so the columns will match and can be merged
away_game_gamelogs_df.rename(columns={'team_id':'opp_id','team_wl':'opp_wl', 'team_pts':'opp_pts','team_reb':'opp_reb', 
                                      'team_ast':'opp_ast', 'team_stl':'opp_stl', 'team_blk':'opp_blk', 'team_fgm':'opp_fgm', 
                                      'team_fga':'opp_fga','team_fg3m':'opp_fg3m','team_fg3a':'opp_fg3a','team_ftm':'opp_ftm',
                                      'team_fta':'opp_fta', 'team_oreb':'opp_oreb', 'team_dreb':'opp_dreb', 
                                      'team_tov':'opp_tov', 'team_home':'opp_home','team_short':'opp_short', 'opp_short':'team_short',
                                      'opp_id':'team_id', 'opp_wl':'team_wl', 'opp_pts':'team_pts', 'opp_reb':'team_reb',
                                      'opp_ast':'team_ast', 'opp_stl':'team_stl', 'opp_blk':'team_blk', 'opp_fgm':'team_fgm', 
                                      'opp_fga':'team_fga', 'opp_fg3m':'team_fg3m','opp_fg3a':'team_fg3a','opp_ftm':'team_ftm',
                                      'opp_fta':'team_fta','opp_oreb':'team_oreb', 'opp_dreb':'team_dreb', 
                                      'opp_tov':'team_tov', 'opp_home':'team_home', 'team_poss':'opp_poss', 'opp_poss':'team_poss',
                                      'team_game_no':'opp_game_no', 'opp_game_no':'team_game_no'}, inplace=True)
                                      
# merges home and away dfs to a single df
game_gamelogs_df = game_gamelogs_df.copy()
game_gamelogs_df = pd.concat([home_game_gamelogs_df, away_game_gamelogs_df], axis=0)

# sorts game_gamelogs_df ready to export
game_gamelogs_df['game_date'] = pd.to_datetime(game_gamelogs_df['game_date'], format='%b %d, %Y')
game_gamelogs_df.sort_values(by=['game_date','game_id'], ascending=True, inplace=True)
game_gamelogs_df.dropna(inplace=True)
game_gamelogs_df = game_gamelogs_df.reset_index(drop=True)

game_gamelogs_df.to_csv('hist_team_gamelogs.csv', index=False)
