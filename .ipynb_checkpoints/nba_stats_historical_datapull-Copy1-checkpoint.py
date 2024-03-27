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

# +
import pandas as pd
import numpy as np
import nba_api
import nba_api_functions as nba
from nba_api.stats.static import players
from nba_api.stats.static import teams
from nba_api.stats.endpoints import commonteamroster as rosters
from nba_api.stats.endpoints import playergamelog
from nba_api.stats.endpoints import playbyplayv2 as pbp
from nba_api.stats.endpoints import commonallplayers
from nba_api.stats.endpoints import teamgamelog
from nba_api.stats.endpoints import winprobabilitypbp as winprob
from nba_api.stats.endpoints import BoxScoreAdvancedV3 as box
from nba_api.stats.endpoints import playernextngames as next
import time

def pull_historical_data(season_year):
    # season_year='2023'
    file_path = '/Users/ryan/Desktop/python/nba_prop_historical/'
    # -
    
    
    start_time = time.time()
    formatted_start = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))
    
    # Returns list of all team_ids
    teams_df = pd.DataFrame(teams.get_teams())
    teams_id_list = teams_df['id'].tolist()
    
    
    # +
    # Returns a df with all team game logs. len should be total games played * 2 (one for each team)
    all_team_gamelogs = []
    for index, team_id in enumerate(teams_id_list):
        # time.sleep(.1)
        team_gamelogs_obj = teamgamelog.TeamGameLog(team_id)
        time.sleep(.05)
        team_gamelogs_df = pd.DataFrame(team_gamelogs_obj.get_data_frames()[0])
    
        all_team_gamelogs.append(team_gamelogs_df)
    
    all_team_gamelogs_df = pd.concat(all_team_gamelogs, ignore_index=True)
    
    all_team_gamelogs_df.dropna(inplace=True)
    all_team_gamelogs_df = all_team_gamelogs_df.reset_index(drop=True)
    
    
    # -
    
    # Returns a list of all game_ids for games in gamelog_df. len should be half len of gamelogs.
    all_gameids = all_team_gamelogs_df['Game_ID'].to_list()
    all_gameids_set = set(all_gameids)
    all_gameids = list(all_gameids_set)
    
    active_players_df = nba.get_active_players_list()
    player_id_list = active_players_df['id'].tolist()
    
    # +
    # Returns df with all players gamelogs
    all_players_gamelogs_list = []
    for index, player_id in enumerate(player_id_list):
        # print(index)
        time.sleep(.1)
        player_gamelog = playergamelog.PlayerGameLog(player_id)
        # print(index + .1)
        player_gamelog_df = player_gamelog.get_data_frames()[0]
        # print(index + .2)
        player_gamelog_filtered = player_gamelog_df[['SEASON_ID','Player_ID','Game_ID','MIN','PTS', 'REB', 'AST', 'STL', 'BLK', 'FG3M', 'WL']]
        # print(index + .3)
        all_players_gamelogs_list.append(player_gamelog_filtered)
        # print(index + .4)
    
    all_players_gamelogs_df = pd.concat(all_players_gamelogs_list, ignore_index=True)
    
    # -
    
    all_players_gamelogs_df.dropna(inplace=True)
    all_players_gamelogs_df = all_players_gamelogs_df.reset_index(drop=True)
    
    all_team_gamelogs_df['game_no'] = all_team_gamelogs_df['W'] + all_team_gamelogs_df['L']
    
    # creates df with game/team level data for all games
    team_gamelogs_filtered_df = all_team_gamelogs_df[[
        'Team_ID','Game_ID', 'game_no', 'MATCHUP','GAME_DATE', 'WL','PTS', 'REB', 'AST', 'STL', 'BLK', 'FGM','FGA','FG3M', 'FG3A', 'FTM','FTA','OREB','DREB','TOV'
    ]]
    team_match = team_gamelogs_filtered_df['MATCHUP'].tolist()
    match_loc = [match for match in team_match if 'vs.' in match]
    team_gamelogs_df = team_gamelogs_filtered_df.copy()
    team_gamelogs_df['IS_HOME'] = team_gamelogs_df['MATCHUP'].isin(match_loc).astype(int)
    team_gamelogs_df[['team_short', 'opp_short']] = team_gamelogs_df['MATCHUP'].str.split(' vs. | @ ', expand=True)
    
    
    # +
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
    # -
    
    
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
    
    # creates team game number df to merge with player df
    team_game_num_df = game_gamelogs_df[['team_id','game_id','team_game_no']]
    
    # +
    # creates player<>team df and adds data to player df
    all_players = commonallplayers.CommonAllPlayers().get_data_frames()[0]
    current_players = all_players[all_players['TO_YEAR'] == season_year]
    player_team_pair = current_players[['PERSON_ID','TEAM_ID']]
    
    players_df = all_players_gamelogs_df.copy()
    players_df = pd.merge(players_df, player_team_pair, left_on='Player_ID', right_on='PERSON_ID')
    players_df = players_df.drop(columns='PERSON_ID')
    # -
    
    # adds data from team_game_num_df
    players_df = pd.merge(players_df, team_game_num_df, left_on=['TEAM_ID','Game_ID'], right_on=['team_id','game_id'])
    players_df = players_df.drop(columns={'team_id','game_id'})
    
    # sorts player df by team, game, player
    players_df.sort_values(by=['TEAM_ID','team_game_no','Player_ID'], ascending=True, inplace=True)
    players_df_raw = players_df.reset_index(drop=True)
    
    
    # +
    one_player = players_df_raw.groupby('TEAM_ID')['Player_ID'].first()
    one_player_df = pd.DataFrame(one_player)
    team_player_id = one_player_df['Player_ID'].tolist()
    
    home_next_game_list = []
    away_next_game_list = []
    for player_id in team_player_id:
        time.sleep(.1)
        # print(player_id)
        next_game_obj = next.PlayerNextNGames(number_of_games = 1, player_id = int(player_id))
        next_game_df = pd.DataFrame(next_game_obj.get_data_frames()[0])
        home_next_game_dict = {
            'next_game_date':next_game_df.loc[0,'GAME_DATE'], 'next_game_opp':next_game_df.loc[0,'HOME_TEAM_ABBREVIATION'], 
            'team_id':next_game_df.loc[0,'VISITOR_TEAM_ID'], 'player_id':player_id
        }
        away_next_game_dict = {
            'next_game_date':next_game_df.loc[0,'GAME_DATE'], 'next_game_opp':next_game_df.loc[0,'VISITOR_TEAM_ABBREVIATION'], 
            'team_id':next_game_df.loc[0,'HOME_TEAM_ID'], 'player_id':player_id
        }
        home_next_game_list.append(home_next_game_dict)
        away_next_game_list.append(away_next_game_dict)
    
    home_next_game_df = pd.DataFrame(home_next_game_list)
    away_next_game_df = pd.DataFrame(away_next_game_list)
    
    
    
    # +
    next_game_df = pd.concat([home_next_game_df, away_next_game_df], axis=0) 
    
    next_game_df['next_game_date'] = pd.to_datetime(next_game_df['next_game_date'], format='%b %d, %Y')
    next_game_df['next_game_date'] = next_game_df['next_game_date'].dt.strftime('%b %d, %Y')
    next_game_df.sort_values(by='team_id', inplace=True)
    next_game_df.drop_duplicates(subset='team_id',keep='first', inplace=True)
    next_game_df.reset_index(drop=True, inplace=True)
    
    players_df_raw = pd.merge(players_df_raw, next_game_df, left_on='TEAM_ID', right_on='team_id')
    
    # +
    # ********** something here is creating duplicates
    # exports a raw file of players_df before adjustments in case need to use without rerunning
    # players_df_raw.to_csv(f'{file_path}player_gamelogs_2023_raw.csv', index=False)
    
    # +
    # players_df_raw = pd.read_csv(f'{file_path}player_gamelogs_2023_raw.csv', index_col=None)
    # cleans up players gamelogs ready for export
    players_df = nba.player_gamelog_cleanup(players_df_raw, game_gamelogs_df, active_players_df)
    
    # sorts game_gamelogs_df ready to export
    game_gamelogs_df.sort_values(by=['game_date','game_id'], ascending=True, inplace=True)
    game_gamelogs_df.dropna(inplace=True)
    game_gamelogs_df = game_gamelogs_df.reset_index(drop=True)
    
    # +
    # player_test = players_df.sort_values(by='next_game_date')
    # next_list = players_df['next_game_date'].unique().tolist()
    # # players_df_raw = players_df_raw.sort_values(by='next_game_date')
    # # players_df_raw
    # next_list
    # -
    
    # adding past next game data and dropping duplicates
    players_df = nba.add_past_next_games(players_df)
    
    
    # exports final copies
    players_df.to_csv(f'{file_path}player_gamelogs_2023.csv', index=False)
    game_gamelogs_df.to_csv(f'{file_path}team_gamelogs_2023.csv', index=False)
    
    
    end_time = time.time()
    duration = round((end_time - start_time)/60, 2)
    formatted_end = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))
    # print(formatted_start)
    # print(formatted_end)
    # print(str(duration) + ' minutes')
    completion_message =  f'{season_year} pull complete. Pulled {str(len(game_gamelogs_df))}
    games with {str(len(players_df))} individual player gamelogs. Process took {str(duration)}
    minutes.'
    return completion_message
    
