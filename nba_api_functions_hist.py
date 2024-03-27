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

if __name__ == '__main__':
    print('this is the main block of code')


# -

def get_active_players_list():
    active_players_obj = players.get_active_players()
    active_players_df = pd.DataFrame(active_players_obj)
    active_players_df = active_players_df.iloc[:, :2]
    return active_players_df


def get_player_id(first_name, last_name):
    active_players = players.get_active_players()
    players_df = pd.DataFrame(active_players)
    player_df = players_df[players_df['first_name'] == first_name]
    player_df = player_df[player_df['last_name'] == last_name]
    player_id = player_df['id'].values[0]
    return player_id


def get_player_team_short(player_id):
    all_players = commonallplayers.CommonAllPlayers().get_data_frames()[0]
    current_players = all_players[all_players['TO_YEAR'] == '2023']
    player = current_players[current_players['PERSON_ID'] == int(player_id)]
    player_team = player['TEAM_ABBREVIATION'].values[0]
    return player_team


def get_team_id(team_short):
    teams_list = teams.get_teams()
    teams_df = pd.DataFrame(teams_list)
    team_df = teams_df[teams_df['abbreviation'] == team_short]
    team_id = team_df['id'].values[0]
    return team_id


# not really needed, thought it was gonna be used for home/away but didnt end up being
def get_team_gamelogs(team_id, season=None):
    if season is None:
        teams_gamelogs = teamgamelog.TeamGameLog(team_id)
    else:
        teams_gamelogs = teamgamelog.TeamGameLog(team_id, season)
    team_gamelogs = teams_gamelogs.get_data_frames()[0]
    # adding home/away column
    team_match = team_gamelogs['MATCHUP'].tolist()
    match_loc = [match for match in team_match if 'vs.' in match]
    team_gamelogs['IS_HOME'] = team_gamelogs['MATCHUP'].isin(match_loc).astype(int)
    return team_gamelogs


def team_is_home(game_id, team_id):
    winprob_obj = winprob.WinProbabilityPBP(str(game_id))
    winprob_df = winprob_obj.get_data_frames()[1]
    is_home_df = winprob_df[winprob_df['HOME_TEAM_ID'] == team_id]
    is_home = len(is_home_df)
    return is_home


def get_player_boxscore_filtered(game_id):
    box_score_obj = box(str(game_id))
    box_score = box_score_obj.get_data_frames()[0] 
    box_score_filtered = box_score[[
    'gameId', 'teamId', 'teamTricode', 'personId', 'firstName', 
    'familyName', 'minutes','offensiveRating','usagePercentage','PIE'
    ]]
    box_score_filtered = box_score_filtered[box_score_filtered['minutes'] != '']
    return box_score_filtered


def get_player_gamelogs(player_id, season=None):
    if season is None:
        player_gamelog = playergamelog.PlayerGameLog(player_id)
    else:
        player_gamelog = playergamelog.PlayerGameLog(player_id, season)
    player_gamelog_df = player_gamelog.get_data_frames()[0]
    return player_gamelog_df


# players that changed teams are getting screwed up when combining game and player data
def player_gamelog_cleanup(df, game_df, active_players_df):
    game_matchup = game_df.loc[:, [
        'game_date','game_id','team_id','opp_id', 'team_short','opp_short', 
        'team_home', 'team_wl', 'team_pts', 'opp_pts'
    ]]
    game_matchup['game_id'] = game_matchup['game_id'].astype('int64')
    df['Game_ID'] = df['Game_ID'].astype('int64')
    all_players_logs_df = pd.merge(df, game_matchup, left_on=['Game_ID','TEAM_ID'], right_on=['game_id','team_id'])
    all_players_logs_df = pd.merge(all_players_logs_df, active_players_df, left_on='Player_ID', right_on='id')
    all_players_logs_df['location'] = all_players_logs_df['team_home'].apply(lambda x: '.vs ' if x == 1 else '@ ')
    all_players_logs_df['matchup'] = all_players_logs_df['location'] + all_players_logs_df['opp_short']
    # all_players_logs_df['result'] = all_players_logs_df['team_wl'].apply(lambda x: 'W' if x ==  else 'L')
    all_players_logs_df['outcome'] = (
        all_players_logs_df['team_wl'] + '  ' + all_players_logs_df['team_pts'].astype(str) 
        + '-' + all_players_logs_df['opp_pts'].astype(str)
    )
    
    
    all_players_logs_df = all_players_logs_df[[
        'Game_ID','game_date', 'team_short', 'opp_short', 'matchup', 'outcome', 'full_name', 'Player_ID',
        'MIN', 'PTS', 'REB', 'AST', 'STL', 'BLK', 'FG3M', 'team_game_no'
    ]]
    all_players_logs_df = all_players_logs_df.rename(columns={'Game_ID':'game_id','Player_ID':'player_id', 'FG3M':'3PT'})
    all_players_logs_df = all_players_logs_df.drop_duplicates(subset=all_players_logs_df.columns)
    return all_players_logs_df


def add_past_next_games(df):
    current_next = df.groupby('player_id')[['next_game_date', 'next_game_opp', 'game_id']].last()
    current_next.reset_index(inplace=True)
    current_next.rename(columns={'next_game_date':'current_next_date', 'next_game_opp':'current_next_opp'}, inplace=True)
    
    df['next_game_date'] = df['game_date'].shift(-1)
    df['next_game_opp'] = df['opp_short'].shift(-1)
    
    updated_df = pd.merge(df, current_next, on=['player_id', 'game_id'], how='left')
    
    updated_df['next_game_date'] = np.where(updated_df['current_next_date'].isna(), updated_df['next_game_date'], updated_df['current_next_date'])
    updated_df['next_game_opp'] = np.where(updated_df['current_next_opp'].isna(), updated_df['next_game_opp'], updated_df['current_next_opp'])
    
    updated_df = updated_df.drop(columns=['current_next_date', 'current_next_opp'])
    return updated_df


def points_in_first(df, player_id):
    # creating a list of game_ids to iterate through
    game_id_list = df['Game_ID'].tolist()

    # creating lists/variables to be used later
    full_results = []
    total_games = len(game_id_list)

    # getting player team short and team_id
    player_team_short = get_player_team_short(player_id)
    player_team_id = get_team_id(player_team_short)

    # starting the loop to iterate through gamelogs
    for game_id in game_id_list:
        # determining if team is home or away
        team_home = team_is_home(game_id, player_team_id)

        # creating the df of pbp info for current game
        pbp_object = pbp.PlayByPlayV2(game_id = game_id)
        pbp_data = pbp_object.get_data_frames()[0]

        # determining which column to pull information from 
        if team_home == 1:
            pbp_desc = 'HOMEDESCRIPTION'
        else:
            pbp_desc = 'AWAYDESCRIPTION'

        # filtering the pbp data down to get the last basket scored in first qtr and total points
        try:
            player_data = pbp_data[pbp_data['PLAYER1_ID'] == int(player_id)]
            player_data = player_data[player_data['PERIOD'] == 1]
            event_desc = player_data[pbp_desc].tolist()
            scoring_event = [event for event in event_desc if 'PTS' in event]
            player_event = player_data[player_data[pbp_desc].isin(scoring_event)]
            last_basket = player_event.loc[player_event.groupby('PLAYER1_ID')['EVENTNUM'].idxmax()]
            last_basket = last_basket[pbp_desc].values

            def extract_pts_number(item):
                match = re.search(r'(\d+)\sPTS', item)
                return int(match.group(1)) if match else None

            pts_q1 = extract_pts_number(last_basket[0])
        
        except:
            pts_q1 = 0

        results_dict = {
                        'game_id' : game_id,
                        'player_id' : player_id,
                        'points_in_first' : pts_q1
        }

    results_list.append(results_dict)
    results_df = pd.DataFrame(full_results)

    return results_df


