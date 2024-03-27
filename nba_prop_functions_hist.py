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
from nba_api.stats.endpoints import playernextngames as next
import time
from datetime import datetime
pd.options.mode.copy_on_write = True

if __name__ == '__main__':
    print('this is the main block of code')


# -

def player_gamelog_id(df, player_id):
    player_df = df[df['player_id'] == player_id]
    player_df = player_df.sort_values(by=['season_id','team_game_no'], ascending = True)
    player_df = player_df.reset_index(drop=True)
    return player_df


def player_gamelog_name(df, full_name):
    player_df = df[df['full_name'] == full_name].reset_index(drop=True)
    return player_df


# ******* PROB SHOULD DO A CAREER ONE AS WELL (OR AT LEAST SINCE 2016) *********
def player_last_x_gamelogs(player_df, season_id, games, unique_id):
    row = player_df[player_df['unique_id'] == unique_id]
    team_game_no = row['team_game_no'].iloc[0]
    player_df = player_df[player_df['season_id'] == season_id]
    player_df.sort_values(by='team_game_no', ascending=False, inplace=True)
    player_df = player_df.reset_index(drop=True)
    row = player_df[player_df['unique_id'] == unique_id]
    game_index = row.index[0]
    start_unique_id = game_index + 1
    end_unique_id = game_index + games + 1
    last_x = player_df.iloc[start_unique_id:end_unique_id]
    return last_x


def player_gamelogs_dfs(player_df, unique_id):
    row = player_df[player_df['unique_id'] == unique_id]
    season_id = row['season_id'].iloc[0]
    team_game_no = row['team_game_no'].iloc[0]
    
    if team_game_no > 5:
        last_5 = player_last_x_gamelogs(player_df, season_id, 5, unique_id)
        last_5.reset_index(drop=True, inplace=True)
    else:
        last_5 = player_last_x_gamelogs(player_df, season_id, team_game_no, unique_id)
    if team_game_no > 10:
        last_10 = player_last_x_gamelogs(player_df, season_id, 10, unique_id)
        last_10.reset_index(drop=True, inplace=True)
    else:
        last_10 = player_last_x_gamelogs(player_df, season_id, team_game_no, unique_id)
    
    season = player_last_x_gamelogs(player_df, season_id, team_game_no, unique_id)
    season = season.sort_values(by='team_game_no', ascending=False)
    # b2b = player_df[player_df['b2b'] == 'Yes']
    # b2b.reset_index(drop=True, inplace=True)
    return last_5, last_10, season


# +
def percent_to_decimal(percent_odds):
    if percent_odds == 0:
        return np.nan
    elif percent_odds > 0 and percent_odds <= 1:
        return 1 / percent_odds
    elif percent_odds > 1:
        return 100 / percent_odds
    else:
        return 0

def decimal_to_american(decimal_odds):
    if decimal_odds == 0:
        return 0
    elif decimal_odds > 2:
        return round((100) * (decimal_odds - 1))
    else:
        return round(-100 / (decimal_odds - 1))

def decimal_to_american_str(decimal_odds):
    if np.isnan(decimal_odds):
        return 'NL'
    elif decimal_odds == 0:
        return 'NL'
    elif decimal_odds == 1:
        return 'NL'
    elif decimal_odds > 2:
        return '+' + str(round((100) * (decimal_odds - 1)))
    else:
        return str(round(-100 / (decimal_odds - 1)))

def percent_to_american_str(percent_odds):
    decimal_odds = percent_to_decimal(percent_odds)
    return decimal_to_american_str(decimal_odds)


# -

def generate_test_prop_results(df, column):
    player_ids = df['player_id']
    unique_ids = df['unique_id']
    lines = df[column]
    prop = column.split('_')[0]
    props = [prop for i in lines]
    results_dfs_list = []
    for player_id, unique_id, line, prop in zip(player_ids, unique_ids, lines, props):
        player_df = player_gamelog_id(df, player_id)
        last_5, last_10, season = player_gamelogs_dfs(player_df, unique_id)
        hit_pct_5 ,hit_pct_10, hit_pct_season, hit_pct_roll_5, hit_pct_roll_10 = past_prop_results(last_5, last_10, season, prop, line)
        results_dict = {'unique_id':unique_id, 'prop':prop, 'line':column, 'hit_pct_5':hit_pct_5, 'hit_pct_10':hit_pct_10, 'hit_pct_season':hit_pct_season, 
                        'hit_pct_roll_5':hit_pct_roll_5, 'hit_pct_roll_10':hit_pct_roll_10
                       }
        results_dfs_list.append(results_dict)
    
    results_df = pd.DataFrame(results_dfs_list)
    return results_df


def generate_test_prop_results_test(df, column):
    player_ids = df['player_id']
    unique_ids = df['unique_id']
    lines = df[column]
    prop = column.split('_')[0]
    
    # Dictionary to store player gamelogs
    player_gamelogs = {}
    
    results_dfs_list = []
    
    for player_id, unique_id, line in zip(player_ids, unique_ids, lines):
        # Check if player gamelog is already computed
        if player_id not in player_gamelogs:
            # Compute player gamelog and store in dictionary
            player_gamelogs[player_id] = player_gamelog_id(df, player_id)
        
        # Retrieve player gamelog from dictionary
        player_df = player_gamelogs[player_id]
        
        last_5, last_10, season = player_gamelogs_dfs(player_df, unique_id)
        hit_pct_5 ,hit_pct_10, hit_pct_season, hit_pct_roll_5, hit_pct_roll_10 = past_prop_results(last_5, last_10, season, prop, line)
        
        results_dict = {'unique_id': unique_id, 'prop': prop, 'line': column, 
                        'hit_pct_5': hit_pct_5, 'hit_pct_10': hit_pct_10, 'hit_pct_season': hit_pct_season, 
                        'hit_pct_roll_5': hit_pct_roll_5, 'hit_pct_roll_10': hit_pct_roll_10}
        
        results_dfs_list.append(results_dict)
    
    results_df = pd.DataFrame(results_dfs_list)
    
    return results_df



# +
# ******* should just hardcode the most popular ones *********

def create_combo_cols(prop, player_df):
    prop_list = prop.split('+')
    num_props = len(prop_list)

    
    if num_props == 2:
        player_df.loc[:, prop] = player_df[prop_list[0]] + player_df[prop_list[1]]
    elif num_props == 3:
        player_df.loc[:, prop] = player_df[prop_list[0]] + player_df[prop_list[1]] + player_df[prop_list[2]]
    elif num_props == 4:
        player_df.loc[:, prop] = player_df[prop_list[0]] + player_df[prop_list[1]] + player_df[prop_list[2]] + player_df[prop_list[3]]
    if num_props == 5:
        player_df.loc[:, prop] = player_df[prop_list[0]] + player_df[prop_list[1]] + player_df[prop_list[2]] + player_df[prop_list[3]] + player_df[prop_list[4]]

    player_df.insert(15, prop, player_df.pop(prop))

    return player_df

# +
# ****** THIS IS WORKING WITH THE HISTORICAL DATA *******
# ***** JUST NEED TO RETURN THE HIT % *******
# **** DO NOT NEED SIDE, JUST INVERSE ******


def past_prop_results(last_5, last_10, season, prop, line):
    if len(season) != 0:
        hit_last_5 = (last_5[prop] > line).sum()
        hit_last_10 = (last_10[prop] > line).sum()
        hit_season = (season[prop] > line).sum()
        # hit_b2b = (b2b[prop] > line).sum()
    
        hit_last_5 = hit_last_5 or 0
        hit_last_10 = hit_last_10 or 0
        hit_season = hit_season or 0
        # hit_b2b = hit_b2b or 0
        
        # total_b2b = len(b2b)
        total_games = len(season)

    
        hit_pct_5 = (hit_last_5 / 5) * 100
        hit_pct_10 = (hit_last_10 / 10) * 100
        hit_pct_season = round((hit_season / total_games) * 100, 1)
        
        # if total_b2b != 0:
        #     hit_pct_b2b = round((hit_b2b / total_b2b) * 100, 1)
        # else:
        #     hit_pct_b2b = 0 
    
        player_rolling = season.copy()

        player_rolling['covered'] = player_rolling[prop].apply(lambda x: 1 if x > line else 0)
        
        player_rolling['rolling_5'] = player_rolling['covered'].transform(lambda x: x.rolling(window=5, min_periods=0).sum()).fillna(0) 
        player_rolling['rolling_5_pct'] = player_rolling['rolling_5'] / 5
        player_rolling['rolling_10'] = player_rolling['covered'].transform(lambda x: x.rolling(window=10, min_periods=0).sum()).fillna(0) 
        player_rolling['rolling_10_pct'] = player_rolling['rolling_10'] / 10
        
        hit_pct_roll_5 = round(player_rolling['rolling_5_pct'].mean(), 1) * 100
        hit_pct_roll_10 = round(player_rolling['rolling_10_pct'].mean(), 1) * 100
        
    else:
        hit_pct_5 = 0
        hit_pct_10 = 0
        hit_pct_season= 0
        hit_pct_roll_5 = 0
        hit_pct_roll_10 = 0
    
    return hit_pct_5 ,hit_pct_10, hit_pct_season, hit_pct_roll_5, hit_pct_roll_10


# -

def calculate_break_even_payout(win_rate):
    # Convert win rate to decimal
    p_win = win_rate / 100.0
    # Calculate probability of loss
    p_loss = 1 - p_win
    # Solve for the payout odds needed for breakeven
    payout_win = p_loss / p_win
    return payout_win


# +
# # *****NOT GOING TO WORRY ABOUT NOW (THO I KNOW RASHID WANTS IT) ********

# def add_b2b_flag(player_df):
#     player_df['game_date'] = pd.to_datetime(player_df['game_date'], format='%b %d, %Y')
#     player_df['days_rest'] = player_df.apply(lambda row: 
#         player_df.loc[row.name, 'game_date'] - player_df.loc[row.name - 1, 'game_date'] - pd.Timedelta(days=1) 
#         if row.name > 0 
#         else pd.Timedelta(days=90), 
#         axis=1
#     )
#     player_df['days_rest'] = player_df['days_rest'].apply(lambda x: x.days)
#     player_df['b2b'] = player_df['days_rest'].apply(lambda x: 'Yes' if x == 0 else 'No')
#     player_df['game_date'] = player_df['game_date'].dt.strftime('%b %d, %Y')
#     return player_df
