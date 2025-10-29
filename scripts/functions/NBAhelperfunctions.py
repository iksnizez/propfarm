##### importing custom modules from the projects folder
import sys
from pathlib import Path
# Start at current working directory
current = Path.cwd()
# Walk up the tree until config.py is found or root is reached
for parent in [current] + list(current.parents):
    config_path = parent / "config.py"
    if config_path.exists():
        sys.path.append(str(parent))
        import config # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< 
        break
else:
    raise FileNotFoundError("config.py not found in any parent directories")

import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# -------------------------------------------------------------------------------
# Functions for database connections
# -------------------------------------------------------------------------------
def connect_to_database(database_creds = config.PYMYSQL_NBA):
    #database_creds = database_creds
    ##importing credentials from txt file
    #with open(database_creds, 'r') as f:
    #    creds = f.read()
    #creds = json.loads(creds)
    #league = "nba"
    #pymysql_conn_str = creds['pymysql'][league]
    
    pymysql_conn_str = database_creds
    engine = create_engine(pymysql_conn_str)
    return engine

# -------------------------------------------------------------------------------
# Functions for updating database
# -------------------------------------------------------------------------------
def update_pbp_database():
    pass

def update_player_boxscores():
    pass

# -------------------------------------------------------------------------------
# Functions for help calculating stats for modeling 
# -------------------------------------------------------------------------------
def calculate_pace_adjustment(team_pace, opp_pace):
    # uses geometric mean to calulate pace adjuster
    return np.sqrt(team_pace * opp_pace) / opp_pace

def calculate_reb_adjustment(opp_reb_pct, league_avg_reb_pct):
    # offensive reb adj = (1 - opp def. reb %) / (league avg offensive reb%)
    # defensive reb adj = (1 - opp off. reb %) / (league avg defensive reb%)
    reb_adj = (1 - opp_reb_pct) /   league_avg_reb_pct

    return reb_adj

def calculate_opp_adjustment(opp_stat_conceded, league_avg_opp_stat_conceded):
    stats_adj = opp_stat_conceded / league_avg_opp_stat_conceded
    return stats_adj

# -------------------------------------------------------------------------------
# Functions for converting odds and probs 
# -------------------------------------------------------------------------------
def convert_probability_to_ameri_odds(prob):
    if (prob == 0) | (prob == 1) | (pd.isnull(prob)): 
        return np.nan   
    elif prob >= 0.5:
        american =  -100 * (prob / (1 - prob))  # Favorite
    else:
        american = 100 * ((1 - prob) / prob)  # Underdog

    return int(american)

def convert_probability_to_deci_odds(prob):   
    if (prob == 0) | (prob == 1) | (pd.isnull(prob)): 
        return np.nan
    elif prob > 0:
        decimal = round(1 / prob, 3)
    else:
        decimal = np.nan

    return decimal

