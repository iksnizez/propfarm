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
from sqlalchemy import create_engine, text
from nba_api.stats.endpoints import scheduleleaguev2
import re


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

def refresh_id_table():
    '''
    this pulls the most recent ID lookup table from nfl-data-py and overwrites my db table
    '''
    from nba_api.stats.endpoints import commonallplayers

    players = commonallplayers.CommonAllPlayers(is_only_current_season=0)
    id_table = players.get_data_frames()[0]
    
    engine = connect_to_database(database_creds = config.PYMYSQL_NBA)
    with engine.connect() as conn:
        id_table.to_sql(
        name = 'player_nbaapi', 
        con=conn, 
        if_exists='replace',
        index=False          
    ) 
    print('player_nbaapi table updated with active players...')
    return

def add_new_players_to_db(refresh_ext_player_table = False, season= 2026):
    """
    TODO THIS IS A COMPLETE MESS BUT WORKS. NEED TO OPTIMIZE WHEN NOT TIRED 

    This will add players to my player table when they are either missing their 
    nba or actnet ids
    """
    
    missing_players = {
        'nbaId':{
            'missing':None,
            'df':None
        },
        'actnetId':{
            'missing':None,
            'df':None
        },
        'hooprId':{
            'missing':None,
            'df':None
        }
    }

    #refreshes the id lookup table from nba_api (NBA dot come data)
    if refresh_ext_player_table:
        refresh_id_table()

    # get the list of players from my database so i can check if the missing players are missing
    # from my database or just missing the outlet id
    engine = connect_to_database(database_creds = config.PYMYSQL_NBA)
    with engine.connect() as conn:
        # my main players data base with multiple player ids
        dfplayer = pd.read_sql(
            sql = "SELECT * FROM players;",
            con = conn
        )
        old = dfplayer.copy()

        # data from the nba_api library (nba dot com)
        missing_players['nbaId']['df'] = pd.read_sql(
            sql = """
            SELECT 
                PERSON_ID AS nbaId,
                DISPLAY_FIRST_LAST AS player
            FROM player_nbaapi 
            WHERE ROSTERSTATUS = 1;
            """,
            con = conn
        )

        # act net player ids where my props come from
        missing_players['actnetId']['df'] = pd.read_sql(
            sql = """
            SELECT playerId as actnetId, player 
            FROM actnetplayers;""",
            con = conn
        )

        # hoopr player IDs 
        missing_players['hooprId']['df'] = pd.read_sql(
            sql = f"""
            SELECT 
                DISTINCT athlete_display_name AS player, 
                athlete_id AS hooprId 
            FROM playerbox 
            WHERE season = {season};""",
            con = conn
        )

    # formatting imported data and prepping maps and list
    dfplayer['joinName']  = dfplayer['player'].str.lower().apply(apply_regex_replacements)
    players_in_db = list(dfplayer['joinName'])

    none_missing = True
    all_missing = []
    for k, v in missing_players.items():
        temp = v['df'].copy()
        temp['joinName']  = temp['player'].str.lower().apply(apply_regex_replacements)
        
        # list of all the players in the table that needs to be added to palyers
        player_list =  list(temp['joinName'])
        
        # players that are not in the players table and need to be added
        players_to_add = np.setdiff1d(player_list, players_in_db)

        if len(players_to_add) > 0:
            none_missing = False
            all_missing.extend(players_to_add)

    all_missing = list(set(all_missing))
    dfAllMissing = pd.DataFrame({'joinName':all_missing})

    for k, v in missing_players.items():
        temp = v['df'].copy()
        temp['joinName']  = temp['player'].str.lower().apply(apply_regex_replacements)
        
        # list of all the players in the table that needs to be added to palyers
        player_list =  list(temp['joinName'])
        
        # players that are not in the players table and need to be added
        players_to_add = np.setdiff1d(player_list, players_in_db)

        if len(players_to_add) > 0:
            temp = temp[temp['joinName'].isin(players_to_add)]

            dfAllMissing = pd.merge(
                left = dfAllMissing,
                right = temp,
                on ='joinName',
                how='left'
            )

    # if there is at least 1 missing player
    if not none_missing:    
    
        #dfplayer = pd.concat([dfplayer, dfAllMissing], ignore_index=True, sort=False)
        #dfplayer.drop(columns=[c for c in ['player_x', 'player_y'] if c in dfplayer], inplace=True)
        dfAllMissing = dfAllMissing.reindex(columns=dfAllMissing.columns.union(['player_x', 'player_y']), fill_value=None)

        dfAllMissing['player'] = dfAllMissing[['player', 'player_x', 'player_y']].bfill(axis=1).iloc[:, 0]
        dfAllMissing.drop(columns=[c for c in ['player_x', 'player_y'] if c in dfAllMissing], inplace=True)

        to_int = [
            'hooprId', 'nbaId', 'actnetId'
        ]
        #dfplayer[to_int] = dfplayer[to_int].apply(pd.to_numeric, errors='coerce').astype('Int64')
        dfAllMissing[to_int] = dfAllMissing[to_int].apply(pd.to_numeric, errors='coerce').astype('Int64')
        
        cols = dfAllMissing.columns.tolist()
        insert_stmt = text(f"""
            INSERT INTO players ({', '.join(cols)})
            VALUES ({', '.join([':' + c for c in cols])})
        """)

        with engine.begin() as conn:
            conn.execute(insert_stmt, dfAllMissing.to_dict(orient='records'))
        
        print(dfAllMissing.shape[0], 'players added to db..')
        

    return

def apply_regex_replacements(value):
    """
    used to format names into their most joinable form
    """
    # regex replacement mapping used to make more joinable names
    suffix_replace = config.suffix_replace

    for pattern, replacement in suffix_replace.items():
        value = re.sub(pattern, replacement, value, flags=re.IGNORECASE)
    return value
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

def elongate_nbaApi_schedule(
    season_str, 
    remove_gametypes = ['001', '003', '004', '005', '006']
):
    '''
    grab the season schedule from nba_api
    season_str = 'YYYY-YY'  2024-25
    cutoff_date = date or datetime, used to filter schedule up to the date

    gameId keys - first 3 digits
    001 = preseason
    002 = regular season
    003 = all star events
    004 = playoffs
    005 = play-in games
    006 = nba cup finals
    '''
    # load schedules
    sched = scheduleleaguev2.ScheduleLeagueV2(league_id='00', season=season_str)
    df_games = sched.season_games.get_data_frame()

    #filter out game types if input
    if remove_gametypes:
        df_games = df_games[~df_games['gameId'].str.startswith(tuple(remove_gametypes))]

    # select cols
    cols = [
        'seasonYear', 'gameId', 'weekNumber',
        'homeTeam_teamId', 'homeTeam_teamName', 
        'awayTeam_teamId', 'awayTeam_teamName',
        'gameDate', 'awayTeamTime', 'homeTeamTime', 'day', 'monthNum', 
        'arenaName', 'arenaState', 'arenaCity'
    ]
    df_games = df_games[cols]

    # reformat that data so each team has a unique record for all of their games
    home_df = df_games.copy()
    home_df['teamId'] = home_df['homeTeam_teamId'].astype(int)
    home_df['team'] = home_df['homeTeam_teamName']
    home_df['oppId'] = home_df['awayTeam_teamId'].astype(int)
    home_df['opp'] = home_df['awayTeam_teamName']
    home_df['home'] = True

    away_df = df_games.copy()
    away_df['teamId'] = away_df['awayTeam_teamId'].astype(int)
    away_df['team'] = away_df['awayTeam_teamName']
    away_df['oppId'] = away_df['homeTeam_teamId'].astype(int)
    away_df['opp'] = away_df['homeTeam_teamName']
    away_df['home'] = False

    df_long = pd.concat([home_df, away_df], ignore_index=True).reset_index()
    df_long['gameDate'] = pd.to_datetime(df_long['gameDate'])
    df_long = df_long.sort_values(['teamId', 'gameDate'])

    return df_games, df_long

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

def convert_ameri_odds_to_probability(odds):
    """
    Convert American odds to implied probabilities.

    Parameters
    ----------
    odds : pd.Series or np.ndarray
        Series or array of American odds (e.g., +150, -120).

    Returns
    -------
    pd.Series
        Series of implied probabilities (0–1 range).
    """
    odds = pd.Series(odds, dtype=float)
    probs = np.where(
        odds > 0,
        round(100 / (odds + 100), 3),
        round(-odds / (-odds + 100), 3)
    )
    
    return probs
