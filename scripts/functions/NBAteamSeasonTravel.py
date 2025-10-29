
from nba_api.stats.endpoints import ScheduleLeagueV2
import pandas as pd
import numpy as np

#### importing custom modules from the projects folder
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

'''
nba cup elimination game dates?
12/9/25, 12/10/25, 12/13/25, 12/16/25
'''

class seasonTeamTravelDistanceCalculator():

    def __init__(self):
        self.schedules = {
            'raw':{},
            'processed':{}
        }

    def get_arena_distances(self):
        '''
        retrieve arena-to-arena distances in miles
        '''
        # arena to arena distances in miles
        arena_distances = pd.read_csv(config.ARENA_DISTANCES)
        self.arena_distances_table = arena_distances.copy()

        arena_distances = arena_distances.melt(
                id_vars='team',
                var_name='to_team_id',
                value_name='distance_miles'
            ).rename(columns={'team': 'from_team_id'})
        arena_distances['from_team_id'] = arena_distances['from_team_id'].astype(int)
        arena_distances['to_team_id'] = arena_distances['to_team_id'].astype(int)
        self.arena_distances = arena_distances.copy()
        return 

    def get_season_schedule(self, season_str):
        '''
        grab the season schedule from nba_api
        season_str = 'YYYY-YY'  2024-25
        cutoff_date = date or datetime, used to filter schedule up to the date
        '''
        # load schedules
        sched = ScheduleLeagueV2(league_id='00', season=season_str)
        games = sched.season_games.get_data_frame()

        self.schedules['raw'][season_str] = games
        return

    def process_schedule(self, cutoff_date = None):

        # loop through the schedules for the seasons pulled
        for k, v in self.schedules['raw'].items():
            
            # get the raw schedules for each saved season
            df_games = v.copy()

            # remove preseason games and filter through requested date
            df_games = df_games.query('weekNumber > 0')

            df_games.loc[:,'gameDate'] = pd.to_datetime(df_games['gameDate'])
            if cutoff_date is not None:
                df_games = df_games.query('gameDate < @cutoff_date')

            # select cols
            cols = [
                'seasonYear',  'gameId', 'weekNumber',
                'homeTeam_teamId', 'homeTeam_teamName','homeTeam_teamTricode', 
                'awayTeam_teamId', 'awayTeam_teamName', 'awayTeam_teamTricode',
                'gameDate', 'awayTeamTime', 'homeTeamTime', 'day', 'monthNum', 
                'arenaName', 'arenaState', 'arenaCity'
            ]
            df_games = df_games[cols]


            # reformat that data so each team has a unique record for all of their games
            home_df = df_games[['gameId', 'gameDate', 'homeTeam_teamId', 'awayTeam_teamId', 'homeTeam_teamName', 'awayTeam_teamName']].copy()
            home_df['teamId'] = home_df['homeTeam_teamId'].astype(int)
            home_df['team'] = home_df['homeTeam_teamName']
            home_df['oppId'] = home_df['awayTeam_teamId'].astype(int)
            home_df['opp'] = home_df['awayTeam_teamName']
            home_df['home'] = True

            away_df = df_games[['gameId', 'gameDate', 'homeTeam_teamId', 'awayTeam_teamId', 'homeTeam_teamName', 'awayTeam_teamName']].copy()
            away_df['teamId'] = away_df['awayTeam_teamId'].astype(int)
            away_df['team'] = away_df['awayTeam_teamName']
            away_df['oppId'] = away_df['homeTeam_teamId'].astype(int)
            away_df['opp'] = away_df['homeTeam_teamName']
            away_df['home'] = False

            df_long = pd.concat([home_df, away_df], ignore_index=True)
            df_long['gameDate'] = pd.to_datetime(df_long['gameDate'])
            df_long = df_long.sort_values(['teamId', 'gameDate'])

            # add travel distances, road streaks, days rest, trip distances,
            # back-to-backs, and return flights.
            df_long = df_long.sort_values(['teamId', 'gameDate'])
            df_long['prev_date'] = df_long.groupby('teamId')['gameDate'].shift(1)
            
            # calandar games between games NOT REST DAYS
            df_long['days_since_last_game'] = ((df_long['gameDate'] - df_long['prev_date']).dt.days)
            # rest days between games
            df_long['days_rest'] = (df_long['gameDate'] - df_long['prev_date']).dt.days - 1

            # Identify back-to-back games
            df_long['is_b2b'] = df_long['days_rest'] == 0

            # Previous opponent and home flag
            df_long['prev_opp'] = (df_long.groupby('teamId')['oppId'].shift(1, fill_value = 0)).astype(int)
            df_long['prev_home'] = df_long.groupby('teamId')['home'].shift(1, fill_value=True)
            df_long['next_home'] = df_long.groupby('teamId')['home'].shift(-1)

            ### generate travel paths for each game
            df_long['arena_from'] = np.where(
                df_long['prev_home'],
                df_long['teamId'],
                df_long['prev_opp']
            )
            df_long['arena_to'] = np.where(
                df_long['home'],
                df_long['teamId'],
                df_long['oppId']
            )

            # add driving miles 
            df_long = pd.merge(
                df_long,
                self.arena_distances,
                how='left',
                left_on=['arena_from', 'arena_to'],
                right_on=['from_team_id', 'to_team_id']
            )
            df_long = df_long.drop(['from_team_id','to_team_id'], axis = 1)

            df_long['distance_miles'] = df_long['distance_miles'].fillna(0)
            
            # season long cumulative miles
            df_long['cum_miles_season'] = df_long.groupby('teamId')['distance_miles'].cumsum()

            # Road streak counter - increments from 1+ on consecutive road games
            # resets to zero after home game
            df_long['road_trip_streak'] = (
                df_long.groupby('teamId')['home']
                .apply(lambda x: (~x).cumsum() - (~x).cumsum().where(x).ffill().fillna(0).astype(int))
                .reset_index(drop=True)
            )

            # distance column used to calculate the cumulative miles per each road trip
            # it sets home games distance traveled to zero even when going away to home
            # this away to home zero is used below in the road trip cumsum to reset it
            df_long['road_trip_dist'] = np.where(
                df_long['road_trip_streak'] == 0,
                0,
                df_long['distance_miles']
            )
            # calculates the cumsum for each road trip
            # resets for every new road trip
            df_long['cum_miles_road_trip'] = df_long.groupby(
                (df_long['road_trip_dist'] == 0).cumsum()
                )['road_trip_dist'].cumsum()
            
            # calculates the number of cal. days on a road trip
            df_long['road_trip_days'] = np.where(
                df_long['road_trip_streak'] == 0,
                0,
                np.where(
                    df_long['road_trip_streak'] == 1,
                    1,
                    df_long['days_since_last_game']
                ) 
            )
            # calculates the cumsum for each road trip
            # resets for every new road trip
            df_long['cum_days_road_trip'] = df_long.groupby(
                (df_long['road_trip_days'] == 0).cumsum()
                )['road_trip_days'].cumsum()
            
            final_order = [
                'gameId', 'gameDate', 
                'team','opp', 'home', 'is_b2b', 'prev_home', 'next_home', 'days_rest',
                     'distance_miles',  'road_trip_streak',
                'road_trip_dist', 'cum_miles_road_trip', 
                'cum_days_road_trip', 'cum_miles_season','road_trip_days',
                'prev_date', 'days_since_last_game', 'prev_opp',
                'arena_from','arena_to', 'homeTeam_teamName', 'awayTeam_teamName',
                'homeTeam_teamId', 'awayTeam_teamId', 'teamId','oppId'
            ]
            df_long = df_long[final_order]

            self.schedules['processed'][k] = df_long
        return 