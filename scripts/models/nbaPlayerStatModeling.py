import time, json, requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine

### # nba_api is not friendly, changed it direct url hit ###
#from nba_api.stats.endpoints import LeagueDashPlayerStats
from nba_api.stats.endpoints import ScoreboardV2
from nba_api.stats.endpoints import PlayerDashboardByGeneralSplits

# models
from scipy.stats import poisson, binom, nbinom
from scipy.signal import convolve

#TODO:
###### immediate
### remove players from df_players before grabbing split data if no props were retrieved for them? 

####### longer term
### add minutes adjustment ( fade expected minutes if injured or increase if increasing role)
### explore penalizing early and afternoon games 1 - 6pm 

def calculate_pace_adjustment(team_pace, opp_pace):
    # uses geometric mean to calulate pace adjuster
    return np.sqrt(team_pace * opp_pace) / opp_pace

def connect_to_database(database_creds = '../../../../Notes-General/config.txt'):
    database_creds = database_creds
    #importing credentials from txt file
    with open(database_creds, 'r') as f:
        creds = f.read()
    creds = json.loads(creds)

    league = "nba"
    pymysql_conn_str = creds['pymysql'][league]

    conn = create_engine(pymysql_conn_str)
    return conn

def calculate_reb_adjustment(opp_reb_pct, league_avg_reb_pct):
    
    # offensive reb adj = (1 - opp def. reb %) / (league avg offensive reb%)
    # defensive reb adj = (1 - opp off. reb %) / (league avg defensive reb%)
    reb_adj = (1 - opp_reb_pct) /   league_avg_reb_pct

    return reb_adj

def calculate_opp_adjustment(opp_stat_conceded, league_avg_opp_stat_conceded):
    stats_adj = opp_stat_conceded / league_avg_opp_stat_conceded
    return stats_adj

def convert_probability_to_ameri_odds(prob):
    if (prob == 0) | (pd.isnull(prob)): 
        american = np.nan  
    elif prob == 1: 
        american =  -100 * (prob / (1 - 0.999999))  # Favorite    
    elif prob >= 0.5:
        american =  -100 * (prob / (1 - prob))  # Favorite
    else:
        american = 100 * ((1 - prob) / prob)  # Underdog

    return int(american)

def convert_probability_to_deci_odds(prob):   
    # decimal style
    if prob > 0:
        decimal = round(1 / prob, 3)
    else:
        decimal = np.nan

    return decimal

def simulate_points(num_simulations, expected_total_shots, fg2a_share, fg3a_share, fta_share, fg2_pct, fg3_pct, ft_pct):
        """
        Simulates a player's total points using a Monte Carlo method.

        Returns:
            np.array: Simulated points scored in each iteration.
        """
        total_shots = np.random.poisson(expected_total_shots, num_simulations)
        fg2a = np.random.binomial(total_shots, fg2a_share)
        fg3a = np.random.binomial(total_shots - fg2a, fg3a_share / (fg3a_share + fta_share))  
        fta = total_shots - fg2a - fg3a  

        fg2m = np.random.binomial(fg2a, fg2_pct)
        fg3m = np.random.binomial(fg3a, fg3_pct)
        ftm = np.random.binomial(fta, ft_pct)

        points = (fg2m * 2) + (fg3m * 3) + ftm
        return points

class playerStatModel():
    
    def __init__(self, day_offset = 0, season = '2024-25', perMode = 'PerGame', num_simulations= 10000):
        

        self.day_offset = day_offset
        self.season = season
        self.perMode = perMode ##['Per100Possessions', 'Totals', 'Per36', 'PerGame']
        self.game_search_date = datetime.today().strftime('%m/%d/%Y')
        self.game_search_dt = datetime.today() + timedelta(days=day_offset)
        self.num_simulations = num_simulations
        
        # NBA API Headers to prevent blocking
        self.HEADERS = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Referer': 'https://www.nba.com/',
        }

        self.pace_gathered = False

    def get_teams_playing(self,
        url = 'https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json',
        use_api = True    
    ):
        """
        get the teams playing on the date; date = today +/- day_offset

        assigns list of team ids and dataframe with minimal game detail to class object
        return nothing
        """
        if use_api:
            time.sleep(2)
            try:
                # Fetch scoreboard data
                scoreboard = ScoreboardV2(
                    game_date = self.game_search_date, 
                    day_offset = self.day_offset
                )

            except Exception as e:
                print('Error fetching data:', e)
                return

            # convert data
            raw_data = scoreboard.get_json()
            data = json.loads(raw_data)

            # get headers and filter to ones of interest
            headers_scoreboard = data['resultSets'][0]['headers']
            filtered_headers = [headers_scoreboard[i] for i in [0,2,4,6,7]]

            # get data of interest and extract necessary info
            games = data['resultSets'][0]['rowSet']
            filtered_games = [[game[0], game[2], game[4], game[6], game[7]] for game in games]
            df_games =  pd.DataFrame(filtered_games, columns=filtered_headers)
            del games, headers_scoreboard, raw_data, filtered_games
        
        else:

            response = requests.get(url)
            data = json.loads(response.text)

            games_data = []
            for i in data['scoreboard']['games']:
                game_data = [
                    i['gameEt'],
                    i['gameId'],
                    i['gameEt'],
                    i['homeTeam']['teamId'],
                    i['awayTeam']['teamId']
                ]

                games_data.append(game_data)

            columns_games = [
                'GAME_DATE_EST','GAME_ID','GAME_STATUS_TEXT','HOME_TEAM_ID','VISITOR_TEAM_ID'
            ]
            df_games = pd.DataFrame(games_data, columns = columns_games)

        # data processing
        #df_games.loc[:,'GAME_DATE_EST'] = pd.to_datetime(df_games.loc[:,'GAME_DATE_EST']).dt.date
        #df_games.loc[:,'GAME_STATUS_TEXT'] = df_games['GAME_STATUS_TEXT'].str.replace(' ET', '', regex=False)
        #df_games.loc[:,'GAME_STATUS_TEXT'] = pd.to_datetime(df_games['GAME_STATUS_TEXT'], format='%I:%M %p', errors='coerce').dt.time
        df_games.loc[:,'GAME_DATE_EST'] = df_games['GAME_DATE_EST'].str.split('T').str[0]
        df_games.loc[:,'GAME_DATE_EST'] = pd.to_datetime(df_games['GAME_DATE_EST']).dt.date
        df_games.loc[:,'GAME_STATUS_TEXT'] = df_games['GAME_STATUS_TEXT'].str.split('T').str[1]
        df_games.loc[:,'GAME_STATUS_TEXT'] = df_games['GAME_STATUS_TEXT'].str.split('Z').str[0]
        df_games.loc[:,'GAME_STATUS_TEXT'] = df_games['GAME_STATUS_TEXT'].str.split('-').str[0]

        # generate long dataframe that will be used for joining easier later
        df_games_long = df_games.melt(id_vars=['GAME_ID', 'GAME_DATE_EST', 'GAME_STATUS_TEXT'], 
                          value_vars=['HOME_TEAM_ID', 'VISITOR_TEAM_ID'], 
                          var_name='TEAM_TYPE', 
                          value_name='TEAM_ID'
        )
        
        df_games_long.loc[:, 'TEAM_TYPE'] =  np.where(df_games_long['TEAM_TYPE'] == 'HOME_TEAM_ID',
                                                'Home',
                                                'Away'
        )

        df_games_long = df_games_long.merge(
            df_games_long[['GAME_ID', 'TEAM_ID']], 
            on='GAME_ID', 
            suffixes=('', '_opp')
        )

        # Assign opponent's TEAM_ID
        df_games_long = df_games_long[df_games_long['TEAM_ID'] != df_games_long['TEAM_ID_opp']].copy()
        df_games_long.rename(columns={'TEAM_ID_opp': 'oppTeamId'}, inplace=True)

        # saving data
        print(df_games.shape[0], ' games today...')
        self.list_team_ids = list(set(df_games['HOME_TEAM_ID']).union(set(df_games['VISITOR_TEAM_ID'])))
        self.list_home_team_ids = list(set(df_games['HOME_TEAM_ID']))
        self.list_away_team_ids = list(set(df_games['VISITOR_TEAM_ID']))
        self.df_games = df_games
        self.df_games_long = df_games_long
        return
    
    # TODO: pull from local db instead of hitting this url
    def get_teams_data(self,
        base_url = 'https://stats.nba.com/stats/leaguedashteamstats?Conference=&DateFrom=&DateTo=&Division=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType={stattype}&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={sid}&SeasonSegment=&SeasonType=Regular%20Season&ShotClockRange=&StarterBench=&TeamID=0&TwoWay=0&VsConference=&VsDivision='
    ):
        base_url_opp = base_url
        url_team_pace = base_url.format(sid=self.season, stattype='Advanced')
        response = requests.get(url=url_team_pace, headers=self.HEADERS).json()

        # generate data frame and collect advanced stats
        teams_pace = []
        for i in response['resultSets'][0]['rowSet']:
            team_pace = [i[0],i[16],i[17], i[23]]
            teams_pace.append(team_pace)

        df_pace = pd.DataFrame(teams_pace, columns=['TEAM_ID', 'OREB_PCT', 'DREB_PCT','PACE'])
        self.league_avg_pace = df_pace['PACE'].mean()
        self.league_avg_oreb_pct = df_pace['OREB_PCT'].mean()
        self.league_avg_dreb_pct = df_pace['DREB_PCT'].mean()

        # generate data frame and collect advanced stats
        url_team_opp_trad = base_url_opp.format(sid=self.season, stattype='Opponent')
        response = requests.get(url=url_team_opp_trad, headers=self.HEADERS).json()
        
        teams_opp_stats = []
        for i in response['resultSets'][0]['rowSet']:
            teams_opp_stat = [i[0], i[19], i[26]]
            teams_opp_stats.append(teams_opp_stat)
        
        df_opp_pts = pd.DataFrame(teams_opp_stats, columns=['TEAM_ID', 'oppAst', 'oppPts'])
        self.league_avg_oppPts = df_opp_pts['oppPts'].mean()
        self.league_avg_oppAst = df_opp_pts['oppAst'].mean()

        # join advanced and opponent data back to class team dataframe
        self.df_games_long = self.df_games_long.merge(df_pace, how='left', on='TEAM_ID')
        self.df_games_long = self.df_games_long.merge(df_opp_pts, how='left', on='TEAM_ID')
        self.pace_gathered = True
        return

    def get_players_playing(self,
        # use format to input perMode (per), season id (sid), and team id (tid)
        url_base_nba_player_stat = 'https://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode={per}&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={sid}&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&StarterBench=&TeamID={tid}&TwoWay=0&VsConference=&VsDivision=&Weight=',
        minute_cutoff = 15.0
    ):
        """
        using the team ids from get_teams_playing(), aggregate player data stats of interest
        nba_api LeagueDashPlayerStats seems to have stopped working so the website is being hit directly

        currently pulling - mins, fga/m (2/3/FT), rebs (o/d/all), ast, to, blk, stl, pts

        assigns dataframe with these stats to class object
        return nothing
        """
        player_stats_list = []
        teams = self.list_team_ids

        for i in range(len(teams)):
            team_id = teams[i]
            roster = []
            player_info_url = url_base_nba_player_stat.format(
                per = self.perMode,
                sid = self.season, 
                tid = team_id
            ) 
            
            # only need to grab column headers on the first pass
            if i == 0:
                response = requests.get(url=player_info_url, headers=self.HEADERS).json()
                columns_player_stats = response['resultSets'][0]['headers']
            else:
                response = requests.get(url=player_info_url, headers=self.HEADERS).json()

            for j in response['resultSets'][0]['rowSet']:
                roster.append(j)
            
            columns_players_keep = [
            'PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBREVIATION', 'MIN', 'FGM', 'FGA', 
            'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB', 
            'AST','TOV', 'STL', 'BLK', 'BLKA', 'PF', 'PFD', 'PTS', 'PLUS_MINUS'
            ]
            temp = pd.DataFrame(roster, columns = columns_player_stats)
            # players stay in team data even after switching teams but their team_abbreviation changes
            # gather the most common team abbreviation on the team (it should be the actual team matching the id)
            correct_team = temp['TEAM_ABBREVIATION'].value_counts()[:1].index.tolist()
            #filter out the non matching players
            temp = temp[temp['TEAM_ABBREVIATION'].isin(correct_team)]
            player_stats_list.append(temp)

            time.sleep(1)


        df_players = pd.concat(player_stats_list)
        df_players = df_players[df_players['TEAM_ID'].isin(teams)][columns_players_keep]
        df_players = df_players[df_players['MIN'] >= minute_cutoff]

        ### processing
        # add home flag
        df_players.loc[:,'homeTeam'] = np.where(df_players['TEAM_ID'].isin(self.list_home_team_ids), 1, 0)

        # add opponent id and pace, if gathered
        if self.pace_gathered:
            cols = ['TEAM_ID', 'oppTeamId', 'PACE']
        else:
            cols = ['TEAM_ID', 'oppTeamId']

        df_players = df_players.merge(
            self.df_games_long[cols], 
            on = 'TEAM_ID', 
            how = 'left'
        )
        if self.pace_gathered:
            cols = ['TEAM_ID', 'OREB_PCT', 'DREB_PCT', 'PACE', 'oppPts', 'oppAst']
            df_players = df_players.merge(
                self.df_games_long[cols],
                left_on = 'oppTeamId',
                right_on = 'TEAM_ID',
                how = 'left'
            )

            df_players = df_players.drop(['TEAM_ID_y'], axis = 1)
            df_players = df_players.rename(columns={
                'TEAM_ID_x':'TEAM_ID',
                'PACE_x':'PACE',
                'PACE_y':'oppPACE'
            })

            df_players.loc[:, 'PACEadj'] = df_players.apply(lambda x: calculate_pace_adjustment(
                                                                            x['PACE'], 
                                                                            x['oppPACE']
                                                                    ), 
                                                            axis = 1
            )
            df_players.loc[:, 'OREBadj'] = df_players.apply(lambda x: calculate_reb_adjustment(
                                                                            opp_reb_pct = x['DREB_PCT'], 
                                                                            league_avg_reb_pct = self.league_avg_oreb_pct
                                                                    ), 
                                                            axis = 1
            )
            df_players.loc[:, 'DREBadj'] = df_players.apply(lambda x: calculate_reb_adjustment(
                                                                            opp_reb_pct = x['OREB_PCT'], 
                                                                            league_avg_reb_pct = self.league_avg_dreb_pct
                                                                    ), 
                                                            axis = 1
            ) 
            df_players.loc[:, 'PTSadj'] = df_players.apply(lambda x: calculate_opp_adjustment(
                                                opp_stat_conceded = x['oppPts'],
                                                league_avg_opp_stat_conceded = self.league_avg_oppPts
                                        ), 
                                axis = 1
            )
            df_players.loc[:, 'ASTadj'] = df_players.apply(lambda x: calculate_opp_adjustment(
                                                opp_stat_conceded = x['oppAst'],
                                                league_avg_opp_stat_conceded = self.league_avg_oppAst
                                        ), 
                                axis = 1
            )        

        self.df_players = df_players
        print(
            df_players['PLAYER_ID'].nunique(), 'players returned from',
            df_players['TEAM_ID'].nunique(), 'teams, out of a total', len(teams)
        )
         
        return
    
    # TODO: add fall back to scrape if no Database present
    def get_props(self):
        """
        **** ONLY FUNCTIONAL ON MY DATABASE ATM *****
        retrieves the class objects search date props for the day
        from my database that scrapes from actnet
        """


        query = """
            SELECT p.nbaId PLAYER_ID, p.player, o.prop, o.line, o.oOdds, o.uOdds  
            FROM odds o
            JOIN players p ON o.playerId = p.actnetId
            WHERE DATE(o.date) = %s;
        """
        engine = connect_to_database()

        # query database for props
        with engine.connect() as connection:
            props = pd.read_sql(
                sql=query, 
                con=connection,  
                params=(self.game_search_dt.strftime('%Y-%m-%d'),)  
            )

        # flatten props data
        df_props_pivot = (
            props.melt(
                id_vars=['PLAYER_ID', 'prop'], 
                value_vars=['line', 'oOdds', 'uOdds']
            ).pivot(
                index='PLAYER_ID',
                columns=['prop', 'variable'],
                values='value'
            )
        )

        df_props_pivot.columns = [f'{prop}_{stat}' for prop, stat in df_props_pivot.columns]
        df_props_pivot = df_props_pivot.reset_index()

        # join props to df_players
        self.df_players = self.df_players.merge(
            df_props_pivot, 
            on = 'PLAYER_ID', 
            how = 'inner'
        )
        self.df_players = self.df_players.fillna(0)
        print(props.shape[0], 'prop bets for', props['PLAYER_ID'].nunique(), 'players...')
        return

    def get_player_home_adv(self, use_default=True):
        """
        use_default = True; will just use a default home court adjuster for all players

        use_default = False; will calculate each players splits for each stat
        
        ******this can hit the NBA API to calculate a players home court adv in each stat but the
        api call is one player at a time******** 

        nothing is returned but the adv is added to the class object df_players
        """
        if use_default:
            home_adj = np.sqrt(1.01)
            away_adj = np.sqrt(1/1.01)

            self.df_players.loc[:,'homeAwayAdj'] = np.where(self.df_players['homeTeam'] == 1,
                                                    home_adj,
                                                    away_adj
            )
        
        else:
            list_players = list(self.df_players['PLAYER_ID'].unique())
            # gather required data to calculate home advantage
            all_splits = []
            count = 1
            total = len(list_players)
            for pid in list_players:
                splits = PlayerDashboardByGeneralSplits(
                    player_id = pid, 
                    season = self.season,
                    headers = self.HEADERS,
                    timeout = 5
                )
                
                df_splits = splits.get_data_frames()[1]  # 1 = Home/Away splits
                df_splits['PLAYER_ID'] = pid  # Add player ID for tracking
                all_splits.append(df_splits)
                print(count, '/', total, 'player splits..')
                count += 1
                time.sleep(1.5)

            # Combine all players into a single DataFrame
            df_location_splits = pd.concat(all_splits, ignore_index=True)

            columns_splits = [
                'PLAYER_ID', 'GROUP_VALUE','GP', 'MIN', 'FGM', 'FGA', 'FG3M', 'FG3A', 'FTM', 
                'FTA', 'OREB', 'DREB', 'REB', 'AST', 'TOV', 'STL', 'BLK', 'BLKA', 'PTS'
            ]

            df_location_splits = df_location_splits[columns_splits]
            df_location_splits = df_location_splits[df_location_splits['GROUP_VALUE'] != 'Neutral']

            # Pivot to separate Home & Road stats
            df_pivot = df_location_splits.pivot(index='PLAYER_ID', columns='GROUP_VALUE')

            # Flatten multi-level column names
            df_pivot.columns = [f"{metric}_{location}" for metric, location in df_pivot.columns]

            # Calculate per-game averages (only for stats that need it)
            cols_to_avg = [
                'MIN', 'FGM', 'FGA', 'FG3M', 'FG3A', 'FTM', 'FTA', 'OREB', 
                'DREB', 'REB', 'AST', 'TOV', 'STL', 'BLK', 'BLKA', 'PTS'
            ]

            for col in cols_to_avg:
                df_pivot[f'{col}_Home_per_G'] = df_pivot[f'{col}_Home'] / df_pivot['GP_Home']
                df_pivot[f'{col}_Road_per_G'] = df_pivot[f'{col}_Road'] / df_pivot['GP_Road']

            # Compute Home - Road % delta in per-game stats - limiting impact at 20% fade and 20% boost 
            #TODO: explore different floors for the fade
            cols_to_display = []
            for col in cols_to_avg:
                df_pivot[f'{col}_ROADadj'] = (1 + ((df_pivot[f'{col}_Home_per_G'] - df_pivot[f'{col}_Road_per_G']
                                                    ) / df_pivot[f'{col}_Home_per_G'])).clip(0.8, 1.2)
                df_pivot[f'{col}_HOMEadj'] = (1 + ((df_pivot[f'{col}_Home_per_G'] - df_pivot[f'{col}_Road_per_G']
                                                    ) / df_pivot[f'{col}_Road_per_G'])).clip(0.8, 1.2)
                cols_to_display.append(f'{col}_HOMEadj')
                cols_to_display.append(f'{col}_ROADadj')

            # columns to keep then join to df_players
            #cols_to_display = [f'{col}_HOMEadj' for col in cols_to_avg]
            #for col in cols_to_avg:
            #    cols_to_display.append(f'{col}_ROADadj')
            cols_to_display.append('PLAYER_ID')

            # create df to join to players
            df_pivot = df_pivot.reset_index(drop=False)[cols_to_display]

            # merge data
            self.df_players = pd.merge(self.df_players, df_pivot, how='left', on='PLAYER_ID')
            return
    
    # TODO model minutes, right now just using season avg as expected
    def model_expected_minutes(self):
        # needs to be a percent
        self.df_players.loc[:,'MINadj'] = 1
        return
    
    def calculate_model_inputs(self):
        df = self.df_players.copy()
        df.loc[:,'FG2M'] = df['FGM'] - df['FG3M']
        df.loc[:,'FG2A'] = df['FGA'] - df['FG3A']
        df.loc[:,'FG2_PCT'] = df['FG2M'] / df['FG2A']
        df.loc[:,'FGA_FTA'] = df['FGA'] + df['FTA']
        # account for zero pct shot share by changing a 0 to a miniscule number
        # TODO better fix for zeros
        df.loc[:,'shotshareFG2A'] = np.where(df['FG2A'] / df['FGA_FTA'] == 0, .00001, df['FG2A'] / df['FGA_FTA'])
        df.loc[:,'shotshareFG3A'] = np.where(df['FG3A'] / df['FGA_FTA'] == 0, .00001, df['FG3A'] / df['FGA_FTA'])
        df.loc[:,'shotshareFTA'] = np.where(df['FTA'] / df['FGA_FTA'] == 0, .00001, df['FTA'] / df['FGA_FTA'])

        # expected stats
        df.loc[:,'expReb'] = np.where(df['homeTeam'] == 1,
            ((df['MINadj'] * df['PACEadj'] * df['OREB_HOMEadj'] * df['OREB'] * df['OREBadj']) + 
             (df['MINadj'] * df['PACEadj'] * df['DREB_HOMEadj'] * df['DREB'] * df['DREBadj'])
            ),
            ((df['MINadj'] * df['PACEadj'] * df['OREB_ROADadj'] * df['OREB'] * df['OREBadj']) + 
             (df['MINadj'] * df['PACEadj'] * df['DREB_ROADadj'] * df['DREB'] * df['DREBadj'])
            )
        )
        
        df.loc[:,'expAst'] = np.where(df['homeTeam'] == 1,
            (df['MINadj'] * df['PACEadj'] * df['AST_HOMEadj'] * df['AST'] * df['ASTadj']),
            (df['MINadj'] * df['PACEadj'] * df['AST_ROADadj'] * df['AST'] * df['ASTadj']) 
        )
        df.loc[:,'expPts'] = np.where(df['homeTeam'] == 1,
            df['MINadj'] * df['PACEadj'] * df['PTS_HOMEadj'] * df['FGA_FTA'] * df['PTSadj'] * (
                (df['shotshareFG2A'] * df['FG2_PCT'] * 2) + 
                (df['shotshareFG3A'] * df['FG3_PCT'] * 3) + 
                (df['shotshareFTA'] * df['FT_PCT'])
            ),
            df['MINadj'] * df['PACEadj'] * df['PTS_ROADadj'] * df['FGA_FTA'] * df['PTSadj'] * (
                (df['shotshareFG2A'] * df['FG2_PCT'] * 2) + 
                (df['shotshareFG3A'] * df['FG3_PCT'] * 3) + 
                (df['shotshareFTA'] * df['FT_PCT'])
            )                            
        )

        self.df_players = df.copy()
        return
    
    def model_expected_stats_poisson(self):
        #lamda = 5  # Expected number of occurrences (λ)
        #samples = np.random.poisson(lamda, size=10)  # Generate 10 samples
        df = self.df_players.copy()

        sim_results = np.random.poisson(df['expReb'].values[:, None], (len(df), self.num_simulations))
        df['REBoProb'] = (sim_results > df['reb_line'].values[:, None]).sum(axis=1) / self.num_simulations

        df.loc[:,'REBoOdds'] =  df['REBoProb'].apply(convert_probability_to_ameri_odds)
        df.loc[:,'REBoOdds_deci'] = df['REBoProb'].apply(convert_probability_to_deci_odds)

        sim_results = np.random.poisson(df['expAst'].values[:, None], (len(df), self.num_simulations))
        df['ASToProb'] = (sim_results > df['ast_line'].values[:, None]).sum(axis=1) / self.num_simulations

        df.loc[:,'ASToOdds'] =  df['ASToProb'].apply(convert_probability_to_ameri_odds)
        df.loc[:,'ASToOdds_deci'] = df['ASToProb'].apply(convert_probability_to_deci_odds)

        self.df_players = df.copy()
        return

    def model_expected_pts(self):
        """
        Adds a column to the DataFrame with the probability of scoring at least X points.

        Returns:
            pd.DataFrame: Updated DataFrame with probability column.
        """
        df = self.df_players.copy()
        probabilities = []
        
        for _, row in df.iterrows():
            simulated_points = np.where(row['homeTeam'] == 1,
                simulate_points(self.num_simulations,
                row['FGA_FTA'] * row['MINadj'] * row['PACEadj']  * row['PTSadj'] * row['PTS_HOMEadj'], #TODO Incorp home/road adj
                row['shotshareFG2A'], row['shotshareFG3A'], row['shotshareFTA'],
                row['FG2_PCT'], row['FG3_PCT'], row['FT_PCT']
                ),
                simulate_points(self.num_simulations,
                row['FGA_FTA'] * row['MINadj'] * row['PACEadj']  * row['PTSadj'] * row['PTS_ROADadj'], #TODO Incorp home/road adj
                row['shotshareFG2A'], row['shotshareFG3A'], row['shotshareFTA'],
                row['FG2_PCT'], row['FG3_PCT'], row['FT_PCT']
                )
            )

            # Compute probability of scoring at least X points
            prob = np.mean(simulated_points >= row['pts_line'])
            probabilities.append(prob)

        df.loc[:,'PTSoProb'] = probabilities
        df.loc[:,'PTSoOdds'] =  df['PTSoProb'].apply(convert_probability_to_ameri_odds)
        df.loc[:,'PTSoOdds_deci'] = df['PTSoProb'].apply(convert_probability_to_deci_odds)
        
        self.df_players = df.copy()
        return 

if __name__ == '__main__':

    model = playerStatModel(
        day_offset = 0, 
        season = '2024-25', 
        perMode = 'PerGame',
        num_simulations = 10000
    )
    # gathers game data for today +/- day_offset
    model.get_teams_playing()

    # add team pace data to game dataframe
    model.get_teams_data(
        base_url = 'https://stats.nba.com/stats/leaguedashteamstats?Conference=&DateFrom=&DateTo=&Division=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Advanced&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={sid}&SeasonSegment=&SeasonType=Regular%20Season&ShotClockRange=&StarterBench=&TeamID=0&TwoWay=0&VsConference=&VsDivision='
    )

    # gather base player stats into dataframe
    model.get_players_playing(
        url_base_nba_player_stat = 'https://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode={per}&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={sid}&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&StarterBench=&TeamID={tid}&TwoWay=0&VsConference=&VsDivision=&Weight=',
        minute_cutoff = 15.0
    )

    # retrieve props from database
    ### THIS WONT WORK WITHOUT ACCESS TO MY DATABASE
    model.get_props()
    
    # add home game % change in stats for prop categories
    model.get_player_home_adv(use_default=True)

    # model expected minutes
    model.model_expected_minutes()

    # add final model inputs 
    model.calculate_model_inputs()

    # model stats


