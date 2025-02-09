import time, json, requests
import pandas as pd
import numpy as np
from datetime import datetime

### # nba_api is not friendly, changed it direct url hit ###
#from nba_api.stats.endpoints import LeagueDashPlayerStats
#from nba_api.stats.endpoints import ScoreboardV2
from nba_api.stats.endpoints import PlayerDashboardByGeneralSplits

#TODO:
###### immediate
### CHeck stat level home/away aggregation and merg

####### longer term
### add minutes adjustment ( fade expected minutes if injured or increase if increasing role)
### explore penalizing early and afternoon games 1 - 6pm 

def calculate_pace_adjustment(team_pace, opp_pace):
    # uses geometric mean to calulate pace adjuster
    return np.sqrt(team_pace * opp_pace) / opp_pace

class playerStatModel():
    
    def __init__(self, day_offset = 0, season = '2024-25', perMode = 'PerGame'):
        

        self.day_offset = day_offset
        self.season = season
        self.perMode = perMode ##['Per100Possessions', 'Totals', 'Per36', 'PerGame']
        self.game_search_date = datetime.today().strftime('%m/%d/%Y')

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
        url = 'https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json'      
    ):
        """
        get the teams playing on the date; date = today +/- day_offset

        assigns list of team ids and dataframe with minimal game detail to class object
        return nothing
        """

        
        '''
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
        filtered_headers = [headers_scoreboard[i] for i in [0,2,4,6,7,8]]

        # get data of interest and extract necessary info
        games = data['resultSets'][0]['rowSet']
        filtered_games = [[game[0], game[2], game[4], game[6], game[7], game[8]] for game in games]
        df_games =  pd.DataFrame(filtered_games, columns=filtered_headers)
        del games, headers_scoreboard, raw_data, filtered_games
        '''

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
    def get_teams_pace(self,
        base_url = 'https://stats.nba.com/stats/leaguedashteamstats?Conference=&DateFrom=&DateTo=&Division=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Advanced&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={sid}&SeasonSegment=&SeasonType=Regular%20Season&ShotClockRange=&StarterBench=&TeamID=0&TwoWay=0&VsConference=&VsDivision='
    ):
        url_team_pace = base_url.format(sid=self.season)
        response = requests.get(url=url_team_pace, headers=self.HEADERS).json()

        # generate data frame
        teams_pace = []
        for i in response['resultSets'][0]['rowSet']:
            team_pace = [i[0], i[23]]
            teams_pace.append(team_pace)

        df_pace = pd.DataFrame(teams_pace, columns=['TEAM_ID', 'PACE'])

        # join pace data back to class team dataframe
        self.df_games_long = pd.merge(self.df_games_long, df_pace, how='left', on='TEAM_ID')
        self.pace_gathered = True
        return

    def get_players_playing(self,
        # use format to input perMode (per), season id (sid), and team id (tid)
        url_base_nba_player_stat = 'https://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode={per}&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={sid}&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&StarterBench=&TeamID={tid}&TwoWay=0&VsConference=&VsDivision=&Weight=',
        use_default_home_adj = True
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
                player_stats_list.append(j)
            
            time.sleep(1)

        columns_players_keep = [
            'PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBREVIATION', 'MIN', 'FGM', 'FGA', 
            'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB', 
            'AST','TOV', 'STL', 'BLK', 'BLKA', 'PF', 'PFD', 'PTS', 'PLUS_MINUS'
        ]
        df_players = pd.DataFrame(player_stats_list, columns = columns_player_stats)
        df_players = df_players[df_players['TEAM_ID'].isin(teams)][columns_players_keep]
        self.list_players = list(df_players['PLAYER_ID'].unique())

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
            cols = ['TEAM_ID', 'PACE']
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

        df_players.loc[:, 'PACEadj'] = df_players.apply(lambda x: calculate_pace_adjustment(x['PACE'], x['oppPACE']), axis = 1)

        self.df_players = df_players
        print(
            df_players['PLAYER_ID'].nunique(), 'players returned from',
            df_players['TEAM_ID'].nunique(), 'teams, out of a total', len(teams)
        )
         

        self.get_player_home_adv(use_default = use_default_home_adj)
        return
    
    # 
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
            # gather required data to calculate home advantage
            all_splits = []

            for pid in self.list_players:
                splits = PlayerDashboardByGeneralSplits(
                    player_id = pid, 
                    season = self.season
                )
                
                df_splits = splits.get_data_frames()[1]  # 1 = Home/Away splits
                df_splits['PLAYER_ID'] = pid  # Add player ID for tracking
                all_splits.append(df_splits)

            # Combine all players into a single DataFrame
            df_location_splits = pd.concat(all_splits, ignore_index=True)

            columns_splits = [
                'PLAYER_ID', 'GROUP_VALUE','GP', 'MIN', 'FGM', 'FGA', 'FG3M', 'FG3A', 'FTM', 
                'FTA', 'OREB', 'DREB', 'REB', 'AST', 'TOV', 'STL', 'BLK', 'BLKA', 'PTS'
            ]

            df_location_splits = df_location_splits[columns_splits]

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

            # Compute Home - Road % delta in per-game stats
            for col in cols_to_avg:
                df_pivot[f'{col}_DIFF'] = (df_pivot[f'{col}_Home_per_G'] - df_pivot[f'{col}_Road_per_G']) / df_pivot[f'{col}_Home_per_G']

            # columns to keep then join to df_players
            cols_to_display = [f'{col}_DIFF' for col in cols_to_avg]
            cols_to_display.append('PLAYER_ID')

            # create df to join to players
            df_pivot = df_pivot[cols_to_display]

            # merge data
            self.df_players = pd.merge(self.df_players, df_pivot, how='left', on='PLAYER_ID')
            return


if __name__ == '__main__':

    model = playerStatModel(
        day_offset = 0, 
        season = '2024-25', 
        perMode = 'PerGame'
    )
    model.get_teams_playing()
    model.get_teams_pace(
        base_url = 'https://stats.nba.com/stats/leaguedashteamstats?Conference=&DateFrom=&DateTo=&Division=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType=Advanced&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={sid}&SeasonSegment=&SeasonType=Regular%20Season&ShotClockRange=&StarterBench=&TeamID=0&TwoWay=0&VsConference=&VsDivision='
    )
    model.get_players_playing(
        url_base_nba_player_stat = 'https://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode={per}&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={sid}&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&StarterBench=&TeamID={tid}&TwoWay=0&VsConference=&VsDivision=&Weight=',
        use_default_home_adj = True
    )
    model.get_player_home_adv(use_default=True)

