import pandas as pd
import numpy as np
import json, requests, time, re
from datetime import datetime

class nbaApi():
    def __init__(self, browser_path, database_export = False, store_locally=True, pymysql_conn_str = None):
        self.browser_path = browser_path
        self.database_export = database_export
        self.store_locally = store_locally
        
        # connection string
        if pymysql_conn_str is None:
            
            #importing credentials from my txt file  
            # #TODO remove the hard coded path
            with open('../../../../Notes-General/config.txt', 'r') as f:
                creds = f.read()

            creds = json.loads(creds)
            league = "nba"
            self.pymysql_conn_str = creds['pymysql'][league]
            del creds

        else:
            self.pymysql_conn_str = pymysql_conn_str
        
        # this will hold data from all scrapes if they are stored locally 
        self.data_all = {}

        # this will hold scrapes that errored out
        self.scrape_error_flag = False
        self.scrape_errors = {}

        # meta data - run date, etc.. 
        self.meta_data = {
            'today_dt':datetime.today().date(),
            'today':datetime.today().strftime('%Y-%m-%d')
        }

        # regex replacement mapping used to make more joinable names
        self.suffix_replace = {
            "\\.":"", "`":"", "'":"",
            " III$":"", " IV$":"", " II$":"", " iii$":"", " ii$":"", " iv$":"", " v$":"", " V$":"",
            " jr$":"", " sr$":"", " jr.$":"", " sr.$":"", " Jr$":"", " Sr$":"", " Jr.$":"", " Sr.$":"", 
            " JR$":"", " SR$":"", " JR.$":"", " SR.$":"",
            "š":"s","ş":"s", "š":"s", 'š':"s", "š":"s",
            "ž":"z",
            "þ":"p","ģ":"g",
            "à":"a","á":"a","â":"a","ã":"a","ä":"a","å":"a",'ā':"a",
            "ç":"c",'ć':"c", 'č':"c",
            "è":"e","é":"e","ê":"e","ë":"e",'é':"e",
            "ì":"i","í":"i","î":"i","ï":"i", "İ":"I",	
            "ð":"o","ò":"o","ó":"o","ô":"o","õ":"o","ö":"o",'ö':"o",
            "ù":"u","ú":"u","û":"u","ü":"u","ū":"u",
            "ñ":"n","ņ":"n",
            "ý":"y",
            "Dario .*":"dario saric", "Alperen .*":"alperen sengun", "Luka.*amanic":"luka samanic"
        }
    
    ##################
    # general functions
    def export_database(self, dataframe, database_table, connection_string):

        try:
            dataframe.to_sql(
                name=database_table, 
                con=connection_string, 
                if_exists='append', 
                index=False)
            return 'successfully added data'
            
        except:
            # save all urls that failed
            message = 'database load failed'
            self.scrape_errors[database_table]['db'] = message
            return message

    def gen_self_dict_entry(self, database_table):
        self.scrape_errors[database_table] = {}
        self.scrape_errors[database_table]['url'] = []
        self.scrape_errors[database_table]['db'] = []

    def apply_regex_replacements(self, value):
        """
        used to format names into their most joinable form
        """
        for pattern, replacement in self.suffix_replace.items():
            value = re.sub(pattern, replacement, value, flags=re.IGNORECASE)
        return value

    def nba_api_get_to_dataframe(self, url):
        nba_headers = {
                'Host': 'stats.nba.com',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0',
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br, zstd',
                'Referer': 'https://www.nba.com/',
                'Origin': 'https://www.nba.com',
                'DNT': '1',
                'Sec-GPC': '1',
                'Connection': 'keep-alive',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-site',
                'Priority': 'u=4'
            }

        response = requests.get(url = url, headers = nba_headers)
        # most responses come back in this format
        try:
            columns = response.json()['resultSets'][0]['headers']
            data = response.json()['resultSets'][0]['rowSet']
            
        # some have a multi-index and the actual column names are in the second level
        except:
            columns = response.json()['resultSets']['headers'][1]['columnNames']
            data = response.json()['resultSets']['rowSet']
        df = pd.DataFrame(data = data, columns= columns)
        return df
    
    def get_last_game_date(self, season = '2024-25'):
        '''
        searches the season provided for all completed games and returns the dat of the last played
        '''
        url = 'https://stats.nba.com/stats/leaguegamefinder?Season={season}&PlayerOrTeam=T&LeagueId=00'.format(season=season)
        games = self.nba_api_get_to_dataframe(url = url)
    
        # Convert GAME_DATE to datetime and find the most recent date
        ##### this data returns all games for the season
        ##### SEASON_ID: is an identifier followed by starting year of season so xYYYY 
        ##### for 24-25 season: 12024 = preseason, 22024=reg, 32024=Allstar weekend , 
        ##### 42024 = playoffs, 52024 = play-ins, 62024 = IST championship 
        games.loc[:,'GAME_DATE'] = pd.to_datetime(games['GAME_DATE'])
        games = games[games['SEASON_ID'].astype(str).str.startswith(('2', '4', '5'))] # filter to regular season and all playoffs
        last_game_date = games['GAME_DATE'].max()

        self.games = games.copy()
        self.last_game_date = last_game_date.date()
        print("Last day with games played:", last_game_date.date())
        return 
    
    ################
    # nba com api hits

    ### --- API Endpoint synergyplaytypes --- ###
    #team
    def request_nba_team_playtype_data(self,
            season, #'YYYY-YY' 
            play_type = [
                'Isolation', 'Transition','PRBallHandler','PRRollman', 'Postup', 'Spotup', 
                'Handoff', 'Cut', 'OffScreen', 'OffRebound', 'Misc'
            ], 
            lid='00',
            per_mode = 'PerGame', #['Totals', 'PerGame'] 
            season_type = 'Regular+Season', #['Regular+Season', 'PlayIn', 'Playoffs']
            sob = ['offensive', 'defensive'],
            sleep_time = 2,
            database_table = None
        ):

        url_nba_playtype = """https://stats.nba.com/stats/synergyplaytypes?
            LeagueID={lid}&
            PerMode={perMode}&
            PlayType={playType}&
            PlayerOrTeam=T&
            SeasonType={seasonType}&
            SeasonYear={season}&
            TypeGrouping={sob}
        """
        
        all_data_teams = []

        # column names from the api response
        cols_keep_team = [
            'TEAM_NAME', 'GP', 'POSS','POSS_PCT','PPP','PTS','FGM', 'FGA','FG_PCT','EFG_PCT',
            'FT_POSS_PCT', 'TOV_POSS_PCT', 'SF_POSS_PCT', 'PLUSONE_POSS_PCT','SCORE_POSS_PCT',
            'PERCENTILE','TEAM_ID','TYPE_GROUPING','PLAY_TYPE'
        ]
        # column names for db and local df
        cols_teams_final = [
            'TEAM_NAME', 'GP', 'POSS','POSS_PCT','PPP','PTS','FGM', 'FGA','FG_PCT','EFG_PCT',
            'FT_POSS_PCT', 'TOV_POSS_PCT', 'SF_POSS_PCT', 'PLUSONE_POSS_PCT','SCORE_POSS_PCT',
            'PERCENTILE', 'tid', 'sob', 'play', 'scoreFreqRank', 'date'
        ]

        # add table to class storage dictionaries
        self.gen_self_dict_entry(database_table)
         
        for side_of_ball in sob:
            for play in play_type:
                url_temp = url_nba_playtype.strip().replace('\n','').replace(' ','').format(
                    lid = lid,
                    perMode = per_mode,
                    playType = play,
                    seasonType = season_type,
                    season = season,
                    sob = side_of_ball
                )

                # hit api and retrieve data
                df = self.nba_api_get_to_dataframe(url = url_temp)
 
                # arrange dataframe
                df = df[cols_keep_team]

                # add additional cols
                df.loc[:,'scoreFreqRank'] = df['SCORE_POSS_PCT'].rank(ascending=False)
                df.loc[:,'date'] = self.meta_data['today_dt'] 

                # rename for database
                df.columns = cols_teams_final

                # store in parent list to aggregate to all play types  to single df
                all_data_teams.append(df)

        #store locally - add to main class object holding all data
        df_all = pd.concat(all_data_teams)
        if self.store_locally:
            self.data_all[database_table] = df_all
        
        # send to db if required
        if self.database_export:
            self.export_database(df_all, database_table, self.pymysql_conn_str)

        print('retrieved nba team play types:', df_all['play'].nunique(), ', sob:', df_all['sob'].nunique(), ', teams:', df_all['tid'].nunique())
        return
    #player
    def request_nba_player_playtype_data(self,
            season, #'YYYY-YY' 
            play_type = [
                'Isolation', 'Transition','PRBallHandler','PRRollman', 'Postup', 'Spotup', 
                'Handoff', 'Cut', 'OffScreen', 'OffRebound', 'Misc'
            ], 
            lid='00',
            per_mode = 'PerGame', #['Totals', 'PerGame']
            season_type = 'Regular+Season', #['Regular+Season', 'PlayIn', 'Playoffs']
            sob = ['offensive'], #['offensive','defensive'],
            sleep_time = 2,
            database_table = None
        ):

        url_nba_playtype = """https://stats.nba.com/stats/synergyplaytypes?
            LeagueID={lid}&
            PerMode={perMode}&
            PlayType={playType}&
            PlayerOrTeam=P&
            SeasonType={seasonType}&
            SeasonYear={season}&
            TypeGrouping={sob}
        """
        all_data_players = []

        # column names from the api response
        cols_keep_players = [
            'PLAYER_NAME', 'TEAM_ABBREVIATION', 'GP', 'POSS', 'POSS_PCT', 'PPP',
            'PTS', 'FGM', 'FGA', 'FG_PCT', 'EFG_PCT', 'FT_POSS_PCT', 'TOV_POSS_PCT',
            'SF_POSS_PCT', 'PLUSONE_POSS_PCT', 'SCORE_POSS_PCT', 'PERCENTILE',
            'PLAYER_ID', 'TEAM_ID', 'TYPE_GROUPING', 'PLAY_TYPE'
        ]
        # column names for the db and local df
        cols_players_final = [
            'PLAYER_NAME', 'TEAM_ABBREVIATION', 'GP', 'POSS', 'POSS_PCT', 'PPP',
            'PTS', 'FGM', 'FGA', 'FG_PCT', 'EFG_PCT', 'FT_POSS_PCT', 'TOV_POSS_PCT',
            'SF_POSS_PCT', 'PLUSONE_POSS_PCT', 'SCORE_POSS_PCT', 'PERCENTILE',
            'pid', 'tid', 'sob', 'play', 'scoreFreqRank', 'freqRank', 'date'
        ]

        # add table to class storage dictionaries
        self.gen_self_dict_entry(database_table)
         
        for side_of_ball in sob:
            for play in play_type:
                url_temp = url_nba_playtype.strip().replace('\n','').replace(' ','').format(
                    lid = lid,
                    perMode = per_mode,
                    playType = play,
                    seasonType = season_type,
                    season = season,
                    sob = side_of_ball
                )

                # hit api and retrieve data
                df = self.nba_api_get_to_dataframe(url = url_temp)

                # arrange cols
                df = df[cols_keep_players]

                # add additional cols
                df.loc[:,'scoreFreqRank'] = df['SCORE_POSS_PCT'].rank(ascending=False)
                df.loc[:,'freqRank'] = df['POSS_PCT'].rank(ascending=False)
                df.loc[:,'date'] = self.meta_data['today_dt'] 

                # rename for database
                df.columns = cols_players_final

                # store in parent list to aggregate to all play types  to single df
                all_data_players.append(df)

                time.sleep(sleep_time)

        
        #store locally - add to main class object holding all data
        df_all = pd.concat(all_data_players)
        if self.store_locally:
            self.data_all[database_table] = df_all
        
        # send to db if required
        if self.database_export:
            self.export_database(df_all, database_table, self.pymysql_conn_str)

        print('retrieved nba player play types:', df_all['play'].nunique(), ', sob:', df_all['sob'].nunique(), ', players:', df_all['pid'].nunique())
        return

    ### --- API Endpoint leaguedashteamshotlocations --- ###
    #team
    def request_nba_team_shotzone_data(self,
            season, #'YYYY-YY'
            start_date, #'MM/DD/YYY'
            end_date, 
            per_mode = 'Totals', #['Totals', 'PerGame'] 
            season_type = 'Regular+Season', #['Regular+Season', 'PlayIn', 'Playoffs']
            distance_type = 'By+Zone', # ['8ft+Range', '5ft+Range','By+Zone'],
            sob = ['Base', 'Opponent'], #['Base', 'Opponent']  base= teams offense, opponent = teams defense
            database_table = None        
    ):
        map_sob = {'Base':'offensive', 'Opponent':'defensive'}
        all_data_team = []

        # force dates to date object
        start_date = pd.to_datetime(start_date).date()
        end_date = pd.to_datetime(end_date).date()

        url_nba_team_shotzone = """https://stats.nba.com/stats/leaguedashteamshotlocations?
            Conference=&DateFrom={startDate}&DateTo={endDate}&DistanceRange={distance}&
            Division=&GameScope=&GameSegment=&ISTRound=&LastNGames=0&Location=&
            MeasureType={measureType}&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&
            PerMode={perMode}&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&
            Rank=N&Season={season}&SeasonSegment=&SeasonType={seasonType}&
            ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=
        """
        # column names from this api hit are all dups. have to override at the start
        cols_rename_team = [
            'TEAM_ID','TEAM_NAME','paintRaFgm', 'paintRaFga', 'paintRaFgPct','paintNoRaFgm','paintNoRaFga','paintNoRaFgPct',
            'midFgm','midFga','midFgPct','leftC3Fgm','leftC3Fga','leftC3FgPct','rightC3Fgm','rightC3Fga',
            'rightC3FgPct','aboveBreak3Fgm','aboveBreak3Fga','aboveBreak3FgPct',
            'backcourt3Fgm','backcourt3Fga','backcourt3FgPct', # not saving these
            'bothC3Fgm','bothC3Fga','bothC3FgPct' 
        ]
        cols_keep_team = [
            'TEAM_NAME','paintRaFgm', 'paintRaFga', 'paintRaFgPct','paintNoRaFgm','paintNoRaFga','paintNoRaFgPct',
            'midFgm','midFga','midFgPct','leftC3Fgm','leftC3Fga','leftC3FgPct','rightC3Fgm','rightC3Fga',
            'rightC3FgPct','bothC3Fgm','bothC3Fga','bothC3FgPct','aboveBreak3Fgm','aboveBreak3Fga','aboveBreak3FgPct',
            #'backcourt3Fgm','backcourt3Fga','backcourt3FgPct', # not saving these
            'TEAM_ID'
        ]
        #column names for database and local df
        cols_teams_final = [
            'TEAM_NAME','paintRaFgm', 'paintRaFga', 'paintRaFgPct','paintNoRaFgm','paintNoRaFga','paintNoRaFgPct',
            'midFgm','midFga','midFgPct','leftC3Fgm','leftC3Fga','leftC3FgPct','rightC3Fgm','rightC3Fga',
            'rightC3FgPct','bothC3Fgm','bothC3Fga','bothC3FgPct','aboveBreak3Fgm','aboveBreak3Fga','aboveBreak3FgPct',
            'tid','sob','paintAllFgm','paintAllFga','paintAllFgPct', 'date'
        ]

        # add table to class storage dictionaries
        self.gen_self_dict_entry(database_table)

        for side_of_ball in sob: 
            url_temp = url_nba_team_shotzone.strip().replace('\n','').replace(' ','').format(
                startDate = start_date.strftime('%m/%d/%Y'),
                endDate = end_date.strftime('%m/%d/%Y'),
                distance = distance_type,
                measureType = side_of_ball,
                perMode = per_mode,
                season = season,
                seasonType = season_type
            )

            # hit api and retrieve data
            df = self.nba_api_get_to_dataframe(url = url_temp)

            # arrange dataframe
            df.columns = cols_rename_team
            df = df[cols_keep_team]

            # add additional cols
            df.loc[:,'sob'] = map_sob[side_of_ball]
            # calculate all points in the paint restricted area + non-ra
            df.loc[:,'paintAllFgm'] =  df['paintRaFgm'] + df['paintNoRaFgm']
            df.loc[:,'paintAllFga'] =  df['paintRaFga'] + df['paintNoRaFga']
            df.loc[:,'paintAllFgPct'] =  round((df['paintAllFgm'] / df['paintAllFga']) * 100,3)

            df.loc[:,'date'] = end_date
            df = df.replace('-',0)
            df.columns = cols_teams_final

            all_data_team.append(df)
        
        df = pd.concat(all_data_team, ignore_index=True)
    
        #store locally - add to main class object holding all data
        if self.store_locally:
            self.data_all[database_table] = df
        
        # send to db if required
        if self.database_export:
            self.export_database(df, database_table, self.pymysql_conn_str)
        
        defCount = df[df['sob']=='offensive'].shape[0]
        offCount = df[df['sob']=='defensive'].shape[0]
        print('nba team shot zone retrieved', offCount, 'offensive, ', defCount, 'defensive loaded...')
        return
    
    ### --- API Endpoint leaguedashplayershotlocations --- ###
    #player
    def request_nba_player_shotzone_data(self,
            season, #'YYYY-YY'
            start_date, #'MM/DD/YYY'
            end_date, 
            per_mode = 'Totals', #['Totals', 'PerGame'] 
            season_type = 'Regular+Season', #['Regular+Season', 'PlayIn', 'Playoffs']
            distance_type = 'By+Zone', # ['8ft+Range', '5ft+Range','By+Zone'],
            sob = ['Base'], #['Base', 'Opponent']  base= teams offense, opponent = teams defense
            database_table = None        
    ):
        map_sob = {'Base':'offensive', 'Opponent':'defensive'}
        all_data = []

        # force dates to date object
        start_date = pd.to_datetime(start_date).date()
        end_date = pd.to_datetime(end_date).date()

        url_nba = """https://stats.nba.com/stats/leaguedashplayershotlocations?
            College=&Conference=&Country=&DateFrom={startDate}&DateTo={endDate}&DistanceRange={distance}&
            Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&ISTRound=&LastNGames=0&Location=&
            MeasureType={measureType}&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode={perMode}&
            Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&
            SeasonType={seasonType}&ShotClockRange=&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight= 
        """

        # column names from this api hit are all dups. have to override at the start
        cols_rename = [
            'PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID','TEAM_ABBREVIATION', 'AGE','NICKNAME',
            'paintRaFgm', 'paintRaFga', 'paintRaFgPct','paintNoRaFgm','paintNoRaFga','paintNoRaFgPct',
            'midFgm','midFga','midFgPct','leftC3Fgm','leftC3Fga','leftC3FgPct','rightC3Fgm','rightC3Fga',
            'rightC3FgPct','aboveBreak3Fgm','aboveBreak3Fga','aboveBreak3FgPct',
            'backcourt3Fgm','backcourt3Fga','backcourt3FgPct', # not saving these
            'bothC3Fgm','bothC3Fga','bothC3FgPct' 
        ]
        cols_keep = [
            'PLAYER_NAME', 'TEAM_ABBREVIATION', 'AGE','paintRaFgm', 'paintRaFga', 'paintRaFgPct',
            'paintNoRaFgm','paintNoRaFga','paintNoRaFgPct','midFgm','midFga','midFgPct',
            'leftC3Fgm','leftC3Fga','leftC3FgPct','rightC3Fgm','rightC3Fga','rightC3FgPct',
            'bothC3Fgm','bothC3Fga','bothC3FgPct','aboveBreak3Fgm','aboveBreak3Fga','aboveBreak3FgPct',
            #'backcourt3Fgm','backcourt3Fga','backcourt3FgPct', # not saving these
            'PLAYER_ID','TEAM_ID'
        ]
        #column names for database and local df
        cols_final = [
            'PLAYER_NAME','TEAM', 'AGE','paintRaFgm', 'paintRaFga', 'paintRaFgPct',
            'paintNoRaFgm','paintNoRaFga','paintNoRaFgPct','midFgm','midFga','midFgPct',
            'leftC3Fgm','leftC3Fga','leftC3FgPct','rightC3Fgm','rightC3Fga','rightC3FgPct',
            'bothC3Fgm','bothC3Fga','bothC3FgPct','aboveBreak3Fgm','aboveBreak3Fga','aboveBreak3FgPct',
            'pid','tid','paintAllFgm','paintAllFga','paintAllFgPct', 'date'
        ]

        # add table to class storage dictionaries
        self.gen_self_dict_entry(database_table)

        for side_of_ball in sob: 
            url_temp = url_nba.strip().replace('\n','').replace(' ','').format(
                startDate = start_date.strftime('%m/%d/%Y'),
                endDate = end_date.strftime('%m/%d/%Y'),
                distance = distance_type,
                measureType = side_of_ball,
                perMode = per_mode,
                season = season,
                seasonType = season_type
            )

            # hit api and retrieve data
            df = self.nba_api_get_to_dataframe(url = url_temp)

            # arrange dataframe
            df.columns = cols_rename
            df = df[cols_keep]

            # add additional cols
            #df.loc[:,'sob'] = map_sob[side_of_ball]
            # calculate all points in the paint restricted area + non-ra
            df.loc[:,'paintAllFgm'] =  df['paintRaFgm'] + df['paintNoRaFgm']
            df.loc[:,'paintAllFga'] =  df['paintRaFga'] + df['paintNoRaFga']
            df.loc[:,'paintAllFgPct'] =  round((df['paintAllFgm'] / df['paintAllFga']) * 100,3)

            df.loc[:,'date'] = end_date
            df = df.replace('-',0)
            df.columns = cols_final

            all_data.append(df)
        
        df = pd.concat(all_data, ignore_index=True)
    
        #store locally - add to main class object holding all data
        if self.store_locally:
            self.data_all[database_table] = df
        
        # send to db if required
        if self.database_export:
            self.export_database(df, database_table, self.pymysql_conn_str)
        
        #defCount = df[df['sob']=='defensive'].shape[0]
        #offCount = df[df['sob']=='offensive'].shape[0]
        print('nba player shot zone retrieved', df.shape[0], 'offensive loaded...')
        return
    
    ### --- API Endpoint leaguedashteamstats --- ###
    #team
    def request_nba_team_stats(self,
            season, #'YYYY-YY'
            start_date, #'MM/DD/YYY'
            end_date, 
            per_mode = 'Totals', #['Totals', 'PerGame'] 
            season_type = 'Regular+Season', #['Regular+Season', 'PlayIn', 'Playoffs']
            measure_type = ['Base', 'Advanced', 'Opponent'], #['Base', 'Advanced', 'Opponent']  
            database_table = None
    ):
        map_measure = {'Base':'traditional', 'Advanced':'advanced', 'Opponent':'opponent'}
        team_data = {}

        # force dates to date object
        start_date = pd.to_datetime(start_date).date()
        end_date = pd.to_datetime(end_date).date()

        url_nba = """https://stats.nba.com/stats/leaguedashteamstats?
            Conference=&DateFrom={startDate}&DateTo={endDate}&Division=&GameScope=&GameSegment=&Height=&
            ISTRound=&LastNGames=0&LeagueID=00&Location=&MeasureType={measureType}&Month=0&OpponentTeamID=0&
            Outcome=&PORound=0&PaceAdjust=N&PerMode={perMode}&Period=0&PlayerExperience=&PlayerPosition=&
            PlusMinus=N&Rank=N&Season={season}&SeasonSegment=&SeasonType={seasonType}&ShotClockRange=&
            StarterBench=&TeamID=0&TwoWay=0&VsConference=&VsDivision=
        """

        # column names from this api hit are all dups. have to override at the start
        #col names
        cols_keep = {
            'traditional':[
                'TEAM_NAME', 'GP', 'W', 'L', 'W_PCT', 'MIN', 'FGM', 'FGA',
                'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 'OREB',
                'DREB', 'REB', 'AST', 'TOV', 'STL', 'BLK', 'BLKA', 'PF', 'PFD', 'PTS',
                'PLUS_MINUS', 'TEAM_ID'
            ],
            'advanced':[
                'TEAM_NAME', 'GP', 'W', 'L', 'MIN',
                'OFF_RATING',  'DEF_RATING', 'NET_RATING', 'AST_PCT', 'AST_TO', 'AST_RATIO', 
                'OREB_PCT', 'DREB_PCT', 'REB_PCT', 'TM_TOV_PCT', 'EFG_PCT', 'TS_PCT', 'PACE',
                'PIE', 'POSS', 'TEAM_ID'
            ],
            'opponent':[
                'TEAM_NAME', 'GP', 'W', 'L', 'MIN', 'OPP_FGM', 'OPP_FGA', 'OPP_FG_PCT', 
                'OPP_FG3M', 'OPP_FG3A', 'OPP_FG3_PCT','OPP_FTM', 'OPP_FTA', 'OPP_FT_PCT', 
                'OPP_OREB', 'OPP_DREB', 'OPP_REB','OPP_AST', 'OPP_TOV', 'OPP_STL', 'OPP_BLK', 
                'OPP_BLKA', 'OPP_PF','OPP_PFD', 'OPP_PTS', 'PLUS_MINUS', 'TEAM_ID'
            ]
        }
        
        cols_final = {
            'traditional':[
                    'team', 'gp', 'w', 'l', 'winPct', 'min', 'pts', 'fgm', 'fga', 'fgPct', '3pm',
                    '3pa', '3pPct',	'ftm', 'fta','ftPct', 'oreb', 'dreb', 'reb', 'ast', 'to', 'stl', 
                    'blk', 'blka', 'pf', 'pfd', 'plusMinus', 'tid', 'date'
            ],
            'advanced':[
                    'team', 'gp', 'w', 'l', 'min', 'offrtg', 'defrtg', 'netrtg', 'astPct', 
                    'astToRatio', 'astRatio', 'orebPct', 'drebPct', 'rebPct', 'toPct', 'efgPct', 
                    'tsPct', 'pace', 'pie', 'poss', 'tid', 'date'
            ],
            'opponent':[
                    'team', 'gp', 'w', 'l', 'min', 'oppFgm', 'oppFga', 'oppfgPct','opp3pm',
                    'opp3pa','opp3pPct', 'oppFtm', 'oppFta', 'oppFtPct', 'oppOreb', 'oppDreb', 
                    'oppReb', 'oppAst', 'oppTo','oppStl', 'oppBlk', 'oppBlka', 'oppPf','oppPfd', 
                    'oppPts', 'oppPlusMinsus', 'tid', 'date'
            ]
        }

        # add table to class storage dictionaries
        self.gen_self_dict_entry(database_table)

        for measure in measure_type:
            stat_type = map_measure[measure]

            url_temp = url_nba.strip().replace('\n','').replace(' ','').format(
                startDate = start_date.strftime('%m/%d/%Y'),
                endDate = end_date.strftime('%m/%d/%Y'),
                measureType = measure,
                perMode = per_mode,
                season = season,
                seasonType = season_type
            )

            # hit api and retrieve data
            df = self.nba_api_get_to_dataframe(url = url_temp)

            # arrange dataframe
            #df.loc[:,'idx'] = 0
            df = df[cols_keep[stat_type]]
            df['date'] = end_date
            # rename the columns
            df.columns = cols_final[stat_type]

            team_data[stat_type] = df

        # assigned the 3 api hits into dfs, time to combine
        df_combined= team_data['traditional'].copy()
        for i in measure_type:
            stat_type = map_measure[i]
            if stat_type == 'traditional':
                pass
            else:
                temp = team_data[stat_type].copy()
                #temp = temp.drop(['idx', 'team', 'gp', 'w', 'l', 'min'], axis = 1)
                temp = temp.drop(['team', 'gp', 'w', 'l', 'min'], axis = 1)
                df_combined = df_combined.merge(temp, on=['tid', 'date'], how='left')
        
        df_combined = df_combined.replace('-', 0)

        #store locally - add to main class object holding all data
        if self.store_locally:
            self.data_all[database_table] = df_combined
        
        # send to db if required
        if self.database_export:
            self.export_database(df_combined, database_table, self.pymysql_conn_str)
        
        print('nba team stats', per_mode,'retrieved', df_combined.shape, 'loaded...')
        return

    ### --- API Endpoint leaguedashptstats --- ###
    #player
    def request_nba_player_tracking(self,
        season, #'YYYY-YY'
        start_date, #'MM/DD/YYY'
        end_date, 
        per_mode = 'Totals', #['Totals', 'PerGame'] 
        season_type = 'Regular+Season', #['Regular+Season', 'PlayIn', 'Playoffs']
        measure_type = None, #['Passing','Rebounding','Drives','Possessions','Efficiency']  
        database_table = None                                
    ):
        # force dates to date object
        start_date = pd.to_datetime(start_date).date()
        end_date = pd.to_datetime(end_date).date()

        url_nba = """https://stats.nba.com/stats/leaguedashptstats?
            College=&Conference=&Country=&DateFrom={startDate}&DateTo={endDate}&Division=&DraftPick=&
            DraftYear=&GameScope=&Height=&ISTRound=&LastNGames=0&LeagueID=00&Location=&Month=0&
            OpponentTeamID=0&Outcome=&PORound=0&PerMode={perMode}&PlayerExperience=&PlayerOrTeam=Player&
            PlayerPosition=&PtMeasureType={measureType}&Season={season}&SeasonSegment=&
            SeasonType={seasonType}&StarterBench=&TeamID=0&VsConference=&VsDivision=&Weight=
        """
        cols_keep = {
            'Passing':[
                'PLAYER_NAME', 'TEAM_ABBREVIATION', 'GP', 'W', 'L', 'MIN', 'PASSES_MADE', 
                'PASSES_RECEIVED', 'AST', 'FT_AST','SECONDARY_AST', 'POTENTIAL_AST', 
                'AST_PTS_CREATED', 'AST_ADJ','AST_TO_PASS_PCT', 'AST_TO_PASS_PCT_ADJ',
                'PLAYER_ID', 'TEAM_ID'
            ],
            'Rebounding':[
                'PLAYER_NAME', 'TEAM_ABBREVIATION', 'GP', 'W','L', 'MIN',
                #'OREB', 'OREB_CONTEST', 'OREB_UNCONTEST','OREB_CONTEST_PCT', 'OREB_CHANCES', 
                # 'OREB_CHANCE_PCT','OREB_CHANCE_DEFER', 'OREB_CHANCE_PCT_ADJ', 'AVG_OREB_DIST', 
                # 'DREB','DREB_CONTEST', 'DREB_UNCONTEST', 'DREB_CONTEST_PCT', 'DREB_CHANCES',
                #'DREB_CHANCE_PCT', 'DREB_CHANCE_DEFER', 'DREB_CHANCE_PCT_ADJ','AVG_DREB_DIST', 
                'REB', 'REB_CONTEST', 
                #'REB_UNCONTEST',
                'REB_CONTEST_PCT', 'REB_CHANCES', 'REB_CHANCE_PCT', 'REB_CHANCE_DEFER','REB_CHANCE_PCT_ADJ', 
                'AVG_REB_DIST','PLAYER_ID', 'TEAM_ID'
            ]
        }
        cols_final = {
            'Passing':[
                'PLAYER_NAME', 'TEAM_ABBREVIATION', 'GP', 'W', 'L', 'MIN', 'PASSES_MADE', 
                'PASSES_RECEIVED', 'AST', 'FT_AST','SECONDARY_AST', 'POTENTIAL_AST', 
                'AST_PTS_CREATED', 'AST_ADJ','AST_TO_PASS_PCT', 'AST_TO_PASS_PCT_ADJ',
                'pid', 'tid', 'date'
            ],
            'Rebounding':[
                'PLAYER_NAME', 'TEAM_ABBREVIATION', 'GP', 'W','L', 'MIN',
                #'OREB', 'OREB_CONTEST', 'OREB_UNCONTEST','OREB_CONTEST_PCT', 'OREB_CHANCES', 
                # 'OREB_CHANCE_PCT','OREB_CHANCE_DEFER', 'OREB_CHANCE_PCT_ADJ', 'AVG_OREB_DIST', 
                # 'DREB','DREB_CONTEST', 'DREB_UNCONTEST', 'DREB_CONTEST_PCT', 'DREB_CHANCES',
                #'DREB_CHANCE_PCT', 'DREB_CHANCE_DEFER', 'DREB_CHANCE_PCT_ADJ','AVG_DREB_DIST', 
                'REB', 'REB_CONTEST', 
                #'REB_UNCONTEST',
                'REB_CONTEST_PCT', 'REB_CHANCES', 'REB_CHANCE_PCT', 'REB_CHANCE_DEFER','REB_CHANCE_PCT_ADJ', 
                'AVG_REB_DIST','pid', 'tid', 'date'
            ]
        }

        # add table to class storage dictionaries
        self.gen_self_dict_entry(database_table)
  
        url_temp = url_nba.strip().replace('\n','').replace(' ','').format(
            startDate = start_date.strftime('%m/%d/%Y'),
            endDate = end_date.strftime('%m/%d/%Y'),
            measureType = measure_type,
            perMode = per_mode,
            season = season,
            seasonType = season_type
        )

        # hit api and retrieve data
        df = self.nba_api_get_to_dataframe(url = url_temp)

        # arrange dataframe
        df = df[cols_keep[measure_type]]
        df.loc[:,'date'] = end_date

        # rename the columns
        df.columns = cols_final[measure_type]

        #store locally - add to main class object holding all data
        if self.store_locally:
            self.data_all[database_table] = df
        
        # send to db if required
        if self.database_export:
            self.export_database(df, database_table, self.pymysql_conn_str)
        
        print('nba player', measure_type, 'retrieved', df.shape[0], 'loaded...')
        return
    
    # TODO
    # https://www.nba.com/stats/players/shots-general
    # https://www.nba.com/stats/players/shots-dribbles
    # https://www.nba.com/stats/players/shots-touch-time
    # https://www.nba.com/stats/players/shots-closest-defender
    # https://www.nba.com/stats/players/hustle
    # https://www.nba.com/stats/players/box-outs
    # 