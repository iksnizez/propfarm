import json, time, requests, random
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from sqlalchemy import create_engine


class actNetApi:
    
    def __init__(
            self, 
            browser_path,
            dates, 
            leagues = None,
            database_export = False, 
            store_locally=True, 
            config_path = '../../../../Notes-General/config.txt',
            second_run = False,
        ):
        self.browser_path = browser_path
        self.database_export = database_export
        self.store_locally = store_locally
        self.config_path = config_path
        self.dates = dates
        self.second_run = second_run        
        
        # static info for league api's and json storage
        self.over_ids = {
            'nba': [42,34,40,30,36,38,341,346,345,344,343],
            'mlb':[506,498,508,56,54,58,52,60,100,500,62],
            'nhl':[50,64,564,568],
            'wnba':[42,40,34,30],
            'nfl':[400, 337, 335, 333, 406, 10, 6, 22, 20, 18, 402, 24, 14, 12, 404]
        }
        self.under_ids = {
            'nba':[43,35,41,31,37,39,342,350,349,348,347],
            'mlb':[507,499,509,57,53,59,55,61,101,501,63],
            'nhl':[51,65,565,569],
            'wnba':[43,41,35,31],
            'nfl':[401, 338, 336, 334, 407, 11, 7, 23, 21, 19, 403, 25, 15, 13, 405]
        }
        # bookid url params removed - bookIds=69,75,68,123,71,32,76,79,369,1599,1533,1900&
        # book ids 15:consenus, 369:mgm, 3585:cesaers
        # stateCode url param removed - stateCode={st}
        
        #'https://api.{site}.com/web/v2/scoreboard/{league}/markets?bookIds=369,2194,2292,3348,3585,3679&customPickTypes={propType}&date={date}',
        #self.urls = 'https://api.{site}.com/web/v2/scoreboard/{league}/markets?customPickTypes={propType}&date={date}'
        self.urls = 'https://api.{site}.com/web/v2/scoreboard/{league}/markets'
        self.params = {
            "bookIds": "15,30",
            "customPickTypes": None,
            "date": None #YYYYMMDD  
        }
        self.headers = {
            "Host": "api.actionnetwork.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:138.0) Gecko/20100101 Firefox/138.0",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Referer": "https://www.actionnetwork.com/{league}/props/",
            "Origin": "https://www.actionnetwork.com",
            "DNT": "1",
            "Sec-GPC": "1",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "Priority": "u=4",
            "TE": "trailers"
        }

        self.map_option_ids = {
            'nba':{
                'core_bet_type_27_points':{'o':42, 'u':43, 'type':'pts', 'html_str_responses':[]},
                'core_bet_type_23_rebounds':{'o':34, 'u':35, 'type':'reb', 'html_str_responses':[]},
                'core_bet_type_26_assists':{'o':40, 'u':41, 'type':'ast', 'html_str_responses':[]},
                'core_bet_type_21_3fgm':{'o':30, 'u':31, 'type':'threes', 'html_str_responses':[]},
                'core_bet_type_24_steals':{'o':36, 'u':37, 'type':'stl', 'html_str_responses':[]},
                'core_bet_type_25_blocks':{'o':38, 'u':39, 'type':'blk', 'html_str_responses':[]},
                'core_bet_type_85_points_rebounds_assists':{'o':341, 'u':342, 'type':'pra', 'html_str_responses':[]},
                'core_bet_type_86_points_rebounds':{'o':346, 'u':350, 'type':'pr', 'html_str_responses':[]},
                'core_bet_type_87_points_assists':{'o':345, 'u':349, 'type':'pa', 'html_str_responses':[]},
                'core_bet_type_88_rebounds_assists':{'o':344, 'u':348, 'type':'ra', 'html_str_responses':[]},
                'core_bet_type_89_steals_blocks':{'o':343, 'u':347, 'type':'sb', 'html_str_responses':[]}
            },
            'mlb':{
                'core_bet_type_37_strikeouts':{'o':62, 'u':63, 'type':'k', 'html_str_responses':[]},
                'core_bet_type_74_earned_runs':{'o':500, 'u':501, 'type':'er', 'html_str_responses':[]},
                'core_bet_type_42_pitching_outs':{'o':100, 'u':101, 'type':'o', 'html_str_responses':[]},
                'core_bet_type_36_hits':{'o':60, 'u':61, 'type':'h', 'html_str_responses':[]},
                'core_bet_type_32_singles':{'o':52, 'u':53, 'type':'single', 'html_str_responses':[]},
                'core_bet_type_35_doubles':{'o':58, 'u':59, 'type':'dbl', 'html_str_responses':[]},
                'core_bet_type_33_hr':{'o':54, 'u':55, 'type':'hr', 'html_str_responses':[]},
                'core_bet_type_34_rbi':{'o':56, 'u':57, 'type':'rbi', 'html_str_responses':[]},
                'core_bet_type_78_runs_scored':{'o':508, 'u':509, 'type':'rs', 'html_str_responses':[]},
                'core_bet_type_73_stolen_bases':{'o':498, 'u':499, 'type':'stlb', 'html_str_responses':[]},
                'core_bet_type_77_total_bases':{'o':506, 'u':507, 'type':'tb', 'html_str_responses':[]}
            },
            'nhl':{
                'core_bet_type_31_shots_on_goal':{'o':50, 'u':51, 'type':'sog', 'html_str_responses':[]},
                'core_bet_type_38_goaltender_saves':{'o':64, 'u':65, 'type':'gs', 'html_str_responses':[]},
                'core_bet_type_280_points':{'o':564, 'u':565, 'type':'pts', 'html_str_responses':[]},
                'core_bet_type_279_assists':{'o':568, 'u':569, 'type':'ast', 'html_str_responses':[]},
                'core_bet_type_313_anytime_goal_scorer':{'o':None, 'u':None, 'type':'ats', 'html_str_responses':[]},
                'core_bet_type_311_to_score_2_or_more_goals':{'o':None, 'u':None, 'type':'gs2plus', 'html_str_responses':[]},
                'core_bet_type_312_to_score_3_or_more_goals':{'o':None, 'u':None, 'type':'gs3plus', 'html_str_responses':[]},
                'core_bet_type_48_first_goal_scorer':{'o':None, 'u':None, 'type':'gs1st', 'html_str_responses':[]},
                'core_bet_type_310_last_goal_scorer':{'o':None, 'u':None, 'type':'gsLast', 'html_str_responses':[]},
            },
            'wnba':{
                'core_bet_type_27_points':{'o':42, 'u':43, 'type':'pts', 'html_str_responses':[]},
                'core_bet_type_23_rebounds':{'o':34, 'u':35, 'type':'reb', 'html_str_responses':[]},
                'core_bet_type_26_assists':{'o':40, 'u':41, 'type':'ast', 'html_str_responses':[]},
                'core_bet_type_21_3fgm':{'o':30, 'u':31, 'type':'threes', 'html_str_responses':[]},
                'core_bet_type_85_points_rebounds_assists':{'o':341, 'u':342, 'type':'pra', 'html_str_responses':[]},
                'core_bet_type_86_points_rebounds':{'o':346, 'u':350, 'type':'pr', 'html_str_responses':[]},
                'core_bet_type_87_points_assists':{'o':345, 'u':349, 'type':'pa', 'html_str_responses':[]},
                'core_bet_type_88_rebounds_assists':{'o':344, 'u':348, 'type':'ra', 'html_str_responses':[]},
                
            },
            'nfl':{
                'core_bet_type_62_anytime_touchdown_scorer':{'o':None, 'u':None, 'type':'tdOne', 'html_str_responses':[], 'id':2002},
                'core_bet_type_67_to_score_2_or_more_touchdowns':{'o':None, 'u':None, 'type':'tdTwo', 'html_str_responses':[], 'id':2829},
                'core_bet_type_68_to_score_3_or_more_touchdowns':{'o':None, 'u':None, 'type':'tdThree', 'html_str_responses':[], 'id':2830},
                'core_bet_type_56_first_touchdown_scorer':{'o':None, 'u':None, 'type':'tdFirst', 'html_str_responses':[], 'id':1936},
                'core_bet_type_60_longest_completion':{'o':337, 'u':338, 'type':'passLong', 'html_str_responses':[], 'id':1940},
                'core_bet_type_59_longest_reception':{'o':335, 'u':336, 'type':'recLong', 'html_str_responses':[], 'id':1939},
                'core_bet_type_58_longest_rush':{'o':333, 'u':334, 'type':'rushLong', 'html_str_responses':[], 'id':1938},
                'core_bet_type_71_passing_rushing_yards':{'o':406, 'u':407, 'type':'passRushYds', 'html_str_responses':[], 'id':2926},
                'core_bet_type_11_passing_tds':{'o':10, 'u':11, 'type':'passTds', 'html_str_responses':[], 'id':363},
                'core_bet_type_9_passing_yards':{'o':6, 'u':7, 'type':'passYds', 'html_str_responses':[], 'id':361},
                'core_bet_type_17_receiving_tds':{'o':22, 'u':23, 'type':'recTds', 'html_str_responses':[], 'id':369},
                'core_bet_type_16_receiving_yards':{'o':20, 'u':21, 'type':'recYds', 'html_str_responses':[], 'id':368},
                'core_bet_type_15_receptions':{'o':18, 'u':19, 'type':'rec', 'html_str_responses':[], 'id':367},
                'core_bet_type_66_rushing_receiving_yards':{'o':402, 'u':403, 'type':'recRushYds', 'html_str_responses':[], 'id':2828},
                'core_bet_type_18_rushing_attempts':{'o':24, 'u':25, 'type':'rushAtt', 'html_str_responses':[], 'id':370},
                'core_bet_type_13_rushing_tds':{'o':14, 'u':15, 'type':'rushTds', 'html_str_responses':[], 'id':365},
                'core_bet_type_12_rushing_yards':{'o':12, 'u':13, 'type':'rushYds', 'html_str_responses':[], 'id':364},
                'core_bet_type_70_tackles_assists':{'o':404, 'u':405, 'type':'tackles', 'html_str_responses':[], 'id':2893},
                'core_bet_type_65_interceptions':{'o':400, 'u':401, 'type':'int', 'html_str_responses':[], 'id':2827}
                # pass atm
                # pass comp
                # pat
                # fgs
            }
        }
        self.prop_names ={
            'nba':[
                'pts', 'reb', 'ast', 'threes','pra', 'pr', 'pa', 'ra', 'stl', 'blk', 'sb'
            ],
            'mlb':[
                'k', 'er', 'o', 'h', 'single', 'dbl', 'hr', 'rbi', 'rs', 'stlb', 'tb'
            ],
            'nhl':[
                'sog', 'gs', 'pts', 'ast', 'ats', 'gs2plus', 'gs3plus', 'gs1st', 'gsLast'
            ],
            'wnba':[
                'pts', 'reb', 'ast', 'threes','pra', 'pr', 'pa', 'ra' #, 'stl', 'blk', 'sb'
           
            ],
            'nfl':[
                'tdOne', 'tdTwo', 'tdThree', 'tdFirst', 'int' 'passLong', 'recLong', 'rushLong',
                'passRushYds','passTds','passYds','recTds', 'recYds', 'rec', 'recRushYds', 'rushAtt', 
                'rushTds', 'rushYds', 'tackles'
            ]
        }
        self.schedule_urls = {
            'mlb':'https://statsapi.mlb.com/api/v1/schedule?sportId=1&sportId=51&sportId=21&startDate={start}&endDate={end}&timeZone=America/New_York&gameType=E&&gameType=S&&gameType=R&&gameType=F&&gameType=D&&gameType=L&&gameType=W&&gameType=A&language=en&leagueId=104&&leagueId=103&&leagueId=160&&leagueId=590&&leagueId=&&leagueId=&sortBy=gameDate,gameType',
            'nfl':'https://site.web.api.espn.com/apis/personalized/v2/scoreboard/header?sport=football&league=nfl&region=us&lang=en&contentorigin=espn&configuration=SITE_DEFAULT&platform=web&buyWindow=1m&showAirings=buy%2Clive%2Creplay&showZipLookup=true&tz=America%2FNew_York&postalCode=20001&authNetworks=espn3',
            'nba':'https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json',
            'wnba':'https://cdn.wnba.com/static/json/liveData/scoreboard/todaysScoreboard_10.json',
            'nhl':'https://api-web.nhle.com/v1/schedule/{date}'
        }

        # statis vars for database maint
        self.columns_players = ['playerId', 'player', 'abbr']
        self.player_list = []
        self.players_avail = False
        self.update_players = True

        # scraped data storage variables
        self.data_all = {}

        # this will hold scrapes that errored out
        self.scrape_error_flag = False
        self.scrape_errors = {}

        self.run_date_str = datetime.today().strftime('%Y-%m-%d')

        # TODO this doesn't really handle when multiple dates are initiated. only good for a single date
        if (self.second_run) or leagues != None: #skips the game checks when they have already been checked
            self.leagues  = leagues
        else: # first run check for games
            self.leagues = self.check_for_league_games(date_check = datetime.today().strftime('%Y-%m-%d'), league_check_list = leagues)

    #############
    # general helper funcs
    def get_pymysql_conn_str(self, league, config_path = '../../../../Notes-General/config.txt'):
        
        with open(config_path, 'r') as f:
            creds = f.read()

        creds = json.loads(creds)
        league = league
        pymysql_conn_str = creds['pymysql'][league]
        del creds

        return pymysql_conn_str

    def gen_self_dict_entry(self, league_name):
        """
        used in scraping loop below to add keys for each league that is being scrapped to the error tracking vars
        """
        self.scrape_errors[league_name] = {}
        self.scrape_errors[league_name]['missing_dates'] = []
        self.scrape_errors[league_name]['missing_props'] = []
        self.scrape_errors[league_name]['db'] = []

    def check_for_league_games(self, date_check = None, league_check_list = None):
        """
            provide a date and return a list of the leagues with a game today. 
            checks for all 5 - NBA, WNBA, NFL, NHL, MLB
        """
        if date_check == None:
            date_check = self.run_date_str
        else:
            # check for string
            if type(date_check) != str:
                date_check = date_check.strftime('%Y-%m-%d')

        # if specific leagues aren't provided then it will check for all leagues.
        leagues = self.schedule_urls.items()
        # if specific leagues are provided then it will only run those that are provided
        if league_check_list != None:
            leagues = {key: leagues[key] for key in league_check_list if key in leagues}

        # will hold league names for one's that have a game on search date
        leagues_with_games = []
        
        for k, v in leagues:
            
            if len(v) == 0 or pd.isnull(v):
                continue

            else:
                url = v
                
                # wnba and nba
                if (k == 'wnba') or (k == 'nba'):

                    r = requests.get(url)
                    url_json = r.json()
                    games = url_json['scoreboard']['games']
                    if len(games) > 0:
                        leagues_with_games.append(k)

                elif k == 'mlb':
                    url = url.format(start = date_check, end = date_check)
                    r = requests.get(url)
                    url_json = r.json()
                    games = int(url_json['totalGames'])
                    if games > 0:
                        leagues_with_games.append(k)
                    
                elif k == 'nhl':
                
                    url = url.format(date = date_check)
                    r = requests.get(url)
                    url_json = r.json()

                    game_list = url_json.get('gameWeek')
                    for i in game_list:
                        
                        dt = i.get('date')
                        if dt == date_check:
                            
                            games = int(i.get('numberOfGames'))
                            if games > 0:
                                leagues_with_games.append(k)

                elif k == 'nfl':
                    # TODO going to need to see how this url looks in the offseason
                    r = requests.get(url)
                    url_json = r.json()

                    nfl_games_in_week = url_json.get('sports')[0].get('leagues')[0].get('events')

                    # loop through the games in the data returned for the week
                    for i in nfl_games_in_week:

                        # looks like dates and times are stored on greenwich/ UTC, adj back to our time zone for correct dates
                        hour_adj_to_eastern = -5
                        game_date_time = pd.to_datetime(i['date']) + timedelta(hours=hour_adj_to_eastern)
                        
                        # check if the game dates match the run date
                        if game_date_time.date().strftime('%Y-%m-%d') == date_check:
                            leagues_with_games.append(k)
                            break
        
        return leagues_with_games

    #############
    # site scraper

    # scrape website for dates and league
    def scrape(self, sleep_secs = 2, specific_props=[], leagues_override = None, an_state_code = 'LV'):
        
        if leagues_override == None:
            # stop the scraper if there are no league games today to avoid hitting the server
            if len(self.leagues) == 0:
                print('no league games today')
                return
            else:
                looper = self.leagues

        else:
            looper = leagues_override


        for i in looper:
            # generate class variable dictionary item for error tracking
            self.gen_self_dict_entry(i)

            # filtering props to scrape, class defaults to all props in init variable above
            if len(specific_props) > 0:
                # this will hold the props to keep
                keepers = []
                
                # loop throug the default prop dict so search for specified props
                for k, v in self.map_option_ids[i].items():
                    # check for props supplied as desired searches and append to list for last filter
                    if v['type'] in specific_props:
                        keepers.append(k)

                # update prop dictionary used in scraping code below   
                self.map_option_ids[i] = {k: v for k, v in self.map_option_ids[i].items() if k in keepers}
            

            league = i.lower()
            failed = []

            # looping through each prop type on the site  
            for pt in self.map_option_ids[league].keys():
                # looping through each date for a single prop in a the season
                for d in self.dates:
                    # formattin date to add to the url search params
                    frmt_date = d.replace("-", "")

                    # build url, params, headers for requests.get()
                    url = self.urls.format(site='actionnetwork', league = league)#, proptype= pt, date= frmt_date)
                    self.params['customPickTypes'] = pt
                    self.params['date'] = frmt_date
                    self.headers['Referer'] = self.headers['Referer'].format(league=league)

                    response = requests.get(url, headers=self.headers, params=self.params)

                    if response.status_code != 200:
                        print(league, d, pt, 'failed...')
                        failed.append([d, pt])
                    
                    data = response.json()
                    self.map_option_ids[league][pt]['html_str_responses'].append(data)

                    # sleep to not overload server with requests
                    time.sleep(sleep_secs)

        return      

    # process html
    def processScrapes(self, leagues_override = None, remove_dups = True, specific_props = []):
        
        if leagues_override == None:
            # stop the scraper if there are no league games today to avoid hitting the server
            if len(self.leagues) == 0:
                print('no league games today')
                return
            else:
                looper = self.leagues

        else:
            looper = leagues_override


        for league in looper:
            print('scraping', league, '...')
            league = league.lower()
            missing_dates = []

            columns = [
                'propId','playerId','teamId', 'gameId', 'date', 
                'prop', 'line', 'oOdds', 'uOdds', 'projValue', 
                'oImpValue', 'oEdge', 'oQual', 'oGrade',
                'uImpValue', 'uEdge', 'uQual', 'uGrade', 'actNetPropId'
            ]
            # df to hold all data from each loop below. 
            # doesn't need actNetPropId column, it is only used for the loop to combine over and unders
            df_props = pd.DataFrame(columns=columns[:-1])

            for i in self.map_option_ids[league].keys():
                # name of prop
                prop = self.map_option_ids[league][i]['type']
                
                # this will hold the player prop data. playerId = key, values = list of data
                all_props_single_type = {}
                
                # loop through each game date for each prop
                for d in range(0,len(self.dates)):
                    # game date
                    date = self.dates[d]
                    
                    json_single_date = self.map_option_ids[league][i]['html_str_responses'][d]

                    #checking if the consensus line was provided, 
                    # if not take first market in the list
                    if json_single_date['markets'].get('15') is None:
                        book = json_single_date['markets'][
                            list(json_single_date['markets'].keys())[0]  # first book in the returned data
                        ]['event'][i]
                    # use consensu book (15) when available
                    else:
                        book = json_single_date['markets']['15']['event'][i]

                    # looping through all of the props for a single type and single day
                    for j in book:

                        #props_single_date = []
                        entry = [np.nan] * (len(columns) - 1)

                        playerId = j['player_id']

                        # actnet propId are not unique in MLB or NHL, creating own propId later
                        actNetPropId = j['market_id']

                        ## creating custom propId
                        # random number doesn't work because the random num changes when having to combine over and under
                        #propId = int(str(j['prop_id']) + str(random.random())[2:6])
                        propId = int(str(j['market_id']) + str(playerId))
                                    
                        ou_check = j['option_type_id']

                        # if the player is not in this dict, it will be added. if it is in then
                        # only the odds that are not present will be added
                        if all_props_single_type.get(propId) is None:
                            
                            entry[0] = playerId
                            entry[1] = None # j['team_id']
                            entry[2] = j['event_id']
                            entry[3] = date
                            entry[4] = prop
                            entry[5] = j['value']  # line
                            entry[17] = actNetPropId # actnetpropId
                            
                            #act net removed projected value data from v2 api
                            entry[8] = np.nan
           
                            #overs and props that don't have over/under designated
                            if ou_check in self.over_ids[league] or ou_check not in self.under_ids[league]:
                                entry[6] = j['odds'] # over odds
                            
                                #act net removed projected value data from v2 api
                                entry[11] = np.nan
                                entry[9] = np.nan 
                                entry[10] = np.nan 
                                entry[12] = np.nan
                                    
                            #unders
                            else:
                                entry[7] = j['odds'] # under odds
                            
                                #act net removed projected value data from v2 api
                                entry[15] = np.nan
                                entry[13] = np.nan
                                entry[14] = np.nan
                                entry[16] = np.nan

                            # loading over and under data to the prop id
                            all_props_single_type[propId] = entry
                        
                        # adding the over or under to the existing propId key
                        else:
                            #overs
                            if ou_check in self.over_ids[league] or ou_check not in self.under_ids[league]:
                                all_props_single_type[propId][6] = j['odds'] # over odds
                            
                            #unders
                            else:
                                all_props_single_type[propId][7] = j['odds'] # under odds
                                       
                    try:
                        # gather player names
                        players = json_single_date['players']
                        for p in players:
                            player = [p['id'], p['full_name'], p['abbr']]#, p['team_id']]
                            self.player_list.append(player)
                        self.players_avail = True
                    except: 
                        continue

                # store the data from this loop                
                temp = pd.DataFrame(all_props_single_type.values(), 
                index=all_props_single_type.keys(), 
                columns=columns[1:]
                ).reset_index(names=['propId'])

                # overwrite the composite propId with the actnetId
                temp.loc[:,'propId'] = temp['actNetPropId']
                del temp['actNetPropId']

                # combine the data from the single loop with all the data    
                df_props = pd.concat([df_props, temp])
            
            if self.players_avail:
                # aggregating player to single df 
                df_players = pd.DataFrame(self.player_list, columns=self.columns_players)           
                df_players.drop_duplicates('playerId', inplace=True)
                
                # merge player names to odds
                df_props = df_props.merge(df_players, on= 'playerId')


            # when pulling data after the league games start for the day then live bets might be present
            # this should remove them
            if remove_dups:
                #remove duplicates before loading to database - and print number of drops
                print("original rows: ", df_props.shape)
                df_props = df_props.sort_values(
                    ['playerId', 'prop', 'projValue'], 
                    ascending=False
                )
                df_props.drop_duplicates(
                    subset=['playerId', 'prop'], 
                    inplace=True
                )
                # keeps first occurance which should be original after sorting.
                print("after dups removed: ", df_props.shape)

            if self.store_locally:
                self.data_all[league] = df_props

            if self.database_export:
                self.loadDb(
                    df_props = df_props, 
                    league = league, 
                    oddsTableName = 'odds', 
                    dbAction = 'append', 
                    update_players = self.update_players,
                    playerTableName = 'actnetplayers'
                )
                

            # final console output and checking for missed props
            if len(specific_props) > 0:  # TODO this is only build for the class initiated to a single date and league (used on missing props from the first run)
                all_props = specific_props
            else:
                all_props = self.prop_names[league]

            retrieved_props = df_props['prop'].unique().tolist()
            missed_props = list(np.setdiff1d(all_props, retrieved_props))
            # update class var to flag for missing props
            if len(missed_props) > 0:
                self.scrape_error_flag = True
                self.scrape_errors[league]['missing_props'].extend(missed_props)
                print("missing props: ", missed_props)

            print(df_props.groupby('prop').agg({'propId':['count']}).T)
        
        return 

    # store scraped data in db 
    def loadDb(
            self, 
            df_props, 
            league, 
            oddsTableName='odds', 
            dbAction='append', 
            update_players = False,
            playerTableName= 'actnetplayers'
        ):
        '''
        df_props = df output from processScrapes(), 
        pymysql_conn_str = standard pymysql db conn str format
        oddsTableName = table name in the db to load odds data, 
        dbAction = pd.to_sql() if exists action for odds table
        update_players = bool to update player ids in the data base with new players in the current data,
        playerTableName= table name in the db to load new players

        '''
        #removing player names to load into db
        if update_players:
            df_players = df_props[['playerId','player','abbr']]
            df_props = df_props[df_props.columns[~df_props.columns.isin(['player','abbr'])]]
        else:
            try:
                df_props = df_props[df_props.columns[~df_props.columns.isin(['player','abbr'])]]
            except:
                pass    

        # make sure data is formatted
        df_props.loc[:,'date'] = pd.to_datetime(df_props['date'])
        df_props = df_props.astype({"uOdds":"Int64","oOdds":"Int64"})

        # connect to db
        pymysql_conn_str = self.get_pymysql_conn_str(
            league = league, 
            config_path = self.config_path
        )
        sqlEngine = create_engine(pymysql_conn_str, pool_recycle=3600)
        
        # handling loads as transaction - useful when doing mult. leagues and one on 1/2 loads.
        with sqlEngine.connect() as dbConnection:
            tran = dbConnection.begin()

            try:
                # load to db
                df_props.to_sql(oddsTableName, dbConnection, if_exists=dbAction, index=False)
                if update_players:
                    # players have multiple props, drop dup ids
                    df_players.drop_duplicates('playerId', inplace=True)
                    # retrieve the existing IDs in the db tbale
                    playerIds = list(pd.read_sql_query('SELECT playerId FROM actnetplayers', dbConnection)['playerId'])
                    # filter out the players that already exists
                    df_players = df_players[~df_players['playerId'].isin(playerIds)]
                    # print out new players added
                    print(df_players['player'].unique())
                    # update db
                    df_players.to_sql(playerTableName, dbConnection, if_exists='append', index=False)
               
                tran.commit()
                dbConnection.close()
                print(league, oddsTableName, 'data loaded...')
            
            except Exception as e:
                print(e)
                tran.rollback()
                dbConnection.close()

    # TODO
    # check for missing props and scrape
    def tryMissingProps(self):
        """
        check if class is flagged for scraping error and initiate another scrape and load
        """
        # check if class flagged for scraping error
        if self.scrape_error_flag:
            wait_secs = random.randint(1,5)

            for k, v in self.scrape_errors.items():

                if len(v['missing_props']) > 0:
                    leagues_with_missing_props = [k]
                    missing_props = v['missing_props']
                    print('rescraping', k, 'for', missing_props)

                    self.second_run = True

                    self.scrape(
                        sleep_secs = wait_secs, 
                        specific_props = missing_props,
                        leagues_override = leagues_with_missing_props
                    )
                    
                    self.processScrapes(
                        remove_dups = True,
                        specific_props = missing_props
                    )



    


       
