import pandas as pd
import json, time, re
from datetime import datetime

from sqlalchemy import create_engine

from bs4 import BeautifulSoup as bs
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

import warnings 
warnings.filterwarnings('ignore')

class scraper():
    """
    facilitates scraping various stat tables from multiple websites
    """

    def __init__(self, browser_path, database_export = False, store_locally=True, pymysql_conn_str = None):
        self.browser_path = browser_path
        self.database_export = database_export
        self.store_locally = store_locally
        
        # connection string
        if pymysql_conn_str is None:
            
            #importing credentials from my txt file  
            # #TODO remove the hard coded path
            with open('../../../Notes-General/config.txt', 'r') as f:
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
        self.scrape_errors = {}

        # meta data - run date, etc.. 
        self.meta_data = {
            'today_dt':datetime.today(),
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
    def open_browser(self, browser_path = None, retry_delay = 5, retry_attempts = 3):
        
        # an override browswer path can be provided but normally use the one provided whe nthe class is created 
        if browser_path is None:
            browser_path = self.browser_path
        
        service = Service(browser_path)

        driver = webdriver.Firefox(service=service)
        driver.implicitly_wait(10)
        
        # loop to catch gecko updates that normally stall the code due to browser restart
        for attempt in range(retry_attempts):
            try:
                print(f"Attempt {attempt + 1} of {retry_attempts} to launch Firefox...")
                # Initialize WebDriver (adjust options/path as needed)
                driver = webdriver.Firefox(service=service)
                driver.get('google.com')
                print("Firefox launched successfully.")
                break  # Exit the loop if successful
            except:
                print("Checking if Firefox is updating...")
                # Wait before retrying
                time.sleep(retry_delay)

        return driver
    
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

    ################
    # nba com scrapes
    def get_nba_team_playtype_data(
            self,
            base_url = 'https://www.nba.com/stats/teams/{playtype}?TypeGrouping={sideofball}&SeasonType={type}',
            play_types = [
                'isolation', 'transition', 'ball-handler', 'roll-man', 'playtype-post-up',
                'spot-up', 'hand-off', 'cut', 'off-screen','putbacks'
            ],
            sides = ['offensive', 'defensive'],
            season_type = 'Regular+Season',  # ['Regular+Season', 'PlayIn', 'Playoffs']
            database_table = 'statsteamplaytypes'
    ):
        """
        function to scrape nba.com team playtype stats on both sides of the ball

        * base_url = desired scraping url
        * playtypes = any eligible play type in the form required by url
            'isolation', 'transition', 'ball-handler', 'roll-man', 'playtype-post-up', 'spot-up', 
            'hand-off', 'cut', 'off-screen','putbacks'
        * sides = stat side of the ball, formatted for url requirement
            'offensive' or 'defensive' 
        * season_type = game type for stats of interest, formatted for url requirement
            'Regular+Season', 'PlayIn', 'Playoffs'
        * database_table = name that will be used if exporting to database and also as the key in dictionary holding all df's
        * database_export = boolean to flag if sending to database or just holding locally
        * store_locally = boolean, flag to hold data in class dictionary as data frame
        
        Data will be scraped and added to the class objects appropiate data frame

        returns nothing since data is stored in class
        """
        # add table to class storage dictionaries
        self.gen_self_dict_entry(database_table)

        driver = self.open_browser()
        data = []
        url_errors = []

        for s in sides:
            for play in play_types:
                # get html page source data
                url = base_url.format(playtype=play, sideofball=s, type= season_type)
                driver.get(url)
                time.sleep(3)
                ps = driver.page_source
                soup = bs(ps)
                
                # extract table holding the data
                try:
                    tables = soup.find_all('table')
                    table = tables[0]
                    
                    #pull out col names
                    headers = table.find('thead').find_all('th')
                    cols = []
                    for i in headers:
                        cols.append(i.get_attribute_list('field')[0])
                    cols.append('tid')
                    cols.append('sob')
                    cols.append('play')
                    
                    # pull out data from the body
                    #data = []
                    rows = table.find('tbody').find_all('tr')
                    for i in rows:
                        row = []
                        rowData = i.find_all('td')
                        for j in rowData:
                            row.append(j.text)
                            try:
                                tid = j.find('a').get_attribute_list('href')
                            except:
                                pass
                        tid = tid[0].split('/')[3]
                        row.append(tid)
                        row.append(s)
                        row.append(play)
                        data.append(row)

                except:
                    # add url input to error list if there are any issues collecting data
                    # this will allow for calling the function again only on the errors
                    url_errors.append([play, s, season_type])
                    continue
                        
        # combine headers with data in a dataframe        
        df = pd.DataFrame(data, columns=cols)

        driver.close()

        # adding ordinal ranks
        cols.append('scoreFreqRank')
        ranked = pd.DataFrame(columns = cols)
        for s in sides:
            for pt in play_types:
                temp = df[(df['sob'] == s) & (df['play'] == pt)]
                temp.loc[:,'scoreFreqRank'] = temp.loc[:,'SCORE_POSS_PCT'].rank(ascending=False)
                ranked = pd.concat([ranked, temp])

        ranked.loc[:,'date'] = self.meta_data['today']


        #saving data
        #ranked.to_csv('../data/' + today + '_teamPlayTypes.csv', index=False)
        if self.database_export:
            self.export_database(ranked, database_table, self.pymysql_conn_str)

        # add to main class object holding all data
        if self.store_locally:
            self.data_all[database_table] = ranked

        # save all urls that failed
        self.scrape_errors[database_table]['url'] = url_errors

        print('nba team play type scraped...')
        return

    def get_nba_team_shotzone_data(            
            self,
            base_url = 'https://www.nba.com/stats/teams/{sideOfBall}?DistanceRange=By+Zone&LastNGames={lastNgames}&SeasonType={type}', 
            sides = {'offensive':'shooting', 'defensive':'opponent-shooting'},
            season_type = 'Regular+Season',  # ['Regular+Season', 'PlayIn', 'Playoffs']
            lastNgames = 10,
            database_table = 'statsteamshotzones'
    ):
        """
        function to scrape nba.com team shot zone stats on both sides of the ball

        * base_url = desired scraping url
        * sides = stat side of the ball, formatted for url requirement - map key since it deviated from the off/def url input
            {'offensive':'shooting', 'defensive':'opponent-shooting'} 
        * season_type = game type for stats of interest, formatted for url requirement
            'Regular+Season', 'PlayIn', 'Playoffs'
        * database_table = name that will be used if exporting to database and also as the key in dictionary holding all df's
        * database_export = boolean to flag if sending to database or just holding locally
        * store_locally = boolean, flag to hold data in class dictionary as data frame
        
        Data will be scraped and added to the class objects appropiate data frame

        returns nothing since data is stored in class
        """
        # add table to class storage dictionaries
        self.gen_self_dict_entry(database_table)

        #col names
        colsZone = [
            'TEAM_NAME','paintRaFgm', 'paintRaFga', 'paintRaFgPct','paintNoRaFgm','paintNoRaFga','paintNoRaFgPct',
            'midFgm','midFga','midFgPct','leftC3Fgm','leftC3Fga','leftC3FgPct','rightC3Fgm','rightC3Fga',
            'rightC3FgPct','bothC3Fgm','bothC3Fga','bothC3FgPct','aboveBreak3Fgm','aboveBreak3Fga','aboveBreak3FgPct',
            'tid','sob'
        ]

        #buttonXpath = "/html/body/div[1]/div[2]/div[2]/div[3]/section[2]/div/div[2]/div[2]/div[1]/div[3]/div/label/div/select/option[1]"

        lastNgames = str(lastNgames)

        driver = self.open_browser()
        data = []
        url_errors = []

        for s, v in sides.items():       
            url = base_url.format(sideOfBall=v, lastNgames=lastNgames, type=season_type)
            #url = "https://www.nba.com/stats/teams/shooting?DistanceRange=By+Zone&SeasonType=Playoffs&DateFrom=04%2F29%2F2024&DateTo=05%2F08%2F2024"
            #url = "https://www.nba.com/stats/teams/opponent-shooting?DistanceRange=By+Zone&SeasonType=Playoffs&DateFrom=04%2F29%2F2024&DateTo=05%2F08%2F2024"
            
            # get html page source data
            driver.get(url)
            time.sleep(3)
            ps = driver.page_source
            soup = bs(ps)
            
            try:
                # extract table holding the data
                tables = soup.find_all('table')
                table = tables[-1]
                
                # pull out data from the body
                #data = []
                rows = table.find('tbody').find_all('tr')
                for i in rows:
                    row = []
                    rowData = i.find_all('td')
                    for j in rowData:
                        # convert number strings to numeric 
                        try:
                            row.append(pd.to_numeric(j.text))
                        except:
                            row.append(j.text)
                            
                        # grab team id from the one td that has it
                        try:
                            tid = j.find('a').get_attribute_list('href')
                        except:
                            pass
                    tid = tid[0].split('/')[3]
                    row.append(tid)
                    row.append(s)
                    data.append(row)
            except:
                url_errors.append([{s:v}, lastNgames, season_type])
                continue

        driver.close()

        # combine headers with data in a dataframe        
        dfZoneShooting = pd.DataFrame(data, columns=colsZone)

        # calculate all points in the paint restricted area + non-ra
        dfZoneShooting.loc[:,'paintAllFgm'] =  dfZoneShooting['paintRaFgm'] + dfZoneShooting['paintNoRaFgm']
        dfZoneShooting.loc[:,'paintAllFga'] =  dfZoneShooting['paintRaFga'] + dfZoneShooting['paintNoRaFga']
        dfZoneShooting.loc[:,'paintAllFgPct'] =  round((dfZoneShooting['paintAllFgm'] / dfZoneShooting['paintAllFga']) * 100,3)

        dfZoneShooting.loc[:,'date'] = self.meta_data['today']

        #save data
        #dfZoneShooting.to_csv('../data/' + today + '_teamShotZones.csv', index=False)
        if self.database_export:
            self.export_database(dfZoneShooting, database_table, self.pymysql_conn_str)

        # add to main class object holding all data
        if self.store_locally:
            self.data_all[database_table] = dfZoneShooting

        # save all urls that failed
        self.scrape_errors[database_table]['url'] = url_errors

        print('nba team shot zone scraped...')
        return

    def get_nba_player_playtype_data(            
            self,
            base_url = 'https://www.nba.com/stats/players/{playtype}?TypeGrouping={sideofball}&SeasonType={type}', 
            play_types = [
                'isolation', 'transition', 'ball-handler', 'roll-man', 'playtype-post-up',
                'spot-up', 'hand-off', 'cut', 'off-screen','putbacks'
            ],
            sides = ['offensive'],
            season_type = 'Regular+Season',  # ['Regular+Season', 'PlayIn', 'Playoffs']
            database_table = 'statsplayerplaytypes'
    ):
        """
        function to scrape nba.com player play type stats on offense

        * base_url = desired scraping url
        * playtypes = any eligible play type in the form required by url
            'isolation', 'transition', 'ball-handler', 'roll-man', 'playtype-post-up', 'spot-up', 
            'hand-off', 'cut', 'off-screen','putbacks'
        * sides = stat side of the ball, formatted for url requirement
            'offensive' or 'defensive' 
        * season_type = game type for stats of interest, formatted for url requirement
            'Regular+Season', 'PlayIn', 'Playoffs'
        * database_table = name that will be used if exporting to database and also as the key in dictionary holding all df's
        * database_export = boolean to flag if sending to database or just holding locally
        * store_locally = boolean, flag to hold data in class dictionary as data frame
        
        Data will be scraped and added to the class objects appropiate data frame

        returns nothing since data is stored in class
        """
        # add table to class storage dictionaries
        self.gen_self_dict_entry(database_table)

        driver = self.open_browser()
        data = []
        url_errors = []
        today = self.meta_data['today']

        buttonXpath = "/html/body/div[1]/div[2]/div[2]/div[3]/section[2]/div/div[2]/div[2]/div[1]/div[3]/div/label/div/select/option[1]"
        for s in sides:
            for play in play_types:
                # get html page source data
                url = base_url.format(playtype=play, sideofball=s, type=season_type)
                
                # get html page source data
                driver.get(url)
                #time.sleep(4)
                
                ## need to open page then change filter from 1 to -1 so that all players load
                ## div class="Crom_cromSettings__ak6Hd"  > 
                #####select class="DropDown_select__4pIg9"
                #######option value=-1
                try:
                    WebDriverWait(driver, timeout=20).until(lambda d: d.find_element("xpath", buttonXpath))
                    pagenationFilter = driver.find_element("xpath", buttonXpath)
                    pagenationFilter.click()
                except:
                    #pagenationFilter = driver.find_element("xpath", buttonXpath)
                    #driver.execute_script("arguments[0].click();", pagenationFilter)
                    pass
                
                time.sleep(1)
                ps = driver.page_source
                soup = bs(ps)

                    
                # extract table holding the data
                try:
                    tables = soup.find_all('table')
                    table = tables[0]
                    
                    #pull out col names
                    headers = table.find('thead').find_all('th')
                    cols = []
                    for i in headers:
                        cols.append(i.get_attribute_list('field')[0])
                    # add column names that will be created
                    cols.append('pid')
                    cols.append('tid')
                    cols.append('sob')
                    cols.append('play')
                    
                    # pull out data from the body
                    #data = []
                    
                    # extract each row
                    rows = table.find('tbody').find_all('tr')
                    for i in rows:
                        row = []
                    
                        # extract each piece of data from a single row
                        rowData = i.find_all('td')
                        for j in rowData:
                            row.append(j.text)
                    
                            try:
                                href = j.find('a').get_attribute_list('href')
                                #assigning href to correct variable for later processing.
                                if "player" in href[0]:
                                    pid = href[0].split('/')[3]
                                    
                                else:
                                    tid = href[0].split('/')[3]      
                            except:
                                pass
                        
                        #add additional data to the row before it is added to the agg'ed data++
                        row.append(pid)
                        row.append(tid)
                        row.append(s)
                        row.append(play)
                    
                        # add the gathered row data into the master list that will be converted to dataframe
                        data.append(row)
                
                except:
                    # add url input to error list if there are any issues collecting data
                    # this will allow for calling the function again only on the errors
                    url_errors.append([play, s, season_type])
                    continue
                    
        # combine headers with data in a dataframe        
        df = pd.DataFrame(data, columns=cols)

        driver.close()

        headers = table.find('thead').find_all('th')
        cols = []
        for i in headers:
            cols.append(i.get_attribute_list('field')[0])
        # add column names that will be created
        cols.append('pid')
        cols.append('tid')
        cols.append('sob')
        cols.append('play')

        # add ordinal ranks
        cols.append('freqRank')
        cols.append('scoreFreqRank')
        ranked = pd.DataFrame(columns = cols)

        for pt in play_types:
            temp = df[(df['sob'] == "offensive") & (df['play'] == pt)]
            temp.loc[:,'scoreFreqRank'] = temp.loc[:,'SCORE_POSS_PCT'].rank(ascending=False)
            temp.loc[:,'freqRank'] = temp.loc[:,'POSS_PCT'].rank(ascending=False)
            ranked = pd.concat([ranked, temp])

        ranked.loc[:,'date'] = today

        #save data
        #ranked.to_csv('../data/' + today + '_playerPlayTypes.csv', index=False)
        if self.database_export:
            self.export_database(ranked, database_table, self.pymysql_conn_str)

        # add to main class object holding all data
        if self.store_locally:
            self.data_all[database_table] = ranked

        # save all urls that failed
        self.scrape_errors[database_table]['url'] = url_errors

        print('nba player play type scraped...')
        return

    def get_nba_player_shotzone_data(            
            self,
            base_url = 'https://www.nba.com/stats/players/shooting?DistanceRange=By+Zone&LastNGames={lastNgames}&SeasonType={type}', 
            season_type = 'Regular+Season',  # ['Regular+Season', 'PlayIn', 'Playoffs']
            lastNgames = 10,
            database_table = 'statsplayershotzones'
    ):
        """
        function to scrape nba.com player shot zone stats on offense

        * base_url = desired scraping url
        * sides = stat side of the ball, formatted for url requirement - map key since it deviated from the off/def url input
            {'offensive':'shooting', 'defensive':'opponent-shooting'} 
        * season_type = game type for stats of interest, formatted for url requirement
            'Regular+Season', 'PlayIn', 'Playoffs'
        * database_table = name that will be used if exporting to database and also as the key in dictionary holding all df's
        * database_export = boolean to flag if sending to database or just holding locally
        * store_locally = boolean, flag to hold data in class dictionary as data frame
        
        Data will be scraped and added to the class objects appropiate data frame

        returns nothing since data is stored in class
        """

        # add table to class storage dictionaries
        self.gen_self_dict_entry(database_table)

        colsZoneP = [
            'PLAYER_NAME', 'TEAM', 'AGE','paintRaFgm', 'paintRaFga', 'paintRaFgPct','paintNoRaFgm','paintNoRaFga',
            'paintNoRaFgPct','midFgm','midFga','midFgPct','leftC3Fgm','leftC3Fga','leftC3FgPct','rightC3Fgm',
            'rightC3Fga','rightC3FgPct','bothC3Fgm','bothC3Fga','bothC3FgPct','aboveBreak3Fgm','aboveBreak3Fga',
            'aboveBreak3FgPct','pid','tid'
        ]

        buttonXpath = "/html/body/div[1]/div[2]/div[2]/div[3]/section[2]/div/div[2]/div[2]/div[1]/div[3]/div/label/div/select/option[1]"

        lastNgames = str(lastNgames)
        data = []
        url_errors = []
        today = self.meta_data['today']

        driver = self.open_browser()

        # get html page source data
        url = base_url.format(lastNgames = lastNgames, type = season_type)
        driver.get(url)
        time.sleep(3)

        ## need to open page then change filter from 1 to -1 so that all players load
        ## div class="Crom_cromSettings__ak6Hd"  > 
        #####select class="DropDown_select__4pIg9"
        #######option value=-1
        try:
            WebDriverWait(driver, timeout=20).until(lambda d: d.find_element("xpath", buttonXpath))
            pagenationFilter = driver.find_element("xpath", buttonXpath)
            pagenationFilter.click()
        except:
            pagenationFilter = driver.find_element("xpath", buttonXpath)
            driver.execute_script("arguments[0].click();", pagenationFilter)

        time.sleep(1)
        ps = driver.page_source
        soup = bs(ps)

        try:
            # extract table holding the data
            tables = soup.find_all('table')
            table = tables[-1]
            
            # pull out data from the body
            #data = []
            rows = table.find('tbody').find_all('tr')
            for i in rows:
                row = []
                rowData = i.find_all('td')
                for j in rowData:
                    # convert number strings to numeric 
                    try:
                        if j.text == '-':
                            txt = 0
                        else:
                            txt = j.text
                        row.append(pd.to_numeric(txt))
                    except:
                        row.append(j.text)
                        
                    # grab team id and player id from the one td that has it
                    try:
                        href = j.find('a').get_attribute_list('href')[0]
                        if 'player' in href:
                            pid = href.split('/')[3]
                        if 'team' in href:
                            tid = href.split('/')[3]
                        
                    except:
                        pass
                
                row.append(pid)
                row.append(tid)
                data.append(row)
        except:
            # add url input to error list if there are any issues collecting data
            # this will allow for calling the function again only on the errors
            url_errors.append([lastNgames, season_type])
            

        driver.close()

        # combine headers with data in a dataframe        
        dfpZoneShooting = pd.DataFrame(data, columns=colsZoneP)

        # calculate all points in the paint restricted area + non-ra
        dfpZoneShooting.loc[:,'paintAllFgm'] =  dfpZoneShooting['paintRaFgm'] + dfpZoneShooting['paintNoRaFgm']
        dfpZoneShooting.loc[:,'paintAllFga'] =  dfpZoneShooting['paintRaFga'] + dfpZoneShooting['paintNoRaFga']
        dfpZoneShooting.loc[:,'paintAllFgPct'] =  round((dfpZoneShooting['paintAllFgm'] / dfpZoneShooting['paintAllFga']) * 100,3)

        dfpZoneShooting.loc[:,'date'] = today
        
        #save data
        #dfpZoneShooting.to_csv('../data/' + today + '_playerShotZones.csv', index=False)
        if self.database_export:
            self.export_database(dfpZoneShooting, database_table, self.pymysql_conn_str)

        # add to main class object holding all data
        if self.store_locally:
            self.data_all[database_table] = dfpZoneShooting

        # save all urls that failed
        self.scrape_errors[database_table]['url'] = url_errors

        print('nba player shot zone scraped...')
        return

    def get_nba_player_passing_data(            
        self,
        base_url = 'https://www.nba.com/stats/players/passing?DateFrom={d1}&DateTo={d2}&LastNGames=0&PerMode=Totals&SeasonType={type}', 
        today_date = None,
        day_adjuster = -1,
        season_type = 'Regular+Season',  # ['Regular+Season', 'PlayIn', 'Playoffs']
        #lastNgames = 10,
        database_table = 'statsplayerpassing'
    ):
        """
        function to scrape nba.com player passing stats on offense
        this runs to gather stats from completed games, 1 game at a time, by the last completed date

        * base_url = desired scraping url
        * day_adjuster = number of days to subtract from todays date 
        * season_type = game type for stats of interest, formatted for url requirement
            'Regular+Season', 'PlayIn', 'Playoffs'
        * database_table = name that will be used if exporting to database and also as the key in dictionary holding all df's
        * database_export = boolean to flag if sending to database or just holding locally
        * store_locally = boolean, flag to hold data in class dictionary as data frame
        
        Data will be scraped and added to the class objects appropiate data frame

        returns nothing since data is stored in class
        """
        # add table to class storage dictionaries
        self.gen_self_dict_entry(database_table)

        # this can be farther back than yesterday and will be the last date with games completed.
        if today_date == None:
            today_date = self.meta_data['today_dt']
        else:
            today_date = pd.to_datetime(today_date)
        
        yesterday = today_date + pd.DateOffset(days=day_adjuster)
        dt = yesterday.strftime("%m/%d/%Y")

        data = []
        url_errors = []

        buttonXpath = "/html/body/div[1]/div[2]/div[2]/div[3]/section[2]/div/div[2]/div[2]/div[1]/div[3]/div/label/div/select/option[1]"
        cookieXpath = '//*[@id="onetrust-accept-btn-handler"]'

        # get html page source data
        driver = self.open_browser()
        url = base_url.format(d1=dt, d2=dt, type=season_type)
        driver.get(url)
        time.sleep(3)

        try:
            cookiesButton = driver.find_element("xpath", cookieXpath)
            cookiesButton.click()
        except:
            pass

        try:
            WebDriverWait(driver, timeout=20).until(lambda d: d.find_element("xpath", buttonXpath))
            pagenationFilter = driver.find_element("xpath", buttonXpath)
            pagenationFilter.click()
        except:
            #pagenationFilter = driver.find_element("xpath", buttonXpath)
            #driver.execute_script("arguments[0].click();", pagenationFilter)
            pass

        time.sleep(1)
        ps = driver.page_source
        soup = bs(ps)

        # extract table holding the data
        try:
            tables = soup.find_all('table')
            
            #finding the table of interest, it is not always the first table on the page.
            for t in tables:
                table_class = t.get_attribute_list('class')[0]
                if 'datepicker' in table_class.lower():
                    continue
                else:
                    table = t

            #pull out col names
            headers = table.find('thead').find_all('th')
            cols = []
            for i in headers:
            
                header = i.get_attribute_list('field')[0]
                if  header == 'AST_POINTS_CREATED' or pd.isnull(header):
                    continue
                else:
                    cols.append(header)
            # add column names that will be created
            cols.append('pid')
            cols.append('tid')
        
            # extract each row
            rows = table.find('tbody').find_all('tr')
            for i in rows:
                row = []
                
                # extract each piece of data from a single row
                rowData = i.find_all('td')
                for j in rowData:
                    row.append(j.text)
                    try:
                        href = j.find('a').get_attribute_list('href')
                        #assigning href to correct variable for later processing.
                        if 'player' in href[0]:
                            pid = href[0].split('/')[3]  
                        else:
                            tid = href[0].split('/')[3]      
                    except:
                        pass
                
                #add additional data to the row before it is added to the agg'ed data++
                row.append(pid)
                row.append(tid)
            
                # add the gathered row data into the master list that will be converted to dataframe
                data.append(row)
        except:
            # add url input to error list if there are any issues collecting data
            # this will allow for calling the function again only on the errors
            url_errors.append([today_date, day_adjuster, season_type])
            

                    
        # combine headers with data in a dataframe        
        df = pd.DataFrame(data, columns=cols)
        #df.drop(columns=['W','L'])
        df.loc[:,'date'] = yesterday.date()

        driver.close()


        if self.database_export:
            self.export_database(df, database_table, self.pymysql_conn_str)

        # add to main class object holding all data
        if self.store_locally:
            self.data_all[database_table] = df

        # save all urls that failed
        self.scrape_errors[database_table]['url'] = url_errors

        print('nba player passing scraped...')
        return

    def get_nba_player_rebounding_data(            
        self,
        base_url = 'https://www.nba.com/stats/players/rebounding?DateFrom={d1}&DateTo={d2}&LastNGames=0&PerMode=Totals&SeasonType={type}', 
        today_date = None,
        day_adjuster = -1,
        season_type = 'Regular+Season',  # ['Regular+Season', 'PlayIn', 'Playoffs']
        #lastNgames = 10,
        database_table = 'statsplayerrebounding'
    ):
        """
        function to scrape nba.com player passing stats on offense
        this runs to gather stats from completed games, 1 game at a time, by the last completed date

        * base_url = desired scraping url
        * day_adjuster = number of days to subtract from todays date 
        * season_type = game type for stats of interest, formatted for url requirement
            'Regular+Season', 'PlayIn', 'Playoffs'
        * database_table = name that will be used if exporting to database and also as the key in dictionary holding all df's
        * database_export = boolean to flag if sending to database or just holding locally
        * store_locally = boolean, flag to hold data in class dictionary as data frame
        
        Data will be scraped and added to the class objects appropiate data frame

        returns nothing since data is stored in class
        """
        # add table to class storage dictionaries
        self.gen_self_dict_entry(database_table)

        # this can be farther back than yesterday and will be the last date with games completed.
        if today_date == None:
            today_date = self.meta_data['today_dt']
        else:
            today_date = pd.to_datetime(today_date)
        
        yesterday = today_date + pd.DateOffset(days=day_adjuster)
        dt = yesterday.strftime("%m/%d/%Y")

        data = []
        url_errors = []
        buttonXpath = "/html/body/div[1]/div[2]/div[2]/div[3]/section[2]/div/div[2]/div[2]/div[1]/div[3]/div/label/div/select/option[1]"
        cookieXpath = '//*[@id="onetrust-accept-btn-handler"]'

        # get html page source data
        url = base_url.format(d1=dt, d2=dt, type=season_type)
        driver = self.open_browser()
        driver.get(url)
        time.sleep(3)

        try:
            cookiesButton = driver.find_element("xpath", cookieXpath)
            cookiesButton.click()
        except:
            pass

        try:
            WebDriverWait(driver, timeout=20).until(lambda d: d.find_element("xpath", buttonXpath))
            pagenationFilter = driver.find_element("xpath", buttonXpath)
            pagenationFilter.click()
        except:
            #pagenationFilter = driver.find_element("xpath", buttonXpath)
            #driver.execute_script("arguments[0].click();", pagenationFilter)
            pass

        time.sleep(1)
        ps = driver.page_source
        soup = bs(ps)

        # extract table holding the data
        try:
            tables = soup.find_all('table')
            
            #finding the table of interest, it is not always the first table on the page.
            for t in tables:
                table_class = t.get_attribute_list('class')[0]
                if 'datepicker' in table_class.lower():
                    continue
                else:
                    table = t

            #pull out col names
            headers = table.find('thead').find_all('th')
            cols = []
            for i in headers:
            
                header = i.get_attribute_list('field')[0]
                #if  header == 'AST_POINTS_CREATED':
                #    continue
                #else:
                #    cols.append(header)
                cols.append(header)
            # add column names that will be created
            cols.append('pid')
            cols.append('tid')
        
            # extract each row
            rows = table.find('tbody').find_all('tr')
            for i in rows:
                row = []
                
                # extract each piece of data from a single row
                rowData = i.find_all('td')
                for j in rowData:
                    row.append(j.text)
                    try:
                        href = j.find('a').get_attribute_list('href')
                        #assigning href to correct variable for later processing.
                        if 'player' in href[0]:
                            pid = href[0].split('/')[3]  
                        else:
                            tid = href[0].split('/')[3]      
                    except:
                        pass
                
                #add additional data to the row before it is added to the agg'ed data++
                row.append(pid)
                row.append(tid)
            
                # add the gathered row data into the master list that will be converted to dataframe
                data.append(row)
        except:
            # add url input to error list if there are any issues collecting data
            # this will allow for calling the function again only on the errors
            url_errors.append([today_date, day_adjuster, season_type])
            
                    
        # combine headers with data in a dataframe        
        df = pd.DataFrame(data, columns=cols)
        df.drop(columns=['W','L'])
        df.loc[:,'date'] = yesterday.date()

        driver.close()

        if self.database_export:
            self.export_database(df, database_table, self.pymysql_conn_str)

        # add to main class object holding all data
        if self.store_locally:
            self.data_all[database_table] = df

        # save all urls that failed
        self.scrape_errors[database_table]['url'] = url_errors

        print('nba player rebounding scraped...')
        return

    #TODO: scrape
    #playerDefRatingUrl = 'https://www.nba.com/stats/players/advanced?CF=MIN*GE*15&SeasonType=Regular%20Season&dir=-1&sort=DEF_RATING'
    #teamDefRatingUrl = 'https://www.nba.com/stats/teams/advanced?LastNGames=10&dir=A&sort=DEF_RATING'

    ################
    # basketball reference scrapes
    def get_bref_pos_estimates(
        self,
        base_url = 'https://www.basketball-reference.com/teams/{team}/{season}.html#pbp', 
        today_date = None,
        season = 2025,
        database_table = 'brefmisc'
    ):
        """
        function to scrape basketball reference player position estimates
        they up date the estimates every day

        * base_url = desired scraping url -ex: https://www.basketball-reference.com/teams/PHI/2024.html#pbp
        * database_table = name that will be used if exporting to database and also as the key in dictionary holding all df's
        
        Data will be scraped and added to the class objects appropiate data frame

        returns nothing since data is stored in class
        """
        # add table to class storage dictionaries
        self.gen_self_dict_entry(database_table)

        # this can be farther back than yesterday and will be the last date with games completed.
        if today_date == None:
            today_date = self.meta_data['today_dt']
        else:
            today_date = pd.to_datetime(today_date)
        
        all_team_data = []
        url_errors = []

        # basketball ref team url abbrevs
        bref_team_abbr = [
            'GSW','DEN','POR','SAC','TOR','DAL','PHO','CHI',
            'LAL','HOU','MIA','MEM','DET','MIL','NOP','MIN',
            'CLE','OKC','LAC','BRK','SAS','NYK','WAS','CHO',
            'UTA','IND','BOS','PHI','ATL','ORL'
        ]
        
        # col names in the database
        bref_cols = [
            'player', 'age', 'pos', 'gp', 'gs', 'mp', 'PG', 'SG', 'SF',
            'PF', 'C', 'onCourtPlusMinusPer100', 'onOffPlusMinusPer100',
            'badPass', 'lostBall', 'shootFoulCommitted', 'offFoulCommitted',
            'shootFoulDrawn', 'offFoulDrawn', 'ptsGenFromAst', 'andOnes', 'shotsBlk', 'awards',
            'date', 'team'
        ]

        driver = self.open_browser()
        # loop through each team webpage to gather data        
        for i in bref_team_abbr:
            try:
                url = base_url.format(team = i, season = str(season))
                driver.get(url)
                time.sleep(2)

                table_id = 'pbp_stats'
                table = None  # Placeholder for the table element
                scroll_attempts = 30  # Number of scrolling attempts
                scroll_step = 500  # Pixels to scroll down on each attempt

                # scroll through the page to make the table of interest visible so it can be pulled from the html
                for attempt in range(scroll_attempts):
                    try:
                        # Try to find the table
                        table = driver.find_element(By.ID, table_id)
                        if table.is_displayed():  # Check if the table is now visible
                            break
                    except NoSuchElementException:
                        pass  # Table not found, keep scrolling

                    # Scroll down by the step size
                    driver.execute_script(f"window.scrollBy(0, {scroll_step});")

                # go back to table to grab page source data
                driver.execute_script("arguments[0].scrollIntoView();", table)

                ps = driver.page_source
                soup = bs(ps)
                tables = soup.find_all('table')

                # select the table of interest
                for t in tables:
                    tbl = t.get_attribute_list('id')[0]
                    if tbl == table_id:
                        table = t

                # extract each row
                rows = table.find('tbody').find_all('tr')
                for r in rows:
                    
                    row = []    
                    
                    # extract each piece of data from a single row (cols in the table in the html)
                    rowData = r.find_all('td')
                    for j in rowData:
                        row.append(j.text)

                    row.append(self.meta_data['today'])
                    row.append(i) # team name
                    
                    all_team_data.append(row)
                    
            
            except:
                url_errors.append([i, season])
                continue
        
        driver.close()
        bref_pos_estimates = pd.DataFrame(all_team_data, columns = bref_cols)

        # drop columns that of no interest and process some of the data
        cols_drop = ['pos', 'awards']
        columns_to_convert = ['PG', 'SG', 'SF', 'PF', 'C']

        bref_pos_estimates = bref_pos_estimates.drop(cols_drop, axis = 1)
        # convert pct to decimals
        bref_pos_estimates = bref_pos_estimates.replace('', 0)
        bref_pos_estimates[columns_to_convert] = bref_pos_estimates[columns_to_convert].fillna(0).astype(float).div(100)
        # assign pos based on max estimate from bref
        bref_pos_estimates['pos'] = bref_pos_estimates[columns_to_convert].idxmax(axis=1)

        # use regex replacement mapping to create joinable names.
        bref_pos_estimates['joinName'] = bref_pos_estimates['player'].apply(self.apply_regex_replacements).str.lower()

        
        if self.database_export:
            self.export_database(bref_pos_estimates, database_table, self.pymysql_conn_str)

        # add to main class object holding all data
        if self.store_locally:
            self.data_all[database_table] = bref_pos_estimates

        # save all urls inputs that failed
        self.scrape_errors[database_table]['url'] = url_errors

        print('bref player pos estimates scraped...')
        return


    # TODO: add functionality to check if there were any scraping errors and re run the necessary functions
    # loop over scrape_errors
    ##    {'statsteamplaytypes': {'url': [], 'db': []},
    ##'statsteamshotzones': {'url': [], 'db': []},
    ##'statsplayerplaytypes': {'url': [], 'db': []},
    ##'statsplayershotzones': {'url': [], 'db': []},
    ##'statsplayerpassing': {'url': [], 'db': []},
    ##'statsplayerrebounding': {'url': [], 'db': []}}
    # but will calling gen_self_dict_entry at the start of every function mess up the loop?


#######################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#######################################################################################
if __name__ == '__main__':
    print('running scraper....')
    lastNgames = 10
    day_adj = -1

    scraper = scraper(
        browser_path = '..\\browser\\geckodriver.exe',
        database_export = True, 
        store_locally=True,
        pymysql_conn_str = None
    )

    today = scraper.meta_data['today_dt']

    scraper.get_nba_team_playtype_data(
        base_url = 'https://www.nba.com/stats/teams/{playtype}?TypeGrouping={sideofball}&SeasonType={type}',
        play_types = [
            'isolation', 'transition', 'ball-handler', 'roll-man', 'playtype-post-up',
            'spot-up', 'hand-off', 'cut', 'off-screen','putbacks'
        ],
        sides = ['offensive', 'defensive'],
        season_type = 'Regular+Season',  # ['Regular+Season', 'PlayIn', 'Playoffs']
        database_table = 'statsteamplaytypes'
    )

    scraper.get_nba_team_shotzone_data(
        base_url = 'https://www.nba.com/stats/teams/{sideOfBall}?DistanceRange=By+Zone&LastNGames={lastNgames}&SeasonType={type}', 
        sides = {'offensive':'shooting', 'defensive':'opponent-shooting'},
        season_type = 'Regular+Season',  # ['Regular+Season', 'PlayIn', 'Playoffs']
        lastNgames = lastNgames,
        database_table = 'statsteamshotzones'
    )

    scraper.get_nba_player_playtype_data(
        base_url = 'https://www.nba.com/stats/players/{playtype}?TypeGrouping={sideofball}&SeasonType={type}', 
        play_types = [
            'isolation', 'transition', 'ball-handler', 'roll-man', 'playtype-post-up',
            'spot-up', 'hand-off', 'cut', 'off-screen','putbacks'
        ],
        sides = ['offensive'],
        season_type = 'Regular+Season',  # ['Regular+Season', 'PlayIn', 'Playoffs']
        database_table = 'statsplayerplaytypes'
    )

    scraper.get_nba_player_shotzone_data(
        base_url = 'https://www.nba.com/stats/players/shooting?DistanceRange=By+Zone&LastNGames={lastNgames}&SeasonType={type}', 
        season_type = 'Regular+Season',  # ['Regular+Season', 'PlayIn', 'Playoffs']
        lastNgames = lastNgames,
        database_table = 'statsplayershotzones'
    )

    scraper.get_nba_player_passing_data(
        base_url = 'https://www.nba.com/stats/players/passing?DateFrom={d1}&DateTo={d2}&LastNGames=0&PerMode=Totals&SeasonType={type}', 
        today_date = today,
        day_adjuster = day_adj,
        season_type = 'Regular+Season',  # ['Regular+Season', 'PlayIn', 'Playoffs']
        #lastNgames = 10,
        database_table = 'statsplayerpassing'
    )

    scraper.get_nba_player_rebounding_data(
        base_url = 'https://www.nba.com/stats/players/rebounding?DateFrom={d1}&DateTo={d2}&LastNGames=0&PerMode=Totals&SeasonType={type}', 
        today_date = today,
        day_adjuster = day_adj,
        season_type = 'Regular+Season',  # ['Regular+Season', 'PlayIn', 'Playoffs']
        #lastNgames = 10,
        database_table = 'statsplayerrebounding'
    )

    scraper.get_bref_pos_estimates(
        base_url = 'https://www.basketball-reference.com/teams/{team}/{season}.html#pbp', 
        today_date = today,
        season = 2025,
        database_table = 'brefmisc'
    )

    print('scraper finished....')

    # TODO: add check for number of errors
    url_error_count = 0
    db_error_count = 0
    for k, v in scraper.scrape_errors.items():
        url_error_count += len(v['url'])
        db_error_count += len(v['db'])

    total_errors = url_error_count + db_error_count
    if total_errors > 0:
        print('total scraper errors:', total_errors)
