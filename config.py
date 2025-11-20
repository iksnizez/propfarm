# config.py
from pathlib import Path
from dotenv import load_dotenv
import os

def get_project_root():
    """
    Returns the project root regardless of whether we're in a script,
    an interactive shell, or Jupyter Notebook.
    """
    try:
        # Running from a file on disk
        return Path(__file__).resolve().parent   # the number of parents should be how many levels below the root the config is saved
    except NameError:
        # Interactive mode (IPython, Jupyter, or plain Python shell)
        return Path.cwd().parent if Path.cwd().name == "scripts" else Path.cwd()

PROJECT_ROOT = get_project_root()

# Load environment variables without overwriting existing ones
ENV_PATH = PROJECT_ROOT / '.env'
load_dotenv(dotenv_path=ENV_PATH, override=False)
PYMYSQL_NFL=os.environ.get('PYMYSQL_NFL')
PYMYSQL_NBA=os.environ.get('PYMYSQL_NBA')
PYMYSQL_MLB=os.environ.get('PYMYSQL_MLB')
PYMYSQL_NHL=os.environ.get('PYMYSQL_NHL')
PYMYSQL_WNBA=os.environ.get('PYMYSQL_WNBA')
KEY_OPENROUTESERVICE=os.environ.get('KEY_OPENROUTESERVICE')
map_conn_str = {
    'nba':PYMYSQL_NBA,
    'wnba':PYMYSQL_WNBA,
    'nhl':PYMYSQL_NHL,
    'nfl':PYMYSQL_NFL,
    'mlb':PYMYSQL_MLB
}

# config data 
DATA_DIR = PROJECT_ROOT / 'data'
LOGOS_DIR = DATA_DIR / 'teamLogos'
ARENA_DISTANCES_DIR = DATA_DIR / 'arena distances and loc data'
ARENA_DISTANCES = ARENA_DISTANCES_DIR / 'arenaDistanceFlightMatrix_nbaId.csv'
BROWSER_DIR = PROJECT_ROOT / 'browser'
SCRIPTS_DIR = PROJECT_ROOT / 'scripts'

suffix_replace = {
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
            "ì":"i","í":"i","î":"i","ï":"i", "İ":"i",	
            "ð":"o","ò":"o","ó":"o","ô":"o","õ":"o","ö":"o",'ö':"o",
            "ù":"u","ú":"u","û":"u","ü":"u","ū":"u",
            "ñ":"n","ņ":"n",
            "ý":"y",
            "Đ":'d', "đ":"d",

            "Dario .*":"dario saric", "Alperen .*":"alperen sengun", "Luka.*amanic":"luka samanic"
}

map_nbaAbbrv_to_nbaId = {
            'ATL':1610612737, 'BOS':1610612738, 'BKN':1610612751, 'CHA':1610612766,
            'CHI':1610612741, 'CLE':1610612739, 'DAL':1610612742, 'DEN':1610612743,  
            'DET':1610612765, 'GSW':1610612744, 'HOU':1610612745, 'IND':1610612754, 
            'LAC':1610612746, 'LAL':1610612747, 'MIA':1610612748, 'MIL':1610612749,  
            'MIN':1610612750, 'MEM':1610612763, 'NOP':1610612740, 'NYK':1610612752, 
            'ORL':1610612753, 'OKC':1610612760, 'PHI':1610612755, 'PHX':1610612756, 
            'POR':1610612757, 'SAC':1610612758, 'SAS':1610612759, 'TOR':1610612761,  
            'UTA':1610612762, 'WAS':1610612764
        }

map_nbaTid_to_hooprTid = {
    1610612737:1,     
    1610612751:17, 
    1610612738:2,     
    1610612766:30, 
    1610612741:4,     
    1610612739:5,     
    1610612742:6,
    1610612743:7,     
    1610612765:8,     
    1610612744:9,     
    1610612745:10, 
    1610612754:11, 
    1610612746:12, 
    1610612747:13, 
    1610612763:29, 
    1610612748:14, 
    1610612749:15, 
    1610612750:16, 
    1610612740:3, 
    1610612752:18, 
    1610612760:25, 
    1610612753:19, 
    1610612755:20, 
    1610612756:21, 
    1610612757:22, 
    1610612758:23, 
    1610612759:24, 
    1610612761:28, 
    1610612762:26, 
    1610612764:27
}

map_nbaAbbrv_to_hooprAbbrv = {
    'NOP':'NO',
    'NYK':'NY',
    'SAS':'SA',
    'UTA':'UTAH',
    'WAS':'WSH',
    'GSW':'GS',
}
