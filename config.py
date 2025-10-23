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
ARENA_DISTANCES = DATA_DIR / 'arena distances and loc data//arenaDistanceMatrix_teamIds nba.csv'
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
            "ì":"i","í":"i","î":"i","ï":"i", "İ":"I",	
            "ð":"o","ò":"o","ó":"o","ô":"o","õ":"o","ö":"o",'ö':"o",
            "ù":"u","ú":"u","û":"u","ü":"u","ū":"u",
            "ñ":"n","ņ":"n",
            "ý":"y",
            "Dario .*":"dario saric", "Alperen .*":"alperen sengun", "Luka.*amanic":"luka samanic"
}

