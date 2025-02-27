import time, json, requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine

### # nba_api is not friendly, changed it direct url hit ###
#from nba_api.stats.endpoints import LeagueDashPlayerStats
from nba_api.stats.endpoints import ScoreboardV2, ShotChartDetail, PlayerDashboardByGeneralSplits

