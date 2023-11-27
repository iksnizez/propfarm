library(hoopR)
library(DBI)
library(RMySQL)
library(jsonlite)
library(dplyr)
library(stringr)
source("scripts/functions/dbhelpers.R")

season <- "2023-24"
league <- 'nba'

conn <- harvestDBconnect(league = league)
dbSendQuery(conn, "SET GLOBAL local_infile = true;")

################
# update database
################
#update pbp
update_nba_db(dbname="nba", 
              tblname = "pbp", 
              force_rebuild = c(2024:2024), #TRUE, 
              db_connection=conn)


load_nba_player_box(
    seasons = c(2024),
    dbConnection = conn,
    tablename = 'playerbox',
    
)

######



################
# load players table in DB with initial data 
# it will be the Hoopr players name, ID, Position + nbaId, rotoId, cleaned name for joins
################
# list to remove suffixes and punct from names
suffix.rep <- c("\\."="", "`"="", "'"="",
                " III$"="", " IV$"="", " II$"="", " iii$"="", " ii$"="", " iv$"="",
                " jr$"="", " sr$"="", " jr.$"="", " sr.$"="", " Jr$"="", " Sr$"="", " Jr.$"="", " Sr.$"="",
                "š"="s","ş"="s", "š"="s", 'š'="s", "š"="s",
                "ž"="z",
                "þ"="p","ģ"="g",
                "à"="a","á"="a","â"="a","ã"="a","ä"="a","å"="a",'ā'="a",
                "ç"="c",'ć'="c", 'č'="c",
                "è"="e","é"="e","ê"="e","ë"="e",'é'="e",
                "ì"="i","í"="i","î"="i","ï"="i",
                "ð"="o","ò"="o","ó"="o","ô"="o","õ"="o","ö"="o",'ö'="o",
                "ù"="u","ú"="u","û"="u","ü"="u","ū"="u",
                "ñ"="n","ņ"="n",
                "ý"="y",
                "Dario .*"="dario saric", "Alperen .*"="alperen sengun", "Luka.*amanic"="luka samanic"
)
# import external id data
currentRoto <- read.csv("data/rotoId.csv") %>% 
                    mutate(join.names = tolower(stringr::str_replace_all(player, suffix.rep))) %>%
                    select(join.names, rotowireId)

p <- hoopR::nba_commonallplayers(season=season)$CommonAllPlayers
# creating cleaned name join for NBA names and IDs
currentNBA <- p %>%
            #filter(ROSTERSTATUS==1) %>%
            mutate(join.names = tolower(stringr::str_replace_all(DISPLAY_FIRST_LAST,suffix.rep))) %>%
            select(join.names, PERSON_ID)

currenthoopR <- hoopR::load_nba_player_box(seasons = c(2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,
                                                       2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023)) %>%
                    select(athlete_display_name, athlete_id, athlete_position_abbreviation) %>%
    mutate(join.names = tolower(stringr::str_replace_all(athlete_display_name,suffix.rep))) %>%
    distinct(athlete_id, .keep_all= TRUE)

datadump <- left_join(currenthoopR, currentRoto, by="join.names")
datadump <- left_join(datadump, currentNBA, by="join.names")
datadump <- datadump %>% 
    mutate(
        hooprId = as.integer(athlete_id),
        nbaId = as.integer(PERSON_ID)
    ) %>%
    select(-athlete_id, -PERSON_ID) %>%
    rename(c(
        player=athlete_display_name, pos=athlete_position_abbreviation,joinName=join.names
    ))
dbWriteTable(conn, name = "players", value= as.data.frame(datadump), row.names = FALSE, overwrite=FALSE, append=TRUE)

#####

dbSendQuery(conn, "SET GLOBAL local_infile = false;")
dbDisconnect(conn)