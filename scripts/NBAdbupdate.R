library(hoopR)
library(DBI)
library(RMySQL)
library(jsonlite)
library(dplyr)
library(stringr)
source("scripts/functions/dbhelpers.R")

conn <- harvestDBconnect()
dbSendQuery(conn, "SET GLOBAL local_infile = true;")

################
# update database
################
#update pbp
update_nba_db(dbname="nba", 
              tblname = "pbp", 
              force_rebuild = c(2023), #TRUE, 
              db_connection=conn)
######



################
# loan players table in DB with initial data 
# it will be the Hoopr players name, ID, Position + nbaId, rotoId, cleaned name for joins
################
# list to remove suffixes and punct from names
suffix.rep <- c(" Jr."="", " Sr."="", " III"="", " IV"="", " II"="", 
                "\\."="", "'"="", "'"="")

# import external id data
currentRoto <- read.csv("data/rotoId.csv") %>% 
                    mutate(join.names = tolower(stringr::str_replace_all(player, suffix.rep))) %>%
                    select(join.names, rotowireId)

p <- hoopR::nba_commonallplayers(season="2022-23")$CommonAllPlayers
# creating cleaned name join for NBA names and IDs
currentNBA <- p %>%
            filter(ROSTERSTATUS==1) %>%
            mutate(join.names = tolower(stringr::str_replace_all(DISPLAY_FIRST_LAST,suffix.rep))) %>%
            select(join.names, PERSON_ID)

currenthoopR <- hoopR::load_nba_player_box() %>%
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
dbWriteTable(conn, name = "players", value= datadump, row.names = FALSE)

#####

dbSendQuery(conn, "SET GLOBAL local_infile = false;")
dbDisconnect(conn)