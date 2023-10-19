library(wehoop)
library(DBI)
library(RMySQL)
library(jsonlite)
library(dplyr)
library(stringr)
source("scripts/functions/dbhelpers.R")
remotes::install_github("sportsdataverse/wehoop")

conn <- harvestDBconnect(league='wnba')
dbSendQuery(conn, "SET GLOBAL local_infile = true;")

################
# update database
################
#update pbp
load_wnba_pbp(
    seasons  = most_recent_wnba_season(),
    dbConnection = conn,
    tablename = 'pbp',
    append = TRUE
)

load_wnba_player_box(
    seasons  = most_recent_wnba_season(),
    dbConnection = conn,
    tablename = 'playerbox',
    append = TRUE
)
######

################
# 
################

################

################
# loan players table in DB with initial data 
# it will be the Hoopr players name, ID, Position + nbaId, rotoId, cleaned name for joins
################
# list to remove suffixes and punct from names
suffix.rep <- c("\\."="", "'"="", "'"=""
    #" Jr."="", " Sr."="", " III"="", " IV"="", " II"="", 
                )


p <- wehoop::wnba_commonallplayers()$CommonAllPlayers
# creating cleaned name join for NBA names and IDs
currentWNBA <- p %>%
            #filter(ROSTERSTATUS==1) %>%
            mutate(join.names = tolower(stringr::str_replace_all(DISPLAY_FIRST_LAST,suffix.rep))) %>%
            select(join.names, PERSON_ID)


wehoopId <- dbGetQuery(conn, "SELECT DISTINCT athlete_id, athlete_display_name, athlete_position_abbreviation FROM playerbox")

currentWhoopR <- wehoopId %>%
                    select(athlete_display_name, athlete_id, athlete_position_abbreviation) %>%
                    mutate(join.names = tolower(stringr::str_replace_all(athlete_display_name,suffix.rep))) %>%
                    distinct(athlete_id, .keep_all= TRUE)

datadump <- left_join(currentWhoopR, currentWNBA, by="join.names")
datadump <- datadump %>% 
                mutate(
                    wehoopId = as.integer(athlete_id),
                    wnbaId = as.integer(PERSON_ID)
                ) %>%
                select(-athlete_id, -PERSON_ID) %>% 
                rename(c(
                    player=athlete_display_name, pos=athlete_position_abbreviation,joinName=join.names
                ))


actnetId <- dbGetQuery(conn, "SELECT DISTINCT * FROM actnetplayers") %>%
                mutate(joinName = tolower(stringr::str_replace_all(player, suffix.rep))) %>%
                rename(actnetName=player,
                       actnetAbbr=abbr,
                       actnetPlayerId=playerId)

datadump <- left_join(datadump, actnetId, by="joinName")

dbWriteTable(conn, name = "players", value= datadump, row.names = FALSE, append=TRUE)#overwrite=TRUE)



#####

dbSendQuery(conn, "SET GLOBAL local_infile = false;")
dbDisconnect(conn)



