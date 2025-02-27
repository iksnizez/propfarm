library(hoopR)
library(DBI)
library(RMySQL)
library(jsonlite)
library(dplyr)
library(dbx)
library(stringr)
source("scripts/functions/dbConnhelpers.R")
source("scripts/functions/NBAdatacrackers.R")

season <- "2024-25"
s <- 2025
league <- 'nba'



################
# load players table in DB with initial data 
# it will be the Hoopr players name, ID, Position + nbaId, rotoId, cleaned name for joins
################
# import external id data
currentRoto <- read.csv("data/rotoId.csv") %>% 
                    mutate(join.names = tolower(stringr::str_replace_all(player, suffix.rep))) %>%
                    select(join.names, rotowireId)

p <- hoopR::nba_commonallplayers(season=season)$CommonAllPlayers
# creating cleaned name join for NBA names and IDs
currentNBA <- p %>%
            filter(ROSTERSTATUS==1) %>%
            mutate(join.names = tolower(stringr::str_replace_all(DISPLAY_FIRST_LAST,suffix.rep))) %>%
            select(join.names, PERSON_ID)

currenthoopR <- hoopR::load_nba_player_box(seasons = c(2020,2021, 2022, 2023, 2024,2025)) %>%
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
    select(-athlete_id, -PERSON_ID) 


### add actnet to data
conn <- harvestDBconnect(league = league)
dbSendQuery(conn, "SET GLOBAL local_infile = true;")

actnet <- dbGetQuery(conn, "SELECT * FROM actnetplayers;") %>% 
    mutate(join.names = tolower(stringr::str_replace_all(player, suffix.rep))) %>%
    select(join.names, playerId, abbr) %>% 
    rename('actnetId'='playerId')

datadump <- left_join(datadump, actnet, by="join.names") %>%
    rename(c(
        player=athlete_display_name, pos=athlete_position_abbreviation,joinName=join.names
    ))

# sending the data back to the db for updating the table
###### this doesn't add new players... 
#dbx::dbxUpdate(conn, "players", datadump, where_cols = c("joinName"))

###### can be used when only adding new players, otherwise duplicates will be appended or fail on PK dup
#dbWriteTable(conn, name = "players", value= as.data.frame(datadump), row.names = FALSE, overwrite=FALSE, append=TRUE)

# upsert
dbx::dbxUpsert(conn, "players", datadump,  where_cols = c("joinName"))#, skip_existing = TRUE)

# delete all records and insert fresh
#dbxExecute(conn, 'TRUNCATE TABLE players;')
#dbxInsert(conn, 'players', datadump)

dbSendQuery(conn, "SET GLOBAL local_infile = false;")
dbDisconnect(conn)

######
##### REMOVE DUPLICATES - NOT REQUIRED ON EVERY RUN!
#####
#conn <- harvestDBconnect(league = league)
#dbSendQuery(conn, "SET GLOBAL local_infile = true;")

#data <- dbGetQuery(conn, "SELECT * FROM players;") %>%
#    group_by(joinName) %>%
#    arrange(desc(!is.na(actnetId))) %>%  # Prioritize non-null actnetId within each joinName group
#    slice(1) %>%  # Keep the first record in each group after sorting
#    ungroup()

#dbWriteTable(conn, name = "players", value= as.data.frame(data), row.names = FALSE, ####overwrite=TRUE)

#dbSendQuery(conn, "SET GLOBAL local_infile = false;")
#dbDisconnect(conn)
######

#####

