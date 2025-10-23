library(nflfastR)
library(DBI)
library(dplyr)
library(jsonlite)
source("scripts/functions/dbhelpers.R")


league <- 'nfl'
season <- 2023

################
# loan players table in DB with initial data 
# it will be the Hoopr players name, ID, Position + nbaId, rotoId, cleaned name for joins
################

# list to remove suffixes and punct from names
suffix.rep <- c("\\."="", "'"="", "'"=""
                #" Jr."="", " Sr."="", " III"="", " IV"="", " II"="", 
)




conn <- harvestDBconnect(league=league)
dbSendQuery(conn, "SET GLOBAL local_infile = true;")

anplayers <- dbGetQuery(conn, "SELECT * FROM actnetplayers;") %>%
                mutate(joinName = tolower(stringr::str_replace_all(player,suffix.rep))) %>%
                rename(actnetName = player,
                       actnetPlayerId = playerId,
                       actnetAbbr = abbr
                       )

player <- dbGetQuery(conn, "SELECT espnId, nflid FROM player;")


players <- nflfastR::fast_scraper_roster(season) %>% 
                mutate(joinName = tolower(stringr::str_replace_all(full_name,suffix.rep))) 


datadump <- left_join(players, anplayers, by="joinName", keep=FALSE)

#dbWriteTable(conn, name = "players", value= datadump, row.names = FALSE, overwrite=TRUE)#append=TRUE)#overwrite=TRUE)

# sending the data back to the db for updating the table
dbx::dbxUpdate(conn, "players", datadump, where_cols = c("season", "full_name", "gsis_id"))


dbSendQuery(conn, "SET GLOBAL local_infile = false;")
dbDisconnect(conn)
#####

###### URL TO RETRIEVE ALL PLAYERS IN THE LEAGUE
##http://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2023/athletes/

datadump %>% filter(!is.na(actnetPlayerId) & season == 2023) %>% select(actnetPlayerId.y)

