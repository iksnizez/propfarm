season <- "2024-25"
s <- 2025

library(hoopR)
library(DBI)
library(dplyr)
source("scripts/functions/dbConnhelpers.R")

league <- "nba"

################
# UPDATE PBP DATABASE TABLE - manually checks existing db for latest date and loads everything after it
################
## PBP ##
## quit working #update_nba_db(dbname="nba", tblname = "pbp", force_rebuild = c(2024), db_connection=conn)
conn <- harvestDBconnect(league = league)
dbSendQuery(conn, "SET GLOBAL local_infile = true;")

# get last date in db and filter season boxes
last.date.query <- paste("SELECT MAX(game_date) FROM pbp WHERE season = ", s, ";", sep="")
last.date <- dbGetQuery(conn, last.date.query)[[1]]
print(paste("pbp: last loaded date = ", last.date))

df <- hoopR::load_nba_pbp(s) %>% filter(game_date > last.date)
print(paste("pbp: loading dates: ", df$game_date %>% unique()))
DBI::dbWriteTable(conn, name = "pbp", value= data.frame(df), row.names = FALSE, overwrite = FALSE, append = TRUE)

## PLAYER BOXSCORES ##
## quit working #load_nba_player_box(seasons = c(2024), dbConnection = conn,tablename = 'playerbox',)

#manually append missing games
# get last date in db and filter season boxes
last.date.query <- paste("SELECT MAX(game_date) FROM playerbox WHERE season = ", s, ";", sep="")
last.date <- dbGetQuery(conn, last.date.query)[[1]]
print(paste("playerbox: last loaded date = ", last.date))

df <- hoopR::load_nba_player_box(s) %>% filter(game_date > last.date)
print(paste("playerbox: loading dates: ", df$game_date %>% unique()))
DBI::dbWriteTable(conn, name = "playerbox", value= data.frame(df), row.names = FALSE, overwrite = FALSE, append = TRUE)

dbSendQuery(conn, "SET GLOBAL local_infile = false;")
dbDisconnect(conn)
######
