library(hoopR)
library(DBI)
library(RMySQL)
library(jsonlite)
library(dplyr)
library(tidyr)
library(stringr)
library(rvest) # html scraping
library(zoo) # rolling averages
#RETICULATE_PYTHON="../propfarmVenv/Scripts/python"
#library(reticulate) # running python script to get the odds scrape
source("scripts/functions/datacrackers.R")


# static parameters used throughout the script
season = "2022-23"
s = 2023
n.games <- 3
cutoff_date <- Sys.Date() - 12
search.date <- Sys.Date()
betting.file.path <- paste("data/", search.date, "_odds.csv", sep="")
harvest.file.path <- paste("output/", search.date, "_harvest.csv", sep="")

# adding the additional IDs and team names to the nba_teams 
# the default dataframe that loads with the package. using function from datacrackers.R
nba_teams <- update.default.team.data()

####

##################
# connect to db
##################
#import credentials
path <- '../../Notes-General/config.txt'
creds<-readLines(path)
creds<-lapply(creds,fromJSON)

dbUser <- creds[[1]][2]$mysqlSurface$users[2]
dbPw <- creds[[1]][2]$mysqlSurface$creds$data
dbHost <- creds[[1]][2]$mysqlSurface$dbNBA$host
dbName <- creds[[1]][2]$mysqlSurface$dbNBA$database


#connect to MySQL db    
conn = DBI::dbConnect(RMySQL::MySQL(),
                      dbname=dbName,
                      host=dbHost,
                      port=3306,
                      user=dbUser,
                      password=dbPw)
remove(dbUser)
remove(dbPw)
remove(dbHost)
remove(dbName)
remove(creds)
#####

##################
##data import - 2022-23 season
##################
# search on the year the season ends
nba_pbp <- hoopR::load_nba_pbp(2023)
#####

##################
# updating team records and ats records
##################
#updating pbp to be able to calculate all teams ATS records
finals <- nba_pbp %>% filter(type_text == "End Game") %>% 
    mutate(
        homeWin = ifelse(home_score > away_score,1,0),
        awayWin = ifelse(away_score > home_score,1,0),
        homeATSw= ifelse((home_score + game_spread) > away_score, 1,0),
        awayATSw = ifelse((away_score + home_team_spread) > home_score, 1,0),
        pushATS = ifelse((away_score + home_team_spread) == home_score, 1,0)
    )

# calculating records and ats records for every team
teams = unique(c(finals$away_team_abbrev, finals$home_team_abbrev))
team.records <- data.frame(team=character(), w=integer(), l=integer(), 
                           wATS=integer(), lATS=integer(), tATS=integer())

for (team in teams) {
    team.results <- finals %>% 
        filter(away_team_abbrev == team | home_team_abbrev == team) %>%
        mutate(teamWin= ifelse((away_team_abbrev == team & awayWin == 1) |(home_team_abbrev == team & homeWin == 1),  
                               1, 0),
               teamATSwin= ifelse((away_team_abbrev == team & awayATSw== 1) |(home_team_abbrev == team & homeATSw == 1),  
                                  1, 0)
        )
    
    record <- list(team, sum(team.results$teamWin), # wins
                   nrow(team.results) - sum(team.results$teamWin), # losses
                   sum(team.results$teamATSwin), # wins ATS
                   nrow(team.results) - sum(team.results$teamATSwin) - sum(team.results$pushATS), #losses ATS
                   sum(team.results$pushATS) # pushes
    )
    
    team.records <- rbind(team.records, setNames(record, names(team.records)))
    
}
team.records %>% arrange(team) %>% select(team, wATS, lATS, tATS)
#####

##################
# gathering teams and players playing today
##################
# convert today's date to string for schedule function
today.date.char <- format(search.date, "%Y%m%d")
yesterday.date.char <- format(search.date - 1, "%Y%m%d")  #using to look for B2Bs
tomorrow.date.char <- format(search.date + 1, "%Y%m%d")   #using to look for B2Bs

#grab the games playing today, tomorrow and yesterday
games.today <- espn_nba_scoreboard (season = today.date.char)
games.yesterday <- espn_nba_scoreboard (season = yesterday.date.char)
games.tomorrow <- espn_nba_scoreboard (season = tomorrow.date.char)

#grab the teams playing today, tomorrow and yesterday
team.names.today <-  unique(c(games.today$away_team_abb, games.today$home_team_abb))
team.names.yesterday <-  unique(c(games.yesterday$away_team_abb, games.yesterday$home_team_abb))
team.names.tomorrow <-  unique(c(games.tomorrow$away_team_abb, games.tomorrow$home_team_abb))
team.id.today <- unique(c(games.today$home_team_id, games.today$away_team_id))
team.id.yesterday <- unique(c(games.yesterday$home_team_id, games.today$away_team_id))
team.id.tomorrow <- unique(c(games.tomorrow$home_team_id, games.today$away_team_id))

# grabbing the gameIds for today
gids.today <- unique(games.today$game_id)

#creating team list for front- and backend back-to-backs
back.to.back.first.id <- intersect(team.id.today, team.id.yesterday)
back.to.back.last.id <- intersect(team.id.today, team.id.tomorrow)
back.to.back.first <- intersect(team.names.today, team.names.yesterday)
back.to.back.last <- intersect(team.names.today, team.names.tomorrow)

#creating matchup lookup
matchups.today <- games.today %>% select(home_team_abb, away_team_abb)

# retrieving the player boxscore for the season, 
# this will be used to access players that are playing today and agg stats
boxscore.player <- hoopR::load_nba_player_box(s) 
harvest <- propfarming(boxscore.player, team.id.today, matchups.today)
harvest$date <- search.date
#####

##################
# scrape odds data and join to player harvest data
##################
betting.table <- read.csv(betting.file.path) %>%
                    pivot_wider(names_from = stat,
                                values_from = c(line, oOdds, uOdds))


# store the players from the harvest data that did not have any betting info
missing.players <- setdiff(harvest$athlete_display_name, betting.table$PLAYER)
harvest <- merge(harvest, betting.table, 
                 by.x = "athlete_display_name",
                 by.y = "PLAYER"
                 ) %>%
            select(-team, -date.y)
#####

##################
# calculating line score - actual line vs avg calcs
##################
harvest <- harvest %>%
            mutate(
                ptsOscore = (ptsSynth - ptsStdL3) - line_PTS,
                ptsUscore = line_PTS - (ptsSynth + ptsStdL3),
                rebOscore = (rebSynth - rebStdL3) - line_REB,
                rebUscore = line_REB - (rebSynth + rebStdL3),
                astOscore = (astSynth - astStdL3) - line_AST,
                astUscore = line_AST - (astSynth + astStdL3),
                #stlOscore = (stlSynth - stlStdL3) - line_STL,
                #stlUscore = line_STL - (stlSynth + stlStdL3),
                #blkOscore = (blkSynth - blkStdL3) - line_BLK,
                #blkUscore = line_BLK - (blkSynth + blkStdL3),
                fg3mOscore = (fg3mSynth - fg3mStdL3) - line_THREES,
                fg3mUscore = line_THREES - (fg3mSynth + fg3mStdL3),
                praOscore = (praSynth - praStdL3) - line_PTSREBAST,
                praUscore = line_PTSREBAST - (praSynth + praStdL3),
                paOscore = (paSynth - paStdL3) - line_PTSAST,
                paUscore = line_PTSAST - (paSynth + paStdL3),
                raOscore = (raSynth - raStdL3) - line_REBAST,
                raUscore = line_REBAST - (raSynth + raStdL3),
                #sbOscore = (sbSynth - sbStdL3) - line_STLBLK,
                #sbsUscore = line_STLBLK - (sbSynth + sbStdL3)
            )

View(harvest)
#####

##################
# retrieving opponent ranks by position for last N games
##################
schedule <- load_nba_schedule(s)
# calling function to return teams opponent stats
opp.stats <- opp.stats.last.n.games(season=s,
                       num.game.lookback =15, 
                       box.scores=boxscore.player, 
                       schedule=schedule)
# dropping the count columns and keeping the ranks
opp.position.ranks <- opp.stats$team.opp.stats.by.pos %>%
                        select(team, athlete_position_abbreviation, contains("Rank"))
# joining the ranks back to the player stat and prop harvest
harvest <- harvest %>%
            inner_join(opp.position.ranks,
                       by=c('team_abbreviation'='team', 
                            'athlete_position_abbreviation'='athlete_position_abbreviation')
                       )
#renaming columns
harvest <- harvest %>%
    rename(c(team = team_abbreviation,
             player = athlete_display_name,
             pos = athlete_position_abbreviation)
           )

View(harvest)
#####

##################
# retrieving stat line from the last game
##################
gids.today
stats <- espn_nba_player_box(gids.today[1])
#harvest$actPts <- 
#harvest$actReb <- 
#harvest$actAst <- 
#harvest$actStl <- 
#harvest$actBlk <- 
#harvest$actFg3m <- 
#####

# final data output
write.csv(x = harvest,file =  harvest.file.path, row.names=FALSE)

###############################################
# NEED TO ADD: 
    #actual results to determine winners
###############################################

##################
# 
##################
#####


