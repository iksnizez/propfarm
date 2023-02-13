library(hoopR)
library(DBI)
library(RMySQL)
library(jsonlite)
library(dplyr)
library(dbx)
library(tidyr)
library(stringr)
library(rvest) # html scraping
library(zoo) # rolling averages
#RETICULATE_PYTHON="../propfarmVenv/Scripts/python"
#library(reticulate) # running python script to get the odds scrape
source("scripts/functions/datacrackers.R")
source("scripts/functions/dbhelpers.R")

# full player info history
#hoopR::nba_commonallplayers(season="2022-23")

##################
# Setting variables and hitting api
##################
### static parameters used throughout the script
season = "2022-23"
s = 2023
n.games <- 3
cutoff_date <- Sys.Date() - 12
search.date <- Sys.Date()
today.date.char <- format(search.date, "%Y%m%d")
yesterday.date.char <- format(search.date - 1, "%Y%m%d")  #using to look for B2Bs
tomorrow.date.char <- format(search.date + 1, "%Y%m%d")   #using to look for B2Bs

### grab the games playing today, tomorrow and yesterday
games.today <- espn_nba_scoreboard (season = today.date.char)
games.yesterday <- espn_nba_scoreboard (season = yesterday.date.char)
games.tomorrow <- espn_nba_scoreboard (season = tomorrow.date.char)

### retrieving the player boxscore and schedule for the season, 
# this will be used to access players that are playing today and agg stats
boxscore.player <- hoopR::load_nba_player_box(s)
schedule <- load_nba_schedule(s)

#save paths
betting.file.path <- paste("data/", search.date, "_odds.csv", sep="")
harvest.file.path <- paste("output/", search.date, "_harvest.csv", sep="")

# adding the additional IDs and team names to the nba_teams 
# the default dataframe that loads with the package. using function from datacrackers.R
nba_teams <- update.default.team.data()
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
#grab the teams playing today, tomorrow and yesterday
team.names.today <-  unique(c(games.today$away_team_abb, games.today$home_team_abb))
team.id.today <- unique(c(games.today$home_team_id, games.today$away_team_id))

team.names.yesterday <-  unique(c(games.yesterday$away_team_abb, games.yesterday$home_team_abb))
team.id.yesterday <- unique(c(games.yesterday$home_team_id, games.yesterday$away_team_id))

team.names.tomorrow <-  unique(c(games.tomorrow$away_team_abb, games.tomorrow$home_team_abb))
team.id.tomorrow <- unique(c(games.tomorrow$home_team_id, games.tomorrow$away_team_id))

# grabbing the gameIds for today
gids.today <- unique(games.today$game_id)

#creating team list for front- and backend back-to-backs
back.to.back.first.id <- intersect(team.id.today, team.id.tomorrow)
back.to.back.first <- intersect(team.names.today, team.names.tomorrow)

back.to.back.last.id <- intersect(team.id.today, team.id.yesterday)
back.to.back.last <- intersect(team.names.today, team.names.yesterday)

#creating matchup lookup
matchups.today <- games.today %>% select(home_team_abb, away_team_abb)
matchups.today.full <- games.today %>% select(home_team_abb, away_team_abb, game_id)

# pulling player stats
stat.harvest <- propfarming(boxscore.player, team.id.today, matchups.today.full) %>% ungroup()
#addding date
stat.harvest$date <- search.date
# creating name column without suffixes to join with betting data until ID list is compiled
suffix.rep <- c(" Jr."="", " Sr."="", " III"="", " IV"="", " II"="", 
                "\\."="", "'"="", "'"="")
# updating generic positions with 1 of 5
##pos.rep <- c("^G"="SG", "^F"="PF")
stat.harvest <- stat.harvest %>%
                    mutate(
                        join.names = tolower(stringr::str_replace_all(athlete_display_name, suffix.rep)),
                        #athlete_position_abbreviation = stringr::str_replace_all(
                        #                                            athlete_position_abbreviation,
                        #                                            pos.rep)
                    )

# flag players who have seen large jump in minutes
minutes.boosted <- stat.harvest %>% 
                        mutate(minL3diff = minAvgL3 - minAvg - (minstd * 1.2),
                               minL10diff = minAvgL10 - minAvg - minstd,
                               direction =case_when(
                                   (minL3diff > 0 & minL10diff > 0) ~ "up-ramped",
                                   (minL3diff > 0 & minL10diff < 0) ~ "up-ramping",
                                   (minL3diff < 0 & minL10diff > 0) ~ "down-ramping",
                                   (minL3diff < 0 & minL10diff < 0) ~ "down-ramped",
                                   TRUE ~ "flat"
                            )) %>%
                        filter(minL3diff > 0 | minL10diff > 0) %>%
                        select(athlete_id, athlete_display_name, athlete_position_abbreviation, team_abbreviation, 
                               direction, minAvg,minAvgL3, minstd, minAvgL10, minL3diff, minL10diff) %>%
                        arrange(direction)
minutes.boosted

#####

##################
# scrape odds data and join to player harvest data
##################
betting.table <- read.csv(betting.file.path) %>%
                    pivot_wider(names_from = stat,
                                values_from = c(line, oOdds, uOdds)) %>%
                    mutate(
                        PLAYER= tolower(stringr::str_replace_all(PLAYER, suffix.rep))
                    )

# store the players from the harvest data that did not have any betting info
missing.players <- setdiff(stat.harvest$join.names, betting.table$PLAYER)
missing.players
#right join with betting table on the right so that only players with lines/odds are kept
harvest <- right_join(stat.harvest, 
                      betting.table, 
                      by=c("join.names" = "PLAYER")) %>%
                select(-team, -date.y, -join.names) %>%
                rename(c(date = date.x))

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
                prOscore = (prSynth - prStdL3) - line_PTSREB,
                prUscore = line_PTSREB - (prSynth + prStdL3),
                paOscore = (paSynth - paStdL3) - line_PTSAST,
                paUscore = line_PTSAST - (paSynth + paStdL3),
                raOscore = (raSynth - raStdL3) - line_REBAST,
                raUscore = line_REBAST - (raSynth + raStdL3),
                #sbOscore = (sbSynth - sbStdL3) - line_STLBLK,
                #sbsUscore = line_STLBLK - (sbSynth + sbStdL3),
                date = as.Date(date, format="%Y-%m-%d")
            )
#####

##################
# retrieving opponent ranks by position for last N games
##################
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
                       by=c('opp'='team', 
                            'athlete_position_abbreviation'='athlete_position_abbreviation')
                       )
#renaming columns
harvest <- harvest %>%
    rename(c(team = team_abbreviation,
             player = athlete_display_name,
             pos = athlete_position_abbreviation)
           ) 
#####

##################
# add current day harvest to database
##################
conn <- harvestDBconnect()
dbSendQuery(conn, "SET GLOBAL local_infile = true;")
dbWriteTable(conn, name = "props", value= harvest, row.names = FALSE, overwrite = FALSE, append = TRUE)
#dbx::dbxInsert(conn=conn, table="props", records = harvest)
dbSendQuery(conn, "SET GLOBAL local_infile = false;")
dbDisconnect(conn)

##################
# retrieving stat line from the last game and updating db
##################

# filtering to only the most recent games for the actual stats to add to
# the most recent harvest and determine if over or under won
# The boxscores game_date is actual date + 1, to pull yesterday = use today date
# boxscores are not updated until late  night after all games complete
boxscore.most.recent <- boxscore.player %>% 
                            filter(game_id %in% games.yesterday$game_id)


# processing the box scores
boxscore.most.recent <- boxscore.most.recent %>%
    tidyr::separate(fg3, sep = "-", into = c("fg3m","fg3a")) %>%
    select(game_id, athlete_id, min, pts, reb, ast, stl, blk, fg3m, to) %>% 
    rename(c(minAct = min,
             ptsAct = pts,
             rebAct = reb,
             astAct = ast,
             stlAct = stl,
             blkAct = blk,
             fg3mAct = fg3m,
             toAct = to)
           ) %>%
    mutate(athlete_id = as.numeric(athlete_id),
           minAct = as.integer(minAct),       
           ptsAct = as.integer(ptsAct),
           rebAct = as.integer(rebAct),
           astAct = as.integer(astAct),
           stlAct = as.integer(stlAct),
           blkAct = as.integer(blkAct),
           fg3mAct = as.integer(fg3mAct),
           toAct = as.integer(toAct),
           praAct = ptsAct + rebAct + astAct,
           prAct = ptsAct + rebAct,
           paAct = ptsAct  + astAct,
           raAct = rebAct + astAct,
           sbAct = stlAct + blkAct
    )

# pulling the most recent harvest to add actual and calculate wins
conn <- harvestDBconnect()
dbSendQuery(conn, "SET GLOBAL local_infile = true;")

# database table object - represent as lasy table view that needs to be collected
props.table <- tbl(conn, "props")
# filtering the table and collecting the data
yest <- as.numeric(games.yesterday$game_id)
yesterday.harvest <- props.table %>% 
                        filter(game_id %in% yest) %>%
                        collect()

# filtering the box scores for the players of interest and selecting the stats
updates <- boxscore.most.recent %>% 
                select(athlete_id, minAct, ptsAct, rebAct, astAct, 
                       stlAct, blkAct, fg3mAct, toAct, praAct, 
                       prAct, paAct, raAct, sbAct) %>%
                filter(athlete_id %in% yesterday.harvest$athlete_id)

# players who were in the harvest data but did not show up in the boxscore
missing.boxscores <- setdiff(yesterday.harvest$athlete_id %>% unique(),boxscore.most.recent$athlete_id %>% unique())
missing.players <- harvest %>% filter(athlete_id %in% missing.boxscores) %>% select(player, athlete_id)
missing.players     
# updating the data pulled from the database with the scores from the last game
yesterday.harvest <- rows_update(yesterday.harvest, updates, by="athlete_id")


# calculating over under winners with the stats 
yesterday.harvest <- yesterday.harvest %>%
    mutate(
        ptsWin = ifelse(ptsAct > line_PTS, "o", "u"),
        rebWin = ifelse(rebAct > line_REB, "o", "u"),
        astWin = ifelse(astAct > line_AST, "o", "u"),
        stlWin = ifelse(stlAct > line_STL, "o", "u"),
        blkWin = ifelse(blkAct > line_BLK, "o", "u"),
        fg3mWin = ifelse(fg3mAct > line_THREES, "o", "u"),
        praWin = ifelse(praAct > line_PTSREBAST, "o", "u"),
        prWin = ifelse(prAct > line_PTSREB, "o", "u"),
        paWin = ifelse(paAct > line_PTSAST, "o", "u"),
        raWin = ifelse(raAct > line_REBAST, "o", "u"),
        sbWin = ifelse(sbAct > line_STLBLK, "o", "u")
        )

# sending the data back to the db for updating the table
dbx::dbxUpdate(conn, "props", yesterday.harvest, where_cols = c("game_id", "athlete_id", "date"))

dbSendQuery(conn, "SET GLOBAL local_infile = false;")
dbDisconnect(conn)
#####

# final data output
#write.csv(x = stat.harvest,file =  harvest.file.path, row.names=FALSE)
#write.csv(x = yesterday.harvest,file =  "output/dbBackup.csv", row.names=FALSE)


##################
#
##################
#####

##################
# 
##################
#####


