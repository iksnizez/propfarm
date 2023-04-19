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
source("scripts/functions/NBAdatacrackers.R")
source("scripts/functions/NBAdbhelpers.R")

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

#####

### retrieving the player boxscore and schedule for the season, 
# this will be used to access players that are playing today and agg stats
boxscore.player <- hoopR::load_nba_player_box(s) 

###############
# adding in the playin + playoff  games to the boxscores
# originally used  in prop farm when the boxscore wasn't updating at the end of 23
##########
# missing dates need to be added as they happen
missing.game.dates <- c(
    "2023-04-11", "2023-04-12", "2023-04-14", "2023-04-15",  "2023-04-16", 
    "2023-04-17", "2023-04-18"
)
for (i in missing.game.dates){
    gm.date  <-  gsub("-", "" , i,)
    gids <- c(espn_nba_scoreboard (season = gm.date)$game_id)
    
    if(i == missing.game.dates[1]){
        for(j in gids){
            if(j == gids[1]){
                missing <- hoopR::espn_nba_player_box(j)
                missing$game_id <- j
                missing$game_date <- as.Date(i)
            } else{
                temp <- hoopR::espn_nba_player_box(j)
                temp$game_id <- j
                temp$game_date <- as.Date(i)
                missing <- rbind(missing, temp)
            }
        }
    }
    else{
        for(j in gids){
            temp <- hoopR::espn_nba_player_box(j)
            temp$game_id <- j
            temp$game_date <- as.Date(i)
            missing <- rbind(missing, temp)
        }
    }
}

# merging back to season boxscore data
boxscore.player <- rbind(boxscore.player, missing, fill=TRUE) %>%
    arrange(athlete_id, desc(game_date)) 
##################

boxscore.player <- boxscore.player %>%
    # FILTER OUT ASG
    filter(game_id != 401524696) %>%
    # change generic positions 
    mutate(
        athlete_position_abbreviation = case_when(
            athlete_position_abbreviation == "G" ~ "SG",
            athlete_position_abbreviation == "F" ~ "SF",
            TRUE ~ athlete_position_abbreviation
        )
    )

schedule <- load_nba_schedule(s)

player.info <- hoopR::nba_commonallplayers(season="2022-23", is_only_current_season = 1)$CommonAllPlayers %>%
                        mutate(TEAM_ABBREVIATION = case_when(
                                    TEAM_ABBREVIATION == "NOP" ~ "NO",
                                    TEAM_ABBREVIATION == "NYK" ~ "NY",
                                    TEAM_ABBREVIATION == "SAS" ~ "SA",
                                    TEAM_ABBREVIATION == "UTA" ~ "UTAH",
                                    TEAM_ABBREVIATION == "WAS" ~ "WSH",
                                    TEAM_ABBREVIATION == "GSW" ~ "GS",
                                    TRUE ~ TEAM_ABBREVIATION
                        ))

#save paths
betting.file.path <- paste("data/", search.date, "_odds.csv", sep="")
harvest.file.path <- paste("output/", search.date, "_harvest.csv", sep="")

# adding the additional IDs and team names to the nba_teams 
# the default dataframe that loads with the package. using function from datacrackers.R
nba_teams <- update.default.team.data()
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
stat.harvest <- propfarming(boxscore.player, 
                            team.id.today, 
                            matchups.today.full, 10,
                            player.info) %>% 
                    ungroup()
#addding date
stat.harvest$date <- search.date
# creating name column without suffixes to join with betting data until ID list is compiled
suffix.rep <- c(" Jr."="", " Sr."="", " III"="", " IV"="", " II"="", 
                "\\."="", "'"="", "'"="")
# updating generic positions with 1 of 5
##pos.rep <- c("^G"="SG", "^F"="PF")
stat.harvest <- stat.harvest %>%
                    mutate(
                        join.names = tolower(stringr::str_replace_all(athlete_display_name, suffix.rep))
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
                        select(athlete_id,  athlete_display_name, athlete_position_abbreviation, team_abbreviation, 
                               direction, minAvg,minAvgL3, minAvgL10, minstd, minL3diff, minL10diff) %>%
                        arrange(direction)
View(minutes.boosted %>% arrange(desc(direction)))

#####

##################
# scrape odds data and join to player harvest data
##################
betting.table <- read.csv(betting.file.path) %>%
                    pivot_wider(names_from = stat,
                                values_from = c(line, oOdds, uOdds)) %>%
                    mutate(
                        PLAYER= tolower(stringr::str_replace_all(PLAYER, suffix.rep)),
                        team = case_when(
                            team == "NOP" ~ "NO",
                            team == "NYK" ~ "NY",
                            team == "SAS" ~ "SA",
                            team == "UTA" ~ "UTAH",
                            team == "WAS" ~ "WSH",
                            team == "GSW" ~ "GS",
                            TRUE ~ team
                    ))


# store the players from the harvest data that did not have any betting info
missing.players.odds <- setdiff(stat.harvest$join.names, betting.table$PLAYER)
missing.players.odds
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
                ptsOscore = round((ptsSynth - ptsStdL3) - line_PTS,3),
                ptsUscore = round(line_PTS - (ptsSynth + ptsStdL3),3),
                rebOscore = round((rebSynth - rebStdL3) - line_REB,3),
                rebUscore = round(line_REB - (rebSynth + rebStdL3),3),
                astOscore = round((astSynth - astStdL3) - line_AST,3),
                astUscore = round(line_AST - (astSynth + astStdL3),3),
                stlOscore = round((stlSynth - stlStdL3) - line_STL,3),
                stlUscore = round(line_STL - (stlSynth + stlStdL3),3),
                blkOscore = round((blkSynth - blkStdL3) - line_BLK,3),
                blkUscore = round(line_BLK - (blkSynth + blkStdL3),3),
                fg3mOscore = round((fg3mSynth - fg3mStdL3) - line_THREES,3),
                fg3mUscore = round(line_THREES - (fg3mSynth + fg3mStdL3),3),
                praOscore = round((praSynth - praStdL3) - line_PTSREBAST,3),
                praUscore = round(line_PTSREBAST - (praSynth + praStdL3),3),
                prOscore = round((prSynth - prStdL3) - line_PTSREB,3),
                prUscore = round(line_PTSREB - (prSynth + prStdL3),3),
                paOscore = round((paSynth - paStdL3) - line_PTSAST,3),
                paUscore = round(line_PTSAST - (paSynth + paStdL3),3),
                raOscore = round((raSynth - raStdL3) - line_REBAST,3),
                raUscore = round(line_REBAST - (raSynth + raStdL3),3),
                sbOscore = round((sbSynth - sbStdL3) - line_STLBLK,3),
                sbUscore = round(line_STLBLK - (sbSynth + sbStdL3),3),
                date = as.Date(date, format="%Y-%m-%d"),
                game_id = as.numeric(game_id)
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
# adding game over/under and spread to the harvest
##################
game.lines.today <- games.betting.info(gids.today)
harvest <- harvest %>% left_join(game.lines.today, by="game_id")

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
                            mutate(game_id = as.character(game_id)) %>%
                            filter(game_id %in% games.yesterday$game_id)


# processing the box scores
boxscore.most.recent <- boxscore.most.recent %>% 
    select(game_id, athlete_id, minutes, points, rebounds, assists, steals, 
           blocks, three_point_field_goals_made, turnovers) %>% 
    rename(c(minAct = minutes,
             ptsAct = points,
             rebAct = rebounds,
             astAct = assists,
             stlAct = steals,
             blkAct = blocks,
             fg3mAct = three_point_field_goals_made,
             toAct = turnovers)
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
missing.boxscores <- setdiff(boxscore.most.recent$athlete_id %>% unique(),
                             yesterday.harvest$athlete_id %>% unique())
missing.players <- yesterday.harvest %>% filter(athlete_id %in% missing.boxscores) %>% select(player, athlete_id)
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
#
##################
#####

##################
# 
##################
#####


