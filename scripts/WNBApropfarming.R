#2remotes::install_github("sportsdataverse/wehoop")
library(wehoop)
library(DBI)
library(dplyr)
library(jsonlite)
source("scripts/functions/dbConnHelpers.R")
source("scripts/functions/NBAdatacrackers.R")

#####
library(RMySQL)
library(dbx)
library(tidyr)
library(stringr)
library(rvest) # html scraping
library(zoo)
######

##################
# Setting variables and hitting api
##################
### static parameters used throughout the script
league <- 'wnba'
season  <-  "2024-25"
s <-  2025
n.games <- 3
date_change <- -5  ##<<<<<<<<<<<<<<<<<<<<<<<< <<<<<<<<<<<<<<<< ######use negative for going back days
cutoff_date <- Sys.Date() - 12
search.date <- Sys.Date() + date_change

# boxscore  will be used to access players that are playing today and agg stats
boxscore.player <- wehoop::load_wnba_player_box(s) %>% 
                        filter(game_date < search.date,
                               team_id < 90) 

# calculating previous game date
prev.game.dates <- sort(boxscore.player$game_date %>% unique(), decreasing = TRUE)
if(is.na(match(search.date, prev.game.dates))){
    prev.game.index <- 1
} else{
    prev.game.index <- match(search.date, prev.game.dates) + 1
}
 # previous game date
prev.game.date <- prev.game.dates[prev.game.index]

# calculating next game date
season.game.dates <- (wehoop::wnba_schedule(season = most_recent_wnba_season()) %>%
                        select(game_date_est) %>%
                        mutate(game_date_est = as.Date(game_date_est)))$game_date_est %>% 
                        unique() %>%
                        sort(decreasing = TRUE)
                      
next.game.index <- match(search.date, season.game.dates) - 1

#next game date
next.game.date <- season.game.dates[next.game.index]


today.date.char <- format(search.date, "%Y%m%d")
yesterday.date.char <- format(prev.game.date, "%Y%m%d")  #using to look for B2Bs
tomorrow.date.char <- format(next.game.date, "%Y%m%d")   #using to look for B2Bs
 
rm(season.game.dates)

### grab the games playing today, tomorrow and yesterday
games.today <- espn_wnba_scoreboard (season = today.date.char)
games.yesterday <- espn_wnba_scoreboard (season = yesterday.date.char)
games.tomorrow <- espn_wnba_scoreboard (season = tomorrow.date.char)

#####

### retrieving the player boxscore and schedule for the season, 
boxscore.player <- boxscore.player %>%
    # FILTER OUT ASG
    filter(game_id != 401558893) %>%
    # change generic positions 
    mutate(
        athlete_position_abbreviation = case_when(
            athlete_position_abbreviation == "G" ~ "SG",
            athlete_position_abbreviation == "F" ~ "SF",
            TRUE ~ athlete_position_abbreviation
        )
    )

schedule <- load_wnba_schedule(s)

player.info <- wehoop::wnba_commonallplayers(season=season, is_only_current_season = 1)$CommonAllPlayers %>%
    mutate(TEAM_ABBREVIATION = case_when(
        TEAM_ABBREVIATION == "PHO" ~ "PHX",
        TEAM_ABBREVIATION == "CON" ~ "CONN",
        TEAM_ABBREVIATION == "WAS" ~ "WSH",
        TEAM_ABBREVIATION == "LVA" ~ "LV",
        TEAM_ABBREVIATION == "LAS" ~ "LA",
        TEAM_ABBREVIATION == "NYL" ~ "NY",
        TRUE ~ TEAM_ABBREVIATION
    ))

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
stat.harvest <- wnbaPropfarming(boxscore.player,
                         team.id.today, 
                         matchups.today.full, 10,
                         player.info) %>% 
    ungroup()
#addding date
stat.harvest$date <- search.date
# creating name column without suffixes to join with betting data until ID list is compiled
suffix.rep <- c("\\."="", "'"="", "'"=""
                #" Jr."="", " Sr."="", " III"="", " IV"="", " II"="",
                )
# updating generic positions with 1 of 5
##pos.rep <- c("^G"="SG", "^F"="PF")
stat.harvest <- stat.harvest %>%
    mutate(
        join.names = tolower(stringr::str_replace_all(athlete_display_name, suffix.rep))
    ) 


##################
# scrape odds data and join to player harvest data
##################

conn <- harvestDBconnect(league=league)
dbSendQuery(conn, "SET GLOBAL local_infile = true;")

odds.date <- format(search.date, "%Y-%m-%d")
query <- "SELECT 
            o.propId,p.player playerName, p.wehoopId, o.playerId actnetPlayerId, 
            p.joinName, o.date, o.prop, o.line, o.oOdds, o.uOdds
          FROM odds o
          INNER JOIN players p ON o.playerId = p.actnetPlayerId
          WHERE o.date = '"

# flatten a single players odds into a single row
betting.table <- dbGetQuery(conn, paste0(query, odds.date, "'")) %>%
    pivot_wider(names_from = prop,
                values_from = c(line, oOdds, uOdds, propId))

# close conns
dbSendQuery(conn, "SET GLOBAL local_infile = false;")
dbDisconnect(conn)


# store the players from the harvest data that did not have any betting info
# missing.players.odds <- setdiff(stat.harvest$join.names, betting.table$joinName)
# missing.players.odds
#right join with betting table on the right so that only players with lines/odds are kept
harvest <- right_join(stat.harvest, 
                      betting.table, 
                      by=c("join.names" = "joinName")) %>%
    select(-date.y, -join.names) %>%
    rename(c(date = date.x))

#####

##################
# calculating line score - actual line vs avg calcs
##################
harvest <- harvest %>%
    mutate(
        ptsOscore = round((ptsSynth - ptsStdL3) - line_pts,3),
        ptsUscore = round(line_pts - (ptsSynth + ptsStdL3),3),
        rebOscore = round((rebSynth - rebStdL3) - line_reb,3),
        rebUscore = round(line_reb - (rebSynth + rebStdL3),3),
        astOscore = round((astSynth - astStdL3) - line_ast,3),
        astUscore = round(line_ast - (astSynth + astStdL3),3),
        #stlOscore = NA, #round((stlSynth - stlStdL3) - line_STL,3),
        #stlUscore = NA, #round(line_STL - (stlSynth + stlStdL3),3),
        #blkOscore = NA, #round((blkSynth - blkStdL3) - line_BLK,3),
        #blkUscore = NA, #round(line_BLK - (blkSynth + blkStdL3),3),
        fg3mOscore = round((fg3mSynth - fg3mStdL3) - line_threes,3),
        fg3mUscore = round(line_threes - (fg3mSynth + fg3mStdL3),3),
        #praOscore = round((praSynth - praStdL3) - line_pra,3),
        #praUscore = round(line_pra - (praSynth + praStdL3),3),
        #prOscore = round((prSynth - prStdL3) - line_pr,3),
        #prUscore = round(line_pr - (prSynth + prStdL3),3),
        #paOscore = round((paSynth - paStdL3) - line_pa,3),
        #paUscore = round(line_pa - (paSynth + paStdL3),3),
        #raOscore = round((raSynth - raStdL3) - line_ra,3),
        #raUscore = round(line_ra - (raSynth + raStdL3),3),
        #sbOscore = NA, #round((sbSynth - sbStdL3) - line_STLBLK,3),
        #sbUscore = NA, #round(line_STLBLK - (sbSynth + sbStdL3),3),
        date = as.Date(date, format="%Y-%m-%d"),
        game_id = as.numeric(game_id)
    )
#####

##################
# retrieving opponent ranks by position for last N games
##################
# calling function to return teams opponent stats
opp.stats <- stats.last.n.games.opp(season=s,
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
    ) %>%
    select(-c(playerName, wehoopId, actnetPlayerId))

#### need to see if the espn calls have betting info
#game.lines.today <- games.betting.info(gids.today)
#harvest <- harvest %>% left_join(game.lines.today, by="game_id")

wscores <- process.harvest.wnba(harvest = harvest) %>% relocate(c(AvgL3, AvgL10, Synth), .after=line) %>% relocate(Avg, .after=prop)

View(wscores)


# load harvest scores to the db
conn <- harvestDBconnect(league=league)
dbSendQuery(conn, "SET GLOBAL local_infile = true;")

dbWriteTable(conn, name = "props", value= harvest, row.names = FALSE, overwrite = FALSE, append = TRUE)
#dbx::dbxInsert(conn=conn, table="props", records = harvest)

dbSendQuery(conn, "SET GLOBAL local_infile = false;")
dbDisconnect(conn)


#########
# determining winners and losers
#########

# filtering to only the most recent games for the actual stats to add to
# the most recent harvest and determine if over or under won
# The boxscores game_date is actual date + 1, to pull yesterday = use today date
# boxscores are not updated until late  night after all games complete
boxscore.most.recent <- boxscore.player %>%
    mutate(game_id = as.character(game_id)) %>%
    filter(game_id %in% games.yesterday$game_id)

# processing the box scores
boxscore.most.recent <- boxscore.most.recent %>% 
    select(game_id, athlete_id, minutes, points, rebounds, assists, 
           three_point_field_goals_made, steals, blocks, turnovers
    ) %>% 
    rename(c(act_min = minutes,
             act_pts = points,
             act_reb = rebounds,
             act_ast = assists,
             act_stl = steals,
             act_blk = blocks,
             act_threes = three_point_field_goals_made,
             act_to = turnovers
            )
    ) %>%
    mutate(athlete_id = as.numeric(athlete_id),
           act_min = as.integer(act_min),       
           act_pts = as.integer(act_pts),
           act_reb = as.integer(act_reb),
           act_ast = as.integer(act_ast),
           act_stl = as.integer(act_stl),
           act_blk = as.integer(act_blk),
           act_threes = as.integer(act_threes),
           act_to = as.integer(act_to),
           act_pra = act_pts + act_reb + act_ast,
           act_pr = act_pts + act_reb,
           act_pa = act_pts  + act_ast,
           act_ra = act_reb + act_ast,
           act_sb = act_stl + act_blk
    )

# pulling the most recent harvest to add actual and calculate wins
conn <- harvestDBconnect(league=league)
dbSendQuery(conn, "SET GLOBAL local_infile = true;")

yest.prop.query <- paste0("SELECT * FROM props WHERE date = '", prev.game.date, "'")

yesterday.harvest <- dbGetQuery(conn, yest.prop.query)

# filtering the box scores for the players of interest and selecting the stats
updates <- boxscore.most.recent %>% 
                select(athlete_id, act_min, act_pts, act_reb, act_ast, 
                       act_stl, act_blk, act_threes, act_to, act_pra, 
                       act_pr, act_pa, act_ra, act_sb) %>%
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
        win_pts = ifelse(act_pts > line_pts, "o", "u"),
        win_reb = ifelse(act_reb > line_reb, "o", "u"),
        win_ast = ifelse(act_ast > line_ast, "o", "u"),
        #win_stl = NA, #ifelse(act_stl > line_stl, "o", "u"),
        #win_blk = NA, #ifelse(act_blk > line_blk, "o", "u"),
        win_threes = ifelse(act_threes > line_threes, "o", "u"),
        #win_pra = ifelse(act_pra > line_pra, "o", "u"),
        #win_pr = ifelse(act_pr > line_pr, "o", "u"),
        #Win_pa = ifelse(act_pa > line_pa, "o", "u"),
        #win_ra = ifelse(act_ra > line_ra, "o", "u"),
        #win_sb = NA #ifelse(act_sb > line_sb, "o", "u")
    )

# sending the data back to the db for updating the table
dbx::dbxUpdate(conn, "props", yesterday.harvest, where_cols = c("game_id", "athlete_id", "date"))

dbSendQuery(conn, "SET GLOBAL local_infile = false;")
dbDisconnect(conn)
#####


###########
# change in minutes
###########
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
#View(minutes.boosted %>% arrange(desc(direction)))
######






