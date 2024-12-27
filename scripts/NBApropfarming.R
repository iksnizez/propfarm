#remotes::install_github("sportsdataverse/hoopR")
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
source("scripts/functions/dbhelpers.R")

##################
# Setting variables and hitting api
##################
### static parameters used throughout the script
league <- 'nba'
season  <-  "2024-25"
s <-  2025
n.games <- 3
last.n.games.rank <- 10 ##<<<<<<<<<<<<<< <<<<<<<<<<<<<<<<<< # used to calc the last n games stats teams have surrended to pos.
date_change <- 0 ##<<<<<<<<<<<<<<<<<<<<<<<< <<<<<<<<<<<<<<<< ######use negative for going back days
cutoff_date <- Sys.Date() - 12
search.date <- Sys.Date() + date_change

###########################

#############
# DEPRECATED - SCRAPE MOVED TO PYTHON
# retrieve most recent basketball-reference player position estimate - using the players highest % as their POS assignment
#############
### >>>>>>>>>>>>>FUNCTION IN DATACRACKERS NEEDS TO BE UPDATED FOR PLAYERS MOVING, FILTER OUT OLD TEAMS
## use custom function to retrieve basketball-reference position estimates
#bref.pos.estimates <- players.played.position.estimate(s,search.date)
#
## add to playerIds and append to db
#conn <- harvestDBconnect(league = league)
#dbSendQuery(conn, "SET GLOBAL local_infile = true;")
#
## query to retrieve player id since roto doesn't have one
#players.query <- 'SELECT joinName, actnetId actnetPlayerId, hooprId FROM players WHERE hooprId <> 932' # 2 brandon williams, 932 is from decades ago. filter out to remove dups when joining
#playersdb <- dbGetQuery(conn, players.query) %>%
#                mutate(
#                    joinName = trimws(tolower(stringr::str_replace_all(joinName, suffix.rep)))
#                ) 
#
## add actnetid to basketball ref estimates
#bref.pos.estimates <- bref.pos.estimates %>% 
#    left_join(playersdb, by = 'joinName')
#
#rm(playersdb)
#
#dbWriteTable(conn, name = "brefmisc", value= bref.pos.estimates, 
#             row.names = FALSE, overwrite = FALSE, append = TRUE)
#
#dbSendQuery(conn, "SET GLOBAL local_infile = false;")
#dbDisconnect(conn)
#####

#############
# load basketball reference position assignments for players
#############
conn <- harvestDBconnect(league = league)
dbSendQuery(conn, "SET GLOBAL local_infile = true;")

query.pos.ests <- paste("SELECT b.player, b.pos, p.actnetPlayerId, p.hooprId FROM brefmisc b LEFT JOIN players p ON b.joinName = p.joinName WHERE date = '", search.date, "';", sep="")
bref.pos.estimates <-  dbGetQuery(conn, query.pos.ests)

dbSendQuery(conn, "SET GLOBAL local_infile = false;")
dbDisconnect(conn)

bref.pos.estimates <- bref.pos.estimates %>% distinct()

##### FILTER FOR TRADES AND SIGNINGS  Basketball ref doesn't remove players from old teams
bref.pos.estimates <- bref.pos.estimates %>% filter(!(player == 'Scotty Pippen Jr.' & is.na(actnetPlayerId)))
team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Thomas Bryant' & team == 'MIA'))
team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Dennis Schröder' & team == 'BRK'))
team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Reece Beekman' & team == 'GSW'))

#return duplicated rows after players move teams
bref_pos_manual_edits <- bref.pos.estimates[duplicated(bref.pos.estimates$player)|duplicated(bref.pos.estimates$player, fromLast = TRUE),]
if(nrow(bref_pos_manual_edits)>0){
    View(bref_pos_manual_edits)
}
########

# boxscore  will be used to access players that are playing today and agg stats
boxscore.player <- load_nba_player_box(s) %>% 
                        filter(game_date < search.date & team_name != "All-Stars") %>% 
                        distinct()
boxscore.player %>% select(game_date) %>% filter(row_number()==1) %>% pull()

# calculating previous game date
prev.game.dates <- sort(boxscore.player$game_date %>% unique(), decreasing = TRUE)
if(is.na(match(search.date, prev.game.dates))){
    prev.game.index <- 1
} else{
    prev.game.index <- match(search.date, prev.game.dates) + 1
}
# previous game date
prev.game.date <- prev.game.dates[prev.game.index]

# calculating next game date - 
season.game.dates <- nba_schedule(season = season)
#hijacking variable name to extract all-star game gids
dates.allstar <- season.game.dates %>%
                    filter(season_type_description == 'All-Star') %>% 
                    select(game_date)

season.game.dates <- (season.game.dates %>%
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
games.today <- espn_nba_scoreboard (season = today.date.char)
games.yesterday <- espn_nba_scoreboard (season = yesterday.date.char)
games.tomorrow <- espn_nba_scoreboard (season = tomorrow.date.char)

# filter out all star games once they are in the box score data and update player position assignment
boxscore.player <- boxscore.player %>%
    # FILTER OUT ASG
    filter(!game_date %in%  dates.allstar)  %>%
    left_join(bref.pos.estimates %>% select(hooprId, pos) %>% rename(athlete_id = hooprId), by = c('athlete_id')) %>% 
    mutate(athlete_position_abbreviation = case_when(is.na(pos) ~ athlete_position_abbreviation,
                                                     TRUE ~ pos)) %>%
    # change remaining generic positions 
    mutate(
        athlete_position_abbreviation = case_when(
            athlete_position_abbreviation == "G" ~ "SG",
            athlete_position_abbreviation == "F" ~ "SF",
            TRUE ~ athlete_position_abbreviation
        )
    )

schedule <- load_nba_schedule(s)

player.info <- hoopR::nba_commonallplayers(season=season, is_only_current_season = 1)$CommonAllPlayers %>%
                        mutate(TEAM_ABBREVIATION = case_when(
                                    TEAM_ABBREVIATION == "NOP" ~ "NO",
                                    TEAM_ABBREVIATION == "NYK" ~ "NY",
                                    TEAM_ABBREVIATION == "SAS" ~ "SA",
                                    TEAM_ABBREVIATION == "UTA" ~ "UTAH",
                                    TEAM_ABBREVIATION == "WAS" ~ "WSH",
                                    TEAM_ABBREVIATION == "GSW" ~ "GS",
                                    TRUE ~ TEAM_ABBREVIATION
                        ))


# adding the additional IDs and team names to the nba_teams 
# the default dataframe that loads with the package. using function from datacrackers.R
##nba_teams <- update.default.team.data()
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

# if their were days off then today can't be the backend of a BTB
if( (search.date - prev.game.date) > 1){
    back.to.back.last.id <- c()
    back.to.back.last <- c()
# if there were games yesterday compared to search date then today can be the front end of the BTB
} else {
    back.to.back.last.id <- intersect(team.id.today, team.id.yesterday)
    back.to.back.last <- intersect(team.names.today, team.names.yesterday)
}

#creating matchup lookup
matchups.today <- games.today %>% select(home_team_abb, away_team_abb)
matchups.today.full <- games.today %>% select(home_team_abb, away_team_abb, game_id)

# pulling player stats
stat.harvest <- propfarming(box.score.data = boxscore.player, 
                            team.ids = team.id.today, 
                            matchups.today = matchups.today.full, 
                            minFilter = 10,
                            player_info = player.info) %>% 
                    ungroup()
#addding date
stat.harvest$date <- search.date
# creating name column without suffixes to join with betting data until ID list is compiled

# updating generic positions with 1 of 5
stat.harvest <- stat.harvest %>%
                    mutate(
                        join.names = trimws(tolower(stringr::str_replace_all(athlete_display_name, suffix.rep)))
                    ) 
#####

##################
# scrape odds data and join to player harvest data
##################
conn <- harvestDBconnect(league=league)
dbSendQuery(conn, "SET GLOBAL local_infile = true;")

odds.date <- format(search.date, "%Y-%m-%d")
query <- "SELECT 
            p.player PLAYER, t.teamAbbr team, p.hooprId, o.playerId actnetPlayerId, p.joinName, 
            o.date, o.prop, o.line, o.oOdds, o.uOdds
          FROM odds o
          INNER JOIN players p ON o.playerId = p.actnetId
          INNER JOIN teams t on o.teamId = t.actnetTid
          WHERE o.date = '"

# flatten a single players odds into a single row
betting.table <- dbGetQuery(conn, paste0(query, odds.date, "'")) %>%
    pivot_wider(names_from = prop,
                values_from = c(line, oOdds, uOdds))  %>%#, propId))  %>%
    mutate(
        PLAYER= trimws(tolower(stringr::str_replace_all(PLAYER, suffix.rep))),
        team = case_when(
            team == "NOP" ~ "NO",
            team == "NYK" ~ "NY",
            team == "SAS" ~ "SA",
            team == "UTA" ~ "UTAH",
            team == "WAS" ~ "WSH",
            team == "GSW" ~ "GS",
            TRUE ~ team
        ))
#####

##########
# ROTO DOESN"T ALLOW THE CSV DOWNLOADS FOR FREE ANYMORE
##########
# # get roto odds from saved file to fill in any missing from actn
# roto <- read.csv(paste0('data\\',search.date, '_odds.csv')) %>%
#     rename(prop = stat) %>% 
#     mutate(
#         prop = case_when(
#             prop == 'PTS' ~ 'pts',
#             prop == 'REB' ~ 'reb',
#             prop == 'AST' ~ 'ast',
#             prop == 'STL' ~ 'stl',
#             prop == 'BLK' ~ 'blk',
#             prop == 'PTSREBAST' ~ 'pra',
#             prop == 'PTSREB' ~ 'pr',
#             prop == 'PTSAST' ~ 'pa',
#             prop == 'REBAST' ~ 'ra',
#             prop == 'STLBLK' ~ 'sb',
#             prop == 'THREES' ~ 'threes',
#             prop == 'TURNOVERS' ~ 'to'
#         ),
#         PLAYER= trimws(tolower(stringr::str_replace_all(PLAYER, suffix.rep))),
#         joinName = tolower(PLAYER)
#     ) %>% 
#     pivot_wider(names_from = prop,
#                 values_from = c(line, oOdds, uOdds))
# 
# # list of players in roto but not actn
# missing.actn <- setdiff(tolower(roto$PLAYER), betting.table$PLAYER)
# 
# #filter roto widen df to only missing actn players
# roto <- roto %>% 
#             filter(PLAYER %in% missing.actn)
# 
# # query to retrieve player id since roto doesn't have one
# players.query <- 'SELECT joinName, actnetId actnetPlayerId, hooprId FROM players'
# playersdb <- dbGetQuery(conn, players.query)
# 
# #roto doesn't have suffixes but everything else does. UGH!
# # creating name column without suffixes to join with betting data until ID list is compiled
# 
# playersdb <- playersdb %>%
#     mutate(
#         joinName = trimws(tolower(stringr::str_replace_all(joinName, suffix.rep)))
#     ) 
# 
# # add actnetid to roto
# roto <- roto %>% 
#             left_join(playersdb, by = 'joinName') #%>% select(-line_to, -oOdds_to, -uOdds_to)
# 
# rm(playersdb)
# 
# # Save missing prop types from actnet and remove from roto for the first bind with betting table
# # the first bind adds the players that actnet was missing but roto had. after this bind
# # a second bind will add the missing props from actnet with the roto data
# missing.props <- setdiff(colnames(roto), colnames(betting.table))
# roto <- roto %>% 
#             select(-all_of(missing.props))
# 
# # adding any odds that actnet had that roto didn't so the dfs can be rbind
# roto[,setdiff(colnames(betting.table), colnames(roto))] <- NA
# 
# #add roto to betting table
# betting.table <- rbind(betting.table, roto)
# rm(roto)
# 
# # adding in missing actnet props
# roto <- read.csv(paste0('data\\',search.date, '_odds.csv')) %>%
#     rename(prop = stat) %>% 
#     mutate(
#         prop = case_when(
#             prop == 'PTS' ~ 'pts',
#             prop == 'REB' ~ 'reb',
#             prop == 'AST' ~ 'ast',
#             prop == 'STL' ~ 'stl',
#             prop == 'BLK' ~ 'blk',
#             prop == 'PTSREBAST' ~ 'pra',
#             prop == 'PTSREB' ~ 'pr',
#             prop == 'PTSAST' ~ 'pa',
#             prop == 'REBAST' ~ 'ra',
#             prop == 'STLBLK' ~ 'sb',
#             prop == 'THREES' ~ 'threes',
#             prop == 'TURNOVERS' ~ 'to'
#         ),
#         PLAYER= trimws(tolower(stringr::str_replace_all(PLAYER, suffix.rep))),
#         joinName = tolower(PLAYER)
#     ) %>% 
#     pivot_wider(names_from = prop,
#                 values_from = c(line, oOdds, uOdds)) %>%
#     select(joinName, missing.props)
# 
# betting.table <-  left_join(x = betting.table, y = roto, by=c('joinName'))
# rm(roto)
# 
# #dbx::dbxUpdate(conn, "odds", betting.table, where_cols = c("game_id", "athlete_id", "date"))
#####

# close conns
dbSendQuery(conn, "SET GLOBAL local_infile = false;")
dbDisconnect(conn)

#######
# store the players from the harvest data that did not have any betting info
missing.players.odds <- setdiff(stat.harvest$join.names, betting.table$PLAYER)
missing.players.odds

#right join with betting table on the right so that only players with lines/odds are kept

harvest <- right_join(stat.harvest, 
                      betting.table, 
                      #### SINCE HOOPRID IS ADDED ABOVE NOW, THE JOIN HAS BEEN UPDATED TO USE IT 
                      #by=c("join.names" = "joinName")) %>%
                      by = c("athlete_id"="hooprId")) %>% 
                #select(-team, -date.y, -join.names, -hooprId, -PLAYER, -actnetPlayerId) %>%
                select(-team, -date.y, -join.names, -joinName, -PLAYER, -actnetPlayerId) %>%
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
                stlOscore = round((stlSynth - stlStdL3) - line_stl,3),
                stlUscore = round(line_stl - (stlSynth + stlStdL3),3),
                blkOscore = round((blkSynth - blkStdL3) - line_blk,3),
                blkUscore = round(line_blk - (blkSynth + blkStdL3),3),
                fg3mOscore = round((fg3mSynth - fg3mStdL3) - line_threes,3),
                fg3mUscore = round(line_threes - (fg3mSynth + fg3mStdL3),3),
                praOscore = round((praSynth - praStdL3) - line_pra,3),
                praUscore = round(line_pra - (praSynth + praStdL3),3),
                prOscore = round((prSynth - prStdL3) - line_pr,3),
                prUscore = round(line_pr - (prSynth + prStdL3),3),
                paOscore = round((paSynth - paStdL3) - line_pa,3),
                paUscore = round(line_pa - (paSynth + paStdL3),3),
                raOscore = round((raSynth - raStdL3) - line_ra,3),
                raUscore = round(line_ra - (raSynth + raStdL3),3),
                sbOscore = round((sbSynth - sbStdL3) - line_sb,3),
                sbUscore = round(line_sb - (sbSynth + sbStdL3),3),
                date = as.Date(date, format="%Y-%m-%d"),
                game_id = as.numeric(game_id)
            )
#####

##################
# retrieving opponent ranks by position for last N games
##################
# when teams have less than 15 games played, look back to the min played so rank inputs are equal - only important early season
min.gp <- hoopR::nba_leaguestandings()$Standings %>%
                    select(WINS, LOSSES) %>% 
                    mutate(gp = as.numeric(WINS) + as.numeric(LOSSES)) %>%
                    select(gp) %>% 
                    min()
lookback.days.opp.ranks <- min(min.gp, last.n.games.rank)   ############################## <<<<<<<<<<<<<<<<<<<<<<<<< last N game ranks

# calling function to return teams opponent stats
opp.stats <- stats.last.n.games.opp(season=s,
                       num.game.lookback =lookback.days.opp.ranks, 
                       box.scores=boxscore.player, 
                       schedule=schedule %>% filter(date < search.date),
                       no.date=TRUE)

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

scores <- process.harvest(harvest = harvest) %>% relocate(c(AvgL3, AvgL10, Synth), .after=line) %>% relocate(Avg, .after=prop)
View(scores)
#a <- scores
#####

##################
# adding game over/under and spread to the harvest
##################
#game.lines.today <- games.betting.info(gids.today)
#harvest <- harvest %>% left_join(game.lines.today, by="game_id")
#####

##################
# add current day harvest to database
##################
# filter any players out if needed
# remove.players <- c()
# harvest <- harvest %>%  filter(!player %in% remove.players)
conn <- harvestDBconnect(league = league)
dbSendQuery(conn, "SET GLOBAL local_infile = true;")
dbWriteTable(conn, name = "props", value= harvest, row.names = FALSE, overwrite = FALSE, append = TRUE)
#dbx::dbxInsert(conn=conn, table="props", records = harvest)
dbSendQuery(conn, "SET GLOBAL local_infile = false;")
dbDisconnect(conn)
print(paste("most recent prop scores added ", search.date, sep=' ' ))
#####

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
                filter(athlete_id %in% yesterday.harvest$athlete_id) %>% 
                unique()

# players who were in the harvest data but did not show up in the boxscore
missing.boxscores <- setdiff(boxscore.most.recent$athlete_id %>% unique(),
                             yesterday.harvest$athlete_id %>% unique())
missing.players <- yesterday.harvest %>% filter(athlete_id %in% missing.boxscores) %>% select(player, athlete_id)
missing.players
# updating the data pulled from the database with the scores from the last game
#updates <- updates %>% distinct()
yesterday.harvest <- rows_update(yesterday.harvest, updates, by="athlete_id")


# calculating over under winners with the stats
yesterday.harvest <- yesterday.harvest %>%
    mutate(
        win_pts = ifelse(act_pts > line_pts, "o", "u"),
        win_reb = ifelse(act_reb > line_reb, "o", "u"),
        win_ast = ifelse(act_ast > line_ast, "o", "u"),
        win_stl = ifelse(act_stl > line_stl, "o", "u"),
        win_blk = ifelse(act_blk > line_blk, "o", "u"),
        win_threes = ifelse(act_threes > line_threes, "o", "u"),
        win_pra = ifelse(act_pra > line_pra, "o", "u"),
        win_pr = ifelse(act_pr > line_pr, "o", "u"),
        Win_pa = ifelse(act_pa > line_pa, "o", "u"),
        win_ra = ifelse(act_ra > line_ra, "o", "u"),
        win_sb = ifelse(act_sb > line_sb, "o", "u")
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
# flag players who have seen large jump in minutes
##################
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


