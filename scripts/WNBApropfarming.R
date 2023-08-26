library(wehoop)
library(DBI)
library(dplyr)
library(jsonlite)
source("scripts/functions/dbhelpers.R")
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
season = "2022-23"
s = 2023
n.games <- 3
cutoff_date <- Sys.Date() - 12
search.date <- Sys.Date() 
today.date.char <- format(search.date, "%Y%m%d")
yesterday.date.char <- format(search.date - 1, "%Y%m%d")  #using to look for B2Bs
tomorrow.date.char <- format(search.date + 1, "%Y%m%d")   #using to look for B2Bs

### grab the games playing today, tomorrow and yesterday
games.today <- espn_wnba_scoreboard (season = today.date.char)
games.yesterday <- espn_wnba_scoreboard (season = yesterday.date.char)
games.tomorrow <- espn_wnba_scoreboard (season = tomorrow.date.char)
#####

####################
## FUNCTION TO CALCULATE PROPFARM PLAYER STATS FROM wnba BOX
####################

# filtering entire season box score to only the teams playing today
pfarming <- function(box.score.data, team.ids, matchups.today, minFilter=20, player_info=NULL){
    # ingest season boxscore data from load_wnba_player_box and vector of team ids and dataframe of home/away teams
    # minFilter is used to filter out insignificant players.
    # output filtered list of players and their prop farm stat data for today
    
    #convert team ids to player ids in order to filter box scores
    # using team ids doesn't capture player data when they have been traded midseason
    all.pids <- box.score.data %>% 
        filter(team_id %in% team.ids)
    pids <- unique(c(all.pids$athlete_id))
    
    players.today <- box.score.data %>% 
        filter(athlete_id %in% pids & 
                   did_not_play == FALSE
        ) %>% 
        rename(c(
            fgm = field_goals_made,
            fga = field_goals_attempted,
            fg3m = three_point_field_goals_made,
            fg3a = three_point_field_goals_attempted,
            ftm = free_throws_made,
            fta = free_throws_attempted,
            min = minutes,
            pts = points,
            reb = rebounds,
            ast = assists,
            stl = steals,
            blk = blocks,
            to = turnovers,
            pf = fouls,
            oreb = offensive_rebounds,
            dreb = defensive_rebounds
        )) %>%
        arrange(athlete_id, desc(game_date))
    
    # breaking the shooting attempts from makes and converting to numbers
    players.today <-  players.today %>%
        mutate(
            pts =  as.numeric(as.character(pts)),
            reb =  as.numeric(as.character(reb)),
            ast =  as.numeric(as.character(ast)),
            stl =  as.numeric(as.character(stl)),
            blk =  as.numeric(as.character(blk)),
            min =  as.numeric(as.character(min)),
            fgm =  as.numeric(as.character(fgm)),
            fga =  as.numeric(as.character(fga)),
            fg3m =  as.numeric(as.character(fg3m)),
            fg3a =  as.numeric(as.character(fg3a)),
            ftm =  as.numeric(as.character(ftm)),
            fta =  as.numeric(as.character(fta)),
            to =  as.numeric(as.character(to)),
            pf =  as.numeric(as.character(pf)),
            pra = pts + ast + reb,
            pr = pts + reb,
            pa = pts + ast,
            ra = ast + reb,
            sb = stl + blk
        )
    
    # calculating player season averages and standard dev
    players.today.season.avgs <- players.today %>%
        select(athlete_id,min, pts, reb, ast, stl, blk, fg3m, pra, pr, pa, ra, sb) %>%
        group_by(athlete_id) %>%
        summarize(gp = n(),
                  minAvg = round(mean(min, na.rm=TRUE),2),
                  ptsAvg = round(mean(pts, na.rm=TRUE),2),
                  rebAvg = round(mean(reb, na.rm=TRUE),2),
                  astAvg = round(mean(ast, na.rm=TRUE),2),
                  stlAvg = round(mean(stl, na.rm=TRUE),2),
                  blkAvg = round(mean(blk, na.rm=TRUE),2),
                  fg3mAvg = round(mean(fg3m, na.rm=TRUE),2),
                  praAvg = round(mean(pra, na.rm=TRUE),2),
                  prAvg = round(mean(pr, na.rm=TRUE),2),
                  paAvg = round(mean(pa, na.rm=TRUE),2),
                  raAvg = round(mean(ra, na.rm=TRUE),2),
                  sbAvg = round(mean(sb, na.rm=TRUE),2),
                  minstd = round(sd(min, na.rm=TRUE),2),
                  ptsStd = round(sd(pts, na.rm=TRUE),2),
                  rebStd = round(sd(reb, na.rm=TRUE),2),
                  astStd = round(sd(ast, na.rm=TRUE),2),
                  stlStd = round(sd(stl, na.rm=TRUE),2),
                  blkStd = round(sd(blk, na.rm=TRUE),2),
                  fg3mStd = round(sd(fg3m, na.rm=TRUE),2),
                  praStd = round(sd(pra, na.rm=TRUE),2),
                  prStd = round(sd(pr, na.rm=TRUE),2),
                  paStd = round(sd(pa, na.rm=TRUE),2),
                  raStd = round(sd(ra, na.rm=TRUE),2),
                  sbStd = round(sd(sb, na.rm=TRUE),2)
        )
    
    # adding the 3 game averages and standard deviations
    players.today.l3.avgs <- players.today %>%
        select(athlete_id,min, pts, reb, ast, stl, blk, fg3m, pra, pr, pa, ra, sb) %>%
        group_by(athlete_id) %>%
        filter(row_number()<=3) %>%
        summarize(minAvgL3 = round(mean(min, na.rm=TRUE),2),
                  ptsAvgL3 = round(mean(pts, na.rm=TRUE),2),
                  rebAvgL3 = round(mean(reb, na.rm=TRUE),2),
                  astAvgL3 = round(mean(ast, na.rm=TRUE),2),
                  stlAvgL3 = round(mean(stl, na.rm=TRUE),2),
                  blkAvgL3 = round(mean(blk, na.rm=TRUE),2),
                  fg3mAvgL3 = round(mean(fg3m, na.rm=TRUE),2),
                  praAvgL3 = round(mean(pra, na.rm=TRUE),2),
                  prAvgL3 = round(mean(pr, na.rm=TRUE),2),
                  paAvgL3 = round(mean(pa, na.rm=TRUE),2),
                  raAvgL3 = round(mean(ra, na.rm=TRUE),2),
                  sbAvgL3 = round(mean(sb, na.rm=TRUE),2),
                  minStdL3 = round(sd(min, na.rm=TRUE),2),
                  ptsStdL3 = round(sd(pts, na.rm=TRUE),2),
                  rebStdL3 = round(sd(reb, na.rm=TRUE),2),
                  astStdL3 = round(sd(ast, na.rm=TRUE),2),
                  stlStdL3 = round(sd(stl, na.rm=TRUE),2),
                  blkStdL3 = round(sd(blk, na.rm=TRUE),2),
                  fg3mStdL3 = round(sd(fg3m, na.rm=TRUE),2),
                  praStdL3 = round(sd(pra, na.rm=TRUE),2),
                  prStdL3 = round(sd(pr, na.rm=TRUE),2),
                  paStdL3 = round(sd(pa, na.rm=TRUE),2),
                  raStdL3 = round(sd(ra, na.rm=TRUE),2),
                  sbStdL3 = round(sd(sb, na.rm=TRUE),2)
        )
    
    # adding the 10 game averages and standard deviations
    players.today.l10.avgs <- players.today %>%
        select(athlete_id,min, pts, reb, ast, stl, blk, fg3m, pra, pr, pa, ra, sb) %>%
        group_by(athlete_id) %>%
        filter(row_number()<=10) %>%
        summarize(minAvgL10 = round(mean(min, na.rm=TRUE),2),
                  ptsAvgL10 = round(mean(pts, na.rm=TRUE),2),
                  rebAvgL10 = round(mean(reb, na.rm=TRUE),2),
                  astAvgL10 = round(mean(ast, na.rm=TRUE),2),
                  stlAvgL10 = round(mean(stl, na.rm=TRUE),2),
                  blkAvgL10 = round(mean(blk, na.rm=TRUE),2),
                  fg3mAvgL10 = round(mean(fg3m, na.rm=TRUE),2),
                  praAvgL10 = round(mean(pra, na.rm=TRUE),2),
                  prAvgL10 = round(mean(pr, na.rm=TRUE),2),
                  paAvgL10 = round(mean(pa, na.rm=TRUE),2),
                  raAvgL10 = round(mean(ra, na.rm=TRUE),2),
                  sbAvgL10 = round(mean(sb, na.rm=TRUE),2),
                  minStdL10 = round(sd(min, na.rm=TRUE),2),
                  ptsStdL10 = round(sd(pts, na.rm=TRUE),2),
                  rebStdL10 = round(sd(reb, na.rm=TRUE),2),
                  astStdL10 = round(sd(ast, na.rm=TRUE),2),
                  stlStdL10 = round(sd(stl, na.rm=TRUE),2),
                  blkStdL10 = round(sd(blk, na.rm=TRUE),2),
                  fg3mStdL10 = round(sd(fg3m, na.rm=TRUE),2),
                  praStdL10 = round(sd(pra, na.rm=TRUE),2),
                  prStdL10 = round(sd(pr, na.rm=TRUE),2),
                  paStdL10 = round(sd(pa, na.rm=TRUE),2),
                  raStdL10 = round(sd(ra, na.rm=TRUE),2),
                  sbStdL10 = round(sd(sb, na.rm=TRUE),2)
        )
    
    #merging the season avg data with the last 3 avg data and last 10 avg
    df <- merge(players.today.season.avgs, players.today.l3.avgs, by = "athlete_id")
    df <- merge(df, players.today.l10.avgs, by = "athlete_id")
    
    ###adding the player name back to the data
    # grabbing current roster info
    if(is.null(player_info)){
        player.info <- wehoop::wnba_commonallplayers(season="2022-23", is_only_current_season = 1)$CommonAllPlayers %>%
            select(PERSON_ID, DISPLAY_FIRST_LAST, TEAM_ABBREVIATION) %>%
            mutate(TEAM_ABBREVIATION = case_when(
                TEAM_ABBREVIATION == "PHO" ~ "PHX",
                TEAM_ABBREVIATION == "CON" ~ "CONN",
                TEAM_ABBREVIATION == "WAS" ~ "WSH",
                TEAM_ABBREVIATION == "LVA" ~ "LV",
                TEAM_ABBREVIATION == "LAS" ~ "LA",
                TEAM_ABBREVIATION == "NYL" ~ "NY",
                TRUE ~ TEAM_ABBREVIATION
            )) %>%
            rename(c(
                athlete_id = PERSON_ID,
                athlete_display_name = DISPLAY_FIRST_LAST,
                team_abbreviation = TEAM_ABBREVIATION
            )) 
    }
    else{
        player.info <- player_info %>%
            select(PERSON_ID, DISPLAY_FIRST_LAST, TEAM_ABBREVIATION) %>%
            rename(c(
                athlete_id = PERSON_ID,
                athlete_display_name = DISPLAY_FIRST_LAST,
                team_abbreviation = TEAM_ABBREVIATION
            ))
    }
    
    #creating lookup for current team
    pi <- player.info$team_abbreviation
    names(pi) <- player.info$athlete_display_name
    
    # adding back extra athlete details
    df <- merge(players.today %>% 
                    select(athlete_id, athlete_display_name, athlete_position_abbreviation, team_abbreviation) %>% 
                    group_by(athlete_id)%>%
                    filter(row_number()==n()), 
                df, 
                by = "athlete_id",
                all.x = TRUE)
    
    # filtering data to players with >=20(or provided) avg. minutes in the last 10 games
    # adding:
    #back-to-back flag 
    #opponent
    df <- df %>%
        filter(minAvgL10 >= minFilter) %>%
        rowwise() %>%
        mutate(ptsSynth = synth.avg(ptsAvg, ptsAvgL3, 3, gp),
               rebSynth = synth.avg(rebAvg, rebAvgL3, 3, gp),
               astSynth = synth.avg(astAvg, astAvgL3, 3, gp),
               stlSynth = synth.avg(stlAvg, stlAvgL3, 3, gp),
               blkSynth = synth.avg(blkAvg, blkAvgL3, 3, gp),
               fg3mSynth = synth.avg(fg3mAvg, fg3mAvgL3, 3, gp),
               praSynth = synth.avg(praAvg, praAvgL3, 3, gp),
               prSynth = synth.avg(prAvg, prAvgL3, 3, gp),
               paSynth = synth.avg(paAvg, paAvgL3, 3, gp),
               raSynth = synth.avg(raAvg, raAvgL3, 3, gp),
               sbSynth = synth.avg(sbAvg, sbAvgL3, 3, gp)
               #team_abbreviation = pi[athlete_display_name]
        ) %>%
        # opp has to be looked up after the team abrv has been updated to players current team
        # this might be able to be put in the mutate above after the team_abbrv update but 
        # couldnt test while off line
        mutate(
            game_id = ifelse(
                team_abbreviation %in% matchups.today$home_team_abb, 
                matchups.today[matchups.today$home_team_abb == team_abbreviation, "game_id"][[1]],
                matchups.today[matchups.today$away_team_abb == team_abbreviation, "game_id"][[1]]
            ),
            opp = ifelse(team_abbreviation %in% matchups.today$home_team_abb,
                         (matchups.today %>% filter(home_team_abb == team_abbreviation) %>% select(away_team_abb))[[1]],
                         (matchups.today %>% filter(away_team_abb == team_abbreviation) %>% select(home_team_abb))[[1]]
            ),
            btbOpp = case_when((opp %in% back.to.back.first) ~ 1,
                               (opp %in% back.to.back.last) ~ 2,
                               TRUE ~ 0
            ),
            btb = case_when((team_abbreviation %in% back.to.back.first) ~ 1,
                            (team_abbreviation %in% back.to.back.last) ~ 2,
                            TRUE ~ 0
            )
        )
    
    return(df)    
}
########

### retrieving the player boxscore and schedule for the season, 
# this will be used to access players that are playing today and agg stats
boxscore.player <- wehoop::load_wnba_player_box(s) 

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

player.info <- wehoop::wnba_commonallplayers(season="2022-23")$CommonAllPlayers %>%
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
stat.harvest <- pfarming(boxscore.player,
                         #propfarming(boxscore.player, 
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


##################
# scrape odds data and join to player harvest data
##################

conn <- harvestDBconnect(league='wnba')
dbSendQuery(conn, "SET GLOBAL local_infile = true;")

odds.date <- format(Sys.Date(), "%Y-%m-%d")
query <- "SELECT 
            p.player playerName, p.wehoopId, o.playerId actnetPlayerId, p.joinName, o.date, o.prop, o.line, o.oOdds, o.uOdds
          FROM odds o
          INNER JOIN players p ON o.playerId = p.actnetPlayerId
          WHERE o.date = '"

# flatten a single players odds into a single row
betting.table <- dbGetQuery(conn, paste0(query, odds.date, "'")) %>%
    pivot_wider(names_from = prop,
                values_from = c(line, oOdds, uOdds))

# close conns
dbSendQuery(conn, "SET GLOBAL local_infile = false;")
dbDisconnect(conn)




# store the players from the harvest data that did not have any betting info
missing.players.odds <- setdiff(stat.harvest$join.names, betting.table$joinName)
missing.players.odds
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
        # stlOscore = round((stlSynth - stlStdL3) - line_STL,3),
        # stlUscore = round(line_STL - (stlSynth + stlStdL3),3),
        # blkOscore = round((blkSynth - blkStdL3) - line_BLK,3),
        # blkUscore = round(line_BLK - (blkSynth + blkStdL3),3),
        fg3mOscore = round((fg3mSynth - fg3mStdL3) - line_threes,3),
        fg3mUscore = round(line_threes - (fg3mSynth + fg3mStdL3),3),
        # praOscore = round((praSynth - praStdL3) - line_PTSREBAST,3),
        # praUscore = round(line_PTSREBAST - (praSynth + praStdL3),3),
        # prOscore = round((prSynth - prStdL3) - line_PTSREB,3),
        # prUscore = round(line_PTSREB - (prSynth + prStdL3),3),
        # paOscore = round((paSynth - paStdL3) - line_PTSAST,3),
        # paUscore = round(line_PTSAST - (paSynth + paStdL3),3),
        # raOscore = round((raSynth - raStdL3) - line_REBAST,3),
        # raUscore = round(line_REBAST - (raSynth + raStdL3),3),
        # sbOscore = round((sbSynth - sbStdL3) - line_STLBLK,3),
        # sbUscore = round(line_STLBLK - (sbSynth + sbStdL3),3),
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

#### need to see if the espn calls have betting info
#game.lines.today <- games.betting.info(gids.today)
#harvest <- harvest %>% left_join(game.lines.today, by="game_id")

crop <- harvest %>%
    select(player, team, opp, 
           ptsOscore, ptsUscore, rebOscore, rebUscore, astOscore, astUscore, fg3mOscore, fg3mUscore,
           ptsAvg, ptsSynth, line_pts, 
           rebAvg, rebSynth, line_reb, 
           astAvg, astSynth, line_ast, 
           fg3mAvg, fg3mSynth, line_threes) 

crop %>% View()


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






