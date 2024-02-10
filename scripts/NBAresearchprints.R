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


league <- 'nba'
season  <-  "2023-24"
s <-  2024
lookback <- 0 ######################################################## negative numbers are for historical games # BOXSCORE 
last.n.lookback.days <- 10 ########################################## last n games to look back in the stat agg rankings.
search.date <- Sys.Date() - lookback
today.date.char <- format(search.date, "%Y%m%d")

# getting the teams playing on the search date
schedule <- nba_schedule(season = season) %>% 
                mutate(
                    home_team_tricode  = case_when(
                        home_team_tricode == "NOP" ~ "NO",
                        home_team_tricode == "NYK" ~ "NY",
                        home_team_tricode == "SAS" ~ "SA",
                        home_team_tricode == "UTA" ~ "UTAH",
                        home_team_tricode == "WAS" ~ "WSH",
                        home_team_tricode == "GSW" ~ "GS",
                        TRUE ~ home_team_tricode
                    ),
                    away_team_tricode  = case_when(
                        away_team_tricode == "NOP" ~ "NO",
                        away_team_tricode == "NYK" ~ "NY",
                        away_team_tricode == "SAS" ~ "SA",
                        away_team_tricode == "UTA" ~ "UTAH",
                        away_team_tricode == "WAS" ~ "WSH",
                        away_team_tricode == "GSW" ~ "GS",
                        TRUE ~ away_team_tricode
                    )
                )
#getting allstar dates to filter out
dates.allstar <- schedule %>%
    filter(season_type_description == 'All-Star') %>% 
    select(game_date)

# boxscore  will be used to access players that are playing today and agg stats
boxscore.player <- load_nba_player_box(s) %>% 
                        filter(game_date < search.date ) %>%
                        # FILTER OUT ASG
                        filter(!game_date %in%  dates.allstar) %>%
                        left_join(bref.pos.estimates %>% select(hooprId, pos) %>% rename(athlete_id = hooprId), by = c('athlete_id')
                        ) %>% 
                        mutate(athlete_position_abbreviation = case_when(is.na(pos) ~ athlete_position_abbreviation,
                                                                         TRUE ~ pos)
                        ) %>%
                        # change remaining generic positions 
                        mutate(
                            athlete_position_abbreviation = case_when(
                                athlete_position_abbreviation == "G" ~ "SG",
                                athlete_position_abbreviation == "F" ~ "SF",
                                TRUE ~ athlete_position_abbreviation
                            )
                        )


schedule.today <- schedule %>% filter(game_date == search.date)

###### gathering team stats and rankings ######

# when teams have less than 15 games played, look back to the min played so rank inputs are equal - only important early season
min.gp <- hoopR::nba_leaguestandings()$Standings %>%
    select(WINS, LOSSES) %>% 
    mutate(gp = as.numeric(WINS) + as.numeric(LOSSES)) %>%
    select(gp) %>% 
    min()
lookback.days <- min(min.gp, last.n.lookback.days)

schedule <- load_nba_schedule(season = s) %>% filter(date < search.date)

##### OFFENSE STATS/RANKS ######
# using data cracker to calc off boxscores offensive ranks.
# 1 = best, 30 = worst
offense <- stats.last.n.games.offense(s, num.game.lookback=lookback.days, 
                                      box.scores=boxscore.player, schedule=schedule,
                                      no.date=FALSE)
team.ranks.offense <- offense$team.stats %>% 
                            select(team, contains("RANK"), -fgmRank, -fg2mRank, -ftmRank, -ftaRank) %>% 
                        pivot_longer(
                                cols = c(ptsRank, rebRank, astRank, fg3mRank,fg3aRank, fg3PctRank, stlRank, blkRank, 
                                         orebRank, drebRank, fgaRank, fgPctRank, fg2aRank, fg2PctRank, #ftmRank, ftaRank,  
                                         toRank), 
                                names_to = "stat", 
                                values_to = "rank"
                            )
#offense_ranks 30 = worst -- THIS IS THE LIBRARY FUNCTION TO CALC IT. I DIDN"T KNOW ABOUT THE LAST N game VARIABLE AT THE TIME
#offense_ranks <- hoopR::nba_leaguedashteamstats(last_n_games = 5)$LeagueDashTeamStats %>% 
#                    select(TEAM_ID, TEAM_NAME, contains("RANK"))
#View(offense$team.stats)
#####

##### DEFENSE STATS/RANKS ######
# 1 = best, 30 = worst
defense <- stats.last.n.games.opp(s, num.game.lookback=lookback.days, 
                                  box.scores=boxscore.player, schedule=schedule,
                                  no.date=FALSE)
# pivot the team total stats longer
team.ranks.defense <- defense$team.opp.stats %>% 
                            select(team, contains("RANK"), -fgmRank, -fg2mRank, -ftmRank, -ftaRank) %>% 
                            pivot_longer(
                                cols = c(ptsRank, rebRank, astRank, fg3mRank,fg3aRank, fg3PctRank, stlRank, blkRank, 
                                         orebRank, drebRank, fgaRank, fgPctRank, fg2aRank, fg2PctRank, #ftmRank, ftaRank,  
                                         toRank), 
                                names_to = "stat", 
                                values_to = "rank"
                            )

# elongate the opp rank stats to bind it to the comparison
opp.ranks <- defense$team.opp.stats.by.pos %>% 
    select(team, athlete_position_abbreviation, contains("RANK"), -fgmRank, -fg2mRank, -ftmRank, -ftaRank) %>% 
    pivot_longer(
        cols = c(ptsRank, rebRank, astRank, fg3mRank,fg3aRank, fg3PctRank, stlRank, blkRank, 
                 orebRank, drebRank, fgaRank, fgPctRank, fg2aRank, fg2PctRank, #ftmRank, ftaRank,  
                 toRank), 
        names_to = "stat", 
        values_to = "rank"
    ) %>% pivot_wider(names_from = athlete_position_abbreviation,
                      values_from = c(rank))

##### COMBINE TEAMS PLAYING VS ON THE SEARCH DATE #####
for (row in 1:nrow(schedule.today)){
    #
    home.team <- schedule.today[row, 'home_team_tricode'][['home_team_tricode']]
    away.team <- schedule.today[row, 'away_team_tricode'][['away_team_tricode']]
    gid <- schedule.today[row, 'game_id']
    
    homeO <- team.ranks.offense %>% 
        filter(team == home.team) %>% 
        rename(home = team,
               offense = rank)
    homeD <- team.ranks.defense %>% 
        filter(team == home.team) %>% 
        rename(home = team,
               defense = rank)
    awayO <- team.ranks.offense %>% 
        filter(team == away.team) %>% 
        rename(away = team,
               offense = rank) %>% 
        select(-stat)
    awayD <- team.ranks.defense %>% 
        filter(team == away.team) %>% 
        rename(away = team,
               defense = rank) %>% 
        select(-stat)
    
    # column binding the home and away teams 
    if (row == 1){
        homeOff <- cbind(homeO, awayD) 
        homeDef <- cbind(homeD, awayO) 
    }
    else{
        homeOffAwayDef.temp <- cbind(homeO, awayD) 
        homeDefAwayOff.temp <- cbind(homeD, awayO)
        
        homeOff <- rbind(homeOff, homeOffAwayDef.temp)
        homeDef <- rbind(homeDef, homeDefAwayOff.temp)
    }
}

homeOff<- homeOff %>% select(home, offense, stat, defense, away) %>% 
                    mutate(homeOffMatchup = case_when(
                            offense < 10 & defense > 20 ~ 2,
                            offense < 15 & defense > 15 ~ 1,
                            offense < 18 & defense > 12 ~ 0,
                            offense > 15 & defense < 15 ~ -1,
                            offense > 20 & defense < 10 ~ -2,
                            TRUE ~ 3
                        )
                    ) %>% 
                    left_join(opp.ranks, by=c("away" = "team", "stat" = "stat"))

homeDef <- homeDef %>% select(home, defense, stat, offense, away) %>% 
                    mutate(awayOffMatchup = case_when(
                        offense < 10 & defense > 20 ~ 2,
                        offense < 15 & defense > 15 ~ 1,
                        offense < 18 & defense > 12 ~ 0,
                        offense > 15 & defense < 15 ~ -1,
                        offense > 20 & defense < 10 ~ -2,
                        TRUE ~ 3
                        )
                    ) %>% 
                    left_join(opp.ranks, by=c("home" = "team", "stat" = "stat"))


# 1 = best, 30 = worst - 1 offense + 30 defense = best matchup, 30 off + 1 def = worst
#homeOffAdvantage <- homeOff %>% filter(homeOffMatchup > 0 & homeOffMatchup!= 3)
#awayOffAdvantage <- homeDef %>% filter(awayOffMatchup > 0 & awayOffMatchup!= 3)
#homeDefAdvantage <- homeDef %>% filter(awayOffMatchup < 0)
#awayDefAdvantage <- homeOff %>% filter(homeOffMatchup < 0)
#View(homeOffAdvantage)
#View(awayOffAdvantage)
#View(homeDefAdvantage)
#View(awayDefAdvantage)
View(homeOff)
View(homeDef)
d.pos.rank <- defense$team.opp.stats.by.pos %>% select(team, athlete_position_abbreviation, 
                                                      ptsRank, rebRank, astRank, fg3mRank,fg3aRank, fg3PctRank, stlRank, blkRank, 
                                                      orebRank, drebRank, fgaRank, fgPctRank, fg2aRank, fg2PctRank, #ftmRank, ftaRank,  
                                                       toRank)

d.team.rank <- defense$team.opp.stats.by.all.pos %>% select(team, ptsRank, rebRank, astRank, fg3mRank,fg3aRank, fg3PctRank,stlRank, blkRank, 
                                                           orebRank, drebRank, fgaRank, fgPctRank, fg2aRank, fg2PctRank, #ftmRank, ftaRank,  
                                                           toRank)


o.pos.rank <- offense$team.stats.by.pos %>% select(team, athlete_position_abbreviation, 
                                                          ptsRank, rebRank, astRank, fg3mRank,fg3aRank, fg3PctRank, stlRank, blkRank, 
                                                          orebRank, drebRank, fgaRank, fgPctRank, fg2aRank, fg2PctRank, #ftmRank, ftaRank,  
                                                          toRank)

o.team.rank <- offense$team.stats %>% select(team, ptsRank, rebRank, astRank, fg3mRank,fg3aRank, fg3PctRank,stlRank, blkRank, 
                                                           orebRank, drebRank, fgaRank, fgPctRank, fg2aRank, fg2PctRank, #ftmRank, ftaRank,  
                                                           toRank)
View(d.pos.rank)
View(d.team.rank)
View(o.pos.rank)
View(o.team.rank)
#####

##################
# Team opp. stat distribution -Team stats allowed to opponent distribution across positions
##################
stat <- c('pts', 'ast', 'reb', 'fg3m')
colName <- paste(stat, "Count", sep="")

statDist <- defense$team.opp.stats.by.pos %>% 
    group_by(team) %>% 
    select(team, athlete_position_abbreviation, all_of(colName)) %>% 
    mutate(
        ptsPct = ptsCount / sum(ptsCount),
        rebPct = rebCount / sum(rebCount),
        astPct = astCount / sum(astCount),
        fg3mPct = fg3mCount / sum(fg3mCount),
    ) %>% 
    select(team, athlete_position_abbreviation, ptsCount, ptsPct, rebCount, rebPct, astCount, astPct, fg3mCount, fg3mPct)

View(statDist)
#####

##################
# Player Rebs and Ast data
##################
conn <- harvestDBconnect(league = league)
#dbSendQuery(conn, "SET GLOBAL local_infile = true;")

#SQL query to select Ln games and return sum of stats
###### 
# ln <- 3
# query.passing.filtered <- paste("SELECT PLAYER_NAME, pid, sum(AST), sum(POTENTIAL_AST) 
# FROM
# (SELECT * 
# 	FROM (
# 		SELECT *, ROW_NUMBER() OVER (PARTITION BY pid ORDER BY date DESC) AS n
# 		FROM statsplayerpassing
# 	) AS x
# WHERE n <=", ln ,") as y
# GROUP BY x.PLAYER_NAME;", sep="")
######
query.passing.all <- "SELECT 
                        PLAYER_NAME player, pid nbaId, tid, date, CAST(MIN AS UNSIGNED) min,
                        CAST(PASSES_MADE AS UNSIGNED) passes_made, CAST(PASSES_RECEIVED AS UNSIGNED) passes_rec, 
                        CAST(AST AS UNSIGNED) ast, CAST(POTENTIAL_AST AS UNSIGNED) pot_ast
                    FROM statsplayerpassing WHERE DATE > '2023-10-23' ORDER BY pid, date DESC"
df.passing <- dbGetQuery(conn, query.passing.all) 

query.rebounding.all <- "SELECT 
                          PLAYER_NAME player, pid nbaId, tid, date, CAST(MIN AS UNSIGNED) min,
                          CAST(REB AS UNSIGNED) reb, CAST(REB_CHANCES AS UNSIGNED) pot_reb, 
                          CAST(REB_CHANCE_DEFER AS UNSIGNED) defer_reb, CAST(AVG_REB_DIST AS UNSIGNED) reb_dist
                        FROM statsplayerrebounding WHERE DATE > '2023-10-23' ORDER BY pid, date DESC"
df.rebounding<- dbGetQuery(conn, query.rebounding.all)

#dbSendQuery(conn, "SET GLOBAL local_infile = false;")
dbDisconnect(conn)

passL3 <- df.passing %>% 
    arrange(nbaId, desc(date)) %>% 
    group_by(player, nbaId) %>%
    slice_head(n = 3) %>%
    summarise('astL3' = round(mean(ast),1),
              'potAstL3' = round(mean(pot_ast),1),
              'passMadeL3' = round(mean(passes_made),1),
              'passRecL3' = round(mean(passes_rec),1)
    ) 
passL10 <- df.passing %>% 
    arrange(nbaId, desc(date)) %>% 
    group_by(player, nbaId) %>%
    slice_head(n = 10) %>%
    summarise('astL10' = round(mean(ast),1),
              'potAstL10' = round(mean(pot_ast),1),
              'passMadeL10' = round(mean(passes_made),1),
              'passRecL10' = round(mean(passes_rec),1)
    ) 
passAll <- df.passing %>% 
    arrange(nbaId, desc(date)) %>% 
    group_by(player, nbaId) %>%
    summarise('ast' = round(mean(ast),1),
              'potAst' = round(mean(pot_ast),1),
              'passMade' = round(mean(passes_made),1),
              'passRec' = round(mean(passes_rec),1)) 

df.passing <- merge(passL3, passL10, by=c('player', 'nbaId'))
df.passing <- merge(df.passing, passAll, by=c('player', 'nbaId')) %>% 
    select(player, nbaId, astL3, astL10, ast, potAstL3, potAstL10, potAst, passMadeL3, passMadeL10, passMade, passRecL3, passRecL10, passRec)

remove(passL3, passL10, passAll)

rebL3 <- df.rebounding %>% 
    arrange(nbaId, desc(date)) %>% 
    group_by(player, nbaId) %>%
    slice_head(n = 3) %>%
    summarise('rebL3' = round(mean(reb),1),
              'potRebL3' = round(mean(pot_reb),1),
              'deferRebL3' = round(mean(defer_reb),1)
    ) 
rebL10 <- df.rebounding %>% 
    arrange(nbaId, desc(date)) %>% 
    group_by(player, nbaId) %>%
    slice_head(n = 10) %>%
    summarise('rebL10' = round(mean(reb),1),
              'potRebL10' = round(mean(pot_reb),1),
              'deferRebL10' = round(mean(defer_reb),1)
    ) 
rebAll <- df.rebounding %>% 
    arrange(nbaId, desc(date)) %>% 
    group_by(player, nbaId) %>%
    summarise('reb' = round(mean(reb),1),
              'potReb' = round(mean(pot_reb),1),
              'deferReb' = round(mean(defer_reb),1)
    ) 
df.rebounding <- merge(rebL3, rebL10, by=c('player', 'nbaId'))
df.rebounding <- merge(df.rebounding, rebAll, by=c('player', 'nbaId')) %>% 
    select(player, nbaId, rebL3, rebL10, reb, potRebL3, potRebL10, potReb, deferRebL3, deferRebL10, deferReb)

remove(rebL3, rebL10, rebAll)

View(df.passing)
View(df.rebounding)
#####