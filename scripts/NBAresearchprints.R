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
lookback <- 0 ######################################################## negative numbers are for historical games
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
                        filter(game_date <= search.date ) %>%
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
lookback.days <- min(min.gp,15)

schedule <- load_nba_schedule(season = s) 

##### OFFENSE STATS/RANKS ######
# using data cracker to calc off boxscores offensive ranks.
# 1 = best, 30 = worst
offense <- stats.last.n.games.offense(s, num.game.lookback=lookback.days, box.scores=boxscore.player, schedule=schedule, type=FALSE)
team.ranks.offense <- offense$team.stats %>% 
                            select(team, contains("RANK"), -fgmRank, -fg2mRank) %>% 
                        pivot_longer(
                                cols = c(ptsRank, fgaRank, fgPctRank, fg2aRank, fg2PctRank, fg3mRank, fg3aRank, fg3PctRank, ftmRank, ftaRank, orebRank, drebRank, 
                                         rebRank, astRank, stlRank, blkRank, toRank), 
                                names_to = "stat", 
                                values_to = "rank"
                            )
#offense_ranks 30 = worst -- THIS IS THE LIBRARY FUNCTION TO CALC IT. I DIDN"T KNOW ABOUT THE LAST N game VARIABLE AT THE TIME
#offense_ranks <- hoopR::nba_leaguedashteamstats(last_n_games = 5)$LeagueDashTeamStats %>% 
#                    select(TEAM_ID, TEAM_NAME, contains("RANK"))
#View(offense$team.stats)

##### DEFENSE STATS/RANKS ######
# 1 = best, 30 = worst
defense <- stats.last.n.games.opp(s, num.game.lookback=lookback.days, box.scores=boxscore.player, schedule=schedule, type=FALSE)
# pivot the team total stats longer
team.ranks.defense <- defense$team.opp.stats %>% 
                            select(team, contains("RANK"), -fgmRank, -fg2mRank) %>% 
                            pivot_longer(
                                cols = c(ptsRank, fgaRank, fgPctRank, fg2aRank, fg2PctRank, fg3mRank, fg3aRank, fg3PctRank, ftmRank, ftaRank, orebRank, drebRank, 
                                         rebRank, astRank, stlRank, blkRank, toRank), 
                                names_to = "stat", 
                                values_to = "rank"
                            )

# elongate the opp rank stats to bind it to the comparison
opp.ranks <- defense$team.opp.stats.by.pos %>% 
    select(team, athlete_position_abbreviation, contains("RANK"), -fgmRank, -fg2mRank) %>% 
    pivot_longer(
        cols = c(ptsRank, fgaRank, fgPctRank, fg2aRank, fg2PctRank, fg3mRank, fg3aRank, fg3PctRank, ftmRank, ftaRank, orebRank, drebRank, 
                 rebRank, astRank, stlRank, blkRank, toRank), 
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
        homeOffAwayDef <- cbind(homeO, awayD) 
        homeDefAwayOff <- cbind(homeD, awayO) 
    }
    else{
        homeOffAwayDef.temp <- cbind(homeO, awayD) 
        homeDefAwayOff.temp <- cbind(homeD, awayO)
        
        homeOffAwayDef <- rbind(homeOffAwayDef, homeOffAwayDef.temp)
        homeDefAwayOff <- rbind(homeDefAwayOff, homeDefAwayOff.temp)
    }
}

homeOffAwayDef<- homeOffAwayDef %>% select(home, offense, stat, defense, away) %>% 
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

homeDefAwayOff <- homeDefAwayOff %>% select(home, defense, stat, offense, away) %>% 
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
homeOffAdvantage <- homeOffAwayDef %>% filter(homeOffMatchup > 0 & homeOffMatchup!= 3)
awayOffAdvantage <- homeDefAwayOff %>% filter(awayOffMatchup > 0 & awayOffMatchup!= 3)
homeDefAdvantage <- homeDefAwayOff %>% filter(awayOffMatchup < 0)
awayDefAdvantage <- homeOffAwayDef %>% filter(homeOffMatchup < 0)
View(homeOffAdvantage)
View(awayOffAdvantage)
View(homeDefAdvantage)
View(awayDefAdvantage)
View(homeOffAwayDef)
View(homeDefAwayOff)
pos.ranks <- defense$team.opp.stats.by.pos %>% select(team, athlete_position_abbreviation, 
                                                      ptsRank, rebRank, astRank, fg3mRank,fg3aRank, fg3PctRank,stlRank, blkRank, 
                                                      orebRank, drebRank, fgaRank, fgPctRank, fg2aRank, fg2PctRank, ftmRank, ftaRank,  
                                                       toRank)
View(pos.ranks)
team.ranks <- defense$team.opp.stats.by.all.pos %>% select(team, ptsRank, rebRank, astRank, fg3mRank,fg3aRank, fg3PctRank,stlRank, blkRank, 
                                                           orebRank, drebRank, fgaRank, fgPctRank, fg2aRank, fg2PctRank, ftmRank, ftaRank,  
                                                           toRank)
View(team.ranks)
