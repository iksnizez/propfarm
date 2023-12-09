library(dplyr)
library(DBI)
library(RMySQL)
library(rmarkdown)
source("scripts/functions/NBAdatacrackers.R")
source("scripts/functions/dbhelpers.R")



dbPath <-  '../../../../Notes-General/config.txt'
nGamesLookback <-  10
game.date <- Sys.Date()
league <- 'nba'
season  <-  "2023-24"
s <-  2024


today.date.char <- format(game.date, "%Y%m%d")
games.today <- espn_nba_scoreboard (season = today.date.char)
matchups.today <- games.today %>% select(home_team_abb, away_team_abb) %>% as.data.frame()

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

# pulling the most recent pos. estimates from database
conn <- harvestDBconnect(league = league)
dbSendQuery(conn, "SET GLOBAL local_infile = true;")

query.pos.ests <- "SELECT * FROM brefmisc WHERE date = (SELECT MAX(date) FROM brefmisc)"
bref.pos.estimates <-  dbGetQuery(conn, query.pos.ests)

dbSendQuery(conn, "SET GLOBAL local_infile = false;")
dbDisconnect(conn)


# boxscore  will be used to access players that are playing today and agg stats
boxscore.player <- load_nba_player_box(s) %>% 
    filter(game_date < game.date ) %>%
    # FILTER OUT ASG
    filter(!game_date %in%  dates.allstar) %>%
    left_join(bref.pos.estimates %>% 
                  select(hooprId, pos) %>% 
                  rename(athlete_id = hooprId), 
              by = c('athlete_id')
    ) %>% 
    mutate(athlete_position_abbreviation = case_when(
        is.na(pos) ~ athlete_position_abbreviation,
        TRUE ~ pos
    )
    ) %>%
    # change remaining generic positions 
    mutate(
        athlete_position_abbreviation = case_when(
            athlete_position_abbreviation == "G" ~ "SG",
            athlete_position_abbreviation == "F" ~ "SF",
            TRUE ~ athlete_position_abbreviation
        )
    )


schedule.today <- schedule %>% filter(game_date == game.date)

###### gathering team stats and rankings ######

# when teams have less than 15 games played, look back to the min played so rank inputs are equal - only important early season
min.gp <- hoopR::nba_leaguestandings()$Standings %>%
    select(WINS, LOSSES) %>% 
    mutate(gp = as.numeric(WINS) + as.numeric(LOSSES)) %>%
    select(gp) %>% 
    min()
lookback.days <- min(min.gp, nGamesLookback)

schedule <- load_nba_schedule(season = s) 

##### OFFENSE STATS/RANKS ######
# using data cracker to calc off boxscores offensive ranks.
# 1 = best, 30 = worst
offense <- stats.last.n.games.offense(s, num.game.lookback=nGamesLookback, 
                                      box.scores=boxscore.player, schedule=schedule, 
                                      type=FALSE, report.out=TRUE)

team.ranks.offense <- offense$team.stats %>% 
    select(team, contains("RANK"), -fgmRank, -fg2mRank, -ftmRank, -ftaRank) %>% 
    pivot_longer(
        cols = c(ptsRank, rebRank, astRank, fg3mRank,fg3aRank, fg3PctRank, 
                 stlRank, blkRank, orebRank, drebRank, fgaRank, fgPctRank, 
                 fg2aRank, fg2PctRank, #ftmRank, ftaRank,  
                 toRank), 
        names_to = "stat", 
        values_to = "rank"
    )


##### DEFENSE STATS/RANKS ######
# 1 = best, 30 = worst
defense <- stats.last.n.games.opp(s, num.game.lookback=nGamesLookback, 
                                  box.scores=boxscore.player, schedule=schedule, 
                                  type=FALSE, report.out=TRUE)

# pivot the team total stats longer
team.ranks.defense <- defense$team.opp.stats %>% 
    select(team, contains("RANK"), -fgmRank, -fg2mRank, -ftmRank, -ftaRank) %>% 
    pivot_longer(
        cols = c(ptsRank, rebRank, astRank, fg3mRank,fg3aRank, fg3PctRank, 
                 stlRank, blkRank, orebRank, drebRank, fgaRank, fgPctRank, 
                 fg2aRank, fg2PctRank, #ftmRank, ftaRank, 
                 toRank), 
        names_to = "stat", 
        values_to = "rank"
    )


# loop through matchups to render the pdfs
for (i in 1:nrow(matchups.today)){
    game <- matchups.today[i,]

    homeTeam <- game$home_team_abb
    awayTeam <- game$away_team_abb
    outputFileNames <- paste(game.date, homeTeam, awayTeam, sep="")
    
    # filtering data to the two teams in the current game
    # elongate the opp rank stats to bind it to the comparison
    opp.ranks <- defense$team.opp.stats.by.pos %>% 
        select(team, athlete_position_abbreviation, contains("RANK"), -fgmRank, -fg2mRank, -ftmRank, -ftaRank) %>% 
        filter(team == homeTeam | team == awayTeam) %>% 
        pivot_longer(
            cols = c(ptsRank, rebRank, astRank, fg3mRank,fg3aRank, fg3PctRank, stlRank, blkRank, 
                     orebRank, drebRank, fgaRank, fgPctRank, fg2aRank, fg2PctRank, #ftmRank, ftaRank,  
                     toRank), 
            names_to = "stat", 
            values_to = "rank"
        ) %>% pivot_wider(names_from = athlete_position_abbreviation,
                          values_from = c(rank))
    
    homeO <- team.ranks.offense %>% 
        filter(team == homeTeam) %>% 
        rename(home = team,
               offense = rank)
    homeD <- team.ranks.defense %>% 
        filter(team == homeTeam) %>% 
        rename(home = team,
               defense = rank)
    
    awayO <- team.ranks.offense %>% 
        filter(team == awayTeam) %>% 
        rename(away = team,
               offense = rank) %>% 
        select(-stat)
    awayD <- team.ranks.defense %>% 
        filter(team == awayTeam) %>% 
        rename(away = team,
               defense = rank) %>% 
        select(-stat)
    
    
    
    homeOffAwayDef <- cbind(homeO, awayD)
    homeOffAwayDef<- homeOffAwayDef %>% select(home, offense, stat, defense, away) #%>%
    # this would add the positional ranks for the defense
    #left_join(opp.ranks, by=c("away" = "team", "stat" = "stat"))
    
    homeDefAwayOff <- cbind(homeD, awayO)
    homeDefAwayOff <- homeDefAwayOff %>% select(home, defense, stat, offense, away) #%>%
    # this would add the positional ranks for the defense 
    #left_join(opp.ranks, by=c("home" = "team", "stat" = "stat"))
    
    
    off <- homeOffAwayDef %>% select(home, offense, stat) %>% 
        rename(rank = offense, team=home) %>% 
        filter(team == homeTeam)
    
    def <- homeOffAwayDef %>% select(away, defense, stat) %>% 
        rename(rank = defense, team=away) %>% 
        filter(team == awayTeam)
    
    pyradata <- rbind(off,def)
    
    # Set factor order
    pyradata <- pyradata %>% mutate(stat = factor(stat, levels= rev(c("ptsRank", "rebRank", "astRank", "fg3mRank","fg3aRank", "fg3PctRank", "stlRank", "blkRank", 
                                                                      "orebRank", "drebRank", "fgaRank", "fgPctRank", "fg2aRank", "fg2PctRank", #ftmRank, ftaRank,  
                                                                      "toRank"))))

    rmarkdown::render(
        input = "scripts\\reporting\\NBAoppRanksReport.Rmd",
        output_format = "html_document",
        output_file = outputFileNames,
        output_dir = "output",
        params = list(
            homeTeam = homeTeam,
            awayTeam = awayTeam,
            pyraData = pyradata,
            homeColorOne = 1,
            homeColorTwo = 2,
            awayColorOne = 1,
            awayColorTwo = 2,
            show_code = FALSE
        )
    )
}
