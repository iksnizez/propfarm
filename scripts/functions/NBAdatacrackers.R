library(hoopR)
library(dplyr)
library(tidyr)

####################
## FUNCTION TO UPDATE PROVIDED TEAM DATA
####################
update.default.team.data <- function(){
    # data to join with default data frame
    team_id <- c("1610612741", "1610612744", "1610612766", "1610612762", "1610612750", "1610612749", "1610612752","1610612743", "1610612764", "1610612763", "1610612737", "1610612757", "1610612742", "1610612755", "1610612748", "1610612756","1610612751", "1610612747", "1610612745", "1610612738", "1610612758", "1610612761", "1610612759", "1610612739","1610612753", "1610612746", "1610612754", "1610612740", "1610612760", "1610612765")
    teamId_hoopr <- c(4,9,30,26,16,15,18,7,27,29,1,22,6,20,14,21,17,13,10,2,23,28,24,5,19,12,11,3,25,8)
    team_abb <- c("CHI","GS","CHA","UTAH","MIN","MIL","NY","DEN","WSH","MEM","ATL","POR","DAL","PHI","MIA","PHX","BKN","LAL","HOU","BOS","SAC","TOR","SA","CLE","ORL","LAC","IND","NO","OKC","DET")
    teamName_nba <- c("Chicago","Golden State","Charlotte","Utah","Minnesota","Milwaukee","New York","Denver","Washington","Memphis","Atlanta","Portland","Dallas","Philadelphia","Miami","Phoenix","Brooklyn","L.A. Lakers","Houston","Boston","Sacramento","Toronto","San Antonio","Cleveland","Orlando","LA Clippers","Indiana","New Orleans","Oklahoma City","Detroit")
    to_join <- cbind(teamId_hoopr, team_abb)
    to_join <- cbind(to_join, teamName_nba)
    to_join <- cbind(to_join, TeamID)
    
    # joining the 2 data frames
    updated <- merge(as.data.frame(hoopR::nba_teams()), as.data.frame(to_join), by="team_id")
    
 
    ### creating lookup vectors to use with lookup function - 1 for NBA team ID and 1 for hoopR team id
    #getLongTeamId <- lookup.team$TeamID
    #names(getLongTeamId) <- lookup.team$team_abb
    #getHoopRteamId <- lookup.team$teamId_hoopr
    #names(getHoopRteamId) <- lookup.team$team_abb
    ###lookup function to convert hoopR team_abb to teamId to use in stat functions
    #get_value <- function(mykey, mylookupvector){
    #    myvalue <- mylookupvector[mykey]
    #    myvalue <- unname(myvalue)
    #    return(myvalue[1])
    #}
    return(updated)
}
#####

####################
## FUNCTIONS FOR USE WITH PBP DATA
####################
###
tre.ball <- function(x, y, shot) {
    # ingest the x, y coords of a shot and 
    # determine if it is a 3-point or 2-point attempt
    ### any shot in these 2 ranges will always be a 3
    ### side 3 attempts, the y-coord doesn't matter
    if((shot == FALSE) | ((x == -214748340) & (y== -214748365))){
        return(FALSE)
    }
    else if((x <= 3) | ( x >= 47)){
        return(TRUE)
    } 
    else {
        three.arc.center.x <- 25
        three.arc.center.y <- 0
        radius <- 23.75
        # euclid. distance  sqrt((x2-x1)**2 + (y2-y1)**2)
        ### determining distance of the shot to the hoop
        ### this ignores side 3pt attempts
        distance.from.hoop <- sqrt(((three.arc.center.x - x) ** 2) + ((three.arc.center.y - y) ** 2))
        if((distance.from.hoop > 23.75)){
            return(TRUE)
        }
        
        return(FALSE)
    }  
}

paint.bucket <- function(x, y, shot){
    # ingest the x, y coords of a shot and 
    # determine if it was taken in the paint
    #### the data has the basket at y = 0 instead of it higher off the out of bounds
    #### paint height decreased to account for this
    if((shot == FALSE) | ((x == -214748340) & (y== -214748365))){
        return(FALSE)
    }
    else if(((x >= 17) & (x <= 33)) & (y <= 13.75)){
        return(TRUE)
    }
    else{
        return(FALSE)
    }
}

made.shot <- function(shot, pts){
    #ingest a boolean to determine a shot was taken, 
    #and a number to determine if pts were scored
    if((shot == FALSE) | (pts <= 0)){
        return(FALSE)
    }
    else if(pts > 0){
        return(TRUE)
    }
}

off.rebound <- function(text){
    # searches pbp text for offensive rebs
    if(
        (grepl("offensive team rebound", text, fixed = TRUE)) | 
        (grepl("offensive rebound", text, fixed = TRUE))
    ){return(TRUE)}
    else{return(FALSE)}
}

def.rebound <- function(text){
    # searches pbp text for deffensive rebs
    if(
        (grepl("defensive team rebound", text, fixed = TRUE)) | 
        (grepl("defensive rebound", text, fixed = TRUE))
    ){return(TRUE)}
    else{return(FALSE)}
}
#####

####################
## FUNCTION TO CALC SYNTHETIC AVG
####################
synth.avg <- function(season_avg, n_avg, n_games=3, games_played){
    # calculating the avg weights for the recent n games and season avg
    n.avg.wt <- (min(1-(n_games / games_played), 0.6))
    season.avg.wt <- (1 - n.avg.wt)
    # calculating the wt. synthetic avg
    synthetic.avg <- round((n_avg * n.avg.wt) + (season_avg * season.avg.wt), 2)
    
    return(synthetic.avg)
}
#####

####################
## FUNCTION TO CALCULATE PROPFARM PLAYER STATS FROM NBA BOX
####################
# filtering entire season box score to only the teams playing today
propfarming <- function(box.score.data, team.ids, matchups.today, minFilter=20, player_info=NULL){
  # ingest season boxscore data from load_nba_player_box and vector of team ids and dataframe of home/away teams
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
    curr.season.end.year <- most_recent_nba_season()
    curr.season.start.year <- curr.season.end.year - 1
    curr.season.end.year <- curr.season.end.year - 2000
    season.input <- paste(curr.season.start.year, "-", curr.season.end.year, sep = "")
    
    player.info <- hoopR::nba_commonallplayers(season=season.input, is_only_current_season = 1)$CommonAllPlayers %>%
      mutate(TEAM_ABBREVIATION = case_when(
        TEAM_ABBREVIATION == "NOP" ~ "NO",
        TEAM_ABBREVIATION == "NYK" ~ "NY",
        TEAM_ABBREVIATION == "SAS" ~ "SA",
        TEAM_ABBREVIATION == "UTA" ~ "UTAH",
        TEAM_ABBREVIATION == "WAS" ~ "WSH",
        TEAM_ABBREVIATION == "GSW" ~ "GS",
        TRUE ~ TEAM_ABBREVIATION
      )) %>%
      select(PERSON_ID, DISPLAY_FIRST_LAST, TEAM_ABBREVIATION) %>%
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
  # pi <- player.info$team_abbreviation
  # names(pi) <- player.info$athlete_display_name
  player.names <- players.today %>% distinct(athlete_id, athlete_display_name, team_abbreviation)
  
  pi <- player.names$team_abbreviation
  names(pi) <- player.names$athlete_display_name
  
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
           sbSynth = synth.avg(sbAvg, sbAvgL3, 3, gp),
           team_abbreviation = pi[athlete_display_name]
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

####################
# SPLIT AND SUM BOX SCORES
####################
split.sum.box.scores <- function(box.scores, gids, opp=TRUE, t= NULL, min = 0, type = TRUE){
    # this updates the nba box score columns and filters them for specific games by gid
    # it's purpose is to be used to look at the opponents of a single team in the gids
    # specifically when calculating a team surrendered stats.
    # TYPE is to include game_date or not
  
    # filter boxscores depening on opp input. 
    # if True, then calculating stats allowed. If false, calc. stats accumulated
    if(opp){
      split.summed <- box.scores %>%
        filter(game_id %in% gids,
               team_abbreviation != t,
               minutes > 0,
               did_not_play == FALSE) 
    } 
    else{
      split.summed <- box.scores %>%
        filter(game_id %in% gids,
               team_abbreviation == t,
               minutes > 0,
               did_not_play == FALSE) 
    }

    split.summed <- split.summed %>% 
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
        ))
    
    # type = TRUE filtering, grouping for propfarm, if false = filtering for research prints
    if (type){
      split.summed <- split.summed %>% 
        group_by(game_id, team_abbreviation, athlete_position_abbreviation)
    } else{
      split.summed <- split.summed %>% 
        group_by(game_id, game_date, team_abbreviation, athlete_position_abbreviation) 
    }
        
    
    split.summed <- split.summed  %>%
        summarize(fgmCount = sum(as.numeric(fgm)),
                  fgaCount = sum(as.numeric(fga)),
                  fg2mCount = sum(as.numeric(fgm)) - sum(as.numeric(fg3m)),
                  fg2aCount = sum(as.numeric(fga))- sum(as.numeric(fg3a)),
                  fg3mCount = sum(as.numeric(fg3m)),
                  fg3aCount = sum(as.numeric(fg3a)),
                  ftmCount = sum(as.numeric(ftm)),
                  ftaCount = sum(as.numeric(fta)),
                  orebCount = sum(as.numeric(oreb)),
                  drebCount = sum(as.numeric(dreb)),
                  rebCount = sum(as.numeric(reb)),
                  astCount = sum(as.numeric(ast)),
                  stlCount = sum(as.numeric(stl)),
                  blkCount = sum(as.numeric(blk)),
                  toCount = sum(as.numeric(to)),
                  pfCount = sum(as.numeric(pf)),
                  ptsCount = sum(as.numeric(pts)),
        ) %>% 
      mutate(team= t)
      

    return(split.summed)
}

#####

####################
## FUNCTION TO RETRIEVE TEAM OPPONENT RANKS FROM LAST N games
####################
stats.last.n.games.opp  <- function(season, num.game.lookback=15, box.scores=NULL, schedule=NULL, no.date=TRUE){
    # ingest season year, number of games (15 default), data frame of player boxscores,
    # and data frame of the season schedule
    # output dataframe for teams stats allowed to opponents, dataframe for the stats grouped by
    # team and position, and a data frame for each position grouped by team and ranked
    
    # checks if box scores dataframe is provided and pulls box scores data if not
    if(is.null(box.scores)){
        box.scores <- hoopR::load_nba_player_box(seasons = season)  %>%
          mutate(TEAM_ABBREVIATION = case_when(
            TEAM_ABBREVIATION == "GSW" ~ "GS",
            TRUE ~ TEAM_ABBREVIATION
          ))
    }
    
    # checks if schedule dataframe is provided and pulls schedule data if not
    if(is.null(schedule)){
        schedule <- hoopR::load_nba_schedule(seasons = season)  %>%
          filter(date < Sys.Date()) %>% 
          mutate(TEAM_ABBREVIATION = case_when(
            TEAM_ABBREVIATION == "GSW" ~ "GS",
            TRUE ~ TEAM_ABBREVIATION
          ))
    }
    
    # schedule data for only games played
    completed.games <- schedule %>% filter(status_type_completed == TRUE)
    # gather team abbreviations
    teams <- append(unique(completed.games$home_abbreviation), 
                    unique(completed.games$away_abbreviation)) %>%
        unique()
    
    # looping through team abbreviations to get n# of gids for every team
    # originally, looped only to gather all teams gids but this caused some weird results
    # with some teams above and below the desired look back. changed to do each team indiviually 
    # so that the look back holds true for all teams. I ama not sure what caused the descrepancy
    for(t in teams){

        #starting the first dataframe that all other teams will add into
        gids <- (completed.games %>%
                     filter(home_abbreviation == t | away_abbreviation == t) %>%
                     arrange(desc(date)) %>%
                     slice(1:num.game.lookback) %>%
                     select(id))[["id"]]

        if(t == teams[1]){
            #retrieve gid boxscores and split att/made, aggregating stats by team, position
            grouped <- split.sum.box.scores(box.scores, gids, opp=TRUE, t=t, min=0, type=no.date)
        }
        else{
            #retrieve gid boxscores and split att/made, aggregating stats by team, position
            temp <- split.sum.box.scores(box.scores, gids, opp=TRUE, t=t, min=0, type=no.date)
            grouped <- rbind(grouped, temp)
        }
    }

    ### groups the agg stats by team for totals over n games *** GROUPS BY TEAM + POS
    
    # type = TRUE filtering, grouping for propfarm, if false = filtering for research prints
    stats.team.opp.total.pos <- grouped %>%
        group_by(
          team, 
          athlete_position_abbreviation
        )
    
    stats.team.opp.total.pos <- stats.team.opp.total.pos %>%
        summarize(
            ptsCount = sum(ptsCount),
            rebCount = sum(rebCount),
            astCount = sum(astCount),
            fg3mCount = sum(fg3mCount),
            fg3aCount = sum(fg3aCount),
            fg3Pct = sum(fg3mCount) / sum(fg3aCount),
            fg2Pct = (sum(fgmCount) - sum(fg3mCount)) / (sum(fgaCount) - sum(fg3aCount)),
            fgPct = sum(fgmCount) / sum(fgaCount),
            blkCount = sum(blkCount),
            stlCount = sum(stlCount),
            toCount = sum(toCount),
            orebCount = sum(orebCount),
            drebCount = sum(drebCount),
            fgmCount = sum(fgmCount),
            fgaCount = sum(fgaCount),
            fg2mCount = sum(fg2mCount),
            fg2aCount = sum(fg2aCount),
            ftmCount = sum(ftmCount),
            ftaCount = sum(ftaCount),
            pfCount = sum(pfCount)
        )
    
    # groups the agg stats by team for totals over n games *** GROUPS BY TEAM ONLY
    
    # type = TRUE filtering, grouping for propfarm, if false = filtering for research prints
    stats.team.opp.total <- grouped %>%
        group_by(
          team
        )
    
    stats.team.opp.total <- stats.team.opp.total %>%
      summarize(
        ptsCount = sum(ptsCount),
        rebCount = sum(rebCount),
        astCount = sum(astCount),
        fg3mCount = sum(fg3mCount),
        fg3aCount = sum(fg3aCount),
        fg3Pct = sum(fg3mCount) / sum(fg3aCount),
        fg2Pct = (sum(fgmCount) - sum(fg3mCount)) / (sum(fgaCount) - sum(fg3aCount)),
        fgPct = sum(fgmCount) / sum(fgaCount),
        blkCount = sum(blkCount),
        stlCount = sum(stlCount),
        toCount = sum(toCount),
        orebCount = sum(orebCount),
        drebCount = sum(drebCount),
        fgmCount = sum(fgmCount),
        fgaCount = sum(fgaCount),
        fg2mCount = sum(fg2mCount),
        fg2aCount = sum(fg2aCount),
        ftmCount = sum(ftmCount),
        ftaCount = sum(ftaCount),
        pfCount = sum(pfCount)
      ) 
    
      stats.team.opp.total <- stats.team.opp.total %>% 
                                mutate(
                                       ptsRank = rank(ptsCount),
                                       rebRank = rank(rebCount),
                                       astRank = rank(astCount),
                                       fg3mRank = rank(fg3mCount),
                                       fg3aRank = rank(fg3aCount),
                                       fg3PctRank = rank(fg3Pct),
                                       fg2PctRank = rank(fg2Pct),
                                       fgPctRank = rank(fgPct),
                                       blkRank = rank(blkCount),
                                       stlRank = rank(stlCount),
                                       toRank = rank(-toCount),
                                       orebRank = rank(orebCount),
                                       drebRank = rank(drebCount),
                                       fgmRank = rank(fgmCount),
                                       fgaRank = rank(fgaCount),
                                       fg2mRank = rank(fg2mCount),
                                       fg2aRank = rank(fg2aCount),
                                       ftmRank = rank(ftmCount),
                                       ftaRank = rank(ftaCount)
                                )
    
    # creates dataframes for each position to easily create ranks for each  position and stat
    positions <- c("PG", "SG", "SF", "PF", "C")
    for(pos in positions){
        var.name <- pos
               
         assign(var.name, stats.team.opp.total.pos %>% 
                  filter(athlete_position_abbreviation == pos) %>%
                  ungroup() %>% 
           mutate(
                  ptsRank = rank(ptsCount),
                  rebRank = rank(rebCount),
                  astRank = rank(astCount),
                  fg3mRank = rank(fg3mCount),
                  fg3aRank = rank(fg3aCount),
                  fg3PctRank = rank(fg3Pct),
                  fg2PctRank = rank(fg2Pct),
                  fgPctRank = rank(fgPct),
                  blkRank = rank(blkCount),
                  stlRank = rank(stlCount),
                  toRank = rank(-toCount),
                  fgmRank = rank(fgmCount),
                  fgaRank = rank(fgaCount),
                  fg2mRank = rank(fg2mCount),
                  fg2aRank = rank(fg2aCount),
                  ftmRank = rank(ftmCount),
                  ftaRank = rank(ftaCount),
                  orebRank = rank(orebCount),
                  drebRank = rank(drebCount),
           )
         )
    }
    # binding the position dfs with their ranks into a single df
    position.ranks <- rbind(PG, SG)
    position.ranks <- rbind(position.ranks, SF)
    position.ranks <- rbind(position.ranks, PF)
    position.ranks <- rbind(position.ranks, C)
    
    # game.opp.stats.by.pos is game level agg stats given up for each team
    # team.opp.stats.by.pos is team level agg stats given up for each team over n games for main 5 positions + RANKS for them
    # test is same as team.opp.stats.by.pos but includes generic G and F positions and NO ranks
    output <- list(grouped, position.ranks, stats.team.opp.total, stats.team.opp.total)
    names(output) <- c("game.opp.stats.by.pos", "team.opp.stats.by.pos", 
                       "team.opp.stats.by.all.pos", "team.opp.stats")
    
    return(output)
}

#####

####################
## FUNCTION TO RETRIEVE TEAM OFFENSIVE RANKS FROM LAST N games
####################
stats.last.n.games.offense  <- function(season, num.game.lookback=15, box.scores=NULL, schedule=NULL, no.date=TRUE){
  # ingest season year, number of games (15 default), data frame of player boxscores,
  # and data frame of the season schedule
  # output dataframe for teams stats , dataframe for the stats grouped by
  # team and position, and a data frame for each position grouped by team and ranked
  
  # checks if box scores dataframe is provided and pulls box scores data if not
  if(is.null(box.scores)){
    box.scores <- hoopR::load_nba_player_box(seasons = season)  %>%
      mutate(team_abbreviation = case_when(
        team_abbreviation == "GSW" ~ "GS",
        TRUE ~ team_abbreviation
      ))
  }
  
  # checks if schedule dataframe is provided and pulls schedule data if not
  if(is.null(schedule)){
    schedule <- hoopR::load_nba_schedule(seasons = season) %>% 
      filter(date < Sys.Date())%>% 
      mutate(TEAM_ABBREVIATION = case_when(
        TEAM_ABBREVIATION == "GSW" ~ "GS",
        TRUE ~ TEAM_ABBREVIATION
      ))
  }
  
  # schedule data for only games played
  completed.games <- schedule %>% filter(status_type_completed == TRUE)
  # gather team abbreviations
  teams <- append(unique(completed.games$home_abbreviation), 
                  unique(completed.games$away_abbreviation)) %>%
            unique()
  
  # looping through team abbreviations to get n# of gids for every team
  # originally, looped only to gather all teams gids but this caused some weird results
  # with some teams above and below the desired look back. changed to do each team indiviually 
  # so that the look back holds true for all teams. I ama not sure what caused the descrepancy
  for(t in teams){
    
    #starting the first dataframe that all other teams will add into
    gids <- (completed.games %>%
               filter(home_abbreviation == t | away_abbreviation == t) %>%
               arrange(desc(date)) %>%
               slice(1:num.game.lookback) %>%
               select(id))[["id"]]
    
    if(t == teams[1]){
      #retrieve gid boxscores and split att/made, aggregating stats by team, position
      grouped <- split.sum.box.scores(box.scores, gids, opp=FALSE, t=t, min=0, type=no.date)
    }
    else{
      #retrieve gid boxscores and split att/made, aggregating stats by team, position
      temp <- split.sum.box.scores(box.scores, gids,opp=FALSE, t=t, min=0, type=no.date)
      grouped <- rbind(grouped, temp)
    }
  }

  ### groups the agg stats by team for totals over n games *** GROUPS BY TEAM + POSITION
  
  # type = TRUE filtering, grouping for propfarm, if false = filtering for research prints
  stats.team.total.pos <- grouped %>%
      group_by(
        team, 
        athlete_position_abbreviation
      )

  stats.team.total.pos <- stats.team.total.pos %>%
    summarize(
      ptsCount = sum(ptsCount),
      rebCount = sum(rebCount),
      astCount = sum(astCount),
      fg3mCount = sum(fg3mCount),
      fg3aCount = sum(fg3aCount),
      fg3Pct = sum(fg3mCount) / sum(fg3aCount),
      fg2Pct = (sum(fgmCount) - sum(fg3mCount)) / (sum(fgaCount) - sum(fg3aCount)),
      fgPct = sum(fgmCount) / sum(fgaCount),
      blkCount = sum(blkCount),
      stlCount = sum(stlCount),
      toCount = sum(toCount),
      orebCount = sum(orebCount),
      drebCount = sum(drebCount),
      fgmCount = sum(fgmCount),
      fgaCount = sum(fgaCount),
      fg2mCount = sum(fg2mCount),
      fg2aCount = sum(fg2aCount),
      ftmCount = sum(ftmCount),
      ftaCount = sum(ftaCount),
      pfCount = sum(pfCount)
    )
  
  ### groups the agg stats by team for totals over n games *** GROUPS BY TEAM ONLY
  
  # type = TRUE filtering, grouping for propfarm, if false = filtering for research prints
  stats.team.total <- grouped %>%
      group_by(
        team
      )
  
  stats.team.total <- stats.team.total %>%
    summarize(
      ptsCount = sum(ptsCount),
      rebCount = sum(rebCount),
      astCount = sum(astCount),
      fg3mCount = sum(fg3mCount),
      fg3aCount = sum(fg3aCount),
      fg3Pct = sum(fg3mCount) / sum(fg3aCount),
      fg2Pct = (sum(fgmCount) - sum(fg3mCount)) / (sum(fgaCount) - sum(fg3aCount)),
      fgPct = sum(fgmCount) / sum(fgaCount),
      blkCount = sum(blkCount),
      stlCount = sum(stlCount),
      toCount = sum(toCount),
      orebCount = sum(orebCount),
      drebCount = sum(drebCount),
      fgmCount = sum(fgmCount),
      fgaCount = sum(fgaCount),
      fg2mCount = sum(fg2mCount),
      fg2aCount = sum(fg2aCount),
      ftmCount = sum(ftmCount),
      ftaCount = sum(ftaCount),
      pfCount = sum(pfCount)
    ) 
  
    stats.team.total <- stats.team.total %>% 
      mutate(
             ptsRank = rank(-ptsCount),
             rebRank = rank(-rebCount),
             astRank = rank(-astCount),
             fg3mRank = rank(-fg3mCount),
             fg3aRank = rank(-fg3aCount),
             fg3PctRank = rank(-fg3Pct),
             fg2PctRank = rank(-fg2Pct),
             fgPctRank = rank(-fgPct),
             blkRank = rank(-blkCount),
             stlRank = rank(-stlCount),
             toRank = rank(toCount),
             orebRank = rank(-orebCount),
             drebRank = rank(-drebCount),
             fgmRank = rank(-fgmCount),
             fgaRank = rank(-fgaCount),
             fg2mRank = rank(-fg2mCount),
             fg2aRank = rank(-fg2aCount),
             ftmRank = rank(-ftmCount),
             ftaRank = rank(-ftaCount)
    )
  
  # creates dataframes for each position to easily create ranks for each  position and stat
  positions <- c("PG", "SG", "SF", "PF", "C")
  for(pos in positions){
    var.name <- pos
    assign(var.name, stats.team.total.pos %>% 
             filter(athlete_position_abbreviation == pos) %>%
             ungroup() %>% 
             mutate(
                    ptsRank = rank(-1* ptsCount),
                    rebRank = rank(-1* rebCount),
                    astRank = rank(-1* astCount),
                    fg3mRank = rank(-1* fg3mCount),
                    fg3aRank = rank(-1* fg3aCount),
                    fg3PctRank = rank(-1* fg3Pct),
                    fg2PctRank = rank(-1* fg2Pct),
                    fgPctRank = rank(-1* fgPct),
                    blkRank = rank(-1* blkCount),
                    stlRank = rank(-1* stlCount),
                    toRank = rank(toCount),
                    orebRank = rank(-1* orebCount),
                    drebRank = rank(-1* drebCount),
                    fgmRank = rank(-1* fgmCount),
                    fgaRank = rank(-1* fgaCount),
                    fg2mRank = rank(-1* fg2mCount),
                    fg2aRank = rank(-1* fg2aCount),
                    ftmRank = rank(-1* ftmCount),
                    ftaRank = rank(-1* ftaCount)
             )
      )
  }
  # binding the position dfs with their ranks into a single df
  position.ranks <- rbind(PG, SG)
  position.ranks <- rbind(position.ranks, SF)
  position.ranks <- rbind(position.ranks, PF)
  position.ranks <- rbind(position.ranks, C)
  
  # game.opp.stats.by.pos is game level agg stats given up for each team
  # team.opp.stats.by.pos is team level agg stats given up for each team over n games for main 5 positions + RANKS for them
  # test is same as team.opp.stats.by.pos but includes generic G and F positions and NO ranks
  output <- list(grouped, position.ranks, stats.team.total.pos, stats.team.total)
  names(output) <- c("game.stats.by.pos", "team.stats.by.pos", "team.stats.by.all.pos", "team.stats")
  
  return(output)
}

#####

####################
## RETURN A PLAYER'S BOXSCORES AND AVGS  FOR SPECIFIC GAMES
####################
player.box.score.avgs <- function(box_scores, game_ids, stats_player_name){
    
    stats_player_name <- tolower(stats_player_name)
    
    box_scores <- box_scores %>%
                    mutate(athlete_display_name = tolower(athlete_display_name)) %>%
                    filter(game_id %in% game_ids & 
                           athlete_display_name == stats_player_name,
                           did_not_play == FALSE) %>% 
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
                    ))
    
    avgs <-   box_scores %>%  
        group_by(athlete_display_name) %>%
        summarize(gp=n(),
            minAvg = mean(as.numeric(min), na.rm = TRUE),
            fgmAvg = mean(as.numeric(fgm), na.rm = TRUE),
            fgaAvg = mean(as.numeric(fga), na.rm = TRUE),
            fg3mAvg = mean(as.numeric(fg3m), na.rm = TRUE),
            fg3aAvg = mean(as.numeric(fg3a), na.rm = TRUE),
            ftmAvg = mean(as.numeric(ftm), na.rm = TRUE),
            ftaAvg = mean(as.numeric(fta), na.rm = TRUE),
            orebAvg = mean(as.numeric(oreb), na.rm = TRUE),
            dreb = mean(as.numeric(dreb), na.rm = TRUE),
            rebAvg = mean(as.numeric(reb), na.rm = TRUE),
            astAvg = mean(as.numeric(ast), na.rm = TRUE),
            stlAvg = mean(as.numeric(stl), na.rm = TRUE),
            blkAvg = mean(as.numeric(blk), na.rm = TRUE),
            toAvg = mean(as.numeric(to), na.rm = TRUE),
            ptsAvg = mean(as.numeric(pts), na.rm = TRUE)
        )
        
    
    return(list(box_scores, avgs))
}
#####

###############
# retrieving a player's missed games and calculating player avg for those games
###############
player.missed.games.stats <- function(team_abb, missed_player_names, stats_player_name, season,
                                      box_scores=NULL, schedule=NULL){
    ## ingest team, names of players who you want to see missed games for,
    ## the name of a single player you want to see how they performed with the other
    ## players missing and season
    ## return a list of the games  that the players missed, 
    ## the stats for the other player in those games,
    ## the the avg stats for those games
    ## comparison of the stats in the games the players missed vs the games present
    
    # grab season schedule if not provided
    if(is.null(schedule)){
        schedule <- hoopR::load_nba_schedule(seasons = season)
    }
    
    # grab box scores if not provided
    if(is.null(box_scores)){
        box_scores <- hoopR::load_nba_player_box(seasons = season)
    }
    
    #convert text inputs to lower case for better filtering
    team_abb <- tolower(team_abb)
    missed_player_names <- tolower(missed_player_names)
    stats_player_name <- tolower(stats_player_name)
    
    # return the team's completed games for the season
    games.team <- schedule %>%
        mutate(home_abbreviation = tolower(home_abbreviation),
               away_abbreviation = tolower(away_abbreviation)
        ) %>%
        filter((home_abbreviation == team_abb | away_abbreviation == team_abb),
               status_type_completed == TRUE
        )%>%
        select(id)
    
    # this will hold the  games played for the player(s) we are interested in their misses
    games.played <- c()
    
    # looping through each player to return their missed game ids
    for (player in missed_player_names){
        #filtering box scores to the player of interest
        games.player <- box_scores %>%
            mutate(athlete_display_name = tolower(athlete_display_name)) %>%
            filter(athlete_display_name == player & did_not_play == FALSE) %>%
            select(game_id)
        
        games.played <- append(games.played, games.player$game_id)
        
    }
    games.played <- games.played %>% unique()
    
    # final list of all missed games
    missed.games <- setdiff(games.team$id, games.played)

    
    # checking players missed no games
    if(length(missed.games) == 0){
        return("player missed no games")
    }
    else{

        #player avgs after filtering for games and POI
        missing <- player.box.score.avgs(box_scores, missed.games, stats_player_name)
        avgs.missing <- missing[[2]]
        box.games.missed <- missing[[1]]
        
        # gathering the stat players season avgs
        active <- player.box.score.avgs(box_scores, games.played, stats_player_name)
        avgs.active <- active[[2]]
        box.games.active <- active[[1]]
        
        # combining the avg when players are missing and full season
        avg.change <- rbind(avgs.missing, avgs.active)
        
        #caclulting the difference between the 2 avgs
        avg.change <- data.frame(avg.change[1,-1] - avg.change[2,-1])
        avg.change$athlete_display_name <- stats_player_name
        
    }
    
    # function returns the 1)list of missed game ids for requested players
    # 2)boxscores from missed games for POI, 3) avgs from missed games for POI
    # 4) box scores from games missing players active, 5) avgs for active
    # games for POI, 6) different in missing/active avgs
    return(list("missed.games"=missed.games, 
                "box.scores.missed"=box.games.missed, "avg.missed"=avgs.missing, 
                "box.scores.active"=box.games.active, "avg.active"=avgs.active, 
                "avg.change"=avg.change))
}
#####

###############
# build dataframe for betting info from list of gids
###############
games.betting.info <- function(gids){
    # ingest a df that contains game ids in col = "game_id"
    # output dataframe with the betting info added
    
    for(i in 1:length(gids)){
        gid <- as.numeric(gids[i])
        if(i ==1){
            betting.info.game <- espn_nba_betting(gid)$pickcenter %>%
                filter(provider_name == "consensus") %>%
                select(over_under, spread) %>%
                rename(c(overUnder = over_under)) %>%
                mutate(game_id = gid)
        }
        else{
            temp <- espn_nba_betting(gid)$pickcenter %>%
                filter(provider_name == "consensus") %>%
                select(over_under, spread) %>%
                rename(c(overUnder = over_under)) %>%
                mutate(game_id = gid)
            
            betting.info.game <- rbind(betting.info.game, temp)
        }
    }
    return(betting.info.game)
}
#####

###############
# calculate player avg against current opp. in current season and previous season
###############
player.avg.vs.opp <- function(players, opp,  
                              schedule=NULL,
                              box_scores=NULL,
                              seasons=NULL){
  #  opp team abbreviation, vector of player names, 
  ## df of boxscores, df of schedules 
  ## can also provide a list of seasons if more than current and prev is desired
  ## if season list  is provided, dfs are ignored.
  # output players avgs against teams and boxscores for the games into avgs
  
  opp <- toupper(opp)
  players <- tolower(players)

  # check to see if specific seasons were provided. 
  # loop through the seasons to build the boxscores and schedules
  # filter the schedules to only games the OPP of interest are in
  if(!is.null(seasons) ){
    #schedules <- hoopR::load_nba_schedule(seasons = seasons[1]) %>%
    #                      filter(home_abbreviation == opp | away_abbreviation == opp)
    schedules <- hoopR::espn_nba_scoreboard(season = seasons[1]) %>%
                         filter(home_team_abb == opp | away_team_abb == opp)
    
    box_scores <- hoopR::load_nba_player_box(seasons = seasons[1])
    for(s in 2:length(seasons)){
      #temp.scheds <- hoopR::load_nba_schedule(seasons = seasons[s]) %>%
      #                        filter(home_abbreviation == opp | away_abbreviation == opp)
      temp.scheds <- hoopR::espn_nba_scoreboard(season = seasons[s]) %>%
                            filter(home_team_abb == opp | away_team_abb == opp)
      
      schedules <- rbind(schedules, temp.scheds)
      
      temp.bs <- hoopR::load_nba_player_box(seasons = seasons[s])
      box_scores <- rbind(box_scores, temp.bs)
    }
     
  }
  else{
    # no seasons provided, either pulls current and previous or use provided
    # box scores and schedules
    
    # storing current season value
    current.season <- hoopR::most_recent_nba_season()
    # Checking if schedules and box scores are provided. gathering them if not.
    if(is.null(box_scores)){
      box.scores.current <- hoopR::load_nba_player_box(seasons = current.season)
      box.scores.prev <- hoopR::load_nba_player_box(seasons = current.season - 1)
      box_scores <- rbind(box.scores.current, box.scores.prev)
    }
    
    # if specific seasons were not provided pull current and previous seasons and filter for OPP games
    if(is.null(schedule)){
      #schedule_current <- hoopR::load_nba_schedule(seasons = current.season) %>%
      #                            filter(home_abbreviation == opp | away_abbreviation == opp)
      #schedule_prev <- hoopR::load_nba_schedule(seasons = current.season - 1) %>%
      #                            filter(home_abbreviation == opp | away_abbreviation == opp)
      schedule_current <- hoopR::espn_nba_scoreboard(season = current.season) %>%
                                   filter(home_team_abb == opp | away_team_abb == opp)
      schedule_prev <- hoopR::espn_nba_scoreboard(season = current.season - 1) %>%
                                filter(home_team_abb == opp | away_team_abb == opp)
      schedules <- rbind(schedule_current, schedule_prev)
    }
  }
  # assign the game_ids for the OPP
  #opp.game.ids <- (schedules %>% select(id))$id
  opp.game.ids <- (schedules %>% select(game_id))$game_id
  print(opp.game.ids)
  # splitting shooting stats in box scores
  box.scores <-   box_scores %>% 
                    filter(minutes >0) %>% 
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
                    mutate(athlete_display_name = tolower(athlete_display_name))
  
  for(i in 1:length(players)){
    # final filter of the box scores to be just the single player  in the game_ids the OPP played
    if(i == 1){
      avgs <- box.scores %>%
                filter(athlete_display_name == players[i] & 
                       game_id %in% opp.game.ids
                      )
    }
    else{
      temp <- box.scores %>%
                filter(athlete_display_name == players[i] & 
                       game_id %in% opp.game.ids
                       )

      avgs <- rbind(avgs, temp)
    }
    
  }
  agged <- avgs %>%  
    group_by(athlete_display_name) %>%
    summarize(gp=n(),
              minAvg = mean(as.numeric(min), na.rm = TRUE),
              fgmAvg = mean(as.numeric(fgm), na.rm = TRUE),
              fgaAvg = mean(as.numeric(fga), na.rm = TRUE),
              fg3mAvg = mean(as.numeric(fg3m), na.rm = TRUE),
              fg3aAvg = mean(as.numeric(fg3a), na.rm = TRUE),
              ftmAvg = mean(as.numeric(ftm), na.rm = TRUE),
              ftaAvg = mean(as.numeric(fta), na.rm = TRUE),
              orebAvg = mean(as.numeric(oreb), na.rm = TRUE),
              drebAvg = mean(as.numeric(dreb), na.rm = TRUE),
              rebAvg = mean(as.numeric(reb), na.rm = TRUE),
              astAvg = mean(as.numeric(ast), na.rm = TRUE),
              stlAvg = mean(as.numeric(stl), na.rm = TRUE),
              blkAvg = mean(as.numeric(blk), na.rm = TRUE),
              toAvg = mean(as.numeric(to), na.rm = TRUE),
              ptsAvg = mean(as.numeric(pts), na.rm = TRUE)
    )
  # returns df of individual game boxscores and the df of the box scores aggregated
  return(list("matchup.boxscores"=avgs, 
              "matchup.agg"=agged))

}

#####

###############
# calculating all teams travel +/- throughout the season schedule
###############
### this is setup to receive a processed distance matrix with rows and columns named as db teamIds
### to aid in multi sport use.
distance.traveled <- function(distanceMatrix){
  return(0) 
}

#####

###############
# adding in the playin + playoff  games to the boxscores
# originally used  in prop farm when the boxscore wasn't updating at the end of 23
##########
# missing dates need to be added as they happen
# missing.game.dates <- c(
#     "2023-04-11", "2023-04-12", "2023-04-14", "2023-04-15",  "2023-04-16", 
#     "2023-04-17", "2023-04-18"
# )
# for (i in missing.game.dates){
#     gm.date  <-  gsub("-", "" , i,)
#     gids <- c(espn_nba_scoreboard (season = gm.date)$game_id)
#     
#     if(i == missing.game.dates[1]){
#         for(j in gids){
#             if(j == gids[1]){
#                 missing <- hoopR::espn_nba_player_box(j)
#                 missing$game_id <- j
#                 missing$game_date <- as.Date(i)
#             } else{
#                 temp <- hoopR::espn_nba_player_box(j)
#                 temp$game_id <- j
#                 temp$game_date <- as.Date(i)
#                 missing <- rbind(missing, temp)
#             }
#         }
#     }
#     else{
#         for(j in gids){
#             temp <- hoopR::espn_nba_player_box(j)
#             temp$game_id <- j
#             temp$game_date <- as.Date(i)
#             missing <- rbind(missing, temp)
#         }
#     }
# }
# 
# # merging back to season boxscore data
# boxscore.player <- rbind(boxscore.player, missing, fill=TRUE) %>%
#     arrange(athlete_id, desc(game_date)) 
##################

##################
# scraper basketball reference to get players positional percent estimates
##################
players.played.position.estimate <- function(season, pull.date){
  ## season should be four digit numerical
  ## db process loads into my custom db, it applies the hoopr player ids to the bref player names
  today <- pull.date
  
  # https://www.basketball-reference.com/teams/PHI/2024.html#pbp
  basketball.reference.team.abbr <- c('GSW','DEN','POR','SAC','TOR','DAL','PHO','CHI',
                                      'LAL','HOU','MIA','MEM','DET','MIL','NOP','MIN',
                                      'CLE','OKC','LAC','BRK','SAS','NYK','WAS','CHO',
                                      'UTA','IND','BOS','PHI','ATL','ORL'
  )
  
  start.url <- "https://www.basketball-reference.com/teams/" 
  end.url <- ".html"
  
  #empty data frame to agg all the teams in the loop
  bref.cols <- c('player', 'age', 'gp', 'mp', 'PG', 'SG', 'SF', 'PF', 'C',
                 'onCourtPlusMinusPer100', 'onOffPlusMinusPer100', 'badPass', 'lostBall', 
                 'shootFoulCommitted', 'offFoulCommitted', 'shootFoulDrawn', 'offFoulDrawn', 
                 'ptsGenFromAst', 'andOnes','shotsBlk', 'date', 'team')
  
  bref.pos.estimates <- data.frame(matrix(nrow=0, ncol = length(bref.cols)))
  colnames(bref.pos.estimates) <- bref.cols
  
  # looping through each teams page on bbref to build dataframe for the date
  for (i in basketball.reference.team.abbr){
    # build team url
    url <- paste(start.url, i, '/', season, end.url, sep = "")
    
    # retrieve table from the website html - the table is at the bottom of the page and dynamically accessed
    # until it is accessed it is in the html as a comment and needs to be extracted.
    team.pos.estimates <- url %>%
      read_html %>%
      html_nodes(xpath = '//comment()') %>%
      html_text() %>%
      paste(collapse='') %>%
      read_html() %>% 
      html_node("#pbp") %>% 
      html_table(trim = TRUE)
    
    Sys.sleep(2)
    # correcting headers
    colnames(team.pos.estimates) <- c('rk', 'player', 'age', 'gp', 'mp', 'PG', 'SG', 'SF',
                                      'PF', 'C', 'onCourtPlusMinusPer100', 'onOffPlusMinusPer100',
                                      'badPass', 'lostBall', 'shootFoulCommitted', 'offFoulCommitted',
                                      'shootFoulDrawn', 'offFoulDrawn', 'ptsGenFromAst', 'andOnes', 'shotsBlk')
    
    # removing row that was used for headers and their rank and converting str pct to nums
    team.pos.estimates <- team.pos.estimates[-1,-1] %>% 
      mutate(date = today,
             PG = as.numeric(gsub('%', '', PG)) / 100,
             SG = as.numeric(gsub('%', '', SG))/ 100,
             SF = as.numeric(gsub('%', '', SF))/ 100,
             PF = as.numeric(gsub('%', '', PF))/ 100,
             C = as.numeric(gsub('%', '', C))/ 100,
             team = i
      ) %>% 
      mutate_at(c('PG', 'SG', 'SF', 'PF', 'C'), replace_na, 0)
    
    # filter out traded players that aren't udpated in the site
    #trade
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Nicolas Batum' & team == 'LAC'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'P.J. Tucker' & team == 'PHI'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Robert Covington' & team == 'LAC'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'KJ Martin' & team == 'LAC'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Filip Petrušev' & team == 'PHI'))
    # acquistion
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Daniel Theis' & team == 'IND'))
    #trade
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'OG Anunoby' & team == 'TOR'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Precious Achiuwa' & team == 'TOR'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Malachi Flynn' & team == 'TOR'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'RJ Barrett' & team == 'NYK'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Immanuel Quickley' & team == 'NYK'))
    #acquisition
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Kenneth Lofton Jr.' & team == 'MEM'))
    #acquisition
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Dylan Windler' & team == 'NYK'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Skylar Mays' & team == 'POR'))
    #trade
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Bruce Brown' & team == 'IND'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Jordan Nwora' & team == 'IND'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Kira Lewis Jr.' & team == 'NOP'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Pascal Siakam' & team == 'TOR'))
    #trade
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Danilo Gallinari' & team == 'DET'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Danilo Gallinari' & team == 'WAS'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Mike Muscala' & team == 'WAS'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Isaiah Livers' & team == 'DET'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Marvin Bagley III' & team == 'DET'))
    # trade
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Kyle Lowry' & team == 'MIA'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Terry Rozier' & team == 'CHO'))
    # trade
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Simone Fontecchio' & team == 'UTA'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Kevin Knox' & team == 'DET'))
    # trade
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Xavier Tillman Sr.' & team == 'MEM'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Bismack Biyombo' & team == 'MEM'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Lamar Stevens' & team == 'BOS'))
    #trade
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Shake Milton' & team == 'MIN'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Troy Brown Jr.' & team == 'MIN'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Monte Morris' & team == 'DET'))
    #trade
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Alec Burks' & team == 'DET'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Bojan Bogdanović' & team == 'DET'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Quentin Grimes' & team == 'NYK'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Malachi Flynn' & team == 'NYK'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Evan Fournier' & team == 'NYK'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Ryan Arcidiacono' & team == 'NYK'))
    #trade
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Spencer Dinwiddie' & team == 'BRK'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Thaddeus Young' & team == 'TOR'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Dennis Schröder' & team == 'TOR'))
    #trade
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Danuel House Jr.' & team == 'PHI'))
    #trade
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Kelly Olynyk' & team == 'UTA'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Ochai Agbaji' & team == 'UTA'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Otto Porter Jr.' & team == 'TOR'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Kira Lewis Jr.' & team == 'TOR'))
    #trade
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Marcus Morris' & team == 'IND'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Doug McDermott' & team == 'SAS'))
    #trade
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Yuta Watanabe' & team == 'PHO'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Keita Bates-Diop' & team == 'PHO'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Jordan Goodwin' & team == 'PHO'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Chimezie Metu' & team == 'PHO'))
    #trade
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Robin Lopez' & team == 'MIL'))
    #trade
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Patrick Beverley' & team == 'PHI'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Cameron Payne' & team == 'MIL'))
    #trade
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Richaun Holmes' & team == 'DAL'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Grant Williams' & team == 'DAL'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Seth Curry' & team == 'DAL'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Daniel Gafford' & team == 'WAS'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'P.J. Washington' & team == 'CHO'))
    #trade
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Gordon Hayward' & team == 'CHO'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Vasilije Micić' & team == 'OKC'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Dāvis Bertāns' & team == 'OKC'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Tre Mann' & team == 'OKC'))
    #trade
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'David Roddy' & team == 'MEM'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == "Royce O'Neale" & team == 'BRK'))
    #trade
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Buddy Hield' & team == 'IND'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Marcus Morris' & team == 'PHI'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Furkan Korkmaz' & team == 'PHI'))
    # trade
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Cory Joseph' & team == 'GSW'))
    # trade
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Dalano Banton' & team == 'BOS'))
    #waived
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Joe Harris' & team == 'DET'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Killian Hayes' & team == 'DET'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Harry Giles' & team == 'DET'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Jaden Springer' & team == 'PHI'))
    #cleanups
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Trey Jemison' & team == 'MEM'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Tosan Evbuomwan' & team == 'MEM'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Théo Maledon' & team == 'CHO'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Delon Wright' & team == 'WAS'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Marques Bolden' & team == 'MIL'))
    
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Harry Giles' & team == 'BRK'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Mike Muscala' & team == 'DET'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Aleksej Pokusevski' & team == 'OKC'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Shake Milton' & team == 'DET'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Patty Mills' & team == 'ATL'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Ish Wainright' & team == 'POR'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Dylan Windler' & team == 'LAL'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Taj Gibson' & team == 'NYK'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Jacob Gilyard' & team == 'MEM'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Mamadi Diakite' & team == 'SAS'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Jaylen Nowell' & team == 'MEM'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Darius Bazley' & team == 'PHI'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Kenneth Lofton Jr.' & team == 'PHI'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Ryan Rollins' & team == 'WAS'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Jamaree Bouyea' & team == 'POR'))
    team.pos.estimates <- team.pos.estimates %>% filter(!(player == 'Alex Fudge' & team == 'LAL'))
    
    
    team.pos.estimates <- team.pos.estimates  %>% unique()
    
    # combine temp team df to agg df
    bref.pos.estimates <- rbind(bref.pos.estimates, team.pos.estimates)
    Sys.sleep(1)
  }
  
  # creating column to designate players position by using the one with the highest % of plays
  bref.pos.estimates$pos <- colnames(bref.pos.estimates)[(5:9)[max.col(bref.pos.estimates[5:9], "first")]]
  
  #roto doesn't have suffixes but everything else does. UGH!
  # creating name column without suffixes to join with betting data until ID list is compiled
  suffix.rep <- c("\\."="", "`"="", "'"="",
                  " III$"="", " IV$"="", " II$"="", " iii$"="", " ii$"="", " iv$"="",
                  " jr$"="", " sr$"="", " jr.$"="", " sr.$"="", " Jr$"="", " Sr$"="", " Jr.$"="", " Sr.$"="",
                  "š"="s","ş"="s", "š"="s", 'š'="s", "š"="s",
                  "ž"="z",
                  "þ"="p","ģ"="g",
                  "à"="a","á"="a","â"="a","ã"="a","ä"="a","å"="a",'ā'="a",
                  "ç"="c",'ć'="c", 'č'="c",
                  "è"="e","é"="e","ê"="e","ë"="e",'é'="e",
                  "ì"="i","í"="i","î"="i","ï"="i",
                  "ð"="o","ò"="o","ó"="o","ô"="o","õ"="o","ö"="o",'ö'="o",
                  "ù"="u","ú"="u","û"="u","ü"="u","ū"="u",
                  "ñ"="n","ņ"="n",
                  "ý"="y",
                  "Dario .*"="dario saric", "Alperen .*"="alperen sengun", "Luka.*amanic"="luka samanic"
  )
  
  # add actnetid to roto
  bref.pos.estimates <- bref.pos.estimates %>% 
    mutate(
      joinName = trimws(tolower(stringr::str_replace_all(player, suffix.rep)))
    ) 
  
  return(bref.pos.estimates)
}

#################

####################
# process harvest into easy to read output
####################
process.harvest <- function(harvest){
  
  p <- harvest %>% 
    # filter((((ptsOscore > -1) & (line_pts > 3)) | (ptsOscore > 0)) | (((ptsUscore > -1) & (line_pts > 1)) | (ptsUscore > 0))) %>%
    select(player, pos, team, opp, ptsOscore, ptsUscore, 
           ptsSynth, line_pts, btb, btbOpp, 
           ptsRank, rebRank, astRank, fg3mRank, stlRank, blkRank, fgPctRank, fg2PctRank, fg3PctRank,
           minAvg, minAvgL3, minAvgL10, ptsAvg, ptsAvgL3, ptsAvgL10,  game_id) %>%
    arrange(desc(ptsOscore))
  #View(p)
  colnames(p) <- c("player", "pos", "team", "opp", "Oscore", "Uscore", "Synth", "line", "btb", "btbOpp", 
                   "oppPtsRank", "oppRebRank", "oppAstRank", "oppFg3mRank", "oppStlRank", "oppBlkRank", "oppFgPctRank", "oppFg2PctRank", "oppFg3PctRank",
                   "minAvg", "minAvgL3", "minAvgL10", "Avg", "AvgL3", "AvgL10", "game_id" )
  p$prop = 'pts'
  
  r <- harvest %>% 
    #filter( (((rebOscore > -1) & (line_reb > 3)) | (rebOscore > 0)) | (((rebUscore > -1) & (line_reb > 1)) | (rebUscore > 0))) %>%
    select(player, pos, team, opp, #overUnder, spread,
           rebOscore, rebUscore,rebSynth, line_reb, btb, btbOpp, 
           ptsRank, rebRank, astRank, fg3mRank, stlRank, blkRank,fgPctRank, fg2PctRank, fg3PctRank,
           minAvg, minAvgL3, minAvgL10,  rebAvg, rebAvgL3, rebAvgL10,  game_id) %>%
    arrange(desc(rebOscore))
  #View(r)
  colnames(r) <- c("player", "pos", "team", "opp", "Oscore", "Uscore", "Synth", "line", "btb", "btbOpp", 
                   "oppPtsRank", "oppRebRank", "oppAstRank", "oppFg3mRank", "oppStlRank", "oppBlkRank", "oppFgPctRank", "oppFg2PctRank", "oppFg3PctRank",
                   "minAvg", "minAvgL3", "minAvgL10", "Avg", "AvgL3", "AvgL10", "game_id" )
  r$prop = 'reb'
  
  a <- harvest %>% 
    # filter( (((astOscore > -1) & (line_ast > 3)) | (astOscore > 0)) | (((astUscore > -1) & (line_ast > 1)) | (astUscore > 0))) %>%
    select(player, pos, team, opp, #overUnder, spread,
           astOscore, astUscore, 
           astSynth, line_ast, btb, btbOpp, 
           ptsRank, rebRank, astRank, fg3mRank, stlRank, blkRank,fgPctRank, fg2PctRank, fg3PctRank,
           minAvg, minAvgL3, minAvgL10,  astAvg, astAvgL3, astAvgL10,  game_id) %>%
    arrange(desc(astOscore))
  #View(a)
  colnames(a) <- c("player", "pos", "team", "opp", "Oscore", "Uscore", "Synth", "line", "btb", "btbOpp", 
                   "oppPtsRank", "oppRebRank", "oppAstRank", "oppFg3mRank", "oppStlRank", "oppBlkRank", "oppFgPctRank", "oppFg2PctRank", "oppFg3PctRank",
                   "minAvg", "minAvgL3", "minAvgL10", "Avg", "AvgL3", "AvgL10", "game_id" )
  a$prop = 'ast'
  
  tre <- harvest %>% 
    #filter( (((fg3mOscore > -1) & (line_threes > 1)) | (fg3mOscore > 0.25)) | (((fg3mUscore > -1) & (line_threes > 1)) | (fg3mUscore > 0.25))) %>%
    select(player, pos, team, opp, # overUnder, spread,
           fg3mOscore, fg3mUscore, fg3mSynth, line_threes,
           btb, btbOpp, 
           ptsRank, rebRank, astRank, fg3mRank, stlRank, blkRank,fgPctRank, fg2PctRank, fg3PctRank,
           minAvg, minAvgL3, minAvgL10, fg3mAvg, fg3mAvgL3, fg3mAvgL10,  game_id ) %>%
    arrange(desc(fg3mOscore))
  #View(tre)
  colnames(tre) <- c("player", "pos", "team", "opp", "Oscore", "Uscore", "Synth", "line", "btb", "btbOpp", 
                     "oppPtsRank", "oppRebRank", "oppAstRank", "oppFg3mRank", "oppStlRank", "oppBlkRank", "oppFgPctRank", "oppFg2PctRank", "oppFg3PctRank",
                     "minAvg", "minAvgL3", "minAvgL10", "Avg", "AvgL3", "AvgL10", "game_id" )
  tre$prop = 'fg3m'
  
  pra <- harvest %>% 
    # filter( (((praOscore > -1) & (line_pra > 3)) | (praOscore > 0)) | (((praUscore > -1) & (line_pra > 1)) | (praUscore > 0))) %>%
    select(player, pos, team,  opp, #overUnder, spread,
           praOscore, praUscore,praSynth, line_pra, 
           btb, btbOpp, 
           ptsRank, rebRank, astRank, fg3mRank, stlRank, blkRank,fgPctRank, fg2PctRank, fg3PctRank,
           minAvg, minAvgL3, minAvgL10, praAvg, praAvgL3, praAvgL10, game_id) %>%
    arrange(desc(praOscore))
  #View(pra)
  colnames(pra) <- c("player", "pos", "team", "opp", "Oscore", "Uscore", "Synth", "line", "btb", "btbOpp", 
                     "oppPtsRank", "oppRebRank", "oppAstRank", "oppFg3mRank", "oppStlRank", "oppBlkRank", "oppFgPctRank", "oppFg2PctRank", "oppFg3PctRank",
                     "minAvg", "minAvgL3", "minAvgL10", "Avg", "AvgL3", "AvgL10", "game_id" )
  pra$prop = 'pra'
  
  pr <- harvest %>% 
    #filter( (((prOscore > -1) & (line_pr > 3)) | (prOscore > 0)) | (((prUscore > -1) & (line_pr > 1)) | (prUscore > 0)) ) %>%
    select(player, pos, team, opp, #overUnder, spread,
           prOscore, prUscore, prSynth, line_pr, 
           btb, btbOpp, 
           ptsRank, rebRank, astRank, fg3mRank, stlRank, blkRank,fgPctRank, fg2PctRank, fg3PctRank,
           minAvg, minAvgL3, minAvgL10,  prAvg, prAvgL3, prAvgL10, game_id ) %>%
    arrange(desc(prOscore))
  #View(pr)
  colnames(pr) <- c("player", "pos", "team", "opp", "Oscore", "Uscore", "Synth", "line", "btb", "btbOpp", 
                    "oppPtsRank", "oppRebRank", "oppAstRank", "oppFg3mRank", "oppStlRank", "oppBlkRank", "oppFgPctRank", "oppFg2PctRank", "oppFg3PctRank",
                    "minAvg", "minAvgL3", "minAvgL10", "Avg", "AvgL3", "AvgL10", "game_id" )
  pr$prop = 'pr'
  
  pa <- harvest %>% 
    # filter( (((paOscore > -1) & (line_pa > 3)) | (paOscore > 0)) | (((paUscore > -1) & (line_pa > 1)) | (paUscore > 0))) %>%
    select(player, pos, team, opp, #overUnder, spread,
           paOscore, paUscore, paSynth, line_pa, btb, btbOpp, 
           ptsRank, rebRank, astRank, fg3mRank, stlRank, blkRank,fgPctRank, fg2PctRank, fg3PctRank,
           minAvg, minAvgL3, minAvgL10,  paAvg, paAvgL3, paAvgL10,game_id) %>%
    arrange(desc(paOscore))
  #View(pa)
  colnames(pa) <- c("player", "pos", "team", "opp", "Oscore", "Uscore", "Synth", "line", "btb", "btbOpp", 
                    "oppPtsRank", "oppRebRank", "oppAstRank", "oppFg3mRank", "oppStlRank", "oppBlkRank", "oppFgPctRank", "oppFg2PctRank", "oppFg3PctRank",
                    "minAvg", "minAvgL3", "minAvgL10", "Avg", "AvgL3", "AvgL10", "game_id" )
  pa$prop = 'pa'
  
  ra <- harvest %>% 
    #filter( (((raOscore > -1) & (line_ra > 3)) | (raOscore > 0)) | (((raUscore > -1) & (line_ra > 1)) | (raUscore > 0)) ) %>%
    select(player, pos, team, opp, #overUnder, spread,
           raOscore, raUscore, raSynth, line_ra,btb, btbOpp, 
           ptsRank, rebRank, astRank, fg3mRank, stlRank, blkRank,fgPctRank, fg2PctRank, fg3PctRank,
           minAvg, minAvgL3, minAvgL10,  raAvg, raAvgL3, raAvgL10, game_id 
    ) %>%
    arrange(desc(raOscore))
  #View(ra)
  colnames(ra) <- c("player", "pos", "team", "opp", "Oscore", "Uscore", "Synth", "line", "btb", "btbOpp", 
                    "oppPtsRank", "oppRebRank", "oppAstRank", "oppFg3mRank", "oppStlRank", "oppBlkRank", "oppFgPctRank", "oppFg2PctRank", "oppFg3PctRank",
                    "minAvg", "minAvgL3", "minAvgL10", "Avg", "AvgL3", "AvgL10", "game_id" )
  ra$prop = 'ra'
  
  stls <- harvest %>% 
    #filter((stlOscore > 0.25) | (stlUscore > 0.25)) %>%
    select(player, pos, team, opp, #overUnder, spread,
           stlOscore, stlUscore, stlSynth, line_stl, 
           btb, btbOpp, 
           ptsRank, rebRank, astRank, fg3mRank, stlRank, blkRank,fgPctRank, fg2PctRank, fg3PctRank,
           minAvg, minAvgL3, minAvgL10,  stlAvg, stlAvgL3, stlAvgL10,  game_id) %>%
    arrange(desc(stlOscore))
  #View(stls)
  colnames(stls) <- c("player", "pos", "team", "opp", "Oscore", "Uscore", "Synth", "line", "btb", "btbOpp", 
                      "oppPtsRank", "oppRebRank", "oppAstRank", "oppFg3mRank", "oppStlRank", "oppBlkRank", "oppFgPctRank", "oppFg2PctRank", "oppFg3PctRank",
                      "minAvg", "minAvgL3", "minAvgL10", "Avg", "AvgL3", "AvgL10", "game_id" )
  stls$prop = 'stl'
  
  b <- harvest %>% 
    #filter((blkOscore > 0.25) | (blkUscore > 0.25)) %>%
    select(player, pos, team, opp, #overUnder, spread,
           blkOscore, blkUscore, blkSynth, line_blk, 
           btb, btbOpp, 
           ptsRank, rebRank, astRank, fg3mRank, stlRank, blkRank,fgPctRank, fg2PctRank, fg3PctRank,
           minAvg, minAvgL3, minAvgL10,  blkAvg, blkAvgL3, blkAvgL10, game_id ) %>%
    arrange(desc(blkOscore))
  #View(b)
  colnames(b) <- c("player", "pos", "team", "opp", "Oscore", "Uscore", "Synth", "line", "btb", "btbOpp", 
                   "oppPtsRank", "oppRebRank", "oppAstRank", "oppFg3mRank", "oppStlRank", "oppBlkRank", "oppFgPctRank", "oppFg2PctRank", "oppFg3PctRank",
                   "minAvg", "minAvgL3", "minAvgL10", "Avg", "AvgL3", "AvgL10", "game_id" )
  b$prop = 'blk'
  
  stocks <- harvest %>% 
    #filter( (sbOscore > 0.25) |  (sbUscore > 0.25)) %>%
    select(player, pos, team, opp,# overUnder, spread,
           sbOscore, sbUscore, sbSynth, line_sb, btb, btbOpp, 
           ptsRank, rebRank, astRank, fg3mRank, stlRank, blkRank,fgPctRank, fg2PctRank, fg3PctRank,
           minAvg, minAvgL3, minAvgL10,  sbAvg, sbAvgL3, sbAvgL10, game_id 
    ) %>%
    arrange(desc(sbOscore))
  #View(stocks)
  colnames(stocks) <- c("player", "pos", "team", "opp", "Oscore", "Uscore", "Synth", "line", "btb", "btbOpp", 
                        "oppPtsRank", "oppRebRank", "oppAstRank", "oppFg3mRank", "oppStlRank", "oppBlkRank", "oppFgPctRank", "oppFg2PctRank", "oppFg3PctRank",
                        "minAvg", "minAvgL3", "minAvgL10", "Avg", "AvgL3", "AvgL10", "game_id" )
  stocks$prop = 'sb'
  
  scores <- rbind(p, r)
  scores <- rbind(scores, a)
  scores <- rbind(scores, tre)
  scores <- rbind(scores, pra)
  scores <- rbind(scores, pr)
  scores <- rbind(scores, pa)
  scores <- rbind(scores, ra)
  scores <- rbind(scores, stls)
  scores <- rbind(scores, b)
  scores <- rbind(scores, stocks) %>% 
    select(player, pos, team, opp, prop, line, Oscore, Uscore, Synth, btb, btbOpp, oppPtsRank, oppRebRank,
           oppAstRank, oppFg3mRank, oppStlRank, oppBlkRank, oppFgPctRank, oppFg2PctRank, oppFg3PctRank,
           minAvg, minAvgL3, minAvgL10, Avg, AvgL3, AvgL10, game_id) %>% 
    arrange(desc(Oscore))
  
  return(scores)
}
####################

####################
## FUNCTION TO CALCULATE PROPFARM PLAYER STATS FROM WNBA BOX
####################
wnbaPropfarming <- function(box.score.data, team.ids, matchups.today, minFilter=20, player_info=NULL){
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
######

####################
# process harvest into easy to read output
####################
process.harvest.wnba <- function(harvest){
  
  p <- harvest %>% 
    # filter((((ptsOscore > -1) & (line_pts > 3)) | (ptsOscore > 0)) | (((ptsUscore > -1) & (line_pts > 1)) | (ptsUscore > 0))) %>%
    select(player, pos, team, opp, ptsOscore, ptsUscore, 
           ptsSynth, line_pts, btb, btbOpp, 
           ptsRank, rebRank, astRank, fg3mRank, stlRank, blkRank, fgPctRank, fg2PctRank, fg3PctRank,
           minAvg, minAvgL3, minAvgL10, ptsAvg, ptsAvgL3, ptsAvgL10,  game_id) %>%
    arrange(desc(ptsOscore))
  #View(p)
  colnames(p) <- c("player", "pos", "team", "opp", "Oscore", "Uscore", "Synth", "line", "btb", "btbOpp", 
                   "oppPtsRank", "oppRebRank", "oppAstRank", "oppFg3mRank", "oppStlRank", "oppBlkRank", "oppFgPctRank", "oppFg2PctRank", "oppFg3PctRank",
                   "minAvg", "minAvgL3", "minAvgL10", "Avg", "AvgL3", "AvgL10", "game_id" )
  p$prop = 'pts'
  
  r <- harvest %>% 
    #filter( (((rebOscore > -1) & (line_reb > 3)) | (rebOscore > 0)) | (((rebUscore > -1) & (line_reb > 1)) | (rebUscore > 0))) %>%
    select(player, pos, team, opp, #overUnder, spread,
           rebOscore, rebUscore,rebSynth, line_reb, btb, btbOpp, 
           ptsRank, rebRank, astRank, fg3mRank, stlRank, blkRank,fgPctRank, fg2PctRank, fg3PctRank,
           minAvg, minAvgL3, minAvgL10,  rebAvg, rebAvgL3, rebAvgL10,  game_id) %>%
    arrange(desc(rebOscore))
  #View(r)
  colnames(r) <- c("player", "pos", "team", "opp", "Oscore", "Uscore", "Synth", "line", "btb", "btbOpp", 
                   "oppPtsRank", "oppRebRank", "oppAstRank", "oppFg3mRank", "oppStlRank", "oppBlkRank", "oppFgPctRank", "oppFg2PctRank", "oppFg3PctRank",
                   "minAvg", "minAvgL3", "minAvgL10", "Avg", "AvgL3", "AvgL10", "game_id" )
  r$prop = 'reb'
  
  a <- harvest %>% 
    # filter( (((astOscore > -1) & (line_ast > 3)) | (astOscore > 0)) | (((astUscore > -1) & (line_ast > 1)) | (astUscore > 0))) %>%
    select(player, pos, team, opp, #overUnder, spread,
           astOscore, astUscore, 
           astSynth, line_ast, btb, btbOpp, 
           ptsRank, rebRank, astRank, fg3mRank, stlRank, blkRank,fgPctRank, fg2PctRank, fg3PctRank,
           minAvg, minAvgL3, minAvgL10,  astAvg, astAvgL3, astAvgL10,  game_id) %>%
    arrange(desc(astOscore))
  #View(a)
  colnames(a) <- c("player", "pos", "team", "opp", "Oscore", "Uscore", "Synth", "line", "btb", "btbOpp", 
                   "oppPtsRank", "oppRebRank", "oppAstRank", "oppFg3mRank", "oppStlRank", "oppBlkRank", "oppFgPctRank", "oppFg2PctRank", "oppFg3PctRank",
                   "minAvg", "minAvgL3", "minAvgL10", "Avg", "AvgL3", "AvgL10", "game_id" )
  a$prop = 'ast'
  
  tre <- harvest %>% 
    #filter( (((fg3mOscore > -1) & (line_threes > 1)) | (fg3mOscore > 0.25)) | (((fg3mUscore > -1) & (line_threes > 1)) | (fg3mUscore > 0.25))) %>%
    select(player, pos, team, opp, # overUnder, spread,
           fg3mOscore, fg3mUscore, fg3mSynth, line_threes,
           btb, btbOpp, 
           ptsRank, rebRank, astRank, fg3mRank, stlRank, blkRank,fgPctRank, fg2PctRank, fg3PctRank,
           minAvg, minAvgL3, minAvgL10, fg3mAvg, fg3mAvgL3, fg3mAvgL10,  game_id ) %>%
    arrange(desc(fg3mOscore))
  #View(tre)
  colnames(tre) <- c("player", "pos", "team", "opp", "Oscore", "Uscore", "Synth", "line", "btb", "btbOpp", 
                     "oppPtsRank", "oppRebRank", "oppAstRank", "oppFg3mRank", "oppStlRank", "oppBlkRank", "oppFgPctRank", "oppFg2PctRank", "oppFg3PctRank",
                     "minAvg", "minAvgL3", "minAvgL10", "Avg", "AvgL3", "AvgL10", "game_id" )
  tre$prop = 'fg3m'
  
  pra <- harvest %>% 
    # filter( (((praOscore > -1) & (line_pra > 3)) | (praOscore > 0)) | (((praUscore > -1) & (line_pra > 1)) | (praUscore > 0))) %>%
    select(player, pos, team,  opp, #overUnder, spread,
           praOscore, praUscore,praSynth, line_pra, 
           btb, btbOpp, 
           ptsRank, rebRank, astRank, fg3mRank, stlRank, blkRank,fgPctRank, fg2PctRank, fg3PctRank,
           minAvg, minAvgL3, minAvgL10, praAvg, praAvgL3, praAvgL10, game_id) %>%
    arrange(desc(praOscore))
  #View(pra)
  colnames(pra) <- c("player", "pos", "team", "opp", "Oscore", "Uscore", "Synth", "line", "btb", "btbOpp", 
                     "oppPtsRank", "oppRebRank", "oppAstRank", "oppFg3mRank", "oppStlRank", "oppBlkRank", "oppFgPctRank", "oppFg2PctRank", "oppFg3PctRank",
                     "minAvg", "minAvgL3", "minAvgL10", "Avg", "AvgL3", "AvgL10", "game_id" )
  pra$prop = 'pra'
  
  pr <- harvest %>%
    #filter( (((prOscore > -1) & (line_pr > 3)) | (prOscore > 0)) | (((prUscore > -1) & (line_pr > 1)) | (prUscore > 0)) ) %>%
    select(player, pos, team, opp, #overUnder, spread,
           prOscore, prUscore, prSynth, line_pr,
           btb, btbOpp,
           ptsRank, rebRank, astRank, fg3mRank, stlRank, blkRank,fgPctRank, fg2PctRank, fg3PctRank,
           minAvg, minAvgL3, minAvgL10,  prAvg, prAvgL3, prAvgL10, game_id ) %>%
    arrange(desc(prOscore))
  #View(pr)
  colnames(pr) <- c("player", "pos", "team", "opp", "Oscore", "Uscore", "Synth", "line", "btb", "btbOpp",
                    "oppPtsRank", "oppRebRank", "oppAstRank", "oppFg3mRank", "oppStlRank", "oppBlkRank", "oppFgPctRank", "oppFg2PctRank", "oppFg3PctRank",
                    "minAvg", "minAvgL3", "minAvgL10", "Avg", "AvgL3", "AvgL10", "game_id" )
  pr$prop = 'pr'

  pa <- harvest %>%
    # filter( (((paOscore > -1) & (line_pa > 3)) | (paOscore > 0)) | (((paUscore > -1) & (line_pa > 1)) | (paUscore > 0))) %>%
    select(player, pos, team, opp, #overUnder, spread,
           paOscore, paUscore, paSynth, line_pa, btb, btbOpp,
           ptsRank, rebRank, astRank, fg3mRank, stlRank, blkRank,fgPctRank, fg2PctRank, fg3PctRank,
           minAvg, minAvgL3, minAvgL10,  paAvg, paAvgL3, paAvgL10,game_id) %>%
    arrange(desc(paOscore))
  #View(pa)
  colnames(pa) <- c("player", "pos", "team", "opp", "Oscore", "Uscore", "Synth", "line", "btb", "btbOpp",
                    "oppPtsRank", "oppRebRank", "oppAstRank", "oppFg3mRank", "oppStlRank", "oppBlkRank", "oppFgPctRank", "oppFg2PctRank", "oppFg3PctRank",
                    "minAvg", "minAvgL3", "minAvgL10", "Avg", "AvgL3", "AvgL10", "game_id" )
  pa$prop = 'pa'

  ra <- harvest %>%
    #filter( (((raOscore > -1) & (line_ra > 3)) | (raOscore > 0)) | (((raUscore > -1) & (line_ra > 1)) | (raUscore > 0)) ) %>%
    select(player, pos, team, opp, #overUnder, spread,
           raOscore, raUscore, raSynth, line_ra,btb, btbOpp,
           ptsRank, rebRank, astRank, fg3mRank, stlRank, blkRank,fgPctRank, fg2PctRank, fg3PctRank,
           minAvg, minAvgL3, minAvgL10,  raAvg, raAvgL3, raAvgL10, game_id
    ) %>%
    arrange(desc(raOscore))
  #View(ra)
  colnames(ra) <- c("player", "pos", "team", "opp", "Oscore", "Uscore", "Synth", "line", "btb", "btbOpp",
                    "oppPtsRank", "oppRebRank", "oppAstRank", "oppFg3mRank", "oppStlRank", "oppBlkRank", "oppFgPctRank", "oppFg2PctRank", "oppFg3PctRank",
                    "minAvg", "minAvgL3", "minAvgL10", "Avg", "AvgL3", "AvgL10", "game_id" )
  ra$prop = 'ra'
  
  # stls <- harvest %>% 
  #   #filter((stlOscore > 0.25) | (stlUscore > 0.25)) %>%
  #   select(player, pos, team, opp, #overUnder, spread,
  #          stlOscore, stlUscore, stlSynth, line_stl, 
  #          btb, btbOpp, 
  #          ptsRank, rebRank, astRank, fg3mRank, stlRank, blkRank,fgPctRank, fg2PctRank, fg3PctRank,
  #          minAvg, minAvgL3, minAvgL10,  stlAvg, stlAvgL3, stlAvgL10,  game_id) %>%
  #   arrange(desc(stlOscore))
  # #View(stls)
  # colnames(stls) <- c("player", "pos", "team", "opp", "Oscore", "Uscore", "Synth", "line", "btb", "btbOpp", 
  #                     "oppPtsRank", "oppRebRank", "oppAstRank", "oppFg3mRank", "oppStlRank", "oppBlkRank", "oppFgPctRank", "oppFg2PctRank", "oppFg3PctRank",
  #                     "minAvg", "minAvgL3", "minAvgL10", "Avg", "AvgL3", "AvgL10", "game_id" )
  # stls$prop = 'stl'
  # 
  # b <- harvest %>% 
  #   #filter((blkOscore > 0.25) | (blkUscore > 0.25)) %>%
  #   select(player, pos, team, opp, #overUnder, spread,
  #          blkOscore, blkUscore, blkSynth, line_blk, 
  #          btb, btbOpp, 
  #          ptsRank, rebRank, astRank, fg3mRank, stlRank, blkRank,fgPctRank, fg2PctRank, fg3PctRank,
  #          minAvg, minAvgL3, minAvgL10,  blkAvg, blkAvgL3, blkAvgL10, game_id ) %>%
  #   arrange(desc(blkOscore))
  # #View(b)
  # colnames(b) <- c("player", "pos", "team", "opp", "Oscore", "Uscore", "Synth", "line", "btb", "btbOpp", 
  #                  "oppPtsRank", "oppRebRank", "oppAstRank", "oppFg3mRank", "oppStlRank", "oppBlkRank", "oppFgPctRank", "oppFg2PctRank", "oppFg3PctRank",
  #                  "minAvg", "minAvgL3", "minAvgL10", "Avg", "AvgL3", "AvgL10", "game_id" )
  # b$prop = 'blk'
  # 
  # stocks <- harvest %>% 
  #   #filter( (sbOscore > 0.25) |  (sbUscore > 0.25)) %>%
  #   select(player, pos, team, opp,# overUnder, spread,
  #          sbOscore, sbUscore, sbSynth, line_sb, btb, btbOpp, 
  #          ptsRank, rebRank, astRank, fg3mRank, stlRank, blkRank,fgPctRank, fg2PctRank, fg3PctRank,
  #          minAvg, minAvgL3, minAvgL10,  sbAvg, sbAvgL3, sbAvgL10, game_id 
  #   ) %>%
  #   arrange(desc(sbOscore))
  # #View(stocks)
  # colnames(stocks) <- c("player", "pos", "team", "opp", "Oscore", "Uscore", "Synth", "line", "btb", "btbOpp", 
  #                       "oppPtsRank", "oppRebRank", "oppAstRank", "oppFg3mRank", "oppStlRank", "oppBlkRank", "oppFgPctRank", "oppFg2PctRank", "oppFg3PctRank",
  #                       "minAvg", "minAvgL3", "minAvgL10", "Avg", "AvgL3", "AvgL10", "game_id" )
  # stocks$prop = 'sb'
  
  scores <- rbind(p, r)
  scores <- rbind(scores, a)
  scores <- rbind(scores, tre)
  scores <- rbind(scores, pra)
  scores <- rbind(scores, pr)
  scores <- rbind(scores, pa)
  scores <- rbind(scores, ra) %>%
  # scores <- rbind(scores, stls)
  # scores <- rbind(scores, b)
  # scores <- rbind(scores, stocks) %>% 
    select(player, pos, team, opp, prop, line, Oscore, Uscore, Synth, btb, btbOpp, oppPtsRank, oppRebRank,
           oppAstRank, oppFg3mRank, oppStlRank, oppBlkRank, oppFgPctRank, oppFg2PctRank, oppFg3PctRank,
           minAvg, minAvgL3, minAvgL10, Avg, AvgL3, AvgL10, game_id) %>% 
    arrange(desc(Oscore))
  
  return(scores)
}
####################
