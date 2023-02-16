library(hoopR)
library(dplyr)
library(tidyr)

####################
## FUNCTION TO UPDATE PROVIDED TEAM DATA
####################
update.default.team.data <- function(){
    # data to join with default data frame
    TeamID <- c("1610612741", "1610612744", "1610612766", "1610612762", "1610612750", "1610612749", "1610612752","1610612743", "1610612764", "1610612763", "1610612737", "1610612757", "1610612742", "1610612755", "1610612748", "1610612756","1610612751", "1610612747", "1610612745", "1610612738", "1610612758", "1610612761", "1610612759", "1610612739","1610612753", "1610612746", "1610612754", "1610612740", "1610612760", "1610612765")
    teamId_hoopr <- c(4,9,30,26,16,15,18,7,27,29,1,22,6,20,14,21,17,13,10,2,23,28,24,5,19,12,11,3,25,8)
    team_abb <- c("CHI","GS","CHA","UTAH","MIN","MIL","NY","DEN","WSH","MEM","ATL","POR","DAL","PHI","MIA","PHX","BKN","LAL","HOU","BOS","SAC","TOR","SA","CLE","ORL","LAC","IND","NO","OKC","DET")
    teamName_nba <- c("Chicago","Golden State","Charlotte","Utah","Minnesota","Milwaukee","New York","Denver","Washington","Memphis","Atlanta","Portland","Dallas","Philadelphia","Miami","Phoenix","Brooklyn","L.A. Lakers","Houston","Boston","Sacramento","Toronto","San Antonio","Cleveland","Orlando","LA Clippers","Indiana","New Orleans","Oklahoma City","Detroit")
    to_join <- cbind(teamId_hoopr, team_abb)
    to_join <- cbind(to_join, teamName_nba)
    to_join <- cbind(to_join, TeamID)
    
    # joining the 2 data frames
    updated <- merge(data.frame(hoopR::nba_teams), data.frame(to_join), by="TeamID")
    
    
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
propfarming <- function(box.score.data, team.ids, matchups.today){
    # ingest season boxscore data from load_nba_player_box and vector of team ids and dataframe of home/away teams
    # output filtered list of players and their prop farm stat data for today
    
    #convert team ids to player ids in order to filter box scores
    # using team ids doesn't capture player data when they have been traded midseason
    all.pids <- box.score.data %>% 
        filter(team_id %in% team.ids)
    pids <- unique(c(all.pids$athlete_id))

    players.today <- box.score.data %>% 
        filter(athlete_id %in% pids) %>% 
        arrange(athlete_id, team_id,  desc(game_date))

    # breaking the shooting attempts from makes and converting to numbers
    players.today <-  players.today %>%
        tidyr::separate(fg, sep = "-", into = c("fgm","fga")) %>%
        tidyr::separate(fg3, sep = "-", into = c("fg3m","fg3a")) %>%
        tidyr::separate(ft, sep = "-", into = c("ftm","fta")) %>%
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
      
    #adding the player name back to the data
    df <- merge(players.today %>% 
                    select(athlete_id, athlete_display_name, athlete_position_abbreviation, team_abbreviation) %>% 
                    group_by(athlete_id)%>%
                    filter(row_number()==1), 
                df, 
                by = "athlete_id",
                all.x = TRUE)
    
    # filtering data to players with >=20 avg. minutes in the last 10 games
    # adding:
        #back-to-back flag 
        #opponent
    df <- df %>%
        filter(minAvgL10 >= 20) %>%
        rowwise() %>%
        mutate(game_id = ifelse(
                            team_abbreviation %in% matchups.today$home_team_abb, 
                            matchups.today[matchups.today$home_team_abb == team_abbreviation, "game_id"][[1]],
                            matchups.today[matchups.today$away_team_abb == team_abbreviation, "game_id"][[1]]
                        ),
               btb = case_when((team_abbreviation %in% back.to.back.first) ~ 1,
                               (team_abbreviation %in% back.to.back.last) ~ 2,
                               TRUE ~ 0
                     ),
                opp = ifelse(team_abbreviation %in% matchups.today$home_team_abb,
                             (matchups.today %>% filter(home_team_abb == team_abbreviation) %>% select(away_team_abb))[[1]],
                             (matchups.today %>% filter(away_team_abb == team_abbreviation) %>% select(home_team_abb))[[1]]
                      ),
                btbOpp = case_when((opp %in% back.to.back.first) ~ 1,
                                   (opp %in% back.to.back.last) ~ 2,
                                   TRUE ~ 0
                ),
                ptsSynth = synth.avg(ptsAvg, ptsAvgL3, 3, gp),
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
        )
    
    return(df)    
}

#####

####################
# SPLIT AND SUM BOX SCORES
####################
split.sum.box.scores <- function(box.scores, gids, t= NULL, min = 0){
    
    split.summed <- box.scores %>%
        filter(game_id %in% gids,
               team_abbreviation != t,
               min > 0) %>% 
        tidyr::separate(fg, sep = "-", into = c("fgm","fga")) %>%
        tidyr::separate(fg3, sep = "-", into = c("fg3m","fg3a")) %>%
        tidyr::separate(ft, sep = "-", into = c("ftm","fta")) %>%
        group_by(game_id, team_abbreviation, athlete_position_abbreviation) %>%
        summarize(fgmCount = sum(as.numeric(fgm)),
                  fgaCount = sum(as.numeric(fga)),
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
        )%>%
        mutate(team= t)
    
    return(split.summed)
}

#####

####################
## FUNCTION TO RETRIEVE TEAM OPPONENT RANKS FROM LAST N games
####################
opp.stats.last.n.games  <- function(season, num.game.lookback=15, box.scores=NULL, schedule=NULL){
    # ingest season year, number of games (15 default), data frame of player boxscores,
    # and data frame of the season schedule
    # output dataframe for teams stats allowed to opponents, dataframe for the stats grouped by
    # team and position, and a data frame for each position grouped by team and ranked
    
    # checks if box scores dataframe is provided and pulls box scores data if not
    if(is.null(box.scores)){
        box.scores <- hoopR::load_nba_player_box(seasons = season)
    }
    
    # checks if schedule dataframe is provided and pulls schedule data if not
    if(is.null(schedule)){
        schedule <- hoopR::load_nba_schedule(seasons = season)
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
            grouped <- split.sum.box.scores(box.scores, gids, t=t, min=0)
        }
        else{
            #retrieve gid boxscores and split att/made, aggregating stats by team, position
            temp <- split.sum.box.scores(box.scores, gids, t=t, min=0)
            grouped <- rbind(grouped, temp)
        }
    }
    
    # groups the agg stats by team for totals over n games
    stats.team.opp.total <- grouped %>%
        group_by(
            team, 
            athlete_position_abbreviation
        ) %>%
        summarize(
            fgmCount = sum(fgmCount),
            fgaCount = sum(fgaCount),
            fg3mCount = sum(fg3mCount),
            fg3aCount = sum(fg3aCount),
            ftmCount = sum(ftmCount),
            ftaCount = sum(ftaCount),
            orebCount = sum(orebCount),
            drebCount = sum(drebCount),
            rebCount = sum(rebCount),
            astCount = sum(astCount),
            stlCount = sum(stlCount),
            blkCount = sum(blkCount),
            toCount = sum(toCount),
            pfCount = sum(pfCount),
            ptsCount = sum(ptsCount)
        )
    
    # creates dataframes for each position to easily create ranks for each  position and stat
    positions <- c("PG", "SG", "SF", "PF", "C")
    for(pos in positions){
        var.name <- pos
        assign(var.name, stats.team.opp.total %>% 
                   filter(athlete_position_abbreviation == pos) %>%
                   ungroup() %>% 
                   mutate(fgmRank = rank(fgmCount),
                          fgaRank = rank(fgaCount),
                          fg3mRank = rank(fg3mCount),
                          fg3aRank = rank(fg3aCount),
                          ftmRank = rank(ftmCount),
                          ftaRank = rank(ftaCount),
                          orebRank = rank(orebCount),
                          drebRank = rank(drebCount),
                          rebRank = rank(rebCount),
                          astRank = rank(astCount),
                          stlRank = rank(stlCount),
                          blkRank = rank(blkCount),
                          toRank = rank(toCount),
                          ptsRank = rank(ptsCount)
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
    output <- list(grouped, position.ranks, stats.team.opp.total)
    names(output) <- c("game.opp.stats.by.pos", "team.opp.stats.by.pos", "team.opp.stats.by.all.pos")
    
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
                           athlete_display_name == stats_player_name) %>%
                    tidyr::separate(fg, sep = "-", into = c("fgm","fga")) %>%
                    tidyr::separate(fg3, sep = "-", into = c("fg3m","fg3a")) %>%
                    tidyr::separate(ft, sep = "-", into = c("ftm","fta"))
    
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
player.missed.games.stats <- function(team_abb, missed_player_names, stats_player_name, 
                                      box_scores=NULL, schedule=NULL, season){
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
    
    # this will hold the  games played for the player(s) we are interest in their misses
    games.played <- c()
    
    # looping through each player to return their missed game ids
    for (player in missed_player_names){
        #filtering box scores to the player of interest
        games.player <- box_scores %>%
            mutate(athlete_display_name = tolower(athlete_display_name)) %>%
            filter(athlete_display_name == player) %>%
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
player.avg.vs.opp <- function(opp, players, box_scores_current=NULL, box_scores_prev=NULL){
  # ingest opp team abbreviation, vector of player names, box scores from current and previous seasons
  # output players avgs against teams
  
  current.season <- hoopR::most_recent_nba_season()
  if(is.null(box_scores_current)){
    box_scores_current <- hoopR::load_nba_player_box(seasons = current.season) %>% 
                              tidyr::separate(fg, sep = "-", into = c("fgm","fga")) %>%
                              tidyr::separate(fg3, sep = "-", into = c("fg3m","fg3a")) %>%
                              tidyr::separate(ft, sep = "-", into = c("ftm","fta"))
  }
  
  if(is.null(box_scores_prev)){
    box_scores_prev <- hoopR::load_nba_player_box(seasons = current.season - 1) %>% 
      tidyr::separate(fg, sep = "-", into = c("fgm","fga")) %>%
      tidyr::separate(fg3, sep = "-", into = c("fg3m","fg3a")) %>%
      tidyr::separate(ft, sep = "-", into = c("ftm","fta"))
  }
  
  box_scores_current <- box_scores_current %>% 
    tidyr::separate(fg, sep = "-", into = c("fgm","fga")) %>%
    tidyr::separate(fg3, sep = "-", into = c("fg3m","fg3a")) %>%
    tidyr::separate(ft, sep = "-", into = c("ftm","fta"))
  box_scores_prev <- box_scores_prev %>%
    tidyr::separate(fg, sep = "-", into = c("fgm","fga")) %>%
    tidyr::separate(fg3, sep = "-", into = c("fg3m","fg3a")) %>%
    tidyr::separate(ft, sep = "-", into = c("ftm","fta"))
  
  opp <- tolower(opp)
  players <- tolower(players)
  
  for(i in 1:length(players)){
    if(i == 1){
      avgs <- box_scores_current %>%
        mutate(athlete_display_name = tolower(athlete_display_name)) %>%
        filter(athlete_display_name == player) %>%
        select(game_id)
    }
    else{
      
    }
  }
  return(avgs)
}

#####
