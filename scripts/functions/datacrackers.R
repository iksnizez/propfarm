library(hoopR)
library(dplyr)
library(tidyr)
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
## FUNCTIONS TO AGG. OPPONENT STATS FOR TEAMS FROM ESPN BOX SCORES
####################
##
opponent.stat.agg <- function(gid){
    #retrieve gid boxscore
    one.box <- hoopR::espn_nba_player_box(gid)  %>% 
        tidyr::separate(fg, sep = "-", into = c("fgm","fga")) %>%
        tidyr::separate(fg3, sep = "-", into = c("fg3m","fg3a")) %>%
        tidyr::separate(ft, sep = "-", into = c("ftm","fta"))
    #retrieve team names
    uni <- one.box$team_abbreviation %>% unique()
    # aggregating stats by team, position
    grouped <- one.box %>%
        filter(min > 0) %>%
        group_by(team_abbreviation, athlete_position_abbreviation) %>%
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
        ) %>%
        mutate(opp = setdiff(uni, team_abbreviation))
    return(grouped)
}
#####

####################
## FUNCTION TO CALC SYNTHETIC AVG
####################
synth.avg <- function(season_avg, n_avg, n_games, games_played){
    # calculating the avg weights for the recent n games and season avg
    n.avg.wt <- (min(1-(n_games / games_played), 0.6))
    season.avg.wt <- (1 - n.avg.wt)
    # calculating the wt. synthetic avg
    sythetic.avg <- (n_avg * n.avg.wt) + (season_avg * season.avg.wt)
    
    return(sythetic.avg)
}
#####

####################
## FUNCTION TO CALCULATE PROPFARM PLAYER STATS FROM NBA BOX
####################
# filtering entire season box score to only the teams playing today
propfarming <- function(box.score.data, team.ids, matchups.today){
    # ingest season boxscore data from load_nba_player_box and vector of team ids and dataframe of home/away teams
    # output filtered list of players and their prop farm stat data for today
    players.today <- box.score.data %>% 
        filter(team_id %in% team.ids) %>% 
        arrange(team_id, athlete_id, desc(game_date))
    
    # breaking the shooting attempts from makes
    players.today <-  players.today %>%
        tidyr::separate(fg, sep = "-", into = c("fgm","fga")) %>%
        tidyr::separate(fg3, sep = "-", into = c("fg3m","fg3a")) %>%
        tidyr::separate(ft, sep = "-", into = c("ftm","fta"))
    
    # converting data to numeric for calculations
    players.today$pts <-  as.numeric(as.character(players.today$pts))
    players.today$reb <-  as.numeric(as.character(players.today$reb))
    players.today$ast  <-  as.numeric(as.character(players.today$ast))
    players.today$stl  <-  as.numeric(as.character(players.today$stl))
    players.today$blk <-  as.numeric(as.character(players.today$blk))
    players.today$min <-  as.numeric(as.character(players.today$min))
    players.today$fgm <-  as.numeric(as.character(players.today$fgm))
    players.today$fga <-  as.numeric(as.character(players.today$fga))
    players.today$fg3m <-  as.numeric(as.character(players.today$fg3m))
    players.today$fg3a <-  as.numeric(as.character(players.today$fg3a))
    players.today$ftm <-  as.numeric(as.character(players.today$ftm))
    players.today$fta <-  as.numeric(as.character(players.today$fta))
    players.today$to <-  as.numeric(as.character(players.today$to))
    players.today$pf <-  as.numeric(as.character(players.today$pf))
    players.today$pra <- players.today$pts + players.today$ast + players.today$reb
    players.today$pr <- players.today$pts + players.today$reb
    players.today$pa <- players.today$pts + players.today$ast
    players.today$ra <- players.today$ast + players.today$reb
    players.today$sb <- players.today$stl + players.today$blk
    
    # calculating player season averages and standard dev
    players.today.season.avgs <- players.today %>%
        select(athlete_id,min, pts, reb, ast, stl, blk, fg3m, pra, pr, pa, ra, sb) %>%
        group_by(athlete_id) %>%
        summarize(gp = n(),
                  minAvg = mean(min, na.rm=TRUE),
                  ptsAvg = mean(pts, na.rm=TRUE),
                  rebAvg = mean(reb, na.rm=TRUE),
                  astAvg = mean(ast, na.rm=TRUE),
                  stlAvg = mean(stl, na.rm=TRUE),
                  blkAvg = mean(blk, na.rm=TRUE),
                  fg3mAvg = mean(fg3m, na.rm=TRUE),
                  praAvg = mean(pra, na.rm=TRUE),
                  prAvg = mean(pr, na.rm=TRUE),
                  paAvg = mean(pa, na.rm=TRUE),
                  raAvg = mean(ra, na.rm=TRUE),
                  sbAvg = mean(sb, na.rm=TRUE),
                  minstd = sd(min, na.rm=TRUE),
                  ptsStd = sd(pts, na.rm=TRUE),
                  rebStd = sd(reb, na.rm=TRUE),
                  astStd = sd(ast, na.rm=TRUE),
                  stlStd = sd(stl, na.rm=TRUE),
                  blkStd = sd(blk, na.rm=TRUE),
                  fg3mStd = sd(fg3m, na.rm=TRUE),
                  praStd = sd(pra, na.rm=TRUE),
                  prStd = sd(pr, na.rm=TRUE),
                  paStd = sd(pa, na.rm=TRUE),
                  raStd = sd(ra, na.rm=TRUE),
                  sbStd = sd(sb, na.rm=TRUE)
        )
    
    # adding the 3 game averages and standard deviations
    players.today.l3.avgs <- players.today %>%
        select(athlete_id,min, pts, reb, ast, stl, blk, fg3m, pra, pr, pa, ra, sb) %>%
        group_by(athlete_id) %>%
        filter(row_number()<=3) %>%
        summarize(minAvgL3 = mean(min, na.rm=TRUE),
                  ptsAvgL3 = mean(pts, na.rm=TRUE),
                  rebAvgL3 = mean(reb, na.rm=TRUE),
                  astAvgL3 = mean(ast, na.rm=TRUE),
                  stlAvgL3 = mean(stl, na.rm=TRUE),
                  blkAvgL3 = mean(blk, na.rm=TRUE),
                  fg3mAvgL3 = mean(fg3m, na.rm=TRUE),
                  praAvgL3 = mean(pra, na.rm=TRUE),
                  prAvgL3 = mean(pr, na.rm=TRUE),
                  paAvgL3 = mean(pa, na.rm=TRUE),
                  raAvgL3 = mean(ra, na.rm=TRUE),
                  sbAvgL3 = mean(sb, na.rm=TRUE),
                  minStdL3 = sd(min, na.rm=TRUE),
                  ptsStdL3 = sd(pts, na.rm=TRUE),
                  rebStdL3 = sd(reb, na.rm=TRUE),
                  astStdL3 = sd(ast, na.rm=TRUE),
                  stlStdL3 = sd(stl, na.rm=TRUE),
                  blkStdL3 = sd(blk, na.rm=TRUE),
                  fg3mStdL3 = sd(fg3m, na.rm=TRUE),
                  praStdL3 = sd(pra, na.rm=TRUE),
                  prStdL3 = sd(pr, na.rm=TRUE),
                  paStdL3 = sd(pa, na.rm=TRUE),
                  raStdL3 = sd(ra, na.rm=TRUE),
                  sbStdL3 = sd(sb, na.rm=TRUE)
        )
    
    # adding last 3  Standard dev ranges.
    #players.today.l3.avgs$ptsL3Upper = players.today.l3.avgs$ptsAvgL3 + players.today.l3.avgs$ptsStdL3
    #players.today.l3.avgs$ptsL3Lower = players.today.l3.avgs$ptsAvgL3 - players.today.l3.avgs$ptsStdL3
    #players.today.l3.avgs$rebL3Upper = players.today.l3.avgs$rebAvgL3 + players.today.l3.avgs$rebStdL3
    #players.today.l3.avgs$rebL3Lower = players.today.l3.avgs$rebAvgL3 - players.today.l3.avgs$rebStdL3
    #players.today.l3.avgs$astL3Upper = players.today.l3.avgs$astAvgL3 + players.today.l3.avgs$astStdL3
    #players.today.l3.avgs$astL3Lower = players.today.l3.avgs$astAvgL3 - players.today.l3.avgs$astStdL3
    #players.today.l3.avgs$stlL3Upper = players.today.l3.avgs$stlAvgL3 + players.today.l3.avgs$stlStdL3
    #players.today.l3.avgs$stlL3Lower = players.today.l3.avgs$stlAvgL3 - players.today.l3.avgs$stlStdL3
    #players.today.l3.avgs$blkL3Upper = players.today.l3.avgs$blkAvgL3 + players.today.l3.avgs$blkStdL3
    #players.today.l3.avgs$blkL3Lower = players.today.l3.avgs$blkAvgL3 - players.today.l3.avgs$blkStdL3
    #players.today.l3.avgs$fg3mL3Upper = players.today.l3.avgs$fg3mAvgL3 + players.today.l3.avgs$fg3mStdL3
    #players.today.l3.avgs$fg3mL3Lower = players.today.l3.avgs$fg3mAvgL3 - players.today.l3.avgs$fg3mStdL3
    #players.today.l3.avgs$praL3Upper = players.today.l3.avgs$praAvgL3 + players.today.l3.avgs$praStdL3
    #players.today.l3.avgs$praL3Lower = players.today.l3.avgs$praAvgL3 - players.today.l3.avgs$praStdL3
    #players.today.l3.avgs$prL3Upper = players.today.l3.avgs$prAvgL3 + players.today.l3.avgs$prStdL3
    #players.today.l3.avgs$prL3Lower = players.today.l3.avgs$prAvgL3 - players.today.l3.avgs$prStdL3
    #players.today.l3.avgs$paL3Upper = players.today.l3.avgs$paAvgL3 + players.today.l3.avgs$paStdL3
    #players.today.l3.avgs$paL3Lower = players.today.l3.avgs$paAvgL3 - players.today.l3.avgs$paStdL3
    #players.today.l3.avgs$raL3Upper = players.today.l3.avgs$raAvgL3 + players.today.l3.avgs$raStdL3
    #players.today.l3.avgs$raL3Lower = players.today.l3.avgs$raAvgL3 - players.today.l3.avgs$raStdL3
    #players.today.l3.avgs$sbL3Upper = players.today.l3.avgs$sbAvgL3 + players.today.l3.avgs$sbStdL3
    #players.today.l3.avgs$sbL3Lower = players.today.l3.avgs$sbAvgL3 - players.today.l3.avgs$sbStdL3
    
    # adding the 10 game averages and standard deviations
    players.today.l10.avgs <- players.today %>%
        select(athlete_id,min, pts, reb, ast, stl, blk, fg3m, pra, pr, pa, ra, sb) %>%
        group_by(athlete_id) %>%
        filter(row_number()<=10) %>%
        summarize(minAvgL10 = mean(min, na.rm=TRUE),
                  ptsAvgL10 = mean(pts, na.rm=TRUE),
                  rebAvgL10 = mean(reb, na.rm=TRUE),
                  astAvgL10 = mean(ast, na.rm=TRUE),
                  stlAvgL10 = mean(stl, na.rm=TRUE),
                  blkAvgL10 = mean(blk, na.rm=TRUE),
                  fg3mAvgL10 = mean(fg3m, na.rm=TRUE),
                  praAvgL10 = mean(pra, na.rm=TRUE),
                  prAvgL10 = mean(pr, na.rm=TRUE),
                  paAvgL10 = mean(pa, na.rm=TRUE),
                  raAvgL10 = mean(ra, na.rm=TRUE),
                  sbAvgL10 = mean(sb, na.rm=TRUE),
                  minStdL10 = sd(min, na.rm=TRUE),
                  ptsStdL10 = sd(pts, na.rm=TRUE),
                  rebStdL10 = sd(reb, na.rm=TRUE),
                  astStdL10 = sd(ast, na.rm=TRUE),
                  stlStdL10 = sd(stl, na.rm=TRUE),
                  blkStdL10 = sd(blk, na.rm=TRUE),
                  fg3mStdL10 = sd(fg3m, na.rm=TRUE),
                  praStdL10 = sd(pra, na.rm=TRUE),
                  prStdL10 = sd(pr, na.rm=TRUE),
                  paStdL10 = sd(pa, na.rm=TRUE),
                  raStdL10 = sd(ra, na.rm=TRUE),
                  sbStdL10 = sd(sb, na.rm=TRUE)
        )
    
    # adding last 10 Standard dev ranges.
    #players.today.l10.avgs$ptsL10Upper = players.today.l10.avgs$ptsAvgL10 + players.today.l10.avgs$ptsStdL10
    #players.today.l10.avgs$ptsL10Lower = players.today.l10.avgs$ptsAvgL10 - players.today.l10.avgs$ptsStdL10
    #players.today.l10.avgs$rebL10Upper = players.today.l10.avgs$rebAvgL10 + players.today.l10.avgs$rebStdL10
    #players.today.l10.avgs$rebL10Lower = players.today.l10.avgs$rebAvgL10 - players.today.l10.avgs$rebStdL10
    #players.today.l10.avgs$astL10Upper = players.today.l10.avgs$astAvgL10 + players.today.l10.avgs$astStdL10
    #players.today.l10.avgs$astL10Lower = players.today.l10.avgs$astAvgL10 - players.today.l10.avgs$astStdL10
    #players.today.l10.avgs$stlL10Upper = players.today.l10.avgs$stlAvgL10 + players.today.l10.avgs$stlStdL10
    #players.today.l10.avgs$stlL10Lower = players.today.l10.avgs$stlAvgL10 - players.today.l10.avgs$stlStdL10
    #players.today.l10.avgs$blkL10Upper = players.today.l10.avgs$blkAvgL10 + players.today.l10.avgs$blkStdL10
    #players.today.l10.avgs$blkL10Lower = players.today.l10.avgs$blkAvgL10 - players.today.l10.avgs$blkStdL10
    #players.today.l10.avgs$fg3mL10Upper = players.today.l10.avgs$fg3mAvgL10 + players.today.l10.avgs$fg3mStdL10
    #players.today.l10.avgs$fg3mL10Lower = players.today.l10.avgs$fg3mAvgL10 - players.today.l10.avgs$fg3mStdL10
    #players.today.l10.avgs$praL10Upper = players.today.l10.avgs$praAvgL10 + players.today.l10.avgs$praStdL10
    #players.today.l10.avgs$praL10Lower = players.today.l10.avgs$praAvgL10 - players.today.l10.avgs$praStdL10
    #players.today.l10.avgs$prL10Upper = players.today.l10.avgs$prAvgL10 + players.today.l10.avgs$prStdL10
    #players.today.l10.avgs$prL10Lower = players.today.l10.avgs$prAvgL10 - players.today.l10.avgs$prStdL10
    #players.today.l10.avgs$paL10Upper = players.today.l10.avgs$paAvgL10 + players.today.l10.avgs$paStdL10
    #players.today.l10.avgs$paL10Lower = players.today.l10.avgs$paAvgL10 - players.today.l10.avgs$paStdL10
    #players.today.l10.avgs$raL10Upper = players.today.l10.avgs$raAvgL10 + players.today.l10.avgs$raStdL10
    #players.today.l10.avgs$raL10Lower = players.today.l10.avgs$raAvgL10 - players.today.l10.avgs$raStdL10
    #players.today.l10.avgs$sbL10Upper = players.today.l10.avgs$sbAvgL10 + players.today.l10.avgs$sbStdL10
    #players.today.l10.avgs$sbL10Lower = players.today.l10.avgs$sbAvgL10 - players.today.l10.avgs$sbStdL10
    
    #merging the season avg data with the last 3 avg data and last 10 avg
    df <- merge(players.today.season.avgs, players.today.l3.avgs, by = "athlete_id")
    df <- merge(df, players.today.l10.avgs, by = "athlete_id")
    #adding the player name back to the data
    df <- merge(players.today %>% 
                    select(athlete_id, athlete_display_name, team_abbreviation) %>% 
                    group_by(athlete_id)%>%
                    filter(row_number()==1), 
                df, 
                by = "athlete_id",
                all.x = TRUE)
    
    # filtering data to players with >=21 avg. minutes in the last 10 games
    # adding:
        #back-to-back flag 
        #opponent
    df <- df %>%
        filter(minAvgL10 >= 21) %>%
        rowwise() %>%
        mutate(btb = ifelse((team_abbreviation %in% back.to.back.first) | (team_abbreviation %in% back.to.back.last),
                            1,
                            0
        ),
        opp = ifelse(team_abbreviation %in% matchups.today$home_team_abb,
                     (matchups.today %>% filter(home_team_abb == team_abbreviation) %>% select(away_team_abb))[[1]],
                     (matchups.today %>% filter(away_team_abb == team_abbreviation) %>% select(home_team_abb))[[1]]
        )
        )
    
    return(df)    
}

#####

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
## FUNCTION TO RETRIEVE TEAM OPPONENT RANKS FROM LAST 15 games
####################
# date info
last.n.games <- function(n, season){
    
    #load schedule
    schedule <- hoopR::load_nba_schedule(seasons = season)
    # schedule data for only games played
    completed.games <- schedule %>% filter(status_type_completed == TRUE)
    # gather team abbreviations
    teams <- append(completed.games$home_abbreviation %>% unique(), 
                    completed.games$away_abbreviation %>% unique()
    ) %>%
        unique()
    # looping through team abbreviations to get 15 gids for every team
    gids <- c()
    for(t in teams){
        temp.gids <- completed.games %>%
            filter(home_abbreviation == t | away_abbreviation == t) %>%
            arrange(desc(date)) %>%
            slice(1:n) %>%
            select(id)
        gids <- append(gids, temp.gids[["id"]])
    }
    # last 15 gids from all teams 
    last.15.gids <- gids %>% unique() 
    
    #using first gid in list to create the main df that will be appended in the loop below
    gid.first <- last.15.gids[1]
    stats.team.opp <- opponent.stat.agg(gid.first)
    
    #remove 1st gid from list so it doesn't loop
    gids <- last.15.gids[!last.15.gids %in% c(gid.first)]
    
    #looping through remaining games to build dataframe
    for(gid in gids){
        temp.team.stats <-  opponent.stat.agg(gid)
        stats.team.opp <- rbind(stats.team.opp, temp.team.stats)
    }
    
    stats.team.opp.total <- stats.team.opp %>%
        group_by(
            opp, 
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
    all <- rbind(PG, SG)
    all <- rbind(all, SF)
    all <- rbind(all, PF)
    all <- rbind(all, C)
    return(all)
}


#####


###############
# retrieving a player's missed games and calculating player avg for those games
###############
############################### ID LIKE TO ADD FUNCTIONALITY TO SHOW THE % CHANGE FOR THE PLAYER
############################### WITHOUT THE PLAYER PLAYING VS WITH THE PLAYER PLAYING

player.missed.games.stats <- function(team_abb, missed_player_names, stats_player_name, season){
    ## ingest team, names of players who you want to see missed games for,
    ## the name of a single player you want to see how they performed with the other
    ## players missing and season
    ## return a list of the games  that the players missed, 
    ## the stats for the other player in those games,
    ## the the avg stats for those games
    ## comparison of the stats in the games the players missed vs the games present
    
    # grab season schedule
    sched <- hoopR::load_nba_schedule(seasons = season)
    
    #convert text inputs to lower case for better filtering
    team_abb <- tolower(team_abb)
    missed_player_names <- tolower(missed_player_names)
    stats_player_name <- tolower(stats_player_name)
    
    # return the team's completed games for the season
    games.team <- sched %>%
        mutate(home_abbreviation = tolower(home_abbreviation),
               away_abbreviation = tolower(away_abbreviation)
        ) %>%
        filter((home_abbreviation == team_abb | away_abbreviation == team_abb),
               status_type_completed == TRUE
        )%>%
        select(id)
    
    # retrieve all individual boxscores for the season
    box.scores <- hoopR::load_nba_player_box(seasons = season)
    
    # this will hold the  games played for the player(s) we are interest in their misses
    games.played <- c()
    
    # looping through each player to return their missed game ids
    for (player in missed_player_names){
        #filtering box scores to the player of interest
        games.player <- box.scores %>%
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
        # storing the first game in the list for loop navigation
        first.game <- missed.games[1]
        
        # looping through missed games to agg stats for the player of interest
        for(g in missed.games){
            # create the main data frame on the first loop that will receiving additional game data
            if(g == first.game){
                box.games <- hoopR::espn_nba_player_box(g) %>%
                    mutate(athlete_display_name = tolower(athlete_display_name)) %>%
                    filter(athlete_display_name == stats_player_name)
            }
            else{
                # create temp data frame appended to the main for every new game after the first
                temp.game <- hoopR::espn_nba_player_box(g) %>%
                    mutate(athlete_display_name = tolower(athlete_display_name)) %>%
                    filter(athlete_display_name == stats_player_name)
                
                box.games <- rbind(box.games, temp.game)
            }
        }
        
        avg <- box.games %>% 
            tidyr::separate(fg, sep = "-", into = c("fgm","fga")) %>%
            tidyr::separate(fg3, sep = "-", into = c("fg3m","fg3a")) %>%
            tidyr::separate(ft, sep = "-", into = c("ftm","fta")) %>%  
            group_by(athlete_display_name) %>%
            summarize(
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
        
        #####  LOOP TO REMOVE IF CHANGING TO FULL SEASON AVG #######
        # storing the first game in the list for loop navigation
        first.game.2 <- games.played[1]
        
        # looping through missed games to agg stats for the player of interest
        for(g in games.played){
            # create the main data frame on the first loop that will receiving additional game data
            if(g == first.game.2){
                season.avgs <- hoopR::espn_nba_player_box(g) %>%
                    mutate(athlete_display_name = tolower(athlete_display_name)) %>%
                    filter(athlete_display_name == stats_player_name)
            }
            else{
                # create temp data frame appended to the main for every new game after the first
                temp.game <- hoopR::espn_nba_player_box(g) %>%
                    mutate(athlete_display_name = tolower(athlete_display_name)) %>%
                    filter(athlete_display_name == stats_player_name)
                
                season.avgs <- rbind(season.avgs, temp.game)
            }
        }        
        
        
        # gathering the stat players season avgs
        #season.avgs <- box.scores %>%   ###### UNCOMMENT THIS LINE AND REMOVE LOOP RIGHT ABOVE IF YOU WANT AVG DIFF TO BE CALCULATED ON SEASONS STATS AND NOT ONLY WHEN MISSING PLAYERS ACTIVE STATS
        season.avgs <- season.avgs %>%
            mutate(athlete_display_name = tolower(athlete_display_name)) %>%
            filter(athlete_display_name == stats_player_name) %>%
            tidyr::separate(fg, sep = "-", into = c("fgm","fga")) %>%
            tidyr::separate(fg3, sep = "-", into = c("fg3m","fg3a")) %>%
            tidyr::separate(ft, sep = "-", into = c("ftm","fta")) %>%  
            group_by(athlete_display_name) %>%
            summarize(
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
        

        # combining the avg when players are missing and full season
        avg.change <- rbind(avg, season.avgs)
        
        #caclulting the difference between the 2 avgs
        avg.change <- data.frame(avg.change[1,-1] - avg.change[2,-1])
        avg.change$athlete_display_name <- stats_player_name
        
    }
    
    # game ids for the missed games, box scores for the POI when other players missed
    # POI avgs in those games, the difference in avg for POI when players missing vs when they are active
    return(list(missed.games, box.games, avg, avg.change))
}
#####

