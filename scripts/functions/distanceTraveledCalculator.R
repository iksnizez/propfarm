library(googleway)
library(jsonlite)
library(dplyr)
#remotes::install_github('sportsdataverse/hoopR')
library(hoopR)

#import key
path <- '../../Notes-General/apis.txt'
key <-readLines(path)
key <-lapply(key,fromJSON)
key <- key[[1]]$googlemaps
googleway::set_key(key = key)


#import location data ***** requires lat and long coordinates ****** 
df <- read.csv('data/arenas.csv')
df <- df %>% 
        select(team, lat, long)


# create empty  matrices
nplaces <- nrow(df)
dist_mi_matrix <- matrix(0, nrow = nplaces, ncol = nplaces)
dist_me_matrix <- matrix(0, nrow = nplaces, ncol = nplaces)
duration_matrix <- matrix(0, nrow = nplaces, ncol = nplaces)

# calculate distance matrix
for (i in 1:nplaces){
    # origin lat and long
    oLat = df[i,]$lat
    oLon = df[i,]$long
    origin <- c(lat = oLat, lon = oLon)

    # loop through each destination for the origin.
    ###### THIS COULD BE OPTIMIZED BY PUTTING IN THE LAT/LONGS for EACH DESTINATION INTO 
    ###### A SINGLE FUNCTION CALL. THE DISTANCES FOR EACH DESTINATION WOULD BE RETURNED AT ONCE
    for (j in 1:nplaces){
        if (i != j){
            # destination lat and long
            dLat = df[j,]$lat
            dLon = df[j,]$long
            destination <- c(lat = dLat, lon = dLon)
            
            # retrieve the driving distance data from google
            data <- googleway::google_distance(
                     origin = origin,
                     destination = destination,
                     mode = 'driving',
                     units='imperial',
                     key = key
                 )

            dist_mi_matrix[i, j] <- sub( ",", "",strsplit(data$rows$elements[[1]]$distance$text, " ")[[1]][1]) # miles
            dist_me_matrix[i, j] <- data$rows$elements[[1]]$distance$value # meters
            duration_matrix[i, j] <- data$rows$elements[[1]]$duration$value # seconds
        }
    }
}


# adding names back to rows and columns and saving to csv
dfMiles <- as.data.frame(dist_mi_matrix)
colnames(dfMiles) <-  df$team
dfMiles$origin <-  df$team
dfMiles$type <- 'mi'

#write.csv(dfMiles, 'data/arenaDistanceMatrix.csv',row.names = FALSE)


#######
# I manually updated a file from team name to team hooprID for row and col names
######

# import file with team ids. 
dist.matrix <- read.csv('data\\arenaDistanceMatrix_teamIds nba.csv', header=TRUE)

# update data frame so row name and cols are the ids.
teamIds <- dist.matrix$team
row.names(dist.matrix) <- teamIds
dist.matrix <- dist.matrix %>% 
                    select(!team)
colnames(dist.matrix) <- teamIds

# import season schedule
### ESPN function pulls in games by calendar year, not season
season.schedule <- hoopR::nba_schedule() %>% 
                        filter(season_type_id != 1)

# in-season tourney TBD window
tbd.start.date <- '2023-12-04'
tbd.end.date <- '2023-12-09'

#################################################################################
#################################################################################
filter.date <- tbd.end.date
#################################################################################

# vector to hold each teams current location - order matches the order of teams in distance matrix
current_location <- teamIds

########## THIS CAN INITATED TO 0 as a single number in the loop, reseting for each new team iter
#####vector to hold number of days on the road
####current_days_from_home <- rep(0, length(teamIds))
cols <- c('date', 'gid', 'team', 'opp', 'cumDist', 'home', 'distOnRoad', 'daysOnRoad')
travel.logs <- data.frame(matrix(ncol = length(cols), nrow = 0))
colnames(travel.logs) <- cols

# loop through each team to calculate the miles and days traveled at each game
for (i in 1:length(teamIds)){
    team <- teamIds[i]
    
    # tracker for current number of days away from home
    current.days.away <- 0
    current.mi.traveled <- 0
    dist.traveled <- 0
    
    # filter schedule to team games 
    team.season.schedule <- season.schedule %>% 
                                filter((home_team_id == team | away_team_id == team) & (game_date < filter.date))
    
    # loop through each game for the current team
    for (g in 1:nrow(team.season.schedule)){
        # extract relevant data from the games row in the schedule
        game.date <- team.season.schedule$game_date[g]
        gid <- team.season.schedule$game_id[g]
        home.team <- team.season.schedule$home_team_id[g]
        away.team <- team.season.schedule$away_team_id[g]
        
        # first game of the season handling for the team of interest
        if (g == 1){
            # team of interest = home
            if (team == home.team){
                # first game for the home team has no travel
                opp  <-  away.team
                home <- 1
            }
            # team of interest = away
            else {
                opp = home.team
                home <- 0
                # get the row index for the team of interest
                row.index <- which(rownames(dist.matrix) == team)
                
                # use the index to return the distance traveled to the opp in this game
                dist <- dist.matrix[row.index,  as.character(home.team)]
                dist.traveled <- dist.traveled + dist
                current.mi.traveled <- current.mi.traveled + dist
                
                # days on the road = 1 for the first game if it is away
                current.days.away <- current.days.away + 1
            }
        }
        # all games after the first 
        else{
            # the team of interests previous game home team
            prev.game.home.team <- team.season.schedule$home_team_id[g - 1]
            
            # team of interest = home after the first game of the season
            if (team == home.team){
                opp  <-  away.team
                home <- 1
                # check if previous game was also a home
                if(team == prev.game.home.team){
                    # no dist when playing home after home, days on the road reset to 0
                    dist.traveled <- dist.traveled + 0
                    current.days.away <- 0
                    current.mi.traveled <- 0
                }
                # if this is the first home game back from the road
                else{
                    # prev game was away, need to return distance from the away game back home to add to cumDist
                    # get the row index for the previous game arena
                    row.index <- which(rownames(dist.matrix) == prev.game.home.team)
                    
                    # use the index to return the distance traveled from previous game back home
                    dist <- dist.matrix[row.index,  as.character(home.team)]
                    dist.traveled <- dist.traveled + dist
                    
                    # calculate the days from the last game to the current game and add it to current days away
                    ###############################################################################################
                    ##################################### THIS CURRENTLY INCLUDES TRAVEL FROM AWAY TO HOME AS DAYS ON THE ROAD
                    ##################################### FOR THE HOME GAME. SHOULD IT DAYS ON THE ROAD = 0 FOR THE FIRST HOME
                    ##################################### GAME BACK UNLESS FIRST BACK IS ON THE END OF A BACK TO BACK?????
                    ###############################################################################################
                    prev.game.date <- team.season.schedule$game_date[g - 1]
                    prev.to.current.days <- game.date - prev.game.date
                    # if first home game back is the end of a back to back then mark game with road trip + 1 day
                    if(prev.to.current.days == 1){
                        current.days.away <-  current.days.away + prev.to.current.days
                        current.mi.traveled <- current.mi.traveled + dist
                        
                    }
                    # at least one day between last away game and current home game resets the days on the road
                    else{
                        current.days.away <- 0
                        current.mi.traveled <- 0
                        
                    }
                }
            }
            # team of interest = away, after the first game of season
            else {
                opp <- home.team
                home <- 0
                ## calculate distance from the team of interest last game - use the home team from that game
                # get the row index for the previous game arena
                row.index <- which(rownames(dist.matrix) == prev.game.home.team)
                
                # use the index to return the distance traveled from previous game back home
                dist <- dist.matrix[row.index,  as.character(home.team)]
                dist.traveled <- dist.traveled + dist
                
                # add the days on the road to the tally
                # if going home to away then days on road = 1
                if(team == prev.game.home.team){
                    current.days.away <- 1
                    current.mi.traveled <- dist
                    
                }
                # if going away to away then calc based on calendar
                else{
                    prev.game.date <- team.season.schedule$game_date[g - 1]
                    prev.to.current.days <- game.date - prev.game.date
                    current.days.away <- current.days.away + prev.to.current.days
                    current.mi.traveled <- current.mi.traveled + dist
                }
            }
        }
        
        # add the game data to the main dataframe
        travel.logs <- rbind(travel.logs, data.frame(
            date = game.date,
            gid = gid,
            team = team,
            opp = opp,
            cumDist = dist.traveled,
            home = home,
            distOnRoad = current.mi.traveled,
            daysOnRoad = current.days.away
        ))
    }
}

View(travel.logs)

write.csv(travel.logs, paste0("data/travellogs",filter.date, ".csv"), row.names = FALSE)





