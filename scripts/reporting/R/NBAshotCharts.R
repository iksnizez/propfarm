library(hoopR)
library(dplyr)
#library(tidyr)
library(ggplot2)
### couple of functions needed from here - tre.ball(x, y, shot), paint.bucket(x, y, shot), made.shot(shot, pts)
#source("scripts/functions/NBAdatacrackers.R")

####################
## FUNCTIONS FOR USE WITH PBP DATA
####################
###
get.shot.distance <- function(x, y, shot){
    #ingest shot coordinates and output distance from the center of the hoop
    arc.center.x <- 25
    arc.center.y <- 0.25 # the min y value is -5, the center of the hoop is 5.25ft from the base line
    
    ifelse(shot == TRUE, 
           sqrt((arc.center.x - x) ^ 2 + (arc.center.y - y) ^ 2),
           0
    )
}

fg2a.check <- function(x, y, shot, description) {
    # ingest the x, y coords of a shot and 
    # determine if it is a 3-point or 2-point attempt
    
    three.arc.center.x <- 25
    three.arc.center.y <- 0.25 # the min y value is -5, the center of the hoop is 5.25ft from the base line
    radius <- 23.75 # from the center of the hoop to the 3 line (except for x <=3 and x >= 47 but those are captured in the logic below)
    
    ifelse(
        (shot == FALSE) | ((x == -214748340) & (y == -214748365)) | (grepl("Free Throw", description)), FALSE,
        ifelse(
            # any x values that are true here are out side of the arc since there is 3 ft between the oob's and the 3pt line
            (x <= 3) | (x >= 47), FALSE,
            # calculates euclid. distance  sqrt((x2-x1)**2 + (y2-y1)**2) to see if it is far enough to qual as  3
            sqrt((three.arc.center.x - x) ^ 2 + (three.arc.center.y - y) ^ 2) < radius
        )
    )
}

fg3a.check <- function(x, y, shot, description) {
    # ingest the x, y coords of a shot and 
    # determine if it is a 3-point or 2-point attempt
    
    three.arc.center.x <- 25
    three.arc.center.y <- 0.25 # the min y value is -5, the center of the hoop is 5.25ft from the base line
    radius <- 23.75 # from the center of the hoop to the 3 line (except for x <=3 and x >= 47 but those are captured in the logic below)
    
    ifelse(
        (shot == FALSE) | ((x == -214748340) & (y == -214748365)) | (grepl("Free Throw", description)), FALSE,
        ifelse(
            # any x values that are true here are out side of the arc since there is 3 ft between the oob's and the 3pt line
            (x <= 3) | (x >= 47), TRUE,
            # calculates euclid. distance  sqrt((x2-x1)**2 + (y2-y1)**2) to see if it is far enough to qual as  3
            sqrt((three.arc.center.x - x) ^ 2 + (three.arc.center.y - y) ^ 2) > radius
        )
    )
}

in.the.paint.check <- function(x, y, shot, description){
    # ingest the x, y coords of a shot and 
    # determine if it was taken in the paint
    #### the data has the basket at y = 0 instead of it higher off the out of bounds
    #### paint height decreased to account for this
    ifelse(
        (shot == FALSE) | ((x == -214748340) & (y == -214748365)) | (grepl("Free Throw", description)), FALSE,
        ifelse(
            # court is 50 wide, paint is centered 50 - 16 = 34 / 2 = 17 ft on each side of the paint
            # coords X reference to 0 so 0-16 outside paint, 17-33 paint, 34 - 50 outside paint
            # coords y = 0 references the basket, y min = -5, court measures paint 19 ft out from baseline
            ( (x > 17) & (x < 34) ) & (y <= 19), TRUE, 
            FALSE
        )
    )
}

made.shot <- function(shot, pts){
    #ingest a boolean to determine a shot was taken, 
    #and a number to determine if pts were scored
    ifelse((shot == FALSE) | (pts <= 0), FALSE, TRUE)
}
######

##################
# IMPORT 
##################
pbp <- hoopR::load_nba_pbp() %>% 
    filter(
        team_id <= 32,
    )
#####


# ant = 4594268, curry = 3975, darius =4396907, wemby = 5104157 , gobert = 3032976
player_id  <-  4396907
##################
# PROCESS DATA 
##################
shots.all <- pbp %>% 
    filter(
        coordinate_x != -214748407,
        coordinate_x != 214748407,
        coordinate_y != -214748365,
        coordinate_y != 214748365,
        athlete_id_1 == player_id,
        shooting_play == TRUE
    ) %>% 
    select(
        game_id, game_play_number, sequence_number,game_date,  text,type_text, score_value, scoring_play, shooting_play, 
        coordinate_x, coordinate_y, coordinate_x_raw, coordinate_y_raw,
    ) %>% 
    mutate(
        shotDistance = get.shot.distance(coordinate_x_raw, coordinate_y_raw, shooting_play),
        madeShot = made.shot(
            shooting_play, score_value
        ),
        dunk = grepl("Dunk", type_text),
        paintAtt = in.the.paint.check(
            coordinate_x_raw, coordinate_y_raw, shooting_play, type_text
        ),
        FG2A = fg2a.check(
            coordinate_x_raw, coordinate_y_raw, shooting_play, type_text
        ),
        FG3A = fg3a.check(
            coordinate_x_raw, coordinate_y_raw, shooting_play, type_text
        ),
        FTA = grepl("Free Throw", type_text)
    )

shots.fga <- shots.all %>% 
    filter(
        FTA == FALSE
    )
shots.fgm <- shots.all %>% 
    filter(
        FTA == FALSE,
        madeShot == TRUE
    )

#nba shot data, x, y coords are in a different format that espn pbp
#also, need to pull by single player
## BUT IT DOES RETURN LEAGUE AVERAGES****************
#shot_data <- nba_shotchartdetail(
#    player_id = "201939",  # Example: Stephen Curry's Player ID
#    season = "2024-25"
#)

######


##################
# plot
##################
### court
# outer - 50ft wide x 94ft long
# mid court = 47ft
# middle of basket = 5.25ft from baseline
# 3pt line top of key - 23.75ft (from middle of basket)
# 3pt line corners - 22ft (from middle of basket)
# paint - 16ft wide x 19ft long (from baseline)
# free throw line - 19 ft from baseline, 15 feet from back board
# restricted area  = 4ft radius from middle of basket
# backboard width = 6ft
# rim width = 18 in


# not sure what these are assigned too yet, probably non-plays and missing coords?
# coords min x = -214748407  max x = 214748407 min y = -214748365  max y = 214748365  

### actual coord dims
# coordinate_x [-46.75, 46.75] and coordinate_y = [-25, 25]
# coordinate_x_raw [0, 50] and coordinate_y_raw = [-5, 85]
## it appears that _raw flips the axes 

# raw are the coordinates in ft oriented to the offensive players basket, standardizes for shot charts
# assuming that 0,0 right corner 3 at basket level


# basic scatter plot
#ggplot(shots.fga, aes(x=coordinate_x_raw, y=coordinate_y_raw, color=scoring_play)) + geom_point()
# pixelated
#ggplot(shots.fga, aes(x = coordinate_x_raw, y = coordinate_y_raw)) + stat_density_2d(aes(fill = ..density..), geom = "raster", contour = FALSE) + theme_minimal() + labs(title = "Shooting Heatmap", x = "X Coordinate", y = "Y Coordinate")

ggplot(shots.fga, aes(x = coordinate_x_raw, y = coordinate_y_raw)) +
    geom_density_2d_filled(alpha = 1)  +  
    scale_x_continuous(limits = c(0, 50)) + 
    scale_y_continuous(limits = c(-5, 50)) +
    #scale_fill_brewer(palette = "Oranges") +  # Orange-Red color scheme
    scale_fill_manual(values = colorRampPalette(c("white", "orangered", "darkred"))(15)) +
    theme_minimal() +
    labs(title = "FGA Heatmap", x = "X Coordinate", y = "Y Coordinate")

ggplot(shots.fgm, aes(x = coordinate_x_raw, y = coordinate_y_raw)) +
    geom_density_2d_filled(alpha = 1)  +  
    scale_x_continuous(limits = c(0, 50)) + 
    scale_y_continuous(limits = c(-5, 50)) +
    #scale_fill_brewer(palette = "Oranges") +  # Orange-Red color scheme
    scale_fill_manual(values = colorRampPalette(c("white", "orangered", "darkred"))(15)) +
    theme_minimal() +
    labs(title = "FGM Heatmap", x = "X Coordinate", y = "Y Coordinate")




######

library(RColorBrewer)
display.brewer.all()


##################
#
##################

######