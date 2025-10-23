library(dplyr)
library(tidyr)
library(baseballr)

##### import player ids that will be added to the baseballR player table #####
import.missing.player.ids <- function(file_name='SFBB Player ID Map.csv') {
    # this will update the player name/id table. 
    # it checks a file with the missing IDs
    # and downloads it if it is not present
    file_path <- paste('data/', file_name, sep="")
    
    if (!file.exists(file_path)) {
        # if file doesn't exist, download the file from the website 
        players <- read.csv('https://www.smartfantasybaseball.com/PLAYERIDMAPCSV') %>%
                        filter(PLAYERNAME != 'Last Player')
    } else {
        # if file exists, access data fro csv
        players <- read.csv(file_path) %>%
            filter(PLAYERNAME != 'Last Player') 
    }
    return(players)
}
##### << #####

##### add additional team abbreviations used throughout the baseballR functions #####
#adding the mlb probables team abbrev. to team table
update.teams.lu.table <- function(){
    # these team abbreviations are used in the mlb_probables function but are not present in the lookup table
    additional.abbrev <- data.frame(
        id = c(109,144,110,111,112,145,113,114,115,116,117,118,108,119,146,158,142,121,147,133,143,134,135,137,136,138,139,140,141,120),
        abb = c('ARI','ATL','BAL','BOS','CHC','CHW','CIN','CLE','COL','DET','HOU','KCR','LAA','LAD','MIA', 'MIL','MIN','NYM','NYY','OAK','PHI','PIT','SDP','SFG','SEA','STL','TBR','TEX','TOR','WSN')
    )
    # adding the abbreviations to the look up table.
    teams_lu_table <- teams_lu_table %>% 
        filter(sport.id == 1) %>%
        merge(additional.abbrev, by='id')
    
    return(teams_lu_table)
} 
##### << #####

##### gathering data on probable starts and creating a df with all info #####

# build df from looping through mlb_probables for all games today
gather.probable.starters <- function(df_today_games, fangraph_ids, season, 
                                     pitcher_qual=0, pitcherType="sta", ind=0
                                     ){
    # ingest df of game data, dataframe of player mlbID and fgId, season year, 
    # ind >> 1 = split seasons, 0 = aggregate seasons.
    # pitcher_type = "pit", "sta", "rel".
    # qual = IP for pitchers
    
    #pull current date game data if no df provided
    if(is.null(df_today_games)){
        df.today.games <- mlb_game_pks(Sys.Date())
        today.game.ids <- df.today.games %>% 
            select(game_pk) %>% 
            unique()
        
    }
    else{
        today.game.ids <- df_today_games %>% 
            select(game_pk) %>% 
            unique()
    }
    
    raw.p.leaders <- fg_pitcher_leaders(x=season, y=season, 
                                        qual=pitcher_qual, 
                                        ind=ind, 
                                        pitcher_type = pitcherType
    )
    # average the 2 data sources for Fangraphs on plate discipline
    # add ranks for select stats
    stats.p.leaders <- raw.p.leaders %>%
        mutate(
            swing_p = (Swing_pct + Swing_pct_pi) / 2,
            zSwing_p = (`Z-Swing_pct` + `Z-Swing_pct_pi`) / 2,
            oSwing_p = (`O-Swing_pct` + `O-Swing_pct_pi`) / 2,
            contact_p = (Contact_pct + Contact_pct_pi) / 2,
            zContact_p = (`Z-Contact_pct` + `Z-Contact_pct_pi`) / 2,
            oContact_p = (`O-Contact_pct` + `O-Contact_pct_pi`) / 2,
            zone_p = (Zone_pct + Zone_pct_pi) / 2,
            fK_p = `F-Strike_pct`,
            whiff_p = 100 - contact_p,
            r_era = dense_rank(desc(ERA)),
            r_hr = dense_rank(desc(HR)),
            r_so = dense_rank(desc(SO)),
            r_k_9 = dense_rank(desc(K_9)),
            r_k_p = dense_rank(desc(K_pct)),
            r_bb_p = dense_rank(desc(BB_pct)),
            r_whiff_p = dense_rank(desc(whiff_p)),
            r_swing_p = dense_rank(desc(swing_p))
        ) %>%
        select(playerid, Name, Team,
               GS, IP, ERA, HR,
               SO, K_9, K_pct, BB_pct,  
               swing_p, zSwing_p, oSwing_p,
               contact_p, zContact_p, oContact_p,
               zone_p, fK_p, 
               whiff_p,
               r_hr,
               r_so, r_k_9, r_k_p, r_bb_p, 
               r_whiff_p, r_swing_p
        )
    
    # aggregate the probable starters for today usings the game ids
    for(i in today.game.ids[[1]]){
        if(i == today.game.ids[[1]][1]){
            data.pitchers <- mlb_probables(i)
        } 
        else{
            temp <- mlb_probables(i)
            data.pitchers <- bind_rows(data.pitchers, temp)
        }
    }
    
    # adding fangraphs ids to the df
    data.pitchers <- data.pitchers %>%
        merge(map.players[, c('IDFANGRAPHS', 'MLBID')], by.x = 'id', by.y = 'MLBID')
    
    # filter down to only the starters going on the search date and add throwing arm
    today.pitchers <- stats.p.leaders %>% 
        filter(playerid %in% data.pitchers$IDFANGRAPHS ) %>%
        merge(map.players[, c('IDFANGRAPHS', 'THROWS')], by.x = 'playerid', by.y = 'IDFANGRAPHS')
    
    # adding in team id, oppId, game_pk
    today.pitchers <- today.pitchers %>%
        rowwise() %>%
        mutate(
            teamId = teams_lu_table[teams_lu_table$abb == Team, 'id'],
            gameId = ifelse(teamId %in% df_today_games$teams.home.team.id | 
                                teamId %in% df_today_games$teams.away.team.id,
                            (df_today_games %>% 
                                 filter(teamId == df_today_games$teams.home.team.id | 
                                            teamId == df_today_games$teams.away.team.id
                                 ) %>%
                                 select(game_pk)
                            )[[1]],
                            NULL
            ),
            oppId = ifelse(teamId %in% df_today_games$teams.home.team.id,
                           (df_today_games %>% filter(teams.home.team.id == teamId) %>% select(teams.away.team.id))[[1]],
                           (df_today_games %>% filter(teams.away.team.id == teamId) %>% select(teams.home.team.id))[[1]]
            ),
            opp = teams_lu_table[teams_lu_table$id == oppId, 'abb']
        ) 
    ###### WANT TO ADD OPP BATTING RANKINGS FOR ##############
    ## HR, SO, SO%, Whiff% -  OVERALL and vs HANDEDNESS TO today.pitcher
    
    ##### 
    return(today.pitchers)
}

##### << #####

