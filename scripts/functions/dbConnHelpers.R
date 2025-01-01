library(DBI)
library(RMySQL)
library(jsonlite)
####################
## FUNCTION TO CONNECT TO DB
####################
harvestDBconnect <- function(league, path_override=NULL){
    # function to connect to harvest data base
    # input league to control the db access
    # outputs the db connection
    
    league <- tolower(league)
    
    #import credentials
    if(!is.null(path_override)){
        path <- path_override
    }
    else{
        path <- '../../Notes-General/config.txt' 
    }
    
    creds<-readLines(path)
    creds<-lapply(creds,fromJSON)
    
    #dbUser <- creds[[1]]$mysqlSurface$users[3]
    #dbPw <- creds[[1]]$mysqlSurface$creds$data
    dbUser <- creds[[1]]$mysqlSurface$users[2]
    dbPw <- creds[[1]]$mysqlSurface$creds$jb
    
    if(league == 'nba'){
        dbHost <- creds[[1]]$mysqlSurface$dbNBA$host
        dbName <- creds[[1]]$mysqlSurface$dbNBA$database
        dbPort <- creds[[1]]$mysqlSurface$dbNBA$port
    }
    if(league == 'wnba'){
        dbHost <- creds[[1]]$mysqlSurface$dbWNBA$host
        dbName <- creds[[1]]$mysqlSurface$dbWNBA$database
        dbPort <- creds[[1]]$mysqlSurface$dbWNBA$port
    }
    if(league == 'mlb'){
        dbHost <- creds[[1]]$mysqlSurface$dbMLB$host
        dbName <- creds[[1]]$mysqlSurface$dbMLB$database
        dbPort <- creds[[1]]$mysqlSurface$dbMLBA$port
    }
    if(league == 'nfl'){
        dbHost <- creds[[1]]$mysqlSurface$dbNFL$host
        dbName <- creds[[1]]$mysqlSurface$dbNFL$database
        dbPort <- creds[[1]]$mysqlSurface$dbNFL$port
    }
    if(league == 'nhl'){
        dbHost <- creds[[1]]$mysqlSurface$dbNHL$host
        dbName <- creds[[1]]$mysqlSurface$dbNHL$database
        dbPort <- creds[[1]]$mysqlSurface$dbNHL$port
    }

    #connect to MySQL db    
    conn = DBI::dbConnect(RMySQL::MySQL(),
                          dbname=dbName,
                          host=dbHost,
                          port=dbPort,
                          user=dbUser,
                          password=dbPw)
    return(conn)
}
#####

