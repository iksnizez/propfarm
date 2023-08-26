library(DBI)
library(RMySQL)
library(jsonlite)
####################
## FUNCTION TO CONNECT TO DB
####################
harvestDBconnect <- function(league){
    # function to connect to harvest data base
    # input league to control the db access
    # outputs the db connection
    
    #import credentials
    path <- '../../Notes-General/config.txt'
    creds<-readLines(path)
    creds<-lapply(creds,fromJSON)
    
    #dbUser <- creds[[1]]$mysqlSurface$users[3]
    #dbPw <- creds[[1]]$mysqlSurface$creds$data
    dbUser <- creds[[1]]$mysqlSurface$users[2]
    dbPw <- creds[[1]]$mysqlSurface$creds$jb
    dbHost <- creds[[1]]$mysqlSurface$dbWNBA$host
    dbName <- creds[[1]]$mysqlSurface$dbWNBA$database
    dbPort <- creds[[1]]$mysqlSurface$dbWNBA$port
    
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

