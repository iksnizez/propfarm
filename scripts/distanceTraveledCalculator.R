library(googleway)
library(jsonlite)

#import key
path <- '../../Notes-General/apis.txt'
key <-readLines(path)
key <-lapply(key,fromJSON)
key <- key[[1]]$googlemaps

#import location data ***** requires lat and long coordinates ****** 
df <- read.csv('../../data/arenas.csv')
df
