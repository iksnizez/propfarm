library(dplyr)
library(rmarkdown)

########################
####
#### generate all the data here and then loop through it and pass each matchup to the render to create 
#### and save the outpud
###  variables to populate: outputFileNames, ffenses, defenses, game.date, nGamesLookback
########################




# loop through matchups to render the pdfs
for (game in games){
    
    rmarkdown::render(
        input = "NBAoppRanksReport.Rmd",
        output_format = "html_document",
        output_file = outputFileNames,
        output_dir = "..\\..\\output",
        params = list(
            offTeam = offenses,
            defTeam = defenses, 
            gameDate = game.date,
            lnGames: nGamesLookback,
            show_code: FALSE
        )
    )
}