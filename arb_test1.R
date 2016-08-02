library(dplyr)
library(RSQLite)
library(tidyr)

Mode <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}


setwd('C:/Users/XPS/Dropbox/Osiris/Github/Data')

connection = dbConnect(SQLite(), dbname = "osiris.db")

pinnacle = dbGetQuery(connection,'Select * from odds_pinnacle')

betfair = dbGetQuery(connection,'Select * from odds_betfair')
betfair$market_id = as.character(betfair$market_id)


# Giving games an osiris_event_id
betfair_games = betfair %>%
  select(market_id, runner_name) %>%
  distinct(market_id, runner_name, .keep_all = TRUE)

betfair_games = betfair_games %>%
  mutate(team = rep(c("Team_1","Team_2","Team_3"), nrow(betfair_games) / 3)) %>%
  spread(team,runner_name) %>% mutate(osiris_id = NA)

pin_games = pinnacle %>%
  select(event_id, home_team, away_team) %>%
  distinct(event_id, home_team, away_team) %>% mutate(osiris_id = NA)

osid = 1

for (i in seq_along(betfair_games[,1])) {
  
  home_team = betfair_games[i,"Team_1"]
  away_team = betfair_games[i,"Team_2"]
  
  a = grep(home_team, pin_games$home_team, ignore.case = TRUE)
  b = grep(home_team, pin_games$away_team, ignore.case = TRUE) 
  c = grep(away_team, pin_games$home_team, ignore.case = TRUE)
  d = grep(away_team, pin_games$away_team, ignore.case = TRUE)
  
  if (length(c(a,b,c,d)) > 1) {
    bet_row = Mode(c(a,b,c,d))
    
    betfair_games[i,"osiris_id"] = osid
    pin_games[bet_row,"osiris_id"] = osid
    osid = osid + 1
  }
}

# Adding in the Osiris ID to the data sets

