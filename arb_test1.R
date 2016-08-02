library(dplyr)
library(RSQLite)
library(tidyr)
library(sqldf)

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
# Only keep games where we know we have a definite match
betfair_short = betfair %>%
  left_join(betfair_games, by = "market_id") %>% select(-Team_1,-Team_2,-Team_3) %>%
  filter(!is.na(osiris_id)) 

betfair_short = betfair_short %>%
  mutate(team = rep(c("Team_1","Team_2","Team_3"), nrow(betfair_short) / 3))%>%
  gather(market, value, available_to_back_price,available_to_back_market,available_to_lay_price,available_to_lay_market) %>%
  spread(team, runner) %>%
  unite(surrogate,team, market) %>%
  spread(surrogate, value)

pinnacle_short = pinnacle %>%
  left_join(pin_games, by = "event_id", copy=FALSE) %>%
  filter(!is.na(osiris_id))


# Now we join

joined = sqldf::sqldf(
"
SELECT
  a.log_time,
  b.league_name,
  a.gom
FROM
  betfair_short a
  INNER JOIN pinnacle_short b ON (
    a.osiris_id = b.osiris_id AND
    a.log_time = b.log_time)
                      
                      
                      ")





