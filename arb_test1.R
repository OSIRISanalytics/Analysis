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
  left_join(betfair_games, by = "market_id") %>% select(-price_selection_id,-Team_1,-Team_2,-Team_3) %>%
  filter(!is.na(osiris_id)) 

betfair_short = betfair_short %>%
  mutate(team = rep(c("Team_1","Team_2","Team_3"), nrow(betfair_short) / 3))%>%
  gather(market, value, runner_name,available_to_back_price,available_to_back_market,available_to_lay_price,available_to_lay_market) %>%
  unite(surrogate,team, market) %>%
  spread(surrogate, value) %>%
  select(log_time,osiris_id, Team_1_runner_name,Team_2_runner_name,Team_3_runner_name,Team_1_available_to_back_price,Team_2_available_to_back_price,Team_3_available_to_back_price)

pinnacle_short = pinnacle %>%
  left_join(pin_games, by = "event_id", copy=FALSE) %>%
  filter(!is.na(osiris_id), market_type == 'moneyline') %>%
  select(log_time, osiris_id, home_team.x, away_team.x, market_value, market_odds) %>%
  mutate(runner_name = ifelse(market_value==1,home_team.x, ifelse(market_value==0,"The Draw",away_team.x)),
         team = ifelse(market_value==1,"Team_1", ifelse(market_value==0,"Team_3","Team_2"))) %>%
  select(-market_value,-home_team.x,-away_team.x) %>% gather(var,value,runner_name,market_odds) %>%
  unite(surrogate, team, var) %>%
  spread(surrogate, value)


# Now we join

joined = sqldf::sqldf(
"
SELECT
  a.log_time,
  a.osiris_id,
  a.Team_1_runner_name as home_team,
  a.Team_2_runner_name as away_team,
  a.Team_1_available_to_back_price as betfair_home,
  a.Team_2_available_to_back_price as betfair_away,
  a.Team_3_available_to_back_price as betfair_draw,
  b.Team_1_market_odds as pin_home,
  b.Team_2_market_odds as pin_away,
  b.Team_3_market_odds as pin_draw
FROM
  betfair_short a
  INNER JOIN pinnacle_short b ON (
    a.osiris_id = b.osiris_id AND
    a.log_time = b.log_time)")

#Apply commision
joined$betfair_home = (1+(as.numeric(joined$betfair_home)-1)*.95)
joined$betfair_away = (1+(as.numeric(joined$betfair_away)-1)*.95)
joined$betfair_draw = (1+(as.numeric(joined$betfair_draw)-1)*.95)

joined$pin_home = as.numeric(joined$pin_home)
joined$pin_away = as.numeric(joined$pin_away)
joined$pin_draw = as.numeric(joined$pin_draw)

joined_fil = subset(joined, pin_home >=1)

joined_fil$hmax = ifelse(joined_fil$betfair_home > joined_fil$pin_home, joined_fil$betfair_home,joined_fil$pin_home)
joined_fil$amax =  ifelse(joined_fil$betfair_away > joined_fil$pin_away, joined_fil$betfair_away,joined_fil$pin_away)
joined_fil$dmax =  ifelse(joined_fil$betfair_draw > joined_fil$pin_draw, joined_fil$betfair_draw,joined_fil$pin_draw)

joined_fil$arb = 1 / joined_fil$hmax + 1/joined_fil$amax+ 1/joined_fil$dmax
