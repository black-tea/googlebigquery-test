# load libraries
library(here)
library(bigrquery)
library(DBI)
library(httr)
library(dplyr)

# Google BigQuery Settings: token, billing, project, data
set_service_token(here('nextbus-la-159f7085d190.json'))
billing <- 'nextbus-la' # this will also be 'project' when querying own data
project <- 'nextbus-la'
dataset <- 'nextbus'

# BigQuery DB Connection
conn <- dbConnect(
  bigrquery::bigquery(),
  billing = billing,
  project = project,
  dataset = dataset
)

# current UTC time
utcTime <- format(Sys.time(), usetz=TRUE, tz='UTC')

# Metro GET request, format as df 
req <- GET("http://api.metro.net/agencies/lametro/vehicles/")
jsonRespParsed <- content(req, as='parsed')
vehicles <- jsonRespParsed$items %>%
  bind_rows %>%
  mutate(time = utcTime) %>% # add current time
  mutate(id = as.integer(id)) %>%
  mutate(route_id = as.integer(route_id)) %>%
  mutate(heading = as.integer(heading)) %>%
  select(id, run_id, route_id, predictable, seconds_since_report, heading, longitude, latitude, time)

# Append data to BigQuery table
bigrquery::dbWriteTable(
  con = conn,
  name = 'nbrealtime',
  row.names = FALSE,
  value = vehicles,
  #field.types = c('INTEGER', 'STRING', 'INTEGER', 'BOOLEAN', 'INTEGER', 'FLOAT', 'FLOAT', 'TIMESTAMP'),
  append = TRUE
)

