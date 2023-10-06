#######################################
# MOHIT NEGI
# Last Updated : 17th May 2023
# Contact on : mohit.negi@studbocconi.it
####################################### FIRST DEFINING FUNCTIONS


################### LIBRARIES
library(tidyverse)
library(stringr)
library(stringi)
library(lubridate)
library(sp)
library(sf)
library(data.table)
library(tmap)
library(concaveman)
library(stars)
library(parallel)
library(data.table)
library(glue)
###################


################### PARAMETERS THAT NEED TO BE SET FOR THE ALGO.
granularity <- 5000 # Can be changed to 500, 1000 or 5000 (units are meters)
periods <- 'weeks' # Can be changed to 'weeks' or 'months' (can choose weeks so that we have weekly CSVs when months is too big for R to handle in a single dataframe)
###################

############################################ CHANGEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE
prd <- seq(as.Date('2012-01-01'), as.Date('2022-12-31'), by = periods)
prd2 <- (prd[2:length(prd)])-1
prd2[length(prd)] <- as.Date('2023-01-01')

intervals <- data.frame(cbind(prd, prd2))
intervals$start <- as.Date(prd)
intervals$end <- as.Date(prd2)
# This will be used later to filter weather data to keep only chunks of it at a time.

# Component 1 : Inter-grid (Medsea grid and weather grid) matchings
inter_grid_matching <- fread(glue('./Data/Cleaned/{granularity}mgrid_to_weather_IDmatches.csv'),
                             colClasses=c(unique_id="character"))

inter_grid_matching$unique_id <- as.character(inter_grid_matching$unique_id)
head(inter_grid_matching)
colnames(inter_grid_matching)

# Component 2 : weather data for all the years, they are split up in different CSVs.
csv_2020 <- fread('./Data/Cleaned/weathercleandata_2020-22.csv') %>% 
  mutate('Coors' = paste0(Lon, Lat)) %>% 
  group_by(Coors) %>% 
  mutate(pointID=cur_group_id()) %>% 
  ungroup() %>% 
  select(-c('Coors', 'Lon', 'Lat'))

csv_2018 <- fread('./Data/Cleaned/weathercleandata_2018-20.csv') %>% 
  mutate('Coors' = paste0(Lon, Lat)) %>% 
  group_by(Coors) %>% 
  mutate(pointID=cur_group_id()) %>% 
  ungroup() %>% 
  select(-c('Coors', 'Lon', 'Lat'))

csv_2016 <- fread('./Data/Cleaned/weathercleandata_2016-18.csv') %>% 
  mutate('Coors' = paste0(Lon, Lat)) %>% 
  group_by(Coors) %>% 
  mutate(pointID=cur_group_id()) %>% 
  ungroup() %>% 
  select(-c('Coors', 'Lon', 'Lat'))

csv_2014 <- fread('./Data/Cleaned/weathercleandata_2014-16.csv') %>% 
  mutate('Coors' = paste0(Lon, Lat)) %>% 
  group_by(Coors) %>% 
  mutate(pointID=cur_group_id()) %>% 
  ungroup() %>% 
  select(-c('Coors', 'Lon', 'Lat'))

csv_2012 <- fread('./Data/Cleaned/weathercleandata_2012-14.csv') %>% 
  mutate('Coors' = paste0(Lon, Lat)) %>% 
  group_by(Coors) %>% 
  mutate(pointID=cur_group_id()) %>% 
  ungroup() %>% 
  select(-c('Coors', 'Lon', 'Lat'))

csv_2022 <- fread('./Data/Cleaned/weathercleandata_2022.csv')
colnames(csv_2022) <- colnames(csv_2020)

# Component 3 : SOS warnings, matched with Medsea 500/1000/5000m grid.
SOS_data <- sf::read_sf(glue('./Output/warnings_all_years_with_{granularity}m_medgrid.shp'))
colnames(SOS_data)

# Want only some columns and don't want geometries.
SOS_data_relevant <- SOS_data %>% 
  select('m_id', 'o_id', 'i_date', 'unique_id') %>% 
  st_drop_geometry() %>% 
  mutate('Day' = floor_date(ymd_hms(i_date), unit = 'day'))
head(SOS_data_relevant)

# Collapsing it to get counts.
SOS_counts <- SOS_data_relevant %>% 
  group_by(Day, unique_id) %>% 
  summarise(SOScount = n(), .groups = 'keep')
SOS_counts$Day <- as.Date(SOS_counts$Day)

# Don't need these anymore.
rm(SOS_data)
rm(SOS_data_relevant)

# Final components look like this,
colnames(inter_grid_matching)
colnames(csv_2018)
colnames(SOS_counts)

# This function extracts the relevant time period from the separate files. It is very flexible
# so that a specific interval can extend in multiple files too if needed.
month_filter <- function(start, end) {
  
  df_of_interest1 <- csv_2012 %>% 
    filter(Day >= start & Day <= end)
  df_of_interest2 <- csv_2014 %>% 
    filter(Day >= start & Day <= end)
  df_of_interest3 <- csv_2016 %>% 
    filter(Day >= start & Day <= end)
  df_of_interest4 <- csv_2018 %>% 
    filter(Day >= start & Day <= end)
  df_of_interest5 <- csv_2020 %>% 
    filter(Day >= start & Day <= end)
  df_of_interest6 <- csv_2022 %>% 
    filter(Day >= start & Day <= end)
  
  df_of_interest <- rbind(df_of_interest1,
                          df_of_interest2,
                          df_of_interest3,
                          df_of_interest4,
                          df_of_interest5,
                          df_of_interest6)
  
  return(df_of_interest)
  
}

# This function does the bulk of the work. It takes a subset of the grid cells (matched to weather points)
# and matches them to the relevant SOS count and weather data.
matcher <- function(sub_intergrid) {
  
  days <- unique(weather_subdf$Day)
  
  # Replicate the set of grid cells again and again since we have multiple days of data.
  expanded_subdf <- sub_intergrid %>% 
    slice(rep(row_number(), length(days)))
  
  day_column <- days %>% 
    rep(nrow(sub_intergrid)) %>% 
    sort()
  
  expanded_subdf$Day <- day_column
  
  # Match with SOS count data using the day and the Medsea grid cell.
  grid_SOS_match <- data.table::merge.data.table(expanded_subdf, SOS_counts, 
                                                 by = c('Day', 'unique_id'),
                                                 all.x = TRUE)
  
  grid_SOS_match <- rename(grid_SOS_match, 'pointID' = 'nearest_weather_point')
  
  # Further match with weather data using day and the weather point ID corresponding to the Medsea grid cell.
  grid_SOS_weather_match <- data.table::merge.data.table(grid_SOS_match, weather_subdf, 
                                                         by = c('Day', 'pointID'),
                                                         all.x = TRUE)
  return(grid_SOS_weather_match)
  
}

n <- 2000
# Split into n parts.
parts <- split(x = 1:nrow(inter_grid_matching), f = cut(1:nrow(inter_grid_matching), n))

# We will apply matcher separately to these parts of the big dataset. This is done to make it compatible with
# parLapply.
list_of_gridpoint_matches <- list()
for(k in 1:n) {
  
  list_of_gridpoint_matches[[k]] <- inter_grid_matching[parts[[k]],]
  
}

# Remove from environment as no longer needed.
rm(inter_grid_matching)


# If picking up after stopping mid-loop, start here!
completed_periods <- readLines(glue('./Output/Final_Datasets/meters_{granularity}/{periods}/completed_periods.txt'))
completed_periods <- completed_periods[completed_periods != '']

intervals_left = intervals[!(as.character(intervals$start) %in% completed_periods),]

# Now go week/month wise.
for(i in 1:nrow(intervals_left)) {
  
  print(glue('\n\n\n\n================================ {granularity}m grid - {periods} : {intervals_left$start[i]} {intervals_left$end[i]} ================================'))
  
  # Store name of start date.
  prd_id <- intervals_left$start[i]
  
  # Extract the relevant weather data only.
  weather_subdf <- month_filter(intervals_left$start[i], intervals_left$end[i])
  weather_subdf$Day <- as.Date(weather_subdf$Day)
  
  # Now I'll parallelize the matcher process to make it faster. Works like a charm for 5000 and 1000m.
  # Some error was popping up for 500m, not sure if its because of my machine being too weak or not.
  # Define cluster
  cl <- makePSOCKcluster(detectCores()-1)
  setDefaultCluster(cl)
  clusterExport(NULL, c('matcher', 'weather_subdf', 'SOS_counts'))
  clusterEvalQ(cl, {
    library(dplyr, data.table)
  })
  #
  
  # This takes a while to run.
  list_of_matched_subdfs <- parLapply(NULL, list_of_gridpoint_matches, matcher)
  
  # This will also take some time.
  full_monthly_df <- bind_rows(list_of_matched_subdfs) %>% 
    arrange(unique_id)
  
  # Finally save as CSV in relevant folder, then repeat for next interval.
  fwrite(full_monthly_df, glue('./Output/Final_Datasets/meters_{granularity}/{periods}/final_matched_dataset_{prd_id}.csv'))
  
  cat(
    glue('{prd_id}\r'), 
    file = glue('./Output/Final_Datasets/meters_{granularity}/{periods}/completed_periods.txt'), 
    append = TRUE
  )
  
  stopCluster(cl)
  
}
