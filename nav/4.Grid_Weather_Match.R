####################################### FIRST DEFINING FUNCTIONS
# MOHIT NEGI
# Last Updated : 7th May 2023
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


###################
# Now load in the weather data.
###################

# Just load in the 2022 data instead of the entire thing as we anyway just need a slice.
weather_data <- sf::read_sf('./Data/Cleaned/weathercleandata_2022.shp')
weather_data <- weather_data %>%
  mutate('Coors' = paste0(Lon, Lat)) %>% 
  group_by(Coors) %>% 
  mutate(pointID=cur_group_id()) %>% 
  arrange(pointID) %>% 
  select(-'Coors')

# Only a slice needed for nearest matching purposes.
weather_slice <- weather_data %>%
  filter(Day == '2022-01-01')

###################
# Now on to matching.
###################

# This function matches cells in a subgrid to the nearest weather point.
nearest_weather_point_assigner <- function(subgrid) {

  subgrid$nearest_weather_point <- st_nearest_feature(subgrid, weather_slice)
  #subgrid$distance <- st_distance(subgrid, weather_slice[subgrid$nearest_weather_point,], by_element=TRUE)
  # Removed distance for now, takes too long.

  return(subgrid)

}

# Now I'll parallelize the st_join process to make it faster.
cl <- makePSOCKcluster(detectCores()-1)
setDefaultCluster(cl)
clusterExport(NULL, c('nearest_weather_point_assigner', 'weather_slice'))
clusterEvalQ(cl, {
  library(sf)
})
#

n <- 2000
full_list_of_joins <- list()

i = 500
for(j in 1:4) {
  
  # Load in m1 or other grid.
  big_grid <- sf::read_sf(glue('./Data/Cleaned/medsea_grid_{i}m_m{j}.shp')) %>% 
    mutate('subgrid_id' = j, 'point_id' = 1:n(), 'unique_id' = paste(subgrid_id, point_id, sep = '.'))
  
  # Split into n parts.
  parts <- split(x = 1:nrow(big_grid), f = cut(1:nrow(big_grid), n))
  
  # Join with SOS data for every part separately.
  list_of_subgrids <- list()
  for(k in 1:n) {
    
    list_of_subgrids[[k]] <- big_grid[parts[[k]],]
    
  }
  
  # Remove from environment as no longer needed.
  rm(big_grid)
  
  list_of_joins <- parLapply(NULL, list_of_subgrids, nearest_weather_point_assigner)
  full_list_of_joins <- full_list_of_joins %>%
    append(list_of_joins)
  
}

# Finally, for each granularity, we get a final matched dataframe.
joined_df <- bind_rows(full_list_of_joins)

# Just keep the id matches, saves space.
just_matchings <- joined_df %>%
  st_drop_geometry %>% 
  select('unique_id', 'nearest_weather_point')

fwrite(just_matchings, glue('./Data/Cleaned/{i}mgrid_to_weather_IDmatches.csv'))

n <- 2000
full_list_of_joins <- list()

i = 1000
for(j in 1:4) {

  # Load in m1 or other grid.
  big_grid <- sf::read_sf(glue('./Data/Cleaned/medsea_geoms_{i}m_m{j}.shp')) %>% 
    mutate('subgrid_id' = j, 'point_id' = 1:n(), 'unique_id' = paste(subgrid_id, point_id, sep = '.'))
  
  # Split into n parts.
  parts <- split(x = 1:nrow(big_grid), f = cut(1:nrow(big_grid), n))

  # Join with SOS data for every part separately.
  list_of_subgrids <- list()
  for(k in 1:n) {
    
    list_of_subgrids[[k]] <- big_grid[parts[[k]],]
    
  }
  
  # Remove from environment as no longer needed.
  rm(big_grid)

  list_of_joins <- parLapply(NULL, list_of_subgrids, nearest_weather_point_assigner)
  full_list_of_joins <- full_list_of_joins %>%
    append(list_of_joins)

}

# Finally, for each granularity, we get a final matched dataframe.
joined_df <- bind_rows(full_list_of_joins)

# Just keep the id matches, saves space.
just_matchings <- joined_df %>%
  st_drop_geometry %>% 
  select('unique_id', 'nearest_weather_point')

fwrite(just_matchings, glue('./Data/Cleaned/{i}mgrid_to_weather_IDmatches.csv'))

n <- 2000
full_list_of_joins <- list()

i = 5000
for(j in 1:4) {
  
  # Load in m1 or other grid.
  big_grid <- sf::read_sf(glue('./Data/Cleaned/medsea_geoms_{i}m_m{j}.shp')) %>% 
    mutate('subgrid_id' = j, 'point_id' = 1:n(), 'unique_id' = paste(subgrid_id, point_id, sep = '.'))
  
  # Split into n parts.
  parts <- split(x = 1:nrow(big_grid), f = cut(1:nrow(big_grid), n))
  
  # Join with SOS data for every part separately.
  list_of_subgrids <- list()
  for(k in 1:n) {
    
    list_of_subgrids[[k]] <- big_grid[parts[[k]],]
    
  }
  
  # Remove from environment as no longer needed.
  rm(big_grid)
  
  list_of_joins <- parLapply(NULL, list_of_subgrids, nearest_weather_point_assigner)
  full_list_of_joins <- full_list_of_joins %>%
    append(list_of_joins)
  
}

# Finally, for each granularity, we get a final matched dataframe.
joined_df <- bind_rows(full_list_of_joins)

# Just keep the id matches, saves space.
just_matchings <- joined_df %>%
  st_drop_geometry %>% 
  select('unique_id', 'nearest_weather_point')

just_matchings$unique_id <- as.factor(just_matchings$unique_id)

fwrite(just_matchings, glue('./Data/Cleaned/{i}mgrid_to_weather_IDmatches.csv'))

stopCluster(cl)

#######################################

