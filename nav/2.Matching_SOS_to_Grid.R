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
###################


####################################### Now match with a Mediterranean Sea grid.

# The gridder function takes a larger grid and subdivides into much smaller ones. How small?
# The granularity argument determines that.
gridder <- function(geom, granularity) {

   # Constructing a smaller grid for that subregion.
   medsea_final_subgrid <- st_as_stars(st_bbox(geom),
                                       dx = granularity, dy = granularity) %>%
      st_as_sf()
   
   medsea_final_subgrid <- st_make_valid(medsea_final_subgrid) %>% 
     st_transform(crs = 4326) %>% 
     st_as_sf()
   # Additionally, we can again intersect it but I don't do it now because it takes a lot of time to run.
   #( %>% st_intersection_faster(medsea_read_robin) )

   return(medsea_final_subgrid)

}

# The function joiner takes a grid (called subgrid here) and joins SOS warnings to it.
joiner <- function(subgrid) {
  
  df_long2_intersect <- st_join(df_long2_intersect, subgrid, left = F)
  
}

# First read in the raw Mediterranean Sea shapefile.
medsea_read <- sf::read_sf('./Data/Raw/iho.shp')

# Filter SOS data to keep only those inside the Sea.
df_long2_read <- sf::read_sf(paste('./Data/Cleaned/warnings_all_years', '.shp', sep = ''))
df_long2_intersect <- df_long2_read %>% 
  st_intersection(medsea_read)

# Converting to Robin as we want to use stars package, which is much faster for creating fine grids.
medsea_read_robin <- st_transform(medsea_read,
                                  crs = "+proj=robin +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs")

# 500m grid was already done, so now we focus on creating the 1km and 5km grids.

# Now I'll parallelize the st_join process to make it faster.
cl <- makePSOCKcluster(detectCores()-1)
setDefaultCluster(cl)
clusterExport(NULL, c('gridder', 'joiner', 'df_long2_intersect'))
clusterEvalQ(cl, {library(sf); library(stars)})
#

# The i argument consists of the granularities we are interested in.
for(gran in c(500, 1000, 5000)) {
  
  # First we make a pretty coarse preliminary grid of 50 kms.
  medsea_prelim_grid <- st_as_stars(st_bbox(medsea_read_robin),
                                    dx = gran, dy = gran) %>%
    st_as_sf() %>%
    st_intersection(medsea_read_robin)
  
  # Use just the geometries....
  medsea_prelim_geoms <- medsea_prelim_grid[,2]
  
  mapview::mapview(medsea_prelim_geoms)
  
  # This huge grid is also too large, so we split it up into 4 subgrids called
  # m1, m2, m3 and m4.
  splits <- seq(1, nrow(medsea_prelim_geoms), length.out = 5)
  m1 <- medsea_prelim_geoms[splits[1]:(splits[2]-1),]
  m2 <- medsea_prelim_geoms[splits[2]:(splits[3]-1),]
  m3 <- medsea_prelim_geoms[splits[3]:(splits[4]-1),]
  m4 <- medsea_prelim_geoms[splits[4]:splits[5],]
  
  mapview::mapview(m1)
  
  # Convert these dataframes into list format to use with parLapply later.
  m1list <- list()
  for(i in 1:nrow(m1)) {
    m1list[[i]] <- m1[i,]
  }
  
  m2list <- list()
  for(i in 1:nrow(m2)) {
    m2list[[i]] <- m2[i,]
  }
  
  m3list <- list()
  for(i in 1:nrow(m3)) {
    m3list[[i]] <- m3[i,]
  }
  
  m4list <- list()
  for(i in 1:nrow(m4)) {
    m4list[[i]] <- m4[i,]
  }
  
  # Commenting it out since done once.
  
  # m1, m2, m3, m4 done separately.
  medsea_final_geoms_m1 <- parLapply(NULL, m1list, fun = gridder, granularity = gran) %>%
     bind_rows()
  
  # The saved file is a shapefile of the grid on just m1 section of the preliminary grid.
  st_write(medsea_final_geoms_m1, glue('./Data/Cleaned/medsea_geoms_{gran}m_m1.shp'), driver="ESRI Shapefile")
   
  # Remove it from environment just to keep it light.
  rm(medsea_final_geoms_m1)
   
  medsea_final_geoms_m2 <- parLapply(NULL, m2list, fun = gridder, granularity = gran) %>%
     bind_rows()
  st_write(medsea_final_geoms_m2, glue('./Data/Cleaned/medsea_geoms_{gran}m_m2.shp'), driver="ESRI Shapefile")

  rm(medsea_final_geoms_m2)

  medsea_final_geoms_m3 <- parLapply(NULL, m3list, fun = gridder, granularity = gran) %>%
     bind_rows()
  st_write(medsea_final_geoms_m3, glue('./Data/Cleaned/medsea_geoms_{gran}m_m3.shp'), driver="ESRI Shapefile")

  rm(medsea_final_geoms_m3)

  medsea_final_geoms_m4 <- parLapply(NULL, m4list, fun = gridder, granularity = gran) %>%
     bind_rows()
  st_write(medsea_final_geoms_m4, glue('./Data/Cleaned/medsea_geoms_{gran}m_m4.shp'), driver="ESRI Shapefile")

  rm(medsea_final_geoms_m4)
  
  # Now we have the grids, next step - matching it to SOS data!
  
  # Usual trick of initializing empty list and populating it in a loop.
  full_list_of_joins <- list()
  for(j in 1:4) {
    
    # Load in m1 or other grid.
    big_grid <- sf::read_sf(glue('./Data/Cleaned/medsea_geoms_{gran}m_m{j}.shp')) %>% 
      mutate('subgrid_id' = j, 'point_id' = 1:n(), 'unique_id' = paste(subgrid_id, point_id, sep = '.'))
    
    # We will do this in parts or the computer crashes.
    n <- 2000
    
    # Split into n parts.
    parts <- split(x = 1:nrow(big_grid), f = cut(1:nrow(big_grid), n))
    
    # Join with SOS data for every part separately.
    list_of_subgrids <- list()
    for(k in 1:n) {
      
      list_of_subgrids[[k]] <- big_grid[parts[[k]],]
      
    }
    
    # Remove from environment as no longer needed.
    rm(big_grid)
    
    list_of_joins <- parLapply(NULL, list_of_subgrids, joiner)
    full_list_of_joins <- full_list_of_joins %>% 
      append(list_of_joins)
    
  }
  
  # Finally, for the particular i granularity, get final dataframe version of matching.
  joined_df <- bind_rows(full_list_of_joins)
  st_write(joined_df, glue('./Output/warnings_all_years_with_{gran}m_medgrid.shp'), driver="ESRI Shapefile")

}

stopCluster(cl)

#######################################


####################################### Just checking if it works.....it does!

# subgrid_for_check <-  sf::read_sf(paste0('./Data/Cleaned/medsea_grid_500m_m',4,'.shp')) %>%
#    mutate('subgrid_id' = i, 'point_id' = 1:n(), 'unique_id' = paste(subgrid_id, point_id, sep = '.'))
# check <- subgrid_for_check %>%
#    filter(unique_id == '4.1835149')
# 
# check <- big_grid %>%
#    filter(unique_id == '4.1835149')
# check1 <- check$geometry
# check2 <- df_long2_read[1,c(12)]
# 
# dummydf <- data.frame('a' = c('', ''))
# dummydf <- st_set_geometry(dummydf, c(check1, check2$geometry))
# 
# mapview::mapview(dummydf)

#######################################


#######################################





