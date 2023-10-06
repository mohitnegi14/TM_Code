####################################### 
# MOHIT NEGI
# Last Updated : 7th May 2023
# Contact on : mohit.negi@studbocconi.it
####################################### 


################### LIBRARIES
library(ncdf4)
library(chron)
library(lattice)
library(RColorBrewer)
library(stringr)
library(tmap)
library(sf)
library(tidyverse)
library(lubridate)
library(data.table)
library(abind)
library(matrixStats)
library(glue)
library(parallel)
library(data.table)
###################


#################################################################
# Raw weather data downloaded from the CDS Copernicus website. Not available on my google drive currently.
# Choose the following variables - Significant height of combined waves and swell, Benjamin Freir Index,
# 10m u component of wind, 10m v component, low cloud cover, mean total precipitation rate. Take 2 years
# daily data at a time (for example 2012-14, 2014-16, ....2020-2022 and then 2022 alone separately).
# https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-single-levels?tab=form


# First, simply loading in the NetCDF formatted data.
nc_df <- nc_open('./Data/Raw/weatherrawdata_2022.nc')

longs <- ncvar_get(nc_df, 'longitude')
lats <- ncvar_get(nc_df, 'latitude')
#times <- ncvar_get(nc_df, 'time')
#time_units <- ncatt_get(nc_df,'time','units')

# Check the dimensions, look good.
print(c(dim(longs), dim(lats)))

# Time in unusable format, clean it up.

time_cleaner <- function(time_units) {
  
  tustr <- str_split(time_units$value, ' ')
  tdstr <- strsplit(unlist(tustr)[3], "-")
  tmonth <- as.integer(unlist(tdstr)[2])
  tday <- as.integer(unlist(tdstr)[3])
  tyear <- as.integer(unlist(tdstr)[1])
  times_in_correct_format <- as.POSIXct(times*60*60, 
                                        origin = paste(unlist(tustr)[3], 
                                                       unlist(tustr)[4]))
  return(times_in_correct_format)
  
}

#################################################################


#################################################################

latslongs <- as.matrix(expand.grid(longs, lats)) %>% 
  as.data.frame() %>% 
  mutate(id = 1:n())
colnames(latslongs) <- c('Lon', 'Lat', 'id')
# These are too many, I will filter them to restrict to Med Sea.

# Using st_intersection to do it, so first need conversion to shapefile.
llshapefile <- sf::st_as_sf(latslongs, coords = c('Lon', 'Lat'), 
                            crs = "+proj=longlat +datum=WGS84 +ellps=WGS84 +towgs84=0,0,0")
medsea_read <- sf::read_sf('./Data/Raw/iho.shp')
llshapefile <- llshapefile %>% 
  st_intersection(medsea_read)

# Check them out!
# mapview::mapview(llshapefile)
# ggplot()+ geom_sf(data =llshapefile) + geom_sf(data = medsea_read, fill = NA)

# Now going back, from point coordinates to long and lat separately.
justgeoms <- as.character(llshapefile$geometry)
mat_with_relevant_latlongs <- matrix(, length(justgeoms), 2)

for(i in 1:length(justgeoms)) {
  
  rawstring_cleaned <- str_sub(justgeoms[i], 3, -2)
  rawstring_splitup <- str_split_1(rawstring_cleaned, pattern = ', ')
  
  mat_with_relevant_latlongs[i,] <- rawstring_splitup
  
}

# Matrix to Dataframe
relevant_latslongs <- mat_with_relevant_latlongs %>% 
  as.data.frame()
relevant_latslongs$V1 <- as.numeric(relevant_latslongs$V1)
relevant_latslongs$V2 <- as.numeric(relevant_latslongs$V2)
colnames(relevant_latslongs) <- colnames(latslongs)[1:2]

# Doing it this way instead of just using relevant_latslongs simply because I want
# to ensure they are in the correct order. This is because I will filter only the relevant
# wind and wave data, and they are ordered in a particular way.
final_latslongs <- inner_join(latslongs, relevant_latslongs) %>% 
  arrange(id) %>% 
  select(-id)

head(final_latslongs) # Looks great

#################################################################


#################################################################
# Finally, bring everything together and extract the relevant wind and wave data.

# First create a dataset with the relevant "indices", not the actual coords and times.
final_longs_indices <- sapply(final_latslongs$Lon, FUN = function(x) which(longs == x))
final_lats_indices <- sapply(final_latslongs$Lat, FUN = function(x) which(lats == x))
final_latslongs_indices <- cbind(final_longs_indices, final_lats_indices)
colnames(final_latslongs_indices) <- c('Lon', 'Lat')

# Now I start with the wind and wave data.
wdata_varlist <- names(nc_df$var)
print(wdata_varlist)

# First create a blank list, populate using the loop.
list_of_datasets_by_year <- list.files(path = './Data/Raw/', pattern = '*.nc')

# We will do this one by one manually, so select 1, 2,.... and so on. Also change in line 275 appropriately.
list_of_datasets_by_year <- list_of_datasets_by_year[1]

# An approach will be used again and again (in a nested way)
# Create a blank list, then populate it with data, then rbind it to get a dataframe.
final_wdata_allvars <- list()

relevant_slice_retriever <- function(dataset) {

  sublist <- list()
  for(index in 1:dim(dataset)[3]) {

    slice <- dataset[,,index]
    relevant_slice <- slice[final_latslongs_indices]

    sublist[[index]] <- relevant_slice

  }

  df <- do.call(rbind.data.frame, sublist)
  colnames(df) <- 1:4079

  return(df)
  
}

# Now I'll parallelize the st_join process to make it faster.
cl <- makePSOCKcluster(detectCores() - 1)
setDefaultCluster(cl)
clusterExport(NULL, c('relevant_slice_retriever', 'final_latslongs_indices'))
clusterEvalQ(cl, {
  library(dplyr)
})
#

for(i in 1:length(wdata_varlist)) {
  
  wdata_variable <- wdata_varlist[i]
  print(wdata_variable)
  
  final_wdata_by_year <- list()
  # First, by year (or 2 year NetCDF files)
  for(j in list_of_datasets_by_year) {
    
    print(j)
    
    nc_data <- nc_open(glue('./Data/Raw/{j}'))
    wdata <- ncvar_get(nc_data, wdata_variable)
    time_units <- ncatt_get(nc_data,'time','units')
    times <- ncvar_get(nc_data, 'time')
    times_in_correct_format <- time_cleaner(time_units)
    
    n = 50
    # Split into n parts.
    parts <- split(x = 1:length(times_in_correct_format), f = cut(1:length(times_in_correct_format), n))
    
    # Join with SOS data for every part separately.
    list_of_slices <- list()
    for(k in 1:n) {
      
      list_of_slices[[k]] <- wdata[,,parts[[k]]]
      
    }
    
    rm(wdata)
    
    print('parallel processing....')
    
    # Second, by time within the year, parallelized
    wdata_by_time_list <- list()
    wdata_by_time_list <- parLapply(NULL,
                                    list_of_slices,
                                    fun = relevant_slice_retriever)
    
    print('parallel processing over....')
    
    final_wdata <- do.call(rbind.data.frame, wdata_by_time_list)
    
    rm(wdata_by_time_list)
    
    final_wdata <- t(final_wdata)
    
    day_breaks <- which(!duplicated(floor_date(ymd_hms(times_in_correct_format), unit = 'days')))
    
    daily_means_and_sds <- list()
    for(l in 1:(length(day_breaks)-1)) {
      
      start <- day_breaks[l]
      end <- day_breaks[l+1]-1
      
      sub_wdata <- final_wdata[,start:end]
      
      x <- rowMeans(sub_wdata)
      y <- rowSds(sub_wdata)
      
      day_mean_and_sd <- cbind(x, y)
      colnames(day_mean_and_sd) <- c(glue('{wdata_variable}_daily_mean'),
                                     glue('{wdata_variable}_daily_sd'))
      
      daily_means_and_sds[[l]] <- day_mean_and_sd
      
    }
    
    rm(final_wdata)
    
    final_wdata_daily <- do.call(rbind.data.frame, daily_means_and_sds)
    rownames(final_wdata_daily) <- NULL
    
    final_wdata_by_year[[j]] <- final_wdata_daily 
    
    rm(final_wdata_daily)
    
  }
  
  final_wdata_var <- do.call(rbind.data.frame, final_wdata_by_year)
  
  rm(final_wdata_by_year)
  
  final_wdata_allvars[[i]] <- final_wdata_var
  
}

stopCluster(cl)

final_wdata_allvars <- do.call(cbind.data.frame, final_wdata_allvars)
latcolumn <- rep(final_latslongs$Lat, length(daily_means_and_sds))
loncolumn <- rep(final_latslongs$Lon, length(daily_means_and_sds))
daycolumn <- rep(unique(floor_date(ymd_hms(times_in_correct_format), unit = 'days'))[-length(day_breaks)], 
                 nrow(final_latslongs)) %>% 
  sort()
final_wdata_allvars$Lon <- loncolumn
final_wdata_allvars$Lat <- latcolumn
final_wdata_allvars$Day <- daycolumn

final_wdata_allvars <- final_wdata_allvars %>% 
  select('Lon', 'Lat', 'Day', everything())

rownames(final_wdata_allvars) <- NULL

options(scipen = 999)

# One file has data for a two year period. Except the last one which is only for 2022.
fwrite(final_wdata_allvars, './Data/Cleaned/weathercleandata_2012-14.csv')

# Finally, making it a nice and tidy shapefile for future merging by adding geometries (only for 2022).
# final_wdata_allvars_sf <- st_as_sf(final_wdata_allvars, coords = c('Lon', 'Lat'), 
#                                    crs = 4326)
# st_geometry(final_wdata_allvars) <- final_wdata_allvars_sf$geometry
# head(final_wdata_allvars) # Perfect.
# st_write(final_wdata_allvars, './Data/Cleaned/weathercleandata_2022.shp', driver="ESRI Shapefile")
#################################################################


#################################################################
# Manual check that values are correct. They are.
# u10 <- ncvar_get(nc_df, 'u10')
# u10[94,18,1:23]
#################################################################




