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


####################################### FIRST DEFINING FUNCTIONS

####################################### EXTRACTS DATE AND TIME FROM ISSUE AND CANCEL DATE COLUMNS

dateextractor <- function(rawdate) {
   if(!is.na(rawdate)) {

  splitdate <- str_split_1(rawdate, ' ')
  mnth <- match(str_to_title(splitdate[2]), month.abb)
  yr <- splitdate[3]
  day <- str_sub(splitdate[1], 1, 2)
  hr <- str_sub(splitdate[1], 3, 4)
  mins <- str_sub(splitdate[1], 5, 6)
  datestr1 <- paste(yr, mnth, day, sep = '-')
  datestr2 <- paste(hr, mins, sep = ':')
  datestr3 <- paste(datestr1, datestr2, sep = 'T')
  cleandate <- as.character(ymd_hm(datestr3))

   } else cleandate <- NA

  return(cleandate)
}

####################################### EXTRACTS ALL COORDINATE TYPE STRINGS FROM A MESSAGE

coordinateformat_regex <- '([:digit:][:digit:]|[:digit:])[:punct:]([:digit:][:digit:]|[:digit:])([:punct:]([:digit:][:digit:][:digit:]|[:digit:][:digit:]|[:digit:])|)([NSEW]|\\s[NSEW])\\s([:digit:][:digit:][:digit:]|[:digit:][:digit:]|[:digit:])[:punct:]([:digit:][:digit:]|[:digit:])([:punct:]([:digit:][:digit:][:digit:]|[:digit:][:digit:]|[:digit:])|)([NSEW]|\\s[NSEW])'
# | is for 'or'
#(12|1).(34|3)(.(567|56|5)|)(N| N) (123|12|1).(34|3)(.(567|56|5)|)(E| E)

coordsextractor <- function(message) {
   # Simply extracts all text following the coordinate format.
   coordsvec <- str_extract_all(message, coordinateformat_regex)
   return(coordsvec)
}

####################################### SPLIT_OR_NOT WILL BE USED IN FINAL_MESSAGE_EXTRACTOR
## TRUE = START A NEW OBJECT

split_or_not <- function(string, prestring) {

   # string is the submessage corresponding to the coordinate pair this function is being applied to.
   #prestring is the submessage corresponding to the pair appearing just before this one in the message.

   logic1 <- str_detect(string, c('[:alpha:]', 'TO', 'AND', '[:alpha:]\\.', '\\)', '\\(', 'LAST KNOWN'))
   logic2 <- str_detect(prestring, c('BETWEEN', 'TRACKLINE'))

   # = FALSE (i.e. don't start a new object) iff less than 8 words AND 'TO' appears AND ') , (' appear.
   chunk_A <- !(str_count(string,'\\S+') <= 8 & logic1[2] & logic1[5] & logic1[6])

   # = FALSE (i.e. don't start a new object) iff less than 4 words AND 'AND' appears OR
   # previous submessage has 'BETWEEN' in it AND this submessage has 'AND' in it OR
   # previous submessage has 'TRACKLINE' in it AND this submessage has 'AND' in it.
   # In such cases, it's always a coordinate pair belonging to the same object as the previous one.
   chunk_B <- !((str_count(string,'\\S+') <= 4 & logic1[3]) | (logic2[1] & logic1[3]) | (logic2[2] & logic1[3]))

   # = FALSE (i.e. don't start a new object) iff there are no alphabets in it.
   chunk_C <- logic1[1]

   # = FALSE (i.e. don't start a new object) iff it doesn't have a custom end message I'll add later to all messages.
   chunk_D <- str_detect(string, 'message ends here terminate terminate terminate')

   # = FALSE (i.e. don't start a new object) iff it has the words 'LAST KNOWN' in it.
   # This will be beneficial later when we want to extract the last known position coordinate instead of the route start and end points.
   chunk_E <- !(logic1[7])

   # = TRUE (i.e. start a new object) iff it has the custom end message or none of the special FALSE conditions are satisfied.
   logicfinal <- ((chunk_A & chunk_B & chunk_C & chunk_E) | chunk_D)
   return(logicfinal)
}

####################################### CLEAN UP THE COORDINATES, THEY MAY BE IN WEIRD FORMATS
coords_cleaner <- function(coordsvec) {
   coordsvec <- coordsvec %>%
      sapply(FUN = function(x) str_split_1(x, pattern = ' ')) %>%
      sapply(FUN = function(x) paste(str_sub(x, 1, -2), str_sub(x, -1), sep = '*'))

   for(k in 1: length(coordsvec)) {
      if(str_count(coordsvec[k], '[-,.]') < 2) {
         coordsvec[k] <- paste(paste(str_split(coordsvec[k], pattern = '\\*', simplify = TRUE)[1], '.00', sep = ''), str_split(coordsvec[k], pattern = '\\*', simplify = TRUE)[2], sep = '*')
      } else {coordsvec[k] <- coordsvec[k]}
      coordsvec_split <- sapply(coordsvec[k], FUN = function(x) str_split_1(x, pattern = '[-,.]'))
      coordsvec[k] <- paste(paste(coordsvec_split[1], coordsvec_split[2], sep = '!'), coordsvec_split[3], sep = '.')
   }

   for(i in seq(1, length(coordsvec), 2)) {
      coordsvec[i] <- paste(coordsvec[i], coordsvec[i+1])
   }
   coordsvec <- coordsvec[seq(1, length(coordsvec), 2)] %>%  unname()

   return(coordsvec)
}

####################################### NEEDED TO ENSURE PROPER SEPARATION OF DIFFERENT SPATIAL OBJECTS IN MESSAGE

final_message_extractor_UTM <- function(message) {

   # Modifying the message to correct for some common typos and issues.
   message <- paste(message, 'message ends here terminate terminate terminate') %>%
      str_replace_all(pattern = '\t', replacement = ' ') %>%
      str_replace_all(pattern = '\n', replacement = ' ') %>%
      str_replace_all(pattern = '  ', replacement = ' ') %>%
      str_replace_all(pattern = '000-00 ', replacement = '000-00E ')

   message <- gsub('\\s(?=[NSEW])', '', message, perl = TRUE)

   # A submessage is the part of the message between two coordinate pairs.
   submessages <- str_split(message, pattern = coordinateformat_regex,
                            simplify = TRUE)
   # We will use the submessage and the preceding one to determine beginning of a new object, so add a blank first submessage for the first submessage.
   submessages_with_blank <- c('', submessages)

   # This vector will hold the positions of all new object starting submessages.
   vec_showing_start_of_new_object <- c()

   # Do this for each submessage separately
   for(i in 2:length(submessages_with_blank)) {
      pre_submessage <- submessages_with_blank[i-1]
      current_submessage <- submessages_with_blank[i]

      vec_showing_start_of_new_object[i] <- split_or_not(current_submessage, pre_submessage)
   }

   # Remove the first one as it will be NA since we started from second element for the for loop.
   vec_showing_start_of_new_object <- vec_showing_start_of_new_object[-1]

   # Since first coordinate will always be a start of a new one, label it T forcefully.
   vec_showing_start_of_new_object[1] <- TRUE

   # Extract the coordinates from the message now (The easy part).
   coords_in_message <- coordsextractor(message)[[1]]

   if(length(coords_in_message) != 0) {

      # First standardize the coordinate formats.
      coords_in_message <- coords_in_message %>%
         coords_cleaner()

      # This vector contains the number of coordinates each unique object has.
      num_of_coords_in_each_object <- which(unname(vec_showing_start_of_new_object) == TRUE) - c(0,which(unname(vec_showing_start_of_new_object) == TRUE)[-length(which(unname(vec_showing_start_of_new_object) == TRUE))])
      num_of_coords_in_each_object <- num_of_coords_in_each_object[-1]

      # Split the coordinates using above vector to get separate coordinates corresponding to each object.
      final_coords_list <- split(coords_in_message, rep(1:sum(unname(vec_showing_start_of_new_object[-length(vec_showing_start_of_new_object)])), num_of_coords_in_each_object))
      # Do the same for submessages (remove the last end submessage)
      final_submessages_list <- split(submessages[-length(submessages)], rep(1:sum(unname(vec_showing_start_of_new_object[-length(vec_showing_start_of_new_object)])), num_of_coords_in_each_object))

   } else {

      final_coords_list <- ''
      final_submessages_list <- ''

   }

    final_combined_list <- c(final_coords_list, final_submessages_list)
    return(final_combined_list)

}

#######################################


####################################### FUNCTIONS OVER, NOW CLEANING

####################################### Load in all raw data for all years between 2012 and 2022.

filelist <- list.files(path = './Data/Raw/', pattern = '_', full.names = TRUE)

compiled_data <- read_csv(filelist[1]) %>%
   select('msgNumber', 'navArea', 'subregion', 'authority', 'issueDate', 'cancelDate', 'text')

for(i in filelist[-1]) {
   new <- read_csv(i) %>%
      select('msgNumber', 'navArea', 'subregion', 'authority', 'issueDate', 'cancelDate', 'text')
   compiled_data <- rbind(compiled_data, new)
}

# Add a unique message id.
df <- compiled_data %>%
   mutate(id = row_number())

#######################################

# Convert weird date format in messages to standard format - ymd_hms.
df$IssueDate <- sapply(df$issueDate, dateextractor)
df$CancelDate <- sapply(df$cancelDate, dateextractor)

# Extract all coordinates in the message AND separate them when they refer to different spatial objects.
df$Objects_list <- sapply(df$text, final_message_extractor_UTM)

# In every message, for every object, now we have a separate list of coordinates and the corresponding submessages.
df$Objects_list_coordinates <- sapply(df$Objects_list, FUN = function(x) x[1:(length(x)/2)])
df$Objects_list_submessages <- sapply(df$Objects_list, FUN = function(x) x[(length(x)/2) + 1 : length(x)])

# We will start with a (very very) wide data format.
df$Object1 <- ''
df$Object2 <- ''
df$Object3 <- ''
df$Object4 <- ''
df$Object5 <- ''
df$Object6 <- ''
df$Object7 <- ''
df$Object8 <- ''
df$Object9 <- ''
df$Object10 <- ''
df$Object11 <- ''
df$Object12 <- ''
df$Object13 <- ''
df$Object14 <- ''
df$Object15 <- ''
df$Object16 <- ''
df$Object17 <- ''
df$Object18 <- ''
df$Object19 <- ''
df$Object20 <- ''
df$Object21 <- ''
df$Object22 <- ''
df$Object23 <- ''
df$Object24 <- ''
df$Object25 <- ''
df$Object26 <- ''
df$Object27 <- ''
df$Object28 <- ''
df$Object29 <- ''
df$Object30 <- ''
df$Object31 <- ''
df$Object32 <- ''
df$Object33 <- ''
df$Object34 <- ''
df$Object35 <- ''
df$Object36 <- ''
df$Object37 <- ''
df$Object38 <- ''
df$Object39 <- ''
df$Object40 <- ''
df$Object41 <- ''
df$Object42 <- ''
df$Object43 <- ''
df$Object44 <- ''
df$Object45 <- ''

df$Submessage1 <- ''
df$Submessage2 <- ''
df$Submessage3 <- ''
df$Submessage4 <- ''
df$Submessage5 <- ''
df$Submessage6 <- ''
df$Submessage7 <- ''
df$Submessage8 <- ''
df$Submessage9 <- ''
df$Submessage10 <- ''
df$Submessage11 <- ''
df$Submessage12 <- ''
df$Submessage13 <- ''
df$Submessage14 <- ''
df$Submessage15 <- ''
df$Submessage16 <- ''
df$Submessage17 <- ''
df$Submessage18 <- ''
df$Submessage19 <- ''
df$Submessage20 <- ''
df$Submessage21 <- ''
df$Submessage22 <- ''
df$Submessage23 <- ''
df$Submessage24 <- ''
df$Submessage25 <- ''
df$Submessage26 <- ''
df$Submessage27 <- ''
df$Submessage28 <- ''
df$Submessage29 <- ''
df$Submessage30 <- ''
df$Submessage31 <- ''
df$Submessage32 <- ''
df$Submessage33 <- ''
df$Submessage34 <- ''
df$Submessage35 <- ''
df$Submessage36 <- ''
df$Submessage37 <- ''
df$Submessage38 <- ''
df$Submessage39 <- ''
df$Submessage40 <- ''
df$Submessage41 <- ''
df$Submessage42 <- ''
df$Submessage43 <- ''
df$Submessage44 <- ''
df$Submessage45 <- ''

df$Type1 <- ''
df$Type2 <- ''
df$Type3 <- ''
df$Type4 <- ''
df$Type5 <- ''
df$Type6 <- ''
df$Type7 <- ''
df$Type8 <- ''
df$Type9 <- ''
df$Type10 <- ''
df$Type11 <- ''
df$Type12 <- ''
df$Type13 <- ''
df$Type14 <- ''
df$Type15 <- ''
df$Type16 <- ''
df$Type17 <- ''
df$Type18 <- ''
df$Type19 <- ''
df$Type20 <- ''
df$Type21 <- ''
df$Type22 <- ''
df$Type23 <- ''
df$Type24 <- ''
df$Type25 <- ''
df$Type26 <- ''
df$Type27 <- ''
df$Type28 <- ''
df$Type29 <- ''
df$Type30 <- ''
df$Type31 <- ''
df$Type32 <- ''
df$Type33 <- ''
df$Type34 <- ''
df$Type35 <- ''
df$Type36 <- ''
df$Type37 <- ''
df$Type38 <- ''
df$Type39 <- ''
df$Type40 <- ''
df$Type41 <- ''
df$Type42 <- ''
df$Type43 <- ''
df$Type44 <- ''
df$Type45 <- ''

# From coordinate list to Object columns. Just spreading the list of object coordinates over a bunch of columns. This way it is easier to deal with it in the future.
for(i in 1:nrow(df)) {
   objectslist <- df$Objects_list_coordinates[i]
   if(length(objectslist[[1]]) != 0) {
      for(j in 1:length(objectslist[[1]])) {
         df[i,j+13] <- str_c(unlist(objectslist[[1]][j]), collapse = ' | ')
      }
   }
}

# From submessage list to Object columns.
for(i in 1:nrow(df)) {
   objectslist <- df$Objects_list_submessages[i]
   if(length(objectslist[[1]]) != 0) {
      for(j in 1:length(objectslist[[1]])) {
         df[i,j+13+45] <- str_c(unlist(objectslist[[1]][j]), collapse = ' | ')
      }
   }
}

# Remove messages not referring to locations. Many cancellation messages without any coordinates.
df <- df %>%
   filter(Object1 != '')

# Remove ugly lists from df.
df <- df %>%
   select(-'Objects_list', -'Objects_list_coordinates', -'Objects_list_submessages', -'msgNumber', -'navArea', -'subregion', -'authority', -'issueDate', -'cancelDate') %>%
   select('id', everything()) %>%
   rename('message_id' = 'id')

# Populating Type columns.
for(i in 1:nrow(df)) {
   # For every row i in df....

   # Create a vector of all object coordinates.
   objects_vec_coords <- stri_remove_empty(as.character(df[i,5:(5+44)]))
   # Create a vector of all object submessages.
   objects_vec_submessages <-  stri_remove_empty(as.character(df[i,(5+45):(5+45+44)]))
   # In this one, we will store the classified types.
   objects_vec_types <- ''

   for(j in 1:length(objects_vec_coords)) {
      # For every object in that row....j

      # Determine number of points in the object using | separator.
      number_of_points_in_object <- (str_count(objects_vec_coords[j], '\\|') + 1)

      submessage_of_this_object <- objects_vec_submessages[j]

      if(str_detect(submessage_of_this_object, pattern = 'LAST KNOWN')) {

         if(number_of_points_in_object == 2 & str_detect(submessage_of_this_object, pattern = 'BETWEEN')) {

            objects_vec_types[j] <- 'line_unknown'

         } else objects_vec_types[j] <- 'lastknown_point'

      } else if(number_of_points_in_object > 2) {

         # No need to discriminate between areas and tracklines really. Need only centroid eventually.
         objects_vec_types[j] <- 'multi-point object'

      } else if (number_of_points_in_object == 2) {
         # These are lines between two points. But what kind?

         if(str_detect(submessage_of_this_object, pattern = 'TRACKLINE|JOINING')) {

            objects_vec_types[j] <- 'trackline'

         } else if(min(str_detect(submessage_of_this_object, pattern = c('TO', '\\)', '\\('))) == 1) {

            objects_vec_types[j] <- 'route'

         } else objects_vec_types[j] <- 'line_unknown'

      } else {
         # Now we just have the objects with a single coordinate pair in it, i.e. a point.

         objects_vec_types[j] <- 'point'

      }
   }

   # Now with our final classification vector, we fill in the columns.
   objects_vec_types <- c(objects_vec_types, rep('', (45-length(objects_vec_types))))
   df[i,(5+45+45):(5+45+45+44)] <- t(objects_vec_types)

}

#######################################


####################################### Fixing the cases where coordinates arranged wrongly.
# Some messages, for example, list two points as - xx.xx.xxN yy.yy.yyN and xxx.xx.xxE yyy.yy.yyE
# When it should be xx.xx.xxN xxx.xx.xxE and yy.yy.yyN yyy.yy.yyE

# Since its a very inefficient nested for loop, it takes quite a while to run.

coordinateformat_regex_wrong <- '([:digit:][:digit:]|[:digit:])!([:digit:][:digit:]|[:digit:])(\\.([:digit:][:digit:][:digit:]|[:digit:][:digit:]|[:digit:])|)\\*[NS]\\s([:digit:][:digit:]|[:digit:])!([:digit:][:digit:]|[:digit:])(\\.([:digit:][:digit:][:digit:]|[:digit:][:digit:]|[:digit:])|)\\*[NS]'

for(i in 1:45) {
   for(j in 1:nrow(df)) {
      coords_in_the_cell <- as.character(df[j,i+4])
      separated_coords_in_the_cell <- str_split_1(coords_in_the_cell, pattern = ' \\| ')
      is_wrong <- str_detect(separated_coords_in_the_cell, pattern = coordinateformat_regex_wrong)
      switch_coor_1 <- separated_coords_in_the_cell[is_wrong]
      switch_coor_2 <- separated_coords_in_the_cell[which(is_wrong) + 1]
      corrected <- c()
      if(length(switch_coor_1) > 0) {
         for(k in 1:length(switch_coor_1)) {
            correct_lat <- str_split(switch_coor_1, pattern = ' ')[[k]][1]
            correct_lon <- str_split(switch_coor_2, pattern = ' ')[[k]][2]
            switch_lat <- str_split(switch_coor_1, pattern = ' ')[[k]][2]
            switch_lon <- str_split(switch_coor_2, pattern = ' ')[[k]][1]
            first_correct_pair <- paste(correct_lat, switch_lon)
            second_correct_pair <- paste(switch_lat, correct_lon)
            corrected <- c(corrected, first_correct_pair, second_correct_pair)
         }
         indices_to_replace <- sort(c(which(is_wrong), which(is_wrong)+1))
         separated_coords_in_the_cell[indices_to_replace] <- corrected
         final_correct_coordinates <- paste(separated_coords_in_the_cell, collapse = ' | ')
         df[j,i+4] <- final_correct_coordinates
      }
   }
}

#######################################


####################################### RANDOM SAMPLE CHECK OF SIZE 1000

df1 <- select(df, 'message_id', 'text', contains('Type'))

set.seed(3458)
random_manual_check <- sample_n(df1, 1000)

write.csv(random_manual_check, './Data/Cleaned/Test.csv')

#######################################


##################################### SAVE FINAL OUTPUT TO DATA/CLEAN

# write.csv(df, paste('./Data/Cleaned/warnings', '_all_years', '.csv', sep = ''))
# rm(list = ls())
df <- read_csv(paste('./Data/Cleaned/warnings', '_all_years', '.csv', sep = ''))
df <- df[,-1]

#####################################


####################################### WIDE TO LONG PIVOT

df_long <- data.table(df) %>%
   melt(id.vars = c('message_id', 'text', 'IssueDate', 'CancelDate'),
        measure.vars = patterns('^Object', '^Submessage', '^Type'),
        variable.name = 'Object_Number',
        value.name = c('Coordinates', 'Submessage', 'Type')) %>%
   arrange(message_id) %>%
   filter(Coordinates != '') %>%
   mutate(object_id = row_number()) %>%
   select('message_id', 'object_id', everything()) %>%
   mutate('Confidence' = case_when(Type %in% c('multi-point object', 'line_unknown', 'trackline') ~ 'interpolated',
                                   Type %in% c('lastknown_point', 'point') ~ 'exact_point',
                                   Type == 'route' ~ 'uncertain'))

#######################################


####################################### CHOOSE LAST COORDINATE FOR LAST KNOWN
# There are many messages where a route start and end point are mentioned. And then a last known coordinate is mentioned. We want the latter.

lastknown_extractor <- function(coors) {
   vec <- str_split(coors, pattern = ' \\| ', simplify = TRUE)
   last <- vec[length(unname(vec))]

   return(last)

}

df_long$Last_Coordinate <- sapply(df_long$Coordinates, lastknown_extractor)

df_long <- df_long %>%
   mutate('Coordinates_relevant' = case_when(Type == 'lastknown_point' ~ Last_Coordinate,
                                             .default = Coordinates)) %>%
   select(-'Last_Coordinate')

# Coordinates_relevant is our key variable of interest from now on.

####################################### CALCULATE CENTROID/MIDPOINT/LASTKNOWN

####################################### FIRST DEFINING THE RELEVANT FUNCTIONS

centroid_calculator <- function(coors_string) {
   separated_coors <- str_split_1(coors_string, pattern = ' \\| ')
   separated_coors <- str_split(separated_coors, pattern = ' ')
   separated_coors_decimals <- sapply(unlist(separated_coors),
                                      FUN = function(x)
                                         as.numeric(char2dms(x, chd = "!", chm = "\\.", chs='\\*')))

   separated_coors_decimals <- c(separated_coors_decimals[c(TRUE, FALSE)],
                                 separated_coors_decimals[c(FALSE, TRUE)])

   mat <- matrix(separated_coors_decimals, ncol = 2)
   mat <- mat[,2:1]
   mat <- mat %>%
      data.frame() %>%
      mutate('id' = '1') %>%
      select('id', everything()) %>%
      st_as_sf(coords = c("X1", "X2")) %>%
      concaveman::concaveman()

   shape <- mat$polygons
   centroid <- st_centroid(shape)

   return(centroid)

}

midpoint_calculator <- function(coors_string) {
   separated_coors <- str_split_1(coors_string, pattern = ' \\| ')
   separated_coors <- str_split(separated_coors, pattern = ' ')
   separated_coors_decimals <- sapply(separated_coors,
                                      FUN = function(x)
                                         as.numeric(char2dms(x, chd = "!", chm = "\\.", chs='\\*')))

   separated_coors_decimals <- c(separated_coors_decimals[c(TRUE, FALSE)],
                                 separated_coors_decimals[c(FALSE, TRUE)])

   mat <- matrix(separated_coors_decimals, ncol = 2)
   mat <- mat[,2:1]

   shape <- st_linestring(mat)
   centroid <- st_centroid(shape)

   return(centroid)

}

point_calculator <- function(coors_string) {
   separated_coors <- str_split_1(coors_string, pattern = ' \\| ')
   separated_coors <- str_split(separated_coors, pattern = ' ')
   separated_coors_decimals <- sapply(unlist(separated_coors), FUN = function(x) as.numeric(char2dms(x, chd = "!", chm = "\\.", chs='\\*')))
   mat <- matrix(separated_coors_decimals, ncol = 2)
   mat <- mat[,2:1]

   shape <- st_point(mat)

   return(shape)

}

#######################################

# For the very first object, do it using if else. Later, avoid for speed.
subdf <- df_long[1,]

if(subdf$Type %in% c('multi-point object')) {

   shape <- centroid_calculator(subdf$Coordinates_relevant)

} else if(subdf$Type %in% c('line_unknown', 'trackline','route')) {

   shape <- midpoint_calculator(subdf$Coordinates_relevant)

} else {

   shape <- point_calculator(subdf$Coordinates_relevant)

}

st_geometry(subdf) <- st_as_sfc(list(shape), crs = 4326)
df_long2 <- subdf

# First use centroid calculator on the multi-point objects.
df_long_polygons <- df_long[-1,] %>%
   filter(Type %in% c('multi-point object'))

for(i in 1:nrow(df_long_polygons)) {
   subdf <- df_long_polygons[i,]

   shape <- centroid_calculator(subdf$Coordinates_relevant)
   st_geometry(subdf) <- st_as_sfc(list(shape[[1]]), crs = 4326)

   df_long2 <- rbind(df_long2, subdf)

}

# Second use midpoint calculator on the line segments.
df_long_lines <- df_long[-1,] %>%
   filter(Type %in% c('line_unknown', 'trackline','route'))

for(i in 1:nrow(df_long_lines)) {
   subdf <- df_long_lines[i,]

   shape <- midpoint_calculator(subdf$Coordinates_relevant)
   st_geometry(subdf) <- st_as_sfc(list(shape), crs = 4326)

   df_long2 <- rbind(df_long2, subdf)

}

# Third use point calculator on single point objects.
df_long_points <- df_long[-1,] %>%
   filter(Type %in% c('point','lastknown_point'))

for(i in 1:nrow(df_long_points)) {
   subdf <- df_long_points[i,]

   shape <- point_calculator(subdf$Coordinates_relevant)
   st_geometry(subdf) <- st_as_sfc(list(shape), crs = 4326)

   df_long2 <- rbind(df_long2, subdf)

}

df_long2 <- df_long2 %>%
   arrange('object_id')

####################################### SAVE SHAPEFILE

# ESRI hates long column names, so shortening it.
colnames(df_long2) <- c('m_id', 'o_id', 'msg', 'i_date', 'c_date', 'o_num', 'coors', 'submsg', 'type', 'conf', 'coors_r', 'geometry')
df_long2$i_date <- as.character(df_long2$i_date)
df_long2$c_date <- as.character(df_long2$c_date)
st_write(df_long2, paste('./Data/Cleaned/warnings_all_years', '.shp', sep = ''), driver="ESRI Shapefile")

#######################################