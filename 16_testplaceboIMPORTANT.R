###############################################################################################
################## INTRO
# MOHIT NEGI
# Last Updated : 8 October 2023
# Contact on : mohit.negi@studbocconi.it
################## 

################## OBJECTIVES
# This script performs the following tasks.
# 1. Compute Placebo Networks : Connections between people who worked at the same establishment
# in the past but NOT during the same time.
##################

################## PATHS
harddrive <- 'D:/Mohit_Work/MOCANU/RAIS_tasks/intermediate/test'
box <- 'C:/Users/mohit/Box/mohit_ra/intermediate/test'
##################

################## PACKAGES
# We will use these in this script.
packages <- c('glue',
              'data.table',
              'parallel',
              'stringr',
              'purrr',
              'R.utils',
              'fixest',
              'stargazer',
              'ggplot2',
              'egg',
              'did',
              'dplyr',
              'sqldf',
              'zoo',
              'plyr')

# Installs them if not yet installed.
installed_packages <- packages %in% rownames(installed.packages())
if(any(installed_packages == FALSE)) { install.packages(packages[!installed_packages]) }

# And load them in.
invisible(lapply(packages, library, character.only = T))

# Disable scientific notation.
options(scipen = 999)

rm(packages, installed_packages)

# For faster reading.
setDTthreads(0L)
##################
###############################################################################################






# ###############################################################################################
# ##################
# year_param <- 1988
# month_param <- 5
# workplaces_df <- data.table::fread(glue('{box}/intermediate/workplaces/workplaces_{year_param}/workplacedata_month{month_param}_{year_param}.csv'),
#                                    colClasses = c(rep('character', 2)))
# 
# sample <- sample_n(workplaces_df, 10000)
# 
# sample2 <- workplaces_df[workplace == '96923487000153_430535_1988_5']
# 
# # 96923487000153_430535_1988_5, 120191630811, 122726131701
# mylines <- read.csv.sql(glue('{box}/intermediate/workplaces/workplaces_{year_param}/workplacedata_month{month_param}_{year_param}.csv'), 
#                         sql = "select * from file where `pis_encoded` in (120191630811, 122726131701)", eol = "\n")
# 
# 
# workplace_names <- unique(sample$workplace)
# workplace_names_string <- paste0('"', paste(workplace_names, collapse = '", "'), '"')
# 
# mylines <- read.csv.sql(glue('{box}/intermediate/workplaces/workplaces_{year_param}/workplacedata_month{month_param}_{year_param}.csv'), 
#                         sql = glue('select * from file where `workplace` in ({print(workplace_names_string, quote = F)})'), eol = "\n")
# 
# entrants_workplaces_nonconcursado <- data.table::fread(glue('{harddrive}/workplaces_batchwise_2/workplaces_entrants_nonconcursado.csv'),
#                                                        colClasses = c('character', 'numeric', 'character', 'character', 'character', 'character', 'character', 'numeric', 'numeric'))
# 
# entrants_workplaces_nonconcursado_thismonthyear <- entrants_workplaces_nonconcursado[monthyear_workplace == '5_1988']
# 
# workplace_names <- unique(entrants_workplaces_nonconcursado_thismonthyear$workplace)
# workplace_names_string <- paste0('"', paste(workplace_names, collapse = '", "'), '"')
# 
# mylines <- read.csv.sql(glue('{box}/intermediate/workplaces/workplaces_{year_param}/workplacedata_month{month_param}_{year_param}.csv'), 
#                         sql = glue('select * from file where `workplace` in ({print(workplace_names_string, quote = F)})'), eol = "\n")
# 
# 
# entrants <- data.table::fread(glue('{cleaned}/entrants_public_sector.csv'),
#                               colClasses = rep('character', 11))
# 
# length(unique(entrants$establishment))
# ###############################################################################################






# ###############################################################################################
# ##################
# # Idea : Split references into different batches but according to the establishment. There will be
# # duplication.
# entrants <- data.table::fread(glue('{harddrive}/intermediate/public_sector_workers/entrants_public_sector.csv'),
#                               colClasses = c('character', 'numeric', rep('character', 6), 'numeric', rep('character', 2)))
# 
# references <- data.table::fread(glue('{harddrive}/intermediate/public_sector_workers/references_public_sector.csv'),
#                                 colClasses = rep('character', 5))
# 
# relevant_estab_cohorts <- entrants[, c('establishment', 'cohort')] %>% unique()
# 
# references_relevant <- references[relevant_estab_cohorts, on = .(establishment, reference_year = cohort), nomatch = 0]
# references_relevant <- references_relevant[, c('establishment', 'pis_encoded')] %>% unique()
# 
# rm(entrants)
# rm(references)
# rm(relevant_estab_cohorts)
# 
# # Now get row numbers of each establishment chunk start.
# data.table::setorder(references_relevant, 'establishment')
# 
# # Get a count of references by establishment. 
# workernumbers <- references_relevant[, .(count = .N), by = 'establishment']
# setorder(workernumbers, 'count')
# 
# # Add a cumulative sum column.
# workernumbers[, cumul_count := cumsum(count)]
# 
# # Find the closest multiple of 100,000 for the particular cumulative sum.
# workernumbers[, batch := plyr::round_any(cumul_count, 1, f = ceiling)]
# 
# # To find the batch number, divide by 100,000.
# workernumbers[, batch := batch/1]
# 
# # Keep only relevant columns
# estab_batches_crosswalk <- workernumbers[, c('establishment', 'cumul_count', 'batch')]
# rm(workernumbers)
# 
# # Create a dataframe with just the batch numbers.
# batchnumbers <- data.frame('batch' = 1:estab_batches_crosswalk$batch[nrow(estab_batches_crosswalk)])
# 
# # Now join so that we have all batch numbers.
# estab_batches_crosswalk <- estab_batches_crosswalk[batchnumbers, on = .(batch)]
# 
# rm(batchnumbers)
# 
# # Fill the establishment values from the ones below. All this is because some estabs are so big that
# # they don't fit in a single batch.
# estab_batches_crosswalk$establishment <- zoo::na.locf(estab_batches_crosswalk$establishment, fromLast = TRUE)
# 
# # Label the subbatch numbers within a batch.
# estab_batches_crosswalk[, subbatch := 1:.N, by = establishment]
# 
# # Now we have a crosswalk between the establishments and the corresponding workplace batches they reside in.
# # Now match references with batches.
# references_relevant[, subbatch := 1:.N, by = establishment]
# references_relevant[, subbatch := ceiling(subbatch/1)]
# 
# references_relevant <- estab_batches_crosswalk[references_relevant, on = .(establishment, subbatch)]
# 
# rm(estab_batches_crosswalk)
# 
# data.table::setorder(references_relevant, establishment, subbatch)
# 
# references_relevant[, batch := zoo::na.locf(batch)]
# 
# # Now it should divide them up beautifully!
# collapsed <- references_relevant[, .(batch_count = .N), by = batch]
# 
# rm(collapsed)
# 
# # Final dataset to create the batches.
# references_in_batches <- references_relevant[, c('pis_encoded', 'establishment', 'batch')]
# 
# rm(references_relevant)
# 
# # Save.
# # data.table::fwrite(references_in_batches, file = glue('{harddrive}/intermediate/public_sector_workers/references_public_sector_batches.csv'))
# rm(references_in_batches)
# ###############################################################################################
# 
# 
# 
# 
# 
# 
# ###############################################################################################
# ################## CREATE BATCH-WISE WORKPLACE HISTORY DATA.
# # Currently, we have separate files for each period (month-year). This is because the entire
# # dataset, if left as a single file, will be way too large. However, for the purposes of 
# # calculating connections, working with one file at a time will not be feasible since
# # we will need to track if the same two people were already connected in a previous period
# # workplace. If we do not keep track, we'll count this single connection many times.
# 
# # The solution is to still split the workplaces, but instead of splitting by period, we split them
# # by pis codes. So divide all the pis codes (only the relevant ones from above) into batches, and
# # each file contains workplaces corresponding to pis codes in only that batch for all periods.
# # This way, we calculate the connections separately with each batch (so the computer can handle it)
# # and then sum over the connections across batches. This way it will be valid and no double-counting
# # will happen.
# 
# # Here is the plan:
# # 1. Divide the relevant pis codes into smaller batches.
# # 2. Load a period's workplaces file (say january 1985) from the Box folder.
# # 3. Subset it by keeping only rows corresponding to pis codes in batch 1.
# # 4. Save it to a batch file : workplaces_batch1.csv and so on.
# # 5. Repeat for batch 2, 3, .... and so on.
# # 6. Then next period (february 1985 and so on), this time append to same workplaces csv's.
# ################## 
# 
# ##################
# references_in_batches <- data.table::fread(glue('{harddrive}/intermediate/public_sector_workers/references_public_sector_batches.csv'),
#                                            colClasses = c('character', 'character', 'numeric'))
# data.table::setorder(references_in_batches, batch)
# ##################
# 
# ################## 
# redo_workplaces_fn <- function(period_param){
#   
#   # Extract the year and month from the period.
#   year_param <- stringr::str_split(period_param, pattern = '_')[[1]][2]
#   month_param <- stringr::str_split(period_param, pattern = '_')[[1]][1]
#   
#   message(glue('WORKING ON MONTH {month_param} OF YEAR {year_param}.'))
#   message(glue('----------------------------------------------------'))
#   
#   # Load in the appropriate dataset.
#   workplaces_df <- data.table::fread(glue('{box}/intermediate/workplaces/workplaces_{year_param}/workplacedata_month{month_param}_{year_param}.csv'),
#                                      colClasses = c(rep('character', 2)))
#   
#   # Add year and month-year variables. This will be useful later.
#   workplaces_df <- workplaces_df[, `:=`(year_workplace = year_param, monthyear_workplace = paste(month_param, year_param, sep = '_'))]
#   
#   # Many many duplicates i.e. same pis appearing in the same workplace many times.
#   # Most of these seem to be because of catch-all pis codes which do not correspond to a single
#   # individual. Other times, it might be because the same worker appeared many times in the same
#   # year's data in the same establishment. This is because they are doing multiple sub-jobs
#   # at the same place. We are ultimately only interested in how much time was shared between
#   # two workers. If one of those appears many times in the same workplace, still it should be
#   # counted as once. So solution for now is to just keep unique values.
#   workplaces_df <- workplaces_df %>% unique(by = c('pis_encoded', 'workplace'))
#   
#   # # Keep only the rows corresponding to concursado entrants.
#   # workplaces_df_concursado <- workplaces_df[pis_encoded %in% entrants_pis_concursado,]
#   # 
#   # # Keep only the rows corresponding to concursado entrants.
#   # workplaces_df_nonconcursado <- workplaces_df[pis_encoded %in% entrants_pis_nonconcursado,]
#   
#   # # Save the entrants' batches now.
#   # if(period_param == '1_1985') {
#   #   
#   #   data.table::fwrite(workplaces_df_concursado, glue('{harddrive}/workplaces_batchwise_2/workplaces_concursado.csv'),
#   #                      append = F)
#   #   data.table::fwrite(workplaces_df_nonconcursado, glue('{harddrive}/workplaces_batchwise_2/workplaces_nonconcursado.csv'),
#   #                      append = F)
#   #   
#   # } else {
#   #   
#   #   data.table::fwrite(workplaces_df_concursado, glue('{harddrive}/workplaces_batchwise_2/workplaces_concursado.csv'),
#   #                      append = T)
#   #   data.table::fwrite(workplaces_df_nonconcursado, glue('{harddrive}/workplaces_batchwise_2/workplaces_nonconcursado.csv'),
#   #                      append = T)
#   #   
#   # } # Ends if-else.
#   
#   # rm(workplaces_df_concursado)
#   # rm(workplaces_df_nonconcursado)
#   
#   # Now split by batches.
#   for(batch_number in 1:(references_in_batches$batch[nrow(references_in_batches)])) {
#     
#     message(glue('BATCH {batch_number} IN {month_param}/{year_param}'))
#     
#     # Get a vector of the pis codes in this batch.
#     pis_in_batch <- references_in_batches[batch == batch_number]$pis_encoded %>% unique()
#     
#     # Get the subset of the workplaces.
#     workplaces_df_batch <- workplaces_df[pis_encoded %in% pis_in_batch,]
#     
#     # Save to a separate batch file (in the hard-drive).
#     if(period_param == '1_1985') {
#       data.table::fwrite(workplaces_df_batch, glue('{harddrive}/intermediate/workplaces_batchwise_2/workplaces_batch_{batch_number}.csv'),
#                          append = F)
#     } else {
#       data.table::fwrite(workplaces_df_batch, glue('{harddrive}/intermediate/workplaces_batchwise_2/workplaces_batch_{batch_number}.csv'),
#                          append = T)
#     } # Ends if-else.
#     
#     rm(workplaces_df_batch)
#     
#   } # Ends for-loop over batches.
#   
#   rm(workplaces_df)
#   gc()
#   
#   message(glue('----------------------------------------------------'))
#   
#   return(NULL)
#   
# } # Ends function, then call it again for next period.
# ##################
# 
# ##################
# # We need these things before starting the function.
# periods <- data.table('year' = c(rep('1985', 12),
#                                  rep('1986', 12),
#                                  rep('1987', 12)),
#                       'month' = rep(1:12, 3))
# 
# # Add month-year column.
# periods <- periods[, monthyear := paste(month, year, sep = '_')]
# 
# # # Load in the entrants' data.
# # entrants <- data.table::fread(glue('{harddrive}/public_sector_workers/entrants_public_sector.csv'),
# #                               colClasses = rep('character', 11))
# # 
# # # Now record the pis codes of entrants, separately for concursado and non-concursado. 
# # # This will be helpful in the next step.
# # entrants_pis_concursado <- unique(entrants[concursado == 1]$pis_encoded)
# # entrants_pis_nonconcursado <- unique(entrants[concursado == 0]$pis_encoded)
# 
# lapply(periods$monthyear, FUN = redo_workplaces_fn)
# 
# rm(periods)
# rm(references_in_batches)
# 
# gc()
# ################## 
# ###############################################################################################






###############################################################################################
################## Now to compute the placebo connections.
# First, load in entrants' workplaces data.
workplaces_entrants_concursado <- data.table::fread(glue('{harddrive}/intermediate/public_sector_workers/workplaces_entrants_concursado.csv'),
                                                    colClasses = c('character', 'numeric', 'character', 'character', 'character', 'character', 'character', 'numeric', 'numeric'),
                                                    select = c('workplace_establishment', 'public', 'workplace', 'establishment', 'pis_encoded', 'monthyear_workplace', 'cohort'))

# Now load in the references data.
references <- data.table::fread(glue('{harddrive}/intermediate/public_sector_workers/references_public_sector.csv'),
                                colClasses = c('character', 'numeric', 'character', 'character', 'numeric'),
                                select = c('reference_year', 'establishment', 'pis_encoded', 'concursado'))

# Now we will load in the workplace data for the references and then we will match. We do this establishment-cohort-year wise.
# First get a list of these establishment-cohort pairs. These will go in as parameters for parMapply.
estabs_cohortyears <- workplaces_entrants_concursado[, c('establishment', 'cohort')] %>% unique()

# Now load in crosswalk between establishments and batches.
references_in_batches <- data.table::fread(glue('{harddrive}/intermediate/public_sector_workers/references_public_sector_batches.csv'),
                                           colClasses = c('character', 'character', 'numeric'))

data.table::setorder(references_in_batches, batch)

# Batch number wise now compute connections.
batch_numbers <- 1:(references_in_batches$batch[nrow(references_in_batches)])

# Define the function that computes placebo connections.
placebo_connection_fn <- function(estab_cohort_param) {
  
  estab_param <- stringr::str_split(estab_cohort_param, pattern = '\\|')[[1]][1]
  cohort_param <- stringr::str_split(estab_cohort_param, pattern = '\\|')[[1]][2]
  
  message(glue('Working on year {which(estabs_cohortyears_in_batch[establishment == estab_param]$cohort == cohort_param)} of {length(unique(estabs_cohortyears_in_batch[establishment == estab_param]$cohort))} for establishment {which(unique(estabs_cohortyears_in_batch$establishment) == estab_param)} of {length(unique(estabs_cohortyears_in_batch$establishment))} in this batch.'))
  message(glue('---------------------------------------------------------'))
  
  workplaces_entrants_concursado_subset <- workplaces_entrants_concursado[establishment == estab_param,][cohort == cohort_param,]
  references_subset <- references[establishment == estab_param,][reference_year == cohort_param, c('pis_encoded', 'concursado')]
  
  # Now filter workplaces to have only those for the references for that estab-year.
  workplaces_references_batch_subset <- workplaces_references_batch[references_subset, on = .(pis_encoded)][year_workplace < cohort_param]
  
  rm(references_subset)
  
  # Join the two workplace datasets now - workplaces_entrants_concursado_subset and workplaces_references_batch_subset
  # Set the keys first for faster compute.
  data.table::setkey(workplaces_entrants_concursado_subset,
                     workplace)
  data.table::setkey(workplaces_references_batch_subset,
                     workplace)
  
  # First find out those who are actually connected (these cannot be placebo connections).
  connected_references <- workplaces_references_batch_subset[workplaces_entrants_concursado_subset, .(i.pis_encoded, pis_encoded), nomatch = 0, allow.cartesian = T][(pis_encoded != i.pis_encoded) | is.na(pis_encoded),][, actually_connected := 1] %>% unique()
  
  joined_workplaces <- workplaces_references_batch_subset[workplaces_entrants_concursado_subset, .(establishment, i.pis_encoded, cohort, pis_encoded, monthyear_workplace, concursado, public), nomatch = 0, allow.cartesian = T][(pis_encoded != i.pis_encoded) | is.na(pis_encoded),]
  
  # If a person A and a person B worked at the same establishment xyz in month m. And then in the same month
  # also worked at a different establishment abc in month m itself.
  # In such a case, I don't want to double count the number of shared months between A and B to be 2. It is just 1.
  # To do this, take only unique rows from the above dataset based on establishment, i.pis_encoded, cohort, monthyear and pis_encoded.
  # This will make sure that when an entrant (combination of estab, i.pis and cohort) appears twice (or more)
  # with the same reference (pis_encoded) in the same month-year, only one instance is considered.
  joined_workplaces <- unique(joined_workplaces, by = c('i.pis_encoded', 'monthyear_workplace', 'pis_encoded'))
  
  ## STEP 4: COLLAPSE TO GET JOB-COHORT LEVEL DATA. OUR MAIN VARIABLES OF INTEREST ARE,
  ## n_connections : Number of connections of entrant with workers who already worked at the establishment on joining year (references).
  ## sum_time_connected : Sum of number of months shared with each connection.
  ## n_connections_public : Number of connections who shared a public sector workplace with the entrant.
  ## n_connections_private : Number of connections who shared a private sector workplace with the entrant.
  ## n_connections_conc : Number of connections who are concursado type in current establishment.
  ## n_connections_nonconc : Number of connections who are non-concursado type in current establishment.
  
  # First set the keys for faster compute.
  data.table::setkey(joined_workplaces,
                     i.pis_encoded)
  
  # First we do n_connections and sum_time_connected.
  collapsed_sum_time_connected <- joined_workplaces[, .(sum_time_connected = .N), by = .(i.pis_encoded, establishment, cohort)]
  
  # Now, n_connections_public and private.
  collapsed_n_connections_public <- joined_workplaces[public == 1, .(n_connections_public = uniqueN(pis_encoded)), by = .(i.pis_encoded, establishment, cohort)]
  collapsed_n_connections_private <- joined_workplaces[public == 0, .(n_connections_private = uniqueN(pis_encoded)), by = .(i.pis_encoded, establishment, cohort)]
  
  # Finally, n_connections, n_connections_conc and n_connections_nonconc.
  joined_workplaces <- unique(joined_workplaces, 
                              by = c('pis_encoded',
                                     'i.pis_encoded'))
  
  collapsed_n_connections <- joined_workplaces[, .(n_connections = .N, n_connections_conc = sum(concursado, na.rm = T)), by = .(i.pis_encoded, establishment, cohort)]
  collapsed_n_connections[, n_connections_nonconc := n_connections - n_connections_conc]
  
  collapsed_data <- data.table::merge.data.table(collapsed_n_connections, collapsed_sum_time_connected, all = T)
  collapsed_data <- data.table::merge.data.table(collapsed_data, collapsed_n_connections_public, all = T)
  collapsed_data <- data.table::merge.data.table(collapsed_data, collapsed_n_connections_private, all = T)
  
  # Change the NAs to 0s.
  collapsed_data[is.na(collapsed_data)] <- 0

  rm(joined_workplaces)
  rm(collapsed_n_connections)
  rm(collapsed_n_connections_public)
  rm(collapsed_n_connections_private)
  rm(collapsed_sum_time_connected)

  # Now join for placebo networks i.e. same workplace establishment (not workplace!)
  # Re-set the keys first for faster compute.
  data.table::setkey(workplaces_entrants_concursado_subset,
                     NULL)
  data.table::setkey(workplaces_references_batch_subset,
                     NULL)
  data.table::setkey(workplaces_entrants_concursado_subset,
                     workplace_establishment)
  data.table::setkey(workplaces_references_batch_subset,
                     workplace_estab)
  
  joined_workplaces <- workplaces_references_batch_subset[workplaces_entrants_concursado_subset, .(establishment, i.pis_encoded, cohort, pis_encoded), nomatch = 0, allow.cartesian = T][(pis_encoded != i.pis_encoded) | is.na(pis_encoded),]

  # Now remove the matches corresponding to actually connected references.
  data.table::setkey(connected_references,
                     pis_encoded,
                     i.pis_encoded)
  data.table::setkey(joined_workplaces,
                     pis_encoded,
                     i.pis_encoded)
  
  joined_workplaces <- connected_references[joined_workplaces, .(establishment, i.pis_encoded, cohort, pis_encoded, actually_connected)]
  joined_workplaces <- joined_workplaces[actually_connected != 1,][, actually_connected := NULL]
  
  rm(workplaces_references_batch_subset)
  rm(workplaces_entrants_concursado_subset)
  
  joined_workplaces <- unique(joined_workplaces, 
                              by = c('pis_encoded',
                                     'i.pis_encoded'))
  
  data.table::setkey(joined_workplaces,
                     i.pis_encoded)
  
  collapsed_n_connections_placebo <- joined_workplaces[, .(n_connections_placebo = .N), by = .(i.pis_encoded, establishment, cohort)]
  
  collapsed_data <- data.table::merge.data.table(collapsed_data, collapsed_n_connections_placebo, all = T)
  
  # Change the NAs to 0s.
  collapsed_n_connections_placebo[is.na(collapsed_n_connections_placebo)] <- 0
  collapsed_data[is.na(collapsed_data)] <- 0

  colnames(collapsed_data)[1] <- 'pis_encoded'
  colnames(collapsed_n_connections_placebo)[1] <- 'pis_encoded'
  
  rm(joined_workplaces)
  
  # # Now calculate the connections.
  # connections_count_fn <- function(x, y, z) {
  #   
  #   # First find out those who are actually connected (these cannot be placebo connections).
  #   # If a person A and a person B worked at the same establishment xyz in month m. And then in the same month
  #   # also worked at a different establishment abc in month m itself.
  #   # In such a case, I don't want to double count the number of shared months between A and B to be 2. It is just 1.
  #   # To do this, take only unique rows from the above dataset based on establishment, i.pis_encoded, cohort, monthyear and pis_encoded.
  #   # This will make sure that when an entrant (combination of estab, i.pis and cohort) appears twice (or more)
  #   # with the same reference (pis_encoded) in the same month-year, only one instance is considered.
  #   conns_subset <- workplaces_references_batch_subset[pis_encoded != z,][workplace %in% x,] %>% unique(by = c('pis_encoded', 'monthyear_workplace'))
  #   
  #   # Compute sum of shared months with all connections. We will convert this to average months later.
  #   sum_time_connected <- conns_subset %>% nrow()
  #   
  #   n_connections_public <- conns_subset[public == 1, c('pis_encoded')] %>% unique() %>% nrow()
  #   n_connections_private <- conns_subset[public == 0, c('pis_encoded')] %>% unique() %>% nrow()
  #   
  #   conns_subset <- conns_subset[, c('pis_encoded', 'concursado')] %>% unique(by = c('pis_encoded'))
  #   n_connections <- conns_subset %>% nrow()
  #   n_connections_conc <- sum(conns_subset$concursado)
  #   n_connections_nonconc <- n_connections - n_connections_conc
  #   
  #   # Now filter to keep out the actually connected pis codes and find placebo connections.
  #   n_connections_placebo <- workplaces_references_batch_subset[pis_encoded != z,][!(pis_encoded %in% conns_subset$pis_encoded),][workplace_estab %in% y, c('pis_encoded')] %>% unique() %>% nrow()
  #   
  #   rm(conns_subset)
  #   
  #   connection_counts <- list(sum_time_connected, n_connections, n_connections_public, n_connections_private, n_connections_conc, n_connections_nonconc, n_connections_placebo)
  #   
  #   return(connection_counts)
  #   
  # }
  # 
  # workplaces_entrants_concursado_subset[, c('sum_time_connected', 'n_connections', 'n_connections_public', 'n_connections_private', 'n_connections_conc', 'n_connections_nonconc', 'n_connections_placebo') := (connections_count_fn(workplace, workplace_establishment, pis_encoded)), by = pis_encoded]
  
  # workplaces_entrants_concursado_subset <- workplaces_entrants_concursado_subset[, c('pis_encoded', 'establishment', 'cohort', 'sum_time_connected', 'n_connections', 'n_connections_public', 'n_connections_private', 'n_connections_conc', 'n_connections_nonconc', 'n_connections_placebo')] %>% unique(by = c('pis_encoded'))
  
  return(collapsed_data)
  
}

# This function will be applied to each batch and then combined.
for(batch_param in batch_numbers) {
  
  message(glue('Working on batch {batch_param} of {length(batch_numbers)}.'))
  message(glue('---------------------------------------------------------'))
  
  # First load in the workplace data of this batch.
  workplaces_references_batch <- data.table::fread(glue('{harddrive}/intermediate/workplaces_batchwise_2/workplaces_batch_{batch_param}.csv'),
                                  colClasses = c('character', 'character', 'numeric', 'character'))
  
  # Create a workplace_establishment variable.
  workplaces_references_batch[, c('cnpj', 'munic') := data.table::tstrsplit(workplace, "_", fixed = TRUE, keep = 1:2)]
  workplaces_references_batch[, `:=`(workplace_estab = paste(cnpj, munic, sep = '_'), cnpj = NULL, munic = NULL)]
  
  # Add a public establishment indicator.
  workplaces_references_batch <- workplaces_entrants_concursado[, c('public', 'workplace')][workplaces_references_batch, on = .(workplace)]
  
  # Note the establishments in this batch.
  estabs_in_batch <- references_in_batches[batch == batch_param, c('establishment')] %>% unique()
  
  # Note the establishment-cohorts in this batch.
  estabs_cohortyears_in_batch <- estabs_cohortyears[establishment %in% estabs_in_batch$establishment]
  data.table::setorder(estabs_cohortyears_in_batch, establishment, cohort)
  
  estabs_cohortyears_in_batch[, estab_cohort := paste(establishment, cohort, sep = '|')]
  
  placebo_connections_estabcohort_list <- lapply(estabs_cohortyears_in_batch$estab_cohort, FUN = placebo_connection_fn)
  
  gc()
  
  placebo_connections_estabcohort_df <- data.table::rbindlist(placebo_connections_estabcohort_list)
  
  # Write it to a separate csv.
  data.table::fwrite(placebo_connections_estabcohort_df, file = glue('{harddrive}/intermediate/connections2/collapsed_data_{batch_param}.csv'))
  # data.table::fwrite(collapsed_data_one_year_old, file = glue('{harddrive}/intermediate/connections/collapsed_data_{batch_param}_one_year_old.csv'))
  
  rm(placebo_connections_estabcohort_list)
  rm(estabs_cohortyears_in_batch)
  rm(estabs_in_batch)
  rm(workplaces_references_batch)
  rm(placebo_connections_estabcohort_df)
  
  write(batch_param, file = glue('{harddrive}/intermediate/connections2/completed_connections_match_concursado.txt'), append = T, sep = '\n')
  
}

collapsed_files <- list.files(path = glue('{harddrive}/intermediate/connections2/'), pattern = 'collapsed')
collapsed_files_one_year_old <- list.files(path = glue('{harddrive}/intermediate/connections2/'), pattern = '*_one_year_old')
collapsed_files <- setdiff(collapsed_files, collapsed_files_one_year_old)

# Start cluster.
cl <- makePSOCKcluster(detectCores()-1)
setDefaultCluster(cl)
clusterExport(NULL, c('harddrive', 'collapsed_files', 'collapsed_files_one_year_old'))
clusterEvalQ(cl, {library(data.table); library(glue); library(stringr); library(purrr)})
#
# Run the function.
connected_entrants <- parLapplyLB(NULL, collapsed_files, fun = function(x) {
  df <- data.table::fread(glue('{harddrive}/intermediate/connections2/{x}'),
                          colClasses = c('character',
                                         'character',
                                         rep('numeric', 8)))
  return(df)
})

# connected_entrants_one_year <- parLapplyLB(NULL, collapsed_files_one_year_old, fun = function(x) {
#   df <- data.table::fread(glue('{harddrive}/intermediate/connections/{x}'),
#                           colClasses = c('character', 
#                                          'numeric', 
#                                          'character', 
#                                          rep('numeric', 6)))
#   return(df)
# })
#
# Close cluster.
stopCluster(cl)
rm(cl)
#

# Append them.
connected_entrants <- rbindlist(connected_entrants)
# connected_entrants_one_year <- rbindlist(connected_entrants_one_year)

# Now collapse for each cohort-job.
connected_entrants <- connected_entrants[, .(n_connections = sum(n_connections, na.rm = T),
                                             n_connections_conc = sum(n_connections_conc, na.rm = T),
                                             n_connections_nonconc = sum(n_connections_nonconc, na.rm = T),
                                             sum_time_connected = sum(sum_time_connected, na.rm = T),
                                             n_connections_public = sum(n_connections_public, na.rm = T),
                                             n_connections_private = sum(n_connections_private, na.rm = T),
                                             n_connections_placebo = sum(n_connections_placebo, na.rm = T))
                                         , by = .(pis_encoded, cohort, establishment)]
# connected_entrants_one_year <- connected_entrants_one_year[, .(n_connections = sum(n_connections, na.rm = T),
#                                                                n_connections_conc = sum(n_connections_conc, na.rm = T),
#                                                                n_connections_nonconc = sum(n_connections_nonconc, na.rm = T),
#                                                                sum_time_connected = sum(sum_time_connected, na.rm = T),
#                                                                n_connections_public = sum(n_connections_public, na.rm = T),
#                                                                n_connections_private = sum(n_connections_private, na.rm = T),
#                                                                n_connections_placebo = sum(n_connections_placebo, na.rm = T))
#                                                            , by = .(pis_encoded, cohort, establishment)]

# Now calculate the average_time_connected and has_connections variables.
connected_entrants[, `:=`(avg_time_connected = (sum_time_connected/n_connections),
                          has_connections = 1)]

# connected_entrants_one_year[, `:=`(avg_time_connected = (sum_time_connected/n_connections),
#                                    has_connections = 1)]

# Now add in the entrants who have 0 connections. Do this by using the cohorts dataset
# and then matching with the new values we created. Those which didn't match get a has_connections
# value of 0.
entrants <- data.table::fread(glue('{box}/intermediate/public_sector_workers/entrants_public_sector.csv'),
                              colClasses = c('character',
                                             'numeric',
                                             rep('character', 6),
                                             'numeric',
                                             rep('character', 2)))

entrants_concursado <- entrants[concursado == 1,]
entrants_concursado[, concursado := NULL]

rm(entrants)

final_data_connections <- connected_entrants[entrants_concursado, on = .(pis_encoded, cohort, establishment)]
# final_data_connections_one_year <- connected_entrants_one_year[entrants_concursado, on = .(pis_encoded, cohort, establishment)]

# Convert the NAs to zeroes.
final_data_connections[is.na(final_data_connections)] <- 0
# final_data_connections_one_year[is.na(final_data_connections_one_year)] <- 0

# Save once then comment out everything to avoid overwrite.
# data.table::fwrite(final_data_connections, glue('{harddrive}/intermediate/connections/final_data_connections_concursado_entrants.csv'))
# data.table::fwrite(final_data_connections_one_year, glue('{harddrive}/intermediate/connections/final_data_connections_one_year_concursado_entrants.csv'))
################## 
###############################################################################################

# Compare. YES!
ddd <- fread(glue('{box}/intermediate/connections/final_data_connections_concursado_entrants.csv'))



