###############################################################################################
################## INTRO
# MOHIT NEGI
# Last Updated : 8 October 2023
# Contact on : mohit.negi@studbocconi.it
################## 

################## OBJECTIVES
# This script performs the following tasks.
# 1. Placebo Networks
##################

################## PATHS
raw <- 'C:/Users/mohit/Desktop/EP/TM/TM_Data/Raw'
cleaned <- 'C:/Users/mohit/Desktop/EP/TM/TM_Data/Cleaned'
output <- 'C:/Users/mohit/Desktop/EP/TM/TM_Data/Output'
box <- 'C:/Users/mohit/Box/mohit_ra'
harddrive <- 'D:/Mohit_Work/MOCANU'
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






###############################################################################################
##################
year_param <- 1988
month_param <- 5
workplaces_df <- data.table::fread(glue('{box}/intermediate/workplaces/workplaces_{year_param}/workplacedata_month{month_param}_{year_param}.csv'),
                                   colClasses = c(rep('character', 2)))

sample <- sample_n(workplaces_df, 10000)

sample2 <- workplaces_df[workplace == '96923487000153_430535_1988_5']

# 96923487000153_430535_1988_5, 120191630811, 122726131701
mylines <- read.csv.sql(glue('{box}/intermediate/workplaces/workplaces_{year_param}/workplacedata_month{month_param}_{year_param}.csv'), 
                        sql = "select * from file where `pis_encoded` in (120191630811, 122726131701)", eol = "\n")


workplace_names <- unique(sample$workplace)
workplace_names_string <- paste0('"', paste(workplace_names, collapse = '", "'), '"')

mylines <- read.csv.sql(glue('{box}/intermediate/workplaces/workplaces_{year_param}/workplacedata_month{month_param}_{year_param}.csv'), 
                        sql = glue('select * from file where `workplace` in ({print(workplace_names_string, quote = F)})'), eol = "\n")

entrants_workplaces_nonconcursado <- data.table::fread(glue('{harddrive}/workplaces_batchwise_2/workplaces_entrants_nonconcursado.csv'),
                                                       colClasses = c('character', 'numeric', 'character', 'character', 'character', 'character', 'character', 'numeric', 'numeric'))

entrants_workplaces_nonconcursado_thismonthyear <- entrants_workplaces_nonconcursado[monthyear_workplace == '5_1988']

workplace_names <- unique(entrants_workplaces_nonconcursado_thismonthyear$workplace)
workplace_names_string <- paste0('"', paste(workplace_names, collapse = '", "'), '"')

mylines <- read.csv.sql(glue('{box}/intermediate/workplaces/workplaces_{year_param}/workplacedata_month{month_param}_{year_param}.csv'), 
                        sql = glue('select * from file where `workplace` in ({print(workplace_names_string, quote = F)})'), eol = "\n")


entrants <- data.table::fread(glue('{cleaned}/entrants_public_sector.csv'),
                              colClasses = rep('character', 11))

length(unique(entrants$establishment))
###############################################################################################






###############################################################################################
##################
# Idea : Split references into different batches but according to the establishment. There will be
# duplication.
entrants <- data.table::fread(glue('{cleaned}/entrants_public_sector.csv'),
                              colClasses = rep('character', 11))

references <- data.table::fread(glue('{cleaned}/references_public_sector.csv'),
                                colClasses = rep('character', 5))

relevant_estab_cohorts <- entrants[, c('establishment', 'cohort')] %>% unique()

references_relevant <- references[relevant_estab_cohorts, on = .(establishment, reference_year = cohort), nomatch = 0]
references_relevant <- references_relevant[, c('establishment', 'pis_encoded')] %>% unique()

rm(entrants)
rm(references)
rm(relevant_estab_cohorts)

# Now get row numbers of each establishment chunk start.
data.table::setorder(references_relevant, 'establishment')

# Get a count of references by establishment. 
workernumbers <- references_relevant[, .(count = .N), by = 'establishment']
setorder(workernumbers, 'count')

# Add a cumulative sum column.
workernumbers[, cumul_count := cumsum(count)]

# Find the closest multiple of 100,000 for the particular cumulative sum.
workernumbers[, batch := plyr::round_any(cumul_count, 10000, f = ceiling)]

# To find the batch number, divide by 100,000.
workernumbers[, batch := batch/10000]

# Keep only relevant columns
estab_batches_crosswalk <- workernumbers[, c('establishment', 'cumul_count', 'batch')]
rm(workernumbers)

# Create a dataframe with just the batch numbers.
batchnumbers <- data.frame('batch' = 1:estab_batches_crosswalk$batch[nrow(estab_batches_crosswalk)])

# Now join so that we have all batch numbers.
estab_batches_crosswalk <- estab_batches_crosswalk[batchnumbers, on = .(batch)]

rm(batchnumbers)

# Fill the establishment values from the ones below. All this is because some estabs are so big that
# they don't fit in a single batch.
estab_batches_crosswalk$establishment <- zoo::na.locf(estab_batches_crosswalk$establishment, fromLast = TRUE)

# Label the subbatch numbers within a batch.
estab_batches_crosswalk[, subbatch := 1:.N, by = establishment]

# Now we have a crosswalk between the establishments and the corresponding workplace batches they reside in.
# Now match references with batches.
references_relevant[, subbatch := 1:.N, by = establishment]
references_relevant[, subbatch := ceiling(subbatch/10000)]

references_relevant <- estab_batches_crosswalk[references_relevant, on = .(establishment, subbatch)]

rm(estab_batches_crosswalk)

data.table::setorder(references_relevant, establishment, subbatch)

references_relevant[, batch := zoo::na.locf(batch)]

# Now it should divide them up beautifully!
collapsed <- references_relevant[, .(batch_count = .N), by = batch]

rm(collapsed)

# Final dataset to create the batches.
references_in_batches <- references_relevant[, c('pis_encoded', 'establishment', 'batch')]

rm(references_relevant)

# Save.
# data.table::fwrite(references_in_batches, file = glue('{harddrive}/public_sector_workers/references_public_sector_batches.csv'))
rm(references_in_batches)
###############################################################################################






###############################################################################################
################## CREATE BATCH-WISE WORKPLACE HISTORY DATA.
# Currently, we have separate files for each period (month-year). This is because the entire
# dataset, if left as a single file, will be way too large. However, for the purposes of 
# calculating connections, working with one file at a time will not be feasible since
# we will need to track if the same two people were already connected in a previous period
# workplace. If we do not keep track, we'll count this single connection many times.

# The solution is to still split the workplaces, but instead of splitting by period, we split them
# by pis codes. So divide all the pis codes (only the relevant ones from above) into batches, and
# each file contains workplaces corresponding to pis codes in only that batch for all periods.
# This way, we calculate the connections separately with each batch (so the computer can handle it)
# and then sum over the connections across batches. This way it will be valid and no double-counting
# will happen.

# Here is the plan:
# 1. Divide the relevant pis codes into smaller batches.
# 2. Load a period's workplaces file (say january 1985) from the Box folder.
# 3. Subset it by keeping only rows corresponding to pis codes in batch 1.
# 4. Save it to a batch file : workplaces_batch1.csv and so on.
# 5. Repeat for batch 2, 3, .... and so on.
# 6. Then next period (february 1985 and so on), this time append to same workplaces csv's.
################## 

##################
references_in_batches <- data.table::fread(glue('{harddrive}/public_sector_workers/references_public_sector_batches.csv'),
                                           colClasses = c('character', 'character', 'numeric'))
data.table::setorder(references_in_batches, batch)
##################

################## 
redo_workplaces_fn <- function(period_param){
  
  # Extract the year and month from the period.
  year_param <- stringr::str_split(period_param, pattern = '_')[[1]][2]
  month_param <- stringr::str_split(period_param, pattern = '_')[[1]][1]
  
  message(glue('WORKING ON MONTH {month_param} OF YEAR {year_param}.'))
  message(glue('----------------------------------------------------'))
  
  # Load in the appropriate dataset.
  workplaces_df <- data.table::fread(glue('{box}/intermediate/workplaces/workplaces_{year_param}/workplacedata_month{month_param}_{year_param}.csv'),
                                     colClasses = c(rep('character', 2)))
  
  # Add year and month-year variables. This will be useful later.
  workplaces_df <- workplaces_df[, `:=`(year_workplace = year_param, monthyear_workplace = paste(month_param, year_param, sep = '_'))]
  
  # Many many duplicates i.e. same pis appearing in the same workplace many times.
  # Most of these seem to be because of catch-all pis codes which do not correspond to a single
  # individual. Other times, it might be because the same worker appeared many times in the same
  # year's data in the same establishment. This is because they are doing multiple sub-jobs
  # at the same place. We are ultimately only interested in how much time was shared between
  # two workers. If one of those appears many times in the same workplace, still it should be
  # counted as once. So solution for now is to just keep unique values.
  workplaces_df <- workplaces_df %>% unique(by = c('pis_encoded', 'workplace'))
  
  # # Keep only the rows corresponding to concursado entrants.
  # workplaces_df_concursado <- workplaces_df[pis_encoded %in% entrants_pis_concursado,]
  # 
  # # Keep only the rows corresponding to concursado entrants.
  # workplaces_df_nonconcursado <- workplaces_df[pis_encoded %in% entrants_pis_nonconcursado,]
  
  # # Save the entrants' batches now.
  # if(period_param == '1_1985') {
  #   
  #   data.table::fwrite(workplaces_df_concursado, glue('{harddrive}/workplaces_batchwise_2/workplaces_concursado.csv'),
  #                      append = F)
  #   data.table::fwrite(workplaces_df_nonconcursado, glue('{harddrive}/workplaces_batchwise_2/workplaces_nonconcursado.csv'),
  #                      append = F)
  #   
  # } else {
  #   
  #   data.table::fwrite(workplaces_df_concursado, glue('{harddrive}/workplaces_batchwise_2/workplaces_concursado.csv'),
  #                      append = T)
  #   data.table::fwrite(workplaces_df_nonconcursado, glue('{harddrive}/workplaces_batchwise_2/workplaces_nonconcursado.csv'),
  #                      append = T)
  #   
  # } # Ends if-else.
  
  # rm(workplaces_df_concursado)
  # rm(workplaces_df_nonconcursado)
  
  # Now split by batches.
  for(batch_number in 1:(references_in_batches$batch[nrow(references_in_batches)])) {
    
    message(glue('BATCH {batch_number} IN {month_param}/{year_param}'))
    
    # Get a vector of the pis codes in this batch.
    pis_in_batch <- references_in_batches[batch == batch_number]$pis_encoded %>% unique()
    
    # Get the subset of the workplaces.
    workplaces_df_batch <- workplaces_df[pis_encoded %in% pis_in_batch,]
    
    # Save to a separate batch file (in the hard-drive).
    if(period_param == '1_1985') {
      data.table::fwrite(workplaces_df_batch, glue('{harddrive}/workplaces_batchwise_3/workplaces_batch_{batch_number}.csv'),
                         append = F)
    } else {
      data.table::fwrite(workplaces_df_batch, glue('{harddrive}/workplaces_batchwise_3/workplaces_batch_{batch_number}.csv'),
                         append = T)
    } # Ends if-else.
    
    rm(workplaces_df_batch)
    
  } # Ends for-loop over batches.
  
  rm(workplaces_df)
  gc()
  
  message(glue('----------------------------------------------------'))
  
  return(NULL)
  
} # Ends function, then call it again for next period.
##################

##################
# We need these things before starting the function.
periods <- data.table('year' = c(rep('1985', 12),
                                 rep('1986', 12),
                                 rep('1987', 12),
                                 rep('1988', 12),
                                 rep('1989', 12),
                                 rep('1990', 12),
                                 rep('1991', 12),
                                 rep('1992', 12),
                                 rep('1993', 12),
                                 rep('1994', 12),
                                 rep('1995', 12)),
                      'month' = rep(1:12, 11))

# Add month-year column.
periods <- periods[, monthyear := paste(month, year, sep = '_')]

# # Load in the entrants' data.
# entrants <- data.table::fread(glue('{cleaned}/entrants_public_sector.csv'),
#                               colClasses = rep('character', 11))
# 
# # Now record the pis codes of entrants, separately for concursado and non-concursado. 
# # This will be helpful in the next step.
# entrants_pis_concursado <- unique(entrants[concursado == 1]$pis_encoded)
# entrants_pis_nonconcursado <- unique(entrants[concursado == 0]$pis_encoded)

lapply(periods$monthyear, FUN = redo_workplaces_fn)

rm(periods)
rm(entrants_pis_concursado)
rm(entrants_pis_nonconcursado)

gc()
################## 
###############################################################################################






###############################################################################################
################## Now to compute the placebo connections.

# First, load in entrants' workplaces data.
entrants_workplaces_concursado <- data.table::fread(glue('{harddrive}/workplaces_batchwise_2/workplaces_entrants_concursado.csv'),
                                                       colClasses = c('character', 'numeric', 'character', 'character', 'character', 'character', 'character', 'numeric', 'numeric'))

entrants_workplaces_concursado[, workplace_establishment := purrr::map_chr(stringr::str_split(workplace, pattern = '_'), 1)]

# Remove the columns not relevant for the match.
entrants_workplaces_concursado[, `:=`(workplace_establishment = NULL,
                                         job = NULL,
                                         year_workplace = NULL)]

# Set key for faster matching.
data.table::setkey(entrants_workplaces_concursado,
                   workplace,
                   establishment,
                   cohort)

gc()

# Now load in the references data.
references <- data.table::fread(glue('{cleaned}/references_public_sector.csv'),
                                colClasses = c('character', 'numeric', 'character', 'character', 'numeric'))

# Now we will load in the workplace data for the references and then we will match. We do this establishment
# - cohort-year wise.
# First get a list of these establishment-cohort pairs. These will go in as parameters for parMapply.
estabs_cohortyears <- entrants_workplaces_concursado[, c('establishment', 'cohort')] %>% unique()

# Define the function that computes placebo connections.
placebo_connection_fn <- function(estab_param, cohort_param) {
  
  message(glue('Working on establishment {which(unique(estabs_cohortyears_in_batch$establishment) == estab_param)} out of {length(unique(estabs_cohortyears_in_batch$establishment))} and year {which(estabs_cohortyears_in_batch[establishment == estab_param]$cohort == cohort_param)} out of {length(unique(estabs_cohortyears_in_batch[establishment == estab_param]$cohort))}'))
  message(glue('-------------------------------------'))
  
  entrants_workplaces_subset <- entrants_workplaces_concursado[establishment == estab_param & cohort == cohort_param,]
  references_subset <- references[establishment == estab_param & reference_year == cohort_param,]
  
  # Now filter workplaces to have only those for the references for that estab-year.
  workplaces_subset <- workplaces[pis_encoded %in% unique(references_subset$pis_encoded),]
  
  rm(references_subset)
  
  # Now calculate the connections.
  subsetter_fn <- function(x,y) {
    
    ws <- workplaces_subset[(!(workplace %in% x)) & 
                        (workplace_establishment %in% y), c('pis_encoded')]
    
    return(ws)
    
  }
  
  entrants_workplaces_subset[, n_placebo_conns := (subsetter_fn(workplace, workplace_establishment) %>% 
    unique() %>% nrow()), by = .(pis_encoded, cohort)]
  
  df <- entrants_workplaces_subset[, c('pis_encoded', 'establishment', 'cohort', 'n_placebo_conns')]
  
  rm(entrants_workplaces_subset)
  rm(workplaces_subset)
  
  return(df)
  
}

# Now load in crosswalk between establishments and batches.
references_in_batches <- data.table::fread(glue('{harddrive}/public_sector_workers/references_public_sector_batches.csv'),
                                           colClasses = c('character', 'character', 'numeric'))
data.table::setorder(references_in_batches, batch)

# Batch number wise now compute connections.
batch_numbers <- 1:(references_in_batches$batch[nrow(references_in_batches)])

for(batch_param in batch_numbers) {
  
  # First load in the workplace data of this batch.
  workplaces <- data.table::fread(glue('{harddrive}/workplaces_batchwise_3/workplaces_batch_{batch_param}.csv'),
                                  colClasses = c('character', 'character', 'numeric', 'character'))
  
  # Create a workplace_establishment variable.
  workplaces[, workplace_establishment := purrr::map_chr(stringr::str_split(workplace, pattern = '_'), 1)]
  
  # Note the establishments in this batch.
  estabs_in_batch <- references_in_batches[batch == batch_param, c('establishment')] %>% unique()
  
  # Note the establishment-cohorts in this batch.
  estabs_cohortyears_in_batch <- estabs_cohortyears[establishment == estabs_in_batch]
  data.table::setorder(estabs_cohortyears_in_batch, establishment, cohort)
  
  df_list <- mapply(placebo_connection_fn, estabs_cohortyears_in_batch$establishment, estabs_cohortyears_in_batch$cohort)
  
  rm(estabs_cohortyears_in_batch)
  rm(estabs_in_batch)
  rm(workplaces)
  
}




