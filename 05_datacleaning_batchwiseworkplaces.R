###############################################################################################
################## INTRO
# MOHIT NEGI
# Last Updated : 8 October 2023
# Contact on : mohit.negi@studbocconi.it
################## 

################## OBJECTIVES
# This script performs the following tasks.
# 1. Creates new workplaces data files. Instead of separate files for each month-year,
# now we will create separate files for batches of workers (entire workplace history
# of a particular worker will be in one file only).
# 2. Create datasets containing all workplace data of entrants, separate for concursado and non-conc entrants.
# 3. Label workplaces in those datasets as public or private sector, which will be helpful for later analysis.
# 4. Create a new crosswalk to match establishment-years to number of employees and new entrants.
##################

################## PATHS
harddrive <- 'D:/Mohit_Work/MOCANU/RAIS_tasks'
box <- 'C:/Users/mohit/Box/mohit_ra'
##################

################## PACKAGES
# We will use these in this script.
packages <- c('glue',
              'data.table',
              'parallel',
              'stringr',
              'purrr',
              'R.utils')

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
load(file = glue('{harddrive}/intermediate/public_sector_workers/pis_codes_public_sector_workers.rda'))
size_of_one_batch <- 5000
pis_codes_public_sector_workers_batches <- split(pis_codes_public_sector_workers, ceiling(seq_along(pis_codes_public_sector_workers) / size_of_one_batch))

# Save once and then comment it out.
# save(pis_codes_public_sector_workers_batches, file = glue('{harddrive}/intermediate/public_sector_workers/pis_codes_public_sector_workers_batches.rda'))
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
  
  # Keep only the rows corresponding to concursado entrants.
  workplaces_df_concursado <- workplaces_df[pis_encoded %in% entrants_pis_concursado,]
  
  # Keep only the rows corresponding to non-concursado entrants.
  workplaces_df_nonconcursado <- workplaces_df[pis_encoded %in% entrants_pis_nonconcursado,]
  
  # Save the entrants' batches now.
  if(period_param == '1_1985') {
    
    data.table::fwrite(workplaces_df_concursado, glue('{harddrive}/intermediate/public_sector_workers/workplaces_concursado.csv'),
                       append = F)
    data.table::fwrite(workplaces_df_nonconcursado, glue('{harddrive}/intermediate/public_sector_workers/workplaces_nonconcursado.csv'),
                       append = F)
    
  } else {
    
    data.table::fwrite(workplaces_df_concursado, glue('{harddrive}/intermediate/public_sector_workers/workplaces_concursado.csv'),
                       append = T)
    data.table::fwrite(workplaces_df_nonconcursado, glue('{harddrive}/intermediate/public_sector_workers/workplaces_nonconcursado.csv'),
                       append = T)
    
  } # Ends if-else.
  
  rm(workplaces_df_concursado)
  rm(workplaces_df_nonconcursado)
  
  # Now split by batches.
  for(batch_number in 1:length(pis_codes_public_sector_workers_batches)) {
    
    message(glue('BATCH {batch_number} IN {month_param}/{year_param}'))
    
    # Get a vector of the pis codes in this batch.
    pis_in_batch <- pis_codes_public_sector_workers_batches[[batch_number]]
    
    # Get the subset of the workplaces.
    workplaces_df_batch <- workplaces_df[pis_encoded %in% pis_in_batch,]
    
    # Save to a separate batch file (in the hard-drive).
    if(period_param == '1_1985') {
      data.table::fwrite(workplaces_df_batch, glue('{harddrive}/intermediate/workplaces_batchwise/workplaces_batch_{batch_number}.csv'),
                         append = F)
    } else {
      data.table::fwrite(workplaces_df_batch, glue('{harddrive}/intermediate/workplaces_batchwise/workplaces_batch_{batch_number}.csv'),
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

# Load in the entrants' data.
entrants <- data.table::fread(glue('{harddrive}/intermediate/public_sector_workers/entrants_public_sector.csv'),
                              colClasses = rep('character', 11))

# Now record the pis codes of entrants, separately for concursado and non-concursado. 
# This will be helpful in the next step.
entrants_pis_concursado <- unique(entrants[concursado == 1]$pis_encoded)
entrants_pis_nonconcursado <- unique(entrants[concursado == 0]$pis_encoded)

# CAUTION : THIS RUNS FOR A LONG TIME.
lapply(periods$monthyear, FUN = redo_workplaces_fn)

rm(periods)
rm(pis_codes_public_sector_workers_batches)
rm(pis_codes_public_sector_workers)
rm(entrants_pis_concursado)
rm(entrants_pis_nonconcursado)

gc()
################## 
###############################################################################################






###############################################################################################
################## NOW, WE SAVE THE ENTRANTS' WORKPLACE DATA.
# Load in the workplaces data we just collected.
workplaces_entrants_concursado <- data.table::fread(glue('{harddrive}/intermediate/public_sector_workers/workplaces_concursado.csv'),
                                                    colClasses = rep('character', 4))
workplaces_entrants_nonconcursado <- data.table::fread(glue('{harddrive}/intermediate/public_sector_workers/workplaces_nonconcursado.csv'),
                                                       colClasses = rep('character', 4))

# Now match these entrants to their workplaces (only those workplaces that are before
# their cohort year).
workplaces_entrants_concursado[, year_workplace := as.numeric(year_workplace)]
workplaces_entrants_nonconcursado[, year_workplace := as.numeric(year_workplace)]

entrants[, cohort := as.numeric(cohort)]

workplaces_entrants_concursado <- workplaces_entrants_concursado[entrants[concursado == 1], on = .(pis_encoded, year_workplace < cohort), .(workplace, job, establishment, pis_encoded, monthyear_workplace, year_workplace = x.year_workplace, cohort), nomatch = 0]
workplaces_entrants_nonconcursado <- workplaces_entrants_nonconcursado[entrants[concursado == 0], on = .(pis_encoded, year_workplace < cohort), .(workplace, job, establishment, pis_encoded, monthyear_workplace, year_workplace = x.year_workplace, cohort), nomatch = 0]

rm(entrants)

# Remove the invalid cases. 
# Recall the main issue : If person x entered estab A in 1974 and then again in 1986. Since we don't have data before 1985,
# the 1986 entrance will be her "first entrance" in estab A according to how we coded it above. However, the same issue
# persists. Since she actually started in 1974, she will be working at estab A in 1985 (she will be in workplace - A_1_1985 for example).
# However, in the data it appears that she is joining in 1986. So we will compare her history with her colleagues, say y.
# y will most probably be working at A in 1985 too (she will be in workplace - A_1_1985 also), 
# so it will appear as if x and y were connected which is technically true but not what we want. We want the cases where x and y shared
# workplaces somewhere else and after some time, y helped refer x to estab A. To focus on these cases, for the new entrants, we remove any workplaces
# that involve the same establishment as the establishment they are entering in their cohort year.
workplaces_entrants_concursado[, c('cnpj', 'munic') := data.table::tstrsplit(workplace, "_", fixed = TRUE, keep = 1:2)]
workplaces_entrants_concursado[, `:=`(workplace_estab = paste(cnpj, munic, sep = '_'), cnpj = NULL, munic = NULL)]
workplaces_entrants_concursado <- workplaces_entrants_concursado[workplace_estab != establishment,]

workplaces_entrants_nonconcursado[, c('cnpj', 'munic') := data.table::tstrsplit(workplace, "_", fixed = TRUE, keep = 1:2)]
workplaces_entrants_nonconcursado[, `:=`(workplace_estab = paste(cnpj, munic, sep = '_'), cnpj = NULL, munic = NULL)]
workplaces_entrants_nonconcursado <- workplaces_entrants_nonconcursado[workplace_estab != establishment,]

# Save once then comment out everything to avoid overwrite.
# data.table::fwrite(workplaces_entrants_concursado, glue('{harddrive}/intermediate/public_sector_workers/workplaces_entrants_concursado.csv'))
# data.table::fwrite(workplaces_entrants_nonconcursado, glue('{harddrive}/intermediate/public_sector_workers/workplaces_entrants_nonconcursado.csv'))
###############################################################################################






###############################################################################################
# Determine which estabs are in the public sector. Anything else will be considered private.
# Concursado and non-concursado entries make up all the public sector entries, so we use that.
combined_concursado_df <- data.table::fread(glue('{harddrive}/intermediate/public_sector_workers/combined_concursado_df.csv'),
                                            colClasses = c(rep('character', 11)),
                                            select = c('establishment', 'year', 'pis_encoded'))

# Keep only uniques to make sure that the same employee is not double counted.
combined_concursado_df <- unique(combined_concursado_df)

establishment_concursado_employee_counts <- combined_concursado_df[, .(employee_count = .N), by = .(establishment, year)]

public_estabs1 <- unique(combined_concursado_df$establishment)

rm(combined_concursado_df)

combined_nonconcursado_df <- data.table::fread(glue('{harddrive}/intermediate/public_sector_workers/combined_nonconcursado_df.csv'),
                                            colClasses = c(rep('character', 11)),
                                            select = c('establishment', 'year', 'pis_encoded'))

# Keep only uniques to make sure that the same employee is not double counted.
combined_nonconcursado_df <- unique(combined_nonconcursado_df)

establishment_nonconcursado_employee_counts <- combined_nonconcursado_df[, .(employee_count = .N), by = .(establishment, year)]

public_estabs2 <- unique(combined_nonconcursado_df$establishment)

rm(combined_nonconcursado_df)

# Now we want to know how many entrants in each public establishment-year, and what job type they were.
entrants <- data.table::fread(glue('{harddrive}/intermediate/public_sector_workers/entrants_public_sector.csv'),
                              colClasses = rep('character', 11),
                              select = c('establishment', 'cohort', 'pis_encoded', 'job_level', 'concursado'))

# Keep unique values.
entrants <- unique(entrants)

entrants_concursado <- entrants[concursado == 1, .(concursado_entrants_count = .N), by = .(establishment, cohort, job_level)]
entrants_concursado[job_level == '', job_level := 'unclassified']

entrants_concursado <- dcast(entrants_concursado,
                             establishment + cohort ~ job_level,
                             value.var = 'concursado_entrants_count') %>% 
  dplyr::rename('year' = 'cohort')

# Now do the same for nonconcursado.
entrants_nonconcursado <- entrants[concursado == 0, .(nonconcursado_entrants_count = .N), by = .(establishment, cohort, job_level)]
entrants_nonconcursado[job_level == '', job_level := 'unclassified']

rm(entrants)

entrants_nonconcursado <- dcast(entrants_nonconcursado,
                             establishment + cohort ~ job_level,
                             value.var = 'nonconcursado_entrants_count') %>% 
  dplyr::rename('year' = 'cohort')

# Now combine everything.
establishment_employee_counts <- merge.data.table(establishment_concursado_employee_counts,
                                                  establishment_nonconcursado_employee_counts,
                                                  by = c('establishment', 'year'),
                                                  all = T)

rm(establishment_concursado_employee_counts)
rm(establishment_nonconcursado_employee_counts)

establishment_crosswalk <- entrants_concursado[establishment_employee_counts, on = .(establishment, year)]
establishment_crosswalk <- entrants_nonconcursado[establishment_crosswalk, on = .(establishment, year)] 

rm(entrants_concursado)
rm(entrants_nonconcursado)
rm(establishment_employee_counts)

colnames(establishment_crosswalk) <- c('establishment',
                                       'year',
                                       'concursado_entrants_bluecollar',
                                       'concursado_entrants_managerial',
                                       'concursado_entrants_unclassified',
                                       'concursado_entrants_whitecollar',
                                       'nonconcursado_entrants_bluecollar',
                                       'nonconcursado_entrants_managerial',
                                       'nonconcursado_entrants_unclassified',
                                       'nonconcursado_entrants_whitecollar',
                                       'total_nonconcursado_employees',
                                       'total_concursado_employees')

# Finally, convert the NAs to 0s.
establishment_crosswalk[is.na(establishment_crosswalk)] <- 0

# Save and comment out.
data.table::fwrite(establishment_crosswalk, file = glue('{harddrive}/intermediate/crosswalks/establishment_size_crosswalk.csv'))

rm(establishment_crosswalk)

public_establishments <- c(public_estabs1, public_estabs2) %>% 
  unique() %>% 
  as.data.table() %>% 
  dplyr::rename('establishment' = '.') %>% 
  dplyr::mutate(public = 1)

rm(public_estabs1)
rm(public_estabs2)

# data.table::fwrite(public_establishments, file = glue('{harddrive}/intermediate/crosswalks/public_establishments.csv'))

public_establishments <- fread(glue('{harddrive}/intermediate/crosswalks/public_establishments.csv'))

# Now match the workplace data with this crosswalk to classify each workplace as public or private.
workplaces_entrants_concursado <- data.table::fread(glue('{harddrive}/intermediate/public_sector_workers/workplaces_entrants_concursado.csv'),
                                                    colClasses = rep('character', 8))

workplaces_entrants_concursado <- public_establishments[workplaces_entrants_concursado, on = .(establishment = workplace_estab)]

workplaces_entrants_concursado <- workplaces_entrants_concursado %>% 
  dplyr::rename('workplace_establishment' =  'establishment', 'establishment' = 'i.establishment')

# Convert the NAs in public to 0 (indicating a private establishment).
workplaces_entrants_concursado[is.na(workplaces_entrants_concursado$public), public := 0]

# data.table::fwrite(workplaces_entrants_concursado, file = glue('{harddrive}/intermediate/public_sector_workers/workplaces_entrants_concursado.csv'))

rm(workplaces_entrants_concursado)

workplaces_entrants_nonconcursado <- data.table::fread(glue('{harddrive}/intermediate/public_sector_workers/workplaces_entrants_nonconcursado.csv'),
                                                       colClasses = rep('character', 8))

workplaces_entrants_nonconcursado <- public_establishments[workplaces_entrants_nonconcursado, on = .(establishment = workplace_estab)]

workplaces_entrants_nonconcursado <- workplaces_entrants_nonconcursado %>% 
  dplyr::rename('workplace_establishment' =  'establishment', 'establishment' = 'i.establishment')

# Convert the NAs in public to 0 (indicating a private establishment).
workplaces_entrants_nonconcursado[is.na(workplaces_entrants_nonconcursado$public), public := 0]

# data.table::fwrite(workplaces_entrants_nonconcursado, file = glue('{harddrive}/intermediate/public_sector_workers/workplaces_entrants_nonconcursado.csv'))

rm(workplaces_entrants_nonconcursado)
rm(public_establishments)
###############################################################################################












