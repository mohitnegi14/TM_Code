###############################################################################################
################## INTRO
# MOHIT NEGI
# Last Updated : 28 September 2023
# Contact on : mohit.negi@studbocconi.it
################## 

################## OBJECTIVES
# This script performs the following tasks.
# 1. 
##################

################## PATHS
raw <- 'C:/Users/mohit/Desktop/EP/TM/TM_Data/Raw'
cleaned <- 'C:/Users/mohit/Desktop/EP/TM/TM_Data/Cleaned'
output <- 'C:/Users/mohit/Desktop/EP/TM/TM_Data/Output'
box <- 'C:/Users/mohit/Box/mohit_ra'
harddrive <- 'D:/Mohit_Work/MOCANU/Workplaces_batchwise'
##################

################## PACKAGES
# We will use these in this script.
packages <- c('glue',
              'data.table',
              'parallel',
              'stringr',
              'purrr')

# Installs them if not yet installed.
installed_packages <- packages %in% rownames(installed.packages())
if(any(installed_packages == FALSE)) { install.packages(packages[!installed_packages]) }

# And load them in.
invisible(lapply(packages, library, character.only = T))

# Disable scientific notation.
options(scipen = 999)

rm(packages, installed_packages)
##################
###############################################################################################






###############################################################################################
################## FIRST LOAD IN PROMOTIONS DATASET AND KNIT ALL YEARS' DATA TOGETHER.
# load_promotions_data <- function(y) {
#   
#   # Load that year's data.
#   promotions_df <- fread(glue('{box}/intermediate/promotions/promotions_{y}.csv'),
#                          select = c('job', 'year', 'anoadmissao', 'establishment'))
#   
#   # Add pis_encoded column.
#   promotions_df[, pis_encoded := purrr::map_chr(stringr::str_split(job, pattern = '_'), 3)]
#   
#   return(promotions_df)
#   
#   rm(promotions_df)
#   gc()
#   
# }
# 
# ################## Now parallelize the process of loading the promotions datasets in, 
# ################## and then knit them together.
# 
# # Start cluster.
# cl <- makePSOCKcluster(detectCores()-1)
# setDefaultCluster(cl)
# clusterExport(NULL, c('load_promotions_data', 'years', 'box'))
# clusterEvalQ(cl, {library(data.table); library(glue); library(stringr); library(purrr)})
# #
# 
# # Store the datasets.
# list_of_promotions_dfs <- parLapply(NULL, years, fun = load_promotions_data)
# 
# # Now row bind them.
# combined_promotions_df <- data.table::rbindlist(list_of_promotions_dfs)
# rm(list_of_promotions_dfs)
# 
# # Close cluster.
# stopCluster(cl)
# rm(cl)
# #

# Save it once. Then comment everything out.
#data.table::fwrite(combined_promotions_df, file = glue('{output}/combined_promotions_df.csv'))
###############################################################################################

com <- fread(glue('{output}/combined_promotions_df.csv'))
com2 <- com[pis_encoded == '100155823421']
rm(com)

see <- joined_workplaces[i.pis_encoded == '100155823421']

###############################################################################################
################## NOW LOAD IN THE COMBINED PROMOTIONS DATASET.
# For faster reading.
# setDTthreads(0L)
# combined_promotions_df <- data.table::fread(glue('{output}/combined_promotions_df.csv'),
#                                             colClasses = c(rep('character', 5)))
# 
# # Save the pis_encoded's that are relevant, only load those workplaces. Around 6 million.
# # relevant_pis_codes <- unique(combined_promotions_df$pis_encoded)
# 
# # Collect observations where the job was started in the year itself. This is the
# # set of observations corresponding to new incoming workers.
# entrants <- combined_promotions_df[year == anoadmissao,][, .(job, cohort = year, establishment, pis_encoded)]
# 
# # The other observations are the "references" i.e. the people who can be connections
# # for a new incoming worker.
# references <- combined_promotions_df[year > anoadmissao,][, .(job, reference_year = year, establishment, pis_encoded)]
# 
# entrants_summary <- entrants[, .(appearances = .N), by = pis_encoded]
# references_summary <- references[, .(appearances = .N), by = pis_encoded]
# 
# # From the summaries, we can clearly see some pis codes are suspicious, they appear way too
# # many times. They do not seem to correspond to individuals. Remove them.
# suspicious_pis <- c('000000000011', '000000000191', '000000001911', '010101010101', '111111111111', '999999999901')
# entrants <- entrants[!(pis_encoded %in% suspicious_pis),]
# references <- references[!(pis_encoded %in% suspicious_pis),]
# 
# rm(combined_promotions_df)
# rm(entrants_summary)
# rm(references_summary)
# 
# data.table::fwrite(entrants, glue('{raw}/entrants.csv'))
# data.table::fwrite(references, glue('{raw}/references.csv'))
# 
# rm(entrants)
# rm(references)
################## 
###############################################################################################






###############################################################################################
################## Re-doing the workplaces dataset.
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
# 2. Load a period's workplaces file from the Box folder.
# 3. Subset it by keeping only rows corresponding to pis codes in batch 1.
# 4. Save it to a batch file : workplaces_batch1.csv (export it to hard-drive).
# 5. Remove it from R environment and then repeat for batch 2, 3, .... and so on.
# 6. Then next period, this time write to same workplaces_batch1.csv and append it. Then the other batches.
################## 

################## 
# size_of_one_batch <- 100000
# relevant_pis_codes_batches <- split(relevant_pis_codes,ceiling(seq_along(relevant_pis_codes) / 100000))

# Run it once only and then save it. Then always load it and comment out the above part.
# save(relevant_pis_codes_batches, file = glue('{raw}/relevant_pis_codes_batches.rda'))
##################

################## 
# redo_workplaces <- function(period_param){
#   
#   # Extract the year and month from the period.
#   year_param <- stringr::str_split(period_param, pattern = '_')[[1]][2]
#   month_param <- stringr::str_split(period_param, pattern = '_')[[1]][1]
#   
#   message(glue('WORKING ON MONTH {month_param} OF YEAR {year_param}.'))
#   message(glue('----------------------------------------------------'))
#   
#   # For faster reading.
#   setDTthreads(0L)
#   
#   # Load in the appropriate dataset.
#   workplaces_df <- data.table::fread(glue('{box}/intermediate/workplaces/workplaces_{year_param}/workplacedata_month{month_param}_{year_param}.csv'),
#                                      colClasses = c(rep('character', 2)))
#   
#   # Add year and month-year variables. This will be useful later.
#   workplaces_df <- workplaces_df[, `:=`(year_workplace = year_param, monthyear_workplace = paste(month_param, year_param, sep = '_'))]
#   
#   # Now split by batches.
#   for(batch_number in 1:length(relevant_pis_codes_batches)) {
#     
#     message(glue('BATCH {batch_number} IN {month_param}/{year_param}'))
#     
#     # Get a vector of the pis codes in this batch.
#     pis_in_batch <- relevant_pis_codes_batches[[batch_number]]
#     
#     # Get the subset of the workplaces.
#     workplaces_df_batch <- workplaces_df[pis_encoded %in% pis_in_batch,]
#     
#     # Save to a separate batch file (in the hard-drive).
#     if(period_param == '1_1985') {
#       data.table::fwrite(workplaces_df_batch, glue('{harddrive}/workplaces_batch_{batch_number}.csv'),
#                          append = F)
#     } else {
#       data.table::fwrite(workplaces_df_batch, glue('{harddrive}/workplaces_batch_{batch_number}.csv'),
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
# load(glue('{raw}/relevant_pis_codes_batches.rda'))
# 
# periods <- data.table('year' = c(rep('1985', 12), 
#                                  rep('1986', 12), 
#                                  rep('1987', 12),
#                                  rep('1988', 12),
#                                  rep('1989', 12),
#                                  rep('1990', 12),
#                                  rep('1991', 12),
#                                  rep('1992', 12),
#                                  rep('1993', 12),
#                                  rep('1994', 12),
#                                  rep('1995', 12)), 
#                       'month' = rep(1:12, 11))
# 
# # Add month-year column.
# periods <- periods[, monthyear := paste(month, year, sep = '_')]
# ################## 

################## 
# Now apply the function to each period. This will take some time. Comment out after use.
# lapply(periods$monthyear, FUN = redo_workplaces)
################## 
###############################################################################################






###############################################################################################
################## We will collect all workplaces that correspond to entrants. And only
# those which will be relevant - i.e. those in periods before their cohort year.
# batch_numbers <- 1:68
 
# entrants <- data.table::fread(glue('{raw}/cohorts.csv'),
#                               colClasses = c('character', 'numeric', 'character', 'character'))

# In this data, a few workers are such that they enter an establishment in
# say, 1985. And then do so again in 1986, 87, etc. Maybe even more than once in a year.
# This might be because they are given different sub-jobs (eg: math and geography teacher
# at same school/establishment)
# The problem is, if consider worker x entering establishment y in 1986.
# Most of the people working there already will also have been working in 1985
# when person x herself was there. So, x shares 12 workplaces with each of her
# colleagues. This is of course, misleading since we are interested in connections
# and hiring. So we keep only FIRST ENTRANCE into an establishment.

# setorder(entrants, 'cohort')
# new_entrants <- entrants %>% unique(by = 'job')
# 
# rm(entrants)
#   
# # Now record the pis codes of these entrants. This will be helpful in the next step.  
# new_entrants_pis <- unique(new_entrants$pis_encoded)
# 
# new_entrants_workplaces_fn <- function(batch_param) {
# 
#   message(glue('PROCESSING {batch_param}/68.'))
#   message(glue('--------------------------------------------'))
# 
#   # Load in that batch workplaces.
#   workplaces_df <- data.table::fread(glue('{harddrive}/workplaces_batch_{batch_param}.csv'),
#                                      colClasses = c('character', 'character', 'numeric', 'character'))
# 
#   # Keep only the rows corresponding to new entrants.
#   workplaces_df <- workplaces_df[pis_encoded %in% new_entrants_pis,]
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
#   return(workplaces_df)
# 
# }
# 
# # Now apply it to all batches.
# new_entrants_workplaces <- lapply(batch_numbers, FUN = new_entrants_workplaces_fn)
# new_entrants_workplaces <- data.table::rbindlist(new_entrants_workplaces)
# 
# rm(new_entrants_pis)
# 
# # Save once then comment out everything to avoid overwrite.
# data.table::fwrite(new_entrants_workplaces, glue('{output}/new_entrants_workplaces.csv'))
# 
# # Now match these new entrants to their workplaces (only those workplaces that are before
# # their cohort year).
# new_entrants_workplaces <- new_entrants_workplaces[new_entrants, on = .(pis_encoded, year_workplace < cohort), .(workplace, job, establishment, pis_encoded, monthyear_workplace, year_workplace = x.year_workplace, cohort), nomatch = 0]
# 
# Remove the invalid cases.
# new_entrants_workplaces[, c('cnpj', 'munic') := data.table::tstrsplit(workplace, "_", fixed = TRUE, keep = 1:2)]
# new_entrants_workplaces[, `:=`(workplace_estab = paste(cnpj, munic, sep = '_'), cnpj = NULL, munic = NULL)]
# new_entrants_workplaces <- new_entrants_workplaces[workplace_estab != establishment,]
# 
# # Save once then comment out everything to avoid overwrite.
# data.table::fwrite(new_entrants_workplaces, glue('{output}/new_entrants_workplaces_matched.csv'))
# rm(new_entrants)
###############################################################################################










###############################################################################################
################## Now we begin computing number of connections for each person joining a job
# in a particular year.
# The unit is a cohort-job (cohort x establishment x pis_encoded)
# From here, we repeat the entire process below for each batch of 
# relevant pis codes separately and then add them together. I also use parallel
# processing to speed up the process.

# First, we will need the following inputs for the function.
batch_numbers <- 1:68

references <- data.table::fread(glue('{raw}/references.csv'),
                                colClasses = c('character', 'numeric', 'character', 'character'))

load(glue('{raw}/relevant_pis_codes_batches.rda'))

# The workplaces for all the new_entrants is around 50 million. That's a lot. So we will have to do this in chunks.
# Split it in 11 groups based on pis codes. Process them separately and them knit them all back together.
# new_entrants_workplaces_pis <- unique(new_entrants_workplaces$pis_encoded)
# new_entrants_workplaces_pis_batches <- split(new_entrants_workplaces_pis, ceiling(seq_along(new_entrants_workplaces_pis) / 100000))
# rm(new_entrants_workplaces_pis)
# # Run it once only and then save it. Then always load it and comment out the above part.
# save(new_entrants_workplaces_pis_batches, file = glue('{raw}/new_entrants_workplaces_pis_batches.rda'))

for(new_entrants_batch in 1:11) {
  
  message(glue('WORKING ON NEW_ENTRANTS BATCH {new_entrants_batch} OF 11'))
  message(glue('--------------------------------------------------------'))
  
  # First load in the entire thing. I do it separately for every iteration of the loop just because I dont want too many
  # big dataframes open at the same time in R. It's inefficient to load it in 11 times instead of 1 though.
  gc()
  new_entrants_workplaces <- data.table::fread(glue('{output}/new_entrants_workplaces_matched.csv'),
                                         colClasses = c('character', 'character', 'character', 'character', 'character', 'numeric', 'numeric'))
  # Now filter.
  load(glue('{raw}/new_entrants_workplaces_pis_batches.rda'))
  
  new_entrants_workplaces <- new_entrants_workplaces[pis_encoded %in% new_entrants_workplaces_pis_batches[[new_entrants_batch]]]
  
  rm(new_entrants_workplaces_pis_batches)
  
  gc()
  
  # Now do the rest of the process.
  # First define the function that we will apply to each workplaces batch.
  compute_connections <- function(batch_param) {
    
    message(glue('WORKING ON REFERENCES BATCH {batch_param} OF 68.'))
    message(glue('-------------------------------------'))
    
    ## STEP 1: LOAD IN ALL THE COHORTS AND REFERENCES DATA AND THE WORKPLACES DATA FOR THE BATCH.
    workplaces_df <- data.table::fread(glue('{harddrive}/workplaces_batch_{batch_param}.csv'),
                                       colClasses = c('character', 'character', 'numeric', 'character'))
    
    # Take unique ones. Same reasoning as above.
    workplaces_df <- unique(workplaces_df, by = c('workplace', 'pis_encoded'))
    
    # There seems to catch-all pis codes that repeat many times. This causes huge problems in the code
    # because we get millions of unnecessary duplicates. Remove any observations with this code.
    suspicious_pis <- c('000000000011', '000000000191', '000000001911', '010101010101', '111111111111', '999999999901')
    workplaces_df <- workplaces_df[!(pis_encoded %in% suspicious_pis),]
    
    references_in_batch <- references[pis_encoded %in% relevant_pis_codes_batches[[batch_param]],]
    
    message(glue('STEP 1 COMPLETED FOR BATCH {batch_param}/68.'))
    message(glue('--------------------------------------------'))
    
    ## STEP 2: JOIN THE REFERENCES DATA TO WORKPLACES.
    gc()
    references_workplaces <- workplaces_df[references_in_batch, on = .(pis_encoded, year_workplace < reference_year), .(workplace, job, establishment, pis_encoded, monthyear_workplace, year_workplace = x.year_workplace, reference_year), nomatch = 0]
    
    rm(workplaces_df)
    rm(references_in_batch)
    
    gc()
    
    message(glue('STEP 2 COMPLETED FOR BATCH {batch_param}/68.'))
    message(glue('--------------------------------------------'))
    
    ## So now we have 2 datasets - new_entrants_workplaces and references_workplaces.
    ## The former has workplace data corresponding to new entrants and so each
    ## workplace is associated with the new entrant - their pis, their cohort year
    ## and the establishment they joined that year.
    ## The latter has workplaces data corresponding to references and so each
    ## workplace is associated with the references i.e those already working there - their pis, their
    ## reference year and their establishment in that reference year.
    ## Suppose a worker x works at the same establishment for 3 years (say 1986, 1987, 1988). 
    ## Then the workplaces she has worked at appear thrice in the dataset - each with a different reference year.
    ## So if a cohort member entered that establishment in year 1986, their past workplaces will
    ## be compared with the workplaces of pis = x and reference year = 1986. And if another
    ## enters in 1987, then compare with pis = x and reference year = 1987. 
    
    ## Another option was to just keep the max year as the reference year and prevent duplication.
    ## And then compare entrants to workplaces with pis = x and reference year > cohort year.
    ## But this will not work if the reference worker exited the establishment and then re-entered.
    ## For example if worker x worked there in 1986, quit in 1987 and then rejoined in 1988. Then
    ## new entrants in 1987 should not be compared with worker x's past workplaces but they still
    ## will since max reference year for her is 1988 and cohort year 1987 < 1988.
    
    ## STEP 3: JOIN THE NEW ENTRANTS' WORKPLACES AND REFERENCES' WORKPLACES DATA.
    joined_workplaces <- r[new_entrants_workplaces, on = .(workplace, establishment, year_workplace, monthyear_workplace, reference_year == cohort), .(workplace, monthyear_workplace, i.pis_encoded, establishment, cohort, pis_encoded), nomatch = 0, allow.cartesian = T][(pis_encoded != i.pis_encoded) | is.na(pis_encoded),]
    
    rm(references_workplaces)
    
    gc()
    
    # If a person A and a person B worked at the same establishment xyz in month m. And then in the same month
    # also worked at a different establishment abc in month m itself.
    # In such a case, I don't want to double count the number of shared months between A and B to be 2. It is just 1.
    # To do this, take only unique rows from the above dataset based on establishment, i.pis_encoded, cohort, monthyear and pis_encoded.
    # This will make sure that when a new entrant (combination of estab, i.pis and cohort) appears twice (or more)
    # with the same reference (pis_encoded) in the same month-year, only one instance is considered.
    joined_workplaces <- unique(joined_workplaces, by = c('establishment', 'i.pis_encoded', 'cohort', 'monthyear_workplace', 'pis_encoded'))
    
    message(glue('STEP 3 COMPLETED FOR BATCH {batch_param}/68.'))
    message(glue('--------------------------------------------'))
    
    ## STEP 4: COLLAPSE TO GET JOB-COHORT LEVEL DATA. OUR MAIN VARIABLES OF INTEREST ARE,
    ## n_connections : Number of past connections among workers who already worked at the establishment on joining year.
    ## sum_time_connected : Sum of number of months shared with each connection.
    collapsed_data <- joined_workplaces[, .(n_connections = uniqueN(pis_encoded), sum_time_connected = .N), by = .(i.pis_encoded, cohort, establishment)]
    
    rm(joined_workplaces)
    
    # Write it to a separate csv.
    data.table::fwrite(collapsed_data, file = glue('{harddrive}/collapsed/collapsed_data_{batch_param}_{new_entrants_batch}.csv'))
    
    message(glue('STEP 4 COMPLETED FOR BATCH {batch_param}/68.'))
    message(glue('--------------------------------------------'))
    
    rm(collapsed_data)
    gc()
    
    return(NULL)
    
  }
  
  lapply(batch_numbers, FUN = compute_connections)

}

# # PARALLEL PROCESSING : <<< SET UP >>>
# # Start cluster
# cl <- makePSOCKcluster(4)
# setDefaultCluster(cl)
# clusterExport(NULL, c('compute_connections', 'batch_numbers', 'harddrive', 'new_entrants_workplaces', 'references', 'relevant_pis_codes_batches'))
# clusterEvalQ(cl, {library(data.table); library(glue); library(stringr)})
# #
# 
# list_of_connected_cohortmembers <- parallel::parLapply(cl = NULL, batch_numbers, fun = compute_connections) 
# 
# connected_cohortmembers <- rbindlist(list_of_connected_cohortmembers)

# Now add up the n_connections and sum_time_connected variables for each cohort-job.
connected_cohortmembers <- connected_cohortmembers[, .(n_connections = sum(n_connections, na.rm = T), sum_time_connected = sum(sum_time_connected, na.rm = T)), by = .(i.pis_encoded, cohort, establishment)]

# Now calculate the average_time_connected and has_connections variables.
connected_cohortmembers[, `=:`(avg_time_connected = (sum_time_connected/n_connections),
                               has_connections = 1)]

# Now add in the cohort members who have 0 connections. Do this by using the cohorts dataset
# and then matching with the new values we created. Those which didn't match get a has_connections
# value of 0.
final_data_connections <- connected_cohortmembers[cohorts, on = .(pis_encoded = i.pis_encoded, cohort, establishment)]

data.table::fwrite(final_data_connections, glue('{output}/final_data_connections.csv'))
###############################################################################################






###############################################################################################
################## NOW THE FINAL ANALYSIS : FIRST A SIMPLE DID.
# Our treatment is federal employer, control is state employer.
# Our pre-period is pre 1989 and post is after that.

# First, we need to match our establishments with natjuridica values that help us classify
# that establishment as federal or state.
# So we have to go back to the raw data, extract the establishments and natjuridica and
# make a crosswalk. Then match it with final_data_connections.
years <- 1985:1995

crosswalk_fn <- function(year) {
  
  # First load in that year's data. Only the 2 columns we need so it's fast.
  df <- data.table::fread(glue('D:/Mohit_Work/MOCANU/MocanuFiles/allstates_blind_{year}_newvars.csv'),
                          select = c('establishment', 'natjuridica'),
                          colClasses = c('character', 'character'))
  
  df <- unique(df)
  
  return(df)
  
}

# Now apply this.
estab_natjuridica_crosswalk <- lapply(years, FUN = crosswalk_fn)
estab_natjuridica_crosswalk <- data.table::rbindlist(estab_natjuridica_crosswalk)
estab_natjuridica_crosswalk <- unique(estab_natjuridica_crosswalk)

data.table::fwrite(estab_natjuridica_crosswalk, glue('{output}/estab_natjuridica_crosswalk.csv'))

final_data_connections <- estab_natjuridica_crosswalk[final_data_connections, on = .(establishment)]
# Make sure there are no duplications i.e. one establishment having two different natjuridica.

# Now classify as federal vs. state vs. municipal.
federal_codes <- c('1015', '1040', '1074', '1104', '1139', '1163', '1252')
state_codes <- c('1023', '1058', '1082', '1112', '1147', '1171', '1260')
municipal_codes <- c('1031', '1066', '1120', '1155', '1180', '1244', '1279')

final_data_connections[, government := fcase(natjuridica %in% federal_codes, 'federal',
                                             natjuridica %in% state_codes, 'state',
                                             natjuridica %in% municipal_codes, 'municipal')]

# For our DiD analysis, the unit will be a cohort-establishment, not a cohort-job (i.e. cohort-(establishment x pis)).
# So collapse the pis codes in each cohort-establishment.
analysis_data <- final_data_connections[, .(perc_connected = (sum(has_connections)/.N)), by = .(establishment, cohort)]

# Basic idea is to compare cohort-estabs before reform (1989) with those after.

# list_of_connected_cohortmembers <- lapply(batch_numbers , FUN = compute_connections) 


try <- cohort_workplaces[, .(count = .N), by = .(pis_encoded, workplace)]
try2 <- try[count > 1,]
try3 <- unique(try2$pis_encoded)




try <- joined_workplaces[i.pis_encoded == '108632277991',]

unique(joined_workplaces$monthyear_workplace)

repeater <- cohorts[job == '82916818000113_420460_170334311451']

repeat_jobs <- cohorts[, .(count = .N), by = job]

cohorts <- unique(cohorts, by = c('job'))


see <- joined_workplaces[i.pis_encoded == '100444933361',]


# Compare with pproc.
# # PARALLEL PROCESSING : <<< SET UP >>>
# # Start cluster
cl <- makePSOCKcluster(2)
setDefaultCluster(cl)
clusterExport(NULL, c('compute_connections', 'batch_numbers', 'harddrive', 'new_entrants_workplaces', 'references', 'relevant_pis_codes_batches'))
clusterEvalQ(cl, {library(data.table); library(glue); library(stringr)})
#

parallel::parLapplyLB(cl = NULL, batch_numbers[1:2], fun = compute_connections)

#
stopCluster()
#

