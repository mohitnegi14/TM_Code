###############################################################################################
################## INTRO
# MOHIT NEGI
# Last Updated : 8 October 2023
# Contact on : mohit.negi@studbocconi.it
################## 

################## OBJECTIVES
# This script performs the following tasks.
# 1. Compute connections between entrants and their coworkers using past workplace histories.
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


# We are gonna make 2 changes,
# 1. An additional match which looks at only 1 year ago matches and splits by  or not.
# 2. Divide the matches as private or public workplaces.



###############################################################################################
################## Now we begin computing number of connections for each person joining a job
# in a particular year.
# The unit is a cohort-job (cohort x establishment x pis_encoded)
# From here, we repeat the entire process below for each batch of
# relevant pis codes separately and then add them together.

references <- data.table::fread(glue('{harddrive}/intermediate/public_sector_workers/references_public_sector.csv'),
                                colClasses = c('character', 'numeric', 'character', 'character', 'numeric'))

load(glue('{harddrive}/intermediate/public_sector_workers/pis_codes_public_sector_workers_batches.rda'))

entrants_workplaces_non-concursado <- data.table::fread(glue('{harddrive}/intermediate/public_sector_workers/workplaces_entrants_non-concursado.csv'),
                                                    colClasses = c('character', 'numeric', 'character', 'character', 'character', 'character', 'character', 'numeric', 'numeric'))

# Remove the columns not relevant for the match.
entrants_workplaces_non-concursado[, `:=`(workplace_establishment = NULL,
                                      job = NULL,
                                      year_workplace = NULL)]

data.table::setkey(entrants_workplaces_non-concursado,
                   workplace,
                   establishment,
                   cohort)

gc()

num_batches <- length(pis_codes_public_sector_workers_batches)

# First define the function that we will apply to each workplaces batch.
compute_connections_fn <- function(batch_param) {
  
  tryCatch(
    withTimeout(
      {
        
        message(glue('WORKING ON BATCH {batch_param} OF {num_batches}.'))
        message(glue('-------------------------------------'))
        
        ## STEP 1: LOAD IN THE WORKPLACES DATA FOR THE BATCH.
        workplaces_df <- data.table::fread(glue('{harddrive}/intermediate/workplaces_batchwise/workplaces_batch_{batch_param}.csv'),
                                           colClasses = c('character', 'character', 'numeric', 'character'))
        
        pis_in_batch <- pis_codes_public_sector_workers_batches[[batch_param]]
        
        references_in_batch <- references[pis_encoded %in% pis_in_batch,]
        
        rm(pis_in_batch)
        
        message(glue('STEP 1 COMPLETED FOR BATCH {batch_param}/{num_batches}.'))
        message(glue('--------------------------------------------'))
        
        ## STEP 2: JOIN THE REFERENCES DATA TO WORKPLACES.
        references_workplaces <- workplaces_df[references_in_batch, on = .(pis_encoded, year_workplace < reference_year), .(workplace, job, establishment, pis_encoded, monthyear_workplace, year_workplace = x.year_workplace, reference_year, concursado), nomatch = 0]
        
        # Add an extra label : whether the workplace was no earlier than 1 year prior to the reference year.
        # With this, we can restrict the definition of "connections" to just 1 year prior shared workplaces if wanted.
        references_workplaces[, one_year_prior := as.numeric(reference_year == (year_workplace + 1))]
        
        # Remove the columns not relevant for the match.
        references_workplaces[, `:=`(job = NULL,
                                     year_workplace = NULL)]
        
        rm(workplaces_df)
        rm(references_in_batch)

        message(glue('STEP 2 COMPLETED FOR BATCH {batch_param}/{num_batches}.'))
        message(glue('--------------------------------------------'))
        
        ## So now we have 2 datasets - entrants_workplaces_non-concursado and references_workplaces.
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
        data.table::setkey(references_workplaces,
                           workplace,
                           establishment,
                           reference_year)
        
        joined_workplaces <- references_workplaces[entrants_workplaces_non-concursado, .(establishment, i.pis_encoded, cohort, pis_encoded, monthyear_workplace, concursado, public, one_year_prior), nomatch = 0, allow.cartesian = T][(pis_encoded != i.pis_encoded) | is.na(pis_encoded),]
        
        rm(references_workplaces)
        
        # If a person A and a person B worked at the same establishment xyz in month m. And then in the same month
        # also worked at a different establishment abc in month m itself.
        # In such a case, I don't want to double count the number of shared months between A and B to be 2. It is just 1.
        # To do this, take only unique rows from the above dataset based on establishment, i.pis_encoded, cohort, monthyear and pis_encoded.
        # This will make sure that when a new entrant (combination of estab, i.pis and cohort) appears twice (or more)
        # with the same reference (pis_encoded) in the same month-year, only one instance is considered.
        joined_workplaces <- unique(joined_workplaces, by = c('establishment', 'i.pis_encoded', 'cohort', 'monthyear_workplace', 'pis_encoded'))
        
        message(glue('STEP 3 COMPLETED FOR BATCH {batch_param}/{num_batches}.'))
        message(glue('--------------------------------------------'))
        
        ## STEP 4: COLLAPSE TO GET JOB-COHORT LEVEL DATA. OUR MAIN VARIABLES OF INTEREST ARE,
        ## n_connections : Number of connections of entrant with workers who already worked at the establishment on joining year (references).
        ## sum_time_connected : Sum of number of months shared with each connection.
        ## n_connections_public : Number of connections who shared a public sector workplace with the entrant.
        ## n_connections_private : Number of connections who shared a private sector workplace with the entrant.
        ## n_connections_conc : Number of connections who are concursado type in current establishment.
        ## n_connections_nonconc : Number of connections who are non-concursado type in current establishment.
        
        # First, keep only one year old connections. After that, we use all the workplace history.
        joined_workplaces_one_year_old <- joined_workplaces[one_year_prior == 1,]
        
        data.table::setkey(joined_workplaces_one_year_old,
                           i.pis_encoded,
                           cohort,
                           establishment)
        
        # First we do n_connections and sum_time_connected.
        collapsed_sum_time_connected <- joined_workplaces_one_year_old[, .(sum_time_connected = .N), by = .(i.pis_encoded, cohort, establishment)]
        
        # Now, n_connections_public and private.
        collapsed_n_connections_public <- joined_workplaces_one_year_old[public == 1, .(n_connections_public = uniqueN(pis_encoded)), by = .(i.pis_encoded, cohort, establishment)]
        collapsed_n_connections_private <- joined_workplaces_one_year_old[public == 0, .(n_connections_private = uniqueN(pis_encoded)), by = .(i.pis_encoded, cohort, establishment)]
        
        # Finally, n_connections, n_connections_conc and n_connections_nonconc.
        joined_workplaces_one_year_old <- unique(joined_workplaces_one_year_old, 
                                                 by = c('pis_encoded',
                                                        'i.pis_encoded',
                                                        'cohort', 
                                                        'establishment'))
        
        collapsed_n_connections <- joined_workplaces_one_year_old[, .(n_connections = .N, n_connections_conc = sum(concursado, na.rm = T)), by = .(i.pis_encoded, cohort, establishment)]
        collapsed_n_connections[, n_connections_nonconc := n_connections - n_connections_conc]
        
        collapsed_data_one_year_old <- data.table::merge.data.table(collapsed_n_connections, collapsed_sum_time_connected, all = T)
        collapsed_data_one_year_old <- data.table::merge.data.table(collapsed_data_one_year_old, collapsed_n_connections_public, all = T)
        collapsed_data_one_year_old <- data.table::merge.data.table(collapsed_data_one_year_old, collapsed_n_connections_private, all = T)
        
        rm(joined_workplaces_one_year_old)
        rm(collapsed_n_connections)
        rm(collapsed_n_connections_public)
        rm(collapsed_n_connections_private)
        rm(collapsed_sum_time_connected)
        
        # Now do the same but with entire workplace history.
        data.table::setkey(joined_workplaces,
                           i.pis_encoded,
                           cohort,
                           establishment)
        
        # First we do n_connections and sum_time_connected.
        collapsed_sum_time_connected <- joined_workplaces[, .(sum_time_connected = .N), by = .(i.pis_encoded, cohort, establishment)]
        
        # Now, n_connections_public and private.
        collapsed_n_connections_public <- joined_workplaces[public == 1, .(n_connections_public = uniqueN(pis_encoded)), by = .(i.pis_encoded, cohort, establishment)]
        collapsed_n_connections_private <- joined_workplaces[public == 0, .(n_connections_private = uniqueN(pis_encoded)), by = .(i.pis_encoded, cohort, establishment)]
        
        # Finally, n_connections, n_connections_conc and n_connections_nonconc.
        joined_workplaces <- unique(joined_workplaces, 
                                    by = c('pis_encoded',
                                           'i.pis_encoded',
                                           'cohort', 
                                           'establishment'))
        
        collapsed_n_connections <- joined_workplaces[, .(n_connections = .N, n_connections_conc = sum(concursado, na.rm = T)), by = .(i.pis_encoded, cohort, establishment)]
        collapsed_n_connections[, n_connections_nonconc := n_connections - n_connections_conc]
        
        collapsed_data <- data.table::merge.data.table(collapsed_n_connections, collapsed_sum_time_connected, all = T)
        collapsed_data <- data.table::merge.data.table(collapsed_data, collapsed_n_connections_public, all = T)
        collapsed_data <- data.table::merge.data.table(collapsed_data, collapsed_n_connections_private, all = T)

        # Change the NAs to 0s.
        collapsed_data[is.na(collapsed_data)] <- 0
        collapsed_data_one_year_old[is.na(collapsed_data_one_year_old)] <- 0

        rm(joined_workplaces)
        rm(collapsed_n_connections)
        rm(collapsed_n_connections_public)
        rm(collapsed_n_connections_private)
        rm(collapsed_sum_time_connected)
        
        # Write it to a separate csv.
        data.table::fwrite(collapsed_data, file = glue('{harddrive}/intermediate/connections/collapsed_data_{batch_param}.csv'))
        data.table::fwrite(collapsed_data_one_year_old, file = glue('{harddrive}/intermediate/connections/collapsed_data_{batch_param}_one_year_old.csv'))
        
        message(glue('STEP 4 (FINAL) COMPLETED FOR {batch_param}/{num_batches}.'))
        message(glue('--------------------------------------------'))
        
        rm(collapsed_data)
        rm(collapsed_data_one_year_old)
        
        write(batch_param, file = glue('{harddrive}/intermediate/connections/completed_connections_match_non-concursado.txt'), append = T, sep = '\n')
        
        gc()
        
        return(NULL)
        
      }, timeout = 1200),
    
    error = function(e) {
      write(batch_param, file = glue('{harddrive}/intermediate/connections/errors_connections_match_non-concursado.txt'), append = T, sep = '\n')
      message(glue('Error for {batch_param}'))
      gc()
    }
    
  )
  
}

# Now apply this function to all batches. (WARNING : THIS RUNS FOR ABOUT 10 HOURS ON MY COMPUTER)
lapply(1:num_batches, FUN = compute_connections_fn)
################## 
###############################################################################################






###############################################################################################
# Some batches throw out errors due to RAM limitations. Deal with them by splitting them into
# mini sub-batches and saving.
error_batches <- c(83,84,85,176,240,241,242,426)

compute_connections_subbatches_fn <- function(error_batch_param) {
  
  tryCatch(
    withTimeout(
      {
        
        message(glue('WORKING ON ERROR BATCH {which(error_batches == error_batch_param)} OF {length(error_batches)}.'))
        message(glue('-------------------------------------'))
        
        ## STEP 1: LOAD IN THE WORKPLACES DATA FOR THE BATCH.
        workplaces_df <- data.table::fread(glue('{harddrive}/intermediate/workplaces_batchwise/workplaces_batch_{error_batch_param}.csv'),
                                           colClasses = c('character', 'character', 'numeric', 'character'))
        
        pis_in_batch <- pis_codes_public_sector_workers_batches[[error_batch_param]]
        
        # Split the pis in 5 sub-batches of 1000 pis codes each.
        pis_in_batch_subbatches <- split(pis_in_batch, ceiling(seq_along(pis_in_batch) / 1000))
        
        rm(pis_in_batch)
        
        # Now we do the process but one subbatch at a time.
        collapsed_data_list <- list()
        collapsed_data_one_year_old_list <- list()
        
        for(i in 1:length(pis_in_batch_subbatches)) {
          
          pis_in_subbatch <- pis_in_batch_subbatches[[i]]
          
          references_in_subbatch <- references[pis_encoded %in% pis_in_subbatch,]
          
          rm(pis_in_subbatch)

          ## STEP 2: JOIN THE REFERENCES DATA TO WORKPLACES.
          references_workplaces <- workplaces_df[references_in_subbatch, on = .(pis_encoded, year_workplace < reference_year), .(workplace, job, establishment, pis_encoded, monthyear_workplace, year_workplace = x.year_workplace, reference_year, concursado), nomatch = 0]
          
          # Add an extra label : whether the workplace was no earlier than 1 year prior to the reference year.
          # With this, we can restrict the definition of "connections" to just 1 year prior shared workplaces if wanted.
          references_workplaces[, one_year_prior := as.numeric(reference_year == (year_workplace + 1))]
          
          # Remove the columns not relevant for the match.
          references_workplaces[, `:=`(job = NULL,
                                       year_workplace = NULL)]
          
          rm(references_in_subbatch)

          message(glue('STEP 2 COMPLETED FOR SUBBATCH {i} OF {length(pis_in_batch_subbatches)} IN ERROR BATCH {which(error_batches == error_batch_param)}/{length(error_batches)}.'))
          message(glue('--------------------------------------------'))
          
          ## So now we have 2 datasets - entrants_workplaces_non-concursado and references_workplaces.
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
          data.table::setkey(references_workplaces,
                             workplace,
                             establishment,
                             reference_year)
          
          joined_workplaces <- references_workplaces[entrants_workplaces_non-concursado, .(establishment, i.pis_encoded, cohort, pis_encoded, monthyear_workplace, concursado, public, one_year_prior), nomatch = 0, allow.cartesian = T][(pis_encoded != i.pis_encoded) | is.na(pis_encoded),]
          
          rm(references_workplaces)
          
          # If a person A and a person B worked at the same establishment xyz in month m. And then in the same month
          # also worked at a different establishment abc in month m itself.
          # In such a case, I don't want to double count the number of shared months between A and B to be 2. It is just 1.
          # To do this, take only unique rows from the above dataset based on establishment, i.pis_encoded, cohort, monthyear and pis_encoded.
          # This will make sure that when a new entrant (combination of estab, i.pis and cohort) appears twice (or more)
          # with the same reference (pis_encoded) in the same month-year, only one instance is considered.
          joined_workplaces <- unique(joined_workplaces, by = c('establishment', 'i.pis_encoded', 'cohort', 'monthyear_workplace', 'pis_encoded'))
          
          message(glue('STEP 3 COMPLETED FOR SUBBATCH {i} OF {length(pis_in_batch_subbatches)} IN ERROR BATCH {which(error_batches == error_batch_param)}/{length(error_batches)}.'))
          message(glue('--------------------------------------------'))
          
          ## STEP 4: COLLAPSE TO GET JOB-COHORT LEVEL DATA. OUR MAIN VARIABLES OF INTEREST ARE,
          ## n_connections : Number of connections of entrant with workers who already worked at the establishment on joining year (references).
          ## sum_time_connected : Sum of number of months shared with each connection.
          ## n_connections_public : Number of connections who shared a public sector workplace with the entrant.
          ## n_connections_private : Number of connections who shared a private sector workplace with the entrant.
          ## n_connections_conc : Number of connections who are concursado type in current establishment.
          ## n_connections_nonconc : Number of connections who are non-concursado type in current establishment.
          
          # First, keep only one year old connections. After that, we use all the workplace history.
          joined_workplaces_one_year_old <- joined_workplaces[one_year_prior == 1,]
          
          data.table::setkey(joined_workplaces_one_year_old,
                             i.pis_encoded,
                             cohort,
                             establishment)
          
          # First we do n_connections and sum_time_connected.
          collapsed_sum_time_connected <- joined_workplaces_one_year_old[, .(sum_time_connected = .N), by = .(i.pis_encoded, cohort, establishment)]
          
          # Now, n_connections_public and private.
          collapsed_n_connections_public <- joined_workplaces_one_year_old[public == 1, .(n_connections_public = uniqueN(pis_encoded)), by = .(i.pis_encoded, cohort, establishment)]
          collapsed_n_connections_private <- joined_workplaces_one_year_old[public == 0, .(n_connections_private = uniqueN(pis_encoded)), by = .(i.pis_encoded, cohort, establishment)]
          
          # Finally, n_connections, n_connections_conc and n_connections_nonconc.
          joined_workplaces_one_year_old <- unique(joined_workplaces_one_year_old, 
                                                   by = c('pis_encoded',
                                                          'i.pis_encoded',
                                                          'cohort', 
                                                          'establishment'))
          
          collapsed_n_connections <- joined_workplaces_one_year_old[, .(n_connections = .N, n_connections_conc = sum(concursado, na.rm = T)), by = .(i.pis_encoded, cohort, establishment)]
          collapsed_n_connections[, n_connections_nonconc := n_connections - n_connections_conc]
          
          collapsed_data_one_year_old <- data.table::merge.data.table(collapsed_n_connections, collapsed_sum_time_connected, all = T)
          collapsed_data_one_year_old <- data.table::merge.data.table(collapsed_data_one_year_old, collapsed_n_connections_public, all = T)
          collapsed_data_one_year_old <- data.table::merge.data.table(collapsed_data_one_year_old, collapsed_n_connections_private, all = T)
          
          rm(joined_workplaces_one_year_old)
          rm(collapsed_n_connections)
          rm(collapsed_n_connections_public)
          rm(collapsed_n_connections_private)
          rm(collapsed_sum_time_connected)
          
          # Now do the same but with entire workplace history.
          data.table::setkey(joined_workplaces,
                             i.pis_encoded,
                             cohort,
                             establishment)
          
          # First we do n_connections and sum_time_connected.
          collapsed_sum_time_connected <- joined_workplaces[, .(sum_time_connected = .N), by = .(i.pis_encoded, cohort, establishment)]
          
          # Now, n_connections_public and private.
          collapsed_n_connections_public <- joined_workplaces[public == 1, .(n_connections_public = uniqueN(pis_encoded)), by = .(i.pis_encoded, cohort, establishment)]
          collapsed_n_connections_private <- joined_workplaces[public == 0, .(n_connections_private = uniqueN(pis_encoded)), by = .(i.pis_encoded, cohort, establishment)]
          
          # Finally, n_connections, n_connections_conc and n_connections_nonconc.
          joined_workplaces <- unique(joined_workplaces, 
                                      by = c('pis_encoded',
                                             'i.pis_encoded',
                                             'cohort', 
                                             'establishment'))
          
          collapsed_n_connections <- joined_workplaces[, .(n_connections = .N, n_connections_conc = sum(concursado, na.rm = T)), by = .(i.pis_encoded, cohort, establishment)]
          collapsed_n_connections[, n_connections_nonconc := n_connections - n_connections_conc]
          
          collapsed_data <- data.table::merge.data.table(collapsed_n_connections, collapsed_sum_time_connected, all = T)
          collapsed_data <- data.table::merge.data.table(collapsed_data, collapsed_n_connections_public, all = T)
          collapsed_data <- data.table::merge.data.table(collapsed_data, collapsed_n_connections_private, all = T)
          
          # Change the NAs to 0s.
          collapsed_data[is.na(collapsed_data)] <- 0
          collapsed_data_one_year_old[is.na(collapsed_data_one_year_old)] <- 0
          
          rm(joined_workplaces)
          rm(collapsed_n_connections)
          rm(collapsed_n_connections_public)
          rm(collapsed_n_connections_private)
          rm(collapsed_sum_time_connected)
          
          collapsed_data_list[[i]] <- collapsed_data
          collapsed_data_one_year_old_list[[i]] <- collapsed_data_one_year_old
          
          rm(collapsed_data)
          rm(collapsed_data_one_year_old)
          
          message(glue('STEP 4 COMPLETED FOR SUBBATCH {i} OF {length(pis_in_batch_subbatches)} IN ERROR BATCH {which(error_batches == error_batch_param)}/{length(error_batches)}.'))
          message(glue('--------------------------------------------'))
          
        }
        
        rm(workplaces_df)
        
        collapsed_data <- rbindlist(collapsed_data_list)
        collapsed_data_one_year_old <- rbindlist(collapsed_data_one_year_old_list)
        
        rm(collapsed_data_list)
        rm(collapsed_data_one_year_old_list)
        
        # Collapse once more to combine the subbatches.
        collapsed_data <- collapsed_data[, .(n_connections = sum(n_connections, na.rm = T),
                                                      n_connections_conc = sum(n_connections_conc, na.rm = T),
                                                      n_connections_nonconc = sum(n_connections_nonconc, na.rm = T),
                                                      sum_time_connected = sum(sum_time_connected, na.rm = T),
                                                      n_connections_public = sum(n_connections_public, na.rm = T),
                                                      n_connections_private = sum(n_connections_private, na.rm = T))
                                                  , by = .(i.pis_encoded, cohort, establishment)]
        
        collapsed_data_one_year_old <- collapsed_data_one_year_old[, .(n_connections = sum(n_connections, na.rm = T),
                                                      n_connections_conc = sum(n_connections_conc, na.rm = T),
                                                      n_connections_nonconc = sum(n_connections_nonconc, na.rm = T),
                                                      sum_time_connected = sum(sum_time_connected, na.rm = T),
                                                      n_connections_public = sum(n_connections_public, na.rm = T),
                                                      n_connections_private = sum(n_connections_private, na.rm = T))
                                                  , by = .(i.pis_encoded, cohort, establishment)]

        # Write it to a separate csv.
        data.table::fwrite(collapsed_data, file = glue('{harddrive}/intermediate/connections/collapsed_data_{error_batch_param}.csv'))
        data.table::fwrite(collapsed_data_one_year_old, file = glue('{harddrive}/intermediate/connections/collapsed_data_{error_batch_param}_one_year_old.csv'))
        
        message(glue('COMBINED DATA FROM ALL SUBBATCHES FOR ERROR BATCH {which(error_batches == error_batch_param)} OF {length(error_batches)}.'))
        message(glue('--------------------------------------------'))
        
        rm(collapsed_data)
        rm(collapsed_data_one_year_old)
        rm(pis_in_batch_subbatches)
        
        write(error_batch_param, file = glue('{harddrive}/intermediate/connections/completed_connections_match_non-concursado.txt'), append = T, sep = '\n')
        
        gc()
        
        return(NULL)
        
      }, timeout = 1200),
    
    error = function(e) {
      write(error_batch_param, file = glue('{harddrive}/intermediate/connections/errors_connections_match_non-concursado.txt'), append = T, sep = '\n')
      message(glue('Error for {error_batch_param}'))
      gc()
    }
    
  )
  
}

# Now apply this function to all error batches.
lapply(error_batches, FUN = compute_connections_subbatches_fn)
################## 
###############################################################################################






###############################################################################################
################## Now stitch all together and combine.
collapsed_files <- list.files(path = glue('{harddrive}/intermediate/connections/'), pattern = 'collapsed')
collapsed_files_one_year_old <- list.files(path = glue('{harddrive}/intermediate/connections/'), pattern = '*_one_year_old')
collapsed_files <- setdiff(collapsed_files, collapsed_files_one_year_old)

# Start cluster.
cl <- makePSOCKcluster(detectCores()-1)
setDefaultCluster(cl)
clusterExport(NULL, c('box', 'collapsed_files', 'collapsed_files_one_year_old'))
clusterEvalQ(cl, {library(data.table); library(glue); library(stringr); library(purrr)})
#
# Run the function.
connected_entrants <- parLapplyLB(NULL, collapsed_files, fun = function(x) {
  df <- data.table::fread(glue('{harddrive}/intermediate/connections/{x}'),
                          colClasses = c('character', 
                                         'numeric', 
                                         'character', 
                                         rep('numeric', 6)))
  return(df)
})

connected_entrants_one_year <- parLapplyLB(NULL, collapsed_files_one_year_old, fun = function(x) {
  df <- data.table::fread(glue('{harddrive}/intermediate/connections/{x}'),
                          colClasses = c('character', 
                                         'numeric', 
                                         'character', 
                                         rep('numeric', 6)))
  return(df)
})
#
# Close cluster.
stopCluster(cl)
rm(cl)
#

# Append them.
connected_entrants <- rbindlist(connected_entrants)
connected_entrants_one_year <- rbindlist(connected_entrants_one_year)

# Now collapse for each cohort-job.
connected_entrants <- connected_entrants[, .(n_connections = sum(n_connections, na.rm = T),
                                                     n_connections_conc = sum(n_connections_conc, na.rm = T),
                                                     n_connections_nonconc = sum(n_connections_nonconc, na.rm = T),
                                                     sum_time_connected = sum(sum_time_connected, na.rm = T),
                                                     n_connections_public = sum(n_connections_public, na.rm = T),
                                                     n_connections_private = sum(n_connections_private, na.rm = T))
                                                 , by = .(i.pis_encoded, cohort, establishment)]
connected_entrants_one_year <- connected_entrants_one_year[, .(n_connections = sum(n_connections, na.rm = T),
                                             n_connections_conc = sum(n_connections_conc, na.rm = T),
                                             n_connections_nonconc = sum(n_connections_nonconc, na.rm = T),
                                             sum_time_connected = sum(sum_time_connected, na.rm = T),
                                             n_connections_public = sum(n_connections_public, na.rm = T),
                                             n_connections_private = sum(n_connections_private, na.rm = T))
                                         , by = .(i.pis_encoded, cohort, establishment)]

# Now calculate the average_time_connected and has_connections variables.
connected_entrants[, `:=`(avg_time_connected = (sum_time_connected/n_connections),
                              has_connections = 1)]

connected_entrants_one_year[, `:=`(avg_time_connected = (sum_time_connected/n_connections),
                          has_connections = 1)]

# Now add in the entrants who have 0 connections. Do this by using the cohorts dataset
# and then matching with the new values we created. Those which didn't match get a has_connections
# value of 0.
entrants <- data.table::fread(glue('{harddrive}/intermediate/public_sector_workers/entrants_public_sector.csv'),
                              colClasses = c('character',
                                             'numeric',
                                             rep('character', 6),
                                             'numeric',
                                             rep('character', 2)))

entrants_non-concursado <- entrants[concursado == 0,]
entrants_non-concursado[, concursado := NULL]

rm(entrants)

final_data_connections <- connected_entrants[entrants_non-concursado, on = .(i.pis_encoded = pis_encoded, cohort, establishment)]
final_data_connections_one_year <- connected_entrants_one_year[entrants_non-concursado, on = .(i.pis_encoded = pis_encoded, cohort, establishment)]

# Convert the NAs to zeroes.
final_data_connections[is.na(final_data_connections)] <- 0
final_data_connections_one_year[is.na(final_data_connections_one_year)] <- 0

# Save once then comment out everything to avoid overwrite.
# data.table::fwrite(final_data_connections, glue('{harddrive}/intermediate/connections/final_data_connections_non-concursado_entrants.csv'))
# data.table::fwrite(final_data_connections_one_year, glue('{harddrive}/intermediate/connections/final_data_connections_one_year_non-concursado_entrants.csv'))
################## 
###############################################################################################
