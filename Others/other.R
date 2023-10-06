################## Define a new function that loads in workplace data for a period p.
# This includes only those workplaces where certain people worked i.e. the vector of pis_encoded (relcodes).

load_workplaces_data <- function(monthyear_param, month_param, year_param, relcodes) {
  
  # Load in the appropriate dataset.
  workplaces_df <- data.table::fread(glue('{box}/intermediate/workplaces/workplaces_{year_param}/workplacedata_month{month_param}_{year_param}.csv'),
                                     colClasses = c(rep('character', 2)))
  
  # Add year, month and month-year variables.
  workplaces_df <- workplaces_df[pis_encoded %in% relcodes,][, `:=`(year_workplace = year_param, monthyear_workplace = paste(month_param, year_param, sep = '_'))]
  
  return(workplaces_df)
  
}
################## 

# load_workplaces_data <- function(p, relcodes) {
# 
#   # Extract the year and month from 
#   y <- stringr::str_split(p, pattern = ',')[[1]][2]
#   m <- stringr::str_split(p, pattern = ',')[[1]][1]
# 
#   workplaces_df <- data.table::fread(glue('{box}/intermediate/workplaces/workplaces_{y}/workplacedata_month{m}_{y}.csv'),
#                                      colClasses = c(rep('character', 2)))
#   
#   workplaces_df <- workplaces_df[pis_encoded %in% relcodes,][, `:=`(year = y, monthyear = paste(m, y, sep = '.'))]
# 
#   return(workplaces_df)
# 
# }

# Start cluster
cl <- makePSOCKcluster(detectCores()-1)
setDefaultCluster(cl)
clusterExport(NULL, c('load_workplaces_data', 'periods', 'pis_in_batch2', 'box'))
clusterEvalQ(cl, {library(data.table); library(glue); library(stringr); library(sqldf)})
#

pis_in_batch2 <- pis_in_batch[44:45]

list_of_workplaces_dfs <- parLapply(NULL, periods$monthyear, pis_in_batch2, fun = load_workplaces_data)




load_workplaces_data <- function(period_param, codes_param) {
  
  # Load in the appropriate dataset.
  # workplaces_df <- data.table::fread(glue('{box}/intermediate/workplaces/workplaces_{year_param}/workplacedata_month{month_param}_{year_param}.csv'),
  #                                    colClasses = c(rep('character', 2)))
  # Add year and month-year variables. This will be useful later.
  # workplaces_df <- workplaces_df[pis_encoded %in% codes_param,][, `:=`(year_workplace = year_param, monthyear_workplace = paste(month_param, year_param, sep = '_'))]
  
  # Extract the year and month from the period.
  year_param <- stringr::str_split(period_param, pattern = '_')[[1]][2]
  month_param <- stringr::str_split(period_param, pattern = '_')[[1]][1]
  
  workplaces_df <- sqldf::read.csv.sql(glue('{box}/intermediate/workplaces/workplaces_{year_param}/workplacedata_month{month_param}_{year_param}.csv'),
                                       sql = "select * from file where pis_encoded = '100158820361'",
                                       sep = ',')
  
  return(workplaces_df)
  
}

setDTthreads(0L)




workplaces_df <- data.table::fread(glue('workplacedata_month{month_param}_{year_param}.csv'),
                                   colClasses = c(rep('character', 2)))

#################
# # Parallelize it my friend.
# # Start cluster
# cl <- makePSOCKcluster(2)
# setDefaultCluster(cl)
# clusterExport(NULL, c('redo_workplaces', 'periods', 'relevant_pis_codes_chunks', 'box', 'harddrive'))
# clusterEvalQ(cl, {library(data.table); library(glue); library(stringr)})
# #
# 
# #
# start.time <- Sys.time()
# parallel::parLapplyLB(cl = NULL, periods$monthyear, fun = redo_workplaces)
# end.time <- Sys.time()
# time.taken_eff <- end.time - start.time
# time.taken_eff
# #
# 
# # Close cluster.
# stopCluster(cl)
# rm(cl)
# #
#################
#### TOY DATASETS.
cohorts2 <- data.frame('job' = c('xyz_123', 'abc_880', 'xyz_123'),
                       'cohort' = c(1986, 1987, 1987), 
                       'establishment' = c('xyz','abc', 'xyz'), 
                       'pis_encoded' = c('123', '880', '123')) %>% 
  as.data.table()

workplaces_df2 <- data.frame('workplace' = c('pqr_1_1985', 'rst_1_1985', 'rst_2_1985', 'pqr_1_1985', 'rst_1_1985', 'rst_1_1985', 'rst_2_1988'), 
                             'pis_encoded' = c('123', '123', '123', '571', '571', '880', '880'), 
                             'year_workplace' = c(1985, 1985, 1985, 1985, 1985, 1985, 1988), 
                             'monthyear_workplace' = c('1_1985', '1_1985', '2_1985', '1_1985', '1_1985', '1_1985', '1_1988')) %>% 
  as.data.table()

references2 <- data.frame('job' = c('xyz_571', 'xyz_880', 'xyz_571', 'xyz_571', 'abc_880', 'xyz_123'),
                          'reference_year' = c(1986, 1986, 1987, 1988, 1988, 1988), 
                          'establishment' = c('xyz','xyz', 'xyz', 'xyz', 'abc', 'xyz'), 
                          'pis_encoded' = c('571', '880', '571', '571', '880', '123')) %>% 
  as.data.table()

try <- references_workplaces[, .(number = .N), by = pis_encoded]
try2 <- references[, .(number = .N), by = pis_encoded]
try3 <- workplaces_df[, .(number = .N), by = pis_encoded]

try4 <- references[pis_encoded == '100240412431',]

new_guys <- cohorts[establishment == '00394460001113_330455',]
old_guys <- references[establishment == '00394460001113_330455',]

new_guys2 <- new_guys[, .(count = .N), by = cohort] 
old_guys2 <- old_guys[, .(count = .N), by = reference_year] 

this_guy <- cohorts[pis_encoded == '100139095571',]

data1987 <- fread(glue('{raw}/allstates_blind_1987_newvars.csv'))

## STEP 2: JOIN THE COHORT DATA TO WORKPLACES DATA. AS A RESULT, WE WILL BE ABLE TO
## ASSOCIATE THE WORKPLACES TO THE COHORTS-JOBS (I.E. COHORT-YEAR, ESTABLISHMENT AND PIS).
############## SHIFTED DOWN

message(glue('STEP 2 COMPLETED FOR BATCH {batch_param}/68.'))
message(glue('--------------------------------------------'))