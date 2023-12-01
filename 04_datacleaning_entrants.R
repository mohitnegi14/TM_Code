################## 
# MOHIT NEGI
# Last Updated : 18 September 2023
# Contact on : mohit.negi@studbocconi.it
################## 

################## OBJECTIVES
# This script performs the following tasks.
# 1. Load and combine concursado and non-concursado workers' data into one - public sector workers.
# 2. Split data into entrants (those who joined the establishment in a particular year) and references (those already working there).
##################

################## PATHS
harddrive <- 'D:/Mohit_Work/MOCANU/RAIS_tasks'
box <- 'C:/Users/mohit/Box/mohit_ra'
##################

################## PACKAGES
# We will use these in this script.
packages <- c('tidyverse',
              'data.table',
              'haven',
              'parallel',
              'glue',
              'stringr',
              'stringi',
              'kableExtra',
              'beepr')

# Installs them if not yet installed.
installed_packages <- packages %in% rownames(installed.packages())
if(any(installed_packages == FALSE)) { install.packages(packages[!installed_packages]) }

# And load them in.
invisible(lapply(packages, library, character.only = T))

# Disable scientific notation.
options(scipen = 999)

rm(packages, installed_packages)
##################


##############################################################################
# Load in the concursado workers first.
load_public_sector_data_fn <- function(year_param) {
  
  message(glue('Working on year {year_param}'))
  
  # Load that year's data.
  concursado_workers <- data.table::fread(glue('{harddrive}/intermediate/public_sector_workers/concursado_workers_{year_param}.csv'),
                                          select = c('job', 'year', 'anoadmissao', 'establishment', 'education', 'race', 'gender', 'job_level', 'wage', 'cboid'))
  
  # Add pis_encoded column.
  concursado_workers[, pis_encoded := purrr::map_chr(stringr::str_split(job, pattern = '_'), 3)]
  
  return(concursado_workers)
  
}

## Now parallelize the process of loading the promotions datasets in,
## and then knit them together.

# Start cluster.
cl <- makePSOCKcluster(detectCores()-1)
setDefaultCluster(cl)
clusterExport(NULL, c('load_public_sector_data_fn', 'box'))
clusterEvalQ(cl, {library(data.table); library(glue); library(stringr); library(purrr)})
#
# Store the datasets by running the function.
list_of_concursado_dfs <- parallel::parLapplyLB(NULL, 1985:1995, fun = load_public_sector_data_fn)
#
# Close cluster.
stopCluster(cl)
rm(cl)
#

# Now row bind them.
combined_concursado_df <- data.table::rbindlist(list_of_concursado_dfs)
rm(list_of_concursado_dfs)
gc()

# Save it once. Then comment it out.
# data.table::fwrite(combined_concursado_df, file = glue('{harddrive}/intermediate/public_sector_workers/combined_concursado_df.csv'))
# rm(combined_concursado_df)

# Load in the nonconcursado workers now - divide into entrants and references.
load_public_sector_data_fn <- function(year_param) {
  
  message(glue('Working on year {year_param}'))
  
  # Load that year's data.
  nonconcursado_workers <- data.table::fread(glue('{harddrive}/intermediate/public_sector_workers/nonconcursado_workers_{year_param}.csv'),
                                          select = c('job', 'year', 'anoadmissao', 'establishment', 'education', 'race', 'gender', 'job_level', 'wage', 'cboid'))
  
  # Add pis_encoded column.
  nonconcursado_workers[, pis_encoded := purrr::map_chr(stringr::str_split(job, pattern = '_'), 3)]
  
  return(nonconcursado_workers)
  
}

## Now parallelize the process of loading the promotions datasets in,
## and then knit them together.

# Start cluster.
cl <- makePSOCKcluster(detectCores()-1)
setDefaultCluster(cl)
clusterExport(NULL, c('load_public_sector_data_fn', 'box'))
clusterEvalQ(cl, {library(data.table); library(glue); library(stringr); library(purrr)})
#
# Store the datasets by running the function.
list_of_nonconcursado_dfs <- parallel::parLapplyLB(NULL, 1985:1995, fun = load_public_sector_data_fn)
#
# Close cluster.
stopCluster(cl)
rm(cl)
#

# Now row bind them.
combined_nonconcursado_df <- data.table::rbindlist(list_of_nonconcursado_dfs)
rm(list_of_nonconcursado_dfs)
gc()

# Save it once. Then comment it out.
# data.table::fwrite(combined_nonconcursado_df, file = glue('{harddrive}/intermediate/public_sector_workers/combined_nonconcursado_df.csv'))
# rm(combined_concursado_df)

################## NOW COMBINE THE TWO DATASETS.
combined_concursado_df <- data.table::fread(glue('{harddrive}/intermediate/public_sector_workers/combined_concursado_df.csv'),
                                            colClasses = c(rep('character', 11)))

combined_nonconcursado_df <- data.table::fread(glue('{harddrive}/intermediate/public_sector_workers/combined_nonconcursado_df.csv'),
                                               colClasses = c(rep('character', 11)))

combined_concursado_df[, concursado := 1]
combined_nonconcursado_df[, concursado := 0]

combined_public_sector_workers_df <- rbind(combined_concursado_df, combined_nonconcursado_df)

# Order the dataset by anoadmissao (year of joining).
data.table::setorder(combined_public_sector_workers_df, anoadmissao)

rm(combined_concursado_df)
rm(combined_nonconcursado_df)

# We will define an entrant as someone who joins a new job at the establishment in a given year. 
# Many times, the same worker appears more than once in the same establishment-year. This is because they
# might hold multiple jobs within the same establishment. If they are already working at the establishment
# and join the same establishment with another new job, they will not qualify as an entrant. Since we sorted
# on the basis of anoadmissao, taking unique values now will automatically remove such "new" jobs (where the
# worker was already working).
# So in a given establishment-year, a worker will appear at most once.
combined_public_sector_workers_df <- unique(combined_public_sector_workers_df, by = c('establishment', 'year', 'pis_encoded'))

# Save the pis codes that are relevant, we will later load work history data only for them. 10,177,780 of these.
pis_codes_public_sector_workers <- unique(combined_public_sector_workers_df$pis_encoded)
save(pis_codes_public_sector_workers, file = glue('{harddrive}/intermediate/public_sector_workers/pis_codes_public_sector_workers.rda'))

# Collect observations where the job was started in the year itself. This is the
# set of observations corresponding to new incoming workers (entrants).
entrants <- combined_public_sector_workers_df[year == anoadmissao,][, .(job, cohort = year, establishment, pis_encoded, education, race, gender, job_level, wage, cboid, concursado)]

# The other observations are the "references" i.e. the people who can be connections
# for a new incoming worker.
references <- combined_public_sector_workers_df[year > anoadmissao,][, .(job, reference_year = year, establishment, pis_encoded, concursado)]

entrants_summary <- entrants[, .(appearances = .N), by = pis_encoded]
references_summary <- references[, .(appearances = .N), by = .(pis_encoded)]

# From the summaries, we can clearly see some pis codes are suspicious, they appear way too
# many times. They do not seem to correspond to individuals. Remove them.
suspicious_pis <- c('000000000011',
                    '000000000191',
                    '000000001911',
                    '010101010101',
                    '111111111111',
                    '999999999901',
                    '111111111161')

entrants <- entrants[!(pis_encoded %in% suspicious_pis),]
references <- references[!(pis_encoded %in% suspicious_pis),]

rm(combined_public_sector_workers_df)
rm(entrants_summary)
rm(references_summary)

# Save once and then comment it out.
data.table::fwrite(entrants, file = glue('{harddrive}/intermediate/public_sector_workers/entrants_public_sector.csv'))
data.table::fwrite(references, glue('{harddrive}/intermediate/public_sector_workers/references_public_sector.csv'))
################## 
###############################################################################################
