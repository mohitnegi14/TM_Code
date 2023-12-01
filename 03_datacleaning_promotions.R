################## 
# MOHIT NEGI
# Last Updated : 13 November 2023
# Contact on : mohit.negi@studbocconi.it
################## 

################## OBJECTIVES
# This script performs the following tasks.
# 1. Create new datasets with only public sector concursado and non-concursado workers.
# 2. Create a dataset called promotions_1985_to_1995 that labels
# whether a job (estab x pis) received a promotion in the 10 year
# span, defined as a sustained 10% or larger wage bump.
# Also labels if the job received an educational upgrade.
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
###########
########### Change year from 1985 to 1995 one by one.
year <- 1985  
###########
###########
# Just read in the first column to get the number of rows.
df1_numberofrows <- fread(glue('{harddrive}/intermediate/datasets_with_new_vars/allstates_blind_{year}_newvars.csv'),
                          select = 'anoadmissao')

number_of_rows <- nrow(df1_numberofrows)

rm(df1_numberofrows)
gc()

# Divide up the rows in smaller batches (of 2 million each) for reading with limited RAM.
batches <- seq(0, number_of_rows, by = 2000000)
##############################################################################


##############################################################################
# Load just the columns to determine first.
names_of_columns <- fread(glue('{harddrive}/intermediate/datasets_with_new_vars/allstates_blind_{year}_newvars.csv'),
                          nrows = 0) %>% colnames()

# Track number of columns too.
number_of_columns <- length(names_of_columns)
##############################################################################


##############################################################################
# Divide up the rows in smaller batches (of a million each) for reading with limited RAM.
# Starting from 1 now because fread works a bit different than read_dta.
batches[1] <- 1
##############################################################################


##############################################################################
# Add the cbo to isco converter to code jobs as blue collar,
# white collar and managerial.
# cbo_crosswalk <- fread(glue('{harddrive}/intermediate/crosswalks/cbo-isco-conc.csv'))
# cbo_crosswalk[, digit_1 := str_sub(iscoid, 1, 1)]
# cbo_crosswalk[, job_level := fcase(digit_1 %in% '1', 'managerial',
#                                    digit_1 %in% c('2', '3', '4', '5'), 'white-collar',
#                                    digit_1 %in% c('6', '7', '8', '9'), 'blue-collar')]
# cbo_crosswalk <- cbo_crosswalk[, .(cboid, job_level)]
# cbo_crosswalk[, cboid := str_replace_all(cboid, pattern = '\\-', '')][, cboid := str_replace_all(cboid, pattern = '\\.', '')]
# 
# cbo_94_to_2002 <- read_dta(glue('{harddrive}/intermediate/crosswalks/crosswalk_cbo94_2002.dta')) %>% 
#   select('cbo2002', 'cbo94')
# 
# # Create a crosswalk between cbo94, 2002 and isco88.
# occupation_crosswalk <- merge.data.table(cbo_crosswalk,
#                                          cbo_94_to_2002,
#                                          by.x = 'cboid',
#                                          by.y = 'cbo94',
#                                          all = T)

# Save it. Only once run it, then comment everything out.
# fwrite(occupation_crosswalk, file = glue('{harddrive}/intermediate/crosswalks/occupation_codes_crosswalk.csv'))
##############################################################################


##############################################################################
for(i in 1:length(batches)) {
  
  message(glue('We are working on the year {year} and on part {i} of {length(batches)}'))
  
  # Now load it in to work on it.
  input_df <- fread(glue('{harddrive}/intermediate/datasets_with_new_vars/allstates_blind_{year}_newvars.csv'),
                    colClasses = c(rep('character', number_of_columns)),
                    nrows = 2000000,
                    skip = batches[i],
                    header = F)
  
  # Do this, otherwise the 1000000th row will repeat, since the first batch ends at 1000000
  # and the second starts at the same.
  if(i == 1) {input_df <- input_df[-2000000,]} else {input_df <- input_df}
  
  # Add the column names.
  colnames(input_df) <- names_of_columns
  
  ############## Now starting with the promotions.
  
  # Sort by jobs.
  setorder(input_df, job)
  
  # For the years 1985-93, concursado was tipovinculo == '02' not
  # tipovinculo == '30' or '31'. Make this correction.
  if(year %in% 1985:1993) {
    
    input_df[, concursado := as.numeric(tipovinculo == '02')]
    
  }
  
  if(year %in% c(1994)) {
    
    concursado_workers <- input_df[concursado == 1 & firm_type == 'public_sector',]
    nonconcursado_workers <- input_df[concursado == 0 & firm_type == 'public_sector',]
    
  } else { 
    
    concursado_workers <- input_df[concursado == 1 & firm_type == 'public sector',]
    nonconcursado_workers <- input_df[concursado == 0 & firm_type == 'public sector',] 
    
    }
  
  rm(input_df)
  
  # Keep only relevant columns : job, year, code for multiple active jobs and
  # average wage over the year.
  relevant_columns <- c('job', 
                        'year',
                        'education',
                        'multiple_entries',
                        'multiple_active',
                        'cbo94',
                        'cbo2002',
                        'vlremunmediasm',
                        'anoadmissao',
                        'mesadmissao',
                        'mesdesligamento',
                        'race',
                        'gender',
                        'employer_name',
                        'employer',
                        'cnpj_cei',
                        'establishment')
  
  concursado_workers <- concursado_workers[, ..relevant_columns]
  nonconcursado_workers <- nonconcursado_workers[, ..relevant_columns]
  
  # Create additional date of employment and separation variables.
  concursado_workers[, date_emp := paste(mesadmissao, anoadmissao, sep = '-')]
  concursado_workers[, date_sep := paste(mesdesligamento, year, sep = '-')]
  
  nonconcursado_workers[, date_emp := paste(mesadmissao, anoadmissao, sep = '-')]
  nonconcursado_workers[, date_sep := paste(mesdesligamento, year, sep = '-')]
  
  # Convert wage to correct format.
  concursado_workers[, wage := gsub("\\.", "\\*", vlremunmediasm)]
  concursado_workers[, wage := gsub("\\,", "\\.", wage)]
  concursado_workers[, wage := gsub("\\*", "\\,", wage)]
  concursado_workers[, wage := as.numeric(wage)]
  
  nonconcursado_workers[, wage := gsub("\\.", "\\*", vlremunmediasm)]
  nonconcursado_workers[, wage := gsub("\\,", "\\.", wage)]
  nonconcursado_workers[, wage := gsub("\\*", "\\,", wage)]
  nonconcursado_workers[, wage := as.numeric(wage)]
  
  # Remove the wrongly formatted wage column.
  concursado_workers[, vlremunmediasm := NULL]
  nonconcursado_workers[, vlremunmediasm := NULL]
  
  # First way to define promotions is by classifying jobs (using the crosswalk)
  # as blue-collar, white-collar and managerial. Promotion is going from BC to WC
  # and from WC to managerial.
  
  # Add isco88-based labels by matching on cbo94 codes of the professions.
  occupation_crosswalk <- fread(glue('{harddrive}/intermediate/crosswalks/occupation_codes_crosswalk.csv'))
  occupation_crosswalk[, cbo2002 := NULL]
  concursado_workers <- occupation_crosswalk[concursado_workers, on = .(cboid == cbo94)]
  nonconcursado_workers <- occupation_crosswalk[nonconcursado_workers, on = .(cboid == cbo94)]
  
  # The strategy is as follows :
  # 1. Save (in a loop by appending) the dataset we have till now for all the years
  # from 1985 to 1995.
  # It has the job ids with corresponding variables.
  # 2. Load the entire thing in which contains data on all the 10 years, and then
  # perform operations on that to find promotion status in the 10 year frame.
  if(i == 1) {
    
    fwrite(concursado_workers, file = glue('{harddrive}/intermediate/public_sector_workers/concursado_workers_{year}.csv'),
           append = FALSE)
    fwrite(nonconcursado_workers, file = glue('{harddrive}/intermediate/public_sector_workers/nonconcursado_workers_{year}.csv'),
           append = FALSE)
    
  } else {
    
    fwrite(concursado_workers, file = glue('{harddrive}/intermediate/public_sector_workers/concursado_workers_{year}.csv'),
           append = TRUE)
    fwrite(nonconcursado_workers, file = glue('{harddrive}/intermediate/public_sector_workers/nonconcursado_workers_{year}.csv'),
           append = TRUE)
    
  }
  
} # For loop ends here, all batches for 1985, then 1986, so on.... done one by one.
beep() # For notification of loop end.  
##############################################################################






##############################################################################
# Final Promotions dataset now (only for concursado workers).
# Now, once we do this for all years till 1995, run the below code. If loop
# to avoid mistaken runs.
if(year == 1995){
  
  list <- list()
  years_list <- 1985:1995
  
  for(j in 1:length(years_list)) {
    
    year2 <- years_list[j]
    df <- fread(glue('{harddrive}/intermediate/public_sector_workers/concursado_workers_{year2}.csv'),
                colClasses = c(rep('character', 19), 'numeric'))
    
    list[[j]] <- df
    
    rm(df)
    
  }
  
  input_df <- data.table::rbindlist(list)
  rm(list)
  # All the operations below will only be run after 1995 done since we're still
  # in the if statement!  
  
  input_df[, group_index := 1:.N, by = job]
  
  # Now create a battery of variables to get the final output,
  # promotions and education upgrades.
  
  # Previous wage
  input_df[, prev_wage := shift(wage,1), by = job]
  
  # Percentage Increase
  input_df[, perc_inc := ((wage - prev_wage)/prev_wage)]
  
  # Promoted or demoted dummy
  input_df[, promoted := fcase(perc_inc >= 0.1 , 1,
                               perc_inc < 0, -1,
                               default = 0)]
  
  # Next promotion when? Use a rolling join method.
  df1 <- copy(input_df)
  df1 <- df1[, group_index := group_index + 1]
  df2 <- copy(input_df)
  df2 <- df2[promoted == 1, .(group_index, next_promotion = group_index, job)]
  input_df <- df2[df1, on = .(job, group_index), roll = -Inf]
  input_df[, group_index := group_index - 1]
  rm(df1, df2)
  
  # Next demotion when?
  df1 <- copy(input_df)
  df1 <- df1[, group_index := group_index + 1]
  df2 <- copy(input_df)
  df2 <- df2[promoted == -1, .(group_index, next_demotion = group_index, job)]
  input_df <- df2[df1, on = .(job, group_index), roll = -Inf]
  input_df[, group_index := group_index - 1]
  rm(df1, df2)
  
  # Next promotion before demotion?
  input_df[, prom_pre_dem := as.numeric(next_promotion < next_demotion)]
  
  # Create a sustained promotion variables. This is when you get promoted and
  # not get demoted again before getting promoted again.
  input_df[, sustained_prom := as.numeric(promoted == 1 & prom_pre_dem == 1)]
  
  # Now change NAs to 0.
  setnafill(input_df, cols = 'sustained_prom', fill = 0)
  
  # Now record the year of promotion.
  input_df[, promotion_year := fcase(sustained_prom == 1, year,
                                     sustained_prom == 0, '')]
  
  # Record if final termination happened, that is, the last record is
  # is a terminated record.
  input_df[, terminated := as.numeric(mesdesligamento[.N] != '00'), by = job]
  
  # Record separation date if terminated, else keep it blank.
  input_df[, date_sep := fcase(terminated == 1, date_sep,
                               terminated == 0, '')]
  ##################################################################
  ##################################################################
  # Now to education. We want to record sustained education upgrades
  # and their years.
  
  # But first, need to determine which education is higher than the other.
  input_df <- input_df[, educ_numeric := fcase(education == 'no_school', 0,
                                               education == 'less_than_hs', 1,
                                               education == 'hs_incomplete', 1,
                                               education == 'hs', 2,
                                               education == 'college_incomplete', 3,
                                               education == 'college', 4,
                                               education == 'college_plus', 5)]
  
  # Previous education
  input_df[, prev_educ := shift(educ_numeric,1), by = job]
  
  # Upgrades
  input_df[, educ_upgrade := educ_numeric - prev_educ]
  
  # Upgrade or degrade dummy
  input_df[, educ_upgrade := fcase(educ_upgrade > 0 , 1,
                                   educ_upgrade < 0, -1,
                                   default = 0)]
  
  # Next upgrade when? Use a rolling join method.
  df1 <- copy(input_df)
  df1 <- df1[, group_index := group_index + 1]
  df2 <- copy(input_df)
  df2 <- df2[educ_upgrade == 1, .(group_index, next_educ_upgrade = group_index, job)]
  input_df <- df2[df1, on = .(job, group_index), roll = -Inf]
  input_df[, group_index := group_index - 1]
  rm(df1, df2)
  
  # Next degrade when?
  df1 <- copy(input_df)
  df1 <- df1[, group_index := group_index + 1]
  df2 <- copy(input_df)
  df2 <- df2[educ_upgrade == -1, .(group_index, next_educ_degrade = group_index, job)]
  input_df <- df2[df1, on = .(job, group_index), roll = -Inf]
  input_df[, group_index := group_index - 1]
  rm(df1, df2)
  
  # Next upgrade before degrade?
  input_df[, upgrade_pre_degrade := as.numeric(next_educ_upgrade < next_educ_degrade)]
  
  # Create a sustained upgrade variables. This is when you upgrade and
  # not degrade again before another upgrade.
  input_df[, sustained_upgrade := as.numeric(educ_upgrade == 1 & upgrade_pre_degrade == 1)]
  
  # Now change NAs to 0.
  setnafill(input_df, cols = 'sustained_upgrade', fill = 0)
  
  # Now record the year of upgrade.
  input_df[, upgrade_year := fcase(sustained_upgrade == 1, year,
                                   sustained_upgrade == 0, '')]
  
  # Reorder
  input_df <- input_df[, .(job,
                           year,
                           multiple_entries,
                           multiple_active,
                           cboid,
                           cbo2002,
                           race,
                           gender,
                           employer_name,
                           employer,
                           cnpj_cei,
                           establishment,
                           anoadmissao,
                           mesadmissao,
                           mesdesligamento,
                           date_emp,
                           date_sep,
                           sustained_prom,
                           promotion_year,
                           sustained_upgrade,
                           upgrade_year,
                           terminated)]
  
  # Now collapse.
  collapsed_df <- input_df[, .(ever_promoted = max(sustained_prom, na.rm = T),
                               promotion_year = paste(unique(promotion_year[promotion_year != '']), collapse = ','),
                               ever_upgrade = max(sustained_upgrade, na.rm = T),
                               upgrade_year = paste(unique(upgrade_year[upgrade_year != '']), collapse = ','),
                               date_emp = first(date_emp),
                               date_sep = last(date_sep),
                               terminated = max(terminated, na.rm = T),
                               race = first(race),
                               gender = first(gender),
                               employer_name = first(employer_name),
                               employer = first(employer),
                               cnpj_cei = first(cnpj_cei),
                               establishment = first(establishment),
                               multiple_active = first(multiple_active),
                               multiple_entries = first(multiple_entries)), by = job]
  
  
  # Change some column names as per Prof. Mocanu
  colnames(collapsed_df)[which(colnames(collapsed_df) == 'promotion_year')] <- 'years_promoted'
  colnames(collapsed_df)[which(colnames(collapsed_df) == 'ever_upgrade')] <- 'upgrade_educ'
  colnames(collapsed_df)[which(colnames(collapsed_df) == 'upgrade_year')] <- 'upgrade_educ_year'
  colnames(collapsed_df)[which(colnames(collapsed_df) == 'date_emp')] <- 'date_employment'
  colnames(collapsed_df)[which(colnames(collapsed_df) == 'date_sep')] <- 'date_separation'
  colnames(input_df)[which(colnames(input_df) == 'cboid')] <- 'cbo94'
  
  # Save final output.
  fwrite(collapsed_df, file = glue('{harddrive}/intermediate/promotions/promotions_1985_to_1995_collapsed.csv'))
  fwrite(input_df, file = glue('{harddrive}/intermediate/promotions/promotions_1985_to_1995_combined.csv'))
  
  # Also save a dta for Prof (csv for me).
  haven::write_dta(collapsed_df, path = glue('{harddrive}/intermediate/promotions/promotions_1985_to_1995_collapsed.dta'))
  
  rm(collapsed_df)
  rm(input_df)
  
  gc()  
  
} # Close if statement
##############################################################################
