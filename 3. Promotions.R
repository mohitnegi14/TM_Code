################## 
# MOHIT NEGI
# Last Updated : 7 September 2023
# Contact on : mohit.negi@studbocconi.it
################## 

################## OBJECTIVES
# This script performs the following tasks.
# 1. Rectify the issue with some cnpj_cei ids which didn't have
# leading zeroes.
# 2. Create a dataset called promotions_1985_to_1995 that labels
# whether a job (estab x pis) received a promotion in the 10 year
# span, defined as a sustained 10% or larger wage bump.
# Also lables if the job received an educational upgrade.
##################

################## PATHS
raw <- 'C:/Users/mohit/Desktop/EP/TM/TM_Data/Raw'
cleaned <- 'C:/Users/mohit/Desktop/EP/TM/TM_Data/Cleaned'
output <- 'C:/Users/mohit/Desktop/EP/TM/TM_Data/Output'
##################

################## LIBRARIES
library(tidyverse)
library(data.table)
library(haven)
library(glue)
library(stringr)
library(stringi)
library(kableExtra)
options(scipen = 999)
##################


##############################################################################
###########
###########
year <- 1985  
###########
###########
# Just read in the first column to get the number of rows.
df1_numberofrows <- fread(glue('{raw}/allstates_blind_{year}_newvars.csv'),
                          select = 'anoadmissao')

number_of_rows <- nrow(df1_numberofrows)

rm(df1_numberofrows)
gc()

# Divide up the rows in smaller batches (of a million each) for reading with limited RAM.
batches <- seq(0, number_of_rows, by = 1000000)
##############################################################################


##############################################################################
# Load just the columns to determine first.
names_of_columns <- fread(glue('{raw}/allstates_blind_{year}_newvars.csv'),
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
for(i in 1:length(batches)) {

  # Now load it in to work on it.
  input_df <- fread(glue('{raw}/allstates_blind_{year}_newvars.csv'),
                    colClasses = c(rep('character', number_of_columns)),
                    nrows = 1000000,
                    skip = batches[i],
                    header = F)
  
  # Do this, otherwise the 1000000th row will repeat, since the first batch ends at 1000000
  # and the second starts at the same.
  if(i == 1) {input_df <- input_df[-1000000,]} else {input_df <- input_df}
  
  # Add the column names.
  colnames(input_df) <- names_of_columns
  
  ####################################################################
  ####################################################################
  # Make corrections to cnpj_cei : Some leading zeroes were missing in
  # the original raw files (need 14 digits in a cnpj_cei id), I am adding them and rewriting the columns.
  # The establishment and employer variables need to be changed too now.
  
  ## First, cnpj_cei
  input_df[, cnpj_cei := str_pad(cnpj_cei, width = 14, side = 'left', pad = '0')]
  
  ## Now, employer
  input_df[, employer := str_sub(cnpj_cei, 1, 8)]
  
  ## Finally, re-do establishment
  input_df[, establishment := fcase(
    municipality_code %in% c('', '.'), paste(cnpj_cei, municipio, sep = '_'), 
    !(municipality_code %in% c('', '.')), paste(cnpj_cei, municipality_code, sep = '_'))]
  ####################################################################  
  ####################################################################
  
  # Construct a concursado variable.
  input_df[, concursado := as.numeric(tipovinculo == '30' | tipovinculo == '31')]
  
  # Now create a "job" variable that is a unique establishment-pis pair.
  input_df[, job := paste(establishment, pis_encoded, sep = '_')]
  
  # Remove the catch-all pis code.
  input_df <- input_df[pis_encoded != '000000000001',]
  
  # Flag the ones with more than 1 jobs in a year. These might be super
  # complicated to deal with. Separately flag those who had multiple active
  # jobs at the end of the year.
  input_df <- input_df[, multiple_entries := as.numeric(.N > 1), by = job]
  input_df <- input_df[, multiple_active := as.numeric(length(mesdesligamento[mesdesligamento == '00']) > 1), by = job]
  
  input_df[, year := year]
  
  ############## Now re-save the data since we fixed the cnpj_cei ids.
  if(i == 1) {
    
    fwrite(input_df, file = glue('{output}/allstates_blind_{year}_newvars.csv'),
           append = FALSE)
    
  } else {
    
    fwrite(input_df, file = glue('{output}/allstates_blind_{year}_newvars.csv'),
           append = TRUE)
    
  }
  ##############
  
  ############## Now starting with the promotions.
  
  # Sort by jobs.
  setorder(input_df, job)
  
  # Keep only concursado workers and public sector jobs.
  input_df[concursado == 1 & firm_type == 'public sector']
  
  # Keep only relevant columns : job, year, code for multiple active jobs and
  # average wage over the year.
  relevant_columns <- c('job', 
                        'year',
                        'education',
                        'multiple_entries',
                        'multiple_active',
                        'cbo2002',
                        'vlremunmediasm',
                        'anoadmissao',
                        'mesadmissao',
                        'mesdesligamento')
  
  input_df <- input_df[, ..relevant_columns]
  
  # Create additional date of employment and separation variables.
  input_df[, date_emp := paste(mesadmissao, anoadmissao, sep = '-')]
  input_df[, date_sep := paste(mesdesligamento, year, sep = '-')]
  
  # Convert wage to correct format.
  input_df[, wage := gsub("\\.", "\\*", vlremunmediasm)]
  input_df[, wage := gsub("\\,", "\\.", wage)]
  input_df[, wage := gsub("\\*", "\\,", wage)]
  input_df[, wage := as.numeric(wage)]
  
  # Remove the wrongly formatted wage column.
  input_df[, vlremunmediasm := NULL]
  
  # The strategy is as follows :
  # 1. Save (in a loop by appending) the dataset we have till now for all the years
  # from 1985 to 1995.
  # It has the job ids with corresponding variables.
  # 2. Load the entire thing in which contains data on all the 10 years, and then
  # perform operations on that to find promotion status in the 10 year frame.
  if(i == 1) {
    
    fwrite(input_df, file = glue('{data_output_folder}/promotions_{year}.csv'),
           append = FALSE)
    
  } else {
    
    fwrite(input_df, file = glue('{data_output_folder}/promotions_{year}.csv'),
           append = TRUE)
    
  }
  
} # For loop ends here, all batches for 1985, then 1986, so on.... done one by one.
##############################################################################


# Final Promotions dataset now.
##############################################################################
# Now, once we do this for all years till 1995, run the below code. If loop
# to avoid mistaken runs.
if(year == 1995){

list <- list()
years_list <- 1985:1995

for(j in 1:length(years_list)) {
  
  year2 <- years_list[j]
  df <- fread(glue('{data_output_folder}/promotions_{year2}.csv'),
              colClasses = c(rep('character', 13)))
  
  list[[j]] <- df
  
}

input_df <- bind_rows(list)

# All the operations below will only be run after 1995 done since we're still
# in the if statement!  

# Set key for efficiency.
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

# Next demotion when?
df1 <- copy(input_df)
df1 <- df1[, group_index := group_index + 1]
df2 <- copy(input_df)
df2 <- df2[promoted == -1, .(group_index, next_demotion = group_index, job)]
input_df <- df2[df1, on = .(job, group_index), roll = -Inf]
input_df[, group_index := group_index - 1]

# Next promotion before demotion?
input_df[, prom_pre_dem := as.numeric(next_promotion < next_demotion)]

# Create a sustained promotion variables. This is when you get promoted and
# not get demoted again before getting promoted again.
input_df[, sustained_prom := as.numeric(promoted == 1 & prom_pre_dem == 1)]

# Now change NAs to 0.
setnafill(input_df, cols = 'sustained_prom', fill = 0)

# Now record the year of promotion.
input_df[, promotion_year := year*sustained_prom]

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

# Next degrade when?
df1 <- copy(input_df)
df1 <- df1[, group_index := group_index + 1]
df2 <- copy(input_df)
df2 <- df2[educ_upgrade == -1, .(group_index, next_educ_degrade = group_index, job)]
input_df <- df2[df1, on = .(job, group_index), roll = -Inf]
input_df[, group_index := group_index - 1]

# Next upgrade before degrade?
input_df[, upgrade_pre_degrade := as.numeric(next_educ_upgrade < next_educ_degrade)]

# Create a sustained upgrade variables. This is when you upgrade and
# not degrade again before another upgrade.
input_df[, sustained_upgrade := as.numeric(educ_upgrade == 1 & upgrade_pre_degrade == 1)]

# Now change NAs to 0.
setnafill(input_df, cols = 'sustained_upgrade', fill = 0)

# Now record the year of upgrade.
input_df[, upgrade_year := year*sustained_upgrade]

# Reorder
input_df <- input_df[, .(job,
                       year,
                       multiple_entries,
                       multiple_active,
                       cbo2002,
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
                           promotion_year = paste(unique(promotion_year), collapse = ','),
                           ever_upgrade = max(sustained_upgrade, na.rm = T),
                           upgrade_year = paste(unique(upgrade_year), collapse = ','),
                           date_emp = first(date_emp),
                           date_sep = last(date_sep),
                           terminated = max(terminated, na.rm = T),
                           multiple_active = first(multiple_active),
                           multiple_entries = first(multiple_entries)), by = job]


# Save final output.
fwrite(collapsed_df, file = glue('{data_output_folder}/promotions_1985_to_1995.csv'))

rm(collapsed_df)
rm(input_df)

gc()  

} # Close if statement
##############################################################################
##################################### END ####################################