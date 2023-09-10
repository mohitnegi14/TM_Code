################## 
# MOHIT NEGI
# Last Updated : 7 September 2023
# Contact on : mohit.negi@studbocconi.it
################## 

################## OBJECTIVES
# This script performs the following tasks.
# 1. Rectify the issue with some cnpj_cei ids which didn't have
# leading zeroes.
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
# Here is the deal,
# For the years 1985 to 1995, the script 3. Promotions makes changes to
# allstates....newvars but still the workplaces datasets are wrong.
# This script corrects that.
# For the years beyond 1995, we need to make changes to allstates....newvars
# as well as workplaces.
##############################################################################


##################
##################
year_list <- c(1985,1986,1987,1988,1989,1990,1991,1992,1993,1994,1995,1996,1997,1998,2000,
               2001,2002,2003,2004,2005,2006,2007)
##################
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
  
  print(glue('Working on batch {i} of {length(batches)}'))

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
  
  ####################################################################
  ############## Now re-save the data since we fixed the cnpj_cei ids.
  if(i == 1) {
    
    fwrite(input_df, file = glue('{output}/allstates_blind_{year}_newvars.csv'),
           append = FALSE)
    
  } else {
    
    fwrite(input_df, file = glue('{output}/allstates_blind_{year}_newvars.csv'),
           append = TRUE)
    
  }
  ##############
  ####################################################################
  
  
  # The below tasks will be for both cases.
  ####################################################################
  
  # For the next tasks, only some columns needed.
  if(!('mesadmissao' %in% colnames(input_df)) | 
     !('anoadmissao' %in% colnames(input_df)) | 
     !('mesdesligamento' %in% colnames(input_df))) {
    
    # Find potentially differently spelled month of ending job.
    endmonth_variable_index <- which(str_detect(colnames(input_df), pattern = 'me') & 
                                       str_detect(colnames(input_df), pattern = 'desl'))
    endmonth_variable <- colnames(input_df)[endmonth_variable_index]
    
    # Find potentially differently spelled month of starting job.
    startmonth_variable_index <- which(str_detect(colnames(input_df), pattern = 'me') & 
                                         str_detect(colnames(input_df), pattern = 'admis'))
    startmonth_variable <- colnames(input_df)[startmonth_variable_index]
    
    # Find potentially differently spelled year of starting job.
    startyear_variable_index <- which(str_detect(colnames(input_df), pattern = 'ano') & 
                                        str_detect(colnames(input_df), pattern = 'admis'))
    startyear_variable <- colnames(input_df)[startyear_variable_index]
    
    # Now, it might be that there is no startmonth and startyear (like in 2002),
    # So for them, we will have starting date.
    if(length(startmonth_variable) == 0 | length(startyear_variable) == 0) {
      
      startdate_variable_index <- which(str_detect(colnames(input_df), pattern = 'dt') & 
                                          str_detect(colnames(input_df), pattern = 'admis'))
      startdate_variable <- colnames(input_df)[startdate_variable_index]
      
      # Label it as case 3.
      case <- 3
      
    } else case <- 2 # Mis-spelled case.
    
  } else case <- 1 # Normal case.
  
  # In cases 2 and 3, we need to do some work to get the correct columns for the
  # later stages.
  if(case == 2) {
    
    colnames(input_df)[startyear_variable_index] <- 'anoadmissao'
    colnames(input_df)[startmonth_variable_index] <- 'mesadmissao'
    colnames(input_df)[endmonth_variable_index] <- 'mesdesligamento'
    
  } else if(case == 3) {
    
    # Create mesadmissao in similar way as other years, using the dtadmissao variable.
    input_df[, (startdate_variable) := lubridate::dmy(as.numeric(get(startdate_variable)))]
    input_df[, anoadmissao := year(get(startdate_variable))]
    input_df[, mesadmissao := as.numeric((anoadmissao == year)*(month(get(startdate_variable))))]
    
    # Rename end month variable to correct name.
    colnames(input_df)[endmonth_variable_index] <- 'mesdesligamento'
    
  }
  
  # Now, only certain columns needed.
  input_df2 <- input_df[, c('cnpj_cei',
                            'establishment',
                            'municipality_code',
                            'pis_encoded',
                            'anoadmissao',
                            'mesadmissao',
                            'mesdesligamento')]
  
  rm(input_df)
  gc()
  
  ## 3. Workplace dataframe separately.
  # Convert months to numeric.
  input_df2[, `:=`(mesadmissao = as.numeric(mesadmissao),
                   mesdesligamento = as.numeric(mesdesligamento))]
  
  for(refmonth in 1:12) {
    
    print(glue('Working on month {refmonth} of 12'))
    
    new_df <- input_df2
    new_df[, month := refmonth*as.numeric(mesadmissao <= refmonth & (mesdesligamento >= refmonth | mesdesligamento == 0))]
    new_df <- new_df[month == refmonth]
    new_df[, workplace := paste(establishment, glue('{year}'), month, sep = '_')]
    
    new_df <- new_df[, c('workplace', 'pis_encoded')]
    
    if(i == 1) {
      
      fwrite(new_df, file = glue('{output}/workplacedata_month{refmonth}_{year}.csv'),
             append = FALSE)
      
    } else {
      
      fwrite(new_df, file = glue('{output}/workplacedata_month{refmonth}_{year}.csv'),
             append = TRUE)
      
    }
    
    rm(new_df)
    
  }
  
  rm(input_df2)
  gc()  
  
}  
##############################################################################
##################################### END ####################################