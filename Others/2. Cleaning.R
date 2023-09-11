################## 
# MOHIT NEGI
# Last Updated : 10 September 2023
# Contact on : mohit.negi@studbocconi.it
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

################## PATHS
global <- '' # This can be filled by the user of this code according to their own needs.
data_output_folder <- glue('{global}/Box/mohit_ra/intermediate')
raw <- glue('{global}/Box/mohit_ra/data/rais/deidentified')
##################

##################
years_list <- 1985:2017
super_large_files <- c(1999, 2008:2017)

#
# The super large years are those whose RAIS files are larger than 20GB. 
# I was not able to run these due to lack of RAM and storage space on my computer.
# These super large ones still need to be processed!
#

years_list <- setdiff(years_list, super_large_files)

for(year in years_list) {
    
  # First, just read in the first column to get the number of rows.
  input_df <- read_dta(glue('{raw}/allstates_blind_{year}.dta'),
                       col_select = 1)
  
  # input_df2 <- fread(glue('{raw}/allstates_blind_{year}.csv'),
  #                    select = 'dtadmissao')
  
  number_of_rows <- nrow(input_df)
  
  rm(input_df)
  gc()
  
  # Divide up the rows in smaller batches (of a million each) for reading with limited RAM.
  batches <- seq(0, number_of_rows, by = 1000000)
  ##############################################################################
  
  
  
  ##############################################################################
  for(i in 1:length(batches)) {
    
    input_df <- read_dta(glue('{raw}/allstates_blind_{year}.dta'),
                         n_max = 1000000,
                         skip = batches[i])
    
    if(i == 1) {
      
      fwrite(input_df, file = glue('{raw}/allstates_blind_{year}.csv'),
             append = FALSE)
      
    } else {
      
      fwrite(input_df, file = glue('{raw}/allstates_blind_{year}.csv'),
             append = TRUE)
      
    }
    
    rm(input_df)
    
  }
  ##############################################################################
  
  gc()
  
  ##############################################################################
  # Load just the columns to determine first.
  names_of_columns <- fread(glue('{raw}/allstates_blind_{year}.csv'),
                            nrows = 0) %>% colnames()
  
  # Track number of columns too.
  number_of_columns <- length(names_of_columns)
  ##############################################################################
  
  # Divide up the rows in smaller batches (of a million each) for reading with limited RAM.
  # Starting from 1 now because fread works a bit different than read_dta.
  batches[1] <- 1
  
  ##############################################################################
  for(i in 1:length(batches)) {
    
    # Now load it in to work on it.
    input_df <- fread(glue('{raw}/allstates_blind_{year}.csv'),
                      colClasses = c(rep('character', number_of_columns)),
                      nrows = 1000000,
                      skip = batches[i],
                      header = F)
    
    # Do this, otherwise the 1000000th row will repeat, since the first batch ends at 1000000
    # and the second starts at the same.
    if(i == 1) {input_df <- input_df[-1000000,]} else {input_df <- input_df}
    
    colnames(input_df) <- names_of_columns
    
    ## 1.
    # First add leading zeroes when missing.
    input_df[, cnpj_cei := str_pad(cnpj_cei, width = 14, side = 'left', pad = '0')]
    
    # Defining employer as first 8 digits of cnpj_cei.
    input_df[, employer := str_sub(cnpj_cei, 1, 8)]
    
    ## 2. 
    # Defining an establishment.
    input_df[, establishment := fcase(
      municipality_code %in% c('', '.'), paste(cnpj_cei, municipio, sep = '_'), 
      !(municipality_code %in% c('', '.')), paste(cnpj_cei, municipality_code, sep = '_'))]
    
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
    
    if(i == 1) {
      
      fwrite(input_df, file = glue('{data_output_folder}/allstates_blind_{year}_newvars.csv'),
             append = FALSE)
      
    } else {
      
      fwrite(input_df, file = glue('{data_output_folder}/allstates_blind_{year}_newvars.csv'),
             append = TRUE)
      
    }
    
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
        
        fwrite(new_df, file = glue('{data_output_folder}/workplacedata_month{refmonth}_{year}.csv'),
               append = FALSE)
        
      } else {
        
        fwrite(new_df, file = glue('{data_output_folder}/workplacedata_month{refmonth}_{year}.csv'),
               append = TRUE)
        
      }
      
      rm(new_df)
      
    }
    
    rm(input_df2)
    gc()  
    
  }
  ##############################################################################     
  
  
  
  ##############################################################################     
  # Check if everything was good.
  workplace_df <- fread(glue('{data_output_folder}/workplacedata_month1_{year}.csv'),
                        colClasses = c(rep('character', 2)),
                        nrows = 1000)
  
  workplace_df
  # Now remove as unneeded.
  rm(workplace_df)
  ##############################################################################     
  
  
  
  ##############################################################################  
  ## 4.
  # Redundancy table
  input_df <- fread(glue('{data_output_folder}/allstates_blind_{year}_newvars.csv'),
                    colClasses = c(rep('character', (number_of_columns + 2))),
                    select = c('cnpj_cei', 'establishment', 'municipality_code'))
  
  redundancy_table <- input_df[, .(unique_cnpj_cei = uniqueN(cnpj_cei, na.rm = T),
                                   unique_establishments = uniqueN(establishment, na.rm = T))]
  
  # Add the year label.
  redundancy_table[, year := glue('{year}')]
  
  colnames(redundancy_table) <- c('Unique cnpj_cei', 'Unique Establishments', 'Year')
  
  # Save to join later.
  fwrite(redundancy_table, file = glue('{cleaned}/redundancy_table_{year}.csv'))
  
  # First check if missing municipality_code imputed correctly for the establishment variable.
  test_impute <- sample_n(input_df, 10000)
  
  # 5. Investigate extra establishments.
  # Now find the problematic cnpj_cei IDs.
  estabs_per_cnpj <- input_df[, .(unique_estabs = uniqueN(establishment, na.rm = TRUE)), by = cnpj_cei]
  estabs_per_cnpj <- estabs_per_cnpj[unique_estabs > 1,]
  
  # Save it.
  fwrite(estabs_per_cnpj, file = glue('{data_output_folder}/repeatnumber_by_cnpj_{year}.csv'), quote = T)
  
  # Remove as no longer needed.
  rm(input_df)
  ##############################################################################
  
  
  
  ##############################################################################
  for(j in 1:length(batches)) {
    
    # Now keep only the ones corresponding to cnpj that have multiple establishments.
    input_df3 <- fread(glue('{data_output_folder}/allstates_blind_{year}_newvars.csv'),
                       colClasses = c(rep('character', (number_of_columns + 2))),
                       nrows = 1000000,
                       skip = batches[j],
                       header = F)
    
    # Do this, otherwise the 1000000th row will repeat, since the first batch ends at 1000000
    # and the second starts at the same.
    if(j == 1) {input_df3 <- input_df3[-1000000,]} else {input_df3 <- input_df3}
    
    colnames(input_df3) <- c(names_of_columns, 'employer', 'establishment')
    
    input_df3 <- input_df3[cnpj_cei %in% estabs_per_cnpj$cnpj_cei]
    
    if(j == 1) {
      
      fwrite(input_df3, file = glue('{data_output_folder}/repeated_cnpj_{year}.csv'), quote = T,
             append = FALSE)
      
    } else {
      
      fwrite(input_df3, file = glue('{data_output_folder}/repeated_cnpj_{year}.csv'), quote = T,
             append = TRUE)
      
    }
    
    rm(input_df3)
    
  }
  ##############################################################################
  
  gc()
  
  ##############################################################################
  
}

# Make the final redundancy table.
list_of_tables <- list()
for(i in 1:length(years_list)) {

  year = years_list[i]
  redundancy_table <- fread(glue('{data_output_folder}/outputs/tabs/redundancy_table_{year}.csv'), colClasses = c(rep('character', 3))) %>%
    select('Year', everything()) # Make the Year column first.
  
  redundancy_table <- redundancy_table %>% 
    mutate('Slippage' = as.numeric(`Unique cnpj_cei`)/as.numeric(`Unique Establishments`))
  
  colnames(redundancy_table) <- c('Year', 'Unique Firms', 'Unique Establishments', 'Slippage')
  
  list_of_tables[[i]] <- redundancy_table

}
redundancy_table_full <- bind_rows(list_of_tables)

redundancy_table_full %>%
  kbl(caption = "Redundancy Table", format = "latex", booktabs = T, align = rep('c', 3),
      linesep = "",
      longtable = T,
      escape = F
      ) %>%
  kable_classic() %>%
  kable_styling(full_width = TRUE) %>%
  footnote(general = "Firms are identified by their unique IDs (cnpj_cei) and Establishments are defined as unique Firm x Municipalities. As is evident from differing values in the two columns, some firms might correspond to multiple municipalities (and hence establishments). This necessitates the creation of our new Establishment variable.",
           footnote_as_chunk = T,
           threeparttable = T) %>%
  writeLines(glue('{data_output_folder}/outputs/tabs/redundancy_table.tex'))

############################################## END.

