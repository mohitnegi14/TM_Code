################## 
# MOHIT NEGI
# Last Updated : 7 August 2023
# Contact on : mohit.negi@studbocconi.it
################## 

################## PATHS
data_folder <- 'C:/Users/anjun/Box/mohit_ra/data/rais/deidentified'
# data_output_folder <- 'C:/Users/anjun/Box/mohit_ra/intermediate'
data_output_folder <- 'C:/Users/anjun/OneDrive/Desktop/EP/TM/TM_Data/Output'
raw <- 'C:/Users/anjun/OneDrive/Desktop/EP/TM/TM_Data/Raw'
cleaned <- 'C:/Users/anjun/OneDrive/Desktop/EP/TM/TM_Data/Cleaned'
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

##################
years_list <- c(1985, 1986, 1987, 1988, 1989, 1990, 1991, 1992, 1993, 1994, 1995)
# 
# start_from <- 2
# installed_till <- 3

# for(year in years_list[start_from:installed_till]) {

for(year in 1995) {
    
  # # First read them in one year at a time because of space and memory constraints.
  input_df <- read_dta(glue('{raw}/allstates_blind_{year}.dta'))
  # 
  # # See a small sample to ensure everything is OK.
  test <- sample_n(input_df, 100)
  # 
  # # First save it as a csv file for faster read/write.
  fwrite(input_df, file = glue('{raw}/allstates_blind_{year}.csv'))
  
  number_of_columns <- ncol(input_df)
  
  # Now remove as unneeded.
  rm(input_df)
  
  # Now load it in to work on it. 35 for 1985, 34 for 1990.
  input_df <- fread(glue('{raw}/allstates_blind_{year}.csv'),
                    colClasses = c(rep('character', number_of_columns)))
  
  # # See a small sample to ensure everything is OK.
  test <- sample_n(input_df, 100)
  
  ## 1.
  # Defining employer as first 8 digits of cnpj_cei.
  input_df[, employer := str_sub(cnpj_cei, 1, 8)]
  
  ## 2. 
  # Defining an establishment.
  input_df[, establishment := fcase(
    municipality_code %in% c('', '.'), paste(cnpj_cei, municipio, sep = '_'), 
    !(municipality_code %in% c('', '.')), paste(cnpj_cei, municipality_code, sep = '_'))]
  
  # Save it.
  fwrite(input_df, file = glue('{data_output_folder}/allstates_blind_{year}_newvars.csv'))
  
  # Now only need a smaller set of columns.
  input_df <- input_df[, c('cnpj_cei',
                           'establishment',
                           'anoadmissao', 
                           'mesadmissao', 
                           'mesdesligamento',
                           'pis_encoded')] 
  
  ## 3.
  # Redundancy table : bit unclear on what "annual counts" means, 
  # is the year admission year or 1985 (i.e. only one year in this test case.)
  redundancy_table <- input_df[, .(unique_cnpj_cei = uniqueN(cnpj_cei, na.rm = T),
                                   unique_establishments = uniqueN(establishment, na.rm = T))]
  
  # Add the year label.
  redundancy_table[, year := glue('{year}')]
  
  colnames(redundancy_table) <- c('Unique cnpj_cei', 'Unique Establishments', 'Year')
  
  # Save to join later.
  fwrite(redundancy_table, file = glue('{cleaned}/redundancy_table_{year}.csv'))
  
  ## 4. Workplace dataframe separately.
  # First remove the default pis code.
  input_df <- input_df[pis_encoded != '000000000001',]
  
  # Convert the months back to numeric.
  input_df[, `:=`(mesadmissao = as.numeric(mesadmissao),
                  mesdesligamento = as.numeric(mesdesligamento))]
  
  for(refmonth in 1:12) {
    
    print(glue('Working on month {refmonth} of 12'))
    
    new_df <- input_df
    new_df[, month := refmonth*as.numeric(mesadmissao <= refmonth & (mesdesligamento >= refmonth | mesdesligamento == 0))]
    new_df <- new_df[month == refmonth]
    new_df[, workplace := paste(establishment, glue('{year}'), month, sep = '_')]
    
    new_df <- new_df[, c('workplace', 'pis_encoded')]
    
    fwrite(new_df, file = glue('{data_output_folder}/workplacedata_month{refmonth}_{year}.csv'))
    
    rm(new_df)
    
  }
  
  rm(input_df)
  
  workplace_df <- fread(glue('{data_output_folder}/workplacedata_month1_{year}.csv'),
                        colClasses = c(rep('character', 2)))
  
  workplace_df[1000:1100,] # Looks good!
  
  rm(workplace_df)
  
  # 5. Investigate extra establishments.
  input_df <- fread(glue('{data_output_folder}/allstates_blind_{year}_newvars.csv'),
                    colClasses = c(rep('character', (number_of_columns + 2))))
  
  # First check if missing municipality_code imputed correctly for the establishment variable.
  ccc <- input_df
  
  input_df <- input_df[, c('municipality_code', 'establishment')]
  test <- input_df[100000:110000,]
  
  ccc1 <- ccc[, .(unique_estabs = uniqueN(establishment)), by = cnpj_cei]
  
  ccc2 <- ccc1[unique_estabs > 1,]
  
  ccc <- ccc[cnpj_cei %in% ccc2$cnpj_cei]
  
  fwrite(ccc2, file = glue('{data_output_folder}/repeatnumber_by_cnpj_{year}.csv'), quote = T)
  fwrite(ccc, file = glue('{data_output_folder}/repeated_cnpj_{year}.csv'), quote = T)
  
}

# # Make the final redundancy table.
# list_of_tables <- list()
# for(i in 1:length(years_list)) {
#   
#   year = years_list[i]
#   redundancy_table <- fread(glue('{cleaned}/redundancy_table_{year}.csv'), colClasses = c(rep('character', 3))) %>% 
#     select('Year', everything()) # Make the Year column first.
#   colnames(redundancy_table) <- c('Year', 'Unique Firms', 'Unique Establishments')
#   list_of_tables[[i]] <- redundancy_table
#   
# }
# redundancy_table_full <- bind_rows(list_of_tables)
# 
# redundancy_table_full %>%
#   kbl(caption = "Redundancy Table", format = "latex", booktabs = T, align = rep('c', 3),
#       linesep = "",
#       longtable = T,
#       escape = F
#       ) %>%
#   kable_classic() %>%
#   kable_styling(full_width = TRUE) %>% 
#   footnote(general = "Firms are identified by their unique IDs (cnpj_cei) and Establishments are defined as unique Firm x Municipalities. As is evident from differing values in the two columns, some firms might correspond to multiple municipalities (and hence establishments). This necessitates the creation of our new Establishment variable.",
#            footnote_as_chunk = T,
#            threeparttable = T) %>%
#   writeLines(glue('{data_output_folder}/redundancy_table.tex'))

############################################## END.

# footnote(general = "Firms are identified by their unique IDs - cnpj_cei and Establishments are defined as unique Firm x Municipalities. 
#            As is evident from differing values in the two columns, some firms might correspond to multiple municipalities (and hence establishments).
#            This necessitates the creation of our new Establishment variable.") %>% 
#   
############################################## EXTRA STUFF


# First of all, it seems that those who have admission/termination dates outside 1985 have 0 as
# the value in msadmisso and msdesligamento. But Professor has said 0 indicates missing date of ad/term.
# Get this cleared up.
# sample_table <- sample_n(input_df, 1000) %>% arrange(msadmisso)
# Note that the msadmisso is non 0 for all those who have anoadmisso in 1985,
# And it is 0 for all those who have it before.
# The code below does the job when my this assumption is correct. Otherwise, we will need to 
# remove the missing ones for the workplace.
# fwrite(sample_table, file = 'sampletable.csv')


# ## 5.
# # Missing admission/termination dates.
# relevant_df2 <- input_df[, c('anoadmissao', 
#                             'mesadmissao', 
#                             'mesdesligamento',
#                             'pis_encoded')]
# 
# relevant_df2[, `:=`(mesadmissao = as.numeric(mesadmissao),
#                    mesdesligamento = as.numeric(mesdesligamento))]
# 
# relevant_df2[, `:=`(missing_anoadmissao = as.numeric(anoadmissao == '{ c'),
#                     missing_mesadmissao = as.numeric(mesadmissao == 0),
#                     missing_mesdesligamento = as.numeric(mesdesligamento == 0))]
# 
# missing_table <- relevant_df2[, .(missing_anoadmissao = sum(missing_anoadmissao, na.rm = T),
#                                   missing_mesadmissao = sum(missing_mesadmissao, na.rm = T),
#                                   missing_mesdesligamento = sum(missing_mesdesligamento, na.rm = T)),
#                               by = pis_encoded]
# 
# missing_table_relevant <- missing_table[(missing_anoadmissao != 0 | 
#                                            missing_mesadmissao != 0 |
#                                            missing_mesdesligamento != 0),]

# # Save it as tex table.
# missing_table %>%
#   kbl(caption = "Missing Admission/Termination Table",
#       format = "latex",
#       booktabs = T) %>%
#   kable_classic(html_font = "Cambria") %>% writeLines(glue('{data_output_folder}/outputs/tabs/missing_admission_table.tex'))
