################## 
# MOHIT NEGI
# Last Updated : 27 July 2023
# Contact on : mohit.negi@studbocconi.it
################## 

################## OBJECTIVES
# This script performs the following tasks.
# 1. Scrape pdf files to create occupation codes crosswalk.
##################

################## PATHS
harddrive <- 'D:/Mohit_Work/MOCANU/RAIS_tasks'
box <- 'C:/Users/mohit/Box/mohit_ra'
##################

################## PACKAGES
# We will use these in this script.
packages <- c('glue',
              'tidyverse',
              'pdftools',
              'stringr',
              'haven',
              'stringi')

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
# First load in the entire pdf.
pdf_raw <- pdf_text(glue('{harddrive}/intermediate/crosswalks/pdftable.pdf'))

pdf_clean <- data.frame('cbo2002' = '', 'occupation' = '', 'cbo94' = '', 'ciuo88' = '')
# Now loop scraping over the pages.
for(pagenum in 1:length(pdf_raw)) {
  
  # Select just one page.
  page <- pdf_raw[pagenum] %>% str_split("\n")
  page <- page[[1]]
  
  # Trim to remove unneccessary spaces.
  page <- page %>% trimws()
  
  # Keep only relevant rows, remove top and bottom extra rows.
  page <- page[(grep("CBO2002 T", page) + 1):length(page)]
  page <- page[which(page != "")]
  page <- page[which(page != glue("{pagenum}"))]
  
  # Split the columns, doesn't work with first one, we deal with it later.
  page <- str_split_fixed(page, " {2,}", 3)
  
  # Now split first column. Also convert to ASCII characters and remove dashes.
  col12 <- page[,1]
  col1 <- str_sub(col12, 1, 7) %>% str_replace_all(pattern = '-', replacement = '')
  col2 <- str_sub(col12, 9) %>%  stri_trans_general("Latin-ASCII")
  
  # Remove dots and dashes from occupational codes.
  col3 <- page[,2] %>% str_replace_all(pattern = '[:punct:]', replacement = '')
  col4 <- page[,3]
  
  # Combine everything.
  page <- cbind(col1, col2, col3, col4)
  
  # Finally, convert to dataframe.
  page <- data.frame(page)
  names(page) <- c('cbo2002', 'occupation', 'cbo94', 'ciuo88')
  
  pdf_clean <- rbind(pdf_clean, page)

}

# Remove first dummy row.
pdf_clean <- pdf_clean[2:nrow(pdf_clean),]

# Save as STATA crosswalk.
write_dta(pdf_clean, glue('{harddrive}/intermediate/crosswalks/crosswalk_cbo94_2002.dta'))
###############################################################################################







