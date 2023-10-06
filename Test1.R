################## 
# MOHIT NEGI
# Last Updated : 28 September 2023
# Contact on : mohit.negi@studbocconi.it
################## 

################## OBJECTIVES
# This script performs the following tasks.
# 1. Checks a command to load in specific rows from a .csv
##################

################## PATHS
raw <- 'C:/Users/mohit/Desktop/EP/TM/TM_Data/Raw'
cleaned <- 'C:/Users/mohit/Desktop/EP/TM/TM_Data/Cleaned'
output <- 'C:/Users/mohit/Desktop/EP/TM/TM_Data/Output'
##################

################## PACKAGES
# We will use these in this script.
packages <- c('glue',
              'data.table',
              'sqldf')

# Installs them if not yet installed.
installed_packages <- packages %in% rownames(installed.packages())
if(any(installed_packages == FALSE)) { install.packages(packages[!installed_packages]) }

# And load them in.
invisible(lapply(packages, library, character.only = T))

# Disable scientific notation.
options(scipen = 999)

rm(packages, installed_packages)
##################

# start.time <- Sys.time()
# df1 <- data.table::fread(glue('{raw}/promotions_1988.csv'),
#                          select = 'year')
# df1 <- df1[multiple_entries == 1]
# end.time <- Sys.time()
# time.taken_ineff <- end.time - start.time
# time.taken_ineff

# Took about 7 minutes.

# start.time <- Sys.time()
# df2 <- sqldf::read.csv.sql(glue('{raw}/workplacedata_month1_1988.csv'),
#                            colClasses = c(rep('character', 2)),
#                            "select * from file where pis_encoded = '000000000191'",
#                            sep = ',')
# end.time <- Sys.time()
# time.taken_eff <- end.time - start.time
# time.taken_eff

start.time <- Sys.time()
df2 <- fread('C:/Users/mohit/Box/mohit_ra/intermediate/workplaces/workplaces_1988/workplacedata_month1_1988.csv',
                           colClasses = c(rep('character', 2)))
end.time <- Sys.time()
time.taken_eff <- end.time - start.time
time.taken_eff

# person1_workplaces <- df2[pis_encoded == '000000000191', .(workplace)]
# person1_workplaces <- person1_workplaces$workplace
# 
# person2_workplaces <- sample_n(person1_workplaces, 16)
# person2_workplaces <- c('234234','12424','345245','123243','45t34','4234','234','23434')
# 
# match1 <- max(as.numeric(person1_workplaces %in% person2_workplaces))
# 
# 
# # 154 needed.
# # Took about 7 minutes.
# 
# df2 <- as.data.table(df2)
# 
# df2 <- df2[, .(number = .N), by = pis_encoded]
# 
# 000000000191