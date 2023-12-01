###############################################################################################
################## INTRO
# MOHIT NEGI
# Last Updated : 8 October 2023
# Contact on : mohit.negi@studbocconi.it
################## 

################## OBJECTIVES
# This script performs the following tasks.
# 1. Merges data on whether the establishments are federal, state or municipal recruiters and size of the establishment.
# 2. Conducts a DiD analysis on effects of a 1989 reform on percentage of connected entrants.
# 3. Conducts a DiD analysis on effects of the same reform on number of new recruits (entrants).
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
              'R.utils',
              'fixest',
              'stargazer',
              'ggplot2',
              'egg',
              'did')

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
################## FIRST A SIMPLE DID.
# Our treatment is federal employer, control is state employer.
# Our pre-period is pre 1989 and post is after that.

# First, we need to match our establishments with natjuridica values that help us classify
# that establishment as federal or state.
# So we have to go back to the raw data, extract the establishments and natjuridica and
# make a crosswalk. Then match it with final_data_connections.
# crosswalk_fn <- function(year) {
#   
#   message(glue('Loading in year {year}'))
#   
#   numcols <- data.table::fread(glue('{harddrive}/intermediate/datasets_with_new_vars/allstates_blind_{year}_newvars.csv'),
#                                nrows = 0) %>% ncol()
#   
#   if(year == 1995) {
#     
#     df <- data.table::fread(glue({harddrive}/intermediate/datasets_with_new_vars/allstates_blind_{year}_newvars.csv'),
#                             select = c('establishment', 'employer_name', 'naturezajuridica'),
#                             colClasses = c(rep('character', numcols)))
#     
#     federal_codes <- c('1015', '1040', '1074', '1104', '1139', '1163', '1252')
#     state_codes <- c('1023', '1058', '1082', '1112', '1147', '1171', '1260')
#     municipal_codes <- c('1031', '1066', '1120', '1155', '1180', '1244', '1279')
#     
#     df[, gov_type := fcase(naturezajuridica %in% federal_codes, 'federal',
#                            naturezajuridica %in% state_codes, 'state',
#                            naturezajuridica %in% municipal_codes, 'municipal')]
#     
#   } else {
#     
#     df <- data.table::fread(glue('{harddrive}/intermediate/datasets_with_new_vars/allstates_blind_{year}_newvars.csv'),
#                             select = c('establishment', 'employer_name', 'ibgesubatividade'),
#                             colClasses = c(rep('character', numcols)))
#     
#     df[, gov_type := fcase(ibgesubatividade == '7013', 'federal',
#                            ibgesubatividade == '7014', 'state',
#                            ibgesubatividade == '7015', 'municipal')]
#     
#     df[ibgesubatividade == '6310', gov_type := fcase(stringr::str_detect(employer_name, pattern = 'federal'), 'federal',
#                                                      stringr::str_detect(employer_name, pattern = 'estadual|universidade|superior'), 'state',
#                                                      default = 'municipal')]
#     
#   }
#   
#   df <- df[, .(establishment, gov_type)] %>% unique()
#   
#   return(df)
#   
# }
# 
# # Now apply this.
# estab_govtype_crosswalk <- lapply(1985:1995, FUN = crosswalk_fn)
# estab_govtype_crosswalk <- data.table::rbindlist(estab_natjuridica_crosswalk) %>% 
#   unique(by = 'establishment')

# Save once and comment out.
# data.table::fwrite(estab_govtype_crosswalk, glue('{harddrive}/intermediate/crosswalks/estab_govtype_crosswalk.csv'))
###############################################################################################






###############################################################################################
################## FIRST, CONNECTED, PUBLIC CONNECTED, PRIVATE CONNECTED FOR ENTIRE WORK HISTORY,
################## BUT SEPARATE FOR ALL ESTABS, <= 1000 AND <= 500 EMPLOYEE ESTABS.

################## TABLES FIRST
# Load in our main dataset.
final_data_connections <- fread(glue('{harddrive}/intermediate/connections/final_data_connections_concursado_entrants.csv'),
                                colClasses = c('character', 'numeric', 'character', rep('numeric', 8), 'character', 'character', 'character', 'numeric', 'character', 'numeric', 'character'))

# Add additional indicators on whether,
## entrant is connected to atleast one coworker in a previous workplace in the public sector.
## entrant is connected ....... private sector.
final_data_connections[, `:=`(has_connections_public = as.numeric(n_connections_public > 0),
                              has_connections_private = as.numeric(n_connections_private > 0))]

# For our DiD analysis, the unit will be a cohort-establishment, not a cohort-job (i.e. cohort-(establishment x pis)).
# So collapse the pis codes in each cohort-establishment.
analysis_data <- final_data_connections[, .(perc_connected = (sum(has_connections)/.N),
                                            perc_connected_public = (sum(has_connections_public)/.N),
                                            perc_connected_private = (sum(has_connections_private)/.N)), by = .(establishment, cohort)]

# Load in the crosswalk just created.
estab_govtype_crosswalk <- fread(glue('{harddrive}/intermediate/crosswalks/estab_govtype_crosswalk.csv'), colClasses = c('character', 'character'))

# Add government type of establishment.
analysis_data <- estab_govtype_crosswalk[analysis_data, on = .(establishment)]

# Now add establishment size using the crosswalk made earlier.
estab_size_crosswalk <- data.table::fread(glue('{harddrive}/intermediate/crosswalks/establishment_size_crosswalk.csv'),
                                          colClasses = c('character', rep('numeric', 11)))

analysis_data <- estab_size_crosswalk[analysis_data, on = .(establishment, year = cohort)]

# Rename year as cohort for easier interpretation.
analysis_data <- dplyr::rename(analysis_data, 'cohort' = 'year')

# Convert establishment and cohort variables to factors.
analysis_data[, `:=`(cohort = as.factor(cohort),
                     establishment = as.factor(establishment))]

# Create a total employees variable and filter to keep only those establishments with count <= 1000.
analysis_data[, total_employees := total_concursado_employees + total_nonconcursado_employees]
analysis_data_1000 <- analysis_data[total_employees <= 1000,]

# Now keep only <= 500 for robustness.
analysis_data_500 <- analysis_data[total_employees <= 500,]

# For now, municipal will be NA.
analysis_data[, `:=`(post_reform = as.numeric(as.numeric(as.character(cohort)) >= 1989), federal_employer = fcase(gov_type == 'federal', 1,
                                                                                                    gov_type == 'state', 0))]
analysis_data_500[, `:=`(post_reform = as.numeric(as.numeric(as.character(cohort)) >= 1989), federal_employer = fcase(gov_type == 'federal', 1,
                                                                                                    gov_type == 'state', 0))]
analysis_data_1000[, `:=`(post_reform = as.numeric(as.numeric(as.character(cohort)) >= 1989), federal_employer = fcase(gov_type == 'federal', 1,
                                                                                                        gov_type == 'state', 0))]

analysis_data[, post_reform_federal := post_reform * federal_employer]
analysis_data_500[, post_reform_federal := post_reform * federal_employer]
analysis_data_1000[, post_reform_federal := post_reform * federal_employer]

analysis_data$federal_employer <- as.factor(analysis_data$federal_employer)
analysis_data_500$federal_employer <- as.factor(analysis_data_500$federal_employer)
analysis_data_1000$federal_employer <- as.factor(analysis_data_1000$federal_employer)

# Basic idea is to compare cohort-estabs before reform (1989) with those after.
model_did_1.1.all <- fixest::feols(perc_connected ~ post_reform_federal | cohort + establishment, data = analysis_data, vcov = ~establishment)
model_did_1.1.1000 <- fixest::feols(perc_connected ~ post_reform_federal | cohort + establishment, data = analysis_data_1000, vcov = ~establishment)
model_did_1.1.500 <- fixest::feols(perc_connected ~ post_reform_federal | cohort + establishment, data = analysis_data_500, vcov = ~establishment)

model_did_1.2.all <- fixest::feols(perc_connected_public ~ post_reform_federal | cohort + establishment, data = analysis_data, vcov = ~establishment)
model_did_1.2.1000 <- fixest::feols(perc_connected_public ~ post_reform_federal | cohort + establishment, data = analysis_data_1000, vcov = ~establishment)
model_did_1.2.500 <- fixest::feols(perc_connected_public ~ post_reform_federal | cohort + establishment, data = analysis_data_500, vcov = ~establishment)

model_did_1.3.all <- fixest::feols(perc_connected_private ~ post_reform_federal | cohort + establishment, data = analysis_data, vcov = ~establishment)
model_did_1.3.1000 <- fixest::feols(perc_connected_private ~ post_reform_federal | cohort + establishment, data = analysis_data_1000, vcov = ~establishment)
model_did_1.3.500 <- fixest::feols(perc_connected_private ~ post_reform_federal | cohort + establishment, data = analysis_data_500, vcov = ~establishment)

file.remove(glue('{harddrive}/intermediate/outputs/tabs/model_did_1.all.tex'))
etable(model_did_1.1.all, model_did_1.2.all, model_did_1.3.all,
       digits = 'r3',
       dict = c(cohort = 'Cohort', 
                establishment = 'Establishment', 
                perc_connected = '$\\%$ Connected', 
                perc_connected_public = '$\\%$ Connected (Public Sector)',
                perc_connected_private = '$\\%$ Connected (Private Sector)',
                post_reform_federal = 'Post-Reform $\\times$ Federal Employer'),
       title = 'DiD analysis of reform on percentage of connected concursado entrants (All establishments)',
       notes = 'Notes : $\\%$ Connected (Public Sector) and $\\%$ Connected (Private Sector) refer to connections where the entrant and coworker worked at the same public or private sector establishment previously.',
       tex = TRUE, file = glue('{harddrive}/intermediate/outputs/tabs/model_did_1.all.tex'))

file.remove(glue('{harddrive}/intermediate/outputs/tabs/model_did_1.1000.tex'))
etable(model_did_1.1.1000, model_did_1.2.1000, model_did_1.3.1000,
       digits = 'r3',
       dict = c(cohort = 'Cohort', 
                establishment = 'Establishment', 
                perc_connected = '$\\%$ Connected', 
                perc_connected_public = '$\\%$ Connected (Public Sector)',
                perc_connected_private = '$\\%$ Connected (Private Sector)',
                post_reform_federal = 'Post-Reform $\\times$ Federal Employer'),
       title = 'DiD analysis of reform on percentage of connected concursado entrants (<= 1000 employees)',
       tex = TRUE, file = glue('{harddrive}/intermediate/outputs/tabs/model_did_1.1000.tex'))

file.remove(glue('{harddrive}/intermediate/outputs/tabs/model_did_1.500.tex'))
etable(model_did_1.1.500, model_did_1.2.500, model_did_1.3.500,
       digits = 'r3',
       dict = c(cohort = 'Cohort', 
                establishment = 'Establishment', 
                perc_connected = '$\\%$ Connected', 
                perc_connected_public = '$\\%$ Connected (Public Sector)',
                perc_connected_private = '$\\%$ Connected (Private Sector)',
                post_reform_federal = 'Post-Reform $\\times$ Federal Employer'),
       title = 'DiD analysis of reform on percentage of connected concursado entrants (<= 500 employees)',
       tex = TRUE, file = glue('{harddrive}/intermediate/outputs/tabs/model_did_1.500.tex'))
##################

################## NOW GRAPHS
# Add a dynamic graph to investigate pre-trends.
dynamic_graphs_generator_fn <- function(outcome_param, limit) {
  
  if(limit == 'all') {
    
    did_graph_data <- analysis_data[, .(average_outcome_variable = mean(get(outcome_param))), by = .(federal_employer, cohort)][-(1:3),]
    analysis_data_adjusted <- analysis_data[, .(first_treated = (as.numeric(as.character(federal_employer))*1989),
                                                cohort = as.numeric(as.character(cohort)),
                                                establishment,
                                                perc_connected,
                                                perc_connected_public,
                                                perc_connected_private)][, id := .GRP, by = .(establishment)]
    
  } else if(limit == '1000') {
    
    did_graph_data <- analysis_data_1000[, .(average_outcome_variable = mean(get(outcome_param))), by = .(federal_employer, cohort)][-(1:3),]
    analysis_data_adjusted <- analysis_data_1000[, .(first_treated = (as.numeric(as.character(federal_employer))*1989),
                                                     cohort = as.numeric(as.character(cohort)),
                                                     establishment,
                                                     perc_connected,
                                                     perc_connected_public,
                                                     perc_connected_private)][, id := .GRP, by = .(establishment)]
    
  } else { 
    
    did_graph_data <- analysis_data_500[, .(average_outcome_variable = mean(get(outcome_param))), by = .(federal_employer, cohort)][-(1:3),] 
    analysis_data_adjusted <- analysis_data_500[, .(first_treated = (as.numeric(as.character(federal_employer))*1989),
                                                    cohort = as.numeric(as.character(cohort)),
                                                    establishment,
                                                    perc_connected,
                                                    perc_connected_public,
                                                    perc_connected_private)][, id := .GRP, by = .(establishment)]
    
  }
  
  outcome_label <- fcase(outcome_param == 'perc_connected', 'Percentage Connected',
                         outcome_param == 'perc_connected_public', 'Percentage Connected (Public Sector)',
                         outcome_param == 'perc_connected_private', 'Percentage Connected (Private Sector)')
  
  did_graph_data <- did_graph_data[!is.na(federal_employer),]
  
  did_dynamic_graph <- ggplot(did_graph_data, aes(cohort, average_outcome_variable, colour = federal_employer, group = federal_employer)) +
    geom_point() +
    geom_line() +
    geom_vline(xintercept = '1989', linetype = 'dashed') +
    labs(x = 'Cohort Year', y = glue('{outcome_label}'), colour = 'Federal Employer', title = 'Dynamic Diff-in-Diff') + 
    scale_color_grey(start = 0.8,
                     end = 0.2) +
    theme_bw() +
    theme(plot.title = element_text(hjust = 0.5))
  
  did_dynamic_graph2 <- did::att_gt(yname = glue('{outcome_param}'),
                                    tname = 'cohort',
                                    idname = 'id',
                                    gname = 'first_treated',
                                    data = analysis_data_adjusted,
                                    bstrap = FALSE,
                                    cband = FALSE)
  
  did_dynamic_graph2 <- did::ggdid(did_dynamic_graph2)
  
  combined_did_graph <- ggpubr::ggarrange(did_dynamic_graph, did_dynamic_graph2, ncol = 1)
  
  ggsave(glue('{harddrive}/intermediate/outputs/figs/did_dynamic_graphs_1_{outcome_param}_{limit}.pdf'), combined_did_graph, width = 12, height = 8)
  
}

dynamic_graphs_generator_fn('perc_connected', 'all')
dynamic_graphs_generator_fn('perc_connected_public', 'all')
dynamic_graphs_generator_fn('perc_connected_private', 'all')

dynamic_graphs_generator_fn('perc_connected', '1000')
dynamic_graphs_generator_fn('perc_connected_public', '1000')
dynamic_graphs_generator_fn('perc_connected_private', '1000')

dynamic_graphs_generator_fn('perc_connected', '500')
dynamic_graphs_generator_fn('perc_connected_public', '500')
dynamic_graphs_generator_fn('perc_connected_private', '500')
##################
###############################################################################################






###############################################################################################
################## NOW, CONNECTED, PUBLIC CONNECTED, PRIVATE CONNECTED FOR ONE-YEAR WORK HISTORY,
################## BUT SEPARATE FOR ALL ESTABS, <= 1000 AND <= 500 EMPLOYEE ESTABS.

################## TABLES FIRST
# Load in our main dataset.
final_data_connections <- fread(glue('{harddrive}/intermediate/connections/final_data_connections_one_year_concursado_entrants.csv'),
                                colClasses = c('character', 'numeric', 'character', rep('numeric', 8), 'character', 'character', 'character', 'numeric', 'character', 'numeric', 'character'))

# Add additional indicators on whether,
## entrant is connected to atleast one coworker in a previous workplace in the public sector.
## entrant is connected ....... private sector.
final_data_connections[, `:=`(has_connections_public = as.numeric(n_connections_public > 0),
                              has_connections_private = as.numeric(n_connections_private > 0))]

# For our DiD analysis, the unit will be a cohort-establishment, not a cohort-job (i.e. cohort-(establishment x pis)).
# So collapse the pis codes in each cohort-establishment.
analysis_data <- final_data_connections[, .(perc_connected = (sum(has_connections)/.N),
                                            perc_connected_public = (sum(has_connections_public)/.N),
                                            perc_connected_private = (sum(has_connections_private)/.N)), by = .(establishment, cohort)]


# Load in the crosswalk just created.
estab_govtype_crosswalk <- fread(glue('{harddrive}/intermediate/crosswalks/estab_govtype_crosswalk.csv'), colClasses = c('character', 'character'))

# Add government type of establishment.
analysis_data <- estab_govtype_crosswalk[analysis_data, on = .(establishment)]

# Now add establishment size using the crosswalk made earlier.
estab_size_crosswalk <- data.table::fread(glue('{harddrive}/intermediate/crosswalks/establishment_size_crosswalk.csv'),
                                          colClasses = c('character', rep('numeric', 11)))

analysis_data <- estab_size_crosswalk[analysis_data, on = .(establishment, year = cohort)]

# Rename year as cohort for easier interpretation.
analysis_data <- dplyr::rename(analysis_data, 'cohort' = 'year')

# Convert establishment and cohort variables to factors.
analysis_data[, `:=`(cohort = as.factor(cohort),
                     establishment = as.factor(establishment))]

# Create a total employees variable and filter to keep only those establishments with count <= 1000.
analysis_data[, total_employees := total_concursado_employees + total_nonconcursado_employees]
analysis_data_1000 <- analysis_data[total_employees <= 1000,]

# Now keep only <= 500 for robustness.
analysis_data_500 <- analysis_data[total_employees <= 500,]

# For now, municipal will be NA.
analysis_data[, `:=`(post_reform = as.numeric(as.numeric(as.character(cohort)) >= 1989), federal_employer = fcase(gov_type == 'federal', 1,
                                                                                                    gov_type == 'state', 0))]
analysis_data_500[, `:=`(post_reform = as.numeric(as.numeric(as.character(cohort)) >= 1989), federal_employer = fcase(gov_type == 'federal', 1,
                                                                                                        gov_type == 'state', 0))]
analysis_data_1000[, `:=`(post_reform = as.numeric(as.numeric(as.character(cohort)) >= 1989), federal_employer = fcase(gov_type == 'federal', 1,
                                                                                                         gov_type == 'state', 0))]

analysis_data[, post_reform_federal := post_reform * federal_employer]
analysis_data_500[, post_reform_federal := post_reform * federal_employer]
analysis_data_1000[, post_reform_federal := post_reform * federal_employer]

analysis_data$federal_employer <- as.factor(analysis_data$federal_employer)
analysis_data_500$federal_employer <- as.factor(analysis_data_500$federal_employer)
analysis_data_1000$federal_employer <- as.factor(analysis_data_1000$federal_employer)

# Basic idea is to compare cohort-estabs before reform (1989) with those after.
model_did_2.1.all <- fixest::feols(perc_connected ~ post_reform_federal | cohort + establishment, data = analysis_data, vcov = ~establishment)
model_did_2.1.1000 <- fixest::feols(perc_connected ~ post_reform_federal | cohort + establishment, data = analysis_data_1000, vcov = ~establishment)
model_did_2.1.500 <- fixest::feols(perc_connected ~ post_reform_federal | cohort + establishment, data = analysis_data_500, vcov = ~establishment)

model_did_2.2.all <- fixest::feols(perc_connected_public ~ post_reform_federal | cohort + establishment, data = analysis_data, vcov = ~establishment)
model_did_2.2.1000 <- fixest::feols(perc_connected_public ~ post_reform_federal | cohort + establishment, data = analysis_data_1000, vcov = ~establishment)
model_did_2.2.500 <- fixest::feols(perc_connected_public ~ post_reform_federal | cohort + establishment, data = analysis_data_500, vcov = ~establishment)

model_did_2.3.all <- fixest::feols(perc_connected_private ~ post_reform_federal | cohort + establishment, data = analysis_data, vcov = ~establishment)
model_did_2.3.1000 <- fixest::feols(perc_connected_private ~ post_reform_federal | cohort + establishment, data = analysis_data_1000, vcov = ~establishment)
model_did_2.3.500 <- fixest::feols(perc_connected_private ~ post_reform_federal | cohort + establishment, data = analysis_data_500, vcov = ~establishment)

file.remove(glue('{harddrive}/intermediate/outputs/tabs/model_did_2.all.tex'))
etable(model_did_2.1.all, model_did_2.2.all, model_did_2.3.all,
       digits = 'r3',
       dict = c(cohort = 'Cohort', 
                establishment = 'Establishment', 
                perc_connected = '$\\%$ Connected', 
                perc_connected_public = '$\\%$ Connected (Public Sector)',
                perc_connected_private = '$\\%$ Connected (Private Sector)',
                post_reform_federal = 'Post-Reform $\\times$ Federal Employer'),
       title = 'DiD analysis of reform on percentage of connected concursado entrants (All establishments, only one-year old workplace history)',
       tex = TRUE, file = glue('{harddrive}/intermediate/outputs/tabs/model_did_2.all.tex'))

file.remove(glue('{harddrive}/intermediate/outputs/tabs/model_did_2.1000.tex'))
etable(model_did_2.1.1000, model_did_2.2.1000, model_did_2.3.1000,
       digits = 'r3',
       dict = c(cohort = 'Cohort', 
                establishment = 'Establishment', 
                perc_connected = '$\\%$ Connected', 
                perc_connected_public = '$\\%$ Connected (Public Sector)',
                perc_connected_private = '$\\%$ Connected (Private Sector)',
                post_reform_federal = 'Post-Reform $\\times$ Federal Employer'),
       title = 'DiD analysis of reform on percentage of connected concursado entrants (<= 1000 employees, only one-year old workplace history)',
       tex = TRUE, file = glue('{harddrive}/intermediate/outputs/tabs/model_did_2.1000.tex'))

file.remove(glue('{harddrive}/intermediate/outputs/tabs/model_did_2.500.tex'))
etable(model_did_2.1.500, model_did_2.2.500, model_did_2.3.500,
       digits = 'r3',
       dict = c(cohort = 'Cohort', 
                establishment = 'Establishment', 
                perc_connected = '$\\%$ Connected', 
                perc_connected_public = '$\\%$ Connected (Public Sector)',
                perc_connected_private = '$\\%$ Connected (Private Sector)',
                post_reform_federal = 'Post-Reform $\\times$ Federal Employer'),
       title = 'DiD analysis of reform on percentage of connected concursado entrants (<= 500 employees, only one-year old workplace history)',
       tex = TRUE, file = glue('{harddrive}/intermediate/outputs/tabs/model_did_2.500.tex'))
##################

################## NOW GRAPHS
# Add a dynamic graph to investigate pre-trends.
dynamic_graphs_generator_fn <- function(outcome_param, limit) {
  
  if(limit == 'all') {
    
    did_graph_data <- analysis_data[, .(average_outcome_variable = mean(get(outcome_param))), by = .(federal_employer, cohort)][-(1:3),]
    analysis_data_adjusted <- analysis_data[, .(first_treated = (as.numeric(as.character(federal_employer))*1989),
                                                cohort = as.numeric(as.character(cohort)),
                                                establishment,
                                                perc_connected,
                                                perc_connected_public,
                                                perc_connected_private)][, id := .GRP, by = .(establishment)]
    
  } else if(limit == '1000') {
    
    did_graph_data <- analysis_data_1000[, .(average_outcome_variable = mean(get(outcome_param))), by = .(federal_employer, cohort)][-(1:3),]
    analysis_data_adjusted <- analysis_data_1000[, .(first_treated = (as.numeric(as.character(federal_employer))*1989),
                                                     cohort = as.numeric(as.character(cohort)),
                                                     establishment,
                                                     perc_connected,
                                                     perc_connected_public,
                                                     perc_connected_private)][, id := .GRP, by = .(establishment)]
    
  } else { 
    
    did_graph_data <- analysis_data_500[, .(average_outcome_variable = mean(get(outcome_param))), by = .(federal_employer, cohort)][-(1:3),] 
    analysis_data_adjusted <- analysis_data_500[, .(first_treated = (as.numeric(as.character(federal_employer))*1989),
                                                    cohort = as.numeric(as.character(cohort)),
                                                    establishment,
                                                    perc_connected,
                                                    perc_connected_public,
                                                    perc_connected_private)][, id := .GRP, by = .(establishment)]
    
  }
  
  outcome_label <- fcase(outcome_param == 'perc_connected', 'Percentage Connected',
                         outcome_param == 'perc_connected_public', 'Percentage Connected (Public Sector)',
                         outcome_param == 'perc_connected_private', 'Percentage Connected (Private Sector)')
  
  did_graph_data <- did_graph_data[!is.na(federal_employer),]
  
  did_dynamic_graph <- ggplot(did_graph_data, aes(cohort, average_outcome_variable, colour = federal_employer, group = federal_employer)) +
    geom_point() +
    geom_line() +
    geom_vline(xintercept = '1989', linetype = 'dashed') +
    labs(x = 'Cohort Year', y = glue('{outcome_label}'), colour = 'Federal Employer', title = 'Dynamic Diff-in-Diff') + 
    scale_color_grey(start = 0.8,
                     end = 0.2) +
    theme_bw() +
    theme(plot.title = element_text(hjust = 0.5))
  
  did_dynamic_graph2 <- did::att_gt(yname = glue('{outcome_param}'),
                                    tname = 'cohort',
                                    idname = 'id',
                                    gname = 'first_treated',
                                    data = analysis_data_adjusted,
                                    bstrap = FALSE,
                                    cband = FALSE)
  
  did_dynamic_graph2 <- did::ggdid(did_dynamic_graph2)
  
  combined_did_graph <- ggpubr::ggarrange(did_dynamic_graph, did_dynamic_graph2, ncol = 1)
  
  ggsave(glue('{harddrive}/intermediate/outputs/figs/did_dynamic_graphs_2_{outcome_param}_{limit}.pdf'), combined_did_graph, width = 12, height = 8)
  
}

dynamic_graphs_generator_fn('perc_connected', 'all')
dynamic_graphs_generator_fn('perc_connected_public', 'all')
dynamic_graphs_generator_fn('perc_connected_private', 'all')

dynamic_graphs_generator_fn('perc_connected', '1000')
dynamic_graphs_generator_fn('perc_connected_public', '1000')
dynamic_graphs_generator_fn('perc_connected_private', '1000')

dynamic_graphs_generator_fn('perc_connected', '500')
dynamic_graphs_generator_fn('perc_connected_public', '500')
dynamic_graphs_generator_fn('perc_connected_private', '500')
##################
###############################################################################################






###############################################################################################
################## ADDITIONAL ANALYSIS ON NUMBER OF NEW RECRUITS (ENTRANTS).

# 1. EFFECT OF THE POLICY ON NEW CONCURSADO & NON-CONCURSADO HIRES (DiD)
# Aggregate the job-level wise new entrants numbers to total entrants.
# 1. EFFECT OF THE POLICY ON NEW CONCURSADO & NON-CONCURSADO HIRES (DiD)
analysis_data <- data.table::fread(glue('{harddrive}/intermediate/crosswalks/establishment_size_crosswalk.csv'),
                                   colClasses = c('character', rep('numeric', 11)))

# Load in the crosswalk for establishment and government type.
estab_govtype_crosswalk <- fread(glue('{harddrive}/intermediate/crosswalks/estab_govtype_crosswalk.csv'), colClasses = c('character', 'character'))

analysis_data <- estab_govtype_crosswalk[analysis_data, on = .(establishment)]

rm(estab_govtype_crosswalk)

# Rename year as cohort for easier interpretation.
analysis_data <- dplyr::rename(analysis_data, 'cohort' = 'year')

# Convert establishment and cohort variables to factors.
analysis_data[, `:=`(cohort = as.factor(cohort),
                     establishment = as.factor(establishment))]

# For now, municipal will be NA.
analysis_data[, `:=`(post_reform = as.numeric(as.numeric(as.character(cohort)) >= 1989), federal_employer = fcase(gov_type == 'federal', 1,
                                                                                      gov_type == 'state', 0))]
analysis_data[, post_reform_federal := post_reform * federal_employer]

# Create a total employees variable and filter to keep only those establishments with count <= 1000.
analysis_data[, total_employees := total_concursado_employees + total_nonconcursado_employees]

analysis_data[, post_reform_federal := post_reform * federal_employer]

analysis_data[, `:=`(total_concursado_entrants = (concursado_entrants_bluecollar +
                                                    concursado_entrants_whitecollar +
                                                    concursado_entrants_managerial +
                                                    concursado_entrants_unclassified),
                     total_nonconcursado_entrants = (nonconcursado_entrants_bluecollar +
                                                       nonconcursado_entrants_whitecollar +
                                                       nonconcursado_entrants_managerial +
                                                       nonconcursado_entrants_unclassified))
]

analysis_data$federal_employer <- as.factor(analysis_data$federal_employer)

# Now estimate the model.
model_entrants_did_concursado <- fixest::feols(total_concursado_entrants ~ post_reform_federal | cohort + establishment, data = analysis_data, vcov = ~establishment)
model_entrants_did_nonconcursado <- fixest::feols(total_nonconcursado_entrants ~ post_reform_federal | cohort + establishment, data = analysis_data, vcov = ~establishment)

# Save tables.
file.remove(glue('{harddrive}/intermediate/outputs/tabs/model_entrants_did.tex'))
etable(model_entrants_did_concursado, model_entrants_did_nonconcursado,
       dict = c(cohort = 'Cohort', 
                establishment = 'Establishment', 
                total_concursado_entrants = 'New Entrants (Concursado)', 
                total_nonconcursado_entrants = 'New Entrants (Non-Concursado)', 
                post_reform_federal = 'Post-Reform $\\times$ Federal Employer'),
       title = 'DiD Analysis of reform on new recruitments across entry types',
       tex = TRUE, file = glue('{harddrive}/intermediate/outputs/tabs/model_entrants_did.tex'))

# Add a dynamic graph to investigate pre-trends.
did_graph_data_concursado <- analysis_data[, .(avg_total_concursado_entrants = mean(total_concursado_entrants)), by = .(federal_employer, cohort)]
did_graph_data_nonconcursado <- analysis_data[, .(avg_total_nonconcursado_entrants = mean(total_nonconcursado_entrants)), by = .(federal_employer, cohort)]

data.table::setorder(did_graph_data_concursado, cohort)
data.table::setorder(did_graph_data_nonconcursado, cohort)

did_graph_data_concursado <- did_graph_data_concursado[-(1:3),]
did_graph_data_nonconcursado <- did_graph_data_nonconcursado[-(1:3),]

did_graph_data_concursado <- did_graph_data_concursado[!is.na(federal_employer),]
did_graph_data_nonconcursado <- did_graph_data_nonconcursado[!is.na(federal_employer),]

# Graph first the concursado data.
did_dynamic_graph_concursado <- ggplot(did_graph_data_concursado, aes(cohort, avg_total_concursado_entrants, colour = federal_employer, group = federal_employer)) +
  geom_point() +
  geom_line() +
  geom_vline(xintercept = '1989', linetype = 'dashed') +
  labs(x = 'Cohort Year', y = 'Average Number of Entrants (Concursado)', colour = 'Federal Employer', title = 'Dynamic Diff-in-Diff') + 
  scale_color_grey(start = 0.8,
                   end = 0.2) +
  theme_bw() +
  theme(plot.title = element_text(hjust = 0.5))

# Now graph the non-concursado data.
did_dynamic_graph_nonconcursado <- ggplot(did_graph_data_nonconcursado, aes(cohort, avg_total_nonconcursado_entrants, colour = federal_employer, group = federal_employer)) +
  geom_point() +
  geom_line() +
  geom_vline(xintercept = '1989', linetype = 'dashed') +
  labs(x = 'Cohort Year', y = 'Average Number of Entrants (Non-Concursado)', colour = 'Federal Employer', title = 'Dynamic Diff-in-Diff') + 
  scale_color_grey(start = 0.8,
                   end = 0.2) +
  theme_bw() +
  theme(plot.title = element_text(hjust = 0.5))

did_dynamic_graph_concursado
did_dynamic_graph_nonconcursado

analysis_data_adjusted <- analysis_data[, .(first_treated = (as.numeric(as.character(federal_employer))*1989),
                                            cohort = as.numeric(as.character(cohort)),
                                            establishment,
                                            total_concursado_entrants,
                                            total_nonconcursado_entrants)][, id := .GRP, by = .(establishment)]

# Graph first the concursado data.
did_dynamic_graph2_concursado <- did::att_gt(yname = 'total_concursado_entrants',
                                             tname = 'cohort',
                                             idname = 'id',
                                             gname = 'first_treated',
                                             data = analysis_data_adjusted,
                                             bstrap = FALSE,
                                             cband = FALSE)

# Now graph the non-concursado data.
did_dynamic_graph2_nonconcursado <- did::att_gt(yname = 'total_nonconcursado_entrants',
                                                tname = 'cohort',
                                                idname = 'id',
                                                gname = 'first_treated',
                                                data = analysis_data_adjusted,
                                                bstrap = FALSE,
                                                cband = FALSE)

did_dynamic_graph2_concursado <- did::ggdid(did_dynamic_graph2_concursado)
did_dynamic_graph2_nonconcursado <- did::ggdid(did_dynamic_graph2_nonconcursado)

did_dynamic_graph2_concursado
did_dynamic_graph2_nonconcursado

# Combine and save the graphs.
combined_did_graph_concursado <- ggpubr::ggarrange(did_dynamic_graph_concursado,
                                                   did_dynamic_graph2_concursado, ncol = 1)
ggsave(glue('{harddrive}/intermediate/outputs/figs/did_entrants_dynamic_graphs_concursado.pdf'), combined_did_graph_concursado, width = 12, height = 8)

combined_did_graph_nonconcursado <- ggpubr::ggarrange(did_dynamic_graph_nonconcursado,
                                                      did_dynamic_graph2_nonconcursado, ncol = 1)
ggsave(glue('{harddrive}/intermediate/outputs/figs/did_entrants_dynamic_graphs_nonconcursado.pdf'), combined_did_graph_nonconcursado, width = 12, height = 8)
###############################################################################################

# 2. RESULTS SEPARATED BY JOB-LEVELS.
# First for concursado.
model_entrants_did_concursado_bluecollar <- fixest::feols(concursado_entrants_bluecollar ~ post_reform_federal | cohort + establishment, data = analysis_data, vcov = ~establishment)
model_entrants_did_concursado_whitecollar <- fixest::feols(concursado_entrants_whitecollar ~ post_reform_federal | cohort + establishment, data = analysis_data, vcov = ~establishment)
model_entrants_did_concursado_managerial <- fixest::feols(concursado_entrants_managerial ~ post_reform_federal | cohort + establishment, data = analysis_data, vcov = ~establishment)

# Now for non-concursado.
model_entrants_did_nonconcursado_bluecollar <- fixest::feols(nonconcursado_entrants_bluecollar ~ post_reform_federal | cohort + establishment, data = analysis_data, vcov = ~establishment)
model_entrants_did_nonconcursado_whitecollar <- fixest::feols(nonconcursado_entrants_whitecollar ~ post_reform_federal | cohort + establishment, data = analysis_data, vcov = ~establishment)
model_entrants_did_nonconcursado_managerial <- fixest::feols(nonconcursado_entrants_managerial ~ post_reform_federal | cohort + establishment, data = analysis_data, vcov = ~establishment)

# Save tables.
file.remove(glue('{harddrive}/intermediate/outputs/tabs/model_entrants_did_joblevel.tex'))
etable(model_entrants_did_concursado_bluecollar, 
       model_entrants_did_concursado_whitecollar,
       model_entrants_did_concursado_managerial,
       model_entrants_did_nonconcursado_bluecollar,
       model_entrants_did_nonconcursado_whitecollar,
       model_entrants_did_nonconcursado_managerial,
       digits = 'r2',
       headers = list(':_:Entry Type' = list('Concursado' = 3, 'Non-Concursado' = 3),
                      ':_:Job Level' = list('Blue-collar' = 1, 'White-collar' = 1, 'Managerial' = 1,
                                            'Blue-collar' = 1, 'White-collar' = 1, 'Managerial' = 1)),
       depvar = F,
       dict = c(cohort = 'Cohort', 
                establishment = 'Establishment', 
                concursado_entrants_bluecollar = 'Blue-collar New Entrants (Concursado)', 
                concursado_entrants_whitecollar = 'White-collar New Entrants (Concursado)',
                concursado_entrants_managerial = 'Managerial New Entrants (Concursado)',
                nonconcursado_entrants_bluecollar = 'Blue-collar New Entrants (Non-Concursado)', 
                nonconcursado_entrants_whitecollar = 'White-collar New Entrants (Non-Concursado)',
                nonconcursado_entrants_managerial = 'Managerial New Entrants (Non-Concursado)',
                post_reform_federal = 'Post-Reform $\\times$ Federal Employer'),
       title = 'DiD Analysis of reform on new recruitments across job-levels and entry types',
       tex = TRUE, file = glue('{harddrive}/intermediate/outputs/tabs/model_entrants_did_joblevel.tex'))
################## 
###############################################################################################

