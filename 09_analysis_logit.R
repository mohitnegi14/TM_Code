###############################################################################################
################## INTRO
# MOHIT NEGI
# Last Updated : 13 November 2023
# Contact on : mohit.negi@studbocconi.it
################## 

################## OBJECTIVES
# This script performs the following tasks.
# 1. Merges data on whether the establishments are federal, state or municipal recruiters.
# 2. Conducts several regressions to find the determinants of the propensity to be connected.
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
              'did',
              'dplyr')

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
################## STEP 1 : PREPARE THE DATA. 

# Load in the crosswalk just created.
estab_govtype_crosswalk <- fread(glue('{harddrive}/intermediate/crosswalks/estab_govtype_crosswalk.csv'), colClasses = c('character', 'character'))

# Load in our main dataset.
final_data_connections <- fread(glue('{harddrive}/intermediate/connections/final_data_connections_concursado_entrants.csv'),
                                colClasses = c('character', 'numeric', 'character', rep('numeric', 8), 'character', 'character', 'character', 'numeric', 'character', 'numeric', 'character'))

final_data_connections <- estab_govtype_crosswalk[final_data_connections, on = .(establishment)]

# Now, make all the factor variables neater.
final_data_connections$education <- factor(final_data_connections$education,
                                           levels = c('no_school', 'less_than_hs', 'hs_incomplete', 'hs', 'college_incomplete', 'college'),
                                           labels = c('no_school', 'less_than_hs', 'hs_incomplete', 'hs', 'college_incomplete', 'college'))

final_data_connections$race <- factor(final_data_connections$race,
                                      levels = c('white', 'brown', 'black', 'asian', 'indigenous'),
                                      labels = c('white', 'brown', 'black', 'asian', 'indigenous'))

final_data_connections$gender <- factor(final_data_connections$gender,
                                        levels = c(0, 1),
                                        labels = c('female', 'male'))

final_data_connections$job_level <- factor(final_data_connections$job_level,
                                           levels = c('blue-collar', 'white-collar', 'managerial'),
                                           labels = c('bluecollar', 'whitecollar', 'managerial'))

final_data_connections[, `:=`(cohort = as.factor(cohort),
                              establishment = as.factor(establishment))]
###############################################################################################






###############################################################################################
################## WE WILL CONDUCT THE FOLLOWING REGRESSIONS.

# 1 : Logit model with no f.e. for all, federal, state and municipal establishments.
# 2 : Logit model with cohort f.e. for all, federal, state and municipal establishments.
# 3 : Logit model with occupation f.e. for all, federal, state and municipal establishments.
# 4 : Logit model with establishment f.e. for all, federal, state and municipal establishments.
# 5 : Logit model with occupation and cohort f.e. for all, federal, state and municipal establishments.
# 6 : Logit model with establishment and cohort f.e. for all, federal, state and municipal establishments.
# 7 : Logit model with establishment-cohort f.e. for all, federal, state and municipal establishments.
# 8 : Year-wise Logit model for all govtypes.
# 9 and 10 : Heterogeneity by skill level : All years and govtypes, different specifications and separated by high (managerial and white-collar) vs. low skill (blue-collar)
# 11 and 12 : Heterogeneity by managerial or not : All years and govtypes, different specifications and separated by managerial vs. non-managerial (white and blue-collar)
###############################################################################################






###############################################################################################
##################  CONDUCTING REGRESSIONS 1-7.
model_logit_1.1 <- fixest::feglm(has_connections ~ education + gender + race + job_level, family = binomial(link = 'logit'), data = final_data_connections, vcov = ~establishment)
model_logit_2.1 <- fixest::feglm(has_connections ~ education + gender + race + job_level | cohort, family = binomial(link = 'logit'), data = final_data_connections, vcov = ~establishment)
model_logit_3.1 <- fixest::feglm(has_connections ~ education + gender + race | job_level, family = binomial(link = 'logit'), data = final_data_connections, vcov = ~establishment)
model_logit_4.1 <- fixest::feglm(has_connections ~ education + gender + race + job_level | establishment, family = binomial(link = 'logit'), data = final_data_connections, vcov = ~establishment)
model_logit_5.1 <- fixest::feglm(has_connections ~ education + gender + race | job_level + cohort, family = binomial(link = 'logit'), data = final_data_connections, vcov = ~establishment)
model_logit_6.1 <- fixest::feglm(has_connections ~ education + gender + race + job_level | establishment + cohort, family = binomial(link = 'logit'), data = final_data_connections, vcov = ~establishment)
model_logit_7.1 <- fixest::feglm(has_connections ~ education + gender + race + job_level | establishment^cohort, family = binomial(link = 'logit'), data = final_data_connections, vcov = ~establishment)

model_logit_1.2 <- fixest::feglm(has_connections ~ education + gender + race + job_level, family = binomial(link = 'logit'), data = final_data_connections[gov_type == 'federal'], vcov = ~establishment)
model_logit_2.2 <- fixest::feglm(has_connections ~ education + gender + race + job_level | cohort, family = binomial(link = 'logit'), data = final_data_connections[gov_type == 'federal'], vcov = ~establishment)
model_logit_3.2 <- fixest::feglm(has_connections ~ education + gender + race | job_level, family = binomial(link = 'logit'), data = final_data_connections[gov_type == 'federal'], vcov = ~establishment)
model_logit_4.2 <- fixest::feglm(has_connections ~ education + gender + race + job_level | establishment, family = binomial(link = 'logit'), data = final_data_connections[gov_type == 'federal'], vcov = ~establishment)
model_logit_5.2 <- fixest::feglm(has_connections ~ education + gender + race | job_level + cohort, family = binomial(link = 'logit'), data = final_data_connections[gov_type == 'federal'], vcov = ~establishment)
model_logit_6.2 <- fixest::feglm(has_connections ~ education + gender + race + job_level | establishment + cohort, family = binomial(link = 'logit'), data = final_data_connections[gov_type == 'federal'], vcov = ~establishment)
model_logit_7.2 <- fixest::feglm(has_connections ~ education + gender + race + job_level | establishment^cohort, family = binomial(link = 'logit'), data = final_data_connections[gov_type == 'federal'], vcov = ~establishment)

model_logit_1.3 <- fixest::feglm(has_connections ~ education + gender + race + job_level, family = binomial(link = 'logit'), data = final_data_connections[gov_type == 'state'], vcov = ~establishment)
model_logit_2.3 <- fixest::feglm(has_connections ~ education + gender + race + job_level | cohort, family = binomial(link = 'logit'), data = final_data_connections[gov_type == 'state'], vcov = ~establishment)
model_logit_3.3 <- fixest::feglm(has_connections ~ education + gender + race | job_level, family = binomial(link = 'logit'), data = final_data_connections[gov_type == 'state'], vcov = ~establishment)
model_logit_4.3 <- fixest::feglm(has_connections ~ education + gender + race + job_level | establishment, family = binomial(link = 'logit'), data = final_data_connections[gov_type == 'state'], vcov = ~establishment)
model_logit_5.3 <- fixest::feglm(has_connections ~ education + gender + race | job_level + cohort, family = binomial(link = 'logit'), data = final_data_connections[gov_type == 'state'], vcov = ~establishment)
model_logit_6.3 <- fixest::feglm(has_connections ~ education + gender + race + job_level | establishment + cohort, family = binomial(link = 'logit'), data = final_data_connections[gov_type == 'state'], vcov = ~establishment)
model_logit_7.3 <- fixest::feglm(has_connections ~ education + gender + race + job_level | establishment^cohort, family = binomial(link = 'logit'), data = final_data_connections[gov_type == 'state'], vcov = ~establishment)

model_logit_1.4 <- fixest::feglm(has_connections ~ education + gender + race + job_level, family = binomial(link = 'logit'), data = final_data_connections[gov_type == 'municipal'], vcov = ~establishment)
model_logit_2.4 <- fixest::feglm(has_connections ~ education + gender + race + job_level | cohort, family = binomial(link = 'logit'), data = final_data_connections[gov_type == 'municipal'], vcov = ~establishment)
model_logit_3.4 <- fixest::feglm(has_connections ~ education + gender + race | job_level, family = binomial(link = 'logit'), data = final_data_connections[gov_type == 'municipal'], vcov = ~establishment)
model_logit_4.4 <- fixest::feglm(has_connections ~ education + gender + race + job_level | establishment, family = binomial(link = 'logit'), data = final_data_connections[gov_type == 'municipal'], vcov = ~establishment)
model_logit_5.4 <- fixest::feglm(has_connections ~ education + gender + race | job_level + cohort, family = binomial(link = 'logit'), data = final_data_connections[gov_type == 'municipal'], vcov = ~establishment)
model_logit_6.4 <- fixest::feglm(has_connections ~ education + gender + race + job_level | establishment + cohort, family = binomial(link = 'logit'), data = final_data_connections[gov_type == 'municipal'], vcov = ~establishment)
model_logit_7.4 <- fixest::feglm(has_connections ~ education + gender + race + job_level | establishment^cohort, family = binomial(link = 'logit'), data = final_data_connections[gov_type == 'municipal'], vcov = ~establishment)
################## 

################## 
file.remove(glue('{harddrive}/intermediate/outputs/tabs/model_logit.tex'))
etable(model_logit_1.1, model_logit_2.1, model_logit_3.1, model_logit_4.1, model_logit_5.1, model_logit_6.1, model_logit_7.1,
       model_logit_1.2, model_logit_2.2, model_logit_3.2, model_logit_4.2, model_logit_5.2, model_logit_6.2, model_logit_7.2,
       model_logit_1.3, model_logit_2.3, model_logit_3.3, model_logit_4.3, model_logit_5.3, model_logit_6.3, model_logit_7.3,
       model_logit_1.4, model_logit_2.4, model_logit_3.4, model_logit_4.4, model_logit_5.4, model_logit_6.4, model_logit_7.4,
       digits = 'r2',
       headers = list(':_:Government Type' = list('All' = 7, 'Federal' = 7, 'State' = 7, 'Municipal' = 7)),
       dict = c(has_connections = 'Has Connections', 
                educationless_than_hs = 'Less than High School',
                educationhs_incomplete = 'High School Incomplete',
                educationhs = 'High School',
                educationcollege_incomplete = 'College Incomplete',
                educationcollege = 'College',
                gendermale = 'Male',
                racebrown = 'Brown',
                raceblack = 'Black',
                raceasian = 'Asian',
                raceindigenous = 'Indigenous',
                job_levelwhitecollar = 'White-Collar',
                job_levelmanagerial = 'Managerial',
                cohort = 'Cohort',
                establishment = 'Establishment',
                job_level = 'Job Level'),
       title = 'Logit Model for Connectedness',
       notes = 'Notes : Coefficients on education variables are relative to no education and those on race variables are relative to white.',
       tex = TRUE, file = glue('{harddrive}/intermediate/outputs/tabs/model_logit.tex'))
################## 
###############################################################################################






###############################################################################################
################## CONDUCTING REGRESSION 8 (YEAR-WISE)
years <- 1986:1995

list_coefs <- list()
for(i in 1:length(years)) {
  
  year <- years[i]
  
  message(glue('Working on year {year}.'))
  
  model_logit_8_yearwise <- fixest::feglm(has_connections ~ education + gender + race + job_level, family = binomial(link = 'logit'), data = final_data_connections[cohort == as.character(year)], vcov = ~establishment)
  
  # First generate the tables.
  file.remove(glue('{harddrive}/intermediate/outputs/tabs/model_logit_{year}.tex'))
  etable(model_logit_8_yearwise,
         digits = 'r2',
         dict = c(has_connections = 'Has Connections', 
                  educationless_than_hs = 'Less than High School',
                  educationhs_incomplete = 'High School Incomplete',
                  educationhs = 'High School',
                  educationcollege_incomplete = 'College Incomplete',
                  educationcollege = 'College',
                  gendermale = 'Male',
                  racebrown = 'Brown',
                  raceblack = 'Black',
                  raceasian = 'Asian',
                  raceindigenous = 'Indigenous',
                  job_levelwhitecollar = 'White-Collar',
                  job_levelmanagerial = 'Managerial',
                  cohort = 'Cohort',
                  establishment = 'Establishment',
                  job_level = 'Job Level'),
         title = glue('Logit Model for Connectedness (Cohort - {year})'),
         notes = 'Notes : Coefficients on education variables are relative to no education and those on race variables are relative to white.',
         tex = TRUE, file = glue('{harddrive}/intermediate/outputs/tabs/model_logit_{year}.tex'))
  
  # Now store coefficients from the all-employer table for a combined graph later.
  coef <- broom::tidy(model_logit_8_yearwise, conf.int = T)
  coef$year <- glue('{year}')
  
  list_coefs[[i]] <- coef
  
}

# Now combine into a single dataframe.
coefs_all_years <- rbindlist(list_coefs)
coefs_all_years$term <- factor(coefs_all_years$term,
                               levels = unique(coefs_all_years$term),
                               labels = c('(Intercept)','Less than High School','High School Incomplete','High School','College Incomplete','College','Male','Brown','Black','Asian','Indigenous','White-Collar','Managerial'))

# Split in 4 graphs for better visibility.
terms <- unique(coefs_all_years$term)
##################  

################## Make a graph to display them together.
# Plot 1 : c1
c1 <- ggplot(subset(coefs_all_years, coefs_all_years$term %in% terms[2:6]), aes(estimate, term, colour = year)) +
  geom_point(position = position_dodge(width = .75)) +
  geom_errorbarh(aes(xmin = conf.low, xmax = conf.high), position = position_dodge(width = .75), height = 0) + 
  geom_vline(xintercept = 0, linetype = 'dashed') +
  labs(x = 'Estimate', y = '', colour = 'Year', title = 'Education') + 
  scale_color_grey(start = 0.8,
                   end = 0.2) +
  scale_x_continuous(limits = c(-2, 2), oob = scales::rescale_none) +
  theme_bw() +
  theme(plot.title = element_text(hjust = 0.5))

# Plot 2 : c2
c2 <- ggplot(subset(coefs_all_years, coefs_all_years$term %in% terms[7]), aes(estimate, term, colour = year)) +
  geom_point(position = position_dodge(width = .75)) +
  geom_errorbarh(aes(xmin = conf.low, xmax = conf.high), position = position_dodge(width = .75), height = 0) + 
  geom_vline(xintercept = 0, linetype = 'dashed') +
  labs(x = '', y = '', title = 'Gender') + 
  scale_color_grey(start = 0.8,
                   end = 0.2, guide = 'none') +
  scale_x_continuous(limits = c(-2, 2), oob = scales::rescale_none) +
  theme_bw() +
  theme(plot.title = element_text(hjust = 0.5))

# Plot 3 : c3
c3 <- ggplot(subset(coefs_all_years, coefs_all_years$term %in% terms[8:11]), aes(estimate, term, colour = year)) +
  geom_point(position = position_dodge(width = .75)) +
  geom_errorbarh(aes(xmin = conf.low, xmax = conf.high), position = position_dodge(width = .75), height = 0) + 
  geom_vline(xintercept = 0, linetype = 'dashed') +
  labs(x = '', y = '', title = 'Race') + 
  scale_color_grey(start = 0.8,
                   end = 0.2, guide = 'none') +
  scale_x_continuous(limits = c(-2, 2), oob = scales::rescale_none) +
  theme_bw() +
  theme(plot.title = element_text(hjust = 0.5))

# Plot 4 : c4
c4 <- ggplot(subset(coefs_all_years, coefs_all_years$term %in% terms[12:13]), aes(estimate, term, colour = year)) +
  geom_point(position = position_dodge(width = .75)) +
  geom_errorbarh(aes(xmin = conf.low, xmax = conf.high), position = position_dodge(width = .75), height = 0) + 
  geom_vline(xintercept = 0, linetype = 'dashed') +
  labs(x = '', y = '') + 
  ggtitle('Job Level') +
  scale_color_grey(start = 0.8,
                   end = 0.2, guide = 'none') +
  scale_x_continuous(limits = c(-2, 2), oob = scales::rescale_none) +
  theme_bw() +
  theme(plot.title = element_text(hjust = 0.5))

# Combined plot
combined_graph <- ggarrange(c1, c2, c3, c4, ncol = 2)
ggsave(glue('{harddrive}/intermediate/outputs/figs/model_logit_year-wise.pdf'), combined_graph, width = 12, height = 8)
##################  
###############################################################################################






###############################################################################################
##################  CONDUCTING REGRESSIONS 9 and 10 - HIGH VS. LOW SKILL.
model_logit_9.1 <- fixest::feglm(has_connections ~ education + gender + race, family = binomial(link = 'logit'), data = final_data_connections[job_level == 'managerial' |  job_level == 'whitecollar',], vcov = ~establishment)
model_logit_9.2 <- fixest::feglm(has_connections ~ education + gender + race | cohort, family = binomial(link = 'logit'), data = final_data_connections[job_level == 'managerial' |  job_level == 'whitecollar',], vcov = ~establishment)
model_logit_9.3 <- fixest::feglm(has_connections ~ education + gender + race | establishment, family = binomial(link = 'logit'), data = final_data_connections[job_level == 'managerial' |  job_level == 'whitecollar',], vcov = ~establishment)
model_logit_9.4 <- fixest::feglm(has_connections ~ education + gender + race | establishment + cohort, family = binomial(link = 'logit'), data = final_data_connections[job_level == 'managerial' |  job_level == 'whitecollar',], vcov = ~establishment)
model_logit_9.5 <- fixest::feglm(has_connections ~ education + gender + race | establishment^cohort, family = binomial(link = 'logit'), data = final_data_connections[job_level == 'managerial' |  job_level == 'whitecollar',], vcov = ~establishment)

model_logit_10.1 <- fixest::feglm(has_connections ~ education + gender + race, family = binomial(link = 'logit'), data = final_data_connections[job_level == 'bluecollar',], vcov = ~establishment)
model_logit_10.2 <- fixest::feglm(has_connections ~ education + gender + race | cohort, family = binomial(link = 'logit'), data = final_data_connections[job_level == 'bluecollar',], vcov = ~establishment)
model_logit_10.3 <- fixest::feglm(has_connections ~ education + gender + race | establishment, family = binomial(link = 'logit'), data = final_data_connections[job_level == 'bluecollar',], vcov = ~establishment)
model_logit_10.4 <- fixest::feglm(has_connections ~ education + gender + race | establishment + cohort, family = binomial(link = 'logit'), data = final_data_connections[job_level == 'bluecollar',], vcov = ~establishment)
model_logit_10.5 <- fixest::feglm(has_connections ~ education + gender + race | establishment^cohort, family = binomial(link = 'logit'), data = final_data_connections[job_level == 'bluecollar',], vcov = ~establishment)
##################  


################## 
file.remove(glue('{harddrive}/intermediate/outputs/tabs/model_logit_skilllevel.tex'))
etable(model_logit_9.1, model_logit_9.2, model_logit_9.3, model_logit_9.4, model_logit_9.5,
       model_logit_10.1, model_logit_10.2, model_logit_10.3, model_logit_10.4, model_logit_10.5,
       digits = 'r2',
       headers=list(':_:Skill Level' = list('High' = 5, 'Low' = 5)),
       dict = c(has_connections = 'Has Connections', 
                educationless_than_hs = 'Less than High School',
                educationhs_incomplete = 'High School Incomplete',
                educationhs = 'High School',
                educationcollege_incomplete = 'College Incomplete',
                educationcollege = 'College',
                gendermale = 'Male',
                racebrown = 'Brown',
                raceblack = 'Black',
                raceasian = 'Asian',
                raceindigenous = 'Indigenous',
                cohort = 'Cohort',
                establishment = 'Establishment'),
       title = 'Logit Model for Connectedness (By Skill Level)',
       notes = 'Notes : Coefficients on education variables are relative to no education and those on race variables are relative to white.',
       tex = TRUE, file = glue('{harddrive}/intermediate/outputs/tabs/model_logit_skilllevel.tex'))
################## 
###############################################################################################






###############################################################################################
##################  CONDUCTING REGRESSIONS 11 and 12 - MANAGERIAL VS. NON-MANAGERIAL.
model_logit_11.1 <- fixest::feglm(has_connections ~ education + gender + race, family = binomial(link = 'logit'), data = final_data_connections[job_level == 'managerial',], vcov = ~establishment)
model_logit_11.2 <- fixest::feglm(has_connections ~ education + gender + race | cohort, family = binomial(link = 'logit'), data = final_data_connections[job_level == 'managerial',], vcov = ~establishment)
model_logit_11.3 <- fixest::feglm(has_connections ~ education + gender + race | establishment, family = binomial(link = 'logit'), data = final_data_connections[job_level == 'managerial',], vcov = ~establishment)
model_logit_11.4 <- fixest::feglm(has_connections ~ education + gender + race | establishment + cohort, family = binomial(link = 'logit'), data = final_data_connections[job_level == 'managerial',], vcov = ~establishment)
model_logit_11.5 <- fixest::feglm(has_connections ~ education + gender + race | establishment^cohort, family = binomial(link = 'logit'), data = final_data_connections[job_level == 'managerial',], vcov = ~establishment)

model_logit_12.1 <- fixest::feglm(has_connections ~ education + gender + race, family = binomial(link = 'logit'), data = final_data_connections[job_level == 'bluecollar' |  job_level == 'whitecollar',], vcov = ~establishment)
model_logit_12.2 <- fixest::feglm(has_connections ~ education + gender + race | cohort, family = binomial(link = 'logit'), data = final_data_connections[job_level == 'bluecollar' |  job_level == 'whitecollar',], vcov = ~establishment)
model_logit_12.3 <- fixest::feglm(has_connections ~ education + gender + race | establishment, family = binomial(link = 'logit'), data = final_data_connections[job_level == 'bluecollar' |  job_level == 'whitecollar',], vcov = ~establishment)
model_logit_12.4 <- fixest::feglm(has_connections ~ education + gender + race | establishment + cohort, family = binomial(link = 'logit'), data = final_data_connections[job_level == 'bluecollar' |  job_level == 'whitecollar',], vcov = ~establishment)
model_logit_12.5 <- fixest::feglm(has_connections ~ education + gender + race | establishment^cohort, family = binomial(link = 'logit'), data = final_data_connections[job_level == 'bluecollar' |  job_level == 'whitecollar',], vcov = ~establishment)
##################  


################## 
file.remove(glue('{harddrive}/intermediate/outputs/tabs/model_logit_managerial.tex'))
etable(model_logit_11.1, model_logit_11.2, model_logit_11.3, model_logit_11.4, model_logit_11.5,
       model_logit_12.1, model_logit_12.2, model_logit_12.3, model_logit_12.4, model_logit_12.5,
       digits = 'r2',
       headers=list(':_:Job Type' = list('Managerial' = 5, 'Non-managerial' = 5)),
       dict = c(has_connections = 'Has Connections', 
                educationless_than_hs = 'Less than High School',
                educationhs_incomplete = 'High School Incomplete',
                educationhs = 'High School',
                educationcollege_incomplete = 'College Incomplete',
                educationcollege = 'College',
                gendermale = 'Male',
                racebrown = 'Brown',
                raceblack = 'Black',
                raceasian = 'Asian',
                raceindigenous = 'Indigenous',
                cohort = 'Cohort',
                establishment = 'Establishment'),
       title = 'Logit Model for Connectedness (By Managerial vs. Non-managerial)',
       notes = 'Notes : Coefficients on education variables are relative to no education and those on race variables are relative to white.',
       tex = TRUE, file = glue('{harddrive}/intermediate/outputs/tabs/model_logit_managerial.tex'))
################## 
###############################################################################################