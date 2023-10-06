# Test 1994 data vs. 1993.

data_1993 <- fread(glue('{raw}/allstates_blind_1993_newvars.csv'),
                   select = c('tipovinculo', 'firm_type'))

data_1994 <- fread(glue('{raw}/allstates_blind_1994_newvars.csv'),
                   select = c('tipovinculo', 'firm_type'))

data_1993_a <- data_1993[, .(count = .N), by = .(tipovinculo, firm_type)]                  
data_1994_a <- data_1994[, .(count = .N), by = .(tipovinculo, firm_type)]                  

data_1993_b <- data_1993[, .(count = .N), by = .(tipovinculo)]                  
data_1994_b <- data_1994[, .(count = .N), by = .(tipovinculo)]

