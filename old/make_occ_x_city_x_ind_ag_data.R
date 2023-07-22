#### SETUP ####
remove(list = ls())

library("devtools")
install_github("trinker/textclean")
library("textclean")
library("data.table")
library("tidyverse")
library("janitor")
library("lubridate")
library("doParallel")
#library("textclean")
#install.packages("qdapRegex")
library("quanteda")
library("tokenizers")
library("stringi")
#library("readtext")
library("rvest")
library("xml2")
#library("DescTools")
library("zoo")
library("stargazer")
library("readxl")
library("ggplot2")
library("scales")
library("ggpubr")
library("ggthemr")
ggthemr('flat')

setDTthreads(1)
getDTthreads()
detectCores()
quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg_combined/")

#### SETUP ####
remove(list = ls())
options(scipen=999)
library("devtools")
#install_github("trinker/textclean")
library("textclean")
library("data.table")
library("tidyverse")
library("janitor")
library("lubridate")
library("doParallel")
#library("textclean")
#install.packages("qdapRegex")
#library("quanteda")
#library("tokenizers")
library("stringi")
#library("readtext")
#library("rvest")
#library("xml2")
#library("DescTools")
library("zoo")
library("stargazer")
library("readxl")
library("ggplot2")
library("scales")
library("ggpubr")
library("ggthemr")
ggthemr('flat')
library(egg)
library(extrafont)
library(fixest)
# font_import()
#loadfonts(device="postscript")
#fonts()

setDTthreads(1)
getDTthreads()
#quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg_combined/")

#### PREPARE WEIGHTED DATA #### ####
remove(list = ls())

w_aus_2019 <- fread("./aux_data/emp_weights/w_aus_2019.csv")
w_nz_2019 <- fread("./aux_data/emp_weights/w_nz_2019.csv") %>%
  .[, anzsco_code := as.numeric(str_sub(anzsco_code, 1, 4))] %>%
  .[, .(tot_emp = sum(as.numeric(tot_emp), na.rm = T)), by = anzsco_code] %>%
  .[, emp_share := tot_emp / sum(tot_emp, na.rm = T)]
w_can_2019 <- fread("./aux_data/emp_weights/w_can_2019.csv") %>%
  .[, can_noc := as.numeric(can_noc)]
w_uk_2019 <- fread("./aux_data/emp_weights/w_uk_2019.csv") %>%
  .[, emp_share := ifelse(is.na(emp_share), 0, emp_share)]
w_us_2019 <- fread("./aux_data/emp_weights/w_us_2019.csv") %>%
  .[, us_soc18 := str_sub(us_soc18, 1, 5)] %>%
  .[, .(tot_emp = sum(tot_emp, na.rm = T)), by = us_soc18] %>%
  .[, emp_share := tot_emp / sum(tot_emp, na.rm = T)]


#### IMPORT AND COLLAPSE ####

# CANADA #

paths <- list.files("../bg-can/int_data/", pattern = ".csv", full.names = T)

colnames(fread(paths[1], nrow = 100))

"job_date", "canon_country","canon_state","canon_city","consolidated_onet","onet_name","ussoc_detailed_occupation",
"ussoc_broad_occupation"      "ussoc_minor_group"           "ussoc_major_group"          
"noc_code"                    "noc_unit_group"              "noc_minor_group"             "noc_major_group"             "noc_broad_category"         
"bgt_occ"                     "bgt_occ_name"                "bgt_occ_group_name"          "bgt_career_area_name"        "consolidated_inferred_naics"
"industry_name"               "industry_group_name"         "sub_sector_name"             "sector_name"                 "stock_ticker"               
"job_ymd"                     "year_quarter"                "wfh_wham_prob"               "wfh_wham"   


#### END ####








