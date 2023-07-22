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
library("tmaptools")

#library("textclean")
#install.packages("qdapRegex")
#library("quanteda")
#library("tokenizers")
library("stringi")
library("DescTools")
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
library("ggpmisc")
ggthemr('flat')
library(egg)
library(extrafont)
library(fixest)
# font_import()
#loadfonts(device="postscript")
#fonts()

setDTthreads(8)
getDTthreads()
#quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg_combined/")

#### LOAD "ALL" ####
df_nz <- fread("./int_data/df_nz_standardised.csv", nThread = 8, integer64 = "numeric", select = c("employer", "country", "state", "city", "job_date"))
df_aus <- fread("./int_data/df_aus_standardised.csv", nThread = 8, integer64 = "numeric", select = c("employer", "country", "state", "city", "job_date"))
df_can <- fread("./int_data/df_can_standardised.csv", nThread = 8, integer64 = "numeric", select = c("employer", "country", "state", "city", "job_date"))
df_uk <- fread("./int_data/df_uk_standardised.csv", nThread = 8, integer64 = "numeric", select = c("employer", "country", "state", "city", "job_date"))

colnames(fread("../bg-us/int_data/us_stru_2014_wfh.csv", nrows = 100))

df_us_2014 <- fread("../bg-us/int_data/us_stru_2014_wfh.csv", nThread = 8, integer64 = "numeric", select = c("employer", "state", "county", "job_date"))
df_us_2015 <- fread("../bg-us/int_data/us_stru_2015_wfh.csv", nThread = 8, integer64 = "numeric", select = c("employer", "state", "county", "job_date"))
df_us_2016 <- fread("../bg-us/int_data/us_stru_2016_wfh.csv", nThread = 8, integer64 = "numeric", select = c("employer", "state", "county", "job_date"))
df_us_2017 <- fread("../bg-us/int_data/us_stru_2017_wfh.csv", nThread = 8, integer64 = "numeric", select = c("employer", "state", "county", "job_date"))
df_us_2018 <- fread("../bg-us/int_data/us_stru_2018_wfh.csv", nThread = 8, integer64 = "numeric", select = c("employer", "state", "county", "job_date"))
df_us_2019 <- fread("../bg-us/int_data/us_stru_2019_wfh.csv", nThread = 8, integer64 = "numeric", select = c("employer", "state", "county", "job_date"))
df_us_2020 <- fread("../bg-us/int_data/us_stru_2020_wfh.csv", nThread = 8, integer64 = "numeric", select = c("employer", "state", "county", "job_date"))
df_us_2021 <- fread("../bg-us/int_data/us_stru_2021_wfh.csv", nThread = 8, integer64 = "numeric", select = c("employer", "state", "county", "job_date"))
df_us_2022 <- fread("../bg-us/int_data/us_stru_2022_wfh.csv", nThread = 8, integer64 = "numeric", select = c("employer", "state", "county", "job_date"))

df_us <- rbindlist(list(df_us_2014,df_us_2015,df_us_2016,df_us_2017,df_us_2018,df_us_2019,df_us_2020,df_us_2021,df_us_2022))

remove(list = setdiff(ls(), "df_us"))

df_us_employer <- df_us %>%
  .[employer != "" & !is.na(employer)] %>%
  .[, .(.N,
        min_date = min(job_date),
        max_date = max(job_date),
        N_state = uniqueN(state),
        N_county = uniqueN(county)),
    by = employer] %>%
  .[N >= 5]

fwrite(df_us_employer, "./aux_data/us_employers_5ormore.csv")

df_us_employer <- df_us %>%
  .[employer != "" & !is.na(employer)] %>%
  .[, .(.N,
        min_date = min(job_date),
        max_date = max(job_date)),
    by = .(employer, state, county)] %>%
  .[N >= 5]

fwrite(df_us_employer, "./aux_data/us_employers_5ormore_state_county.csv")







