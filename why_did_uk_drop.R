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

##### GITHUB RUN ####
# cd /mnt/disks/pdisk/bgt_code_repo
# git init
# git add .
# git pull -m "check"
# git commit -m "autosave"
# git push origin master

setDTthreads(4)
getDTthreads()
#quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg_combined/")

df_uk_2014 <- fread("./int_data/df_uk_2014_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()
df_uk_2015 <- fread("./int_data/df_uk_2015_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()
df_uk_2016 <- fread("./int_data/df_uk_2016_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()
df_uk_2017 <- fread("./int_data/df_uk_2017_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()
df_uk_2018 <- fread("./int_data/df_uk_2018_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()
df_uk_2019 <- fread("./int_data/df_uk_2019_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()
df_uk_2020 <- fread("./int_data/df_uk_2020_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()
df_uk_2021 <- fread("./int_data/df_uk_2021_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()
df_uk_2022 <- fread("./int_data/df_uk_2022_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()
df_uk_2023 <- fread("./int_data/df_uk_2023_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()

df_uk_stand <- rbindlist(list(df_uk_2014, df_uk_2015, df_uk_2016, df_uk_2017, df_uk_2018,df_uk_2019,df_uk_2020,df_uk_2021,df_uk_2022,df_uk_2023))
remove(list = c("df_uk_2014", "df_uk_2015", "df_uk_2016", "df_uk_2017", "df_uk_2018","df_uk_2019","df_uk_2020","df_uk_2021","df_uk_2022","df_uk_2023"))

df_uk_2014 <- fread("../bg-uk/int_data/uk_stru_2014_wfh.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()
df_uk_2015 <- fread("../bg-uk/int_data/uk_stru_2015_wfh.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()
df_uk_2016 <- fread("../bg-uk/int_data/uk_stru_2016_wfh.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()
df_uk_2017 <- fread("../bg-uk/int_data/uk_stru_2017_wfh.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()
df_uk_2018 <- fread("../bg-uk/int_data/uk_stru_2018_wfh.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()
df_uk_2019 <- fread("../bg-uk/int_data/uk_stru_2019_wfh.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()
df_uk_2020 <- fread("../bg-uk/int_data/uk_stru_2020_wfh.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()
df_uk_2021 <- fread("../bg-uk/int_data/uk_stru_2021_wfh.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()
df_uk_2022 <- fread("../bg-uk/int_data/uk_stru_2022_wfh.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()
df_uk_2023 <- fread("../bg-uk/int_data/uk_stru_2023_wfh.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()

df_uk_stru <- rbindlist(list(df_uk_2014, df_uk_2015, df_uk_2016, df_uk_2017, df_uk_2018,df_uk_2019,df_uk_2020,df_uk_2021,df_uk_2022,df_uk_2023))
remove(list = c("df_uk_2014", "df_uk_2015", "df_uk_2016", "df_uk_2017", "df_uk_2018","df_uk_2019","df_uk_2020","df_uk_2021","df_uk_2022","df_uk_2023"))









