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

#### END ####

#### LOAD DATA ####
df_us_2019 <- fread("../bg-us/int_data/us_stru_2019_wfh.csv", nThread = 4, integer64 = "numeric", select = c("job_id","job_date","soc","onet","year_quarter","wfh_wham","job_domain","job_url"))
df_us_2020 <- fread("../bg-us/int_data/us_stru_2020_wfh.csv", nThread = 4, integer64 = "numeric", select = c("job_id","job_date","soc","onet","year_quarter","wfh_wham","job_domain","job_url"))
df_us_2021 <- fread("../bg-us/int_data/us_stru_2021_wfh.csv", nThread = 4, integer64 = "numeric", select = c("job_id","job_date","soc","onet","year_quarter","wfh_wham","job_domain","job_url"))
df_us_2022 <- fread("../bg-us/int_data/us_stru_2022_wfh.csv", nThread = 4, integer64 = "numeric", select = c("job_id","job_date","soc","onet","year_quarter","wfh_wham","job_domain","job_url"))

df_us <- rbindlist(list(df_us_2019,df_us_2020,df_us_2021,df_us_2022))
ls()
remove(list = setdiff(ls(), "df_us"))

soc2010_names <- fread(file = "./aux_data/us_soc_2010_names.csv") %>% rename(soc_2_digit_name = name)
occupations_workathome <- read_csv("./aux_data/occupations_workathome.csv") %>% rename(onet = onetsoccode) %>% select(onet, title) %>% rename(onet_name = title)

df_us <- df_us %>%
  .[, soc10_2d := str_sub(soc, 1, 2)]

soc2010_names$soc10_2d <- as.numeric(soc2010_names$soc10_2d)
occupations_workathome$onet <- as.character(occupations_workathome$onet)

df_us$soc10_2d <- as.numeric(df_us$soc10_2d)
df_us$onet <- as.character(df_us$onet)

nrow(df_us)
df_us <- df_us %>%
  left_join(soc2010_names) %>%
  left_join(occupations_workathome) %>%
  setDT(.)
nrow(df_us)

colnames(df_us)

df_us <- df_us %>% select(job_id, job_date, soc10_2d, soc_2_digit_name, soc, onet, onet_name, wfh_wham, job_url) %>% rename(soc_2d = soc10_2d, soc_2_d_name = soc_2_digit_name) %>% setDT()

fwrite(df_us, file = "./aux_data/us_ads_2019_22_occupation_lookup.csv")



