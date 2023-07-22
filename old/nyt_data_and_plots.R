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

setDTthreads(4)
getDTthreads()
#quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg-us")

#### LOAD DATA AND COLLAPSE BY CITY/STATE x QUARTER ####
paths <- list.files("/mnt/disks/pdisk/bg-us/int_data", pattern = "_wfh.csv", full.names = T)
paths

df_ag_2019 <- fread(paths[1], nThread = 8) %>%
  .[!is.na(wfh_wham)] %>%
  .[, city := str_to_title(city)] %>%
  .[, state := str_to_title(state)] %>%
  .[!is.na(city) & !is.na(soc) & !is.na(state),
    .(.N, wfh_share = mean(wfh_wham, na.rm = T)),
    by = .(year_quarter, city, state)] %>%
  .[N > 250]

df_ag_2020 <- fread(paths[2], nThread = 8) %>%
  .[!is.na(wfh_wham)] %>%
  .[, city := str_to_title(city)] %>%
  .[, state := str_to_title(state)] %>%
  .[!is.na(city) & !is.na(soc) & !is.na(state),
    .(.N, wfh_share = mean(wfh_wham, na.rm = T)),
    by = .(year_quarter, city, state)] %>%
  .[N > 250]

df_ag_2021 <- fread(paths[3], nThread = 8) %>%
  .[!is.na(wfh_wham)] %>%
  .[, city := str_to_title(city)] %>%
  .[, state := str_to_title(state)] %>%
  .[!is.na(city) & !is.na(soc) & !is.na(state),
    .(.N, wfh_share = mean(wfh_wham, na.rm = T)),
    by = .(year_quarter, city, state)] %>%
  .[N > 250]

df_ag_2022 <- fread(paths[4], nThread = 8) %>%
  .[ymd(job_date) <= as.Date("2022-05-18")] %>%
  .[!is.na(wfh_wham)] %>%
  .[, city := str_to_title(city)] %>%
  .[, state := str_to_title(state)] %>%
  .[!is.na(city) & !is.na(soc) & !is.na(state),
    .(.N, wfh_share = mean(wfh_wham, na.rm = T)),
    by = .(year_quarter, city, state)] %>%
  .[N > 250]

df_ag <- bind_rows(df_ag_2019, df_ag_2020, df_ag_2021, df_ag_2022) %>% setDT(.)

table(df_ag$year_quarter)

remove(list = setdiff(ls(), "df_ag"))

df_ag$year_quarter <- as.character(as.yearqtr(df_ag$year_quarter))

# Check coverage
#df_ag_check <- df_ag %>%
#  .[, .(N = sum(N)), by = year_quarter]
#View(df_ag_check)
#rm(df_ag_check)

View(df_ag)

# Seattle and Portland

df_ag_c <- df_ag %>%
  .[, city_state := paste0(city,"--",state)] %>%
  .[city_state %in% c("Birmingham--Alabama","Boston--Massachusetts","Cleveland--Ohio",
                      "Columbus--Ohio","Des Moines--Iowa","Houston--Texas","Indianapolis--Indiana",
                      "Jacksonville--Florida","Louisville--Kentucky","Memphis--Tennessee",
                      "New York--New York","Oklahoma City--Oklahoma","San Francisco--California",
                      "Savannah--Georgia","Wichita--Kansas", "Seattle--Washington", "Portland--Oregon")]

df_ag_final <- df_ag_c %>%
  .[, .(N = sum(N), wfh_share = sum(wfh_share * (N/sum(N)))), by = .(city, state, year_quarter)]

df_ag_final$wfh_share <- 100*round(df_ag_final$wfh_share, 3)

colnames(df_ag_final)

df_ag_final <- df_ag_final %>%
  arrange(city, state) %>%
  mutate(city = paste0(city,", ",state)) %>%
  select(-c(N,state))

df_ag_final <- df_ag_final %>%
  group_by(year_quarter) %>%
  pivot_wider(., names_from = city, values_from = wfh_share)

fwrite(df_ag_final, "./aux_data/city_quarter_wfh_nyt_final_plus_2cit_for_nb.csv")
  
#### END ####






#### CHECK SPIKE ####

df_2022 <- fread(paths[4], nThread = 8) %>%
  select(city, state, wfh_wham, job_date, employer, soc) %>%
  .[, year_quarter := as.character(as.yearqtr(ymd(job_date)))] %>%
  .[!is.na(wfh_wham)] %>%
  .[, city := str_to_title(city)] %>%
  .[, state := str_to_title(state)] %>%
  .[!is.na(city) & !is.na(state)]

# Check weeks

head(df_2022)

df_2022_weekly_3c <- df_2022 %>%
  filter(job_date != "2022-05-19" & job_date != "2022-05-20" & job_date != "2022-05-21") %>%
  filter((city == "Kansas City" & state == "Missouri") | (city == "Columbus" & state == "Ohio")) %>%
  .[, year_week := paste0(year(ymd(job_date)),"-",sprintf("%02d",week(ymd(job_date))))] %>%
  #.[ year_week %in% c("2022-20", "2022-18")] %>%
  .[, .(.N, wfh_share = mean(wfh_wham)), by = .(year_week, city, state)]

df_2022_daily_3c <- df_2022 %>%
  filter((city == "Kansas City" & state == "Missouri") | (city == "Columbus" & state == "Ohio")) %>%
  .[, year_week := paste0(year(ymd(job_date)),"-",sprintf("%02d",week(ymd(job_date))))] %>%
  .[year(ymd(job_date)) == 2022] %>%
  .[ year_week %in% c("2022-18", "2022-19", "2022-20", "2022-21", "2022-22")] %>%
  .[, .(.N, wfh_share = mean(wfh_wham)), by = .(year_week, job_date, city, state)]


df_2022_check_days <- df_2022 %>%
  filter((city == "Columbus" & state == "Ohio")) %>%
  filter(year_quarter == "2022 Q2") %>%
  filter(job_date != "2022-05-20") %>%
  .[, .(.N, wfh_share = mean(wfh_wham)), by = .(job_date)]

max(df_2022_check_days$N)
sd(df_2022_check_days$N)
median(df_2022_check_days$N)
mean(df_2022_check_days$N)


# Check occs

df_2022_weekly_3c_soc15_vs_not <- df_2022 %>%
  #filter(job_date != "2022-05-20" & job_date != "2022-05-21") %>%
  filter((city == "Kansas City" & state == "Missouri") | (city == "Columbus" & state == "Ohio")) %>%
  .[, year_week := paste0(year(ymd(job_date)),"-",sprintf("%02d",week(ymd(job_date))))] %>%
  .[, soc2 := str_sub(soc, 1, 2)] %>%
  .[, soc2_111523 := ifelse(soc2 %in% c(11,15,23), "YES", "NOT")] %>%
  #.[ year_week %in% c("2022-20", "2022-18")] %>%
  .[, .(.N, wfh_share = mean(wfh_wham)), by = .(year_week, city, state)] %>%
  .[, share := N/sum(N), by = .(year_week, city, state)]

df_2022_daily_3c_soc15_vs_not <- df_2022 %>%
  #filter(job_date != "2022-05-20" & job_date != "2022-05-21") %>%
  filter((city == "Kansas City" & state == "Missouri") | (city == "Columbus" & state == "Ohio")) %>%
  .[, year_week := paste0(year(ymd(job_date)),"-",sprintf("%02d",week(ymd(job_date))))] %>%
  .[, soc2 := str_sub(soc, 1, 2)] %>%
  .[, soc2_111523 := ifelse(soc2 %in% c(11,15,23), "YES", "NOT")] %>%
  .[year(ymd(job_date)) == 2022] %>%
  .[ year_week %in% c("2022-18", "2022-19", "2022-20", "2022-21", "2022-22")] %>%
  .[, .(.N, wfh_share = mean(wfh_wham)), by = .(job_date, year_week, city, state, soc2_111523)] %>%
  .[, share := N/sum(N), by = .(job_date, year_week, city, state)]



#### 


