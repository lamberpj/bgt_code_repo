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
library("devtools")
#install_github("markwestcott34/stargazer-booktabs")
library("zoo")
library("stargazer")
library("tsibble")
library("readxl")
library("ggplot2")
library("scales")
library("ggpubr")
library("ggthemr")
library("ggpmisc")
library("RColorBrewer")

ggthemr('flat')
library(egg)
library(extrafont)
library(fixest)
library(stats)

setwd("/mnt/disks/pdisk/bg_combined/")

#### COMPARE US STATES TO GMD ####
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

setDTthreads(8)
getDTthreads()
#quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg_combined/")

#### LOAD "ALL" ####
remove(list = ls())

setwd("/mnt/disks/pdisk/bg-us/")
df_us_2014 <- fread("./int_data/us_stru_2014_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "US") %>% setDT()
df_us_2015 <- fread("./int_data/us_stru_2015_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "US") %>% setDT()
df_us_2016 <- fread("./int_data/us_stru_2016_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "US") %>% setDT()
df_us_2017 <- fread("./int_data/us_stru_2017_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "US") %>% setDT()
df_us_2018 <- fread("./int_data/us_stru_2018_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "US") %>% setDT()
df_us_2019 <- fread("./int_data/us_stru_2019_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "US") %>% setDT()
df_us_2020 <- fread("./int_data/us_stru_2020_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "US") %>% setDT()
df_us_2021 <- fread("./int_data/us_stru_2021_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "US") %>% setDT()
df_us_2022 <- fread("./int_data/us_stru_2022_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "US") %>% setDT()
df_us_2023 <- fread("./int_data/us_stru_2023_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "US") %>% setDT()

setwd("/mnt/disks/pdisk/bg-uk/")
df_uk_2014 <- fread("./int_data/uk_stru_2014_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "UK") %>% setDT()
df_uk_2015 <- fread("./int_data/uk_stru_2015_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "UK") %>% setDT()
df_uk_2016 <- fread("./int_data/uk_stru_2016_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "UK") %>% setDT()
df_uk_2017 <- fread("./int_data/uk_stru_2017_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "UK") %>% setDT()
df_uk_2018 <- fread("./int_data/uk_stru_2018_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "UK") %>% setDT()
df_uk_2019 <- fread("./int_data/uk_stru_2019_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "UK") %>% setDT()
df_uk_2020 <- fread("./int_data/uk_stru_2020_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "UK") %>% setDT()
df_uk_2021 <- fread("./int_data/uk_stru_2021_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "UK") %>% setDT()
df_uk_2022 <- fread("./int_data/uk_stru_2022_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "UK") %>% setDT()
df_uk_2023 <- fread("./int_data/uk_stru_2023_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "UK") %>% setDT()

setwd("/mnt/disks/pdisk/bg_combined/")

df_uk <- rbindlist(list(df_uk_2014, df_uk_2015, df_uk_2016, df_uk_2017, df_uk_2018,df_uk_2019,df_uk_2020,df_uk_2021,df_uk_2022,df_uk_2023))
remove(list = c("df_uk_2014", "df_uk_2015", "df_uk_2016", "df_uk_2017", "df_uk_2018","df_uk_2019","df_uk_2020","df_uk_2021","df_uk_2022","df_uk_2023"))

df_us <- rbindlist(list(df_us_2014, df_us_2015, df_us_2016, df_us_2017, df_us_2018,df_us_2019,df_us_2020,df_us_2021,df_us_2022,df_us_2023))
remove(list = c("df_us_2014", "df_us_2015", "df_us_2016", "df_us_2017", "df_us_2018","df_us_2019","df_us_2020","df_us_2021","df_us_2022","df_us_2023"))

colnames(df_uk)
colnames(df_us)

df_uk <- df_uk %>% .[, county_state := paste0(canon_county,"_",nation)]
df_us <- df_us %>% .[, county_state := paste0(county,"_",state)]

uniqueN(df_uk$county_state) # 251
uniqueN(df_us$county_state) # 3,285

df_uk <- df_uk[!is.na(job_domain) & job_domain != ""]
df_us <- df_us[!is.na(job_domain) & job_domain != ""]

df_uk <- df_uk %>% .[!grepl("jobisjob", job_domain)]
df_us <- df_us %>% .[!grepl("careerbuilder", job_domain)]

df_uk <- df_uk %>% rename(state = nation, county = canon_county)
df_uk <- df_uk %>% .[, year_month := as.yearmon(job_date)]
df_us <- df_us %>% .[, year_month := as.yearmon(job_date)]

df_uk <- df_uk %>% select(wfh_wham, country, state, county, year_month, job_date)
df_us <- df_us %>% select(wfh_wham, country, state, county, year_month, job_date)

df_all_list <- list(df_uk,df_us)
remove(list = setdiff(ls(), "df_all_list"))

daily_data <- lapply(1:length(df_all_list), function(i) {
  df_all_list[[i]] %>%
    .[county != "" & !is.na(county)] %>%
    .[, .(daily_share = mean(wfh_wham, na.rm = T), N = .N), by = .(country, state, county, year_month, job_date)] %>%
    .[, job_date := ymd(job_date)] %>%
    .[as.yearmon(year_month) >= as.yearmon(ymd("20170101")) & as.yearmon(year_month) <= as.yearmon(ymd("20230301"))] %>%
    .[, county_state := paste0(county, ", ", state)]
}) %>%
  rbindlist(.)

fwrite(daily_data, file = "./aux_data/all_counties_daily.csv")

# Set SD filter paramters
level_threshold <- 0.02 # absolute deviation in level of raw mean and trimmed mean
log_threshold <- 0.1 # absolute deviation in level of log(raw mean) and log(trimmed mean)

# Load all data
remove(list = ls())
daily_data <- fread(file = "./aux_data/all_counties_daily.csv")
head(daily_data)
check <- daily_data %>%
  .[, .(N = sum(N)), by = .(country, county_state)]

ts_for_plot <- daily_data %>%
  .[, job_date := ymd(job_date)] %>%
  .[as.yearmon(year_month) >= as.yearmon(ymd("20180901")) & as.yearmon(year_month) <= as.yearmon(ymd("20230301"))] %>%
  #.[count_for_thresh >= 150000] %>%
  .[, l1o_monthly_mean := (sum(daily_share*N)-daily_share*N)/(sum(N) - N), by = .(country, county, state, county_state, year_month)] %>%
  .[, monthly_mean := sum(daily_share*N)/(sum(N)), by = .(country, county, state, county_state, year_month)] %>%
  .[, l1o_keep := ifelse(abs(monthly_mean - l1o_monthly_mean) > 0.02 | abs(log(monthly_mean/l1o_monthly_mean)) > 0.10, 0, 1)] %>%
  .[, l1o_keep := ifelse(is.na(l1o_keep), 1, l1o_keep)] %>%
  .[, l1o_daily_with_nas := ifelse(l1o_keep == 1, daily_share, NA)]

check <- ts_for_plot %>%
  .[, .(share_outlier_postings_percent = 100*sum((1-l1o_keep)*N)/sum(N)), by = .(country)] %>%
  select(country, share_outlier_postings_percent)
  
(days_replaced <- 100*round(sum(check$replaced)/sum(check$days), digits = 4))

ts_for_plot <- ts_for_plot %>%
  .[order(county, state, county_state, job_date)] %>%
  .[, monthly_mean_l1o := sum(l1o_daily_with_nas[!is.na(l1o_daily_with_nas)]*N[!is.na(l1o_daily_with_nas)], na.rm = T)/(sum(N[!is.na(l1o_daily_with_nas)], na.rm = T)), by = .(county, state, county_state, year_month)] %>%
  .[, .(N = sum(N),
        job_date = min(job_date),
        drop_days_share = sum(1-l1o_keep)/.N,
        monthly_mean = monthly_mean[1],
        monthly_mean_l1o = monthly_mean_l1o[1]),
    by = .(country, county, state, county_state, year_month)] %>%
  .[, monthly_mean_3ma := rollmean(monthly_mean, k = 3, align = "right", fill = NA), by = .(country, county, state, county_state)] %>%
    .[, monthly_mean_3ma_l1o := rollmean(monthly_mean_l1o, k = 3, align = "right", fill = NA), by = .(country, county, state, county_state)] %>%
  #.[, monthly_mean_l1o := ifelse(as.yearmon(year_month) %in% as.yearmon(ymd(c("20200301", "20200401", "20200501", "20200601"))), monthly_mean, monthly_mean_l1o)] %>%
  #.[, monthly_mean_3ma := ifelse(as.yearmon(year_month) %in% as.yearmon(ymd(c("20200301", "20200401", "20200501", "20200601"))), monthly_mean, monthly_mean_3ma)] %>%
  #.[, monthly_mean_3ma_l1o := ifelse(as.yearmon(year_month) %in% as.yearmon(ymd(c("20200301", "20200401", "20200501", "20200601"))), monthly_mean, monthly_mean_3ma_l1o)] %>%
  .[as.yearmon(year_month) >= as.yearmon(ymd("20190101")) & as.yearmon(year_month) <= as.yearmon(ymd("20230301"))]

ts_for_plot <- ts_for_plot %>%
  group_by(county, state, county_state, year_month, job_date, N) %>%
  pivot_longer(cols = c(drop_days_share:monthly_mean_3ma_l1o)) %>%
  setDT(.)

fwrite(ts_for_plot, file = "./aux_data/county_level_ts.csv")

unique(ts_for_plot$country)
