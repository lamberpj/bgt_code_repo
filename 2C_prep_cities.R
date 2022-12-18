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
df_nz <- fread("./int_data/df_nz_standardised.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""]
df_aus <- fread("./int_data/df_aus_standardised.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""]
df_can <- fread("./int_data/df_can_standardised.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""]
df_uk <- fread("./int_data/df_uk_standardised.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""]
df_us_2014 <- fread("./int_data/df_us_2014_standardised.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""]
df_us_2015 <- fread("./int_data/df_us_2015_standardised.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""]
df_us_2016 <- fread("./int_data/df_us_2016_standardised.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""]
df_us_2017 <- fread("./int_data/df_us_2017_standardised.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""]
df_us_2018 <- fread("./int_data/df_us_2018_standardised.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""]
df_us_2019 <- fread("./int_data/df_us_2019_standardised.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""]
df_us_2020 <- fread("./int_data/df_us_2020_standardised.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""]
df_us_2021 <- fread("./int_data/df_us_2021_standardised.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""]
df_us_2022 <- fread("./int_data/df_us_2022_standardised.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""]

df_us <- rbindlist(list(df_us_2014, df_us_2015, df_us_2016, df_us_2017, df_us_2018,df_us_2019,df_us_2020,df_us_2021,df_us_2022))
remove(list = c("df_us_2014", "df_us_2015", "df_us_2016", "df_us_2017", "df_us_2018","df_us_2019","df_us_2020","df_us_2021","df_us_2022"))

# uniqueN(df_nz$job_id) # 1,297,812
# uniqueN(df_aus$job_id) # 4,936,765
# uniqueN(df_can$job_id) # 7,540,054
# uniqueN(df_uk$job_id) # 36,914,129
# uniqueN(df_us$job_id) # 148,224,554
# 
# uniqueN(df_nz$employer) # 27,712
# uniqueN(df_aus$employer) # 131,995
# uniqueN(df_can$employer) # 586,165
# uniqueN(df_uk$employer) # 619,641
# uniqueN(df_us$employer) # 3,262,838

df_nz <- df_nz %>% .[, city_state := paste0(city,"_",state)]
df_aus <- df_aus %>% .[, city_state := paste0(city,"_",state)]
df_can <- df_can %>% .[, city_state := paste0(city,"_",state)]
df_uk <- df_uk %>% .[, city_state := paste0(city,"_",state)]
df_us <- df_us %>% .[, city_state := paste0(city,"_",state)]

uniqueN(df_nz$city_state) # 67
uniqueN(df_aus$city_state) # 57
uniqueN(df_can$city_state) # 3,653
uniqueN(df_uk$city_state) # 2,249
uniqueN(df_us$city_state) # 31,568

df_nz <- df_nz %>% .[!grepl("mercadojobs", job_url)]
df_can <- df_can %>% .[!grepl("workopolis", job_url)]
df_can <- df_can %>% .[!grepl("careerjet", job_url)]
df_uk <- df_uk %>% .[!grepl("jobisjob", job_url)]
df_us <- df_us %>% .[!grepl("careerbuilder", job_url)]

df_all_list <- list(df_nz,df_aus,df_can,df_uk,df_us)
remove(list = setdiff(ls(), "df_all_list"))

daily_data <- lapply(1:length(df_all_list), function(i) {
  df_all_list[[i]] %>%
    .[city != "" & !is.na(city)] %>%
    .[, .(daily_share = sum(job_id_weight*wfh_wham, na.rm = T)/sum(job_id_weight, na.rm = T), N = sum(job_id_weight)), by = .(country, state, city, year_month, job_date)] %>%
    .[, job_date := ymd(job_date)] %>%
    .[as.yearmon(year_month) >= as.yearmon(ymd("20170101")) & as.yearmon(year_month) <= as.yearmon(ymd("20220601"))] %>%
    .[, city_state := paste0(city, ", ", state)]
}) %>%
  rbindlist(.)

daily_data_national <- lapply(1:length(df_all_list), function(i) {
  df_all_list[[i]] %>%
    .[, .(daily_share = sum(job_id_weight*wfh_wham, na.rm = T)/sum(job_id_weight, na.rm = T), N = sum(job_id_weight)), by = .(country, year_month, job_date)] %>%
    .[, job_date := ymd(job_date)] %>%
    .[as.yearmon(year_month) >= as.yearmon(ymd("20170101")) & as.yearmon(year_month) <= as.yearmon(ymd("20220601"))] %>%
    .[, city_state := country]
}) %>%
  rbindlist(.)

daily_data_old <- daily_data

daily_data <- bind_rows(daily_data, daily_data_national)

thresh_cities <- daily_data %>%
  .[as.yearmon(year_month) >= as.yearmon(ymd("20190101")) & as.yearmon(year_month) <= as.yearmon(ymd("20220601"))] %>%
  .[, .(count_for_thresh = sum(N)), by = .(country, city_state)] %>%
  .[order(country, desc(count_for_thresh))] %>%
  .[, rank := 1:.N, by = country] %>%
  .[, keep := ifelse(country == "US" & rank <= 401, 1, 0)] %>%
  .[, keep := ifelse(country == "UK" & rank <= 31, 1, keep)] %>%
  .[, keep := ifelse(country == "Canada" & rank <= 11, 1, keep)] %>%
  .[, keep := ifelse(country == "NZ" & rank <= 4, 1, keep)] %>%
  .[, keep := ifelse(country == "Australia" & rank <= 6, 1, keep)]

daily_data <- daily_data %>%
  left_join(thresh_cities) %>%
  setDT(.) %>%
  .[keep == 1] %>%
  select(-keep)

fwrite(daily_data, file = "./aux_data/all_cities_daily.csv")

# Set SD filter paramters
level_threshold <- 0.02 # absolute deviation in level of raw mean and trimmed mean
log_threshold <- 0.1 # absolute deviation in level of log(raw mean) and log(trimmed mean)

# Load all data
remove(list = ls())
daily_data <- fread(file = "./aux_data/all_cities_daily.csv")

check <- daily_data %>%
  .[, .(.N), by = .(country, city_state, rank)]

ts_for_plot <- daily_data %>%
  .[, job_date := ymd(job_date)] %>%
  .[as.yearmon(year_month) >= as.yearmon(ymd("20180901")) & as.yearmon(year_month) <= as.yearmon(ymd("20220601"))] %>%
  #.[count_for_thresh >= 150000] %>%
  .[, l1o_monthly_mean := (sum(daily_share*N)-daily_share*N)/(sum(N) - N), by = .(country, city, state, city_state, year_month)] %>%
  .[, monthly_mean := sum(daily_share*N)/(sum(N)), by = .(country, city, state, city_state, year_month)] %>%
  .[, l1o_keep := ifelse(abs(monthly_mean - l1o_monthly_mean) > 0.02 | abs(log(monthly_mean/l1o_monthly_mean)) > 0.10, 0, 1)] %>%
  .[, l1o_keep := ifelse(is.na(l1o_keep), 1, l1o_keep)] %>%
  .[, l1o_daily_with_nas := ifelse(l1o_keep == 1, daily_share, NA)]

check <- ts_for_plot %>%
  .[, .(share_outlier_postings_percent = 100*sum((1-l1o_keep)*N)/sum(N)), by = .(country)] %>%
  select(country, share_outlier_postings_percent)
  
(days_replaced <- 100*round(sum(check$replaced)/sum(check$days), digits = 4))

ts_for_plot <- ts_for_plot %>%
  .[order(city, state, city_state, job_date)] %>%
  .[, monthly_mean_l1o := sum(l1o_daily_with_nas[!is.na(l1o_daily_with_nas)]*N[!is.na(l1o_daily_with_nas)], na.rm = T)/(sum(N[!is.na(l1o_daily_with_nas)], na.rm = T)), by = .(city, state, city_state, year_month)] %>%
  .[, .(N = sum(N),
        job_date = min(job_date),
        drop_days_share = sum(1-l1o_keep)/.N,
        monthly_mean = monthly_mean[1],
        monthly_mean_l1o = monthly_mean_l1o[1]),
    by = .(country, city, state, city_state, year_month)] %>%
  .[, monthly_mean_3ma := rollmean(monthly_mean, k = 3, align = "right", fill = NA), by = .(country, city, state, city_state)] %>%
    .[, monthly_mean_3ma_l1o := rollmean(monthly_mean_l1o, k = 3, align = "right", fill = NA), by = .(country, city, state, city_state)] %>%
  #.[, monthly_mean_l1o := ifelse(as.yearmon(year_month) %in% as.yearmon(ymd(c("20200301", "20200401", "20200501", "20200601"))), monthly_mean, monthly_mean_l1o)] %>%
  #.[, monthly_mean_3ma := ifelse(as.yearmon(year_month) %in% as.yearmon(ymd(c("20200301", "20200401", "20200501", "20200601"))), monthly_mean, monthly_mean_3ma)] %>%
  #.[, monthly_mean_3ma_l1o := ifelse(as.yearmon(year_month) %in% as.yearmon(ymd(c("20200301", "20200401", "20200501", "20200601"))), monthly_mean, monthly_mean_3ma_l1o)] %>%
  .[as.yearmon(year_month) >= as.yearmon(ymd("20190101")) & as.yearmon(year_month) <= as.yearmon(ymd("20220601"))]

ts_for_plot <- ts_for_plot %>%
  group_by(city, state, city_state, year_month, job_date, N) %>%
  pivot_longer(cols = c(drop_days_share:monthly_mean_3ma_l1o)) %>%
  setDT(.)

View(ts_for_plot[city_state == "US"])

View(head(ts_for_plot, 1000))

fwrite(ts_for_plot, file = "./aux_data/city_level_ts.csv")

Sys.time()

