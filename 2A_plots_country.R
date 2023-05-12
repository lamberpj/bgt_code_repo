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

#### LOAD "ALL" ####
# df_nz <- fread("./int_data/df_nz_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "NZ") %>% setDT()
# df_aus <- fread("./int_data/df_aus_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "Australia") %>% setDT()
# df_can <- fread("./int_data/df_can_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "Canada") %>% setDT()
# df_uk_2014 <- fread("./int_data/df_uk_2014_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()
# df_uk_2015 <- fread("./int_data/df_uk_2015_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()
# df_uk_2016 <- fread("./int_data/df_uk_2016_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()
# df_uk_2017 <- fread("./int_data/df_uk_2017_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()
# df_uk_2018 <- fread("./int_data/df_uk_2018_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()
# df_uk_2019 <- fread("./int_data/df_uk_2019_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()
# df_uk_2020 <- fread("./int_data/df_uk_2020_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()
# df_uk_2021 <- fread("./int_data/df_uk_2021_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()
# df_uk_2022 <- fread("./int_data/df_uk_2022_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()
# df_uk_2023 <- fread("./int_data/df_uk_2023_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "UK") %>% setDT()
# df_us_2014 <- fread("./int_data/df_us_2014_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "US") %>% setDT()
# df_us_2015 <- fread("./int_data/df_us_2015_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "US") %>% setDT()
# df_us_2016 <- fread("./int_data/df_us_2016_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "US") %>% setDT()
# df_us_2017 <- fread("./int_data/df_us_2017_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "US") %>% setDT()
# df_us_2018 <- fread("./int_data/df_us_2018_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "US") %>% setDT()
# df_us_2019 <- fread("./int_data/df_us_2019_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "US") %>% setDT()
# df_us_2020 <- fread("./int_data/df_us_2020_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "US") %>% setDT()
# df_us_2021 <- fread("./int_data/df_us_2021_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "US") %>% setDT()
# df_us_2022 <- fread("./int_data/df_us_2022_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "US") %>% setDT()
# df_us_2023 <- fread("./int_data/df_us_2023_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight","job_domain", "tot_emp_ad")) %>% mutate(country = "US") %>% setDT()
# 
# df_uk <- rbindlist(list(df_uk_2014, df_uk_2015, df_uk_2016, df_uk_2017, df_uk_2018,df_uk_2019,df_uk_2020,df_uk_2021,df_uk_2022,df_uk_2023))
# df_us <- rbindlist(list(df_us_2014, df_us_2015, df_us_2016, df_us_2017, df_us_2018,df_us_2019,df_us_2020,df_us_2021,df_us_2022,df_us_2023))
# remove(list = c("df_uk_2014", "df_uk_2015", "df_uk_2016", "df_uk_2017", "df_uk_2018","df_uk_2019","df_uk_2020","df_uk_2021","df_uk_2022","df_uk_2023"))
# remove(list = c("df_us_2014", "df_us_2015", "df_us_2016", "df_us_2017", "df_us_2018","df_us_2019","df_us_2020","df_us_2021","df_us_2022","df_us_2023"))
# 
# df_nz <- df_nz %>% .[, city_state := paste0(city,"_",state)]
# df_aus <- df_aus %>% .[, city_state := paste0(city,"_",state)]
# df_can <- df_can %>% .[, city_state := paste0(city,"_",state)] # No city found?
# df_uk <- df_uk %>% .[, city_state := paste0(city,"_",state)]
# df_us <- df_us %>% .[, city_state := paste0(city,"_",state)]
# 
# sum(df_nz[!is.na(job_domain) & job_domain != ""]$job_id_weight)/sum(df_nz$job_id_weight) # 0.9999867
# sum(df_aus[!is.na(job_domain) & job_domain != ""]$job_id_weight)/sum(df_aus$job_id_weight) # 0.9998609
# sum(df_can[!is.na(job_domain) & job_domain != ""]$job_id_weight)/sum(df_can$job_id_weight) # 0.9979549
# sum(df_uk[!is.na(job_domain) & job_domain != ""]$job_id_weight)/sum(df_uk$job_id_weight) # 0.9998301
# sum(df_us[!is.na(job_domain) & job_domain != ""]$job_id_weight)/sum(df_us$job_id_weight) # 0.9998896
# 
# df_nz <- df_nz[!is.na(job_domain) & job_domain != ""]
# df_aus <- df_aus[!is.na(job_domain) & job_domain != ""]
# df_can <- df_can[!is.na(job_domain) & job_domain != ""]
# df_uk <- df_uk[!is.na(job_domain) & job_domain != ""]
# df_us <- df_us[!is.na(job_domain) & job_domain != ""]
# 
# df_nz <- df_nz %>% .[!grepl("mercadojobs", job_domain)]
# df_can <- df_can %>% .[!grepl("workopolis", job_domain)]
# df_can <- df_can %>% .[!grepl("careerjet", job_domain)]
# df_uk <- df_uk %>% .[!grepl("jobisjob", job_domain)]
# df_us <- df_us %>% .[!grepl("careerbuilder", job_domain)]
# 
# uniqueN(df_nz$job_id) # 1700523
# uniqueN(df_aus$job_id) # 8607160
# uniqueN(df_can$job_id) # 11711357
# uniqueN(df_uk$job_id) # 74576747
# uniqueN(df_us$job_id) # 161872915
# sum(c(uniqueN(df_nz$job_id),uniqueN(df_aus$job_id),uniqueN(df_can$job_id),uniqueN(df_uk$job_id),uniqueN(df_us$job_id)))
# # 258,468,702
# uniqueN(df_nz$employer) # 36201
# uniqueN(df_aus$employer) # 197870
# uniqueN(df_can$employer) # 712577
# uniqueN(df_uk$employer) # 876103
# uniqueN(df_us$employer) # 3485630
# sum(c(uniqueN(df_nz$employer),uniqueN(df_aus$employer),uniqueN(df_can$employer),uniqueN(df_uk$employer),uniqueN(df_us$employer)))
# # 5,308,381
# uniqueN(df_nz$city_state) # 67
# uniqueN(df_aus$city_state) # 59
# uniqueN(df_can$city_state) # 3691
# uniqueN(df_uk$city_state) # 2268
# uniqueN(df_us$city_state) # 31635
# sum(c(uniqueN(df_nz$city_state),uniqueN(df_aus$city_state),uniqueN(df_can$city_state),uniqueN(df_uk$city_state),uniqueN(df_us$city_state)))
# # 37,720
# 
# df_all_list <- list(df_nz,df_aus,df_can,df_uk,df_us)
# remove(list = setdiff(ls(), "df_all_list"))
# #### END ####
# 
# df_all_list <- lapply(df_all_list, function(x) {
#   x <- x %>% select(country, state, wfh_wham, job_date, bgt_occ, month, job_id_weight, tot_emp_ad)
#   return(x)
# })
# 
# 
# df_all_list <- lapply(df_all_list, function(x) {
#   x %>% setDT(.) %>% .[as.yearmon(job_date) <= as.yearmon(ymd("20230301"))]
# })
# 
# save(df_all_list, file = "./aux_data/df_all_list.RData")
load(file = "./aux_data/df_all_list.RData")

#### END ####


#### PREPARE UNWEIGHTED DATA ####

# Make Unweighted daily shares
wfh_daily_share_list <- lapply(df_all_list, function(x) {
  x <- x %>% select(country, wfh_wham, job_date, bgt_occ, month, job_id_weight)
  x <- x[!is.na(wfh_wham)]
  x <- x[!is.na(bgt_occ)]
  x <- x[, month := factor(month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))]
  x <- x[, job_ymd := ymd(job_date)]
  x <- x[, year_month := as.yearmon(job_ymd)]
  x <- x[, year := year(job_ymd)]
  x <- setDT(x)
  x <- x %>%
    select(country, job_date, year_month, year, wfh_wham, job_id_weight) %>%
    setDT(.) %>%
    .[, .(wfh_sum = sum(job_id_weight*wfh_wham, na.rm = T), job_ads_sum = sum(job_id_weight, na.rm = T)), by = .(country, job_date, year_month, year)] %>%
    .[, wfh_share := wfh_sum / job_ads_sum] %>%
    setDT(.)
  return(x)
})

wfh_daily_share <- rbindlist(wfh_daily_share_list)
rm("wfh_daily_share_list")
table(wfh_daily_share$country)
wfh_daily_share <- wfh_daily_share %>% rename(daily_share = wfh_share, N = job_ads_sum)
saveRDS(wfh_daily_share, file = "./int_data/country_daily_wfh.rds")

# Filter Unweighted daily shares
wfh_daily_share <- readRDS(file = "./int_data/country_daily_wfh.rds")
table(wfh_daily_share$country)
ts_for_plot <- wfh_daily_share %>%
  .[, job_date := ymd(job_date)] %>%
  .[as.yearmon(year_month) <= as.yearmon(ymd("20230301"))] %>%
  .[, l1o_monthly_mean := (sum(daily_share*N)-daily_share*N)/(sum(N) - N), by = .(country, year_month)] %>%
  .[, monthly_mean := sum(daily_share*N)/(sum(N)), by = .(country, year_month)] %>%
  .[, l1o_keep := ifelse(abs(monthly_mean - l1o_monthly_mean) > 0.02 | abs(log(monthly_mean/l1o_monthly_mean)) > 0.10, 0, 1)] %>%
  .[, l1o_keep := ifelse(as.yearmon(year_month) %in% as.yearmon(ymd(c("20200301", "20200401", "20200501", "20200601"))), 1, l1o_keep)] %>%
  .[, l1o_daily_with_nas := ifelse(l1o_keep == 1, daily_share, NA)]

table(ts_for_plot$country)

dropped_days <- ts_for_plot %>%
  .[l1o_keep == 0] %>%
  select(country, job_date) %>%
  .[, drop := 1]

check <- ts_for_plot %>%
  .[, .(days = .N, replaced = sum(1-l1o_keep)), by = .(country, year_month)] %>%
  .[, share_replaced := replaced/days]
(days_replaced <- 100*round(sum(check$replaced)/sum(check$days), digits = 4))

ts_for_plot <- ts_for_plot %>%
  .[order(country, job_date)] %>%
  .[, daily_share_l10 := na.approx(l1o_daily_with_nas, rule = 2), by = .(country)] %>%
  .[, monthly_mean_l1o := sum(daily_share_l10[!is.na(daily_share_l10)]*N[!is.na(daily_share_l10)], na.rm = T)/(sum(N[!is.na(daily_share_l10)], na.rm = T)),
    by = .(country, year_month)] %>%
  .[, .(job_date = min(job_date),
        monthly_mean = monthly_mean[1],
        monthly_mean_l1o = monthly_mean_l1o[1]),
    by = .(country, year_month)] %>%
  .[, monthly_mean_3ma := rollmean(monthly_mean, k = 3, align = "right", fill = NA), by = .(country)] %>%
  .[, monthly_mean_3ma_l1o := rollmean(monthly_mean_l1o, k = 3, align = "right", fill = NA), by = .(country)] %>%
  .[, monthly_mean_l1o := ifelse(as.yearmon(year_month) %in% as.yearmon(ymd(c("20200301", "20200401", "20200501", "20200601"))), monthly_mean, monthly_mean_l1o)] %>%
  .[, monthly_mean_3ma := ifelse(as.yearmon(year_month) %in% as.yearmon(ymd(c("20200301", "20200401", "20200501", "20200601"))), monthly_mean, monthly_mean_3ma)] %>%
  .[, monthly_mean_3ma_l1o := ifelse(as.yearmon(year_month) %in% as.yearmon(ymd(c("20200301", "20200401", "20200501", "20200601"))), monthly_mean, monthly_mean_3ma_l1o)]

ts_for_plot <- ts_for_plot %>%
  group_by(country, year_month, job_date) %>%
  pivot_longer(cols = c(monthly_mean:monthly_mean_3ma_l1o)) %>%
  setDT(.)

table(ts_for_plot$country)

saveRDS(dropped_days, file = "./int_data/dropped_days_from_jkf.rds")
saveRDS(ts_for_plot, file = "./int_data/wfh_across_countries_monthly_jkf_raw.rds")

#### END ####

#### MAKE MONTHLY x COUNTRY SERIES, 2019 US VACANCY WEIGHTS ####
remove(list = setdiff(ls(), "df_all_list"))
dropped_days <- readRDS(file = "./int_data/dropped_days_from_jkf.rds")

# Make Month x BGT Occ 5 digits x Country series (dropping days which are filtered by the Jackknife filter)
wfh_monthly_bgt_occ_share_list <- lapply(df_all_list, function(x) {
  x <- x %>% select(country, wfh_wham, job_date, bgt_occ, month, job_id_weight)
  x <- x[!is.na(wfh_wham)]
  x <- x[!is.na(bgt_occ)]
  x <- x[, bgt_occ5 := str_sub(bgt_occ, 1, 6)]
  x <- x[, month := factor(month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))]
  x <- x[, job_ymd := ymd(job_date)]
  x <- x[, year_month := as.yearmon(job_ymd)]
  x <- x[, year := year(job_ymd)]
  x <- setDT(x)
  nrow(x)
  x <- merge(x = x, y = dropped_days, all.x = TRUE, all.y = FALSE, by = c("country", "job_date"))
  x <- x[is.na(drop)]
  nrow(x)
  x <- x %>%
    select(country, bgt_occ5, year_month, year, wfh_wham, job_id_weight) %>%
    setDT(.) %>%
    .[, .(wfh_sum = sum(job_id_weight*wfh_wham, na.rm = T), job_ads_sum = sum(job_id_weight, na.rm = T)),
      by = .(country, bgt_occ5, year_month, year)] %>%
    .[, wfh_share := wfh_sum / job_ads_sum] %>%
    setDT(.)
  return(x)
})

wfh_monthly_bgt_occ_share <- rbindlist(wfh_monthly_bgt_occ_share_list)
rm("wfh_monthly_bgt_occ_share_list")
wfh_monthly_bgt_occ_share <- wfh_monthly_bgt_occ_share %>%
  rename(monthly_share = wfh_share, N = job_ads_sum)

# Make BGT Occ 5 weights, based on 2019 US vacancy shares
head(wfh_monthly_bgt_occ_share)
shares_df <- wfh_monthly_bgt_occ_share %>%
  .[bgt_occ5 != "" & !is.na(bgt_occ5)] %>%
  .[year == 2019 & country == "US"] %>%
  .[, .(N = sum(N, na.rm = T)), by = .(bgt_occ5)] %>%
  .[, share := N/sum(N, na.rm = T)] %>%
  select(bgt_occ5, share)

# Aggregate Monthly x BGT Occ 5 x Country series, using occ weights bsased on 2019 US vacancy shares
wfh_monthly_bgt_occ_share <- wfh_monthly_bgt_occ_share %>%
  .[bgt_occ5 != "" & !is.na(bgt_occ5)] %>%
  merge(x = ., y = shares_df, by = "bgt_occ5", all.x = TRUE, all.y = FALSE) %>%
  setDT(.)
wfh_monthly_bgt_occ_share
wfh_monthly_bgt_occ_share <- wfh_monthly_bgt_occ_share %>%
  .[, .(monthly_mean = sum(monthly_share*(share/sum(share, na.rm = T)), na.rm = T)), by = .(country, year_month)]

ts_for_plot_us_vac_weight <- wfh_monthly_bgt_occ_share
saveRDS(ts_for_plot_us_vac_weight, file = "./int_data/wfh_across_countries_monthly_jkf_2019_us_vac_w.rds")

#### END ####

#### MAKE MONTHLY x COUNTRY SERIES, 2019 EMPLOYMENT WEIGHTS ####
remove(list = setdiff(ls(), "df_all_list"))
dropped_days <- readRDS(file = "./int_data/dropped_days_from_jkf.rds")
dropped_days
# Make Month x BGT Occ 5 digits x Country series (dropping days which are filtered by the Jackknife filter)
wfh_monthly_bgt_occ_share_list <- lapply(df_all_list, function(x) {
  x <- x %>% select(country, wfh_wham, job_date, bgt_occ, month, job_id_weight, tot_emp_ad)
  x <- x[!is.na(wfh_wham)]
  x <- x[!is.na(bgt_occ)]
  x <- x[, bgt_occ5 := str_sub(bgt_occ, 1, 6)]
  x <- x[, month := factor(month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))]
  x <- x[, job_ymd := ymd(job_date)]
  x <- x[, year_month := as.yearmon(job_ymd)]
  x <- x[, year := year(job_ymd)]
  x <- setDT(x)
  nrow(x)
  x <- merge(x = x, y = dropped_days, all.x = TRUE, all.y = FALSE, by = c("country", "job_date"))
  x <- x[is.na(drop)]
  nrow(x)
  x <- x %>%
    select(country, bgt_occ5, year_month, year, wfh_wham, job_id_weight, tot_emp_ad) %>%
    setDT(.) %>%
    .[, .(wfh_sum = sum(job_id_weight*wfh_wham*tot_emp_ad, na.rm = T), job_ads_sum = sum(job_id_weight*tot_emp_ad, na.rm = T)),
      by = .(country, bgt_occ5, year_month, year)] %>%
    .[, wfh_share := wfh_sum / job_ads_sum] %>%
    setDT(.)
  return(x)
})

wfh_monthly_bgt_occ_share <- rbindlist(wfh_monthly_bgt_occ_share_list)
rm("wfh_monthly_bgt_occ_share_list")
wfh_monthly_bgt_occ_share <- wfh_monthly_bgt_occ_share %>%
  rename(monthly_share = wfh_share, N = job_ads_sum)

# Make BGT Occ 5 weights, based on 2019 US employment shares
head(wfh_monthly_bgt_occ_share)
shares_df <- wfh_monthly_bgt_occ_share %>%
  .[bgt_occ5 != "" & !is.na(bgt_occ5)] %>%
  .[year == 2019] %>%
  .[, .(N = sum(N, na.rm = T)), by = .(bgt_occ5, country)] %>%
  .[, share := N/sum(N, na.rm = T)] %>%
  select(bgt_occ5, share, country)

# Aggregate Monthly x BGT Occ 5 x Country series, using occ weights bsased on 2019 US vacancy shares
shares_df <- as.data.frame(shares_df) %>% as.data.table(.)
wfh_monthly_bgt_occ_share <- as.data.frame(wfh_monthly_bgt_occ_share) %>% as.data.table(.)

wfh_monthly_bgt_occ_share <- wfh_monthly_bgt_occ_share %>%
  .[bgt_occ5 != "" & !is.na(bgt_occ5)] %>%
  left_join(shares_df) %>%
  setDT(.)
wfh_monthly_bgt_occ_share
wfh_monthly_bgt_occ_share <- wfh_monthly_bgt_occ_share %>%
  .[, .(monthly_mean = sum(monthly_share*(share/sum(share, na.rm = T)), na.rm = T)), by = .(country, year_month)]

ts_for_plot_glob_emp_weight <- wfh_monthly_bgt_occ_share
saveRDS(ts_for_plot_glob_emp_weight, file = "./int_data/wfh_across_countries_monthly_jkf_2019_global_emp_w.rds")

#### END ####


#### MAKE MONTHLY x COUNTRY SERIES, 2019 US EMPLOYMENT WEIGHTS ####
remove(list = setdiff(ls(), "df_all_list"))
dropped_days <- readRDS(file = "./int_data/dropped_days_from_jkf.rds")

# Make Month x BGT Occ 5 digits x Country series (dropping days which are filtered by the Jackknife filter)
wfh_monthly_bgt_occ_share_list <- lapply(df_all_list, function(x) {
  x <- x %>% select(country, wfh_wham, job_date, bgt_occ, month, job_id_weight, tot_emp_ad)
  x <- x[!is.na(wfh_wham)]
  x <- x[!is.na(bgt_occ)]
  x <- x[, bgt_occ5 := str_sub(bgt_occ, 1, 6)]
  x <- x[, month := factor(month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))]
  x <- x[, job_ymd := ymd(job_date)]
  x <- x[, year_month := as.yearmon(job_ymd)]
  x <- x[, year := year(job_ymd)]
  x <- setDT(x)
  nrow(x)
  x <- merge(x = x, y = dropped_days, all.x = TRUE, all.y = FALSE, by = c("country", "job_date"))
  x <- x[is.na(drop)]
  nrow(x)
  x <- x %>%
    select(country, bgt_occ5, year_month, year, wfh_wham, job_id_weight, tot_emp_ad) %>%
    setDT(.) %>%
    .[, .(wfh_sum = sum(job_id_weight*wfh_wham*tot_emp_ad, na.rm = T), job_ads_sum = sum(job_id_weight*tot_emp_ad, na.rm = T)),
      by = .(country, bgt_occ5, year_month, year)] %>%
    .[, wfh_share := wfh_sum / job_ads_sum] %>%
    setDT(.)
  return(x)
})

wfh_monthly_bgt_occ_share <- rbindlist(wfh_monthly_bgt_occ_share_list)
rm("wfh_monthly_bgt_occ_share_list")
wfh_monthly_bgt_occ_share <- wfh_monthly_bgt_occ_share %>%
  rename(monthly_share = wfh_share, N = job_ads_sum)

# Make BGT Occ 5 weights, based on 2019 US vacancy shares
head(wfh_monthly_bgt_occ_share)
shares_df <- wfh_monthly_bgt_occ_share %>%
  .[bgt_occ5 != "" & !is.na(bgt_occ5) & country == "US"] %>%
  .[year == 2019] %>%
  .[, .(N = sum(N, na.rm = T)), by = .(bgt_occ5)] %>%
  .[, share := N/sum(N, na.rm = T)] %>%
  select(bgt_occ5, share)

# Aggregate Monthly x BGT Occ 5 x Country series, using occ weights bsased on 2019 US vacancy shares
shares_df <- as.data.frame(shares_df) %>% as.data.table(.)
wfh_monthly_bgt_occ_share <- as.data.frame(wfh_monthly_bgt_occ_share) %>% as.data.table(.)

wfh_monthly_bgt_occ_share <- wfh_monthly_bgt_occ_share %>%
  .[bgt_occ5 != "" & !is.na(bgt_occ5)] %>%
  left_join(shares_df) %>%
  setDT(.)
wfh_monthly_bgt_occ_share
wfh_monthly_bgt_occ_share <- wfh_monthly_bgt_occ_share %>%
  .[, .(monthly_mean = sum(monthly_share*(share/sum(share, na.rm = T)), na.rm = T)), by = .(country, year_month)]

ts_for_plot_us_emp_weight <- wfh_monthly_bgt_occ_share
saveRDS(ts_for_plot_us_emp_weight, file = "./int_data/wfh_across_countries_monthly_jkf_2019_us_emp_w.rds")

#### END ####

#### COMBINE SERIES ####

# Combine different weighted series
remove(list = setdiff(ls(), "df_all_list"))
unweighted <- readRDS(file = "./int_data/wfh_across_countries_monthly_jkf_raw.rds")
us2019_vac_weighted <- readRDS(file = "./int_data/wfh_across_countries_monthly_jkf_2019_us_vac_w.rds")
glob2019_emp_weighted <- readRDS(file = "./int_data/wfh_across_countries_monthly_jkf_2019_global_emp_w.rds")
us2019_emp_weighted <- readRDS(file = "./int_data/wfh_across_countries_monthly_jkf_2019_us_emp_w.rds")

unweighted <- unweighted %>%
  filter(name == "monthly_mean_l1o") %>%
  mutate(weight = "Unweighted") %>%
  select(-name) %>%
  select(-job_date)

us2019_vac_weighted <- us2019_vac_weighted %>% rename(value = monthly_mean) %>% mutate(weight = "US 2019 Vacancy")
glob2019_emp_weighted <- glob2019_emp_weighted %>% rename(value = monthly_mean) %>% mutate(weight = "Global 2019 Employment")
us2019_emp_weighted <- us2019_emp_weighted %>% rename(value = monthly_mean) %>% mutate(weight = "US 2019 Employment")

ts_for_plot <- unweighted %>%
  bind_rows(us2019_vac_weighted) %>%
  bind_rows(glob2019_emp_weighted) %>%
  bind_rows(us2019_emp_weighted)

ts_for_plot <- ts_for_plot %>%
  group_by(country, weight) %>%
  arrange(year_month) %>%
  mutate(value_3ma = (value + lag(value, n = 1) + lag(value, n = 2))/3)

remove(list = setdiff(ls(), c("df_all_list", "ts_for_plot")))

saveRDS(ts_for_plot, file = "./int_data/country_ts_combined_weights.rds")

#### END ####

#### PLOTS ####
remove(list = setdiff(ls(), "df_all_list"))
ts_for_plot <- readRDS(file = "./int_data/country_ts_combined_weights.rds")

cbbPalette <- c("#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00")
(country_list <- unique(ts_for_plot$country))

head(ts_for_plot)

# Unweighted
p = ts_for_plot %>%
  filter(weight == "Unweighted") %>%
  filter(year_month <= as.yearmon(ymd("20230301"))) %>%
  mutate(Date = as.Date(as.yearmon(year_month)),
         Percent = 100*value,
         Country = country) %>%
  ggplot(., aes(x = Date, y = Percent, colour = Country,
                group=Country,
                text = paste0(Country,"</br></br><b>Percent:</b> ", round(Percent,1),"</br>Date: ",format(Date, "%b %y")))) +
  geom_point(size = 2.25) +
  geom_line(linewidth = 0.5) +
  ylab("Percent") +
  xlab("Date") +
  scale_x_date(breaks = as.Date(c("2014-01-01", "2015-01-01", "2016-01-01",
                                  "2017-01-01", "2018-01-01", "2019-01-01",
                                  "2020-01-01", "2021-01-01", "2022-01-01",
                                  "2023-01-01")),
               date_labels = '%Y') +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,2)) +
  coord_cartesian(ylim = c(0, 22)) +
  scale_colour_manual(values=cbbPalette) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(nrow = 1, reverse = T)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=15, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=15, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white")) +
  theme(aspect.ratio=3/5)
p
save(p, file = "./ppt/ggplots/rwa_country_ts_w_unweighted_month.RData")
remove(p)

# Occ Weighted to US
cbbPalette <- c("#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00")
p = ts_for_plot %>%
  filter(weight == "US 2019 Vacancy") %>%
  filter(year_month <= as.yearmon(ymd("20230301"))) %>%
  mutate(Date = as.Date(as.yearmon(year_month)),
         Percent = 100*value,
         Country = country) %>%
  ggplot(., aes(x = Date, y = Percent, colour = Country,
                group=Country,
                text = paste0(Country,"</br></br><b>Percent:</b> ", round(Percent,1),"</br>Date: ",format(Date, "%b %y")))) +
  geom_point(size = 2.25) +
  geom_line(linewidth = 0.5) +
  ylab("Percent") +
  xlab("Date") +
  scale_x_date(breaks = as.Date(c("2014-01-01", "2015-01-01", "2016-01-01",
                                  "2017-01-01", "2018-01-01", "2019-01-01",
                                  "2020-01-01", "2021-01-01", "2022-01-01",
                                  "2023-01-01")),
               date_labels = '%Y') +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,2)) +
  coord_cartesian(ylim = c(0, 18)) +
  scale_colour_manual(values=cbbPalette) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(nrow = 1, reverse = T)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=15, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=15, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white")) +
  theme(aspect.ratio=3/5)
p
save(p, file = "./ppt/ggplots/rwa_country_ts_w_us_vac_month.RData")
remove(p)

# Global Employment Weighted
cbbPalette <- c("#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00")
p = ts_for_plot %>%
  filter(weight == "Global 2019 Employment") %>%
  filter(year_month <= as.yearmon(ymd("20230301"))) %>%
  mutate(Date = as.Date(as.yearmon(year_month)),
         Percent = 100*value,
         Country = country) %>%
  ggplot(., aes(x = Date, y = Percent, colour = Country,
                group=Country,
                text = paste0(Country,"</br></br><b>Percent:</b> ", round(Percent,1),"</br>Date: ",format(Date, "%b %y")))) +
  geom_point(size = 2.25) +
  geom_line(linewidth = 0.5) +
  ylab("Percent") +
  xlab("Date") +
  scale_x_date(breaks = as.Date(c("2014-01-01", "2015-01-01", "2016-01-01",
                                  "2017-01-01", "2018-01-01", "2019-01-01",
                                  "2020-01-01", "2021-01-01", "2022-01-01",
                                  "2023-01-01")),
               date_labels = '%Y') +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,2)) +
  coord_cartesian(ylim = c(0, 16)) +
  scale_colour_manual(values=cbbPalette) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(nrow = 1, reverse = T)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=15, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=15, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white")) +
  theme(aspect.ratio=3/5)
p
save(p, file = "./ppt/ggplots/rwa_country_ts_w_glob_emp_month.RData")
remove(p)

# US 2019 Employment Weighted
cbbPalette <- c("#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00")
p = ts_for_plot %>%
  filter(weight == "US 2019 Employment") %>%
  mutate(Date = as.Date(as.yearmon(year_month)),
         Percent = 100*value,
         Country = country) %>%
  ggplot(., aes(x = Date, y = Percent, colour = Country,
                group=Country,
                text = paste0(Country,"</br></br><b>Percent:</b> ", round(Percent,1),"</br>Date: ",format(Date, "%b %y")))) +
  geom_point(size = 2.25) +
  geom_line(linewidth = 0.5) +
  ylab("Percent") +
  xlab("Date") +
  scale_x_date(breaks = as.Date(c("2014-01-01", "2015-01-01", "2016-01-01",
                                  "2017-01-01", "2018-01-01", "2019-01-01",
                                  "2020-01-01", "2021-01-01", "2022-01-01",
                                  "2023-01-01")),
               date_labels = '%Y') +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,2)) +
  coord_cartesian(ylim = c(0, 14)) +
  scale_colour_manual(values=cbbPalette) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(nrow = 1, reverse = T)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=15, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=15, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white")) +
  theme(aspect.ratio=3/5)
p
save(p, file = "./ppt/ggplots/rwa_country_ts_w_us_emp_month.RData")
remove(p)

#### END ####
