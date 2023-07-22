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
df_nz <- fread("./int_data/df_nz_standardised.csv", nThread = 8, integer64 = "numeric")
df_aus <- fread("./int_data/df_aus_standardised.csv", nThread = 8, integer64 = "numeric")
df_can <- fread("./int_data/df_can_standardised.csv", nThread = 8, integer64 = "numeric")
df_uk <- fread("./int_data/df_uk_standardised.csv", nThread = 8, integer64 = "numeric")
df_us_2014 <- fread("./int_data/df_us_2014_standardised.csv", nThread = 8, integer64 = "numeric")
df_us_2015 <- fread("./int_data/df_us_2015_standardised.csv", nThread = 8, integer64 = "numeric")
df_us_2016 <- fread("./int_data/df_us_2016_standardised.csv", nThread = 8, integer64 = "numeric")
df_us_2017 <- fread("./int_data/df_us_2017_standardised.csv", nThread = 8, integer64 = "numeric")
df_us_2018 <- fread("./int_data/df_us_2018_standardised.csv", nThread = 8, integer64 = "numeric")
df_us_2019 <- fread("./int_data/df_us_2019_standardised.csv", nThread = 8, integer64 = "numeric")
df_us_2020 <- fread("./int_data/df_us_2020_standardised.csv", nThread = 8, integer64 = "numeric")
df_us_2021 <- fread("./int_data/df_us_2021_standardised.csv", nThread = 8, integer64 = "numeric")
df_us_2022 <- fread("./int_data/df_us_2022_standardised.csv", nThread = 8, integer64 = "numeric")

df_us <- rbindlist(list(df_us_2014, df_us_2015, df_us_2016, df_us_2017, df_us_2018,df_us_2019,df_us_2020,df_us_2021,df_us_2022))
remove(list = c("df_us_2014", "df_us_2015", "df_us_2016", "df_us_2017", "df_us_2018","df_us_2019","df_us_2020","df_us_2021","df_us_2022"))

uniqueN(df_nz$job_id) # 1297812
uniqueN(df_aus$job_id) # 4936765
uniqueN(df_can$job_id) # 7540054
uniqueN(df_uk$job_id) # 36914129
uniqueN(df_us$job_id) # 148224554

uniqueN(df_nz$employer) # 27,712
uniqueN(df_aus$employer) # 131,995
uniqueN(df_can$employer) # 586,165
uniqueN(df_uk$employer) # 619,641
uniqueN(df_us$employer) # 3,262,838

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

colnames(df_all_list[[1]])

daily_data <- lapply(1:length(df_all_list), function(i) {
    df_all_list[[i]] %>%
      .[, .(daily_share = mean(wfh_wham, na.rm = T), N = uniqueN(job_id)), by = .(country, state, city, year_month, job_date)] %>%
      .[, job_date := ymd(job_date)] %>%
      .[city != ""] %>%
      .[as.yearmon(year_month) >= as.yearmon(ymd("20170101")) & as.yearmon(year_month) <= as.yearmon(ymd("20220601"))] %>%
      .[, city_state := paste0(city, ", ", state)]
  }) %>%
  rbindlist(.)

thresh_cities <- daily_data %>%
  .[as.yearmon(year_month) >= as.yearmon(ymd("20190101")) & as.yearmon(year_month) <= as.yearmon(ymd("20220601"))] %>%
  .[, .(count_for_thresh = sum(N)), by = .(country, city_state)] %>%
  .[order(country, desc(count_for_thresh))] %>%
  .[, rank := 1:.N, by = country] %>%
  .[, keep := ifelse(country == "US" & rank <= 400, 1, 0)] %>%
  .[, keep := ifelse(country == "UK" & rank <= 30, 1, keep)] %>%
  .[, keep := ifelse(country == "Canada" & rank <= 10, 1, keep)] %>%
  .[, keep := ifelse(country == "NZ" & rank <= 3, 1, keep)] %>%
  .[, keep := ifelse(country == "Australia" & rank <= 5, 1, keep)]

daily_data <- daily_data %>%
  left_join(thresh_cities) %>%
  setDT(.) %>%
  .[keep == 1] %>%
  select(-keep)

fwrite(daily_data, file = "./aux_data/all_cities_daily.csv")

check <- daily_data %>%
  .[, .(.N), by = .(country, city_state, rank)]

View(check)

