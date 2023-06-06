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
library("tidyquant")

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
library('officer')
library('rvg')
library('here')

library("plotly")
library("ggplot2")
library("dplyr")
library("car")
library("babynames")
library("gapminder")

library("highcharter")
# Set highcharter options
options(highcharter.theme = hc_theme_smpl(tooltip = list(valueDecimals = 2)))

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

##################################################
# CATEGORY A
##################################################

#### 1 REMOTE WORK ACROSS COUNTRIES ####
remove(list = ls())

# Create country-level data
wfh_across_country <- readRDS(file = "./int_data/country_ts_combined_weights.rds") %>%
  ungroup() %>%
  filter(weight == "US 2019 Vacancy") %>%
  filter(year_month >= as.yearmon(ymd("20190101"))) %>%
  filter(!(year_month == as.yearmon(ymd("20230301")) & country %in% c("Australia", "Canada", "NZ"))) %>%
  arrange(desc(country), year_month) %>%
  select(country, year_month, value) %>%
  rename(Percent = value, Country = country)

wfh_across_country <- wfh_across_country %>% mutate(Year = as.character(year(year_month)))
wfh_across_country <- wfh_across_country %>% mutate(Month = as.character(month(year_month, label = TRUE)))
wfh_across_country <- wfh_across_country %>% mutate(Percent = round(100*Percent, 2))
wfh_across_country <- wfh_across_country %>% mutate("Year-Month" = year_month)

wfh_across_country <- wfh_across_country %>% select(Year, Month, "Year-Month", Country, Percent)

fwrite(wfh_across_country, file = "./public_data/country_by_month_wfh_share.csv")

wfh_across_country
wfh_across_country_wide <- wfh_across_country %>%
  select(`Year-Month`, Country, Percent) %>%
  mutate(`Year-Month` = as.Date(`Year-Month`)) %>%
  group_by(`Year-Month`) %>%
  pivot_wider(values_from = Percent, names_from = Country)

fwrite(wfh_across_country_wide, file = "./public_data/country_by_month_wfh_share_wide.csv")


#### END ####

#### 2 SOC2 USA ####
remove(list = ls())
setwd("/mnt/disks/pdisk/bg-us/")

#colnames(fread("./int_data/us_stru_2018_wfh.csv", nrow = 100))

df_us_2019 <- fread("./int_data/us_stru_2019_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "soc", "wfh_wham", "job_domain")) %>% mutate(country = "US") %>% setDT()
df_us_2020 <- fread("./int_data/us_stru_2020_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "soc", "wfh_wham", "job_domain")) %>% mutate(country = "US") %>% setDT()
df_us_2021 <- fread("./int_data/us_stru_2021_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "soc", "wfh_wham", "job_domain")) %>% mutate(country = "US") %>% setDT()
df_us_2022 <- fread("./int_data/us_stru_2022_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "soc", "wfh_wham", "job_domain")) %>% mutate(country = "US") %>% setDT()
df_us_2023 <- fread("./int_data/us_stru_2023_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "soc", "wfh_wham", "job_domain")) %>% mutate(country = "US") %>% setDT()

df_us <- rbindlist(list(df_us_2019,df_us_2020,df_us_2021,df_us_2022,df_us_2023))
remove(list = c("df_us_2019","df_us_2020","df_us_2021","df_us_2022","df_us_2023"))

df_us <- df_us[!is.na(job_domain) & job_domain != ""]
df_us <- df_us %>% .[!grepl("careerbuilder", job_domain)]
df_us <- df_us %>% .[!is.na(soc) & soc != ""]
df_us <- df_us %>% .[!is.na(wfh_wham) & wfh_wham != ""]
df_us <- df_us %>% .[, soc_2_digit := str_sub(soc, 1, 2)]
df_us <- df_us %>% .[, year_month := as.yearmon(job_date)]
df_us <- df_us %>% .[year_month <= as.yearmon(ymd("20230401"))] # Up to March 2023
df_us <- df_us %>% .[year_month >= as.yearmon(ymd("20190101"))] # From Jan 2018
df_us_soc_year_month <- df_us %>% .[, .(percent = mean(wfh_wham), N = .N), by = .(soc_2_digit, year_month)]
df_us_soc_year_month$soc_2_digit <- as.numeric(df_us_soc_year_month$soc_2_digit)

soc2_names <- fread("./aux_data/bls_soc_2010_2_digit_names.csv", header = FALSE)
colnames(soc2_names) <- c("soc_2_digit", "name")
soc2_names <- soc2_names %>% .[, soc_2_digit := str_sub(soc_2_digit, 1, 2)]
soc2_names$soc_2_digit <- as.numeric(soc2_names$soc_2_digit)

df_us_soc_year_month <- df_us_soc_year_month %>% left_join(soc2_names)

df_us_soc_year_month <- df_us_soc_year_month %>% select(soc_2_digit, year_month, name, percent, N)
df_us_soc_year_month <- df_us_soc_year_month %>% arrange(soc_2_digit, year_month)

df_us_soc_year_month <- df_us_soc_year_month %>% mutate(year = as.character(year(year_month)))
df_us_soc_year_month <- df_us_soc_year_month %>% mutate(month = as.character(month(year_month, label = TRUE)))
df_us_soc_year_month <- df_us_soc_year_month %>% mutate(percent = round(100*percent, 2))
df_us_soc_year_month <- df_us_soc_year_month %>% mutate("year_month" = year_month)

df_us_soc_year_month <- df_us_soc_year_month %>% select(year, month, "year_month", soc_2_digit, name, percent, N)
df_us_soc_year_month <- df_us_soc_year_month %>% rename(n = N,
                                                        "us_soc_major_group_2digits" = soc_2_digit,
                                                        "us_soc_major_group_name" = name)

tail(df_us_soc_year_month)

df_us_soc_year_month <- df_us_soc_year_month %>% .[, measurement := "1 Month Average"]

setwd("/mnt/disks/pdisk/bg_combined/")
fwrite(df_us_soc_year_month, file = "./public_data/us_soc2digit_by_month_wfh_share.csv")
#### END ####

#### DROPPED SOC2 UK ####
# remove(list = ls())
# setwd("/mnt/disks/pdisk/bg-uk/")
# 
# df_uk_2019 <- fread("./int_data/uk_stru_2019_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "uksoc_code", "uksoc_sub_major_group", "wfh_wham", "job_domain")) %>% mutate(country = "UK") %>% setDT()
# df_uk_2020 <- fread("./int_data/uk_stru_2020_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "uksoc_code", "uksoc_sub_major_group", "wfh_wham", "job_domain")) %>% mutate(country = "UK") %>% setDT()
# df_uk_2021 <- fread("./int_data/uk_stru_2021_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "uksoc_code", "uksoc_sub_major_group", "wfh_wham", "job_domain")) %>% mutate(country = "UK") %>% setDT()
# df_uk_2022 <- fread("./int_data/uk_stru_2022_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "uksoc_code", "uksoc_sub_major_group", "wfh_wham", "job_domain")) %>% mutate(country = "UK") %>% setDT()
# df_uk_2023 <- fread("./int_data/uk_stru_2023_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "uksoc_code", "uksoc_sub_major_group", "wfh_wham", "job_domain")) %>% mutate(country = "UK") %>% setDT()
# 
# df_uk <- rbindlist(list(df_uk_2019,df_uk_2020,df_uk_2021,df_uk_2022,df_uk_2023))
# remove(list = c("df_uk_2019","df_uk_2020","df_uk_2021","df_uk_2022","df_uk_2023"))
# 
# df_uk <- df_uk[!is.na(job_domain) & job_domain != ""]
# df_uk <- df_uk %>% .[!grepl("jobisjob", job_domain)]
# df_uk <- df_uk %>% .[!is.na(uksoc_code) & uksoc_code != ""]
# df_uk <- df_uk %>% .[!is.na(wfh_wham) & wfh_wham != ""]
# df_uk <- df_uk %>% .[, soc_2_digit := str_sub(uksoc_code, 1, 2)]
# df_uk <- df_uk %>% .[uksoc_sub_major_group != ""]
# unique(df_uk$soc_2_digit)
# unique(df_uk$uksoc_sub_major_group)
# 
# df_uk <- df_uk %>% .[, year_month := as.yearmon(job_date)]
# df_uk <- df_uk %>% .[year_month <= as.yearmon(ymd("20230401"))] # Up to Jan 2023
# df_uk <- df_uk %>% .[year_month >= as.yearmon(ymd("20190101"))] # From Jan 2018
# df_uk_soc_year_month <- df_uk %>% .[, .(percent = mean(wfh_wham), N = .N), by = .(soc_2_digit, year_month, uksoc_sub_major_group)]
# df_uk_soc_year_month$soc_2_digit <- as.numeric(df_uk_soc_year_month$soc_2_digit)
# df_uk_soc_year_month$uksoc_sub_major_group <- str_to_title(df_uk_soc_year_month$uksoc_sub_major_group)
# 
# df_uk_soc_year_month <- df_uk_soc_year_month %>% select(soc_2_digit, year_month, uksoc_sub_major_group, percent)
# df_uk_soc_year_month <- df_uk_soc_year_month %>% arrange(soc_2_digit, year_month)
# 
# df_uk_soc_year_month <- df_uk_soc_year_month %>% mutate(Year = as.character(year(year_month)))
# df_uk_soc_year_month <- df_uk_soc_year_month %>% mutate(Month = as.character(month(year_month, label = TRUE)))
# df_uk_soc_year_month <- df_uk_soc_year_month %>% mutate(Percent = round(100*percent, 2))
# df_uk_soc_year_month <- df_uk_soc_year_month %>% mutate("Year-Month" = year_month)
# 
# df_uk_soc_year_month <- df_uk_soc_year_month %>% select(Year, Month, "Year-Month", soc_2_digit, uksoc_sub_major_group, Percent)
# df_uk_soc_year_month <- df_uk_soc_year_month %>% rename("UK SOC Major Group (2-digit)" = soc_2_digit,
#                                                         "UK SOC Major Group (name)" = uksoc_sub_major_group)
# 
# setwd("/mnt/disks/pdisk/bg_combined/")
# fwrite(df_uk_soc_year_month, file = "./public_data/uk_soc2digit_by_month_wfh_share.csv")

#### END ####

##################################################
# CATEGORY B
##################################################

#### 4. US City-level results ####
remove(list = ls())
df_us_city_year_month <- fread(file = "./aux_data/city_level_ts.csv")

table(df_us_city_year_month$name)
df_us_city_year_month <- df_us_city_year_month %>% .[country == "US"] %>% .[name %in% c("monthly_mean_l1o", "monthly_mean_3ma_l1o")]
divisions <- fread(file = "./aux_data/us census bureau regions and divisions.csv") %>% clean_names
unique(df_us_city_year_month$state)
nrow(df_us_city_year_month) # 102,102
df_us_city_year_month <- df_us_city_year_month %>%
  left_join(divisions)
nrow(df_us_city_year_month) # 102,102
df_us_city_year_month <- df_us_city_year_month %>%
  .[, state_code := ifelse(city_state == "US", "-", state_code)] %>%
  .[, region := ifelse(city_state == "US", "-", region)] %>%
  .[, division := ifelse(city_state == "US", "-", division)]
nrow(df_us_city_year_month)
df_us_city_year_month <- df_us_city_year_month %>% .[!is.na(state_code)]
nrow(df_us_city_year_month)
class(df_us_city_year_month$year_month)

# Drop cities with fewer than 500 postings in any year_month cell
df_us_city_year_month <- df_us_city_year_month %>%
  .[, keep := ifelse(any(N < 250), 0, 1), by = .(city, region, state)] %>%
  .[keep == 1]

# Cities with fewer than 750 postings in any one cell will get the 3ma, otherwise monthly
df_us_city_year_month <- df_us_city_year_month %>%
  .[, ma_flag := ifelse(any(N < 750), 1, 0), by = .(city, region, state)]

df_us_city_year_month <- df_us_city_year_month %>%
  .[(ma_flag == 1 & name == "monthly_mean_3ma_l1o") | (ma_flag == 0 & name == "monthly_mean_l1o")] %>%
  .[, measurement := ifelse(name == "monthly_mean_3ma_l1o", "3 Month Average", "1 Month Average")]

# df_us_city_2022 <- df_us_city_year_month %>% .[, year := year(as.yearmon(year_month))] %>% .[year == 2022] %>% .[, .(N = sum(N)), by = .(city, state_code, city_state)] %>% .[order(desc(N))]
# View(df_us_city_2022)

df_us_city_year_month <- df_us_city_year_month %>% setDT(.) %>% .[, city := ifelse(city_state == "US", "UNITED STATES - NATIONAL", city)]
df_us_city_year_month <- df_us_city_year_month %>% select(year_month, city, region, division, state_code, value, N, measurement)
df_us_city_year_month <- df_us_city_year_month %>% arrange(division, region, state_code, city, year_month)
df_us_city_year_month <- df_us_city_year_month %>% mutate(year = as.character(year(as.yearmon(year_month))))
df_us_city_year_month <- df_us_city_year_month %>% mutate(month = as.character(month(as.yearmon(year_month), label = TRUE)))
df_us_city_year_month <- df_us_city_year_month %>% mutate(percent = round(100*value, 2))
df_us_city_year_month <- df_us_city_year_month %>% mutate("year_month" = year_month)

df_us_city_year_month <- df_us_city_year_month %>% select(year, month, year_month, city, region, division, state_code, percent, N, measurement)
df_us_city_year_month <- df_us_city_year_month %>% rename(city = city,
                                                          region = region,
                                                          division = division,
                                                          state = state_code,
                                                          n = N)

df_us_city_year_month <- df_us_city_year_month %>% setDT(.) %>% .[, mean := mean(n, na.rm = T), by = .(city, region, division, state)] %>% .[order(-mean, city, state, year_month)]

setwd("/mnt/disks/pdisk/bg_combined/")
fwrite(df_us_city_year_month, file = "./public_data/us_city_by_month_wfh_share.csv")
tail(df_us_city_year_month)

class(df_us_city_year_month$year_month)

us_city_list <- c("Boston, MA","Cleveland, OH",
                  "Columbus, OH","Des Moines, IA","Houston, TX","Indianapolis, IN",
                  "Jacksonville, FL","Louisville, KY","Memphis, TN",
                  "New York, NY","Des Moines, IA","San Francisco, CA",
                  "Savannah, GA","Wichita, KS","Miami Beach, FL",
                  "Houston, TX", "Phoenix, AZ","Denver, CO")


class(df_us_city_year_month$year_month)

us_city_list <- c("Boston, MA","Cleveland, OH",
                  "Columbus, OH","Des Moines, IA","Houston, TX","Indianapolis, IN",
                  "Jacksonville, FL","Louisville, KY","Memphis, TN",
                  "New York, NY","Des Moines, IA","San Francisco, CA",
                  "Savannah, GA","Wichita, KS","Miami Beach, FL",
                  "Houston, TX", "Phoenix, AZ","Denver, CO")

df_us_city_year_month_plot <- df_us_city_year_month %>%
  mutate(city_state = paste0(city, ", ", state)) %>%
  mutate(year_month = as.Date(as.yearmon(year_month))) %>%
  select(city_state, year_month, percent) %>%
  filter(city_state %in% us_city_list) %>%
  group_by(year_month) %>%
  pivot_wider(names_from = city_state, values_from = percent)

df_us_city_year_month_plot

fwrite(df_us_city_year_month_plot, file = "./public_data/df_us_city_year_month_plot.csv")

#### END ####

#### 4. US County-level results ####
remove(list = ls())
df_us_county_year_month <- fread(file = "./aux_data/county_level_ts.csv")

df_us_county_year_month <- df_us_county_year_month %>% .[country == "US"] %>% .[name %in% c("monthly_mean_l1o", "monthly_mean_3ma_l1o")]
divisions <- fread(file = "./aux_data/us census bureau regions and divisions.csv") %>% clean_names
unique(df_us_county_year_month$state)
nrow(df_us_county_year_month) # 102,102
df_us_county_year_month <- df_us_county_year_month %>%
  left_join(divisions)
nrow(df_us_county_year_month) # 102,102
df_us_county_year_month <- df_us_county_year_month %>%
  .[, state_code := ifelse(county_state == "US", "-", state_code)] %>%
  .[, region := ifelse(county_state == "US", "-", region)] %>%
  .[, division := ifelse(county_state == "US", "-", division)] %>%
  .[, fips := ifelse(county_state == "US", "-", fips)]
nrow(df_us_county_year_month)
df_us_county_year_month <- df_us_county_year_month %>% .[!is.na(state_code)]
nrow(df_us_county_year_month)
class(df_us_county_year_month$year_month)

head(df_us_county_year_month)

# Drop cities with fewer than 500 postings in any year_month cell
df_us_county_year_month <- df_us_county_year_month %>%
  .[, keep := ifelse(any(N < 250), 0, 1), by = .(county, region, state)] %>%
  .[keep == 1]

# Cities with fewer than 750 postings in any one cell will get the 3ma, otherwise monthly
df_us_county_year_month <- df_us_county_year_month %>%
  .[, ma_flag := ifelse(any(N < 750), 1, 0), by = .(county, region, state)]

df_us_county_year_month <- df_us_county_year_month %>%
  .[(ma_flag == 1 & name == "monthly_mean_3ma_l1o") | (ma_flag == 0 & name == "monthly_mean_l1o")] %>%
  .[, measurement := ifelse(name == "monthly_mean_3ma_l1o", "3 Month Average", "1 Month Average")]

# df_us_county_2022 <- df_us_county_year_month %>% .[, year := year(as.yearmon(year_month))] %>% .[year == 2022] %>% .[, .(N = sum(N)), by = .(county, state_code, county_state)] %>% .[order(desc(N))]
# View(df_us_county_2022)

df_us_county_year_month <- df_us_county_year_month %>% setDT(.) %>% .[, county := ifelse(county_state == "US", "UNITED STATES - NATIONAL", county)]
df_us_county_year_month <- df_us_county_year_month %>% select(year_month, county, fips, region, division, state_code, value, N, measurement)
df_us_county_year_month <- df_us_county_year_month %>% arrange(division, region, state_code, county, year_month)
df_us_county_year_month <- df_us_county_year_month %>% mutate(year = as.character(year(as.yearmon(year_month))))
df_us_county_year_month <- df_us_county_year_month %>% mutate(month = as.character(month(as.yearmon(year_month), label = TRUE)))
df_us_county_year_month <- df_us_county_year_month %>% mutate(percent = round(100*value, 2))
df_us_county_year_month <- df_us_county_year_month %>% mutate("year_month" = year_month)

df_us_county_year_month <- df_us_county_year_month %>% select(year, month, year_month, county, fips, region, division, state_code, percent, N, measurement)
df_us_county_year_month <- df_us_county_year_month %>% rename(county = county,
                                                          region = region,
                                                          division = division,
                                                          state = state_code,
                                                          n = N)

View(df_us_county_year_month)

setwd("/mnt/disks/pdisk/bg_combined/")
fwrite(df_us_county_year_month, file = "./public_data/us_county_by_month_wfh_share.csv")
tail(df_us_county_year_month)
#### END ####

#### 4. US State-level results ####
remove(list = ls())
df_us_state_year_month <- fread(file = "./aux_data/state_level_ts.csv")

table(df_us_state_year_month$name)
df_us_state_year_month <- df_us_state_year_month %>% .[country == "US"] %>% .[name %in% c("monthly_mean_l1o", "monthly_mean_3ma_l1o")]
divisions <- fread(file = "./aux_data/us census bureau regions and divisions.csv") %>% clean_names
unique(df_us_state_year_month$state)
nrow(df_us_state_year_month) # 102,102
df_us_state_year_month <- df_us_state_year_month %>%
  left_join(divisions)
nrow(df_us_state_year_month) # 102,102
df_us_state_year_month <- df_us_state_year_month %>%
  .[, state_code := ifelse(state == "US", "-", state_code)] %>%
  .[, region := ifelse(state == "US", "-", region)] %>%
  .[, division := ifelse(state == "US", "-", division)]
nrow(df_us_state_year_month)
df_us_state_year_month <- df_us_state_year_month %>% .[!is.na(state_code)]
nrow(df_us_state_year_month)
class(df_us_state_year_month$year_month)

# Drop cities with fewer than 500 postings in any year_month cell
nrow(df_us_state_year_month)
df_us_state_year_month <- df_us_state_year_month %>%
  .[, keep := ifelse(any(N < 250), 0, 1), by = .(state, region, state)] %>%
  .[keep == 1]
nrow(df_us_state_year_month)
# Cities with fewer than 750 postings in any one cell will get the 3ma, otherwise monthly
df_us_state_year_month <- df_us_state_year_month %>%
  .[, ma_flag := ifelse(any(N < 750), 1, 0), by = .(state, region, state)]

df_us_state_year_month <- df_us_state_year_month %>%
  .[(ma_flag == 1 & name == "monthly_mean_3ma_l1o") | (ma_flag == 0 & name == "monthly_mean_l1o")] %>%
  .[, measurement := ifelse(name == "monthly_mean_3ma_l1o", "3 Month Average", "1 Month Average")]

# df_us_state_2022 <- df_us_state_year_month %>% .[, year := year(as.yearmon(year_month))] %>% .[year == 2022] %>% .[, .(N = sum(N)), by = .(state, state_code, state)] %>% .[order(desc(N))]
# View(df_us_state_2022)

df_us_state_year_month <- df_us_state_year_month %>% setDT(.) %>% .[, state := ifelse(state == "US", "UNITED STATES - NATIONAL", state)]
df_us_state_year_month <- df_us_state_year_month %>% select(year_month, state, region, division, state_code, value, N, measurement)
df_us_state_year_month <- df_us_state_year_month %>% arrange(division, region, state_code, state, year_month)
df_us_state_year_month <- df_us_state_year_month %>% mutate(year = as.character(year(as.yearmon(year_month))))
df_us_state_year_month <- df_us_state_year_month %>% mutate(month = as.character(month(as.yearmon(year_month), label = TRUE)))
df_us_state_year_month <- df_us_state_year_month %>% mutate(percent = round(100*value, 2))
df_us_state_year_month <- df_us_state_year_month %>% mutate("year_month" = year_month)

df_us_state_year_month <- df_us_state_year_month %>% select(year, month, year_month, state, region, division, state_code, percent, N, measurement)
df_us_state_year_month <- df_us_state_year_month %>% rename(state = state,
                                                              region = region,
                                                              division = division,
                                                            state_code = state_code,
                                                              n = N)

df_us_state_year_month <- df_us_state_year_month %>% setDT(.) %>% .[, mean := mean(n, na.rm = T), by = .(state, state_code, region, division, state)] %>% .[order(-mean, state_code, year_month)]

View(df_us_state_year_month)

setwd("/mnt/disks/pdisk/bg_combined/")
fwrite(df_us_state_year_month, file = "./public_data/us_state_by_month_wfh_share.csv")
tail(df_us_state_year_month)

nrow(df_us_state_year_month)
df_us_state_year_month <- df_us_state_year_month %>%
  .[state %in% states]
nrow(df_us_state_year_month)

df_us_state_year_month_map <- df_us_state_year_month %>%
  .[as.numeric(year_month) >= max(year_month)-1] %>%
  .[, .(percent = round(sum(percent*n)/sum(n), 1)), by = state]

df_us_state_year_month_map <- df_us_state_year_month_map %>% .[order(-percent)]

df_us_state_year_month_map <- df_us_state_year_month_map %>% .[!grepl("District of Col", state)]

df_us_state_year_month_map

fwrite(df_us_state_year_month_map, "./public_data/state_map_past_12months.csv")

#### END ####

#### 5. UK City-level results ####
remove(list = ls())
df_uk_city_year_month <- fread(file = "./aux_data/city_level_ts.csv")

table(df_uk_city_year_month$name)
df_uk_city_year_month <- df_uk_city_year_month %>% .[country == "UK"] %>% .[name %in% c("monthly_mean_l1o", "monthly_mean_3ma_l1o")]
nrow(df_uk_city_year_month)
nrow(df_uk_city_year_month)
class(df_uk_city_year_month$year_month)
df_uk_city_year_month
# Drop cities with fewer than 500 postings in any year_month cell
df_uk_city_year_month <- df_uk_city_year_month %>%
  .[, keep := ifelse(any(N < 250), 0, 1), by = .(city, state)] %>%
  .[keep == 1]

# Cities with fewer than 750 postings in any one cell will get the 3ma, otherwise monthly
df_uk_city_year_month <- df_uk_city_year_month %>%
  .[, ma_flag := ifelse(any(N < 750), 1, 0), by = .(city, state)]

df_uk_city_year_month <- df_uk_city_year_month %>%
  .[(ma_flag == 1 & name == "monthly_mean_3ma_l1o") | (ma_flag == 0 & name == "monthly_mean_l1o")] %>%
  .[, measurement := ifelse(name == "monthly_mean_3ma_l1o", "3 Month Average", "1 Month Average")]

df_uk_city_year_month <- df_uk_city_year_month %>% setDT(.) %>% .[, city := ifelse(city_state == "UK", "UNITED KINGDOM - NATIONAL", city)]

df_uk_city_year_month <- df_uk_city_year_month %>% select(year_month, city, state, value, N, measurement)
df_uk_city_year_month <- df_uk_city_year_month %>% arrange(state, city, year_month)

df_uk_city_year_month <- df_uk_city_year_month %>% mutate(year = as.character(year(as.yearmon(year_month))))
df_uk_city_year_month <- df_uk_city_year_month %>% mutate(month = as.character(month(as.yearmon(year_month), label = TRUE)))
df_uk_city_year_month <- df_uk_city_year_month %>% mutate(percent = round(100*value, 2))

df_uk_city_year_month <- df_uk_city_year_month %>% select(year, month, year_month, city, state, percent, N, measurement)
df_uk_city_year_month <- df_uk_city_year_month %>% rename(city = city,
                                                              nation = state,
                                                              n = N)
head(df_uk_city_year_month)
tail(df_uk_city_year_month)

setwd("/mnt/disks/pdisk/bg_combined/")
fwrite(df_uk_city_year_month, file = "./public_data/uk_city_by_month_wfh_share.csv")

#### END ####

#### 5. UK County-level results ####
remove(list = ls())
df_uk_county_year_month <- fread(file = "./aux_data/county_level_ts.csv")

table(df_uk_county_year_month$name)
df_uk_county_year_month <- df_uk_county_year_month %>% .[country == "UK"] %>% .[name %in% c("monthly_mean_l1o", "monthly_mean_3ma_l1o")]
nrow(df_uk_county_year_month)
nrow(df_uk_county_year_month)
class(df_uk_county_year_month$year_month)
df_uk_county_year_month
# Drop cities with fewer than 500 postings in any year_month cell
df_uk_county_year_month <- df_uk_county_year_month %>%
  .[, keep := ifelse(any(N < 250), 0, 1), by = .(county, state)] %>%
  .[keep == 1]

# Cities with fewer than 750 postings in any one cell will get the 3ma, otherwise monthly
df_uk_county_year_month <- df_uk_county_year_month %>%
  .[, ma_flag := ifelse(any(N < 750), 1, 0), by = .(county, state)]

df_uk_county_year_month <- df_uk_county_year_month %>%
  .[(ma_flag == 1 & name == "monthly_mean_3ma_l1o") | (ma_flag == 0 & name == "monthly_mean_l1o")] %>%
  .[, measurement := ifelse(name == "monthly_mean_3ma_l1o", "3 Month Average", "1 Month Average")]

df_uk_county_year_month <- df_uk_county_year_month %>% setDT(.) %>% .[, county := ifelse(county_state == "UK", "UNITED KINGDOM - NATIONAL", county)]

df_uk_county_year_month <- df_uk_county_year_month %>% select(year_month, county, state, value, N, measurement)
df_uk_county_year_month <- df_uk_county_year_month %>% arrange(state, county, year_month)

df_uk_county_year_month <- df_uk_county_year_month %>% mutate(year = as.character(year(as.yearmon(year_month))))
df_uk_county_year_month <- df_uk_county_year_month %>% mutate(month = as.character(month(as.yearmon(year_month), label = TRUE)))
df_uk_county_year_month <- df_uk_county_year_month %>% mutate(percent = round(100*value, 2))

df_uk_county_year_month <- df_uk_county_year_month %>% select(year, month, year_month, county, state, percent, N, measurement)
df_uk_county_year_month <- df_uk_county_year_month %>% rename(county = county,
                                                          nation = state,
                                                          n = N)
head(df_uk_county_year_month)
tail(df_uk_county_year_month)

setwd("/mnt/disks/pdisk/bg_combined/")
fwrite(df_uk_county_year_month, file = "./public_data/uk_county_by_month_wfh_share.csv")

#### END ####

#### 6 NAICS 2-DIGIT USA ####
remove(list = ls())
setwd("/mnt/disks/pdisk/bg-us/")

#colnames(fread("./int_data/us_stru_2019_wfh.csv", nrow = 100))

df_us_2019 <- fread("./int_data/us_stru_2019_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "sector", "sector_name", "wfh_wham", "job_domain")) %>% mutate(country = "US") %>% setDT()
df_us_2020 <- fread("./int_data/us_stru_2020_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "sector", "sector_name", "wfh_wham", "job_domain")) %>% mutate(country = "US") %>% setDT()
df_us_2021 <- fread("./int_data/us_stru_2021_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "sector", "sector_name", "wfh_wham", "job_domain")) %>% mutate(country = "US") %>% setDT()
df_us_2022 <- fread("./int_data/us_stru_2022_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "sector", "sector_name", "wfh_wham", "job_domain")) %>% mutate(country = "US") %>% setDT()
df_us_2023 <- fread("./int_data/us_stru_2023_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "sector", "sector_name", "wfh_wham", "job_domain")) %>% mutate(country = "US") %>% setDT()

setwd("/mnt/disks/pdisk/bg_combined/")

df_us <- rbindlist(list(df_us_2019,df_us_2020,df_us_2021,df_us_2022,df_us_2023))
remove(list = c("df_us_2019","df_us_2020","df_us_2021","df_us_2022","df_us_2023"))

df_us <- df_us[!is.na(job_domain) & job_domain != ""]
df_us <- df_us %>% .[!grepl("careerbuilder", job_domain)]
df_us <- df_us %>% .[!is.na(sector) & sector != ""]
df_us <- df_us %>% .[!is.na(wfh_wham) & wfh_wham != ""]
df_us <- df_us %>% .[, year_month := as.yearmon(job_date)]
df_us <- df_us %>% .[year_month <= as.yearmon(ymd("20230401"))] # Up to Jan 2023
df_us <- df_us %>% .[year_month >= as.yearmon(ymd("20190101"))] # From Jan 2018

df_us_naics2_year_month <- df_us %>% .[, .(percent = mean(wfh_wham), N = .N), by = .(sector, sector_name, year_month)]

df_us_naics2_year_month <- df_us_naics2_year_month %>% select(sector, sector_name, year_month, percent, N)
df_us_naics2_year_month <- df_us_naics2_year_month %>% arrange(sector, year_month)

df_us_naics2_year_month <- df_us_naics2_year_month %>% mutate(year = as.character(year(year_month)))
df_us_naics2_year_month <- df_us_naics2_year_month %>% mutate(month = as.character(month(year_month, label = TRUE)))
df_us_naics2_year_month <- df_us_naics2_year_month %>% mutate(percent = round(100*percent, 2))
df_us_naics2_year_month <- df_us_naics2_year_month %>% mutate("year_month" = year_month)

df_us_naics2_year_month <- df_us_naics2_year_month %>% select(year, month, "year_month", sector, sector_name, percent, N)
df_us_naics2_year_month <- df_us_naics2_year_month %>% rename("naics_2_digit_code" = sector,
                                                              "naics_name" = sector_name,
                                                              n = N)
df_us_naics2_year_month <- df_us_naics2_year_month %>% .[, measurement := "1 Month Average"]
head(df_us_naics2_year_month)
tail(df_us_naics2_year_month)


fwrite(df_us_naics2_year_month, file = "./public_data/us_naics_2digit_by_month_wfh_share.csv")

uniqueN(df_us_naics2_year_month$naics_name)

df_us_naics2_year_month_bar <- df_us_naics2_year_month %>%
  .[year %in% c(2019, 2022)] %>%
  .[, .(percent = sum(percent*n)/sum(n), n = sum(n)), by = .(year, naics_name)] %>%
  .[, percent := round(percent, 1)] %>%
  select(-n) %>%
  group_by(naics_name) %>%
  pivot_wider(names_from = year, values_from = percent)

df_us_naics2_year_month_bar

df_us_naics2_year_month_bar <- df_us_naics2_year_month_bar %>% setDT(.) %>% .[order(-`2022`)] %>% select(naics_name, `2022`, `2019`) %>% setDT()

df_us_naics2_year_month_bar <- df_us_naics2_year_month_bar %>%
  mutate(naics_name = case_when(
    naics_name == "Accommodation and Food Services" ~ "Accom & Food Service",
    naics_name == "Retail Trade" ~ "Retail",
    naics_name == "Arts, Entertainment, and Recreation" ~ "Arts & Entertainment",
    naics_name == "Transportation and Warehousing" ~ "Transportation",
    naics_name == "Health Care and Social Assistance" ~ "Helath Care",
    naics_name == "Mining, Quarrying, and Oil and Gas Extraction" ~ "Mining",
    naics_name == "Other Services (except Public Administration)" ~ "Other Services",
    naics_name == "Construction" ~ "Construction",
    naics_name == "Educational Services" ~ "Education",
    naics_name == "Real Estate and Rental and Leasing" ~ "Real Estate",
    naics_name == "Administrative and Support and Waste Management and Remediation Services" ~ "Administration",
    naics_name == "Wholesale Trade" ~ "Wholesale",
    naics_name == "Agriculture, Forestry, Fishing and Hunting" ~ "Agriculture",
    naics_name == "Management of Companies and Enterprises" ~ "Management",
    naics_name == "Utilities" ~ "Utilities",
    naics_name == "Manufacturing" ~ "Manufacturing",
    naics_name == "Public Administration" ~ "Public Administration",
    naics_name == "Information" ~ "Information & IT",
    naics_name == "Professional, Scientific, and Technical Services" ~ "Professional & Scientific",
    naics_name == "Finance and Insurance" ~ "Finance & Insurance",
    TRUE ~ ""
  )) %>% setDT(.)

df_us_naics2_year_month_bar

fwrite(df_us_naics2_year_month_bar, file = "./public_data/us_naics_2digit_by_month_wfh_share_bar.csv")

#### END ####

#### DROPPED 7 SIC2 UK ####
# remove(list = ls())
# setwd("/mnt/disks/pdisk/bg-uk/")
# 
# check <- fread("./int_data/uk_stru_2019_wfh.csv", nrow = 100000)
# 
# df_uk_2019 <- fread("./int_data/uk_stru_2019_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "sic_section", "wfh_wham", "job_domain")) %>% mutate(country = "UK") %>% setDT()
# df_uk_2020 <- fread("./int_data/uk_stru_2020_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "sic_section", "wfh_wham", "job_domain")) %>% mutate(country = "UK") %>% setDT()
# df_uk_2021 <- fread("./int_data/uk_stru_2021_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "sic_section", "wfh_wham", "job_domain")) %>% mutate(country = "UK") %>% setDT()
# df_uk_2022 <- fread("./int_data/uk_stru_2022_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "sic_section", "wfh_wham", "job_domain")) %>% mutate(country = "UK") %>% setDT()
# df_uk_2023 <- fread("./int_data/uk_stru_2023_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "sic_section", "wfh_wham", "job_domain")) %>% mutate(country = "UK") %>% setDT()
# 
# df_uk <- rbindlist(list(df_uk_2019,df_uk_2020,df_uk_2021,df_uk_2022,df_uk_2023))
# remove(list = c("df_uk_2019","df_uk_2020","df_uk_2021","df_uk_2022","df_uk_2023"))
# 
# df_uk <- df_uk[!is.na(job_domain) & job_domain != ""]
# df_uk <- df_uk %>% .[!grepl("jobisjob", job_domain)]
# df_uk <- df_uk %>% .[!is.na(sic_section) & sic_section != ""]
# df_uk <- df_uk %>% .[!is.na(wfh_wham) & wfh_wham != ""]
# 
# df_uk <- df_uk %>% .[, year_month := as.yearmon(job_date)]
# df_uk <- df_uk %>% .[year_month <= as.yearmon(ymd("20230401"))] # Up to Jan 2023
# df_uk <- df_uk %>% .[year_month >= as.yearmon(ymd("20190101"))] # From Jan 2018
# df_uk_sic_section_year_month <- df_uk %>% .[, .(percent = mean(wfh_wham), N = .N), by = .(sic_section, year_month)]
# df_uk_sic_section_year_month$sic_section <- str_to_title(df_uk_sic_section_year_month$sic_section)
# 
# df_uk_sic_section_year_month <- df_uk_sic_section_year_month %>% select(sic_section, year_month, percent)
# df_uk_sic_section_year_month <- df_uk_sic_section_year_month %>% arrange(sic_section, year_month)
# 
# df_uk_sic_section_year_month <- df_uk_sic_section_year_month %>% mutate(Year = as.character(year(year_month)))
# df_uk_sic_section_year_month <- df_uk_sic_section_year_month %>% mutate(Month = as.character(month(year_month, label = TRUE)))
# df_uk_sic_section_year_month <- df_uk_sic_section_year_month %>% mutate(Percent = round(100*percent, 2))
# df_uk_sic_section_year_month <- df_uk_sic_section_year_month %>% mutate("Year-Month" = year_month)
# 
# df_uk_sic_section_year_month <- df_uk_sic_section_year_month %>% select(Year, Month, "Year-Month", sic_section, Percent)
# df_uk_sic_section_year_month <- df_uk_sic_section_year_month %>% rename("UK SIC Section" = sic_section)
# 
# setwd("/mnt/disks/pdisk/bg_combined/")
# fwrite(df_uk_sic_section_year_month, file = "./public_data/uk_sic_section2_by_month_wfh_share.csv")
#
#### END ####