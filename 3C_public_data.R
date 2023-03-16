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

# Plot unweighted
wfh_across_country <- readRDS(file = "./int_data/country_ts_combined_weights.rds") %>%
  ungroup() %>%
  filter(weight == "US 2019 Vacancy") %>%
  filter(year_month >= as.yearmon(ymd("20190101"))) %>%
  arrange(desc(country), year_month) %>%
  select(country, year_month, value) %>%
  rename(Percent = value, Country = country)

wfh_across_country <- wfh_across_country %>% mutate(Year = as.character(year(year_month)))
wfh_across_country <- wfh_across_country %>% mutate(Month = as.character(month(year_month, label = TRUE)))
wfh_across_country <- wfh_across_country %>% mutate(Percent = round(100*Percent, 2))
wfh_across_country <- wfh_across_country %>% mutate("Year-Month" = year_month)

wfh_across_country <- wfh_across_country %>% select(Year, Month, "Year-Month", Country, Percent)
  
fwrite(wfh_across_country, file = "./public_data/country_by_month_wfh_share.csv")

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
df_us <- df_us %>% .[year_month <= as.yearmon(ymd("20230101"))] # Up to Jan 2023
df_us <- df_us %>% .[year_month >= as.yearmon(ymd("20190101"))] # From Jan 2018
df_us_soc_year_month <- df_us %>% .[, .(percent = mean(wfh_wham), N = .N), by = .(soc_2_digit, year_month)]
df_us_soc_year_month$soc_2_digit <- as.numeric(df_us_soc_year_month$soc_2_digit)

soc2_names <- fread("./aux_data/bls_soc_2010_2_digit_names.csv", header = FALSE)
colnames(soc2_names) <- c("soc_2_digit", "name")
soc2_names <- soc2_names %>% .[, soc_2_digit := str_sub(soc_2_digit, 1, 2)]
soc2_names$soc_2_digit <- as.numeric(soc2_names$soc_2_digit)

df_us_soc_year_month <- df_us_soc_year_month %>% left_join(soc2_names)

df_us_soc_year_month <- df_us_soc_year_month %>% select(soc_2_digit, year_month, name, percent)
df_us_soc_year_month <- df_us_soc_year_month %>% arrange(soc_2_digit, year_month)

df_us_soc_year_month <- df_us_soc_year_month %>% mutate(Year = as.character(year(year_month)))
df_us_soc_year_month <- df_us_soc_year_month %>% mutate(Month = as.character(month(year_month, label = TRUE)))
df_us_soc_year_month <- df_us_soc_year_month %>% mutate(Percent = round(100*percent, 2))
df_us_soc_year_month <- df_us_soc_year_month %>% mutate("Year-Month" = year_month)

df_us_soc_year_month <- df_us_soc_year_month %>% select(Year, Month, "Year-Month", soc_2_digit, name, Percent)
df_us_soc_year_month <- df_us_soc_year_month %>% rename("US SOC Major Group (2-digit)" = soc_2_digit,
                                                    "US SOC Major Group (name)" = name)

setwd("/mnt/disks/pdisk/bg_combined/")
fwrite(df_us_soc_year_month, file = "./public_data/us_soc2digit_by_month_wfh_share.csv")

#### 3 SOC2 UK ####
remove(list = ls())
setwd("/mnt/disks/pdisk/bg-uk/")

df_uk_2019 <- fread("./int_data/uk_stru_2019_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "uksoc_code", "uksoc_sub_major_group", "wfh_wham", "job_domain")) %>% mutate(country = "UK") %>% setDT()
df_uk_2020 <- fread("./int_data/uk_stru_2020_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "uksoc_code", "uksoc_sub_major_group", "wfh_wham", "job_domain")) %>% mutate(country = "UK") %>% setDT()
df_uk_2021 <- fread("./int_data/uk_stru_2021_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "uksoc_code", "uksoc_sub_major_group", "wfh_wham", "job_domain")) %>% mutate(country = "UK") %>% setDT()
df_uk_2022 <- fread("./int_data/uk_stru_2022_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "uksoc_code", "uksoc_sub_major_group", "wfh_wham", "job_domain")) %>% mutate(country = "UK") %>% setDT()
df_uk_2023 <- fread("./int_data/uk_stru_2023_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "uksoc_code", "uksoc_sub_major_group", "wfh_wham", "job_domain")) %>% mutate(country = "UK") %>% setDT()

df_uk <- rbindlist(list(df_uk_2019,df_uk_2020,df_uk_2021,df_uk_2022,df_uk_2023))
remove(list = c("df_uk_2019","df_uk_2020","df_uk_2021","df_uk_2022","df_uk_2023"))

df_uk <- df_uk[!is.na(job_domain) & job_domain != ""]
df_uk <- df_uk %>% .[!grepl("jobisjob", job_domain)]
df_uk <- df_uk %>% .[!is.na(uksoc_code) & uksoc_code != ""]
df_uk <- df_uk %>% .[!is.na(wfh_wham) & wfh_wham != ""]
df_uk <- df_uk %>% .[, soc_2_digit := str_sub(uksoc_code, 1, 2)]
df_uk <- df_uk %>% .[uksoc_sub_major_group != ""]
unique(df_uk$soc_2_digit)
unique(df_uk$uksoc_sub_major_group)

df_uk <- df_uk %>% .[, year_month := as.yearmon(job_date)]
df_uk <- df_uk %>% .[year_month <= as.yearmon(ymd("20230101"))] # Up to Jan 2023
df_uk <- df_uk %>% .[year_month >= as.yearmon(ymd("20190101"))] # From Jan 2018
df_uk_soc_year_month <- df_uk %>% .[, .(percent = mean(wfh_wham), N = .N), by = .(soc_2_digit, year_month, uksoc_sub_major_group)]
df_uk_soc_year_month$soc_2_digit <- as.numeric(df_uk_soc_year_month$soc_2_digit)
df_uk_soc_year_month$uksoc_sub_major_group <- str_to_title(df_uk_soc_year_month$uksoc_sub_major_group)

df_uk_soc_year_month <- df_uk_soc_year_month %>% select(soc_2_digit, year_month, uksoc_sub_major_group, percent)
df_uk_soc_year_month <- df_uk_soc_year_month %>% arrange(soc_2_digit, year_month)

df_uk_soc_year_month <- df_uk_soc_year_month %>% mutate(Year = as.character(year(year_month)))
df_uk_soc_year_month <- df_uk_soc_year_month %>% mutate(Month = as.character(month(year_month, label = TRUE)))
df_uk_soc_year_month <- df_uk_soc_year_month %>% mutate(Percent = round(100*percent, 2))
df_uk_soc_year_month <- df_uk_soc_year_month %>% mutate("Year-Month" = year_month)

df_uk_soc_year_month <- df_uk_soc_year_month %>% select(Year, Month, "Year-Month", soc_2_digit, uksoc_sub_major_group, Percent)
df_uk_soc_year_month <- df_uk_soc_year_month %>% rename("UK SOC Major Group (2-digit)" = soc_2_digit,
                                                        "UK SOC Major Group (name)" = uksoc_sub_major_group)

setwd("/mnt/disks/pdisk/bg_combined/")
fwrite(df_uk_soc_year_month, file = "./public_data/uk_soc2digit_by_month_wfh_share.csv")

#### END ####

##################################################
# CATEGORY B
##################################################

#### 4. US City-level results ####
remove(list = ls())
df_us_city_year_month <- fread(file = "./aux_data/city_level_ts.csv")
df_us_city_year_month <- df_us_city_year_month %>% .[country == "US"] %>% .[name == "monthly_mean_l1o"]
divisions <- fread(file = "./aux_data/us census bureau regions and divisions.csv") %>% clean_names

unique(df_us_city_year_month$state)
nrow(df_us_city_year_month) # 1208926
df_us_city_year_month <- df_us_city_year_month %>%
  left_join(divisions)
nrow(df_us_city_year_month) # 1208926
View(df_us_city_year_month)
df_us_city_year_month <- df_us_city_year_month %>%
  .[, state_code := ifelse(city_state == "US", "-", state_code)] %>%
  .[, region := ifelse(city_state == "US", "-", region)] %>%
  .[, division := ifelse(city_state == "US", "-", division)]
df_us_city_year_month
df_us_city_year_month <- df_us_city_year_month %>% .[!is.na(state_code)]
class(df_us_city_year_month$year_month)
df_us_city_2022 <- df_us_city_year_month %>% .[, year := year(as.yearmon(year_month))] %>% .[year == 2022] %>% .[, .(N = sum(N)), by = .(city, state_code, city_state)] %>% .[order(desc(N))]

df_us_city_year_month <- df_us_city_year_month %>% .[city_state %in% df_us_city_2022[1:51]$city_state]

df_us_city_year_month <- df_us_city_year_month %>% setDT(.) %>% .[, city := ifelse(city_state == "US", "UNITED STATES - NATIONAL", city)]

df_us_city_year_month <- df_us_city_year_month %>% select(year_month, city, region, division, state_code, value, N)
df_us_city_year_month <- df_us_city_year_month %>% arrange(division, region, state_code, city, year_month)

df_us_city_year_month <- df_us_city_year_month %>% mutate(Year = as.character(year(as.yearmon(year_month))))
df_us_city_year_month <- df_us_city_year_month %>% mutate(Month = as.character(month(as.yearmon(year_month), label = TRUE)))
df_us_city_year_month <- df_us_city_year_month %>% mutate(Percent = round(100*value, 2))
df_us_city_year_month <- df_us_city_year_month %>% mutate("Year-Month" = year_month)

df_us_city_year_month <- df_us_city_year_month %>% select(Year, Month, "Year-Month", city, region, division, state_code, Percent, N)
df_us_city_year_month <- df_us_city_year_month %>% rename(City = city,
                                                          Region = region,
                                                          Division = division,
                                                          State = state_code)
setwd("/mnt/disks/pdisk/bg_combined/")
fwrite(df_us_city_year_month, file = "./public_data/us_city_by_month_wfh_share.csv")

#### 5. UK City-level results ####
remove(list = ls())
df_uk_city_year_month <- fread(file = "./aux_data/city_level_ts.csv")
unique(df_uk_city_year_month$country)
df_uk_city_year_month <- df_uk_city_year_month %>% .[country == "UK"] %>% .[name == "monthly_mean_l1o"]

unique(df_uk_city_year_month$city)
nrow(df_uk_city_year_month) # 1208926

View(df_uk_city_year_month)

df_uk_city_year_month

df_us_city_2022 <- df_uk_city_year_month %>% .[, year := year(as.yearmon(year_month))] %>% .[year == 2022] %>% .[, .(N = sum(N)), by = .(city, state, city_state)] %>% .[order(desc(N))]

df_uk_city_year_month <- df_uk_city_year_month %>% .[city_state %in% df_us_city_2022[1:9]$city_state]

df_uk_city_year_month <- df_uk_city_year_month %>% setDT(.) %>% .[, city := ifelse(city_state == "UK", "UNITED KINGDOM - NATIONAL", city)]

df_uk_city_year_month <- df_uk_city_year_month %>% select(year_month, city, state, value, N)
df_uk_city_year_month <- df_uk_city_year_month %>% arrange(state, city, year_month)

df_uk_city_year_month <- df_uk_city_year_month %>% mutate(Year = as.character(year(as.yearmon(year_month))))
df_uk_city_year_month <- df_uk_city_year_month %>% mutate(Month = as.character(month(as.yearmon(year_month), label = TRUE)))
df_uk_city_year_month <- df_uk_city_year_month %>% mutate(Percent = round(100*value, 2))
df_uk_city_year_month <- df_uk_city_year_month %>% mutate("Year-Month" = year_month)

df_uk_city_year_month <- df_uk_city_year_month %>% select(Year, Month, "Year-Month", city, state, Percent, N)
df_uk_city_year_month <- df_uk_city_year_month %>% rename(City = city,
                                                          "Nation" = state)
setwd("/mnt/disks/pdisk/bg_combined/")
fwrite(df_uk_city_year_month, file = "./public_data/uk_city_by_month_wfh_share.csv")

#### 6 NAICS 2-DIGIT USA ####
remove(list = ls())
setwd("/mnt/disks/pdisk/bg-us/")

#colnames(fread("./int_data/us_stru_2019_wfh.csv", nrow = 100))

df_us_2019 <- fread("./int_data/us_stru_2019_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "sector", "sector_name", "wfh_wham", "job_domain")) %>% mutate(country = "US") %>% setDT()
df_us_2020 <- fread("./int_data/us_stru_2020_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "sector", "sector_name", "wfh_wham", "job_domain")) %>% mutate(country = "US") %>% setDT()
df_us_2021 <- fread("./int_data/us_stru_2021_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "sector", "sector_name", "wfh_wham", "job_domain")) %>% mutate(country = "US") %>% setDT()
df_us_2022 <- fread("./int_data/us_stru_2022_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "sector", "sector_name", "wfh_wham", "job_domain")) %>% mutate(country = "US") %>% setDT()
df_us_2023 <- fread("./int_data/us_stru_2023_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "sector", "sector_name", "wfh_wham", "job_domain")) %>% mutate(country = "US") %>% setDT()

df_us <- rbindlist(list(df_us_2019,df_us_2020,df_us_2021,df_us_2022,df_us_2023))
remove(list = c("df_us_2019","df_us_2020","df_us_2021","df_us_2022","df_us_2023"))

df_us <- df_us[!is.na(job_domain) & job_domain != ""]
df_us <- df_us %>% .[!grepl("careerbuilder", job_domain)]
df_us <- df_us %>% .[!is.na(sector) & sector != ""]
df_us <- df_us %>% .[!is.na(wfh_wham) & wfh_wham != ""]
df_us <- df_us %>% .[, year_month := as.yearmon(job_date)]
df_us <- df_us %>% .[year_month <= as.yearmon(ymd("20230101"))] # Up to Jan 2023
df_us <- df_us %>% .[year_month >= as.yearmon(ymd("20190101"))] # From Jan 2018

df_us_naics2_year_month <- df_us %>% .[, .(percent = mean(wfh_wham), N = .N), by = .(sector, sector_name, year_month)]

df_us_naics2_year_month <- df_us_naics2_year_month %>% select(sector, sector_name, year_month, percent)
df_us_naics2_year_month <- df_us_naics2_year_month %>% arrange(sector, year_month)

df_us_naics2_year_month <- df_us_naics2_year_month %>% mutate(Year = as.character(year(year_month)))
df_us_naics2_year_month <- df_us_naics2_year_month %>% mutate(Month = as.character(month(year_month, label = TRUE)))
df_us_naics2_year_month <- df_us_naics2_year_month %>% mutate(Percent = round(100*percent, 2))
df_us_naics2_year_month <- df_us_naics2_year_month %>% mutate("Year-Month" = year_month)

df_us_naics2_year_month <- df_us_naics2_year_month %>% select(Year, Month, "Year-Month", sector, sector_name, Percent)
df_us_naics2_year_month <- df_us_naics2_year_month %>% rename("US Industry Sector (NAICS 2-digit code)" = sector,
                                                        "US Industry Sector (NAICS name)" = sector_name)

setwd("/mnt/disks/pdisk/bg_combined/")
fwrite(df_us_naics2_year_month, file = "./public_data/us_naics_2digit_by_month_wfh_share.csv")


#### 7 SIC2 UK ####
remove(list = ls())
setwd("/mnt/disks/pdisk/bg-uk/")

check <- fread("./int_data/uk_stru_2019_wfh.csv", nrow = 100000)

df_uk_2019 <- fread("./int_data/uk_stru_2019_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "sic_section", "wfh_wham", "job_domain")) %>% mutate(country = "UK") %>% setDT()
df_uk_2020 <- fread("./int_data/uk_stru_2020_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "sic_section", "wfh_wham", "job_domain")) %>% mutate(country = "UK") %>% setDT()
df_uk_2021 <- fread("./int_data/uk_stru_2021_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "sic_section", "wfh_wham", "job_domain")) %>% mutate(country = "UK") %>% setDT()
df_uk_2022 <- fread("./int_data/uk_stru_2022_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "sic_section", "wfh_wham", "job_domain")) %>% mutate(country = "UK") %>% setDT()
df_uk_2023 <- fread("./int_data/uk_stru_2023_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "sic_section", "wfh_wham", "job_domain")) %>% mutate(country = "UK") %>% setDT()

df_uk <- rbindlist(list(df_uk_2019,df_uk_2020,df_uk_2021,df_uk_2022,df_uk_2023))
remove(list = c("df_uk_2019","df_uk_2020","df_uk_2021","df_uk_2022","df_uk_2023"))

df_uk <- df_uk[!is.na(job_domain) & job_domain != ""]
df_uk <- df_uk %>% .[!grepl("jobisjob", job_domain)]
df_uk <- df_uk %>% .[!is.na(sic_section) & sic_section != ""]
df_uk <- df_uk %>% .[!is.na(wfh_wham) & wfh_wham != ""]

df_uk <- df_uk %>% .[, year_month := as.yearmon(job_date)]
df_uk <- df_uk %>% .[year_month <= as.yearmon(ymd("20230101"))] # Up to Jan 2023
df_uk <- df_uk %>% .[year_month >= as.yearmon(ymd("20190101"))] # From Jan 2018
df_uk_sic_section_year_month <- df_uk %>% .[, .(percent = mean(wfh_wham), N = .N), by = .(sic_section, year_month)]
df_uk_sic_section_year_month$sic_section <- str_to_title(df_uk_sic_section_year_month$sic_section)

df_uk_sic_section_year_month <- df_uk_sic_section_year_month %>% select(sic_section, year_month, percent)
df_uk_sic_section_year_month <- df_uk_sic_section_year_month %>% arrange(sic_section, year_month)

df_uk_sic_section_year_month <- df_uk_sic_section_year_month %>% mutate(Year = as.character(year(year_month)))
df_uk_sic_section_year_month <- df_uk_sic_section_year_month %>% mutate(Month = as.character(month(year_month, label = TRUE)))
df_uk_sic_section_year_month <- df_uk_sic_section_year_month %>% mutate(Percent = round(100*percent, 2))
df_uk_sic_section_year_month <- df_uk_sic_section_year_month %>% mutate("Year-Month" = year_month)

df_uk_sic_section_year_month <- df_uk_sic_section_year_month %>% select(Year, Month, "Year-Month", sic_section, Percent)
df_uk_sic_section_year_month <- df_uk_sic_section_year_month %>% rename("UK SIC Section" = sic_section)

setwd("/mnt/disks/pdisk/bg_combined/")
fwrite(df_uk_sic_section_year_month, file = "./public_data/uk_sic_section2_by_month_wfh_share.csv")