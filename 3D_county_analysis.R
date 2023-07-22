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
setwd("/mnt/disks/pdisk/bg-us/")

#### MAKE FIPS by Month by SOC2


df_us_2019 <- fread("./int_data/us_stru_2019_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "county", "fips", "state", "soc", "job_domain", "wfh_wham")) %>% mutate(country = "US") %>% setDT()
df_us_2020 <- fread("./int_data/us_stru_2020_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "county", "fips", "state", "soc", "job_domain", "wfh_wham")) %>% mutate(country = "US") %>% setDT()
df_us_2021 <- fread("./int_data/us_stru_2021_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "county", "fips", "state", "soc", "job_domain", "wfh_wham")) %>% mutate(country = "US") %>% setDT()
df_us_2022 <- fread("./int_data/us_stru_2022_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "county", "fips", "state", "soc", "job_domain", "wfh_wham")) %>% mutate(country = "US") %>% setDT()
df_us_2023 <- fread("./int_data/us_stru_2023_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_date", "county", "fips", "state", "soc", "job_domain", "wfh_wham")) %>% mutate(country = "US") %>% setDT()

df_us <- rbindlist(list(df_us_2019,df_us_2020,df_us_2021,df_us_2022,df_us_2023))
remove(list = c("df_us_2019","df_us_2020","df_us_2021","df_us_2022","df_us_2023"))

df_us <- df_us[!is.na(job_domain) & job_domain != ""]
df_us <- df_us %>% .[!grepl("careerbuilder", job_domain)]
df_us <- df_us %>% .[!is.na(soc) & soc != ""]
nrow(df_us)
df_us <- df_us %>% .[!is.na(fips) & fips != ""]
nrow(df_us)

1 - 164067310/166898883

df_us <- df_us %>% .[!is.na(wfh_wham) & wfh_wham != ""]
df_us <- df_us %>% .[, soc_2_digit := str_sub(soc, 1, 2)]
df_us <- df_us %>% .[, year_month := as.yearmon(job_date)]
df_us <- df_us %>% .[year_month <= as.yearmon(ymd("20230401"))] # Up to March 2023
df_us <- df_us %>% .[year_month >= as.yearmon(ymd("20190101"))] # From Jan 2018
df_us$soc_2_digit <- as.numeric(df_us$soc_2_digit)

df_us_soc5_fips_ym <- df_us %>%
  setDT(.) %>%
  .[, .(N = .N, wfh_percent = mean(wfh_wham)), by = .(year_month, county, fips, state, soc_2_digit, soc)]

# Create wfh_prop

nrow(df_us_soc5_fips_ym) # 16,945,528
df_us_soc5_fips_ym <- df_us_soc5_fips_ym %>%
  .[!is.na(N) & !is.na(wfh_percent)]
nrow(df_us_soc5_fips_ym) # 16,945,528

df_us_soc5_fips_ym <- df_us_soc5_fips_ym %>%
  .[, wfh_N_soc := sum(N*wfh_percent), by = .(year_month, soc)] %>%
  .[, N_soc := sum(N), by = .(year_month, soc)] %>%
  .[, wfh_prop_soc := wfh_N_soc/N_soc] %>%
  .[, wfh_prop_soc_nf := (wfh_N_soc - N*wfh_percent)/(N_soc-N)]

df_us_soc5_fips_ym <- df_us_soc5_fips_ym %>% .[, year_month := as.Date(as.yearmon(year_month))]

mod1 <- feols(fml = wfh_percent ~ wfh_prop_soc, data = df_us_soc5_fips_ym[year_month >= ymd("20220101")], cluster = ~ fips)
mod2 <- feols(fml = wfh_percent ~ wfh_prop_soc, data = df_us_soc5_fips_ym[year_month >= ymd("20220101")], weights = ~ N, cluster = ~ fips)
mod3 <- feols(fml = wfh_percent ~ wfh_prop_soc_nf, data = df_us_soc5_fips_ym[year_month >= ymd("20220101")], cluster = ~ fips)
mod4 <- feols(fml = wfh_percent ~ wfh_prop_soc_nf, data = df_us_soc5_fips_ym[year_month >= ymd("20220101")], weights = ~ N, cluster = ~ fips)

etable(mod1, mod2, mod3, mod4, title = "Panel A: County x Year-Month x SOC 5-digit Results", digits = "r2", digits.stats = "r3", tex = TRUE)

fwrite(df_us_soc5_fips_ym %>% .[, year_month := as.Date(as.yearmon(year_month))] %>% .[, year := year(year_month)] %>% .[, month := month(year_month)] %>% select(year_month, year, month, fips, county, state, soc, wfh_percent, N, wfh_prop_soc, wfh_prop_soc_nf),
       "./aux_data/county_by_month_by_soc5.csv")

df_us_soc2_fips_ym <- df_us_soc5_fips_ym %>%
  .[, .(N = sum(N),
        wfh_percent = sum(wfh_percent*N)/sum(N),
        wfh_prop_soc = sum(wfh_prop_soc*N)/sum(N),
        wfh_prop_soc_nf = sum(wfh_prop_soc_nf*N)/sum(N)),
    by = .(year_month, county, fips, state, soc_2_digit)]

mod1 <- feols(fml = wfh_percent ~ wfh_prop_soc, data = df_us_soc2_fips_ym[year_month >= ymd("20220101")], cluster = ~ fips)
mod2 <- feols(fml = wfh_percent ~ wfh_prop_soc, data = df_us_soc2_fips_ym[year_month >= ymd("20220101")], weights = ~ N, cluster = ~ fips)
mod3 <- feols(fml = wfh_percent ~ wfh_prop_soc_nf, data = df_us_soc2_fips_ym[year_month >= ymd("20220101")], cluster = ~ fips)
mod4 <- feols(fml = wfh_percent ~ wfh_prop_soc_nf, data = df_us_soc2_fips_ym[year_month >= ymd("20220101")], weights = ~ N, cluster = ~ fips)

etable(mod1, mod2, mod3, mod4, title = "Panel B: County x Year-Month x SOC 2-Digit Results", digits = "r2", digits.stats = "r3", tex = TRUE)

fwrite(df_us_soc2_fips_ym %>%  .[, year_month := as.Date(as.yearmon(year_month))] %>% .[, year := year(year_month)] %>% .[, month := month(year_month)] %>% select(year_month, year, month, fips, county, state, soc_2_digit, wfh_percent, N, wfh_prop_soc, wfh_prop_soc_nf),
       "./aux_data/county_by_month_by_soc2.csv")

df_us_fips_ym <- df_us_soc2_fips_ym %>%
  .[, .(N = sum(N),
        wfh_percent = sum(wfh_percent*N)/sum(N),
        wfh_prop_soc = sum(wfh_prop_soc*N)/sum(N),
        wfh_prop_soc_nf = sum(wfh_prop_soc_nf*N)/sum(N)),
    by = .(year_month, county, fips, state)]

mod1 <- feols(fml = wfh_percent ~ wfh_prop_soc, data = df_us_fips_ym[year_month >= ymd("20220101")], weights = ~ N, cluster = ~ fips)
mod2 <- feols(fml = wfh_percent ~ wfh_prop_soc_nf, data = df_us_fips_ym[year_month >= ymd("20220101")], weights = ~ N, cluster = ~ fips)
mod3 <- feols(fml = wfh_percent ~ wfh_prop_soc | year_month, data = df_us_fips_ym[year_month >= ymd("20220101")], weights = ~ N, cluster = ~ fips)
mod4 <- feols(fml = wfh_percent ~ wfh_prop_soc | fips, data = df_us_fips_ym[year_month >= ymd("20220101")], weights = ~ N, cluster = ~ fips)
mod5 <- feols(fml = wfh_percent ~ wfh_prop_soc | year_month + fips, data = df_us_fips_ym[year_month >= ymd("20220101")], weights = ~ N, cluster = ~ fips)

fwrite(df_us_fips_ym %>%  .[, year_month := as.Date(as.yearmon(year_month))] %>% .[, year := year(year_month)] %>% .[, month := month(year_month)] %>% select(year_month, year, month, fips, county, state, wfh_percent, N, wfh_prop_soc, wfh_prop_soc_nf),
       "./aux_data/county_by_month.csv")

etable(mod1, mod2, mod3, mod4, mod5, title = "County x Year-Month Results", digits = 2, digits.stats = 3, tex = TRUE)

uniqueN(df_us_fips_ym[year_month >= ymd("20220101")]$fips) # 3222
uniqueN(df_us_fips_ym[year_month >= ymd("20220101")]$year_month) # 16

# Check
check <- df_us %>%
  .[, wfh_N_soc := sum(wfh_wham), by = .(year_month, soc)] %>%
  .[, wfh_N_soc_fips := sum(wfh_wham), by = .(year_month, soc, fips, county, state)] %>%
  .[, N_soc := .N, by = .(year_month, soc)] %>%
  .[, N_soc_fips := .N, by = .(year_month, soc, fips, county, state)] %>%
  .[, wfh_prop_soc := wfh_N_soc/N_soc] %>%
  .[, wfh_prop_soc_nf := (wfh_N_soc - wfh_N_soc_fips)/(N_soc - N_soc_fips)]
  
check <- check %>%
  .[, .(N = .N,
        wfh_percent = mean(wfh_wham, na.rm = T),
        wfh_prop_soc = mean(wfh_prop_soc, na.rm = T),
        wfh_prop_soc_nf = mean(wfh_prop_soc_nf, na.rm = T)),
    by = .(year_month, county, fips, state)]

mod1c <- feols(fml = wfh_percent ~ wfh_prop_soc, data = check, cluster = ~ fips)
mod2c <- feols(fml = wfh_percent ~ wfh_prop_soc, data = check, weights = ~ N, cluster = ~ fips)
mod3c <- feols(fml = wfh_percent ~ wfh_prop_soc_nf, data = check, cluster = ~ fips)
mod4c <- feols(fml = wfh_percent ~ wfh_prop_soc_nf, data = check, weights = ~ N, cluster = ~ fips)

etable(mod1, mod1c, mod2, mod2c, mod3, mod3c, mod4, mod4c)

#### END ####


#### OLD ####






soc2_names <- fread("./aux_data/bls_soc_2010_2_digit_names.csv", header = FALSE)
colnames(soc2_names) <- c("soc_2_digit", "name")
soc2_names <- soc2_names %>% .[, soc_2_digit := str_sub(soc_2_digit, 1, 2)]
soc2_names$soc_2_digit <- as.numeric(soc2_names$soc_2_digit)

nrow(df_us) # 2,558,381
df_us <- df_us %>% left_join(soc2_names)
nrow(df_us) # 2,558,381

colnames(df_us)

df_us <- df_us %>%
  select(year_month, county, fips, state, soc_2_digit, name, N, wfh_percent)

fwrite(df_us, "./aux_data/county_by_month_soc21.csv")

head(df_us)

df_us <- df_us %>%
  setDT(.) %>%
  .[, .(wfh_percent = sum(N*wfh_percent)/sum(N), N = sum(N)), by = .(year_month, county, fips, state)]

nrow(df_us) # 167,931

fwrite(df_us, "./aux_data/county_by_month1.csv")

# Fix year month
remove(list = ls())

df_month_occ <- fread("./aux_data/county_by_month_soc21.csv")
df_month <- fread("./aux_data/county_by_month1.csv")

df_month <- df_month %>%
  .[, year_month := as.Date(as.yearmon(year_month))] %>%
  .[, year := year(year_month)] %>%
  .[, month := month(year_month)] %>%
  select(year_month, year, month, county, fips, state, wfh_percent, N)

df_month_occ <- df_month_occ %>%
  .[, year_month := as.Date(as.yearmon(year_month))] %>%
  .[, year := year(year_month)] %>%
  .[, month := month(year_month)] %>%
  select(year_month, year, month, county, fips, state, soc_2_digit, name, wfh_percent, N)

tail(df_month)
tail(df_month_occ)

fwrite(df_month_occ, "./aux_data/county_by_month_soc2.csv")
fwrite(df_month, "./aux_data/county_by_month.csv")


