#### SETUP ####
remove(list = ls())

options(repos = c(CRAN = "https://mran.revolutionanalytics.com/snapshot/2021-09-01"))

options(scipen=999)

#install.packages("corpus")
library("data.table")
library("tidyverse")
library("janitor")
library("lubridate")
#library("dplyr")
#library("stringr")
library("doParallel")
#library("textclean")
#library("quanteda")
#library("readtext")
#library("rvest")
#library("xml2")
#library("DescTools")
library("zoo")
#library("lsa")
#library("fuzzyjoin")
#library("refinr")
#library("FactoMineR")
#library("ggpubr")
#library("scales")
#install.packages("ggthemr")
#ggthemr('flat')
library("fixest")
#library("lfe")
library("stargazer")
#library("texreg")
#library("sjPlot")
#library("margins")
#library("DescTools")
#library("fuzzyjoin")
library("readxl")
# Translation packages
#library("rvest")
#library("googleLanguageR")
#library("cld2")
#library("datasets")
#library("vroom")
library("Hmisc")

library(ggplot2)
library(scales)
library("ggpubr")
#library("devtools")
#devtools::install_github('Mikata-Project/ggthemr')
library("ggthemr")
ggthemr('flat')
library("stats")

#quanteda_options(threads = 16)
setwd("/mnt/disks/pdisk/bg-uk/")
setDTthreads(6)

remove(list = ls())

#### LAOD DATA ####
remove(list = ls())
setwd("/mnt/disks/pdisk/bg-us/")

df_stru_us_sum <- fread("./int_data/df_stru.csv", select = c("job_date","bgt_occ","state", "county", "wfh_nn","wfh_w_nn"), nThread = 6) %>%
  mutate(year_quarter = as.yearqtr(ymd(job_date))) %>%
  mutate(year_month = as.yearmon(ymd(job_date))) %>%
  mutate(soc5_us = str_sub(bgt_occ, 1, 6)) %>%
  group_by(year_quarter, year_month, soc5_us, state, county) %>%
  summarise(n = n(),
            wfh_nn = sum(wfh_nn)/n(),
            wfh_w_nn = sum(wfh_w_nn)/n())
df_stru_us_sum <- df_stru_us_sum %>% rename(region = state)
df_stru_us_sum <- df_stru_us_sum %>% mutate(source = "USA")

setwd("/mnt/disks/pdisk/bg-uk/")
colnames(fread("./int_data/df_stru.csv", nrow = 100))
df_stru_uk_sum <- fread("./int_data/df_stru.csv", select = c("job_date","bgt_occ","nation", "canon_county", "wfh_nn","wfh_w_nn"), nThread = 6) %>%
  rename(county = canon_county) %>%
  mutate(year_quarter = as.yearqtr(ymd(job_date))) %>%
  mutate(year_month = as.yearmon(ymd(job_date))) %>%
  mutate(soc5_us = str_sub(bgt_occ, 1, 6)) %>%
  group_by(year_quarter, year_month, soc5_us, nation, county) %>%
  summarise(n = n(),
            wfh_nn = sum(wfh_nn)/n(),
            wfh_w_nn = sum(wfh_w_nn)/n())
df_stru_uk_sum <- df_stru_uk_sum %>% rename(region = nation)
df_stru_uk_sum <- df_stru_uk_sum %>% mutate(source = "UK")

df_stru_sum <- bind_rows(df_stru_uk_sum, df_stru_us_sum)

nrow(df_stru_sum) # 10,447,372
df_stru_sum <- df_stru_sum %>% filter(soc5_us != "" & county != "")
nrow(df_stru_sum) # 10,296,939

df_stru_sum <- df_stru_sum %>% ungroup() %>% mutate(year = as.numeric(str_sub(year_quarter, 1, 4)))
head(df_stru_sum)

df_stru_sum <- df_stru_sum %>% mutate(region_county = paste0(region,"_",county))
df_stru_sum <- df_stru_sum %>% mutate(region_county_soc5_us = paste0(region,"_",county,"_",soc5_us))

head(df_stru_sum)
summary(df_stru_sum$wfh_nn)

#### TECHNOLOGY ####
# ADDITIVE
vd1_2019 <- feols(data = df_stru_sum %>% filter(year == 2019), weights = ~ n,
                  fml = wfh_nn ~ -1 | year_month + region_county + soc5_us)

df_year_month <- fixef(vd1_2019)[[1]] %>% as.data.frame() %>% rownames_to_column("year_month") %>% rename(year_month_fe = ".") %>% mutate(year_month = as.yearmon(year_month))
df_county <- fixef(vd1_2019)[[2]] %>% as.data.frame() %>% rownames_to_column("region_county") %>% rename(county_fe = ".")
df_soc5_us <- fixef(vd1_2019)[[3]] %>% as.data.frame() %>% rownames_to_column("soc5_us") %>% rename(soc5_us_fe = ".")

df <- df_stru_sum %>% filter(year == 2019) %>%
  left_join(df_year_month) %>%
  left_join(df_county) %>%
  left_join(df_soc5_us) %>%
  mutate(residuals = vd1_2019$residuals)

head(df)

wtd.var(df$year_month_fe, df$n)/wtd.var(df$wfh_nn, df$n)
wtd.var(df$county_fe, df$n)/wtd.var(df$wfh_nn, df$n)
wtd.var(df$soc5_us_fe, df$n)/wtd.var(df$wfh_nn, df$n)

vd1_2021 <- feols(data = df_stru_sum %>% filter(year == 2021), weights = ~ n,
                  fml = wfh_nn ~ -1 | year_month + region_county + soc5_us)

df_year_month <- fixef(vd1_2021)[[1]] %>% as.data.frame() %>% rownames_to_column("year_month") %>% rename(year_month_fe = ".") %>% mutate(year_month = as.yearmon(year_month))
df_county <- fixef(vd1_2021)[[2]] %>% as.data.frame() %>% rownames_to_column("region_county") %>% rename(county_fe = ".")
df_soc5_us <- fixef(vd1_2021)[[3]] %>% as.data.frame() %>% rownames_to_column("soc5_us") %>% rename(soc5_us_fe = ".")

df <- df_stru_sum %>% filter(year == 2021) %>%
  left_join(df_year_month) %>%
  left_join(df_county) %>%
  left_join(df_soc5_us) %>%
  mutate(residuals = vd1_2021$residuals)

head(df)

wtd.var(df$year_month_fe, df$n)/wtd.var(df$wfh_nn, df$n) # 0.03717896
wtd.var(df$county_fe, df$n)/wtd.var(df$wfh_nn, df$n) # 0.04931123
wtd.var(df$soc5_us_fe, df$n)/wtd.var(df$wfh_nn, df$n) # 0.2439197

ss <- data.frame("Year" = c(2019, 2021), "Month FEs" = 100*c(0.0003789958, 0.0371811), "County FEs" = 100*c(0.06300912, 0.05679788), "SOC5 FEs" = 100*c(0.08922197, 0.2409632))

stargazer(ss, summary = F, title = "Variance Decomposition Pre- and Post-COVID", rownames = F)

### INTERACTED
vd1_2019 <- feols(data = df_stru_sum %>% filter(year == 2019), weights = ~ n,
                  fml = wfh_nn ~ -1 | year_month + region_county_soc5_us)

df_year_month <- fixef(vd1_2019)[[1]] %>% as.data.frame() %>% rownames_to_column("year_month") %>% rename(year_month_fe = ".") %>% mutate(year_month = as.yearmon(year_month))
df_county_soc5_us <- fixef(vd1_2019)[[2]] %>% as.data.frame() %>% rownames_to_column("region_county_soc5_us") %>% rename(region_county_soc5_us_fe = ".")

df <- df_stru_sum %>% filter(year == 2019) %>%
  left_join(df_year_month) %>%
  left_join(df_county_soc5_us) %>%
  mutate(residuals = vd1_2019$residuals)

wtd.var(df$year_month_fe, df$n)/wtd.var(df$wfh_nn, df$n) # 0.0003322594
wtd.var(df$region_county_soc5_us_fe, df$n)/wtd.var(df$wfh_nn, df$n) # 0.4620807

vd1_2021 <- feols(data = df_stru_sum %>% filter(year == 2021), weights = ~ n,
                  fml = wfh_nn ~ -1 | year_month + region_county_soc5_us)

df_year_month <- fixef(vd1_2021)[[1]] %>% as.data.frame() %>% rownames_to_column("year_month") %>% rename(year_month_fe = ".") %>% mutate(year_month = as.yearmon(year_month))
df_county_soc5_us <- fixef(vd1_2021)[[2]] %>% as.data.frame() %>% rownames_to_column("region_county_soc5_us") %>% rename(region_county_soc5_us_fe = ".")

df <- df_stru_sum %>% filter(year == 2021) %>%
  left_join(df_year_month) %>%
  left_join(df_county_soc5_us) %>%
  mutate(residuals = vd1_2021$residuals)

summary(df)

summary(vd1_2021)

wtd.var(df$year_month_fe, df$n)/wtd.var(df$wfh_nn, df$n) # 0.03691335
wtd.var(df$region_county_soc5_us_fe, df$n)/wtd.var(df$wfh_nn, df$n) # 0.5454674

ss <- data.frame("Year" = c(2019, 2021), "Month FEs" = 100*c(0.0003322594, 0.03691335), "County FEs x SOC 5" = 100*c(0.4620807, 0.5454674))

stargazer(ss, summary = F, title = "Variance Decomposition Pre- and Post-COVID", rownames = F)

### AR2 ##
vd1_2019 <- feols(data = df_stru_sum %>% filter(year == 2019), weights = ~ n,
                  fml = wfh_nn ~ as.character(year_month) | region_county)

vd2_2019 <- feols(data = df_stru_sum %>% filter(year == 2019), weights = ~ n,
                  fml = wfh_nn ~ as.character(year_month) | soc5_us)

vd3_2019 <- feols(data = df_stru_sum %>% filter(year == 2019), weights = ~ n,
                  fml = wfh_nn ~ as.character(year_month) | region_county + soc5_us)

vd4_2019 <- feols(data = df_stru_sum %>% filter(year == 2019), weights = ~ n,
                  fml = wfh_nn ~ as.character(year_month) | region_county_soc5_us)

vd1_2021 <- feols(data = df_stru_sum %>% filter(year == 2021), weights = ~ n,
                  fml = wfh_nn ~ as.character(year_month) | region_county)

vd2_2021 <- feols(data = df_stru_sum %>% filter(year == 2021), weights = ~ n,
                  fml = wfh_nn ~ as.character(year_month) | soc5_us)

vd3_2021 <- feols(data = df_stru_sum %>% filter(year == 2021), weights = ~ n,
                  fml = wfh_nn ~ as.character(year_month) | region_county + soc5_us)

vd4_2021 <- feols(data = df_stru_sum %>% filter(year == 2021), weights = ~ n,
                  fml = wfh_nn ~ as.character(year_month) | region_county_soc5_us)


etable(vd1_2019, vd2_2019, vd3_2019, vd4_2019, title = "2019 FE R-Square", tex = T, fitstat=c('r2', 'ar2'))
etable(vd1_2021, vd2_2021, vd3_2021, vd4_2021, title = "2021 FE R-Square", tex = T, fitstat=c('r2', 'ar2'))

### DISAG RSQUARED ###

#### LAOD DATA ####
remove(list = ls())
setwd("/mnt/disks/pdisk/bg-us/")

df_stru_us_sum <- fread("./int_data/df_stru.csv", select = c("job_date","bgt_occ","state", "county", "wfh_nn","wfh_w_nn"), nThread = 6) %>%
  mutate(year = year(ymd(job_date))) %>%
  filter(year %in% c(2019, 2021)) %>%
  mutate(year_quarter = as.yearqtr(ymd(job_date))) %>%
  mutate(year_month = as.yearmon(ymd(job_date))) %>%
  mutate(soc5_us = str_sub(bgt_occ, 1, 6)) %>%
  select(year, year_quarter, year_month, soc5_us, state, county, wfh_nn) %>%
  rename(region = state) %>%
  mutate(source = "USA")

head(df_stru_us_sum)


setwd("/mnt/disks/pdisk/bg-uk/")
df_stru_uk_sum <- fread("./int_data/df_stru.csv", select = c("job_date","bgt_occ","nation", "canon_county", "wfh_nn","wfh_w_nn"), nThread = 6) %>%
  mutate(year = year(ymd(job_date))) %>%
  filter(year %in% c(2019, 2021)) %>%
  rename(county = canon_county) %>%
  mutate(year_quarter = as.yearqtr(ymd(job_date))) %>%
  mutate(year_month = as.yearmon(ymd(job_date))) %>%
  mutate(soc5_us = str_sub(bgt_occ, 1, 6)) %>%
  select(year, year_quarter, year_month, soc5_us, nation, county, wfh_nn) %>%
  rename(region = nation) %>%
  mutate(source = "UK")

head(df_stru_uk_sum)

df_stru_sum <- bind_rows(df_stru_uk_sum, df_stru_us_sum) %>% ungroup()

head(df_stru_sum)

nrow(df_stru_sum) # 35246773
df_stru_sum <- df_stru_sum %>% filter(soc5_us != "" & county != "")
nrow(df_stru_sum) # 33401805

remove(list = c("df_stru_uk_sum","df_stru_us_sum"))

df_stru_sum <- df_stru_sum %>% ungroup()
df_stru_sum <- df_stru_sum %>% mutate(region_county = paste0(region,"_",county))
df_stru_sum <- df_stru_sum %>% mutate(region_county_soc5_us = paste0(region,"_",county,"_",soc5_us))

head(df_stru_sum)

### AR2 ##
vd1_2019 <- feols(data = df_stru_sum %>% filter(year == 2019),
                  fml = wfh_nn ~ as.character(year_month) | region_county)

vd2_2019 <- feols(data = df_stru_sum %>% filter(year == 2019),
                  fml = wfh_nn ~ as.character(year_month) | soc5_us)

vd3_2019 <- feols(data = df_stru_sum %>% filter(year == 2019),
                  fml = wfh_nn ~ as.character(year_month) | region_county + soc5_us)

vd4_2019 <- feols(data = df_stru_sum %>% filter(year == 2019),
                  fml = wfh_nn ~ as.character(year_month) | region_county_soc5_us)

vd1_2021 <- feols(data = df_stru_sum %>% filter(year == 2021),
                  fml = wfh_nn ~ as.character(year_month) | region_county)

vd2_2021 <- feols(data = df_stru_sum %>% filter(year == 2021),
                  fml = wfh_nn ~ as.character(year_month) | soc5_us)

vd3_2021 <- feols(data = df_stru_sum %>% filter(year == 2021),
                  fml = wfh_nn ~ as.character(year_month) | region_county + soc5_us)

vd4_2021 <- feols(data = df_stru_sum %>% filter(year == 2021),
                  fml = wfh_nn ~ as.character(year_month) | region_county_soc5_us)


etable(vd1_2019, vd2_2019, vd3_2019, vd4_2019, title = "2019 FE R-Square", tex = T, fitstat=c('r2', 'ar2'))
etable(vd1_2021, vd2_2021, vd3_2021, vd4_2021, title = "2021 FE R-Square", tex = T, fitstat=c('r2', 'ar2'))

cor3_df <- fixef(vd3_2021)[[1]] %>% as.data.frame() %>% rownames_to_column("region_county") %>% rename(region_county_fe_2019 = ".") %>%
  left_join(fixef(vd3_2021)[[1]] %>% as.data.frame() %>% rownames_to_column("region_county") %>% rename(region_county_fe_2021 = "."))

cor2_df <- fixef(vd1_2019)[[1]] %>% as.data.frame() %>% rownames_to_column("region_county") %>% rename(region_county_fe_2019 = ".") %>%
  left_join(fixef(vd1_2021)[[1]] %>% as.data.frame() %>% rownames_to_column("region_county") %>% rename(region_county_fe_2021 = "."))


