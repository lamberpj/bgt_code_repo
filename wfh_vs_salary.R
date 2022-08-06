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
library("textclean")
library("quanteda")
#library("readtext")
#library("rvest")
library("xml2")
#library("DescTools")
library("zoo")
#library("lsa")
#library("fuzzyjoin")
library("quanteda")
library("refinr")
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
library("DescTools")
#library("fuzzyjoin")
library("readxl")
# Translation packages
#library("rvest")
#library("googleLanguageR")
#library("cld2")
#library("datasets")
#library("vroom")

library(ggplot2)
library(scales)
library("ggpubr")
#library("devtools")
#devtools::install_github('Mikata-Project/ggthemr')
library("ggthemr")
ggthemr('flat')

quanteda_options(threads = 16)
setwd("/mnt/disks/pdisk/bg-uk/")
setDTthreads(1)

remove(list = ls())

#### INCOME REGS UK ####
df_stru_uk <- readRDS(file = "/mnt/disks/pdisk/bg-uk/int_data/df_stru.rds")
df_stru_us <- readRDS(file = "/mnt/disks/pdisk/bg-us/int_data/df_stru.rds")

fwrite(df_stru_uk, file = "/mnt/disks/pdisk/bg-uk/int_data/df_stru.csv")
fwrite(df_stru_us, file = "/mnt/disks/pdisk/bg-us/int_data/df_stru.csv")

df_stru_uk <- df_stru_uk %>% mutate(ln_salary = Winsorize(log(as.numeric(max_annual_salary)),na.rm = T))
df_stru_us <- df_stru_us %>% mutate(ln_salary = Winsorize(log(as.numeric(max_salary)),na.rm = T))

df_stru_uk <- df_stru_uk %>% mutate(dum21 = ifelse(grepl("2021", year_month), 1, 0))
df_stru_us <- df_stru_us %>% mutate(dum21 = ifelse(grepl("2021", year_month), 1, 0))

df_stru_uk <- df_stru_uk %>% mutate(dum20 = ifelse(grepl("2020", year_month), 1, 0))
df_stru_us <- df_stru_us %>% mutate(dum20 = ifelse(grepl("2020", year_month), 1, 0))

df_stru_uk <- df_stru_uk %>% mutate(dum19 = ifelse(grepl("2019", year_month), 1, 0))
df_stru_us <- df_stru_us %>% mutate(dum19 = ifelse(grepl("2019", year_month), 1, 0))

df_stru_uk <- df_stru_uk %>% mutate(dum14_18 = ifelse(grepl("2014|2015|2016|2017|2018", year_month), 1, 0))
df_stru_us <- df_stru_us %>% mutate(dum14_18 = ifelse(grepl("2014|2015|2016|2017|2018", year_month), 1, 0))

remove(list = setdiff(ls(),c("df_stru_uk","df_stru_us")))

test_uk <- df_stru_uk %>% filter(!is.na(ln_salary) & is.finite(ln_salary) & !is.na(canon_county) & !is.na(bgt_occ) & !is.na(sic_section)) %>% mutate(salary = exp(ln_salary))
test_us <- df_stru_us %>% filter(!is.na(ln_salary) & is.finite(ln_salary) & !is.na(county) & !is.na(bgt_occ) & !is.na(sector_name)) %>% mutate(salary = exp(ln_salary))

test_uk$salary[1:100]

stargazer(data.frame("Salary (USD)" = test_us$salary), summary = T, summary.stat = c("min", "p25", "median", "p75", "max"))




head(df_stru_uk)

# UK
model_uk_fes1 <- feols(data = df_stru_uk %>% filter(!is.na(ln_salary) & is.finite(ln_salary) & !is.na(canon_county) & !is.na(bgt_occ) & !is.na(sic_section)), 
                    fml = ln_salary ~ wfh_w_nn:dum21 + wfh_w_nn:dum20 + wfh_w_nn:dum19 + wfh_w_nn:dum14_18 | year_quarter,
                    cluster = ~ year_quarter, lean = T)

model_uk_fes2 <- feols(data = df_stru_uk %>% filter(!is.na(ln_salary) & is.finite(ln_salary) & !is.na(canon_county) & !is.na(bgt_occ) & !is.na(sic_section)), 
                    fml = ln_salary ~ wfh_w_nn:dum21 + wfh_w_nn:dum20 + wfh_w_nn:dum19 + wfh_w_nn:dum14_18 | bgt_occ + sic_section + year_quarter,
                    cluster = ~ year_quarter, lean = T)

model_uk_fes3 <- feols(data = df_stru_uk %>% filter(!is.na(ln_salary) & is.finite(ln_salary) & !is.na(canon_county) & !is.na(bgt_occ) & !is.na(sic_section)), 
                    fml = ln_salary ~ wfh_w_nn:dum21 + wfh_w_nn:dum20 + wfh_w_nn:dum19 + wfh_w_nn:dum14_18 | canon_county + bgt_occ + sic_section + year_quarter,
                    cluster = ~ year_quarter, lean = T)

model_uk_fes4 <- feols(data = df_stru_uk, 
                    fml = ln_salary ~ wfh_w_nn:dum21 + wfh_w_nn:dum20 + wfh_w_nn:dum19 + wfh_w_nn:dum14_18 | canon_employer + year_quarter,
                    cluster = ~ year_quarter, lean = T)

# US
model_us_fes1 <- feols(data = df_stru_us %>% filter(!is.na(ln_salary) & is.finite(ln_salary) & !is.na(county) & !is.na(bgt_occ) & !is.na(sector_name)), 
                    fml = ln_salary ~ wfh_w_nn:dum21 + wfh_w_nn:dum20 + wfh_w_nn:dum19 + wfh_w_nn:dum14_18 | year_quarter,
                    cluster = ~ year_quarter, lean = T)

model_us_fes2 <- feols(data = df_stru_us %>% filter(!is.na(ln_salary) & is.finite(ln_salary) & !is.na(county) & !is.na(bgt_occ) & !is.na(sector_name)), 
                    fml = ln_salary ~ wfh_w_nn:dum21 + wfh_w_nn:dum20 + wfh_w_nn:dum19 + wfh_w_nn:dum14_18 | bgt_occ + sector_name + year_quarter,
                    cluster = ~ year_quarter, lean = T)

model_us_fes3 <- feols(data = df_stru_us %>% filter(!is.na(ln_salary) & is.finite(ln_salary) & !is.na(county) & !is.na(bgt_occ) & !is.na(sector_name)), 
                    fml = ln_salary ~ wfh_w_nn:dum21 + wfh_w_nn:dum20 + wfh_w_nn:dum19 + wfh_w_nn:dum14_18 | county + bgt_occ + sector_name + year_quarter,
                    cluster = ~ year_quarter, lean = T)

model_us_fes4 <- feols(data = df_stru_us, 
                    fml = ln_salary ~ wfh_w_nn:dum21 + wfh_w_nn:dum20 + wfh_w_nn:dum19 + wfh_w_nn:dum14_18 | employer + year_quarter,
                    cluster = ~ year_quarter, lean = T)

etable(model_uk_fes1, model_uk_fes2, model_uk_fes3, model_uk_fes4, model_us_fes1, model_us_fes2, model_us_fes3, model_us_fes4, title = "Income on WFH", tex = T, digits = 2, digits.stats = 3)

# UK
model_uk_fes5 <- feols(data = df_stru_uk %>% filter(!is.na(ln_salary) & is.finite(ln_salary) & !is.na(canon_county) & !is.na(bgt_occ) & !is.na(sic_section)), 
                       fml = ln_salary ~ wfh_w_nn:dum21 + wfh_w_nn:dum20 + wfh_w_nn:dum19 + wfh_w_nn:dum14_18 | canon_county + bgt_occ + sic_section + year_quarter + canon_employer,
                       cluster = ~ year_quarter, lean = T)

model_uk_fes6 <- feols(data = df_stru_uk %>% filter(!is.na(ln_salary) & is.finite(ln_salary) & !is.na(canon_county) & !is.na(bgt_occ) & !is.na(sic_section)), 
                       fml = ln_salary ~ wfh_w_nn:dum21 + wfh_w_nn:dum20 + wfh_w_nn:dum19 + wfh_w_nn:dum14_18 | canon_county^bgt_occ^sic_section^year_quarter,
                       cluster = ~ year_quarter, lean = T)

# US
model_us_fes5 <- feols(data = df_stru_us %>% filter(!is.na(ln_salary) & is.finite(ln_salary) & !is.na(county) & !is.na(bgt_occ) & !is.na(sector_name)), 
                       fml = ln_salary ~ wfh_w_nn:dum21 + wfh_w_nn:dum20 + wfh_w_nn:dum19 + wfh_w_nn:dum14_18 | county + bgt_occ + sector_name + year_quarter + employer,
                       cluster = ~ year_quarter, lean = T)

model_us_fes6 <- feols(data = df_stru_us %>% filter(!is.na(ln_salary) & is.finite(ln_salary) & !is.na(county) & !is.na(bgt_occ) & !is.na(sector_name)), 
                       fml = ln_salary ~ wfh_w_nn:dum21 + wfh_w_nn:dum20 + wfh_w_nn:dum19 + wfh_w_nn:dum14_18 | county^bgt_occ^sector_name^year_quarter,
                       cluster = ~ year_quarter, lean = T)

etable(model_uk_fes5, model_uk_fes6, model_us_fes5, model_us_fes6, title = "Income on WFH", tex = T, digits = 2, digits.stats = 3)



#### END ####

#### INCOME REGS UK ####
df_stru_uk <- readRDS(file = "/mnt/disks/pdisk/bg-uk//int_data/df_stru.rds")

head(df_stru_uk)

model <- feols(data = df_stru_uk, 
               fml = ln_salary ~ wfh_w_nn | region + uksoc_code + year_quarter,
               cluster = ~ year_quarter, lean = T)

summary(model)

#### END ####