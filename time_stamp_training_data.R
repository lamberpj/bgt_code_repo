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
df_nz <- fread("./int_data/df_nz_standardised.csv", nThread = 8, integer64 = "numeric", select = c("job_id", "job_date"))
df_aus <- fread("./int_data/df_aus_standardised.csv", nThread = 8, integer64 = "numeric", select = c("job_id", "job_date"))
df_can <- fread("./int_data/df_can_standardised.csv", nThread = 8, integer64 = "numeric", select = c("job_id", "job_date"))
df_uk <- fread("./int_data/df_uk_standardised.csv", nThread = 8, integer64 = "numeric", select = c("job_id", "job_date"))
df_us_2014 <- fread("./int_data/df_us_2014_standardised.csv", nThread = 8, integer64 = "numeric", select = c("job_id", "job_date"))
df_us_2015 <- fread("./int_data/df_us_2015_standardised.csv", nThread = 8, integer64 = "numeric", select = c("job_id", "job_date"))
df_us_2016 <- fread("./int_data/df_us_2016_standardised.csv", nThread = 8, integer64 = "numeric", select = c("job_id", "job_date"))
df_us_2017 <- fread("./int_data/df_us_2017_standardised.csv", nThread = 8, integer64 = "numeric", select = c("job_id", "job_date"))
df_us_2018 <- fread("./int_data/df_us_2018_standardised.csv", nThread = 8, integer64 = "numeric", select = c("job_id", "job_date"))
df_us_2019 <- fread("./int_data/df_us_2019_standardised.csv", nThread = 8, integer64 = "numeric", select = c("job_id", "job_date"))
df_us_2020 <- fread("./int_data/df_us_2020_standardised.csv", nThread = 8, integer64 = "numeric", select = c("job_id", "job_date"))
df_us_2021 <- fread("./int_data/df_us_2021_standardised.csv", nThread = 8, integer64 = "numeric", select = c("job_id", "job_date"))
df_us_2022 <- fread("./int_data/df_us_2022_standardised.csv", nThread = 8, integer64 = "numeric", select = c("job_id", "job_date"))

df_nz <- df_nz %>% setDT(.) %>% .[,country := "NZ"]
df_aus <- df_aus %>% setDT(.) %>% .[,country := "Australia"]
df_can <- df_can %>% setDT(.) %>% .[,country := "Canada"]
df_uk <- df_uk %>% setDT(.) %>% .[,country := "UK"]
df_us_2014 <- df_us_2014 %>% setDT(.) %>% .[,country := "US"]
df_us_2015 <- df_us_2015 %>% setDT(.) %>% .[,country := "US"]
df_us_2016 <- df_us_2016 %>% setDT(.) %>% .[,country := "US"]
df_us_2017 <- df_us_2017 %>% setDT(.) %>% .[,country := "US"]
df_us_2018 <- df_us_2018 %>% setDT(.) %>% .[,country := "US"]
df_us_2019 <- df_us_2019 %>% setDT(.) %>% .[,country := "US"]
df_us_2020 <- df_us_2020 %>% setDT(.) %>% .[,country := "US"]
df_us_2021 <- df_us_2021 %>% setDT(.) %>% .[,country := "US"]
df_us_2022 <- df_us_2022 %>% setDT(.) %>% .[,country := "US"]

df <- rbindlist(list(df_nz,df_aus,df_can,df_uk,df_us_2014,df_us_2015,df_us_2016,df_us_2017,df_us_2018,df_us_2019,df_us_2020,df_us_2021,df_us_2022))

remove(list = setdiff(ls(), "df"))

df <- df %>%
  distinct(job_id, job_date, country)

fwrite(df, file = "./aux_data/all_job_id_date_country.csv.gz")

td_df <- fread("./aux_data/test_seq_ids.csv")
td_df$job_id <- gsub("_.*", "", td_df$seq_id)
td_df$job_id <- as.numeric(td_df$job_id)
df$job_id <- as.numeric(df$job_id)

df_ss <- df %>%
  setDT(.) %>%
  .[job_id %in% td_df$job_id]

df_ss <- df_ss %>%
  left_join(td_df)

table(year(df_ss$job_date))
table(df_ss$country)

fwrite(df_ss, file = "./aux_data/all_job_id_date_country_training_data.csv")




