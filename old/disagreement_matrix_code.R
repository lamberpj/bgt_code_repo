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

#### USA ####

us_seq_wham <- fread("./int_data/us_seq_level_wfh_measures.csv", select = c("job_id", "wfh_prob", "neg", "dict", "oecd_dict"))
us_seq_wham <- us_seq_wham %>% select(job_id, wfh_prob, neg, dict, oecd_dict)
us_seq_wham$wfh <- as.numeric(us_seq_wham$wfh_prob>0.5)
us_seq_wham$job_id <- as.numeric(us_seq_wham$job_id)

load("./int_data/bgt_structured/us_stru_wfh.RData")
df_all_us <- df_all_us %>% select(job_id, country, year_month)
df_all_us <- df_all_us %>% setDT(.)
df_all_us <- df_all_us %>%
  .[, year := str_sub(year_month, -4, -1)]
df_all_us <- df_all_us %>% select(-year_month)
us_seq_wham <- us_seq_wham %>%
  setDT(.)
us_seq_wham$job_id <- as.numeric(us_seq_wham$job_id) 
df <- merge.data.table(x = us_seq_wham, y = df_all_us)

df_ag <- df %>%
  .[, .(n_d0w0 = sum(oecd_dict == 0 & wfh == 0),
        n_d1w0 = sum(oecd_dict == 1 & wfh == 0),
        n_d0w1 = sum(oecd_dict == 0 & wfh == 1),
        n_d1w1 = sum(oecd_dict == 1 & wfh == 1)),
    by = .(year)]

fwrite(df_ag, "./aux_data/seq_level_agreement_rates_US.csv")

#### END ####

#### UK ####
remove(list = ls())

uk_seq_wham <- fread("./int_data/uk_seq_level_wfh_measures.csv", select = c("job_id", "wfh_prob", "neg", "dict", "oecd_dict"))
uk_seq_wham <- uk_seq_wham %>% select(job_id, wfh_prob, neg, dict, oecd_dict)
uk_seq_wham$wfh <- as.numeric(uk_seq_wham$wfh_prob>0.5)
uk_seq_wham$job_id <- as.numeric(uk_seq_wham$job_id)

load("./int_data/bgt_structured/uk_stru_wfh.RData")
df_all_uk <- df_all_uk %>% select(job_id, country, year_month)
df_all_uk <- df_all_uk %>% setDT(.)
df_all_uk <- df_all_uk %>%
  .[, year := str_sub(year_month, -4, -1)]
df_all_uk <- df_all_uk %>% select(-year_month)
uk_seq_wham <- uk_seq_wham %>%
  setDT(.)
uk_seq_wham$job_id <- as.numeric(uk_seq_wham$job_id) 
df <- merge.data.table(x = uk_seq_wham, y = df_all_uk)

df_ag <- df %>%
  .[, .(n_d0w0 = sum(oecd_dict == 0 & wfh == 0),
        n_d1w0 = sum(oecd_dict == 1 & wfh == 0),
        n_d0w1 = sum(oecd_dict == 0 & wfh == 1),
        n_d1w1 = sum(oecd_dict == 1 & wfh == 1)),
    by = .(year)]

fwrite(df_ag, "./aux_data/seq_level_agreement_rates_uk.csv")

#### END ####

#### CAN ####
remove(list = ls())

can_seq_wham <- fread("./int_data/can_seq_level_wfh_measures.csv", select = c("job_id", "wfh_prob", "neg", "dict", "oecd_dict"))
can_seq_wham <- can_seq_wham %>% select(job_id, wfh_prob, neg, dict, oecd_dict)
can_seq_wham$wfh <- as.numeric(can_seq_wham$wfh_prob>0.5)
can_seq_wham$job_id <- as.numeric(can_seq_wham$job_id)

load("./int_data/bgt_structured/can_stru_wfh.RData")
df_all_can <- df_all_can %>% select(job_id, country, year_month)
df_all_can <- df_all_can %>% setDT(.)
df_all_can <- df_all_can %>%
  .[, year := str_sub(year_month, -4, -1)]
df_all_can <- df_all_can %>% select(-year_month)
can_seq_wham <- can_seq_wham %>%
  setDT(.)
can_seq_wham$job_id <- as.numeric(can_seq_wham$job_id) 
df <- merge.data.table(x = can_seq_wham, y = df_all_can)

df_ag <- df %>%
  .[, .(n_d0w0 = sum(oecd_dict == 0 & wfh == 0),
        n_d1w0 = sum(oecd_dict == 1 & wfh == 0),
        n_d0w1 = sum(oecd_dict == 0 & wfh == 1),
        n_d1w1 = sum(oecd_dict == 1 & wfh == 1)),
    by = .(year)]

fwrite(df_ag, "./aux_data/seq_level_agreement_rates_can.csv")

#### END ####


#### ANZ ####
remove(list = ls())

anz_seq_wham <- fread("./int_data/anz_seq_level_wfh_measures.csv", select = c("job_id", "wfh_prob", "neg", "dict", "oecd_dict"))
anz_seq_wham <- anz_seq_wham %>% select(job_id, wfh_prob, neg, dict, oecd_dict)
anz_seq_wham$wfh <- as.numeric(anz_seq_wham$wfh_prob>0.5)
anz_seq_wham$job_id <- as.numeric(anz_seq_wham$job_id)

load("./int_data/bgt_structured/anz_stru_wfh.RData")
df_all_anz <- df_all_anz %>% select(job_id, country, year_month)
df_all_anz <- df_all_anz %>% setDT(.)
df_all_anz <- df_all_anz %>%
  .[, year := str_sub(year_month, -4, -1)]
df_all_anz <- df_all_anz %>% select(-year_month)
anz_seq_wham <- anz_seq_wham %>%
  setDT(.)
anz_seq_wham$job_id <- as.numeric(anz_seq_wham$job_id) 
df <- merge.data.table(x = anz_seq_wham, y = df_all_anz)

df_ag <- df %>%
  .[, .(n_d0w0 = sum(oecd_dict == 0 & wfh == 0),
        n_d1w0 = sum(oecd_dict == 1 & wfh == 0),
        n_d0w1 = sum(oecd_dict == 0 & wfh == 1),
        n_d1w1 = sum(oecd_dict == 1 & wfh == 1)),
    by = .(year)]

fwrite(df_ag, "./aux_data/seq_level_agreement_rates_anz.csv")

#### END ####

