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

setDTthreads(8)
getDTthreads()
#quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg_combined/")

colnames(fread("../bg-us/int_data/us_stru_2019_wfh.csv", nThread = 8, nrows = 1000))

df_us_2022 <- fread("../bg-us/int_data/us_stru_2022_wfh.csv", nThread = 8, integer64 = "numeric", select = c("employer","naics4","wfh_wham", "job_url", "soc"))

nrow(df_us_2022) # 33,759,106
df_us_2022 <- df_us_2022 %>%
  .[!grepl("careerbuilder", job_url)]
nrow(df_us_2022) # 29,017,592

df_us_2022_firms <- df_us_2022 %>%
  .[employer != "" & !is.na(employer)] %>%
  .[soc != "" & !is.na(soc)] %>%
  .[, .(.N, naics4 = median(naics4, na.rm = T), wfh_share = mean(wfh_wham, na.rm = T)), by = employer] %>%
  .[order(-N)] %>%
  .[N >= 500]

fwrite(df_us_2022_firms, "./aux_data/extract_for_scoop.csv")

df_us_2022_firms_soc3 <- df_us_2022 %>%
  .[employer != "" & !is.na(employer)] %>%
  .[soc != "" & !is.na(soc)] %>%
  .[, soc3 := str_sub(soc, 1, 4)] %>%
  .[employer %in% df_us_2022_firms$employer] %>%
  .[order(match(employer,df_us_2022_firms$employer))] %>%
  .[, .(.N, naics4 = median(naics4, na.rm = T), wfh_share = mean(wfh_wham, na.rm = T)), by = .(employer, soc3)]

fwrite(df_us_2022_firms_soc3, "./aux_data/extract_for_scoop_soc3.csv")

nrow(df_us_2022_firms)

View(df_us_2022_firms)

