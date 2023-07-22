#### SETUP ####
remove(list = ls())

library("devtools")
install_github("trinker/textclean")
library("textclean")
library("data.table")
library("tidyverse")
library("janitor")
library("lubridate")
library("doParallel")
#library("textclean")
#install.packages("qdapRegex")
library("quanteda")
library("tokenizers")
library("stringi")
#library("readtext")
library("rvest")
library("xml2")
#library("DescTools")
library("zoo")
library("stargazer")
library("readxl")
library("ggplot2")
library("scales")
library("ggpubr")
library("ggthemr")
ggthemr('flat')
library(egg)
library(extrafont)
# font_import()
#loadfonts(device="postscript")
#fonts()

setDTthreads(1)
getDTthreads()
quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg_combined/")


# Australian & NZ Duplicate Sequences
df_anz <- fread("./int_data/anz_seq_level_wfh_measures.csv")
df_anz <- df_anz %>%
  .[wfh_prob>0.5] %>%
  .[, .N, by = sequence] %>%
  .[, share := N/sum(N)] %>%
  .[order(N,decreasing=TRUE)] %>%
  .[1:100]

# Canada
df_can <- fread("./int_data/can_seq_level_wfh_measures.csv")
df_can <- df_can %>%
  .[wfh_prob>0.5] %>%
  .[, .N, by = sequence] %>%
  .[, share := N/sum(N)] %>%
  .[order(N,decreasing=TRUE)] %>%
  .[1:100]

# UK
df_uk <- fread("./int_data/uk_seq_level_wfh_measures.csv")
df_uk <- df_uk %>%
  .[wfh_prob>0.5] %>%
  .[, .N, by = sequence] %>%
  .[, share := N/sum(N)] %>%
  .[order(N,decreasing=TRUE)] %>%
  .[1:100]

# US
df_us <- fread("./int_data/us_seq_level_wfh_measures.csv")
df_us <- df_us %>%
  .[wfh_prob>0.5] %>%
  .[, .N, by = sequence] %>%
  .[, share := N/sum(N)] %>%
  .[order(N,decreasing=TRUE)] %>%
  .[1:100]