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

# LOAD SEQUENCE PATHS #

colnames(df)

clean_sequences <- function(seq_vec){
  seq_vec <- tolower(seq_vec)
  seq_vec <- gsub("@\\w+", "", seq_vec)
  seq_vec <- gsub("https?://.+", "", seq_vec)
  seq_vec <- gsub("\\d+\\w*\\d*", "", seq_vec)
  seq_vec <- gsub("#\\w+", "", seq_vec)
  seq_vec <- gsub("[^\x01-\x7F]", "", seq_vec)
  seq_vec <- gsub("[[:punct:]]", " ", seq_vec)
  seq_vec <- gsub("\n", " ", seq_vec)
  seq_vec <- gsub("^\\s+", "", seq_vec)
  seq_vec <- gsub("\\s+$", "", seq_vec)
  seq_vec <- gsub("[ |\t]+", " ", seq_vec)
  return(seq_vec)
}

# Read in dictionary files
dict_narrow_df <- read_xlsx("../bg-us/aux_data/wfh_v8.xlsx", sheet = "WFH") %>%
  mutate(glob_en = clean_sequences(glob_en)) %>%
  filter(dictionary == "narrow") %>%
  select(glob_en) %>%
  distinct()

dict_neg_df <- read_xlsx("../bg-us/aux_data/wfh_v8.xlsx", sheet = "Negation") %>%
  mutate(glob_en = clean_sequences(glob_en)) %>%
  select(glob_en) %>% distinct

# Create dictionary objects
(narrow_dict <- paste(dict_narrow_df$glob_en, collapse = "|") %>% paste0("(", ., ")"))
(neg_dict <- paste(dict_neg_df$glob_en, collapse = "|") %>% paste0("(", ., ")"))

# Load sequences(s)
paths <- list.files("../bg-us/int_data/sequences", full.names = T)
names <- list.files("../bg-us/int_data/sequences", full.names = F)

paths <- paths[grepl("2019|2020|2021|2022", paths)]
names <- names[grepl("2019|2020|2021|2022", names)]

df <- readRDS(paths[1]) %>%
  select(-c(nfeat, nchar)) %>%
  setDT(.)

df <- df %>% .[, sequence_clean := clean_sequences(sequence)]
df <- df[, dict := str_detect(sequence_clean, narrow_dict)]
sum(df$dict)
df <- df[, dict_nn := ifelse(dict, !((str_detect(sequence_clean, regex(paste0("(?<=", neg_dict, ")\\s(?:\\w+\\s){0,3}(?=", narrow_dict, ")")))) | (str_detect(sequence_clean, regex(paste0("(?<=", narrow_dict, ")\\s(?:\\w+\\s){0,2}(?=(no|not))"))))), FALSE)]
sum(df$dict != df$dict_nn)





