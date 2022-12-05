#### CLEAR ENVIRONMENT ####
remove(list=ls())
options(repos = c(CRAN = "https://mran.revolutionanalytics.com/snapshot/2021-09-01"))
options(scipen=999)

#### PACKAGES ####
library("janitor")
library("vroom")
#library("usethis")
#library("devtools")
library("tidyverse")
library("haven")
library("stringi")
library("stringr")
#library("gtools")
#library("foreign")
library("data.table")
#library("readr")
library("lubridate")
library("data.table")
library("foreach")
library("doParallel")
#library("rlang")
#library("fixest")
#library("modelsummary")
#library("kableExtra")
#library("DescTools")
library("ggpubr")
library(ggplot2)
library(scales)
#devtools::install_github('Mikata-Project/ggthemr')
library(ggthemr)
library(refinr)

remove(list = ls())
setwd("/mnt/disks/pdisk/bg-us/")

#### LOAD SENTENCES ####

df <- fread("./aux_data/bert_examples/supervised_candidates.txt", data.table = F, colClasses = "character", stringsAsFactors = FALSE)
stri_enc_mark(df$sentence)
df$sentence <- stri_encode(df$sentence,"UTF-8", "ASCII",to_raw=FALSE)
df$sentence <- gsub("[^[[:punct:]][:alnum:][:space:]]","", df$sentence)
df$sentence <- gsub("\\*", "", df$sentence)
df$sentence <- gsub("\n", ".  ", df$sentence)
df$sentence <- str_trim(df$sentence, side = "both") %>% str_squish(.)

df$sent_clust <- n_gram_merge(vect = gsub("^[:alnum:][:space:]", "", df$sentence), bus_suffix = F, numgram = 2, edit_threshold = 10)

df <- df %>%
  group_by(sentence) %>%
  mutate(n_sent = n()) %>%
  group_by(sent_clust) %>%
  mutate(n_cl_sent = n()) %>%
  ungroup

df$job_id <- as.numeric(df$job_id)
class(df$job_id)
#### LOAD in COVARIATES ####
paths <- list.files("./raw_data/main", pattern = ".txt", full.names = T)
#colnames(fread(paths[1], nrow = 10))
df_stru <- safe_mclapply(1:length(paths), function(i) {
  warning(paste0("\nSTART: ",i))
  df <- fread(paths[i], data.table = F, nThread = 1, colClasses = "character", stringsAsFactors = FALSE,
              select = c("BGTJobId", "JobDate", "SOC", "ONET", "State", "City", "County", "MSA", "SectorName", "NAICS3", "NAICS4", "Employer")) %>%
    clean_names %>%
    mutate(bgt_job_id = as.numeric(bgt_job_id)) %>%
    filter(bgt_job_id %in% df$job_id)
  warning(paste0("\nDONE: ",i))
  return(df)
}, mc.cores = 32)

df_stru1 <- df_stru %>%
  bind_rows %>%
  distinct(bgt_job_id, .keep_all = T)

nrow(df_stru1) # 17,612
View(head(df_stru1))
#### MERGE IN ###
df <- df %>%
  left_join(df_stru1, by = c("job_id" = "bgt_job_id"))

df[df == "na"] <- NA
df[df == "-999"] <- NA

df <- df %>%
  arrange(job_id, WFH_prob) %>%
  group_by(job_id) %>%
  mutate(sentence_id = paste0(job_id,"-",sprintf("%04d", row_number()))) %>%
  ungroup

df <- df %>%
  select(sentence_id,job_id,job_date,soc,onet,state,city,county,msa,sector_name,naics3,naics4,employer, sentence,sent_clust,WFH_prob,n_sent,n_cl_sent)

fwrite(df, "./aux_data/bert_examples/full_clustered_merged.csv")

df_unique <- df %>%
  group_by(sent_clust) %>%
  filter(row_number() == 1)

fwrite(df_unique, "./aux_data/bert_examples/full_clustered_merged_unique.csv")

#### SUBET to ignore HEALTHCARE and SOCIAL WORK industries and occupations ####

df_unique <- fread("./aux_data/bert_examples/full_clustered_merged_unique.csv")
nrow(df_unique) # 9,833
df_unique_ss <- df_unique %>% filter(!(sector_name %in% c("Health Care and Social Assistance", "Transportation and Warehousing")) & !(str_sub(soc, 1, 2) %in% c("29", "31", "21", "45", "47", "49")) & !(soc %in% c("41-9020")))
nrow(df_unique_ss) # 8,417
df_unique_ss <- df_unique_ss %>% filter(!grepl("community", sentence, ignore.case = T))

View(df_unique_ss)

fwrite(df_unique_ss, "./aux_data/bert_examples/full_clustered_merged_unique_subset.csv")
