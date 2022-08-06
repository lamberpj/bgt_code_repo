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

setDTthreads(1)
getDTthreads()
quanteda_options(threads = 1)
#setDTthreads(1)
set.seed(123)

#### MAKE SUBSET FOR LABELS - USA ####
setwd("/mnt/disks/pdisk/bg-us/")
source("/mnt/disks/pdisk/code/safe_mclapply.R")
paths <- list.files("./int_data/wfh_v6/", pattern = "wfh_v6_raw_sequences", full.names = T) %>% sort(decreasing = T)
# WFH Examples
wfh_v6 <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    mutate(across(x100_percent_remote:browse, ~replace(., is.na(.), 0))) %>%
    mutate(across(x100_percent_remote:browse, ~ifelse(.>0, 1, 0)))
  df <- df %>% setDT(.)
  df$comb_wfh <- pmin(rowSums(df[, select(.SD, x100_percent_remote:working_virtually)]),1)
  df$comb_negated <- pmin(rowSums(df[, select(.SD, cant:wont_2)]),1)
  df <- df %>% .[comb_wfh == 1 & comb_negated == 0]
  df <- df %>% sample_n(., pmin(nrow(.), 500))
  return(df)
}, mc.cores = 20)
wfh_v6 <- rbindlist(wfh_v6, fill = T)
wfh_v6 <- wfh_v6 %>%
  select(job_id, seq_id, sequence, x100_percent_remote:working_virtually)
wfh_v6_long <- wfh_v6 %>%
  group_by(job_id, seq_id, sequence) %>%
  pivot_longer(., cols = x100_percent_remote:working_virtually, names_to = "key") %>%
  filter(value == 1) %>%
  group_by(key) %>%
  sample_n(., size = pmin(n(),pmax(10, log(n())))) %>%
  setDT(.) %>%
  .[, N := .N, by = key]
rm(wfh_v6)
nrow(wfh_v6_long) # 1,254

# Negated Examples
wfh_neg_v6 <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    mutate(across(x100_percent_remote:browse, ~replace(., is.na(.), 0))) %>%
    mutate(across(x100_percent_remote:browse, ~ifelse(.>0, 1, 0)))
  df <- df %>% setDT(.)
  df$comb_wfh <- pmin(rowSums(df[, select(.SD, x100_percent_remote:working_virtually)]),1)
  df$comb_negated <- pmin(rowSums(df[, select(.SD, cant:wont_2)]),1)
  df <- df %>% .[comb_wfh == 1 & comb_negated == 1]
  df <- df %>% sample_n(., pmin(nrow(.), 500))
  return(df)
}, mc.cores = 20)
wfh_neg_v6 <- rbindlist(wfh_neg_v6, fill = T)
wfh_neg_v6 <- wfh_neg_v6 %>%
  select(job_id, seq_id, sequence, x100_percent_remote:working_virtually)
wfh_neg_v6_long <- wfh_neg_v6 %>%
  group_by(job_id, seq_id, sequence) %>%
  pivot_longer(., cols = x100_percent_remote:working_virtually, names_to = "key") %>%
  filter(value == 1) %>%
  group_by(key) %>%
  sample_n(., size = pmin(n(),pmax(10, log(n())))) %>%
  setDT(.) %>%
  .[, N := .N, by = key]
rm(wfh_neg_v6)
nrow(wfh_neg_v6_long) # 720

# Intensity Examples
wfh_intensity_v6 <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    mutate(across(x100_percent_remote:browse, ~replace(., is.na(.), 0))) %>%
    mutate(across(x100_percent_remote:browse, ~ifelse(.>0, 1, 0)))
  df <- df %>% setDT(.)
  df$comb_wfh <- pmin(rowSums(df[, select(.SD, x100_percent_remote:working_virtually)]),1)
  df$comb_intensity <- pmin(rowSums(df[, select(.SD, x1:road)]),1)
  df <- df %>% .[comb_wfh == 1 & comb_intensity == 1]
  df <- df %>% sample_n(., pmin(nrow(.), 500))
  return(df)
}, mc.cores = 20)
wfh_intensity_v6 <- rbindlist(wfh_intensity_v6, fill = T)
wfh_intensity_v6 <- wfh_intensity_v6 %>%
  select(job_id, seq_id, sequence, x1:road)
wfh_intensity_v6_long <- wfh_intensity_v6 %>%
  group_by(job_id, seq_id, sequence) %>%
  pivot_longer(., cols = x1:road, names_to = "key") %>%
  filter(value == 1) %>%
  group_by(key) %>%
  sample_n(., size = pmin(n(),pmax(10, log(n())))) %>%
  setDT(.) %>%
  .[, N := .N, by = key]
rm(wfh_intensity_v6)
nrow(wfh_intensity_v6_long) # 541

# Excemptions Examples
excemptions_v6 <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    mutate(across(x100_percent_remote:browse, ~replace(., is.na(.), 0))) %>%
    mutate(across(x100_percent_remote:browse, ~ifelse(.>0, 1, 0)))
  df <- df %>% setDT(.)
  df$comb_wfh <- pmin(rowSums(df[, select(.SD, x100_percent_remote:working_virtually)]),1)
  df$comb_disqual <- pmin(rowSums(df[, select(.SD, hotel:browse)]),1)
  df$comb_intensity <- pmin(rowSums(df[, select(.SD, x1:road)]),1)
  df <- df %>% .[comb_wfh == 0 & comb_intensity == 0 & comb_disqual == 1]
  df <- df %>% sample_n(., pmin(nrow(.), 500))
  return(df)
}, mc.cores = 20)
excemptions_v6 <- rbindlist(excemptions_v6, fill = T)
excemptions_v6 <- excemptions_v6 %>%
  select(job_id, seq_id, sequence, hotel:browse)
excemptions_v6_long <- excemptions_v6 %>%
  group_by(job_id, seq_id, sequence) %>%
  pivot_longer(., cols = hotel:browse, names_to = "key") %>%
  filter(value == 1) %>%
  group_by(key) %>%
  sample_n(., size = pmin(n(),pmax(10, log(n())))) %>%
  setDT(.) %>%
  .[, N := .N, by = key]
rm(excemptions_v6)
nrow(excemptions_v6_long) # 869

# Generic Examples
generic_v6 <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    mutate(across(x100_percent_remote:browse, ~replace(., is.na(.), 0))) %>%
    mutate(across(x100_percent_remote:browse, ~ifelse(.>0, 1, 0)))
  df <- df %>% setDT(.)
  df$comb_wfh <- pmin(rowSums(df[, select(.SD, x100_percent_remote:working_virtually)]),1)
  df$comb_disqual <- pmin(rowSums(df[, select(.SD, hotel:browse)]),1)
  df$comb_intensity <- pmin(rowSums(df[, select(.SD, x1:road)]),1)
  df$comb_generic <- pmin(rowSums(df[, select(.SD, remote_ast:tele_ast)]),1)
  df <- df %>% .[comb_generic == 1]
  df <- df %>% sample_n(., pmin(nrow(.), 1000))
  return(df)
}, mc.cores = 20)
generic_v6 <- rbindlist(generic_v6, fill = T)
generic_v6 <- generic_v6 %>%
  select(job_id, seq_id, sequence, remote_ast:tele_ast)
generic_v6_long <- generic_v6 %>%
  group_by(job_id, seq_id, sequence) %>%
  pivot_longer(., cols = remote_ast:tele_ast, names_to = "key") %>%
  filter(value == 1) %>%
  group_by(key) %>%
  sample_n(., size = pmin(n(),pmax(10, sqrt(n())))) %>%
  setDT(.) %>%
  .[, N := .N, by = key]
rm(generic_v6)
nrow(generic_v6_long) # 1,337

# Random Examples
paths <- list.files("./int_data/wfh_v6/", pattern = "wfh_v6_raw_sequences", full.names = T) %>% sort(decreasing = T)
all_tagged_seq <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    select(seq_id) %>%
    setDT(.)
}, mc.cores = 20)
all_tagged_seq <- rbindlist(all_tagged_seq, fill = T)

paths <- list.files("./int_data/sequences/", pattern = "*.rds", full.names = T) %>% sort(decreasing = T)
random_non_tagged <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    filter(!(seq_id %in% all_tagged_seq$seq_id)) %>%
    sample_n(., size = pmin(n(), 2)) %>%
    setDT(.)
}, mc.cores = 20)
random_non_tagged <- rbindlist(random_non_tagged, fill = T)
nrow(random_non_tagged) # 816
ls()

nrow(wfh_v6_long) # 1,270
nrow(wfh_neg_v6_long) # 720
nrow(wfh_intensity_v6_long) # 539
nrow(excemptions_v6_long) # 865
nrow(generic_v6_long) # 1,337
nrow(random_non_tagged) # 816

df <- bind_rows(wfh_v6_long %>% select(job_id, seq_id, sequence) %>% mutate(part = "wfh"),
                wfh_neg_v6_long %>% select(job_id, seq_id, sequence) %>% mutate(part = "wfh_neg"),
                wfh_intensity_v6_long %>% select(job_id, seq_id, sequence) %>% mutate(part = "intensity"),
                excemptions_v6_long %>% select(job_id, seq_id, sequence) %>% mutate(part = "excemptions"),
                generic_v6_long %>% select(job_id, seq_id, sequence) %>% mutate(part = "generic"),
                random_non_tagged %>% select(job_id, seq_id, sequence) %>% mutate(part = "random"))
nrow(df) # 5,547
df <- df %>% setDT(.) %>% .[, nfeat := str_count(sequence, '\\w+')]

saveRDS(df, file = "/mnt/disks/pdisk/bg-us/int_data/training_v6/training_v6.rds")
#### END ####

#### MAKE SUBSET FOR LABELS - UK ####
setwd("/mnt/disks/pdisk/bg-uk/")
source("/mnt/disks/pdisk/code/safe_mclapply.R")
paths <- list.files("./int_data/wfh_v6/", pattern = "wfh_v6_raw_sequences", full.names = T) %>% sort(decreasing = T)
# WFH Examples
wfh_v6 <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    mutate(across(x100_percent_remote:browse, ~replace(., is.na(.), 0))) %>%
    mutate(across(x100_percent_remote:browse, ~ifelse(.>0, 1, 0)))
  df <- df %>% setDT(.)
  df$comb_wfh <- pmin(rowSums(df[, select(.SD, x100_percent_remote:working_virtually)]),1)
  df$comb_negated <- pmin(rowSums(df[, select(.SD, cant:wont_2)]),1)
  df <- df %>% .[comb_wfh == 1 & comb_negated == 0]
  df <- df %>% sample_n(., pmin(nrow(.), 500))
  return(df)
}, mc.cores = 20)
wfh_v6 <- rbindlist(wfh_v6, fill = T)
wfh_v6 <- wfh_v6 %>%
  select(job_id, seq_id, sequence, x100_percent_remote:working_virtually)
wfh_v6_long <- wfh_v6 %>%
  group_by(job_id, seq_id, sequence) %>%
  pivot_longer(., cols = x100_percent_remote:working_virtually, names_to = "key") %>%
  filter(value == 1) %>%
  group_by(key) %>%
  sample_n(., size = pmin(n(),pmax(10, log(n())))) %>%
  setDT(.) %>%
  .[, N := .N, by = key]
rm(wfh_v6)
nrow(wfh_v6_long) # 1,254

# Negated Examples
wfh_neg_v6 <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    mutate(across(x100_percent_remote:browse, ~replace(., is.na(.), 0))) %>%
    mutate(across(x100_percent_remote:browse, ~ifelse(.>0, 1, 0)))
  df <- df %>% setDT(.)
  df$comb_wfh <- pmin(rowSums(df[, select(.SD, x100_percent_remote:working_virtually)]),1)
  df$comb_negated <- pmin(rowSums(df[, select(.SD, cant:wont_2)]),1)
  df <- df %>% .[comb_wfh == 1 & comb_negated == 1]
  df <- df %>% sample_n(., pmin(nrow(.), 500))
  return(df)
}, mc.cores = 20)
wfh_neg_v6 <- rbindlist(wfh_neg_v6, fill = T)
wfh_neg_v6 <- wfh_neg_v6 %>%
  select(job_id, seq_id, sequence, x100_percent_remote:working_virtually)
wfh_neg_v6_long <- wfh_neg_v6 %>%
  group_by(job_id, seq_id, sequence) %>%
  pivot_longer(., cols = x100_percent_remote:working_virtually, names_to = "key") %>%
  filter(value == 1) %>%
  group_by(key) %>%
  sample_n(., size = pmin(n(),pmax(10, log(n())))) %>%
  setDT(.) %>%
  .[, N := .N, by = key]
rm(wfh_neg_v6)
nrow(wfh_neg_v6_long) # 720

# Intensity Examples
wfh_intensity_v6 <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    mutate(across(x100_percent_remote:browse, ~replace(., is.na(.), 0))) %>%
    mutate(across(x100_percent_remote:browse, ~ifelse(.>0, 1, 0)))
  df <- df %>% setDT(.)
  df$comb_wfh <- pmin(rowSums(df[, select(.SD, x100_percent_remote:working_virtually)]),1)
  df$comb_intensity <- pmin(rowSums(df[, select(.SD, x1:road)]),1)
  df <- df %>% .[comb_wfh == 1 & comb_intensity == 1]
  df <- df %>% sample_n(., pmin(nrow(.), 500))
  return(df)
}, mc.cores = 20)
wfh_intensity_v6 <- rbindlist(wfh_intensity_v6, fill = T)
wfh_intensity_v6 <- wfh_intensity_v6 %>%
  select(job_id, seq_id, sequence, x1:road)
wfh_intensity_v6_long <- wfh_intensity_v6 %>%
  group_by(job_id, seq_id, sequence) %>%
  pivot_longer(., cols = x1:road, names_to = "key") %>%
  filter(value == 1) %>%
  group_by(key) %>%
  sample_n(., size = pmin(n(),pmax(10, log(n())))) %>%
  setDT(.) %>%
  .[, N := .N, by = key]
rm(wfh_intensity_v6)
nrow(wfh_intensity_v6_long) # 541

# Excemptions Examples
excemptions_v6 <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    mutate(across(x100_percent_remote:browse, ~replace(., is.na(.), 0))) %>%
    mutate(across(x100_percent_remote:browse, ~ifelse(.>0, 1, 0)))
  df <- df %>% setDT(.)
  df$comb_wfh <- pmin(rowSums(df[, select(.SD, x100_percent_remote:working_virtually)]),1)
  df$comb_disqual <- pmin(rowSums(df[, select(.SD, hotel:browse)]),1)
  df$comb_intensity <- pmin(rowSums(df[, select(.SD, x1:road)]),1)
  df <- df %>% .[comb_wfh == 0 & comb_intensity == 0 & comb_disqual == 1]
  df <- df %>% sample_n(., pmin(nrow(.), 500))
  return(df)
}, mc.cores = 20)
excemptions_v6 <- rbindlist(excemptions_v6, fill = T)
excemptions_v6 <- excemptions_v6 %>%
  select(job_id, seq_id, sequence, hotel:browse)
excemptions_v6_long <- excemptions_v6 %>%
  group_by(job_id, seq_id, sequence) %>%
  pivot_longer(., cols = hotel:browse, names_to = "key") %>%
  filter(value == 1) %>%
  group_by(key) %>%
  sample_n(., size = pmin(n(),pmax(10, log(n())))) %>%
  setDT(.) %>%
  .[, N := .N, by = key]
rm(excemptions_v6)
nrow(excemptions_v6_long) # 869

# Generic Examples
generic_v6 <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    mutate(across(x100_percent_remote:browse, ~replace(., is.na(.), 0))) %>%
    mutate(across(x100_percent_remote:browse, ~ifelse(.>0, 1, 0)))
  df <- df %>% setDT(.)
  df$comb_wfh <- pmin(rowSums(df[, select(.SD, x100_percent_remote:working_virtually)]),1)
  df$comb_disqual <- pmin(rowSums(df[, select(.SD, hotel:browse)]),1)
  df$comb_intensity <- pmin(rowSums(df[, select(.SD, x1:road)]),1)
  df$comb_generic <- pmin(rowSums(df[, select(.SD, remote_ast:tele_ast)]),1)
  df <- df %>% .[comb_generic == 1]
  df <- df %>% sample_n(., pmin(nrow(.), 1000))
  return(df)
}, mc.cores = 20)
generic_v6 <- rbindlist(generic_v6, fill = T)
generic_v6 <- generic_v6 %>%
  select(job_id, seq_id, sequence, remote_ast:tele_ast)
generic_v6_long <- generic_v6 %>%
  group_by(job_id, seq_id, sequence) %>%
  pivot_longer(., cols = remote_ast:tele_ast, names_to = "key") %>%
  filter(value == 1) %>%
  group_by(key) %>%
  sample_n(., size = pmin(n(),pmax(10, sqrt(n())))) %>%
  setDT(.) %>%
  .[, N := .N, by = key]
rm(generic_v6)
nrow(generic_v6_long) # 1,337

# Random Examples
paths <- list.files("./int_data/wfh_v6/", pattern = "wfh_v6_raw_sequences", full.names = T) %>% sort(decreasing = T)
all_tagged_seq <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    select(seq_id) %>%
    setDT(.)
}, mc.cores = 20)
all_tagged_seq <- rbindlist(all_tagged_seq, fill = T)

paths <- list.files("./int_data/sequences/", pattern = "*.rds", full.names = T) %>% sort(decreasing = T)
random_non_tagged <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    filter(!(seq_id %in% all_tagged_seq$seq_id)) %>%
    sample_n(., size = pmin(n(), 2)) %>%
    setDT(.)
}, mc.cores = 20)
random_non_tagged <- rbindlist(random_non_tagged, fill = T)
nrow(random_non_tagged) # 816
ls()

nrow(wfh_v6_long) # 1,270
nrow(wfh_neg_v6_long) # 720
nrow(wfh_intensity_v6_long) # 539
nrow(excemptions_v6_long) # 865
nrow(generic_v6_long) # 1,337
nrow(random_non_tagged) # 816

df <- bind_rows(wfh_v6_long %>% select(job_id, seq_id, sequence) %>% mutate(part = "wfh"),
                wfh_neg_v6_long %>% select(job_id, seq_id, sequence) %>% mutate(part = "wfh_neg"),
                wfh_intensity_v6_long %>% select(job_id, seq_id, sequence) %>% mutate(part = "intensity"),
                excemptions_v6_long %>% select(job_id, seq_id, sequence) %>% mutate(part = "excemptions"),
                generic_v6_long %>% select(job_id, seq_id, sequence) %>% mutate(part = "generic"),
                random_non_tagged %>% select(job_id, seq_id, sequence) %>% mutate(part = "random"))
nrow(df) # 5,547
df <- df %>% setDT(.) %>% .[, nfeat := str_count(sequence, '\\w+')]

saveRDS(df, file = "/mnt/disks/pdisk/bg-uk/int_data/training_v6/training_v6.rds")

#### END ####

#### MAKE SUBSET FOR LABELS - CAN ####
setwd("/mnt/disks/pdisk/bg-can/")
source("/mnt/disks/pdisk/code/safe_mclapply.R")
paths <- list.files("./int_data/wfh_v6/", pattern = "wfh_v6_raw_sequences", full.names = T) %>% sort(decreasing = T)
# WFH Examples
wfh_v6 <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    mutate(across(x100_percent_remote:browse, ~replace(., is.na(.), 0))) %>%
    mutate(across(x100_percent_remote:browse, ~ifelse(.>0, 1, 0)))
  df <- df %>% setDT(.)
  df$comb_wfh <- pmin(rowSums(df[, select(.SD, x100_percent_remote:working_virtually)]),1)
  df$comb_negated <- pmin(rowSums(df[, select(.SD, cant:wont_2)]),1)
  df <- df %>% .[comb_wfh == 1 & comb_negated == 0]
  df <- df %>% sample_n(., pmin(nrow(.), 500))
  return(df)
}, mc.cores = 20)
wfh_v6 <- rbindlist(wfh_v6, fill = T)
wfh_v6 <- wfh_v6 %>%
  select(job_id, seq_id, sequence, x100_percent_remote:working_virtually)
wfh_v6_long <- wfh_v6 %>%
  group_by(job_id, seq_id, sequence) %>%
  pivot_longer(., cols = x100_percent_remote:working_virtually, names_to = "key") %>%
  filter(value == 1) %>%
  group_by(key) %>%
  sample_n(., size = pmin(n(),pmax(10, log(n())))) %>%
  setDT(.) %>%
  .[, N := .N, by = key]
rm(wfh_v6)
nrow(wfh_v6_long) # 1,254

# Negated Examples
wfh_neg_v6 <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    mutate(across(x100_percent_remote:browse, ~replace(., is.na(.), 0))) %>%
    mutate(across(x100_percent_remote:browse, ~ifelse(.>0, 1, 0)))
  df <- df %>% setDT(.)
  df$comb_wfh <- pmin(rowSums(df[, select(.SD, x100_percent_remote:working_virtually)]),1)
  df$comb_negated <- pmin(rowSums(df[, select(.SD, cant:wont_2)]),1)
  df <- df %>% .[comb_wfh == 1 & comb_negated == 1]
  df <- df %>% sample_n(., pmin(nrow(.), 500))
  return(df)
}, mc.cores = 20)
wfh_neg_v6 <- rbindlist(wfh_neg_v6, fill = T)
wfh_neg_v6 <- wfh_neg_v6 %>%
  select(job_id, seq_id, sequence, x100_percent_remote:working_virtually)
wfh_neg_v6_long <- wfh_neg_v6 %>%
  group_by(job_id, seq_id, sequence) %>%
  pivot_longer(., cols = x100_percent_remote:working_virtually, names_to = "key") %>%
  filter(value == 1) %>%
  group_by(key) %>%
  sample_n(., size = pmin(n(),pmax(10, log(n())))) %>%
  setDT(.) %>%
  .[, N := .N, by = key]
rm(wfh_neg_v6)
nrow(wfh_neg_v6_long) # 720

# Intensity Examples
wfh_intensity_v6 <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    mutate(across(x100_percent_remote:browse, ~replace(., is.na(.), 0))) %>%
    mutate(across(x100_percent_remote:browse, ~ifelse(.>0, 1, 0)))
  df <- df %>% setDT(.)
  df$comb_wfh <- pmin(rowSums(df[, select(.SD, x100_percent_remote:working_virtually)]),1)
  df$comb_intensity <- pmin(rowSums(df[, select(.SD, x1:road)]),1)
  df <- df %>% .[comb_wfh == 1 & comb_intensity == 1]
  df <- df %>% sample_n(., pmin(nrow(.), 500))
  return(df)
}, mc.cores = 20)
wfh_intensity_v6 <- rbindlist(wfh_intensity_v6, fill = T)
wfh_intensity_v6 <- wfh_intensity_v6 %>%
  select(job_id, seq_id, sequence, x1:road)
wfh_intensity_v6_long <- wfh_intensity_v6 %>%
  group_by(job_id, seq_id, sequence) %>%
  pivot_longer(., cols = x1:road, names_to = "key") %>%
  filter(value == 1) %>%
  group_by(key) %>%
  sample_n(., size = pmin(n(),pmax(10, log(n())))) %>%
  setDT(.) %>%
  .[, N := .N, by = key]
rm(wfh_intensity_v6)
nrow(wfh_intensity_v6_long) # 541

# Excemptions Examples
excemptions_v6 <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    mutate(across(x100_percent_remote:browse, ~replace(., is.na(.), 0))) %>%
    mutate(across(x100_percent_remote:browse, ~ifelse(.>0, 1, 0)))
  df <- df %>% setDT(.)
  df$comb_wfh <- pmin(rowSums(df[, select(.SD, x100_percent_remote:working_virtually)]),1)
  df$comb_disqual <- pmin(rowSums(df[, select(.SD, hotel:browse)]),1)
  df$comb_intensity <- pmin(rowSums(df[, select(.SD, x1:road)]),1)
  df <- df %>% .[comb_wfh == 0 & comb_intensity == 0 & comb_disqual == 1]
  df <- df %>% sample_n(., pmin(nrow(.), 500))
  return(df)
}, mc.cores = 20)
excemptions_v6 <- rbindlist(excemptions_v6, fill = T)
excemptions_v6 <- excemptions_v6 %>%
  select(job_id, seq_id, sequence, hotel:browse)
excemptions_v6_long <- excemptions_v6 %>%
  group_by(job_id, seq_id, sequence) %>%
  pivot_longer(., cols = hotel:browse, names_to = "key") %>%
  filter(value == 1) %>%
  group_by(key) %>%
  sample_n(., size = pmin(n(),pmax(10, log(n())))) %>%
  setDT(.) %>%
  .[, N := .N, by = key]
rm(excemptions_v6)
nrow(excemptions_v6_long) # 869

# Generic Examples
generic_v6 <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    mutate(across(x100_percent_remote:browse, ~replace(., is.na(.), 0))) %>%
    mutate(across(x100_percent_remote:browse, ~ifelse(.>0, 1, 0)))
  df <- df %>% setDT(.)
  df$comb_wfh <- pmin(rowSums(df[, select(.SD, x100_percent_remote:working_virtually)]),1)
  df$comb_disqual <- pmin(rowSums(df[, select(.SD, hotel:browse)]),1)
  df$comb_intensity <- pmin(rowSums(df[, select(.SD, x1:road)]),1)
  df$comb_generic <- pmin(rowSums(df[, select(.SD, remote_ast:tele_ast)]),1)
  df <- df %>% .[comb_generic == 1]
  df <- df %>% sample_n(., pmin(nrow(.), 1000))
  return(df)
}, mc.cores = 20)
generic_v6 <- rbindlist(generic_v6, fill = T)
generic_v6 <- generic_v6 %>%
  select(job_id, seq_id, sequence, remote_ast:tele_ast)
generic_v6_long <- generic_v6 %>%
  group_by(job_id, seq_id, sequence) %>%
  pivot_longer(., cols = remote_ast:tele_ast, names_to = "key") %>%
  filter(value == 1) %>%
  group_by(key) %>%
  sample_n(., size = pmin(n(),pmax(10, sqrt(n())))) %>%
  setDT(.) %>%
  .[, N := .N, by = key]
rm(generic_v6)
nrow(generic_v6_long) # 1,337

# Random Examples
paths <- list.files("./int_data/wfh_v6/", pattern = "wfh_v6_raw_sequences", full.names = T) %>% sort(decreasing = T)
all_tagged_seq <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    select(seq_id) %>%
    setDT(.)
}, mc.cores = 20)
all_tagged_seq <- rbindlist(all_tagged_seq, fill = T)

paths <- list.files("./int_data/sequences/", pattern = "*.rds", full.names = T) %>% sort(decreasing = T)
random_non_tagged <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    filter(!(seq_id %in% all_tagged_seq$seq_id)) %>%
    sample_n(., size = pmin(n(), 2)) %>%
    setDT(.)
}, mc.cores = 20)
random_non_tagged <- rbindlist(random_non_tagged, fill = T)
nrow(random_non_tagged) # 816
ls()

nrow(wfh_v6_long) # 1,270
nrow(wfh_neg_v6_long) # 720
nrow(wfh_intensity_v6_long) # 539
nrow(excemptions_v6_long) # 865
nrow(generic_v6_long) # 1,337
nrow(random_non_tagged) # 816

df <- bind_rows(wfh_v6_long %>% select(job_id, seq_id, sequence) %>% mutate(part = "wfh"),
                wfh_neg_v6_long %>% select(job_id, seq_id, sequence) %>% mutate(part = "wfh_neg"),
                wfh_intensity_v6_long %>% select(job_id, seq_id, sequence) %>% mutate(part = "intensity"),
                excemptions_v6_long %>% select(job_id, seq_id, sequence) %>% mutate(part = "excemptions"),
                generic_v6_long %>% select(job_id, seq_id, sequence) %>% mutate(part = "generic"),
                random_non_tagged %>% select(job_id, seq_id, sequence) %>% mutate(part = "random"))
nrow(df) # 5,547
df <- df %>% setDT(.) %>% .[, nfeat := str_count(sequence, '\\w+')]

saveRDS(df, file = "/mnt/disks/pdisk/bg-can/int_data/training_v6/training_v6.rds")

#### END ####

#### MAKE SUBSET FOR LABELS - ANZ ####
setwd("/mnt/disks/pdisk/bg-anz/")
source("/mnt/disks/pdisk/code/safe_mclapply.R")
paths <- list.files("./int_data/wfh_v6/", pattern = "wfh_v6_raw_sequences", full.names = T) %>% sort(decreasing = T)
# WFH Examples
wfh_v6 <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    mutate(across(x100_percent_remote:browse, ~replace(., is.na(.), 0))) %>%
    mutate(across(x100_percent_remote:browse, ~ifelse(.>0, 1, 0)))
  df <- df %>% setDT(.)
  df$comb_wfh <- pmin(rowSums(df[, select(.SD, x100_percent_remote:working_virtually)]),1)
  df$comb_negated <- pmin(rowSums(df[, select(.SD, cant:wont_2)]),1)
  df <- df %>% .[comb_wfh == 1 & comb_negated == 0]
  df <- df %>% sample_n(., pmin(nrow(.), 500))
  return(df)
}, mc.cores = 20)
wfh_v6 <- rbindlist(wfh_v6, fill = T)
wfh_v6 <- wfh_v6 %>%
  select(job_id, seq_id, sequence, x100_percent_remote:working_virtually)
wfh_v6_long <- wfh_v6 %>%
  group_by(job_id, seq_id, sequence) %>%
  pivot_longer(., cols = x100_percent_remote:working_virtually, names_to = "key") %>%
  filter(value == 1) %>%
  group_by(key) %>%
  sample_n(., size = pmin(n(),pmax(10, log(n())))) %>%
  setDT(.) %>%
  .[, N := .N, by = key]
rm(wfh_v6)
nrow(wfh_v6_long) # 1,254

# Negated Examples
wfh_neg_v6 <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    mutate(across(x100_percent_remote:browse, ~replace(., is.na(.), 0))) %>%
    mutate(across(x100_percent_remote:browse, ~ifelse(.>0, 1, 0)))
  df <- df %>% setDT(.)
  df$comb_wfh <- pmin(rowSums(df[, select(.SD, x100_percent_remote:working_virtually)]),1)
  df$comb_negated <- pmin(rowSums(df[, select(.SD, cant:wont_2)]),1)
  df <- df %>% .[comb_wfh == 1 & comb_negated == 1]
  df <- df %>% sample_n(., pmin(nrow(.), 500))
  return(df)
}, mc.cores = 20)
wfh_neg_v6 <- rbindlist(wfh_neg_v6, fill = T)
wfh_neg_v6 <- wfh_neg_v6 %>%
  select(job_id, seq_id, sequence, x100_percent_remote:working_virtually)
wfh_neg_v6_long <- wfh_neg_v6 %>%
  group_by(job_id, seq_id, sequence) %>%
  pivot_longer(., cols = x100_percent_remote:working_virtually, names_to = "key") %>%
  filter(value == 1) %>%
  group_by(key) %>%
  sample_n(., size = pmin(n(),pmax(10, log(n())))) %>%
  setDT(.) %>%
  .[, N := .N, by = key]
rm(wfh_neg_v6)
nrow(wfh_neg_v6_long) # 720

# Intensity Examples
wfh_intensity_v6 <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    mutate(across(x100_percent_remote:browse, ~replace(., is.na(.), 0))) %>%
    mutate(across(x100_percent_remote:browse, ~ifelse(.>0, 1, 0)))
  df <- df %>% setDT(.)
  df$comb_wfh <- pmin(rowSums(df[, select(.SD, x100_percent_remote:working_virtually)]),1)
  df$comb_intensity <- pmin(rowSums(df[, select(.SD, x1:road)]),1)
  df <- df %>% .[comb_wfh == 1 & comb_intensity == 1]
  df <- df %>% sample_n(., pmin(nrow(.), 500))
  return(df)
}, mc.cores = 20)
wfh_intensity_v6 <- rbindlist(wfh_intensity_v6, fill = T)
wfh_intensity_v6 <- wfh_intensity_v6 %>%
  select(job_id, seq_id, sequence, x1:road)
wfh_intensity_v6_long <- wfh_intensity_v6 %>%
  group_by(job_id, seq_id, sequence) %>%
  pivot_longer(., cols = x1:road, names_to = "key") %>%
  filter(value == 1) %>%
  group_by(key) %>%
  sample_n(., size = pmin(n(),pmax(10, log(n())))) %>%
  setDT(.) %>%
  .[, N := .N, by = key]
rm(wfh_intensity_v6)
nrow(wfh_intensity_v6_long) # 541

# Excemptions Examples
excemptions_v6 <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    mutate(across(x100_percent_remote:browse, ~replace(., is.na(.), 0))) %>%
    mutate(across(x100_percent_remote:browse, ~ifelse(.>0, 1, 0)))
  df <- df %>% setDT(.)
  df$comb_wfh <- pmin(rowSums(df[, select(.SD, x100_percent_remote:working_virtually)]),1)
  df$comb_disqual <- pmin(rowSums(df[, select(.SD, hotel:browse)]),1)
  df$comb_intensity <- pmin(rowSums(df[, select(.SD, x1:road)]),1)
  df <- df %>% .[comb_wfh == 0 & comb_intensity == 0 & comb_disqual == 1]
  df <- df %>% sample_n(., pmin(nrow(.), 500))
  return(df)
}, mc.cores = 20)
excemptions_v6 <- rbindlist(excemptions_v6, fill = T)
excemptions_v6 <- excemptions_v6 %>%
  select(job_id, seq_id, sequence, hotel:browse)
excemptions_v6_long <- excemptions_v6 %>%
  group_by(job_id, seq_id, sequence) %>%
  pivot_longer(., cols = hotel:browse, names_to = "key") %>%
  filter(value == 1) %>%
  group_by(key) %>%
  sample_n(., size = pmin(n(),pmax(10, log(n())))) %>%
  setDT(.) %>%
  .[, N := .N, by = key]
rm(excemptions_v6)
nrow(excemptions_v6_long) # 869

# Generic Examples
generic_v6 <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    mutate(across(x100_percent_remote:browse, ~replace(., is.na(.), 0))) %>%
    mutate(across(x100_percent_remote:browse, ~ifelse(.>0, 1, 0)))
  df <- df %>% setDT(.)
  df$comb_wfh <- pmin(rowSums(df[, select(.SD, x100_percent_remote:working_virtually)]),1)
  df$comb_disqual <- pmin(rowSums(df[, select(.SD, hotel:browse)]),1)
  df$comb_intensity <- pmin(rowSums(df[, select(.SD, x1:road)]),1)
  df$comb_generic <- pmin(rowSums(df[, select(.SD, remote_ast:tele_ast)]),1)
  df <- df %>% .[comb_generic == 1]
  df <- df %>% sample_n(., pmin(nrow(.), 1000))
  return(df)
}, mc.cores = 20)
generic_v6 <- rbindlist(generic_v6, fill = T)
generic_v6 <- generic_v6 %>%
  select(job_id, seq_id, sequence, remote_ast:tele_ast)
generic_v6_long <- generic_v6 %>%
  group_by(job_id, seq_id, sequence) %>%
  pivot_longer(., cols = remote_ast:tele_ast, names_to = "key") %>%
  filter(value == 1) %>%
  group_by(key) %>%
  sample_n(., size = pmin(n(),pmax(10, sqrt(n())))) %>%
  setDT(.) %>%
  .[, N := .N, by = key]
rm(generic_v6)
nrow(generic_v6_long) # 1,337

# Random Examples
paths <- list.files("./int_data/wfh_v6/", pattern = "wfh_v6_raw_sequences", full.names = T) %>% sort(decreasing = T)
all_tagged_seq <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    select(seq_id) %>%
    setDT(.)
}, mc.cores = 20)
all_tagged_seq <- rbindlist(all_tagged_seq, fill = T)

paths <- list.files("./int_data/sequences/", pattern = "*.rds", full.names = T) %>% sort(decreasing = T)
random_non_tagged <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    filter(!(seq_id %in% all_tagged_seq$seq_id)) %>%
    sample_n(., size = pmin(n(), 2)) %>%
    setDT(.)
}, mc.cores = 20)
random_non_tagged <- rbindlist(random_non_tagged, fill = T)
nrow(random_non_tagged) # 816
ls()

nrow(wfh_v6_long) # 1,270
nrow(wfh_neg_v6_long) # 720
nrow(wfh_intensity_v6_long) # 539
nrow(excemptions_v6_long) # 865
nrow(generic_v6_long) # 1,337
nrow(random_non_tagged) # 816

df <- bind_rows(wfh_v6_long %>% select(job_id, seq_id, sequence) %>% mutate(part = "wfh"),
                wfh_neg_v6_long %>% select(job_id, seq_id, sequence) %>% mutate(part = "wfh_neg"),
                wfh_intensity_v6_long %>% select(job_id, seq_id, sequence) %>% mutate(part = "intensity"),
                excemptions_v6_long %>% select(job_id, seq_id, sequence) %>% mutate(part = "excemptions"),
                generic_v6_long %>% select(job_id, seq_id, sequence) %>% mutate(part = "generic"),
                random_non_tagged %>% select(job_id, seq_id, sequence) %>% mutate(part = "random"))
nrow(df) # 5,547
df <- df %>% setDT(.) %>% .[, nfeat := str_count(sequence, '\\w+')]

saveRDS(df, file = "/mnt/disks/pdisk/bg-anz/int_data/training_v6/training_v6.rds")

#### END ####

#### CONSTRUCT FINAL FOR AMT ####
remove(list = ls())
df <- bind_rows(readRDS(file = "/mnt/disks/pdisk/bg-anz/int_data/training_v6/training_v6.rds") %>% mutate(country = "anz"),
                readRDS(file = "/mnt/disks/pdisk/bg-can/int_data/training_v6/training_v6.rds") %>% mutate(country = "can"),
                readRDS(file = "/mnt/disks/pdisk/bg-uk/int_data/training_v6/training_v6.rds") %>% mutate(country = "uk"),
                readRDS(file = "/mnt/disks/pdisk/bg-us/int_data/training_v6/training_v6.rds") %>% mutate(country = "us"))
table(df$country)

df_problem <- df %>% filter(grepl("Good inter-personal skills are required, along with a positive", sequence))

Encoding(df$sequence) <- "ASCII"
df$sequence <- iconv(df$sequence, "latin1", "ASCII", sub="")

nrow(df) # 17,726
df <- df %>% distinct(sequence, .keep_all = T)
nrow(df) # 16,974

df$seq_refinr<-n_gram_merge(df$sequence) # refinr function
df$seq_chopped<-substr(df$seq_refinr, 1, 600) # up to 600 characters considered for UK, 700 for US, subjective!

## obtain duplicates
df_dup<-setDT(df)[, n := uniqueN(sequence), by = seq_chopped] 
df_dup<-df_dup[n!=1, ]
df_unique_add<-df_dup %>% group_by(seq_chopped) %>% slice_max(seq_id, n=1) # only get the first seq_id

## bind non repeats
df_final<-df[! seq_id %in% df_dup$seq_id,]
df_final<-bind_rows(df_final, df_unique_add)

df <- df_final

remove(list = setdiff(ls(), "df"))

df <- df %>% select(job_id, seq_id, nfeat, country, part, sequence)
set.seed(123)
df <- df %>% sample_n(., 10000)
  
#### ADD MARKUP ####
# Load dictionary
df_dict <- bind_rows(read_xlsx("./aux_data/wfh_v6.xlsx", sheet = 1) %>% clean_names %>% select(glob_en) %>% mutate(source = "wfh"),
                     read_xlsx("./aux_data/wfh_v6.xlsx", sheet = 2) %>% clean_names %>% select(glob_en) %>% mutate(source = "generic"),
                     read_xlsx("./aux_data/wfh_v6.xlsx", sheet = 3) %>% clean_names %>% select(glob_en) %>% mutate(source = "excemptions"),
                     read_xlsx("./aux_data/wfh_v6.xlsx", sheet = 4) %>% clean_names %>% select(glob_en) %>% mutate(source = "intensity"),
                     read_xlsx("./aux_data/wfh_v6.xlsx", sheet = 5) %>% clean_names %>% select(glob_en) %>% mutate(source = "negation"))

df_dict$glob_en <- tolower(df_dict$glob_en)
df_dict[df_dict == ""] <- NA
df_dict <- df_dict %>% group_by(source) %>% distinct(glob_en, .keep_all = T) %>% ungroup
df_dict <- df_dict %>%
  mutate(key = glob_en) %>%
  mutate(key = gsub("\\s+", "_", key)) %>%
  mutate(key = gsub("[*]", "_AST_", key)) %>%
  mutate(key = gsub("[:]", "_C_", key)) %>%
  mutate(key = gsub("^_", "", key)) %>%
  mutate(key = gsub("_$", "", key)) %>%
  mutate(key = gsub("__", "_", key))

df_dict <- df_dict %>%
  mutate(key = make_clean_names(key)) %>%
  select(key, glob_en, source)

all_dict <- setNames(as.list(df_dict$glob_en), df_dict$key) %>% dictionary()

df_dict$lu <- NA
df_dict$lu <- gsub("*", "\\w+", df_dict$glob_en, fixed = T)
df_dict$lu <- paste0("\\b(",gsub(" ", ")\\s(", df_dict$lu, fixed = T),")\\b")

df_dict$rp <- NA
df_dict$rp[df_dict$source == "wfh"] <-"<strong>\\1 \\2 \\3 \\4</strong>"
df_dict$rp[df_dict$source %in% c("generic", "negation")] <-"<em>\\1 \\2 \\3 \\4</em>"

df <- df %>%
  setDT(.) %>%
  .[, sequence_tagged := mgsub(x = sequence,
                            pattern = df_dict$lu[df_dict$source %in% c("wfh", "negation", "generic")],
                            replacement = df_dict$rp[df_dict$source %in% c("wfh", "negation", "generic")],
                            ignore.case = TRUE, fixed = FALSE, safe = TRUE)]

df <- df %>%
  .[, sequence_tagged := gsub("\n", "<br>", df$sequence_tagged)]

df <- df %>%
  .[, sequence_tagged := str_squish(sequence_tagged)]

df$sequence_tagged[df$seq_id == "767329136_0002"]
df$sequence_tagged <- gsub(" </strong>", "</strong>", df$sequence_tagged, fixed = T)
df$sequence_tagged <- gsub(" </em>", "</em>", df$sequence_tagged, fixed = T)

df$sequence_tagged[df$seq_id == "767329136_0002"]

Encoding(df$sequence_tagged) <- "ASCII"
df$sequence_tagged <- iconv(df$sequence_tagged, "latin1", "ASCII", sub="")

df <- df %>%
  .[, sequence_tagged := gsub("$", "<br><br><br><br>", df$sequence_tagged)]


View(df)

df <- df[sample(nrow(df)),]

fwrite(df, "/mnt/disks/pdisk/bgt-all/training_v6.csv")


#### EXTRACT DICTIONARY WFH DATA ####
remove(list = ls())

rm(list = ls())
paths_us <- list.files("/mnt/disks/pdisk/bg-us/int_data/wfh_v6/", pattern = "wfh_v6_raw_sequences", full.names = T) %>% sort(decreasing = T)
paths_uk <- list.files("/mnt/disks/pdisk/bg-uk/int_data/wfh_v6/", pattern = "wfh_v6_raw_sequences", full.names = T) %>% sort(decreasing = T)
paths_can <- list.files("/mnt/disks/pdisk/bg-can/int_data/wfh_v6/", pattern = "wfh_v6_raw_sequences", full.names = T) %>% sort(decreasing = T)
paths_anz <- list.files("/mnt/disks/pdisk/bg-anz/int_data/wfh_v6/", pattern = "wfh_v6_raw_sequences", full.names = T) %>% sort(decreasing = T)

paths <- c(paths_us, paths_uk, paths_can, paths_anz)

write.table(paths, file = "/mnt/disks/pdisk/bgt-all/paths.txt", sep = "\t",
            row.names = FALSE, col.names = FALSE)

write_lines(paths)

setwd("/mnt/disks/pdisk/bgt-all/")

system("zip archive -@ < /mnt/disks/pdisk/bgt-all/paths.txt")

#### EXTRACT SAMPLING FRAMES ####
paths <- c("/mnt/disks/pdisk/bg-can/int_data/aux_data/df_stru_ss_plus_v5_can.RData",
           "/mnt/disks/pdisk/bg-us/int_data/aux_data/df_stru_ss_plus_v5_us.RData",
           "/mnt/disks/pdisk/bg-uk/int_data/aux_data/df_stru_ss_plus_v5_uk.RData",
           "/mnt/disks/pdisk/bg-anz/int_data/aux_data/df_stru_ss_plus_v5_anz.RData")

file.exists(paths)

#### EXTRACT LIST OF PARSED SEQUENCES ####
# US
remove(list = ls())
setwd("/mnt/disks/pdisk/bg-us/")
source("/mnt/disks/pdisk/code/safe_mclapply.R")
paths <- list.files("./int_data/sequences/", pattern = ".rds", full.names = T) %>% sort(decreasing = T)
job_id_us <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    select(job_id) %>%
    distinct()
  return(df)
}, mc.cores = 16)

job_id_us <- job_id_us %>% bind_rows
job_id_us <- job_id_us %>% distinct()
head(job_id_us)
saveRDS(job_id_us, file = "/mnt/disks/pdisk/bgt-all/ads_list/us_job_id_list.rds")

# UK
remove(list = ls())
setwd("/mnt/disks/pdisk/bg-uk/")
source("/mnt/disks/pdisk/code/safe_mclapply.R")
paths <- list.files("./int_data/sequences/", pattern = ".rds", full.names = T) %>% sort(decreasing = T)
# WFH Examples
job_id_uk <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    select(job_id) %>%
    distinct()
  return(df)
}, mc.cores = 16)

job_id_uk <- job_id_uk %>% bind_rows
job_id_uk <- job_id_uk %>% distinct()
head(job_id_uk)
saveRDS(job_id_uk, file = "/mnt/disks/pdisk/bgt-all/ads_list/uk_job_id_list.rds")

# CAN
remove(list = ls())
setwd("/mnt/disks/pdisk/bg-can/")
source("/mnt/disks/pdisk/code/safe_mclapply.R")
paths <- list.files("./int_data/sequences/", pattern = ".rds", full.names = T) %>% sort(decreasing = T)
# WFH Examples
job_id_can <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    select(job_id) %>%
    distinct()
  return(df)
}, mc.cores = 16)

job_id_can <- job_id_can %>% bind_rows
job_id_can <- job_id_can %>% distinct()
head(job_id_can)
saveRDS(job_id_can, file = "/mnt/disks/pdisk/bgt-all/ads_list/can_job_id_list.rds")

# ANZ
remove(list = ls())
setwd("/mnt/disks/pdisk/bg-anz/")
source("/mnt/disks/pdisk/code/safe_mclapply.R")
paths <- list.files("./int_data/sequences/", pattern = ".rds", full.names = T) %>% sort(decreasing = T)
# WFH Examples
job_id_anz <- safe_mclapply(1:length(paths), function(i) {
  df <- readRDS(paths[i]) %>%
    select(job_id) %>%
    distinct()
  return(df)
}, mc.cores = 16)

job_id_anz <- job_id_anz %>% bind_rows
job_id_anz <- job_id_anz %>% distinct()
head(job_id_anz)
saveRDS(job_id_anz, file = "/mnt/disks/pdisk/bgt-all/ads_list/anz_job_id_list.rds")
