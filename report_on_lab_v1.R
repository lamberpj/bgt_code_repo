#### SETUP ####
remove(list = ls())

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

#quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg-us/")
#setDTthreads(1)
remove(list = ls())
#### end ####

#### CHECK NEW v4 same structure as OLD v4 ####
old_paths <- list.files("./int_data/sentences_old/")
new_paths <- list.files("./int_data/sentences/")

all.equal(old_paths, new_paths)

old_paths <- list.files("./int_data/sentences_old/", full.names = T)
new_paths <- list.files("./int_data/sentences/", full.names = T)

source("/mnt/disks/pdisk/code/safe_mclapply.R")
equal_structure <- safe_mclapply(1:length(old_paths), function(i) {
  old <- readRDS(old_paths[[i]])
  new <- readRDS(new_paths[[i]])
  
  return(all.equal(old$sentence_id, new$sentence_id))
  
}, mc.cores = 12)

equal_structure
#### END ####

#### GET LABELS AND UPDATE SETENCES ####
lab_df <- fread("./int_data/labels/labels_18-01-22.csv")

lab_df <- lab_df %>% select(sentence_id, sentences_tagged, weighted.label)

# Get new sentences
paths <- list.files("./int_data/sentences/", full.names = T)

source("/mnt/disks/pdisk/code/safe_mclapply.R")
new_sentences <- safe_mclapply(1:length(paths), function(i) {
  x <- readRDS(paths[[i]]) %>%
    setDT(.) %>%
    .[sentence_id %in% lab_df$sentence_id] %>%
    .[, file := paths[[i]]]
  return(x)
}, mc.cores = 12)
new_sentences <- new_sentences %>% rbindlist()

colnames(lab_df)
colnames(new_sentences)

lab_df <- lab_df %>%
  merge(., new_sentences, by = "sentence_id")

fwrite(lab_df, file = "./int_data/labels/labels_18-01-22.csv")

#### /END ####


#### AUGMENT LABELS WITH DICT ####

# Import dictionary
remove(list = ls())
lab_df <- fread("./int_data/labels/labels_18-01-22.csv")
load(file = "./large_sample_us_sent_dictionary.RData")
remove(list = setdiff(ls(), c("lab_df", "df_kw")))

setDT(lab_df)
setDT(df_kw)

# Everything
head(df_kw)

nrow(lab_df) # 10,500
lab_df <- lab_df %>%
  merge(., df_kw %>% select(-c(job_id, sentences_tagged)), all.x = FALSE, all.y = FALSE, by = "sentence_id")
nrow(lab_df) # 10,486
head(lab_df)
check <- lab_df %>%
  .[, del := max(as.numeric(source == "wfh")), by = sentence_id] %>%
  .[del == 0 & weighted.label > 0] %>%
  select(-del) %>%
  distinct(sentence_id, .keep_all = T)

fwrite(check, "check.csv")

lab_df_wide <- lab_df %>%
  select(-c(glob_en, source, sentences_tagged)) %>%
  group_by(sentence_id) %>%
  pivot_wider(., values_from = hit, names_from = key)

lab_df_wide <- lab_df_wide %>% ungroup()
lab_df_wide[is.na(lab_df_wide)] <- 0

fwrite(lab_df, file = "./int_data/labels/lab_df.csv")
fwrite(lab_df_wide, file = "./int_data/labels/lab_df_wide.csv")

# Check WFH not negated intensity
head(df_kw)

df_kw_wfh_nn <- df_kw %>%
  .[, rm := ifelse(source == "negation", 1, 0)] %>%
  .[, rm := max(rm), by = sentence_id] %>%
  .[rm == 0] %>%
  select(-rm) %>%
  .[source == "wfh" | source == "intensity"]

nrow(lab_df) # 10,500
lab_df <- lab_df %>%
  merge(., df_kw_wfh_nn %>% select(-c(job_id, sentences_tagged)), all.x = FALSE, all.y = FALSE, by = "sentence_id")
nrow(lab_df) # 10,486

df_wfh_and_day <- lab_df %>%
  filter(weighted.label == 1) %>%
  group_by(sentence_id) %>%
  filter(any(key %in% c("day", "days"))) %>%
  ungroup() %>%
  filter(source == "wfh") %>%
  select(sentence_id, sentence, weighted.label, key)

fwrite(df_wfh_and_day, file = "./example_intensit")

# Check WFH not negated
head(df_kw)

df_kw_wfh_nn <- df_kw %>%
  .[, rm := ifelse(source == "negation", 1, 0)] %>%
  .[, rm := max(rm), by = sentence_id] %>%
  .[rm == 0] %>%
  select(-rm) %>%
  .[source == "wfh"]
  
nrow(lab_df) # 10,500
lab_df <- lab_df %>%
  merge(., df_kw_wfh_nn %>% select(-c(job_id, sentences_tagged)), all.x = FALSE, all.y = FALSE, by = "sentence_id")
nrow(lab_df) # 10,486

lab_df_wide <- lab_df %>%
  select(-c(glob_en, source, sentences_tagged)) %>%
  group_by(sentence_id) %>%
  pivot_wider(., values_from = hit, names_from = key)

lab_df_wide <- lab_df_wide %>% ungroup()
lab_df_wide[is.na(lab_df_wide)] <- 0

lab_df_wide_lit_wfh <- lab_df_wide[lab_df_wide$work_from_home == 1 & lab_df_wide$weighted.label == 0,]
lab_df_wide_lit_wfh <- lab_df_wide[lab_df_wide$work_from_home == 1 & lab_df_wide$weighted.label == 0,]



table(lab_df$key[lab_df$source == "intensity"])

colnames(lab_df_wide[(lab_df_wide$days == 1 | lab_df_wide$day == 1) & lab_df_wide$weighted.label > 0.5,])

df_wfh_and_day <- lab_df_wide[(lab_df_wide$days == 1 | lab_df_wide$day == 1) & lab_df_wide$weighted.label > 0.5,] %>% select(sentence_id, weighted.label, sentence)

colnames(lab_df_wide)
feat_list <- colnames(lab_df_wide)[8:110]

test <- lapply(1:length(feat_list), function(i) {
  print(i)
  lab_df_wide %>% select(sentence_id, weighted.label, feat_list[[i]]) %>%
    filter(.[,3] == 1) %>%
    summarise(mean_wfh = mean(weighted.label), n_lab_sent = n()) %>%
    mutate(key = feat_list[[i]]) %>%
    select(key, n_lab_sent, mean_wfh)
}) %>% bind_rows %>%
  mutate(abs_contr_wfh = round(mean_wfh*n_lab_sent))

lu <- lab_df %>% select(key, glob_en, source) %>% distinct()
nrow(test) # 268
test <- test %>% left_join(lu)
nrow(test) # 268

View(test)

plot(test$mean_wfh, test$abs_contr_wfh/sum(test$abs_contr_wfh))


# Check Generic in WFH Hit
head(df_kw)

nrow(lab_df) # 10,500
lab_df <- lab_df %>%
  merge(., df_kw %>% select(-c(job_id, sentences_tagged)), all.x = FALSE, all.y = FALSE, by = "sentence_id")
nrow(lab_df) # 42,547

colnames(lab_df)

table(lab_df$key[lab_df$source == "generic"])

lab_df_wide <- lab_df %>%
  select(-c(glob_en, source, sentences_tagged)) %>%
  group_by(sentence_id) %>%
  pivot_wider(., values_from = hit, names_from = key)

table()

lab_df_wide <- lab_df_wide %>% ungroup()

lab_df_wide[is.na(lab_df_wide)] <- 0

lab_df_wide$generic <- pmax(lab_df_wide$home + lab_df_wide$location + lab_df_wide$remote_ast + lab_df_wide$work)

cor(lab_df_wide$generic, lab_df_wide$weighted.label)

colnames(lab_df_wide)
feat_list <- colnames(lab_df_wide)[8:110]

test <- lapply(1:length(feat_list), function(i) {
  print(i)
  lab_df_wide %>% select(sentence_id, weighted.label, feat_list[[i]]) %>%
    filter(.[,3] == 1) %>%
    summarise(mean_wfh = mean(weighted.label), n_lab_sent = n()) %>%
    mutate(key = feat_list[[i]]) %>%
    select(key, n_lab_sent, mean_wfh)
}) %>% bind_rows %>%
  mutate(abs_contr_wfh = round(mean_wfh*n_lab_sent))

lu <- lab_df %>% select(key, glob_en, source) %>% distinct()
nrow(test) # 268
test <- test %>% left_join(lu)
nrow(test) # 268

View(test)

plot(test$mean_wfh, test$abs_contr_wfh/sum(test$abs_contr_wfh))




