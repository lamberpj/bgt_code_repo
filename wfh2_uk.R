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
#library("fixest")
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
#### end ####

#### SUBSET DATA ####
# LOAD EXAMPLE

paths <- list.files("./raw_data/main", pattern = ".txt", full.names = T)
paths

drop_occ <- 
  data.frame(soc2 = c("29","31","35","37","45","47","49","51","53","55"),
             label = c("Healthcare Practitioners and Technical Occupations",
                       "Healthcare Support Occupations",
                       "Food Preparation and Serving Related Occupations",
                       "Building and Grounds Cleaning and Maintenance Occupations",
                       "Farming, Fishing, and Forestry Occupations",
                       "Construction and Extraction Occupations",
                       "Installation, Maintenance, and Repair Occupations",
                       "Production Occupations",
                       "Transportation and Material Moving Occupations",
                       "Military Specific Occupations"))

drop_ind <- data.frame(sector_name = c("ACCOMMODATION AND FOOD SERVICE ACTIVITIES", "TRANSPORTATION AND STORAGE", "CONSTRUCTION"))

source("/mnt/disks/pdisk/code/safe_mclapply.R")
#colnames(fread(paths[1], nrow = 10))
df_stru <- safe_mclapply(1:length(paths), function(i) {
  i = 1
  warning(paste0("\nSTART: ",i,"\n"))
  warning(paste0(paths[i]))
  df <- fread(paths[i], data.table = F, nThread = 1, colClasses = "character", stringsAsFactors = FALSE,
              select = c("JobID", "JobDate", "BGTCareerAreaName", "UKSOCCode", "BGTOcc", "CanonCounty", "SICSection", "CanonEmployer")) %>%
    clean_names
  df[df == "na"] <- NA
  df[df == "-999"] <- NA
  df[df == ""] <- NA
  
  df <- df %>% mutate(keep1 = ifelse(!is.na(uksoc_code) & !is.na(canon_county) & !is.na(canon_employer), T, F))
  df <- df %>%
    mutate(soc2 = str_sub(bgt_occ, 1, 2)) %>%
    mutate(keep2 = ifelse(!(soc2 %in% drop_occ$soc2) & !(sic_section %in% drop_ind$sector_name), T, F))
  warning(paste0("\nDONE: ",i))
  return(df)
}, mc.cores = 16)

df_stru <- bind_rows(df_stru)
head(df_stru)

df_stru <- df_stru %>% mutate(job_ymd = ymd(job_date))
df_stru <- df_stru %>% mutate(job_ym = paste0(year(job_ymd), "-", sprintf("%02d", month(job_ymd))))

1-0.3197381

1-0.746958

1-0.2194153

mean(df_stru$keep1) # 0.3197381
mean(df_stru$keep2) # 0.746958
nrow(df_stru[df_stru$keep1 == T & df_stru$keep2 == T,])/nrow(df_stru) # 0.2194153
rm(list = setdiff(ls(),"df_stru"))

job_id_vector <- df_stru$job_id
save(job_id_vector, file = "./aux_data/job_id_vector.RData")
job_id_keep_vector <- df_stru$job_id[df_stru$keep1 == T & df_stru$keep2 == T]
save(job_id_keep_vector, file = "./aux_data/job_id_keep_vector.RData")
rm("df_stru")
rm(list = setdiff(ls(), c("job_id_keep_vector", "job_id_vector", "test")))
#### IMPORT RAW TEXT ####
load(file = "./aux_data/job_id_keep_vector.RData")
#load(file = "./aux_data/job_id_vector.RData")

#### GET PATH NAMES TO MAKE SENTENCES ####
paths <- list.files("./raw_data/text/", pattern = "*.zip", full.names = T, recursive = T)
paths_done <- list.files("./int_data/sentences/", pattern = "*.rds", full.names = F) %>%
  gsub("sentences_", "", .) %>% gsub(".rds", "", .) %>% unique
paths_check <- list.files("./raw_data/text/", pattern = "*.zip", full.names = T) %>%
  str_sub(., -21, -5)
paths_check
paths <- paths[!(paths_check %in% paths_done)]
paths
remove(list = c("paths_check", "paths_done"))
set.seed(1)

paths <- paths[grepl("20160129_20160204", paths)]

#### /END ####

source("/mnt/disks/pdisk/code/safe_mclapply.R")
#### READ XML NAD MAKE SENTENCES ####
safe_mclapply(1:length(paths), function(i) {
  i = 1
  name <- str_sub(paths[i], 38, -5)
  name
  warning(paste0("\nBEGIN: ",i,"  '",name,"'"))
  sink("./aux_data/log_file_make_sentences.txt", append=TRUE)
  cat(paste0("\nBEGIN: ",i,"  '",name,"'"))
  sink()
  system(paste0("unzip ",paths[i]," -d ./raw_data/text/"))
  xml_path = gsub(".zip", ".xml", paths[i])
  xml_path
  
  df_xml <- read_xml(xml_path) %>%
    xml_find_all(., ".//Job")
  
  df_job_id <- xml_find_all(df_xml, ".//JobID") %>% xml_text
  df_job_title <- xml_find_all(df_xml, ".//CleanJobTitle") %>% xml_text
  df_job_text <- xml_find_all(df_xml, ".//JobText") %>% xml_text
  remove("df_xml")
  df <- data.frame("job_id" = df_job_id, "job_text" = paste0(df_job_title,". ",df_job_text))
  remove(list = c("df_job_id","df_job_title","df_job_text"))
  
  df_ss <- df %>% filter(job_id %in% job_id_keep_vector)
  
  rm(df)
  
  head(df_ss)
  
  # Clean text
  df_ss$job_text_clean <- df_ss$job_text
  df_ss$job_text_clean <- gsub("\\|", "\\:", df_ss$job_text_clean)
  df_ss$job_text_clean <- gsub("%", " pc ", df_ss$job_text_clean, fixed = T)
  df_ss$job_text_clean <- gsub("(?<=\\b[A-Z])\\.", "", df_ss$job_text_clean, perl = T)
  df_ss$job_text_clean <- gsub("(?<=[0-9\\s])\\.", "\\'", df_ss$job_text_clean, perl = T)
  df_ss$job_text_clean <- gsub("(?<=[0-9])\\.", "-", df_ss$job_text_clean, perl = T)
  df_ss$job_text_clean <- stringi::stri_replace_all_fixed(str = df_ss$job_text_clean, pattern = corpus::abbreviations_en, replacement = gsub("\\.", "", corpus::abbreviations_en, perl = T), vectorize_all=FALSE)
  df_ss$job_text_clean <- gsub("([A-Z][a-z])\\.\\s", "\\1 ", df_ss$job_text_clean)
  df_ss$job_text_clean <- gsub("\u00A0", " ", df_ss$job_text_clean, fixed =TRUE)
  df_ss$job_text_clean <- replace_html(df_ss$job_text_clean,  symbol = TRUE)
  df_ss$job_text_clean <- gsub("[^[[:punct:]][:alnum:][:space:]]","", df_ss$job_text_clean)
  df_ss$job_text_clean <- str_squish(df_ss$job_text_clean)
  df_ss$job_text_clean <- gsub("s\\-", " ", df_ss$job_text_clean)
  df_ss$job_text_clean <- gsub("@", " at ", df_ss$job_text_clean)
  df_ss$job_text_clean <- gsub("[^[[:punct:]][:alnum:][:space:]]","", df_ss$job_text_clean)
  df_ss$job_text_clean <- gsub("\\*", "", df_ss$job_text_clean)
  df_ss$job_text_clean <- gsub('([[:punct:]])\\1+', '\\1', df_ss$job_text_clean)
  df_ss$job_text_clean <- gsub('([[:punct:]])', '\\1 ', df_ss$job_text_clean)
  df_ss$job_text_clean <- gsub('\\t', '\\s', df_ss$job_text_clean)
  df_ss$job_text_clean <- gsub("(\\n|^)(.{1,20})(?=[\\n|$])", "\\1 \\2: ", df_ss$job_text_clean, perl = T)
  df_ss$job_text_clean <- gsub("(\\n|^)(.{20,})(?=[\\n|$])", "\\1 \\2\\. ", df_ss$job_text_clean, perl = T)
  df_ss$job_text_clean <- gsub("([[:punct:]])\\s*\\.", "\\1", df_ss$job_text_clean)
  df_ss$job_text_clean <- gsub("\\s+:", ":", df_ss$job_text_clean)
  df_ss$job_text_clean <- gsub(":", ": ", df_ss$job_text_clean)
  df_ss$job_text_clean <- gsub('([[:punct:]])\\1+', '\\1', df_ss$job_text_clean)
  
  df_ss$job_text_clean <- str_squish(df_ss$job_text_clean)
  
  df_ss_sentence <- df_ss %>% 
    mutate(sentence = strsplit(as.character(job_text_clean), "(?<=[[\\.]|[\\?]|[!]])", perl = T)) %>% 
    unnest(sentence) %>%
    mutate(sentence = str_squish(sentence)) %>%
    select(-c(job_text_clean, job_text))
  
  rm("df_ss")
  
  head(df_ss_sentence)
  
  df_ss_sentence <- df_ss_sentence %>%
    group_by(job_id) %>%
    mutate(sentence_id = paste0(job_id,"_",sprintf("%04d", row_number()))) %>%
    ungroup()
  
  saveRDS(df_ss_sentence, file = paste0("./int_data/sentences/sentences_",name,".rds"))
  
  unlink(xml_path)
  
  warning(paste0("SUCCESS: ",i))
  sink("./aux_data/log_file_make_sentences.txt", append=TRUE)
  cat(paste0("\nSUCCESS: ",i,"\n"))
  sink()
  return("")
}, mc.cores = 16)

sink()
system("echo sci2007! | sudo -S shutdown -h now")


#### END ####

#### DICTIONARIES ####
df_dict <- read_xlsx("/mnt/disks/pdisk/bg-us/aux_data/wfh_v2xlsx.xlsx") %>% clean_names
df_dict$glob_en <- tolower(df_dict$glob_en)
df_dict[df_dict == ""] <- NA
df_dict <- df_dict %>%
  distinct(glob_en, .keep_all = T)
length(df_dict$glob_en) # 169
length(unique(df_dict$glob_en)) # 169
df_dict <- df_dict %>%
  mutate(key = glob_en) %>%
  mutate(key = gsub("\\s+", "_", key)) %>%
  mutate(key = gsub("[*]", "_AST_", key)) %>%
  mutate(key = gsub("[:]", "_C_", key)) %>%
  mutate(key = gsub("^_", "", key)) %>%
  mutate(key = gsub("_$", "", key)) %>%
  mutate(key = gsub("__", "_", key))
df_dict$key[df_dict$source == "bgt"] <- paste0("bgt_",df_dict$key[df_dict$source == "bgt"])
df_dict <- df_dict %>%
  mutate(key = make_clean_names(key)) %>%
  select(key, glob_en)

df_neg <- data.frame("glob_en" = c("no","not","can't","cant","cannot","unable","can not"), "key" = c("neg","neg","neg","neg","neg","neg","neg"))

df_disq <- read_xlsx("/mnt/disks/pdisk/bg-us/aux_data/wfh_v2xlsx.xlsx", sheet = 3) %>% clean_names %>% select(key) %>% distinct(key) %>% rename(glob_en = key) %>% mutate(key = "disqual")

head(df_dict)
head(df_neg)
head(df_disq)

df_dict <- bind_rows(df_dict, df_neg, df_disq)

length(df_dict$glob_en) # 169
length(unique(df_dict$glob_en)) # 169
ls_dict_wfh <- setNames(as.list(df_dict$glob_en), df_dict$key)
wfh_dict <- dictionary(ls_dict_wfh)
rm(list = setdiff(ls(),c("wfh_dict")))
#### end ####

#### GET PATH NAMES TO MAKE DFM ####
paths <- list.files("./int_data/sentences/", pattern = "*.rds", full.names = T) %>% sort(decreasing = T)
paths
paths_check <- paths %>% gsub("./int_data/sentences//sentences_","",., fixed = T) %>% gsub(".rds", "", ., fixed = T) %>% gsub("_AddFeed_", "", ., fixed = T)
paths_done <- list.files("./int_data/wfh_v2/", pattern = "wfh_v2_raw_sentences_" , full.names = F) %>% gsub(".rds", "", ., fixed = T) %>% gsub("wfh_v2_raw_sentences_", "", ., fixed = T)
paths_check
paths_done
paths <- paths[!(paths_check %in% paths_done)]
rm(list = c("paths_check", "paths_done"))
#### /END ####
paths


#### MAKE DFM ####
source("/mnt/disks/pdisk/code/safe_mclapply.R")
safe_mclapply(1:length(paths), function(i) {
  
  timeStart<-Sys.time()
  
  sink("./aux_data/log_file_make_wfh_feats.txt", append=TRUE)
  #warning(paste0("BEGIN FILE: ",i))
  cat(paste0("\nBEGIN FILE: ",i,"\n"))
  sink()
  name <- gsub("./int_data/sentences//sentences_","", paths[i], fixed = T) %>% gsub(".rds", "", ., fixed = T) %>% gsub("_AddFeed_", "", ., fixed = T)
  df_ss_sentence <- readRDS(paths[i])
  df_ss_sentence$sentence <- tolower(df_ss_sentence$sentence)
  df_ss_sentence <- df_ss_sentence %>% filter(nchar(sentence) > 10)
  
  m <- nrow(df_ss_sentence)
  df_ss_sentence_ls <- df_ss_sentence %>%
    mutate(group_id = seq(1:m) %/% 10000) %>%
    group_split(group_id)
  
  rm(df_ss_sentence)
  
  q <- length(df_ss_sentence_ls)
  head(df_ss_sentence_ls[[1]])
  df_dfm_ls <- safe_mclapply(1:q, function(j) {
    #sink("./aux_data/log_file_make_wfh_feats.txt", append=TRUE)
    #warning(paste("BEGIN PEICE: ",j,"  ",j/q))
    #cat(paste("BEGIN PEICE: ",j,"  ",j/q))
    x <- df_ss_sentence_ls[[j]] %>%
      quanteda::corpus(., text_field = "sentence", docid_field = "sentence_id", unique_docnames = TRUE) %>%
      quanteda::tokens(., what = "word", remove_punct = F, remove_symbols = F, remove_url = T, remove_separators = T,  split_hyphens = T, verbose = T)
    x_dfm <- quanteda::dfm(tokens_lookup(x,  wfh_dict, valuetype = "glob", case_insensitive = F, verbose = TRUE))
    rm(x)
    colnames(x_dfm)
    x_dfm <- x_dfm[rowSums(x_dfm[, -match(c("neg"), colnames(x_dfm))])>0,]
    #warning(paste("DONE PEICE: ",j,"  ",j/q))
    #cat(paste("DONE PEICE: ",j,"  ",j/q))
    return(x_dfm)
  }, mc.cores = 30)
  
  #warning(paste("DONE ALL PEICES: ",j/q))
  sink("./aux_data/log_file_make_wfh_feats.txt", append=TRUE)
  cat(paste("\nDONE PEICES: ",i,"\n"))
  sink()
  
  rm(df_ss_sentence_ls)
  
  str <-paste0("df_dfm <- rbind(",paste0(paste0("df_dfm_ls[[",seq(1,q,1),"]]"), collapse = ", "),")")
  eval(parse(text = str))
  
  rm(df_dfm_ls)
  
  df_ss_sentence <- readRDS(paths[i]) %>% filter(sentence_id %in% docnames(df_dfm))
  
  sink("./aux_data/log_file_make_wfh_feats.txt", append=TRUE)
  cat(paste0("\nALL EQUAL: ",i,"  ",all.equal(df_ss_sentence$sentence_id, docnames(df_dfm))),"\n")
  sink()
  
  df_ss_sentence <- cbind(df_ss_sentence, convert(df_dfm, to = "data.frame"))
  
  saveRDS(df_ss_sentence, file = paste0("./int_data/wfh_v2/wfh_v2_raw_sentences_",name,".rds"))
  saveRDS(df_dfm, file = paste0("./int_data/wfh_v2/wfh_v2_dfm_",name,".rds"))
  
  timeEnd<-Sys.time()
  difference <- round(as.numeric(difftime(timeEnd, timeStart, units='mins')),digits = 2)
  difference
  sink("./aux_data/log_file_make_wfh_feats.txt", append=TRUE)
  cat(paste0("\nSUCCESS: ",i,"\n"))
  cat(paste0("\nDID: ",i," IN  ",difference," minutes\n"))
  sink()
  return("")
}, mc.cores = 56)

#sink()
#system("echo sci2007! | sudo -S shutdown -h now")

#### END ####

#### BEGIN CHECK KEYWORDS ####
# Combine all data
rm(list = ls())
source("/mnt/disks/pdisk/code/safe_mclapply.R")
paths <- list.files("./int_data/wfh_v2/", pattern = "wfh_v2_raw_sentences", full.names = T) %>% sort(decreasing = T)
wfh_v2_raw_sentences <- safe_mclapply(paths, readRDS, mc.cores = 28) %>% bind_rows

colnames(wfh_v2_raw_sentences)
nrow(wfh_v2_raw_sentences) # 39,555,564
wfh_v2_raw_sentences <- wfh_v2_raw_sentences[rowSums(wfh_v2_raw_sentences[,c(5:207)])>0,]
nrow(wfh_v2_raw_sentences) # 1,127,210

wfh_v2_raw_sentences[,c(5:209)][wfh_v2_raw_sentences[,c(5:209)]>0] <- 1

sentence_weights <- wfh_v2_raw_sentences %>%
  group_by(sentence) %>%
  summarise(n = n()) %>%
  ungroup %>%
  arrange(desc(n), sentence) %>%
  mutate(n_weight = 1/n)

nrow(wfh_v2_raw_sentences) # 1,127,210
wfh_v2_raw_sentences <- wfh_v2_raw_sentences %>%
  left_join(sentence_weights)
nrow(wfh_v2_raw_sentences) # 1,127,210

colnames(wfh_v2_raw_sentences)

df_n <- wfh_v2_raw_sentences %>%
  summarise(across(based_ast_at_home:bgt_telecommute_yes, ~ sum(.x, na.rm = TRUE))) %>% pivot_longer(cols = everything()) %>% rename(n = value)

df_n_w <- wfh_v2_raw_sentences %>%
  summarise(across(based_ast_at_home:bgt_telecommute_yes, ~ sum(.x*n_weight, na.rm = TRUE))) %>% pivot_longer(cols = everything()) %>% rename(n_weighted = value)

df <- df_n %>% left_join(df_n_w) %>% rename(key = name)

fwrite(df, file = "./aux_data/keyword_hits.csv")

rm(list = setdiff(ls(), "wfh_v2_raw_sentences"))

check_keys <- c("role_in_home","location_anywhere","home_pc","stay_ast_at_home","home_post","wah","work_off_site","work_virtually","office_ast_at_home","home_computer","office_ast_from_home","homeoffice","home_role","home_position","remote_first","stay_at_home","wfh","homeworking","home_office","home_job","from_home")

head(wfh_v2_raw_sentences)

source("/mnt/disks/pdisk/code/safe_mclapply.R")
safe_mclapply(1:length(check_keys), function(i) {
  check_keys[[i]]
  x <- wfh_v2_raw_sentences %>%
    filter(.[,c(check_keys[[i]])] > 0) %>%
    mutate() %>%
    select(job_id, sentence_id, sentence, check_keys[[i]], neg, n_weight) %>%
    arrange(neg) %>%
    group_by(neg) %>%
    sample_n(., size = pmin(50,n()), replace=F, weight = n_weight) %>%
    ungroup() %>%
    mutate(wfh = "") %>%
    select(sentence_id, sentence, check_keys[[i]], neg, wfh)
  
  if (nrow(x) > 0) {fwrite(x, file = paste0("./aux_data/wfh_v2_checks/",check_keys[[i]],".csv"))}
  return("")
}, mc.cores = 32)


#### end ####

#### MAKE VECTOR ####
colnames(wfh_v2_raw_sentences)
wfh_v2 <- wfh_v2_raw_sentences %>%
  select(job_id,sentence_id,n,n_weight,at_home_position,done_at_home,worked_at_home,completed_from_home,virtual_role,completed_at_home,undertaken_from_home,working_from_anywhere,
         job_from_home,based_ast_at_home,hybrid_position,partially_remote,working_ast_at_home,hybrid_work,telecommuting,mobile_work,working_virtually,at_home_job,role_ast_from_home,
         virtual_work,work_remote,performed_remotely,worked_from_home,working_remote,teleworking,from_home_position,homeworking,done_from_home,office_ast_at_home,work_in_home,
         from_home_role,located_anywhere,work_ast_at_home,job_ast_from_home,work_ast_remotely,based_ast_from_home,working_ast_remotely,office_ast_from_home,remote_first,
         from_home_work,telework,location_remote,telecommute,work_from_anywhere,work_ast_remote,working_at_home,home_work,remote_position,based_at_home,from_home_job,stay_ast_from_home,
         x100_pc_remote,working_ast_remote,work_at_home,remote_based,working_ast_from_home,remote_work,based_from_home,work_ast_from_home,hybrid_working,location_ast_remote,homebased,
         fully_remote,working_remotely,wfh,work_off_site,home_working,working_from_home,remote_working,home_based,work_remotely,work_from_home,work_virtually,neg,disqual)

colnames(wfh_v2)

save(wfh_v2, file = "./aux_data/disag_keywords_wfh2.RData")

wfh_v2$wfh <- ifelse(rowSums(wfh_v2[,c(5:77)])>0,1,0)

wfh_v2 <- wfh_v2 %>%
  select(job_id,sentence_id,n,n_weight,wfh,neg,disqual) %>%
  filter(wfh == 1)

wfh_v2_job_id <- wfh_v2 %>%
  group_by(job_id) %>%
  summarise(neg = max(neg), wfh_w = mean(n_weight)) %>%
  mutate(wfh = 1)

head(wfh_v2_job_id)

nrow(wfh_v2_job_id) # 675,659
length(job_id_keep_vector) # 13,390,132

save(wfh_v2_job_id, file = "./aux_data/wfh_v2_job_id.RData")

#### COMBINE AND MERGE ####
remove(list = ls())
load(file = "./aux_data/wfh_v2_job_id.RData")
load(file = "./aux_data/job_id_keep_vector.RData")

paths <- list.files("./raw_data/main", pattern = ".txt", full.names = T)

colnames(fread(paths[1], data.table = F, nThread = 1, colClasses = "character", stringsAsFactors = FALSE, nrow = 500))

source("/mnt/disks/pdisk/code/safe_mclapply.R")
#colnames(fread(paths[1], nrow = 10))
df_stru <- safe_mclapply(1:length(paths), function(i) {
  
  warning(paste0("\nSTART: ",i,"\n"))
  warning(paste0(paths[i]))
  df <- fread(paths[i], data.table = F, nThread = 1, colClasses = "character", stringsAsFactors = FALSE,
              select = c("JobID", "JobDate", "BGTCareerAreaName", "UKSOCCode", "BGTOcc", "Nation", "Region", "TTWA", "CanonCounty", "SICSection", "CanonEmployer",
                         "CanonJobType","MinAnnualSalary", "MaxAnnualSalary")) %>%
    clean_names %>%
    filter(job_id %in% job_id_keep_vector)
  df[df == "na"] <- NA
  df[df == "-999"] <- NA
  df[df == ""] <- NA
  
  df <- df %>% filter(job_id %in% job_id_keep_vector)
  
  df <- df %>% mutate(job_date = ymd(job_date)) %>%
    mutate(year_month = paste0(year(job_date), "-", sprintf("%02d", month(job_date)))) %>%
    mutate(year_quarter = as.yearqtr(job_date, with_year = T))
  
  warning(paste0("\nDONE: ",i))
  return(df)
}, mc.cores = 32)

df_stru <- bind_rows(df_stru)
head(df_stru)

df_stru <- df_stru %>%
  left_join(wfh_v2_job_id)

df_stru$neg[is.na(df_stru$neg)] <- 0
df_stru$wfh_w[is.na(df_stru$wfh_w)] <- 0
df_stru$wfh[is.na(df_stru$wfh)] <- 0

df_stru <- df_stru %>%
  mutate(wfh_nn = ifelse(wfh>0 & neg==0, 1, 0),
         wfh_w_nn = ifelse(wfh>0 & neg==0, wfh_w, 0))

df_stru <- df_stru 

saveRDS(df_stru, file = "./int_data/df_stru.rds")

#### END ####

#### CREATE AGGREGATED DATA ####
df_stru_ag <- df_stru %>%
  mutate(county = canon_county,
         soc_us = str_sub(bgt_occ, 1, -4)) %>%
  group_by(year_month, soc_us, bgt_occ, county) %>%
  summarise(n = n(),
            wfh = sum(wfh, na.rm = T),
            wfh_nn = sum(wfh_nn, na.rm = T),
            wfh_w = sum(wfh_w, na.rm = T),
            wfh_w_nn = sum(wfh_w_nn, na.rm = T))

head(df_stru_ag)

fwrite(df_stru_ag, "./int_data/uk_wfh_ag_df.csv")

#### END ####

#### VISUALISE KEYWORDS ####
remove(list = ls())
load(file = "./aux_data/disag_keywords_wfh2.RData")
df_stru <- readRDS(file = "./int_data/df_stru.rds")

df_stru <- df_stru %>% filter(job_id %in% df_stru$job_id)

wfh_v2 <- wfh_v2 %>%
  left_join(df_stru %>% select(job_id, year_quarter), by = c("job_id" = "job_id"))

wfh_v2 <- wfh_v2 %>%
  mutate(year = str_sub(year_quarter, 1, 4)) %>%
  group_by(year) %>%
  summarise(across(at_home_position:work_virtually, sum))

wfh_v2$row_sum <- rowSums(wfh_v2[,c(2:74)])

wfh_v2 <- wfh_v2 %>%
  group_by(year) %>%
  summarise(across(at_home_position:work_virtually, ~ round(100*.x/row_sum),digits = 2))

wfh_v2_t <- as.data.frame(t(wfh_v2)) %>% rownames_to_column(var = "keyword")
colnames(wfh_v2_t) <- c("keyword", "year_2014", "year_2015", "year_2016", "year_2017", "year_2018", "year_2019", "year_2020", "year_2021")
nrow(wfh_v2_t) # 74
wfh_v2_t <- wfh_v2_t[2:74,]
colnames(wfh_v2_t)

wfh_v2_t <- wfh_v2_t %>% select(keyword, year_2021) %>% arrange(keyword)

wfh_v2_t$keyword
wfh_v2_t$keyword[59:73]

stargazer(as.data.frame(cbind(wfh_v2_t$keyword[1:19], wfh_v2_t$keyword[20:38], wfh_v2_t$keyword[39:57], wfh_v2_t$keyword[58:73])), summary = F, title = "Keywords by Share of Hits (UK)", rownames = F)

#### END ####

#### CASE STUDIES ####
remove(list = ls())
uk_wfh_ag_df <- fread("./int_data/uk_wfh_ag_df.csv")
head(uk_wfh_ag_df)
uk_wfh_ag_df <- uk_wfh_ag_df %>% mutate(year = str_sub(year_month, 1, 4)) %>% group_by(year, soc_us) %>% summarise(n = sum(n), wfh_nn = sum(wfh_nn)/sum(n), wfh_w_nn = sum(wfh_w_nn)/sum(n)) %>% filter(n > 500)

View(uk_wfh_ag_df[grepl("15-", uk_wfh_ag_df$soc_us),])
#### END ####

#### TABLES ####
remove(list = ls())

soc2010 <- read_xls("./aux_data/soc_structure_2010.xls", skip = 11) %>% clean_names() %>%
  mutate(minor_gropup_name = ifelse(!is.na(minor_group), x5, NA)) %>%
  fill(., minor_gropup_name, .direction = "downup") %>%
  filter(!is.na(broad_group)) %>%
  select(broad_group, minor_gropup_name) %>%
  mutate(soc5 = str_sub(broad_group, 1, 6))

us_wfh_ag_df <- fread("./int_data/us_wfh_ag_df.csv")
head(us_wfh_ag_df)
us_wfh_ag_df <- us_wfh_ag_df %>% mutate(year = str_sub(year_month, 1, 4), soc5 = str_sub(soc_us, 1, 6)) %>%
  left_join(soc2010)

head(us_wfh_ag_df)

us_wfh_ag_df <- us_wfh_ag_df %>% 
  group_by(year, minor_gropup_name) %>%
  summarise(n = sum(n), wfh_nn = sum(wfh_nn)/sum(n), wfh_w_nn = sum(wfh_w_nn)/sum(n)) %>%
  ungroup

test_2021 <- us_wfh_ag_df %>% filter(year == 2021) %>% mutate(quartile = ntile(wfh_nn, 50)) %>% mutate(wfh_nn = round(100*wfh_nn, digits = 2)) %>% arrange(desc(wfh_nn))
test_2018 <- us_wfh_ag_df %>% filter(year == 2018) %>% mutate(quartile = ntile(wfh_nn, 50)) %>% mutate(wfh_nn = round(100*wfh_nn, digits = 2)) %>% arrange(desc(wfh_nn))

stargazer(test_2021[1:15, c(1:2)], summary = F, title = "Top WFH Occupations (2021)", rownames = F)
stargazer(test_2018[1:15, c(1:2)], summary = F, title = "Top WFH Occupations (2018)", rownames = F)

#### END ####

#### MAKE TRAINING DATA FOR YABRA ####
# Combine all data
rm(list = ls())
source("/mnt/disks/pdisk/code/safe_mclapply.R")
paths <- list.files("./int_data/wfh_v2/", pattern = "wfh_v2_raw_sentences", full.names = T) %>% sort(decreasing = T)
wfh_v2_raw_sentences <- safe_mclapply(paths, readRDS, mc.cores = 28) %>% bind_rows

wfh_v2 <- wfh_v2_raw_sentences %>%
  select(job_id,sentence_id,sentence,at_home_position,done_at_home,worked_at_home,completed_from_home,virtual_role,completed_at_home,undertaken_from_home,working_from_anywhere,
         job_from_home,based_ast_at_home,hybrid_position,partially_remote,working_ast_at_home,hybrid_work,telecommuting,mobile_work,working_virtually,at_home_job,role_ast_from_home,
         virtual_work,work_remote,performed_remotely,worked_from_home,working_remote,teleworking,from_home_position,homeworking,done_from_home,office_ast_at_home,work_in_home,
         from_home_role,located_anywhere,work_ast_at_home,job_ast_from_home,work_ast_remotely,based_ast_from_home,working_ast_remotely,office_ast_from_home,remote_first,
         from_home_work,telework,location_remote,telecommute,work_from_anywhere,work_ast_remote,working_at_home,home_work,remote_position,based_at_home,from_home_job,stay_ast_from_home,
         x100_pc_remote,working_ast_remote,work_at_home,remote_based,working_ast_from_home,remote_work,based_from_home,work_ast_from_home,hybrid_working,location_ast_remote,homebased,
         fully_remote,working_remotely,wfh,work_off_site,home_working,working_from_home,remote_working,home_based,work_remotely,work_from_home,work_virtually,neg,disqual)

remove("wfh_v2_raw_sentences")
colnames(wfh_v2)

wfh_v2 <- wfh_v2 %>%
  mutate(across(at_home_position:disqual, ~replace(., is.na(.), 0))) %>%
  mutate(across(at_home_position:disqual, ~ifelse(.x>0, 1, 0)))

nrow(wfh_v2) # 39,555,564

wfh_v2$sum_wfh_hits <- rowSums(wfh_v2[,c(4:76)])

sum(wfh_v2$sum_wfh_hits == 0)
sum(wfh_v2$sum_wfh_hits == 1)
sum(wfh_v2$sum_wfh_hits > 1)
nrow(wfh_v2)

remove(list = setdiff(ls(), "wfh_v2"))

#### WFH not NEGATED
remove(list = setdiff(ls(), "wfh_v2"))
wfh_v2_wfh_nn <- wfh_v2 %>%
  filter(neg == 0) %>%
  filter(sum_wfh_hits>0 & disqual == 0) %>%
  distinct(sentence, .keep_all = T)
nrow(wfh_v2_wfh_nn) # 317,590

wfh_v2_wfh_nn <- wfh_v2_wfh_nn %>%
  group_by(at_home_position,done_at_home,worked_at_home,completed_from_home,virtual_role,completed_at_home,undertaken_from_home,working_from_anywhere,
           job_from_home,based_ast_at_home,hybrid_position,partially_remote,working_ast_at_home,hybrid_work,telecommuting,mobile_work,working_virtually,at_home_job,role_ast_from_home,
           virtual_work,work_remote,performed_remotely,worked_from_home,working_remote,teleworking,from_home_position,homeworking,done_from_home,office_ast_at_home,work_in_home,
           from_home_role,located_anywhere,work_ast_at_home,job_ast_from_home,work_ast_remotely,based_ast_from_home,working_ast_remotely,office_ast_from_home,remote_first,
           from_home_work,telework,location_remote,telecommute,work_from_anywhere,work_ast_remote,working_at_home,home_work,remote_position,based_at_home,from_home_job,stay_ast_from_home,
           x100_pc_remote,working_ast_remote,work_at_home,remote_based,working_ast_from_home,remote_work,based_from_home,work_ast_from_home,hybrid_working,location_ast_remote,homebased,
           fully_remote,working_remotely,wfh,work_off_site,home_working,working_from_home,remote_working,home_based,work_remotely,work_from_home,work_virtually) %>%
  sample_n(size = ceiling(n()^0.75), replace = F) %>%
  ungroup()
nrow(wfh_v2_wfh_nn) # 34,694

wfh_v2_wfh_nn$nchar <- nchar(wfh_v2_wfh_nn$sentence)

wfh_v2_wfh_nn <- wfh_v2_wfh_nn %>%
  mutate(wfh = ifelse(sum_wfh_hits>0,1,0)) %>%
  select(job_id,sentence_id,sentence, nchar, wfh, neg, disqual)

fwrite(wfh_v2_wfh_nn, file = "./int_data/bert_train/uk_wfh.csv")

#### WFH not NEGATED
remove(list = setdiff(ls(), "wfh_v2"))
wfh_v2_wfh_n <- wfh_v2 %>%
  filter(neg == 1) %>%
  filter(sum_wfh_hits>0 & disqual == 0) %>%
  distinct(sentence, .keep_all = T)
nrow(wfh_v2_wfh_nn) # 317,590

wfh_v2_wfh_n <- wfh_v2_wfh_n %>%
  group_by(at_home_position,done_at_home,worked_at_home,completed_from_home,virtual_role,completed_at_home,undertaken_from_home,working_from_anywhere,
           job_from_home,based_ast_at_home,hybrid_position,partially_remote,working_ast_at_home,hybrid_work,telecommuting,mobile_work,working_virtually,at_home_job,role_ast_from_home,
           virtual_work,work_remote,performed_remotely,worked_from_home,working_remote,teleworking,from_home_position,homeworking,done_from_home,office_ast_at_home,work_in_home,
           from_home_role,located_anywhere,work_ast_at_home,job_ast_from_home,work_ast_remotely,based_ast_from_home,working_ast_remotely,office_ast_from_home,remote_first,
           from_home_work,telework,location_remote,telecommute,work_from_anywhere,work_ast_remote,working_at_home,home_work,remote_position,based_at_home,from_home_job,stay_ast_from_home,
           x100_pc_remote,working_ast_remote,work_at_home,remote_based,working_ast_from_home,remote_work,based_from_home,work_ast_from_home,hybrid_working,location_ast_remote,homebased,
           fully_remote,working_remotely,wfh,work_off_site,home_working,working_from_home,remote_working,home_based,work_remotely,work_from_home,work_virtually) %>%
  sample_n(size = ceiling(n()^0.75), replace = F) %>%
  ungroup()
nrow(wfh_v2_wfh_n) # 34,694

wfh_v2_wfh_n$nchar <- nchar(wfh_v2_wfh_n$sentence)

wfh_v2_wfh_n <- wfh_v2_wfh_n %>%
  mutate(wfh = ifelse(sum_wfh_hits>0,1,0)) %>%
  select(job_id,sentence_id,sentence, nchar, wfh, neg, disqual)

fwrite(wfh_v2_wfh_n, file = "./int_data/bert_train/uk_wfh_negated.csv")

#### WFH DISQUAL
remove(list = setdiff(ls(), "wfh_v2"))

wfh_v2_wfh_d <- wfh_v2 %>%
  filter(disqual == 1 & sum_wfh_hits==0) %>%
  filter(nchar(sentence) > 196 & nchar(sentence) < 306) %>%
  mutate(nchar = nchar(sentence)) %>%
  mutate(wfh = 0, neg = 0) %>%
  select(job_id,sentence_id,sentence, nchar, wfh, neg, disqual)
nrow(wfh_v2_wfh_d) # 9,705,559

library("quanteda")
disqual_dict <- read_xlsx("/mnt/disks/pdisk/bg-us/aux_data/wfh_v2xlsx.xlsx", sheet = 3) %>% clean_names %>% select(key) %>% distinct(key) %>% rename(glob_en = key) %>% mutate(key = make_clean_names(glob_en))
disqual_dict <- setNames(as.list(disqual_dict$glob_en), disqual_dict$key)
disqual_dict <- dictionary(disqual_dict)

wfh_v2_wfh_d_dfm <- wfh_v2_wfh_d %>%
  corpus(., text_field = "sentence", docid_field = "sentence_id", unique_docnames = TRUE) %>%
  tokens(., verbose = T)

wfh_v2_wfh_d_dfm_disq <- quanteda::dfm(tokens_lookup(wfh_v2_wfh_d_dfm,  disqual_dict, valuetype = "glob", case_insensitive = T, verbose = TRUE))

wfh_v2_wfh_d_dfm_disq <- convert(wfh_v2_wfh_d_dfm_disq, to = "data.frame")

wfh_v2_wfh_d_dfm_disq$group_id <- wfh_v2_wfh_d_dfm_disq %>% group_indices(hotel,home_saftey,builder,home_build,home_construction,homebuild,off_site,remote_construction,child,children,disabilities,disables,nursing,nursing_home,remedy,rest_home,therapy,treatment,go_home,group_home,home_bot,home_daily,home_focused,home_phone,home_setting,home_settings,home_time,home_watch,home_weekly,homebot,homefocused,near_home,return_home,take_home,taking_home,time_at_home,home_and_away,home_grown,homegrown,from_work,home_depot,home_store,homer,remote_offices,accomodation,apartment,apartments,home_owner,homeowner,house,lodging,property,residence,trade_home,tradehome,home_care,home_stead,homecare,homeless,homestead,hospice,kitchen,peoples_home,refuge,retirement_home,shelter,youth,tele_com,tele_comm,telecom,telecomm,telemedicine,around_home,around_homes,garden,home_instructor,home_service,home_services,home_visit,maintenance,on_site,repair,home_school,home_study,home_training,home_builder,home_like,home_made,homebuilder,homelike,homemade,homephone,work_with,care,assessments,training,supervisory,health,provider,services)

wfh_v2_wfh_d_dfm_disq <- wfh_v2_wfh_d_dfm_disq %>%
  rename(sentence_id = doc_id) %>% select(sentence_id, group_id)

wfh_v2_wfh_d <- wfh_v2_wfh_d %>%
  left_join(wfh_v2_wfh_d_dfm_disq)

remove(list = setdiff(ls(), c("wfh_v2_wfh_d", "wfh_v2")))

wfh_v2_wfh_d_ss <- wfh_v2_wfh_d %>%
  distinct(sentence, .keep_all = T) %>%
  group_by(group_id) %>%
  sample_n(size = pmin(n(),250), replace = F) %>%
  ungroup()
nrow(wfh_v2_wfh_d_ss) # 89,536

wfh_v2_wfh_d_ss$nchar <- nchar(wfh_v2_wfh_d_ss$sentence)

wfh_v2_wfh_d_ss <- wfh_v2_wfh_d_ss %>%
  select(job_id,sentence_id,sentence, nchar, wfh, neg, disqual)

View(wfh_v2_wfh_d_ss)

fwrite(wfh_v2_wfh_d_ss, file = "./int_data/bert_train/uk_disqual.csv")

#### END ####

#### RANDOM NON
set.seed(321)
remove(list = setdiff(ls(), "wfh_v2"))

source("/mnt/disks/pdisk/code/safe_mclapply.R")
paths <- list.files("./int_data/sentences/", pattern = ".rds", full.names = T) %>% sort(decreasing = T) %>% sample(., 56)
paths
raw_sentences <- safe_mclapply(paths, readRDS, mc.cores = 28) %>% bind_rows

head(wfh_v2)
head(raw_sentences)
raw_sentences <- raw_sentences %>%
  left_join(wfh_v2 %>% select(sentence_id, sum_wfh_hits, neg, disqual))

head(raw_sentences)

raw_sentences <- raw_sentences %>%
  mutate(across(sum_wfh_hits:disqual, ~replace(., is.na(.), 0))) %>%
  mutate(across(sum_wfh_hits:disqual, ~ifelse(.x>0, 1, 0)))

raw_sentences <- raw_sentences %>%
  filter(sum_wfh_hits == 0 & neg == 0 & disqual == 0)

summary(nchar(raw_sentences$sentence))

raw_sentences <- raw_sentences %>%
  filter(nchar(sentence) > 104 & nchar(sentence) < 170)

raw_sentences <- raw_sentences %>% distinct(sentence, .keep_all = T)

raw_sentences <- raw_sentences %>% sample_frac(., size = 0.2)

fwrite(raw_sentences, file = "./int_data/bert_train/uk_random_non_wfh_non_disqual.csv")

#### END ####
