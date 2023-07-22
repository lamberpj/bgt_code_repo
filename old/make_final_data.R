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
detectCores()
quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg_combined/")
df_weights <- readRDS(file = "./aux_data/dictionary_weights.rds") %>% select(neg, name, accuracy)

#### CANADA #####
paths <- list.files("/mnt/disks/pdisk/bg-can/int_data/wfh_v6", full.names = T, pattern = "wfh_v6_raw_sequences")
paths
source("/mnt/disks/pdisk/code/safe_mclapply.R")
df_dict_can <- safe_mclapply(1:length(paths), function(i) {
  warning(paste0("\nSTART: ",i,"\n"))
  warning(paste0(paths[i]))
  df <- readRDS(paths[i]) %>%
    clean_names %>%
    setDT(.) %>%
    .[, country := "can"]
  df <- df %>%
    .[, neg := pmax(cant, cant_2, dont, dont_2, isnt, isnt_2, no, not, unable, wont, wont_2)] %>%
    select(country, job_id, seq_id, neg, x100_percent_remote:working_virtually) %>%
    pivot_longer(x100_percent_remote:working_virtually) %>%
    setDT(.) %>%
    .[, value := as.numeric(value > 0)] %>%
    .[, neg := as.numeric(neg > 0)] %>%
    .[value == 1] %>%
    .[, oecd_dict := ifelse(
      name %in% c("x100_percent_remote","fully_remote","home_based","home_office","location_ast_remote","location_remote","partially_remote",
                  "partly_remote","percent_remote","remote_assignment","remote_based","remote_cl_yes","remote_first","remote_initially",
                  "remote_option","remote_position","remote_work","remote_workable","remote_working","remote_yes","remotote_work_teleworking",
                  "telecommute","telecommuting","telework","teleworking","work_ast_remote","work_ast_remotely","work_at_home","work_at_home_cl_yes",
                  "work_at_home_yes","work_from_home","work_from_home_cl_yes","work_from_home_yes","work_remote","work_remotely","workable_remote",
                  "working_ast_remote","working_from_home","working_remote","working_remotely"), 1, 0)] %>%
    left_join(df_weights) %>%
    setDT(.) %>%
    rename(dict = value,
           dict_weight = accuracy) %>%
    .[, .(neg = max(neg, na.rm = T), dict = max(dict, na.rm = T), oecd_dict = max(oecd_dict, na.rm = T), dict_weight = max(dict_weight, na.rm = T)), by = .(country, job_id, seq_id)] %>%
    .[, job_id := as.integer(job_id)]
}, mc.cores = 30)

df_dict_can <- rbindlist(df_dict_can) %>% distinct(seq_id, .keep_all = T) %>% setDT(.)
#nrow(df_dict_can[dict_weight == -Inf]) / nrow(df_dict_can)
df_bert_pred_can <- fread("./int_data/CAN_predictions_complete.csv")
df_bert_pred_can <- df_bert_pred_can %>% distinct(seq_id, .keep_all = T) %>% setDT(.)
df_bert_pred_can <- df_bert_pred_can %>%
  .[, job_id := as.numeric(str_sub(seq_id, 1, -6))] %>%
  select(job_id, seq_id, wfh_prob)
nrow(df_bert_pred_can) # 24,766,262
df_all_can <- df_bert_pred_can %>%
  merge(., df_dict_can, by = c("job_id", "seq_id"), all.x = TRUE, all.y = FALSE)
colnames(df_all_can)
df_all_can$country <- "can"
View(head(df_all_can, 1000))
df_all_can[, c("neg", "dict", "oecd_dict", "dict_weight")][is.na(df_all_can[, c("neg", "dict", "oecd_dict", "dict_weight")])] <- 0
# Load all sequences to merge in
paths <- list.files("/mnt/disks/pdisk/bg-can/int_data/sequences/", full.names = T, pattern = ".rds")
source("/mnt/disks/pdisk/code/safe_mclapply.R")
df_seq_can <- safe_mclapply(1:length(paths), function(i) {
  warning(paste0("\nSTART: ",i,"\n"))
  warning(paste0(paths[i]))
  df <- readRDS(paths[i]) %>% select(-job_id) %>% distinct(seq_id, .keep_all = T)
}, mc.cores = 16)

df_seq_can <- rbindlist(df_seq_can) %>% setDT(.)

nrow(df_all_can) # 26,156,386
df_all_can <- df_all_can %>%
  merge(., df_seq_can, by = "seq_id")
nrow(df_all_can)

View(head(df_all_can, 10000))

#fwrite(df_all_can, file = "./int_data/can_seq_level_wfh_measures.csv")
df_all_can <- fread(file = "./int_data/can_seq_level_wfh_measures.csv")

# Make ad level
df_all_can <- df_all_can %>%
  .[, .(wfh_prob = max(wfh_prob, na.rm = T), neg = max(neg, na.rm = T), dict = max(dict, na.rm = T), oecd_dict = max(oecd_dict, na.rm = T), dict_weight = max(dict_weight, na.rm = T)), by = .(country, job_id)]
# Load job ads
paths <- list.files("/mnt/disks/pdisk/bg-can/raw_data/main", pattern = ".txt", full.names = T)
paths
source("/mnt/disks/pdisk/code/safe_mclapply.R")
df_stru <- safe_mclapply(1:length(paths), function(i) {
  warning(paste0("\nSTART: ",i,"\n"))
  warning(paste0(paths[i]))
  df <- fread(paths[i], nThread = 2, colClasses = "character", stringsAsFactors = FALSE) %>%
    clean_names %>%
    setDT(.) %>%
    .[job_id %in% df_all_can$job_id]
  
  df[df == "na"] <- NA
  df[df == "-999"] <- NA
  df[df == ""] <- NA
  
  df <- df %>%
    .[, job_ymd := ymd(job_date)] %>%
    .[, year_quarter := as.yearqtr(job_ymd)]
  
  warning(paste0("\nDONE: ",i))
  return(df)
}, mc.cores = 16)

df_stru <- rbindlist(df_stru)
df_stru <- setDT(df_stru)

df_stru <- df_stru %>% distinct(job_id, .keep_all = T)
df_stru$job_id <- as.numeric(df_stru$job_id)
df_all_can$job_id <- as.numeric(df_all_can$job_id)
nrow(df_all_can) # 3,503,348
df_all_can <- df_all_can %>%
  merge(., df_stru, by = "job_id", all.x = FALSE, all.y = FALSE)
nrow(df_all_can) # 3,503,348

head(df_all_can)

df_all_can$dict_weight[df_all_can$dict_weight<0] <- 0
df_all_can$year_month <- as.yearmon(df_all_can$job_ymd)

max(df_all_can$year_month)

View(head(df_all_can, 1000))

save(df_all_can, file = "./int_data/can_stru_wfh.RData")

#### END ####

#### USA #####
paths <- list.files("/mnt/disks/pdisk/bg-us/int_data/wfh_v6", full.names = T, pattern = "wfh_v6_raw_sequences")
source("/mnt/disks/pdisk/code/safe_mclapply.R")

df_dict_us <- safe_mclapply(1:length(paths), function(i) {
  warning(paste0("\nSTART: ",i,"\n"))
  warning(paste0(paths[i]))
  df <- readRDS(paths[i]) %>%
    clean_names %>%
    setDT(.) %>%
    .[, country := "us"]
  df <- df %>%
    .[, neg := pmax(cant, cant_2, dont, dont_2, isnt, isnt_2, no, not, unable, wont, wont_2)] %>%
    select(country, job_id, seq_id, neg, x100_percent_remote:working_virtually) %>%
    pivot_longer(x100_percent_remote:working_virtually) %>%
    setDT(.) %>%
    .[, value := as.numeric(value > 0)] %>%
    .[, neg := as.numeric(neg > 0)] %>%
    .[value == 1] %>%
    .[, oecd_dict := ifelse(
      name %in% c("x100_percent_remote","fully_remote","home_based","home_office","location_ast_remote","location_remote","partially_remote",
                  "partly_remote","percent_remote","remote_assignment","remote_based","remote_cl_yes","remote_first","remote_initially",
                  "remote_option","remote_position","remote_work","remote_workable","remote_working","remote_yes","remotote_work_teleworking",
                  "telecommute","telecommuting","telework","teleworking","work_ast_remote","work_ast_remotely","work_at_home","work_at_home_cl_yes",
                  "work_at_home_yes","work_from_home","work_from_home_cl_yes","work_from_home_yes","work_remote","work_remotely","workable_remote",
                  "working_ast_remote","working_from_home","working_remote","working_remotely"), 1, 0)] %>%
    left_join(df_weights) %>%
    setDT(.) %>%
    rename(dict = value,
           dict_weight = accuracy) %>%
    .[, dict_weight := ifelse(is.na(dict_weight), 0, dict_weight)] %>%
    .[, .(neg = max(neg, na.rm = T), dict = max(dict, na.rm = T),
          oecd_dict = max(oecd_dict, na.rm = T),
          dict_weight = max(dict_weight, na.rm = T)), by = .(country, job_id, seq_id)]
}, mc.cores = 4)

df_dict_us <- rbindlist(df_dict_us) %>% distinct(seq_id, .keep_all = T) %>% setDT(.)
df_dict_us$job_id <- as.numeric(df_dict_us$job_id)

#nrow(df_dict_us[dict_weight == -Inf]) / nrow(df_dict_us)
df_bert_pred_us <- fread("./int_data/US_predictions_complete.csv", nThread = 4)

df_bert_pred_us <- df_bert_pred_us %>% distinct(seq_id, .keep_all = T) %>% setDT(.)
df_bert_pred_us <- df_bert_pred_us %>%
  .[, job_id := as.numeric(str_sub(seq_id, 1, -6))] %>%
  select(job_id, seq_id, wfh_prob)
nrow(df_bert_pred_us) # 91,842,990
df_all_us <- df_bert_pred_us %>%
  merge(., df_dict_us, by = c("job_id", "seq_id"), all.x = TRUE, all.y = FALSE)
nrow(df_bert_pred_us) # 91,842,990

colnames(df_all_us)
df_all_us$country <- "us"
df_all_us[, c("neg", "dict", "oecd_dict", "dict_weight")][is.na(df_all_us[, c("neg", "dict", "oecd_dict", "dict_weight")])] <- 0

# Load all sequences to merge in
paths <- list.files("/mnt/disks/pdisk/bg-us/int_data/sequences/", full.names = T, pattern = ".rds")

source("/mnt/disks/pdisk/code/safe_mclapply.R")
df_seq_us <- safe_mclapply(1:length(paths), function(i) {
  warning(paste0("\nSTART: ",i,"\n"))
  warning(paste0(paths[i]))
  df <- readRDS(paths[i]) %>% select(-job_id) %>% distinct(seq_id, .keep_all = T)
}, mc.cores = 8)

df_seq_us <- rbindlist(df_seq_us) %>% setDT(.)

nrow(df_all_us) # 91,842,990
df_all_us <- df_all_us %>%
  merge(., df_seq_us, by = "seq_id")

df_all_us <- df_all_us %>%
  distinct(seq_id, .keep_all = T)
nrow(df_all_us) # 91,839,891

fwrite(df_all_us, file = "./int_data/us_seq_level_wfh_measures.csv")
remove(list = setdiff(ls(),"df_all_us"))
colnames(df_all_us)
remove(list = ls())
library(vroom)
df_all_us <- vroom(num_threads = 32, file = "./int_data/us_seq_level_wfh_measures.csv")
remove(list = setdiff(ls(),"df_all_us"))
df_all_us <- df_all_us %>% select(-sequence)
df_all_us <- df_all_us %>% setDT(.)

# Make ad level
df_all_us <- df_all_us %>%
  .[, .(wfh_prob = max(wfh_prob, na.rm = T), neg = max(neg, na.rm = T), dict = max(dict, na.rm = T), oecd_dict = max(oecd_dict, na.rm = T), dict_weight = max(dict_weight, na.rm = T)), by = .(country, job_id)]
# Load job ads

remove(list = setdiff(ls(), "df_all_us"))

paths <- list.files("/mnt/disks/pdisk/bg-us/raw_data/main", pattern = ".txt", full.names = T)
source("/mnt/disks/pdisk/code/safe_mclapply.R")

df_stru <- safe_mclapply(1:length(paths), function(i) {
  warning(paste0("\nSTART: ",i,"\n"))
  warning(paste0(paths[i]))
  
  df <- fread(paths[i], nThread = 2, colClasses = "character", stringsAsFactors = FALSE,
              select = c("BGTJobId", "JobDate","CleanTitle","CanonTitle","SOC", "ONET", "BGTOcc", "BGTOccName", "Employer", "Sector", "SectorName", "NAICS3", "NAICS4", "NAICS5", "NAICS6", "City", "State", "County", "FIPSState",
                         "FIPSCounty", "FIPS", "BestFitMSA", "MSA", "MSAName", "Edu", "MaxEdu", "Degree", "MaxDegree", "MinSalary", "MaxSalary", "MinHrlySalary", "MaxHrlySalary", "SalaryType","JobHours", "TaxTerm", "Internship", "Exp", "MaxExp")) %>%
    clean_names %>%
    setDT(.) %>%
    rename(job_id = bgt_job_id) %>%
    .[job_id %in% df_all_us$job_id]
  
  df[df == "na"] <- NA
  df[df == "-999"] <- NA
  df[df == ""] <- NA
  
  df <- df %>%
    .[, job_ymd := ymd(job_date)] %>%
    .[, year_quarter := as.yearqtr(job_ymd)]
  
  warning(paste0("\nDONE: ",i))
  return(df)
}, mc.cores = 16)

df_stru <- rbindlist(df_stru)
df_stru <- setDT(df_stru)

df_stru <- df_stru %>% distinct(job_id, .keep_all = T)
df_stru$job_id <- as.numeric(df_stru$job_id)
df_all_us$job_id <- as.numeric(df_all_us$job_id)
nrow(df_all_us) # 11,608,045
df_all_us <- df_all_us %>%
  merge(., df_stru, by = "job_id", all.x = FALSE, all.y = FALSE)
nrow(df_all_us) # 11,608,027

df_all_us$dict_weight[df_all_us$dict_weight<0] <- 0
df_all_us$year_month <- as.yearmon(df_all_us$job_ymd)

save(df_all_us, file = "./int_data/us_stru_wfh.RData")

#### END ####

#### UK #####
paths <- list.files("/mnt/disks/pdisk/bg-uk/int_data/wfh_v6", full.names = T, pattern = "wfh_v6_raw_sequences")
source("/mnt/disks/pdisk/code/safe_mclapply.R")
df_dict_uk <- safe_mclapply(1:length(paths), function(i) {
  warning(paste0("\nSTART: ",i,"\n"))
  warning(paste0(paths[i]))
  df <- readRDS(paths[i]) %>%
    clean_names %>%
    setDT(.) %>%
    .[, country := "uk"]
  df <- df %>%
    .[, neg := pmax(cant, cant_2, dont, dont_2, isnt, isnt_2, no, not, unable, wont, wont_2)] %>%
    select(country, job_id, seq_id, neg, x100_percent_remote:working_virtually) %>%
    pivot_longer(x100_percent_remote:working_virtually) %>%
    setDT(.) %>%
    .[, value := as.numeric(value > 0)] %>%
    .[, neg := as.numeric(neg > 0)] %>%
    .[value == 1] %>%
    .[, oecd_dict := ifelse(
      name %in% c("x100_percent_remote","fully_remote","home_based","home_office","location_ast_remote","location_remote","partially_remote",
                  "partly_remote","percent_remote","remote_assignment","remote_based","remote_cl_yes","remote_first","remote_initially",
                  "remote_option","remote_position","remote_work","remote_workable","remote_working","remote_yes","remotote_work_teleworking",
                  "telecommute","telecommuting","telework","teleworking","work_ast_remote","work_ast_remotely","work_at_home","work_at_home_cl_yes",
                  "work_at_home_yes","work_from_home","work_from_home_cl_yes","work_from_home_yes","work_remote","work_remotely","workable_remote",
                  "working_ast_remote","working_from_home","working_remote","working_remotely"), 1, 0)] %>%
    left_join(df_weights) %>%
    setDT(.) %>%
    rename(dict = value,
           dict_weight = accuracy) %>%
    .[, .(neg = max(neg, na.rm = T), dict = max(dict, na.rm = T), oecd_dict = max(oecd_dict, na.rm = T), dict_weight = max(dict_weight, na.rm = T)), by = .(country, job_id, seq_id)] %>%
    .[, job_id := as.integer(job_id)]
}, mc.cores = 32)

df_dict_uk <- rbindlist(df_dict_uk) %>% distinct(seq_id, .keep_all = T) %>% setDT(.)
#nrow(df_dict_uk[dict_weight == -Inf]) / nrow(df_dict_uk)
df_bert_pred_uk <- fread("./int_data/UK_predictions_complete.csv")
df_bert_pred_uk <- df_bert_pred_uk %>% distinct(seq_id, .keep_all = T) %>% setDT(.)
df_bert_pred_uk <- df_bert_pred_uk %>%
  .[, job_id := as.numeric(str_sub(seq_id, 1, -6))] %>%
  select(job_id, seq_id, wfh_prob)
nrow(df_bert_pred_uk) # 24,766,262
df_all_uk <- df_bert_pred_uk %>%
  merge(., df_dict_uk, by = c("job_id", "seq_id"), all.x = TRUE, all.y = FALSE)
nrow(df_bert_pred_uk) # 24,766,262
colnames(df_all_uk)
df_all_uk$country <- "uk"
df_all_uk[, c("neg", "dict", "oecd_dict", "dict_weight")][is.na(df_all_uk[, c("neg", "dict", "oecd_dict", "dict_weight")])] <- 0

# Load all sequences to merge in
paths <- list.files("/mnt/disks/pdisk/bg-uk/int_data/sequences/", full.names = T, pattern = ".rds")
source("/mnt/disks/pdisk/code/safe_mclapply.R")
df_seq_uk <- safe_mclapply(1:length(paths), function(i) {
  warning(paste0("\nSTART: ",i,"\n"))
  warning(paste0(paths[i]))
  df <- readRDS(paths[i]) %>% select(-job_id) %>% distinct(seq_id, .keep_all = T)
}, mc.cores = 16)

df_seq_uk <- rbindlist(df_seq_uk) %>% setDT(.)

nrow(df_all_uk)
df_all_uk <- df_all_uk %>%
  merge(., df_seq_uk, by = "seq_id")
nrow(df_all_uk)

View(head(df_all_uk, 10000))

fwrite(df_all_uk, file = "./int_data/uk_seq_level_wfh_measures.csv")

library(vroom)
df_all_uk <- vroom(num_threads = 32, file = "./int_data/uk_seq_level_wfh_measures.csv")
remove(list = setdiff(ls(),"df_all_uk"))
df_all_uk <- df_all_uk %>% setDT(.)

remove(list = setdiff(ls(),"df_all_uk"))

# Make ad level
df_all_uk <- df_all_uk %>%
  .[, .(wfh_prob = max(wfh_prob, na.rm = T), neg = max(neg, na.rm = T), dict = max(dict, na.rm = T), oecd_dict = max(oecd_dict, na.rm = T), dict_weight = max(dict_weight, na.rm = T)), by = .(country, job_id)]
# Load job ads

remove(list = setdiff(ls(), "df_all_uk"))

paths <- list.files("/mnt/disks/pdisk/bg-uk/raw_data/main", pattern = ".txt", full.names = T)
source("/mnt/disks/pdisk/code/safe_mclapply.R")

colnames(fread(paths[1], nrow = 100))

df_stru <- safe_mclapply(1:length(paths), function(i) {
  warning(paste0("\nSTART: ",i,"\n"))
  warning(paste0(paths[i]))
  
  df <- fread(paths[i], nThread = 2, colClasses = "character", stringsAsFactors = FALSE,
              select = c("JobID","JobDate", "CleanJobTitle", "Language","CanonCountry","Nation","Region","TTWA","CanonCounty","CanonCity","Latitude","Longitude","CanonEmployer",
                         "InternshipFlag","MaxDegreeLevel","CanonMinimumDegree","MinDegreeLevel","CanonJobHours","CanonJobType","MaxExperience","MinExperience",
                         "MaxAnnualSalary","MinAnnualSalary","MaxHourlySalary","MinHourlySalary","WorkFromHome","UKSOCCode","UKSOCUnitGroup","UKSOCMinorGroup",
                         "UKSOCSubMajorGroup","UKSOCMajorGroup","BGTOcc","BGTOccName","BGTOccGroupName","BGTCareerAreaName","SICCode","SICClass","SICGroup",
                         "SICDivision","SICSection","StockTicker","LocalAuthorityDistrict","LocalEnterprisePartnership","PreferredNQFLevels","RequiredNQFLevels")) %>%
    clean_names %>%
    setDT(.) %>%
    .[job_id %in% df_all_uk$job_id]
  
  df[df == "na"] <- NA
  df[df == "-999"] <- NA
  df[df == ""] <- NA
  
  df <- df %>%
    .[, job_ymd := ymd(job_date)] %>%
    .[, year_quarter := as.yearqtr(job_ymd)]
  
  warning(paste0("\nDONE: ",i))
  return(df)
}, mc.cores = 8)

df_stru <- rbindlist(df_stru)
df_stru <- setDT(df_stru)

df_stru <- df_stru %>% distinct(job_id, .keep_all = T)
df_stru$job_id <- as.numeric(df_stru$job_id)
df_all_uk$job_id <- as.numeric(df_all_uk$job_id)
nrow(df_all_uk) # 10,814,924
df_all_uk <- df_all_uk %>%
  merge(., df_stru, by = "job_id", all.x = FALSE, all.y = FALSE)
nrow(df_all_uk) # 10,814,906

df_all_uk$dict_weight[df_all_uk$dict_weight<0] <- 0
df_all_uk$year_month <- as.yearmon(df_all_uk$job_ymd)

View(head(df_all_uk, 1000))

save(df_all_uk, file = "./int_data/uk_stru_wfh.RData")

#### END ####

#### ANZ #####
paths <- list.files("/mnt/disks/pdisk/bg-anz/int_data/wfh_v6", full.names = T, pattern = "wfh_v6_raw_sequences")
source("/mnt/disks/pdisk/code/safe_mclapply.R")
df_dict_anz <- safe_mclapply(1:length(paths), function(i) {
  warning(paste0("\nSTART: ",i,"\n"))
  warning(paste0(paths[i]))
  df <- readRDS(paths[i]) %>%
    clean_names %>%
    setDT(.) %>%
    .[, country := "anz"]
  df <- df %>%
    .[, neg := pmax(cant, cant_2, dont, dont_2, isnt, isnt_2, no, not, unable, wont, wont_2)] %>%
    select(country, job_id, seq_id, neg, x100_percent_remote:working_virtually) %>%
    pivot_longer(x100_percent_remote:working_virtually) %>%
    setDT(.) %>%
    .[, value := as.numeric(value > 0)] %>%
    .[, neg := as.numeric(neg > 0)] %>%
    .[value == 1] %>%
    .[, oecd_dict := ifelse(
      name %in% c("x100_percent_remote","fully_remote","home_based","home_office","location_ast_remote","location_remote","partially_remote",
                  "partly_remote","percent_remote","remote_assignment","remote_based","remote_cl_yes","remote_first","remote_initially",
                  "remote_option","remote_position","remote_work","remote_workable","remote_working","remote_yes","remotote_work_teleworking",
                  "telecommute","telecommuting","telework","teleworking","work_ast_remote","work_ast_remotely","work_at_home","work_at_home_cl_yes",
                  "work_at_home_yes","work_from_home","work_from_home_cl_yes","work_from_home_yes","work_remote","work_remotely","workable_remote",
                  "working_ast_remote","working_from_home","working_remote","working_remotely"), 1, 0)] %>%
    left_join(df_weights) %>%
    setDT(.) %>%
    rename(dict = value,
           dict_weight = accuracy) %>%
    .[, .(neg = max(neg, na.rm = T), dict = max(dict, na.rm = T), oecd_dict = max(oecd_dict, na.rm = T), dict_weight = max(dict_weight, na.rm = T)), by = .(country, job_id, seq_id)] %>%
    .[, job_id := as.integer(job_id)]
}, mc.cores = 32)

df_dict_anz <- rbindlist(df_dict_anz) %>% distinct(seq_id, .keep_all = T) %>% setDT(.)
#nrow(df_dict_anz[dict_weight == -Inf]) / nrow(df_dict_anz)
df_bert_pred_anz <- fread("./int_data/ANZ_predictions_complete.csv")
df_bert_pred_anz <- df_bert_pred_anz %>% distinct(seq_id, .keep_all = T) %>% setDT(.)
df_bert_pred_anz <- df_bert_pred_anz %>%
  .[, job_id := as.numeric(str_sub(seq_id, 1, -6))] %>%
  select(job_id, seq_id, wfh_prob)
nrow(df_bert_pred_anz) # 24,766,262
df_all_anz <- df_bert_pred_anz %>%
  merge(., df_dict_anz, by = c("job_id", "seq_id"), all.x = TRUE, all.y = FALSE)
nrow(df_bert_pred_anz) # 24,766,262
colnames(df_all_anz)
df_all_anz$country <- "anz"
df_all_anz[, c("neg", "dict", "oecd_dict", "dict_weight")][is.na(df_all_anz[, c("neg", "dict", "oecd_dict", "dict_weight")])] <- 0

# Load all sequences to merge in
paths <- list.files("/mnt/disks/pdisk/bg-anz/int_data/sequences/", full.names = T, pattern = ".rds")
source("/mnt/disks/pdisk/code/safe_mclapply.R")
df_seq_anz <- safe_mclapply(1:length(paths), function(i) {
  warning(paste0("\nSTART: ",i,"\n"))
  warning(paste0(paths[i]))
  df <- readRDS(paths[i]) %>% select(-job_id) %>% distinct(seq_id, .keep_all = T)
}, mc.cores = 32)

df_seq_anz <- rbindlist(df_seq_anz) %>% setDT(.)

nrow(df_all_anz)
df_all_anz <- df_all_anz %>%
  merge(., df_seq_anz, by = "seq_id")
nrow(df_all_anz)

View(head(df_all_anz, 10000))

fwrite(df_all_anz, file = "./int_data/anz_seq_level_wfh_measures.csv")

remove(list = setdiff(ls(),"df_all_anz"))

# Make ad level
df_all_anz <- df_all_anz %>%
  .[, .(wfh_prob = max(wfh_prob, na.rm = T), neg = max(neg, na.rm = T), dict = max(dict, na.rm = T), oecd_dict = max(oecd_dict, na.rm = T), dict_weight = max(dict_weight, na.rm = T)), by = .(country, job_id)]
# Load job ads

remove(list = setdiff(ls(), "df_all_anz"))

paths <- list.files("/mnt/disks/pdisk/bg-anz/raw_data/main", pattern = ".txt", full.names = T)
source("/mnt/disks/pdisk/code/safe_mclapply.R")

colnames(fread(paths[1], nrow = 100))

df_stru <- safe_mclapply(1:length(paths), function(i) {
  warning(paste0("\nSTART: ",i,"\n"))
  warning(paste0(paths[i]))
  
  df <- fread(paths[i], nThread = 1, colClasses = "character", stringsAsFactors = FALSE) %>%
    clean_names %>%
    setDT(.) %>%
    .[job_id %in% df_all_anz$job_id]
  
  df[df == "na"] <- NA
  df[df == "-999"] <- NA
  df[df == ""] <- NA
  
  df <- df %>%
    .[, job_ymd := ymd(job_date)] %>%
    .[, year_quarter := as.yearqtr(job_ymd)]
  
  warning(paste0("\nDONE: ",i))
  return(df)
}, mc.cores = 16)

df_stru <- rbindlist(df_stru)
df_stru <- setDT(df_stru)

df_stru <- df_stru %>% distinct(job_id, .keep_all = T)
df_stru$job_id <- as.numeric(df_stru$job_id)
df_all_anz$job_id <- as.numeric(df_all_anz$job_id)
nrow(df_all_anz) # 10,814,924
df_all_anz <- df_all_anz %>%
  merge(., df_stru, by = "job_id", all.x = FALSE, all.y = FALSE)
nrow(df_all_anz) # 10,814,906

df_all_anz$dict_weight[df_all_anz$dict_weight<0] <- 0
df_all_anz$year_month <- as.yearmon(df_all_anz$job_ymd)

View(head(df_all_anz, 1000))

save(df_all_anz, file = "./int_data/anz_stru_wfh.RData")


#### END ####

##### ZIP ####
remove(list = ls())
paths <- list.files("./int_data", "seq_level_wfh_measures", full.names = T)
file_name <- list.files("./int_data", "seq_level_wfh_measures", full.names = F) %>% gsub(".csv", ".zip", .)

i = 1
paste0('zip -r ./int_data/', file_name[i],' ',paths[i])

system(paste0("zip -r ./int_data/us_stru_wfh.zip ./int_data/us_stru_wfh.RData"))

lapply(1:length(paths), function(i) {
  system(paste0('zip -r ./int_data/', file_name[i],' ',paths[i]))
})

system(paste0('zip -r ./int_data/bgt_structured.zip ./int_data/bgt_structured/'))


#### END ####