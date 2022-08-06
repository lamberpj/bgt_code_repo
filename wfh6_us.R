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
setwd("/mnt/disks/pdisk/bg-us/")
#setDTthreads(1)

remove(list = ls())
#### end ####

#### PREPARE DATA ####

#system("gsutil -m cp -r gs://for_transfer/bgt-us/Main/ /mnt/disks/pdisk/bg-us/raw_data/main/")
system("gsutil -m cp -r gs://for_transfer/bgt-us/XML /mnt/disks/pdisk/bg-us/raw_data/text/")
#
#library(filesstrings)
#paths <- list.files("/mnt/disks/pdisk/bg-us/raw_data/main/", full.names = T, pattern = ".zip", recursive = T)
#paths
#lapply(paths, function(x) {
#  file.move(x, "/mnt/disks/pdisk/bg-anz/raw_data/text")
#})

#lapply(1:length(paths), function(i) {
#  system(paste0("unzip -n ",paths[i]," -d ./raw_data/main"))
#})

#lapply(1:length(paths), function(i) {
#  unlink(paths[i])
#})


#### SUBSET DATA ####
# LOAD EXAMPLE
paths <- list.files("./raw_data/main", pattern = ".txt", full.names = T)
source("/mnt/disks/pdisk/code/safe_mclapply.R")
df_stru <- safe_mclapply(1:length(paths), function(i) {
  
  warning(paste0("\nSTART: ",i,"\n"))
  warning(paste0(paths[i]))
  df <- fread(paths[i], nThread = 1, colClasses = "character", stringsAsFactors = FALSE,
              select = c("BGTJobId",  "JobDate", "BGTOcc", "MSA")) %>%
    clean_names %>%
    setDT(.)
  
  df[df == "na"] <- NA
  df[df == "-999"] <- NA
  df[df == ""] <- NA
  
  df <- df %>%
    .[!is.na(bgt_occ) & !is.na(msa)] %>%
    .[, job_ymd := ymd(job_date)] %>%
    .[, year_quarter := as.yearqtr(job_ymd)]
  
  warning(paste0("\nDONE: ",i))
  return(df)
}, mc.cores = 48)

df_stru <- rbindlist(df_stru)
df_stru <- setDT(df_stru)

max(df_stru$year_quarter)

nrow(df_stru) # 237,240,928

uniqueN(df_stru$year_quarter)
k <- 353859

set.seed(123)

df_stru_ss <- df_stru %>%
  group_by(year_quarter) %>%
  sample_n(., size = k) %>%
  setDT(.)

nrow(df_stru_ss) # 10,969,629
nrow(df_stru_ss)/nrow(df_stru) # 0.05

bgt_job_id_keep_ss_vector <- df_stru_ss %>% select(bgt_job_id)
save(bgt_job_id_keep_ss_vector, file = "./aux_data/bgt_job_id_keep_ss_v6_vector.RData")
ls()

#### END ####

#### IMPORT RAW TEXT ####
remove(list = ls())
load(file = "./aux_data/bgt_job_id_keep_ss_v6_vector.RData")
ls()
#### /END ####

#### GET PATH NAMES TO MAKE SEQUENCES ####
paths <- list.files("./raw_data/text/", pattern = "*.zip", full.names = T)
paths_done <- list.files("./int_data/sequences/", pattern = "*.rds", full.names = F) %>%
  gsub("sequences_", "", .) %>% gsub(".rds", "", .) %>% unique
paths_check <- list.files("./raw_data/text/", pattern = "*.zip", full.names = T) %>%
  str_sub(., -21, -5)
paths_check[!(paths_check %in% paths_done)]
paths <- paths[!(paths_check %in% paths_done)]
remove(list = c("paths_check", "paths_done"))
set.seed(123)

paths
#### /END ####

#### READ XML NAD MAKE SEQUENCES ####
paths
source("/mnt/disks/pdisk/code/safe_mclapply.R")

safe_mclapply(1:length(paths), function(i) {
  
  name <- str_sub(paths[i], -21, -5)
  name
  warning(paste0("\nBEGIN: ",i,"  '",name,"'"))
  cat(paste0("\nBEGIN: ",i,"  '",name,"'"))
  system(paste0("unzip -n ",paths[i]," -d ./raw_data/text/"))
  xml_path = gsub(".zip", ".xml", paths[i])
  xml_path
  try( {
    df_xml <- read_xml(xml_path) %>%
      xml_find_all(., ".//Job")
    
    df_job_id <- xml_find_all(df_xml, ".//JobID") %>% xml_text
    df_job_title <- xml_find_all(df_xml, ".//CleanJobTitle") %>% xml_text
    df_job_text <- xml_find_all(df_xml, ".//JobText") %>% xml_text
    remove("df_xml")
    
    df_title <- data.table(job_id = df_job_id, seq_id = paste0(df_job_id,"_0000"), sequence = df_job_title) %>%
      .[job_id %in% bgt_job_id_keep_ss_vector$bgt_job_id] %>%
      .[, nchar := nchar(sequence)] %>%
      .[, nfeat := str_count(sequence, '\\w+')]
    
    df_ads <- data.table(job_id = df_job_id, job_text = df_job_text) %>%
      .[job_id %in% bgt_job_id_keep_ss_vector$bgt_job_id] %>%
      .[, nchar := nchar(job_text)] %>%
      .[, nfeat := str_count(job_text, '\\w+')]
    
    remove(list = c("df_job_id","df_job_title","df_job_text"))
    
    df_ads <- setDT(df_ads)
    
    # Sequence Tokeniser
    df_chunked <- copy(df_ads) %>%
      .[str_count(job_text, '\\w+')>5] %>%
      .[, job_text_clean := job_text] %>%
      .[, job_text_clean := replace_html(job_text_clean, symbol = F)] %>% # Remove HTML tags - preserves line breaks
      .[, job_text_clean := stringi::stri_replace_all_fixed(str = job_text_clean,
                                                            pattern = corpus::abbreviations_en,
                                                            replacement = gsub("\\.", "", corpus::abbreviations_en, perl = T),
                                                            vectorize_all=FALSE)] %>%
      .[, job_text_clean := gsub("([\r\n])", "\\\n", job_text_clean)] %>%
      .[, job_text_clean := gsub("\\n\\s+\\n", "\\\n\\\n", job_text_clean)] %>%
      .[, job_text_clean := gsub("\\n{2,}", "\\\n\\\n", job_text_clean)] %>%
      .[, job_text_clean := gsub("\n", " +|+ ", job_text_clean, fixed = T)] %>%
      .[, para := strsplit(as.character(job_text_clean), " +|+ ", fixed = T, )] %>%
      unnest(cols = para) %>%
      setDT(.) %>%
      .[, para := str_squish(para)] %>%
      #.[nchar(para) > 0] %>%
      select(job_id, para) %>%
      .[, nchar := nchar(para)] %>%
      .[, nfeat := str_count(para, '\\w+')] %>%
      .[, id_real := 1:.N, by = job_id] %>%
      .[, id := ifelse(nfeat < 20 & id_real != 1, NA, id_real)] %>%
      #.[, id := ifelse(is.na(id), shift(id)+1, id), by = job_id] %>%
      .[, id := nafill(id, type = "locf"), by = job_id] %>%
      .[, id := nafill(id, type = "nocb"), by = job_id] %>%
      .[, cs := cumsum(nfeat) %/% 100, by = .(job_id, id)] %>%
      .[, id := id+(cs/100)] %>%
      .[, .(para = paste0(para, collapse = "\n")), by = .(job_id, id)] %>%
      .[, nchar := nchar(para)] %>%
      .[, nfeat := str_count(para, '\\w+')] %>%
      .[, large := ifelse(nfeat > 200, TRUE, FALSE)] %>%
      .[, sequence := ifelse(large == TRUE, strsplit(as.character(para), "(?<=[\\.?!\\n])", perl = T), para)] %>%
      unnest(cols = sequence) %>%
      setDT(.) %>%
      select(job_id, large, sequence) %>%
      .[, nchar := nchar(sequence)] %>%
      .[, nfeat := str_count(sequence, '\\w+')] %>%
      .[, id_real := 1:.N, by = job_id] %>%
      .[, id := ifelse(nfeat < 100 & large == TRUE & id_real != 1, NA, id_real)] %>%
      #.[, id := ifelse(is.na(id), shift(id)+1, id), by = job_id] %>%
      .[, id := nafill(id, type = "locf"), by = job_id] %>%
      .[, id := nafill(id, type = "nocb"), by = job_id] %>%
      .[, cs := cumsum(nfeat) %/% 200, by = .(job_id, id)] %>%
      .[, id := id+(cs/100)] %>%
      .[, .(sequence = paste0(sequence, collapse = " "), n = .N), by = .(job_id, id, large)] %>%
      .[, nchar := nchar(sequence)] %>%
      .[, nfeat := str_count(sequence, '\\w+')] %>%
      .[, large := ifelse(nfeat > 200, TRUE, FALSE)] %>%
      .[, sequence := ifelse(large == TRUE, strsplit(as.character(sequence), "(?<=[\\.?!\\n\\*,])", perl = T), sequence)] %>%
      unnest(cols = sequence) %>%
      setDT(.) %>%
      select(job_id, large, sequence) %>%
      .[, nchar := nchar(sequence)] %>%
      .[, nfeat := str_count(sequence, '\\w+')] %>%
      .[, id_real := 1:.N, by = job_id] %>%
      .[, id := ifelse(nfeat < 100 & large == TRUE & id_real != 1, NA, id_real)] %>%
      #.[, id := ifelse(is.na(id), shift(id)+1, id), by = job_id] %>%
      .[, id := nafill(id, type = "locf"), by = job_id] %>%
      .[, id := nafill(id, type = "nocb"), by = job_id] %>%
      .[, cs := cumsum(nfeat) %/% 200, by = .(job_id, id)] %>%
      .[, id := id+(cs/100)] %>%
      .[, .(sequence = paste0(sequence, collapse = " "), n = .N), by = .(job_id, id, large)] %>%
      .[, nchar := nchar(sequence)] %>%
      .[, nfeat := str_count(sequence, '\\w+')] %>%
      .[, id_real := 1:.N, by = job_id] %>%
      .[, id := ifelse(nfeat < 10, NA, id_real)] %>%
      .[, id := nafill(id, type = "locf"), by = job_id] %>%
      .[, id := nafill(id, type = "nocb"), by = job_id] %>%
      .[, .(sequence = paste0(sequence, collapse = " "), n = .N), by = .(job_id, id)] %>%
      .[, nchar := nchar(sequence)] %>%
      .[, nfeat := str_count(sequence, '\\w+')] %>%
      .[, id_real := 1:.N, by = job_id] %>%
      .[, seq_id := paste0(job_id,"_",sprintf("%04d", as.numeric(id_real)))] %>%
      select(job_id, seq_id, sequence, nfeat, nchar)
    
    df_chunked <- setDT(bind_rows(df_chunked, df_title)) %>% .[order(seq_id)]
    
    saveRDS(df_chunked, file = paste0("./int_data/sequences/sequences_",name,".rds"))
    
  })
  
  unlink(xml_path)
  
  warning(paste0("SUCCESS: ",i))
  cat(paste0("\nSUCCESS: ",i,"\n"))
  return("")
}, mc.cores = 1)

#sink()
#system("echo sci2007! | sudo -S shutdown -h now")
#### END ####

#### DICTIONARIES ####
remove(list = ls())
df_dict <- bind_rows(read_xlsx("./aux_data/wfh_v6.xlsx", sheet = 1) %>% clean_names %>% select(glob_en) %>% mutate(source = "wfh"),
                     read_xlsx("./aux_data/wfh_v6.xlsx", sheet = 2) %>% clean_names %>% select(glob_en) %>% mutate(source = "generic"),
                     read_xlsx("./aux_data/wfh_v6.xlsx", sheet = 3) %>% clean_names %>% select(glob_en) %>% mutate(source = "excemptions"),
                     read_xlsx("./aux_data/wfh_v6.xlsx", sheet = 4) %>% clean_names %>% select(glob_en) %>% mutate(source = "intensity"),
                     read_xlsx("./aux_data/wfh_v6.xlsx", sheet = 5) %>% clean_names %>% select(glob_en) %>% mutate(source = "negation"))

length(unique(df_dict$glob_en[df_dict$source == "wfh"])) # 176
length(unique(df_dict$glob_en[df_dict$source == "generic"])) # 5

df_dict$glob_en <- tolower(df_dict$glob_en)
df_dict[df_dict == ""] <- NA
df_dict <- df_dict %>% group_by(source) %>% distinct(glob_en, .keep_all = T) %>% ungroup

length(unique(df_dict$glob_en[df_dict$source == "wfh"])) # 176
length(unique(df_dict$glob_en[df_dict$source == "generic"])) # 5

length(df_dict$glob_en) # 353
length(unique(df_dict$glob_en)) # 350

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

wfh_dict_glob <- df_dict %>% filter(source %in% c("wfh"))
wfh_dict_glob <- setNames(as.list(wfh_dict_glob$glob_en), wfh_dict_glob$key) %>% dictionary()

neg_dict_glob <- df_dict %>% filter(source %in% c("negation"))
neg_dict_glob <- setNames(as.list(neg_dict_glob$glob_en), neg_dict_glob$key) %>% dictionary()

wfh_and_neg_dict_glob <- df_dict %>% filter(source %in% c("wfh", "negation"))
wfh_and_neg_dict_glob <- setNames(as.list(wfh_and_neg_dict_glob$glob_en), wfh_and_neg_dict_glob$key) %>% dictionary()

intensity_dict_glob <- df_dict %>% filter(source %in% c("intensity"))
intensity_dict_glob <- setNames(as.list(intensity_dict_glob$glob_en), intensity_dict_glob$key) %>% dictionary()

wfh_and_generic_dict_glob <- df_dict %>% filter(source %in% c("wfh", "generic"))
wfh_and_generic_dict_glob <- setNames(as.list(wfh_and_generic_dict_glob$glob_en), wfh_and_generic_dict_glob$key) %>% dictionary()

generic_dict_glob <- df_dict %>% filter(source %in% c("generic"))
generic_dict_glob <- setNames(as.list(generic_dict_glob$glob_en), generic_dict_glob$key) %>% dictionary()

generic_and_excemptions_dict_glob <- df_dict %>% filter(source %in% c("generic", "excemptions"))
generic_and_excemptions_dict_glob <- setNames(as.list(generic_and_excemptions_dict_glob$glob_en), generic_and_excemptions_dict_glob$key) %>% dictionary()

wfh_dict_glob <- df_dict %>% filter(source %in% c("wfh"))
wfh_dict_glob <- setNames(as.list(wfh_dict_glob$glob_en), wfh_dict_glob$key) %>% dictionary()

all_dict <- setNames(as.list(df_dict$glob_en), df_dict$key) %>% dictionary()

df_dict$lu <- NA
df_dict$lu <- gsub("*", "\\w+", df_dict$glob_en, fixed = T)
df_dict$lu <- paste0("\\b(",gsub(" ", ")\\s(", all_dict$lu, fixed = T),")\\b")
df_dict$rp <- NA
df_dict$rp[df_dict$source == "wfh"] <-"<\\U\\1 \\U\\2 \\U\\3 \\U\\4>"
df_dict$rp[df_dict$source == "generic"] <-"[\\U\\1 \\U\\2 \\U\\3 \\U\\4]"
df_dict$lu[!(df_dict$source %in% c("wfh", "generic"))] <- paste0("\\b([",df_dict$glob_en[!(df_dict$source %in% c("wfh", "generic"))],"])\\b")
df_dict$rp[!(df_dict$source %in% c("wfh", "generic"))] <- " \\U\\1 "
rm(list = setdiff(ls(),c("df_dict", "all_dict", "wfh_dict_glob", "neg_dict_glob", "wfh_and_neg_dict_glob", "intensity_dict_glob", "wfh_and_generic_dict_glob", "generic_dict_glob", "generic_and_excemptions_dict_glob")))
#### end ####

#### GET PATH NAMES TO MAKE DFM ####
paths <- list.files("./int_data/sequences/", pattern = "*.rds", full.names = T) %>% sort(decreasing = T)
paths
paths_check <- paths %>% gsub("./int_data/sequences//sequences_","",., fixed = T) %>% gsub(".rds", "", ., fixed = T) %>% str_sub(., -8, -1)
paths_done <- list.files("./int_data/wfh_v6/", pattern = "*.rds" , full.names = F) %>% gsub(".rds", "", ., fixed = T) %>% str_sub(., -8, -1) %>% unique()
paths_done
paths <- paths[!(paths_check %in% paths_done)]
paths
rm(list = setdiff(ls(),c("paths", "df_dict", "all_dict", "wfh_dict_glob", "neg_dict_glob", "wfh_and_neg_dict_glob", "intensity_dict_glob", "wfh_and_generic_dict_glob", "generic_dict_glob", "generic_and_excemptions_dict_glob")))
#### /END ####

#### MAKE DFM ####
source("/mnt/disks/pdisk/code/safe_mclapply.R")
safe_mclapply(1:length(paths), function(i) {
  
  warning(paste0("BEGIN FILE: ",i))
  name <- gsub("./int_data/sequences//sequences_","", paths[i], fixed = T) %>% gsub(".rds", "", ., fixed = T) %>% gsub("_AddFeed_", "", ., fixed = T)
  df_ss_sequence <- readRDS(paths[i]) %>%
    setDT(.) %>%
    .[, sequence_clean := str_replace_all(sequence, "[?]", " QM ")] %>%
    .[, sequence_clean := str_replace_all(sequence_clean, "[:]", " CL ")] %>%
    .[, sequence_clean := str_squish(tolower(str_replace_all(sequence_clean, "[^[:alnum:]]|\\s+", " ")))]
  
  m <- nrow(df_ss_sequence)
  df_ss_sequence_ls <- df_ss_sequence %>%
    .[, group_id := seq(1:m) %/% 50000] %>%
    group_split(group_id)
  
  rm(df_ss_sequence)
  
  q <- length(df_ss_sequence_ls)
  #head(df_ss_sequence_ls[[1]])
  source("/mnt/disks/pdisk/code/safe_mclapply.R")
  df_dfm_ls <- safe_mclapply(1:q, function(j) {
    # WFH with Negation or Intensity
    
    x <- df_ss_sequence_ls[[j]] %>%
      quanteda::corpus(., text_field = "sequence", docid_field = "seq_id", unique_docnames = TRUE) %>%
      quanteda::tokens(., what = "word", remove_punct = T, remove_symbols = T, remove_url = T, remove_separators = T,  split_hyphens = T, verbose = T, padding = FALSE)
    
    # WFH and negation
    x_wfh_neg_window <- tokens_select(x, pattern = wfh_dict_glob, selection = "keep", valuetype = "glob", case_insensitive = TRUE, padding = FALSE, window = c(4,3), verbose = T)
    x_dfm_wfh_w_neg <- quanteda::dfm(tokens_lookup(x_wfh_neg_window,  wfh_and_neg_dict_glob, valuetype = "glob", case_insensitive = T, verbose = TRUE))
    remove(x_wfh_neg_window)
    #topfeatures(x_dfm_wfh_w_neg, n = 100)
    
    # Intensity
    x_wfh_intensity_window <- tokens_select(x, pattern = wfh_dict_glob, selection = "keep", valuetype = "glob", case_insensitive = TRUE, padding = FALSE, window = 6, verbose = T)
    x_dfm_intensity <- quanteda::dfm(tokens_lookup(x_wfh_intensity_window,  intensity_dict_glob, valuetype = "glob", case_insensitive = T, verbose = TRUE))
    remove(x_wfh_intensity_window)
    #topfeatures(x_dfm_intensity, n = 100)
    
    # Generic and Disqual
    x_generic_window <- tokens_select(x, pattern = generic_dict_glob, selection = "keep", valuetype = "glob", case_insensitive = TRUE, padding = FALSE, window = 20, verbose = T)
    x_dfm_generic_and_excemptions <- quanteda::dfm(tokens_lookup(x_generic_window, generic_and_excemptions_dict_glob, valuetype = "glob", case_insensitive = T, verbose = TRUE))
    remove(x_generic_window)
    #topfeatures(x_dfm_generic_and_excemptions, n = 100)
    
    x_dfm <- cbind(x_dfm_wfh_w_neg, x_dfm_intensity, x_dfm_generic_and_excemptions)
    
    remove(list = c("x_dfm_wfh_w_neg", "x_dfm_intensity", "x_dfm_generic_and_excemptions"))
    
    rm(x)
    colnames(x_dfm)
    x_dfm <- x_dfm[rowSums(x_dfm)>0,]
    #warning(paste("DONE PEICE: ",j,"  ",j/q))
    #cat(paste("DONE PEICE: ",j,"  ",j/q))
    return(x_dfm)
  }, mc.cores = 24)
  
  #warning(paste("DONE ALL PEICES: ",j/q))
  #sink("./aux_data/log_file_make_wfh_feats.txt", append=TRUE)
  #cat(paste("\nDONE PEICES: ",i,"\n"))
  #sink()
  
  rm(df_ss_sequence_ls)
  
  str <-paste0("df_dfm <- rbind(",paste0(paste0("df_dfm_ls[[",seq(1,q,1),"]]"), collapse = ", "),")")
  str
  eval(parse(text = str))
  
  rm(df_dfm_ls)
  
  df_ss_sequence <- readRDS(paths[i]) %>%
    setDT(.) %>%
    .[seq_id %in% docnames(df_dfm)]
  
  #sink("./aux_data/log_file_make_wfh_feats.txt", append=TRUE)
  warning(paste0("\nALL EQUAL: ",i,"  ",all.equal(df_ss_sequence$seq_id, docnames(df_dfm))),"\n")
  #sink()
  
  df_ss_sequence <- cbind(df_ss_sequence, convert(df_dfm, to = "data.frame"))

  # Export
  saveRDS(df_ss_sequence, file = paste0("./int_data/wfh_v6/wfh_v6_raw_sequences_",name,".rds"))
  saveRDS(df_dfm, file = paste0("./int_data/wfh_v6/wfh_v6_dfm_",name,".rds"))
  
  #timeEnd<-Sys.time()
  #difference <- round(as.numeric(difftime(timeEnd, timeStart, units='mins')),digits = 2)
  #difference
  #sink("./aux_data/log_file_make_wfh_feats.txt", append=TRUE)
  warning(paste0("\nSUCCESS: ",i,"\n"))
  #cat(paste0("\nDID: ",i," IN  ",difference," minutes\n"))
  return("")
}, mc.cores = 2)

sink()
system("echo sci2007! | sudo -S shutdown -h now")
#### END ####

#### CREATE SAMPLE FOR LABELS ####
rm(list = ls())
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
nrow(wfh_neg_v6_long) # 721

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
  df <- df %>% .[comb_wfh == 0 & comb_intensity == 0 & comb_disqual == 0 & comb_generic == 1]
  df <- df %>% sample_n(., pmin(nrow(.), 500))
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
nrow(generic_v6_long) # 863

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
  df <- df %>% .[comb_wfh == 0 & comb_intensity == 0 & comb_disqual == 0 & comb_generic == 1]
  df <- df %>% sample_n(., pmin(nrow(.), 500))
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
  sample_n(., size = pmin(n(),pmax(10, n()^(2/3)))) %>%
  setDT(.) %>%
  .[, N := .N, by = key]
rm(generic_v6)
nrow(generic_v6_long) # 863

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
    sample_n(., size = pmin(n(), 3)) %>%
    setDT(.)
}, mc.cores = 20)
random_non_tagged <- rbindlist(random_non_tagged, fill = T)
nrow(random_non_tagged) # 1,224
ls()

df <- bind_rows(wfh_v6_long,
                wfh_neg_v6_long,
                wfh_intensity_v6_long,
                excemptions_v6_long,
                generic_v6_long,
                random_non_tagged)
nrow(df) # 5,472
df <- df %>% distinct(sequence, .keep_all = T)
nrow(df) # 5,298

saveRDS(df, file = "./int_data/training_v6/usa_training_v6_unclustered.rds")

#### END ####



