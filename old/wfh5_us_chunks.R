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

setDTthreads(2)
getDTthreads()
quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg-us/")
#setDTthreads(1)

remove(list = ls())
#### end ####

#### SUBSET DATA ####
# LOAD EXAMPLE
paths <- list.files("./raw_data/main", pattern = ".txt", full.names = T)
source("/mnt/disks/pdisk/code/safe_mclapply.R")

df_stru <- safe_mclapply(1:length(paths), function(i) {
  warning(paste0("\nSTART: ",i,"\n"))
  warning(paste0(paths[i]))
  df <- fread(paths[i], nThread = 2, colClasses = "character", stringsAsFactors = FALSE,
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
}, mc.cores = 16)

df_stru <- rbindlist(df_stru)
df_stru <- setDT(df_stru)

nrow(df_stru) # 219,392,772

uniqueN(df_stru$year_quarter) # 31
k <- round(nrow(df_stru)*0.01/uniqueN(df_stru$year_quarter))
k # 70772

df_stru_ss <- df_stru %>%
  group_by(year_quarter) %>%
  sample_n(., size = k) %>%
  setDT(.)

nrow(df_stru_ss) # 2,193,932
nrow(df_stru_ss)/nrow(df_stru) # 0.01

bgt_job_id_keep_ss_vector <- df_stru_ss %>% select(bgt_job_id)
save(bgt_job_id_keep_ss_vector, file = "./aux_data/bgt_job_id_keep_ss_v5_vector.RData")
ls()
# Make stru data
load(file = "./aux_data/bgt_job_id_keep_ss_v5_vector.RData")
colnames(df_stru)
df_stru_ss_plus <- df_stru %>%
  .[bgt_job_id %in% bgt_job_id_keep_ss_vector$bgt_job_id]
save(df_stru_ss_plus, file = "./aux_data/df_stru_ss_plus_v5_us.RData")

#### END ####

#### IMPORT RAW TEXT ####
remove(list = ls())
load(file = "./aux_data/bgt_job_id_keep_ss_v5_vector.RData")
ls()
#### /END ####

#### GET PATH NAMES TO MAKE SENTENCES ####
paths <- list.files("./raw_data/text/", pattern = "*.zip", full.names = T)
paths_done <- list.files("./int_data/sentences/", pattern = "*.rds", full.names = F) %>%
  gsub("sentences_", "", .) %>% gsub(".rds", "", .) %>% unique
paths_check <- list.files("./raw_data/text/", pattern = "*.zip", full.names = T) %>%
  str_sub(., -21, -5)
paths_check[!(paths_check %in% paths_done)]
paths <- paths[!(paths_check %in% paths_done)]
remove(list = c("paths_check", "paths_done"))
set.seed(1)
#### /END ####

#### READ XML NAD MAKE SENTENCES ####
paths
source("/mnt/disks/pdisk/code/safe_mclapply.R")
safe_mclapply(1:length(paths), function(i) {
  i = 350
  #sink("./aux_data/log_file_make_sentences.txt", append=TRUE)
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
    
    df_title <- data.table(job_id = df_job_id, frag_id = paste0(df_job_id,"_0000"), frag = df_job_title) %>%
      .[job_id %in% bgt_job_id_keep_ss_vector$bgt_job_id] %>%
      .[, nchar := nchar(frag)] %>%
      .[, nfeat := str_count(frag, '\\w+')]
    
    df_ads <- data.table(job_id = df_job_id, job_text = df_job_text) %>%
      .[job_id %in% bgt_job_id_keep_ss_vector$bgt_job_id] %>%
      .[, nchar := nchar(job_text)] %>%
      .[, nfeat := str_count(job_text, '\\w+')]
    
    remove(list = c("df_job_id","df_job_title","df_job_text"))
    
    df_ads <- setDT(df_ads)
    
    quantile(df_ads$nfeat, probs = seq(0, 1, 0.05))
    
    # Clean text
    df_chunked <- copy(df_ads) %>%
      .[, job_text_clean := job_text] %>%
      .[, job_text_clean := replace_html(job_text_clean, symbol = F)] %>% # Remove HTML tags - preserves line breaks
      .[, job_text_clean := stringi::stri_replace_all_fixed(str = job_text_clean,
                                                            pattern = corpus::abbreviations_en,
                                                            replacement = gsub("\\.", "", corpus::abbreviations_en, perl = T),
                                                            vectorize_all=FALSE)] %>%
      .[, job_text_clean := gsub("([\r\n])", "\\\n", job_text_clean)] %>%
      .[, job_text_clean := gsub("(\\n)\\s+(\\n)", "\\1\\2", job_text_clean)] %>%
      .[, job_text_clean := gsub("[\n]{2,})", "\\\n\\\n", job_text_clean)] %>%
      .[str_count(job_text_clean, '\\w+')>25] %>% # Drop ads with fewer than 25 words? Not essential.
      .[, nchar := nchar(job_text_clean)] %>%
      .[, nfeat := str_count(job_text_clean, '\\w+')] %>%
      .[, nlines := str_count(job_text_clean, '\\n+')]
    
    df_chunked2 <- copy(df_chunked) %>%
      .[, job_text_clean := gsub("\n", " +|+ ", job_text_clean, fixed = T)] %>%
      .[, para := strsplit(as.character(job_text_clean), " +|+ ", fixed = T, )] %>%
      unnest(cols = para) %>%
      setDT(.) %>%
      .[, para := str_squish(para)] %>%
      select(job_id, para) %>%
      .[, nchar := nchar(para)] %>%
      .[, nfeat := str_count(para, '\\w+')] %>%
      .[, id_real := 1:.N, by = job_id] %>%
      .[, id := ifelse(nfeat < 15 & id_real != 1, NA, id_real)] %>% # If a line is less than 50 words, group it with one or more adjacent lines with the same property
      .[, id := nafill(id, type = "locf"), by = job_id] %>%
      .[, id := nafill(id, type = "nocb"), by = job_id] %>%
      .[, .(para = paste0(para, collapse = "\n"), .N), by = .(job_id, id)] %>%
      .[, nchar := nchar(para)] %>%
      .[, nfeat := str_count(para, '\\w+')] %>%
      .[, nlines := str_count(para, '\\n+')] %>%
      .[, nfeat_by_nlines := nfeat/nlines] %>%
      .[, nlines_by_nfeat := nlines/nfeat]

    df_chunked3 <- copy(df_chunked2) %>%
      .[, large := ifelse(nfeat > 100 & nlines < 5, 1, 0)] %>%
      .[, sentence := ifelse(large == 1, tokenize_sentences(para), para)] %>%
      unnest(cols = sentence) %>%
      setDT(.) %>%
      select(job_id, large, sentence) %>%
      .[, nchar := nchar(sentence)] %>%
      .[, nfeat := str_count(sentence, '\\w+')] %>%
      .[, id_real := 1:.N, by = .(job_id, large)] %>%
      .[, id_real := ifelse(large == 0, id_real+0.1, id_real)] %>%
      .[, id := ifelse(large == 1, NA, id_real)] %>%
      .[, id := nafill(id, type = "nocb"), by = job_id] %>%
      .[, id := nafill(id, type = "locf"), by = job_id] %>%
      .[, cs := cumsum(nfeat) %/% 100, by = .(job_id, id)] %>%
      .[, id := id+(cs/10000)] %>%
      .[, .(sentence = paste0(sentence, collapse = " "), nsent = .N), by = .(job_id, id, large)] %>%
      select(job_id, sentence, large) %>%
      .[, nchar := nchar(sentence)] %>%
      .[, nfeat := str_count(sentence, '\\w+')] %>%
      .[, nlines := str_count(sentence, '\\n+')] %>%
      .[, nfeat_by_nlines := nfeat/nlines] %>%
      .[, nlines_by_nfeat := nlines/nfeat]
      
    
    quantile(df_chunked$nfeat, probs = seq(0, 1, 0.05))
    quantile(df_chunked2$nfeat, probs = seq(0, 1, 0.05))
    quantile(df_chunked3$nfeat, probs = seq(0, 1, 0.05))
    
    # Add back in chunks with < 30 words with a line break
    
    df_chunked <- setDT(bind_rows(df_chunked, df_title)) %>% .[order(frag_id)]
    
    saveRDS(df_chunked, file = paste0("./int_data/sentences/sentences_",name,".rds"))
    
  })
  
  unlink(xml_path)
  
  warning(paste0("SUCCESS: ",i))
  cat(paste0("\nSUCCESS: ",i,"\n"))
  return("")
}, mc.cores = 6)

sink()
system("echo sci2007! | sudo -S shutdown -h now")
length(list.files("./int_data/sentences_old/"))
#### END ####

#### DICTIONARIES ####
remove(list = ls())
df_dict <- bind_rows(read_xlsx("./aux_data/wfh_v5.xlsx", sheet = 1) %>% clean_names %>% select(glob_en) %>% mutate(source = "wfh"),
                     read_xlsx("./aux_data/wfh_v5.xlsx", sheet = 2) %>% clean_names %>% select(glob_en) %>% mutate(source = "generic"),
                     read_xlsx("./aux_data/wfh_v5.xlsx", sheet = 3) %>% clean_names %>% select(glob_en) %>% mutate(source = "excemptions"),
                     read_xlsx("./aux_data/wfh_v5.xlsx", sheet = 4) %>% clean_names %>% select(glob_en) %>% mutate(source = "intensity"),
                     read_xlsx("./aux_data/wfh_v5.xlsx", sheet = 5) %>% clean_names %>% select(glob_en) %>% mutate(source = "negation"))

length(unique(df_dict$glob_en[df_dict$source == "wfh"])) # 182
length(unique(df_dict$glob_en[df_dict$source == "generic"])) # 5

df_dict$glob_en <- tolower(df_dict$glob_en)
df_dict[df_dict == ""] <- NA
df_dict <- df_dict %>% group_by(source) %>% distinct(glob_en, .keep_all = T) %>% ungroup

length(unique(df_dict$glob_en[df_dict$source == "wfh"])) # 182
length(unique(df_dict$glob_en[df_dict$source == "generic"])) # 5

length(df_dict$glob_en) # 357
length(unique(df_dict$glob_en)) # 357

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

df_dict_global <- df_dict %>% filter(source %in% c("wfh", "generic"))
wfh_dict_glob <- setNames(as.list(df_dict_global$glob_en), df_dict_global$key) %>% dictionary()
wfh_dict <- setNames(as.list(df_dict$glob_en), df_dict$key) %>% dictionary()
df_dict$lu <- NA
#df_dict$lu[df_dict$source == "wfh"] <- toupper(paste0("<", df_dict$glob_en[df_dict$source == "wfh"], ">"))
#df_dict$lu[df_dict$source == "generic"] <- toupper(paste0("|", df_dict$glob_en[df_dict$source == "generic"], "|"))
df_dict$lu <- gsub("*", "\\w+", df_dict$glob_en, fixed = T)
df_dict$lu <- paste0("\\b(",gsub(" ", ")\\s(", df_dict$lu, fixed = T),")\\b")
df_dict$rp <- NA
df_dict$rp[df_dict$source == "wfh"] <-"<\\U\\1 \\U\\2 \\U\\3 \\U\\4>"
df_dict$rp[df_dict$source == "generic"] <-"[\\U\\1 \\U\\2 \\U\\3 \\U\\4]"
df_dict$lu[!(df_dict$source %in% c("wfh", "generic"))] <- paste0("\\b([",df_dict$glob_en[!(df_dict$source %in% c("wfh", "generic"))],"])\\b")
df_dict$rp[!(df_dict$source %in% c("wfh", "generic"))] <- " \\U\\1 "
rm(list = setdiff(ls(),c("df_dict", "wfh_dict_glob", "wfh_dict")))
#### end ####

#### GET PATH NAMES TO MAKE DFM ####
paths <- list.files("./int_data/sentences/", pattern = "*.rds", full.names = T) %>% sort(decreasing = T)
paths
paths_check <- paths %>% gsub("./int_data/sentences//sentences_","",., fixed = T) %>% gsub(".rds", "", ., fixed = T) %>% str_sub(., -8, -1)
paths_done <- list.files("./int_data/wfh_v4/", pattern = "*.rds" , full.names = F) %>% gsub(".rds", "", ., fixed = T) %>% str_sub(., -8, -1) %>% unique()
paths_done
paths <- paths[!(paths_check %in% paths_done)]
rm(list = setdiff(ls(),c("paths", "df_dict", "wfh_dict_glob", "wfh_dict")))
#### /END ####

#### MAKE DFM ####
source("/mnt/disks/pdisk/code/safe_mclapply.R")
safe_mclapply(1:length(paths), function(i) {
  
  warning(paste0("BEGIN FILE: ",i))
  name <- gsub("./int_data/sentences//sentences_","", paths[i], fixed = T) %>% gsub(".rds", "", ., fixed = T) %>% gsub("_AddFeed_", "", ., fixed = T)
  df_ss_frag <- readRDS(paths[i]) %>%
    setDT(.) %>%
    .[, frag_clean := str_replace_all(frag, "[?]", " QM ")] %>%
    .[, frag_clean := str_replace_all(frag_clean, "[:]", " CL ")] %>%
    .[, frag_clean := str_squish(tolower(str_replace_all(frag_clean, "[^[:alnum:]]|\\s+", " ")))]
  
  m <- nrow(df_ss_frag)
  df_ss_frag_ls <- df_ss_frag %>%
    .[, group_id := seq(1:m) %/% 50000] %>%
    group_split(group_id)
  
  rm(df_ss_frag)
  
  q <- length(df_ss_frag_ls)
  #head(df_ss_sentence_ls[[1]])
  source("/mnt/disks/pdisk/code/safe_mclapply.R")
  df_dfm_ls <- safe_mclapply(1:q, function(j) {
    #sink("./aux_data/log_file_make_wfh_feats.txt", append=TRUE)
    #warning(paste("BEGIN PEICE: ",j,"  ",j/q))
    #cat(paste("BEGIN PEICE: ",j,"  ",j/q))
    
    x <- df_ss_frag_ls[[j]] %>%
      quanteda::corpus(., text_field = "frag", docid_field = "frag_id", unique_docnames = TRUE) %>%
      quanteda::tokens(., what = "word", remove_punct = T, remove_symbols = T, remove_url = T, remove_separators = T,  split_hyphens = T, verbose = T)
    x_select <- tokens_select(x, pattern = wfh_dict_glob, selection = c("keep", "remove"), valuetype = "glob", case_insensitive = TRUE, padding = FALSE, window = 4, verbose = T)
    x_dfm <- quanteda::dfm(tokens_lookup(x_select,  wfh_dict, valuetype = "glob", case_insensitive = T, verbose = TRUE))
    
    rm(x)
    colnames(x_dfm)
    x_dfm <- x_dfm[rowSums(x_dfm)>0,]
    #warning(paste("DONE PEICE: ",j,"  ",j/q))
    #cat(paste("DONE PEICE: ",j,"  ",j/q))
    return(x_dfm)
  }, mc.cores = 1)
  
  #warning(paste("DONE ALL PEICES: ",j/q))
  #sink("./aux_data/log_file_make_wfh_feats.txt", append=TRUE)
  #cat(paste("\nDONE PEICES: ",i,"\n"))
  #sink()
  
  rm(df_ss_frag_ls)
  
  str <-paste0("df_dfm <- rbind(",paste0(paste0("df_dfm_ls[[",seq(1,q,1),"]]"), collapse = ", "),")")
  eval(parse(text = str))
  
  rm(df_dfm_ls)
  
  df_ss_frag <- readRDS(paths[i]) %>%
    setDT(.) %>%
    .[frag_id %in% docnames(df_dfm)]
  
  #sink("./aux_data/log_file_make_wfh_feats.txt", append=TRUE)
  warning(paste0("\nALL EQUAL: ",i,"  ",all.equal(df_ss_frag$frag_id, docnames(df_dfm))),"\n")
  #sink()
  
  df_ss_frag <- cbind(df_ss_frag, convert(df_dfm, to = "data.frame"))
  
  # Text format global
  df_ss_frag <- df_ss_frag %>%
    setDT(.) %>%
    .[, frags_tagged := mgsub(x = frag,
                              pattern = df_dict$lu[df_dict$source %in% c("wfh", "generic")],
                              replacement = df_dict$rp[df_dict$source %in% c("wfh", "generic")],
                              ignore.case = TRUE, fixed = FALSE, perl = TRUE, safe = TRUE)] %>%
    .[, frags_tagged := gsub(x = frags_tagged, pattern = "\\s+(>|\\])", replacement = "\\1", ignore.case = TRUE, fixed = FALSE, perl = TRUE)] %>%
    select(job_id, frag_id, nchar, frag, frags_tagged, everything())
  
  # Export
  saveRDS(df_ss_frag, file = paste0("./int_data/wfh_v4/wfh_v4_raw_frags_",name,".rds"))
  saveRDS(df_dfm, file = paste0("./int_data/wfh_v4/wfh_v4_dfm_",name,".rds"))
  
  #timeEnd<-Sys.time()
  #difference <- round(as.numeric(difftime(timeEnd, timeStart, units='mins')),digits = 2)
  #difference
  #sink("./aux_data/log_file_make_wfh_feats.txt", append=TRUE)
  warning(paste0("\nSUCCESS: ",i,"\n"))
  #cat(paste0("\nDID: ",i," IN  ",difference," minutes\n"))
  return("")
}, mc.cores = 40)

sink()
system("echo sci2007! | sudo -S shutdown -h now")
#### END ####


system("zip ./int_data/wfh_v4/wfh_v4_us.zip -r ./int_data/wfh_v4")



