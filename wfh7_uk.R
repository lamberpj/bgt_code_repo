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
setwd("/mnt/disks/pdisk/bg-uk/")
#setDTthreads(1)

remove(list = ls())
#### end ####

#### PREPARE DATA ####

#system("gsutil -m cp -r gs://for_transfer/data_ingest/bgt_upload/UK_stru /mnt/disks/pdisk/bg-uk/raw_data/main/")
#system("gsutil -m cp -r gs://for_transfer/data_ingest/bgt_upload/UK_raw /mnt/disks/pdisk/bg-uk/raw_data/text/")
#system("gsutil -m cp -r gs://for_transfer/wham/UK /mnt/disks/pdisk/bg-uk/int_data/wham_pred/")



#
#library(filesstrings)
#paths <- list.files("/mnt/disks/pdisk/bg-uk/raw_data/main/", full.names = T, pattern = ".zip", recursive = T)
#paths
#
#lapply(1:length(paths), function(i) {
#  system(paste0("unzip -n ",paths[i]," -d ./raw_data/main"))
#  unlink(paths[i])
#})

#### END ####

#### IMPORT RAW TEXT ####
remove(list = ls())
#### /END ####

#### GET PATH NAMES TO MAKE SEQUENCES ####
paths <- list.files("./raw_data/text/", pattern = "*.zip", full.names = T)
paths <- paths[grepl("2019|2020|2021|2022", paths)]
paths_done <- list.files("./int_data/sequences/", pattern = "*.rds", full.names = F) %>%
  gsub("sequences_", "", .) %>% gsub(".rds", "", .) %>% unique
paths_done <- paths_done[grepl("2019|2020|2021|2022", paths_done)]
paths_check <- list.files("./raw_data/text/", pattern = "*.zip", full.names = T) %>%
  str_sub(., -21, -5)
paths_check <- paths_check[grepl("2019|2020|2021|2022", paths_check)]
paths_check[!(paths_check %in% paths_done)]
paths <- paths[!(paths_check %in% paths_done)]
remove(list = c("paths_check", "paths_done"))

paths
#### /END ####

#### READ XML NAD MAKE SEQUENCES ####
paths
source("/mnt/disks/pdisk/bgt_code_repo/safe_mclapply.R")

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
      .[, nchar := nchar(sequence)] %>%
      .[, nfeat := str_count(sequence, '\\w+')]
    
    df_ads <- data.table(job_id = df_job_id, job_text = df_job_text) %>%
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
}, mc.cores = 18)

#sink()
system("echo sci2007! | sudo -S shutdown -h now")


# Send sequences to bucket
system("gsutil -m cp -r /mnt/disks/pdisk/bg-uk/int_data/sequences/ gs://for_transfer/sequences_uk/")

#### END ####

#### DICTIONARIES ####
remove(list = ls())
df_dict <- bind_rows(read_xlsx("./aux_data/wfh_v8.xlsx", sheet = 1) %>% clean_names %>% filter(source == "oecd") %>% select(glob_en) %>% mutate(source = "wfh"),
                     read_xlsx("./aux_data/wfh_v8.xlsx", sheet = 2) %>% clean_names %>% select(glob_en) %>% mutate(source = "generic"),
                     read_xlsx("./aux_data/wfh_v8.xlsx", sheet = 3) %>% clean_names %>% select(glob_en) %>% mutate(source = "excemptions"),
                     read_xlsx("./aux_data/wfh_v8.xlsx", sheet = 4) %>% clean_names %>% select(glob_en) %>% mutate(source = "intensity"),
                     read_xlsx("./aux_data/wfh_v8.xlsx", sheet = 5) %>% clean_names %>% select(glob_en) %>% mutate(source = "negation"))

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
paths_check <- paths %>% gsub("./int_data/sequences/sequences_","",., fixed = T) %>% gsub(".rds", "", ., fixed = T) %>% str_sub(., -8, -1)
paths_done <- list.files("./int_data/wfh_v8/", pattern = "*.rds" , full.names = F) %>% gsub(".rds", "", ., fixed = T) %>% str_sub(., -8, -1) %>% unique()
paths_done
paths <- paths[!(paths_check %in% paths_done)]
paths
rm(list = setdiff(ls(),c("paths", "df_dict", "all_dict", "wfh_dict_glob", "neg_dict_glob", "wfh_and_neg_dict_glob", "intensity_dict_glob", "wfh_and_generic_dict_glob", "generic_dict_glob", "generic_and_excemptions_dict_glob")))
#### /END ####

#### MAKE DFM ####
source("/mnt/disks/pdisk/bgt_code_repo/safe_mclapply.R")
safe_mclapply(1:length(paths), function(i) {

  warning(paste0("BEGIN FILE: ",i))
  name <- gsub("./int_data/sequences//sequences_","", paths[i], fixed = T) %>% gsub(".rds", "", ., fixed = T) %>% gsub("_AddFeed_", "", ., fixed = T)
  df_ss_sequence <- readRDS(paths[i]) %>%
    setDT(.)
  
  %>%
    .[, sequence := str_trim(gsub("[^[:alnum:][:space:]]"," ",sequence))]
  
  # WFH without Negation or Intensity
  x <- df_ss_sequence %>%
    quanteda::corpus(., text_field = "sequence", docid_field = "seq_id", unique_docnames = TRUE) %>%
    quanteda::tokens(., what = "word", remove_punct = T,  remove_symbols = T, remove_url = T, remove_separators = T, split_hyphens = T, verbose = T, padding = FALSE)
  
  rm(df_ss_sequence)
  
  # WFH and negation
  x_wfh_neg_window <- tokens_select(x, pattern = wfh_dict_glob, selection = "keep", valuetype = "glob", case_insensitive = TRUE, padding = FALSE, window = c(3,2), verbose = T)
  x_dfm_wfh_w_neg <- quanteda::dfm(tokens_lookup(x_wfh_neg_window,  wfh_and_neg_dict_glob, valuetype = "glob", case_insensitive = T, verbose = TRUE))
  remove(x_wfh_neg_window)
  
  x_dfm <- x_dfm_wfh_w_neg
  
  length(df_ss_sequence$sequence[grepl("remote", df_ss_sequence$sequence, ignore.case = T)])
  length(df_ss_sequence$sequence[as.data.table(x_dfm)$remote>0])
  check <- df_ss_sequence$sequence[grepl("remote", df_ss_sequence$sequence) & as.data.table(x_dfm)$remote==0]
  
  sum(grepl("remote", df_ss_sequence$sequence))
  
  remove(list = c("x_dfm_wfh_w_neg"))
  
  x_dfm <- x_dfm[rowSums(x_dfm)>0,]
  
  saveRDS(x_dfm, file = paste0("./int_data/wfh_v8/wfh_v8_dfm_",name,".rds"))
  
  warning(paste0("\nSUCCESS: ",i,"\n"))
  #cat(paste0("\nDID: ",i," IN  ",difference," minutes\n"))
  return("")
}, mc.cores = 32)

sink()
system("echo sci2007! | sudo -S shutdown -h now")
#### END ####

#### AGGREGATE WHAM TO JOB AD LEVEL ####
remove(list = ls())
paths <- list.files("./int_data/wham_pred", pattern = "*.txt", full.names = T)
paths <- paths[grepl("2019|2020|2021|2022", paths)]
source("/mnt/disks/pdisk/bgt_code_repo/safe_mclapply.R")

df_wham <- safe_mclapply(1:length(paths), function(i) {
  df <- fread(paths[i])  %>%
    .[, job_id := str_sub(seq_id,1, -6)] %>%
    .[, .(wfh_prob = max(wfh_prob)), by = job_id]
  warning(paste0("\nDONE: ",i/length(paths)))
  return(df)
}, mc.cores = 16)

df_wham <- rbindlist(df_wham)

df_wham$job_id <- as.numeric(df_wham$job_id)
df_wham$wfh_prob <- as.numeric(df_wham$wfh_prob)

df_wham_preperiod <- fread(file = "/mnt/disks/pdisk/bg_combined/int_data/subsample_wham/df_ss_wham.csv")
#table(df_wham_preperiod$year)
table(df_wham_preperiod$country)
df_wham_preperiod <- setDT(df_wham_preperiod) %>%
  .[country == "UK" & year < 2019] %>%
  select(job_id, wfh_prob)

df_wham_preperiod$job_id <- as.numeric(df_wham_preperiod$job_id)
df_wham_preperiod$wfh_prob <- as.numeric(df_wham_preperiod$wfh_prob)

head(df_wham)
head(df_wham_preperiod)

uniqueN(df_wham$job_id)/length(df_wham$job_id)
uniqueN(df_wham_preperiod$job_id)/length(df_wham_preperiod$job_id)

df_wham <- bind_rows(df_wham, df_wham_preperiod)

remove(list = setdiff(ls(), "df_wham"))

df_wham <- df_wham %>%
  unique(., by = "job_id")

df_wham <- df_wham %>%
  .[, wfh := as.numeric(wfh_prob>0.5)] %>%
  .[, wfh_prob := round(wfh_prob, 3)]

df_wham <- df_wham %>%
  .[, job_id := as.numeric(job_id)]

df_wham <- df_wham %>%
  rename(wfh_wham_prob = wfh_prob,
         wfh_wham = wfh)
#### /END ####

#### EXTRACT URL AND SOURCE FOR ANZ ####
paths <- list.files("./raw_data/text/", pattern = "*.zip", full.names = T)
paths
source("/mnt/disks/pdisk/bgt_code_repo/safe_mclapply.R")
df_src <- safe_mclapply(1:length(paths), function(i) {
  name <- str_sub(paths[i], -21, -5)
  name
  warning(paste0("\nBEGIN: ",i,"  '",name,"'"))
  cat(paste0("\nBEGIN: ",i,"  '",name,"'"))
  system(paste0("unzip -n ",paths[i]," -d ./raw_data/text/"))
  xml_path = gsub(".zip", ".xml", paths[i])
  xml_path
  df_xml <- read_xml(xml_path) %>%
    xml_find_all(., ".//Job")
  df_job_id <- xml_find_all(df_xml, ".//JobID") %>% xml_text
  df_job_url <- xml_find_all(df_xml, ".//JobURL") %>% xml_text
  df_job_domain <- xml_find_all(df_xml, ".//JobDomain") %>% xml_text
  remove("df_xml")
  df <- data.table(job_id = df_job_id, job_domain = df_job_domain, job_url = df_job_url)
  unlink(xml_path)
  warning(paste0("SUCCESS: ",i))
  cat(paste0("\nSUCCESS: ",i,"\n"))
  return(df)
}, mc.cores = 12)

df_src <- rbindlist(df_src)
df_src$job_id <- as.numeric(df_src$job_id)
df_src <- df_src %>% unique(., by = "job_id")
#### END ####

#### MERGE WHAM PREDICTIONS INTO THE STRUCTURED DATA AND RESAVE ####
mean(df_wham$wfh_wham, na.rm = T)

paths <- list.files("/mnt/disks/pdisk/bg-uk/raw_data/main", pattern = ".zip", full.names = T)
paths
source("/mnt/disks/pdisk/bgt_code_repo/safe_mclapply.R")

safe_mclapply(2014:2022, function(x) {

  paths_year <- paths[grepl(x, paths)]
  
  df_stru <- safe_mclapply(1:length(paths_year), function(i) {
    
    warning(paste0("\nSTART: ",i,"\n"))
    warning(paste0(paths_year[i]))
    df <- fread(cmd = paste0('unzip -p ', paths_year[i]), nThread = 8, colClasses = "character", stringsAsFactors = FALSE,
                select = c("JobID","JobDate","CanonCountry","Nation","Region","TTWA","CanonCounty","CanonCity","CanonEmployer",
                           "InternshipFlag","MaxDegreeLevel","CanonMinimumDegree","MinDegreeLevel","CanonJobHours","CanonJobType","MaxExperience","MinExperience",
                           "MaxAnnualSalary","MinAnnualSalary","MaxHourlySalary","MinHourlySalary","WorkFromHome","UKSOCCode","UKSOCUnitGroup","UKSOCMinorGroup",
                           "UKSOCSubMajorGroup","UKSOCMajorGroup","BGTOcc","BGTOccName","BGTOccGroupName","BGTCareerAreaName","SICCode","SICClass","SICGroup",
                           "SICDivision","SICSection","StockTicker","LocalAuthorityDistrict","LocalEnterprisePartnership","PreferredNQFLevels","RequiredNQFLevels")) %>%
      clean_names %>%
      setDT(.) %>%
      .[, job_id := as.numeric(job_id)]
    
    df[df == "na"] <- NA
    df[df == "-999"] <- NA
    df[df == "-999.00"] <- NA
    df[df == ""] <- NA
    
    df <- df %>%
      .[, job_ymd := ymd(job_date)] %>%
      .[, year_quarter := as.yearqtr(job_ymd)]
    
    df <- df %>%
      merge(x = ., y = df_wham, by = "job_id", all.x = TRUE, all.y = FALSE)
    
    df <- df %>%
      merge(x = ., y = df_src, by = "job_id", all.x = TRUE, all.y = FALSE)
    
    warning(paste0("\nDONE: ",x,"   ",i))
    return(df)
  }, mc.cores = 1)
  
  df_stru <- rbindlist(df_stru)
  df_stru <- setDT(df_stru)
  
  fwrite(df_stru, file = paste0("./int_data/uk_stru_",x,"_wfh.csv"), nThread = 8)
  
  return("")
  
}, mc.cores = 1)

#### END ####

#### EXTRACT QUARTERLY DATA ####
remove(list = ls())
df_uk_stru_2014 <- fread("../bg-uk/int_data/uk_stru_2014_wfh.csv", nThread = 8) %>% .[!is.na(wfh_wham)]
df_uk_stru_2015 <- fread("../bg-uk/int_data/uk_stru_2015_wfh.csv", nThread = 8) %>% .[!is.na(wfh_wham)]
df_uk_stru_2016 <- fread("../bg-uk/int_data/uk_stru_2016_wfh.csv", nThread = 8) %>% .[!is.na(wfh_wham)]
df_uk_stru_2017 <- fread("../bg-uk/int_data/uk_stru_2017_wfh.csv", nThread = 8) %>% .[!is.na(wfh_wham)]
df_uk_stru_2018 <- fread("../bg-uk/int_data/uk_stru_2018_wfh.csv", nThread = 8) %>% .[!is.na(wfh_wham)]
df_uk_stru_2019 <- fread("../bg-uk/int_data/uk_stru_2019_wfh.csv", nThread = 8) %>% .[!is.na(wfh_wham)]
df_uk_stru_2020 <- fread("../bg-uk/int_data/uk_stru_2020_wfh.csv", nThread = 8) %>% .[!is.na(wfh_wham)]
df_uk_stru_2021 <- fread("../bg-uk/int_data/uk_stru_2021_wfh.csv", nThread = 8) %>% .[!is.na(wfh_wham)]
df_uk_stru_2022 <- fread("../bg-uk/int_data/uk_stru_2022_wfh.csv", nThread = 8) %>% .[!is.na(wfh_wham)]

df_all_uk <- rbindlist(list(df_uk_stru_2014,df_uk_stru_2015,df_uk_stru_2016,df_uk_stru_2017,df_uk_stru_2018, df_uk_stru_2019,df_uk_stru_2020,df_uk_stru_2021,df_uk_stru_2022))

colnames(df_all_uk)

remove(list = setdiff(ls(),"df_all_uk"))

df_all_uk <- df_all_uk %>%
  .[, year_quarter := as.yearqtr(job_ymd)] %>%
  .[, year_month := as.yearmon(job_ymd)]

# load weights
w_uk_2019 <- fread("../bg_combined/aux_data/emp_weights/w_uk_2019.csv") %>%
  .[, emp_share := ifelse(is.na(emp_share), 0, emp_share)]

# Merge in weights
df_all_uk <- df_all_uk %>%
  .[, uk_soc10 := as.numeric(str_sub(uksoc_code, 1, 4))] %>%
  .[!is.na(uk_soc10)] %>%
  .[, country := "UK"] %>%
  merge(x = ., y = w_uk_2019, by = "uk_soc10", all.x = TRUE, all.y = FALSE) %>%
  setDT(.) %>%
  .[!is.na(emp_share) & !is.na(tot_emp)]

# Apportion employment across (weighted) vacancies
df_all_uk <- df_all_uk %>%
  setDT(.) %>%
  .[, job_id_weight := 1/.N, by = job_id] %>%
  setDT(.) %>%
  .[, tot_emp_ad := (tot_emp*job_id_weight)/sum(tot_emp*job_id_weight), by = .(year_month, uk_soc10)]

# Check how problematic the extensive margin is
df_all_uk <- df_all_uk %>%
  select(job_id,country,year_month,job_date,wfh_wham_prob,wfh_wham,nation,region,canon_county,canon_city,ttwa,
         canon_employer,min_experience,max_experience,canon_minimum_degree,min_degree_level,min_annual_salary,canon_job_hours,bgt_occ,sic_code,sic_class,
         sic_group,sic_division,sic_section,tot_emp_ad,job_id_weight,job_domain,job_url)

# Remove Cannon
colnames(df_all_uk) <- gsub("canon_","", colnames(df_all_uk))

# Experience - call this disjoint_exp_max, and disjoint_degree_level
df_all_uk <- df_all_uk %>% rename(disjoint_exp_max = max_experience, disjoint_exp_min = min_experience)

# Min Degree - call this disjoint_degree_name, and disjoint_degree_level
df_all_uk <- df_all_uk %>% rename(disjoint_degree_level = min_degree_level, disjoint_degree_name = minimum_degree)

# Go back and check if other vars

# Industry

# Need to cluster properly, for now let's just make them all "disjoint_sector"
df_all_uk <- df_all_uk %>% rename(disjoint_sector = sic_section)

# Min Salary (call this disjoint_salary")
df_all_uk <- df_all_uk %>% rename(disjoint_salary = min_annual_salary)

# Make country state
df_all_uk <- df_all_uk %>% rename(state = nation)

# Final Subset
df_all_uk <- df_all_uk %>% select(job_id, country, state, region, county, ttwa, city, year_month, job_date, wfh_wham_prob,wfh_wham, employer, bgt_occ, disjoint_exp_max, disjoint_exp_min, job_hours, disjoint_sector, disjoint_degree_level, disjoint_degree_name, disjoint_salary, tot_emp_ad, job_id_weight,job_domain,job_url)
df_all_uk$year <- year(df_all_uk$year_month)
df_all_uk$month <- str_sub(as.character(df_all_uk$year_month), 1, 3)
df_all_uk$month <- factor(df_all_uk$month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))

df_all_uk$disjoint_salary <- as.numeric(df_all_uk$disjoint_salary)
df_all_uk$disjoint_salary <- ifelse(df_all_uk$disjoint_salary<0, NA, df_all_uk$disjoint_salary)

df_all_uk <- setDT(df_all_uk)

# Cluster education
table(df_all_uk$disjoint_degree_name)
df_all_uk$bach_or_higher<-grepl("bachelor|master|doctor|PhD", df_all_uk$disjoint_degree_name, ignore.case = T)

df_all_uk$bach_or_higher<-ifelse(df_all_uk$bach_or_higher==FALSE & 
                                 df_all_uk$disjoint_degree_level >=16 & ! is.na(df_all_uk$disjoint_degree_level), 
                               TRUE, df_all_uk$bach_or_higher)

df_all_uk$bach_or_higher<-ifelse(is.na(df_all_uk$disjoint_degree_level) & df_all_uk$disjoint_degree_name == "",
                              NA, df_all_uk$bach_or_higher)
df_all_uk$bach_or_higher<-ifelse(df_all_uk$bach_or_higher == TRUE, 1, ifelse(df_all_uk$bach_or_higher == FALSE, 0, NA))

# Cluster industry
df_all_uk$sector_clustered<-ifelse(df_all_uk$disjoint_sector %in% c("Wholesale Trade", "Retail Trade", "WHOLESALE AND RETAIL TRADE; REPAIR OF MOTOR VEHICLES AND MOTORCYCLES"), "Wholesale and Retail Trade",
                                   ifelse(df_all_uk$disjoint_sector %in% c("Accommodation and Food Services","ACCOMMODATION AND FOOD SERVICE ACTIVITIES"),"Accomodation and Food Services",
                                          ifelse(df_all_uk$disjoint_sector %in% c("Electricity, Gas, Water and Waste Services","WATER SUPPLY; SEWERAGE, WASTE MANAGEMENT AND REMEDIATION ACTIVITIES", "Utilities", "ELECTRICITY, GAS, STEAM AND AIR CONDITIONING SUPPLY"), "Utility Services",
                                                 ifelse(df_all_uk$disjoint_sector %in% c("Administrative and Support Services","Administrative and Support and Waste Management and Remediation Services","ADMINISTRATIVE AND SUPPORT SERVICE ACTIVITIES"),"Administrative and Support",
                                                        ifelse(df_all_uk$disjoint_sector %in% c("Professional, Scientific and Technical Services","Professional, Scientific, and Technical Services", "PROFESSIONAL, SCIENTIFIC AND TECHNICAL ACTIVITIES"),"Technical Services",
                                                               ifelse(df_all_uk$disjoint_sector %in% c("Other Services", "Other Services (except Public Administration)", "OTHER SERVICE ACTIVITIES"), "Other Services",
                                                                      ifelse(df_all_uk$disjoint_sector %in% c("Education and Training", "Educational Services", "EDUCATION"), "Education",
                                                                             ifelse(df_all_uk$disjoint_sector %in% c("Health Care and Social Assistance", "HUMAN HEALTH AND SOCIAL WORK ACTIVITIES"), "Healthcare",
                                                                                    ifelse(df_all_uk$disjoint_sector %in% c("Public Administration and Safety", "Public Administration", "PUBLIC ADMINISTRATION AND DEFENCE; COMPULSORY SOCIAL SECURITY"), "Public Administration",
                                                                                           ifelse(df_all_uk$disjoint_sector %in% c("Financial and Insurance Services", "Financial and Insurance", "FINANCIAL AND INSURANCE ACTIVITIES"), "Finance and Insurance",
                                                                                                  ifelse(df_all_uk$disjoint_sector %in% c("Information Media and Telecommunications", "Information", "INFORMATION AND COMMUNICATION"), "Information and Communication",
                                                                                                         ifelse(df_all_uk$disjoint_sector %in% c("Manufacturing", "MANUFACTURING"), "Manufacturing",
                                                                                                                ifelse(df_all_uk$disjoint_sector %in% c("Construction", "CONSTRUCTION"), "Construction",
                                                                                                                       ifelse(df_all_uk$disjoint_sector %in% c("Rental, Hiring and Real Estate Services","Real Estate and Rental and Leasing",  "REAL ESTATE ACTIVITIES"), "Real Estate",
                                                                                                                              ifelse(df_all_uk$disjoint_sector %in% c("Agriculture, Forestry and Fishing",  "Agriculture, Forestry, Fishing and Hunting","AGRICULTURE, FORESTRY AND FISHING"),"Agriculture",
                                                                                                                                     ifelse(df_all_uk$disjoint_sector %in% c("Transport, Postal and Warehousing", "Transportation and Warehousing","TRANSPORTATION AND STORAGE"), "Transportation",
                                                                                                                                            ifelse(df_all_uk$disjoint_sector %in% c("Arts and Recreation Services","Arts, Entertainment, and Recreation",  "ARTS, ENTERTAINMENT AND RECREATION"), "Arts and Entertainment",
                                                                                                                                                   ifelse(df_all_uk$disjoint_sector %in% c("Mining", "Mining, Quarrying, and Oil and Gas Extraction","MINING AND QUARRYING"), "Mining", NA))))))))))))))))))

# CLUSTER EXP #
df_all_uk <- df_all_uk %>%
  .[, disjoint_exp_min := ifelse(disjoint_exp_min == -999, NA, disjoint_exp_min)] %>%
  .[, disjoint_exp_max := ifelse(disjoint_exp_max == -999, NA, disjoint_exp_max)]

df_all_uk <- df_all_uk %>%
  .[, exp_max := disjoint_exp_max] %>%
  select(-c(disjoint_exp_min, disjoint_exp_max))

# ARRANGE #
df_all_uk <- df_all_uk %>%
  setDT(.) %>%
  .[order(job_date, job_id)]

head(df_all_uk)

# SAVE #
fwrite(df_all_uk, file = "./int_data/df_uk_standardised.csv")

#### END ####








