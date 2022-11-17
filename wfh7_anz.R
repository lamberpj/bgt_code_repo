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

#system("git commit -a --allow-empty-message -m ''")
#system("git push origin master")

setDTthreads(2)
getDTthreads()
quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg-anz/")
#setDTthreads(1)

remove(list = ls())
#### end ####

#### PREPARE DATA ####

#system("gsutil -m cp -r gs://for_transfer/data_ingest/bgt_upload/ANZ_raw /mnt/disks/pdisk/bg-anz/raw_data/text/")
#system("gsutil -m cp -r gs://for_transfer/data_ingest/bgt_upload/ANZ_stru /mnt/disks/pdisk/bg-anz/raw_data/main")
#system("gsutil -m cp -r gs://for_transfer/data_ingest/bgt_upload/ANZ_raw /mnt/disks/pdisk/bg-anz/raw_data/text")
#system("gsutil -m cp -r gs://for_transfer/data_ingest/bgt_upload/ANZ_stru /mnt/disks/pdisk/bg-anz/raw_data/main")
#system("gsutil -m cp -r gs://for_transfer/wham/ANZ /mnt/disks/pdisk/bg-anz/int_data/wham_pred")
system("gsutil -m cp -r gs://for_transfer/dict_results/ANZ_full_results.rds /mnt/disks/pdisk/bg-anz/int_data/")

#
#library(filesstrings)
#paths <- list.files("/mnt/disks/pdisk/bg-anz/raw_data/main/", full.names = T, pattern = ".zip", recursive = T)
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

#system("zip -r /mnt/disks/pdisk/bg-anz/int_data/us_sequences.zip /mnt/disks/pdisk/bg-anz/int_data/sequences/")

# Upload Sequences
#system("gsutil -m cp -r /mnt/disks/pdisk/bg-anz/int_data/sequences/ gs://for_transfer/sequences_anz/")

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
    df_chunked <- df_ads %>%
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
      setDT(.)
    
    df_chunked <- df_chunked %>%
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
      setDT(.) 
    
    df_chunked <- df_chunked %>%
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
      setDT(.)
    
    df_chunked <- df_chunked %>%
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
}, mc.cores = 10)

#sink()
system("echo sci2007! | sudo -S shutdown -h now")
#### END ####

# Transfer sequences to instance
#system("gsutil -m cp -r /mnt/disks/pdisk/bg-anz/int_data/sequences/ gs://for_transfer/sequences_anz/")

#### AGGREGATE WHAM TO JOB AD LEVEL ####
remove(list = ls())
paths <- list.files("./int_data/wham_pred", pattern = "*.txt", full.names = T)
paths <- paths[grepl("2019|2020|2021|2022", paths)]
paths
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
  .[country %in% c("Australia","NZ") & year < 2019] %>%
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

remove(list = setdiff(ls(), "df_wham"))
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
}, mc.cores = 4)

df_src <- rbindlist(df_src)
df_src$job_id <- as.numeric(df_src$job_id)
df_src <- df_src %>% unique(., by = "job_id")

remove(list = setdiff(ls(), c("df_wham", "df_src")))
#### END ####

#### DICT ####
df_dict <- readRDS("/mnt/disks/pdisk/bg-anz/int_data/ANZ_full_results.rds")
df_dict <- df_dict %>% select(-sequence_clean)
head(df_dict)
df_dict <- df_dict %>%
  mutate(job_id = str_sub(seq_id,1, -6)) %>%
  select(-seq_id) %>%
  group_by(job_id) %>%
  summarise(narrow_result = max(narrow_result, na.rm = T),
            neg_narrow_result = max(neg_narrow_result, na.rm = T))
head(df_dict)
df_dict <- df_dict %>%
  unique(., by = "job_id")
df_dict <- df_dict %>%
  setDT(.) %>%
  .[, narrow_result := as.numeric(narrow_result)] %>%
  .[, neg_narrow_result := as.numeric(neg_narrow_result)]
df_dict <- df_dict %>%
  .[, job_id := as.numeric(job_id)]

remove(list = setdiff(ls(), c("df_wham", "df_dict", "df_src")))
#### END ####

#### MERGE WHAM PREDICTIONS (!!AND WEB SOURCE!!) INTO THE STRUCTURED DATA AND RESAVE ####
remove(list = setdiff(ls(), c("df_wham", "df_dict", "df_src")))

paths <- list.files("/mnt/disks/pdisk/bg-anz/raw_data/main", pattern = ".zip", full.names = T)
paths
source("/mnt/disks/pdisk/bgt_code_repo/safe_mclapply.R")

safe_mclapply(2014:2022, function(x) {
  
  paths_year <- paths[grepl(x, paths)]
  
  df_stru <- safe_mclapply(1:length(paths_year), function(i) {
    
    warning(paste0("\nSTART: ",i,"\n"))
    warning(paste0(paths_year[i]))
    df <- fread(cmd = paste0('unzip -p ', paths_year[i]), nThread = 8, colClasses = "character", stringsAsFactors = FALSE,
                select = c("JobID","JobDate","CanonCountry","CanonState","CanonCity",
                           "CanonEmployer","InternshipFlag","MinDegreeLevel","CanonMinimumDegree","MaxDegreeLevel","CanonJobHours","CanonJobType",
                           "MaxExperience","MinExperience","MaxAnnualSalary","MinAnnualSalary","MaxHourlySalary","MinHourlySalary","WorkFromHome",
                           "ANZSCOCode","ANZSCOOccupation","ANZSCOMinorGroup","ANZSCOSubMajorGroup","ANZSCOMajorGroup","BGTOcc","BGTOccName",
                           "BGTOccGroupName","BGTCareerAreaName","ANZSICCode","ANZSICClass","ANZSICGroup","ANZSICSubdivision","ANZSICDivision",
                           "StockTicker","SA4","SA4Name")) %>%
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
      merge(x = ., y = df_dict, by = "job_id", all.x = TRUE, all.y = FALSE)
    
    df <- df %>%
      merge(x = ., y = df_src, by = "job_id", all.x = TRUE, all.y = FALSE)
    
    warning(paste0("\nDONE: ",x,"   ",i))
    return(df)
  }, mc.cores = 1)
  
  df_stru <- rbindlist(df_stru)
  df_stru <- setDT(df_stru)
  
  fwrite(df_stru, file = paste0("./int_data/anz_stru_",x,"_wfh.csv"), nThread = 8)
  
  return("")
  
}, mc.cores = 1)

#### END ####

#### EXTRACT QUARTERLY DATA - AUSTRALIA ####
remove(list = ls())
df_anz_stru_2014 <- fread("../bg-anz/int_data/anz_stru_2014_wfh.csv", nThread = 8) %>% .[!is.na(wfh_wham)]
df_anz_stru_2015 <- fread("../bg-anz/int_data/anz_stru_2015_wfh.csv", nThread = 8) %>% .[!is.na(wfh_wham)]
df_anz_stru_2016 <- fread("../bg-anz/int_data/anz_stru_2016_wfh.csv", nThread = 8) %>% .[!is.na(wfh_wham)]
df_anz_stru_2017 <- fread("../bg-anz/int_data/anz_stru_2017_wfh.csv", nThread = 8) %>% .[!is.na(wfh_wham)]
df_anz_stru_2018 <- fread("../bg-anz/int_data/anz_stru_2018_wfh.csv", nThread = 8) %>% .[!is.na(wfh_wham)]
df_anz_stru_2019 <- fread("../bg-anz/int_data/anz_stru_2019_wfh.csv", nThread = 8) %>% .[!is.na(wfh_wham)]
df_anz_stru_2020 <- fread("../bg-anz/int_data/anz_stru_2020_wfh.csv", nThread = 8) %>% .[!is.na(wfh_wham)]
df_anz_stru_2021 <- fread("../bg-anz/int_data/anz_stru_2021_wfh.csv", nThread = 8) %>% .[!is.na(wfh_wham)]
df_anz_stru_2022 <- fread("../bg-anz/int_data/anz_stru_2022_wfh.csv", nThread = 8) %>% .[!is.na(wfh_wham)]

df_all_aus <- rbindlist(list(df_anz_stru_2014,df_anz_stru_2015,df_anz_stru_2016,df_anz_stru_2017,df_anz_stru_2018,df_anz_stru_2019,df_anz_stru_2020,df_anz_stru_2021,df_anz_stru_2022)) %>% filter(canon_country == "AUS") %>% setDT(.)

remove(list = setdiff(ls(),"df_all_aus"))

df_all_aus <- df_all_aus %>%
  setDT(.) %>%
  .[, year_quarter := as.yearqtr(job_ymd)] %>%
  .[, year_month := as.yearmon(job_ymd)]

# load weights
w_aus_2019 <- fread("../bg_combined/aux_data/emp_weights/w_aus_2019.csv") %>%
  .[, emp_share := ifelse(is.na(emp_share), 0, emp_share)]

head(w_aus_2019)

# Merge in weights
nrow(df_all_aus) # 4,254,688
df_all_aus<-setDT(df_all_aus) %>%
  .[, anzsco_code := as.numeric(str_sub(anzsco_code, 1, 4))] %>%
  .[!is.na(anzsco_code)] %>%
  .[, country := "Australia"] %>%
  left_join(w_aus_2019) %>%
  setDT(.) %>%
  .[!is.na(emp_share) & !is.na(tot_emp)]
nrow(df_all_aus) # 3,490,160

# Apportion employment across (weighted) vacancies
df_all_aus <- df_all_aus %>%
  setDT(.) %>%
  .[, job_id_weight := 1/.N, by = job_id] %>%
  setDT(.) %>%
  .[, tot_emp_ad := (job_id_weight*tot_emp)/sum(job_id_weight*tot_emp), by = .(year_month, anzsco_code)]

# Check how problematic the extensive margin is
df_all_aus <- df_all_aus %>%
  select(job_id,country,year_month,job_date, narrow_result, neg_narrow_result, wfh_wham_prob,wfh_wham,canon_state,canon_city,
         canon_employer,min_experience,max_experience,canon_minimum_degree,min_degree_level,canon_job_hours,min_annual_salary,bgt_occ,anzsic_code,anzsic_class,anzsic_group,anzsic_subdivision,anzsic_division,
         tot_emp_ad,job_id_weight, job_domain, job_url)

# Remove Cannon
colnames(df_all_aus) <- gsub("canon_","", colnames(df_all_aus))

# Experience - call this disjoint_exp_max, and disjoint_degree_level
df_all_aus <- df_all_aus %>% rename(disjoint_exp_max = max_experience, disjoint_exp_min = min_experience)

# Min Degree - call this disjoint_degree_name, and disjoint_degree_level
df_all_aus <- df_all_aus %>% rename(disjoint_degree_level = min_degree_level, disjoint_degree_name = minimum_degree)

# Go back and check if other vars

# Need to cluster properly, for now let's just make them all "disjoint_sector"
df_all_aus <- df_all_aus %>% rename(disjoint_sector = anzsic_division)

# Min Salary (call this disjoint_salary")
df_all_aus <- df_all_aus %>% rename(disjoint_salary = min_annual_salary)

# Final Subset
df_all_aus <- df_all_aus %>% select(job_id, country, state, city, year_month, job_date, narrow_result, neg_narrow_result, wfh_wham_prob,wfh_wham, employer, bgt_occ, disjoint_exp_max, disjoint_exp_min, job_hours, disjoint_sector, disjoint_degree_level, disjoint_degree_name, disjoint_salary, tot_emp_ad, job_id_weight, job_domain, job_url)
df_all_aus$year <- year(df_all_aus$year_month)
df_all_aus$month <- str_sub(as.character(df_all_aus$year_month), 1, 3)
df_all_aus$month <- factor(df_all_aus$month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))

df_all_aus$disjoint_salary <- as.numeric(df_all_aus$disjoint_salary)
df_all_aus$disjoint_salary <- ifelse(df_all_aus$disjoint_salary<0, NA, df_all_aus$disjoint_salary)

df_all_aus <- setDT(df_all_aus)

# Cluster education
table(df_all_aus$disjoint_degree_name)
df_all_aus$bach_or_higher<-grepl("bachelor|master|doctor|PhD", df_all_aus$disjoint_degree_name, ignore.case = T)

df_all_aus$bach_or_higher<-ifelse(df_all_aus$bach_or_higher==FALSE & 
                                    df_all_aus$disjoint_degree_level >=16 & ! is.na(df_all_aus$disjoint_degree_level), 
                                  TRUE, df_all_aus$bach_or_higher)

df_all_aus$bach_or_higher<-ifelse(is.na(df_all_aus$disjoint_degree_level) & df_all_aus$disjoint_degree_name == "",
                                  NA, df_all_aus$bach_or_higher)
df_all_aus$bach_or_higher<-ifelse(df_all_aus$bach_or_higher == TRUE, 1, ifelse(df_all_aus$bach_or_higher == FALSE, 0, NA))

# Cluster industry
df_all_aus$sector_clustered<-ifelse(df_all_aus$disjoint_sector %in% c("Wholesale Trade", "Retail Trade", "WHOLESALE AND RETAIL TRADE; REPAIR OF MOTOR VEHICLES AND MOTORCYCLES"), "Wholesale and Retail Trade",
                                    ifelse(df_all_aus$disjoint_sector %in% c("Accommodation and Food Services","ACCOMMODATION AND FOOD SERVICE ACTIVITIES"),"Accomodation and Food Services",
                                           ifelse(df_all_aus$disjoint_sector %in% c("Electricity, Gas, Water and Waste Services","WATER SUPPLY; SEWERAGE, WASTE MANAGEMENT AND REMEDIATION ACTIVITIES", "Utilities", "ELECTRICITY, GAS, STEAM AND AIR CONDITIONING SUPPLY"), "Utility Services",
                                                  ifelse(df_all_aus$disjoint_sector %in% c("Administrative and Support Services","Administrative and Support and Waste Management and Remediation Services","ADMINISTRATIVE AND SUPPORT SERVICE ACTIVITIES"),"Administrative and Support",
                                                         ifelse(df_all_aus$disjoint_sector %in% c("Professional, Scientific and Technical Services","Professional, Scientific, and Technical Services", "PROFESSIONAL, SCIENTIFIC AND TECHNICAL ACTIVITIES"),"Technical Services",
                                                                ifelse(df_all_aus$disjoint_sector %in% c("Other Services", "Other Services (except Public Administration)", "OTHER SERVICE ACTIVITIES"), "Other Services",
                                                                       ifelse(df_all_aus$disjoint_sector %in% c("Education and Training", "Educational Services", "EDUCATION"), "Education",
                                                                              ifelse(df_all_aus$disjoint_sector %in% c("Health Care and Social Assistance", "HUMAN HEALTH AND SOCIAL WORK ACTIVITIES"), "Healthcare",
                                                                                     ifelse(df_all_aus$disjoint_sector %in% c("Public Administration and Safety", "Public Administration", "PUBLIC ADMINISTRATION AND DEFENCE; COMPULSORY SOCIAL SECURITY"), "Public Administration",
                                                                                            ifelse(df_all_aus$disjoint_sector %in% c("Financial and Insurance Services", "Financial and Insurance", "FINANCIAL AND INSURANCE ACTIVITIES"), "Finance and Insurance",
                                                                                                   ifelse(df_all_aus$disjoint_sector %in% c("Information Media and Telecommunications", "Information", "INFORMATION AND COMMUNICATION"), "Information and Communication",
                                                                                                          ifelse(df_all_aus$disjoint_sector %in% c("Manufacturing", "MANUFACTURING"), "Manufacturing",
                                                                                                                 ifelse(df_all_aus$disjoint_sector %in% c("Construction", "CONSTRUCTION"), "Construction",
                                                                                                                        ifelse(df_all_aus$disjoint_sector %in% c("Rental, Hiring and Real Estate Services","Real Estate and Rental and Leasing",  "REAL ESTATE ACTIVITIES"), "Real Estate",
                                                                                                                               ifelse(df_all_aus$disjoint_sector %in% c("Agriculture, Forestry and Fishing",  "Agriculture, Forestry, Fishing and Hunting","AGRICULTURE, FORESTRY AND FISHING"),"Agriculture",
                                                                                                                                      ifelse(df_all_aus$disjoint_sector %in% c("Transport, Postal and Warehousing", "Transportation and Warehousing","TRANSPORTATION AND STORAGE"), "Transportation",
                                                                                                                                             ifelse(df_all_aus$disjoint_sector %in% c("Arts and Recreation Services","Arts, Entertainment, and Recreation",  "ARTS, ENTERTAINMENT AND RECREATION"), "Arts and Entertainment",
                                                                                                                                                    ifelse(df_all_aus$disjoint_sector %in% c("Mining", "Mining, Quarrying, and Oil and Gas Extraction","MINING AND QUARRYING"), "Mining", NA))))))))))))))))))

# CLUSTER EXP #
df_all_aus <- df_all_aus %>%
  .[, disjoint_exp_min := ifelse(disjoint_exp_min == -999, NA, disjoint_exp_min)] %>%
  .[, disjoint_exp_max := ifelse(disjoint_exp_max == -999, NA, disjoint_exp_max)]

df_all_aus <- df_all_aus %>%
  .[, exp_max := disjoint_exp_max] %>%
  select(-c(disjoint_exp_min, disjoint_exp_max))

# ARRANGE #
df_all_aus <- df_all_aus %>%
  setDT(.) %>%
  .[order(job_date, job_id)]

head(df_all_aus)

# SAVE #
fwrite(df_all_aus, file = "./int_data/df_aus_standardised.csv")

#### END AUSTRALIA ####




#### EXTRACT QUARTERLY DATA - NZ ####
remove(list = ls())
df_anz_stru_2014 <- fread("../bg-anz/int_data/anz_stru_2014_wfh.csv", nThread = 8) %>% .[!is.na(wfh_wham)]
df_anz_stru_2015 <- fread("../bg-anz/int_data/anz_stru_2015_wfh.csv", nThread = 8) %>% .[!is.na(wfh_wham)]
df_anz_stru_2016 <- fread("../bg-anz/int_data/anz_stru_2016_wfh.csv", nThread = 8) %>% .[!is.na(wfh_wham)]
df_anz_stru_2017 <- fread("../bg-anz/int_data/anz_stru_2017_wfh.csv", nThread = 8) %>% .[!is.na(wfh_wham)]
df_anz_stru_2018 <- fread("../bg-anz/int_data/anz_stru_2018_wfh.csv", nThread = 8) %>% .[!is.na(wfh_wham)]
df_anz_stru_2019 <- fread("../bg-anz/int_data/anz_stru_2019_wfh.csv", nThread = 8) %>% .[!is.na(wfh_wham)]
df_anz_stru_2020 <- fread("../bg-anz/int_data/anz_stru_2020_wfh.csv", nThread = 8) %>% .[!is.na(wfh_wham)]
df_anz_stru_2021 <- fread("../bg-anz/int_data/anz_stru_2021_wfh.csv", nThread = 8) %>% .[!is.na(wfh_wham)]
df_anz_stru_2022 <- fread("../bg-anz/int_data/anz_stru_2022_wfh.csv", nThread = 8) %>% .[!is.na(wfh_wham)]

df_all_nz <- rbindlist(list(df_anz_stru_2014,df_anz_stru_2015,df_anz_stru_2016,df_anz_stru_2017,df_anz_stru_2018,df_anz_stru_2019,df_anz_stru_2020,df_anz_stru_2021,df_anz_stru_2022)) %>% filter(canon_country == "NZL") %>% setDT(.)

remove(list = setdiff(ls(),"df_all_nz"))

df_all_nz <- df_all_nz %>%
  setDT(.) %>%
  .[, year_quarter := as.yearqtr(job_ymd)] %>%
  .[, year_month := as.yearmon(job_ymd)]

# load weights
w_nz_2019 <- fread("../bg_combined//aux_data/emp_weights/w_nz_2019.csv") %>%
  .[, anzsco_code := as.numeric(str_sub(anzsco_code, 1, 4))] %>%
  .[, .(tot_emp = sum(as.numeric(tot_emp), na.rm = T)), by = anzsco_code] %>%
  .[, emp_share := tot_emp / sum(tot_emp, na.rm = T)]

head(w_nz_2019)

# merge in weights
nrow(df_all_nz) # 1,188,667
df_all_nz<-setDT(df_all_nz) %>%
  .[, anzsco_code := as.numeric(str_sub(anzsco_code, 1, 4))] %>%
  .[, country := "NZ"] %>%
  left_join(w_nz_2019) %>%
  setDT(.) %>%
  .[!is.na(emp_share) & !is.na(tot_emp)]
colnames(df_all_nz)

# Apportion employment across (weighted) vacancies
df_all_nz <- df_all_nz %>%
  setDT(.) %>%
  .[, job_id_weight := 1/.N, by = job_id] %>%
  setDT(.) %>%
  .[, tot_emp_ad := (job_id_weight*tot_emp)/sum(job_id_weight**tot_emp), by = .(year_month, anzsco_code)]

colnames(df_all_nz)
# Check how problematic the extensive margin is
#check_nz <- df_all_nz %>%
#  group_by(year_month) %>%
#  summarise(sum(tot_emp_ad))
#View(check_nz)

df_all_nz <- df_all_nz %>%
  select(job_id,country,year_month,job_date, narrow_result, neg_narrow_result, wfh_wham_prob,wfh_wham,canon_state,canon_city,
         canon_employer,min_experience,max_experience,canon_minimum_degree,min_degree_level,canon_job_hours,min_annual_salary,bgt_occ,anzsic_code,anzsic_class,anzsic_group,anzsic_subdivision,anzsic_division,
         tot_emp_ad,job_id_weight, job_domain, job_url)

# Remove Cannon
colnames(df_all_nz) <- gsub("canon_","", colnames(df_all_nz))

# Experience - call this disjoint_exp_max, and disjoint_degree_level
df_all_nz <- df_all_nz %>% rename(disjoint_exp_max = max_experience, disjoint_exp_min = min_experience)

# Min Degree - call this disjoint_degree_name, and disjoint_degree_level
df_all_nz <- df_all_nz %>% rename(disjoint_degree_level = min_degree_level, disjoint_degree_name = minimum_degree)

# Go back and check if other vars

# Need to cluster properly, for now let's just make them all "disjoint_sector"
df_all_nz <- df_all_nz %>% rename(disjoint_sector = anzsic_division)

# Min Salary (call this disjoint_salary")
df_all_nz <- df_all_nz %>% rename(disjoint_salary = min_annual_salary)

# Final Subset
df_all_nz <- df_all_nz %>% select(job_id, country, state, city, year_month, job_date, narrow_result, neg_narrow_result, wfh_wham_prob,wfh_wham, employer, bgt_occ, disjoint_exp_max, disjoint_exp_min, job_hours, disjoint_sector, disjoint_degree_level, disjoint_degree_name, disjoint_salary, tot_emp_ad, job_id_weight, job_domain, job_url)
df_all_nz$year <- year(df_all_nz$year_month)
df_all_nz$month <- str_sub(as.character(df_all_nz$year_month), 1, 3)
df_all_nz$month <- factor(df_all_nz$month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))

df_all_nz$disjoint_salary <- as.numeric(df_all_nz$disjoint_salary)
df_all_nz$disjoint_salary <- ifelse(df_all_nz$disjoint_salary<0, NA, df_all_nz$disjoint_salary)

df_all_nz <- setDT(df_all_nz)

# Cluster education
table(df_all_nz$disjoint_degree_name)
df_all_nz$bach_or_higher<-grepl("bachelor|master|doctor|PhD", df_all_nz$disjoint_degree_name, ignore.case = T)

df_all_nz$bach_or_higher<-ifelse(df_all_nz$bach_or_higher==FALSE & 
                                   df_all_nz$disjoint_degree_level >=16 & ! is.na(df_all_nz$disjoint_degree_level), 
                                 TRUE, df_all_nz$bach_or_higher)

df_all_nz$bach_or_higher<-ifelse(is.na(df_all_nz$disjoint_degree_level) & df_all_nz$disjoint_degree_name == "",
                                 NA, df_all_nz$bach_or_higher)
df_all_nz$bach_or_higher<-ifelse(df_all_nz$bach_or_higher == TRUE, 1, ifelse(df_all_nz$bach_or_higher == FALSE, 0, NA))

# Cluster industry
df_all_nz$sector_clustered<-ifelse(df_all_nz$disjoint_sector %in% c("Wholesale Trade", "Retail Trade", "WHOLESALE AND RETAIL TRADE; REPAIR OF MOTOR VEHICLES AND MOTORCYCLES"), "Wholesale and Retail Trade",
                                   ifelse(df_all_nz$disjoint_sector %in% c("Accommodation and Food Services","ACCOMMODATION AND FOOD SERVICE ACTIVITIES"),"Accomodation and Food Services",
                                          ifelse(df_all_nz$disjoint_sector %in% c("Electricity, Gas, Water and Waste Services","WATER SUPPLY; SEWERAGE, WASTE MANAGEMENT AND REMEDIATION ACTIVITIES", "Utilities", "ELECTRICITY, GAS, STEAM AND AIR CONDITIONING SUPPLY"), "Utility Services",
                                                 ifelse(df_all_nz$disjoint_sector %in% c("Administrative and Support Services","Administrative and Support and Waste Management and Remediation Services","ADMINISTRATIVE AND SUPPORT SERVICE ACTIVITIES"),"Administrative and Support",
                                                        ifelse(df_all_nz$disjoint_sector %in% c("Professional, Scientific and Technical Services","Professional, Scientific, and Technical Services", "PROFESSIONAL, SCIENTIFIC AND TECHNICAL ACTIVITIES"),"Technical Services",
                                                               ifelse(df_all_nz$disjoint_sector %in% c("Other Services", "Other Services (except Public Administration)", "OTHER SERVICE ACTIVITIES"), "Other Services",
                                                                      ifelse(df_all_nz$disjoint_sector %in% c("Education and Training", "Educational Services", "EDUCATION"), "Education",
                                                                             ifelse(df_all_nz$disjoint_sector %in% c("Health Care and Social Assistance", "HUMAN HEALTH AND SOCIAL WORK ACTIVITIES"), "Healthcare",
                                                                                    ifelse(df_all_nz$disjoint_sector %in% c("Public Administration and Safety", "Public Administration", "PUBLIC ADMINISTRATION AND DEFENCE; COMPULSORY SOCIAL SECURITY"), "Public Administration",
                                                                                           ifelse(df_all_nz$disjoint_sector %in% c("Financial and Insurance Services", "Financial and Insurance", "FINANCIAL AND INSURANCE ACTIVITIES"), "Finance and Insurance",
                                                                                                  ifelse(df_all_nz$disjoint_sector %in% c("Information Media and Telecommunications", "Information", "INFORMATION AND COMMUNICATION"), "Information and Communication",
                                                                                                         ifelse(df_all_nz$disjoint_sector %in% c("Manufacturing", "MANUFACTURING"), "Manufacturing",
                                                                                                                ifelse(df_all_nz$disjoint_sector %in% c("Construction", "CONSTRUCTION"), "Construction",
                                                                                                                       ifelse(df_all_nz$disjoint_sector %in% c("Rental, Hiring and Real Estate Services","Real Estate and Rental and Leasing",  "REAL ESTATE ACTIVITIES"), "Real Estate",
                                                                                                                              ifelse(df_all_nz$disjoint_sector %in% c("Agriculture, Forestry and Fishing",  "Agriculture, Forestry, Fishing and Hunting","AGRICULTURE, FORESTRY AND FISHING"),"Agriculture",
                                                                                                                                     ifelse(df_all_nz$disjoint_sector %in% c("Transport, Postal and Warehousing", "Transportation and Warehousing","TRANSPORTATION AND STORAGE"), "Transportation",
                                                                                                                                            ifelse(df_all_nz$disjoint_sector %in% c("Arts and Recreation Services","Arts, Entertainment, and Recreation",  "ARTS, ENTERTAINMENT AND RECREATION"), "Arts and Entertainment",
                                                                                                                                                   ifelse(df_all_nz$disjoint_sector %in% c("Mining", "Mining, Quarrying, and Oil and Gas Extraction","MINING AND QUARRYING"), "Mining", NA))))))))))))))))))

# CLUSTER EXP #
df_all_nz <- df_all_nz %>%
  .[, disjoint_exp_min := ifelse(disjoint_exp_min == -999, NA, disjoint_exp_min)] %>%
  .[, disjoint_exp_max := ifelse(disjoint_exp_max == -999, NA, disjoint_exp_max)]

df_all_nz <- df_all_nz %>%
  .[, exp_max := disjoint_exp_max] %>%
  select(-c(disjoint_exp_min, disjoint_exp_max))

# ARRANGE #
df_all_nz <- df_all_nz %>%
  setDT(.) %>%
  .[order(job_date, job_id)]

head(df_all_nz)

# SAVE #
fwrite(df_all_nz, file = "./int_data/df_nz_standardised.csv")
#### END NEW ZEALAND ####

