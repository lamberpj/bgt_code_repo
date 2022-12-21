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
#quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg-us/")
#setDTthreads(1)
remove(list = ls())
#### end ####

#### PREPARE DATA ####
#system("gsutil -m cp -r gs://for_transfer/data_ingest/bgt_upload/US_stru /mnt/disks/pdisk/bg-us/raw_data/main/")
#system("gsutil -m cp -r gs://for_transfer/data_ingest/bgt_upload/US_raw /mnt/disks/pdisk/bg-us/raw_data/text/")
#system("gsutil -m cp -r gs://for_transfer/wham/US /mnt/disks/pdisk/bg-us/int_data/wham_pred/")

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

#system("zip -r /mnt/disks/pdisk/bg-us/int_data/us_sequences.zip /mnt/disks/pdisk/bg-us/int_data/sequences/")

# Download WHAM Predictions
#system("gsutil -m cp -r gs://for_transfer/wham/US /mnt/disks/pdisk/bg-us/int_data/wham_pred")

#### END ####

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

#### READ XML AND MAKE SEQUENCES ####
paths
source("/mnt/disks/pdisk/bgt_code_repo/old/safe_mclapply.R")

safe_mclapply(1:length(paths), function(i) {

  name <- str_sub(paths[i], -21, -5)
  name
  warning(paste0("\nBEGIN: ",i,"  '",name,"'"))
  cat(paste0("\nBEGIN: ",i,"  '",name,"'"))
  system(paste0("unzip -n ",paths[i]," -d ./raw_data/text/"))
  xml_path = gsub(".zip", ".xml", paths[i])
  xml_path
  #try( {
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
    
    df_ads <- df_ads %>% .[, group := as.character(.I %/% 50000)]
    df_ads <- split(df_ads, by = "group")
    
    # Sequence Tokeniser
    df_chunked <- safe_mclapply(1:length(df_ads), function(j) {
      
      warning(paste0("\nSTART: ",100*round(j/23, 2)))
      
      df_chunked <- df_ads[[j]] %>%
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
      
      warning(paste0("\nDONE: ",100*round(j/23, 2)))
      
      return(df_chunked)
      
    }, mc.cores = 8)
    
    df_chunked <- rbindlist(df_chunked)
    
    df_chunked <- setDT(bind_rows(df_chunked, df_title)) %>% .[order(seq_id)]
    
    saveRDS(df_chunked, file = paste0("./int_data/sequences/sequences_",name,".rds"))
  #})
  
  unlink(xml_path)
  
  warning(paste0("SUCCESS: ",i))
  cat(paste0("\nSUCCESS: ",i,"\n"))
  return("")
}, mc.cores = 1)

#sink()
system("echo sci2007! | sudo -S shutdown -h now")
#### END ####

#### EXTRACT SOURCE AND URL AND SAVE ####
# remove(list = ls())
# paths <- list.files("./raw_data/text/", pattern = "*.zip", full.names = T)
# source("/mnt/disks/pdisk/bgt_code_repo/old/safe_mclapply.R")
# 
# lapply(2014:2022, function(year) {
#   paths <- paths[grepl(paste0(year), paths)]
#   df <- safe_mclapply(1:length(paths), function(i) {
#     name <- str_sub(paths[i], -21, -5)
#     name
#     warning(paste0("\nBEGIN: ",i,"  '",name,"'"))
#     cat(paste0("\nBEGIN: ",i,"  '",name,"'"))
#     system(paste0("unzip -o ",paths[i]," -d ./raw_data/text/"))
#     xml_path = gsub(".zip", ".xml", paths[i])
#     xml_path
#     #try( {
#     df_xml <- read_xml(xml_path) %>%
#       xml_find_all(., ".//Job")
#     
#     df_job_id <- xml_find_all(df_xml, ".//JobID") %>% xml_text
#     df_job_url <- xml_find_all(df_xml, ".//JobURL") %>% xml_text
#     df_job_domain <- xml_find_all(df_xml, ".//JobDomain") %>% xml_text
#     
#     remove("df_xml")
#     
#     df <- data.table(job_id = df_job_id, job_domain = df_job_domain, job_url = df_job_url, stringsAsFactors = FALSE)
#     head(df)
#     unlink(xml_path)
#     
#     warning(paste0("SUCCESS: ",i))
#     cat(paste0("\nSUCCESS: ",i,"\n"))
#     return(df)
#   }, mc.cores = 4)
#   df <- rbindlist(df)
#   fwrite(df, file = paste0("./int_data/sources/us_src_",year,"_wfh.csv"))
# })
# 
# #sink()
# system("echo sci2007! | sudo -S shutdown -h now")
#### /END ####

#### AGGREGATE WHAM TO JOB AD LEVEL ####
remove(list = ls())
paths <- list.files("./int_data/wham_pred", pattern = "*.txt", full.names = T)
paths <- paths[grepl("2014|2015|2016|2017|2018|2019|2020|2021|2022", paths)]
paths
source("/mnt/disks/pdisk/bgt_code_repo/old/safe_mclapply.R")

df_wham <- safe_mclapply(1:length(paths), function(i) {
  df <- fread(paths[i])  %>%
    .[, job_id := str_sub(seq_id,1, -6)] %>%
    .[, .(wfh_prob = max(wfh_prob)), by = job_id]
  warning(paste0("\nDONE: ",i/length(paths)))
  return(df)
}, mc.cores = 8)

df_wham <- rbindlist(df_wham)

df_wham_old <- fread("/mnt/disks/pdisk/bg_combined/int_data/subsample_wham/df_ss_wham.csv") %>%
  .[country == "US"] %>%
  .[year %in% c(2014:2018)]

df_wham <- df_wham %>%
  .[, job_id := as.numeric(job_id)]

df_wham_old <- df_wham_old %>%
  .[, job_id := as.numeric(job_id)]

df_wham <- bind_rows(df_wham_old, df_wham) %>% setDT(.)

rm(df_wham_old)

df_wham <- df_wham %>%
  .[, wfh := as.numeric(wfh_prob>0.5)] %>%
  .[, wfh_prob := round(wfh_prob, 3)]

df_wham <- df_wham %>%
  rename(wfh_wham_prob = wfh_prob,
         wfh_wham = wfh)

df_wham <- df_wham %>%
  unique(., by = "job_id")
#### /END ####

#### LOAD SRC ####
paths <- list.files("./int_data/sources/", pattern = "*.csv", full.names = T)
paths <- paths[grepl("2014|2015|2016|2017|2018|2019|2020|2021|2022", paths)]
source("/mnt/disks/pdisk/bgt_code_repo/old/safe_mclapply.R")

df_src <- safe_mclapply(1:length(paths), function(i) {
  df <- fread(paths[i])
  warning(paste0("\nDONE: ",i/length(paths)))
  return(df)
}, mc.cores = 1) %>%
  rbindlist(.)

df_src <- df_src %>%
  .[, job_id := as.numeric(job_id)]

nrow(df_src) # 265,912,726
df_src <- df_src %>%
  unique(., by = "job_id")
nrow(df_src) # 263,438,320

#### END ####
ls()
#### MERGE WHAM PREDICTIONS INTO THE STRUCTURED DATA AND RESAVE ####
remove(list = setdiff(ls(), c("df_wham", "df_src")))
paths <- list.files("/mnt/disks/pdisk/bg-us/raw_data/main", pattern = ".zip", full.names = T)
paths
source("/mnt/disks/pdisk/bgt_code_repo/old/safe_mclapply.R")

df_wham$job_id <- as.numeric(df_wham$job_id)
df_src$job_id <- as.numeric(df_src$job_id)

safe_mclapply(2014:2022, function(x) {

  paths_year <- paths[grepl(x, paths)]
  df_stru <- safe_mclapply(1:length(paths_year), function(i) {
    warning(paste0("\nSTART: ",i,"\n"))
    warning(paste0(paths_year[i]))
    df <- fread(cmd = paste0('unzip -p ', paths_year[i]), nThread = 8, colClasses = "character", stringsAsFactors = FALSE,
                select = c("BGTJobId", "JobDate", "SOC", "ONET", "BGTOcc", "Employer", "Sector", "SectorName",
                           "NAICS3", "NAICS4", "NAICS5", "NAICS6", "City", "State", "County", "FIPS", "MSA",
                           "Edu", "MaxEdu", "Degree", "MaxDegree", "MinSalary", "MaxSalary", "MinHrlySalary",
                           "MaxHrlySalary", "SalaryType", "JobHours", "Exp", "MaxExp")) %>%
      clean_names %>%
      setDT(.) %>%
      rename(job_id = bgt_job_id) %>%
      .[, job_id := as.numeric(job_id)] %>%
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
  ls()
  df_stru <- rbindlist(df_stru)
  df_stru <- setDT(df_stru)
  fwrite(df_stru, file = paste0("./int_data/us_stru_",x,"_wfh.csv"), nThread = 8)
  return("")
}, mc.cores = 1)

#### END ####

#### EXTRACT ANNUAL HARMONISED DATA, manually changing year ####
remove(list = ls())
#df_us_stru_2019 <- fread("../bg-us/int_data/us_stru_2019_wfh.csv", nThread = 8)
#df_us_stru_2020 <- fread("../bg-us/int_data/us_stru_2020_wfh.csv", nThread = 8)
#df_us_stru_2021 <- fread("../bg-us/int_data/us_stru_2021_wfh.csv", nThread = 8)
df_us_stru_2022 <- fread("../bg-us/int_data/us_stru_2022_wfh.csv", nThread = 8)

df_all_us <- df_us_stru_2022

remove(list = setdiff(ls(),"df_all_us"))

colnames(df_all_us)

View(as.data.table(table(as.yearmon(df_all_us[!is.na(wfh_wham) & wfh_wham != ""]$job_ymd))))

df_all_us <- df_all_us %>%
  .[, year_quarter := as.yearqtr(job_ymd)] %>%
  .[, year_month := as.yearmon(job_ymd)]

w_us_2019 <- fread("../bg_combined/aux_data/emp_weights/w_us_2019.csv") %>%
  .[, us_soc18 := str_sub(us_soc18, 1, 5)] %>%
  .[, .(tot_emp = sum(tot_emp, na.rm = T)), by = us_soc18] %>%
  .[, emp_share := tot_emp / sum(tot_emp, na.rm = T)]

soc10_soc18_xwalk <- readxl::read_xlsx("../bg_combined/aux_data/emp_weights/us_weights/soc_2010_to_2018_crosswalk.xlsx", skip = 8) %>%
  clean_names %>%
  select(x2010_soc_code, x2018_soc_code)

#soc10_soc18_xwalk

nrow(df_all_us) # 145,969,778
df_all_us <- df_all_us %>%
  merge(x = ., y = soc10_soc18_xwalk, by.x = "soc", by.y = "x2010_soc_code", all.x = TRUE, all.y = FALSE, allow.cartesian=TRUE)
nrow(df_all_us) # 188,713,932

#w_us_2019

df_all_us <- df_all_us %>%
  .[, us_soc18 := str_sub(x2018_soc_code, 1, 5)] %>%
  .[!is.na(us_soc18)] %>%
  .[, country := "US"] %>%
  merge(x = ., y = w_us_2019, by = "us_soc18", all.x = TRUE, all.y = FALSE) %>%
  setDT(.) %>%
  .[!is.na(emp_share) & !is.na(tot_emp)]

# Apportion employment across (weighted) vacancies
df_all_us <- df_all_us %>%
  setDT(.) %>%
  .[, job_id_weight := 1/.N, by = job_id] %>%
  setDT(.) %>%
  .[, tot_emp_ad := (tot_emp*job_id_weight)/sum(tot_emp*job_id_weight), by = .(year_month, us_soc18)]

# Check how problematic the extensive margin is
df_all_us <- df_all_us %>%
  select(job_id,country,year_month,job_date,wfh_wham_prob,wfh_wham,state,city,msa,fips,county,
         employer,exp, max_exp,degree,min_salary,job_hours,bgt_occ,
         onet,sector_name,tot_emp_ad,job_id_weight, job_url, job_domain)

# Remove Cannon
colnames(df_all_us) <- gsub("canon_","", colnames(df_all_us))

# Experience - call this disjoint_exp_max, and disjoint_degree_level
df_all_us <- df_all_us %>% rename(disjoint_exp_max = max_exp, disjoint_exp_min = exp)

# Min Degree - call this disjoint_degree_name, and disjoint_degree_level
df_all_us <- df_all_us %>% rename(disjoint_degree_name = degree)

# Need to cluster properly, for now let's just make them all "disjoint_sector"
df_all_us <- df_all_us %>% rename(disjoint_sector = sector_name)

# Min Salary (call this disjoint_salary")
df_all_us <- df_all_us %>% rename(disjoint_salary = min_salary)

# Final Subset
df_all_us <- df_all_us %>% select(job_id, country, state, msa, county, fips, city, year_month, job_date, wfh_wham_prob,wfh_wham,employer, bgt_occ, onet,  disjoint_exp_max, disjoint_exp_min, job_hours, disjoint_sector, disjoint_degree_name, disjoint_salary, tot_emp_ad, job_id_weight, job_url, job_domain)
df_all_us$year <- year(df_all_us$year_month)
df_all_us$month <- str_sub(as.character(df_all_us$year_month), 1, 3)
df_all_us$month <- factor(df_all_us$month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))

df_all_us$disjoint_salary <- as.numeric(df_all_us$disjoint_salary)
df_all_us$disjoint_salary <- ifelse(df_all_us$disjoint_salary<0, NA, df_all_us$disjoint_salary)

df_all_us <- setDT(df_all_us)

#### MZ CODE 
#table(df_all_us$disjoint_degree_name)
df_all_us$bach_or_higher<-grepl("bachelor|master|doctor|PhD", df_all_us$disjoint_degree_name, ignore.case = T)

# df_all_us$bach_or_higher<-ifelse(df_all_us$bach_or_higher==FALSE & 
#                                 df_all_us$disjoint_degree_level >=16 & ! is.na(df_all_us$disjoint_degree_level), 
#                               TRUE, df_all_us$bach_or_higher)

#df_all_us$bach_or_higher<-ifelse(is.na(df_all_us$disjoint_degree_level) & df_all_us$disjoint_degree_name == "",
#                              NA, df_all_us$bach_or_higher)
#df_all_us$bach_or_higher<-ifelse(df_all_us$bach_or_higher == TRUE, 1, ifelse(df_all_us$bach_or_higher == FALSE, 0, NA))

df_all_us$sector_clustered<-ifelse(df_all_us$disjoint_sector %in% c("Wholesale Trade", "Retail Trade", "WHOLESALE AND RETAIL TRADE; REPAIR OF MOTOR VEHICLES AND MOTORCYCLES"), "Wholesale and Retail Trade",
                                ifelse(df_all_us$disjoint_sector %in% c("Accommodation and Food Services","ACCOMMODATION AND FOOD SERVICE ACTIVITIES"),"Accomodation and Food Services",
                                       ifelse(df_all_us$disjoint_sector %in% c("Electricity, Gas, Water and Waste Services","WATER SUPPLY; SEWERAGE, WASTE MANAGEMENT AND REMEDIATION ACTIVITIES", "Utilities", "ELECTRICITY, GAS, STEAM AND AIR CONDITIONING SUPPLY"), "Utility Services",
                                              ifelse(df_all_us$disjoint_sector %in% c("Administrative and Support Services","Administrative and Support and Waste Management and Remediation Services","ADMINISTRATIVE AND SUPPORT SERVICE ACTIVITIES"),"Administrative and Support",
                                                     ifelse(df_all_us$disjoint_sector %in% c("Professional, Scientific and Technical Services","Professional, Scientific, and Technical Services", "PROFESSIONAL, SCIENTIFIC AND TECHNICAL ACTIVITIES"),"Technical Services",
                                                            ifelse(df_all_us$disjoint_sector %in% c("Other Services", "Other Services (except Public Administration)", "OTHER SERVICE ACTIVITIES"), "Other Services",
                                                                   ifelse(df_all_us$disjoint_sector %in% c("Education and Training", "Educational Services", "EDUCATION"), "Education",
                                                                          ifelse(df_all_us$disjoint_sector %in% c("Health Care and Social Assistance", "HUMAN HEALTH AND SOCIAL WORK ACTIVITIES"), "Healthcare",
                                                                                 ifelse(df_all_us$disjoint_sector %in% c("Public Administration and Safety", "Public Administration", "PUBLIC ADMINISTRATION AND DEFENCE; COMPULSORY SOCIAL SECURITY"), "Public Administration",
                                                                                        ifelse(df_all_us$disjoint_sector %in% c("Financial and Insurance Services", "Financial and Insurance", "FINANCIAL AND INSURANCE ACTIVITIES"), "Finance and Insurance",
                                                                                               ifelse(df_all_us$disjoint_sector %in% c("Information Media and Telecommunications", "Information", "INFORMATION AND COMMUNICATION"), "Information and Communication",
                                                                                                      ifelse(df_all_us$disjoint_sector %in% c("Manufacturing", "MANUFACTURING"), "Manufacturing",
                                                                                                             ifelse(df_all_us$disjoint_sector %in% c("Construction", "CONSTRUCTION"), "Construction",
                                                                                                                    ifelse(df_all_us$disjoint_sector %in% c("Rental, Hiring and Real Estate Services","Real Estate and Rental and Leasing",  "REAL ESTATE ACTIVITIES"), "Real Estate",
                                                                                                                           ifelse(df_all_us$disjoint_sector %in% c("Agriculture, Forestry and Fishing",  "Agriculture, Forestry, Fishing and Hunting","AGRICULTURE, FORESTRY AND FISHING"),"Agriculture",
                                                                                                                                  ifelse(df_all_us$disjoint_sector %in% c("Transport, Postal and Warehousing", "Transportation and Warehousing","TRANSPORTATION AND STORAGE"), "Transportation",
                                                                                                                                         ifelse(df_all_us$disjoint_sector %in% c("Arts and Recreation Services","Arts, Entertainment, and Recreation",  "ARTS, ENTERTAINMENT AND RECREATION"), "Arts and Entertainment",
                                                                                                                                                ifelse(df_all_us$disjoint_sector %in% c("Mining", "Mining, Quarrying, and Oil and Gas Extraction","MINING AND QUARRYING"), "Mining", NA))))))))))))))))))

# CLUSTER EXP #
df_all_us <- df_all_us %>%
  .[, disjoint_exp_min := ifelse(disjoint_exp_min == -999, NA, disjoint_exp_min)] %>%
  .[, disjoint_exp_max := ifelse(disjoint_exp_max == -999, NA, disjoint_exp_max)]

df_all_us <- df_all_us %>%
  .[, exp_max := disjoint_exp_max] %>%
  select(-c(disjoint_exp_min, disjoint_exp_max))

# ARRANGE #
df_all_us <- df_all_us %>%
  setDT(.) %>%
  .[order(job_date, job_id)]

# SAVE #
fwrite(df_all_us, file = "./int_data/df_us_2022_standardised.csv")










