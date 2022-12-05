#### SETUP ####
options(repos = c(CRAN = "https://mran.revolutionanalytics.com/snapshot/2021-04-01"))

options(scipen=999)

library("data.table")
library("tidyverse")
library("janitor")
library("lubridate")
library("doParallel")
#library("lsa")
#library("fuzzyjoin")
#library("quanteda")
#library("refinr")
#library("FactoMineR")
#library("ggpubr")
#library("scales")
#install.packages("ggthemr")
#ggthemr('flat')
library("fixest")
#library("lfe")
#library("stargazer")
#library("texreg")
#library("sjPlot")
#library("margins")
#library("DescTools")
#library("fuzzyjoin")

# Translation packages
#library("rvest")
#library("googleLanguageR")
#library("cld2")
#library("datasets")
#library("vroom")
#library("refinr")
#library(stringi)

library(ggplot2)
library(scales)
library("ggpubr")
devtools::install_github('Mikata-Project/ggthemr')
library(ggthemr)

#################################
# USA #
#################################

#### LOAD DATA ####
setwd("/mnt/disks/pdisk/bg-us/")
remove(list = ls())
file_names_p <- list.files("./raw_data/main", "*.txt", full.names = F)
file_names_p <- as.data.frame(file_names_p) %>% separate(file_names_p, sep = "_", c("type", "year_month"), remove = F)
file_names_p$file_names_p <- as.data.frame(paths <- list.files("./raw_data/main", "*.txt", full.names = T))$paths
file_names_p <- file_names_p %>% mutate(year = as.numeric(str_sub(year_month, 1, 4)))
file_names_p <- file_names_p %>% mutate(year_month = str_sub(year_month, 1, -5))
file_names_p <- file_names_p %>% rename(file_names = file_names_p)
file_names_p <- file_names_p %>% filter(year > 2013)
nrow(file_names_p)

file_names_s <- list.files("./raw_data/skills", "*.txt", full.names = F)
file_names_s <- as.data.frame(file_names_s) %>% separate(file_names_s, sep = "_", c("type", "year_month"), remove = F)
file_names_s$file_names_s <- as.data.frame(paths <- list.files("./raw_data/skills", "*.txt", full.names = T))$paths
file_names_s <- file_names_s %>% mutate(year = as.numeric(str_sub(year_month, 1, 4)))
file_names_s <- file_names_s %>% mutate(year_month = str_sub(year_month, 1, -5))
file_names_s <- file_names_s %>% rename(file_names = file_names_s)
file_names_s <- file_names_s %>% filter(year > 2013)
nrow(file_names_s)

file_names <- bind_rows(file_names_p, file_names_s)

remove("file_names_s")
remove("file_names_p")

file_names <- file_names %>% arrange(year_month, type)

file_names <- file_names %>% group_by(year_month) %>% mutate(n = n())

length(file_names$file_names[file_names$type == "Main"])
length(file_names$file_names[file_names$type == "Skills"])

m <- nrow(file_names)/2

# View(fread(file_names$file_names[file_names$type == "postings" & file_names$country == countries[[i]]][1], nrow = 10))

sc_df <- fread("./aux_data/US_Deming.csv") %>% select(-V1) %>% rename(skill = skill) %>%
  select(skill,social,business_analysis,cog,noncog,mgmt,service,computer,admin_support,tech_support,gen_soft,products,project_mgmt,creativ,bus_sys,data,database,data_analysis,ML_AI) %>%
  mutate(skill = toupper(str_trim(str_squish(skill)))) %>%
  distinct(skill, .keep_all = T)



#### COMPILE INCOME DISTRIBUTIONS ####
#### compile data ####
setMKLthreads(1)

df <- safe_mclapply(1:m, function(i) {
  
  y1 <- fread(file_names$file_names[file_names$type == "Main"][i], data.table = F, nThread = 1, colClasses = "character", stringsAsFactors = FALSE,
              select = c("BGTJobId", "JobDate", "SOC", "SOCName", "Edu", "Degree", "Exp", "Employer", "MSA", "State", "Sector", "JobHours")) %>%
    clean_names
  
  y1 <- y1 %>% mutate_if(is.character, list(~na_if(.,"")))
  
  y1 <- y1 %>% filter(!is.na(msa) & !is.na(employer) & !is.na(soc))
  
  y1 <- y1 %>% mutate_if(is.character, list(~na_if(.,""))) 
  y1 <- y1 %>% mutate_if(is.character, list(~na_if(.,"na"))) 
  y1 <- y1 %>% mutate_if(is.character, list(~na_if(.,"-999")))
  
  y1 <- y1 %>%
    mutate(companyname_clean = str_replace_all(employer, "\\(.*\\)", "")) %>%
    mutate(companyname_clean = str_replace_all(companyname_clean, ",", "")) %>%
    mutate(companyname_clean = str_replace_all(companyname_clean, "\\.", "")) %>%
    mutate(companyname_clean = str_replace_all(companyname_clean, "&", " AND ")) %>%
    mutate(companyname_clean = str_replace_all(companyname_clean, "'", "")) %>%
    mutate(companyname_clean = str_replace_all(companyname_clean, '[[:punct:] ]+', ' ')) %>%
    mutate(companyname_clean = toupper(companyname_clean)) %>%
    mutate(companyname_clean = str_squish(companyname_clean)) %>%
    mutate(companyname_clean = str_trim(companyname_clean)) %>%
    mutate(companyname_clean = str_replace_all(companyname_clean, "[^[:alnum:][:space:]]", "")) %>%
    mutate(companyname_clean = str_trim(companyname_clean))
  
  y1 <- y1 %>% filter(!is.na(companyname_clean) & companyname_clean != "")
  
  y1 <- y1 %>%
    mutate(grab_date = ymd(job_date)) %>%
    mutate(year_month = paste0(year(grab_date),".", sprintf("%02d", month(grab_date)))) %>%
    select(-c(job_date)) %>%
    distinct(bgt_job_id, .keep_all = T)
  
  #y1 <- y1 %>%
  #  mutate(companyname_clean_merged = n_gram_merge(y1$companyname_clean[!is.na(y1$companyname_clean)],
  #                                                 numgram = 2,
  #                                                 bus_suffix = TRUE,
  #                                                 edit_threshold = 1,
  #                                                 weight = c(d = 0.33, i = 0.33, s = 1, t = 0.5)))
  
  y2 <- fread(file_names$file_names[file_names$type == "Skills"][i], data.table = F, nThread = 1, colClasses = "character", stringsAsFactors = FALSE, select = c("BGTJobId", "Skill")) %>%
    clean_names %>%
    select(bgt_job_id, skill)
  
  y2 <- y2 %>% filter(bgt_job_id %in% y1$bgt_job_id)
  y1 <- y1 %>% filter(bgt_job_id %in% y2$bgt_job_id)
  
  y1 <- y1 %>% mutate_if(is.character, list(~na_if(.,""))) 
  y2 <- y2 %>% mutate_if(is.character, list(~na_if(.,"")))
  
  y2 <- y2 %>% mutate_if(is.character, list(~na_if(.,""))) 
  y2 <- y2 %>% mutate_if(is.character, list(~na_if(.,"na"))) 
  y2 <- y2 %>% mutate_if(is.character, list(~na_if(.,"-999")))
  
  y2 <- y2 %>%
    filter(!is.na(skill)) %>%
    mutate(skill = toupper(str_trim(str_squish(skill))))
  
  y2 <- y2 %>%
    left_join(sc_df)
  
  y2 <- y2 %>%
    select(-skill)
  
  y2[is.na(y2)] <- 0
  
  y2 <- y2 %>%
    group_by(bgt_job_id) %>%
    summarise(across(everything(), max))
  
  y1 <- y1 %>% distinct(bgt_job_id, .keep_all = T)
  y2 <- y2 %>% distinct(bgt_job_id, .keep_all = T)
  
  y1 <- y1 %>%
    left_join(y2)
  
  y1 <- y1 %>% distinct(bgt_job_id, .keep_all = T)
  
  remove(y2)
  saveRDS(y1, file = paste0("./int_data/temp/file",i,".rds"), compress = F)
  remove(y1)
  
}, mc.cores = 60)


remove(df)
files <- list.files("./int_data/temp/", "*.rds", full.names = F)
files <- gsub("file", "", files)
files <- gsub(".rds", "", files)
files <- sort(as.numeric(files))
files <- setdiff(1:m, files)
files
files <- list.files("./int_data/temp/", "*.rds", full.names = T)

df <- safe_mclapply(1:length(files), function(i){
  x <- readRDS(files[[i]])
  warning(paste0("\n\n####### ",i," = ",nrow(x)," ########\n\n"))
  return(x)
  
}, mc.cores = 32)

print("done")

df <- df %>% bind_rows

# SOC Lookup
# soc_df <- df %>% select(soc, soc_name) %>% distinct(.) %>% ungroup()
# df_bls <- readRDS("./aux_data/bls_scrape/bls_static_soc_educ_dist.rds") %>% rename(soc = code)
# df_bls <- df_bls %>%
#   mutate(college_share_bls = bachelors_degree + masters_degree + doctoral_or_professional_degree) %>%
#   select(soc, college_share_bls) %>%
#   mutate(college_share_bls = college_share_bls / 100)
# soc_df <- soc_df %>%
#   left_join(df_bls)
# 
# # Compare BG and BLS
# df_bgt_bls <- df %>%
#   mutate(edu = as.numeric(edu)) %>%
#   mutate(year = str_sub(year_month, 1, 4)) %>%
#   filter(year == "2019") %>%
#   group_by(soc) %>%
#   summarise(college_share_bgt = sum(edu >= 16, na.rm = T) / n(),
#             no_college_share_bgt = sum(edu == 0 | edu == 12, na.rm = T) / n(),
#             na_share_bgt = sum(is.na(edu)) / n()) %>%
#   left_join(soc_df)
# 
# df_bgt_bls <- df_bgt_bls %>% mutate(college_share_diff = college_share_bls - college_share_bgt) %>%
#   select(soc, soc_name, college_share_bgt, no_college_share_bgt, na_share_bgt, college_share_bls, college_share_diff)
# 
# nrow(df_bgt_bls) # # 832

# Select Occupations from BLS

df <- df %>% 
  mutate(drop = case_when(
    str_sub(soc, 1, 2) == "55" ~ 1,      # Military
    str_sub(soc, 1, 2) == "29" ~ 1,      # Healthcare
    str_sub(soc, 1, 5) == "23-10" ~ 1,   # Lawyers and Judges
    str_sub(soc, 1, 5) == "25-10" ~ 1,   # Postsecondary
    str_sub(soc, 1, 5) == "25-11" ~ 1,   # Postsecondary (continued)
    str_sub(soc, 1, 6) == "25-202" ~ 1,  # Elementary School Teachers
    str_sub(soc, 1, 6) == "25-203" ~ 1,  # Secondary School Teachers
    str_sub(soc, 1, 5) == "19-20" ~ 1,   # Physical Scientists
    TRUE ~ 0))

#df <- df %>%
#  left_join(df_bgt_bls %>% select(soc, college_share_bls, drop))

df <- df %>% 
  mutate(edu = as.numeric(edu)) %>%
  mutate(year = str_sub(year_month, 1, 4)) %>%
  mutate(month = str_sub(year_month, -2, -1)) %>%
  mutate(bach_or_higher = ifelse(edu >= 16, 1, 0))

df <- df %>% 
  mutate(bach_or_higher = ifelse(is.na(edu), 0, bach_or_higher))

df <- df %>% 
  mutate(later_quarters = case_when(
    year_month %in% c("2019.01", "2019.02", "2019.03") ~ "2019Q1",
    year_month %in% c("2019.04", "2019.05", "2019.06") ~ "2019Q2",
    year_month %in% c("2019.07", "2019.08", "2019.09") ~ "2019Q3",
    year_month %in% c("2019.10", "2019.11", "2019.12") ~ "2019Q4",
    year_month %in% c("2020.01", "2020.02", "2020.03") ~ "2020Q1",
    year_month %in% c("2020.04", "2020.05", "2020.06") ~ "2020Q2",
    year_month %in% c("2020.07", "2020.08", "2020.09") ~ "2020Q3",
    year_month %in% c("2020.10", "2020.11", "2020.12") ~ "2020Q4",
    year_month %in% c("2021.01", "2021.02", "2021.03") ~ "2021Q1",
    year_month %in% c("2021.04", "2021.05", "2021.06") ~ "2021Q2",
    TRUE ~ "1pre"
  ))

n_distinct(df$companyname_clean) # 3,240,979
print("done")

# At least 5 postings 
df_firm_list_3yr <- df %>%
  select(companyname_clean, soc, year_month) %>%
  mutate(year = str_sub(year_month, 1, 4)) %>%
  filter(year %in% c("2018","2019","2020")) %>%
  select(-year_month) %>%
  group_by(companyname_clean, soc, year) %>%
  summarise(n = n()) %>%
  group_by(companyname_clean, year) %>%
  summarise(n = sum(n),
            n_occ = n()) %>%
  ungroup()

df_firm_list_3yr <- df_firm_list_3yr %>%
  group_by(companyname_clean) %>%
  mutate(above_2_n_all = ifelse(any(n<2),0,1),
         above_5_n_all = ifelse(any(n<5),0,1),
         above_10_n_all = ifelse(any(n<10),0,1),
         above_20_n_all = ifelse(any(n<20),0,1),
         above_50_n_all = ifelse(any(n<50),0,1),
         a1_soc_all = ifelse(any(n_occ<1),0,1),
         a2_soc_all = ifelse(any(n_occ<2),0,1),
         a5_soc_all = ifelse(any(n_occ<5),0,1),
         a10_soc_all = ifelse(any(n_occ<10),0,1)) %>%
  ungroup()


#df_firm_list_2020 <- df %>%
#  select(companyname_clean, soc, year_month) %>%
#  mutate(year = str_sub(year_month, 1, 4)) %>%
#  filter(year %in% c("2020")) %>%
#  select(-year_month) %>%
#  group_by(companyname_clean, soc, year) %>%
#  summarise(n = n()) %>%
#  group_by(companyname_clean, year) %>%
#  summarise(n = sum(n),
#            n_occ = n()) %>%
#  ungroup()
#
#df_firm_list_2020 <- df_firm_list_2020 %>%
#  group_by(companyname_clean) %>%
#  mutate(above_5_n_all = ifelse(any(n<5),0,1),
#         above_10_n_all = ifelse(any(n<10),0,1),
#         above_20_n_all = ifelse(any(n<20),0,1),
#         above_50_n_all = ifelse(any(n<50),0,1),
#         a1_soc_all = ifelse(any(n_occ<1),0,1),
#         a2_soc_all = ifelse(any(n_occ<2),0,1),
#         a5_soc_all = ifelse(any(n_occ<5),0,1),
#         a10_soc_all = ifelse(any(n_occ<10),0,1)) %>%
#  ungroup()

# trend <- data.frame("year_month" = c("2014.01", "2014.02", "2014.03", "2014.04", "2014.05", "2014.06", "2014.07", "2014.08", "2014.09", "2014.10", "2014.11", "2014.12",
#                                     "2015.01", "2015.02", "2015.03", "2015.04", "2015.05", "2015.06", "2015.07", "2015.08", "2015.09", "2015.10", "2015.11", "2015.12",
#                                     "2016.01", "2016.02", "2016.03", "2016.04", "2016.05", "2016.06", "2016.07", "2016.08", "2016.09", "2016.10", "2016.11", "2016.12",
#                                     "2017.01", "2017.02", "2017.03", "2017.04", "2017.05", "2017.06", "2017.07", "2017.08", "2017.09", "2017.10", "2017.11", "2017.12",
#                                     "2018.01", "2018.02", "2018.03", "2018.04", "2018.05", "2018.06", "2018.07", "2018.08", "2018.09", "2018.10", "2018.11", "2018.12",
#                                     "2019.01", "2019.02", "2019.03", "2019.04", "2019.05", "2019.06", "2019.07", "2019.08", "2019.09", "2019.10", "2019.11", "2019.12",
#                                     "2020.01", "2020.02", "2020.03", "2020.04", "2020.05", "2020.06", "2020.07", "2020.08", "2020.09", "2020.10", "2020.11", "2020.12",
#                                     "2021.01", "2021.02", "2021.03", "2021.04", "2021.05", "2021.06", "2021.07", "2021.08", "2021.09", "2021.10", "2021.11", "2021.12"),
#                    "month_trend" = c(1:96))
# df <- df %>%
#  left_join(trend)

df <- df %>% as_tibble

nrow(df)
#### /end ####
remove(list = ls())
#### REGRESSIONS FOR BACHELOR OR HIGHER ####
#save.image(file = "./usa_bgt_image2.RData")
"test"
load(file = "./usa_bgt_image2.RData")

head(df)
# All
regs <- safe_mclapply(1:5, function(i) {
  if(i == 1) {lm <- feols(bach_or_higher ~ later_quarters | month, df %>% filter(drop == 0), cluster = ~ year_month, lean = T)}
  if(i == 2) {lm <- feols(bach_or_higher ~ later_quarters | month + soc, df %>% filter(drop == 0), cluster = ~ year_month, lean = T)}
  if(i == 3) {lm <- feols(bach_or_higher ~ later_quarters | month + msa, df %>% filter(drop == 0), cluster = ~ year_month, lean = T)}
  if(i == 4) {lm <- feols(bach_or_higher ~ later_quarters | month + msa + soc, df %>% filter(drop == 0), cluster = ~ year_month, lean = T)}
  if(i == 5) {lm <- feols(bach_or_higher ~ later_quarters | month + companyname_clean, df %>% filter(drop == 0 & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  warning("\n\n############# ",i," #############\n\n")
  return(lm)
}, mc.cores = 5)

lapply(regs, summary)
saveRDS(regs, "./col_share_bgt_regs.rds")
remove(regs)

#### SKILL CLUSTER COVID ####
regs <- safe_mclapply(1:44, function(i) {
  if(i == 1) {lm <- feols(social ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 2) {lm <- feols(social ~ later_quarters | month + msa, df, cluster = ~ year_month, lean = T)}
  if(i == 3) {lm <- feols(social ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 4) {lm <- feols(business_analysis ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 5) {lm <- feols(business_analysis ~ later_quarters | month + msa, df, cluster = ~ year_month, lean = T)}
  if(i == 6) {lm <- feols(business_analysis ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 7) {lm <- feols(cog ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 8) {lm <- feols(cog ~ later_quarters | month + msa, df, cluster = ~ year_month, lean = T)}
  if(i == 9) {lm <- feols(cog ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 10) {lm <- feols(noncog ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 11) {lm <- feols(noncog ~ later_quarters | month + msa, df, cluster = ~ year_month, lean = T)}
  if(i == 12) {lm <- feols(noncog ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 13) {lm <- feols(mgmt ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 14) {lm <- feols(mgmt ~ later_quarters | month + msa, df, cluster = ~ year_month, lean = T)}
  if(i == 15) {lm <- feols(mgmt ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 16) {lm <- feols(service ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 17) {lm <- feols(service ~ later_quarters | month + msa, df, cluster = ~ year_month, lean = T)}
  if(i == 18) {lm <- feols(service ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 19) {lm <- feols(computer ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 20) {lm <- feols(computer ~ later_quarters | month + msa, df, cluster = ~ year_month, lean = T)}
  if(i == 21) {lm <- feols(computer ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 22) {lm <- feols(admin_support ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 23) {lm <- feols(admin_support ~ later_quarters | month + msa, df, cluster = ~ year_month, lean = T)}
  if(i == 24) {lm <- feols(admin_support ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 25) {lm <- feols(tech_support ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 26) {lm <- feols(tech_support ~ later_quarters | month + msa, df, cluster = ~ year_month, lean = T)}
  if(i == 27) {lm <- feols(tech_support ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 28) {lm <- feols(gen_soft ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 29) {lm <- feols(gen_soft ~ later_quarters | month + msa, df, cluster = ~ year_month, lean = T)}
  if(i == 20) {lm <- feols(gen_soft ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 21) {lm <- feols(products ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 22) {lm <- feols(products ~ later_quarters | month + msa, df, cluster = ~ year_month, lean = T)}
  if(i == 23) {lm <- feols(products ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 24) {lm <- feols(project_mgmt ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 25) {lm <- feols(project_mgmt ~ later_quarters | month + msa, df, cluster = ~ year_month, lean = T)}
  if(i == 26) {lm <- feols(project_mgmt ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 27) {lm <- feols(creativ ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 28) {lm <- feols(creativ ~ later_quarters | month + msa, df, cluster = ~ year_month, lean = T)}
  if(i == 29) {lm <- feols(creativ ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 30) {lm <- feols(bus_sys ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 31) {lm <- feols(bus_sys ~ later_quarters | month + msa, df, cluster = ~ year_month, lean = T)}
  if(i == 32) {lm <- feols(bus_sys ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 33) {lm <- feols(data ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 34) {lm <- feols(data ~ later_quarters | month + msa, df, cluster = ~ year_month, lean = T)}
  if(i == 35) {lm <- feols(data ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 36) {lm <- feols(database ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 37) {lm <- feols(database ~ later_quarters | month + msa, df, cluster = ~ year_month, lean = T)}
  if(i == 38) {lm <- feols(database ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 39) {lm <- feols(data_analysis ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 40) {lm <- feols(data_analysis ~ later_quarters | month + msa, df, cluster = ~ year_month, lean = T)}
  if(i == 41) {lm <- feols(data_analysis ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 42) {lm <- feols(ML_AI ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 43) {lm <- feols(ML_AI ~ later_quarters | month + msa, df, cluster = ~ year_month, lean = T)}
  if(i == 44) {lm <- feols(ML_AI ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  
  warning("\n\n############# ",i," #############\n\n")
  return(lm)
}, mc.cores = 3)

lapply(regs[(c(1:13)-1)*3 + 3], summary)
saveRDS(regs, "./sc_bgt_regs.rds")
remove(regs)

#### LONG RUN ####
head(df)

df <- df %>%
  mutate(lr_trend = case_when(
    year == "2014" ~ 1,
    year == "2014" ~ 2,
    year == "2014" ~ 3,
    year == "2014" ~ 4,
    year == "2019" ~ 5,
    TRUE ~ 0))

regs <- safe_mclapply(1:5, function(i) {
  if(i == 1) {lm <- feols(bach_or_higher ~ lr_trend, df %>% filter(drop == 0 & year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 2) {lm <- feols(bach_or_higher ~ lr_trend | soc, df %>% filter(drop == 0 & year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 3) {lm <- feols(bach_or_higher ~ lr_trend | msa, df %>% filter(drop == 0 & year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 4) {lm <- feols(bach_or_higher ~ lr_trend | msa + soc, df %>% filter(drop == 0 & year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 5) {lm <- feols(bach_or_higher ~ lr_trend | companyname_clean, df %>% filter(drop == 0 & year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  warning("\n\n############# ",i," #############\n\n")
  return(lm)
}, mc.cores = 3)

lapply(regs, summary)
saveRDS(regs, "./col_share_lr_bgt_regs.rds")
remove(regs)

#### SKILL CLUSTER COVID ####
regs <- safe_mclapply(1:39, function(i) {
  if(i == 1) {lm <- feols(social ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 2) {lm <- feols(social ~ lr_trend | month + msa, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 3) {lm <- feols(social ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 4) {lm <- feols(business_analysis ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 5) {lm <- feols(business_analysis ~ lr_trend | month + msa, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 6) {lm <- feols(business_analysis ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 7) {lm <- feols(cog ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 8) {lm <- feols(cog ~ lr_trend | month + msa, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 9) {lm <- feols(cog ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 10) {lm <- feols(noncog ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 11) {lm <- feols(noncog ~ lr_trend | month + msa, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 12) {lm <- feols(noncog ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 13) {lm <- feols(mgmt ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 14) {lm <- feols(mgmt ~ lr_trend | month + msa, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 15) {lm <- feols(mgmt ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 16) {lm <- feols(service ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 17) {lm <- feols(service ~ lr_trend | month + msa, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 18) {lm <- feols(service ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 19) {lm <- feols(computer ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 20) {lm <- feols(computer ~ lr_trend | month + msa, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 21) {lm <- feols(computer ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 22) {lm <- feols(admin_support ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 23) {lm <- feols(admin_support ~ lr_trend | month + msa, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 24) {lm <- feols(admin_support ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 25) {lm <- feols(tech_support ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 26) {lm <- feols(tech_support ~ lr_trend | month + msa, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 27) {lm <- feols(tech_support ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 28) {lm <- feols(gen_soft ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 29) {lm <- feols(gen_soft ~ lr_trend | month + msa, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 20) {lm <- feols(gen_soft ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 21) {lm <- feols(products ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 22) {lm <- feols(products ~ lr_trend | month + msa, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 23) {lm <- feols(products ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 24) {lm <- feols(project_mgmt ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 25) {lm <- feols(project_mgmt ~ lr_trend | month + msa, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 26) {lm <- feols(project_mgmt ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 27) {lm <- feols(creativ ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 28) {lm <- feols(creativ ~ lr_trend | month + msa, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 29) {lm <- feols(creativ ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 30) {lm <- feols(bus_sys ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 31) {lm <- feols(bus_sys ~ lr_trend | month + msa, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 32) {lm <- feols(bus_sys ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 33) {lm <- feols(data ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 34) {lm <- feols(data ~ lr_trend | month + msa, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 35) {lm <- feols(data ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 36) {lm <- feols(database ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 37) {lm <- feols(database ~ lr_trend | month + msa, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 38) {lm <- feols(database ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 39) {lm <- feols(data_analysis ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 40) {lm <- feols(data_analysis ~ lr_trend | month + msa, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 41) {lm <- feols(data_analysis ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 42) {lm <- feols(ML_AI ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 43) {lm <- feols(ML_AI ~ lr_trend | month + msa, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 44) {lm <- feols(ML_AI ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  
  warning("\n\n############# ",i," #############\n\n")
  return(lm)
}, mc.cores = 3)

lapply(regs[(c(1:13)-1)*3 + 3], summary)
saveRDS(regs, "./sc_bgt_lr_regs.rds")
remove(regs)

### SKILL CLUSTER SS ###

educ_sum <- df %>%
  select(year, bach_or_higher) %>%
  group_by(year) %>%
  summarise(across(everything(), ~ sum(.x, na.rm = TRUE)/n()))

saveRDS(educ_sum, "./educ_sum.rds")

sc_sum <- df %>%
  select(year, social,business_analysis,cog,noncog,mgmt,service,computer,admin_support,tech_support,gen_soft,products,project_mgmt,creativ,bus_sys,data,database,data_analysis,ML_AI) %>%
  group_by(year) %>%
  summarise(across(everything(), ~ sum(.x, na.rm = TRUE)/n()))

saveRDS(sc_sum, "./sc_sum.rds")