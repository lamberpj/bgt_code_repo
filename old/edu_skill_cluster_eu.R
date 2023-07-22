#### SETUP ####
options(repos = c(CRAN = "https://mran.revolutionanalytics.com/snapshot/2021-04-01"))
options(scipen=999)

library("data.table")
library("tidyverse")
library("janitor")
library("lubridate")
library("doParallel")
#library("ICSNP")
#library(gtools)
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
#library("caret")
# Translation packages
#library("rvest")
#library("googleLanguageR")
#library("cld2")
#library("datasets")
#library("vroom")
#library("refinr")
#library(stringi)
#library("readxl")
#library(quanteda)
#library("topicmodels")
#library(ggplot2)
#library(scales)
#library("ggpubr")
#devtools::install_github('Mikata-Project/ggthemr')
#library(ggthemr)

library(foreach)
library(doParallel)

#library("igraph")

#library("matrixcalc")
#install.packages("graph4lg")


#################################
# EU ALTERNATIVE #
#################################
setwd("/mnt/disks/pdisk/bg-eu/")
#### LOAD DATA ####
remove(list = ls())
file_names <- list.files("./bg-eu-bucket/alternative/", "*.csv", full.names = F)
file_names <- gsub("^skills", "integrative skills", file_names)
file_names <- as.data.frame(file_names) %>% separate(file_names, sep = " ", c("indexdata", "type", "country", "file"), remove = F) %>% select(-indexdata)
file_names$file_names <- as.data.frame(paths <- list.files("./bg-eu-bucket/alternative", "*.csv", full.names = T))$paths
file_names <- file_names %>% mutate(year = str_sub(parse_number(file), 1, 4))

file_names <- file_names %>% arrange(country, year, type)

file_names <- file_names %>%
  group_by(country, year) %>%
  mutate(n = n())

file_names <- file_names %>% distinct(file_names, .keep_all = T)

length(file_names$file_names[file_names$type == "postings"])
length(file_names$file_names[file_names$type == "skills"])

table(file_names$year)
m <- nrow(file_names)/2



sc_df <- fread("./aux_data/EU_Deming.csv") %>% select(-V1) %>% rename(escoskill_level_3 = skill) %>%
  select(escoskill_level_3,social,business_analysis,cog,noncog,mgmt,service,computer,admin_support,tech_support,gen_soft,products,project_mgmt,creativ,bus_sys,data,database,data_analysis,ML_AI) %>%
  mutate(escoskill_level_3 = toupper(str_trim(str_squish(escoskill_level_3)))) %>%
  distinct(escoskill_level_3, .keep_all = T)

drop_df <- fread("./aux_data/ISCO.csv") %>% select(x) %>% rename(idesco_level_4 = x)

#### COMPILE INCOME DISTRIBUTIONS ####
#### compile data ####
setMKLthreads(1)
options(warn=1)

df_list <- safe_mclapply(1:m, function(i) {
  
  y1 <- fread(file_names$file_names[file_names$type == "postings"][i],
              data.table = F,
              nThread = 1,
              select = c("general_id","year_grab_date","month_grab_date","day_grab_date",
                         "ideducational_level", "idregion", "idprovince",
                         "idesco_level_4","idmacro_sector",
                         "idcountry", "companyname")) %>%
    clean_names %>%
    distinct(general_id, .keep_all = T)
  
  y1 <- y1 %>% mutate_if(is.character, list(~na_if(.,"")))
  
  y1 <- y1 %>% filter(!is.na(idprovince) & !is.na(companyname) & !is.na(idesco_level_4))
  
  y1 <- y1 %>%
    mutate(companyname_clean = str_replace_all(companyname, "\\(.*\\)", "")) %>%
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
    mutate(grab_date = dmy(paste0(day_grab_date,"/",month_grab_date,"/",year_grab_date))) %>%
    mutate(year_month = paste0(year(grab_date),".", sprintf("%02d", month(grab_date)))) %>%
    select(-c(day_grab_date, month_grab_date, year_grab_date)) %>%
    distinct(general_id, .keep_all = T)
  
  #y1 <- y1 %>%
  #  mutate(companyname_clean_merged = n_gram_merge(y1$companyname_clean[!is.na(y1$companyname_clean)],
  #                                                 numgram = 2,
  #                                                 bus_suffix = TRUE,
  #                                                 edit_threshold = 1,
  #                                                 weight = c(d = 0.33, i = 0.33, s = 1, t = 0.5)))
  
  y2 <- fread(file_names$file_names[file_names$type == "skills"][i], data.table = F, nThread = 1) %>%
    clean_names %>% select(general_id, escoskill_level_3)
  
  y1 <- y1 %>% mutate_if(is.character, list(~na_if(.,""))) 
  y2 <- y2 %>% mutate_if(is.character, list(~na_if(.,"")))
  
  nrow(y2) # 10,163,276
  y2 <- y2 %>%
    filter(!is.na(escoskill_level_3)) %>%
    filter(general_id %in% y1$general_id) %>%
    mutate(escoskill_level_3 = toupper(str_trim(str_squish(escoskill_level_3)))) %>%
    left_join(sc_df)
  y2 <- y2 %>%
    select(-escoskill_level_3)
  
  y2[is.na(y2)] <- 0
  
  y2 <- y2 %>%
    group_by(general_id) %>%
    summarise(across(everything(), max))
  
  n_distinct(y2$general_id) #
  nrow(y2) #
  
  n_distinct(y1$general_id) #
  nrow(y1) #
  
  y1 <- y1 %>% filter(general_id %in% y2$general_id)
  y2 <- y2 %>% filter(general_id %in% y1$general_id)
  
  y1 <- y1 %>% distinct(general_id, .keep_all = T)
  y2 <- y2 %>% distinct(general_id, .keep_all = T)
  
  y1 <- y1 %>%
    left_join(y2)
  
  y1 <- y1 %>% distinct(general_id, .keep_all = T)
  
  remove(y2)
  
  # ESCO Drop Lookup
  
  
  y1 <- y1 %>%
    mutate(drop = ifelse(idesco_level_4 %in% drop_df$idesco_level_4[nchar(drop_df$idesco_level_4) == 4] | str_sub(idesco_level_4, 1, 3) %in% drop_df$idesco_level_4[nchar(drop_df$idesco_level_4) == 3], 1, 0))
  
  y1 <- y1 %>% as_tibble
  
  head(df)
  
  y1 <- y1 %>%
    mutate(ideducational_level = as.numeric(ideducational_level)) %>%
    mutate(year = str_sub(year_month, 1, 4)) %>%
    mutate(month = str_sub(year_month, -2, -1)) %>%
    mutate(bach_or_higher = ifelse(ideducational_level >= 6, 1, 0))
  
  y1 <- y1 %>%
    mutate(bach_or_higher = ifelse(is.na(ideducational_level), 0, bach_or_higher))
  
  y1 <- y1 %>%
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
  
  saveRDS(y1, file = paste0("./int_data/temp/file",i,".rds"), compress = F)
  remove(y1)
  
  #warning(paste0("\n\n########### NODE ",i," CHECK ############\n\n"))
  return("")
}, mc.cores = 32)

files <- list.files("./int_data/temp/", "*.rds", full.names = F)
files <- gsub("file", "", files)
files <- gsub(".rds", "", files)
files <- as.numeric(files)
files <- setdiff(1:m, files)
files

files <- list.files("./int_data/temp/", "*.rds", full.names = T)

df <- safe_mclapply(1:length(files), function(i){
  x <- readRDS(files[[i]])
  warning(paste0("\n\n####### ",i," = ",nrow(x)," ########\n\n"))
  return(x)
}, mc.cores = 32)

df <- df %>% bind_rows

nrow(df) # 85,107,620

print("done!")

head(df)

n_distinct(df$companyname_clean) # 3,240,979

# At least 5 postings 
df_firm_list_3yr <- df %>%
  select(companyname_clean, idesco_level_4, year_month) %>%
  mutate(year = str_sub(year_month, 1, 4)) %>%
  filter(year %in% c("2018","2019","2020")) %>%
  select(-year_month) %>%
  group_by(companyname_clean, idesco_level_4, year) %>%
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

head(df_firm_list_3yr)

#df_firm_list_2020 <- df %>%
#  select(companyname_clean, idesco_level_4, year_month) %>%
#  mutate(year = str_sub(year_month, 1, 4)) %>%
#  filter(year %in% c("2020")) %>%
#  select(-year_month) %>%
#  group_by(companyname_clean, idesco_level_4, year) %>%
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
#         a1_idesco_level_4_all = ifelse(any(n_occ<1),0,1),
#         a2_idesco_level_4_all = ifelse(any(n_occ<2),0,1),
#         a5_idesco_level_4_all = ifelse(any(n_occ<5),0,1),
#         a10_idesco_level_4_all = ifelse(any(n_occ<10),0,1)) %>%
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

#### /end ####

#### REGRESSIONS FOR BACHELOR OR HIGHER ####
save.image(file = "./eu_alt_bgt_image2.RData")
load(file = "./eu_alt_bgt_image2.RData")
"test"
head(df)
# All
regs <- safe_mclapply(1:5, function(i) {
  if(i == 1) {lm <- feols(bach_or_higher ~ later_quarters | month, df %>% filter(drop == 0), cluster = ~ year_month, lean = T)}
  if(i == 2) {lm <- feols(bach_or_higher ~ later_quarters | month + idesco_level_4, df %>% filter(drop == 0), cluster = ~ year_month, lean = T)}
  if(i == 3) {lm <- feols(bach_or_higher ~ later_quarters | month + idregion, df %>% filter(drop == 0), cluster = ~ year_month, lean = T)}
  if(i == 4) {lm <- feols(bach_or_higher ~ later_quarters | month + idregion + idesco_level_4, df %>% filter(drop == 0), cluster = ~ year_month, lean = T)}
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
  if(i == 2) {lm <- feols(social ~ later_quarters | month + idregion, df, cluster = ~ year_month, lean = T)}
  if(i == 3) {lm <- feols(social ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 4) {lm <- feols(business_analysis ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 5) {lm <- feols(business_analysis ~ later_quarters | month + idregion, df, cluster = ~ year_month, lean = T)}
  if(i == 6) {lm <- feols(business_analysis ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 7) {lm <- feols(cog ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 8) {lm <- feols(cog ~ later_quarters | month + idregion, df, cluster = ~ year_month, lean = T)}
  if(i == 9) {lm <- feols(cog ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 10) {lm <- feols(noncog ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 11) {lm <- feols(noncog ~ later_quarters | month + idregion, df, cluster = ~ year_month, lean = T)}
  if(i == 12) {lm <- feols(noncog ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 13) {lm <- feols(mgmt ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 14) {lm <- feols(mgmt ~ later_quarters | month + idregion, df, cluster = ~ year_month, lean = T)}
  if(i == 15) {lm <- feols(mgmt ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 16) {lm <- feols(service ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 17) {lm <- feols(service ~ later_quarters | month + idregion, df, cluster = ~ year_month, lean = T)}
  if(i == 18) {lm <- feols(service ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 19) {lm <- feols(computer ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 20) {lm <- feols(computer ~ later_quarters | month + idregion, df, cluster = ~ year_month, lean = T)}
  if(i == 21) {lm <- feols(computer ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 22) {lm <- feols(admin_support ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 23) {lm <- feols(admin_support ~ later_quarters | month + idregion, df, cluster = ~ year_month, lean = T)}
  if(i == 24) {lm <- feols(admin_support ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 25) {lm <- feols(tech_support ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 26) {lm <- feols(tech_support ~ later_quarters | month + idregion, df, cluster = ~ year_month, lean = T)}
  if(i == 27) {lm <- feols(tech_support ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 28) {lm <- feols(gen_soft ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 29) {lm <- feols(gen_soft ~ later_quarters | month + idregion, df, cluster = ~ year_month, lean = T)}
  if(i == 20) {lm <- feols(gen_soft ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 21) {lm <- feols(products ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 22) {lm <- feols(products ~ later_quarters | month + idregion, df, cluster = ~ year_month, lean = T)}
  if(i == 23) {lm <- feols(products ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 24) {lm <- feols(project_mgmt ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 25) {lm <- feols(project_mgmt ~ later_quarters | month + idregion, df, cluster = ~ year_month, lean = T)}
  if(i == 26) {lm <- feols(project_mgmt ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 27) {lm <- feols(creativ ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 28) {lm <- feols(creativ ~ later_quarters | month + idregion, df, cluster = ~ year_month, lean = T)}
  if(i == 29) {lm <- feols(creativ ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 30) {lm <- feols(bus_sys ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 31) {lm <- feols(bus_sys ~ later_quarters | month + idregion, df, cluster = ~ year_month, lean = T)}
  if(i == 32) {lm <- feols(bus_sys ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 33) {lm <- feols(data ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 34) {lm <- feols(data ~ later_quarters | month + idregion, df, cluster = ~ year_month, lean = T)}
  if(i == 35) {lm <- feols(data ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 36) {lm <- feols(database ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 37) {lm <- feols(database ~ later_quarters | month + idregion, df, cluster = ~ year_month, lean = T)}
  if(i == 38) {lm <- feols(database ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 39) {lm <- feols(data_analysis ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 40) {lm <- feols(data_analysis ~ later_quarters | month + idregion, df, cluster = ~ year_month, lean = T)}
  if(i == 41) {lm <- feols(data_analysis ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 42) {lm <- feols(ML_AI ~ later_quarters | month, df, cluster = ~ year_month, lean = T)}
  if(i == 43) {lm <- feols(ML_AI ~ later_quarters | month + idregion, df, cluster = ~ year_month, lean = T)}
  if(i == 44) {lm <- feols(ML_AI ~ later_quarters | month + companyname_clean, df %>% filter(companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  
  warning("\n\n############# ",i," #############\n\n")
  return(lm)
}, mc.cores = 5)

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
  if(i == 2) {lm <- feols(bach_or_higher ~ lr_trend | idesco_level_4, df %>% filter(drop == 0 & year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 3) {lm <- feols(bach_or_higher ~ lr_trend | idregion, df %>% filter(drop == 0 & year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 4) {lm <- feols(bach_or_higher ~ lr_trend | idregion + idesco_level_4, df %>% filter(drop == 0 & year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 5) {lm <- feols(bach_or_higher ~ lr_trend | companyname_clean, df %>% filter(drop == 0 & year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  warning("\n\n############# ",i," #############\n\n")
  return(lm)
}, mc.cores = 5)

lapply(regs, summary)
saveRDS(regs, "./col_share_lr_bgt_regs.rds")
remove(regs)

#### SKILL CLUSTER COVID ####
regs <- safe_mclapply(1:39, function(i) {
  if(i == 1) {lm <- feols(social ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 2) {lm <- feols(social ~ lr_trend | month + idregion, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 3) {lm <- feols(social ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 4) {lm <- feols(business_analysis ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 5) {lm <- feols(business_analysis ~ lr_trend | month + idregion, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 6) {lm <- feols(business_analysis ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 7) {lm <- feols(cog ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 8) {lm <- feols(cog ~ lr_trend | month + idregion, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 9) {lm <- feols(cog ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 10) {lm <- feols(noncog ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 11) {lm <- feols(noncog ~ lr_trend | month + idregion, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 12) {lm <- feols(noncog ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 13) {lm <- feols(mgmt ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 14) {lm <- feols(mgmt ~ lr_trend | month + idregion, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 15) {lm <- feols(mgmt ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 16) {lm <- feols(service ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 17) {lm <- feols(service ~ lr_trend | month + idregion, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 18) {lm <- feols(service ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 19) {lm <- feols(computer ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 20) {lm <- feols(computer ~ lr_trend | month + idregion, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 21) {lm <- feols(computer ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 22) {lm <- feols(admin_support ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 23) {lm <- feols(admin_support ~ lr_trend | month + idregion, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 24) {lm <- feols(admin_support ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 25) {lm <- feols(tech_support ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 26) {lm <- feols(tech_support ~ lr_trend | month + idregion, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 27) {lm <- feols(tech_support ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 28) {lm <- feols(gen_soft ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 29) {lm <- feols(gen_soft ~ lr_trend | month + idregion, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 20) {lm <- feols(gen_soft ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 21) {lm <- feols(products ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 22) {lm <- feols(products ~ lr_trend | month + idregion, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 23) {lm <- feols(products ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 24) {lm <- feols(project_mgmt ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 25) {lm <- feols(project_mgmt ~ lr_trend | month + idregion, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 26) {lm <- feols(project_mgmt ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 27) {lm <- feols(creativ ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 28) {lm <- feols(creativ ~ lr_trend | month + idregion, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 29) {lm <- feols(creativ ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 30) {lm <- feols(bus_sys ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 31) {lm <- feols(bus_sys ~ lr_trend | month + idregion, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 32) {lm <- feols(bus_sys ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 33) {lm <- feols(data ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 34) {lm <- feols(data ~ lr_trend | month + idregion, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 35) {lm <- feols(data ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 36) {lm <- feols(database ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 37) {lm <- feols(database ~ lr_trend | month + idregion, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 38) {lm <- feols(database ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 39) {lm <- feols(data_analysis ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 40) {lm <- feols(data_analysis ~ lr_trend | month + idregion, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 41) {lm <- feols(data_analysis ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  if(i == 42) {lm <- feols(ML_AI ~ lr_trend | month, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 43) {lm <- feols(ML_AI ~ lr_trend | month + idregion, df %>% filter(year != "2020" & year != "2021"), cluster = ~ year_month, lean = T)}
  if(i == 44) {lm <- feols(ML_AI ~ lr_trend | month + companyname_clean, df %>% filter(year != "2020" & year != "2021" & companyname_clean %in% df_firm_list_3yr$companyname_clean[df_firm_list_3yr$above_10_n_all == 1]), cluster = ~ year_month, lean = T)}
  
  warning("\n\n############# ",i," #############\n\n")
  return(lm)
}, mc.cores = 7)

lapply(regs[(c(1:13)-1)*3 + 3], summary)
saveRDS(regs, "./sc_bgt_lr_regs.rds")
remove(regs)

### SKILL CLUSTER SS ###

sc_sum <- df %>%
  select(year, social,business_analysis,cog,noncog,mgmt,service,computer,admin_support,tech_support,gen_soft,products,project_mgmt,creativ,bus_sys,data,database,data_analysis,ML_AI) %>%
  group_by(year) %>%
  summarise(across(everything(), ~ sum(.x, na.rm = TRUE)/n()))

saveRDS(sc_sum, "./sc_sum.rds")
  
