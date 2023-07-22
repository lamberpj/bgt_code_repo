#### SETUP ####
options(repos = c(CRAN = "https://mran.revolutionanalytics.com/snapshot/2021-04-01"))

options(scipen=999)

library("data.table")
library("tidyverse")
library("janitor")
library("lubridate")
library("doParallel")
#library("lsa")
library("fuzzyjoin")
library("stringdist")
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
  select(colnames(.)[!grepl("[A-Z]", colnames(.))]) %>%
  select(-sum) %>%
  mutate(skill = toupper(str_trim(str_squish(skill)))) %>%
  distinct(skill, .keep_all = T)


#### COMPILE INCOME DISTRIBUTIONS ####
#### compile data ####
setMKLthreads(1)
setDTthreads(2)

df_list <- safe_mclapply(1:m, function(i) {
  
  y2 <- fread(file_names$file_names[file_names$type == "Skills"][i], data.table = F, nThread = 1, colClasses = "character", stringsAsFactors = FALSE, select = c("Skill", "SkillCluster")) %>%
    clean_names %>%
    distinct(skill, .keep_all = T)
  
}, mc.cores = 64)

df <- df_list %>%
  bind_rows %>%
  distinct(skill, .keep_all = T)

df <- df %>%
  mutate(skill = toupper(str_trim(str_squish(skill)))) %>%
  distinct(skill, .keep_all = T)

sc_df <- sc_df %>% mutate(joined = 1)

### EXACT MATCH ###
df_1 <- df %>%
  left_join(sc_df)

sum(df_1$joined, na.rm = T) # 13,059
length(df_1$joined) # 15,901

## FUZZY MATCH ##

cosine_dist <- function(left, right) {
  stringdist(left, right, method="cosine", q=3) < 0.5
}

df_2 <- df %>%
  filter(!(skill %in% df_1$skill[df_1$joined == 1]))

nrow(df_2) # 2,842

df_2 <- df_2 %>%
  stringdist_left_join(sc_df, by = "skill", method = "cosine", max_dist = 0.5, distance_col = "cosine_dist", q = 3)

df_2$cosine_dist[is.na(df_2$cosine_dist)] <- 0

nrow(df_2) # 49,441

head(df_2)

df_3 <- df_2 %>%
  group_by(skill.x) %>%
  filter(cosine_dist == min(cosine_dist))

saveRDS(df_3, "./cosine_fuzzy_merge.rds")
  
nrow(df_3) # 49,441
