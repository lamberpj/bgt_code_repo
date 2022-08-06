#### SETUP ####
options(repos = c(CRAN = "https://mran.revolutionanalytics.com/snapshot/2021-04-01"))

options(scipen=999)

library("data.table")
library("tidyverse")
library("janitor")
library("lubridate")
library("doParallel")
library("ICSNP")
library(gtools)
#library("lsa")
#library("fuzzyjoin")
#library("quanteda")
#library("refinr")
#library("FactoMineR")
#library("ggpubr")
#library("scales")
#install.packages("ggthemr")
#ggthemr('flat')
#library("fixest")
#library("lfe")
#library("stargazer")
#library("texreg")
#library("sjPlot")
#library("margins")
#library("DescTools")
#library("fuzzyjoin")
library("caret")
# Translation packages
#library("rvest")
#library("googleLanguageR")
#library("cld2")
#library("datasets")
#library("vroom")
#library("refinr")
#library(stringi)
#library("readxl")
library(quanteda)
library("topicmodels")
#library(ggplot2)
#library(scales)
#library("ggpubr")
#devtools::install_github('Mikata-Project/ggthemr')
#library(ggthemr)

library(foreach)
library(doParallel)

#library("igraph")

library("matrixcalc")
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

length(file_names$file_names[file_names$type == "postings"])
length(file_names$file_names[file_names$type == "skills"])

table(file_names$year)
m <- nrow(file_names)/2

# View(fread(file_names$file_names[file_names$type == "postings" & file_names$country == countries[[i]]][1], nrow = 10))

#### COMPILE INCOME DISTRIBUTIONS ####
#### compile data ####
setMKLthreads(1)
options(warn=1)

df_list <- safe_mclapply(1:m, function(i) {
  
  y1 <- fread(file_names$file_names[file_names$type == "postings"][i],
              data.table = F,
              nThread = 1,
              select = c("general_id","year_grab_date","month_grab_date","idesco_level_4", "esco_level_4","idcountry")) %>%
    clean_names %>%
    distinct(general_id, .keep_all = T)
  
  j <- n_distinct(y1$general_id)
  
  y2 <- fread(file_names$file_names[file_names$type == "skills"][i], data.table = F, nThread = 1) %>%
    clean_names %>% select(general_id, escoskill_level_3)
  
  y1 <- y1 %>% mutate_if(is.character, list(~na_if(.,""))) 
  y2 <- y2 %>% mutate_if(is.character, list(~na_if(.,""))) 
  
  y2 <- y2 %>% filter(!is.na(escoskill_level_3)) %>%
    mutate(escoskill_level_3 = toupper(str_squish(escoskill_level_3))) %>%
    mutate(escoskill_level_3 = gsub("[^[:alnum:][:space:]]","",escoskill_level_3)) %>%
    mutate(escoskill_level_3 = gsub("[[:space:]]","_",escoskill_level_3))
  
  y2 <- y2 %>%
    left_join(y1)
  
  k <- n_distinct(y2$general_id)
  
  remove(y1)
  
  y2 <- y2 %>%
    group_by(year_grab_date,month_grab_date,idesco_level_4,esco_level_4,idcountry) %>%
    summarise(escoskill_level_3 = paste(escoskill_level_3, collapse = " "),
              n = n_distinct(general_id))
  
  warning(paste0("\n \n########### NODE ",i," CHECK ############\n",
                 "Postings file: ",file_names$file_names[file_names$type == "postings"][i],"\n",
                 "Skill file: ",file_names$file_names[file_names$type == "skills"][i],"\n",
                 "Fraction of Raw Retained = ",k,"/",j,"=",k/j),"\n########### #CHECK ############\n")
  
  return(y2)
}, mc.cores = 32)

df <- bind_rows(df_list)

table(df$year_grab_date, df$idcountry)

df <- df %>%
  group_by(year_grab_date,month_grab_date,idesco_level_4,esco_level_4) %>%
  summarise(escoskill_level_3 = paste(escoskill_level_3, collapse = " "),
            n = sum(n)) %>%
  mutate(doc_id = paste0(year_grab_date,"_",month_grab_date,"_",idesco_level_4))

remove(list = setdiff(ls(),"df"))

save(df, file = "./eu_alt_occ_skill_text.RData")
remove(list = ls())
load(file = "./eu_alt_occ_skill_text.RData")

quanteda_options(threads = 16)

df_dfm <- df %>%
  corpus(.,
         docid_field = "doc_id",
         text_field = "escoskill_level_3") %>%
  tokens(
    .,
    what = "fastestword",
    remove_punct = FALSE,
    remove_symbols = FALSE,
    remove_numbers = FALSE,
    remove_url = FALSE,
    remove_separators = TRUE,
    split_hyphens = FALSE,
    include_docvars = TRUE,
    padding = FALSE,
    verbose = quanteda_options("verbose")) %>%
  dfm()

df_dfm_prop <- df_dfm %>% 
  dfm_weight(., scheme = "prop")

df_dfm_prop <- df_dfm_prop * 100

# Drop Collinear Features
corr_aj_mat <- textstat_simil(df_dfm_prop,
                              margin = c("features"),
                              method = c("correlation"),
                              min_simil = NULL) %>%
  as.matrix

indexesToDrop <- findCorrelation(corr_aj_mat, cutoff = 0.75) # Identifies features to remove to limit pairwise correlation to 0.75

(drop_list <- rownames(corr_aj_mat)[indexesToDrop])

nfeat(df_dfm) # 1,885

df_dfm_prop_f <- df_dfm_prop %>% dfm_remove(., pattern = drop_list)
df_dfm_f <- df_dfm %>% dfm_remove(., pattern = drop_list)

df_dfm_prop_f <- dfm_subset(df_dfm_prop_f, rowSums(df_dfm_prop_f) > 1)
df_dfm_f <- dfm_subset(df_dfm_f, rowSums(df_dfm_f) > 1)

nfeat(df_dfm_f) # 1,498

corr_aj_mat <- textstat_simil(df_dfm_prop_f,
                              margin = c("features"),
                              method = c("correlation"),
                              min_simil = NULL) %>%
  as.matrix

sd_df <- apply(df_dfm_prop_f,2,sd) %>% as.matrix()

cov_aj_mat <- sd_df %*% t(sd_df) * corr_aj_mat

is.positive.definite(cov_aj_mat, tol=1e-10) # Check threshold at which the Covariance matrix is PD

# Make fyear data
docvars(df_dfm_f)$fyear <- ifelse(docvars(df_dfm_f)$month_grab_date > 3, docvars(df_dfm_f)$year_grab_date, docvars(df_dfm_f)$year_grab_date-1)
docvars(df_dfm_f)$fyear_occ <- paste0(docvars(df_dfm_f)$fyear,"_",docvars(df_dfm_f)$idesco_level_4)

df_dfm_f <- df_dfm_f %>% dfm_subset(., rowSums(df_dfm_f)>0)
df_dfm_f_fyear_occ <- df_dfm_f %>% dfm_group(groups = docvars(df_dfm_f)$fyear_occ, fill = FALSE, force = FALSE)
df_dfm_f_fyear_occ_prop <- df_dfm_f_fyear_occ %>% dfm_weight(., scheme = "prop")
df_dfm_f_fyear_occ_prop <- df_dfm_f_fyear_occ_prop * 100

#### MAHALANOBIS and ABSOLUTE DISTANCES ####
occ_list <- unique(docvars(df_dfm_f_fyear_occ_prop)$idesco_level_4)

df_dfm_f_fyear_occ_prop_listsplit <- lapply(occ_list, function(x) {
  df_dfm_f_fyear_occ_prop_example <- dfm_subset(df_dfm_f_fyear_occ_prop, docvars(df_dfm_f_fyear_occ_prop)$idesco_level_4 == x)
})
length(df_dfm_f_fyear_occ_prop_listsplit) # 417
occ_list <- occ_list[lapply(df_dfm_f_fyear_occ_prop_listsplit, ndoc) > 1]
df_dfm_f_fyear_occ_prop_listsplit <- df_dfm_f_fyear_occ_prop_listsplit[lapply(df_dfm_f_fyear_occ_prop_listsplit, ndoc) > 1]
length(occ_list) # 416
length(df_dfm_f_fyear_occ_prop_listsplit) # 416

vcov <- cov_aj_mat
inv_vcov <- solve(vcov, tol = 1e-50)
# Function #
md_function <- function(mat, inv_vcov) {
  mat <- as.matrix(mat)
  d <- pair.diff(mat)
  rnames <- combinations(n = length(rownames(mat)), r = 2, v = rownames(mat), repeats.allowed = FALSE)
  rownames(d) <- paste0(rnames[,1],"__",rnames[,2])
  colnames(d) <- colnames(mat)
  md <- sqrt(as.matrix(diag(d %*% inv_vcov %*% t(d))))
  ad <- sqrt(as.matrix(diag(d %*% t(d))))
  md <- data.frame("year_change" = rownames(md), "md" = md)
  ad <- data.frame("year_change" = rownames(md), "ad" = ad)
  dist_diff <- md %>% left_join(ad)
}

df_dist <- lapply(1:length(df_dfm_f_fyear_occ_prop_listsplit), function(i) {
  print(i)
  md_function(df_dfm_f_fyear_occ_prop_listsplit[[i]], inv_vcov = inv_vcov) %>%
    mutate(occ = occ_list[[i]])
}) %>% bind_rows

View(df_dist)
cor(df_dist$md, df_dist$ad) # 0.231
summary(df_dist$md)
summary(df_dist$ad)

remove(list = setdiff(ls(),c("df_dist","df")))

head(df_dist)

df_dist <- df_dist %>%
  separate(year_change, sep = "__", into = c("from_year", "to_year")) %>%
  mutate(from_year = as.numeric(str_sub(from_year, 1, 4)),
         to_year = as.numeric(str_sub(to_year, 1, 4))) %>%
  select(from_year, to_year, occ, ad, md)

df_merge_from <- df %>%
  mutate(fyear = ifelse(month_grab_date > 3, year_grab_date, year_grab_date-1)) %>%
  group_by(fyear, idesco_level_4, esco_level_4) %>%
  summarise(n = sum(n)) %>%
  rename(from_year = fyear, occ = idesco_level_4, occ_desc = esco_level_4, n_postings_from_year = n)

df_merge_to <- df %>%
  mutate(fyear = ifelse(month_grab_date > 3, year_grab_date, year_grab_date-1)) %>%
  group_by(fyear, idesco_level_4, esco_level_4) %>%
  summarise(n = sum(n)) %>%
  rename(to_year = fyear, occ = idesco_level_4, occ_desc = esco_level_4, n_postings_to_year = n)

df_dist_m <- df_dist %>%
  left_join(df_merge_from) %>%
  left_join(df_merge_to)

View(df_dist_m)

saveRDS(df_dist_m, "./eu_alt_df_dist.rds")

###################


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

#### COMPILE INCOME DISTRIBUTIONS ####
#### compile data ####
setMKLthreads(1)
options(warn=1)

df_list <- safe_mclapply(1:m, function(i) {

  y1 <- fread(file_names$file_names[file_names$type == "Main"][i], data.table = F, nThread = 1,
              select = c("BGTJobId", "JobDate", "SOC", "SOCName")) %>%
    clean_names %>%
    mutate(year_grab_date = year(ymd(job_date)),
           month_grab_date = month(ymd(job_date)))
  
  j <- n_distinct(y1$bgt_job_id)
  
  y2 <- fread(file_names$file_names[file_names$type == "Skills"][i], data.table = F, nThread = 1, select = c("BGTJobId", "Skill")) %>%
    clean_names %>%
    select(bgt_job_id, skill)
  
  y1 <- y1 %>% mutate_if(is.character, list(~na_if(.,""))) 
  y2 <- y2 %>% mutate_if(is.character, list(~na_if(.,""))) 
  
  y2 <- y2 %>% filter(!is.na(skill)) %>%
    mutate(skill = toupper(str_squish(skill))) %>%
    mutate(skill = gsub("[^[:alnum:][:space:]]","",skill)) %>%
    mutate(skill = gsub("[[:space:]]","_",skill))
  
  y2 <- y2 %>%
    left_join(y1)
  
  k <- n_distinct(y2$bgt_job_id)
  
  remove(y1)
  
  y2 <- y2 %>%
    group_by(year_grab_date,month_grab_date,soc,soc_name) %>%
    summarise(skill = paste(skill, collapse = " "),
              n = n_distinct(bgt_job_id))
  
  warning(paste0("\n \n########### NODE ",i," CHECK ############\n",
                 "Postings file: ",file_names$file_names[file_names$type == "Main"][i],"\n",
                 "Skill file: ",file_names$file_names[file_names$type == "Skills"][i],"\n",
                 "Fraction of Raw Retained = ",k,"/",j,"=",k/j),"\n########### #CHECK ############\n")
  
  return(y2)
  
}, mc.cores = 32)

df <- bind_rows(df_list)

table(df$year_grab_date, df$idcountry)

df <- df %>%
  group_by(year_grab_date,month_grab_date,soc,soc_name) %>%
  summarise(skill = paste(skill, collapse = " "),
            n = sum(n)) %>%
  mutate(doc_id = paste0(year_grab_date,"_",month_grab_date,"_",soc))

remove(list = setdiff(ls(),"df"))

save(df, file = "./us_occ_skill_text.RData")
remove(list = ls())
load(file = "./us_occ_skill_text.RData")

quanteda_options(threads = 16)

df_dfm <- df %>%
  corpus(.,
         docid_field = "doc_id",
         text_field = "skill") %>%
  tokens(
    .,
    what = "fastestword",
    remove_punct = FALSE,
    remove_symbols = FALSE,
    remove_numbers = FALSE,
    remove_url = FALSE,
    remove_separators = TRUE,
    split_hyphens = FALSE,
    include_docvars = TRUE,
    padding = FALSE,
    verbose = quanteda_options("verbose")) %>%
  dfm()

df_dfm_prop <- df_dfm %>% 
  dfm_weight(., scheme = "prop")

df_dfm_prop <- df_dfm_prop * 100

# Drop Collinear Features
corr_aj_mat <- textstat_simil(df_dfm_prop,
                              margin = c("features"),
                              method = c("correlation"),
                              min_simil = NULL) %>%
  as.matrix

indexesToDrop <- findCorrelation(corr_aj_mat, cutoff = 0.75) # Identifies features to remove to limit pairwise correlation to 0.75

(drop_list <- rownames(corr_aj_mat)[indexesToDrop])

nfeat(df_dfm) # 15,896
ndoc(df_dfm) # 74,055
df_dfm_prop_f <- df_dfm_prop %>% dfm_remove(., pattern = drop_list)
df_dfm_f <- df_dfm %>% dfm_remove(., pattern = drop_list)

df_dfm_prop_f <- dfm_subset(df_dfm_prop_f, rowSums(df_dfm_prop_f) > 1)
df_dfm_f <- dfm_subset(df_dfm_f, rowSums(df_dfm_f) > 1)

nfeat(df_dfm_f) # 14,942
ndoc(df_dfm_f) # 73,938

corr_aj_mat <- textstat_simil(df_dfm_prop_f,
                              margin = c("features"),
                              method = c("correlation"),
                              min_simil = NULL) %>%
  as.matrix

sd_df <- apply(df_dfm_prop_f,2,sd) %>% as.matrix()

cov_aj_mat <- sd_df %*% t(sd_df) * corr_aj_mat

is.positive.definite(cov_aj_mat, tol=1e-20) # Check threshold at which the Covariance matrix is PD

# Make fyear data
docvars(df_dfm_f)$fyear <- ifelse(docvars(df_dfm_f)$month_grab_date > 3, docvars(df_dfm_f)$year_grab_date, docvars(df_dfm_f)$year_grab_date-1)
docvars(df_dfm_f)$fyear_occ <- paste0(docvars(df_dfm_f)$fyear,"_",docvars(df_dfm_f)$soc)

tail(docvars(df_dfm_f))

df_dfm_f <- df_dfm_f %>% dfm_subset(., rowSums(df_dfm_f)>0)
df_dfm_f_fyear_occ <- df_dfm_f %>% dfm_group(groups = docvars(df_dfm_f)$fyear_occ, fill = FALSE, force = FALSE)
nfeat(df_dfm_f_fyear_occ)
df_dfm_f_fyear_occ_prop <- df_dfm_f_fyear_occ %>% dfm_weight(., scheme = "prop")
df_dfm_f_fyear_occ_prop <- df_dfm_f_fyear_occ_prop * 100

tail(docvars(df_dfm_f_fyear_occ_prop))

#### MAHALANOBIS and ABSOLUTE DISTANCES ####
occ_list <- unique(docvars(df_dfm_f_fyear_occ_prop)$soc)

df_dfm_f_fyear_occ_prop_listsplit <- lapply(occ_list, function(x) {
  df_dfm_f_fyear_occ_prop_example <- dfm_subset(df_dfm_f_fyear_occ_prop, docvars(df_dfm_f_fyear_occ_prop)$soc == x)
})
length(df_dfm_f_fyear_occ_prop_listsplit) # 837
occ_list <- occ_list[lapply(df_dfm_f_fyear_occ_prop_listsplit, ndoc) > 1]
df_dfm_f_fyear_occ_prop_listsplit <- df_dfm_f_fyear_occ_prop_listsplit[lapply(df_dfm_f_fyear_occ_prop_listsplit, ndoc) > 1]
length(occ_list) # 836
length(df_dfm_f_fyear_occ_prop_listsplit) # 836

vcov <- cov_aj_mat

inv_vcov <- solve(vcov, tol = 1e-50)

# Function #
md_function <- function(mat, inv_vcov) {
  mat <- as.matrix(mat)
  d <- pair.diff(mat)
  rnames <- combinations(n = length(rownames(mat)), r = 2, v = rownames(mat), repeats.allowed = FALSE)
  rownames(d) <- paste0(rnames[,1],"__",rnames[,2])
  colnames(d) <- colnames(mat)
  md <- sqrt(as.matrix(diag(d %*% inv_vcov %*% t(d))))
  ad <- sqrt(as.matrix(diag(d %*% t(d))))
  md <- data.frame("year_change" = rownames(md), "md" = md)
  ad <- data.frame("year_change" = rownames(md), "ad" = ad)
  dist_diff <- md %>% left_join(ad)
}

df_dist <- lapply(1:length(df_dfm_f_fyear_occ_prop_listsplit), function(i) {
  print(i)
  md_function(df_dfm_f_fyear_occ_prop_listsplit[[i]], inv_vcov = inv_vcov) %>%
    mutate(occ = occ_list[[i]])
}) %>% bind_rows


head(df_dist)
df_dist <- df_dist %>% filter(occ != "na")

View(df_dist)
cor(df_dist$md, df_dist$ad) # 0.421
summary(df_dist$md)
summary(df_dist$ad)

remove(list = setdiff(ls(),c("df_dist","df")))

head(df_dist_t)

nchar("2013_11-1011__")

## CHECK STR_SUB VALUES!!!!!!!!
df_dist <- df_dist %>%
  separate(year_change, sep = "__", into = c("from_year", "to_year")) %>%
  mutate(from_year = as.numeric(str_sub(from_year, 1, 4)),
         to_year = as.numeric(str_sub(to_year, 1, 4))) %>%
  select(from_year, to_year, occ, ad, md)

df_merge_from <- df %>%
  mutate(fyear = ifelse(month_grab_date > 3, year_grab_date, year_grab_date-1)) %>%
  group_by(fyear, soc, soc_name) %>%
  summarise(n = sum(n)) %>%
  rename(from_year = fyear, occ = soc, occ_desc = soc_name, n_postings_from_year = n)

df_merge_to <- df %>%
  mutate(fyear = ifelse(month_grab_date > 3, year_grab_date, year_grab_date-1)) %>%
  group_by(fyear, soc, soc_name) %>%
  summarise(n = sum(n)) %>%
  rename(to_year = fyear, occ = soc, occ_desc = soc_name, n_postings_to_year = n)

df_dist_m <- df_dist %>%
  left_join(df_merge_from) %>%
  left_join(df_merge_to)

View(df_dist_m)

saveRDS(df_dist_m, "./us_df_dist.rds")

###################

#################################
# ANALYSE SKILL CHANGE #
#################################
remove(list = ls())
setwd("/mnt/disks/pdisk/")
df_dist_us <- readRDS("./bg-us/us_df_dist.rds") %>% mutate(year_diff = to_year - from_year)
df_dist_eu_alt <- readRDS("./bg-eu/eu_alt_df_dist.rds") %>% mutate(year_diff = to_year - from_year)

# Hist 5 year change pre / post COVID in EU unweighted
fp <- df_dist_eu_alt %>% filter(year_diff == 5 & to_year %in% c(2019, 2020)) %>% mutate(to_year = as.factor(to_year))

p = ggplot(fp, aes(x = md, color = to_year, fill = to_year, linetype = to_year)) +
  geom_density(size = 1, alpha = 0.05, adjust = 1) +
  ggtitle("EU KDensity of 5 Year Skill Change (MD, Unweighted)") +
  ylab("Density") +
  xlab("Skill Change (Mahalanobis Distance)") +
  scale_y_continuous() +
  theme(legend.position = "bottom") +
  labs(caption = "Note: Each Data Point is a occupation cell (ESCO4 or SOC6).\nData is unweighted.")
p
remove(p)

# Hist 5 year change pre / post COVID in EU unweighted
fp <- df_dist_us %>% filter(year_diff == 5 & to_year %in% c(2019, 2020)) %>% mutate(to_year = as.factor(to_year))

p = ggplot(fp, aes(x = md, color = to_year, fill = to_year, linetype = to_year)) +
  geom_density(size = 1, alpha = 0.05, adjust = 1) +
  ggtitle("USA KDensity of 5 Year Skill Change (MD, Unweighted)") +
  ylab("Density") +
  xlab("Skill Change (Mahalanobis Distance)") +
  scale_y_continuous() +
  theme(legend.position = "bottom") +
  labs(caption = "Note: Each Data Point is a occupation cell (ESCO4 or SOC6).\nData is unweighted.")
p
remove(p)

# Hist 1 year change pre / post COVID in EU unweighted
fp <- df_dist_eu_alt %>% filter(year_diff == 1 & to_year %in% c(2018, 2019, 2020)) %>% mutate(to_year = as.factor(to_year))

p = ggplot(fp, aes(x = ad, color = to_year, fill = to_year, linetype = to_year)) +
  geom_density(size = 1, alpha = 0.05, adjust = 1) +
  ggtitle("EU KDensity of 1 YearSkill Change (MD, Unweighted)") +
  ylab("Density") +
  xlab("Skill Change (Mahalanobis Distance)") +
  scale_y_continuous() +
  theme(legend.position = "bottom") +
  labs(caption = "Note: Each Data Point is a occupation cell (ESCO4 or SOC6).\nData is unweighted.")
p
remove(p)

# Hist 1 year change pre / post COVID in EU unweighted
fp <- df_dist_us %>% filter(year_diff == 1 & to_year %in% c(2018, 2019, 2020)) %>% mutate(to_year = as.factor(to_year))

p = ggplot(fp, aes(x = ad, color = to_year, fill = to_year, linetype = to_year)) +
  geom_density(size = 1, alpha = 0.05, adjust = 1) +
  ggtitle("USA KDensity of 1 Year Skill Change (MD, Unweighted)") +
  ylab("Density") +
  xlab("Skill Change (Mahalanobis Distance)") +
  scale_y_continuous() +
  theme(legend.position = "bottom") +
  labs(caption = "Note: Each Data Point is a occupation cell (ESCO4 or SOC6).\nData is unweighted.")
p
remove(p)



