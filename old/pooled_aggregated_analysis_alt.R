#### SETUP ####
options(repos = c(CRAN = "https://mran.revolutionanalytics.com/snapshot/2021-04-01"))

library("data.table")
library("tidyverse")
library("janitor")
library("lubridate")
library("doParallel")
#library("lsa")
#library("fuzzyjoin")
#library("quanteda")
library("refinr")
#library("FactoMineR")
library("ggpubr")
library("scales")
#install.packages("ggthemr")
#ggthemr('flat')
library("fixest")
library("lfe")
library("stargazer")
library("texreg")
library("sjPlot")
library("margins")
library("DescTools")
library("fuzzyjoin")

#### IMPORT DATA EU #####
setwd("/mnt/disks/pdisk/bg-eu/")

#### file paths ####
remove(list = ls())
file_names <- list.files("./bg-eu-bucket/new/structured/", "*.csv", full.names = F)
file_names <- as.data.frame(file_names) %>% separate(file_names, sep = " ", c("type", "country", "file"), remove = F)
file_names$file_names <- as.data.frame(paths <- list.files("./bg-eu-bucket/new/structured/", "*.csv", full.names = T))$paths
colnames(file_names)
countries <- file_names$country %>% unique()
m <- length(countries)
i = 3
#### /end ####

#### Load crosswalk ####
onet_soc_xwalk <- read_csv("./aux_data/onet_soc_xwalk.csv") %>% arrange(semantic_similarity)
nrow(onet_soc_xwalk) #1680
onet_soc_xwalk <- onet_soc_xwalk %>% group_by(onet_code) %>%
  filter(semantic_similarity == max(semantic_similarity, na.rm = T)) %>% ungroup() %>%
  mutate(fuzzy_onet_code = paste0("cccc",isco_code)) %>%
  mutate(onet_code_numeric = as.numeric(gsub("-","",onet_code)))
nrow(onet_soc_xwalk) # 670
#### /end ####

#### compile data ####
df <- lapply(1:m, function(i) {
  y1_lines <- lapply(file_names$file_names[file_names$type == "postings" & file_names$country == countries[[i]]], function(x) {
    read_lines(paste0(x))}) %>% Reduce(c,.)
  
  n <- length(y1_lines)
  remove(y1_lines)
  
  y1 <- lapply(file_names$file_names[file_names$type == "postings" & file_names$country == countries[[i]]], function(x) {
    fread(paste0(x), data.table = F, nThread = 8,
          select = c("general_id","year_grab_date","month_grab_date","day_grab_date", "salary", "idcountry",
                     "idesco_level_2", "idesco_level_4", "ideducational_level", "educational_level", "idsector", "idmacro_sector",
                     "idcategory_sector", "companyname"))}) %>% bind_rows %>% clean_names
  
  print(countries[[i]])
  print(n - nrow(y1))
  
  y1 <- y1 %>%
    mutate(grab_date = dmy(paste0(day_grab_date,"/",month_grab_date,"/",year_grab_date))) %>%
    mutate(year_month = paste0(year(grab_date),".", sprintf("%02d", month(grab_date)))) %>%
    select(-c(day_grab_date, month_grab_date, year_grab_date)) %>%
    distinct(general_id, .keep_all = T)
  
  y1 <- y1 %>%
    mutate(bach_or_higher = ifelse(ideducational_level >= 6,"above_col","below_col")) %>%
    group_by(idcountry, year_month, idesco_level_4, bach_or_higher) %>%
    summarise(n = n()) %>%
    group_by(idcountry, year_month, idesco_level_4) %>%
    pivot_wider(., names_from = bach_or_higher, values_from = n) %>%
    ungroup()
  
  y1$below_col[is.na(y1$below_col)] <- 0
  y1$above_col[is.na(y1$above_col)] <- 0
  
  y1 <- y1 %>%
    mutate(col_share = above_col/(above_col+below_col))
  
  return(y1)
})

saveRDS(df, "./int_data/educ_regs/all_EU_aggregated_list.rds")

#### end ####

#### IMPORT DATA EU ALT #####
setwd("/mnt/disks/pdisk/bg-eu/")

#### file paths ####
remove(list = ls())
file_names <- list.files("./bg-eu-bucket/alternative/", "*.csv", full.names = F)
file_names <- as.data.frame(file_names) %>% separate(file_names, sep = " ", c("indexdata", "type", "country", "file"), remove = F) %>% select(-indexdata)
file_names$file_names <- as.data.frame(paths <- list.files("./bg-eu-bucket/alternative", "*.csv", full.names = T))$paths
colnames(file_names)
countries <- file_names$country %>% unique()
m <- length(countries)
#### /end ####

#### Load crosswalk ####
onet_soc_xwalk <- read_csv("./aux_data/onet_soc_xwalk.csv") %>% arrange(semantic_similarity)
nrow(onet_soc_xwalk) #1680
onet_soc_xwalk <- onet_soc_xwalk %>% group_by(onet_code) %>%
  filter(semantic_similarity == max(semantic_similarity, na.rm = T)) %>% ungroup() %>%
  mutate(fuzzy_onet_code = paste0("cccc",isco_code)) %>%
  mutate(onet_code_numeric = as.numeric(gsub("-","",onet_code)))
nrow(onet_soc_xwalk) # 670
#### /end ####

#### compile data ####
df <- lapply(1:m, function(i) {
  y1_lines <- lapply(file_names$file_names[file_names$type == "postings" & file_names$country == countries[[i]]], function(x) {
   read_lines(paste0(x))}) %>% Reduce(c,.)
  
  n <- length(y1_lines)
  remove(y1_lines)
  
  y1 <- lapply(file_names$file_names[file_names$type == "postings" & file_names$country == countries[[i]]], function(x) {
    fread(paste0(x), data.table = F, nThread = 8,
          select = c("general_id","year_grab_date","month_grab_date","day_grab_date", "salary", "idcountry",
                     "idesco_level_2", "idesco_level_4", "ideducational_level", "educational_level", "idsector", "idmacro_sector",
                     "idcategory_sector", "companyname"))}) %>% bind_rows %>% clean_names

  print(countries[[i]])
  print(n - nrow(y1))
  
  
  
  y1 <- y1 %>%
    mutate(grab_date = dmy(paste0(day_grab_date,"/",month_grab_date,"/",year_grab_date))) %>%
    mutate(year_month = paste0(year(grab_date),".", sprintf("%02d", month(grab_date)))) %>%
    select(-c(day_grab_date, month_grab_date, year_grab_date)) %>%
    distinct(general_id, .keep_all = T)

  y1 <- y1 %>%
    mutate(bach_or_higher = ifelse(ideducational_level >= 6,"above_col","below_col")) %>%
    group_by(idcountry, year_month, idesco_level_4, bach_or_higher) %>%
    summarise(n = n()) %>%
    group_by(idcountry, year_month, idesco_level_4) %>%
    pivot_wider(., names_from = bach_or_higher, values_from = n) %>%
    ungroup()
  
  y1$below_col[is.na(y1$below_col)] <- 0
  y1$above_col[is.na(y1$above_col)] <- 0
  
  y1 <- y1 %>%
    mutate(col_share = above_col/(above_col+below_col))
  
  return(y1)
})

saveRDS(df, "./int_data/educ_regs/all_EU_alt_aggregated_list.rds")

#### end ####

#### IMPORT US DATA #####
setwd("/mnt/disks/pdisk/bg-us/")

#### file paths ####
remove(list = ls())
file_names <- list.files("./raw_data/main", "*.txt", full.names = F)
file_names <- as.data.frame(file_names) %>% separate(file_names, sep = "_", c("type", "year-month"), remove = F)
file_names$file_names <- as.data.frame(paths <- list.files("./raw_data/main", "*.txt", full.names = T))$paths
colnames(file_names)
m <- length(file_names$file_names)
#i = 14
#### /end ####

#### compile data ####
df <- lapply(1:m, function(i) {
  y1_lines <- read_lines(paste0(file_names$file_names[i]))
  
  n <- length(y1_lines)
  remove("y1_lines")
  
  y1 <- fread(paste0(file_names$file_names[i]),
              data.table = F,
              nThread = 8,
              integer64 = "double",
              na.strings = c("",NA,"na"),
              stringsAsFactors = FALSE,
              colClasses = 'character',
              select = c("BGTJobId", "JobDate", "SOC", "State", "Edu")) %>%
    clean_names
  
  print(i)
  print(nrow(y1) - n)
  
  y1 <- y1 %>%
    filter(soc != "na")
  
  y1$edu[y1$edu == -999] <- NA
  
  y1 <- y1 %>%
    mutate(grab_date = ymd(job_date)) %>%
    mutate(year_month = paste0(year(grab_date),".", sprintf("%02d", month(grab_date)))) %>%
    select(-c(grab_date)) %>%
    distinct(bgt_job_id, .keep_all = T)
  
  head(y1)
  
  y1 <- y1 %>%
    mutate(bach_or_higher = ifelse(edu >= 16,"above_col","below_col")) %>%
    group_by(state, year_month, soc, bach_or_higher) %>%
    summarise(n = n()) %>%
    group_by(state, year_month, soc) %>%
    pivot_wider(., names_from = bach_or_higher, values_from = n) %>%
    ungroup()

  head(y1)
    
  y1$below_col[is.na(y1$below_col)] <- 0
  y1$above_col[is.na(y1$above_col)] <- 0
  
  y1 <- y1 %>%
    mutate(col_share = above_col/(above_col+below_col))
  
  return(y1)
  
})

remove(list = setdiff(ls(), "df"))

saveRDS(df, "./int_data/educ_regs/all_US_aggregated_list.rds")

#### end ####

#### IMPORT UK DATA #####
setwd("/mnt/disks/pdisk/bg-uk/")

#### file paths ####
remove(list = ls())
file_names <- list.files("./raw_data/main", "*.txt", full.names = F)
file_names <- as.data.frame(file_names) %>% separate(file_names, sep = "_", c("type", "year-month"), remove = F)
file_names$file_names <- as.data.frame(paths <- list.files("./raw_data/main", "*.txt", full.names = T))$paths
colnames(file_names)
m <- length(file_names$file_names)
#### /end ####

#### compile data ####
df <- lapply(1:m, function(i) {
  y1_lines <- read_lines(paste0(file_names$file_names[i]))
  
  n <- length(y1_lines)
  remove("y1_lines")
  
  y1 <- fread(paste0(file_names$file_names[i]),
              data.table = F,
              nThread = 8,
              integer64 = "double",
              na.strings = c("",NA,"na"),
              stringsAsFactors = FALSE,
              colClasses = 'character',
              select = c("JobID", "JobDate", "UKSOCCode", "MinDegreeLevel")
              ) %>%
    clean_names
  
  print(i)
  print(nrow(y1) - n)
  
  y1 <- y1 %>%
    filter(uksoc_code != "na") %>%
    mutate(edu = as.numeric(min_degree_level)) %>%
    select(-min_degree_level)
  
  y1$edu[y1$edu == -999] <- NA
  
  y1 <- y1 %>%
    mutate(grab_date = ymd(job_date)) %>%
    mutate(year_month = paste0(year(grab_date),".", sprintf("%02d", month(grab_date)))) %>%
    select(-c(grab_date)) %>%
    distinct(job_id, .keep_all = T)
  
  y1 <- y1 %>%
    mutate(bach_or_higher = ifelse(edu >= 16,"above_col","below_col")) %>%
    group_by(year_month, uksoc_code, bach_or_higher) %>%
    summarise(n = n()) %>%
    group_by(year_month, uksoc_code) %>%
    pivot_wider(., names_from = bach_or_higher, values_from = n) %>%
    ungroup()
  
  y1$below_col[is.na(y1$below_col)] <- 0
  y1$above_col[is.na(y1$above_col)] <- 0
  
  y1 <- y1 %>%
    mutate(col_share = above_col/(above_col+below_col))
  
  return(y1)
})

remove(list = setdiff(ls(), "df"))

saveRDS(df, "./int_data/educ_regs/all_UK_aggregated_list.rds")

#### end ####

#### COMPARE ####
remove(list = ls())
setwd("/mnt/disks/pdisk/")

df_eu <- readRDS("./bg-eu/int_data/educ_regs/all_EU_aggregated_list.rds") %>% bind_rows() %>% mutate(year = as.numeric(str_sub(year_month, 1, 4))) %>% rename(na_col = `NA`)
df_eu_alt <- readRDS("./bg-eu/int_data/educ_regs/all_EU_alt_aggregated_list.rds") %>% bind_rows() %>% mutate(year = as.numeric(str_sub(year_month, 1, 4))) %>% rename(na_col = `NA`)
df_us <- readRDS("./bg-us/int_data/educ_regs/all_US_aggregated_list.rds") %>% bind_rows() %>% mutate(year = as.numeric(str_sub(year_month, 1, 4))) %>% rename(na_col = `NA`)
df_uk <- readRDS("./bg-uk/int_data/educ_regs/all_UK_aggregated_list.rds") %>% bind_rows() %>% mutate(year = as.numeric(str_sub(year_month, 1, 4))) %>% rename(na_col = `NA`)

colnames(df_eu)

df_eu[,c("below_col","above_col","na_col","col_share")][is.na(df_eu[,c("below_col","above_col","na_col","col_share")])] <- 0
df_eu_alt[,c("below_col","above_col","na_col","col_share")][is.na(df_eu_alt[,c("below_col","above_col","na_col","col_share")])] <- 0
df_us[,c("below_col","above_col","na_col","col_share")][is.na(df_us[,c("below_col","above_col","na_col","col_share")])] <- 0
df_uk[,c("below_col","above_col","na_col","col_share")][is.na(df_uk[,c("below_col","above_col","na_col","col_share")])] <- 0

df_eu <- df_eu %>% filter(year > 2013) %>% mutate(region = "eu") %>%
  rename(state = idcountry,
         isco_code = idesco_level_4) %>%
  filter(state != "UK")

df_eu_alt <- df_eu_alt %>% filter(year > 2013) %>% mutate(region = "eu_alt") %>%
  rename(state = idcountry,
         isco_code = idesco_level_4)

df_us <- df_us %>% filter(year > 2013) %>% mutate(region = "us")
df_uk <- df_uk %>% filter(year > 2013) %>% mutate(region = "uk")

df_eu_dist <- df_eu %>%
  filter(year == 2019) %>%
  group_by(state) %>%
  summarise(x = quantile(col_share, c(0.25, 0.5, 0.75)), q = c(0.25, 0.5, 0.75)) %>%
  group_by(state) %>%
  pivot_wider(., names_from = q, values_from = x)

df_eu_max <- df_eu %>%
  filter(year == 2019) %>%
  group_by(state) %>%
  summarise(max_col_share = max(col_share))

# Check distributions for EU and EU Alt
p1 = ggplot(df_eu %>% filter(year == 2019 & state %in% eu_countries[1:9] & col_share > 0.4), aes(x = col_share, color = state, fill = state)) +
  geom_density(size = 1, alpha = 0.10, adjust = 0.8, aes(weight = below_col+above_col+na_col)) +
  ggtitle("KDensity of College Share 2019 (Weighted) by EU State") +
  scale_x_continuous(breaks = seq(0,1,0.1)) +
  ylab("Density") +
  xlab("Share of Postings Requiring College or Higher") +
  scale_y_continuous() +
  theme(legend.position = "bottom") +
  labs(caption = "Note: Each Data Point is a (Month x ESCO 4 Digit Occupation x Region) cell.\nData is weighted by posting counts.")
p1

# Aggregated

df_eu <- df_eu %>%
  filter(state != "DE") %>%
  group_by(region, isco_code, year,  year_month) %>%
  summarise(below_col = sum(below_col, na.rm = T),
            above_col = sum(above_col, na.rm = T),
            na_col = sum(na_col, na.rm = T)) %>%
  ungroup() %>%
  mutate(col_share = above_col/(below_col+above_col)) %>%
  mutate(occ_code = as.character(isco_code)) %>%
  select(region, year,  year_month, occ_code, below_col, above_col, na_col, col_share)

df_eu_alt <- df_eu_alt %>%
  filter(state != "DE") %>%
  group_by(region, year,  isco_code, year_month) %>%
  summarise(below_col = sum(below_col, na.rm = T),
            above_col = sum(above_col, na.rm = T),
            na_col = sum(na_col, na.rm = T)) %>%
  ungroup() %>%
  mutate(col_share = above_col/(below_col+above_col)) %>%
  mutate(occ_code = as.character(isco_code)) %>%
  select(region, year,  year_month, occ_code, below_col, above_col, na_col, col_share)

df_us <- df_us %>% group_by(region, year,  soc, year_month) %>%
  summarise(below_col = sum(below_col, na.rm = T),
            above_col = sum(above_col, na.rm = T),
            na_col = sum(na_col, na.rm = T)) %>%
  ungroup() %>%
  mutate(col_share = above_col/(below_col+above_col)) %>%
  mutate(occ_code = as.character(soc)) %>%
  select(region, year,  year_month, occ_code, below_col, above_col, na_col, col_share)

df_uk <- df_uk %>% group_by(region, year,  uksoc_code, year_month) %>%
  summarise(below_col = sum(below_col, na.rm = T),
            above_col = sum(above_col, na.rm = T),
            na_col = sum(na_col, na.rm = T)) %>%
  ungroup() %>%
  mutate(col_share = above_col/(below_col+above_col)) %>%
  mutate(occ_code = as.character(uksoc_code)) %>%
  select(region, year, year_month, occ_code, below_col, above_col, na_col, col_share)

df <- bind_rows(df_eu, df_eu_alt, df_us, df_uk)

p = ggplot(df %>% filter(year == 2019), aes(x = col_share, color = region, fill = region)) +
  geom_density(size = 1, alpha = 0.05, adjust = 0.8, aes(weight = below_col+above_col+na_col)) +
  ggtitle("KDensity of College Share 2019 (Weighted)") +
  scale_x_continuous(breaks = seq(0,1,0.1)) +
  ylab("Density") +
  xlab("Share of Postings Requiring College or Higher") +
  scale_y_continuous() +
  theme(legend.position = "bottom") +
  labs(caption = "Note: Each Data Point is a (Month x ESCO 4 Digit Occupation x Region) cell.\nData is weighted by posting counts.")

p

p = ggplot(df %>% filter(year == 2018), aes(x = col_share, color = region, fill = region)) +
  geom_density(size = 1, alpha = 0.05, adjust = 0.8, aes(weight = below_col+above_col+na_col)) +
  ggtitle("KDensity of College Share 2019 (Unweighted)") +
  scale_x_continuous(breaks = seq(0,1,0.1)) +
  ylab("Density") +
  xlab("Share of Postings Requiring College or Higher") +
  scale_y_continuous() +
  theme(legend.position = "bottom") +
  labs(caption = "Note: Each Data Point is a (Month x ESCO 4 Digit Occupation x Region) cell.\nData is weighted by posting counts.")

# REGS
df <- bind_rows(df_eu_alt, df_us, df_uk) %>% mutate(month = str_sub(year_month, 6,7)) %>% mutate(weights = below_col+above_col+na_col) %>% filter(below_col+above_col > 0)

df <- df %>%
  mutate(occ_code2 = str_sub(occ_code, 1, 2),
         occ_code3 = str_sub(occ_code, 1, 3),
         occ_code4 = str_sub(occ_code, 1, 4))

head(df)

model.4 <- feols(fml = col_share ~ -1 | month + paste0(occ_code4,"_",region),
                 data = df)

model.4.w <- feols(fml = col_share ~ -1 | month + paste0(occ_code4,"_",region),
                 data = df,
                 weights = ~ weights)

model.3 <- feols(fml = col_share ~ -1 | month + paste0(occ_code3,"_",region),
                 data = df)

model.3.w <- feols(fml = col_share ~ -1 | month + paste0(occ_code3,"_",region),
                   data = df,
                   weights = ~ weights)

model.2 <- feols(fml = col_share ~ -1 | month + paste0(occ_code2,"_",region),
                 data = df)

model.2.w <- feols(fml = col_share ~ -1 | month + paste0(occ_code2,"_",region),
                   data = df,
                   weights = ~ weights)

df_ts <- df %>%
  mutate(model.4.resid = model.4$residuals,
         model.4.w.resid = model.4.w$residuals,
         model.3.resid = model.3$residuals,
         model.3.w.resid = model.3.w$residuals,
         model.2.resid = model.2$residuals,
         model.2.w.resid = model.2.w$residuals) %>%
  group_by(year_month, region) %>%
  summarise(mean.model.4.resid = mean(model.4.resid),
            mean.model.4.w.resid = mean(model.4.w.resid),
            mean.model.3.resid = mean(model.3.resid),
            mean.model.3.w.resid = mean(model.3.w.resid),
            mean.model.2.resid = mean(model.2.resid),
            mean.model.2.w.resid = mean(model.2.w.resid)) %>%
  ungroup() %>%
  mutate(date = as.Date(parse_date_time(year_month, "ym")))

p1 = ggplot(df_ts, aes(x = date, y = mean.model.2.resid, color = region)) +
  geom_line(span = 0.1) +
  scale_x_date(date_labels = "%b'%y", breaks = date_breaks("3 months"), minor_breaks = date_breaks("months")) +
  ggtitle("At-least Bachelor Requirement by Year-Month") +
  ylab("At-least Bachelors") +
  scale_y_continuous() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        legend.position = "bottom",
        axis.title.x=element_blank()) +
  guides(col = guide_legend(ncol = 3)) +
  labs(subtitle = "Residuals after projecting out region x 4 digit OCC and Monthly FEs")

p1
ggexport(p, filename = "./plots/educ_inflation/ts_residuals_bach_or_higher_by_esco1.pdf")

# BP REGS
df <- bind_rows(df_eu_alt, df_us, df_uk) %>% mutate(month = str_sub(year_month, 6,7)) %>% mutate(weights = below_col+above_col+na_col) %>% filter(below_col+above_col > 0)

n_distinct(df$year_month)

df <- df %>%
  mutate(occ_code2 = str_sub(occ_code, 1, 2),
         occ_code3 = str_sub(occ_code, 1, 3),
         occ_code4 = str_sub(occ_code, 1, 4))
nrow(df) # 133,619
df <- df %>% group_by(region, occ_code) %>% filter(n() == 84) %>% ungroup()
nrow(df) # 120,876

df <- df %>% filter(!(year_month == "2018.04" & region == "eu_alt"))

df <- df %>%
  group_by(region, year,  occ_code4, year_month) %>%
  summarise(below_col = sum(below_col, na.rm = T),
            above_col = sum(above_col, na.rm = T),
            na_col = sum(na_col, na.rm = T)) %>%
  ungroup() %>%
  mutate(col_share = above_col/(below_col+above_col))



nrow(df) # 120,488

model.4.bp <- feols(fml = col_share ~ month |  paste0(occ_code4,"_",region),
                 data = df)
summary(model.4.bp)

model.4.w.bp <- feols(fml = col_share ~ month | paste0(occ_code4,"_",region),
                   data = df,
                   weights = ~ weights)
summary(model.4.w.bp)

model.3.bp <- feols(fml = col_share ~ month | paste0(occ_code3,"_",region),
                 data = df)

model.3.w.bp <- feols(fml = col_share ~ month | paste0(occ_code3,"_",region),
                   data = df,
                   weights = ~ weights)

model.2.bp <- feols(fml = col_share ~ month | paste0(occ_code2,"_",region),
                 data = df)

model.2.w.bp <- feols(fml = col_share ~ month | paste0(occ_code2,"_",region),
                   data = df,
                   weights = ~ weights)
summary(model.2.bp)

sum(model.2.bp$residuals - model.4.bp$residuals)
sum(model.3.bp$residuals - model.4.bp$residuals)

df_ts <- df %>%
  mutate(model.4.bp.resid = model.4.bp$residuals,
         model.4.w.bp.resid = model.4.w.bp$residuals,
         model.3.bp.resid = model.3.bp$residuals,
         model.3.w.bp.resid = model.3.w.bp$residuals,
         model.2.bp.resid = model.2.bp$residuals,
         model.2.w.bp.resid = model.2.w.bp$residuals) %>%
  group_by(year_month, region) %>%
  summarise(mean.model.4.bp.resid = mean(model.4.bp.resid),
            mean.model.4.w.bp.resid = mean(model.4.w.bp.resid),
            mean.model.3.bp.resid = mean(model.3.bp.resid),
            mean.model.3.w.bp.resid = mean(model.3.w.bp.resid),
            mean.model.2.bp.resid = mean(model.2.bp.resid),
            mean.model.2.w.bp.resid = mean(model.2.w.bp.resid)) %>%
  ungroup() %>%
  mutate(date = as.Date(parse_date_time(year_month, "ym")))



p1 = ggplot(df_ts, aes(x = date, y = mean.model.4.bp.resid, color = region)) +
  geom_line(span = 0.1) +
  scale_x_date(date_labels = "%b'%y", breaks = date_breaks("3 months"), minor_breaks = date_breaks("months")) +
  ggtitle("Educ Share Residual Time Series (Month & OCC 4-Digit FEs, Unweighted)") +
  ylab("At-least Bachelors") +
  scale_y_continuous() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        legend.position = "bottom",
        axis.title.x=element_blank()) +
  guides(col = guide_legend(ncol = 3)) +
  labs(subtitle = "Note: We drop April 2018 for EU Alternative.")
p1
ggexport(p1, filename = "./bg_combined/plots/ts_residuals_occ4_eu_alt_uk_us.pdf")

p2 = ggplot(df_ts, aes(x = date, y = mean.model.3.bp.resid, color = region)) +
  geom_line(span = 0.1) +
  scale_x_date(date_labels = "%b'%y", breaks = date_breaks("3 months"), minor_breaks = date_breaks("months")) +
  ggtitle("Educ Share Residual Time Series (Month & OCC 3-Digit FEs, Unweighted)") +
  ylab("At-least Bachelors") +
  scale_y_continuous() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        legend.position = "bottom",
        axis.title.x=element_blank()) +
  guides(col = guide_legend(ncol = 3)) +
  labs(subtitle = "Note: We drop April 2018 for EU Alternative.")
p2
ggexport(p2, filename = "./bg_combined/plots/ts_residuals_occ3_eu_alt_uk_us.pdf")

p3 = ggplot(df_ts, aes(x = date, y = mean.model.2.bp.resid, color = region)) +
  geom_line(span = 0.1) +
  scale_x_date(date_labels = "%b'%y", breaks = date_breaks("3 months"), minor_breaks = date_breaks("months")) +
  ggtitle("Educ Share Residual Time Series (Month & OCC 2-Digit FEs, Unweighted)") +
  ylab("At-least Bachelors") +
  scale_y_continuous() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        legend.position = "bottom",
        axis.title.x=element_blank()) +
  guides(col = guide_legend(ncol = 3)) +
  labs(subtitle = "Note: We drop April 2018 for EU Alternative.")
p3
ggexport(p3, filename = "./bg_combined/plots/ts_residuals_occ2_eu_alt_uk_us.pdf")


### DIFF REGS 

df <- bind_rows(df_eu_alt, df_us, df_uk) %>% mutate(month = str_sub(year_month, 6,7)) %>% mutate(weights = below_col+above_col+na_col) %>% filter(below_col+above_col > 0)

n_distinct(df$year_month)

df <- df %>%
  mutate(occ_code2 = str_sub(occ_code, 1, 2),
         occ_code3 = str_sub(occ_code, 1, 3),
         occ_code4 = str_sub(occ_code, 1, 4))
nrow(df) # 133,619
df <- df %>% group_by(region, occ_code) %>% filter(n() == 84) %>% ungroup()
nrow(df) # 120,876

df <- df %>%
  group_by(region, year,  occ_code4, year_month) %>%
  summarise(below_col = sum(below_col, na.rm = T),
            above_col = sum(above_col, na.rm = T),
            na_col = sum(na_col, na.rm = T)) %>%
  ungroup() %>%
  mutate(col_share = above_col/(below_col+above_col)) %>%
  arrange(region, occ_code4, year_month)

nrow(df) # 66,276

df <- df %>%
  group_by(region, occ_code4) %>%
  mutate(col_share_m2m_diff = col_share - lag(col_share)) %>%
  ungroup() %>%
  mutate(month = str_sub(year_month, 6,7)) %>%
  filter(!is.na(col_share_m2m_diff))

df <- df %>% filter(!(year_month == "2018.04"))

nrow(df) # 64,698

model.diff.bp <- feols(fml = col_share_m2m_diff ~ month,
                       data = df)

df_ts <- df %>%
  mutate(model.diff.bp.resid = model.diff.bp$residuals) %>%
  group_by(year_month, region, year) %>%
  summarise(mean.model.diff.bp.resid = mean(model.diff.bp.resid)) %>%
  ungroup() %>%
  mutate(date = as.Date(parse_date_time(year_month, "ym")))

p1 = ggplot(df_ts %>% filter(year > 2018), aes(x = date, y = mean.model.diff.bp.resid, color = region)) +
  geom_line(span = 0.1) +
  scale_x_date(date_labels = "%b'%y", breaks = date_breaks("3 months"), minor_breaks = date_breaks("months")) +
  ggtitle("Educ Share Change Residual Time Series (Month FEs, Unweighted)") +
  ylab("At-least Bachelors") +
  scale_y_continuous() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        legend.position = "bottom",
        axis.title.x=element_blank()) +
  guides(col = guide_legend(ncol = 3)) +
  labs(subtitle = "Note: We drop April 2018.")
p1
ggexport(p1, filename = "./bg_combined/plots/ts_residuals_occ4_eu_alt_uk_us.pdf")
