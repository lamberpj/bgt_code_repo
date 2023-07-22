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

# Translation packages
library("rvest")
library("googleLanguageR")
library("cld2")
library("datasets")
library("vroom")

setwd("/mnt/disks/pdisk/bg-eu/")

#### LOAD DATA ####
remove(list = ls())
file_names <- list.files("./bg-eu-bucket/new/structured", "*.csv", full.names = F)
file_names <- as.data.frame(file_names) %>% separate(file_names, sep = " ", c("type", "country", "file"), remove = F)
file_names$file_names <- as.data.frame(paths <- list.files("./bg-eu-bucket/new/structured", "*.csv", full.names = T))$paths
head(file_names)
file_names <- file_names %>% group_by(type, country) %>% mutate(n = n())

colnames(file_names)
countries <- file_names$country %>% unique()

m <- length(countries)

i = 12

colnames(fread(file_names$file_names[file_names$type == "postings" & file_names$country == countries[[i]]][1], nrow = 10))

#### COMPILE INCOME DISTRIBUTIONS ####
#### compile data ####
df <- mclapply(1:m, function(i) {
  y1 <- lapply(file_names$file_names[file_names$type == "postings" & file_names$country == countries[[i]]], function(x) {
    fread(paste0(x), data.table = F, nThread = 1, select = c("general_id","year_grab_date","month_grab_date","day_grab_date",
                                                             "idexperience", "experience", "idregion", "region", "idesco_level_4", "esco_level_4",
                                                             "idmacro_sector", "macro_sector", "category_sector", "idcountry", "companyname"))}) %>%
    bind_rows %>%
    clean_names
  
  y1[y1 == ""] <- NA
  y1 <- y1 %>% filter(!is.na(idexperience))
  
  y1 <- y1 %>%
    mutate(grab_date = dmy(paste0(day_grab_date,"/",month_grab_date,"/",year_grab_date))) %>%
    mutate(year_month = paste0(year(grab_date),".", sprintf("%02d", month(grab_date)))) %>%
    select(-c(day_grab_date, month_grab_date, year_grab_date)) %>%
    distinct(general_id, .keep_all = T)
}, mc.cores = 16)

df <- bind_rows(df)
nrow(df) # 43,618,226

table(df$working_hours, df$idworking_hours)

df <- df %>% distinct(general_id, .keep_all = T)
df <- df %>% mutate(date = as.Date(parse_date_time(year_month, "ym")))
df <- df %>% filter(as.numeric(str_sub(year_month, 1, 4)) > 2017)
df <- df %>% mutate(covid = case_when(
  str_sub(year_month, 1, 4) == "2019" | year_month  %in% c("2020.01", "2020.02") ~ "1pre",
  year_month %in% c("2020.03", "2020.04", "2020.05") ~ "2early",
  year_month %in% c("2020.06", "2020.07", "2020.08") ~ "3earlymid",
  year_month %in% c("2020.09", "2020.10", "2020.11") ~ "4mid",
  year_month %in% c("2020.12", "2021.01", "2021.02", "2021.03") ~ "5late"
))
df <- df %>% mutate(month = month(date))
table(df$month)
table(df$covid)
remove(list = setdiff(ls(),"df"))

table(df$experience, df$idexperience)

df <- df %>%
  mutate(exp_years = case_when(
    experience == "No experience" ~ 0,
    experience == "Up to 1 year" ~ 1,
    experience == "From 1 to 2 years" ~ 2,
    experience == "From 2 to 4 years" ~ 4,
    experience == "From 4 to 6 years" ~ 6,
    experience == "From 6 to 8 years" ~ 8,
    experience == "From 8 to 10 years" ~ 10,
    experience == "Over 10 years" ~ 12
  ))

#### DEEMEAN ####
df1 <- df
df2 <- df %>% filter(!is.na(idesco_level_4))
df3 <- df %>% filter(!is.na(idesco_level_4) & !is.na(idmacro_sector))
df4 <- df %>% filter(!is.na(idesco_level_4) & !is.na(idmacro_sector) & !is.na(idregion))

lm1 <- feols(exp_years ~ covid + as.factor(month) | idcountry, df1)
lm2 <- feols(exp_years ~ covid + as.factor(month) | idcountry + idesco_level_4, df2)
lm3 <- feols(exp_years ~ covid + as.factor(month) | idcountry + idesco_level_4 + idmacro_sector, df3)
lm4 <- feols(exp_years ~ covid + as.factor(month) | idcountry + idesco_level_4 + idmacro_sector + idregion, df4)

summary(lm1)
summary(lm2)
summary(lm3)
summary(lm4)

df1$resid <- lm1$residuals
df2$resid <- lm2$residuals
df3$resid <- lm3$residuals
df4$resid <- lm4$residuals

df1 <- df1 %>% group_by(year_month) %>% summarise(mean_resid = mean(resid)) %>% mutate(date = as.Date(parse_date_time(year_month, "ym")))

p1 <- ggplot(df1, aes(x = date, y = mean_resid)) +
  geom_smooth() +
  scale_x_date(date_labels = "%b'%y", breaks = date_breaks("3 months"), minor_breaks = date_breaks("months"))
p1

df2 <- df2 %>% group_by(year_month) %>% summarise(mean_resid = mean(resid)) %>% mutate(date = as.Date(parse_date_time(year_month, "ym")))

p2 <- ggplot(df2, aes(x = date, y = mean_resid)) +
  geom_smooth() +
  scale_x_date(date_labels = "%b'%y", breaks = date_breaks("3 months"), minor_breaks = date_breaks("months"))
p2










