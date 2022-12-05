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
library("refinr")
library(stringi)

library(ggplot2)
library(scales)
library("ggpubr")
devtools::install_github('Mikata-Project/ggthemr')
library(ggthemr)

library(foreach)
library(doParallel)

setwd("/mnt/disks/pdisk/bg-eu/")

#################################
# EU PRIMARY #
#################################

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


# View(fread(file_names$file_names[file_names$type == "postings" & file_names$country == countries[[i]]][1], nrow = 10))

#### COMPILE INCOME DISTRIBUTIONS ####
#### compile data ####
setMKLthreads(1)
registerDoParallel(16)

df <- foreach (i=1:m) %dopar% {
  
  y1 <- fread(file_names$file_names[file_names$type == "postings"][i], data.table = F, nThread = 2, select = c("general_id","year_grab_date","month_grab_date","day_grab_date",
                                                                                                               "ideducational_level", "educational_level", "idregion", "region", "idprovince", "province",
                                                                                                               "idesco_level_4", "esco_level_4","idmacro_sector", "macro_sector", "category_sector",
                                                                                                               "idcountry", "companyname")) %>%
    clean_names
  
  y1[y1 == ""] <- NA
  y1 <- y1 %>% filter(!is.na(ideducational_level))
  
  y1 <- y1 %>%
    mutate(grab_date = dmy(paste0(day_grab_date,"/",month_grab_date,"/",year_grab_date))) %>%
    mutate(year_month = paste0(year(grab_date),".", sprintf("%02d", month(grab_date)))) %>%
    select(-c(day_grab_date, month_grab_date, year_grab_date)) %>%
    distinct(general_id, .keep_all = T)
  
  y1 <- y1 %>%
    mutate(companyname_clean = str_replace_all(companyname, "\\(.*\\)", "")) %>%
    mutate(companyname_clean = str_replace_all(companyname_clean, ",", "")) %>%
    mutate(companyname_clean = str_replace_all(companyname_clean, "\\.", "")) %>%
    mutate(companyname_clean = str_replace_all(companyname_clean, "&", " AND ")) %>%
    mutate(companyname_clean = str_replace_all(companyname_clean, "'", "")) %>%
    mutate(companyname_clean = str_replace_all(companyname_clean, '[[:punct:] ]+', ' ')) %>%
    mutate(companyname_clean = toupper(companyname_clean)) %>%
    mutate(companyname_clean = str_squish(companyname_clean)) %>%
    mutate(companyname_clean = str_replace_all(companyname_clean, "[^[:alnum:][:space:]]", ""))
  
  y1$companyname_merged <- NA
  
  #y1$companyname_merged[!is.na(y1$companyname_clean)] <- n_gram_merge(y1$companyname_clean[!is.na(y1$companyname_clean)],
  #                                                             numgram = 2,
  #                                                             bus_suffix = TRUE,
  #                                                             edit_threshold = 1,
  #                                                             weight = c(d = 0.33, i = 0.33, s = 1, t = 0.5))
  
  saveRDS(y1,
          paste0("./int_data/bg_eu_int_educ/eu_alt_",
                 file_names$country[file_names$type == "postings"][i],"_",
                 file_names$year[file_names$type == "postings"][i],".rds"))
  
}
remove(list = ls())
df <- list.files("./int_data/bg_eu_int_educ/", "*.rds", full.names = T) %>%
  mclapply(readRDS, mc.cores = 32) %>%
  bind_rows

df_plot <- df %>%
  group_by(year_month) %>%
  summarise(n = n()/1000) %>%
  mutate(date = as.Date(parse_date_time(year_month, "ym")))

### FOR SH ###
# Make Plots
ggthemr('flat')
p <- ggplot(df_plot, aes(x = date, y = n)) +
  geom_line(se = F, span = 0.5, alpha = 0.5, size = 0.75) +
  geom_point(size = 2.5) +
  ylab("Job Postings (1,000)") +
  xlab("Date") +
  scale_y_continuous(breaks = seq(0,3000,250)) +
  scale_x_date(date_labels = "%b'%y", breaks = date_breaks("6 months")) +
  scale_colour_ggthemr_d() +
  ggtitle("Job Postings in EU") +
  theme(legend.title=element_blank()) +
  labs(caption = "Source: Web-scraped Job Ads from EU27 Countries, provided by Burning Glass Technologies.") +
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))
ggexport(p, filename = "bg_count_e27_job_vacancy_postings_by_month.pdf")
### END ###

nrow(df) # 120,337,388

table(df$year_month) # 120,337,388

df <- df %>%
  mutate(bach_or_higher = ifelse(ideducational_level >= 6, 1, 0))

df <- df %>% mutate(quarter = quarter(grab_date),
                    year = year(grab_date)) %>%
  mutate(year_quarter = paste0(year,".",quarter))

df <- df %>% mutate(month = month(grab_date))

head(df)

df <- df %>% mutate(covid = case_when(
  year_month %in% c("2020.03", "2020.04", "2020.05") ~ "2early",
  year_month %in% c("2020.06", "2020.07", "2020.08") ~ "3earlymid",
  year_month %in% c("2020.09", "2020.10", "2020.11") ~ "4mid",
  year_month %in% c("2020.12", "2021.01", "2021.02", "2021.03") ~ "5late",
  TRUE ~ "1pre"
))

#### DEEMEAN ####

remove(list = setdiff(ls(),"df"))


lm1 <- feols(bach_or_higher ~ covid | month + idcountry, df, cluster = ~ idcountry)
lm2 <- feols(bach_or_higher ~ covid | month + idcountry + idesco_level_4, df, cluster = ~ idcountry)
lm3 <- feols(bach_or_higher ~ covid | month + idcountry + idmacro_sector, df, cluster = ~ idcountry)
lm4 <- feols(bach_or_higher ~ covid | month + idprovince, df, cluster = ~ idcountry)
lm5 <- feols(bach_or_higher ~ covid | month + idprovince + idesco_level_4 + idmacro_sector, df, cluster = ~ idcountry)

### FIRMS
df_firms <- df %>%
  filter(!is.na(companyname_clean)) %>%
  group_by(companyname_clean) %>%
  filter(n() < 20000 & n() > 50) %>%
  ungroup()

lm1_firms <- feols(bach_or_higher ~ covid | month + companyname_clean, df_firms, cluster = ~ companyname_clean)

#### EXPORT RESULTS ####

etable(lm1, lm2, lm3, lm4, lm5, lm1_firms,
       tex = TRUE,
       title = "Demand for College Education or Higher",
       digits = 3,
       digits.stats = 4
)

###################



#################################
# EU ALTERNATIVE #
#################################

#### LOAD DATA ####
remove(list = ls())
file_names <- list.files("./bg-eu-bucket/alternative/", "*.csv", full.names = F)
file_names <- as.data.frame(file_names) %>% separate(file_names, sep = " ", c("indexdata", "type", "country", "file"), remove = F) %>% select(-indexdata)
file_names$file_names <- as.data.frame(paths <- list.files("./bg-eu-bucket/alternative", "*.csv", full.names = T))$paths
file_names <- file_names %>% mutate(year = str_sub(parse_number(file), 1, 4)) %>% filter(type != "skills")

table(file_names$year)
m <- nrow(file_names)

# View(fread(file_names$file_names[file_names$type == "postings" & file_names$country == countries[[i]]][1], nrow = 10))

#### COMPILE INCOME DISTRIBUTIONS ####
#### compile data ####
setMKLthreads(1)
registerDoParallel(32)

df <- foreach (i=1:m) %dopar% {
  
  y1 <- fread(file_names$file_names[file_names$type == "postings"][i], data.table = F, nThread = 2, select = c("general_id","year_grab_date","month_grab_date","day_grab_date",
                                                             "ideducational_level", "educational_level", "idregion", "region", "idprovince", "province",
                                                             "idesco_level_4", "esco_level_4","idmacro_sector", "macro_sector", "category_sector",
                                                             "idcountry", "companyname")) %>%
    clean_names
  
  y1[y1 == ""] <- NA
  y1 <- y1 %>% filter(!is.na(ideducational_level))
  
  y1 <- y1 %>%
    mutate(grab_date = dmy(paste0(day_grab_date,"/",month_grab_date,"/",year_grab_date))) %>%
    mutate(year_month = paste0(year(grab_date),".", sprintf("%02d", month(grab_date)))) %>%
    select(-c(day_grab_date, month_grab_date, year_grab_date)) %>%
    distinct(general_id, .keep_all = T)
  
  y1 <- y1 %>%
    mutate(companyname_clean = str_replace_all(companyname, "\\(.*\\)", "")) %>%
    mutate(companyname_clean = str_replace_all(companyname_clean, ",", "")) %>%
    mutate(companyname_clean = str_replace_all(companyname_clean, "\\.", "")) %>%
    mutate(companyname_clean = str_replace_all(companyname_clean, "&", " AND ")) %>%
    mutate(companyname_clean = str_replace_all(companyname_clean, "'", "")) %>%
    mutate(companyname_clean = str_replace_all(companyname_clean, '[[:punct:] ]+', ' ')) %>%
    mutate(companyname_clean = toupper(companyname_clean)) %>%
    mutate(companyname_clean = str_squish(companyname_clean)) %>%
    mutate(companyname_clean = str_replace_all(companyname_clean, "[^[:alnum:][:space:]]", ""))
  
  y1$companyname_merged <- NA
  
  #y1$companyname_merged[!is.na(y1$companyname_clean)] <- n_gram_merge(y1$companyname_clean[!is.na(y1$companyname_clean)],
  #                                                             numgram = 2,
  #                                                             bus_suffix = TRUE,
  #                                                             edit_threshold = 1,
  #                                                             weight = c(d = 0.33, i = 0.33, s = 1, t = 0.5))
  
  saveRDS(y1,
          paste0("./int_data/bg_eu_int_educ/eu_alt_",
                 file_names$country[file_names$type == "postings"][i],"_",
                 file_names$year[file_names$type == "postings"][i],".rds"))
  
}

df <- list.files("./int_data/bg_eu_int_educ/", "*.rds", full.names = T) %>%
  mclapply(readRDS, mc.cores = 4) %>%
  bind_rows

nrow(df) # 120,337,388

View(df[1:1000,])

df$companyname_merged <- key_collision_merge(df$companyname_clean)

n_distinct(df$companyname_merged) # 1,793,120
n_distinct(df$companyname_clean) # 1,804,387

df <- df %>%
  arrange(companyname_merged, year_month)

test <- df %>%
  group_by(companyname_merged) %>%
  filter(any(companyname_merged != companyname_clean))

test <- test %>%
  group_by(companyname, companyname_clean, companyname_merged) %>%
  summarise(n = n())
  
colnames(test)


table(df$year_month) # 120,337,388

df <- df %>%
  mutate(bach_or_higher = ifelse(ideducational_level >= 6, 1, 0))

df <- df %>% mutate(quarter = quarter(grab_date),
                    year = year(grab_date)) %>%
  mutate(year_quarter = paste0(year,".",quarter))

df <- df %>% mutate(month = month(grab_date))

head(df)

df <- df %>% mutate(covid = case_when(
  year_month %in% c("2020.03", "2020.04", "2020.05") ~ "2early",
  year_month %in% c("2020.06", "2020.07", "2020.08") ~ "3earlymid",
  year_month %in% c("2020.09", "2020.10", "2020.11") ~ "4mid",
  year_month %in% c("2020.12", "2021.01", "2021.02", "2021.03") ~ "5late",
  TRUE ~ "1pre"
))

trend <- data.frame("year_month" = c("2014.01", "2014.02", "2014.03", "2014.04", "2014.05", "2014.06", "2014.07", "2014.08", "2014.09", "2014.10", "2014.11", "2014.12", "2015.01", "2015.02", "2015.03", "2015.04", "2015.05", "2015.06", "2015.07", "2015.08", "2015.09", "2015.10", "2015.11", "2015.12", "2016.01", "2016.02", "2016.03", "2016.04", "2016.05", "2016.06", "2016.07", "2016.08", "2016.09", "2016.10", "2016.11", "2016.12", "2017.01", "2017.02", "2017.03", "2017.04", "2017.05", "2017.06", "2017.07", "2017.08", "2017.09", "2017.10", "2017.11", "2017.12", "2018.01", "2018.02", "2018.03", "2018.04", "2018.05", "2018.06", "2018.07", "2018.08", "2018.09", "2018.10", "2018.11", "2018.12", "2019.01", "2019.02", "2019.03", "2019.04", "2019.05", "2019.06", "2019.07", "2019.08", "2019.09", "2019.10", "2019.11", "2019.12", "2020.01", "2020.02", "2020.03", "2020.04", "2020.05", "2020.06", "2020.07", "2020.08", "2020.09", "2020.10", "2020.11", "2020.12", "2021.01", "2021.02", "2021.03"),
                    "month_trend" = c(1:87))

df <- df %>%
  left_join(trend)

#### DEEMEAN ####

head(df)

remove(list = setdiff(ls(),"df"))

lm1 <- feols(bach_or_higher ~ covid + month_trend | month + idcountry, df, cluster = ~ idcountry, lean = T)
lm2 <- feols(bach_or_higher ~ covid + month_trend | month + idcountry + idesco_level_4, df, cluster = ~ idcountry, lean = T)
lm3 <- feols(bach_or_higher ~ covid + month_trend | month + idcountry + idmacro_sector, df, cluster = ~ idcountry, lean = T)
lm4 <- feols(bach_or_higher ~ covid + month_trend | month + idprovince, df, cluster = ~ idcountry, lean = T)
lm5 <- feols(bach_or_higher ~ covid + month_trend | month + idprovince + idesco_level_4 + idmacro_sector, df, cluster = ~ idcountry, lean = T)

### FIRMS
df_firms <- df %>%
  filter(!is.na(companyname_clean)) %>%
  group_by(companyname_clean) %>%
  filter(n() > 100) %>%
  ungroup()

lm1_firms <- feols(bach_or_higher ~ covid + month_trend | month + companyname_clean, df_firms, cluster = ~ companyname_clean, lean = T)

#### EXPORT RESULTS ####

etable(lm1, lm2, lm3, lm4, lm5, lm1_firms,
       tex = TRUE,
       title = "Demand for College Education or Higher",
       digits = 3,
       digits.stats = 4
)


lm5 <- feols(bach_or_higher ~ covid + month_trend + month_trend^2 | month + idprovince + idesco_level_4 + idmacro_sector, df, cluster = ~ idcountry, lean = T)
saveRDS(lm5, "./int_data/fe_model_2rd_order_trend.rds")
summary(lm5)

###################


#################################
# USA #
#################################

#### LOAD DATA ####
setwd("/mnt/disks/pdisk/bg-us/")
remove(list = ls())
file_names <- list.files("./raw_data/main", "*.txt", full.names = F)
file_names <- as.data.frame(file_names) %>% separate(file_names, sep = "_", c("type", "year_month"), remove = F)
file_names$file_names <- as.data.frame(paths <- list.files("./raw_data/main", "*.txt", full.names = T))$paths
file_names <- file_names %>% mutate(year = as.numeric(str_sub(year_month, 1, 4)))
file_names <- file_names %>% filter(type == "Main" & year >= 2021)
file_names <- file_names %>% mutate(year_month = str_sub(year_month, 1, -5))
head(file_names)

m <- length(file_names$file_names)

table(file_names$year)
m <- nrow(file_names)

# View(fread(file_names$file_names[file_names$type == "postings" & file_names$country == countries[[i]]][1], nrow = 10))

#### COMPILE INCOME DISTRIBUTIONS ####
#### compile data ####
i = 20
setMKLthreads(1)
registerDoParallel(6)

df <- foreach (i=1:m) %dopar% {
  
  y1 <- fread(file_names$file_names[i], data.table = F, nThread = 1, colClasses = "character", stringsAsFactors = FALSE,
              select = c("BGTJobId", "JobDate", "SOC", "SOCName", "Employer", "Sector", "SectorName", "City", "State", "County", "MSA", "Edu", "Degree", "Exp")) %>%
    clean_names
  
  y1[y1 == ""] <- NA
  y1[y1 == "na"] <- NA
  
  tail(y1, 40)
  
  y1 <- y1 %>%
    mutate(grab_date = ymd(job_date)) %>%
    mutate(year_month = paste0(year(grab_date),".", sprintf("%02d", month(grab_date)))) %>%
    select(-c(job_date)) %>%
    distinct(bgt_job_id, .keep_all = T)
  
  y1 <- y1 %>%
    mutate(companyname_clean = str_replace_all(employer, "\\(.*\\)", "")) %>%
    mutate(companyname_clean = str_replace_all(companyname_clean, ",", "")) %>%
    mutate(companyname_clean = str_replace_all(companyname_clean, "\\.", "")) %>%
    mutate(companyname_clean = str_replace_all(companyname_clean, "&", " AND ")) %>%
    mutate(companyname_clean = str_replace_all(companyname_clean, "'", "")) %>%
    mutate(companyname_clean = str_replace_all(companyname_clean, '[[:punct:] ]+', ' ')) %>%
    mutate(companyname_clean = toupper(companyname_clean)) %>%
    mutate(companyname_clean = str_squish(companyname_clean)) %>%
    mutate(companyname_clean = str_replace_all(companyname_clean, "[^[:alnum:][:space:]]", ""))
  
  #y1$companyname_merged[!is.na(y1$companyname_clean)] <- n_gram_merge(y1$companyname_clean[!is.na(y1$companyname_clean)],
  #                                                             numgram = 2,
  #                                                             bus_suffix = TRUE,
  #                                                             edit_threshold = 1,
  #                                                             weight = c(d = 0.33, i = 0.33, s = 1, t = 0.5))
  
  saveRDS(y1,
          paste0("./int_data/bg_us_int_educ/usa_",
                 file_names$year_month[i],".rds"))
  
}
remove(list = ls())

df <- list.files("./int_data/bg_us_int_educ/", "*.rds", full.names = T)

df <- df[grepl("2018|2019|2020|2021", df)]

df <- df %>%
  mclapply(readRDS, mc.cores = 32) %>%
  bind_rows

sum(df$msa == "-999", na.rm = T) # 7,799,596
# sum(df$edu == "-999", na.rm = T) # 55,855,571
sum(is.na(df$employer), na.rm = T) # 7,799,596

table(df$edu)

remove(list = setdiff(ls(),"df"))

df <- df %>% 
  filter(msa != "-999" & !is.na(employer))

df <- df %>% 
  mutate(missing_edu = ifelse(edu == "-999", 1, 0))

df <- df %>%
  mutate(bach_or_higher = ifelse(edu >= 16, 1, 0))

df <- df %>% mutate(quarter = quarter(grab_date),
                    year = year(grab_date)) %>%
  mutate(year_quarter = paste0(year,".",quarter)) %>%
  mutate(month = month(grab_date))

#df <- df %>% mutate(covid = case_when(
#  year_month %in% c("2020.03", "2020.04", "2020.05") ~ "2early",
#  year_month %in% c("2020.06", "2020.07", "2020.08") ~ "3earlymid",
#  year_month %in% c("2020.09", "2020.10", "2020.11") ~ "4mid",
#  year_month %in% c("2020.12", "2021.01", "2021.02", "2021.03") ~ "5late",
#  TRUE ~ "1pre"
#))

df <- df %>% mutate(covid = case_when(
  year_month %in% c("2020.03", "2020.04", "2020.05") ~ "2early",
  year_month %in% c("2020.06", "2020.07", "2020.08", "2020.09", "2020.10") ~ "3mid",
  year_month %in% c("2020.11", "2020.12", "2021.01", "2021.02", "2021.03") ~ "4late",
  TRUE ~ "1pre"
))

trend <- data.frame("year_month" = c("2014.01", "2014.02", "2014.03", "2014.04", "2014.05", "2014.06", "2014.07", "2014.08", "2014.09", "2014.10", "2014.11", "2014.12",
                                     "2015.01", "2015.02", "2015.03", "2015.04", "2015.05", "2015.06", "2015.07", "2015.08", "2015.09", "2015.10", "2015.11", "2015.12",
                                     "2016.01", "2016.02", "2016.03", "2016.04", "2016.05", "2016.06", "2016.07", "2016.08", "2016.09", "2016.10", "2016.11", "2016.12",
                                     "2017.01", "2017.02", "2017.03", "2017.04", "2017.05", "2017.06", "2017.07", "2017.08", "2017.09", "2017.10", "2017.11", "2017.12",
                                     "2018.01", "2018.02", "2018.03", "2018.04", "2018.05", "2018.06", "2018.07", "2018.08", "2018.09", "2018.10", "2018.11", "2018.12",
                                     "2019.01", "2019.02", "2019.03", "2019.04", "2019.05", "2019.06", "2019.07", "2019.08", "2019.09", "2019.10", "2019.11", "2019.12",
                                     "2020.01", "2020.02", "2020.03", "2020.04", "2020.05", "2020.06", "2020.07", "2020.08", "2020.09", "2020.10", "2020.11", "2020.12",
                                     "2021.01", "2021.02", "2021.03", "2021.04", "2021.05", "2021.06", "2021.07", "2021.08", "2021.09", "2021.10", "2021.11", "2021.12"),
                    "month_trend" = c(1:96))
df <- df %>%
  left_join(trend)

#### DEEMEAN ####

remove(list = setdiff(ls(),"df"))

lm1 <- feols(bach_or_higher ~ covid + month_trend | month + state, df %>% filter(missing_edu == 0), cluster = ~ year_month, lean = T)
lm2 <- feols(bach_or_higher ~ covid + month_trend | month + state + soc, df %>% filter(missing_edu == 0), cluster = ~ year_month, lean = T)
lm3 <- feols(bach_or_higher ~ covid + month_trend | month + state + sector, df %>% filter(missing_edu == 0), cluster = ~ year_month, lean = T)
lm4 <- feols(bach_or_higher ~ covid + month_trend | month + msa, df %>% filter(missing_edu == 0), cluster = ~ year_month, lean = T)
lm5 <- feols(bach_or_higher ~ covid + month_trend | month + msa + soc + sector, df %>% filter(missing_edu == 0), cluster = ~ year_month, lean = T)

summary(lm1)
summary(lm2)
summary(lm3)
summary(lm4)
summary(lm5)

### FIRMS
df_firms <- df %>%
  filter(!is.na(companyname_clean)) %>%
  group_by(companyname_clean) %>%
  filter(n() > 100) %>%
  ungroup()

lm1_firms <- feols(bach_or_higher ~  covid + month_trend | month + companyname_clean, df_firms, cluster = ~ companyname_clean)

#### EXPORT RESULTS ####

etable(lm1, lm2, lm3, lm4, lm5, lm1_firms,
       tex = TRUE,
       title = "Demand for College Education or Higher",
       digits = 3,
       digits.stats = 4
)

###################

