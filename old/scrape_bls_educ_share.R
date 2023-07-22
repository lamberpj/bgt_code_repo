#### SETUP ####
options(repos = c(CRAN = "https://mran.revolutionanalytics.com/snapshot/2021-04-01"))

#### CLEAR ENVIRONMENT ####
remove(list=ls())

options(scipen=999)

#### PACKAGES ####
library("janitor")
library("vroom")
library("usethis")
library("devtools")
library("tidyverse")
library("haven")
library("gtools")
library("foreign")
library("data.table")
library("readr")
library("lubridate")
library("vroom")
library("data.table")
library("foreach")
library("doParallel")
library("rlang")
library("fixest")
library("modelsummary")
library("kableExtra")
library("DescTools")
library("fixest")
library("ggpubr")
library("stringi")
library(data.table)
library(ggplot2)
library(scales)
library("ggpubr")
devtools::install_github('Mikata-Project/ggthemr')
library(ggthemr)
library(rvest)

ggthemr('flat')

remove(list = ls())
setwd("/mnt/disks/pdisk/bg-us/")


url_list <- c("https://www.bls.gov/emp/tables/educational-attainment.htm","https://web.archive.org/web/20181006012600/https://www.bls.gov/emp/tables/educational-attainment.htm","https://web.archive.org/web/20181111061333/https://www.bls.gov/emp/tables/educational-attainment.htm","https://web.archive.org/web/20190817034145/https://www.bls.gov/emp/tables/educational-attainment.htm","https://web.archive.org/web/20191008142631/https://www.bls.gov/emp/tables/educational-attainment.htm","https://web.archive.org/web/20200415144347/https://www.bls.gov/emp/tables/educational-attainment.htm","https://web.archive.org/web/20200418212252/https://www.bls.gov/emp/tables/educational-attainment.htm","https://web.archive.org/web/20200606235028/https://www.bls.gov/emp/tables/educational-attainment.htm","https://web.archive.org/web/20200812041413/https://www.bls.gov/emp/tables/educational-attainment.htm","https://web.archive.org/web/20200831032500/https://www.bls.gov/emp/tables/educational-attainment.htm","https://web.archive.org/web/20200907122253/https://www.bls.gov/emp/tables/educational-attainment.htm","https://web.archive.org/web/20200917201811/https://www.bls.gov/emp/tables/educational-attainment.htm","https://web.archive.org/web/20200918143701/https://www.bls.gov/emp/tables/educational-attainment.htm","https://web.archive.org/web/20200922223929/https://www.bls.gov/emp/tables/educational-attainment.htm","https://web.archive.org/web/20201009035910/https://www.bls.gov/emp/tables/educational-attainment.htm","https://web.archive.org/web/20201016202252/https://www.bls.gov/emp/tables/educational-attainment.htm","https://web.archive.org/web/20201021221812/https://www.bls.gov/emp/tables/educational-attainment.htm","https://web.archive.org/web/20201112003058/https://www.bls.gov/emp/tables/educational-attainment.htm","https://web.archive.org/web/20201127082825/https://www.bls.gov/emp/tables/educational-attainment.htm","https://web.archive.org/web/20201208144453/https://www.bls.gov/emp/tables/educational-attainment.htm","https://web.archive.org/web/20210126024750/https://www.bls.gov/emp/tables/educational-attainment.htm","https://web.archive.org/web/20210319030313/https://www.bls.gov/emp/tables/educational-attainment.htm","https://web.archive.org/web/20210414030615/https://www.bls.gov/emp/tables/educational-attainment.htm","https://web.archive.org/web/20210416012107/https://www.bls.gov/emp/tables/educational-attainment.htm")

content <- lapply(url_list, read_html)

saveRDS(content, "./aux_data/bls_scrape/content.rds")
remove(list = setdiff(ls(),c("url_list","content")))

# EXTRACT Last Modified Date
lmd_df <- lapply(1:24, function(i) {
  content[[i]] %>% html_elements(".update") %>% html_text
}) %>% unlist(recursive = F, use.names = F)

# CLEAN

df <- lapply(content, function(x) {html_table(x, fill = T)}) %>%
  unlist(., recursive = F, use.names = F)
df[[2]] <- fread("./aux_data/bls_scrape/item_2.csv", data.table = F)
df[[4]] <- fread("./aux_data/bls_scrape/item_4.csv", data.table = F)

df <- lapply(1:24, function(i) {
  df[[i]] %>%
    clean_names %>%
    mutate(lmd = mdy(str_sub(lmd_df[i], 21,-1))) %>%
    mutate(source = url_list[[i]])})
df <- lapply(1:24, function(i) {
  x <- df[[i]]
  x <- x %>% mutate(code_name = colnames(.)[2])
  colnames(x)[1:2] <- c("desc", "code")
  x <- x %>% mutate(across(1:9, as.character))
  return(x)})

df <- bind_rows(df)

df <- df %>% filter(!grepl("Footnotes",desc))
df <- df %>% mutate(date = as.numeric(str_sub(source, 29, 36)))
df$date[is.na(df$date)] <- "20210708"
df <- df %>% mutate(date = ymd(date))
df <- df %>% as_tibble
df <- df %>% mutate(across(less_than_high_school_diploma:doctoral_or_professional_degree, as.numeric))
colnames(df)
df <- df %>% arrange(code, lmd)

n_distinct(df$code)

df_s <- df %>% filter(str_sub(date, 1, 4) %in% c("2019","2020")) %>% arrange(date) %>% distinct(code, .keep_all = T)
saveRDS(df, "./aux_data/bls_scrape/bls_soc_educ_dist.rds")
saveRDS(df_s, "./aux_data/bls_scrape/bls_static_soc_educ_dist.rds")

#### AVERAGE EDUC REQUIREMENT CHANGES COVID #####
remove(list = ls())
df <- readRDS("./aux_data/bls_scrape/bls_soc_educ_dist.rds")
colnames(df_plot)
df_plot <- df %>% filter(code == "00-0000") %>%
  group_by(lmd) %>%
  pivot_longer(., cols = less_than_high_school_diploma:doctoral_or_professional_degree, names_to = "educ", values_to = "proportion")


p <- ggplot(df_plot, aes(x = as.Date(date), y = proportion, color = educ)) +
  geom_line(se = F, span = 0.5, alpha = 0.5, size = 0.75) +
  geom_point(size = 2.5) +
  ylab("Percent") +
  xlab("Date") +
  scale_y_continuous(breaks = seq(0,100,5), minor_breaks = 2.5) +
  scale_x_date(date_labels = "%b'%y", breaks = date_breaks("3 months")) +
  scale_colour_ggthemr_d() +
  ggtitle("Proportion of Workers (Age > 23) by Educational Obtainment")

p




