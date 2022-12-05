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

setwd("/mnt/disks/pdisk/bg-eu/translate/")

#### unzip and delete zipped ####

file_names <- list.files("../bg-eu-bucket/new/raw/for_transfer", full.names = F)
file_names <- as.data.frame(file_names) %>%
  separate(file_names, sep = " ", c("type", "country", "year"), remove = F) %>%
  mutate(year = str_sub(year, 1, 4),
         type = str_sub(file_names, -3, -1)) %>%
  group_by(country) %>%
  mutate(n = n()) %>%
  ungroup()

file_names$path <- as.data.frame(paths <- list.files("../bg-eu-bucket/new/raw/for_transfer", full.names = T))$paths

sapply(list.files("../bg-eu-bucket/new/raw/for_transfer", pattern = "*.zip", full.names = T), unlink)

#### end ####

#### DETERMINE LANGUAGES ####
remove(list = ls())
file_names <- list.files("../bg-eu-bucket/new/raw/", "*.csv", full.names = F)
file_names <- as.data.frame(file_names)
file_names$file_names <- as.data.frame(paths <- list.files("../bg-eu-bucket/new/raw/", "*.csv", full.names = T))$paths
file_names <- file_names %>% separate(file_names, sep = " ", c("type", "country", "year"), remove = F) %>% mutate(year = str_sub(year, 1, 4))

# file_names <- file_names %>% filter(year != "2018")

file_names
m <- nrow(file_names)
#### /end ####

#### CHECK IMPORT ####
setMKLthreads(1)

#i = 3
options(warn=2)
for (i in 23:m) {
  
  print(i)
  print(paste0(file_names$country[[i]]," ",file_names$year[[i]]))
  print(file_names$file_names[[i]])
  #y1_lines <- read_lines(paste0(file_names$file_names[[i]]))
  #j <- length(y1_lines)
  #y1_lines <- y1_lines[grepl("^[0-9]{9}[,]", y1_lines)]
  #k <- length(y1_lines)
  #remove(y1_lines)
  
  y1 <- fread(file_names$file_names[[i]], data.table = F, stringsAsFactors = F, nThread = 16)
  
  remove(y1)
  
  #l <- nrow(y1)
  #
  #print(paste0("Diff 1: ",j-l))
  #print(paste0("Diff 1: ",k-l))

}

#### end ####


#### EXTRACT LANGUAGE ####
setMKLthreads(1)

#i = 3

safe_mclapply(1:m, function(i) {
  
  #print(paste0(file_names$country[[i]]," ",file_names$year[[i]]))
  #y1_lines <- read_lines(paste0(file_names$file_names[[i]]))
  #j <- length(y1_lines)
  #y1_lines <- y1_lines[grepl("^[0-9]{9}[,]", y1_lines)]
  #k <- length(y1_lines)
  #remove(y1_lines)
  
  y1 <- fread(file_names$file_names[[i]], data.table = F, stringsAsFactors = F, nThread = 1) %>%
    clean_names %>%
    as_tibble %>%
    mutate(country = file_names$country[[i]],
           year = file_names$year[[i]])
  
  #l <- nrow(y1)
  #
  #cat(paste0(i,":   ",file_names$file_names[[i]],":  ",j-l), file = "diff.txt", append = T, sep = "\n")
  #cat(paste0(i,":   ",file_names$file_names[[i]],":  ",k-l), file = "diff.txt", append = T, sep = "\n")
  
  y1 <- y1 %>%
   mutate(title_lang = detect_language(title, plain_text = F),
           description_lang = detect_language(description, plain_text = F)) %>%
    select(general_id, title_lang, description_lang, country, year)
  
  saveRDS(y1, paste0("./int_data/df_lang_",file_names$country[[i]],"_",file_names$year[[i]],".rds"))
  return("")
}, mc.cores = 16)

#### end ####

#### CLEAN FOR TRANSLATE ####
remove(list = ls())
file_names <- list.files("../bg-eu-bucket/new/raw/", "*.csv", full.names = F)
file_names <- as.data.frame(file_names) %>% separate(file_names, sep = " ", c("type", "country", "year"), remove = F) %>% mutate(year = str_sub(year, 1, 4))
file_names$file_names <- as.data.frame(paths <- list.files("../bg-eu-bucket/new/raw/", "*.csv", full.names = T))$paths
m <- nrow(file_names)

df_lang <- lapply(list.files("./int_data", "lang", full.names = T), readRDS) %>%
  lapply(., function(x) {x %>% mutate(general_id = as.numeric(general_id))}) %>%
  bind_rows %>%
  mutate(general_id = as.numeric(general_id))
  
i = 10

y1 <- fread(file_names$file_names[[i]], data.table = F, stringsAsFactors = F, nThread = 1) %>%
  clean_names %>%
  as_tibble %>%
  mutate(country = file_names$country[[i]],
         year = file_names$year[[i]]) %>%
  mutate(general_id = as.numeric(general_id))

y1 <- y1 %>%
  left_join(df_lang)

tf <- function(frag) {
  tidy_html(frag) %>%   
    read_html() %>% 
    html_nodes(xpath="//*[not(self::script)]/text()") %>% 
    html_text() %>% 
    paste0(collapse = " ") %>% 
    str_replace_all(pattern = "\n", replacement = " ") %>%
    str_replace_all(pattern = "[\\^]", replacement = " ") %>%
    str_replace_all(pattern = "\"", replacement = " ") %>%
    str_replace_all(pattern = "\\s+", replacement = " ") %>%
    str_trim(side = "both")
}

y1$description_clean <- mclapply(1:length(y1$description), function(i) {
  tf(y1$description[[i]])
}, mc.cores = 8) %>% unlist

y1$title_clean <- mclapply(1:length(y1$title), function(i) {
  tf(y1$title[[i]])
}, mc.cores = 8) %>% unlist

#### END ####

#### TRANSLATE 100 JOB TITLES ####

test <- y1 %>% filter(description_lang == "es" & nchar(title_clean) > 20)
test100 <- test[1:100, ]

gl_auth("burning-glass-eu-1e673ec779c7.json")

test100$title_clean[11]

test100$title_english <- ""

test100$title_english <- gl_translate(test100$title_clean, target = "en")$translatedText

test100$description_clean_english <- gl_translate(test100$description_clean, target = "en")$translatedText

write_csv(test100, "google_machine_translation_test_ES_to_EN.csv")

test100$description_clean_english[20]

sum(nchar(y1$title_clean)) + sum(nchar(y1$description_clean))

sum(nchar(y1$title_clean))

summary(nchar(y1$description_clean))

56517559*27 - 250000000


#### END ####


#### SANDBOX ####





