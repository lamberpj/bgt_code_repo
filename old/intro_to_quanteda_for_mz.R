#### SETUP ####
remove(list = ls())

options(repos = c(CRAN = "https://mran.revolutionanalytics.com/snapshot/2021-09-01"))

options(scipen=999)

library("data.table")
library("tidyverse")
library("janitor")
#library("lubridate")
#library("dplyr")
#library("stringr")
#library("doParallel")
library("quanteda")
#library("readtext")
#library("rvest")
#library("xml2")
#library("DescTools")
#library("zoo")
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
library("readxl")
# Translation packages
#library("rvest")
#library("googleLanguageR")
#library("cld2")
#library("datasets")
#library("vroom")

library(ggplot2)
library(scales)
library("ggpubr")
library("devtools")
devtools::install_github('Mikata-Project/ggthemr')
library("ggthemr")
ggthemr('flat')

quanteda_options(threads = 4)
setwd("/mnt/disks/pdisk/bg-us/")
setDTthreads(4)

remove(list = ls())
#### end ####

#### 2. CORPUS OBJECTS, and KEY WORDS IN CONTEXT ####

#  2.1 Read in DF
paths <- list.files("./int_data/tech", pattern = "tech_raw_text_", full.names = T)
df <- lapply(1:length(paths), function(i) {readRDS(paths[[i]])}) # You can choose any file which is called "tech_raw_text_..."!
df <- bind_rows(df)

# 2.2 Create dictionary
df_dict <- read_xlsx("./int_data/dict_v1.xlsx") %>% clean_names
df_dict$glob_en <- tolower(df_dict$glob_en)
df_dict[df_dict == ""] <- NA
df_dict <- df_dict %>%
  distinct(glob_en, .keep_all = T)
length(df_dict$glob_en) # 162
length(unique(df_dict$glob_en)) # 162
df_dict <- df_dict %>%
  mutate(key = glob_en) %>%
  mutate(key = gsub("\\s+", "_", key)) %>%
  mutate(key = gsub("[*]", "_AST_", key)) %>%
  mutate(key = gsub("[:]", "_C_", key)) %>%
  mutate(key = gsub("^_", "", key)) %>%
  mutate(key = gsub("_$", "", key)) %>%
  mutate(key = gsub("__", "_", key)) %>%
  mutate(key = make_clean_names(key))
length(df_dict$key) # 162
length(unique(df_dict$key)) # 162
ls_dict <- setNames(as.list(df_dict$glob_en), df_dict$key)
dict <- dictionary(ls_dict)
head(df_dict)
dict$data_min_ast

# 2.3 Make a "corpus" object from our data frame of raw text
df_corpus <- corpus(df, text_field = "job_text", docid_field = "job_id")
class(df_corpus)

# 2.4 Play with the "kwic" function!
df_kwic <- kwic(df_corpus, pattern = dict, )

df_kwic_df <- df_kwic %>% arrange(keyword, )

table_df <- as.data.frame(sort(table(tolower(df_kwic$keyword)), decreasing = T))
View(table_df)
#### /end ####

##### 3. USING A DFM OBJECT ####
# 3.1 Load the dfm
paths <- list.files("./int_data/tech", pattern = "tech_dfm_", full.names = T)
paths
dfm <- lapply(1:length(paths), function(i) {
  x <- readRDS(paths[[i]]) %>%
    convert(., to = "data.frame")
  }) %>%
  bind_rows

# 3.4 Plot a wordcloud
set.seed(1)
library("quanteda.textplots")
textplot_wordcloud(dfm, min_count = 6, random_order = FALSE, rotation = 0.25, 
                   color = RColorBrewer::brewer.pal(8, "Dark2"))

#### /end ####

#### WFH Negation ####




neg_dict <- dictionary(list(
  no = "no",
  not = "not",
  cant = "can't",
  cannot = "cannot",
  unable = "unable",
  care = "care",
  assessments = "assessments",
  training = "training",
  supervisory = "supervisory",
  health = "health",
  provider = "provider",
  services = "services",
  travel = "Travel",
  number_of_replacement = "Number of Replacement"))



care|assessments|training|supervisory|health|provider|services