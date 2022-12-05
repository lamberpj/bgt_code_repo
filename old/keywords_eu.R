#### SETUP ####
remove(list = ls())

options(repos = c(CRAN = "https://mran.revolutionanalytics.com/snapshot/2021-09-01"))

options(scipen=999)

library("data.table")
library("tidyverse")
library("janitor")
library("lubridate")
#library("dplyr")
library("stringr")
library("doParallel")
library("quanteda")
library("readtext")
library("rvest")
library("xml2")
library("DescTools")
library("zoo")
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
quanteda_options(threads = 24)
setwd("/mnt/disks/pdisk/bg-eu")
setDTthreads(24)
#### TRANSFER FILES ####

#system("gsutil -m cp -r gs://for_transfer  /mnt/disks/pdisk/bg-eu/bg-eu-bucket/primary/text")

#### / END ####

#### RENAME PATHS ####
#file.rename(list.files("./bg-eu-bucket/primary/text", pattern = "*.zip", full.names = T),gsub("_", " ", list.files("./bg-eu-bucket/primary/text", pattern = "*.zip", full.names = T)))
#### end ####

#unlink(list.files("./int_data/wfh_prim/", pattern = "wfh_dfm_", full.names = T)[grepl("2021", list.files("./int_data/wfh_prim/", pattern = "wfh_dfm_", full.names = T))])


#### GET PATH NAMES ####
paths <- list.files("./bg-eu-bucket/primary/text", pattern = "*.zip", full.names = T)
paths

paths_done <- list.files("./int_data/wfh_prim/", pattern = "wfh_dfm_", full.names = F) %>%
  gsub("wfh_dfm_|wfh_raw_text_", "", .) %>% gsub(".rds", "", .) %>% unique %>%
  gsub("_", " ", .) %>% gsub("raw ", "", .)
paths_done
paths
paths <- paths[!(grepl(paste(paths_done, collapse = "|"), paths))]
paths
# Select countries
paths <- paths[grepl("BE|DE|FR|IT|LU|NL|UK", paths)]
paths

#### /END ####

#### LOAD DICTIONARY ####

df_dict <- read_xlsx("./int_data/dict_v3.xlsx") %>% clean_names
df_dict <- df_dict %>% mutate_all(tolower)
df_dict[df_dict == ""] <- NA
df_dict <- df_dict[df_dict$cluster == "wfh",]
df_dict
length(df_dict$glob_en) # 191
length(unique(df_dict$glob_en)) # 168

df_dict <- df_dict %>%
  mutate(key = glob_en) %>%
  mutate(key = gsub("\\s+", "_", key)) %>%
  mutate(key = gsub("[*]", "_AST_", key)) %>%
  mutate(key = gsub("[:]", "_C_", key)) %>%
  mutate(key = gsub("^_", "", key)) %>%
  mutate(key = gsub("_$", "", key)) %>%
  mutate(key = gsub("__", "_", key)) %>%
  mutate(key = make_clean_names(key))

length(df_dict$key) # 191
length(unique(df_dict$key)) # 191

df_dict <- df_dict %>%
  group_by(key) %>%
  pivot_longer(cols = c(glob_en, glob_fr, glob_de, glob_nl, glob_it, glob_lb), names_to = "language", values_to = "glob") %>%
  filter(!is.na(glob))

length(df_dict$key) # 880
length(unique(df_dict$key)) # 191

df_dict <- df_dict %>%
  mutate(key = paste0(key, str_sub(language, -3, -1)))

length(df_dict$key) # 880
length(unique(df_dict$key)) # 880

length(df_dict$glob) # 880
length(unique(df_dict$glob)) # 880

df_dict

length(df_dict$key) # 463
length(unique(df_dict$key)) # 463

df_dict <- df_dict %>%
  distinct(glob, .keep_all = T)

ls_dict <- setNames(as.list(df_dict$glob), df_dict$key)
dict <- dictionary(ls_dict)
sort(names(dict))
#### /END ####
paths
#### READ XML ####
safe_mclapply(3:length(paths), function(i) {
  
  warning(paste0("BEGIN: ",i))
  paths[i]
  name <- paths[i] %>% gsub("./bg-eu-bucket/primary/text/raw ", "", .) %>%  gsub(".zip", "", .)
  name
  paths[i]
  system(paste0("unzip '", paths[i],"' -d ./bg-eu-bucket/primary/text"))
  name
  csv_path <- list.files("./bg-eu-bucket/primary/text", pattern = ".csv", full.names = T)[grepl(name, list.files("./bg-eu-bucket/primary/text", pattern = ".csv"))]
  file.rename(csv_path,gsub(" ", "_", csv_path))
  name <- gsub(" ", "_", name)
  csv_path <- list.files("./bg-eu-bucket/primary/text", pattern = ".csv", full.names = T)[grepl(name, list.files("./bg-eu-bucket/primary/text", pattern = ".csv"))]
  
  df <- fread(csv_path, stringsAsFactors = F)
  df <- data.frame("job_id" = df$general_id, "job_text" = paste(df$title, df$description))
  
  df_corpus <- corpus(df, text_field = "job_text", docid_field = "job_id")
  
  df_tokens <- tokens(df_corpus)
  remove("df_corpus")
  df_dfm <- dfm(tokens_lookup(df_tokens, dict, valuetype = "glob", case_insensitive = TRUE, verbose = TRUE))
  remove("df_tokens")
  
  df_tagged <- convert(df_dfm, to = "data.frame")
  df_tagged <- df_tagged[rowSums(df_tagged[,-1]) > 0,]
  
  df <- df %>% filter(job_id %in% df_tagged$doc_id)
  
  saveRDS(df_tagged, file = paste0("./int_data/wfh_prim/wfh_dfmdf_",name,".rds"))
  saveRDS(df, file = paste0("./int_data/wfh_prim/wfh_raw_text_",name,".rds"))
  saveRDS(df_dfm, file = paste0("./int_data/wfh_prim/wfh_dfm_",name,".rds"))
  
  unlink(csv_path)
  warning(paste0("DONE: ",i))
  return("")
}, mc.cores = 2)

system("echo sci2007! | sudo -S shutdown -h now")




