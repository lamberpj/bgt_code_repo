#### SETUP ####
remove(list = ls())

library("devtools")
install_github("trinker/textclean")
library("textclean")
library("data.table")
library("tidyverse")
library("janitor")
library("lubridate")
library("doParallel")
#library("textclean")
#install.packages("qdapRegex")
library("quanteda")
library("tokenizers")
library("stringi")
#library("readtext")
library("rvest")
library("xml2")
#library("DescTools")
library("zoo")
library("stargazer")
library("readxl")
library("ggplot2")
library("scales")
library("ggpubr")
library("ggthemr")
ggthemr('flat')

setDTthreads(2)
getDTthreads()
quanteda_options(threads = 2)
setwd("/mnt/disks/pdisk/bg-us/")
#setDTthreads(1)

remove(list = ls())
#### end ####


#### DICTIONARIES ####
remove(list = ls())
df_dict <- bind_rows(read_xlsx("./aux_data/wfh_v8.xlsx", sheet = 1) %>% clean_names %>% filter(source == "oecd") %>% select(glob_en) %>% mutate(source = "wfh"),
                     read_xlsx("./aux_data/wfh_v8.xlsx", sheet = 2) %>% clean_names %>% select(glob_en) %>% mutate(source = "generic"),
                     read_xlsx("./aux_data/wfh_v8.xlsx", sheet = 3) %>% clean_names %>% select(glob_en) %>% mutate(source = "excemptions"),
                     read_xlsx("./aux_data/wfh_v8.xlsx", sheet = 4) %>% clean_names %>% select(glob_en) %>% mutate(source = "intensity"),
                     read_xlsx("./aux_data/wfh_v8.xlsx", sheet = 5) %>% clean_names %>% select(glob_en) %>% mutate(source = "negation"))

length(unique(df_dict$glob_en[df_dict$source == "wfh"])) # 176
length(unique(df_dict$glob_en[df_dict$source == "generic"])) # 5

df_dict$glob_en <- tolower(df_dict$glob_en)
df_dict[df_dict == ""] <- NA
df_dict <- df_dict %>% group_by(source) %>% distinct(glob_en, .keep_all = T) %>% ungroup

length(unique(df_dict$glob_en[df_dict$source == "wfh"])) # 176
length(unique(df_dict$glob_en[df_dict$source == "generic"])) # 5

length(df_dict$glob_en) # 353
length(unique(df_dict$glob_en)) # 350

df_dict <- df_dict %>%
  mutate(key = glob_en) %>%
  mutate(key = gsub("\\s+", "_", key)) %>%
  mutate(key = gsub("[*]", "_AST_", key)) %>%
  mutate(key = gsub("[:]", "_C_", key)) %>%
  mutate(key = gsub("^_", "", key)) %>%
  mutate(key = gsub("_$", "", key)) %>%
  mutate(key = gsub("__", "_", key))

df_dict <- df_dict %>%
  mutate(key = make_clean_names(key)) %>%
  select(key, glob_en, source)

wfh_dict_glob <- df_dict %>% filter(source %in% c("wfh"))
wfh_dict_glob <- setNames(as.list(wfh_dict_glob$glob_en), wfh_dict_glob$key) %>% dictionary()

neg_dict_glob <- df_dict %>% filter(source %in% c("negation"))
neg_dict_glob <- setNames(as.list(neg_dict_glob$glob_en), neg_dict_glob$key) %>% dictionary()

wfh_and_neg_dict_glob <- df_dict %>% filter(source %in% c("wfh", "negation"))
wfh_and_neg_dict_glob <- setNames(as.list(wfh_and_neg_dict_glob$glob_en), wfh_and_neg_dict_glob$key) %>% dictionary()

intensity_dict_glob <- df_dict %>% filter(source %in% c("intensity"))
intensity_dict_glob <- setNames(as.list(intensity_dict_glob$glob_en), intensity_dict_glob$key) %>% dictionary()

wfh_and_generic_dict_glob <- df_dict %>% filter(source %in% c("wfh", "generic"))
wfh_and_generic_dict_glob <- setNames(as.list(wfh_and_generic_dict_glob$glob_en), wfh_and_generic_dict_glob$key) %>% dictionary()

generic_dict_glob <- df_dict %>% filter(source %in% c("generic"))
generic_dict_glob <- setNames(as.list(generic_dict_glob$glob_en), generic_dict_glob$key) %>% dictionary()

generic_and_excemptions_dict_glob <- df_dict %>% filter(source %in% c("generic", "excemptions"))
generic_and_excemptions_dict_glob <- setNames(as.list(generic_and_excemptions_dict_glob$glob_en), generic_and_excemptions_dict_glob$key) %>% dictionary()

wfh_dict_glob <- df_dict %>% filter(source %in% c("wfh"))
wfh_dict_glob <- setNames(as.list(wfh_dict_glob$glob_en), wfh_dict_glob$key) %>% dictionary()

all_dict <- setNames(as.list(df_dict$glob_en), df_dict$key) %>% dictionary()

df_dict$lu <- NA
df_dict$lu <- gsub("*", "\\w+", df_dict$glob_en, fixed = T)
df_dict$lu <- paste0("\\b(",gsub(" ", ")\\s(", all_dict$lu, fixed = T),")\\b")
df_dict$rp <- NA
df_dict$rp[df_dict$source == "wfh"] <-"<\\U\\1 \\U\\2 \\U\\3 \\U\\4>"
df_dict$rp[df_dict$source == "generic"] <-"[\\U\\1 \\U\\2 \\U\\3 \\U\\4]"
df_dict$lu[!(df_dict$source %in% c("wfh", "generic"))] <- paste0("\\b([",df_dict$glob_en[!(df_dict$source %in% c("wfh", "generic"))],"])\\b")
df_dict$rp[!(df_dict$source %in% c("wfh", "generic"))] <- " \\U\\1 "
rm(list = setdiff(ls(),c("df_dict", "all_dict", "wfh_dict_glob", "neg_dict_glob", "wfh_and_neg_dict_glob", "intensity_dict_glob", "wfh_and_generic_dict_glob", "generic_dict_glob", "generic_and_excemptions_dict_glob")))
#### end ####

#### GET ####

df <- fread("./aux_data/training_v6_with_labels_long_dict2.csv")
df$id <- seq(1, nrow(df))
rm(list = setdiff(ls(),c("df", "df_dict", "all_dict", "wfh_dict_glob", "neg_dict_glob", "wfh_and_neg_dict_glob", "intensity_dict_glob", "wfh_and_generic_dict_glob", "generic_dict_glob", "generic_and_excemptions_dict_glob")))
#### /END ####
colnames(df)
#### MAKE DFM ####


  
  # WFH without Negation or Intensity
  x <- df %>%
    quanteda::corpus(., text_field = "sequence", docid_field = "id", unique_docnames = TRUE) %>%
    quanteda::tokens(., what = "word", remove_punct = T, remove_symbols = T, remove_url = T, remove_separators = T, split_hyphens = T, verbose = T, padding = FALSE)
  

  
  # WFH and negation
  x_wfh_neg_window <- tokens_select(x, pattern = wfh_dict_glob, selection = "keep", valuetype = "glob", case_insensitive = TRUE, padding = FALSE, window = c(3,2), verbose = T)
  x_dfm_wfh_w_neg <- quanteda::dfm(tokens_lookup(x_wfh_neg_window,  wfh_and_neg_dict_glob, valuetype = "glob", case_insensitive = T, verbose = TRUE))
  remove(x_wfh_neg_window)
  
  x_dfm <- x_dfm_wfh_w_neg
  
  remove(list = c("x_dfm_wfh_w_neg"))
  
  x_df <- convert(x_dfm, to = "data.frame")
  
  View(head(x_df))
  
  df_wide <- bind_cols(df, x_df)
  
  df_wide <- df_wide %>% select(-doc_id)
  
  View(df_wide)
  
  fwrite(df_wide, "./aux_data/training_v6_with_labels_long_dict2_oecd_tags.csv")
  
  warning(paste0("\nSUCCESS: ",i,"\n"))
  #cat(paste0("\nDID: ",i," IN  ",difference," minutes\n"))
  return("")
}, mc.cores = 32)

sink()
system("echo sci2007! | sudo -S shutdown -h now")
#### END ####

