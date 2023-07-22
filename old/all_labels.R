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
library("quanteda.textmodels")
library("caret")
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

setDTthreads(1)
getDTthreads()
quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg-anz/")

remove(list = ls())
#### end ####

#### DICTIONARIES ####
remove(list = ls())
df_dict <- bind_rows(read_xlsx("./aux_data/wfh_v6.xlsx", sheet = 1) %>% clean_names %>% select(glob_en) %>% mutate(source = "wfh"),
                     read_xlsx("./aux_data/wfh_v6.xlsx", sheet = 2) %>% clean_names %>% select(glob_en) %>% mutate(source = "generic"),
                     read_xlsx("./aux_data/wfh_v6.xlsx", sheet = 3) %>% clean_names %>% select(glob_en) %>% mutate(source = "excemptions"),
                     read_xlsx("./aux_data/wfh_v6.xlsx", sheet = 4) %>% clean_names %>% select(glob_en) %>% mutate(source = "intensity"),
                     read_xlsx("./aux_data/wfh_v6.xlsx", sheet = 5) %>% clean_names %>% select(glob_en) %>% mutate(source = "negation"))

df_dict$glob_en <- tolower(df_dict$glob_en)
df_dict[df_dict == ""] <- NA
df_dict <- df_dict %>% group_by(source) %>% distinct(glob_en, .keep_all = T) %>% ungroup

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

rm(list = setdiff(ls(),c("df_dict", "wfh_and_neg_dict_glob", "wfh_and_neg_dict_glob", "wfh_dict_glob")))
#### end ####

#### MAKE DFM FROM LABELS ####
setwd("/mnt/disks/pdisk/bg_combined/")
df_labs <- fread("./all_labels/df_final.csv")
df_labs <- df_labs %>%
  setDT(.) %>%
  .[, sequence_clean := str_replace_all(text, "[?]", " QM ")] %>%
  .[, sequence_clean := str_replace_all(sequence_clean, "[:]", " CL ")] %>%
  .[, sequence_clean := str_squish(tolower(str_replace_all(sequence_clean, "[^[:alnum:]]|\\s+", " ")))] %>%
  .[, lab_id := 1:.N]

x <- df_labs %>%
    quanteda::corpus(., text_field = "sequence_clean", docid_field = "lab_id", unique_docnames = TRUE) %>%
    quanteda::tokens(., what = "word", remove_punct = T, remove_symbols = T, remove_url = T, remove_separators = T,  split_hyphens = T, verbose = T, padding = FALSE)
# WFH and negation
x_wfh_neg_window <- tokens_select(x, pattern = wfh_dict_glob, selection = "keep", valuetype = "glob", case_insensitive = TRUE, padding = FALSE, window = c(4,3), verbose = T)
x_dfm_wfh_w_neg <- quanteda::dfm(tokens_lookup(x_wfh_neg_window,  wfh_and_neg_dict_glob, valuetype = "glob", case_insensitive = T, verbose = TRUE))

df_dfm <- x_dfm_wfh_w_neg
remove(list = setdiff(ls(), c("df_dfm", "df_labs")))

nrow(df_labs)
df_labs <- df_labs %>%
  setDT(.) %>%
  .[lab_id %in% docnames(df_dfm)]
nrow(df_labs)
  
df_labs_tagged <- cbind(df_labs, convert(df_dfm, to = "data.frame"))
  
# Export
saveRDS(df_labs_tagged, file = paste0("./df_labs_tagged.rds"))
saveRDS(df_dfm, file = paste0("./df_labs_dfm.rds"))
  
#### END ####

#### GET WEIGHTS ####
remove(list = ls())
setwd("/mnt/disks/pdisk/bg_combined/")
df_labs_tagged <- readRDS(file = paste0("./df_labs_tagged.rds"))
df_dfm <- readRDS(file = paste0("./df_labs_dfm.rds"))

nrow(df_labs_tagged)
nrow(df_dfm)

##### DICTIONARY WEIGHTS ######
remove(list = ls())
# For each WFH keyword, we measure the negated and unnegated accuracy
setwd("/mnt/disks/pdisk/bg_combined/")
df_labs_tagged <- readRDS(file = paste0("./df_labs_tagged.rds"))

colnames(df_labs_tagged)

df_labs_long <- df_labs_tagged %>%
  mutate(neg = pmax(cant, cant_2, dont, dont_2, isnt, isnt_2, no, not, unable, wont, wont_2)) %>%
  select(lab_id, label, text, level, neg, x100_percent_remote:working_virtually) %>%
  pivot_longer(x100_percent_remote:working_virtually) %>%
  arrange(name) %>%
  mutate(value = as.numeric(value > 0)) %>%
  mutate(neg = as.numeric(neg > 0))

df_weights <- df_labs_long %>%
  filter(value == 1) %>%
  group_by(name, neg) %>%
  summarise(tp = sum(as.numeric(value == 1 & label == 1)),
            fp = sum(as.numeric(value == 1 & label == 0)),
            n = n()) %>%
  mutate(accuracy = tp/(tp+fp))

saveRDS(df_weights, file = "./aux_data/dictionary_weights.rds")





