#### SETUP ####
remove(list = ls())

options(repos = c(CRAN = "https://mran.revolutionanalytics.com/snapshot/2021-09-01"))

options(scipen=999)

library("data.table")
library("tidyverse")
library("janitor")
library("lubridate")
#library("dplyr")
#library("stringr")
library("doParallel")
library("quanteda")
#library("readtext")
#library("rvest")
#library("xml2")
#library("DescTools")
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
library("stargazer")
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

quanteda_options(threads = 2)
setwd("/mnt/disks/pdisk/bg-eu")
setDTthreads(2)

remove(list = ls())
#### end ####

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

neg_dict <- dictionary(list(
  uneg = c("no","not","can't","cannot","unable","non","ne pas","ne peut pas","ne peux pas","incapable",
           "Nein","nicht","kippen","kann nicht","unfähig","Nee","niet","kan niet","kan niet","niet in staat",
           "no","non","non si può","non può","incapace","no","not","can't","cannot","unable","Nee","net",
           "kann net","kann nët","knapp"),
  hb_ne = c("care","assessments","training","supervisory","health","provider","services","soins","évaluations",
            "formation","supervision","santé","prestataire","services","Pflege","Bewertungen","Ausbildung",
            "Aufsichts","Gesundheit","Anbieter","Dienstleistungen","care","assessments","training",
            "toezichthoudende","gezondheid","provider","diensten","la cura","valutazioni","formazione",
            "vigilanza","salute","fornitore di servizi","Pfleeg","Moosnamen","Formatioun","Kontrollinstanzen",
            "Gesondheet","Provider","Servicer"),
  wah_ne = c("Travel","Number of Replacement","Voyage","Nombre de remplacement","Reisen","Anzahl der Ersatz","Travel",
             "Aantal Replacement","Viaggi","Numero di sostituzione","Travel","Zuel vun Ersatz")
))


#### end ####

#### WFH from DFMs ####
paths_dfm <- list.files("./int_data/wfh_prim", pattern = "wfh_dfmdf_", full.names = T)
paths_dfm
paths_raw_text <- list.files("./int_data/wfh_prim", pattern = "wfh_raw_text_", full.names = T)
paths_raw_text
paths_raw_text
df_wfh <- safe_mclapply(1:length(paths_dfm), function(i) {
  warning(paste0("START: ",i))
  x <- readRDS(paths_dfm[[i]])
  return(x)
}, mc.cores = 16)

df_wfh <- df_wfh %>% bind_rows
colnames(df_wfh)

#### end ####

#### WFH raw text ####
df_kwic <- safe_mclapply(1:length(paths_raw_text), function(i) {

  warning(paste0("START: ",i))
  paths_raw_text[[i]]
  x <- readRDS(paths_raw_text[[i]]) %>%
    filter(job_id %in% df_wfh$doc_id)
  df_corpus <- corpus(x, text_field = "job_text", docid_field = "job_id")
  df_tokens <- tokens(df_corpus)
  x <- x %>%
    mutate(n = ntoken(df_tokens))
  df_kwic <- kwic(df_tokens, pattern = dict, window = 5) %>%
    as.data.frame(.)
  df_kwic$docname <- as.character(as.numeric(df_kwic$docname))
  x$job_id <- as.character(as.numeric(x$job_id))
  df_kwic <- df_kwic %>%
    left_join(x %>% select(job_id, n), by = c("docname" = "job_id"))
  
  # Parse kwic
  dfm_pre <- corpus(df_kwic, text_field = "pre") %>% tokens(.)
  dfm_pre <- dfm(tokens_lookup(dfm_pre, dictionary = neg_dict, valuetype = "glob", case_insensitive = TRUE, verbose = TRUE))
  df_pre <- convert(dfm_pre, to = "data.frame")
  df_pre <- df_pre %>% select(-doc_id)
  colnames(df_pre) <- paste0(colnames(df_pre),"_pre")
  
  dfm_post <- corpus(df_kwic, text_field = "post") %>% tokens(.)
  dfm_post <- dfm(tokens_lookup(dfm_post, dictionary = neg_dict, valuetype = "glob", case_insensitive = TRUE, verbose = TRUE))
  df_post <- convert(dfm_post, to = "data.frame")
  df_post <- df_post %>% select(-doc_id)
  colnames(df_post) <- paste0(colnames(df_post),"_post")
  
  df_kwic <- bind_cols(df_kwic, df_pre, df_post)
  colnames(df_kwic)
  
  warning(paste0("DONE: ",i))
  return(df_kwic)
  
}, mc.cores = 16)

df_kwic <- df_kwic %>%
  bind_rows(.)

head(df_kwic)

saveRDS(df_kwic, "./int_data/df_kwic_eu_prim.rds")

#### end ####

#### INSPECT ####
remove(list = setdiff(ls(),"df_kwic"))
df_kwic <- readRDS("./int_data/df_kwic_eu_prim.rds")
df_kwic$pattern <- as.character(df_kwic$pattern)
sort(unique(df_kwic$pattern))
df_kwic <- df_kwic %>%
  mutate(pre_neg = ifelse((uneg_pre > 0),1,0),
         pre_dis = ifelse((hb_ne_pre > 0 & pattern %in% c("home_based_en","home_based_fr","home_based_de","home_based_nl","home_based_it","home_based_2_en","home_based_2_fr","home_based_2_de","home_based_2_nl","home_based_2_it", "based_from_home_de", "based_from_home_en")) | (wah_ne_pre > 0 & pattern %in% c("work_at_home_de","work_at_home_en","work_at_home_fr","work_at_home_nl")),1,0),
         post_neg = ifelse((uneg_post > 0),1,0),
         post_dis = ifelse((hb_ne_post > 0 & pattern %in% c("home_based_en","home_based_fr","home_based_de","home_based_nl","home_based_it","home_based_2_en","home_based_2_fr","home_based_2_de","home_based_2_nl","home_based_2_it", "based_from_home_de", "based_from_home_en")) | (wah_ne_post > 0 & pattern %in% c("work_at_home_de","work_at_home_en","work_at_home_fr","work_at_home_nl")),1,0))

df_kwic <- df_kwic %>%
  mutate(trim_neg = ifelse(from <= 50 | to >= n-50, 1, 0))

df_kwic <- df_kwic %>%
  group_by(pre, keyword, post) %>%
  mutate(n_string = n()) %>%
  ungroup

df_kwic_u <- df_kwic %>%
  select(docname, uneg_pre, hb_ne_pre, wah_ne_pre, uneg_post, hb_ne_post, wah_ne_post, pre_neg, pre_dis, post_neg, post_dis, trim_neg, n_string) %>%
  group_by(docname) %>%
  mutate(uneg_pre = max(uneg_pre),
         hb_ne_pre = max(hb_ne_pre),
         wah_ne_pre = max(wah_ne_pre),
         uneg_post = max(uneg_post),
         hb_ne_post = max(hb_ne_post),
         wah_ne_post = max(wah_ne_post),
         pre_neg = max(pre_neg),
         pre_dis = max(pre_dis),
         post_neg = max(post_neg),
         post_dis = max(post_dis),
         trim_neg = min(trim_neg),
         n_string = min(n_string)) %>%
  ungroup() %>%
  distinct(., .keep_all = T)

df_kwic_u_ss <- df_kwic_u %>%
  summarise(n = n(),
            
            n_trim = sum(trim_neg == 0),
            n_ustring = sum(n_string < 500),
            
            n_pre_neg = sum(pre_neg == 0),
            n_post_neg = sum(post_neg == 0),
            n_neg = sum(pre_neg == 0 & post_neg == 0),
            
            n_pre_dis = sum(pre_dis == 0),
            n_post_dis = sum(post_dis == 0),
            n_dis = sum(pre_dis == 0 & post_dis == 0),
            
            n_neg_dis = sum(pre_neg == 0 & post_neg == 0 & pre_dis == 0 & post_dis == 0))

wfh_df <- df_kwic_u %>%
  summarise(bgt_job_id = docname,
            wfh_raw = 1,
            wfh_ustring = ifelse(n_string < 500, 1, 0),
            wfh_non_neg = ifelse(pre_neg == 0 & post_neg == 0 & pre_dis == 0 & post_dis == 0, 1, 0),
            wfh_raw_body = ifelse(trim_neg == 0, 1, 0),
            wfh_ustring_body = ifelse(trim_neg == 0 & n_string < 500, 1, 0),
            wfh_non_neg_body = ifelse(trim_neg == 0 & pre_neg == 0 & post_neg == 0 & pre_dis == 0 & post_dis == 0, 1, 0)) %>%
  select(bgt_job_id, wfh_raw, wfh_ustring, wfh_non_neg, wfh_raw_body, wfh_ustring_body, wfh_non_neg_body)
head(wfh_df)
stargazer(t(df_kwic_u_ss), summary=FALSE, rownames=TRUE, title = "BGT EU WFH Hits by Method")

saveRDS(wfh_df, "./int_data/wfh_df_prim.rds")

#### end ####

#### COMBINE AND MERGE ####
remove(list = setdiff(ls(), c("wfh_df")))
wfh_df <- readRDS("./int_data/wfh_df_prim.rds")

#### Structured ####
paths <- list.files("./bg-eu-bucket/primary/postings", pattern = ".zip", full.names = T)
paths
paths <- paths[grepl("BE|DE|FR|IT|LU|NL|UK", paths)]
paths <- paths[grepl("2019|2020|2021", paths)]

df_stru <- safe_mclapply(1:length(paths), function(i) {
  warning(paste0("\nSTART: ",i))
  paths[i]
  system(paste0("unzip '", paths[i],"' -d ./bg-eu-bucket/primary/postings"))
  csv_path <- paths[i] %>% gsub(".zip", ".csv", .)
  csv_path
  file.rename(csv_path,gsub(" ", "_", csv_path))
  csv_path <- gsub(" ", "_", csv_path)
  df <- fread(file = csv_path, data.table = F, nThread = 1, colClasses = "character", stringsAsFactors = FALSE,
              select = c("general_id",  "year_grab_date", "month_grab_date", "day_grab_date", "idesco_level_4", "idregion", "idcountry", "idsector")) %>%
    clean_names %>%
    mutate(job_date = paste0(year_grab_date,"-",month_grab_date,"-",day_grab_date)) %>%
    select(-c(year_grab_date, month_grab_date, day_grab_date))
  unlink(csv_path)
  warning(paste0("\nDONE: ",i))
  return(df)
}, mc.cores = 8)

lapply(1:length(df_stru), function(i) {
  print(paths[[i]])
  print(nrow(df_stru[[i]]))
})
df_stru <- df_stru %>% bind_rows
table(df_stru$idcountry)
#### /end ####

#### Merg in WFH ####
nrow(df_stru) # 81,535,960
df_stru <- df_stru %>%
  left_join(wfh_df, by = c("general_id" = "bgt_job_id"))
nrow(df_stru) # 81,535,960

sapply(df_stru, class)

df_stru <- df_stru  %>%
  mutate(across(where(is.numeric), ~if_else(is.na(.x), 0, .x)))

df_stru <- df_stru %>%
  mutate(idesco_level_2 = str_sub(idesco_level_4, 1, 2)) %>%
  select(general_id, job_date, idesco_level_2, idesco_level_4, everything())

# Make Quarter
df_stru <- df_stru %>%
  mutate(year_quarter = as.yearqtr(ymd(job_date), with_year = T))

df_stru %>% group_by(idcountry) %>% summarise(prop_wfh = sum(wfh_non_neg)/n())

save(df_stru, file = "./int_data/df_stru_wfh_eu_prim.RData")
#### /end ####

#### B. SHORT PLOTS ####

# 1. TIME SERIES PLOTS BY PARSING
# Load Data
remove(list = setdiff(ls(),"df_stru"))
load(file = "./int_data/df_stru_wfh_eu_prim.RData")
df_stru <- df_stru %>% filter(idcountry != "DE")
obs <- nrow(df_stru)

df_levels <- df_stru %>%
  group_by(year_quarter) %>%
  summarise(n_posts = n(),
            wfh_raw = sum(wfh_raw),
            wfh_ustring = sum(wfh_ustring),
            wfh_non_neg = sum(wfh_non_neg),
            wfh_raw_body = sum(wfh_raw_body),
            wfh_ustring_body = sum(wfh_ustring_body),
            wfh_non_neg_body = sum(wfh_non_neg_body)
  ) %>%
  mutate(across(wfh_raw:wfh_non_neg_body, ~ .x / n_posts, .names = "{.col}_prop")) %>%
  mutate(across(wfh_raw:wfh_non_neg_body, ~ .x / .x[year_quarter == "2019 Q1"], .names = "{.col}_index")) %>%
  mutate(across(wfh_raw_prop:wfh_non_neg_body_prop, ~ .x / .x[year_quarter == "2019 Q1"], .names = "{.col}_index"))

# 1.1 By Count
df_ts_posts <- df_levels %>%
  select(year_quarter, wfh_raw, wfh_non_neg, wfh_non_neg_body) %>%
  group_by(year_quarter) %>%
  pivot_longer(., cols = c(wfh_raw, wfh_non_neg, wfh_non_neg_body)) %>%
  ungroup() %>%
  mutate(name = case_when(
    name == "wfh_raw" ~ "Raw",
    name == "wfh_non_neg" ~ "Non-negated",
    name == "wfh_non_neg_body" ~ "Non-negated, body"))

p1 = df_ts_posts %>%
  ggplot(., aes(x = year_quarter, y = value, colour = name, linetype = name)) +
  geom_line(size = 2) +
  ##geom_point(size = 3) +
  ggtitle("Count of WFH Job Ads in BGT EU (Prim)") +
  ylab("Count of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25),  limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(label=comma, limits = c(0,NA), n.breaks = 10) + 
  scale_colour_ggthemr_d() +
  labs(subtitle = paste0("n = ",format(obs, big.mark=",")),
       caption = "Note: Pools data from BE, FR, LU and NE (unweighted)") +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm"),
    text = element_text(size = 16))
p1
ggexport(p1, filename = "./plots/text_based/eu_prim_wfh_level_by_parsing_sr.pdf")
remove(p1)

# 1.2 By Share
df_ts_share <- df_levels %>%
  select(year_quarter, wfh_raw_prop, wfh_non_neg_prop, wfh_non_neg_body_prop) %>%
  group_by(year_quarter) %>%
  pivot_longer(., cols = c(wfh_raw_prop, wfh_non_neg_prop, wfh_non_neg_body_prop)) %>%
  ungroup() %>%
  mutate(name = case_when(
    name == "wfh_raw_prop" ~ "Raw",
    name == "wfh_non_neg_prop" ~ "Non-negated",
    name == "wfh_non_neg_body_prop" ~ "Non-negated, body"))

p2 = df_ts_share %>%
  ggplot(., aes(x = year_quarter, y = value, colour = name, linetype = name)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT EU (Prim)") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01),  breaks = seq(0,1,0.01)) +
  coord_cartesian(ylim = c(0, 0.195)) +
  scale_colour_ggthemr_d() +
  labs(subtitle = paste0("n = ",format(obs, big.mark=",")),
       caption = "Note: Pools data from BE, FR, LU and NE (unweighted)") +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm"),
    text = element_text(size = 16))
p2
ggexport(p2, filename = "./plots/text_based/eu_prim_wfh_share_by_parsing_20_sr.pdf")
remove(p2)

p2 = df_ts_share %>%
  ggplot(., aes(x = year_quarter, y = value, colour = name, linetype = name)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT EU (Prim)") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01),  breaks = seq(0,1,0.01)) +
  coord_cartesian(ylim = c(0, 0.10)) +
  scale_colour_ggthemr_d() +
  labs(subtitle = paste0("n = ",format(obs, big.mark=",")),
       caption = "Note: Pools data from BE, FR, LU and NE (unweighted)") +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm"),
    text = element_text(size = 16))
p2
ggexport(p2, filename = "./plots/text_based/eu_prim_wfh_share_by_parsing_10_sr.pdf")
remove(p2)

p2 = df_ts_share %>%
  ggplot(., aes(x = year_quarter, y = value, colour = name, linetype = name)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT EU (Prim)") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01),  breaks = seq(0,1,0.01)) +
  coord_cartesian(ylim = c(0, 0.05)) +
  scale_colour_ggthemr_d() +
  labs(subtitle = paste0("n = ",format(obs, big.mark=",")),
       caption = "Note: Pools data from BE, FR, LU and NE (unweighted)") +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm"),
    text = element_text(size = 16))
p2
ggexport(p2, filename = "./plots/text_based/eu_prim_wfh_share_by_parsing_5_sr.pdf")
remove(p2)

# 2. TIME SERIES PLOTS BY EU COUNTRY
# Load Data
remove(list = setdiff(ls(),"df_stru"))
#load(file = "./int_data/df_stru_wfh_eu_prim.RData")
obs <- nrow(df_stru)

df_levels_country <- df_stru %>%
  group_by(year_quarter, idcountry) %>%
  summarise(n_posts = n(),
            wfh_non_neg = sum(wfh_non_neg)
  ) %>%
  mutate(across(wfh_non_neg, ~ .x / n_posts, .names = "{.col}_prop")) %>%
  ungroup() %>%
  group_by(idcountry) %>%
  mutate(across(wfh_non_neg, ~ .x / .x[year_quarter == "2019 Q1"], .names = "{.col}_index")) %>%
  mutate(across(wfh_non_neg_prop, ~ .x / .x[year_quarter == "2019 Q1"], .names = "{.col}_index"))

# 2.1 By Count
p3 = df_levels_country %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg, colour = idcountry, linetype = idcountry)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Count of WFH Job Ads in BGT EU (Prim)") +
  ylab("Count of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(label=comma, limits = c(0,NA), n.breaks = 10) + 
  scale_colour_ggthemr_d() +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(nrow = 1)) +
  theme(text = element_text(size = 16))
p3
ggexport(p3, filename = "./plots/text_based/eu_prim_wfh_level_by_country_sr.pdf")
remove(p3)

# 2.2 By Share
p3 = df_levels_country %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop, colour = idcountry, linetype = idcountry)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT EU (Prim)") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01),  breaks = seq(0,1,0.01)) +
  coord_cartesian(ylim = c(0, 0.195)) +
  scale_colour_ggthemr_d() +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(nrow = 1)) +
  theme(text = element_text(size = 16))
p3
ggexport(p3, filename = "./plots/text_based/eu_prim_wfh_share_by_country_20_sr.pdf")
remove(p3)

p3 = df_levels_country %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop, colour = idcountry, linetype = idcountry)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT EU (Prim)") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01),  breaks = seq(0,1,0.01)) +
  coord_cartesian(ylim = c(0, 0.10)) +
  scale_colour_ggthemr_d() +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(nrow = 1)) +
  theme(text = element_text(size = 16))
p3
ggexport(p3, filename = "./plots/text_based/eu_prim_wfh_share_by_country_10_sr.pdf")
remove(p3)

p3 = df_levels_country %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop, colour = idcountry, linetype = idcountry)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT EU (Prim)") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01),  breaks = seq(0,1,0.01)) +
  coord_cartesian(ylim = c(0, 0.05)) +
  scale_colour_ggthemr_d() +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(nrow = 1)) +
  theme(text = element_text(size = 16))
p3
ggexport(p3, filename = "./plots/text_based/eu_prim_wfh_share_by_country_5_sr.pdf")
remove(p3)

# 2.3 By Index (Count)
p4 = df_levels_country %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_index, colour = idcountry, linetype = idcountry)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT EU (Prim) (Count)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,1)) +
  coord_cartesian(ylim = c(0, 19.5)) +
  scale_colour_ggthemr_d() +
  
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(nrow = 1)) +
  theme(text = element_text(size = 16))
p4
ggexport(p4, filename = "./plots/text_based/eu_prim_wfh_index_count_by_country_20_sr.pdf")
remove(p4)

p4 = df_levels_country %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_index, colour = idcountry, linetype = idcountry)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT EU (Prim) (Count)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,1)) +
  coord_cartesian(ylim = c(0, 10)) +
  scale_colour_ggthemr_d() +
  
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(nrow = 1)) +
  theme(text = element_text(size = 16))
p4
ggexport(p4, filename = "./plots/text_based/eu_prim_wfh_index_count_by_country_10_sr.pdf")
remove(p4)

# 2.3 By Index (Share)
p4 = df_levels_country %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop_index, colour = idcountry, linetype = idcountry)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT EU (Prim) (Share)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,1)) +
  coord_cartesian(ylim = c(0, 10)) +
  scale_colour_ggthemr_d() +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(nrow = 1)) +
  theme(text = element_text(size = 16))
p4
ggexport(p4, filename = "./plots/text_based/eu_prim_wfh_index_share_by_country_10_sr.pdf")
remove(p4)

p4 = df_levels_country %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop_index, colour = idcountry, linetype = idcountry)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT EU (Prim) (Share)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,1)) +
  coord_cartesian(ylim = c(0, 5)) +
  scale_colour_ggthemr_d() +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(nrow = 1)) +
  theme(text = element_text(size = 16))
p4
ggexport(p4, filename = "./plots/text_based/eu_prim_wfh_index_share_by_country_5_sr.pdf")
remove(p4)

# 3. TIME SERIES PLOTS BY ESCO MAJOR GROUP
# Load Data
remove(list = setdiff(ls(),"df_stru"))
#load(file = "./int_data/df_stru_wfh_eu_prim.RData")
obs <- nrow(df_stru)
df_stru <- df_stru %>% as_tibble
head(df_stru)
df_levels_esco1 <- df_stru %>%
  mutate(idesco_level_1 = str_sub(idesco_level_2, 1, 1)) %>%
  filter(idesco_level_1 != "") %>%
  filter(!is.na(idesco_level_1)) %>%
  group_by(year_quarter, idesco_level_1) %>%
  summarise(n_posts = n(),
            wfh_non_neg = sum(wfh_non_neg)
  ) %>%
  mutate(across(wfh_non_neg, ~ .x / n_posts, .names = "{.col}_prop")) %>%
  ungroup() %>%
  group_by(idesco_level_1) %>%
  mutate(across(wfh_non_neg, ~ .x / .x[year_quarter == "2019 Q1"], .names = "{.col}_index")) %>%
  mutate(across(wfh_non_neg_prop, ~ .x / .x[year_quarter == "2019 Q1"], .names = "{.col}_index"))

# Names
df_levels_esco1 <- df_levels_esco1 %>%
  mutate(esco1 = case_when(
    idesco_level_1 == "0" ~ "0. Armed forces",
    idesco_level_1 == "1" ~ "1. Managers",
    idesco_level_1 == "2" ~ "2. Professionals",
    idesco_level_1 == "3" ~ "3. Technicians",
    idesco_level_1 == "4" ~ "4. Clerical support",
    idesco_level_1 == "5" ~ "5. Service & sales",
    idesco_level_1 == "6" ~ "6. Skilled agricultural",
    idesco_level_1 == "7" ~ "7. Craft & related trades",
    idesco_level_1 == "8" ~ "8. Plant & machine operators",
    idesco_level_1 == "9" ~ "9. Elementary"))

# 3.1 By Count
p3 = df_levels_esco1 %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg, colour = esco1, linetype = esco1)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Count of WFH Job Ads in BGT EU (Prim)") +
  ylab("Count of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(label=comma, limits = c(0,NA), n.breaks = 10) + 
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(ncol = 2)) +
  theme(text = element_text(size = 16))
p3
ggexport(p3, filename = "./plots/text_based/eu_prim_wfh_level_by_esco1_sr.pdf")
remove(p3)

# 3.2 By Share
p3 = df_levels_esco1 %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop, colour = esco1, linetype = esco1)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT EU (Prim)") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01),  breaks = seq(0,1,0.01)) +
  coord_cartesian(ylim = c(0, 0.20)) +
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(ncol = 2)) +
  theme(text = element_text(size = 16))
p3
ggexport(p3, filename = "./plots/text_based/eu_prim_wfh_share_by_esco1_20_sr.pdf")
remove(p3)

p3 = df_levels_esco1 %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop, colour = esco1, linetype = esco1)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT EU (Prim)") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01),  breaks = seq(0,1,0.01)) +
  coord_cartesian(ylim = c(0, 0.10)) +
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(ncol = 2)) +
  theme(text = element_text(size = 16))
p3
ggexport(p3, filename = "./plots/text_based/eu_prim_wfh_share_by_esco1_10_sr.pdf")
remove(p3)

p3 = df_levels_esco1 %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop, colour = esco1, linetype = esco1)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT EU (Prim)") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01),  breaks = seq(0,1,0.01)) +
  coord_cartesian(ylim = c(0, 0.05)) +
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(ncol = 2)) +
  theme(text = element_text(size = 16))
p3
ggexport(p3, filename = "./plots/text_based/eu_prim_wfh_share_by_esco1_5_sr.pdf")
remove(p3)

# 3.3 By Index (Count)
p4 = df_levels_esco1 %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_index, colour = esco1, linetype = esco1)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT EU (Prim) (Count)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,1)) +
  coord_cartesian(ylim = c(0, 19.5)) +
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(ncol = 2)) +
  theme(text = element_text(size = 16))
p4
ggexport(p4, filename = "./plots/text_based/eu_prim_wfh_index_count_by_esco1_20_sr.pdf")
remove(p4)

p4 = df_levels_esco1 %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_index, colour = esco1, linetype = esco1)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT EU (Prim) (Count)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,1)) +
  coord_cartesian(ylim = c(0, 10)) +
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(ncol = 2)) +
  theme(text = element_text(size = 16))
p4
ggexport(p4, filename = "./plots/text_based/eu_prim_wfh_index_count_by_esco1_10_sr.pdf")
remove(p4)

# 3.4 By Index (Share)
p4 = df_levels_esco1 %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop_index, colour = esco1, linetype = esco1)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT EU (Prim) (Share)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,1)) +
  coord_cartesian(ylim = c(0, 10)) +
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(ncol = 2)) +
  theme(text = element_text(size = 16))
p4
ggexport(p4, filename = "./plots/text_based/eu_prim_wfh_index_share_by_esco1_10_sr.pdf")
remove(p4)

p4 = df_levels_esco1 %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop_index, colour = esco1, linetype = esco1)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT EU (Prim) (Share)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,1)) +
  coord_cartesian(ylim = c(0, 5)) +
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(ncol = 2)) +
  theme(text = element_text(size = 16))
p4
ggexport(p4, filename = "./plots/text_based/eu_prim_wfh_index_share_by_esco1_5_sr.pdf")
remove(p4)

# 4. TIME SERIES PLOTS BY DN TELEWORK COMPARISON
# Load Data
remove(list = setdiff(ls(),"df_stru"))
#load(file = "./int_data/df_stru_wfh_eu_prim.RData")
obs <- nrow(df_stru)
df_stru <- df_stru %>% as_tibble

dn <- haven::read_dta("./aux_data/country_isco08_telework.dta") %>%
  clean_names %>%
  filter(country_code %in% c("BEL", "DEU", "FRA", "ITA", "LUX", "NLD")) %>%
  mutate(idcountry = str_sub(country_code, 1, 2)) %>%
  select(idcountry, isco08_code_2digit, teleworkable) %>%
  as_tibble

nrow(df_stru) # 130,994,648
df_stru <- df_stru %>%
  left_join(dn, by = c("idcountry" = "idcountry", "idesco_level_2" = "isco08_code_2digit"))
nrow(df_stru) #  130,994,648
colnames(df_stru)

df_stru <- df_stru %>%
  mutate(teleworkable_bin = ifelse(teleworkable>=0.5, "Teleworkable", "Not"))

df_levels_telework <- df_stru %>%
  filter(!is.na(teleworkable_bin)) %>%
  group_by(year_quarter, teleworkable_bin) %>%
  summarise(n_posts = n(),
            wfh_non_neg = sum(wfh_non_neg)
  ) %>%
  mutate(across(wfh_non_neg, ~ .x / n_posts, .names = "{.col}_prop")) %>%
  ungroup() %>%
  group_by(teleworkable_bin) %>%
  mutate(across(wfh_non_neg, ~ .x / .x[year_quarter == "2019 Q1"], .names = "{.col}_index")) %>%
  mutate(across(wfh_non_neg_prop, ~ .x / .x[year_quarter == "2019 Q1"], .names = "{.col}_index"))

# 4.1 By Count
p3 = df_levels_telework %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg, colour = teleworkable_bin, linetype = teleworkable_bin)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Count of WFH Job Ads in BGT EU (Prim)") +
  ylab("Count of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(label=comma, limits = c(0,NA), n.breaks = 10) + 
  scale_colour_ggthemr_d() +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  theme(text = element_text(size = 16))
p3
ggexport(p3, filename = "./plots/text_based/eu_prim_wfh_level_by_telework_sr.pdf")
remove(p3)

# 4.2 By Share
p3 = df_levels_telework %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop, colour = teleworkable_bin, linetype = teleworkable_bin)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT EU (Prim)") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01),  breaks = seq(0,1,0.01)) +
  coord_cartesian(ylim = c(0, 0.195)) +
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  theme(text = element_text(size = 16))
p3
ggexport(p3, filename = "./plots/text_based/eu_prim_wfh_share_by_telework_20_sr.pdf")
remove(p3)

p3 = df_levels_telework %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop, colour = teleworkable_bin, linetype = teleworkable_bin)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT EU (Prim)") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01),  breaks = seq(0,1,0.01)) +
  coord_cartesian(ylim = c(0, 0.10)) +
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  theme(text = element_text(size = 16))
p3
ggexport(p3, filename = "./plots/text_based/eu_prim_wfh_share_by_telework_10_sr.pdf")
remove(p3)

p3 = df_levels_telework %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop, colour = teleworkable_bin, linetype = teleworkable_bin)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT EU (Prim)") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01),  breaks = seq(0,1,0.01)) +
  coord_cartesian(ylim = c(0, 0.05)) +
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  theme(text = element_text(size = 16))
p3
ggexport(p3, filename = "./plots/text_based/eu_prim_wfh_share_by_telework_5_sr.pdf")
remove(p3)

# 4.3 By Index (Count)
p4 = df_levels_telework %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_index, colour = teleworkable_bin, linetype = teleworkable_bin)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT EU (Prim) (Count)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,1)) +
  coord_cartesian(ylim = c(0, 19.5)) +
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  theme(text = element_text(size = 16))
p4
ggexport(p4, filename = "./plots/text_based/eu_prim_wfh_index_count_by_telework_20_sr.pdf")
remove(p4)

p4 = df_levels_telework %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_index, colour = teleworkable_bin, linetype = teleworkable_bin)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT EU (Prim) (Count)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,1)) +
  coord_cartesian(ylim = c(0, 10)) +
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  theme(text = element_text(size = 16))
p4
ggexport(p4, filename = "./plots/text_based/eu_prim_wfh_index_count_by_telework_10_sr.pdf")
remove(p4)

# 4.4 By Index (Share)
p4 = df_levels_telework %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop_index, colour = teleworkable_bin, linetype = teleworkable_bin)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT EU (Prim) (Share)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,1)) +
  coord_cartesian(ylim = c(0, 10)) +
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  theme(text = element_text(size = 16))
p4
ggexport(p4, filename = "./plots/text_based/eu_prim_wfh_index_share_by_telework_10_sr.pdf")
remove(p4)

p4 = df_levels_telework %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop_index, colour = teleworkable_bin, linetype = teleworkable_bin)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT EU (Prim) (Share)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,1)) +
  coord_cartesian(ylim = c(0, 5)) +
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  theme(text = element_text(size = 16))
p4
ggexport(p4, filename = "./plots/text_based/eu_prim_wfh_index_share_by_telework_5_sr.pdf")
remove(p4)

#### /end ####

#### COEF PLOTS ####
remove(list = setdiff(ls(),"df_stru"))
load(file = "./int_data/df_stru_wfh_eu_prim.RData")
obs <- nrow(df_stru)

df_stru <- df_stru %>%
  mutate(year_month = paste0(year(ymd(job_date)),"_",month(ymd(job_date)))) %>%
  mutate(year_quarter_text = paste0(year(ymd(job_date)), ".", quarter(ymd(job_date)))) %>%
  mutate(month = month(ymd(job_date)))

df_stru <- df_stru %>%
  mutate(later_quarters = case_when(
    year_quarter_text %in% c("2019.3") ~ "2019Q3",
    year_quarter_text %in% c("2019.4") ~ "2019Q4",
    year_quarter_text %in% c("2020.1") ~ "2020Q1",
    year_quarter_text %in% c("2020.2") ~ "2020Q2",
    year_quarter_text %in% c("2020.3") ~ "2020Q3",
    year_quarter_text %in% c("2020.4") ~ "2020Q4",
    year_quarter_text %in% c("2021.1") ~ "2021Q1",
    year_quarter_text %in% c("2021.2") ~ "2021Q2",
    year_quarter_text %in% c("2021.3") ~ "2021Q3",
    year_quarter_text %in% c("2021.4") ~ "2021Q4",
    TRUE ~ "1pre"
  ))

# 1. Compare Quarter Dummies using different FEs
# 1.1 Run Regressions
regs_wfh <- lapply(1:4, function(i) {
  if(i == 1) {lm <- feols(wfh_non_neg ~ 0 + i(later_quarters, ref = "1pre"), df_stru %>% filter(!is.na(idregion) & idregion != "" & !is.na(idesco_level_4) & idesco_level_4 != ""), cluster = ~ year_month, lean = T)}
  if(i == 2) {lm <- feols(wfh_non_neg ~ i(later_quarters, ref = "1pre") | idesco_level_4, df_stru %>% filter(!is.na(idregion) & idregion != "" & !is.na(idesco_level_4) & idesco_level_4 != ""), cluster = ~ year_month, lean = T)}
  if(i == 3) {lm <- feols(wfh_non_neg ~ i(later_quarters, ref = "1pre") | idregion, df_stru %>% filter(!is.na(idregion) & idregion != "" & !is.na(idesco_level_4) & idesco_level_4 != ""), cluster = ~ year_month, lean = T)}
  if(i == 4) {lm <- feols(wfh_non_neg ~ i(later_quarters, ref = "1pre") | idregion + idesco_level_4, df_stru %>% filter(!is.na(idregion) & idregion != "" & !is.na(idesco_level_4) & idesco_level_4 != ""), cluster = ~ year_month, lean = T)}
  warning("\n\n############# ",i," #############\n\n")
  return(lm)
})

obs <- df_stru %>% filter(!is.na(idregion) & idregion != "" & !is.na(idesco_level_4) & idesco_level_4 != "") %>% nrow(.)

# 1.2 Make Plots
for_plot_wfh <- lapply(1:4, function(i) {
  regs_wfh[[i]]$coeftable %>%
    as_tibble %>%
    mutate(estimate = Estimate, lb = (Estimate - 2*`Std. Error`), ub = (Estimate + 2*`Std. Error`)) %>%
    select(estimate, lb, ub) %>%
    as_tibble %>%
    mutate(quarter = as.yearqtr(gsub("later_quarters::", "", rownames(as.matrix(regs_wfh[[i]]$coefficients)))))
})

# 1.3 Stack Data
for_plot_long_wfh <- bind_rows(for_plot_wfh[[1]] %>% mutate(FEs = "None"),
                               for_plot_wfh[[2]] %>% mutate(FEs = "ESCO4"),
                               for_plot_wfh[[3]] %>% mutate(FEs = "Region"),
                               for_plot_wfh[[4]] %>% mutate(FEs = "ESCO4 + Region"))

View(for_plot_long_wfh)
# 1.4 Plot
p1 = for_plot_long_wfh %>%
  ggplot(., aes(x = quarter, y = estimate, linetype = FEs, color = FEs)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  geom_ribbon(aes(ymin = lb, ymax = ub), alpha = 0.10, fill = "Black", size = 0) +
  ggtitle("Coefficient Plot of WFH in EU (Prim)") +
  ylab("Quarter Dummy Coefficient") +
  xlab("Year/Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q3"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q3"), as.yearqtr("2021 Q2"))) +
  scale_colour_ggthemr_d() +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01), breaks = seq(-0.05,0.3,0.01)) +
  coord_cartesian(ylim = c(-0.01, 0.10)) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=",")),
       caption = "Note: Using BGT EU (Prim) (BE, FR, IT, LU, NL and UK).\n+/- 2 SD in dark grey.") +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(ncol = 4)) +
  theme(text = element_text(size = 16))
p1
ggexport(p1, filename = "./plots/text_based/eu_prim_wfh_coeff_plot_10.pdf")
remove(p1)

p1 = for_plot_long_wfh %>%
  ggplot(., aes(x = quarter, y = estimate, linetype = FEs, color = FEs)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  geom_ribbon(aes(ymin = lb, ymax = ub), alpha = 0.10, fill = "Black", size = 0) +
  ggtitle("Coefficient Plot of WFH in EU (Prim)") +
  ylab("Quarter Dummy Coefficient") +
  xlab("Year/Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q3"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q3"), as.yearqtr("2021 Q2"))) +
  scale_colour_ggthemr_d() +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01), breaks = seq(-0.05,0.3,0.01)) +
  coord_cartesian(ylim = c(-0.01, 0.05)) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=",")),
       caption = "Note: Using BGT EU (Prim) (BE, FR, IT, LU, NL and UK).\n+/- 2 SD in dark grey.") +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(ncol = 4)) +
  theme(text = element_text(size = 16))
p1
ggexport(p1, filename = "./plots/text_based/eu_prim_wfh_coeff_plot_5.pdf")
remove(p1)

p1 = for_plot_long_wfh %>%
  ggplot(., aes(x = quarter, y = estimate, linetype = FEs, color = FEs)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  geom_ribbon(aes(ymin = lb, ymax = ub), alpha = 0.10, fill = "Black", size = 0) +
  ggtitle("Coefficient Plot of WFH in EU (Prim)") +
  ylab("Quarter Dummy Coefficient") +
  xlab("Year/Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q3"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q3"), as.yearqtr("2021 Q2"))) +
  scale_colour_ggthemr_d() +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01), breaks = seq(-0.05,0.3,0.01)) +
  coord_cartesian(ylim = c(-0.01, 0.03)) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=",")),
       caption = "Note: Using BGT EU (Prim) (BE, FR, IT, LU, NL and UK).\n+/- 2 SD in dark grey.") +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(ncol = 4)) +
  theme(text = element_text(size = 16))
p1
ggexport(p1, filename = "./plots/text_based/eu_prim_wfh_coeff_plot_3.pdf")
remove(p1)

# 2. Compare Quarter Dummies across Countries
remove(list = setdiff(ls(),"df_stru"))
#load(file = "./int_data/df_stru_wfh_eu_prim.RData")
obs <- nrow(df_stru)

# 2.1 Run Regressions
regs_wfh <- lapply(1:6, function(i) {
  if(i == 1) {lm <- feols(wfh_non_neg ~ later_quarters | idregion + idesco_level_4, df_stru %>% filter(idcountry == "BE"), cluster = ~ year_month, lean = T)}
  if(i == 2) {lm <- feols(wfh_non_neg ~ later_quarters | idregion + idesco_level_4, df_stru %>% filter(idcountry == "FR"), cluster = ~ year_month, lean = T)}
  if(i == 3) {lm <- feols(wfh_non_neg ~ later_quarters | idregion + idesco_level_4, df_stru %>% filter(idcountry == "IT"), cluster = ~ year_month, lean = T)}
  if(i == 4) {lm <- feols(wfh_non_neg ~ later_quarters | idregion + idesco_level_4, df_stru %>% filter(idcountry == "LU"), cluster = ~ year_month, lean = T)}
  if(i == 5) {lm <- feols(wfh_non_neg ~ later_quarters | idregion + idesco_level_4, df_stru %>% filter(idcountry == "NL"), cluster = ~ year_month, lean = T)}
  if(i == 6) {lm <- feols(wfh_non_neg ~ later_quarters | idregion + idesco_level_4, df_stru %>% filter(idcountry == "UK"), cluster = ~ year_month, lean = T)}
  warning("\n\n############# ",i," #############\n\n")
  return(lm)
})
# 2.2 Make Plots
for_plot_wfh <- lapply(1:6, function(i) {
  regs_wfh[[i]]$coeftable %>%
    as_tibble %>%
    mutate(estimate = Estimate, lb = (Estimate - 2*`Std. Error`), ub = (Estimate + 2*`Std. Error`)) %>%
    select(estimate, lb, ub) %>%
    as_tibble %>%
    mutate(quarter = as.yearqtr(gsub("later_quarters", "", rownames(as.matrix(regs_wfh[[i]]$coefficients)))))
})
# 2.3 Stack Data
for_plot_long_wfh_country <- bind_rows(for_plot_wfh[[1]] %>% mutate(Country = "BE"),
                                       for_plot_wfh[[2]] %>% mutate(Country = "FR"),
                                       for_plot_wfh[[3]] %>% mutate(Country = "IT"),
                                       for_plot_wfh[[4]] %>% mutate(Country = "LU"),
                                       for_plot_wfh[[5]] %>% mutate(Country = "NL"),
                                       for_plot_wfh[[6]] %>% mutate(Country = "UK"))
# 2.4 Plot
# 2.4 Plot
p1 = for_plot_long_wfh_country %>%
  ggplot(., aes(x = quarter, y = estimate, linetype = Country, color = Country)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  geom_ribbon(aes(ymin = lb, ymax = ub), alpha = 0.10, fill = "Black", size = 0) +
  ggtitle("Coefficient Plot of WFH in EU (Prim)") +
  ylab("Quarter Dummy Coefficient") +
  xlab("Year") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2018 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2018 Q1"), as.yearqtr("2021 Q2"))) +
  scale_colour_ggthemr_d() +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01), breaks = seq(-0.05,0.3,0.01)) +
  coord_cartesian(ylim = c(-0.01, 0.10)) +
  labs(subtitle = paste0("Fixed Effects: Month + ESCO4 + NUTS3 Region. n = ",format(obs, big.mark=",")),
       caption = "Note: Using BGT EU (Prim).\n+/- 2 SD in dark grey.") +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(nrow = 1)) +
  theme(text = element_text(size = 16))
p1
ggexport(p1, filename = "./plots/text_based/eu_prim_wfh_coeff_plot_by_country_10.pdf")
remove(p1)

p1 = for_plot_long_wfh_country %>%
  ggplot(., aes(x = quarter, y = estimate, linetype = Country, color = Country)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  geom_ribbon(aes(ymin = lb, ymax = ub), alpha = 0.10, fill = "Black", size = 0) +
  ggtitle("Coefficient Plot of WFH in EU (Prim)") +
  ylab("Quarter Dummy Coefficient") +
  xlab("Year") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2018 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2018 Q1"), as.yearqtr("2021 Q2"))) +
  scale_colour_ggthemr_d() +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01), breaks = seq(-0.05,0.3,0.01)) +
  coord_cartesian(ylim = c(-0.01, 0.05)) +
  labs(subtitle = paste0("Fixed Effects: Month + ESCO4 + NUTS3 Region. n = ",format(obs, big.mark=",")),
       caption = "Note: Using BGT EU (Prim).\n+/- 2 SD in dark grey.") +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(nrow = 1)) +
  theme(text = element_text(size = 16))
p1
ggexport(p1, filename = "./plots/text_based/eu_prim_wfh_coeff_plot_by_country_5.pdf")
remove(p1)

p1 = for_plot_long_wfh_country %>%
  ggplot(., aes(x = quarter, y = estimate, linetype = Country, color = Country)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  geom_ribbon(aes(ymin = lb, ymax = ub), alpha = 0.10, fill = "Black", size = 0) +
  ggtitle("Coefficient Plot of WFH in EU (Prim)") +
  ylab("Quarter Dummy Coefficient") +
  xlab("Year") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2018 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2018 Q1"), as.yearqtr("2021 Q2"))) +
  scale_colour_ggthemr_d() +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01), breaks = seq(-0.05,0.3,0.01)) +
  coord_cartesian(ylim = c(-0.01, 0.03)) +
  labs(subtitle = paste0("Fixed Effects: Month + ESCO4 + NUTS3 Region. n = ",format(obs, big.mark=",")),
       caption = "Note: Using BGT EU (Prim).\n+/- 2 SD in dark grey.") +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(nrow = 1)) +
  theme(text = element_text(size = 16))
p1
ggexport(p1, filename = "./plots/text_based/eu_prim_wfh_coeff_plot_by_country_3.pdf")
remove(p1)

#### END ####


#### CREATE AGGREGATED DATA ####
colnames(df_stru)

df_stru_ag <- df_stru %>%
  group_by(year_quarter, idesco_level_2, idesco_level_4, idcountry, idregion, teleworkable, teleworkable_bin) %>%
  summarise(n = n(),
            wfh_raw = sum(wfh_raw),
            wfh_ustring = sum(wfh_ustring),
            wfh_non_neg = sum(wfh_non_neg),
            wfh_raw_body = sum(wfh_raw_body),
            wfh_ustring_body = sum(wfh_ustring_body),
            wfh_non_neg_bod = sum(wfh_non_neg_body))

fwrite(df_stru_ag, "./int_data/eu_prim_wfh_ag_df.csv")

#### END ####
