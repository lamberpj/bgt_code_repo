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

quanteda_options(threads = 4)
setwd("/mnt/disks/pdisk/bg-us/")
setDTthreads(4)

remove(list = ls())
#### end ####

#### DICTIONARIES ####
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
df_dict_wfh <- df_dict %>% filter(cluster == "wfh")
ls_dict_wfh <- setNames(as.list(df_dict_wfh$glob_en), df_dict_wfh$key)
wfh_dict <- dictionary(ls_dict_wfh)

neg_dict <- dictionary(list(
  uneg = "no",
  uneg = "not",
  uneg = "can't",
  uneg = "cannot",
  uneg = "unable",
  hb_ne = "care",
  hb_ne = "assessments",
  hb_ne = "training",
  hb_ne = "supervisory",
  hb_ne = "health",
  hb_ne = "provider",
  hb_ne = "services",
  wah_ne = "Travel",
  wah_ne = "Number of Replacement"))

#### end ####

#### WFH from DFMs ####
paths_dfm <- list.files("./int_data/tech", pattern = "tech_dfm_", full.names = T)
paths_raw_text <- list.files("./int_data/tech", pattern = "tech_raw_text_", full.names = T)

df_wfh <- safe_mclapply(1:length(paths_dfm), function(i) {
  warning(paste0("START: ",i))
  x <- readRDS(paths_dfm[[i]]) %>%
    convert(., to = "data.frame") %>%
    select(doc_id, based_at_home, based_from_home, home_based, home_based_2, home_working, remote_work,
           telecommute, telecommute_c_yes, telecommute_yes, telecommuting, work_at_home, work_at_home_c_yes,
           work_at_home_yes, work_from_home, work_from_home_2, work_from_home_c_yes, work_from_home_yes,
           working_at_home, working_from_home) %>%
    filter(based_at_home + based_from_home + home_based + home_based_2 + home_working + remote_work + 
             telecommute + telecommute_c_yes + telecommute_yes + telecommuting + work_at_home + 
             work_at_home_c_yes + work_at_home_yes + work_from_home + work_from_home_2 + 
             work_from_home_c_yes + work_from_home_yes + working_at_home + working_from_home>0)
  warning(paste0("DONE: ",i))
  return(x)
}, mc.cores = 30)

df_wfh <- df_wfh %>% bind_rows

df_wfh_mean <- df_wfh %>%
  summarise(across(based_at_home:working_from_home, ~ mean(.x, na.rm = TRUE)))
df_wfh_sum <- df_wfh %>%
  summarise(across(based_at_home:working_from_home, ~ sum(.x, na.rm = TRUE)))
df_wfh_ss <- bind_rows(df_wfh_mean, df_wfh_sum)
df_wfh_ss <- t(df_wfh_ss) %>% as.data.frame
colnames(df_wfh_ss) <- c("Prop", "Count")
df_wfh_ss <- df_wfh_ss %>% rownames_to_column(var = "key")
nrow(df_wfh_ss) # 19
df_wfh_ss <- df_wfh_ss %>%
  left_join(df_dict_wfh %>% select(key, glob_en))

df_wfh_ss <- df_wfh_ss %>% select(glob_en, Prop, Count) %>% rename(Key = glob_en)
stargazer(df_wfh_ss, summary=FALSE, rownames=TRUE, title = "BGT US WFH Hits by Keyword")

#### end ####

#### WFH raw text ####
df_kwic <- safe_mclapply(1:length(paths_raw_text), function(i) {
  warning(paste0("START: ",i))
  x <- readRDS(paths_raw_text[[i]]) %>%
    filter(job_id %in% df_wfh$doc_id)
  df_corpus <- corpus(x, text_field = "job_text", docid_field = "job_id")
  df_tokens <- tokens(df_corpus)
  x <- x %>%
    mutate(n = ntoken(df_tokens))
  df_kwic <- kwic(df_tokens, pattern = wfh_dict, window = 5) %>%
    as.data.frame(.)
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
  
}, mc.cores = 4)

#df_kwic <- df_kwic %>%
#  bind_rows(.)

#saveRDS(df_kwic, "./int_data/df_kwic_us.rds")

head(df_kwic)

#### end ####

#### INSPECT ####
remove(list = ls())
df_kwic <- readRDS("./int_data/df_kwic_us.rds")

df_kwic <- df_kwic %>%
  mutate(pre_neg = ifelse((uneg_pre > 0),1,0),
         pre_dis = ifelse((hb_ne_pre > 0 & pattern %in% c("home_based", "home_based_2", "based_from_home")) | (wah_ne_pre > 0 & pattern %in% c("work_at_home")),1,0),
         post_neg = ifelse((uneg_post > 0),1,0),
         post_dis = ifelse((hb_ne_post > 0 & pattern %in% c("home_based", "home_based_2", "based_from_home")) | (wah_ne_post > 0 & pattern %in% c("work_at_home")),1,0))

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

stargazer(t(df_kwic_u_ss), summary=FALSE, rownames=TRUE, title = "BGT US WFH Hits by Method")

wfh_df <- df_kwic_u %>%
  summarise(bgt_job_id = docname,
            wfh_raw = 1,
            wfh_ustring = ifelse(n_string < 500, 1, 0),
            wfh_non_neg = ifelse(pre_neg == 0 & post_neg == 0 & pre_dis == 0 & post_dis == 0, 1, 0),
            wfh_raw_body = ifelse(trim_neg == 0, 1, 0),
            wfh_ustring_body = ifelse(trim_neg == 0 & n_string < 500, 1, 0),
            wfh_non_neg_body = ifelse(trim_neg == 0 & pre_neg == 0 & post_neg == 0 & pre_dis == 0 & post_dis == 0, 1, 0)) %>%
  select(bgt_job_id, wfh_raw, wfh_ustring, wfh_non_neg, wfh_raw_body, wfh_ustring_body, wfh_non_neg_body)

saveRDS(wfh_df, "./int_data/wfh_df.rds")




head(df_kwic_u)

#### COMBINE AND MERGE ####
remove(list = ls())
wfh_df <- readRDS("./int_data/wfh_df.rds")
remove(list = setdiff(ls(), c("wfh_df", "df_stru")))

#### Structured ####
paths <- list.files("./raw_data/main", pattern = ".txt", full.names = T)

colnames(fread(paths[[1]]))

df_stru <- safe_mclapply(1:length(paths), function(i) {
  warning(paste0("\nSTART: ",i))
  df <- fread(paths[i], data.table = F, nThread = 1, colClasses = "character", stringsAsFactors = FALSE,
              select = c("BGTJobId", "JobDate", "SOC", "ONET", "MSA", "State", "NAICS3")) %>%
    clean_names
  warning(paste0("\nDONE: ",i))
  return(df)
}, mc.cores = 16)

df_stru <- df_stru %>% bind_rows
#### /end ####

#### Merg in WFH ####
nrow(df_stru) # 233,760,170
df_stru <- df_stru %>%
  left_join(wfh_df)
nrow(df_stru)

df_stru <- df_stru  %>%
  mutate(across(where(is.numeric), ~if_else(is.na(.x), 0, .x)))

df_stru <- df_stru  %>%
  mutate(across(where(is.numeric), ~if_else(.x>1, 1, .x)))

df_stru <- df_stru %>%
  mutate(soc2 = str_sub(soc, 1, 2)) %>%
  select(bgt_job_id, job_date, soc2, soc, everything())

# Make Quarter
df_stru <- df_stru %>%
  mutate(year_quarter = as.yearqtr(ymd(job_date), with_year = T))

save(df_stru, file = "./int_data/df_stru_wfh_us.RData")

head(df_stru)

#### /end ####

#### PLOTS ####
remove(list = setdiff(ls(),"df_stru"))
load(file = "./int_data/df_stru_wfh_us.RData")
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
  ggtitle("Count of WFH Job Ads in BGT US") +
  ylab("Count of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2014 Q1"), to = as.yearqtr("2021 Q2"), by = 0.5),  limits = c(as.yearqtr("2014 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(label=comma, limits = c(0,NA), n.breaks = 10) + 
  scale_colour_ggthemr_d() +
  labs(subtitle = paste0("n = ",format(obs, big.mark=",")),
       caption = "Note: BGT US") +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm"),
    text = element_text(size = 16))
p1
ggexport(p1, filename = "./plots/text_based/us_wfh_level_by_parsing.pdf")
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
  ggtitle("Share of WFH Job Ads in BGT US") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2014 Q1"), to = as.yearqtr("2021 Q2"), by = 0.5), limits = c(as.yearqtr("2014 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01),  breaks = seq(0,1,0.01)) +
  coord_cartesian(ylim = c(0, 0.195)) +
  scale_colour_ggthemr_d() +
  labs(subtitle = paste0("n = ",format(obs, big.mark=",")),
       caption = "Note: BGT US") +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm"),
    text = element_text(size = 16))
p2
ggexport(p2, filename = "./plots/text_based/us_wfh_share_by_parsing_20.pdf")
remove(p2)

p2 = df_ts_share %>%
  ggplot(., aes(x = year_quarter, y = value, colour = name, linetype = name)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT US") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2014 Q1"), to = as.yearqtr("2021 Q2"), by = 0.5), limits = c(as.yearqtr("2014 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01),  breaks = seq(0,1,0.01)) +
  coord_cartesian(ylim = c(0, 0.10)) +
  scale_colour_ggthemr_d() +
  labs(subtitle = paste0("n = ",format(obs, big.mark=",")),
       caption = "Note: BGT US") +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm"),
    text = element_text(size = 16))
p2
ggexport(p2, filename = "./plots/text_based/us_wfh_share_by_parsing_10.pdf")
remove(p2)

p2 = df_ts_share %>%
  ggplot(., aes(x = year_quarter, y = value, colour = name, linetype = name)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT US") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2014 Q1"), to = as.yearqtr("2021 Q2"), by = 0.5), limits = c(as.yearqtr("2014 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01),  breaks = seq(0,1,0.01)) +
  coord_cartesian(ylim = c(0, 0.05)) +
  scale_colour_ggthemr_d() +
  labs(subtitle = paste0("n = ",format(obs, big.mark=",")),
       caption = "Note: BGT US") +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm"),
    text = element_text(size = 16))
p2
ggexport(p2, filename = "./plots/text_based/us_wfh_share_by_parsing_5.pdf")
remove(p2)

# 2. TIME SERIES PLOTS BY US REGION
# Load Data
remove(list = setdiff(ls(),"df_stru"))
#load(file = "./int_data/df_stru_wfh_eu_alt.RData")
obs <- nrow(df_stru)

us_regions <- read_xlsx("./aux_data/state-geocodes-v2017.xlsx", skip = 5) %>%
  clean_names %>%
  group_by(region) %>%
  mutate(region_name = name[1]) %>%
  filter(row_number() != 1) %>%
  ungroup() %>%
  mutate(state = tolower(name)) %>%
  select(state, region_name)

df_stru <- df_stru %>% mutate(state = tolower(state))

nrow(df_stru) # 233,760,170
df_stru <- df_stru %>%
  left_join(us_regions)
nrow(df_stru) # 

colnames(df_stru)

df_levels_region <- df_stru %>%
  filter(!is.na(region_name)) %>%
  group_by(year_quarter, region_name) %>%
  summarise(n_posts = n(),
            wfh_non_neg = sum(wfh_non_neg)
  ) %>%
  mutate(across(wfh_non_neg, ~ .x / n_posts, .names = "{.col}_prop")) %>%
  ungroup() %>%
  group_by(region_name) %>%
  mutate(across(wfh_non_neg, ~ .x / .x[year_quarter == "2019 Q1"], .names = "{.col}_index")) %>%
  mutate(across(wfh_non_neg_prop, ~ .x / .x[year_quarter == "2019 Q1"], .names = "{.col}_index"))

View(us_regions)

# 2.1 By Count
p3 = df_levels_region %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg, colour = region_name, linetype = region_name)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Count of WFH Job Ads in BGT US") +
  ylab("Count of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2014 Q1"), to = as.yearqtr("2021 Q2"), by = 0.5), limits = c(as.yearqtr("2014 Q1"), as.yearqtr("2021 Q2"))) +
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
ggexport(p3, filename = "./plots/text_based/us_wfh_level_by_country.pdf")
remove(p3)

# 2.2 By Share
p3 = df_levels_region %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop, colour = region_name, linetype = region_name)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT US") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2014 Q1"), to = as.yearqtr("2021 Q2"), by = 0.5), limits = c(as.yearqtr("2014 Q1"), as.yearqtr("2021 Q2"))) +
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
ggexport(p3, filename = "./plots/text_based/us_wfh_share_by_country_20.pdf")
remove(p3)

p3 = df_levels_region %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop, colour = region_name, linetype = region_name)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT US") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2014 Q1"), to = as.yearqtr("2021 Q2"), by = 0.5), limits = c(as.yearqtr("2014 Q1"), as.yearqtr("2021 Q2"))) +
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
ggexport(p3, filename = "./plots/text_based/us_wfh_share_by_country_10.pdf")
remove(p3)

p3 = df_levels_region %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop, colour = region_name, linetype = region_name)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT US") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2014 Q1"), to = as.yearqtr("2021 Q2"), by = 0.5), limits = c(as.yearqtr("2014 Q1"), as.yearqtr("2021 Q2"))) +
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
ggexport(p3, filename = "./plots/text_based/us_wfh_share_by_country_5.pdf")
remove(p3)

# 2.3 By Index (Count)
p4 = df_levels_region %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_index, colour = region_name, linetype = region_name)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT US (Count)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2014 Q1"), to = as.yearqtr("2021 Q2"), by = 0.5), limits = c(as.yearqtr("2014 Q1"), as.yearqtr("2021 Q2"))) +
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
ggexport(p4, filename = "./plots/text_based/us_wfh_index_count_by_country_20.pdf")
remove(p4)

p4 = df_levels_region %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_index, colour = region_name, linetype = region_name)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT US (Count)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2014 Q1"), to = as.yearqtr("2021 Q2"), by = 0.5), limits = c(as.yearqtr("2014 Q1"), as.yearqtr("2021 Q2"))) +
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
ggexport(p4, filename = "./plots/text_based/us_wfh_index_count_by_country_10.pdf")
remove(p4)

# 2.3 By Index (Share)
p4 = df_levels_region %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop_index, colour = region_name, linetype = region_name)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT US (Share)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2014 Q1"), to = as.yearqtr("2021 Q2"), by = 0.5), limits = c(as.yearqtr("2014 Q1"), as.yearqtr("2021 Q2"))) +
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
ggexport(p4, filename = "./plots/text_based/us_wfh_index_share_by_country_10.pdf")
remove(p4)

p4 = df_levels_region %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop_index, colour = region_name, linetype = region_name)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT US (Share)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2014 Q1"), to = as.yearqtr("2021 Q2"), by = 0.5), limits = c(as.yearqtr("2014 Q1"), as.yearqtr("2021 Q2"))) +
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
ggexport(p4, filename = "./plots/text_based/us_wfh_index_share_by_country_5.pdf")
remove(p4)

# 3. TIME SERIES PLOTS BY SOC MAJOR GROUP
# Load Data
remove(list = setdiff(ls(),"df_stru"))
#load(file = "./int_data/df_stru_wfh_eu_alt.RData")
obs <- nrow(df_stru)
df_stru <- df_stru %>% as_tibble
head(df_stru)

isco_soc <- read_xls("./aux_data/isco_soc_crosswalk.xls", skip = 5) %>%
  clean_names %>%
  select(isco_08_code, x2010_soc_code) %>%
  filter(str_sub(isco_08_code, 1, 1) != "0") %>%
  mutate(idesco_level_1 = str_sub(isco_08_code, 1, 1)) %>%
  group_by(x2010_soc_code) %>%
  summarise(n = n_distinct(idesco_level_1),
            idesco_level_1 = str_sub(isco_08_code[1], 1, 1)) %>%
  rename(soc = x2010_soc_code) %>%
  select(soc, idesco_level_1)
  
df_levels_esco1 <- df_stru %>%
  left_join(isco_soc) %>%
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

View(df_levels_esco1)

# 3.1 By Count
p3 = df_levels_esco1 %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg, colour = esco1, linetype = esco1)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Count of WFH Job Ads in BGT US") +
  ylab("Count of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2014 Q1"), to = as.yearqtr("2021 Q2"), by = 0.5), limits = c(as.yearqtr("2014 Q1"), as.yearqtr("2021 Q2"))) +
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
ggexport(p3, filename = "./plots/text_based/us_wfh_level_by_esco1.pdf")
remove(p3)

# 3.2 By Share
p3 = df_levels_esco1 %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop, colour = esco1, linetype = esco1)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT US") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2014 Q1"), to = as.yearqtr("2021 Q2"), by = 0.5), limits = c(as.yearqtr("2014 Q1"), as.yearqtr("2021 Q2"))) +
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
ggexport(p3, filename = "./plots/text_based/us_wfh_share_by_esco1_20.pdf")
remove(p3)

p3 = df_levels_esco1 %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop, colour = esco1, linetype = esco1)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT US") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2014 Q1"), to = as.yearqtr("2021 Q2"), by = 0.5), limits = c(as.yearqtr("2014 Q1"), as.yearqtr("2021 Q2"))) +
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
ggexport(p3, filename = "./plots/text_based/us_wfh_share_by_esco1_10.pdf")
remove(p3)

p3 = df_levels_esco1 %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop, colour = esco1, linetype = esco1)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT US") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2014 Q1"), to = as.yearqtr("2021 Q2"), by = 0.5), limits = c(as.yearqtr("2014 Q1"), as.yearqtr("2021 Q2"))) +
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
ggexport(p3, filename = "./plots/text_based/us_wfh_share_by_esco1_5.pdf")
remove(p3)

# 3.3 By Index (Count)
p4 = df_levels_esco1 %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_index, colour = esco1, linetype = esco1)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT US (Count)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2014 Q1"), to = as.yearqtr("2021 Q2"), by = 0.5), limits = c(as.yearqtr("2014 Q1"), as.yearqtr("2021 Q2"))) +
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
ggexport(p4, filename = "./plots/text_based/us_wfh_index_count_by_esco1_20.pdf")
remove(p4)

p4 = df_levels_esco1 %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_index, colour = esco1, linetype = esco1)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT US (Count)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2014 Q1"), to = as.yearqtr("2021 Q2"), by = 0.5), limits = c(as.yearqtr("2014 Q1"), as.yearqtr("2021 Q2"))) +
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
ggexport(p4, filename = "./plots/text_based/us_wfh_index_count_by_esco1_10.pdf")
remove(p4)

# 3.4 By Index (Share)
p4 = df_levels_esco1 %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop_index, colour = esco1, linetype = esco1)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT US (Share)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2014 Q1"), to = as.yearqtr("2021 Q2"), by = 0.5), limits = c(as.yearqtr("2014 Q1"), as.yearqtr("2021 Q2"))) +
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
ggexport(p4, filename = "./plots/text_based/us_wfh_index_share_by_esco1_10.pdf")
remove(p4)

p4 = df_levels_esco1 %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop_index, colour = esco1, linetype = esco1)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT US (Share)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2014 Q1"), to = as.yearqtr("2021 Q2"), by = 0.5), limits = c(as.yearqtr("2014 Q1"), as.yearqtr("2021 Q2"))) +
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
ggexport(p4, filename = "./plots/text_based/us_wfh_index_share_by_esco1_5.pdf")
remove(p4)

# 4. TIME SERIES PLOTS BY DN TELEWORK COMPARISON
# Load Data
remove(list = setdiff(ls(),"df_stru"))
#load(file = "./int_data/df_stru_wfh_eu_alt.RData")
obs <- nrow(df_stru)
df_stru <- df_stru %>% as_tibble

dn_df <- fread("./int_data/occupations_workathome.csv") %>% clean_names %>% as_tibble %>% select(-title)
head(dn_df)
head(df_stru)
nrow(df_stru) # 233,760,170
df_stru <- df_stru %>%
  left_join(dn_df, by = c("onet" = "onetsoccode"))
nrow(df_stru) #  233,760,170
colnames(df_stru)

df_stru <- df_stru %>%
  mutate(teleworkable_bin = ifelse(teleworkable==1, "Teleworkable", "Not"))

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
  ggtitle("Count of WFH Job Ads in BGT US") +
  ylab("Count of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2014 Q1"), to = as.yearqtr("2021 Q2"), by = 0.5), limits = c(as.yearqtr("2014 Q1"), as.yearqtr("2021 Q2"))) +
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
ggexport(p3, filename = "./plots/text_based/us_wfh_level_by_telework.pdf")
remove(p3)

# 4.2 By Share
p3 = df_levels_telework %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop, colour = teleworkable_bin, linetype = teleworkable_bin)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT US") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2014 Q1"), to = as.yearqtr("2021 Q2"), by = 0.5), limits = c(as.yearqtr("2014 Q1"), as.yearqtr("2021 Q2"))) +
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
ggexport(p3, filename = "./plots/text_based/us_wfh_share_by_telework_20.pdf")
remove(p3)

p3 = df_levels_telework %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop, colour = teleworkable_bin, linetype = teleworkable_bin)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT US") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2014 Q1"), to = as.yearqtr("2021 Q2"), by = 0.5), limits = c(as.yearqtr("2014 Q1"), as.yearqtr("2021 Q2"))) +
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
ggexport(p3, filename = "./plots/text_based/us_wfh_share_by_telework_10.pdf")
remove(p3)

p3 = df_levels_telework %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop, colour = teleworkable_bin, linetype = teleworkable_bin)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT US") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2014 Q1"), to = as.yearqtr("2021 Q2"), by = 0.5), limits = c(as.yearqtr("2014 Q1"), as.yearqtr("2021 Q2"))) +
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
ggexport(p3, filename = "./plots/text_based/us_wfh_share_by_telework_5.pdf")
remove(p3)

# 4.3 By Index (Count)
p4 = df_levels_telework %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_index, colour = teleworkable_bin, linetype = teleworkable_bin)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT US (Count)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2014 Q1"), to = as.yearqtr("2021 Q2"), by = 0.5), limits = c(as.yearqtr("2014 Q1"), as.yearqtr("2021 Q2"))) +
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
ggexport(p4, filename = "./plots/text_based/us_wfh_index_count_by_telework_20.pdf")
remove(p4)

p4 = df_levels_telework %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_index, colour = teleworkable_bin, linetype = teleworkable_bin)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT US (Count)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2014 Q1"), to = as.yearqtr("2021 Q2"), by = 0.5), limits = c(as.yearqtr("2014 Q1"), as.yearqtr("2021 Q2"))) +
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
ggexport(p4, filename = "./plots/text_based/us_wfh_index_count_by_telework_10.pdf")
remove(p4)

# 4.4 By Index (Share)
p4 = df_levels_telework %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop_index, colour = teleworkable_bin, linetype = teleworkable_bin)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT US (Share)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2014 Q1"), to = as.yearqtr("2021 Q2"), by = 0.5), limits = c(as.yearqtr("2014 Q1"), as.yearqtr("2021 Q2"))) +
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
ggexport(p4, filename = "./plots/text_based/us_wfh_index_share_by_telework_10.pdf")
remove(p4)

p4 = df_levels_telework %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop_index, colour = teleworkable_bin, linetype = teleworkable_bin)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT US (Share)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2014 Q1"), to = as.yearqtr("2021 Q2"), by = 0.5), limits = c(as.yearqtr("2014 Q1"), as.yearqtr("2021 Q2"))) +
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
ggexport(p4, filename = "./plots/text_based/us_wfh_index_share_by_telework_5.pdf")
remove(p4)

#### END ####

#### COEF PLOTS ####
remove(list = setdiff(ls(),"df_stru"))
load(file = "./int_data/df_stru_wfh_us.RData")
obs <- nrow(df_stru)

head(df_stru)

df_stru <- df_stru %>%
  mutate(year_month = paste0(year(ymd(job_date)),"_",month(ymd(job_date)))) %>%
  mutate(year_quarter_text = paste0(year(ymd(job_date)), ".", quarter(ymd(job_date)))) %>%
  mutate(month = month(ymd(job_date)))

head(df_stru)

df_stru <- df_stru %>%
  mutate(later_quarters = case_when(
    year_quarter_text %in% c("2018.1") ~ "2018Q1",
    year_quarter_text %in% c("2018.2") ~ "2018Q2",
    year_quarter_text %in% c("2018.3") ~ "2018Q3",
    year_quarter_text %in% c("2018.4") ~ "2018Q4",
    year_quarter_text %in% c("2019.1") ~ "2019Q1",
    year_quarter_text %in% c("2019.2") ~ "2019Q2",
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

regs_wfh <- safe_mclapply(1:6, function(i) {
  if(i == 1) {lm <- feols(wfh_non_neg ~ later_quarters | month, df_stru, cluster = ~ year_month, lean = T)}
  if(i == 2) {lm <- feols(wfh_non_neg ~ later_quarters | month + soc2, df_stru, cluster = ~ year_month, lean = T)}
  if(i == 3) {lm <- feols(wfh_non_neg ~ later_quarters | month + soc, df_stru, cluster = ~ year_month, lean = T)}
  if(i == 4) {lm <- feols(wfh_non_neg ~ later_quarters | month + state, df_stru, cluster = ~ year_month, lean = T)}
  if(i == 5) {lm <- feols(wfh_non_neg ~ later_quarters | month + msa, df_stru, cluster = ~ year_month, lean = T)}
  if(i == 6) {lm <- feols(wfh_non_neg ~ later_quarters | month + msa + soc, df_stru, cluster = ~ year_month, lean = T)}
  warning("\n\n############# ",i," #############\n\n")
  return(lm)
}, mc.cores = 1)

summary(regs_wfh[[1]])
summary(regs_wfh[[2]])
summary(regs_wfh[[3]])
summary(regs_wfh[[4]])
summary(regs_wfh[[5]])
summary(regs_wfh[[6]])

for_plot_wfh <- lapply(1:6, function(i) {
  regs_wfh[[i]]$coeftable %>%
    as_tibble %>%
    mutate(estimate = Estimate, lb = (Estimate - 2*`Std. Error`), ub = (Estimate + 2*`Std. Error`)) %>%
    select(estimate, lb, ub) %>%
    as_tibble %>%
    mutate(quarter = as.yearqtr(c("2018 Q1","2018 Q2","2018 Q3","2018 Q4","2019 Q1","2019 Q2","2019 Q3","2019 Q4",
                                  "2020 Q1","2020 Q2","2020 Q3","2020 Q4","2021 Q1", "2021 Q2")))
})

for_plot_long_wfh_1 <- bind_rows(for_plot_wfh[[1]] %>% mutate(FEs = "Month"),
                                 for_plot_wfh[[2]] %>% mutate(FEs = "Month + SOC2"),
                                 for_plot_wfh[[3]] %>% mutate(FEs = "Month + SOC"))

for_plot_long_wfh_2 <- bind_rows(for_plot_wfh[[4]] %>% mutate(FEs = "Month + State"),
                                 for_plot_wfh[[5]] %>% mutate(FEs = "Month + MSA"),
                                 for_plot_wfh[[6]] %>% mutate(FEs = "Month + SOC + MSA"))

p1 = for_plot_long_wfh_1 %>%
  ggplot(., aes(x = quarter, y = estimate, linetype = FEs, color = FEs)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  geom_ribbon(aes(ymin = lb, ymax = ub), alpha = 0.10, fill = "Black", size = 0) +
  ggtitle("Coefficient Plot of WFH in US") +
  ylab("Quarter Dummy Coefficient") +
  xlab("Year/Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2018 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2018 Q1"), as.yearqtr("2021 Q2"))) +
  scale_colour_ggthemr_d() +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01), breaks = seq(-0.05,0.3,0.01)) +
  coord_cartesian(ylim = c(-0.01, 0.10)) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=",")),
       caption = "Note: Using BGT US.\n+/- 2 SD in dark grey.") +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(ncol = 2)) +
  theme(text = element_text(size = 16))
p1
ggexport(p1, filename = "./plots/text_based/us_wfh_coeff1_plot_10.pdf")
remove(p1)

p1 = for_plot_long_wfh_1 %>%
  ggplot(., aes(x = quarter, y = estimate, linetype = FEs, color = FEs)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  geom_ribbon(aes(ymin = lb, ymax = ub), alpha = 0.10, fill = "Black", size = 0) +
  ggtitle("Coefficient Plot of WFH in US") +
  ylab("Quarter Dummy Coefficient") +
  xlab("Year/Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2018 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2018 Q1"), as.yearqtr("2021 Q2"))) +
  scale_colour_ggthemr_d() +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01), breaks = seq(-0.05,0.3,0.01)) +
  coord_cartesian(ylim = c(-0.01, 0.05)) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=",")),
       caption = "Note: Using BGT US.\n+/- 2 SD in dark grey.") +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(ncol = 2)) +
  theme(text = element_text(size = 16))
p1
ggexport(p1, filename = "./plots/text_based/us_wfh_coeff1_plot_5.pdf")
remove(p1)


p1 = for_plot_long_wfh_2 %>%
  ggplot(., aes(x = quarter, y = estimate, linetype = FEs, color = FEs)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  geom_ribbon(aes(ymin = lb, ymax = ub), alpha = 0.10, fill = "Black", size = 0) +
  ggtitle("Coefficient Plot of WFH in US") +
  ylab("Quarter Dummy Coefficient") +
  xlab("Year/Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2018 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2018 Q1"), as.yearqtr("2021 Q2"))) +
  scale_colour_ggthemr_d() +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01), breaks = seq(-0.05,0.3,0.01)) +
  coord_cartesian(ylim = c(-0.01, 0.10)) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=",")),
       caption = "Note: Using BGT US.\n+/- 2 SD in dark grey.") +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(ncol = 2)) +
  theme(text = element_text(size = 16))
p1
ggexport(p1, filename = "./plots/text_based/us_wfh_coeff2_plot_10.pdf")
remove(p1)

p1 = for_plot_long_wfh_2 %>%
  ggplot(., aes(x = quarter, y = estimate, linetype = FEs, color = FEs)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  geom_ribbon(aes(ymin = lb, ymax = ub), alpha = 0.10, fill = "Black", size = 0) +
  ggtitle("Coefficient Plot of WFH in US") +
  ylab("Quarter Dummy Coefficient") +
  xlab("Year/Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2018 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2018 Q1"), as.yearqtr("2021 Q2"))) +
  scale_colour_ggthemr_d() +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01), breaks = seq(-0.05,0.3,0.01)) +
  coord_cartesian(ylim = c(-0.01, 0.05)) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=",")),
       caption = "Note: Using BGT US.\n+/- 2 SD in dark grey.") +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(ncol = 2)) +
  theme(text = element_text(size = 16))
p1
ggexport(p1, filename = "./plots/text_based/us_wfh_coeff2_plot_5.pdf")
remove(p1)

#### END ####

#### CREATE AGGREGATED DATA ####
colnames(df_stru)

df_stru_ag <- df_stru %>%
  group_by(year_quarter, soc2, soc, msa, state, region_name, teleworkable_bin) %>%
  summarise(n = n(),
            wfh_raw = sum(wfh_raw),
            wfh_ustring = sum(wfh_ustring),
            wfh_non_neg = sum(wfh_non_neg),
            wfh_raw_body = sum(wfh_raw_body),
            wfh_ustring_body = sum(wfh_ustring_body),
            wfh_non_neg_bod = sum(wfh_non_neg_body))

fwrite(df_stru_ag, "./int_data/us_wfh_ag_df.csv")

#### END ####






