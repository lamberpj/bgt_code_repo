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
quanteda_options(threads = 8)
setwd("/mnt/disks/pdisk/bg-eu")
setDTthreads(8)
#### TRANSFER FILES ####

#system("gsutil -m cp -r gs://for_transfer  /mnt/disks/pdisk/bg-eu/bg-eu-bucket/alternative/postings")

#### / END ####

#### GET PATH NAMES ####
paths <- list.files("./bg-eu-bucket/alternative/text", pattern = "*.zip", full.names = T)
paths

paths_done <- list.files("./int_data/wfh_alt/", pattern = "wfh_dfm_", full.names = F) %>%
  gsub("wfh_dfm_|wfh_raw_text_", "", .) %>% gsub(".rds", "", .) %>% unique %>%
  gsub("_", " ", .) %>% gsub("raw ", "", .)
paths_done
paths
paths <- paths[!(grepl(paste(paths_done, collapse = "|"), paths))]
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
safe_mclapply(1:length(paths), function(i) {
  
  warning(paste0("BEGIN: ",i))
  name <- str_sub(paths[i], -13, -5)
  name
  paths[i]
  
  system(paste0("unzip '", paths[i],"' -d ./bg-eu-bucket/alternative/text"))
  
  csv_path <- list.files("./bg-eu-bucket/alternative/text", pattern = ".csv", full.names = T)[grepl(name, list.files("./bg-eu-bucket/alternative/text", pattern = ".csv"))]
  csv_path
  file.rename(csv_path,gsub(" ", "_", csv_path))
  name <- gsub(" ", "_", name)
  csv_path <- list.files("./bg-eu-bucket/alternative/text", pattern = ".csv", full.names = T)[grepl(name, list.files("./bg-eu-bucket/alternative/text", pattern = ".csv"))]
  csv_path
  name
  df <- fread(csv_path, stringsAsFactors = F)
  df <- data.frame("job_id" = df$general_id, "job_text" = paste(df$title, df$description))
  
  #(num_groups = ceiling(nrow(df)/1000))
  #
  #df_list <- df %>%
  #  group_by((row_number()-1) %/% (n()/num_groups)) %>%
  #  group_split(.keep = F)
  #
  #df <- df_list[[1]]
  
  df_corpus <- corpus(df, text_field = "job_text", docid_field = "job_id")
  
  df_tokens <- tokens(df_corpus)
  remove("df_corpus")
  df_dfm <- dfm(tokens_lookup(df_tokens, dict, valuetype = "glob", case_insensitive = TRUE, verbose = TRUE))
  remove("df_tokens")
  
  df_tagged <- convert(df_dfm, to = "data.frame")
  df_tagged <- df_tagged[rowSums(df_tagged[,-1]) > 0,]
  
  df <- df %>% filter(job_id %in% df_tagged$doc_id)
  
  saveRDS(df_tagged, file = paste0("./int_data/wfh_alt/wfh_dfmdf_",name,".rds"))
  saveRDS(df, file = paste0("./int_data/wfh_alt/wfh_raw_text_",name,".rds"))
  saveRDS(df_dfm, file = paste0("./int_data/wfh_alt/wfh_dfm_",name,".rds"))
  
  unlink(csv_path)
  warning(paste0("DONE: ",i))
  return("")
}, mc.cores = 3)

head(df)

system("echo sci2007! | sudo -S shutdown -h now")

#### /END ####

#### COMBINE ####
remove(list = ls())

paths <- list.files("./bg-eu-bucket/alternative/postings", pattern = ".csv", full.names = T)

df_stru <- safe_mclapply(1:length(paths), function(i) {
  warning(paste0("\nSTART: ",i))
  df <- fread(paths[i], data.table = F, nThread = 1, colClasses = "character", stringsAsFactors = FALSE,
              select = c("general_id", "year_grab_date", "month_grab_date", "day_grab_date", "idesco_level_4", "companyname", "idprovince", "idregion", "idcountry")) %>%
    clean_names
  warning(paste0("\nDONE: ",i))
  warning(paste0(nrow(df)))
  return(df)
}, mc.cores = 24) %>% bind_rows

df_stru <- df_stru %>%
  distinct(general_id, .keep_all = T)

n_distinct(df_stru$general_id) # 115,572,364

paths <- list.files("./bg-eu-bucket/alternative/skills", pattern = ".csv", full.names = T)
remove(df)

df_skills <- safe_mclapply(1:length(paths), function(i) {

  warning(paste0("START: ",i))
  df <- fread(paths[i], data.table = F, nThread = 1, colClasses = "character", stringsAsFactors = FALSE) %>%
    clean_names %>%
    filter(!is.na(escoskill_level_3)) %>%
    mutate(escoskill_level_3 = tolower(escoskill_level_3)) %>%
    filter(escoskill_level_3 %in% c("machine learning","computer vision", "opencv", "neural network", "deep learning", "artificial intelligence"))
  warning(paste0("DONE: ",i))
  warning(paste0(nrow(df)))
  return(df)
}, mc.cores = 24)

df_skills <- df_skills %>% bind_rows

nrow(df_skills) # 126,247

df_skills <- df_skills %>%
  mutate(ml = ifelse(escoskill_level_3 %in% c("machine learning","computer vision", "opencv", "neural network", "deep learning"), 1, 0)) %>%
  mutate(ai = ifelse(escoskill_level_3 %in% c("artificial intelligence"), 1, 0)) %>%
  select(-escoskill_level_3) %>%
  group_by(general_id) %>%
  summarise(ml = max(ml, na.rm = T),
            ai = max(ai, na.rm = T)) %>%
  ungroup()

nrow(df_skills) # 105,159

df_skills <- df_skills %>% distinct(general_id, .keep_all = T)
nrow(df_skills) # 105,159

paths <- list.files("./int_data/wfh", pattern = "*dfm*", full.names = T)

df_tagged <- safe_mclapply(1:length(paths), function(i) {
  warning(paste0("\nSTART: ",i))
  df <- readRDS(paths[[i]]) %>%
    convert(., to = "data.frame") %>%
    clean_names
  warning(paste0("\nEND: ",i))
  return(df)
}, mc.cores = 32) %>%
  bind_rows(.)

df_tagged <- df_tagged %>%
  distinct(doc_id, .keep_all = T)

nrow(df_tagged) # 115,559,366
n_distinct(df_tagged$doc_id) # 115,559,366

nrow(df_stru) # 115,572,364
n_distinct(df_stru$general_id) # 115,572,364

print("test")


head(df_stru)
head(df_tagged)
head(df_skills)

class(df_stru$general_id)
class(df_tagged$doc_id)
class(df_skills$general_id)

colnames(df_tagged)[2:54] <- paste0(colnames(df_tagged)[2:54],"_text")
colnames(df_skills)[2:3] <- paste0(colnames(df_skills)[2:3],"_skill")

head(df_tagged)
head(df_skills)

df_stru <- df_stru %>%
  filter(general_id %in% df_tagged$doc_id)

nrow(df_stru) # 109,003,712

df_stru <- df_stru %>%
  left_join(df_tagged, by = c("general_id" = "doc_id")) %>%
  left_join(df_skills, by = c("general_id" = "general_id"))

nrow(df_stru) # 109,003,712

remove(df_tagged)
remove(df_skills)

df_stru <- df_stru %>% as_tibble

head(df_stru)

df_stru <- df_stru  %>%
  mutate(across(where(is.numeric), ~if_else(is.na(.x), 0, .x)))

df_stru <- df_stru  %>%
  mutate(across(where(is.numeric), ~if_else(.x>1, 1, .x)))

# save(df_stru, file = "./int_data/df_stru_eu_alt.RData")

#### END ####

#### PLOTS ####
remove(list = ls())
load(file = "./int_data/df_stru_eu_alt.RData")
obs <- nrow(df_stru)

df_stru <- df_stru %>%
  mutate(ai_cluster_text = ifelse(ai_chat_bot_text + ai_chatbot_text + ai_kibit_text + artificial_intelligence_text + chat_bot_ai_text + chat_bot_text + chatbot_ai_text + chatbot_text + ibm_watson_text + ithink_text + kibit_ai_text + kibit_text + virtual_agent_text > 0, 1, 0),
         ml_cluster_text = ifelse(amelia_text+cloud_machine_learning_engine_text+cloud_machine_learning_platform_text+cloud_ml_engine_text+cloud_ml_platform_text+computer_vision_text+decision_tree_text+deep_learning_text+deeplearning4j_text+gradient_boosting_text+h2o_ai_text+h2o_text+keras_text+libsvm_text+machine_learning_text+madlib_text+mahout_text+microsoft_cognitive_toolkit_text+mlpack_text+mlpy_text+mxnet_text+nd4j_text+neural_net_text+neural_nets_text+neural_network_text+object_tracking_text+opencv_text+pybrain_text+random_forest_text+random_forests_text+recommendation_system_text+recommender_system_text+sdscm_text+semantic_driven_subtractive_clustering_method_text+support_vector_machine_text+svm_text+vowpal_text+wabbit_text+xgboost_text>0, 1, 0))

df_stru <- df_stru %>%
  select(general_id, year_grab_date, month_grab_date, day_grab_date, idesco_level_4, companyname, idprovince, idregion, idcountry,
         ai_cluster_text, ml_cluster_text, ai_skill, ml_skill)

head(df_stru)

# Make Quarter
df_stru <- df_stru %>%
  mutate(year_quarter = as.yearqtr(ymd(paste0(year_grab_date,"-",month_grab_date,"-",day_grab_date)), with_year = T))

# Make Hit/Miss
df_hm <- df_stru %>%
  mutate(ai_s0_t0 = ifelse(ai_skill == 0 & ai_cluster_text == 0, 1, 0),
         ai_s1_t0 = ifelse(ai_skill == 1 & ai_cluster_text == 0, 1, 0),
         ai_s0_t1 = ifelse(ai_skill == 0 & ai_cluster_text == 1, 1, 0),
         ai_s1_t1 = ifelse(ai_skill == 1 & ai_cluster_text == 1, 1, 0),
         ml_s0_t0 = ifelse(ml_skill == 0 & ml_cluster_text == 0, 1, 0),
         ml_s1_t0 = ifelse(ml_skill == 1 & ml_cluster_text == 0, 1, 0),
         ml_s0_t1 = ifelse(ml_skill == 0 & ml_cluster_text == 1, 1, 0),
         ml_s1_t1 = ifelse(ml_skill == 1 & ml_cluster_text == 1, 1, 0)) %>%
  summarise("AI s=1 t = 0" = sum(ai_s1_t0),
            "AI s=0 t = 1" = sum(ai_s0_t1),
            "AI s=1 t = 1" = sum(ai_s1_t1),
            "ML s=1 t = 0" = sum(ml_s1_t0),
            "ML s=0 t = 1" = sum(ml_s0_t1),
            "ML s=1 t = 1" = sum(ml_s1_t1))

df_hm_sum <- data.frame("names" = rownames(t(df_hm)), "count" = t(df_hm)[,1]) %>%
  mutate(cluster = str_sub(names, 1, 2)) %>%
  group_by(cluster) %>%
  mutate(prop = round(count/sum(count), 3)) %>%
  ungroup() %>%
  mutate(count = format(count, big.mark=",")) %>%
  select(names, count, prop)

stargazer(df_hm_sum, summary=FALSE, rownames=FALSE, title="Compare Text to Skills: US Data Hit/Miss", align=TRUE)

# Levels and Changes
df_levels <- df_stru %>%
  group_by(year_quarter) %>%
  summarise(n_posts = n(),
            ai_cluster_text_posts = sum(ai_cluster_text),
            ml_cluster_text_posts = sum(ml_cluster_text),
            ai_skill_posts = sum(ai_skill),
            ml_skill_posts = sum(ml_skill),
            ai_cluster_text_prop = mean(ai_cluster_text, na.rm = T),
            ml_cluster_text_prop = mean(ml_cluster_text, na.rm = T),
            ai_skill_prop = mean(ai_skill, na.rm = T),
            ml_skill_prop = mean(ml_skill, na.rm = T)
  )

# Plot Posts
df_ts_posts <- df_levels %>%
  select(year_quarter, ai_skill_posts, ml_skill_posts, ai_cluster_text_posts, ml_cluster_text_posts) %>%
  group_by(year_quarter) %>%
  pivot_longer(., cols = c(ai_skill_posts, ml_skill_posts, ai_cluster_text_posts, ml_cluster_text_posts)) %>%
  ungroup() %>%
  mutate(name = case_when(
    name == "ai_skill_posts" ~ "AI (skills)",
    name == "ml_skill_posts" ~ "ML (skills)",
    name == "ai_cluster_text_posts" ~ "AI (text)",
    name == "ml_cluster_text_posts" ~ "ML (text)")) %>%
  mutate(Cluster = str_sub(name, 1, 2),
         Method = str_sub(gsub("[\\(\\)]", "", name), 4, -1))

head(df_ts_posts)

p1 = df_ts_posts %>%
  ggplot(., aes(x = year_quarter, y = value, colour = Cluster, linetype = Method)) +
  geom_line(size = 2) +
  geom_point(size = 3) +
  ylab("Count of Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q',n = 28) +
  scale_y_continuous(label=comma) +
  scale_colour_ggthemr_d() +
  #ggtitle("Count of US Job Ads requiring AI and ML Skills") +
  #labs(subtitle = paste0("n = ",format(obs, big.mark=","),". Comparing Raw Text to BGT Skill Cluster results"),
  #     caption = "Note: Using BGT US Job Ads, Q2/Q4 only.") +
  theme(
    legend.position="bottom",
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm"))

#ggexport(p1, filename = "./plots/text_based/us_level_ai_ml_skills_vs_text.pdf")

# Proportions
df_ts_prop <- df_levels %>%
  select(year_quarter, ai_skill_prop, ml_skill_prop, ai_cluster_text_prop, ml_cluster_text_prop) %>%
  group_by(year_quarter) %>%
  pivot_longer(., cols = c(ai_skill_prop, ml_skill_prop, ai_cluster_text_prop, ml_cluster_text_prop)) %>%
  ungroup() %>%
  mutate(name = case_when(
    name == "ai_skill_prop" ~ "AI (skills)",
    name == "ml_skill_prop" ~ "ML (skills)",
    name == "ai_cluster_text_prop" ~ "AI (text)",
    name == "ml_cluster_text_prop" ~ "ML (text)")) %>%
  mutate(Cluster = str_sub(name, 1, 2),
         Method = str_sub(gsub("[\\(\\)]", "", name), 4, -1))

p2 = df_ts_prop %>%
  ggplot(., aes(x = year_quarter, y = value, colour = Cluster, linetype = Method)) +
  geom_line(size = 2) +
  geom_point(size = 3) +
  ylab("Proportion of Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q',n = 28) +
  scale_y_continuous(label=comma, limits = c(0,0.01)) +
  scale_colour_ggthemr_d() +
  #ggtitle("Proportion of US Job Ads requiring AI and ML Skills") +
  #labs(subtitle = paste0("n = ",format(obs, big.mark=","),". Comparing Raw Text to BGT Skill Cluster results"),
  #     caption = "Note: Using BGT US Job Ads, Q2/Q4 only.") +
  theme(
    legend.position="bottom",
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm"))

a1 = ggarrange(p1, p2, ncol = 1, nrow = 2, legend = "bottom", common.legend = T, align = "v") %>%
  annotate_figure(.,
                  top = text_grob("Figure X: ML/AI in EU Job Ads"))
a1
ggexport(a1, filename = "./plots/text_based/eu_level_prop_ai_ml.pdf")
remove(list = c("p1","p2","a1"))

#### COEF PLOTS ####
df_stru <- df_stru %>%
  mutate(year_month = paste0(year_grab_date,"_",month_grab_date)) %>%
  mutate(year_quarter_text = paste0(year_grab_date, ".", quarter(ymd(paste0(year_grab_date,"-",month_grab_date,"-",day_grab_date))))) %>%
  mutate(month = month_grab_date)

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
colnames(df_stru)

head(df_stru)

regs_ml <- safe_mclapply(1:2, function(i) {
  warning("\n\n############# ",i," #############\n\n")
  #if(i == 1) {lm <- feols(ml_skill ~ later_quarters | month, df_stru, cluster = ~ year_month, lean = T)}
  if(i == 1) {lm <- feols(ml_cluster_text ~ later_quarters | month, df_stru, cluster = ~ year_month, lean = T)}
  #if(i == 3) {lm <- feols(ml_skill ~ later_quarters | month + idregion + idesco_level_4, df_stru, cluster = ~ year_month, lean = T)}
  if(i == 2) {lm <- feols(ml_cluster_text ~ later_quarters | month + idregion + idesco_level_4, df_stru, cluster = ~ year_month, lean = T)}
  warning("\n\n############# ",i," #############\n\n")
  return(lm)
}, mc.cores = 2)

summary(regs_ml[[2]])

regs_ai <- safe_mclapply(1:2, function(i) {
  #if(i == 1) {lm <- feols(ai_skill ~ later_quarters | month, df_stru, cluster = ~ year_month, lean = T)}
  if(i == 1) {lm <- feols(ai_cluster_text ~ later_quarters | month, df_stru, cluster = ~ year_month, lean = T)}
  #if(i == 3) {lm <- feols(ai_skill ~ later_quarters | month + idregion + idesco_level_4, df_stru, cluster = ~ year_month, lean = T)}
  if(i == 2) {lm <- feols(ai_cluster_text ~ later_quarters | month + idregion + idesco_level_4, df_stru, cluster = ~ year_month, lean = T)}
  warning("\n\n############# ",i," #############\n\n")
  return(lm)
}, mc.cores = 2)

for_plot_ml <- lapply(1:2, function(i) {
  regs_ml[[i]]$coeftable %>%
    as_tibble %>%
    mutate(estimate = Estimate, lb = (Estimate - 2*`Std. Error`), ub = (Estimate + 2*`Std. Error`)) %>%
    select(estimate, lb, ub) %>%
    as_tibble %>%
    mutate(quarter = as.yearqtr(c("2018 Q1",
                                  "2018 Q2",
                                  "2018 Q3",
                                  "2018 Q4",
                                  "2019 Q1",
                                  "2019 Q2",
                                  "2019 Q3",
                                  "2019 Q4",
                                  "2020 Q1",
                                  "2020 Q2",
                                  "2020 Q3",
                                  "2020 Q4")))
})

for_plot_ai <- lapply(1:2, function(i) {
  regs_ai[[i]]$coeftable %>%
    as_tibble %>%
    mutate(estimate = Estimate, lb = (Estimate - 2*`Std. Error`), ub = (Estimate + 2*`Std. Error`)) %>%
    select(estimate, lb, ub) %>%
    as_tibble %>%
    mutate(quarter = as.yearqtr(c("2018 Q1",
                                  "2018 Q2",
                                  "2018 Q3",
                                  "2018 Q4",
                                  "2019 Q1",
                                  "2019 Q2",
                                  "2019 Q3",
                                  "2019 Q4",
                                  "2020 Q1",
                                  "2020 Q2",
                                  "2020 Q3",
                                  "2020 Q4")))
})


for_plot_long_ml <- bind_rows(#for_plot_ml[[1]] %>% mutate(group = "1", Method = "Skill", FEs = "Month"),
                              for_plot_ml[[1]] %>% mutate(group = "2", Method = "Text", FEs = "Month"),
                              #for_plot_ml[[3]] %>% mutate(group = "3", Method = "Skill", FEs = "Month x Occ x Geog"),
                              for_plot_ml[[2]] %>% mutate(group = "4", Method = "Text", FEs = "Month x Occ x Geog"))

for_plot_long_ai <- bind_rows(#for_plot_ai[[1]] %>% mutate(group = "1", Method = "Skill", FEs = "Month"),
                              for_plot_ai[[1]] %>% mutate(group = "2", Method = "Text", FEs = "Month"),
                              #for_plot_ai[[3]] %>% mutate(group = "3", Method = "Skill", FEs = "Month x Occ x Geog"),
                              for_plot_ai[[2]] %>% mutate(group = "4", Method = "Text", FEs = "Month x Occ x Geog"))

p1 = for_plot_long_ml %>%
  filter(Method == "Text") %>%
  ggplot(., aes(x = quarter, y = estimate, linetype = FEs, color = "#00d166")) +
  geom_line(size = 2) +
  geom_point(size = 3) +
  geom_ribbon(aes(ymin = lb, ymax = ub), alpha = 0.10, fill = "Black", size = 0) +
  ylab("Quarter Dummy Coefficient") +
  xlab("Year") +
  scale_x_yearqtr(format = '%YQ%q',n = 28) +
  scale_y_continuous(label=comma, limits = c(0, 0.01)) +
  scale_colour_manual(values="#00d166") +
  #ggtitle("Coeff Plot ML Skills") +
  #labs(subtitle = paste0("n = ",obs,". Fixed Effects: Month, Occupation, Region"),
  #     caption = "Note: Using BGT US Job Ads.\n+/- 2 SD in dark grey.  +/- 2 SD in light grey.") +
  theme(legend.position="bottom",
        axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
        legend.key.width=unit(1.5,"cm"))

p2 = for_plot_long_ai %>%
  filter(Method == "Text") %>%
  filter() %>%
  ggplot(., aes(x = quarter, y = estimate, linetype = FEs)) +
  geom_line(size = 2) +
  geom_point(size = 3) +
  geom_ribbon(aes(ymin = lb, ymax = ub), alpha = 0.10, fill = "Black", size = 0) +
  ylab("Quarter Dummy Coefficient") +
  xlab("Year") +
  scale_x_yearqtr(format = '%YQ%q',n = 28) +
  scale_y_continuous(label=comma, limits = c(0, 0.01)) +
  scale_colour_ggthemr_d() +
  #ggtitle("Coeff Plot ML Skills") +
  #labs(subtitle = paste0("n = ",obs,". Fixed Effects: Month, Occupation, Region"),
  #     caption = "Note: Using BGT US Job Ads.\n+/- 2 SD in dark grey.  +/- 2 SD in light grey.") +
  theme(legend.position="bottom",
        axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
        legend.key.width=unit(1.5,"cm"))

p_leg = bind_rows(for_plot_long_ml %>% mutate(Cluster = "ML"), for_plot_long_ai %>% mutate(Cluster = "AI")) %>%
  filter() %>%
  ggplot(., aes(x = quarter, y = estimate, color = Cluster, linetype = FEs)) +
  scale_colour_ggthemr_d() +
  geom_line(size = 2) +
  theme(legend.position="bottom",
        axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
        legend.key.width=unit(1.5,"cm"))

a1 = ggarrange(p1, p2, ncol = 1, nrow = 2, legend = "bottom", legend.grob = get_legend(p_leg), align = "v") %>%
  annotate_figure(.,
                  top = text_grob("Figure X: Coefficient Plots of ML (top) and AI (bottom) in EU"),
  )
a1
ggexport(a1, filename = "./plots/text_based/eu_cp_ai_ml.pdf")
#remove(list = c("p1","p2","a1"))

colnames(df_stru)

unique(df_stru$idcountry)

# Levels and Changes
df_levels_country <- df_stru %>%
  filter(year_grab_date %in% c(2014, 2020)) %>%
  group_by(year_grab_date, idcountry) %>%
  summarise(n_posts = n(),
            ai_cluster_text_posts = sum(ai_cluster_text),
            ml_cluster_text_posts = sum(ml_cluster_text),
            ai_skill_posts = sum(ai_skill),
            ml_skill_posts = sum(ml_skill),
            ai_cluster_text_prop = mean(ai_cluster_text, na.rm = T),
            ml_cluster_text_prop = mean(ml_cluster_text, na.rm = T),
            ai_skill_prop = mean(ai_skill, na.rm = T),
            ml_skill_prop = mean(ml_skill, na.rm = T)
  ) %>%
  ungroup() %>%
  select(idcountry, year_grab_date, ai_cluster_text_posts, ml_cluster_text_posts) %>%
  arrange(idcountry, year_grab_date) %>%
  group_by(idcountry) %>%
  mutate(ai_cluster_text_pc_growth = round(100*(ai_cluster_text_posts - lag(ai_cluster_text_posts))/lag(ai_cluster_text_posts), 0),
         ml_cluster_text_pc_growth = round(100*(ml_cluster_text_posts - lag(ml_cluster_text_posts))/lag(ml_cluster_text_posts), 0)
         ) %>%
  select(idcountry, ai_cluster_text_pc_growth, ml_cluster_text_pc_growth) %>%
  filter(!is.na(ai_cluster_text_pc_growth))

stargazer(df_levels_country, summary=FALSE, rownames=FALSE, title="Percentage Change in Postings (2014-2020) by EU Country", align=TRUE)

df_prop_country <- df_stru %>%
  filter(year_grab_date %in% c(2014, 2020)) %>%
  group_by(year_grab_date, idcountry) %>%
  summarise(n_posts = n(),
            ai_cluster_text_posts = sum(ai_cluster_text),
            ml_cluster_text_posts = sum(ml_cluster_text),
            ai_skill_posts = sum(ai_skill),
            ml_skill_posts = sum(ml_skill),
            ai_cluster_text_prop = mean(ai_cluster_text, na.rm = T),
            ml_cluster_text_prop = mean(ml_cluster_text, na.rm = T),
            ai_skill_prop = mean(ai_skill, na.rm = T),
            ml_skill_prop = mean(ml_skill, na.rm = T)
  ) %>%
  ungroup() %>%
  select(idcountry, year_grab_date, ai_cluster_text_prop, ml_cluster_text_prop) %>%
  arrange(idcountry, year_grab_date) %>%
  group_by(idcountry) %>%
  mutate(ai_cluster_text_pc_growth = round(100*(ai_cluster_text_prop - lag(ai_cluster_text_prop))/lag(ai_cluster_text_prop), 0),
         ml_cluster_text_pc_growth = round(100*(ml_cluster_text_prop - lag(ml_cluster_text_prop))/lag(ml_cluster_text_prop), 0)
  ) %>%
  select(idcountry, ai_cluster_text_pc_growth, ml_cluster_text_pc_growth) %>%
  filter(!is.na(ai_cluster_text_pc_growth))

stargazer(df_prop_country, summary=FALSE, rownames=FALSE, title="Percentage Change in Proportions (2014-2020) by EU Country", align=TRUE)
  
  
