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
devtools::install_github('Mikata-Project/ggthemr')
library("ggthemr")
ggthemr('flat')

quanteda_options(threads = 8)
setwd("/mnt/disks/pdisk/bg-us/")
setDTthreads(8)

#### COPY FILES ####
#system("gsutil -m cp -r gs://for_transfer/  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20140507_20140513.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20140514_20140520.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20140521_20140527.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20140528_20140603.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20150430_20150506.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20150507_20150513.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20150514_20150520.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20150521_20150527.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20150528_20150603.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20160429_20160505.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20160506_20160512.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20160513_20160519.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20160520_20160526.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20160527_20160602.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20170430_20170506.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20170507_20170513.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20170514_20170520.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20170521_20170527.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20170528_20170603.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20180430_20180506.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20180507_20180513.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20180514_20180520.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20180521_20180527.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20180528_20180603.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20190430_20190506.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20190507_20190513.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20190514_20190520.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20190521_20190527.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20190528_20190603.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20200430_20200506.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20200507_20200513.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20200514_20200520.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20200521_20200527.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_AddFeed_20200528_20200603.zip  /mnt/disks/pdisk/bg-us/raw_data/text")

#system("gsutil cp gs://for_transfer/US_XML_Postings_AddFeed_20210429_20210505.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_Postings_AddFeed_20210506_20210512.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_Postings_AddFeed_20210513_20210519.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_Postings_AddFeed_20210520_20210526.zip  /mnt/disks/pdisk/bg-us/raw_data/text")
#system("gsutil cp gs://for_transfer/US_XML_Postings_AddFeed_20210527_20210602.zip  /mnt/disks/pdisk/bg-us/raw_data/text")

#### /END ####



#### LOAD DICTIONARY ####

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

# Get
paste0(sort(df_dict$key[df_dict$prim_or_mod != "mod"]), collapse = " + ")
#### /end ####

#### READ XML ####
safe_mclapply(1:length(paths), function(i) {
  warning(paste0("BEGIN: ",i))
  name <- str_sub(paths[i], -21, -5)
  system(paste0("unzip ",paths[i]," -d ./raw_data/text/"))
  xml_path = gsub(".zip", ".xml", paths[i])
  xml_path
  
  df_xml <- read_xml(xml_path) %>%
    xml2::xml_find_all(., ".//Job")
  
  df_job_id <- xml_find_all(df_xml, ".//JobID") %>% xml_text
  df_job_text <- xml_find_all(df_xml, ".//JobText") %>% xml_text
  remove("df_xml")
  df <- data.frame("job_id" = df_job_id, "job_text" = df_job_text)
  
  remove("df_job_id")
  remove("df_job_text")
  
  df_corpus <- corpus(df, text_field = "job_text", docid_field = "job_id")
  
  df_tokens <- tokens(df_corpus)
  remove("df_corpus")
  df_dfm <- dfm(tokens_lookup(df_tokens, dict, valuetype = "glob", case_insensitive = TRUE, verbose = TRUE))
  remove("df_tokens")
  
  df_tagged <- convert(df_dfm, to = "data.frame") %>%
    mutate(keep = ai + ai_chat_bot_ast + ai_chat_bot_ast_2 + ai_chatbot_ast + ai_chatbot_ast_2 + ai_kibit + ai_machine + 
             ai_technolog_ast + amelia + artificial_intelligence + based_at_home + based_from_home + chat_bot_ast_ai + 
             chatbot_ast_ai + cloud_application_ast + cloud_architect_ast + cloud_architect_ast_2 + cloud_based + 
             cloud_based_2 + cloud_builder + cloud_computering_architect_ast + cloud_computing + cloud_deployment + 
             cloud_environment_ast + cloud_hosting + cloud_infrastructure + cloud_machine_learning_engine + 
             cloud_machine_learning_platform + cloud_management + cloud_ml_engine + cloud_ml_platform + cloud_offering_ast + 
             cloud_platform_ast + cloud_provider_ast + cloud_security + cloud_service_ast + cloud_solution_ast + cloud_stack + 
             cloud_storage + cloud_to_cloud + cloud_to_cloud_2 + cloudera + cloudstack + community_cloud + compute_engine + 
             computer_vision + deep_learn_ast + deeplearning4j + deployment_models + direct_connect + directconnect + 
             distributed_cloud + edge_computing + enterprise_application_ast + enterprise_class + enterprise_cloud + 
             enterprise_network + fog_computing + fog_networking + fogging + gradient_boosting + h2o + h2o_ai + home_based + 
             home_based_2 + home_working + hybrid_cloud + iaas + ibm_ast_watson + ithink + keras + kibit_ai + language_processing + 
             learning_algorithm_ast + libsvm + machine_intelligence + madlib + mahout + microsoft_cognitive_toolkit + mlpack + 
             mlpy + mxnet + natural_language + nd4j + neural_net + neural_nets + neural_network + neural_network_ast + 
             object_tracking + opencv_ast + paas + personal_cloud + private_cloud + public_cloud + pybrain + rackspace + 
             random_forest + random_forests + recommendation_system + recommendation_system_ast + recommender_system_ast + 
             reinforcement_learning + remote_work + saas + sdscm + semantic_driven_subtractive_clustering_method + 
             supervised_learning + support_vector_machine_ast + svm + telecommute + telecommute_c_yes + telecommute_yes + 
             telecommuting + tosca + unsupervised_learning + virtual_agent_ast + virtual_private_cloud + vowpal + vpc + 
             wabbit + work_at_home + work_at_home_c_yes + work_at_home_yes + work_from_home + work_from_home_2 + 
             work_from_home_c_yes + work_from_home_yes + working_at_home + working_from_home + xgboost > 0)
  
  df <- df %>% filter(job_id %in% df_tagged$doc_id)
  
  saveRDS(df_tagged %>% select(-keep), file = paste0("./int_data/tech/tech_dfmdf_",name,".rds"))
  saveRDS(df, file = paste0("./int_data/tech/tech_raw_text_",name,".rds"))
  saveRDS(df_dfm, file = paste0("./int_data/tech/tech_dfm_",name,".rds"))
  
  unlink(xml_path)
  warning(paste0("DONE: ",i))
  return("")
}, mc.cores = 3)

system("echo sci2007! | sudo -S shutdown -h now")

#### /END ####

#### COMBINE ####
remove(list = ls())

#### Structured ####
paths <- list.files("./raw_data/main", pattern = "04.txt|05.txt|06.txt|10.txt|11.txt|12.txt", full.names = T)
df_stru <- safe_mclapply(1:length(paths), function(i) {
  warning(paste0("\nSTART: ",i))
  df <- fread(paths[i], data.table = F, nThread = 1, colClasses = "character", stringsAsFactors = FALSE,
              select = c("BGTJobId", "JobDate", "SOC", "MSA", "State")) %>%
    clean_names
  warning(paste0("\nDONE: ",i))
  return(df)
}, mc.cores = 12) %>% bind_rows
nrow(df_stru) # 115,945,337
n_distinct(df_stru$bgt_job_id) # 115,945,337
remove(paths)
#### /end ####

#### Read Text ####
paths <- list.files("./int_data/tech", pattern = "*dfm*", full.names = T)
df_tagged <- safe_mclapply(1:length(paths), function(i) {
  warning(paste0("\nSTART: ",i))
  df <- readRDS(paths[[i]]) %>%
    convert(., to = "data.frame") %>%
    clean_names
  
  colnames(df)[2:54] <- paste0(colnames(df)[2:54],"_text")
  
  df <- df %>%
    mutate(ai_cluster_text = ifelse(ai_chat_bota_text+ai_chatbota_text+ai_kibit_text+aia_chat_bota_text+aia_chatbota_text+artificial_intelligence_text+chat_bota_ai_text+chatbota_ai_text+amelia_text+kibit_ai_text+kibit_text+ibma_watson_text+ithink_text > 0, 1, 0),
           ml_cluster_text = ifelse(cloud_ml_engine_text+cloud_ml_platform_text+computer_vision_text+decision_treea_text+deep_learning_text+deeplearning4j_text+gradient_boosting_text+h2o_ai_text+h2o_text+virtual_agenta_text+keras_text+libsvm_text+m_l_text+machine_learna_text+madlib_text+mahout_text+microsoft_cognitive_toolkit_text+ml_text+mlpack_text+mlpy_text+mxnet_text+nd4j_text+neural_neta_text+neural_networka_text+object_tracking_text+opencva_text+pybrain_text+random_foresta_text+recommendation_systema_text+recommender_systema_text+s_v_m_text+sdscm_text+semantic_driven_subtractive_clustering_method_text+support_vector_machinea_text+svm_text+vowpal_text+wabbit_text+xgboost_text>0, 1, 0)) %>%
    select(doc_id, ai_cluster_text, ml_cluster_text) %>%
    group_by(doc_id) %>%
    summarise(ai_cluster_text = max(ai_cluster_text),
              ml_cluster_text = max(ml_cluster_text)) %>%
    ungroup
  
  warning(paste0("\nEND: ",i))
  return(df)
}, mc.cores = 32) %>%
  bind_rows(.)

nrow(df_tagged) # 143,972,584
n_distinct(df_tagged$doc_id) # 143,822,844

df_tagged <- df_tagged %>%
  distinct(doc_id, .keep_all = T)

nrow(df_tagged) # 143,822,844
#### /end ####

#### merge ####
df_stru <- df_stru %>%
  filter(bgt_job_id %in% df_tagged$doc_id)

df_stru <- df_stru %>%
  filter(as.numeric(quarter(ymd(df_stru$job_date))) %in% c(2,4))

nrow(df_stru) # 114,523,300
df_stru <- df_stru %>%
  left_join(df_tagged, by = c("bgt_job_id" = "doc_id")) %>%
  left_join(df_skill, by = c("bgt_job_id" = "bgt_job_id"))
nrow(df_stru) # 114,523,300
n_distinct(df_stru$bgt_job_id) # 114,523,300

df_stru <- df_stru %>% as_tibble

df_stru <- df_stru  %>%
  mutate(across(where(is.numeric), ~if_else(is.na(.x), 0, .x)))

df_stru <- df_stru  %>%
  mutate(across(where(is.numeric), ~if_else(.x>1, 1, .x)))

save(df_stru, file = "./int_data/df_stru_us.RData")
remove(df_tagged)
remove(df_skill)
#### /end ####
