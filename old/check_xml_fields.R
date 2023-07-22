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
library("doParallel")

setDTthreads(8)
getDTthreads()
quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg-us/")
#setDTthreads(1)

remove(list = ls())
#### end ####

#system("gsutil -m cp -r gs://for_transfer/bgt_us/main /mnt/disks/pdisk/bg-us/raw_data/main/")
#system("gsutil -m cp -r gs://for_transfer/bgt_us/raw_text /mnt/disks/pdisk/bg-us/raw_data/text/")
#
#library(filesstrings)
#paths <- list.files("/mnt/disks/pdisk/bg-us/raw_data/main/main", full.names = T, pattern = ".zip", recursive = T)
#paths
#lapply(paths, function(x) {
#  file.move(x, "/mnt/disks/pdisk/bg-anz/raw_data/text")
#})

#lapply(1:length(paths), function(i) {
#  system(paste0("unzip -n ",paths[i]," -d ./raw_data/main"))
#})

#lapply(1:length(paths), function(i) {
#  unlink(paths[i])
#})

#### GET PATH NAMES TO MAKE SEQUENCES ####
paths <- list.files("./raw_data/text/", pattern = "*.zip", full.names = T)
#### /END ####

#### READ XML NAD MAKE SEQUENCES ####
remove(list = setdiff(ls(),c("df_xml","df")))
i = 412
name <- str_sub(paths[i], -21, -5)
name
warning(paste0("\nBEGIN: ",i,"  '",name,"'"))
cat(paste0("\nBEGIN: ",i,"  '",name,"'"))
system(paste0("unzip -n ",paths[i]," -d ./raw_data/text/"))
xml_path = gsub(".zip", ".xml", paths[i])
xml_path
  
df_xml <- read_xml(xml_path) %>%
  xml_find_all(., ".//Job")

df_job_id <- xml_find_all(df_xml, ".//JobID") %>% xml_text
head(df_job_id)
head(df$BGTJobId)
df_job_id_keep <- (df_job_id %in% as.numeric(df$BGTJobId))



df_job_title <- xml_find_all(df_xml, ".//CleanJobTitle") %>% xml_text
df_job_constitle <- xml_find_all(df_xml, ".//ConsolidatedTitle") %>% xml_text
df_job_url <- xml_find_all(df_xml, ".//JobURL") %>% xml_text
#df_job_text <- xml_find_all(df_xml, ".//JobText") %>% xml_text

remote_url <- which(grepl("remote", x = df_job_url, ignore.case = T))
remote_constitle <- which(grepl("remote", x = df_job_constitle, ignore.case = T))
remote_title <- which(grepl("remote", x = df_job_title, ignore.case = T))

length(remote_constitle)

mean(remote_title %in% remote_constitle) # 6%
mean(remote_constitle %in% remote_title) # 100%
mean(remote_url %in% remote_title) # 83%

df_char_list <- mclapply(df_xml, as.character, mc.cores = 8)
df_char_list_meta <- df_char_list[df_job_id_keep]

df_char_list_no_jt_cjt <- mclapply(df_char_list, function(x) {
  y <- gsub("<JobText>(.|\n)*</JobText>", "", x)
  y <- gsub("<CleanJobTitle>(.|\n)*</CleanJobTitle>", "", y)
  y <- gsub("<ConsolidatedTitle>(.|\n)*</ConsolidatedTitle>", "", y)
  y <- gsub("<JobURL>(.|\n)*</JobURL>", "", y)
  y <- gsub("<CanonSkills>(.|\n)*</CanonSkills>", "", y)
  return(y)
}, mc.cores = 8)

df_char_list_no_jt_cjt_with_remote <- df_char_list_no_jt_cjt[grepl("remote", df_char_list_no_jt_cjt, ignore.case = TRUE)]

length(df_char_list_no_jt_cjt_with_remote)

single_string <- paste(df_char_list_no_jt_cjt_with_remote, collapse = "######## END ########\n\n######### BEGIN #######")

write.table(single_string, file = "./check_struct_fields.txt")

##### END ####

##### CHECK META ####

df <- fread(file = "./raw_data/main/Main_2022-04.txt")

df <- df[df$Employer == "Meta",]


paths <- list.files("./raw_data/text/", pattern = "*.zip", full.names = T)
#### /END ####

#### READ XML NAD MAKE SEQUENCES ####
remove(list = setdiff(ls(),c("df_xml","df")))
i = 412
name <- str_sub(paths[i], -21, -5)
name
system(paste0("unzip -n ",paths[i]," -d ./raw_data/text/"))
xml_path = gsub(".zip", ".xml", paths[i])
xml_path
df_xml <- read_xml(xml_path) %>%
  xml_find_all(., ".//Job")
df_job_id <- xml_find_all(df_xml, ".//JobID") %>% xml_text
head(df_job_id)
head(df$BGTJobId)
df_job_id_keep <- (df_job_id %in% as.numeric(df$BGTJobId))

df_char_list <- mclapply(df_xml, as.character, mc.cores = 8)
df_char_list_meta <- df_char_list[df_job_id_keep]

single_string <- paste(df_char_list_meta, collapse = "######## END ########\n\n\n######### BEGIN #######")

write.table(single_string, file = "./check_meta.txt")

##### END ####

##### META IN DF STRUE ####

remove(list = ls())

df <- fread(file = "../bg_combined/int_data/df_all_standardised.csv")

df_meta <- df[df$employer == "Facebook" & year >= 2021 & country == "US",]

mean(df_meta$wfh_prob>0.5)

##### END ####




