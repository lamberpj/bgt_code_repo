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
quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/")

#### US ####
paths <- list.files("./bg-us/int_data/sentences/", pattern = "*.rds", full.names = T) %>% sort(decreasing = T)

#### /END ####
source("/mnt/disks/pdisk/code/safe_mclapply.R")
df <- safe_mclapply(1:length(paths), function(i) {
  df_ss_frag <- readRDS(paths[i]) %>%
    setDT(.)
  warning(paste0("\nSUCCESS: ",i,"\n"))
  #cat(paste0("\nDID: ",i," IN  ",difference," minutes\n"))
  return(df_ss_frag)
}, mc.cores = 8)

mean(df[str_sub(frag_id, -4, -1) != "0000"]$nfeat) # 50.68307
sd(df[str_sub(frag_id, -4, -1) != "0000"]$nfeat) # 14.75316

df_nfrag <- df %>%
  .[, .(n_frag = uniqueN(frag_id)-1), by = .(job_id)]

mean(df_nfrag$n_frag) # 8.904184
sd(df_nfrag$n_frag) # 6.580681

set.seed(123)
keep_job_ids <- data.table(job_id = sort(unique(df$job_id)))
keep_job_ids$rand <- runif(nrow(keep_job_ids))
keep_job_ids <- keep_job_ids[order(rand)][1:50]
nrow(keep_job_ids) 
df_ss <- df[job_id %in% keep_job_ids$job_id]
fwrite(df_ss, file = "example_frags_us_bgt.csv")


#### END ####

#### UK ####

paths <- list.files("./bg-uk/int_data/sentences/", pattern = "*.rds", full.names = T) %>% sort(decreasing = T)

#### /END ####
source("/mnt/disks/pdisk/code/safe_mclapply.R")
df <- safe_mclapply(1:length(paths), function(i) {
  df_ss_frag <- readRDS(paths[i]) %>%
    setDT(.)
  warning(paste0("\nSUCCESS: ",i,"\n"))
  #cat(paste0("\nDID: ",i," IN  ",difference," minutes\n"))
  return(df_ss_frag)
}, mc.cores = 8)

df <- df %>% rbindlist(.)

mean(df[str_sub(frag_id, -4, -1) != "0000"]$nfeat) # 49.52606
sd(df[str_sub(frag_id, -4, -1) != "0000"]$nfeat) # 14.27808

df_nfrag <- df %>%
  .[, .(n_frag = uniqueN(frag_id)-1), by = .(job_id)]

mean(df_nfrag$n_frag) # 7.266747
sd(df_nfrag$n_frag) # 4.94425

set.seed(123)
keep_job_ids <- data.table(job_id = sort(unique(df$job_id)))
keep_job_ids$rand <- runif(nrow(keep_job_ids))
keep_job_ids <- keep_job_ids[order(rand)][1:50]
nrow(keep_job_ids) 
df_ss <- df[job_id %in% keep_job_ids$job_id]
fwrite(df_ss, file = "example_frags_uk_bgt.csv")




