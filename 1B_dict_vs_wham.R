#### SETUP ####
remove(list = ls())
options(scipen=999)
library("devtools")
#install_github("trinker/textclean")
library("textclean")
library("data.table")
library("tidyverse")
library("janitor")
library("lubridate")
library("doParallel")
library("tmaptools")

#library("textclean")
#install.packages("qdapRegex")
#library("quanteda")
#library("tokenizers")
library("stringi")
library("DescTools")
#library("readtext")
#library("rvest")
#library("xml2")
#library("DescTools")
library("zoo")
library("stargazer")
library("readxl")
library("ggplot2")
library("scales")
library("ggpubr")
library("ggthemr")
library("ggpmisc")
library("xml2")
ggthemr('flat')
library(egg)
library(extrafont)
library(fixest)
# font_import()
#loadfonts(device="postscript")
#fonts()

##### GITHUB RUN ####
# cd /mnt/disks/pdisk/bgt_code_repo
# git init
# git add .
# git pull -m "check"
# git commit -m "autosave"
# git push origin master

setDTthreads(8)
getDTthreads()
#quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg_combined/")

#### LOAD "ALL" ####
# df_us_2019 <- fread("./int_data/df_us_2019_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight", "job_domain")) %>% mutate(country = "US") %>% setDT()
# df_us_2020 <- fread("./int_data/df_us_2020_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight", "job_domain")) %>% mutate(country = "US") %>% setDT()
# df_us_2021 <- fread("./int_data/df_us_2021_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight", "job_domain")) %>% mutate(country = "US") %>% setDT()
# df_us_2022 <- fread("./int_data/df_us_2022_standardised.csv", nThread = 16, integer64 = "numeric", select = c("job_id", "country", "state", "city", "wfh_wham", "job_date", "bgt_occ", "month", "employer", "job_id_weight", "job_domain")) %>% mutate(country = "US") %>% setDT()
# 
# df_us <- rbindlist(list(df_us_2019,df_us_2020,df_us_2021,df_us_2022))
# remove(list = c("df_us_2019","df_us_2020","df_us_2021","df_us_2022"))
# df_us$job_id <- as.numeric(df_us$job_id)
# 
# #### LOAD DICT ####
# paths <- list.files(path = "/mnt/disks/pdisk/bg-us/int_data/dict/", full.names = T)
# source("/mnt/disks/pdisk/bgt_code_repo/old/safe_mclapply.R")
# df_dict <- safe_mclapply(paths, function(x) {fread(x, integer64 = "numeric")}, mc.cores = 4) %>% rbindlist(., fill=TRUE)
# df_dict$job_id <- as.numeric(df_dict$job_id)
# df_dict <- df_dict %>% select(job_id, dictionary, neg_dictionary)
# rm("paths")
# class(df_us$job_id)
# class(df_dict$job_id)
# 
# head(df_us$job_id)
# head(df_dict$job_id)
# 
# nrow(df_us) # 206,639,824
# df_us <- df_us %>%
#   merge(x = ., y = df_dict, by = "job_id", all.x = TRUE, all.y = FALSE)
# nrow(df_us) #
# 
# summary(df_us$dictionary)
# 
# df_us <- df_us %>% .[, city_state := paste0(city,"_",state)]
# df_us <- df_us[!is.na(job_domain) & job_domain != ""]
# df_us <- df_us %>% .[!grepl("careerbuilder", job_domain)]
# remove(list = setdiff(ls(), "df_us"))
# save(df_us, file = "./aux_data/df_us_with_dict.RData")
#### END ####

#### PREPARE DATA ####
load(file = "./aux_data/df_us_with_dict.RData")
remove(list = setdiff(ls(), "df_us"))


# Filter Unweighted daily shares
wfh_daily_share <- readRDS(file = "./int_data/country_daily_wfh.rds")
table(wfh_daily_share$country)
ts_for_plot <- wfh_daily_share %>%
  .[, job_date := ymd(job_date)] %>%
  .[as.yearmon(year_month) <= as.yearmon(ymd("20221101"))] %>%
  .[, l1o_monthly_mean := (sum(daily_share*N)-daily_share*N)/(sum(N) - N), by = .(country, year_month)] %>%
  .[, monthly_mean := sum(daily_share*N)/(sum(N)), by = .(country, year_month)] %>%
  .[, l1o_keep := ifelse(abs(monthly_mean - l1o_monthly_mean) > 0.02 | abs(log(monthly_mean/l1o_monthly_mean)) > 0.10, 0, 1)] %>%
  .[, l1o_keep := ifelse(as.yearmon(year_month) %in% as.yearmon(ymd(c("20200301", "20200401", "20200501", "20200601"))), 1, l1o_keep)] %>%
  .[, l1o_daily_with_nas := ifelse(l1o_keep == 1, daily_share, NA)]

table(ts_for_plot$country)

dropped_days <- ts_for_plot %>%
  .[l1o_keep == 0] %>%
  select(country, job_date) %>%
  .[, drop := 1]

# Make Vacancy Weighted Monthly Shares (Using Filters from above)
df_us_vac_weighted_monthly <- df_us
df_us_vac_weighted_monthly <- df_us_vac_weighted_monthly %>%
  setDT(.) %>%
  select(country, wfh_wham, job_date, bgt_occ, month, job_id_weight, dictionary) %>%
  .[!is.na(wfh_wham) & !is.na(dictionary)] %>%
  .[!is.na(bgt_occ)] %>%
  .[, bgt_occ5 := str_sub(bgt_occ, 1, 6)] %>%
  .[, month := factor(month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))] %>%
  .[, job_ymd := ymd(job_date)] %>%
  .[, year_month := as.yearmon(job_ymd)] %>%
  .[, year := year(job_ymd)] %>%
  setDT(.)

df_us_vac_weighted_monthly <- df_us_vac_weighted_monthly %>%
  merge(. = x, y = dropped_days, all.x = TRUE, all.y = FALSE, by = c("country", "job_date")) %>%
  .[is.na(drop)] %>%
    select(country, bgt_occ5, year_month, year, wfh_wham, job_id_weight, dictionary) %>%
    setDT(.) %>%
    .[, .(wfh_sum = sum(job_id_weight*wfh_wham, na.rm = T),
          dict_sum = sum(job_id_weight*as.numeric(dictionary), na.rm = T),
          job_ads_sum = sum(job_id_weight, na.rm = T)),
      by = .(country, bgt_occ5, year_month, year)] %>%
    .[, wfh_share := wfh_sum / job_ads_sum] %>%
    .[, dict_share := dict_sum / job_ads_sum] %>%
    setDT(.)

df_us_vac_weighted_monthly <- df_us_vac_weighted_monthly %>%
  rename(monthly_share_wfh = wfh_share,
         monthly_share_dict = dict_share,
         N = job_ads_sum)

# Make US 2019 vacancy weights
head(df_us_vac_weighted_monthly)
shares_df <- df_us_vac_weighted_monthly %>%
  .[bgt_occ5 != "" & !is.na(bgt_occ5)] %>%
  .[year == 2019 & country == "US"] %>%
  .[, .(N = sum(N, na.rm = T)), by = .(bgt_occ5)] %>%
  .[, share := N/sum(N, na.rm = T)] %>%
  select(bgt_occ5, share)

df_us_vac_weighted_monthly <- df_us_vac_weighted_monthly %>%
  .[bgt_occ5 != "" & !is.na(bgt_occ5)] %>%
  merge(x = ., y = shares_df, by = "bgt_occ5", all.x = TRUE, all.y = FALSE) %>%
  setDT(.)

df_us_vac_weighted_monthly <- df_us_vac_weighted_monthly %>%
  .[, .(monthly_mean_wfh = sum(monthly_share_wfh*(share/sum(share, na.rm = T)), na.rm = T),
        monthly_mean_dict = sum(monthly_share_dict*(share/sum(share, na.rm = T)), na.rm = T)),
    by = .(country, year_month)]

df_us_vac_weighted_monthly <- df_us_vac_weighted_monthly %>%
  group_by(country, year_month) %>%
  pivot_longer(monthly_mean_wfh:monthly_mean_dict)

df_us_vac_weighted_monthly <- df_us_vac_weighted_monthly %>%
  setDT(.) %>%
  .[, name := ifelse(name == "monthly_mean_wfh", "WHAM", "Dictionary")]

df_us_vac_weighted_monthly
# Occ Weighted to US
cbbPalette <- c("black", "darkblue")
p = df_us_vac_weighted_monthly %>%
  ggplot(., aes(x = as.Date(as.yearmon(year_month)), y = 100*value, col = name)) +
  #stat_smooth (geom="line", alpha=0.8, size=2, span=0.2) +
  #stat_smooth(span = 0.1, alpha=0.5, se=FALSE, size = 2) +
  geom_point(size = 2.25) +
  geom_line(size = 1.25) +
  ylab("Percent") +
  xlab("Date") +
  scale_x_date(breaks = as.Date(c("2019-01-01", "2020-01-01", "2021-01-01", "2022-01-01","2023-01-01")),
               date_labels = '%Y') +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,5)) +
  coord_cartesian(ylim = c(0, 29)) +
  scale_colour_manual(values=cbbPalette) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(nrow = 1, reverse = T)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=15, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=15, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white")) +
  theme(aspect.ratio=3/5)
p
save(p, file = "./ppt/ggplots/dict_vs_wham_ts_2019_vac_weights.RData")
remove(p)

#### END ####

#### CHECK DICT DROP ####
df_us <-  df_us%>% setDT(.)

df_us <-  df_us %>% .[, year_month := as.yearmon(job_date)]

tail(df_us)

check_url <- df_us %>%
  .[, .(.N), by = job_domain]

summary(check_url$N)

check_url <- check_url %>% .[N > 10000]

c("www.indeed.com","www.simplyhired.com","dejobs.org","www.recruiternetworks.com","www.glassdoor.com")

head(df_us)

monthly_wfh <- df_us %>%
  .[job_domain %in% c("www.indeed.com","www.simplyhired.com","dejobs.org","www.recruiternetworks.com","www.glassdoor.com")] %>%
  #.[job_domain %in% c("www.indeed.com")] %>%
  .[, .(wfh_sum = sum(job_id_weight*wfh_wham, na.rm = T),
         dict_sum = sum(job_id_weight*as.numeric(dictionary), na.rm = T),
         dict_neg_sum = sum(job_id_weight*as.numeric(neg_dictionary), na.rm = T),
         job_ads_sum = sum(job_id_weight, na.rm = T)),
     by = .(year_month, job_domain)] %>%
  setDT(.) %>%
  .[, wfh_share := 100*round(wfh_sum / job_ads_sum, 3)] %>%
  .[, dict_share := 100*round(dict_sum / job_ads_sum, 3)] %>%
  .[, dict_neg_share := 100*round(dict_neg_sum / job_ads_sum, 3)] %>%
  select(year_month, job_domain, wfh_share, dict_share, dict_neg_share)

monthly_wfh <- monthly_wfh %>%
  setDT(.) %>%
  group_by(year_month, job_domain) %>%
    pivot_longer(cols = c(wfh_share, dict_share, dict_neg_share))

monthly_wfh <- monthly_wfh %>% setDT(.)

View(monthly_wfh[name == "dict_share" & job_domain == "www.glassdoor.com"])
  
p = monthly_wfh %>%
  filter(name == "dict_share") %>%
  ggplot(., aes(x = as.Date(as.yearmon(year_month)), y = value, col = job_domain)) +
  geom_point(size = 2.25) + geom_line(size = 1.25)

check <- df_us[job_domain == "www.glassdoor.com" & as.character(year_month) %in% c("Sep 2021", "Nov 2021")]

check2 <- check %>%
  filter(dictionary == TRUE & wfh_wham == 0) %>%
  group_by(year_month, neg_dictionary) %>%
  sample_n(., size = 500)

check2$job_id <- as.numeric(check2$job_id)

saveRDS(check2, file = "./aux_data/check_glassdoor_ads.rds")

rm(df_us)



#### /END ####

#### READ XML AND MAKE SEQUENCES ####

# Check raw text
remove(list = setdiff(ls(), "check2"))
setwd("/mnt/disks/pdisk/bg-us/")
paths <- list.files("./raw_data/text/", pattern = "*.zip", full.names = T)
paths <- paths[grepl("202109|202111", paths)]
paths

paths
source("/mnt/disks/pdisk/bgt_code_repo/old/safe_mclapply.R")

job_ads <- safe_mclapply(1:length(paths), function(i) {
  
  name <- str_sub(paths[i], -21, -5)
  name
  warning(paste0("\nBEGIN: ",i,"  '",name,"'"))
  cat(paste0("\nBEGIN: ",i,"  '",name,"'"))
  system(paste0("unzip -n ",paths[i]," -d ./raw_data/text/"))
  xml_path = gsub(".zip", ".xml", paths[i])
  xml_path
  #try( {
  df_xml <- read_xml(xml_path) %>%
    xml_find_all(., ".//Job")
  
  df_job_id <- xml_find_all(df_xml, ".//JobID") %>% xml_text
  df_job_title <- xml_find_all(df_xml, ".//CleanJobTitle") %>% xml_text
  df_job_text <- xml_find_all(df_xml, ".//JobText") %>% xml_text
  
  df_ads <- data.table(job_id = as.numeric(df_job_id), job_title = df_job_title, job_text = df_job_text)
  
  df_ads <- df_ads %>% setDT(.) %>% .[job_id %in% check2$job_id]

  
  warning(paste0("SUCCESS: ",i))
  cat(paste0("\nSUCCESS: ",i,"\n"))
  return(df_ads)
}, mc.cores = 4)

job_ads <- rbindlist(job_ads)

colnames(check2)

export <- check2 %>%
  distinct(job_id, .keep_all = T) %>%
  select(job_id, year_month, dictionary, neg_dictionary) %>%
  left_join(job_ads)

fwrite(export, file = "./aux_data/examples_glassdoor.csv")

#### END ####

#### READ XML AND MAKE SEQUENCES ####

seq_df <- readRDS("./int_data/sequences/sequences_20221201_20221207.rds")
df_wham <- fread("./int_data/wham_pred/sequences_20221201_20221207.txt")

seq_df$seq_id <- as.character(seq_df$seq_id)
df_wham$seq_id <- as.character(df_wham$seq_id)
head(df_wham)
df <- df_wham %>%
  filter(wfh_prob>0.5) %>%
  left_join(seq_df, by = "seq_id") %>%
  distinct(seq_id, .keep_all = T)

set.seed(12345)

df <- df %>%
  sample_n(size = 100)

fwrite(df, "./aux_data/check_hybrid_vs_fully_remote.csv")

res <- data.table(type = c("fully remote","fully remote","hybrid","fully remote","unclear","unclear","unclear","unclear","hybrid","hybrid","hybrid","unclear","fully remote","unclear","unclear","unclear","fully remote","unclear","unclear","unclear","unclear","unclear","na","fully remote","fully remote","hybrid","unclear","unclear","hybrid","unclear","hybrid","fully remote","unclear","unclear","unclear","fully remote","unclear","unclear","hybrid","hybrid","fully remote","unclear","unclear","hybrid","na","hybrid","fully remote","unclear","unclear","unclear","unclear","hybrid","unclear","unclear","hybrid","hybrid","hybrid","hybrid","hybrid","hybrid","fully remote","unclear","hybrid","fully remote","unclear","unclear","unclear","unclear","na","unclear","unclear","unclear","unclear","unclear","unclear","unclear","unclear","hybrid","unclear","fully remote","unclear","unclear","unclear","unclear","fully remote","na","unclear","hybrid","unclear","unclear","unclear","hybrid","fully remote","unclear","hybrid","unclear","unclear","hybrid","unclear","hybrid"))


table(res$type)

#### END ####

