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
library("devtools")
#install_github("markwestcott34/stargazer-booktabs")
library("zoo")
library("stargazer")
library("tsibble")
library("readxl")
library("ggplot2")
library("scales")
library("ggpubr")
library("ggthemr")
library("ggpmisc")
library("RColorBrewer")

ggthemr('flat')
library(egg)
library(extrafont)
library(fixest)
# font_import()
#loadfonts(device="postscript")
#fonts()

setDTthreads(2)
getDTthreads()
#quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg_combined/")

#########################################
############## CHECK USA ################
#########################################
# Make daily and weekly plot of WHAM na's using full data
setwd("/mnt/disks/pdisk/bg-us/")
df_us_2014 <- fread(file = "./int_data/us_stru_2014_wfh.csv",
                    select = c("job_id","job_date","wfh_wham","job_domain","job_url"),
                    nThread = 8)

df_us_2015 <- fread(file = "./int_data/us_stru_2015_wfh.csv",
                    select = c("job_id","job_date","wfh_wham","job_domain","job_url"),
                    nThread = 8)

df_us_2016 <- fread(file = "./int_data/us_stru_2016_wfh.csv",
                    select = c("job_id","job_date","wfh_wham","job_domain","job_url"),
                    nThread = 8)

df_us_2017 <- fread(file = "./int_data/us_stru_2017_wfh.csv",
                    select = c("job_id","job_date","wfh_wham","job_domain","job_url"),
                    nThread = 8)

df_us_2018 <- fread(file = "./int_data/us_stru_2018_wfh.csv",
                    select = c("job_id","job_date","wfh_wham","job_domain","job_url"),
                    nThread = 8)

df_us_2019 <- fread(file = "./int_data/us_stru_2019_wfh.csv",
                    select = c("job_id","job_date","wfh_wham","job_domain","job_url"),
                    nThread = 8)

df_us_2020 <- fread(file = "./int_data/us_stru_2020_wfh.csv",
                    select = c("job_id","job_date","wfh_wham","job_domain","job_url"),
                    nThread = 8)

df_us_2021 <- fread(file = "./int_data/us_stru_2021_wfh.csv",
                    select = c("job_id","job_date","wfh_wham","job_domain","job_url"),
                    nThread = 8)

df_us_2022 <- fread(file = "./int_data/us_stru_2022_wfh.csv",
                    select = c("job_id","job_date","wfh_wham","job_domain","job_url"),
                    nThread = 8)

df_us <- rbindlist(list(df_us_2014, df_us_2015, df_us_2016, df_us_2017, df_us_2018, df_us_2019, df_us_2020, df_us_2021, df_us_2022))
remove(list = setdiff(ls(), "df_us"))
df_us[df_us == ""] <- NA
class(df_us$job_date)

df_check_daily <- df_us %>% .[, .(.N,
                                  na_share_wham = mean(is.na(wfh_wham)),
                                  na_share_domain = mean(is.na(job_domain)),
                                  na_share_url = mean(is.na(job_url))),
                              by = job_date]

df_check_weekly <- df_us %>% .[, .(.N,
                                   na_share_wham = round(mean(is.na(wfh_wham)), 2),
                                  n_distinct_domains = uniqueN(job_domain),
                                  na_share_domain = round(mean(is.na(job_domain)), 2),
                                  na_share_url = round(mean(is.na(job_url)), 2),
                                  date = min(job_date)),
                              by = yearweek(job_date)]
# WHAM results for 2022 W23 (2022-06-06), 2022 W24, 2022 W25 (2022-06-28) show lots of missing!

#### CHECK US SOURCES ####
sources_for_checking <- df_all_us %>%
  .[year %in% c(2019,2020,2021,2022)] %>%
  .[, .(.N), by = .(year_month, job_domain)] %>%
  .[, posting_share := 100*(N/sum(N)), by = year_month] %>%
  .[posting_share > 5] %>%
  select(job_domain) %>%
  distinct(job_domain)

table(df_all_us$wfh_wham, df_all_us$neg_narrow_result)
sum(df_all_us$wfh_wham == 1)
sum(df_all_us$neg_narrow_result == 1, na.rm = T)

df_all_us$top_job_domain <- ifelse(df_all_us$job_domain %in% sources_for_checking$job_domain, df_all_us$job_domain, "other")
df_all_us$year_month_cat <- as.character(as.yearmon(df_all_us$year_month))

df_all_us$top_job_domain[df_all_us$top_job_domain == ""] <- "other"

top_job_domain_df <- as.data.table(table(df_all_us$top_job_domain)) %>% mutate(other = as.numeric(V1 == "other")) %>%
  arrange(other, desc(N)) %>% select(-other) %>% mutate(prop = 100*N/sum(N))
top_job_domain_df <- top_job_domain_df %>%
  rename(Source = V1, Share = prop) %>%
  mutate(Share = round(Share, 1))
stargazer(top_job_domain_df, title = "Top Job Boards in USA (2020 onwards)", rownames = F, summary = F, style = "aer", digits = 1)

colnames(df_all_us)
#mod1 <- feols(data = df_all_us %>% filter(year %in% c(2020, 2021, 2022)), fml = wfh_wham ~ i(top_job_domain, ref = "other") | year_month_cat^bgt_occ^state)
#etable(mod1, tex = F, title = "Remote Work by Job Board US")
#### END ####

#### LEAVE ONE OUT STRATEGY - US ####
wfh_ag <- lapply(unique(df_all_us$top_job_domain), function(x) {
  df_all_us %>%
    setDT(.) %>%
    .[top_job_domain != x] %>%
    .[, .(share_bert_unweighted = sum(job_id_weight*wfh_wham)/sum(job_id_weight)),
      by = .(year_month, year)] %>%
    .[, sample := paste0("w/o ",gsub("www.","",x))]
}) %>%
  rbindlist

big_pal <- brewer.pal(n = 9, name = "Paired")

big_pal <- c(big_pal, big_pal)

p = wfh_ag %>%
  filter(year %in% c(2020, 2021, 2022)) %>%
  ggplot(., aes(x = as.Date(as.yearmon(year_month)), y = 100*share_bert_unweighted, colour = sample)) +
  geom_line(size = 1) +
  geom_point(size = 2) +
  #geom_area(alpha=1 , size=.5, colour="white") +
  ylab("Share (%)") +
  xlab("Date") +
  labs(title = "Share of Remote Work Vacancy Postings (%)", subtitle = "Unweighted") +
  #scale_x_date(breaks = as.Date(c("2014-01-01", "2015-01-01", "2016-01-01",
  #                                "2017-01-01", "2018-01-01", "2019-01-01",
  #                                "2020-01-01", "2021-01-01", "2022-01-01")),
  #             date_labels = '%Y') +
  #scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,2)) +
  #coord_cartesian(ylim = c(0, 20)) +
  scale_colour_manual(values=big_pal) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(nrow = 5, reverse = T)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"))
p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/us_unweighted_by_job_src_l1o.pdf", width = 7, height = 5)
ggsave(p_egg, filename = "./plots/us_unweighted_by_job_src_l1o.png", width = 7, height = 5)

#### END ####









# UK
remove(list = ls())

# Make daily and weekly plot of WHAM na's using full data
df_uk_2014 <- fread(file = "./int_data/uk_stru_2014_wfh.csv",
                    select = c("job_id","job_date","bgt_occ","employer","sector_name","canon_city","nation",
                               "wfh_wham_prob","wfh_wham","narrow_result","neg_narrow_result",
                               "job_domain","job_url"),
                    nThread = 8)


df_uk_2015 <- fread(file = "./int_data/uk_stru_2015_wfh.csv",
                    select = c("job_id","job_date","bgt_occ","employer","sector_name","canon_city","nation",
                               "wfh_wham_prob","wfh_wham","narrow_result","neg_narrow_result",
                               "job_domain","job_url"),
                    nThread = 8)

df_uk_2016 <- fread(file = "./int_data/uk_stru_2016_wfh.csv",
                    select = c("job_id","job_date","bgt_occ","employer","sector_name","canon_city","nation",
                               "wfh_wham_prob","wfh_wham","narrow_result","neg_narrow_result",
                               "job_domain","job_url"),
                    nThread = 8)


df_uk_2017 <- fread(file = "./int_data/uk_stru_2017_wfh.csv",
                    select = c("job_id","job_date","bgt_occ","employer","sector_name","canon_city","nation",
                               "wfh_wham_prob","wfh_wham","narrow_result","neg_narrow_result",
                               "job_domain","job_url"),
                    nThread = 8)

df_uk_2018 <- fread(file = "./int_data/uk_stru_2018_wfh.csv",
                    select = c("job_id","job_date","bgt_occ","employer","sector_name","canon_city","nation",
                               "wfh_wham_prob","wfh_wham","narrow_result","neg_narrow_result",
                               "job_domain","job_url"),
                    nThread = 8)


df_uk_2019 <- fread(file = "./int_data/uk_stru_2019_wfh.csv",
                    select = c("job_id","job_date","bgt_occ","employer","sector_name","canon_city","nation",
                               "wfh_wham_prob","wfh_wham","narrow_result","neg_narrow_result",
                               "job_domain","job_url"),
                    nThread = 8)


df_uk_2020 <- fread(file = "./int_data/uk_stru_2020_wfh.csv",
                    select = c("job_id","job_date","bgt_occ","employer","sector_name","canon_city","nation",
                               "wfh_wham_prob","wfh_wham","narrow_result","neg_narrow_result",
                               "job_domain","job_url"),
                    nThread = 8)

df_uk_2021 <- fread(file = "./int_data/uk_stru_2021_wfh.csv",
                    select = c("job_id","job_date","bgt_occ","employer","sector_name","canon_city","nation",
                               "wfh_wham_prob","wfh_wham","narrow_result","neg_narrow_result",
                               "job_domain","job_url"),
                    nThread = 8)

df_uk_2022 <- fread(file = "./int_data/uk_stru_2022_wfh.csv",
                    select = c("job_id","job_date","bgt_occ","employer","sector_name","canon_city","nation",
                               "wfh_wham_prob","wfh_wham","narrow_result","neg_narrow_result",
                               "job_domain","job_url"),
                    nThread = 8)

df_uk <- rbindlist(list(df_uk_2014, df_uk_2015, df_uk_2016, df_uk_2017, df_uk_2018, df_uk_2019, df_uk_2020, df_uk_2021, df_uk_2022))
remove(list = setdiff(ls(), "df_uk"))
df_uk[df_uk == ""] <- NA
class(df_uk$job_date)

df_check_daily <- df_uk %>% .[, .(na_share_wham = mean(is.na(wfh_wham)),
                            na_share_narrow = mean(is.na(narrow_result)),
                            na_share_neg_narrow = mean(is.na(neg_narrow_result)),
                            na_share_domain = mean(is.na(job_domain)),
                            na_share_url = mean(is.na(job_url))),
                        by = job_date]

df_check_weekly <- df_uk %>% .[, .(na_share_wham = round(mean(is.na(wfh_wham)), 2),
                                  na_share_narrow = round(mean(is.na(narrow_result)), 2),
                                  na_share_neg_narrow = round(mean(is.na(neg_narrow_result)), 2),
                                  na_share_domain = round(mean(is.na(job_domain)), 2),
                                  na_share_url = round(mean(is.na(job_url)), 2),
                                  date = min(job_date)),
                              by = yearweek(job_date)]

summary(df_uk)


# Canada
remove(list = ls())

# Make daily and weekly plot of WHAM na's using full data
df_can_2014 <- fread(file = "./int_data/can_stru_2014_wfh.csv",
                    select = c("job_id","job_date","bgt_occ","sector_name","canon_city","canon_state",
                               "wfh_wham_prob","wfh_wham","narrow_result","neg_narrow_result",
                               "job_domain","job_url"),
                    nThread = 8)


df_can_2015 <- fread(file = "./int_data/can_stru_2015_wfh.csv",
                     select = c("job_id","job_date","bgt_occ","sector_name","canon_city","canon_state",
                                "wfh_wham_prob","wfh_wham","narrow_result","neg_narrow_result",
                                "job_domain","job_url"),
                    nThread = 8)

df_can_2016 <- fread(file = "./int_data/can_stru_2016_wfh.csv",
                     select = c("job_id","job_date","bgt_occ","sector_name","canon_city","canon_state",
                                "wfh_wham_prob","wfh_wham","narrow_result","neg_narrow_result",
                                "job_domain","job_url"),
                    nThread = 8)


df_can_2017 <- fread(file = "./int_data/can_stru_2017_wfh.csv",
                     select = c("job_id","job_date","bgt_occ","sector_name","canon_city","canon_state",
                                "wfh_wham_prob","wfh_wham","narrow_result","neg_narrow_result",
                                "job_domain","job_url"),
                    nThread = 8)

df_can_2018 <- fread(file = "./int_data/can_stru_2018_wfh.csv",
                     select = c("job_id","job_date","bgt_occ","sector_name","canon_city","canon_state",
                                "wfh_wham_prob","wfh_wham","narrow_result","neg_narrow_result",
                                "job_domain","job_url"),
                    nThread = 8)


df_can_2019 <- fread(file = "./int_data/can_stru_2019_wfh.csv",
                     select = c("job_id","job_date","bgt_occ","sector_name","canon_city","canon_state",
                                "wfh_wham_prob","wfh_wham","narrow_result","neg_narrow_result",
                                "job_domain","job_url"),
                    nThread = 8)


df_can_2020 <- fread(file = "./int_data/can_stru_2020_wfh.csv",
                     select = c("job_id","job_date","bgt_occ","sector_name","canon_city","canon_state",
                                "wfh_wham_prob","wfh_wham","narrow_result","neg_narrow_result",
                                "job_domain","job_url"),
                    nThread = 8)

df_can_2021 <- fread(file = "./int_data/can_stru_2021_wfh.csv",
                     select = c("job_id","job_date","bgt_occ","sector_name","canon_city","canon_state",
                                "wfh_wham_prob","wfh_wham","narrow_result","neg_narrow_result",
                                "job_domain","job_url"),
                    nThread = 8)

df_can_2022 <- fread(file = "./int_data/can_stru_2022_wfh.csv",
                     select = c("job_id","job_date","bgt_occ","sector_name","canon_city","canon_state",
                                "wfh_wham_prob","wfh_wham","narrow_result","neg_narrow_result",
                                "job_domain","job_url"),
                    nThread = 8)

df_can <- rbindlist(list(df_can_2014, df_can_2015, df_can_2016, df_can_2017, df_can_2018, df_can_2019, df_can_2020, df_can_2021, df_can_2022))
remove(list = setdiff(ls(), "df_can"))
df_can[df_can == ""] <- NA
class(df_can$job_date)

df_check_daily <- df_can %>% .[, .(na_share_wham = mean(is.na(wfh_wham)),
                                  na_share_narrow = mean(is.na(narrow_result)),
                                  na_share_neg_narrow = mean(is.na(neg_narrow_result)),
                                  na_share_domain = mean(is.na(job_domain)),
                                  na_share_url = mean(is.na(job_url))),
                              by = job_date]

df_check_weekly <- df_can %>% .[, .(na_share_wham = round(mean(is.na(wfh_wham)), 2),
                                   na_share_narrow = round(mean(is.na(narrow_result)), 2),
                                   na_share_neg_narrow = round(mean(is.na(neg_narrow_result)), 2),
                                   na_share_domain = round(mean(is.na(job_domain)), 2),
                                   na_share_url = round(mean(is.na(job_url)), 2),
                                   date = min(job_date)),
                               by = yearweek(job_date)]

summary(df_can)

# ANZ
remove(list = ls())

colnames(fread(file = "./int_data/anz_stru_2014_wfh.csv", nrow = 100))

# Make daily and weekly plot of WHAM na's using full data
df_anz_2014 <- fread(file = "./int_data/anz_stru_2014_wfh.csv",
                    select = c("job_id","job_date","bgt_occ","sector_name","canon_city","canon_state",
                               "wfh_wham_prob","wfh_wham","narrow_result","neg_narrow_result",
                               "job_domain","job_url"),
                    nThread = 8)


df_anz_2015 <- fread(file = "./int_data/anz_stru_2015_wfh.csv",
                     select = c("job_id","job_date","bgt_occ","sector_name","canon_city","canon_state",
                                "wfh_wham_prob","wfh_wham","narrow_result","neg_narrow_result",
                                "job_domain","job_url"),
                    nThread = 8)

df_anz_2016 <- fread(file = "./int_data/anz_stru_2016_wfh.csv",
                     select = c("job_id","job_date","bgt_occ","sector_name","canon_city","canon_state",
                                "wfh_wham_prob","wfh_wham","narrow_result","neg_narrow_result",
                                "job_domain","job_url"),
                    nThread = 8)


df_anz_2017 <- fread(file = "./int_data/anz_stru_2017_wfh.csv",
                     select = c("job_id","job_date","bgt_occ","sector_name","canon_city","canon_state",
                                "wfh_wham_prob","wfh_wham","narrow_result","neg_narrow_result",
                                "job_domain","job_url"),
                    nThread = 8)

df_anz_2018 <- fread(file = "./int_data/anz_stru_2018_wfh.csv",
                     select = c("job_id","job_date","bgt_occ","sector_name","canon_city","canon_state",
                                "wfh_wham_prob","wfh_wham","narrow_result","neg_narrow_result",
                                "job_domain","job_url"),
                    nThread = 8)


df_anz_2019 <- fread(file = "./int_data/anz_stru_2019_wfh.csv",
                     select = c("job_id","job_date","bgt_occ","sector_name","canon_city","canon_state",
                                "wfh_wham_prob","wfh_wham","narrow_result","neg_narrow_result",
                                "job_domain","job_url"),
                    nThread = 8)


df_anz_2020 <- fread(file = "./int_data/anz_stru_2020_wfh.csv",
                     select = c("job_id","job_date","bgt_occ","sector_name","canon_city","canon_state",
                                "wfh_wham_prob","wfh_wham","narrow_result","neg_narrow_result",
                                "job_domain","job_url"),
                    nThread = 8)

df_anz_2021 <- fread(file = "./int_data/anz_stru_2021_wfh.csv",
                     select = c("job_id","job_date","bgt_occ","sector_name","canon_city","canon_state",
                                "wfh_wham_prob","wfh_wham","narrow_result","neg_narrow_result",
                                "job_domain","job_url"),
                    nThread = 8)

df_anz_2022 <- fread(file = "./int_data/anz_stru_2022_wfh.csv",
                     select = c("job_id","job_date","bgt_occ","sector_name","canon_city","canon_state",
                                "wfh_wham_prob","wfh_wham","narrow_result","neg_narrow_result",
                                "job_domain","job_url"),
                    nThread = 8)

df_anz <- rbindlist(list(df_anz_2014, df_anz_2015, df_anz_2016, df_anz_2017, df_anz_2018, df_anz_2019, df_anz_2020, df_anz_2021, df_anz_2022))
remove(list = setdiff(ls(), "df_anz"))
df_anz[df_anz == ""] <- NA
class(df_anz$job_date)

df_check_daily <- df_anz %>% .[, .(na_share_wham = mean(is.na(wfh_wham)),
                                  na_share_narrow = mean(is.na(narrow_result)),
                                  na_share_neg_narrow = mean(is.na(neg_narrow_result)),
                                  na_share_domain = mean(is.na(job_domain)),
                                  na_share_url = mean(is.na(job_url))),
                              by = job_date]

df_check_weekly <- df_anz %>% .[, .(na_share_wham = round(mean(is.na(wfh_wham)), 2),
                                   na_share_narrow = round(mean(is.na(narrow_result)), 2),
                                   na_share_neg_narrow = round(mean(is.na(neg_narrow_result)), 2),
                                   na_share_domain = round(mean(is.na(job_domain)), 2),
                                   na_share_url = round(mean(is.na(job_url)), 2),
                                   date = min(job_date)),
                               by = yearweek(job_date)]

sum(!is.na(df_anz$wfh_wham))


#### END ####


#### CHECK CAN SOURCES ####
remove(list = ls())
df_all_can <- fread(file = "./int_data/df_can_standardised.csv")
df_all_can <- df_all_can %>% .[!grepl("workopolis", df_all_can$job_url)]

colnames(df_all_can)
summary(df_all_can$wfh_wham)
sources_for_checking <- df_all_can %>%
  #.[year %in% c(2020,2021,2022)] %>%
  .[, .(.N), by = .(year_month, job_domain)] %>%
  .[, posting_share := 100*(N/sum(N)), by = year_month] %>%
  .[posting_share > 5] %>%
  select(job_domain) %>%
  distinct(job_domain)

df_all_can$top_job_domain <- ifelse(df_all_can$job_domain %in% sources_for_checking$job_domain, df_all_can$job_domain, "other")
df_all_can$year_month_cat <- as.character(as.yearmon(df_all_can$year_month))

top_job_domain_df <- as.data.table(table(df_all_can$top_job_domain)) %>% mutate(other = as.numeric(V1 == "other")) %>%
  arrange(other, desc(N)) %>% select(-other) %>% mutate(prop = 100*N/sum(N))
top_job_domain_df
stargazer(top_job_domain_df, title = "Top Job Boards in Canada (Jan 2020 to Jun 2022)", rownames = F, summary = F)
#### END ####

#### LEAVE ONE OUT STRATEGY - CAN ####
wfh_ag <- lapply(unique(df_all_can$top_job_domain)[25:28], function(x) {
  df_all_can %>%
    setDT(.) %>%
    .[top_job_domain != x] %>%
    .[, .(share_bert_unweighted = sum(job_id_weight*wfh_wham)/sum(job_id_weight)),
      by = .(year_month, year)] %>%
    .[, sample := paste0("w/o ",gsub("www.","",x))]
}) %>%
  rbindlist

big_pal <- brewer.pal(n = 12, name = "Set3")
big_pal <- c(big_pal)

p = wfh_ag %>%
  filter(year %in% c(2019, 2020, 2021, 2022)) %>%
  ggplot(., aes(x = as.Date(as.yearmon(year_month)), y = 100*share_bert_unweighted, colour = sample)) +
  geom_line(size = 1) +
  geom_point(size = 2) +
  #geom_area(alpha=1 , size=.5, colour="white") +
  ylab("Share (%)") +
  xlab("Date") +
  labs(title = "Share of Remote Work Vacancy Postings (%)", subtitle = "Unweighted") +
  scale_x_date(breaks = as.Date(c("2014-01-01", "2015-01-01", "2016-01-01",
                                  "2017-01-01", "2018-01-01", "2019-01-01",
                                  "2020-01-01", "2021-01-01", "2022-01-01")),
               date_labels = '%Y') +
  #scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,2)) +
  #coord_cartesian(ylim = c(0, 20)) +
  scale_colour_manual(values=big_pal) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(nrow = 3, reverse = T)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"))
p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/uk_unweighted_by_job_src_l1o.png", width = 7, height = 5)
#### END ####



#### CHECK UK SOURCES ####
remove(list = ls())
df_all_uk <- fread(file = "./int_data/df_uk_standardised.csv")
colnames(df_all_uk)
summary(df_all_uk$wfh_wham)

sum(grepl("jobisjob", df_all_uk$job_domain))/nrow(df_all_uk) # 

df_all_uk <- df_all_uk %>% .[year %in% c(2019, 2020,2021,2022)]

sources_for_checking <- df_all_uk %>%
  .[year %in% c(2019, 2020,2021,2022)] %>%
  .[, .(.N), by = .(year_month, job_domain)] %>%
  .[, posting_share := 100*(N/sum(N)), by = year_month] %>%
  .[posting_share > 1] %>%
  select(job_domain) %>%
  distinct(job_domain)

df_all_uk$top_job_domain <- ifelse(df_all_uk$job_domain %in% sources_for_checking$job_domain, df_all_uk$job_domain, "other")
df_all_uk$year_month_cat <- as.character(as.yearmon(df_all_uk$year_month))

top_job_domain_df <- as.data.table(table(df_all_uk$top_job_domain[df_all_uk$year %in% c(2020,2021,2022)])) %>% mutate(other = as.numeric(V1 == "other")) %>%
  arrange(other, desc(N)) %>% select(-other) %>% mutate(prop = 100*N/sum(N))
top_job_domain_df
stargazer(top_job_domain_df, title = "Top Job Boards in Canada (Jan 2020 to Jun 2022)", rownames = F, summary = F)

colnames(df_all_uk)
mod1 <- feols(data = df_all_uk %>% filter(year %in% c(2020, 2021, 2022)), fml = wfh_wham ~ i(top_job_domain, ref = "other") | year_month_cat^bgt_occ^state)
etable(mod1, tex = F, title = "Remote Work by Job Board UK")
#### END ####

#### LEAVE ONE OUT STRATEGY - UK ####
wfh_ag <- lapply(unique(df_all_uk$top_job_domain), function(x) {
  df_all_uk %>%
    setDT(.) %>%
    .[top_job_domain != x] %>%
    .[, .(share_bert_unweighted = sum(job_id_weight*wfh_wham)/sum(job_id_weight)),
      by = .(year_month, year)] %>%
    .[, sample := paste0("w/o ",gsub("www.","",x))]
}) %>%
  rbindlist


big_pal <- brewer.pal(n = 14, name = "Set3")
big_pal <- c(big_pal, "#50C7C7", "#66455C")
p = wfh_ag %>%
  filter(year %in% c(2019, 2020, 2021, 2022)) %>%
  ggplot(., aes(x = as.Date(as.yearmon(year_month)), y = 100*share_bert_unweighted, colour = sample)) +
  geom_line(size = 1) +
  geom_point(size = 2) +
  #geom_area(alpha=1 , size=.5, colour="white") +
  ylab("Share (%)") +
  xlab("Date") +
  labs(title = "Share of Remote Work Vacancy Postings (%)", subtitle = "Unweighted") +
  scale_x_date(breaks = as.Date(c("2014-01-01", "2015-01-01", "2016-01-01",
                                  "2017-01-01", "2018-01-01", "2019-01-01",
                                  "2020-01-01", "2021-01-01", "2022-01-01")),
               date_labels = '%Y') +
  #scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,2)) +
  #coord_cartesian(ylim = c(0, 20)) +
  scale_colour_manual(values=big_pal) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(nrow = 5, reverse = T)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"))
p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/uk_unweighted_by_job_src_l1o.png", width = 7, height = 5)
#### END ####

#### LEAVE ONE OUT STRATEGY - SCOTLAND + CAMBRIDGE ####
remove(list = ls())
df_all_uk <- fread(file = "./int_data/df_uk_standardised.csv")
colnames(df_all_uk)
summary(df_all_uk$wfh_wham)

df_all_uk_ss <- df_all_uk %>%
  .[grepl("cambridge", city, ignore.case = T) | grepl("scotland", state, ignore.case = T)]

sources_for_checking <- df_all_uk_ss %>%
  .[year %in% c(2019, 2020,2021,2022)] %>%
  .[, .(.N), by = .(year_month, job_domain)] %>%
  .[, posting_share := 100*(N/sum(N)), by = year_month] %>%
  .[posting_share > 5] %>%
  select(job_domain) %>%
  distinct(job_domain)

df_all_uk_ss$top_job_domain <- ifelse(df_all_uk_ss$job_domain %in% sources_for_checking$job_domain, df_all_uk_ss$job_domain, "other")
df_all_uk_ss$year_month_cat <- as.character(as.yearmon(df_all_uk_ss$year_month))

top_job_domain_df <- as.data.table(table(df_all_uk_ss$top_job_domain[df_all_uk_ss$year %in% c(2020,2021,2022)])) %>% mutate(other = as.numeric(V1 == "other")) %>%
  arrange(other, desc(N)) %>% select(-other) %>% mutate(prop = 100*N/sum(N))
top_job_domain_df
stargazer(top_job_domain_df, title = "Top Job Boards in Scotland + Cambridge (Jan 2020 to Jun 2022)", rownames = F, summary = F)

wfh_ag <- lapply(unique(df_all_uk_ss$top_job_domain), function(x) {
  df_all_uk_ss %>%
    setDT(.) %>%
    .[top_job_domain != x] %>%
    .[, .(share_bert_unweighted = sum(job_id_weight*wfh_wham)/sum(job_id_weight)),
      by = .(year_month, year)] %>%
    .[, sample := paste0("w/o ",gsub("www.","",x))]
}) %>%
  rbindlist


big_pal <- brewer.pal(n = 14, name = "Set3")
big_pal <- c(big_pal, big_pal)
p = wfh_ag %>%
  filter(year %in% c(2019, 2020, 2021, 2022)) %>%
  ggplot(., aes(x = as.Date(as.yearmon(year_month)), y = 100*share_bert_unweighted, colour = sample)) +
  geom_line(size = 1) +
  geom_point(size = 2) +
  #geom_area(alpha=1 , size=.5, colour="white") +
  ylab("Share (%)") +
  xlab("Date") +
  labs(title = "Share of Remote Work Vacancy Postings (%)", subtitle = "Unweighted") +
  scale_x_date(breaks = as.Date(c("2014-01-01", "2015-01-01", "2016-01-01",
                                  "2017-01-01", "2018-01-01", "2019-01-01",
                                  "2020-01-01", "2021-01-01", "2022-01-01")),
               date_labels = '%Y') +
  #scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,2)) +
  #coord_cartesian(ylim = c(0, 20)) +
  scale_colour_manual(values=big_pal) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(nrow = 5, reverse = T)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"))
p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/scot_unweighted_by_job_src_l1o.png", width = 7, height = 5)
#### END ####

#### CHECK AUS SOURCES ####
remove(list = ls())
df_all_aus <- fread(file = "./int_data/df_aus_standardised.csv")
colnames(df_all_aus)
summary(df_all_aus$wfh_wham)

table(df_all_aus$state)

sources_for_checking <- df_all_aus %>%
  .[year %in% c(2019,2020,2021,2022)] %>%
  .[, .(.N), by = .(year_month, job_domain)] %>%
  .[, posting_share := 100*(N/sum(N)), by = year_month] %>%
  .[posting_share > 5] %>%
  select(job_domain) %>%
  distinct(job_domain)

df_all_aus$top_job_domain <- ifelse(df_all_aus$job_domain %in% sources_for_checking$job_domain, df_all_aus$job_domain, "other")
df_all_aus$year_month_cat <- as.character(as.yearmon(df_all_aus$year_month))

top_job_domain_df <- as.data.table(table(df_all_aus$top_job_domain[df_all_aus$year %in% c(2020,2021,2022)])) %>% mutate(other = as.numeric(V1 == "other")) %>%
  arrange(other, desc(N)) %>% select(-other) %>% mutate(prop = 100*N/sum(N))
top_job_domain_df
stargazer(top_job_domain_df, title = "Top Job Boards in Australia (Jan 2020 to Jun 2022)", rownames = F, summary = F)

sort(colnames(df_all_aus))

head(df_all_aus)

mod1 <- feols(data = df_all_aus %>% filter(year %in% c(2020, 2021, 2022)), fml = wfh_wham ~ i(top_job_domain, ref = "other") | year_month_cat^bgt_occ^state)
etable(mod1, tex = F, title = "Remote Work by Job Board Australia")
#### END ####

#### LEAVE ONE OUT STRATEGY - AUSTRALIA ####
wfh_ag <- lapply(unique(df_all_aus$top_job_domain), function(x) {
  df_all_aus %>%
    setDT(.) %>%
    .[top_job_domain != x] %>%
    .[, .(share_bert_unweighted = sum(job_id_weight*wfh_wham)/sum(job_id_weight)),
      by = .(year_month, year)] %>%
    .[, sample := paste0("w/o ",gsub("www.","",x))]
}) %>%
  rbindlist

big_pal <- brewer.pal(n = 14, name = "Set3")
big_pal <- c(big_pal, big_pal)

p = wfh_ag %>%
  filter(year %in% c(2019, 2020, 2021, 2022)) %>%
  ggplot(., aes(x = as.Date(as.yearmon(year_month)), y = 100*share_bert_unweighted, colour = sample)) +
  geom_line(size = 1) +
  geom_point(size = 2) +
  #geom_area(alpha=1 , size=.5, colour="white") +
  ylab("Share (%)") +
  xlab("Date") +
  labs(title = "Share of Remote Work Vacancy Postings (%)", subtitle = "Unweighted") +
  scale_x_date(breaks = as.Date(c("2014-01-01", "2015-01-01", "2016-01-01",
                                  "2017-01-01", "2018-01-01", "2019-01-01",
                                  "2020-01-01", "2021-01-01", "2022-01-01")),
               date_labels = '%Y') +
  #scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,2)) +
  #coord_cartesian(ylim = c(0, 20)) +
  scale_colour_manual(values=big_pal) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(nrow = 3, reverse = T)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"))
p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/australia_unweighted_by_job_src_l1o.png", width = 7, height = 5)
#### END ####

#### CHECK NZ SOURCES ####
remove(list = ls())
df_all_nz <- fread(file = "./int_data/df_nz_standardised.csv")
colnames(df_all_nz)
summary(df_all_nz$wfh_wham)
sources_for_checking <- df_all_nz %>%
  #.[year %in% c(2020,2021,2022)] %>%
  .[, .(.N), by = .(year_month, job_domain)] %>%
  .[, posting_share := 100*(N/sum(N)), by = year_month] %>%
  .[posting_share > 5] %>%
  select(job_domain) %>%
  distinct(job_domain)

df_all_nz$top_job_domain <- ifelse(df_all_nz$job_domain %in% sources_for_checking$job_domain, df_all_nz$job_domain, "other")
df_all_nz$year_month_cat <- as.character(as.yearmon(df_all_nz$year_month))

top_job_domain_df <- as.data.table(table(df_all_nz$top_job_domain)) %>% mutate(other = as.numeric(V1 == "other")) %>%
  arrange(other, desc(N)) %>% select(-other) %>% mutate(prop = 100*N/sum(N))
top_job_domain_df
stargazer(top_job_domain_df, title = "Top Job Boards in NZ (Jan 2020 to Jun 2022)", rownames = F, summary = F)

colnames(df_all_nz)
mod1 <- feols(data = df_all_nz %>% filter(year %in% c(2020,2021,2022)), fml = wfh_wham ~ i(top_job_domain, ref = "other") | year_month_cat^bgt_occ^state)
etable(mod1, tex = F, title = "Remote Work by Job Board NZ")
#### END ####

#### LEAVE ONE OUT STRATEGY - NZ ####
wfh_ag <- lapply(unique(df_all_nz$top_job_domain), function(x) {
  df_all_nz %>%
    setDT(.) %>%
    .[top_job_domain != x] %>%
    .[, .(share_bert_unweighted = sum(job_id_weight*wfh_wham)/sum(job_id_weight)),
      by = .(year_month, year)] %>%
    .[, sample := paste0("w/o ",gsub("www.","",x))]
}) %>%
  rbindlist

p = wfh_ag %>%
  filter(year %in% c(2019, 2020, 2021, 2022)) %>%
  ggplot(., aes(x = as.Date(as.yearmon(year_month)), y = 100*share_bert_unweighted, colour = sample)) +
  geom_line(size = 1) +
  geom_point(size = 2) +
  #geom_area(alpha=1 , size=.5, colour="white") +
  ylab("Share (%)") +
  xlab("Date") +
  labs(title = "Share of Remote Work Vacancy Postings (%)", subtitle = "Unweighted") +
  scale_x_date(breaks = as.Date(c("2014-01-01", "2015-01-01", "2016-01-01",
                                  "2017-01-01", "2018-01-01", "2019-01-01",
                                  "2020-01-01", "2021-01-01", "2022-01-01")),
               date_labels = '%Y') +
  #scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,2)) +
  #coord_cartesian(ylim = c(0, 20)) +
  #scale_colour_manual(values=cbbPalette) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(nrow = 3, reverse = T)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"))
p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/nz_unweighted_by_job_src_l1o.png", width = 7, height = 5)
#### END ####

#########################################
###### LOOK AT PROBLEMATIC BOARDS #######
#########################################

#### DAILY WFH SHARE FOR WORKOPOLIS
remove(list = setdiff(ls(), "df_all_can"))
df_all_can_workop <- df_all_can %>% .[top_job_domain == "www.workopolis.com"] %>% filter(year %in% c(2021, 2022))
colnames(df_all_can)
wfh_ag <- df_all_can %>%
  .[top_job_domain == "www.workopolis.com" & year %in% c(2018, 2019, 2020, 2021, 2022)] %>%
  setDT(.) %>%
  .[, date := ymd(job_date)] %>%
  .[, .(share_bert_unweighted = sum(job_id_weight*wfh_wham)/sum(job_id_weight),
        N = sum(job_id_weight)),
    by = .(date, year_month, year)]

p = wfh_ag %>%
  filter(year %in% c(2018, 2019, 2020, 2021, 2022)) %>%
  ggplot(., aes(x = date, y = 100*share_bert_unweighted)) +
  #stat_smooth (geom="line", alpha=0.8, size=2, span=0.2) +
  #stat_smooth(span = 0.1, alpha=0.5, se=FALSE, size = 2) +
  geom_area(alpha=1 , size=.5, colour="white") +
  ylab("Share (%)") +
  xlab("Date") +
  labs(title = "Share of Remote Work Vacancy Postings (%)", subtitle = "Daily Frequency, Workopolis.com") +
  scale_x_date() +
  #scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,2)) +
  #coord_cartesian(ylim = c(0, 20)) +
  #scale_colour_manual(values=cbbPalette) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(nrow = 3, reverse = T)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"))
p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/daily_workopolis_wfh_share.png", width = 7, height = 5)

wfh_ag <- df_all_can %>%
  .[top_job_domain == "www.workopolis.com" & year %in% c(2018, 2019, 2020, 2021, 2022)] %>%
  setDT(.) %>%
  .[, date := ymd(job_date)] %>%
  .[, .(share_bert_unweighted = sum(job_id_weight*wfh_wham)/sum(job_id_weight),
        N = sum(job_id_weight)),
    by = .(date, year_month, year)]



p = wfh_ag %>%
  filter(year %in% c(2018, 2019, 2020, 2021, 2022)) %>%
  ggplot(., aes(x = date, y = 100*share_bert_unweighted)) +
  #stat_smooth (geom="line", alpha=0.8, size=2, span=0.2) +
  #stat_smooth(span = 0.1, alpha=0.5, se=FALSE, size = 2) +
  geom_area(alpha=1 , size=.5, colour="white") +
  ylab("Share (%)") +
  xlab("Date") +
  labs(title = "Share of Remote Work Vacancy Postings (%)", subtitle = "Daily Frequency, Workopolis.com") +
  scale_x_date() +
  #scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,2)) +
  #coord_cartesian(ylim = c(0, 20)) +
  #scale_colour_manual(values=cbbPalette) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(nrow = 3, reverse = T)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"))
p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/daily_workopolis_wfh_share.png", width = 7, height = 5)

#### END ####