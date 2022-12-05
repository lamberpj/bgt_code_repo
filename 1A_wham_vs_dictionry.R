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
ggthemr('flat')
library(egg)
library(extrafont)
library(fixest)
# font_import()
#loadfonts(device="postscript")
#fonts()

setDTthreads(8)
getDTthreads()
#quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg_combined/")

#### LOAD "ALL" ####
df_us_2019 <- fread("../bg-us/int_data/us_stru_2019_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_id","job_date","bgt_occ","soc","wfh_wham","narrow_result", "neg_narrow_result","job_domain","job_url"))
df_us_2020 <- fread("../bg-us/int_data/us_stru_2020_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_id","job_date","bgt_occ","soc","wfh_wham","narrow_result", "neg_narrow_result","job_domain","job_url"))
df_us_2021 <- fread("../bg-us/int_data/us_stru_2021_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_id","job_date","bgt_occ","soc","wfh_wham","narrow_result", "neg_narrow_result","job_domain","job_url"))
df_us_2022 <- fread("../bg-us/int_data/us_stru_2022_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_id","job_date","bgt_occ","soc","wfh_wham","narrow_result", "neg_narrow_result","job_domain","job_url"))

df_us_2019 <- df_us_2019 %>% .[!grepl("careerbuilder", job_url)]
df_us_2020 <- df_us_2020 %>% .[!grepl("careerbuilder", job_url)]
df_us_2021 <- df_us_2021 %>% .[!grepl("careerbuilder", job_url)]
df_us_2022 <- df_us_2022 %>% .[!grepl("careerbuilder", job_url)]

df_us <- rbindlist(list(df_us_2019,df_us_2020,df_us_2021,df_us_2022))
remove(list = setdiff(ls(), "df_us"))
#### END ####

df_us <- df_us %>% select(wfh_wham, job_date, soc, narrow_result, neg_narrow_result)
df_us$year <- year(df_us$job_date)
df_us$soc2 <- str_sub(df_us$soc, 1, 2)
df_us$soc5 <- str_sub(df_us$soc, 1, 6)
df_us <- setDT(df_us)

colnames(df_us)

# Time series bars
wfh_ag <- df_us %>%
  select(year, wfh_wham, narrow_result, neg_narrow_result) %>%
  .[, .(share_bert_vac = mean(wfh_wham, na.rm = T),
        share_dict_nn_vac = mean(neg_narrow_result, na.rm = T),
        share_dict_vac = mean(narrow_result, na.rm = T)),
    by = .(year)]

wfh_ag_ss_usa <- wfh_ag %>%
  group_by(year) %>%
  pivot_longer(., cols = share_bert_vac:share_dict_vac, names_to = "measure", values_to = "share") %>%
  mutate(measure = case_when(
    measure == "share_bert_vac" ~ "WHAM",
    measure == "share_dict_vac" ~ "Dictionary",
    measure == "share_dict_nn_vac" ~ "Dictionary (non-neg)",
    TRUE ~ ""))

# Plot
hr_pal <- c("grey39", "royalblue1", "navy")

p = wfh_ag_ss_usa %>%
  filter(year %in% c(2019, 2020, 2021, 2022)) %>%
  #filter(year_month <= as.yearmon("Sep 2021")) %>%
  #filter(year_month > as.yearmon("Dec 2017")) %>%
  filter(measure != "") %>%
  ggplot(., aes(x = as.factor(year), y = 100*share, fill = measure)) +
  geom_bar(stat = "identity", position = "dodge") +
  ylab("Share (%)") +
  xlab("Date") +
  labs(title = "Share of Remote Work Vacancy Postings (%)", subtitle = "Vacancy Weighted, USA") +
  #scale_x_yearmon(format = '%Y-%-m', breaks = seq(from = as.yearmon("2014-1"), to = as.yearmon("2021-9"), by = 1),  limits = c(as.yearmon("2014-1"), as.yearmon("2021-9"))) +
  #scale_x_date(breaks = as.Date(c("2014-01-01", "2015-01-01", "2016-01-01", "2017-01-01", "2018-01-01", "2019-01-01", "2020-01-01", "2021-01-01", "2022-01-01", "2022-01-01")),
  #             minor_breaks = as.Date(c("2014-01-01","2014-07-01","2015-01-01","2015-07-01","2016-01-01","2016-07-01","2017-01-01","2017-07-01",
  #                                      "2018-01-01","2018-07-01","2019-01-01","2019-07-01","2020-01-01","2020-07-01","2021-01-01","2021-07-01", "2022-01-01")),
  #             date_labels = '%Y') +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,5)) +
  coord_cartesian(ylim = c(0, 17)) +
  scale_fill_manual(values=hr_pal) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(nrow = 1, reverse = T)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm"))
p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/horse_race_usa_bp.pdf", width = 8, height = 6)

p = wfh_ag_ss_usa %>%
  filter(measure != "Dictionary (non-neg)") %>%
  filter(year %in% c(2019, 2020, 2021, 2022)) %>%
  #filter(year_month <= as.yearmon("Sep 2021")) %>%
  #filter(year_month > as.yearmon("Dec 2017")) %>%
  filter(measure != "") %>%
  ggplot(., aes(x = as.factor(year), y = 100*share, fill = measure)) +
  geom_bar(stat = "identity", position = "dodge") +
  ylab("Share (%)") +
  xlab("Date") +
  labs(title = "Share of Remote Work Vacancy Postings (%)", subtitle = "Vacancy Weighted, USA") +
  #scale_x_yearmon(format = '%Y-%-m', breaks = seq(from = as.yearmon("2014-1"), to = as.yearmon("2021-9"), by = 1),  limits = c(as.yearmon("2014-1"), as.yearmon("2021-9"))) +
  #scale_x_date(breaks = as.Date(c("2014-01-01", "2015-01-01", "2016-01-01", "2017-01-01", "2018-01-01", "2019-01-01", "2020-01-01", "2021-01-01", "2022-01-01", "2022-01-01")),
  #             minor_breaks = as.Date(c("2014-01-01","2014-07-01","2015-01-01","2015-07-01","2016-01-01","2016-07-01","2017-01-01","2017-07-01",
  #                                      "2018-01-01","2018-07-01","2019-01-01","2019-07-01","2020-01-01","2020-07-01","2021-01-01","2021-07-01", "2022-01-01")),
  #             date_labels = '%Y') +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,5)) +
  coord_cartesian(ylim = c(0, 17)) +
  scale_fill_manual(values=hr_pal) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(nrow = 1, reverse = T)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm"))
p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))

ggsave(p_egg, filename = "./plots/horse_race_usa_bp_nnn.pdf", width = 8, height = 6)

# Time series bars by SOC5
wfh_ag <- df_us %>%
  select(year, wfh_wham, narrow_result, neg_narrow_result, soc2) %>%
  .[, .(share_bert_vac = mean(wfh_wham, na.rm = T),
        share_dict_nn_vac = mean(neg_narrow_result, na.rm = T),
        share_dict_vac = mean(narrow_result, na.rm = T)),
    by = .(year, soc2)]

wfh_ag_ss_usa <- wfh_ag %>%
  group_by(year) %>%
  pivot_longer(., cols = share_bert_vac:share_dict_vac, names_to = "measure", values_to = "share") %>%
  mutate(measure = case_when(
    measure == "share_bert_vac" ~ "WHAM",
    measure == "share_dict_vac" ~ "Dictionary",
    measure == "share_dict_nn_vac" ~ "Dictionary (non-neg)",
    TRUE ~ ""))

# Plot
hr_pal <- c("grey39", "royalblue1", "navy")

p = wfh_ag_ss_usa %>%
  filter(soc2 != "" & !is.na(soc2)) %>%
  filter(measure != "Dictionary (non-neg)") %>%
  filter(year %in% c(2022)) %>%
  #filter(year_month <= as.yearmon("Sep 2021")) %>%
  #filter(year_month > as.yearmon("Dec 2017")) %>%
  filter(measure != "") %>%
  ggplot(., aes(x = as.factor(soc2), y = 100*share, fill = measure)) +
  geom_bar(stat = "identity", position = "dodge") +
  ylab("Share (%)") +
  xlab("Standard Occupational Classification (SOC)") +
  labs(title = "RW Share by Method (%) (2022)", subtitle = "Vacancy Weighted, USA") +
  #scale_x_yearmon(format = '%Y-%-m', breaks = seq(from = as.yearmon("2014-1"), to = as.yearmon("2021-9"), by = 1),  limits = c(as.yearmon("2014-1"), as.yearmon("2021-9"))) +
  #scale_x_date(breaks = as.Date(c("2014-01-01", "2015-01-01", "2016-01-01", "2017-01-01", "2018-01-01", "2019-01-01", "2020-01-01", "2021-01-01", "2022-01-01", "2022-01-01")),
  #             minor_breaks = as.Date(c("2014-01-01","2014-07-01","2015-01-01","2015-07-01","2016-01-01","2016-07-01","2017-01-01","2017-07-01",
  #                                      "2018-01-01","2018-07-01","2019-01-01","2019-07-01","2020-01-01","2020-07-01","2021-01-01","2021-07-01", "2022-01-01")),
  #             date_labels = '%Y') +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,60,5)) +
  coord_cartesian(ylim = c(0, 40)) +
  scale_fill_manual(values=hr_pal) +
  theme(
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(nrow = 1, reverse = T)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm"))
p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/horse_race_usa_bp_by_soc_2022.pdf", width = 8, height = 6)

p = wfh_ag_ss_usa %>%
  filter(soc2 != "" & !is.na(soc2)) %>%
  filter(measure != "Dictionary (non-neg)") %>%
  filter(year %in% c(2021)) %>%
  #filter(year_month <= as.yearmon("Sep 2021")) %>%
  #filter(year_month > as.yearmon("Dec 2017")) %>%
  filter(measure != "") %>%
  ggplot(., aes(x = as.factor(soc2), y = 100*share, fill = measure)) +
  geom_bar(stat = "identity", position = "dodge") +
  ylab("Share (%)") +
  xlab("Standard Occupational Classification (SOC)") +
  labs(title = "RW Share by Method (%) (2021)", subtitle = "Vacancy Weighted, USA") +
  #scale_x_yearmon(format = '%Y-%-m', breaks = seq(from = as.yearmon("2014-1"), to = as.yearmon("2021-9"), by = 1),  limits = c(as.yearmon("2014-1"), as.yearmon("2021-9"))) +
  #scale_x_date(breaks = as.Date(c("2014-01-01", "2015-01-01", "2016-01-01", "2017-01-01", "2018-01-01", "2019-01-01", "2020-01-01", "2021-01-01", "2022-01-01", "2022-01-01")),
  #             minor_breaks = as.Date(c("2014-01-01","2014-07-01","2015-01-01","2015-07-01","2016-01-01","2016-07-01","2017-01-01","2017-07-01",
  #                                      "2018-01-01","2018-07-01","2019-01-01","2019-07-01","2020-01-01","2020-07-01","2021-01-01","2021-07-01", "2022-01-01")),
  #             date_labels = '%Y') +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,60,5)) +
  coord_cartesian(ylim = c(0, 40)) +
  scale_fill_manual(values=hr_pal) +
  theme(
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(nrow = 1, reverse = T)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm"))
p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))

ggsave(p_egg, filename = "./plots/horse_race_usa_bp_by_soc_2021.pdf", width = 8, height = 6)


