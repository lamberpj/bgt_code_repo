#### COMPARE US STATES TO GMD ####
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
library(scales)
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
library(haven)
library(phonics)
library(PGRdup)
library(qdapDictionaries)
# font_import()
#loadfonts(device="postscript")
#fonts()

setDTthreads(8)
getDTthreads()
#quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg-us/")

#### LOAD "US BGT" ####
df_us_2019 <- fread("./int_data/us_stru_2019_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "US") %>% setDT() %>% .[!is.na(employer) & employer != ""]
df_us_2022 <- fread("./int_data/us_stru_2022_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "US") %>% setDT() %>% .[!is.na(employer) & employer != ""]
df_us_2023 <- fread("./int_data/us_stru_2023_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "US") %>% setDT() %>% .[!is.na(employer) & employer != ""]

df_us <- rbindlist(list(df_us_2019,df_us_2022,df_us_2023), use.names = T)
remove(list = c("df_us_2019","df_us_2020","df_us_2021","df_us_2022","df_us_2023"))

setwd("/mnt/disks/pdisk/bg-us/")

df_us <- df_us %>% .[, city_state := paste0(city,"_",state)]

uniqueN(df_us$city_state) # 31,568

df_us <- df_us[!is.na(job_domain) & job_domain != ""]
df_us <- df_us %>% .[!grepl("careerbuilder", job_domain)]

remove(list = setdiff(ls(), "df_us"))

df_us <- df_us %>%
  .[, year_month := as.yearmon(job_date)]

df_us <- df_us %>%
  .[year(year_month) == 2019 | year_month >= as.yearmon(ymd("20220601"))] %>%
  .[, year := ifelse(year(year_month) == 2019, "2019", "2023")]

table(as.character(df_us$year_month))

colnames(df_us)

unique(df_us$min_salary)
unique(df_us$max_salary)

nrow(df_us) # 60,023,579
df_us <- df_us %>% .[!is.na(min_salary)]
nrow(df_us) # 24,314,305

# Create salary bins
df_us_quintiles <- df_us %>%
  group_by(year) %>%
  mutate(bin = cut(min_salary,c(seq(20000, 120000, 10000), Inf))) %>%
  setDT(.) %>%
  .[, .(mean_wfh = mean(wfh_wham), n = .N, min_sal = min(min_salary), max_sal = max(min_salary)), by = .(bin, year)] %>%
  .[order(bin)]

df_us_quintiles <- df_us_quintiles %>%
  .[order(year, bin)]

df_us_quintiles

p = df_us_quintiles %>%
  ggplot(., aes(y = mean_wfh*100, x = min_sal, shape = as.character(year), colour = as.character(year))) +
  scale_x_continuous(breaks = seq(0, 200000, 20000), labels = comma) +
  scale_y_continuous(breaks = seq(0, 50, 5)) +
  geom_point(size = 4) +
  ylab("Job postings offering hybrid or remote work, %") +
  xlab('Nominal Posted Salary, USD') +
  #labs(title = "Binscatter WFH vs Posting Counts", subtitle = "No reweighting, no residualising") +
  #coord_cartesian(xlim = c(0, 400), ylim = c(2, 13), expand = FALSE) +
  theme(
    #axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 0)) +
  theme(text = element_text(size=11, family="sanserif", colour = "black"),
        axis.text = element_text(size=11, family="sanserif", colour = "black"),
        axis.title = element_text(size=11, family="sanserif", colour = "black"),
        legend.text = element_text(size=11, family="sanserif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm"),
        legend.position = c(0.085, 0.865)) +
  guides(colour = guide_legend(ncol = 1)) +
  scale_shape_manual(values=c(17, 16)) +
  theme(aspect.ratio=3/5)
ggsave(p, file = "./binscatter_posted_wages_wfh.png")
save(p, file = "./ppt/ggplots/wfh_share_vs_firm_size_bs1.RData")





