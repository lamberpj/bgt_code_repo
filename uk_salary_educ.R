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
library(ggrepel)
set.seed(999)
# font_import()
#loadfonts(device="postscript")
#fonts()

setDTthreads(4)
getDTthreads()
#quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg_combined/")

#### LOAD "ALL" ####
df_uk <- fread("./int_data/df_uk_standardised.csv", nThread = 8, integer64 = "numeric")

df_uk <- df_uk %>% .[!grepl("jobisjob", job_url)]

colnames(df_uk)

# Salary
df_uk_salary <- df_uk %>%
  .[year == 2022] %>%
  .[disjoint_salary != "" & !is.na(disjoint_salary)] %>%
  .[, disjoint_salary := as.numeric(disjoint_salary)] %>%
  .[, salary_band := case_when(
    disjoint_salary < 20000 ~ "< £20,000",
    disjoint_salary < 30000 ~ "£20,000 - £30,000",
    disjoint_salary < 50000 ~ "£30,000 - £50,000",
    disjoint_salary < 80000 ~ "£50,000 - £80,000",
    disjoint_salary >= 80000 ~ "> £80,000")] %>%
  .[, .(share_by_salary = mean(wfh_wham), min_salary = min(disjoint_salary), .N), by = salary_band] %>%
  .[order(min_salary)] %>%
  mutate(salary_band = fct_reorder(salary_band, desc(min_salary)))

sum(df_uk_salary$N)

p = ggplot(data = df_uk_salary, aes(x = salary_band, y = 100*share_by_salary)) +
  geom_bar(stat="identity") +
  ylab("Share (%)") +
  xlab("Salary Band (£GBP)") +
  coord_flip() +
  #labs(title = "Remote Work Vacancies vs Travel Arrangements", subtitle = "WHAM Measures of RW Vacancies.  ACS measures working at home.") +
  #coord_cartesian(xlim = c(10, 30), ylim = c(0.1, 0.2)) +
  theme(
    #axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 0)) +
  theme(text = element_text(size=16, family="serif", colour = "black"),
        axis.text = element_text(size=16, family="serif", colour = "black"),
        axis.title = element_text(size=16, family="serif", colour = "black"),
        legend.text = element_text(size=16, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  guides(colour = guide_legend(ncol = 3))

p

p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/uk_salary_bands.pdf", width = 8, height = 6)

# Education

unique(df_uk$disjoint_degree_level)

df_uk_educ <- df_uk %>%
  .[year == 2022] %>%
  .[disjoint_salary != "" & !is.na(disjoint_salary)] %>%
  .[, disjoint_salary := as.numeric(disjoint_salary)] %>%
  .[, salary_band := case_when(
    disjoint_salary < 20000 ~ "< £20,000",
    disjoint_salary < 30000 ~ "£20,000 - £30,000",
    disjoint_salary < 50000 ~ "£30,000 - £50,000",
    disjoint_salary < 80000 ~ "£50,000 - £80,000",
    disjoint_salary >= 80000 ~ "> £80,000")] %>%
  .[, .(share_by_salary = mean(wfh_wham), min_salary = min(disjoint_salary), .N), by = salary_band] %>%
  .[order(min_salary)] %>%
  mutate(salary_band = fct_reorder(salary_band, desc(min_salary)))

sum(df_uk_educ$N)

p = ggplot(data = df_uk_salary, aes(x = salary_band, y = 100*share_by_salary)) +
  geom_bar(stat="identity") +
  ylab("Share (%)") +
  xlab("Salary Band (£GBP)") +
  coord_flip() +
  #labs(title = "Remote Work Vacancies vs Travel Arrangements", subtitle = "WHAM Measures of RW Vacancies.  ACS measures working at home.") +
  #coord_cartesian(xlim = c(10, 30), ylim = c(0.1, 0.2)) +
  theme(
    #axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 0)) +
  theme(text = element_text(size=16, family="serif", colour = "black"),
        axis.text = element_text(size=16, family="serif", colour = "black"),
        axis.title = element_text(size=16, family="serif", colour = "black"),
        legend.text = element_text(size=16, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  guides(colour = guide_legend(ncol = 3))

p

p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/uk_salary_bands.pdf", width = 8, height = 6)

#### END ####


#### City level ####

# Load all data
daily_data <- fread(file = "./aux_data/all_cities_daily.csv")

daily_data <- daily_data %>% .[grepl("Liverpool|Glasgow|Edinburgh|London|Manchester", city)]

head(daily_data)

nrow(df_uk)

df_uk_rest <- df_uk %>%
  .[city != "" & !is.na(city)] %>%
  .[!grepl("Liverpool|Glasgow|Edinburgh|London|Manchester", city)] %>%
  .[, .(daily_share = mean(wfh_wham), .N), by = .(year_month, job_date)] %>%
  mutate(country = "UK",
         state = "Rest",
         city = "Rest",
         city_state = "Rest") %>%
  setDT(.)
  
daily_data <- daily_data[city %in% df_uk$city & country == "UK"]

daily_data <- bind_rows(daily_data, df_uk_rest) %>% setDT(.)

check <- daily_data %>%
  .[, .(.N), by = .(country, city_state, rank)]

ts_for_plot <- daily_data %>%
  .[, job_date := ymd(job_date)] %>%
  .[city != ""] %>%
  .[as.yearmon(year_month) >= as.yearmon(ymd("20180901")) & as.yearmon(year_month) <= as.yearmon(ymd("20220601"))] %>%
  #.[count_for_thresh >= 150000] %>%
  .[, l1o_monthly_mean := (sum(daily_share*N)-daily_share*N)/(sum(N) - N), by = .(country, city, state, city_state, year_month)] %>%
  .[, monthly_mean := sum(daily_share*N)/(sum(N)), by = .(country, city, state, city_state, year_month)] %>%
  .[, l1o_keep := ifelse(abs(monthly_mean - l1o_monthly_mean) > 0.02 | abs(log(monthly_mean/l1o_monthly_mean)) > 0.10, 0, 1)] %>%
  .[, l1o_keep := ifelse(is.na(l1o_keep), 1, l1o_keep)] %>%
  .[, l1o_daily_with_nas := ifelse(l1o_keep == 1, daily_share, NA)]

ts_for_plot <- ts_for_plot %>%
  .[order(city, state, city_state, job_date)] %>%
  .[, monthly_mean_l1o := sum(l1o_daily_with_nas[!is.na(l1o_daily_with_nas)]*N[!is.na(l1o_daily_with_nas)], na.rm = T)/(sum(N[!is.na(l1o_daily_with_nas)], na.rm = T)), by = .(city, state, city_state, year_month)] %>%
  .[, .(job_date = min(job_date),
        drop_days_share = sum(1-l1o_keep)/.N,
        monthly_mean = monthly_mean[1],
        monthly_mean_l1o = monthly_mean_l1o[1]),
    by = .(country, city, state, city_state, year_month)] %>%
  .[, monthly_mean_3ma := rollmean(monthly_mean, k = 3, align = "right", fill = NA), by = .(country, city, state, city_state)] %>%
  .[, monthly_mean_3ma_l1o := rollmean(monthly_mean_l1o, k = 3, align = "right", fill = NA), by = .(country, city, state, city_state)] %>%
  .[, monthly_mean_l1o := ifelse(as.yearmon(year_month) %in% as.yearmon(ymd(c("20200301", "20200401", "20200501", "20200601"))), monthly_mean, monthly_mean_l1o)] %>%
  .[, monthly_mean_3ma := ifelse(as.yearmon(year_month) %in% as.yearmon(ymd(c("20200301", "20200401", "20200501", "20200601"))), monthly_mean, monthly_mean_3ma)] %>%
  .[, monthly_mean_3ma_l1o := ifelse(as.yearmon(year_month) %in% as.yearmon(ymd(c("20200301", "20200401", "20200501", "20200601"))), monthly_mean, monthly_mean_3ma_l1o)] %>%
  .[as.yearmon(year_month) >= as.yearmon(ymd("20190101")) & as.yearmon(year_month) <= as.yearmon(ymd("20220601"))]

ts_for_plot <- ts_for_plot %>%
  group_by(city, state, city_state, year_month, job_date) %>%
  pivot_longer(cols = c(drop_days_share:monthly_mean_3ma_l1o)) %>%
  setDT(.)

cbbPalette <- c("#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00", "#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00", "#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00")

ts_for_plot_cit <- ts_for_plot %>%
  #.[grepl("Odessa|Miami Beach|Savannah|Birm|Boston|Cleveland|Columbus|Des Moines|Indianapolis|Jacksonville|Louisville|Memphis|New York|Oklahoma City|San Francisco|Wichita", city)] %>%
  .[name == "monthly_mean_3ma_l1o"] %>%
  .[country == "UK"]

sort(unique(ts_for_plot_cit$city_state))

remove(ts_for_plot)

cbbPalette <- c("#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00", "#000000")

p = ts_for_plot_cit %>%
  .[year(as.Date(as.yearmon(year_month)))>= 2019] %>%
  .[grepl("Liverpool|Glasgow|Edinburgh|London|Manchester|Rest", city)] %>%
  ggplot(., aes(x = as.Date(as.yearmon(year_month)), y = 100*value, colour = city_state)) +
  #stat_smooth (geom="line", alpha=0.8, size=2, span=0.2) +
  #stat_smooth(span = 0.1, alpha=0.5, se=FALSE, size = 2) +
  geom_point(size = 3) +
  geom_line(size = 1) +
  ylab("Share (%)") +
  xlab("Date") +
  labs(title = "Share of Remote Work Vacancy Postings (%)", subtitle = paste0("Unweighted, Outliers Removed. Removed ")) +
  scale_x_date(date_breaks = "3 months",
               date_labels = '%Y-%m') +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,50,5)) +
  coord_cartesian(ylim = c(0, 30)) +
  scale_color_manual(values = cbbPalette) +
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
        panel.background = element_rect(fill = "white")) +
  guides(shape = "none")
p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/uk_other_cities.pdf", width = 8, height = 6)


