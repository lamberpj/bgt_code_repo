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
library(stats)

setwd("/mnt/disks/pdisk/bg_combined/")

# Set SD filter paramters
level_threshold <- 0.02 # absolute deviation in level of raw mean and trimmed mean
log_threshold <- 0.1 # absolute deviation in level of log(raw mean) and log(trimmed mean)

# Load all data
daily_data <- fread(file = "./aux_data/all_cities_daily.csv")

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

check <- ts_for_plot %>%
  .[, .(share_outlier_postings_percent = 100*sum((1-l1o_keep)*N)/sum(N)), by = .(country)] %>%
  select(country, share_outlier_postings_percent)
  
(days_replaced <- 100*round(sum(check$replaced)/sum(check$days), digits = 4))

ts_for_plot <- ts_for_plot %>%
  .[order(city, state, city_state, job_date)] %>%
  .[, monthly_mean_l1o := sum(l1o_daily_with_nas[!is.na(l1o_daily_with_nas)]*N[!is.na(l1o_daily_with_nas)], na.rm = T)/(sum(N[!is.na(l1o_daily_with_nas)], na.rm = T)), by = .(city, state, city_state, year_month)] %>%
  .[, .(N = sum(N),
        job_date = min(job_date),
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
  group_by(city, state, city_state, year_month, job_date, N) %>%
  pivot_longer(cols = c(drop_days_share:monthly_mean_3ma_l1o)) %>%
  setDT(.)

View(head(ts_for_plot, 1000))

fwrite(ts_for_plot, file = "./aux_data/city_level_ts.csv")

cbbPalette <- c("#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00", "#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00", "#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00")

(method_list <- unique(ts_for_plot$name))
(city_list <- unique(ts_for_plot$city_state))

lapply(1:length(city_list), function(i) {
  
  dat <- ts_for_plot[city_state == city_list[i]]
  (mean_dropped <- 100*round(mean(dat$value[dat$name == "drop_days_share"], na.rm = T), digits = 3))
  (max_dropped <- 100*round(max(dat$value[dat$name == "drop_days_share"], na.rm = T), digits = 3))
  (median_dropped <- 100*round(median(dat$value[dat$name == "drop_days_share"], na.rm = T), digits = 3))
  
  (country <- dat$country[1])
  (city <- dat$city[1])
  (state <- dat$state[1])
  
  dat <- dat %>% .[name %in% c("drop_days_share", "monthly_mean", "monthly_mean_l1o")]
  
  p = ggplot(data = dat, aes(x = as.Date(as.yearmon(job_date)), y = 100*value, colour = name, shape = name)) +
    geom_point(size = 3) +
    geom_line(size = 1) +
    ggtitle(paste0(city_list[i])) +
    ylab("Share (%)") +
    xlab("Date") +
    labs(subtitle = paste0("Mean monthly days dropped: ", mean_dropped,"%. Max monthly days dropped: ",max_dropped, "%.")) +
    scale_x_date(date_breaks = "2 months",
                 date_labels = '%Y-%m') +
    scale_y_continuous(labels = scales::number_format(accuracy = 1), breaks = seq(0, 100, 2)) +
    theme(
      axis.title.x=element_blank(),
      legend.position="bottom",
      legend.title = element_blank(),
      axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
    scale_colour_manual(values=cbbPalette) +
    guides(col = guide_legend(nrow = 1, reverse = F)) +
    theme(text = element_text(size=14, family="serif", colour = "black"),
          axis.text = element_text(size=13, family="serif", colour = "black"),
          axis.title = element_text(size=14, family="serif", colour = "black"),
          legend.text = element_text(size=13, family="serif", colour = "black"),
          panel.background = element_rect(fill = "white")) +
    guides(size = "none")
  
  p
  
  p_egg <- set_panel_size(p = p,
                          width = unit(6, "in"),
                          height = unit(4, "in"))
  ggsave(p_egg, filename = paste0("./plots/cities/",tolower(country),"_",tolower(state),"_",tolower(city),".pdf"), width = 8, height = 8)
  ggsave(p_egg, filename = paste0("./plots/cities/",tolower(country),"_",tolower(state),"_",tolower(city),".png"), width = 8, height = 8)
  
})


