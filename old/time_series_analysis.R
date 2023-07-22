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

# NZ Daily
remove(list = ls())
source("/mnt/disks/pdisk/bgt_code_repo/iqr_outlier_removal.R")
df_all_nz <- fread(file = "./int_data/df_nz_standardised.csv")
df_all_nz <- df_all_nz %>% .[!grepl("mercadojobs", df_all_nz$job_url)]
top_cities <- df_all_nz %>%
  .[city != ""] %>%   .[as.yearmon(year_month) >= as.yearmon("January 2019")] %>%
  .[, .(.N), by = .(city, state)] %>%
  .[N > 150000] %>%
  .[order(desc(N))] %>%
  .[, city_state := paste0(city,", ",state)]

df_all_nz <- df_all_nz %>% .[, city_state := paste0(city,", ",state)] %>% .[city_state %in% top_cities$city_state]

colnames(df_all_nz)

ts_compare <- df_all_nz %>%
  .[, .(.N, daily_mean = mean(wfh_wham, na.rm = T)), by = .(city, state, city_state, year_month, job_date)] %>%
  .[, period := ifelse(as.yearmon(year_month) < as.yearmon("March 2020"), 0,
                       ifelse(as.yearmon(year_month) %in% as.yearmon(c("March 2020", "April 2020")), 1, 2))] %>%
  group_by(city, state, city_state, period) %>%
  group_split(., .keep = T) %>%
  lapply(., function(m) {
    m <- setDT(m) %>%
      .[, rolling_monthly_trim_mean:=rollapply(data = .[,c(6,7)],
                                               width = 90,
                                               function(x) {
                                                 y <- x[, c(2)]
                                                 qnt <- quantile(y, probs=c(.25, .75), na.rm = T)
                                                 H <- 1.5 * IQR(y, na.rm = T)
                                                 w <- x[, c(1)]
                                                 y[y < (qnt[1] - H)] <- NA
                                                 y[y > (qnt[2] + H)] <- NA
                                                 w[y < (qnt[1] - H)] <- NA
                                                 w[y > (qnt[2] + H)] <- NA
                                                 sum(y*w, na.rm = T)/sum(w, na.rm = T)
                                               },
                                               partial = T, by.column = FALSE, align = "center", fill = NA)] %>%
      setDT(.)
  }) %>%
  rbindlist %>%
  .[, dev := abs(rolling_monthly_trim_mean - daily_mean)] %>%
  .[, med_dev := rollapply(data = dev, width = 60, function(x) {median(x, na.rm = T)}, partial = T, align = "center"), by = .(city, state, city_state, period)] %>%
  .[, trim := ifelse(dev > 1.5*med_dev, 1, 0)] %>%
  .[, monthly_mean := sum(daily_mean*N, na.rm = T)/sum(N, na.rm = T), by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean_pl := sum(daily_mean*N*(1-trim), na.rm = T)/sum(N*(1-trim), na.rm = T), by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean_pl := ifelse(period == 1, monthly_mean, monthly_mean_pl)] %>%
  .[, monthly_mean_nb := sum(Winsorize(daily_mean, probs = c(0, 0.9))*Winsorize(N, probs = c(0, 0.9)), na.rm = T)/sum(Winsorize(N, probs = c(0, 0.9)), na.rm = T), by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean_sd_ref := mean(daily_mean, trim = 0.1), by = .(city, state, city_state, year_month)] %>%
  .[, .( job_date = min(job_date),
         monthly_mean = monthly_mean[1],
         monthly_mean_pl = monthly_mean_pl[1],
         monthly_mean_nb = monthly_mean_nb[1],
         monthly_mean_sd_ref = monthly_mean_sd_ref[1]),
    by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean := 100*round(monthly_mean, 4)] %>%
  .[, monthly_mean_pl := 100*round(monthly_mean_pl, 4)] %>%
  .[, monthly_mean_nb := 100*round(monthly_mean_nb, 4)] %>%
  .[, monthly_mean_sd_ref := 100*round(monthly_mean_sd_ref, 4)] %>%
  .[, monthly_mean_sd := ifelse(monthly_mean_sd_ref<=4 & abs(monthly_mean - monthly_mean_sd_ref)>3, NA, monthly_mean)] %>%
  .[, monthly_mean_sd := ifelse(monthly_mean_sd_ref>4 & abs(log(monthly_mean) - log(monthly_mean_sd_ref))>0.1, NA, monthly_mean_sd)] %>%
  .[, monthly_mean_sd := na.approx(monthly_mean_sd, rule = 2, na.rm = F), by = .(city, state, city_state, year_month)] %>%
  .[job_date >= ymd("20190101")] %>%
  .[city != ""]

lapply(1:length(top_cities$city_state), function(i) {
  
  p = ggplot() +
    #geom_point(data = ts_compare[trim == 0 & city_state == top_cities$city_state[i]], aes(x = as.Date(job_date), size = N, y = daily_mean*100), colour = "black", alpha = 0.5) +
    #geom_point(data = ts_compare[trim == 1 & city_state == top_cities$city_state[i]], aes(x = as.Date(job_date), size = N, y = daily_mean*100), colour = "orange", alpha = 0.5) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean, colour = "Mean (raw)"), size = 2, alpha = 0.75) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_nb, colour = "Mean (NBF)"), size = 2, alpha = 0.75) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_sd, colour = "Mean (SDF)"), size = 2, alpha = 0.75) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_pl, colour = "Mean (PLF)"), size = 2, alpha = 0.75) +
    ylab("Share (%)") +
    xlab("Date") +
    labs(title = top_cities$city_state[i], subtitle = paste0("N = ", prettyNum(top_cities$N[i], big.mark = ","))) +
    scale_x_date(date_breaks = "3 months",
                 date_labels = '%Y-%m') +
    scale_y_continuous(labels = scales::number_format(accuracy = 1), breaks = seq(0, 100, 2)) +
    #coord_cartesian(ylim = c(0, 20)) +
    scale_color_manual(name = "Series:", values = c("Mean (raw)" = "darkorange", "Mean (PLF)" = "red", "Mean (NBF)" = "darkblue", "Mean (SDF)" = "black")) +
    theme(
      axis.title.x=element_blank(),
      legend.position="bottom",
      legend.title = element_blank(),
      axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
    guides(col = guide_legend(nrow = 2, reverse = F)) +
    theme(text = element_text(size=14, family="serif", colour = "black"),
          axis.text = element_text(size=13, family="serif", colour = "black"),
          axis.title = element_text(size=14, family="serif", colour = "black"),
          legend.text = element_text(size=13, family="serif", colour = "black"),
          panel.background = element_rect(fill = "white")) +
    guides(size = "none")
  
  p_egg <- set_panel_size(p = p,
                          width = unit(6, "in"),
                          height = unit(10, "in"))
  ggsave(p_egg, filename = paste0("./plots/validate4/nz",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(top_cities$city_state[i]))),".png"), width = 8, height = 12)
  ggsave(p_egg, filename = paste0("./plots/validate4/nz",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(top_cities$city_state[i]))),".pdf"), width = 8, height = 12)
})

# Australia Daily
remove(list = ls())
source("/mnt/disks/pdisk/bgt_code_repo/iqr_outlier_removal.R")
df_all_aus <- fread(file = "./int_data/df_aus_standardised.csv")

top_cities <- df_all_aus %>%
  .[city != ""] %>%   .[as.yearmon(year_month) >= as.yearmon("January 2019")] %>%
  .[, .(.N), by = .(city, state)] %>%
  .[N > 150000] %>%
  .[order(desc(N))] %>%
  .[, city_state := paste0(city,", ",state)]

df_all_aus <- df_all_aus %>% .[, city_state := paste0(city,", ",state)] %>% .[city_state %in% top_cities$city_state]

ts_compare <- df_all_aus %>%
  .[, .(.N, daily_mean = mean(wfh_wham, na.rm = T)), by = .(city, state, city_state, year_month, job_date)] %>%
  .[, period := ifelse(as.yearmon(year_month) < as.yearmon("March 2020"), 0,
                       ifelse(as.yearmon(year_month) %in% as.yearmon(c("March 2020", "April 2020", "May 2020")), 1, 2))] %>%
  group_by(city, state, city_state, period) %>%
  group_split(., .keep = T) %>%
  lapply(., function(m) {
    m <- setDT(m) %>%
  .[, rolling_monthly_trim_mean:=rollapply(data = .[,c(6,7)],
                                           width = 60,
                                           function(x) {
                                             y <- x[, c(2)]
                                             qnt <- quantile(y, probs=c(.25, .75), na.rm = T)
                                             H <- 1.5 * IQR(y, na.rm = T)
                                             w <- x[, c(1)]
                                             y[y < (qnt[1] - H)] <- NA
                                             y[y > (qnt[2] + H)] <- NA
                                             w[y < (qnt[1] - H)] <- NA
                                             w[y > (qnt[2] + H)] <- NA
                                             sum(y*w, na.rm = T)/sum(w, na.rm = T)
                                           },
                                           partial = T, by.column = FALSE, align = "center", fill = NA)] %>%
      setDT(.)
  }) %>%
  rbindlist %>%
  .[, dev := abs(rolling_monthly_trim_mean - daily_mean)] %>%
  .[, med_dev := rollapply(data = dev, width = 60, function(x) {median(x, na.rm = T)}, partial = T, align = "center"), by = .(city, state, city_state, period)] %>%
  .[, trim := ifelse(dev > 1.5*med_dev, 1, 0)] %>%
  .[, monthly_mean := sum(daily_mean*N, na.rm = T)/sum(N, na.rm = T), by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean_pl := sum(daily_mean*N*(1-trim), na.rm = T)/sum(N*(1-trim), na.rm = T), by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean_pl := ifelse(period == 1, monthly_mean, monthly_mean_pl)] %>%
  .[, monthly_mean_nb := sum(Winsorize(daily_mean, probs = c(0, 0.9))*Winsorize(N, probs = c(0, 0.9)), na.rm = T)/sum(Winsorize(N, probs = c(0, 0.9)), na.rm = T), by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean_sd_ref := mean(daily_mean, trim = 0.1), by = .(city, state, city_state, year_month)] %>%
  .[, .( job_date = min(job_date),
         monthly_mean = monthly_mean[1],
         monthly_mean_pl = monthly_mean_pl[1],
         monthly_mean_nb = monthly_mean_nb[1],
         monthly_mean_sd_ref = monthly_mean_sd_ref[1]),
    by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean := 100*round(monthly_mean, 4)] %>%
  .[, monthly_mean_pl := 100*round(monthly_mean_pl, 4)] %>%
  .[, monthly_mean_nb := 100*round(monthly_mean_nb, 4)] %>%
  .[, monthly_mean_sd_ref := 100*round(monthly_mean_sd_ref, 4)] %>%
  .[, monthly_mean_sd := ifelse(monthly_mean_sd_ref<=4 & abs(monthly_mean - monthly_mean_sd_ref)>3, NA, monthly_mean)] %>%
  .[, monthly_mean_sd := ifelse(monthly_mean_sd_ref>4 & abs(log(monthly_mean) - log(monthly_mean_sd_ref))>0.1, NA, monthly_mean_sd)] %>%
  .[, monthly_mean_sd := na.approx(monthly_mean_sd, rule = 2, na.rm = F), by = .(city, state, city_state, year_month)] %>%
  .[job_date >= ymd("20190101")] %>%
  .[city != ""]

lapply(1:length(top_cities$city_state), function(i) {
  
  p = ggplot() +
    #geom_point(data = ts_compare[trim == 0 & city_state == top_cities$city_state[i]], aes(x = as.Date(job_date), size = N, y = daily_mean*100), colour = "black", alpha = 0.5) +
    #geom_point(data = ts_compare[trim == 1 & city_state == top_cities$city_state[i]], aes(x = as.Date(job_date), size = N, y = daily_mean*100), colour = "orange", alpha = 0.5) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean, colour = "Mean (raw)"), size = 2, alpha = 0.75) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_nb, colour = "Mean (NBF)"), size = 2, alpha = 0.75) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_sd, colour = "Mean (SDF)"), size = 2, alpha = 0.75) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_pl, colour = "Mean (PLF)"), size = 2, alpha = 0.75) +
    ylab("Share (%)") +
    xlab("Date") +
    labs(title = top_cities$city_state[i], subtitle = paste0("N = ", prettyNum(top_cities$N[i], big.mark = ","))) +
    scale_x_date(date_breaks = "3 months",
                 date_labels = '%Y-%m') +
    scale_y_continuous(labels = scales::number_format(accuracy = 1), breaks = seq(0, 100, 2)) +
    #coord_cartesian(ylim = c(0, 20)) +
    scale_color_manual(name = "Series:", values = c("Mean (raw)" = "darkorange", "Mean (PLF)" = "red", "Mean (NBF)" = "darkblue", "Mean (SDF)" = "black")) +
    theme(
      axis.title.x=element_blank(),
      legend.position="bottom",
      legend.title = element_blank(),
      axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
    guides(col = guide_legend(nrow = 2, reverse = F)) +
    theme(text = element_text(size=14, family="serif", colour = "black"),
          axis.text = element_text(size=13, family="serif", colour = "black"),
          axis.title = element_text(size=14, family="serif", colour = "black"),
          legend.text = element_text(size=13, family="serif", colour = "black"),
          panel.background = element_rect(fill = "white")) +
    guides(size = "none")
  
  p_egg <- set_panel_size(p = p,
                          width = unit(6, "in"),
                          height = unit(10, "in"))
  ggsave(p_egg, filename = paste0("./plots/validate4/aus",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(top_cities$city_state[i]))),".png"), width = 8, height = 12)
  ggsave(p_egg, filename = paste0("./plots/validate4/aus",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(top_cities$city_state[i]))),".pdf"), width = 8, height = 12)
})



# USA Daily
remove(list = ls())
source("/mnt/disks/pdisk/bgt_code_repo/iqr_outlier_removal.R")
df_all_us_2019 <- fread(file = "./int_data/df_us_2019_standardised.csv")
df_all_us_2020 <- fread(file = "./int_data/df_us_2020_standardised.csv")
df_all_us_2021 <- fread(file = "./int_data/df_us_2021_standardised.csv")
df_all_us_2022 <- fread(file = "./int_data/df_us_2022_standardised.csv")

df_all_us <- rbindlist(list(df_all_us_2019,df_all_us_2020,df_all_us_2021,df_all_us_2022))

remove(list = setdiff(ls(), "df_all_us"))
source("/mnt/disks/pdisk/bgt_code_repo/iqr_outlier_removal.R")

df_all_us <- df_all_us %>% .[!grepl("careerbuilder", df_all_us$job_url)]

top_cities <- df_all_us %>%
  .[city != ""] %>%   .[as.yearmon(year_month) >= as.yearmon("January 2019")] %>%
  .[, .(.N), by = .(city, state)] %>%
  .[N > 150000] %>%
  .[order(desc(N))] %>%
  .[, city_state := paste0(city,", ",state)]
df_all_us <- df_all_us %>% .[, city_state := paste0(city,", ",state)] %>% .[city_state %in% top_cities$city_state]

ts_compare <- df_all_us %>%
  .[, .(.N, daily_mean = mean(wfh_wham, na.rm = T)), by = .(city, state, city_state, year_month, job_date)] %>%
  .[, period := ifelse(as.yearmon(year_month) < as.yearmon("March 2020"), 0,
                       ifelse(as.yearmon(year_month) %in% as.yearmon(c("March 2020", "April 2020", "May 2020")), 1, 2))] %>%
  group_by(city, state, city_state, period) %>%
  group_split(., .keep = T) %>%
  lapply(., function(m) {
    m <- setDT(m) %>%
      .[, rolling_monthly_trim_mean:=rollapply(data = .[,c(6,7)],
                                               width = 60,
                                               function(x) {
                                                 y <- x[, c(2)]
                                                 qnt <- quantile(y, probs=c(.25, .75), na.rm = T)
                                                 H <- 1.5 * IQR(y, na.rm = T)
                                                 w <- x[, c(1)]
                                                 y[y < (qnt[1] - H)] <- NA
                                                 y[y > (qnt[2] + H)] <- NA
                                                 w[y < (qnt[1] - H)] <- NA
                                                 w[y > (qnt[2] + H)] <- NA
                                                 sum(y*w, na.rm = T)/sum(w, na.rm = T)
                                               },
                                               partial = T, by.column = FALSE, align = "center", fill = NA)] %>%
      setDT(.)
  }) %>%
  rbindlist %>%
  .[, dev := abs(rolling_monthly_trim_mean - daily_mean)] %>%
  .[, med_dev := rollapply(data = dev, width = 60, function(x) {median(x, na.rm = T)}, partial = T, align = "center"), by = .(city, state, city_state, period)] %>%
  .[, trim := ifelse(dev > 1.5*med_dev, 1, 0)] %>%
  .[, monthly_mean := sum(daily_mean*N, na.rm = T)/sum(N, na.rm = T), by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean_pl := sum(daily_mean*N*(1-trim), na.rm = T)/sum(N*(1-trim), na.rm = T), by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean_pl := ifelse(period == 1, monthly_mean, monthly_mean_pl)] %>%
  .[, monthly_mean_nb := sum(Winsorize(daily_mean, probs = c(0, 0.9))*Winsorize(N, probs = c(0, 0.9)), na.rm = T)/sum(Winsorize(N, probs = c(0, 0.9)), na.rm = T), by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean_sd_ref := mean(daily_mean, trim = 0.1), by = .(city, state, city_state, year_month)] %>%
  .[, .( job_date = min(job_date),
         monthly_mean = monthly_mean[1],
         monthly_mean_pl = monthly_mean_pl[1],
         monthly_mean_nb = monthly_mean_nb[1],
         monthly_mean_sd_ref = monthly_mean_sd_ref[1]),
    by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean := 100*round(monthly_mean, 4)] %>%
  .[, monthly_mean_pl := 100*round(monthly_mean_pl, 4)] %>%
  .[, monthly_mean_nb := 100*round(monthly_mean_nb, 4)] %>%
  .[, monthly_mean_sd_ref := 100*round(monthly_mean_sd_ref, 4)] %>%
  .[, monthly_mean_sd := ifelse(monthly_mean_sd_ref<=4 & abs(monthly_mean - monthly_mean_sd_ref)>3, NA, monthly_mean)] %>%
  .[, monthly_mean_sd := ifelse(monthly_mean_sd_ref>4 & abs(log(monthly_mean) - log(monthly_mean_sd_ref))>0.1, NA, monthly_mean_sd)] %>%
  .[, monthly_mean_sd := na.approx(monthly_mean_sd, rule = 2, na.rm = F), by = .(city, state, city_state, year_month)] %>%
  .[job_date >= ymd("20190101")] %>%
  .[city != ""]

lapply(1:length(top_cities$city_state), function(i) {
  
  p = ggplot() +
    #geom_point(data = ts_compare[trim == 0 & city_state == top_cities$city_state[i]], aes(x = as.Date(job_date), size = N, y = daily_mean*100), colour = "black", alpha = 0.5) +
    #geom_point(data = ts_compare[trim == 1 & city_state == top_cities$city_state[i]], aes(x = as.Date(job_date), size = N, y = daily_mean*100), colour = "orange", alpha = 0.5) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean, colour = "Mean (raw)"), size = 2, alpha = 0.75) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_nb, colour = "Mean (NBF)"), size = 2, alpha = 0.75) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_sd, colour = "Mean (SDF)"), size = 2, alpha = 0.75) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_pl, colour = "Mean (PLF)"), size = 2, alpha = 0.75) +
    ylab("Share (%)") +
    xlab("Date") +
    labs(title = top_cities$city_state[i], subtitle = paste0("N = ", prettyNum(top_cities$N[i], big.mark = ","))) +
    scale_x_date(date_breaks = "3 months",
                 date_labels = '%Y-%m') +
    scale_y_continuous(labels = scales::number_format(accuracy = 1), breaks = seq(0, 100, 2)) +
    #coord_cartesian(ylim = c(0, 20)) +
    scale_color_manual(name = "Series:", values = c("Mean (raw)" = "darkorange", "Mean (PLF)" = "red", "Mean (NBF)" = "darkblue", "Mean (SDF)" = "black")) +
    theme(
      axis.title.x=element_blank(),
      legend.position="bottom",
      legend.title = element_blank(),
      axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
    guides(col = guide_legend(nrow = 2, reverse = F)) +
    theme(text = element_text(size=14, family="serif", colour = "black"),
          axis.text = element_text(size=13, family="serif", colour = "black"),
          axis.title = element_text(size=14, family="serif", colour = "black"),
          legend.text = element_text(size=13, family="serif", colour = "black"),
          panel.background = element_rect(fill = "white")) +
    guides(size = "none")
  
  p_egg <- set_panel_size(p = p,
                          width = unit(6, "in"),
                          height = unit(10, "in"))
  ggsave(p_egg, filename = paste0("./plots/validate4/us",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(top_cities$city_state[i]))),".png"), width = 8, height = 12)
  ggsave(p_egg, filename = paste0("./plots/validate4/us",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(top_cities$city_state[i]))),".pdf"), width = 8, height = 12)
})


# UK Daily
remove(list = ls())
source("/mnt/disks/pdisk/bgt_code_repo/iqr_outlier_removal.R")
df_all_uk <- fread(file = "./int_data/df_uk_standardised.csv")
colnames(df_all_uk)
df_all_uk <- df_all_uk %>% .[!grepl("jobisjob", df_all_uk$job_url)]

top_cities <- df_all_uk %>%
  .[city != ""] %>%   .[as.yearmon(year_month) >= as.yearmon("January 2019")] %>%
  .[, .(.N), by = .(city, state)] %>%
  .[N > 150000] %>%
  .[order(desc(N))] %>%
  .[, city_state := paste0(city,", ",state)]

df_all_uk <- df_all_uk %>% .[, city_state := paste0(city,", ",state)] %>% .[city_state %in% top_cities$city_state]

ts_compare <- df_all_uk %>%
  .[, .(.N, daily_mean = mean(wfh_wham, na.rm = T)), by = .(city, state, city_state, year_month, job_date)] %>%
  .[, period := ifelse(as.yearmon(year_month) < as.yearmon("March 2020"), 0,
                       ifelse(as.yearmon(year_month) %in% as.yearmon(c("March 2020", "April 2020", "May 2020")), 1, 2))] %>%
  group_by(city, state, city_state, period) %>%
  group_split(., .keep = T) %>%
  lapply(., function(m) {
    m <- setDT(m) %>%
      .[, rolling_monthly_trim_mean:=rollapply(data = .[,c(6,7)],
                                               width = 60,
                                               function(x) {
                                                 y <- x[, c(2)]
                                                 qnt <- quantile(y, probs=c(.25, .75), na.rm = T)
                                                 H <- 1.5 * IQR(y, na.rm = T)
                                                 w <- x[, c(1)]
                                                 y[y < (qnt[1] - H)] <- NA
                                                 y[y > (qnt[2] + H)] <- NA
                                                 w[y < (qnt[1] - H)] <- NA
                                                 w[y > (qnt[2] + H)] <- NA
                                                 sum(y*w, na.rm = T)/sum(w, na.rm = T)
                                               },
                                               partial = T, by.column = FALSE, align = "center", fill = NA)] %>%
      setDT(.)
  }) %>%
  rbindlist %>%
  .[, dev := abs(rolling_monthly_trim_mean - daily_mean)] %>%
  .[, med_dev := rollapply(data = dev, width = 60, function(x) {median(x, na.rm = T)}, partial = T, align = "center"), by = .(city, state, city_state, period)] %>%
  .[, trim := ifelse(dev > 1.5*med_dev, 1, 0)] %>%
  .[, monthly_mean := sum(daily_mean*N, na.rm = T)/sum(N, na.rm = T), by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean_pl := sum(daily_mean*N*(1-trim), na.rm = T)/sum(N*(1-trim), na.rm = T), by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean_pl := ifelse(period == 1, monthly_mean, monthly_mean_pl)] %>%
  .[, monthly_mean_nb := sum(Winsorize(daily_mean, probs = c(0, 0.9))*Winsorize(N, probs = c(0, 0.9)), na.rm = T)/sum(Winsorize(N, probs = c(0, 0.9)), na.rm = T), by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean_sd_ref := mean(daily_mean, trim = 0.1), by = .(city, state, city_state, year_month)] %>%
  .[, .( job_date = min(job_date),
         monthly_mean = monthly_mean[1],
         monthly_mean_pl = monthly_mean_pl[1],
         monthly_mean_nb = monthly_mean_nb[1],
         monthly_mean_sd_ref = monthly_mean_sd_ref[1]),
    by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean := 100*round(monthly_mean, 4)] %>%
  .[, monthly_mean_pl := 100*round(monthly_mean_pl, 4)] %>%
  .[, monthly_mean_nb := 100*round(monthly_mean_nb, 4)] %>%
  .[, monthly_mean_sd_ref := 100*round(monthly_mean_sd_ref, 4)] %>%
  .[, monthly_mean_sd := ifelse(monthly_mean_sd_ref<=4 & abs(monthly_mean - monthly_mean_sd_ref)>3, NA, monthly_mean)] %>%
  .[, monthly_mean_sd := ifelse(monthly_mean_sd_ref>4 & abs(log(monthly_mean) - log(monthly_mean_sd_ref))>0.1, NA, monthly_mean_sd)] %>%
  .[, monthly_mean_sd := na.approx(monthly_mean_sd, rule = 2, na.rm = F), by = .(city, state, city_state, year_month)] %>%
  .[job_date >= ymd("20190101")] %>%
  .[city != ""]

lapply(1:length(top_cities$city_state), function(i) {
  
  p = ggplot() +
    #geom_point(data = ts_compare[trim == 0 & city_state == top_cities$city_state[i]], aes(x = as.Date(job_date), size = N, y = daily_mean*100), colour = "black", alpha = 0.5) +
    #geom_point(data = ts_compare[trim == 1 & city_state == top_cities$city_state[i]], aes(x = as.Date(job_date), size = N, y = daily_mean*100), colour = "orange", alpha = 0.5) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean, colour = "Mean (raw)"), size = 2, alpha = 0.75) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_nb, colour = "Mean (NBF)"), size = 2, alpha = 0.75) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_sd, colour = "Mean (SDF)"), size = 2, alpha = 0.75) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_pl, colour = "Mean (PLF)"), size = 2, alpha = 0.75) +
    ylab("Share (%)") +
    xlab("Date") +
    labs(title = top_cities$city_state[i], subtitle = paste0("N = ", prettyNum(top_cities$N[i], big.mark = ","))) +
    scale_x_date(date_breaks = "3 months",
                 date_labels = '%Y-%m') +
    scale_y_continuous(labels = scales::number_format(accuracy = 1), breaks = seq(0, 100, 2)) +
    #coord_cartesian(ylim = c(0, 20)) +
    scale_color_manual(name = "Series:", values = c("Mean (raw)" = "darkorange", "Mean (PLF)" = "red", "Mean (NBF)" = "darkblue", "Mean (SDF)" = "black")) +
    theme(
      axis.title.x=element_blank(),
      legend.position="bottom",
      legend.title = element_blank(),
      axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
    guides(col = guide_legend(nrow = 2, reverse = F)) +
    theme(text = element_text(size=14, family="serif", colour = "black"),
          axis.text = element_text(size=13, family="serif", colour = "black"),
          axis.title = element_text(size=14, family="serif", colour = "black"),
          legend.text = element_text(size=13, family="serif", colour = "black"),
          panel.background = element_rect(fill = "white")) +
    guides(size = "none")
  
  p_egg <- set_panel_size(p = p,
                          width = unit(6, "in"),
                          height = unit(10, "in"))
  ggsave(p_egg, filename = paste0("./plots/validate4/uk",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(top_cities$city_state[i]))),".png"), width = 8, height = 12)
  ggsave(p_egg, filename = paste0("./plots/validate4/uk",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(top_cities$city_state[i]))),".pdf"), width = 8, height = 12)
})


# Can Daily
remove(list = ls())
source("/mnt/disks/pdisk/bgt_code_repo/iqr_outlier_removal.R")
df_all_can <- fread(file = "./int_data/df_can_standardised.csv")
df_all_can <- df_all_can %>% .[!grepl("workopolis", df_all_can$job_url)]
df_all_can <- df_all_can %>% .[!grepl("careerjet", df_all_can$job_url)]

top_cities <- df_all_can %>%
  .[city != ""] %>%   .[as.yearmon(year_month) >= as.yearmon("January 2019")] %>%
  .[, .(.N), by = .(city, state)] %>%
  .[N > 150000] %>%
  .[order(desc(N))] %>%
  .[, city_state := paste0(city,", ",state)]

df_all_can <- df_all_can %>% .[, city_state := paste0(city,", ",state)] %>% .[city_state %in% top_cities$city_state]

ts_compare <- df_all_can %>%
  .[, .(.N, daily_mean = mean(wfh_wham, na.rm = T)), by = .(city, state, city_state, year_month, job_date)] %>%
  .[, period := ifelse(as.yearmon(year_month) < as.yearmon("March 2020"), 0,
                       ifelse(as.yearmon(year_month) %in% as.yearmon(c("March 2020", "April 2020", "May 2020")), 1, 2))] %>%
  group_by(city, state, city_state, period) %>%
  group_split(., .keep = T) %>%
  lapply(., function(m) {
    m <- setDT(m) %>%
      .[, rolling_monthly_trim_mean:=rollapply(data = .[,c(6,7)],
                                               width = 60,
                                               function(x) {
                                                 y <- x[, c(2)]
                                                 qnt <- quantile(y, probs=c(.25, .75), na.rm = T)
                                                 H <- 1.5 * IQR(y, na.rm = T)
                                                 w <- x[, c(1)]
                                                 y[y < (qnt[1] - H)] <- NA
                                                 y[y > (qnt[2] + H)] <- NA
                                                 w[y < (qnt[1] - H)] <- NA
                                                 w[y > (qnt[2] + H)] <- NA
                                                 sum(y*w, na.rm = T)/sum(w, na.rm = T)
                                               },
                                               partial = T, by.column = FALSE, align = "center", fill = NA)] %>%
      setDT(.)
  }) %>%
  rbindlist %>%
  .[, dev := abs(rolling_monthly_trim_mean - daily_mean)] %>%
  .[, med_dev := rollapply(data = dev, width = 60, function(x) {median(x, na.rm = T)}, partial = T, align = "center"), by = .(city, state, city_state, period)] %>%
  .[, trim := ifelse(dev > 1.5*med_dev, 1, 0)] %>%
  .[, monthly_mean := sum(daily_mean*N, na.rm = T)/sum(N, na.rm = T), by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean_pl := sum(daily_mean*N*(1-trim), na.rm = T)/sum(N*(1-trim), na.rm = T), by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean_pl := ifelse(period == 1, monthly_mean, monthly_mean_pl)] %>%
  .[, monthly_mean_nb := sum(Winsorize(daily_mean, probs = c(0, 0.9))*Winsorize(N, probs = c(0, 0.9)), na.rm = T)/sum(Winsorize(N, probs = c(0, 0.9)), na.rm = T), by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean_sd_ref := mean(daily_mean, trim = 0.1), by = .(city, state, city_state, year_month)] %>%
  .[, .( job_date = min(job_date),
         monthly_mean = monthly_mean[1],
         monthly_mean_pl = monthly_mean_pl[1],
         monthly_mean_nb = monthly_mean_nb[1],
         monthly_mean_sd_ref = monthly_mean_sd_ref[1]),
    by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean := 100*round(monthly_mean, 4)] %>%
  .[, monthly_mean_pl := 100*round(monthly_mean_pl, 4)] %>%
  .[, monthly_mean_nb := 100*round(monthly_mean_nb, 4)] %>%
  .[, monthly_mean_sd_ref := 100*round(monthly_mean_sd_ref, 4)] %>%
  .[, monthly_mean_sd := ifelse(monthly_mean_sd_ref<=4 & abs(monthly_mean - monthly_mean_sd_ref)>3, NA, monthly_mean)] %>%
  .[, monthly_mean_sd := ifelse(monthly_mean_sd_ref>4 & abs(log(monthly_mean) - log(monthly_mean_sd_ref))>0.1, NA, monthly_mean_sd)] %>%
  .[, monthly_mean_sd := na.approx(monthly_mean_sd, rule = 2, na.rm = F), by = .(city, state, city_state, year_month)] %>%
  .[job_date >= ymd("20190101")] %>%
  .[city != ""]

lapply(1:length(top_cities$city_state), function(i) {
  
  p = ggplot() +
    #geom_point(data = ts_compare[trim == 0 & city_state == top_cities$city_state[i]], aes(x = as.Date(job_date), size = N, y = daily_mean*100), colour = "black", alpha = 0.5) +
    #geom_point(data = ts_compare[trim == 1 & city_state == top_cities$city_state[i]], aes(x = as.Date(job_date), size = N, y = daily_mean*100), colour = "orange", alpha = 0.5) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean, colour = "Mean (raw)"), size = 2, alpha = 0.75) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_nb, colour = "Mean (NBF)"), size = 2, alpha = 0.75) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_sd, colour = "Mean (SDF)"), size = 2, alpha = 0.75) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_pl, colour = "Mean (PLF)"), size = 2, alpha = 0.75) +
    ylab("Share (%)") +
    xlab("Date") +
    labs(title = top_cities$city_state[i], subtitle = paste0("N = ", prettyNum(top_cities$N[i], big.mark = ","))) +
    scale_x_date(date_breaks = "3 months",
                 date_labels = '%Y-%m') +
    scale_y_continuous(labels = scales::number_format(accuracy = 1), breaks = seq(0, 100, 2)) +
    #coord_cartesian(ylim = c(0, 20)) +
    scale_color_manual(name = "Series:", values = c("Mean (raw)" = "darkorange", "Mean (PLF)" = "red", "Mean (NBF)" = "darkblue", "Mean (SDF)" = "black")) +
    theme(
      axis.title.x=element_blank(),
      legend.position="bottom",
      legend.title = element_blank(),
      axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
    guides(col = guide_legend(nrow = 2, reverse = F)) +
    theme(text = element_text(size=14, family="serif", colour = "black"),
          axis.text = element_text(size=13, family="serif", colour = "black"),
          axis.title = element_text(size=14, family="serif", colour = "black"),
          legend.text = element_text(size=13, family="serif", colour = "black"),
          panel.background = element_rect(fill = "white")) +
    guides(size = "none")
  
  p_egg <- set_panel_size(p = p,
                          width = unit(6, "in"),
                          height = unit(10, "in"))
  ggsave(p_egg, filename = paste0("./plots/validate4/can",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(top_cities$city_state[i]))),".png"), width = 8, height = 12)
  ggsave(p_egg, filename = paste0("./plots/validate4/can",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(top_cities$city_state[i]))),".pdf"), width = 8, height = 12)
})


