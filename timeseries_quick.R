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

setwd("/mnt/disks/pdisk/bg_combined/aux_data")

# Set SD filter paramters
trim_par <- 0.1 # the threshold for trimming daily mean values from the trimmed mean
assignment <- 4 # assigns log-threshold for months with <, assigns level-threshold for months >=
level_threshold <- 3 # absolute deviation in level of raw mean and trimmed mean
log_threshold <- 0.1 # absolute deviation in level of log(raw mean) and log(trimmed mean)

list.files("./city_level_daily/")

# NZ Daily
ts_compare <- fread("./city_level_daily/nz_daily.csv", integer64 = "numeric") %>%
  group_by(city, state, city_state) %>%
  group_split(., .keep = T) %>%
  lapply(., function(m) {
    m <- setDT(m) %>%
      .[, day_time := as.numeric(job_date)]
    
    m <- m %>%
      .[, daily_hampel_thresh:=rollapply(data = daily_mean,
                                         width = 60,
                                         function(x) {
                                           med <- median(x)
                                           mad <- median(abs(x - med))
                                           lb <- med - 1.5*mad
                                           ub <- med + 1.5*mad
                                           return(paste0(lb,"#",ub,"#",med))
                                         },
                                         partial = T, by.column = FALSE, align = "center", fill = NA)] %>%
      setDT(.) %>%
      .[, c("hampel_lb", "hampel_ub", "med") := tstrsplit(daily_hampel_thresh, "#", 3)] %>%
      select(-daily_hampel_thresh) %>%
      .[, hampel_lb := as.numeric(hampel_lb)] %>%
      .[, hampel_ub := as.numeric(hampel_ub)] %>%
      .[, med := as.numeric(med)] %>%
      .[, day_time := as.numeric(job_date)]
    return(m)
  }) %>%
  rbindlist %>%
  .[, hampel_bind := ifelse(daily_mean <= hampel_ub & daily_mean >= hampel_lb, "no", "yes")] %>%
  .[, hampel_daily_mean := ifelse((daily_mean <= hampel_ub & daily_mean >= hampel_lb) | med < 0.001, daily_mean, med)] %>%
  .[, .(monthly_mean = 100*sum(daily_mean*N, na.rm = T)/sum(N, na.rm = T),
        monthly_mean_hampel = 100*sum(hampel_daily_mean*N, na.rm = T)/sum(N, na.rm = T),
        monthly_mean_trim = mean(daily_mean, trim = trim_par, na.rm = T),
        job_date = min(job_date)),
    by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean_sd := ifelse(monthly_mean_trim<=assignment & abs(monthly_mean - monthly_mean_trim)>level_threshold, NA, monthly_mean)] %>%
  .[, monthly_mean_sd := ifelse(monthly_mean_trim>assignment & abs(log(monthly_mean) - log(monthly_mean_trim))>level_threshold, NA, monthly_mean_sd)] %>%
  .[, monthly_mean_sd := na.approx(monthly_mean_sd, rule = 2, na.rm = F), by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean_hampel := ifelse(as.yearmon(year_month) %in% as.yearmon(c("March 2020", "April 2020", "May 2020", "June 2020")), monthly_mean, monthly_mean_hampel)] %>%
  .[, daily_time := as.numeric(job_date)] %>%
  group_by(city, state, city_state) %>%
  group_split(., .keep = T) %>%
  lapply(., function(m) {
    m <- setDT(m) %>%
      .[, monthly_mean_loess:=predict(loess(
        formula = monthly_mean_hampel ~ daily_time, data = ., span = 0.1, degree = 2
      ), newdata = .)] 
  }) %>%
  rbindlist(.) %>%
  .[, monthly_mean_loess := ifelse(as.yearmon(year_month) %in% as.yearmon(c("March 2020", "April 2020", "May 2020", "June 2020")), monthly_mean, monthly_mean_loess)] %>%
  .[as.yearmon(year_month) >= as.yearmon("Jan 2018")]

city_list <- unique(ts_compare$city_state)

lapply(1:length(city_list), function(i) {
  
  p = ggplot() +
    geom_point(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_hampel, colour = "Mean (Hampel Filter)"), size = 4, alpha = 1, shape = 15) +
    geom_point(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean, colour = "Mean (raw)"), size = 4, alpha = 1, shape = 17) +
    geom_point(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_sd, colour = "Mean (SD)"), size = 4, alpha = 1, shape = 18) +
    geom_line(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_hampel, colour = "Mean (Hampel Filter)"), size = 2, alpha = 1, linetype = "solid") +
    geom_line(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean, colour = "Mean (raw)"), size = 2, alpha = 1) +
    geom_line(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_sd, colour = "Mean (SD)"), size = 2, alpha = 1) +
    ylab("Share (%)") +
    xlab("Date") +
    labs(title = city_list[i]) +
    scale_x_date(date_breaks = "3 months",
                 date_labels = '%Y-%m') +
    scale_y_continuous(labels = scales::number_format(accuracy = 1), breaks = seq(0, 100, 2)) +
    #coord_cartesian(ylim = c(0, 20)) +
    scale_color_manual(name = "Series:", values = c("Mean (raw)" = "#E69F00", "Mean (Hampel Filter)" = "#009E73", "Mean (SD)" = "#CC79A7")) +
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
                          height = unit(4, "in"))
  ggsave(p_egg, filename = paste0("./plots/nz",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(city_list[i]))),".png"), width = 8, height = 6)
  ggsave(p_egg, filename = paste0("./plots/nz",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(city_list[i]))),".pdf"), width = 8, height = 6)
})

remove(list = setdiff(ls(), c("trim_par","assignment","level_threshold","log_threshold")))

# Australia Daily
ts_compare <- fread("./city_level_daily/aus_daily.csv", integer64 = "numeric") %>%
  group_by(city, state, city_state) %>%
  group_split(., .keep = T) %>%
  lapply(., function(m) {
    m <- setDT(m) %>%
      .[, day_time := as.numeric(job_date)]
    
    m <- m %>%
      .[, daily_hampel_thresh:=rollapply(data = daily_mean,
                                         width = 60,
                                         function(x) {
                                           med <- median(x)
                                           mad <- median(abs(x - med))
                                           lb <- med - 1.5*mad
                                           ub <- med + 1.5*mad
                                           return(paste0(lb,"#",ub,"#",med))
                                         },
                                         partial = T, by.column = FALSE, align = "center", fill = NA)] %>%
      setDT(.) %>%
      .[, c("hampel_lb", "hampel_ub", "med") := tstrsplit(daily_hampel_thresh, "#", 3)] %>%
      select(-daily_hampel_thresh) %>%
      .[, hampel_lb := as.numeric(hampel_lb)] %>%
      .[, hampel_ub := as.numeric(hampel_ub)] %>%
      .[, med := as.numeric(med)] %>%
      .[, day_time := as.numeric(job_date)]
    return(m)
  }) %>%
  rbindlist %>%
  .[, hampel_bind := ifelse(daily_mean <= hampel_ub & daily_mean >= hampel_lb, "no", "yes")] %>%
  .[, hampel_daily_mean := ifelse((daily_mean <= hampel_ub & daily_mean >= hampel_lb) | med < 0.001, daily_mean, med)] %>%
  .[, .(monthly_mean = 100*sum(daily_mean*N, na.rm = T)/sum(N, na.rm = T),
        monthly_mean_hampel = 100*sum(hampel_daily_mean*N, na.rm = T)/sum(N, na.rm = T),
        monthly_mean_trim = mean(daily_mean, trim = trim_par, na.rm = T),
        job_date = min(job_date)),
    by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean_sd := ifelse(monthly_mean_trim<=assignment & abs(monthly_mean - monthly_mean_trim)>level_threshold, NA, monthly_mean)] %>%
  .[, monthly_mean_sd := ifelse(monthly_mean_trim>assignment & abs(log(monthly_mean) - log(monthly_mean_trim))>level_threshold, NA, monthly_mean_sd)] %>%
  .[, monthly_mean_sd := na.approx(monthly_mean_sd, rule = 2, na.rm = F), by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean_hampel := ifelse(as.yearmon(year_month) %in% as.yearmon(c("March 2020", "April 2020", "May 2020", "June 2020")), monthly_mean, monthly_mean_hampel)] %>%
  .[, daily_time := as.numeric(job_date)] %>%
  group_by(city, state, city_state) %>%
  group_split(., .keep = T) %>%
  lapply(., function(m) {
    m <- setDT(m) %>%
      .[, monthly_mean_loess:=predict(loess(
        formula = monthly_mean_hampel ~ daily_time, data = ., span = 0.1, degree = 2
      ), newdata = .)] 
  }) %>%
  rbindlist(.) %>%
  .[, monthly_mean_loess := ifelse(as.yearmon(year_month) %in% as.yearmon(c("March 2020", "April 2020", "May 2020", "June 2020")), monthly_mean, monthly_mean_loess)] %>%
  .[as.yearmon(year_month) >= as.yearmon("Jan 2018")]

city_list <- unique(ts_compare$city_state)

lapply(1:length(city_list), function(i) {
  
  p = ggplot() +
    geom_point(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_hampel, colour = "Mean (Hampel Filter)"), size = 4, alpha = 1, shape = 15) +
    geom_point(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean, colour = "Mean (raw)"), size = 4, alpha = 1, shape = 17) +
    geom_point(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_sd, colour = "Mean (SD)"), size = 4, alpha = 1, shape = 18) +
    geom_line(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_hampel, colour = "Mean (Hampel Filter)"), size = 2, alpha = 1, linetype = "solid") +
    geom_line(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean, colour = "Mean (raw)"), size = 2, alpha = 1) +
    geom_line(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_sd, colour = "Mean (SD)"), size = 2, alpha = 1) +
    ylab("Share (%)") +
    xlab("Date") +
    labs(title = city_list[i]) +
    scale_x_date(date_breaks = "3 months",
                 date_labels = '%Y-%m') +
    scale_y_continuous(labels = scales::number_format(accuracy = 1), breaks = seq(0, 100, 2)) +
    #coord_cartesian(ylim = c(0, 20)) +
    scale_color_manual(name = "Series:", values = c("Mean (raw)" = "#E69F00", "Mean (Hampel Filter)" = "#009E73", "Mean (SD)" = "#CC79A7")) +
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
                          height = unit(4, "in"))
  ggsave(p_egg, filename = paste0("./plots/aus",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(city_list[i]))),".png"), width = 8, height = 6)
  ggsave(p_egg, filename = paste0("./plots/aus",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(city_list[i]))),".pdf"), width = 8, height = 6)
})

remove(list = setdiff(ls(), c("trim_par","assignment","level_threshold","log_threshold")))

# USA Daily
ts_compare <- fread("./city_level_daily/usa_daily.csv", integer64 = "numeric") %>%
  group_by(city, state, city_state) %>%
  group_split(., .keep = T) %>%
  lapply(., function(m) {
    m <- setDT(m) %>%
      .[, day_time := as.numeric(job_date)]
    
    m <- m %>%
      .[, daily_hampel_thresh:=rollapply(data = daily_mean,
                                         width = 60,
                                         function(x) {
                                           med <- median(x)
                                           mad <- median(abs(x - med))
                                           lb <- med - 1.5*mad
                                           ub <- med + 1.5*mad
                                           return(paste0(lb,"#",ub,"#",med))
                                         },
                                         partial = T, by.column = FALSE, align = "center", fill = NA)] %>%
      setDT(.) %>%
      .[, c("hampel_lb", "hampel_ub", "med") := tstrsplit(daily_hampel_thresh, "#", 3)] %>%
      select(-daily_hampel_thresh) %>%
      .[, hampel_lb := as.numeric(hampel_lb)] %>%
      .[, hampel_ub := as.numeric(hampel_ub)] %>%
      .[, med := as.numeric(med)] %>%
      .[, day_time := as.numeric(job_date)]
    return(m)
  }) %>%
  rbindlist %>%
  .[, hampel_bind := ifelse(daily_mean <= hampel_ub & daily_mean >= hampel_lb, "no", "yes")] %>%
  .[, hampel_daily_mean := ifelse((daily_mean <= hampel_ub & daily_mean >= hampel_lb) | med < 0.001, daily_mean, med)] %>%
  .[, .(monthly_mean = 100*sum(daily_mean*N, na.rm = T)/sum(N, na.rm = T),
        monthly_mean_hampel = 100*sum(hampel_daily_mean*N, na.rm = T)/sum(N, na.rm = T),
        monthly_mean_trim = mean(daily_mean, trim = trim_par, na.rm = T),
        job_date = min(job_date)),
    by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean_sd := ifelse(monthly_mean_trim<=assignment & abs(monthly_mean - monthly_mean_trim)>level_threshold, NA, monthly_mean)] %>%
  .[, monthly_mean_sd := ifelse(monthly_mean_trim>assignment & abs(log(monthly_mean) - log(monthly_mean_trim))>level_threshold, NA, monthly_mean_sd)] %>%
  .[, monthly_mean_sd := na.approx(monthly_mean_sd, rule = 2, na.rm = F), by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean_hampel := ifelse(as.yearmon(year_month) %in% as.yearmon(c("March 2020", "April 2020", "May 2020", "June 2020")), monthly_mean, monthly_mean_hampel)] %>%
  .[, daily_time := as.numeric(job_date)] %>%
  group_by(city, state, city_state) %>%
  group_split(., .keep = T) %>%
  lapply(., function(m) {
    m <- setDT(m) %>%
      .[, monthly_mean_loess:=predict(loess(
        formula = monthly_mean_hampel ~ daily_time, data = ., span = 0.1, degree = 2
      ), newdata = .)] 
  }) %>%
  rbindlist(.) %>%
  .[, monthly_mean_loess := ifelse(as.yearmon(year_month) %in% as.yearmon(c("March 2020", "April 2020", "May 2020", "June 2020")), monthly_mean, monthly_mean_loess)] %>%
  .[as.yearmon(year_month) >= as.yearmon("Jan 2018")]

city_list <- unique(ts_compare$city_state)

lapply(1:length(city_list), function(i) {
  
  p = ggplot() +
    geom_point(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_hampel, colour = "Mean (Hampel Filter)"), size = 4, alpha = 1, shape = 15) +
    geom_point(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean, colour = "Mean (raw)"), size = 4, alpha = 1, shape = 17) +
    geom_point(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_sd, colour = "Mean (SD)"), size = 4, alpha = 1, shape = 18) +
    geom_line(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_hampel, colour = "Mean (Hampel Filter)"), size = 2, alpha = 1, linetype = "solid") +
    geom_line(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean, colour = "Mean (raw)"), size = 2, alpha = 1) +
    geom_line(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_sd, colour = "Mean (SD)"), size = 2, alpha = 1) +
    ylab("Share (%)") +
    xlab("Date") +
    labs(title = city_list[i]) +
    scale_x_date(date_breaks = "3 months",
                 date_labels = '%Y-%m') +
    scale_y_continuous(labels = scales::number_format(accuracy = 1), breaks = seq(0, 100, 2)) +
    #coord_cartesian(ylim = c(0, 20)) +
    scale_color_manual(name = "Series:", values = c("Mean (raw)" = "#E69F00", "Mean (Hampel Filter)" = "#009E73", "Mean (SD)" = "#CC79A7")) +
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
                          height = unit(4, "in"))
  ggsave(p_egg, filename = paste0("./plots/us",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(city_list[i]))),".png"), width = 8, height = 6)
  ggsave(p_egg, filename = paste0("./plots/us",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(city_list[i]))),".pdf"), width = 8, height = 6)
})

remove(list = setdiff(ls(), c("trim_par","assignment","level_threshold","log_threshold")))

# UK Daily
ts_compare <- fread("./city_level_daily/uk_daily.csv", integer64 = "numeric") %>%
  group_by(city, state, city_state) %>%
  group_split(., .keep = T) %>%
  lapply(., function(m) {
    m <- setDT(m) %>%
      .[, day_time := as.numeric(job_date)]
    
    m <- m %>%
      .[, daily_hampel_thresh:=rollapply(data = daily_mean,
                                         width = 60,
                                         function(x) {
                                           med <- median(x)
                                           mad <- median(abs(x - med))
                                           lb <- med - 1.5*mad
                                           ub <- med + 1.5*mad
                                           return(paste0(lb,"#",ub,"#",med))
                                         },
                                         partial = T, by.column = FALSE, align = "center", fill = NA)] %>%
      setDT(.) %>%
      .[, c("hampel_lb", "hampel_ub", "med") := tstrsplit(daily_hampel_thresh, "#", 3)] %>%
      select(-daily_hampel_thresh) %>%
      .[, hampel_lb := as.numeric(hampel_lb)] %>%
      .[, hampel_ub := as.numeric(hampel_ub)] %>%
      .[, med := as.numeric(med)] %>%
      .[, day_time := as.numeric(job_date)]
    return(m)
  }) %>%
  rbindlist %>%
  .[, hampel_bind := ifelse(daily_mean <= hampel_ub & daily_mean >= hampel_lb, "no", "yes")] %>%
  .[, hampel_daily_mean := ifelse((daily_mean <= hampel_ub & daily_mean >= hampel_lb) | med < 0.001, daily_mean, med)] %>%
  .[, .(monthly_mean = 100*sum(daily_mean*N, na.rm = T)/sum(N, na.rm = T),
        monthly_mean_hampel = 100*sum(hampel_daily_mean*N, na.rm = T)/sum(N, na.rm = T),
        monthly_mean_trim = mean(daily_mean, trim = trim_par, na.rm = T),
        job_date = min(job_date)),
    by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean_sd := ifelse(monthly_mean_trim<=assignment & abs(monthly_mean - monthly_mean_trim)>level_threshold, NA, monthly_mean)] %>%
  .[, monthly_mean_sd := ifelse(monthly_mean_trim>assignment & abs(log(monthly_mean) - log(monthly_mean_trim))>level_threshold, NA, monthly_mean_sd)] %>%
  .[, monthly_mean_sd := na.approx(monthly_mean_sd, rule = 2, na.rm = F), by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean_hampel := ifelse(as.yearmon(year_month) %in% as.yearmon(c("March 2020", "April 2020", "May 2020", "June 2020")), monthly_mean, monthly_mean_hampel)] %>%
  .[, daily_time := as.numeric(job_date)] %>%
  group_by(city, state, city_state) %>%
  group_split(., .keep = T) %>%
  lapply(., function(m) {
    m <- setDT(m) %>%
      .[, monthly_mean_loess:=predict(loess(
        formula = monthly_mean_hampel ~ daily_time, data = ., span = 0.1, degree = 2
      ), newdata = .)] 
  }) %>%
  rbindlist(.) %>%
  .[, monthly_mean_loess := ifelse(as.yearmon(year_month) %in% as.yearmon(c("March 2020", "April 2020", "May 2020", "June 2020")), monthly_mean, monthly_mean_loess)] %>%
  .[as.yearmon(year_month) >= as.yearmon("Jan 2018")]

city_list <- unique(ts_compare$city_state)

lapply(1:length(city_list), function(i) {
  
  p = ggplot() +
    geom_point(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_hampel, colour = "Mean (Hampel Filter)"), size = 4, alpha = 1, shape = 15) +
    geom_point(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean, colour = "Mean (raw)"), size = 4, alpha = 1, shape = 17) +
    geom_point(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_sd, colour = "Mean (SD)"), size = 4, alpha = 1, shape = 18) +
    geom_line(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_hampel, colour = "Mean (Hampel Filter)"), size = 2, alpha = 1, linetype = "solid") +
    geom_line(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean, colour = "Mean (raw)"), size = 2, alpha = 1) +
    geom_line(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_sd, colour = "Mean (SD)"), size = 2, alpha = 1) +
    ylab("Share (%)") +
    xlab("Date") +
    labs(title = city_list[i]) +
    scale_x_date(date_breaks = "3 months",
                 date_labels = '%Y-%m') +
    scale_y_continuous(labels = scales::number_format(accuracy = 1), breaks = seq(0, 100, 2)) +
    #coord_cartesian(ylim = c(0, 20)) +
    scale_color_manual(name = "Series:", values = c("Mean (raw)" = "#E69F00", "Mean (Hampel Filter)" = "#009E73", "Mean (SD)" = "#CC79A7")) +
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
                          height = unit(4, "in"))
  ggsave(p_egg, filename = paste0("./plots/uk",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(city_list[i]))),".png"), width = 8, height = 6)
  ggsave(p_egg, filename = paste0("./plots/uk",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(city_list[i]))),".pdf"), width = 8, height = 6)
})

remove(list = setdiff(ls(), c("trim_par","assignment","level_threshold","log_threshold")))

# Can Daily
ts_compare <- fread("./city_level_daily/can_daily.csv", integer64 = "numeric") %>%
  group_by(city, state, city_state) %>%
  group_split(., .keep = T) %>%
  lapply(., function(m) {
    m <- setDT(m) %>%
      .[, day_time := as.numeric(job_date)]
    
    m <- m %>%
      .[, daily_hampel_thresh:=rollapply(data = daily_mean,
                                         width = 60,
                                         function(x) {
                                           med <- median(x)
                                           mad <- median(abs(x - med))
                                           lb <- med - 1.5*mad
                                           ub <- med + 1.5*mad
                                           return(paste0(lb,"#",ub,"#",med))
                                         },
                                         partial = T, by.column = FALSE, align = "center", fill = NA)] %>%
      setDT(.) %>%
      .[, c("hampel_lb", "hampel_ub", "med") := tstrsplit(daily_hampel_thresh, "#", 3)] %>%
      select(-daily_hampel_thresh) %>%
      .[, hampel_lb := as.numeric(hampel_lb)] %>%
      .[, hampel_ub := as.numeric(hampel_ub)] %>%
      .[, med := as.numeric(med)] %>%
      .[, day_time := as.numeric(job_date)]
    return(m)
  }) %>%
  rbindlist %>%
  .[, hampel_bind := ifelse(daily_mean <= hampel_ub & daily_mean >= hampel_lb, "no", "yes")] %>%
  .[, hampel_daily_mean := ifelse((daily_mean <= hampel_ub & daily_mean >= hampel_lb) | med < 0.001, daily_mean, med)] %>%
  .[, .(monthly_mean = 100*sum(daily_mean*N, na.rm = T)/sum(N, na.rm = T),
        monthly_mean_hampel = 100*sum(hampel_daily_mean*N, na.rm = T)/sum(N, na.rm = T),
        monthly_mean_trim = mean(daily_mean, trim = trim_par, na.rm = T),
        job_date = min(job_date)),
    by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean_sd := ifelse(monthly_mean_trim<=assignment & abs(monthly_mean - monthly_mean_trim)>level_threshold, NA, monthly_mean)] %>%
  .[, monthly_mean_sd := ifelse(monthly_mean_trim>assignment & abs(log(monthly_mean) - log(monthly_mean_trim))>level_threshold, NA, monthly_mean_sd)] %>%
  .[, monthly_mean_sd := na.approx(monthly_mean_sd, rule = 2, na.rm = F), by = .(city, state, city_state, year_month)] %>%
  .[, monthly_mean_hampel := ifelse(as.yearmon(year_month) %in% as.yearmon(c("March 2020", "April 2020", "May 2020", "June 2020")), monthly_mean, monthly_mean_hampel)] %>%
  .[, daily_time := as.numeric(job_date)] %>%
  group_by(city, state, city_state) %>%
  group_split(., .keep = T) %>%
  lapply(., function(m) {
    m <- setDT(m) %>%
      .[, monthly_mean_loess:=predict(loess(
        formula = monthly_mean_hampel ~ daily_time, data = ., span = 0.1, degree = 2
      ), newdata = .)] 
  }) %>%
  rbindlist(.) %>%
  .[, monthly_mean_loess := ifelse(as.yearmon(year_month) %in% as.yearmon(c("March 2020", "April 2020", "May 2020", "June 2020")), monthly_mean, monthly_mean_loess)] %>%
  .[as.yearmon(year_month) >= as.yearmon("Jan 2018")]

city_list <- unique(ts_compare$city_state)

lapply(1:length(city_list), function(i) {
  
  p = ggplot() +
    geom_point(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_hampel, colour = "Mean (Hampel Filter)"), size = 4, alpha = 1, shape = 15) +
    geom_point(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean, colour = "Mean (raw)"), size = 4, alpha = 1, shape = 17) +
    geom_point(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_sd, colour = "Mean (SD)"), size = 4, alpha = 1, shape = 18) +
    geom_line(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_hampel, colour = "Mean (Hampel Filter)"), size = 2, alpha = 1, linetype = "solid") +
    geom_line(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean, colour = "Mean (raw)"), size = 2, alpha = 1) +
    geom_line(data = ts_compare[city_state == city_list[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_sd, colour = "Mean (SD)"), size = 2, alpha = 1) +
    ylab("Share (%)") +
    xlab("Date") +
    labs(title = city_list[i]) +
    scale_x_date(date_breaks = "3 months",
                 date_labels = '%Y-%m') +
    scale_y_continuous(labels = scales::number_format(accuracy = 1), breaks = seq(0, 100, 2)) +
    #coord_cartesian(ylim = c(0, 20)) +
    scale_color_manual(name = "Series:", values = c("Mean (raw)" = "#E69F00", "Mean (Hampel Filter)" = "#009E73", "Mean (SD)" = "#CC79A7")) +
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
                          height = unit(4, "in"))
  ggsave(p_egg, filename = paste0("./plots/can",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(city_list[i]))),".png"), width = 8, height = 6)
  ggsave(p_egg, filename = paste0("./plots/can",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(city_list[i]))),".pdf"), width = 8, height = 6)
})
