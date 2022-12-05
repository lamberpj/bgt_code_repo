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
  .[city != ""] %>% 
  .[as.yearmon(year_month) >= as.yearmon("Jan 2018")] %>%
  .[, .(.N), by = .(city, state)] %>%
  .[N > 150000] %>%
  .[order(desc(N))] %>%
  .[, city_state := paste0(city,", ",state)]

df_all_nz <- df_all_nz %>% .[, city_state := paste0(city,", ",state)] %>% .[city_state %in% top_cities$city_state]

colnames(df_all_nz)

ts_compare <- df_all_nz %>%
  .[city != ""] %>%
  .[, .(.N, daily_mean = mean(wfh_wham, na.rm = T)), by = .(city, state, city_state, year_month, job_date)] %>%
  # .[, period := ifelse(as.yearmon(year_month) < as.yearmon("March 2020"), 0,
  #                      ifelse(as.yearmon(year_month) %in% as.yearmon(c("March 2020", "April 2020")), 1, 2))] %>%
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
        job_date = min(job_date)),
    by = .(city, state, city_state, year_month)] %>%
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

lapply(1:length(top_cities$city_state), function(i) {
  
  p = ggplot() +
    geom_point(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_hampel, colour = "Mean (Hampel Filter)"), size = 4, alpha = 1, shape = 15) +
    geom_point(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean, colour = "Mean (raw)"), size = 4, alpha = 1, shape = 17) +
    geom_point(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_loess, colour = "Mean (Loess)"), size = 4, alpha = 1, shape = 18) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_hampel, colour = "Mean (Hampel Filter)"), size = 2, alpha = 1, linetype = "solid") +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean, colour = "Mean (raw)"), size = 2, alpha = 1) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_loess, colour = "Mean (Loess)"), size = 2, alpha = 1) +
    ylab("Share (%)") +
    xlab("Date") +
    labs(title = top_cities$city_state[i], subtitle = paste0("N = ", prettyNum(top_cities$N[i], big.mark = ","))) +
    scale_x_date(date_breaks = "3 months",
                 date_labels = '%Y-%m') +
    scale_y_continuous(labels = scales::number_format(accuracy = 1), breaks = seq(0, 100, 2)) +
    #coord_cartesian(ylim = c(0, 20)) +
    scale_color_manual(name = "Series:", values = c("Mean (raw)" = "#E69F00", "Mean (Hampel Filter)" = "#009E73", "Mean (Loess)" = "#CC79A7")) +
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
  ggsave(p_egg, filename = paste0("./plots/validate4/nz",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(top_cities$city_state[i]))),".png"), width = 8, height = 6)
  ggsave(p_egg, filename = paste0("./plots/validate4/nz",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(top_cities$city_state[i]))),".pdf"), width = 8, height = 6)
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
  .[city != ""] %>%
  .[, .(.N, daily_mean = mean(wfh_wham, na.rm = T)), by = .(city, state, city_state, year_month, job_date)] %>%
  # .[, period := ifelse(as.yearmon(year_month) < as.yearmon("March 2020"), 0,
  #                      ifelse(as.yearmon(year_month) %in% as.yearmon(c("March 2020", "April 2020")), 1, 2))] %>%
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
        job_date = min(job_date)),
    by = .(city, state, city_state, year_month)] %>%
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

lapply(1:length(top_cities$city_state), function(i) {
  
  p = ggplot() +
    geom_point(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_hampel, colour = "Mean (Hampel Filter)"), size = 4, alpha = 1, shape = 15) +
    geom_point(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean, colour = "Mean (raw)"), size = 4, alpha = 1, shape = 17) +
    geom_point(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_loess, colour = "Mean (Loess)"), size = 4, alpha = 1, shape = 18) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_hampel, colour = "Mean (Hampel Filter)"), size = 2, alpha = 1, linetype = "solid") +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean, colour = "Mean (raw)"), size = 2, alpha = 1) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_loess, colour = "Mean (Loess)"), size = 2, alpha = 1) +
    ylab("Share (%)") +
    xlab("Date") +
    labs(title = top_cities$city_state[i], subtitle = paste0("N = ", prettyNum(top_cities$N[i], big.mark = ","))) +
    scale_x_date(date_breaks = "3 months",
                 date_labels = '%Y-%m') +
    scale_y_continuous(labels = scales::number_format(accuracy = 1), breaks = seq(0, 100, 2)) +
    #coord_cartesian(ylim = c(0, 20)) +
    scale_color_manual(name = "Series:", values = c("Mean (raw)" = "#E69F00", "Mean (Hampel Filter)" = "#009E73", "Mean (Loess)" = "#CC79A7")) +
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
  ggsave(p_egg, filename = paste0("./plots/validate4/aus",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(top_cities$city_state[i]))),".png"), width = 8, height = 6)
  ggsave(p_egg, filename = paste0("./plots/validate4/aus",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(top_cities$city_state[i]))),".pdf"), width = 8, height = 6)
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
  .[city != ""] %>%
  .[, .(.N, daily_mean = mean(wfh_wham, na.rm = T)), by = .(city, state, city_state, year_month, job_date)] %>%
  # .[, period := ifelse(as.yearmon(year_month) < as.yearmon("March 2020"), 0,
  #                      ifelse(as.yearmon(year_month) %in% as.yearmon(c("March 2020", "April 2020")), 1, 2))] %>%
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
        job_date = min(job_date)),
    by = .(city, state, city_state, year_month)] %>%
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

lapply(1:length(top_cities$city_state), function(i) {
  
  p = ggplot() +
    geom_point(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_hampel, colour = "Mean (Hampel Filter)"), size = 4, alpha = 1, shape = 15) +
    geom_point(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean, colour = "Mean (raw)"), size = 4, alpha = 1, shape = 17) +
    geom_point(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_loess, colour = "Mean (Loess)"), size = 4, alpha = 1, shape = 18) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_hampel, colour = "Mean (Hampel Filter)"), size = 2, alpha = 1, linetype = "solid") +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean, colour = "Mean (raw)"), size = 2, alpha = 1) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_loess, colour = "Mean (Loess)"), size = 2, alpha = 1) +
    ylab("Share (%)") +
    xlab("Date") +
    labs(title = top_cities$city_state[i], subtitle = paste0("N = ", prettyNum(top_cities$N[i], big.mark = ","))) +
    scale_x_date(date_breaks = "3 months",
                 date_labels = '%Y-%m') +
    scale_y_continuous(labels = scales::number_format(accuracy = 1), breaks = seq(0, 100, 2)) +
    #coord_cartesian(ylim = c(0, 20)) +
    scale_color_manual(name = "Series:", values = c("Mean (raw)" = "#E69F00", "Mean (Hampel Filter)" = "#009E73", "Mean (Loess)" = "#CC79A7")) +
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
  ggsave(p_egg, filename = paste0("./plots/validate4/us",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(top_cities$city_state[i]))),".png"), width = 8, height = 6)
  ggsave(p_egg, filename = paste0("./plots/validate4/us",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(top_cities$city_state[i]))),".pdf"), width = 8, height = 6)
})

# system("echo sci2007! | sudo -S shutdown -h now")
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

sort(unique(top_cities$city))

df_all_uk <- df_all_uk %>% .[, city_state := paste0(city,", ",state)] %>% .[city_state %in% top_cities$city_state]

ts_compare <- df_all_uk %>%
  .[city != ""] %>%
  .[, .(.N, daily_mean = mean(wfh_wham, na.rm = T)), by = .(city, state, city_state, year_month, job_date)] 

%>%
  # .[, period := ifelse(as.yearmon(year_month) < as.yearmon("March 2020"), 0,
  #                      ifelse(as.yearmon(year_month) %in% as.yearmon(c("March 2020", "April 2020")), 1, 2))] %>%
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
        job_date = min(job_date)),
    by = .(city, state, city_state, year_month)] %>%
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

lapply(1:length(top_cities$city_state), function(i) {
  
  p = ggplot() +
    geom_point(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_hampel, colour = "Mean (Hampel Filter)"), size = 4, alpha = 1, shape = 15) +
    geom_point(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean, colour = "Mean (raw)"), size = 4, alpha = 1, shape = 17) +
    geom_point(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_loess, colour = "Mean (Loess)"), size = 4, alpha = 1, shape = 18) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_hampel, colour = "Mean (Hampel Filter)"), size = 2, alpha = 1, linetype = "solid") +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean, colour = "Mean (raw)"), size = 2, alpha = 1) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_loess, colour = "Mean (Loess)"), size = 2, alpha = 1) +
    ylab("Share (%)") +
    xlab("Date") +
    labs(title = top_cities$city_state[i], subtitle = paste0("N = ", prettyNum(top_cities$N[i], big.mark = ","))) +
    scale_x_date(date_breaks = "3 months",
                 date_labels = '%Y-%m') +
    scale_y_continuous(labels = scales::number_format(accuracy = 1), breaks = seq(0, 100, 2)) +
    #coord_cartesian(ylim = c(0, 20)) +
    scale_color_manual(name = "Series:", values = c("Mean (raw)" = "#E69F00", "Mean (Hampel Filter)" = "#009E73", "Mean (Loess)" = "#CC79A7")) +
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
  ggsave(p_egg, filename = paste0("./plots/validate4/uk",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(top_cities$city_state[i]))),".png"), width = 8, height = 6)
  ggsave(p_egg, filename = paste0("./plots/validate4/uk",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(top_cities$city_state[i]))),".pdf"), width = 8, height = 6)
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
  .[city != ""] %>%
  .[, .(.N, daily_mean = mean(wfh_wham, na.rm = T)), by = .(city, state, city_state, year_month, job_date)] %>%
  # .[, period := ifelse(as.yearmon(year_month) < as.yearmon("March 2020"), 0,
  #                      ifelse(as.yearmon(year_month) %in% as.yearmon(c("March 2020", "April 2020")), 1, 2))] %>%
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
        job_date = min(job_date)),
    by = .(city, state, city_state, year_month)] %>%
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

lapply(1:length(top_cities$city_state), function(i) {
  
  p = ggplot() +
    geom_point(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_hampel, colour = "Mean (Hampel Filter)"), size = 4, alpha = 1, shape = 15) +
    geom_point(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean, colour = "Mean (raw)"), size = 4, alpha = 1, shape = 17) +
    geom_point(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_loess, colour = "Mean (Loess)"), size = 4, alpha = 1, shape = 18) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_hampel, colour = "Mean (Hampel Filter)"), size = 2, alpha = 1, linetype = "solid") +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean, colour = "Mean (raw)"), size = 2, alpha = 1) +
    geom_line(data = ts_compare[city_state == top_cities$city_state[i]], aes(x = as.Date(as.yearmon(job_date)), y = monthly_mean_loess, colour = "Mean (Loess)"), size = 2, alpha = 1) +
    ylab("Share (%)") +
    xlab("Date") +
    labs(title = top_cities$city_state[i], subtitle = paste0("N = ", prettyNum(top_cities$N[i], big.mark = ","))) +
    scale_x_date(date_breaks = "3 months",
                 date_labels = '%Y-%m') +
    scale_y_continuous(labels = scales::number_format(accuracy = 1), breaks = seq(0, 100, 2)) +
    #coord_cartesian(ylim = c(0, 20)) +
    scale_color_manual(name = "Series:", values = c("Mean (raw)" = "#E69F00", "Mean (Hampel Filter)" = "#009E73", "Mean (Loess)" = "#CC79A7")) +
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
  ggsave(p_egg, filename = paste0("./plots/validate4/can",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(top_cities$city_state[i]))),".png"), width = 8, height = 6)
  ggsave(p_egg, filename = paste0("./plots/validate4/can",sprintf("%04d", i),"_",gsub("__", "_", gsub("\\s+|\\,", "_", tolower(top_cities$city_state[i]))),".pdf"), width = 8, height = 6)
})



