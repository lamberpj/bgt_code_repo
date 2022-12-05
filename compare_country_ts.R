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
# font_import()
#loadfonts(device="postscript")
#fonts()

setDTthreads(8)
getDTthreads()
#quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg_combined/")

#### LOAD "ALL" ####
df_nz <- fread("./int_data/df_nz_standardised.csv", nThread = 8, integer64 = "numeric")
df_aus <- fread("./int_data/df_aus_standardised.csv", nThread = 8, integer64 = "numeric")
df_can <- fread("./int_data/df_can_standardised.csv", nThread = 8, integer64 = "numeric")
df_uk <- fread("./int_data/df_uk_standardised.csv", nThread = 8, integer64 = "numeric")
df_us_2014 <- fread("./int_data/df_us_2014_standardised.csv", nThread = 8, integer64 = "numeric")
df_us_2015 <- fread("./int_data/df_us_2015_standardised.csv", nThread = 8, integer64 = "numeric")
df_us_2016 <- fread("./int_data/df_us_2016_standardised.csv", nThread = 8, integer64 = "numeric")
df_us_2017 <- fread("./int_data/df_us_2017_standardised.csv", nThread = 8, integer64 = "numeric")
df_us_2018 <- fread("./int_data/df_us_2018_standardised.csv", nThread = 8, integer64 = "numeric")
df_us_2019 <- fread("./int_data/df_us_2019_standardised.csv", nThread = 8, integer64 = "numeric")
df_us_2020 <- fread("./int_data/df_us_2020_standardised.csv", nThread = 8, integer64 = "numeric")
df_us_2021 <- fread("./int_data/df_us_2021_standardised.csv", nThread = 8, integer64 = "numeric")
df_us_2022 <- fread("./int_data/df_us_2022_standardised.csv", nThread = 8, integer64 = "numeric")

df_us <- rbindlist(list(df_us_2014, df_us_2015, df_us_2016, df_us_2017, df_us_2018,df_us_2019,df_us_2020,df_us_2021,df_us_2022))
remove(list = c("df_us_2014", "df_us_2015", "df_us_2016", "df_us_2017", "df_us_2018","df_us_2019","df_us_2020","df_us_2021","df_us_2022"))

uniqueN(df_nz$job_id) # 1297812
uniqueN(df_aus$job_id) # 4936765
uniqueN(df_can$job_id) # 7540054
uniqueN(df_uk$job_id) # 36914129
uniqueN(df_us$job_id) # 148224554

uniqueN(df_nz$employer) # 27,712
uniqueN(df_aus$employer) # 131,995
uniqueN(df_can$employer) # 586,165
uniqueN(df_uk$employer) # 619,641
uniqueN(df_us$employer) # 3,262,838

df_nz <- df_nz %>% .[, city_state := paste0(city,"_",state)]
df_aus <- df_aus %>% .[, city_state := paste0(city,"_",state)]
df_can <- df_can %>% .[, city_state := paste0(city,"_",state)]
df_uk <- df_uk %>% .[, city_state := paste0(city,"_",state)]
df_us <- df_us %>% .[, city_state := paste0(city,"_",state)]

uniqueN(df_nz$city_state) # 67
uniqueN(df_aus$city_state) # 57
uniqueN(df_can$city_state) # 3,653
uniqueN(df_uk$city_state) # 2,249
uniqueN(df_us$city_state) # 31,568

df_nz <- df_nz %>% .[!grepl("mercadojobs", job_url)]
df_can <- df_can %>% .[!grepl("workopolis", job_url)]
df_can <- df_can %>% .[!grepl("careerjet", job_url)]
df_uk <- df_uk %>% .[!grepl("jobisjob", job_url)]
df_us <- df_us %>% .[!grepl("careerbuilder", job_url)]

df_all_list <- list(df_nz,df_aus,df_can,df_uk,df_us)
remove(list = setdiff(ls(), "df_all_list"))
#### END ####

#### ACROSS COUNTRIES ####

# Unweighted share
wfh_daily_share_list <- lapply(df_all_list, function(x) {
  x <- x %>% select(country, wfh_wham, job_date, bgt_occ, month, narrow_result, neg_narrow_result, job_id_weight)
  x <- x[!is.na(wfh_wham)]
  x <- x[!is.na(bgt_occ)]
  x <- x[, month := factor(month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))]
  x <- x[, job_ymd := ymd(job_date)]
  x <- x[, year_month := as.yearmon(job_ymd)]
  x <- x[, year := year(job_ymd)]
  x <- setDT(x)
  
  x <- x %>%
    select(country, job_date, year_month, year, wfh_wham, job_id_weight) %>%
    setDT(.) %>%
    .[, .(wfh_sum = sum(job_id_weight*wfh_wham, na.rm = T), job_ads_sum = sum(job_id_weight, na.rm = T)), by = .(country, job_date, year_month, year)] %>%
    .[, wfh_share := wfh_sum / job_ads_sum] %>%
    setDT(.)
  
  return(x)
})

wfh_daily_share <- rbindlist(wfh_daily_share_list)
rm("wfh_daily_share_list")

wfh_daily_share <- wfh_daily_share %>%
  rename(daily_share = wfh_share, N = job_ads_sum)

fwrite(wfh_daily_share, file = "./int_data/country_daily_wfh.csv")

ts_for_plot <- wfh_daily_share %>%
  .[, job_date := ymd(job_date)] %>%
  .[as.yearmon(year_month) >= as.yearmon(ymd("20180901")) & as.yearmon(year_month) <= as.yearmon(ymd("20220601"))] %>%
  .[, l1o_monthly_mean := (sum(daily_share*N)-daily_share*N)/(sum(N) - N), by = .(country, year_month)] %>%
  .[, monthly_mean := sum(daily_share*N)/(sum(N)), by = .(country, year_month)] %>%
  .[, l1o_keep := ifelse(abs(monthly_mean - l1o_monthly_mean) > 0.02 | abs(log(monthly_mean/l1o_monthly_mean)) > 0.10, 0, 1)] %>%
  .[, l1o_keep := ifelse(as.yearmon(year_month) %in% as.yearmon(ymd(c("20200301", "20200401", "20200501", "20200601"))), 1, l1o_keep)] %>%
  .[, l1o_daily_with_nas := ifelse(l1o_keep == 1, daily_share, NA)]

check <- ts_for_plot %>%
  .[, .(days = .N, replaced = sum(1-l1o_keep)), by = .(country, year_month)] %>%
  .[, share_replaced := replaced/days]

ts_for_plot <- ts_for_plot %>%
  .[order(country, job_date)] %>%
  .[, daily_share_l10 := na.approx(l1o_daily_with_nas, rule = 2), by = .(country)] %>%
  .[, monthly_mean_l1o := sum(daily_share_l10[!is.na(daily_share_l10)]*N[!is.na(daily_share_l10)], na.rm = T)/(sum(N[!is.na(daily_share_l10)], na.rm = T)),
    by = .(country, year_month)] %>%
  .[, .(job_date = min(job_date),
        monthly_mean = monthly_mean[1],
        monthly_mean_l1o = monthly_mean_l1o[1]),
    by = .(country, year_month)] %>%
  .[, monthly_mean_3ma := rollmean(monthly_mean, k = 3, align = "right", fill = NA), by = .(country)] %>%
  .[, monthly_mean_3ma_l1o := rollmean(monthly_mean_l1o, k = 3, align = "right", fill = NA), by = .(country)] %>%
  .[, monthly_mean_l1o := ifelse(as.yearmon(year_month) %in% as.yearmon(ymd(c("20200301", "20200401", "20200501", "20200601"))), monthly_mean, monthly_mean_l1o)] %>%
  .[, monthly_mean_3ma := ifelse(as.yearmon(year_month) %in% as.yearmon(ymd(c("20200301", "20200401", "20200501", "20200601"))), monthly_mean, monthly_mean_3ma)] %>%
  .[, monthly_mean_3ma_l1o := ifelse(as.yearmon(year_month) %in% as.yearmon(ymd(c("20200301", "20200401", "20200501", "20200601"))), monthly_mean, monthly_mean_3ma_l1o)] %>%
  .[as.yearmon(year_month) >= as.yearmon(ymd("20190101")) & as.yearmon(year_month) <= as.yearmon(ymd("20220601"))]

check <- ts_for_plot %>%
  .[, .(replaced = sum(monthly_mean == monthly_mean_l1o)), by = .(country, year_month)]

ts_for_plot <- ts_for_plot %>%
  group_by(country, year_month, job_date) %>%
  pivot_longer(cols = c(monthly_mean:monthly_mean_3ma_l1o)) %>%
  setDT(.)

cbbPalette <- c("#E69F00", "#009E73", "#CC79A7", "#0072B2")

cbbPalette[1]

(country_list <- unique(ts_for_plot$country))

lapply(1:length(country_list), function(i) {
  
  p = ggplot(data = ts_for_plot[country == country_list[i] & name %in% c("monthly_mean", "monthly_mean_l1o")], aes(x = as.Date(as.yearmon(job_date)), y = 100*value, colour = name, shape = name)) +
    geom_point(size = 4) +
    geom_line(size = 1) +
    ggtitle(country_list[i]) +
    ylab("Share (%)") +
    xlab("Date") +
    scale_x_date(date_breaks = "2 months",
                 date_labels = '%Y-%m') +
    scale_y_continuous(labels = scales::number_format(accuracy = 1), breaks = seq(0, 100, 2)) +
    theme(
      axis.title.x=element_blank(),
      legend.position="bottom",
      legend.title = element_blank(),
      axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
    scale_colour_manual(values=cbbPalette) +
    scale_shape_manual(values=c(15,16,17,18,19)) +
    guides(col = guide_legend(nrow = 5, reverse = F)) +
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
  ggsave(p_egg, filename = paste0("./plots/compare_smoothing_",country_list[i],".pdf"), width = 8, height = 8)
  ggsave(p_egg, filename = paste0("./plots/compare_smoothing_",country_list[i],".png"), width = 8, height = 8)
  
})
