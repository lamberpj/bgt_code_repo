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

##### GITHUB RUN ####
# cd /mnt/disks/pdisk/bgt_code_repo
# git init
# git add .
# git pull -m "check"
# git commit -m "autosave"
# git push origin master

setDTthreads(4)
getDTthreads()
#quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg_combined/")

#### LOAD "ALL" ####
# df_nz <- fread("./int_data/df_nz_standardised.csv", nThread = 8, integer64 = "numeric")
# df_aus <- fread("./int_data/df_aus_standardised.csv", nThread = 8, integer64 = "numeric")
# df_can <- fread("./int_data/df_can_standardised.csv", nThread = 8, integer64 = "numeric")
# df_uk <- fread("./int_data/df_uk_standardised.csv", nThread = 8, integer64 = "numeric")
# df_us_2019 <- fread("./int_data/df_us_2019_standardised.csv", nThread = 8, integer64 = "numeric")
# df_us_2020 <- fread("./int_data/df_us_2020_standardised.csv", nThread = 8, integer64 = "numeric")
# df_us_2021 <- fread("./int_data/df_us_2021_standardised.csv", nThread = 8, integer64 = "numeric")
# df_us_2022 <- fread("./int_data/df_us_2022_standardised.csv", nThread = 8, integer64 = "numeric")
# 
# df_us <- rbindlist(list(df_us_2014, df_us_2015, df_us_2016, df_us_2017, df_us_2018,df_us_2019,df_us_2020,df_us_2021,df_us_2022))
# remove(list = c("df_us_2014", "df_us_2015", "df_us_2016", "df_us_2017", "df_us_2018","df_us_2019","df_us_2020","df_us_2021","df_us_2022"))
# 
# uniqueN(df_nz$job_id) # 1297812
# uniqueN(df_aus$job_id) # 4936765
# uniqueN(df_can$job_id) # 7540054
# uniqueN(df_uk$job_id) # 36914129
# uniqueN(df_us$job_id) # 148224554
# 
# uniqueN(df_nz$employer) # 27,712
# uniqueN(df_aus$employer) # 131,995
# uniqueN(df_can$employer) # 586,165
# uniqueN(df_uk$employer) # 619,641
# uniqueN(df_us$employer) # 3,262,838
# 
# df_nz <- df_nz %>% .[, city_state := paste0(city,"_",state)]
# df_aus <- df_aus %>% .[, city_state := paste0(city,"_",state)]
# df_can <- df_can %>% .[, city_state := paste0(city,"_",state)]
# df_uk <- df_uk %>% .[, city_state := paste0(city,"_",state)]
# df_us <- df_us %>% .[, city_state := paste0(city,"_",state)]
# 
# uniqueN(df_nz$city_state) # 67
# uniqueN(df_aus$city_state) # 57
# uniqueN(df_can$city_state) # 3,653
# uniqueN(df_uk$city_state) # 2,249
# uniqueN(df_us$city_state) # 31,568
# 
# df_nz <- df_nz %>% .[!grepl("mercadojobs", job_url)]
# df_can <- df_can %>% .[!grepl("workopolis", job_url)]
# df_can <- df_can %>% .[!grepl("careerjet", job_url)]
# df_uk <- df_uk %>% .[!grepl("jobisjob", job_url)]
# df_us <- df_us %>% .[!grepl("careerbuilder", job_url)]
# 
# df_all_list <- list(df_nz,df_aus,df_can,df_uk,df_us)
# remove(list = setdiff(ls(), "df_all_list"))
# #### END ####
# 
# df_all_list <- lapply(df_all_list, function(x) {
#   x <- x %>% select(country, state, wfh_wham, job_date, bgt_occ, month, narrow_result, neg_narrow_result, job_id_weight)
#   return(x)
# })
# 
# save(df_all_list, file = "./aux_data/df_all_list.RData")
load(file = "./aux_data/df_all_list.RData")

#### PREPARE DATA ####
# Make Unweighted daily shares
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
wfh_daily_share <- wfh_daily_share %>% rename(daily_share = wfh_share, N = job_ads_sum)
fwrite(wfh_daily_share, file = "./int_data/country_daily_wfh.csv")

# Filter Unweighted daily shares
wfh_daily_share <- fread(file = "./int_data/country_daily_wfh.csv")

ts_for_plot <- wfh_daily_share %>%
  .[, job_date := ymd(job_date)] %>%
  .[as.yearmon(year_month) <= as.yearmon(ymd("20220601"))] %>%
  .[, l1o_monthly_mean := (sum(daily_share*N)-daily_share*N)/(sum(N) - N), by = .(country, year_month)] %>%
  .[, monthly_mean := sum(daily_share*N)/(sum(N)), by = .(country, year_month)] %>%
  .[, l1o_keep := ifelse(abs(monthly_mean - l1o_monthly_mean) > 0.02 | abs(log(monthly_mean/l1o_monthly_mean)) > 0.10, 0, 1)] %>%
  .[, l1o_keep := ifelse(as.yearmon(year_month) %in% as.yearmon(ymd(c("20200301", "20200401", "20200501", "20200601"))), 1, l1o_keep)] %>%
  .[, l1o_daily_with_nas := ifelse(l1o_keep == 1, daily_share, NA)]

dropped_days <- ts_for_plot %>%
  .[l1o_keep == 0] %>%
  select(country, job_date) %>%
  .[, drop := 1]

# check <- ts_for_plot %>%
#   .[, .(days = .N, replaced = sum(1-l1o_keep)), by = .(country, year_month)] %>%
#   .[, share_replaced := replaced/days]
# (days_replaced <- 100*round(sum(check$replaced)/sum(check$days), digits = 4))

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
  .[, monthly_mean_3ma_l1o := ifelse(as.yearmon(year_month) %in% as.yearmon(ymd(c("20200301", "20200401", "20200501", "20200601"))), monthly_mean, monthly_mean_3ma_l1o)]

ts_for_plot <- ts_for_plot %>%
  group_by(country, year_month, job_date) %>%
  pivot_longer(cols = c(monthly_mean:monthly_mean_3ma_l1o)) %>%
  setDT(.)

# Make Vacancy Weighted Monthly Shares (Using Filters from above)
wfh_monthly_bgt_occ_share_list <- lapply(df_all_list, function(x) {
  x <- x %>% select(country, wfh_wham, job_date, bgt_occ, month, narrow_result, neg_narrow_result, job_id_weight)
  x <- x[!is.na(wfh_wham)]
  x <- x[!is.na(bgt_occ)]
  x <- x[, bgt_occ5 := str_sub(bgt_occ, 1, 6)]
  x <- x[, month := factor(month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))]
  x <- x[, job_ymd := ymd(job_date)]
  x <- x[, year_month := as.yearmon(job_ymd)]
  x <- x[, year := year(job_ymd)]
  x <- setDT(x)
  nrow(x)
  x <- merge(x = x, y = dropped_days, all.x = TRUE, all.y = FALSE, by = c("country", "job_date"))
  x <- x[is.na(drop)]
  nrow(x)
  x <- x %>%
    select(country, bgt_occ5, year_month, year, wfh_wham, job_id_weight) %>%
    setDT(.) %>%
    .[, .(wfh_sum = sum(job_id_weight*wfh_wham, na.rm = T), job_ads_sum = sum(job_id_weight, na.rm = T)),
      by = .(country, bgt_occ5, year_month, year)] %>%
    .[, wfh_share := wfh_sum / job_ads_sum] %>%
    setDT(.)
  return(x)
})
wfh_monthly_bgt_occ_share <- rbindlist(wfh_monthly_bgt_occ_share_list)
rm("wfh_monthly_bgt_occ_share_list")
wfh_monthly_bgt_occ_share <- wfh_monthly_bgt_occ_share %>%
  rename(monthly_share = wfh_share, N = job_ads_sum)

# Make US 2019 vacancy weights
head(wfh_monthly_bgt_occ_share)
shares_df <- wfh_monthly_bgt_occ_share %>%
  .[bgt_occ5 != "" & !is.na(bgt_occ5)] %>%
  .[year == 2019 & country == "US"] %>%
  .[, .(N = sum(N, na.rm = T)), by = .(bgt_occ5)] %>%
  .[, share := N/sum(N, na.rm = T)] %>%
  select(bgt_occ5, share)

wfh_monthly_bgt_occ_share <- wfh_monthly_bgt_occ_share %>%
  .[bgt_occ5 != "" & !is.na(bgt_occ5)] %>%
  merge(x = ., y = shares_df, by = "bgt_occ5", all.x = TRUE, all.y = FALSE) %>%
  setDT(.)
wfh_monthly_bgt_occ_share
wfh_monthly_bgt_occ_share <- wfh_monthly_bgt_occ_share %>%
  .[, .(monthly_mean = sum(monthly_share*(share/sum(share, na.rm = T)), na.rm = T)), by = .(country, year_month)]

ts_for_plot_us_weight <- wfh_monthly_bgt_occ_share
fwrite(ts_for_plot_us_weight, file = "./int_data/ts_for_plot_us_weight")

# Filter vacancy-weighted
remove(list = setdiff(ls(), c("ts_for_plot", "ts_for_plot_us_weight", "df_all_list")))

# Plot unweighted
cbbPalette <- c("#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00")
(country_list <- unique(ts_for_plot$country))

ts_for_plot

# Unweighted
p = ts_for_plot %>%
  filter(name == "monthly_mean_l1o") %>%
  ggplot(., aes(x = as.Date(as.yearmon(year_month)), y = 100*value, col = country)) +
  #stat_smooth (geom="line", alpha=0.8, size=2, span=0.2) +
  #stat_smooth(span = 0.1, alpha=0.5, se=FALSE, size = 2) +
  geom_point(size = 2.25) +
  geom_line(size = 1.25) +
  ylab("Share (%)") +
  xlab("Date") +
  scale_x_date(breaks = as.Date(c("2014-01-01", "2015-01-01", "2016-01-01",
                                  "2017-01-01", "2018-01-01", "2019-01-01",
                                  "2020-01-01", "2021-01-01", "2022-01-01")),
               date_labels = '%Y') +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,2)) +
  coord_cartesian(ylim = c(0, 20)) +
  scale_colour_manual(values=cbbPalette) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(nrow = 1, reverse = T)) +
  theme(text = element_text(size=10, family="serif", colour = "black"),
        axis.text = element_text(size=10, family="serif", colour = "black"),
        axis.title = element_text(size=10, family="serif", colour = "black"),
        legend.text = element_text(size=15, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white")) +
  theme(aspect.ratio=3/5)
p
save(p, file = "./ppt/ggplots/rwa_country_ts_w_unweighted_month.RData")
remove(p)

# Occ Weighted to US
cbbPalette <- c("#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00")
p = ts_for_plot_us_weight %>%
  ggplot(., aes(x = as.Date(as.yearmon(year_month)), y = 100*monthly_mean, col = country)) +
  #stat_smooth (geom="line", alpha=0.8, size=2, span=0.2) +
  #stat_smooth(span = 0.1, alpha=0.5, se=FALSE, size = 2) +
  geom_point(size = 2.25) +
  geom_line(size = 1.25) +
  ylab("Share (%)") +
  xlab("Date") +
  scale_x_date(breaks = as.Date(c("2014-01-01", "2015-01-01", "2016-01-01",
                                  "2017-01-01", "2018-01-01", "2019-01-01",
                                  "2020-01-01", "2021-01-01", "2022-01-01")),
               date_labels = '%Y') +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,2)) +
  coord_cartesian(ylim = c(0, 20)) +
  scale_colour_manual(values=cbbPalette) +
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
        panel.background = element_rect(fill = "white")) +
  theme(aspect.ratio=3/5)
p
save(p, file = "./ppt/ggplots/rwa_country_ts_w_us_vac_month.RData")
remove(p)
#### END ####


# OLD CODE - OTHER TYPES OF AGGREGATION #

# Aggregate by occupation
wfh_monthly_share_list <- lapply(df_all_list, function(x) {
  x <- x %>% select(country, wfh_wham, job_date, bgt_occ, month, narrow_result, neg_narrow_result, wfh_wham, tot_emp_ad, job_id_weight)
  x <- x[!is.na(wfh_wham)]
  x <- x[!is.na(bgt_occ)]
  #x <- x[bgt_occ != ""]
  x <- x[, month := factor(month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))]
  x <- x[, wfh := wfh_wham]
  #x <- x[, bgt_occ6 := str_sub(bgt_occ, 1, 7)]
  x <- x[, job_ymd := ymd(job_date)]
  #x <- x[, year_quarter := as.yearqtr(job_ymd)]
  x <- x[, year_month := as.yearmon(job_ymd)]
  x <- x[, year := year(job_ymd)]
  x <- setDT(x)
  
  wfh_occ_ag <- x %>%
    select(country, job_date, year_month, year, narrow_result, neg_narrow_result, wfh, tot_emp_ad, job_id_weight) %>%
    setDT(.) %>%
    .[, .(wfh_sum = sum(job_id_weight*wfh, na.rm = T),
          job_ads_sum = sum(job_id_weight, na.rm = T),
          dict_sum = sum(job_id_weight[!is.na(narrow_result)]*narrow_result[!is.na(narrow_result)], na.rm = T),
          dict_nn_sum = sum(job_id_weight[!is.na(neg_narrow_result)]*neg_narrow_result[!is.na(neg_narrow_result)], na.rm = T),
          dict_all = sum(job_id_weight[!is.na(neg_narrow_result)], na.rm = T)),
      by = .(country, year_month, year)] %>%
    setDT(.)
  
  return(wfh_occ_ag)
})

wfh_monthly_share <- rbindlist(wfh_monthly_share_list, fill = TRUE) %>%
  .[, .(wfh = sum(wfh_sum, na.rm = T)/sum(job_ads_sum, na.rm = T),
        dict = sum(dict_sum, na.rm = T)/sum(dict_all, na.rm = T),
        dict_nn = sum(dict_nn_sum, na.rm = T)/sum(dict_all, na.rm = T)),
    by = .(country, year_month, year)] %>%
  .[, aggregation := "mean"]

wfh_daily_share_list <- lapply(df_all_list, function(x) {
  x <- x %>% select(country, wfh_wham, job_date, bgt_occ, month, narrow_result, neg_narrow_result, wfh_wham, tot_emp_ad, job_id_weight)
  x <- x[!is.na(wfh_wham)]
  x <- x[!is.na(bgt_occ)]
  #x <- x[bgt_occ != ""]
  x <- x[, month := factor(month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))]
  x <- x[, wfh := wfh_wham]
  #x <- x[, bgt_occ6 := str_sub(bgt_occ, 1, 7)]
  x <- x[, job_ymd := ymd(job_date)]
  #x <- x[, year_quarter := as.yearqtr(job_ymd)]
  x <- x[, year_month := as.yearmon(job_ymd)]
  x <- x[, year := year(job_ymd)]
  x <- setDT(x)
  
  wfh_occ_ag <- x %>%
    select(country, job_date, year_month, year, narrow_result, neg_narrow_result, wfh, tot_emp_ad, job_id_weight) %>%
    setDT(.) %>%
    .[, .(wfh_sum = sum(job_id_weight*wfh, na.rm = T),
          job_ads_sum = sum(job_id_weight, na.rm = T),
          dict_sum = sum(job_id_weight[!is.na(narrow_result)]*narrow_result[!is.na(narrow_result)], na.rm = T),
          dict_nn_sum = sum(job_id_weight[!is.na(neg_narrow_result)]*neg_narrow_result[!is.na(neg_narrow_result)], na.rm = T),
          dict_all = sum(job_id_weight[!is.na(neg_narrow_result)], na.rm = T)),
      by = .(country, job_date, year_month, year)] %>%
    setDT(.)
  
  return(wfh_occ_ag)
})

# Emp Weighted
cbbPalette <- c("#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00")

p = wfh_monthly_share %>%
  ggplot(., aes(x = as.Date(as.yearmon(year_month)), y = 100*wfh, col = country)) +
  #stat_smooth (geom="line", alpha=0.8, size=2, span=0.2) +
  #stat_smooth(span = 0.1, alpha=0.5, se=FALSE, size = 2) +
  geom_point(size = 1.75) +
  geom_line(size = 1.25) +
  ylab("Share (%)") +
  xlab("Date") +
  labs(title = "Share of Remote Work Vacancy Postings (%)", subtitle = "Employment Weighted") +
  scale_x_date(breaks = as.Date(c("2014-01-01", "2015-01-01", "2016-01-01",
                                  "2017-01-01", "2018-01-01", "2019-01-01",
                                  "2020-01-01", "2021-01-01", "2022-01-01")),
               date_labels = '%Y') +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,2)) +
  coord_cartesian(ylim = c(0, 20)) +
  scale_colour_manual(values=cbbPalette) +
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
        panel.background = element_rect(fill = "white"))
p

p = wfh_median %>%
  ggplot(., aes(x = as.Date(as.yearmon(year_month)), y = 100*wfh, col = country)) +
  #stat_smooth (geom="line", alpha=0.8, size=2, span=0.2) +
  #stat_smooth(span = 0.1, alpha=0.5, se=FALSE, size = 2) +
  geom_point(size = 1.75) +
  geom_line(size = 1.25) +
  ylab("Share (%)") +
  xlab("Date") +
  labs(title = "Share of Remote Work Vacancy Postings (%)", subtitle = "Employment Weighted") +
  scale_x_date(breaks = as.Date(c("2014-01-01", "2015-01-01", "2016-01-01",
                                  "2017-01-01", "2018-01-01", "2019-01-01",
                                  "2020-01-01", "2021-01-01", "2022-01-01")),
               date_labels = '%Y') +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,2)) +
  coord_cartesian(ylim = c(0, 20)) +
  scale_colour_manual(values=cbbPalette) +
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
        panel.background = element_rect(fill = "white"))
p

remove(list = c("p", "p_egg"))



# USA weights
usa_weights <- wfh_occ_ag %>%
  setDT(.) %>%
  .[country == "US" & year == 2019] %>%
  .[, .(us_tot_emp = sum(tot_emp)/12,
        us_tot_vacs = sum(tot_vac)/12),
    by = .(bgt_occ6)]

# Within-country weights
within_country_weights <- wfh_occ_ag %>%
  setDT(.) %>%
  .[year == 2019] %>%
  .[, .(wc_tot_emp = sum(tot_emp)/12,
        wc_tot_vacs = sum(tot_vac)/12),
    by = .(country, bgt_occ6)]

# Merge in weights
nrow(wfh_occ_ag) # 102,252
wfh_occ_ag <- wfh_occ_ag %>%
  merge(x = ., y = usa_weights, by = "bgt_occ6", all.x = TRUE, all.y = FALSE, allow.cartesian=TRUE) %>%
  merge(x = ., y = within_country_weights, by = c("country", "bgt_occ6"), all.x = TRUE, all.y = FALSE, allow.cartesian=TRUE)
nrow(wfh_occ_ag) # 102,252

colnames(wfh_occ_ag)

# Check weights
#weights_check <- wfh_occ_ag %>%
#  setDT(.) %>%
#  .[, .(share_bert_unweighted = sum(tot_vac, na.rm = T),
#        share_bert_wc_vac = sum(wc_tot_vacs, na.rm = T),
#        share_bert_wc_emp = sum(wc_tot_emp, na.rm = T),
#        share_bert_us_vac = sum(us_tot_vacs, na.rm = T),
#        share_bert_us_emp = sum(us_tot_emp, na.rm = T)),
#    by = .(country, year_month, year)] %>%
#  setDT(.)

# Aggregate
wfh_ag <- wfh_occ_ag %>%
  setDT(.) %>%
  .[, .(share_bert_unweighted = sum(tot_vac*wfh_share, na.rm = T)/sum(tot_vac, na.rm = T),
        share_bert_wc_vac = sum(wc_tot_vacs*wfh_share, na.rm = T)/sum(wc_tot_vacs, na.rm = T),
        share_bert_wc_emp = sum(wc_tot_emp*wfh_share, na.rm = T)/sum(wc_tot_emp, na.rm = T),
        share_bert_us_vac = sum(us_tot_vacs*wfh_share, na.rm = T)/sum(us_tot_vacs, na.rm = T),
        share_bert_us_emp = sum(us_tot_emp*wfh_share, na.rm = T)/sum(us_tot_emp, na.rm = T)),
    by = .(country, year_month, year)] %>%
  setDT(.)

# Table comparing weights
compare_weights <- wfh_ag %>%
  .[year %in% c(2021)] %>%
  .[, .(share_bert_unweighted = 100*mean(share_bert_unweighted),
        share_bert_wc_vac = 100*mean(share_bert_wc_vac),
        share_bert_wc_emp = 100*mean(share_bert_wc_emp),
        share_bert_us_vac = 100*mean(share_bert_us_vac),
        share_bert_us_emp = 100*mean(share_bert_us_emp)),
    by = .(year, country)] %>%
  select(-year) %>%
  as_tibble(.) %>%
  group_by(country) %>%
  pivot_longer(cols = c("share_bert_unweighted","share_bert_wc_vac","share_bert_wc_emp","share_bert_us_vac","share_bert_us_emp"), names_to = "weights", values_to = "wfh_share") %>%
  ungroup() %>%
  group_by(weights) %>%
  pivot_wider(., names_from = "country", values_from = "wfh_share") %>%
  mutate(across(Australia:US, round, 2)) %>%
  ungroup()
compare_weights$`Occupation-weights` <- c("Unweighted", "2019 Vacancy Weighted", "2019 Employment Weighted", "2019 US Vacancy Weighted", "2019 US Employment Weighted")
compare_weights <- compare_weights %>%
  select(-weights) %>%
  select(`Occupation-weights`, everything())
compare_weights
stargazer(compare_weights, title = "2021 Share of Remote Work Vacancy Postings, by Different Occupation-weighting Scheme", summary = F, type = "latex", rownames = F)

# Transitory spike
# colnames(wfh_ag)
# wfh_ag_month_spike <- wfh_ag %>%
#   filter(year_month %in% as.yearmon(c("Jan 2019", "June 2019", "Jan 2020", "June 2020", "Jan 2021", "June 2021", "Jan 2022", "June 2022"))) %>%
#   group_by(country) %>%
#   select(country, year_month, share_bert_wc_emp) %>%
#   mutate(share_bert_wc_emp = share_bert_wc_emp*100) %>%
#   pivot_wider(., names_from = year_month, values_from = share_bert_wc_emp) %>%
#   ungroup() %>%
#   arrange(country)
# wfh_ag_month_spike
# 
# stargazer(wfh_ag_month_spike, type = "latex", title = "COVID Spike in Remote Work Adoption", summary = F, rownames = F, digits = 2, font.size = "footnotesize")

# Emp Weighted
cbbPalette <- c("#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00")

wfh_ag_quar <- wfh_ag %>%
  .[, year_quart := as.yearqtr(as.Date(as.yearmon(year_month)))] %>%
  .[, .(share_bert_unweighted = mean(share_bert_unweighted),
        share_bert_wc_vac = mean(share_bert_wc_vac),
        share_bert_wc_emp = mean(share_bert_wc_emp),
        share_bert_us_vac = mean(share_bert_us_vac),
        share_bert_us_emp = mean(share_bert_us_emp)),
    by = .(country, year, year_quart)]

p = wfh_ag %>%
  ggplot(., aes(x = as.Date(as.yearmon(year_month)), y = 100*share_bert_wc_emp, col = country)) +
  #stat_smooth (geom="line", alpha=0.8, size=2, span=0.2) +
  #stat_smooth(span = 0.1, alpha=0.5, se=FALSE, size = 2) +
  geom_point(size = 1.75) +
  geom_line(size = 1.25) +
  ylab("Share (%)") +
  xlab("Date") +
  labs(title = "Share of Remote Work Vacancy Postings (%)", subtitle = "Employment Weighted") +
  scale_x_date(breaks = as.Date(c("2014-01-01", "2015-01-01", "2016-01-01",
                                  "2017-01-01", "2018-01-01", "2019-01-01",
                                  "2020-01-01", "2021-01-01", "2022-01-01")),
               date_labels = '%Y') +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,2)) +
  coord_cartesian(ylim = c(0, 15)) +
  scale_colour_manual(values=cbbPalette) +
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
        panel.background = element_rect(fill = "white"))
p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/rwa_country_ts_w_emp2019_month.pdf", width = 8, height = 6)

remove(list = c("p", "p_egg"))

# Emp Weighted Index
wfh_ag_year <- wfh_ag %>%
  .[, year_quart := as.yearqtr(as.Date(as.yearmon(year_month)))] %>%
  .[, .(share_bert_unweighted = mean(share_bert_unweighted),
        share_bert_wc_vac = mean(share_bert_wc_vac),
        share_bert_wc_emp = mean(share_bert_wc_emp),
        share_bert_us_vac = mean(share_bert_us_vac),
        share_bert_us_emp = mean(share_bert_us_emp)),
    by = .(country, year)] %>%
  .[, share_bert_wc_emp := 100*share_bert_wc_emp/mean(share_bert_wc_emp[year == 2019]), by = country]

p = wfh_ag_year %>%
  ggplot(., aes(x = year, y = share_bert_wc_emp, col = country)) +
  #stat_smooth (geom="line", alpha=0.8, size=2, span=0.2) +
  #stat_smooth(span = 0.1, alpha=0.5, se=FALSE, size = 2) +
  geom_point(size = 2.25) +
  geom_line(size = 1.25) +
  ylab("Index (2019 = 100)") +
  xlab("Date") +
  labs(title = "Index of Remote Work Share (100 = 2019)", subtitle = "Employment Weighted") +
  #scale_x_yearmon(format = '%Y-%-m', breaks = seq(from = as.yearmon("2014-1"), to = as.yearmon("2021-9"), by = 1),  limits = c(as.yearmon("2014-1"), as.yearmon("2021-9"))) +
  #scale_x_date(breaks = as.Date(c("2014-01-01", "2015-01-01", "2016-01-01", "2017-01-01", "2018-01-01", "2019-01-01", "2020-01-01", "2021-01-01", "2022-01-01", "2022-01-01")),
  #             minor_breaks = as.Date(c("2014-01-01","2014-07-01","2015-01-01","2015-07-01","2016-01-01","2016-07-01","2017-01-01","2017-07-01",
  #                                      "2018-01-01","2018-07-01","2019-01-01","2019-07-01","2020-01-01","2020-07-01","2021-01-01","2021-07-01", "2022-01-01")),
  #             date_labels = '%Y') +
  scale_x_continuous(breaks = seq(2014,2022)) +
  scale_y_continuous(breaks = seq(100,900, 100)) +
  coord_cartesian(ylim = c(90, 900)) +
  scale_colour_manual(values=cbbPalette) +
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
        panel.background = element_rect(fill = "white"))
p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/rwa_country_ts_w_emp2019_index_year.pdf", width = 8, height = 6)

remove(list = c("p", "p_egg"))

# Vac Weighted
p = wfh_ag %>%
  ggplot(., aes(x = as.Date(year_month), y = 100*share_bert_wc_vac, col = country)) +
  #stat_smooth (geom="line", alpha=0.8, size=2, span=0.2) +
  #stat_smooth(span = 0.1, alpha=0.5, se=FALSE, size = 2) +
  geom_point(size = 2.25) +
  geom_line(size = 1.25) +
  ylab("Share (%)") +
  xlab("Date") +
  labs(title = "Share of Remote Work Vacancy Postings (%)", subtitle = "Vac Weighted") +
  scale_x_date(breaks = as.Date(c("2014-01-01", "2015-01-01", "2016-01-01",
                                  "2017-01-01", "2018-01-01", "2019-01-01",
                                  "2020-01-01", "2021-01-01", "2022-01-01")),
               date_labels = '%Y') +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,2)) +
  coord_cartesian(ylim = c(0, 20)) +
  scale_colour_manual(values=cbbPalette) +
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
        panel.background = element_rect(fill = "white"))
p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/rwa_country_ts_w_2019vac_month.pdf", width = 8, height = 6)

remove(list = c("p", "p_egg"))

# USA Emp Weights
p = wfh_ag %>%
  ggplot(., aes(x = as.Date(year_month), y = 100*share_bert_us_emp, col = country)) +
  #stat_smooth (geom="line", alpha=0.8, size=2, span=0.2) +
  #stat_smooth(span = 0.1, alpha=0.5, se=FALSE, size = 2) +
  geom_point(size = 2.25) +
  geom_line(size = 1.25) +
  ylab("Share (%)") +
  xlab("Date") +
  labs(title = "Share of Remote Work Vacancy Postings (%)", subtitle = "USA Emp Weighted") +
  scale_x_date(breaks = as.Date(c("2014-01-01", "2015-01-01", "2016-01-01",
                                  "2017-01-01", "2018-01-01", "2019-01-01",
                                  "2020-01-01", "2021-01-01", "2022-01-01")),
               date_labels = '%Y') +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,2)) +
  coord_cartesian(ylim = c(0, 20)) +
  scale_colour_manual(values=cbbPalette) +
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
        panel.background = element_rect(fill = "white"))

p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/rwa_country_ts_w_us_2019emp_month.pdf", width = 8, height = 6)

remove(list = c("p", "p_egg"))

# USA Vac Weights
p = wfh_ag %>%
  ggplot(., aes(x = as.Date(year_month), y = 100*share_bert_us_vac, col = country)) +
  #stat_smooth (geom="line", alpha=0.8, size=2, span=0.2) +
  #stat_smooth(span = 0.1, alpha=0.5, se=FALSE, size = 2) +
  geom_point(size = 2.25) +
  geom_line(size = 1.25) +
  ylab("Share (%)") +
  xlab("Date") +
  labs(title = "Share of Remote Work Vacancy Postings (%)", subtitle = "USA Vac Weighted") +
  scale_x_date(breaks = as.Date(c("2014-01-01", "2015-01-01", "2016-01-01",
                                  "2017-01-01", "2018-01-01", "2019-01-01",
                                  "2020-01-01", "2021-01-01", "2022-01-01")),
               date_labels = '%Y') +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,2)) +
  coord_cartesian(ylim = c(0, 20)) +
  scale_colour_manual(values=cbbPalette) +
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
        panel.background = element_rect(fill = "white"))

p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/rwa_country_ts_w_us_2019vac_month.pdf", width = 8, height = 6)

remove(list = c("p", "p_egg"))


#### END ####
