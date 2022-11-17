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
df_nz <- fread("./int_data/df_nz_standardised.csv", nThread = 8, integer64 = "numeric")
df_aus <- fread("./int_data/df_aus_standardised.csv", nThread = 8, integer64 = "numeric")
df_can <- fread("./int_data/df_can_standardised.csv", nThread = 8, integer64 = "numeric")
df_uk <- fread("./int_data/df_uk_standardised.csv", nThread = 8, integer64 = "numeric")
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

df_all_list <- lapply(df_all_list, function(x) {
  x <- x %>% select(country, state, wfh_wham, job_date, bgt_occ, month, narrow_result, neg_narrow_result, job_id_weight)
  return(x)
})

save(df_all_list, file = "./aux_data/df_all_list.RData")
load(file = "./aux_data/df_all_list.RData")


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

# Raw Series Dictionary vs Wham
wfh_monthly_share_list <- lapply(df_all_list, function(x) {
  x <- x %>% select(country, wfh_wham, job_date, bgt_occ, month, narrow_result, neg_narrow_result, job_id_weight)
  x <- x[!is.na(wfh_wham)]
  x <- x[, month := factor(month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))]
  x <- x[, job_ymd := ymd(job_date)]
  x <- x[, year_month := as.yearmon(job_ymd)]
  x <- x[, year := year(job_ymd)]
  x <- setDT(x)
  x <- x[, narrow_result := ifelse(narrow_result == "", NA, narrow_result)]
  x <- x[, neg_narrow_result := ifelse(neg_narrow_result == "", NA, neg_narrow_result)]
  x <- x[, wfh_wham := ifelse(wfh_wham == "", NA, wfh_wham)]
  
  x <- x %>%
    select(country, job_date, year_month, year, wfh_wham, narrow_result, neg_narrow_result, job_id_weight) %>%
    setDT(.) %>%
    .[, .(wfh_wham_share = sum(job_id_weight*wfh_wham, na.rm = T)/sum(job_id_weight, na.rm = T),
          wfh_dict_share = sum(job_id_weight[!is.na(narrow_result)]*narrow_result[!is.na(narrow_result)], na.rm = T)/sum(job_id_weight[!is.na(narrow_result)], na.rm = T),
          wfh_dict_nn_share = sum(job_id_weight[!is.na(neg_narrow_result)]*neg_narrow_result[!is.na(neg_narrow_result)], na.rm = T)/sum(job_id_weight[!is.na(neg_narrow_result)], na.rm = T),          job_date = min(job_date)),
      by = .(country, year_month, year)] %>%
    setDT(.)
  
  return(x)
})

wfh_monthly_share <- rbindlist(wfh_monthly_share_list)
rm(wfh_monthly_share_list)

wfh_us_monthly_share <- wfh_monthly_share %>%
  .[country == "US"] %>%
  .[year >= 2019]

head(wfh_us_monthly_share)

wfh_us_monthly_share <- wfh_us_monthly_share %>%
  group_by(country, year_month, year, job_date) %>%
  pivot_longer(cols = wfh_wham_share:wfh_dict_nn_share) %>%
  setDT(.)

p = wfh_us_monthly_share %>%
  filter(name != "wfh_dict_nn_share") %>%
  mutate(measurement = ifelse(name == "wfh_dict_share", "Dictionary", "Our Method")) %>%
  filter(as.Date(year_month) <= ymd("2021-07-01")) %>%
  ggplot(., aes(x = as.Date(year_month), y = 100*value, col = measurement)) +
  #stat_smooth (geom="line", alpha=0.8, size=2, span=0.2) +
  #stat_smooth(span = 0.1, alpha=0.5, se=FALSE, size = 2) +
  geom_point(size = 2.25) +
  geom_line(size = 1.25) +
  ylab("Share (%)") +
  xlab("Date") +
  labs(title = "Share of Remote Work Vacancy Postings (%)") +
  scale_x_date(breaks = as.Date(c("2014-01-01", "2015-01-01", "2016-01-01",
                                  "2017-01-01", "2018-01-01", "2019-01-01",
                                  "2020-01-01", "2021-01-01", "2022-01-01")),
               date_labels = '%Y') +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,2)) +
  coord_cartesian(ylim = c(2, 26)) +
  #scale_colour_manual(values=cbbPalette) +
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
ggsave(p_egg, filename = "./plots/rwa_us_compare_methods_month.pdf", width = 8, height = 6)
remove(list = c("p", "p_egg"))

# Compare Occupations Dict
wfh_monthly_compare_occ_list <- lapply(df_all_list, function(x) {
  x <- x %>% select(country, wfh_wham, job_date, bgt_occ, month, narrow_result, neg_narrow_result, job_id_weight)
  x <- x[, bgt_occ5 := str_sub(bgt_occ, 1, 6)]
  x <- x[!is.na(wfh_wham)]
  x <- x[, month := factor(month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))]
  x <- x[, job_ymd := ymd(job_date)]
  x <- x[, year_month := as.yearmon(job_ymd)]
  x <- x[, year := year(job_ymd)]
  x <- setDT(x)
  x <- x[, narrow_result := ifelse(narrow_result == "", NA, narrow_result)]
  x <- x[, neg_narrow_result := ifelse(neg_narrow_result == "", NA, neg_narrow_result)]
  x <- x[, wfh_wham := ifelse(wfh_wham == "", NA, wfh_wham)]
  
  x <- x %>%
    select(country, job_date, bgt_occ5, year_month, year, wfh_wham, narrow_result, neg_narrow_result, job_id_weight) %>%
    setDT(.) %>%
    .[, .(wfh_wham_share = sum(job_id_weight*wfh_wham, na.rm = T)/sum(job_id_weight, na.rm = T),
          wfh_dict_share = sum(job_id_weight[!is.na(narrow_result)]*narrow_result[!is.na(narrow_result)], na.rm = T)/sum(job_id_weight[!is.na(narrow_result)], na.rm = T),
          wfh_dict_nn_share = sum(job_id_weight[!is.na(neg_narrow_result)]*neg_narrow_result[!is.na(neg_narrow_result)], na.rm = T)/sum(job_id_weight[!is.na(neg_narrow_result)], na.rm = T),          job_date = min(job_date)),
      by = .(country, year, bgt_occ5)] %>%
    setDT(.)
  
  return(x)
})

wfh_monthly_compare_occ <- rbindlist(wfh_monthly_compare_occ_list)
rm(wfh_monthly_compare_occ_list)

wfh_monthly_us_compare_occ <- wfh_monthly_compare_occ %>%
  .[country == "US"] %>%
  .[year == 2022] %>%
  .[, diff := wfh_wham_share - wfh_dict_nn_share]

View(wfh_monthly_us_compare_occ)

wfh_monthly_us_compare_occ <- wfh_monthly_us_compare_occ %>% .[bgt_occ5 %in% c("45-402", "15-115")]

View(wfh_monthly_us_compare_occ)

# Vacancy Weighted Share
wfh_daily_soc3_share_list <- lapply(df_all_list, function(x) {
  x <- x %>% select(country, wfh_wham, job_date, bgt_occ, month, narrow_result, neg_narrow_result, job_id_weight)
  x <- x[!is.na(wfh_wham)]
  x <- x[!is.na(bgt_occ)]
  x <- x[, bgt_occ3 := str_sub(bgt_occ, 1, 4)]
  x <- x[, month := factor(month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))]
  x <- x[, job_ymd := ymd(job_date)]
  x <- x[, year_month := as.yearmon(job_ymd)]
  x <- x[, year := year(job_ymd)]
  x <- setDT(x)
  
  x <- x %>%
    select(country, bgt_occ3, job_date, year_month, year, wfh_wham, job_id_weight) %>%
    setDT(.) %>%
    .[, .(wfh_sum = sum(job_id_weight*wfh_wham, na.rm = T), job_ads_sum = sum(job_id_weight, na.rm = T)), by = .(country, bgt_occ3, job_date, year_month, year)] %>%
    .[, wfh_share := wfh_sum / job_ads_sum] %>%
    setDT(.)
  
  return(x)
})

wfh_daily_soc3_share <- rbindlist(wfh_daily_soc3_share_list)
rm("wfh_daily_soc3_share_list")

wfh_daily_soc3_share <- wfh_daily_soc3_share %>%
  rename(daily_share = wfh_share, N = job_ads_sum)

fwrite(wfh_daily_soc3_share, file = "./int_data/country_soc3_daily_wfh.csv")

# State shares
wfh_daily_state_share_list <- lapply(df_all_list, function(x) {
  x <- x %>% select(country, state, wfh_wham, job_date, bgt_occ, month, narrow_result, neg_narrow_result, job_id_weight)
  x <- x[!is.na(wfh_wham)]
  x <- x[!is.na(bgt_occ)]
  x <- x[, month := factor(month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))]
  x <- x[, job_ymd := ymd(job_date)]
  x <- x[, year_month := as.yearmon(job_ymd)]
  x <- x[, year := year(job_ymd)]
  x <- setDT(x)
  
  x <- x %>%
    select(country, state, job_date, year_month, year, wfh_wham, job_id_weight) %>%
    setDT(.) %>%
    .[, .(wfh_sum = sum(job_id_weight*wfh_wham, na.rm = T), job_ads_sum = sum(job_id_weight, na.rm = T)), by = .(country, state, job_date, year_month, year)] %>%
    .[, wfh_share := wfh_sum / job_ads_sum] %>%
    setDT(.)
  
  return(x)
})

wfh_daily_state_share <- rbindlist(wfh_daily_state_share_list)
rm("wfh_daily_state_share_list")

wfh_daily_state_share <- wfh_daily_state_share %>%
  rename(daily_share = wfh_share, N = job_ads_sum)

fwrite(wfh_daily_state_share, file = "./int_data/country_state_daily_wfh.csv")

# Filter Unweighted
wfh_daily_share <- fread(file = "./int_data/country_daily_wfh.csv")

ts_for_plot <- wfh_daily_share %>%
  .[, job_date := ymd(job_date)] %>%
  .[as.yearmon(year_month) <= as.yearmon(ymd("20220601"))] %>%
  .[, l1o_monthly_mean := (sum(daily_share*N)-daily_share*N)/(sum(N) - N), by = .(country, year_month)] %>%
  .[, monthly_mean := sum(daily_share*N)/(sum(N)), by = .(country, year_month)] %>%
  .[, l1o_keep := ifelse(abs(monthly_mean - l1o_monthly_mean) > 0.02 | abs(log(monthly_mean/l1o_monthly_mean)) > 0.10, 0, 1)] %>%
  .[, l1o_keep := ifelse(as.yearmon(year_month) %in% as.yearmon(ymd(c("20200301", "20200401", "20200501", "20200601"))), 1, l1o_keep)] %>%
  .[, l1o_daily_with_nas := ifelse(l1o_keep == 1, daily_share, NA)]

head(ts_for_plot)

nrow(ts_for_plot)
dropped_days <- ts_for_plot %>%
  .[l1o_keep == 0] %>%
  select(country, job_date) %>%
  .[, drop := 1]
nrow(dropped_days)

check <- ts_for_plot %>%
  .[, .(days = .N, replaced = sum(1-l1o_keep)), by = .(country, year_month)] %>%
  .[, share_replaced := replaced/days]

(days_replaced <- 100*round(sum(check$replaced)/sum(check$days), digits = 4))

# Unweighted ts filter
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

head(ts_for_plot)



head(dropped_days)

ts_for_plot <- ts_for_plot %>%
  group_by(country, year_month, job_date) %>%
  pivot_longer(cols = c(monthly_mean:monthly_mean_3ma_l1o)) %>%
  setDT(.)

# Filter vacancy-weighted
wfh_daily_soc3_share <- fread(file = "./int_data/country_state_daily_wfh.csv")

dropped_days

nrow(wfh_daily_soc3_share) # 1119803
wfh_daily_soc3_share <- wfh_daily_soc3_share %>%
  mutate(job_date = ymd(job_date)) %>%
  left_join(dropped_days %>% mutate(job_date = ymd(job_date))) %>%
  setDT(.) %>%
  .[is.na(drop)] %>%
  select(-drop)
nrow(wfh_daily_soc3_share) # 1112512

# Reweight Vacancy weighted
head(wfh_daily_soc3_share)
shares_df <- wfh_daily_soc3_share %>%
  .[bgt_occ3 != ""] %>%
  .[year == 2019 & country == "US"] %>%
  .[, .(N = sum(N)), by = .(bgt_occ3)] %>%
  .[, share := N/sum(N)] %>%
  select(bgt_occ3, share)

head(shares_df)

nrow(wfh_daily_soc3_share) # 1112512
wfh_daily_soc3_share <- wfh_daily_soc3_share %>%
  left_join(shares_df) %>%
  setDT(.)
nrow(wfh_daily_soc3_share) # 1112512

ts_for_plot_us_weight <- wfh_daily_soc3_share %>%
  .[, job_date := ymd(job_date)] %>%
  .[as.yearmon(year_month) <= as.yearmon(ymd("20220601"))] %>%
  .[, .(job_date = min(job_date), monthly_mean = sum(daily_share*N, na.rm = T)/(sum(N, na.rm = T)), share = share[1]), by = .(country, bgt_occ3, year_month)] %>%
  .[, .(monthly_mean = sum(monthly_mean*share, na.rm = T)), by = .(country, year_month)]

View(ts_for_plot_us_weight)

# Plot unweighted
cbbPalette <- c("#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00")

cbbPalette[1]

(country_list <- unique(ts_for_plot$country))

# Unweighted
p = ts_for_plot %>%
  filter(name == "monthly_mean_l1o") %>%
  ggplot(., aes(x = as.Date(year_month), y = 100*value, col = country)) +
  #stat_smooth (geom="line", alpha=0.8, size=2, span=0.2) +
  #stat_smooth(span = 0.1, alpha=0.5, se=FALSE, size = 2) +
  geom_point(size = 2.25) +
  geom_line(size = 1.25) +
  ylab("Share (%)") +
  xlab("Date") +
  labs(title = "Share of Remote Work Vacancy Postings (%)", subtitle = paste0("Unweighted, Outliers Removed. Removed ",days_replaced, "% of days across all data")) +
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
ggsave(p_egg, filename = "./plots/rwa_country_ts_w_unweighted_month.pdf", width = 8, height = 6)
remove(list = c("p", "p_egg"))
head(ts_for_plot_us_weight)

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
  labs(title = "Share of Remote Work Vacancy Postings (%)", subtitle = paste0("US 2019 Vac Weighted, Outliers Removed. Removed")) +
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
ggsave(p_egg, filename = "./plots/rwa_country_ts_w_us_vac_month.pdf", width = 8, height = 6)
remove(list = c("p", "p_egg"))

# Time series by US division
remove(list = setdiff(ls(), "df_all_list"))
wfh_daily_state_share <- fread(file = "./int_data/country_state_daily_wfh.csv")

divisions <- fread(file = "./aux_data/us census bureau regions and divisions.csv") %>% clean_names

divisions$state[!(divisions$state %in% wfh_daily_state_share$state)]

nrow(wfh_daily_state_share) # 244506
wfh_daily_state_share <- wfh_daily_state_share %>%
  left_join(divisions) %>%
  setDT(.)
nrow(wfh_daily_state_share) # 244506
head(wfh_daily_state_share)
ts_for_plot <- wfh_daily_state_share %>%
  setDT(.) %>%
  .[country == "US"] %>%
  .[, .(daily_share = sum(wfh_sum)/sum(N), N = sum(N)), by = .(job_date, year_month, year, division)] %>%
  .[, job_date := ymd(job_date)] %>%
  .[as.yearmon(year_month) <= as.yearmon(ymd("20220601"))] %>%
  .[, l1o_monthly_mean := (sum(daily_share*N)-daily_share*N)/(sum(N) - N), by = .(division, year_month)] %>%
  .[, monthly_mean := sum(daily_share*N)/(sum(N)), by = .(division, year_month)] %>%
  .[, l1o_keep := ifelse(abs(monthly_mean - l1o_monthly_mean) > 0.02 | abs(log(monthly_mean/l1o_monthly_mean)) > 0.10, 0, 1)] %>%
  .[, l1o_keep := ifelse(as.yearmon(year_month) %in% as.yearmon(ymd(c("20200301", "20200401", "20200501", "20200601"))), 1, l1o_keep)] %>%
  .[, l1o_daily_with_nas := ifelse(l1o_keep == 1, daily_share, NA)] %>%
  .[order(division, job_date)] %>%
  .[, daily_share_l10 := na.approx(l1o_daily_with_nas, rule = 2), by = .(division)] %>%
  .[, monthly_mean_l1o := sum(daily_share_l10[!is.na(daily_share_l10)]*N[!is.na(daily_share_l10)], na.rm = T)/(sum(N[!is.na(daily_share_l10)], na.rm = T)),
    by = .(division, year_month)] %>%
  .[, .(job_date = min(job_date),
        monthly_mean = monthly_mean[1],
        monthly_mean_l1o = monthly_mean_l1o[1]),
    by = .(division, year_month)] %>%
  .[, monthly_mean_3ma := rollmean(monthly_mean, k = 3, align = "right", fill = NA), by = .(division)] %>%
  .[, monthly_mean_3ma_l1o := rollmean(monthly_mean_l1o, k = 3, align = "right", fill = NA), by = .(division)] %>%
  .[, monthly_mean_l1o := ifelse(as.yearmon(year_month) %in% as.yearmon(ymd(c("20200301", "20200401", "20200501", "20200601"))), monthly_mean, monthly_mean_l1o)] %>%
  .[, monthly_mean_3ma := ifelse(as.yearmon(year_month) %in% as.yearmon(ymd(c("20200301", "20200401", "20200501", "20200601"))), monthly_mean, monthly_mean_3ma)] %>%
  .[, monthly_mean_3ma_l1o := ifelse(as.yearmon(year_month) %in% as.yearmon(ymd(c("20200301", "20200401", "20200501", "20200601"))), monthly_mean, monthly_mean_3ma_l1o)]

head(ts_for_plot)

ts_for_plot <- ts_for_plot %>%
  group_by(division, year_month, job_date) %>%
  pivot_longer(cols = c(monthly_mean:monthly_mean_3ma_l1o)) %>%
  setDT(.)

ts_for_plot <- ts_for_plot %>% .[!is.na(division)]

uniqueN(divisions$division)

p = ts_for_plot %>%
  .[year(as.Date(as.yearmon(year_month)))>= 2019] %>%
  filter(name == "monthly_mean_3ma_l1o") %>%
  ggplot(., aes(x = as.Date(as.yearmon(year_month)), y = 100*value, colour = division, shape = division)) +
  #stat_smooth (geom="line", alpha=0.8, size=2, span=0.2) +
  #stat_smooth(span = 0.1, alpha=0.5, se=FALSE, size = 2) +
  geom_point(size = 3) +
  geom_line(size = 1) +
  ylab("Share (%)") +
  xlab("Date") +
  labs(title = "Share of Remote Work Vacancy Postings (%)", subtitle = paste0("Unweighted, Outliers Removed. Removed ")) +
  scale_x_date(date_breaks = "3 months",
               date_labels = '%Y-%m') +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,2)) +
  coord_cartesian(ylim = c(2, 14)) +
  scale_colour_brewer(palette = "Paired") +
  scale_shape_manual(values=c(15,16,17,18,19,15,16,17,18,19)) +
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
ggsave(p_egg, filename = "./plots/rwa_us_region_ts_w_unweighted_3ma.pdf", width = 8, height = 6)
remove(list = c("p", "p_egg"))

# City-level comparisons

# load city plots
head(ts_for_plot)

View(ts_for_plot)

ts_for_plot_cit <- ts_for_plot %>%
  .[grepl("Odessa|Miami Beach|Savannah|Birm|Boston|Cleveland|Columbus|Des Moines|Indianapolis|Jacksonville|Louisville|Memphis|New York|Oklahoma City|San Francisco|Wichita", city)] %>%
  .[name == "monthly_mean_3ma_l1o"] %>%
  .[country == "US"] %>%
  .[city_state != "Columbus, Georgia"]

sort(unique(ts_for_plot_cit$city_state))

remove(ts_for_plot)

View(ts_for_plot)

head(ts_for_plot_cit)

ts_for_plot_ag <- ts_for_plot %>%
  .[country == "US"] %>%
  .[name == "monthly_mean_3ma_l1o"] %>%
  .[, city_state := "US National"]

ts_for_plot_cit_national <- bind_rows(ts_for_plot_cit, ts_for_plot_ag)
View(ts_for_plot_cit_national)
divisions <- fread(file = "./aux_data/us census bureau regions and divisions.csv") %>% clean_names

nrow(ts_for_plot_cit_national)
ts_for_plot_cit_national <- ts_for_plot_cit_national %>%
  left_join(divisions)
nrow(ts_for_plot_cit_national)

View(ts_for_plot_cit_national)

unique(ts_for_plot_cit_national$region)

library("RColorBrewer")
(pal <- brewer.pal(7,"Dark2"))
pal[7] <- "black"

p = ts_for_plot_cit_national %>%
  .[year(as.Date(as.yearmon(year_month)))>= 2019] %>%
  filter(grepl("Odessa|Miami Beach|Savannah|New York|Jacksonville|Cleveland|Los Ang", city) | city_state == "US National") %>%
  filter(city != "North Miami Beach"  | city_state == "US National") %>%
  ggplot(., aes(x = as.Date(as.yearmon(year_month)), y = 100*value, colour = city_state, shape = city_state)) +
  #stat_smooth (geom="line", alpha=0.8, size=2, span=0.2) +
  #stat_smooth(span = 0.1, alpha=0.5, se=FALSE, size = 2) +
  geom_point(size = 3) +
  geom_line(size = 1) +
  ylab("Share (%)") +
  xlab("Date") +
  labs(title = "Share of Remote Work Vacancy Postings (%)", subtitle = paste0("Unweighted, Outliers Removed. Removed ")) +
  scale_x_date(date_breaks = "3 months",
               date_labels = '%Y-%m') +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,28,5)) +
  coord_cartesian(ylim = c(0, 20)) +
  scale_shape_manual(values=c(15,16,17,18,19,15,16,17,18,19)) +
  scale_color_manual(values = pal) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(nrow = 4, reverse = T)) +
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
ggsave(p_egg, filename = "./plots/other_cities.pdf", width = 8, height = 6)

remove(list = c("p", "p_egg"))

unique(ts_for_plot_cit_national$region)

p = ts_for_plot_cit_national %>%
  .[year(as.Date(as.yearmon(year_month)))>= 2019] %>%
  filter(region %in% c("Midwest", "Northeast") | city_state == "US National") %>%
  ggplot(., aes(x = as.Date(as.yearmon(year_month)), y = 100*value, colour = city_state, shape = city_state)) +
  #stat_smooth (geom="line", alpha=0.8, size=2, span=0.2) +
  #stat_smooth(span = 0.1, alpha=0.5, se=FALSE, size = 2) +
  geom_point(size = 3) +
  geom_line(size = 1) +
  ylab("Share (%)") +
  xlab("Date") +
  labs(title = "Share of Remote Work Vacancy Postings (%)", subtitle = paste0("Unweighted, Outliers Removed. Removed ")) +
  scale_x_date(date_breaks = "3 months",
               date_labels = '%Y-%m') +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,2)) +
  coord_cartesian(ylim = c(2, 30)) +
  scale_shape_manual(values=c(15,16,17,18,19,15,16,17,18,19)) +
  scale_color_manual(values = pal) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(nrow = 4, reverse = T)) +
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
ggsave(p_egg, filename = "./plots/san_fran_vs_south_san_fran.pdf", width = 8, height = 6)
remove(list = c("p", "p_egg"))



# OLD

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

#### PLOT by METHOD ####
remove(list = setdiff(ls(), "df_all"))
df_all$month <- factor(df_all$month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))
df_all$wfh <- df_all$wfh_wham
df_all$bgt_occ6 <- str_sub(df_all$bgt_occ, 1, 7)
df_all$bgt_occ2 <- as.numeric(str_sub(df_all$bgt_occ, 1, 2))
df_all$job_ymd <- ymd(df_all$job_date)
df_all$year_quarter <- as.yearqtr(df_all$job_ymd)
df_all$year_month <- as.yearmon(df_all$job_ymd)
df_all <- setDT(df_all)

# Time series bars
wfh_ag <- df_all %>%
  select(year, country, wfh, dict, neg, oecd_dict, tot_emp_ad, job_id_weight) %>%
  .[, .(share_bert_vac = sum(job_id_weight*wfh, na.rm = T)/sum(job_id_weight, na.rm = T),
        share_bert_emp = sum(tot_emp_ad*wfh, na.rm = T)/sum(tot_emp_ad, na.rm = T),
        share_long_dict_vac = sum(job_id_weight*dict, na.rm = T)/sum(job_id_weight, na.rm = T),
        share_long_dict_emp = sum(tot_emp_ad*dict, na.rm = T)/sum(tot_emp_ad, na.rm = T),
        share_dict_nn_vac = sum(job_id_weight*oecd_dict*(1-neg), na.rm = T)/sum(job_id_weight, na.rm = T),
        share_dict_nn_emp = sum(tot_emp_ad*oecd_dict*(1-neg), na.rm = T)/sum(tot_emp_ad, na.rm = T),
        share_dict_vac = sum(job_id_weight*oecd_dict, na.rm = T)/sum(job_id_weight, na.rm = T),
        share_dict_emp = sum(tot_emp_ad*oecd_dict, na.rm = T)/sum(tot_emp_ad, na.rm = T)),
    by = .(year, country)]

wfh_ag_ss_usa <- wfh_ag %>%
  .[country == "US"] %>%
  #mutate(share_dict_vac = ifelse(year_month < as.yearmon("Oct 2021"), share_dict_vac, NA)) %>%
  group_by(year) %>%
  pivot_longer(., cols = share_bert_vac:share_dict_emp, names_to = "measure", values_to = "share") %>%
  mutate(measure = case_when(
    measure == "share_bert_vac" ~ "WHAM",
    measure == "share_dict_vac" ~ "Dictionary",
    measure == "share_dict_nn_vac" ~ "Dictionary (non-neg)",
    TRUE ~ "")) %>%
  mutate(measure = factor(measure, levels = c("Dictionary (non-neg)", "Dictionary", "WHAM"))) %>%
  setDT(.)

# Plot
hr_pal <- c("grey39", "royalblue1", "navy")

p = wfh_ag_ss_usa %>%
  filter(year %in% c(2018, 2019, 2020, 2021)) %>%
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


# Time Series lines

wfh_ag <- df_all %>%
  select(year_month, country, wfh, dict, neg, oecd_dict, tot_emp_ad, job_id_weight) %>%
  .[, .(share_bert_vac = sum(job_id_weight*wfh, na.rm = T)/sum(job_id_weight, na.rm = T),
        share_bert_emp = sum(tot_emp_ad*wfh, na.rm = T)/sum(tot_emp_ad, na.rm = T),
        share_long_dict_vac = sum(job_id_weight*dict, na.rm = T)/sum(job_id_weight, na.rm = T),
        share_long_dict_emp = sum(tot_emp_ad*dict, na.rm = T)/sum(tot_emp_ad, na.rm = T),
        share_dict_nn_vac = sum(job_id_weight*oecd_dict*(1-neg), na.rm = T)/sum(job_id_weight, na.rm = T),
        share_dict_nn_emp = sum(tot_emp_ad*oecd_dict*(1-neg), na.rm = T)/sum(tot_emp_ad, na.rm = T),
        share_dict_vac = sum(job_id_weight*oecd_dict, na.rm = T)/sum(job_id_weight, na.rm = T),
        share_dict_emp = sum(tot_emp_ad*oecd_dict, na.rm = T)/sum(tot_emp_ad, na.rm = T)),
    by = .(year_month, country)]

wfh_ag_ss_usa <- wfh_ag %>%
  .[country == "US"] %>%
  #mutate(share_dict_vac = ifelse(year_month < as.yearmon("Oct 2021"), share_dict_vac, NA)) %>%
  group_by(year_month) %>%
  pivot_longer(., cols = share_bert_vac:share_dict_emp, names_to = "measure", values_to = "share") %>%
  mutate(measure = case_when(
    measure == "share_bert_vac" ~ "WHAM",
    measure == "share_dict_vac" ~ "Dictionary",
    measure == "share_dict_nn_vac" ~ "Dictionary (non-neg)",
    TRUE ~ "")) %>%
  mutate(measure = factor(measure, levels = c("Dictionary (non-neg)", "Dictionary", "WHAM"))) %>%
  setDT(.)

# Plot
hr_pal <- c("grey39", "royalblue1", "navy")

p = wfh_ag_ss_usa %>%
  #filter(year_month <= as.yearmon("Sep 2021")) %>%
  #filter(year_month > as.yearmon("Dec 2017")) %>%
  filter(measure != "") %>%
  ggplot(., aes(x = as.Date(year_month), y = share, colour = measure)) +
  geom_point(size = 2.25) +
  geom_line(size = 1.25) +
  ylab("Share (%)") +
  xlab("Date") +
  labs(title = "Share of Remote Work Vacancy Postings (%)", subtitle = "Vacancy Weighted, USA") +
  #scale_x_yearmon(format = '%Y-%-m', breaks = seq(from = as.yearmon("2014-1"), to = as.yearmon("2021-9"), by = 1),  limits = c(as.yearmon("2014-1"), as.yearmon("2021-9"))) +
  scale_x_date(breaks = as.Date(c("2014-01-01", "2015-01-01", "2016-01-01", "2017-01-01", "2018-01-01", "2019-01-01", "2020-01-01", "2021-01-01", "2022-01-01", "2022-01-01")),
               minor_breaks = as.Date(c("2014-01-01","2014-07-01","2015-01-01","2015-07-01","2016-01-01","2016-07-01","2017-01-01","2017-07-01",
                                        "2018-01-01","2018-07-01","2019-01-01","2019-07-01","2020-01-01","2020-07-01","2021-01-01","2021-07-01", "2022-01-01")),
               date_labels = '%Y') +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.025),  breaks = seq(0,0.3,0.05)) +
  coord_cartesian(ylim = c(0, 0.24)) +
  scale_colour_manual(values=hr_pal) +
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
ggsave(p_egg, filename = "./plots/horse_race_usa_lr_ts.pdf", width = 8, height = 6)

# SOC 15 #
remove(list = setdiff(ls(), "df_all"))

head(df_all)

wfh_ag_15 <- df_all %>%
  filter(country == "US") %>%
  filter(str_sub(bgt_occ6, 1, 2) == "15") %>%
  select(year_month, country, wfh, dict, neg, oecd_dict, tot_emp_ad, job_id_weight) %>%
  .[, .(share_bert_vac = sum(job_id_weight*wfh, na.rm = T)/sum(job_id_weight, na.rm = T),
        share_bert_emp = sum(tot_emp_ad*wfh, na.rm = T)/sum(tot_emp_ad, na.rm = T),
        share_long_dict_vac = sum(job_id_weight*dict, na.rm = T)/sum(job_id_weight, na.rm = T),
        share_long_dict_emp = sum(tot_emp_ad*dict, na.rm = T)/sum(tot_emp_ad, na.rm = T),
        share_dict_nn_vac = sum(job_id_weight*oecd_dict*(1-neg), na.rm = T)/sum(job_id_weight, na.rm = T),
        share_dict_nn_emp = sum(tot_emp_ad*oecd_dict*(1-neg), na.rm = T)/sum(tot_emp_ad, na.rm = T),
        share_dict_vac = sum(job_id_weight*oecd_dict, na.rm = T)/sum(job_id_weight, na.rm = T),
        share_dict_emp = sum(tot_emp_ad*oecd_dict, na.rm = T)/sum(tot_emp_ad, na.rm = T)),
    by = .(year_month, country)]

wfh_ag_ss_usa_15 <- wfh_ag_15 %>%
  .[country == "US"] %>%
  #mutate(share_dict_vac = ifelse(year_month < as.yearmon("Oct 2021"), share_dict_vac, NA)) %>%
  group_by(year_month) %>%
  pivot_longer(., cols = share_bert_vac:share_dict_emp, names_to = "measure", values_to = "share") %>%
  mutate(measure = case_when(
    measure == "share_bert_vac" ~ "WHAM",
    measure == "share_dict_vac" ~ "Dictionary",
    measure == "share_dict_nn_vac" ~ "Dictionary (non-neg)",
    TRUE ~ "")) %>%
  mutate(measure = factor(measure, levels = c("Dictionary (non-neg)", "Dictionary", "WHAM"))) %>%
  setDT(.)

# Plot
hr_pal <- c("grey39", "royalblue1", "navy")

p = wfh_ag_ss_usa_15 %>%
  #filter(year_month <= as.yearmon("Sep 2021")) %>%
  #filter(year_month > as.yearmon("Dec 2017")) %>%
  filter(measure != "") %>%
  ggplot(., aes(x = as.Date(year_month), y = share, colour = measure)) +
  geom_point(size = 2.25) +
  geom_line(size = 1.25) +
  ylab("Share (%)") +
  xlab("Date") +
  labs(title = "Share of WFH (Computer and Mathematical Occupations)", subtitle = "Vacancy Weighted, USA") +
  #scale_x_yearmon(format = '%Y-%-m', breaks = seq(from = as.yearmon("2014-1"), to = as.yearmon("2021-9"), by = 1),  limits = c(as.yearmon("2014-1"), as.yearmon("2021-9"))) +
  scale_x_date(breaks = as.Date(c("2014-01-01", "2015-01-01", "2016-01-01", "2017-01-01", "2018-01-01", "2019-01-01", "2020-01-01", "2021-01-01", "2022-01-01", "2022-01-01")),
               minor_breaks = as.Date(c("2014-01-01","2014-07-01","2015-01-01","2015-07-01","2016-01-01","2016-07-01","2017-01-01","2017-07-01",
                                        "2018-01-01","2018-07-01","2019-01-01","2019-07-01","2020-01-01","2020-07-01","2021-01-01","2021-07-01", "2022-01-01")),
               date_labels = '%Y') +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.025),  breaks = seq(0,0.3,0.05)) +
  coord_cartesian(ylim = c(0, 0.35)) +
  scale_colour_manual(values=hr_pal) +
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
ggsave(p_egg, filename = "./plots/horse_race_usa_lr_ts_comp_and_math.pdf", width = 8, height = 6)

# SOC 29, 31 #
remove(list = setdiff(ls(), "df_all"))

wfh_ag_29_31 <- df_all %>%
  filter(country == "US") %>%
  filter(str_sub(bgt_occ6, 1, 2) %in% c("29", "31")) %>%
  setDT(.) %>%
  select(year_month, wfh, dict, neg, oecd_dict, tot_emp_ad, job_id_weight) %>%
  setDT(.) %>%
  .[, .(share_bert_vac = sum(job_id_weight*wfh, na.rm = T)/sum(job_id_weight, na.rm = T),
        share_bert_emp = sum(tot_emp_ad*wfh, na.rm = T)/sum(tot_emp_ad, na.rm = T),
        share_long_dict_vac = sum(job_id_weight*dict, na.rm = T)/sum(job_id_weight, na.rm = T),
        share_long_dict_emp = sum(tot_emp_ad*dict, na.rm = T)/sum(tot_emp_ad, na.rm = T),
        share_dict_nn_vac = sum(job_id_weight*oecd_dict*(1-neg), na.rm = T)/sum(job_id_weight, na.rm = T),
        share_dict_nn_emp = sum(tot_emp_ad*oecd_dict*(1-neg), na.rm = T)/sum(tot_emp_ad, na.rm = T),
        share_dict_vac = sum(job_id_weight*oecd_dict, na.rm = T)/sum(job_id_weight, na.rm = T),
        share_dict_emp = sum(tot_emp_ad*oecd_dict, na.rm = T)/sum(tot_emp_ad, na.rm = T)),
    by = .(year_month)]

wfh_ag_ss_usa_29_31 <- wfh_ag_29_31 %>%
  #mutate(share_dict_vac = ifelse(year_month < as.yearmon("Oct 2021"), share_dict_vac, NA)) %>%
  group_by(year_month) %>%
  pivot_longer(., cols = share_bert_vac:share_dict_emp, names_to = "measure", values_to = "share") %>%
  mutate(measure = case_when(
    measure == "share_bert_vac" ~ "WHAM",
    measure == "share_dict_vac" ~ "Dictionary",
    measure == "share_dict_nn_vac" ~ "Dictionary (non-neg)",
    TRUE ~ "")) %>%
  mutate(measure = factor(measure, levels = c("Dictionary (non-neg)", "Dictionary", "WHAM"))) %>%
  setDT(.)

# Plot
hr_pal <- c("grey39", "royalblue1", "navy")

p = wfh_ag_ss_usa_29_31 %>%
  #filter(year_month <= as.yearmon("Sep 2021")) %>%
  #filter(year_month > as.yearmon("Dec 2017")) %>%
  filter(measure != "") %>%
  ggplot(., aes(x = as.Date(year_month), y = share, colour = measure)) +
  geom_point(size = 2.25) +
  geom_line(size = 1.25) +
  ylab("Share (%)") +
  xlab("Date") +
  labs(title = "Share of WFH (Health Occupations)", subtitle = "Vacancy Weighted, USA") +
  #scale_x_yearmon(format = '%Y-%-m', breaks = seq(from = as.yearmon("2014-1"), to = as.yearmon("2021-9"), by = 1),  limits = c(as.yearmon("2014-1"), as.yearmon("2021-9"))) +
  scale_x_date(breaks = as.Date(c("2014-01-01", "2015-01-01", "2016-01-01", "2017-01-01", "2018-01-01", "2019-01-01", "2020-01-01", "2021-01-01", "2022-01-01", "2022-01-01")),
               minor_breaks = as.Date(c("2014-01-01","2014-07-01","2015-01-01","2015-07-01","2016-01-01","2016-07-01","2017-01-01","2017-07-01",
                                        "2018-01-01","2018-07-01","2019-01-01","2019-07-01","2020-01-01","2020-07-01","2021-01-01","2021-07-01", "2022-01-01")),
               date_labels = '%Y') +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.025),  breaks = seq(0,0.3,0.05)) +
  coord_cartesian(ylim = c(0, 0.35)) +
  scale_colour_manual(values=hr_pal) +
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
ggsave(p_egg, filename = "./plots/horse_race_usa_lr_ts_health.pdf", width = 8, height = 6)

# Compare Narrow Occ
remove(list = setdiff(ls(), "df_all"))

head(df_all)

table(df_all$country)

sum(df_all$dic)

wfh_ag_narrow_occ <- df_all %>%
  filter(year >= 2020) %>%
  filter(bgt_occ6 %in% c("15-2041", "41-3011", "19-3022", "49-2095", "53-4031")) %>%
  setDT(.) %>%
  select(year_month, bgt_occ6, wfh, dict, neg, oecd_dict, tot_emp_ad, job_id_weight) %>%
  setDT(.) %>%
  .[, .(share_bert_vac = sum(job_id_weight*wfh, na.rm = T)/sum(job_id_weight, na.rm = T),
        share_bert_emp = sum(tot_emp_ad*wfh, na.rm = T)/sum(tot_emp_ad, na.rm = T),
        share_long_dict_vac = sum(job_id_weight*dict, na.rm = T)/sum(job_id_weight, na.rm = T),
        share_long_dict_emp = sum(tot_emp_ad*dict, na.rm = T)/sum(tot_emp_ad, na.rm = T),
        share_dict_nn_vac = sum(job_id_weight*oecd_dict*(1-neg), na.rm = T)/sum(job_id_weight, na.rm = T),
        share_dict_nn_emp = sum(tot_emp_ad*oecd_dict*(1-neg), na.rm = T)/sum(tot_emp_ad, na.rm = T),
        share_dict_vac = sum(job_id_weight*oecd_dict, na.rm = T)/sum(job_id_weight, na.rm = T),
        share_dict_emp = sum(tot_emp_ad*oecd_dict, na.rm = T)/sum(tot_emp_ad, na.rm = T)),
    by = .(bgt_occ6)]

wfh_ag_narrow_occ_ss <- wfh_ag_narrow_occ %>%
  #mutate(share_dict_vac = ifelse(year_month < as.yearmon("Oct 2021"), share_dict_vac, NA)) %>%
  pivot_longer(., cols = share_bert_vac:share_dict_emp, names_to = "measure", values_to = "share") %>%
  mutate(measure = case_when(
    measure == "share_bert_vac" ~ "WHAM",
    measure == "share_dict_vac" ~ "Dictionary",
    measure == "share_dict_nn_vac" ~ "Dictionary (non-neg)",
    TRUE ~ "")) %>%
  mutate(measure = factor(measure, levels = c("WHAM", "Dictionary", "Dictionary (non-neg)"))) %>%
  setDT(.) %>%
  .[!is.na(measure)]

head(wfh_ag_narrow_occ_ss)

#.[, colour := grepl("WHAM", measure)] %>%
#.[, line := grepl("Weight", measure)]

wfh_ag_narrow_occ_ss <- wfh_ag_narrow_occ_ss %>%
  mutate(occ_name = case_when(
    bgt_occ6 == "15-2041" ~ "Statisticians",
    bgt_occ6 == "19-3022" ~ "Survey Researchers",
    bgt_occ6 == "41-3011" ~ "Advertising Sales Agents",
    bgt_occ6 == "49-2095" ~ "Electrical & Electronics Repairs",
    bgt_occ6 == "53-4031" ~ "Railroad Conductors",
    TRUE ~ "NA"))


# Plot
hr_pal <- c("navy", "royalblue1", "grey39")

p = wfh_ag_narrow_occ_ss %>%
  ggplot(., aes(y = share, x = as.factor(occ_name), fill = measure)) +
  geom_bar(stat = "identity", position = position_dodge(), alpha = 0.9)  +
  ylab("Share (%)") +
  xlab("Occupation") +
  labs(title = "Comparison of Methods by Narrow Occupation") +
  #scale_y_continuous(labels = scales::number_format(accuracy = 0.025),  breaks = seq(0,0.3,0.05)) +
  #coord_cartesian(ylim = c(0, 0.24)) +
  scale_fill_manual(values = hr_pal) +
  #scale_shape_manual(values=rep(0:4, 3)) +
  theme(
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 0)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  guides(fill = guide_legend(ncol = 3)) +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 10))

p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/horse_race_narrow_occupations_2020onwards.pdf", width = 8, height = 6)


# COMPARE OCCS #
remove(list = setdiff(ls(), "df_all"))

soc2010_names <- fread(file = "./aux_data/us_soc_2010_names.csv")
soc2010_names$soc10_2d <- as.numeric(soc2010_names$soc10_2d)

df_all <- df_all %>%
  left_join(., soc2010_names, by = c("bgt_occ2" = "soc10_2d")) %>%
  setDT(.)
rm(soc2010_names)
df_all$name <- gsub("and", "&", df_all$name, fixed = T)
df_all$name <- gsub(" Occupations", "", df_all$name)
df_all <- df_all %>% filter(!is.na(df_all$name))
df_all <- setDT(df_all)
df_all$ussoc_2d_wn <- paste0(df_all$bgt_occ2, ". ",df_all$name)

df_all$tot_emp_ad

df_all_compare <- df_all %>%
  filter(year > 2020) %>%
  setDT(.) %>%
  .[, .(share_bert_vac = sum(job_id_weight*wfh, na.rm = T)/sum(job_id_weight, na.rm = T),
        share_bert_emp = sum(tot_emp_ad*wfh, na.rm = T)/sum(tot_emp_ad, na.rm = T),
        share_long_dict_vac = sum(job_id_weight*dict, na.rm = T)/sum(job_id_weight, na.rm = T),
        share_long_dict_emp = sum(tot_emp_ad*dict, na.rm = T)/sum(tot_emp_ad, na.rm = T),
        share_dict_nn_vac = sum(job_id_weight*oecd_dict*(1-neg), na.rm = T)/sum(job_id_weight, na.rm = T),
        share_dict_nn_emp = sum(tot_emp_ad*oecd_dict*(1-neg), na.rm = T)/sum(tot_emp_ad, na.rm = T),
        share_dict_vac = sum(job_id_weight*oecd_dict, na.rm = T)/sum(job_id_weight, na.rm = T),
        share_dict_emp = sum(tot_emp_ad*oecd_dict, na.rm = T)/sum(tot_emp_ad, na.rm = T)),
    by = .( bgt_occ6)]

df_all_compare$percent_diff_bert_neg_dict_vac <- (df_all_compare$share_bert_vac - df_all_compare$share_dict_nn_vac)/df_all_compare$share_bert_vac
df_all_compare$diff_bert_neg_dict_vac <- (df_all_compare$share_bert_vac - df_all_compare$share_dict_nn_vac)

df_all_compare$percent_diff_bert_neg_dict_emp <- (df_all_compare$share_bert_emp - df_all_compare$share_dict_nn_emp)/df_all_compare$share_bert_emp
df_all_compare$diff_bert_neg_dict_emp <- (df_all_compare$share_bert_emp - df_all_compare$share_dict_nn_emp)

df_all_compare <- df_all_compare %>%
  select(bgt_occ6, share_dict_nn_vac, share_dict_nn_emp, share_bert_vac, share_bert_emp, percent_diff_bert_neg_dict_vac,
         diff_bert_neg_dict_vac, percent_diff_bert_neg_dict_emp, diff_bert_neg_dict_emp)

# COMPARE ALL FOR EACH COUNTRY #

head(wfh_ag)

wfh_ag_ss_all_countries <- wfh_ag %>%
  mutate(share_dict_vac = ifelse(year_month < as.yearmon("Oct 2021"), share_dict_vac, NA)) %>%
  group_by(year_month, country) %>%
  pivot_longer(., cols = share_bert_vac:share_dict_emp,  names_to = "measure", values_to = "share") %>%
  mutate(measure = case_when(
    measure == "share_bert_vac" ~ "WHAM",
    measure == "share_bert_emp" ~ "WHAM (Emp Weights)",
    measure == "share_long_dict_vac" ~ "Large Dict.",
    measure == "share_long_dict_emp" ~ "Large Dict. (Emp Weights)",
    measure == "share_dict_nn_vac" ~ "Dict. w/ Negation",
    measure == "share_dict_nn_emp" ~ "Dict. w/ Negation (Emp Weights)",
    measure == "share_dict_vac" ~ "Narrow Dict.",
    measure == "share_dict_emp" ~ "Narrow Dict. (Emp Weights)",
  )) %>%
  #mutate(measure = factor(measure, levels = c("Dictionary", "WHAM", "WHAM (Employment Weights)"))) %>%
  setDT(.) %>%
  .[, colour := grepl("WHAM", measure)] %>%
  .[, line := grepl("Weight", measure)]

hr_pal2 <- c("grey39", "navy", "deepskyblue", "royalblue1")

p_us = wfh_ag_ss_all_countries %>%
  filter(country == "US") %>%
  filter(grepl("Weights", measure)) %>%
  ggplot(., aes(x = as.Date(year_month), y = share, colour = measure)) +
  geom_point(size = 2.25) +
  geom_line(size = 1.25) +
  ylab("Share (%)") +
  xlab("Date") +
  labs(subtitle = "Employment Weighted, USA") +
  #scale_x_yearmon(format = '%Y-%-m', breaks = seq(from = as.yearmon("2014-1"), to = as.yearmon("2021-9"), by = 1),  limits = c(as.yearmon("2014-1"), as.yearmon("2021-9"))) +
  scale_x_date(limits = as.Date(c("2018-01-01", "2022-01-01")), breaks = as.Date(c("2018-01-01", "2019-01-01", "2020-01-01", "2021-01-01", "2022-01-01", "2022-01-01")),
               minor_breaks = as.Date(c("2018-01-01","2018-07-01","2019-01-01","2019-07-01","2020-01-01","2020-07-01","2021-01-01","2021-07-01", "2022-01-01")),
               date_labels = '%Y') +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.025),  breaks = seq(0,0.3,0.05)) +
  coord_cartesian(ylim = c(0, 0.24)) +
  scale_colour_manual(values=hr_pal2) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(nrow = 2)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm"))

p_uk = wfh_ag_ss_all_countries %>%
  filter(country == "UK") %>%
  filter(grepl("Weights", measure)) %>%
  ggplot(., aes(x = as.Date(year_month), y = share, colour = measure)) +
  geom_point(size = 2.25) +
  geom_line(size = 1.25) +
  ylab("Share (%)") +
  xlab("Date") +
  labs(subtitle = "Employment Weighted, UK") +
  #scale_x_yearmon(format = '%Y-%-m', breaks = seq(from = as.yearmon("2014-1"), to = as.yearmon("2021-9"), by = 1),  limits = c(as.yearmon("2014-1"), as.yearmon("2021-9"))) +
  scale_x_date(limits = as.Date(c("2018-01-01", "2022-01-01")), breaks = as.Date(c("2018-01-01", "2019-01-01", "2020-01-01", "2021-01-01", "2022-01-01", "2022-01-01")),
               minor_breaks = as.Date(c("2018-01-01","2018-07-01","2019-01-01","2019-07-01","2020-01-01","2020-07-01","2021-01-01","2021-07-01", "2022-01-01")),
               date_labels = '%Y') +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.025),  breaks = seq(0,0.3,0.05)) +
  coord_cartesian(ylim = c(0, 0.24)) +
  scale_colour_manual(values=hr_pal2) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(nrow = 2)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm"))

p_can = wfh_ag_ss_all_countries %>%
  filter(country == "Canada") %>%
  filter(grepl("Weights", measure)) %>%
  ggplot(., aes(x = as.Date(year_month), y = share, colour = measure)) +
  geom_point(size = 2.25) +
  geom_line(size = 1.25) +
  ylab("Share (%)") +
  xlab("Date") +
  labs(subtitle = "Employment Weighted, Canada") +
  #scale_x_yearmon(format = '%Y-%-m', breaks = seq(from = as.yearmon("2014-1"), to = as.yearmon("2021-9"), by = 1),  limits = c(as.yearmon("2014-1"), as.yearmon("2021-9"))) +
  scale_x_date(limits = as.Date(c("2018-01-01", "2022-01-01")), breaks = as.Date(c("2018-01-01", "2019-01-01", "2020-01-01", "2021-01-01", "2022-01-01", "2022-01-01")),
               minor_breaks = as.Date(c("2018-01-01","2018-07-01","2019-01-01","2019-07-01","2020-01-01","2020-07-01","2021-01-01","2021-07-01", "2022-01-01")),
               date_labels = '%Y') +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.025),  breaks = seq(0,0.3,0.05)) +
  coord_cartesian(ylim = c(0, 0.24)) +
  scale_colour_manual(values=hr_pal2) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(nrow = 2)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm"))

p_aus = wfh_ag_ss_all_countries %>%
  filter(country == "Australia") %>%
  filter(grepl("Weights", measure)) %>%
  ggplot(., aes(x = as.Date(year_month), y = share, colour = measure)) +
  geom_point(size = 2.25) +
  geom_line(size = 1.25) +
  ylab("Share (%)") +
  xlab("Date") +
  labs(subtitle = "Employment Weighted, Australia") +
  #scale_x_yearmon(format = '%Y-%-m', breaks = seq(from = as.yearmon("2014-1"), to = as.yearmon("2021-9"), by = 1),  limits = c(as.yearmon("2014-1"), as.yearmon("2021-9"))) +
  scale_x_date(limits = as.Date(c("2018-01-01", "2022-01-01")), breaks = as.Date(c("2018-01-01", "2019-01-01", "2020-01-01", "2021-01-01", "2022-01-01", "2022-01-01")),
               minor_breaks = as.Date(c("2018-01-01","2018-07-01","2019-01-01","2019-07-01","2020-01-01","2020-07-01","2021-01-01","2021-07-01", "2022-01-01")),
               date_labels = '%Y') +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.025),  breaks = seq(0,0.3,0.05)) +
  coord_cartesian(ylim = c(0, 0.24)) +
  scale_colour_manual(values=hr_pal2) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(nrow = 2)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm"))

p_nz = wfh_ag_ss_all_countries %>%
  filter(country == "NZ") %>%
  filter(grepl("Weights", measure)) %>%
  ggplot(., aes(x = as.Date(year_month), y = share, colour = measure)) +
  geom_point(size = 2.25) +
  geom_line(size = 1.25) +
  ylab("Share (%)") +
  xlab("Date") +
  labs(subtitle = "Employment Weighted, NZ") +
  #scale_x_yearmon(format = '%Y-%-m', breaks = seq(from = as.yearmon("2014-1"), to = as.yearmon("2021-9"), by = 1),  limits = c(as.yearmon("2014-1"), as.yearmon("2021-9"))) +
  scale_x_date(limits = as.Date(c("2018-01-01", "2022-01-01")), breaks = as.Date(c("2018-01-01", "2019-01-01", "2020-01-01", "2021-01-01", "2022-01-01", "2022-01-01")),
               minor_breaks = as.Date(c("2018-01-01","2018-07-01","2019-01-01","2019-07-01","2020-01-01","2020-07-01","2021-01-01","2021-07-01", "2022-01-01")),
               date_labels = '%Y') +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.025),  breaks = seq(0,0.3,0.05)) +
  coord_cartesian(ylim = c(0, 0.24)) +
  scale_colour_manual(values=hr_pal2) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(nrow = 2)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm"))

library("patchwork")

combined <- p_can + p_uk + p_aus + p_nz + p_us & theme(legend.position = "bottom")
combined + plot_layout(guides = "collect")

#### END ####

#### PROB DIST BERT ####
remove(list = ls())

df_seq_us <- fread(file = "./int_data/us_seq_level_wfh_measures.csv", nThread = 8, select = c("job_id", "wfh_prob", "country", "nfeat"))
df_seq_uk <- fread(file = "./int_data/uk_seq_level_wfh_measures.csv", nThread = 8, select = c("job_id", "wfh_prob", "country", "nfeat"))
df_seq_anz <- fread(file = "./int_data/anz_seq_level_wfh_measures.csv", nThread = 8, select = c("job_id", "wfh_prob", "country", "nfeat"))
df_seq_can <- fread(file = "./int_data/can_seq_level_wfh_measures.csv", nThread = 8, select = c("job_id", "wfh_prob", "country", "nfeat"))

df_seq_us$job_id <- as.numeric(df_seq_us$job_id)
df_seq_uk$job_id <- as.numeric(df_seq_uk$job_id)
df_seq_anz$job_id <-as.numeric(df_seq_anz$job_id)
df_seq_can$job_id <-as.numeric(df_seq_can$job_id)

df_seq_us$job_id <- as.character(paste0("us",df_seq_us$job_id))
df_seq_uk$job_id <- as.character(paste0("uk",df_seq_uk$job_id))
df_seq_anz$job_id <-as.character(paste0("anz",df_seq_anz$job_id))
df_seq_can$job_id <-as.character(paste0("can",df_seq_can$job_id))

df_seq <- bind_rows(df_seq_us,df_seq_uk,df_seq_anz,df_seq_can)

df_prob_dist <- df_seq %>%
  .[, wfh_prob_bin :=  as.numeric(cut(wfh_prob,seq(0,1,0.02),include.lowest=TRUE))/50] %>%
  .[, .(n_seq = .N), by = wfh_prob_bin] %>%
  .[, prob_seq := n_seq/sum(n_seq)] %>%
  .[, ln_n_seq := log(n_seq)] %>%
  .[, ln_prob_seq := log(prob_seq)]

summary(df_prob_dist$prob_seq)

eps <- 1e-8
tn <- trans_new("logpeps",
                function(x) log(x+eps),
                function(y) exp(y)-eps,
                domain=c(0, Inf),
                breaks=c(0, 0.1, 1))

b <- 10^-c(Inf, 8:0)

max(df_prob_dist$prob_seq)

p = df_prob_dist %>%
  ggplot(., aes(x = wfh_prob_bin, y = prob_seq)) +
  geom_bar(stat = "identity", position = position_dodge(), alpha = 0.9, fill = "darkgrey")  +
  ylab("Mass (Exponential Scale)") +
  xlab("WHAM Probability") +
  labs(title = "WHAM Probability Distribution (Sequence Level)") +
  #scale_y_continuous(labels = scales::number_format(accuracy = 0.025),  breaks = seq(0,0.3,0.05)) +
  #coord_cartesian(ylim = c(0, 0.24)) +
  #scale_fill_manual(values = mycolors) +
  #scale_shape_manual(values=rep(0:4, 3)) +
  theme(
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 0)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  guides(fill = guide_legend(ncol = 3)) +
  coord_trans(y = tn) +
  scale_y_continuous(breaks = c(0,0.000001,0.00001,0.0001,0.001,0.01,0.1,0.98))
p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/wham_sequence_dist.pdf", width = 8, height = 6)

remove(p)
remove(p_egg)
remove(df_prob_dist)

# Summary Stats
df_job_ad <- df_seq %>%
  setDT(.) %>%
  .[, .(wfh_hits = sum(wfh_prob>0.5)), by = job_id]

df_job_ad_bar <- df_job_ad %>%
  .[, .(.N), by = wfh_hits] %>%
  .[, share := N/sum(N)] %>%
  .[order(wfh_hits)]

df_job_ad_bar_over5 <- df_job_ad_bar %>%
  .[wfh_hits > 5] %>%
  .[, .(wfh_hits = ">5", N = sum(N), share = sum(share))]

df_job_ad_bar_final <- rbind(df_job_ad_bar[wfh_hits<=5],df_job_ad_bar_over5)

df_job_ad_bar_final$share <- df_job_ad_bar_final$share*100

stargazer(df_job_ad_bar_final, title = "Positive Sequences per Job Ad", summary = F, rownames = F)

View(df_job_ad_bar_final)

head(df_seq)


# Summary Stats
df_word_per_post <- df_seq %>%
  setDT(.) %>%
  .[, n_words := sum(nfeat)] %>%
  .[, .(word_per_ad = sum(nfeat), n_words = n_words[1]), by = .(country, job_id)] %>%
  .[, .("Mean" = mean(word_per_ad),
        "Median" = median(word_per_ad),
        "SD" = sd(word_per_ad),
        "Numerator N" = n_words[1],
        "Denominator N" = .N)] %>%
  .[, measure := "Words per Job Ad"] %>%
  select(measure, everything())

df_seq_per_post <- df_seq %>%
  setDT(.) %>%
  .[, n_seq := .N] %>%
  .[, .(seq_per_ad = .N, n_seq = n_seq[1]), by = .(country, job_id)] %>%
  .[, .("Mean" = mean(seq_per_ad),
        "Median" = median(seq_per_ad),
        "SD" = sd(seq_per_ad),
        "Numerator N" = n_seq[1],
        "Denominator N" = .N)] %>%
  .[, measure := "Sequences per Job Ad"] %>%
  select(measure, everything())

df_feat_per_seq <- df_seq %>%
  setDT(.) %>%
  .[, .("Mean" = mean(nfeat),
        "Median" = median(nfeat),
        "SD" = sd(nfeat),
        "Numerator N" = sum(nfeat, na.rm = T),
        "Denominator N" = .N)] %>%
  .[, measure := "Words per Sequence"] %>%
  select(measure, everything())

seq_stats <- bind_rows(df_word_per_post, df_seq_per_post, df_feat_per_seq)

stargazer(seq_stats, type = "latex", title = "Document Feature Distributions", summary = F, rownames = F, digits = 2, font.size = "footnotesize")

remove(list = ls())
#### END ####

#### OCCUPATION BREAK DOWN #### ####
remove(list = setdiff(ls(), "df_all_list"))

wfh_occ_ag_list <- lapply(df_all_list, function(x) {
  x <- x[!is.na(wfh_wham)]
  x <- x[!is.na(bgt_occ)]
  x <- x[bgt_occ != ""]
  x <- x[, wfh := wfh_wham]
  x <- x[, bgt_occ2 := str_sub(bgt_occ, 1, 2)]
  x <- x[, job_ymd := ymd(job_date)]
  x <- x[, year := year(job_ymd)]
  x <- setDT(x)
  
  wfh_occ_ag <- x %>%
    select(country, year, wfh, tot_emp_ad, job_id_weight, bgt_occ2) %>%
    setDT(.) %>%
    .[, .(wfh_share = sum(job_id_weight*wfh, na.rm = T)/sum(job_id_weight, na.rm = T),
          tot_vac = sum(job_id_weight, na.rm = T),
          tot_emp = sum(tot_emp_ad, na.rm = T)),
      by = .(country, year, bgt_occ2)] %>%
    setDT(.)
  
  return(wfh_occ_ag)
})

wfh_occ_ag <- rbindlist(wfh_occ_ag_list, fill = TRUE)

wfh_occ_ag <- wfh_occ_ag %>%
  .[year %in% c(2019, 2021)] %>%
  .[, .(wfh_share = sum(tot_vac*wfh_share, na.rm = T)/sum(tot_vac, na.rm = T)),
    by = .(year, bgt_occ2)]

soc2010_names <- fread(file = "./aux_data/us_soc_2010_names.csv")
soc2010_names$soc10_2d <- as.numeric(soc2010_names$soc10_2d)
wfh_occ_ag$bgt_occ2 <- as.numeric(wfh_occ_ag$bgt_occ2)
nrow(wfh_occ_ag)
wfh_occ_ag <- wfh_occ_ag %>%
  left_join(., soc2010_names, by = c("bgt_occ2" = "soc10_2d")) %>%
  setDT(.)
nrow(wfh_occ_ag)
rm(soc2010_names)
wfh_occ_ag$name <- gsub("and", "&", wfh_occ_ag$name, fixed = T)
wfh_occ_ag$name <- gsub(" Occupations", "", wfh_occ_ag$name)
wfh_occ_ag$name <- gsub(", Sports, & Media| & Technical|, & Repair|Cleaning &|& Serving Related", "", wfh_occ_ag$name)
wfh_occ_ag <- wfh_occ_ag %>% filter(!is.na(wfh_occ_ag$name))
wfh_occ_ag <- setDT(wfh_occ_ag)
wfh_occ_ag$ussoc_2d_wn <- paste0(wfh_occ_ag$name)
wfh_occ_ag
#### BAR PLOT 2021 vs 2019
wfh_occ_ag <- wfh_occ_ag %>%
  group_by(ussoc_2d_wn) %>%
  mutate(prop_growth = ifelse(!is.na(lag(wfh_share)), paste0(round((wfh_share)/lag(wfh_share),1),"X"), NA)) %>%
  ungroup() %>%
  setDT(.)

wfh_occ_ag$wfh_share_index <- 100*wfh_occ_ag$wfh_share_index

cbbPalette <- c("#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00")
cbbPalette_oc <- c("#000000", "#56B4E9")
cbbPalette_ind <- c("#000000", "#F0E442")

wfh_occ_ag

p = wfh_occ_ag %>%
  #filter(ussoc_2d_wn < 27) %>%
  mutate(ussoc_2d_wn = fct_reorder(ussoc_2d_wn, wfh_share, .desc = FALSE)) %>%
  ggplot(., aes(x = ussoc_2d_wn, y = wfh_share, fill = as.factor(year))) +
  geom_bar(stat = "identity", width=1, position = position_dodge(width=0.8))  +
  geom_text(aes(label = prop_growth, family = "serif"), size = 5, vjust = 0, colour = "black", hjust = -0.5) +
  ylab("Share (%)") +
  labs(title = "WFH Share Distribution by Occupation / Year", subtitle = "Employment Weighted, Global") +
  scale_y_continuous(breaks = seq(0,4,0.05), limits = c(0,0.32)) +
  #             labels=format(df_all_occ$date_fake,
  #                           format="%Y")) +
  #             minor_breaks = as.Date(c("2019-01-01","2019-07-01","2020-01-01","2020-07-01","2021-01-01","2021-07-01", "2022-01-01")),
  #             date_labels = '%Y') +
  #scale_y_continuous(labels = scales::number_format(accuracy = 0.025),  breaks = seq(0,0.3,0.05)) +
  #coord_cartesian(ylim = c(0, 0.24)) +
  scale_fill_manual(values = cbbPalette_oc) +
  #scale_shape_manual(values=rep(0:4, 3)) +
  theme(
    #axis.title.x=element_blank(),
    axis.title.y=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 0),
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm"),
        axis.text.y = element_text(hjust=0),
        legend.position = c(.7, .05)) +
  guides(fill = guide_legend(ncol = 2)) +
  coord_flip() +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 60))
p
p_egg <- set_panel_size(p = p,
                        width = unit(3.5, "in"),
                        height = unit(22*0.65, "cm"))
ggsave(p_egg, filename = "./plots/occ_dist_alt.pdf", width = 9, height = 22*0.65+3)

#### END ####

#### OCCUPATION SCATTER PLOTS ####
# Prepare Data
remove(list = setdiff(ls(), "df_all_list"))

wfh_occ_ag_list <- lapply(df_all_list, function(x) {
  x <- x[!is.na(wfh_wham)]
  x <- x[!is.na(bgt_occ)]
  x <- x[bgt_occ != ""]
  x <- x[, wfh := wfh_wham]
  x <- x[, bgt_occ6 := str_sub(bgt_occ, 1, 7)]
  x <- x[, job_ymd := ymd(job_date)]
  x <- x[, year := year(job_ymd)]
  x <- setDT(x)
  
  wfh_occ_ag <- x %>%
    select(country, year, wfh, tot_emp_ad, job_id_weight, bgt_occ6) %>%
    setDT(.) %>%
    .[year %in% c(2019, 2021)] %>%
    .[, .(wfh_share = sum(job_id_weight*wfh, na.rm = T)/sum(job_id_weight, na.rm = T),
          tot_vac = sum(job_id_weight, na.rm = T),
          tot_emp = sum(tot_emp_ad, na.rm = T)),
      by = .(country, year, bgt_occ6)] %>%
    setDT(.)
  
  return(wfh_occ_ag)
})

remove(list = setdiff(ls(), c("wfh_occ_ag_list", "df_all_list")))

wfh_occ_ag <- rbindlist(wfh_occ_ag_list, fill = TRUE)

wfh_occ_ag

wfh_occ_ag <- wfh_occ_ag %>%
  .[year %in% c(2019, 2021)] %>%
  .[, .(wfh_share = sum(tot_vac*wfh_share, na.rm = T)/sum(tot_vac, na.rm = T),
        tot_vac = sum(tot_vac)),
    by = .(year, bgt_occ6)]

occupations_workathome <- read_csv("./aux_data/occupations_workathome.csv") %>% rename(onet = onetsoccode)

occupations_workathome <- occupations_workathome %>%
  setDT(.) %>%
  .[, bgt_occ6 := str_sub(onet, 1, 7)] %>%
  .[, .(teleworkable = as.numeric(mean(teleworkable)>0.5)),
    by = .(bgt_occ6)]

wfh_occ_ag
occupations_workathome

wfh_occ_ag <- wfh_occ_ag %>%
  left_join(occupations_workathome)

wfh_occ_ag <- wfh_occ_ag %>% .[!is.na(teleworkable) & !is.na(bgt_occ6)]

names <- fread(file = "./aux_data/bgt_soc6_names.csv")

names

names$name <- gsub("and", "&", names$name, fixed = T)
names$name <- gsub("(.*?),.*", "\\1", names$name)

names

class(wfh_occ_ag$bgt_occ6)
class(names$bgt_occ6)

wfh_occ_ag <- wfh_occ_ag %>%
  left_join(names)

wfh_occ_ag <- wfh_occ_ag %>% .[!is.na(name) & !is.na(bgt_occ6)]

nrow(wfh_occ_ag)

wfh_occ_ag <- wfh_occ_ag %>%
  unique(., by = c("bgt_occ6", "year"))

nrow(wfh_occ_ag)

wfh_occ_ag <- wfh_occ_ag %>%
  group_by(name, bgt_occ6, teleworkable) %>%
  pivot_wider(., names_from = year, values_from = c("wfh_share", "tot_vac")) %>%
  rename(n_post_2019 = tot_vac_2019,
         n_post_2021 = tot_vac_2021)

df <- wfh_occ_ag

remove(wfh_occ_ag)

library(ggrepel)
set.seed(999)

df <- df %>%
  setDT(.) %>%
  .[!is.na(wfh_share_2019) & !is.na(wfh_share_2021)]

max(df$wfh_share_2019[df$n_post_2019 > 250], na.rm = T)
min(df$wfh_share_2019[df$n_post_2019 > 250], na.rm = T)

df$`D&N (2020) Teleworkable` <- ifelse(df$teleworkable == 1, "Yes", "No")

# Scatter plot (Log-log)
quantile(df$n_post_2019+df$n_post_2021, probs = 0.9, na.rm = T)
cbbPalette_d_and_n <- c("#FF9100", "#0800FF")
summary(lm(data = df %>%
             filter(n_post_2019 > 250) %>%
             filter(wfh_share_2019 > 0.00001 & wfh_share_2021 > 0.00001), formula = log(wfh_share_2021, base=10) ~ 1 + log(wfh_share_2019, base = 10)))
p = df %>%
  filter(n_post_2019 > 250) %>%
  filter(wfh_share_2019 > 0.00001 & wfh_share_2021 > 0.00001) %>%
  ggplot(., aes(x = 100*wfh_share_2019, y = 100*wfh_share_2021,
                color = `D&N (2020) Teleworkable`, shape = `D&N (2020) Teleworkable`)) +
  scale_color_manual(values = cbbPalette_d_and_n) +
  scale_y_log10(limits = c(0.01,100)) +
  scale_x_log10(limits = c(0.01,100)) +
  geom_abline(intercept = 0, slope = 1, size = 0.5, colour = "grey", linetype = "dashed") +
  geom_smooth(method=lm, colour = "grey", se=FALSE, aes(group=1), fullrange=TRUE) +
  geom_point(aes(size = (n_post_2019+n_post_2021)^2), stroke = 1)  +
  stat_poly_eq(aes(group=1, label=paste(..eq.label.., sep = "~~~")),geom="label",alpha=1,method = lm,label.y = log(0.035, base = 10), label.x = log(18, base = 10),
               eq.with.lhs = "plain(log)(y)~`=`~",
               eq.x.rhs = "~plain(log)(italic(x))") +
  stat_poly_eq(aes(group=1, label=paste(..rr.label.., sep = "~~~")),geom="label",alpha=1,method = lm,label.y = log(0.013, base = 10), label.x = log(18, base = 10),
               eq.with.lhs = "plain(log)(y)~`=`~",
               eq.x.rhs = "~plain(log)(italic(x))") +
  ylab("2021 RW Share (%) (Logscale)") +
  xlab("2019 RW Share (%) (Logscale)") +
  labs(title = "Pre- and Post-Pandemic WFH Share by ONET", subtitle = "Vacancy Weighted, USA") +
  theme(
    #axis.title.x=element_blank(),
    legend.position="bottom",
    axis.text.x = element_text(angle = 0)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  guides(size = "none") +
  scale_shape_manual(values=c(1, 2))
#+
#  geom_text_repel(aes(label = title_keep),
#                  fontface = "bold", size = 4, max.overlaps = 500, force_pull = 1, force = 3, box.padding = 0.5,
#                  bg.color = "white",
#                  bg.r = 0.15, seed = 4321)
p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/wfh_pre_post_by_tele.pdf", width = 8, height = 6)

remove(list = c("p", "p_egg"))

# Scatter Plot (Levels)
p = df %>%
  filter(n_post_2019 > 250) %>%
  ggplot(., aes(x = 100*wfh_share_2019, y = 100*wfh_share_2021,
                color = `D&N (2020) Teleworkable`, shape = `D&N (2020) Teleworkable`)) +
  scale_color_manual(values = cbbPalette_d_and_n) +
  geom_abline(intercept = 0, slope = 1, size = 0.5, colour = "grey", linetype = "dashed") +
  geom_smooth(method=lm, colour = "grey", se=FALSE, aes(group=1), fullrange=TRUE) +
  geom_point(aes(size = (n_post_2019+n_post_2021)^2), stroke = 1)  +
  stat_poly_eq(aes(group=1, label=paste(..eq.label.., sep = "~~~")),geom="label",alpha=1,method="lm",label.y = 6.5, label.x = 25,
               eq.with.lhs = "y~`=`~",
               eq.x.rhs = "~italic(x)") +
  stat_poly_eq(aes(group=1, label=paste(..rr.label.., sep = "~~~")),geom="label",alpha=1,method="lm",label.y = 1, label.x = 25,
               eq.with.lhs = "y~`=`~",
               eq.x.rhs = "~italic(x)") +
  ylab("2021 RW Share (%)") +
  xlab("2019 RW Share (%)") +
  scale_x_continuous(breaks = seq(0,100,10)) +
  scale_y_continuous(breaks = seq(0,100,10)) +
  
  labs(title = "Pre- and Post-Pandemic WFH Share by ONET", subtitle = "Vacancy Weighted, USA") +
  theme(
    #axis.title.x=element_blank(),
    legend.position="bottom",
    axis.text.x = element_text(angle = 0)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  coord_cartesian(ylim = c(0,50), xlim = c(0,30)) +
  guides(size = "none") +
  scale_shape_manual(values=c(1, 2))
#+
#  geom_text_repel(aes(label = title_keep),
#                  fontface = "bold", size = 4, max.overlaps = 500, nudge_y = 1, force_pull = 4, force = 3, box.padding = 0.25,
#                  bg.color = "white",
#                  bg.r = 0.15, seed = 4321)
p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/level_wfh_pre_post_by_tele.pdf", width = 8, height = 6)

dist_feats <- df %>%
  filter(n_post_2019 > 250)
mean(dist_feats$wfh_share_2019) # 0.03647199
sd(dist_feats$wfh_share_2019) # 0.0487183
mean(dist_feats$wfh_share_2021) # 0.09670947
sd(dist_feats$wfh_share_2021) # 0.1033183

#### END ####

#### INDUSTRY BREAK DOWN ####
remove(list = ls())
df_all <- fread(file = "./int_data/df_all_standardised.csv", nThread = 8)
df_all$month <- factor(df_all$month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))
df_all$wfh <- as.numeric(df_all$wfh_prob>0.5)
df_all$bgt_occ6 <- str_sub(df_all$bgt_occ, 1, 7)
df_all$bgt_occ2 <- as.numeric(str_sub(df_all$bgt_occ, 1, 2))
df_all$job_ymd <- ymd(df_all$job_date)
df_all$year_quarter <- as.yearqtr(df_all$job_ymd)
df_all$year_month <- as.yearmon(df_all$job_ymd)
df_all <- setDT(df_all)

# Plot Industry Change
df_all_ind <- df_all %>%
  filter(year %in% c(2019, 2021)) %>%
  .[, .(wfh_share = sum(wfh*tot_emp_ad, na.rm = T)/sum(tot_emp_ad, na.rm = T)), by = .(sector_clustered, year)] %>%
  group_by(sector_clustered) %>%
  mutate(prop_growth = ifelse(!is.na(lag(wfh_share)), paste0(round((wfh_share)/lag(wfh_share),1),"X"), NA)) %>%
  ungroup() %>%
  setDT(.)

df_all_ind <- df_all_ind %>%
  filter(!(sector_clustered %in% c("", "ACTIVITIES OF EXTRATERRITORIAL ORGANISATIONS AND BODIES",
                                   "ACTIVITIES OF HOUSEHOLDS AS EMPLOYERS; UNDIFFERENTIATED GOODS-AND SERVICES-PRODUCING ACTIVITIES OF HOUSEHOLDS FOR OWN USE"))) %>%
  filter(!is.na(sector_clustered)) %>%
  setDT(.)

df_all_ind$sector_clustered <- str_to_title(gsub(" and ", " & ", df_all_ind$sector_clustered, fixed = T))

df_all_ind$sector_clustered <- gsub(" Of ", " of ", df_all_ind$sector_clustered, fixed = T)
df_all_ind$sector_clustered <- gsub(" of Companies & Enterprises", "", df_all_ind$sector_clustered, fixed = T)

table(df_all_ind$sector_clustered)

cbbPalette_ind <- c("#000000", "#D66F00")

df_all_ind <- df_all_ind %>% mutate(sector_clustered = fct_reorder(sector_clustered, wfh_share, .fun = max, .desc = FALSE))

p =  df_all_ind %>%
  ggplot(., aes(x = sector_clustered, y = wfh_share, fill = as.factor(year))) +
  geom_bar(stat = "identity", width=1, position = position_dodge(width=0.8))  +
  geom_text(aes(label = prop_growth, family = "serif"), size = 5, vjust = 0, colour = "black", hjust = -0.5) +
  ylab("Share (%)") +
  labs(title = "WFH Share Distribution by Sector / Year", subtitle = "Employment Weighted, Global") +
  scale_y_continuous(breaks = seq(0,4,0.05), limits = c(0,0.3)) +
  #             labels=format(df_all_occ$date_fake,
  #                           format="%Y")) +
  #             minor_breaks = as.Date(c("2019-01-01","2019-07-01","2020-01-01","2020-07-01","2021-01-01","2021-07-01", "2022-01-01")),
  #             date_labels = '%Y') +
  #scale_y_continuous(labels = scales::number_format(accuracy = 0.025),  breaks = seq(0,0.3,0.05)) +
  #coord_cartesian(ylim = c(0, 0.24)) +
  scale_fill_manual(values = cbbPalette_ind) +
  #scale_shape_manual(values=rep(0:4, 3)) +
  theme(
    #axis.title.x=element_blank(),
    axis.title.y=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 0),
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm"),
        axis.text.y = element_text(hjust=0),
        legend.position = c(.7, .08)) +
  guides(fill = guide_legend(ncol = 2)) +
  coord_flip() +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 60))
p
p_egg <- set_panel_size(p = p,
                        width = unit(3.5, "in"),
                        height = unit(19*0.65, "cm"))
ggsave(p_egg, filename = "./plots/ind_dist_alt.pdf", width = 9, height = 19*0.65+3)



#### END ####

#### CITIES BREAK DOWN #### ####
remove(list = ls())
df_all <- fread(file = "./int_data/df_all_standardised.csv", nThread = 8)
df_all$month <- factor(df_all$month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))
df_all$wfh <- as.numeric(df_all$wfh_prob>0.5)
df_all$bgt_occ6 <- str_sub(df_all$bgt_occ, 1, 7)
df_all$bgt_occ2 <- as.numeric(str_sub(df_all$bgt_occ, 1, 2))
df_all$job_ymd <- ymd(df_all$job_date)
df_all$year_quarter <- as.yearqtr(df_all$job_ymd)
df_all$year_month <- as.yearmon(df_all$job_ymd)

remove(list = setdiff(ls(),"df_all"))

df_all$city[df_all$city == "Washington" & df_all$state == "District of Columbia"] <- "Washington, D.C."

#### BAR PLOT 2021 vs 2019
df_all_cities <- df_all %>%
  filter(year == 2019 | year == 2021) %>%
  setDT(.) %>%
  .[, .(n_posts = sum(job_id_weight),
        wfh_share = sum(wfh*job_id_weight, na.rm = T)/sum(job_id_weight, na.rm = T)), by = .(country, city, year)] %>%
  .[order(country, city, year)] %>%
  group_by(city, country) %>%
  mutate(prop_growth = ifelse(!is.na(lag(wfh_share)), paste0(round((wfh_share)/lag(wfh_share),1),"X"), NA)) %>%
  ungroup() %>%
  setDT(.)

df_all_cities <- df_all_cities %>%
  filter((city == "Auckland" & country == "NZ") | (city == "New York" & country == "US") | (city == "Los Angeles" & country == "US") | (city == "San Francisco" & country == "US") | (city == "Atlanta" & country == "US") | (city == "Washington, D.C." & country == "US") | (city == "Miami" & country == "US") | (city == "Dallas" & country == "US") | (city == "Chicago" & country == "US") | (city == "Phoenix" & country == "US") | (city == "Houston" & country == "US") | (city == "Philadelphia" & country == "US") | (city == "London" & country == "UK") | (city == "Manchester" & country == "UK") | (city == "Birmingham" & country == "UK") | (city == "Glasgow" & country == "UK") | (city == "Sydney" & country == "Australia") | (city == "Melbourne" & country == "Australia") | (city == "Brisbane" & country == "Australia") | (city == "Adelaide" & country == "Australia") | (city == "Perth" & country == "Australia") | (city == "Toronto" & country == "Canada") | (city == "Montreal" & country == "Canada") | (city == "Ottowa" & country == "Canada"))

#cbbPalette <- c("#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00")
#cbbPalette_oc <- c("#000000", "#56B4E9")
#cbbPalette_ind <- c("#000000", "#F0E442")
cbbPalette_cit <- c("#000000", "#A020F0")

df_all_cities$country <- ifelse(df_all_cities$country=="Australia", "AUS", df_all_cities$country)
df_all_cities$country <- ifelse(df_all_cities$country=="Canada", "CAN", df_all_cities$country)
df_all_cities$city_country <- paste0(df_all_cities$city, " (",df_all_cities$country,")")

df_all_cities <- df_all_cities %>% mutate(city_country_fac = fct_reorder(city_country, wfh_share, .fun = max, .desc = FALSE)) %>% ungroup()

a <- ifelse(grepl("(AUS)", as.vector(levels(df_all_cities$city_country_fac))), "red", "black")

p = ggplot(df_all_cities, aes(x = city_country_fac, y = wfh_share, fill = as.factor(year))) +
  geom_bar(stat = "identity", width=1, position = position_dodge(width=0.8))  +
  geom_text(aes(label = prop_growth, family = "serif"), size = 5, vjust = 0, colour = "black", hjust = -0.5) +
  ylab("Share (%)") +
  labs(title = "WFH Share Distribution by City / Year", subtitle = "Vacancy Weighted, National") +
  scale_y_continuous(breaks = seq(0,4,0.05), limits = c(0,0.3)) +
  #             labels=format(df_all_occ$date_fake,
  #                           format="%Y")) +
  #             minor_breaks = as.Date(c("2019-01-01","2019-07-01","2020-01-01","2020-07-01","2021-01-01","2021-07-01", "2022-01-01")),
  #             date_labels = '%Y') +
  #scale_y_continuous(labels = scales::number_format(accuracy = 0.025),  breaks = seq(0,0.3,0.05)) +
  #coord_cartesian(ylim = c(0, 0.24)) +
  scale_fill_manual(values = cbbPalette_cit) +
  #scale_shape_manual(values=rep(0:4, 3)) +
  theme(
    #axis.title.x=element_blank(),
    axis.title.y=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 0),
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm"),
        axis.text.y = element_text(hjust=0, colour = "black"),
        legend.position = c(.7, .08)) +
  guides(fill = guide_legend(ncol = 2)) +
  coord_flip() +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 60))
p
p_egg <- set_panel_size(p = p,
                        width = unit(3.5, "in"),
                        height = unit(20*0.65, "cm"))
ggsave(p_egg, filename = "./plots/city_dist_alt.pdf", width = 9, height = 20*0.65+3)

remove(list = setdiff(ls(),"df_all"))

# #### US CITY SCATTER PLOTS
# df_all_cities_us <- df_all %>%
#   filter(year == 2019 | year == 2021) %>%
#   filter(country == "US") %>%
#   setDT(.) %>%
#   .[, .(n_post = sum(job_id_weight),
#         d_n_teleworkable = sum(teleworkable*job_id_weight, na.rm = T)/sum(job_id_weight),
#         wfh_share = sum(wfh*job_id_weight, na.rm = T)/sum(job_id_weight)),
#     by = .(country, city, year)] %>%
#   .[order(country, city, year)] %>%
#   group_by(city, country) %>%
#   pivot_wider(names_from = year, values_from = c(n_post, d_n_teleworkable, wfh_share)) %>%
#   ungroup %>%
#   setDT(.) %>%
#   filter(n_post_2019 > 500 & n_post_2021 > 500)
# 
# library(ggrepel)
# set.seed(999)
# 
# df_all_cities_us_select <- df_all_cities_us %>%
#   filter(country == "US") %>%
#   group_by(country) %>%
#   arrange(desc(sqrt(n_post_2019) + sqrt(n_post_2021))) %>%
#   filter(row_number() < 20) %>%
#   ungroup
# 
# df_all_cities_us$title_keep <- ifelse(df_all_cities_us$city %in% c(df_all_cities_us_select$city, "Washington, D.C.", "San Francisco", "Atlanta", "Chicago", "Seattle"), df_all_cities_us$city, "")
# 
# df_all_cities_us_others <- df_all_cities_us %>%
#   filter(country == "US") %>%
#   filter(n_post_2019 > 1000 & n_post_2021 > 1000) %>%
#   filter(wfh_share_2019 > 0.00001 & wfh_share_2021 > 0.00001)
# 
# summary(df_all_cities_us_others$wfh_share_2021)
# summary(df_all_cities_us_others$d_n_teleworkable_2021)
# 
# cbbPalette <- c("#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00")
# cbbPalette <- c("#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00")
# 
# # Scatter plot (Log-log) 2021 D&N vs 2021 WFH Share
# p = df_all_cities_us %>%
#   filter(country == "US") %>%
#   filter(n_post_2021 > 500) %>%
#   filter(wfh_share_2021 > 0.00001) %>%
#   ggplot(., aes(x = d_n_teleworkable_2021, y = wfh_share_2021,
#                 color = `country`, shape = `country`)) +
#   scale_color_manual(values = cbbPalette) +
#   geom_abline(intercept = 0, slope = 1, size = 0.5, colour = "grey", linetype = "dashed") +
#   geom_smooth(method=lm, colour = "grey", se=FALSE, aes(group=1), fullrange=TRUE) +
#   geom_point(data = df_all_cities_us[df_all_cities_us$title_keep == ""], aes(size = (n_post_2019+n_post_2021)), stroke = 1.5, alpha = 1, colour = "grey60")  +
#   geom_point(data = df_all_cities_us[df_all_cities_us$title_keep != ""], aes(size = (n_post_2019+n_post_2021)), stroke = 1.5, alpha = 0.7) +
#   scale_size(range = c(1, 10)) +
#   stat_poly_eq(aes(group=1, label=paste(..eq.label.., sep = "~~~")),geom="label",alpha=1,method="lm",label.y = 0, label.x = 0.7,
#                eq.with.lhs = "y~`=`~",
#                eq.x.rhs = "~italic(x)",
#                colour = "black",
#                size = 5) +
#   ylab("Share of Vacancies Offering Remote Work (2021)") +
#   xlab("Share of Vacancies in 'Teleworkable' Occupations (2021)") +
#   scale_x_continuous(breaks = seq(0,1,0.1)) +
#   scale_y_continuous(breaks = seq(0,1,0.1)) +
#   labs(title = "Share of Remote Work Vacancies (2021) vs Share of 'Teleworkable' Vacancies (2021)", subtitle = "Vacancy Weighted, USA") +
#   coord_cartesian(xlim = c(0.2, 0.8),
#                   ylim = c(0.00, 0.3)) +
#   theme(
#     #axis.title.x=element_blank(),
#     legend.position="bottom",
#     axis.text.x = element_text(angle = 0)) +
#   theme(text = element_text(size=15, family="serif", colour = "black"),
#         axis.text = element_text(size=14, family="serif", colour = "black"),
#         axis.title = element_text(size=15, family="serif", colour = "black"),
#         legend.text = element_text(size=14, family="serif", colour = "black"),
#         panel.background = element_rect(fill = "white"),
#         legend.key.width = unit(1,"cm"),
#         legend.title = element_blank()) +
#   guides(size = "none") +
#   scale_shape_manual(values=c(1, 3, 4, 5, 6)) +
#   geom_text_repel(aes(label = title_keep),
#                   fontface = "bold", size = 5, max.overlaps = 500, box.padding = 1, point.padding = 0.5, force = 2, force_pull = 2)
# 
# p
# 
# p_egg <- set_panel_size(p = p,
#                         width = unit(5, "in"),
#                         height = unit(3, "in"))
# ggsave(p_egg, filename = "./plots/usa_wfh_vs_d_and_n_by_city.pdf", width = 8, height = 6)
# 
# remove(list = c("p", "p_egg"))
# 
# # Scatter plot (Log-log)
# cbbPalette <- c("#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00")
# 
# p = df_all_cities_us %>%
#   filter(n_post_2019 > 1000 & n_post_2021 > 1000) %>%
#   filter(wfh_share_2019 > 0.00001 & wfh_share_2021 > 0.00001) %>%
#   ggplot(., aes(x = wfh_share_2019, y = wfh_share_2021,
#                 color = `country`, shape = `country`)) +
#   scale_color_manual(values = cbbPalette) +
#   scale_y_log10(limits = c(0.0001,1)) +
#   scale_x_log10(limits = c(0.0001,1)) +
#   geom_abline(intercept = 0, slope = 1, size = 0.5, colour = "grey", linetype = "dashed") +
#   geom_smooth(method=lm, colour = "grey", se=FALSE, aes(group=1), fullrange=TRUE) +
#   geom_point(data = df_all_cities_us[df_all_cities_us$title_keep == ""], aes(size = (n_post_2019+n_post_2021)), stroke = 1.5, alpha = 1, colour = "grey60")  +
#   geom_point(data = df_all_cities_us[df_all_cities_us$title_keep != ""], aes(size = (n_post_2019+n_post_2021)), stroke = 1.5, alpha = 0.7) +  scale_size(range = c(1, 10)) +
#   stat_poly_eq(aes(group=1, label=paste(..eq.label.., sep = "~~~")),geom="label",
#                alpha=1,method = lm,label.y = log(0.0001, base = 10), label.x = log(0.247, base = 10),
#                eq.with.lhs = "plain(log)(y)~`=`~",
#                eq.x.rhs = "~plain(log)(italic(x))") +
#   ylab("WFH Share (2021) (Logscale)") +
#   xlab("WFH Share (2019) (Logscale)") +
#   labs(title = "Pre- and Post-Pandemic WFH Share by ONET", subtitle = "Vacancy Weighted, USA") +
#   coord_cartesian(xlim = c(0.01, 1),
#                   ylim = c(0.01, 1)) +
#   theme(
#     #axis.title.x=element_blank(),
#     legend.position="bottom",
#     axis.text.x = element_text(angle = 0)) +
#   theme(text = element_text(size=15, family="serif", colour = "black"),
#         axis.text = element_text(size=14, family="serif", colour = "black"),
#         axis.title = element_text(size=15, family="serif", colour = "black"),
#         legend.text = element_text(size=14, family="serif", colour = "black"),
#         panel.background = element_rect(fill = "white"),
#         legend.key.width = unit(1,"cm"),
#         legend.title = element_blank()) +
#   guides(size = "none") +
#   scale_shape_manual(values=c(1, 2, 3, 4, 5)) +
#   geom_text_repel(aes(label = title_keep),
#                   fontface = "bold", size = 5, max.overlaps = 500, box.padding = 1, point.padding = 0.5, force = 2, force_pull = 2)
# 
# p
# p_egg <- set_panel_size(p = p,
#                         width = unit(5, "in"),
#                         height = unit(3, "in"))
# ggsave(p_egg, filename = "./plots/us_wfh_pre_post_by_city.pdf", width = 8, height = 6)
# 
# remove(list = c("p", "p_egg"))
# 
# # Scatter Plot (Levels)
# p = df_all_cities_us %>%
#   filter(n_post_2019 > 1000 & n_post_2021 > 1000) %>%
#   filter(wfh_share_2019 > 0.00001 & wfh_share_2021 > 0.00001) %>%
#   ggplot(., aes(x = wfh_share_2019, y = wfh_share_2021,
#                 color = `country`, shape = `country`)) +
#   scale_color_manual(values = cbbPalette) +
#   geom_abline(intercept = 0, slope = 1, size = 0.5, colour = "grey", linetype = "dashed") +
#   geom_smooth(method=lm, colour = "grey", se=FALSE, aes(group=1), fullrange=TRUE) +
#   geom_point(data = df_all_cities_us[df_all_cities_us$title_keep == ""], aes(size = (n_post_2019+n_post_2021)), stroke = 1.5, alpha = 1, colour = "grey60")  +
#   geom_point(data = df_all_cities_us[df_all_cities_us$title_keep != ""], aes(size = (n_post_2019+n_post_2021)), stroke = 1.5, alpha = 0.7) +  scale_size(range = c(1, 10)) +
#   stat_poly_eq(aes(group=1, label=paste(..eq.label.., sep = "~~~")),geom="label",alpha=1,method="lm",label.y = 0, label.x = 0.2,
#                eq.with.lhs = "y~`=`~",
#                eq.x.rhs = "~italic(x)") +
#   ylab("WFH Share (2021)") +
#   xlab("WFH Share (2019)") +
#   scale_x_continuous(breaks = seq(0,1,0.1)) +
#   scale_y_continuous(breaks = seq(0,1,0.1)) +
#   coord_cartesian(xlim = c(0, 0.3),
#                   ylim = c(0, 0.3)) +
#   labs(title = "Pre- and Post-Pandemic WFH Share by ONET", subtitle = "Vacancy Weighted, USA") +
#   theme(
#     #axis.title.x=element_blank(),
#     legend.position="bottom",
#     axis.text.x = element_text(angle = 0)) +
#   theme(text = element_text(size=15, family="serif", colour = "black"),
#         axis.text = element_text(size=14, family="serif", colour = "black"),
#         axis.title = element_text(size=15, family="serif", colour = "black"),
#         legend.text = element_text(size=14, family="serif", colour = "black"),
#         panel.background = element_rect(fill = "white"),
#         legend.key.width = unit(1,"cm"),
#         legend.title = element_blank()) +
#   guides(size = "none") +
#   scale_shape_manual(values=c(1, 3, 4, 5, 6)) +
#   geom_text_repel(aes(label = title_keep),  fontface = "bold", size = 5, max.overlaps = 200, box.padding = 2, point.padding = 1, force = 2, force_pull = 2)
# p
# p_egg <- set_panel_size(p = p,
#                         width = unit(5, "in"),
#                         height = unit(3, "in"))
# ggsave(p_egg, filename = "./plots/level_us_wfh_pre_post_by_city.pdf", width = 8, height = 6)
# 
# remove(list = c("p", "p_egg"))
# 
# remove(list = setdiff(ls(), "df_all"))


#### GLOBAL CITY SCATTER PLOTS
df_all_cities <- df_all %>%
  filter(year == 2019 | year == 2021) %>%
  #filter(country == "US") %>%
  setDT(.) %>%
  .[, .(n_post = sum(job_id_weight),
        d_n_teleworkable = sum(teleworkable*job_id_weight, na.rm = T)/sum(job_id_weight),
        wfh_share = sum(wfh*job_id_weight, na.rm = T)/sum(job_id_weight)),
    by = .(country, city, year)] %>%
  .[order(country, city, year)] %>%
  group_by(city, country) %>%
  pivot_wider(names_from = year, values_from = c(n_post, d_n_teleworkable, wfh_share)) %>%
  ungroup %>%
  setDT(.) %>%
  filter(n_post_2019 > 250)

library(ggrepel)
set.seed(999)

View(df_all_cities)

df_all_cities <- df_all_cities %>%
  .[,  title_keep := ifelse((city == "Auckland" & country == "NZ") | 
                              (city == "New York" & country == "US") | 
                              (city == "Los Angeles" & country == "US") | 
                              (city == "San Francisco" & country == "US") | 
                              (city == "Atlanta" & country == "US") | 
                              (city == "Washington, D.C." & country == "US") | 
                              #(city == "Miami" & country == "US") | 
                              #(city == "Dallas" & country == "US") | 
                              (city == "Chicago" & country == "US") | 
                              #(city == "Phoenix" & country == "US") | 
                              #(city == "Houston" & country == "US") | 
                              #(city == "Philadelphia" & country == "US") | 
                              (city == "London" & country == "UK") | 
                              #(city == "Manchester" & country == "UK") |
                              #(city == "Birmingham" & country == "UK") | 
                              #(city == "Glasgow" & country == "UK") | 
                              (city == "Sydney" & country == "Australia") | 
                              (city == "Melbourne" & country == "Australia") | 
                              #(city == "Brisbane" & country == "Australia") | 
                              #(city == "Adelaide" & country == "Australia") | 
                              #(city == "Perth" & country == "Australia") | 
                              (city == "Toronto" & country == "Canada") | 
                              (city == "Montreal" & country == "Canada"),
                            city, "")]

# Scatter plot (Log-log)
cbbPalette <- c("#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00")

max(df_all_cities$wfh_share_2019)
rm(p)
p = df_all_cities %>%
  filter(n_post_2019 > 500) %>%
  filter(wfh_share_2019 > 0.00001 & wfh_share_2021 > 0.00001) %>%
  ggplot(., aes(x = 100*wfh_share_2019, y = 100*wfh_share_2021,
                color = `country`, shape = `country`)) +
  scale_color_manual(values = cbbPalette) +
  scale_y_log10(limits = c(0.01,100)) +
  scale_x_log10(limits = c(0.01,100), breaks = c(100, 10, 1, 0.1), labels=c(100, 10, 1, 0.1)) +
  geom_abline(intercept = 0, slope = 1, size = 0.5, colour = "grey", linetype = "dashed") +
  geom_smooth(method=lm, colour = "grey", se=FALSE, aes(group=1), fullrange=TRUE) +
  geom_point(data = df_all_cities[df_all_cities$title_keep == "" & df_all_cities$wfh_share_2019 > 0.001 & df_all_cities$wfh_share_2021 > 0.001 & df_all_cities$n_post_2019 > 500,], aes(size = (n_post_2019+n_post_2021)/2), stroke = 1.5, alpha = 0.4)  +
  geom_point(data = df_all_cities[df_all_cities$title_keep != "" & df_all_cities$wfh_share_2019 > 0.001 & df_all_cities$wfh_share_2021 > 0.001,], aes(size = (n_post_2019+n_post_2021)), stroke = 2, alpha = 1) +  scale_size(range = c(1, 6)) +
  stat_poly_eq(aes(group=1, label=paste(..eq.label.., sep = "~~~")),geom="label",
               alpha=1,method = lm,label.y = log(1.8, base = 10), label.x = log(18, base = 10),
               eq.with.lhs = "plain(log)(y)~`=`~",
               eq.x.rhs = "~plain(log)(italic(x))") +
  stat_poly_eq(aes(group=1, label=paste(..rr.label.., sep = "~~~")),geom="label",
               alpha=1,method = lm,label.y = log(1.05, base = 10), label.x = log(18, base = 10),
               eq.with.lhs = "plain(log)(y)~`=`~",
               eq.x.rhs = "~plain(log)(italic(x))") +
  ylab("2021 RW Share (%) (Logscale)") +
  xlab("2019 RW Share (%) (Logscale)") +
  labs(title = "Pre- and Post-Pandemic WFH Share by City",
       subtitle = paste0("x mean (se): ",
                         format(round(mean(df_all_cities$wfh_share_2019[df_all_cities$n_post_2019 > 500]), 3), nsmall = 3),
                         " (",
                         format(round(sd(df_all_cities$wfh_share_2019[df_all_cities$n_post_2019 > 500]), 3), nsmall = 3),
                         "), y mean (se): ",
                         format(round(mean(df_all_cities$wfh_share_2021[df_all_cities$n_post_2019 > 500]), 3), nsmall = 3),
                         " (",
                         format(round(sd(df_all_cities$wfh_share_2021[df_all_cities$n_post_2019 > 500]), 3), nsmall = 3),
                         ").")) +
  coord_cartesian(xlim = c(0.1, 80),
                  ylim = c(1, 80)) +
  theme(
    #axis.title.x=element_blank(),
    legend.position="bottom",
    axis.text.x = element_text(angle = 0)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm"),
        legend.title = element_blank()) +
  guides(size = "none") +
  scale_shape_manual(values=c(1, 3, 4, 5, 6)) +
  geom_text_repel(aes(label = title_keep), 
                  fontface = "bold", size = 4, max.overlaps = 500, force_pull = 5, force = 0.5, box.padding = 1,
                  bg.color = "white",
                  bg.r = 0.15, seed = 4321)

p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/wfh_pre_post_by_city.pdf", width = 8, height = 6)

remove(list = c("p", "p_egg"))

df_all_cities <- df_all_cities %>%
  .[,  title_keep := ifelse((city == "Auckland" & country == "NZ") | 
                              (city == "New York" & country == "US") | 
                              (city == "Los Angeles" & country == "US") | 
                              (city == "San Francisco" & country == "US") | 
                              (city == "Atlanta" & country == "US") | 
                              (city == "Washington, D.C." & country == "US") | 
                              #(city == "Miami" & country == "US") | 
                              #(city == "Dallas" & country == "US") | 
                              (city == "Chicago" & country == "US") | 
                              #(city == "Phoenix" & country == "US") | 
                              #(city == "Houston" & country == "US") | 
                              #(city == "Philadelphia" & country == "US") | 
                              (city == "London" & country == "UK") | 
                              #(city == "Manchester" & country == "UK") |
                              #(city == "Birmingham" & country == "UK") | 
                              #(city == "Glasgow" & country == "UK") | 
                              (city == "Sydney" & country == "Australia") | 
                              (city == "Melbourne" & country == "Australia") | 
                              #(city == "Brisbane" & country == "Australia") | 
                              #(city == "Adelaide" & country == "Australia") | 
                              #(city == "Perth" & country == "Australia") | 
                              (city == "Toronto" & country == "Canada") | 
                              (city == "Montreal" & country == "Canada"),
                            city, "")]

# Scatter Plot (Levels)
p = df_all_cities %>%
  filter(n_post_2019 > 500) %>%
  #filter(wfh_share_2019 > 0.00001 & wfh_share_2021 > 0.00001) %>%
  ggplot(., aes(x = 100*wfh_share_2019, y = 100*wfh_share_2021,
                color = `country`, shape = `country`)) +
  scale_color_manual(values = cbbPalette) +
  geom_abline(intercept = 0, slope = 1, size = 0.5, colour = "grey", linetype = "dashed") +
  geom_smooth(method=lm, colour = "grey", se=FALSE, aes(group=1), fullrange=TRUE) +
  geom_point(data = df_all_cities[df_all_cities$title_keep == "" & df_all_cities$wfh_share_2019 > 0.00001 & df_all_cities$wfh_share_2021 > 0.00001 & df_all_cities$n_post_2019 > 500,], aes(size = (n_post_2019+n_post_2021)/2), stroke = 1.5, alpha = 0.25)  +
  geom_point(data = df_all_cities[df_all_cities$title_keep != "" & df_all_cities$wfh_share_2019 > 0.00001 & df_all_cities$wfh_share_2021 > 0.00001,], aes(size = (n_post_2019+n_post_2021)), stroke = 2, alpha = 1) +  scale_size(range = c(1, 6)) +
  stat_poly_eq(aes(group=1, label=paste(..rr.label.., sep = "~~~")),geom="label",alpha=1,method="lm",label.y = 5, label.x = 12.5,
               eq.with.lhs = "y~`=`~",
               eq.x.rhs = "~italic(x)") +
  stat_poly_eq(aes(group=1, label=paste(..eq.label.., sep = "~~~")),geom="label",alpha=1,method="lm",label.y = 1, label.x = 12.5,
               eq.with.lhs = "y~`=`~",
               eq.x.rhs = "~italic(x)") +
  ylab("WFH Share (2021)") +
  xlab("WFH Share (2019)") +
  scale_x_continuous(breaks = seq(0,100,5)) +
  scale_y_continuous(breaks = seq(0,100,5)) +
  coord_cartesian(xlim = c(0, 15),
                  ylim = c(0, 30)) +
  labs(title = "Pre- and Post-Pandemic WFH Share by ONET",
       subtitle = paste0("x mean (se): ",
                         format(round(mean(df_all_cities$wfh_share_2019[df_all_cities$n_post_2019 > 500]), 3), nsmall = 3),
                         " (",
                         format(round(sd(df_all_cities$wfh_share_2019[df_all_cities$n_post_2019 > 500]), 3), nsmall = 3),
                         "), y mean (se): ",
                         format(round(mean(df_all_cities$wfh_share_2021[df_all_cities$n_post_2019 > 500]), 3), nsmall = 3),
                         " (",
                         format(round(sd(df_all_cities$wfh_share_2021[df_all_cities$n_post_2019 > 500]), 3), nsmall = 3),
                         ").")) +
  theme(
    #axis.title.x=element_blank(),
    legend.position="bottom",
    axis.text.x = element_text(angle = 0)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm"),
        legend.title = element_blank()) +
  guides(size = "none") +
  scale_shape_manual(values=c(1, 3, 4, 5, 6)) +
  geom_text_repel(aes(label = title_keep), 
                  fontface = "bold", size = 4, max.overlaps = 500, force_pull = 1, force = 1, box.padding = 1,
                  bg.color = "white",
                  bg.r = 0.1, seed = 4321)

p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/level_wfh_pre_post_by_city.pdf", width = 8, height = 6)

remove(list = c("p", "p_egg"))

remove(list = setdiff(ls(), "df_all"))

#### END ####


#### POP DENSITY ####
df_all <- df_all %>%
  filter(country == "US") %>%
  mutate(pop_density_group = ntile(density_per_km2, 20))

df_pop_density <- df_all %>%
  filter(year %in% c(2019, 2021)) %>%
  setDT(.) %>%
  .[, .(n = .N,
        wfh_share = sum(wfh*tot_emp_ad, na.rm = T)/sum(tot_emp_ad, na.rm = T),
        density_per_km2 = mean(density_per_km2)),
    by = .(geog, pop_density_group, year)] %>%
  filter(n > 50)

#### BINSCATTER ####
library(RColorBrewer)
mycolors <- colorRampPalette(brewer.pal(8, "Set2"))(22)

p = df_pop_density %>%
  #filter(ussoc_2d_wn < 27) %>%
  filter(year == 2021) %>%
  ggplot(., aes(x = log(density_per_km2), y = log(wfh_share), colour = pop_density_group)) +
  geom_point(size = 2, alpha = 0.9)  +
  ylab("Log WFH Share (2021)") +
  xlab("Log Population Density (US FIPS)") +
  labs(title = "Log WFH Share vs Log Pop. Density (2021)", subtitle = "Employment Weighted, USA") +
  #scale_shape_manual(values=rep(0:4, 3)) +
  theme(
    #axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 0)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  guides(colour = guide_legend(ncol = 3))
p
p_egg <- set_panel_size(p = p,
                        width = unit(12, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/log_pd_vs_wfh.pdf", width = 14, height = 8)

p = df_pop_density %>%
  #filter(ussoc_2d_wn < 27) %>%
  filter(year == 2021) %>%
  ggplot(., aes(x = density_per_km2, y = wfh_share, colour = pop_density_group)) +
  geom_point(size = 2, alpha = 0.9)  +
  ylab("WFH Share (2021)") +
  xlab("Population Density (US FIPS)") +
  labs(title = "WFH Share vs Pop. Density (2021)", subtitle = "Employment Weighted, USA") +
  #scale_shape_manual(values=rep(0:4, 3)) +
  theme(
    #axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 0)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  guides(colour = guide_legend(ncol = 3))
p
p_egg <- set_panel_size(p = p,
                        width = unit(12, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/pd_vs_wfh.pdf", width = 14, height = 8)

#### END ####

#### D&N BS PLOT ####
remove(list = ls())
df_all <- fread(file = "./int_data/df_all_standardised.csv", nThread = 8)
df_all$month <- factor(df_all$month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))
df_all$wfh <- as.numeric(df_all$wfh_prob>0.5)
df_all$bgt_occ6 <- str_sub(df_all$bgt_occ, 1, 7)
df_all$bgt_occ2 <- as.numeric(str_sub(df_all$bgt_occ, 1, 2))
df_all$job_ymd <- ymd(df_all$job_date)
df_all$year_quarter <- as.yearqtr(df_all$job_ymd)
df_all$year_month <- as.yearmon(df_all$job_ymd)

soc2010_names <- fread(file = "./aux_data/us_soc_2010_names.csv")
soc2010_names$soc10_2d <- as.numeric(soc2010_names$soc10_2d)

nrow(df_all)
df_all <- df_all %>%
  left_join(., soc2010_names, by = c("bgt_occ2" = "soc10_2d")) %>%
  setDT(.)
nrow(df_all)
rm(soc2010_names)
df_all$name <- gsub("and", "&", df_all$name, fixed = T)
df_all$name <- gsub(" Occupations", "", df_all$name)
df_all <- df_all %>% filter(!is.na(df_all$name))
df_all <- setDT(df_all)
df_all$ussoc_2d_wn <- paste0(df_all$bgt_occ2, ". ",df_all$name)

# Comparison to D&N
df_d_and_m_cmparison <- df_all %>%
  filter(year %in% c(2019, 2021)) %>%
  setDT(.) %>%
  .[, .(n = .N,
        wfh_share = sum(wfh*tot_emp_ad, na.rm = T)/sum(tot_emp_ad, na.rm = T),
        teleworkable_share = sum(teleworkable*tot_emp_ad, na.rm = T)/sum(tot_emp_ad, na.rm = T)),
    by = .(bgt_occ, ussoc_2d_wn, year)] %>%
  .[, teleworkable := ifelse(teleworkable_share>0.5, "yes", "no")] %>%
  filter(n > 50)

# BINSCATTER
library(RColorBrewer)
mycolors <- colorRampPalette(brewer.pal(8, "Set2"))(22)

p = df_d_and_m_cmparison %>%
  #filter(ussoc_2d_wn < 27) %>%
  filter(year == 2021) %>%
  ggplot(., aes(x = teleworkable, y = wfh_share, colour = ussoc_2d_wn)) +
  geom_point(size = 2, alpha = 0.9)  +
  ylab("WFH Share (2021)") +
  xlab("D&N 'Teleworkable'") +
  labs(title = "WFH Share vs Teleworkable (2021)", subtitle = "Employment Weighted, Global") +
  scale_y_continuous(breaks = seq(0,0.8,0.1)) +
  scale_fill_manual(values = mycolors) +
  #scale_shape_manual(values=rep(0:4, 3)) +
  theme(
    #axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 0)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  guides(colour = guide_legend(ncol = 3))
p
p_egg <- set_panel_size(p = p,
                        width = unit(12, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/teleworkable_bs.pdf", width = 14, height = 8)

#### END ####

##### SALARY #####
remove(list = ls())
df_all <- fread(file = "./int_data/df_all_standardised.csv", nThread = 8)
df_all$month <- factor(df_all$month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))
df_all$wfh <- as.numeric(df_all$wfh_prob>0.5)
df_all$bgt_occ6 <- str_sub(df_all$bgt_occ, 1, 7)
df_all$bgt_occ2 <- as.numeric(str_sub(df_all$bgt_occ, 1, 2))
df_all$job_ymd <- ymd(df_all$job_date)
df_all$year_quarter <- as.yearqtr(df_all$job_ymd)
df_all$year_month <- as.yearmon(df_all$job_ymd)
df_all <- setDT(df_all)

library(fixest)

# Basic correlations
mod1 <- feols(fml = log(disjoint_salary) ~ wfh:i(year) | year_month + country^year,
              data = df_all[year == 2019 | year == 2021],
              weights = ~ tot_emp_ad,
              fixef.rm = "both",
              nthreads = 8,
              verbose = 10,
              cluster = ~ year_month^country
)

mod2 <- feols(fml = log(disjoint_salary) ~ wfh:i(year) | year_month + state^country^year,
              data = df_all[year == 2019 | year == 2021],
              weights = ~ tot_emp_ad,
              fixef.rm = "both",
              nthreads = 8,
              verbose = 10,
              cluster = ~ year_month + country
)

mod3 <- feols(fml = log(disjoint_salary) ~ wfh:i(year) | year_month + state^country^bgt_occ^year,
              data = df_all[year == 2019 | year == 2021],
              weights = ~ tot_emp_ad,
              fixef.rm = "both",
              nthreads = 8,
              verbose = 10,
              cluster = ~ year_month^country
)

mod4 <- feols(fml = log(disjoint_salary) ~ wfh:i(year) | year_month + state^country^job_title^year,
              data = df_all[year == 2019 | year == 2021],
              weights = ~ tot_emp_ad,
              fixef.rm = "both",
              nthreads = 8,
              verbose = 10,
              cluster = ~ year_month^country
)

etable(mod1, mod2, mod3, mod4, digits = 3, digits.stats = 4, tex = T)

#### END ####

#### VARIANCE DECOMPOSITION #### ####
library(fixest)
remove(list = ls())
df_all <- fread(file = "./int_data/df_all_standardised.csv", nThread = 8)
df_all[df_all==""] <- NA
df_all$month <- factor(df_all$month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))
df_all <- df_all %>% filter(year == 2021)
df_all$wfh <- as.numeric(df_all$wfh_prob>0.5)
df_all$bgt_occ2 <- str_sub(df_all$bgt_occ, 1, 2)
df_all$bgt_occ6 <- str_sub(df_all$bgt_occ, 1, 7)
colnames(df_all)
df_all$job_ymd <- ymd(df_all$job_date)
df_all$year_month <- as.yearmon(df_all$job_ymd)
setDTthreads(1)
df_all <- df_all %>% setDT(.)
summary(df_all$bach_or_higher)

# Impute Missing Education Status
df_all <- df_all %>%
  .[!is.na(bgt_occ6)] %>%
  .[, imp_bach_or_higher := mean(bach_or_higher, na.rm = T), by = bgt_occ6] %>%
  .[, imp_bach_or_higher2 := mean(bach_or_higher, na.rm = T), by = bgt_occ2] %>%
  .[, bach_or_higher := ifelse(!is.na(bach_or_higher), bach_or_higher, as.numeric(imp_bach_or_higher>0.5))] %>%
  .[, bach_or_higher := ifelse(!is.na(bach_or_higher), bach_or_higher, as.numeric(imp_bach_or_higher2>0.5))] %>%
  select(-c(imp_bach_or_higher, imp_bach_or_higher2))

# NA weird industry sectors
df_all <- df_all %>%
  filter(!is.na(disjoint_salary)) %>%
  mutate(sector_clustered = ifelse(sector_clustered %in% c("",
                                                           "ACTIVITIES OF EXTRATERRITORIAL ORGANISATIONS AND BODIES",
                                                           "ACTIVITIES OF HOUSEHOLDS AS EMPLOYERS; UNDIFFERENTIATED GOODS-AND SERVICES-PRODUCING ACTIVITIES OF HOUSEHOLDS FOR OWN USE"),
                                   NA, sector_clustered)) %>%
  setDT(.)

# Make Datasets for VD
# Make Datasets for VD
df_all_ag <- df_all %>%
  .[year >= 2021] %>%
  .[!is.na(bgt_occ6) & !is.na(country) & !is.na(state) & !is.na(geog) & !is.na(year_month) & !is.na(bach_or_higher)]

mean(is.na(df_all_ag$sector_clustered)) # 39%
mean(is.na(df_all_ag$employer)) # 37%

df_all_ag_ind <- df_all %>%
  .[year >= 2021] %>%
  .[!is.na(bgt_occ6) & !is.na(country) & !is.na(state) & !is.na(geog) & !is.na(year_month) & !is.na(bach_or_higher) & !is.na(sector_clustered)]

df_all_ag_firm <- df_all %>%
  .[year >= 2021] %>%
  .[!is.na(bgt_occ6) & !is.na(country) & !is.na(state) & !is.na(geog) & !is.na(year_month) & !is.na(bach_or_higher) & !is.na(employer)] %>%
  .[, n := .N, by = employer] %>%
  .[n > 10]

#  Find FEs to drop
fit1 <- feols(fml = wfh ~ 1 | bgt_occ6 + country + state + geog + year_month,
              #weights = ~ tot_emp_ad,
              fixef.rm = "both",
              nthreads = 8,
              verbose = 10,
              combine.quick=FALSE,
              data = df_all_ag)

df_all_ag <- df_all_ag %>%
  .[!(geog %in% fit1$fixef_removed$geog) & !(bgt_occ6 %in% fit1$fixef_removed$bgt_occ6)]

fit2 <- feols(fml = wfh ~ 1 | bgt_occ6 + country + state + geog + year_month + sector_clustered,
              #weights = ~ tot_emp_ad,
              fixef.rm = "both",
              nthreads = 8,
              verbose = 10,
              combine.quick=FALSE,
              data = df_all_ag_ind)

df_all_ag_ind <- df_all_ag_ind %>%
  .[!(geog %in% fit2$fixef_removed$geog) & !(bgt_occ6 %in% fit2$fixef_removed$bgt_occ6)]

fit3 <- feols(fml = wfh ~ 1 | bgt_occ6 + country + state + geog + year_month + employer,
              #weights = ~ tot_emp_ad,
              fixef.rm = "both",
              nthreads = 8,
              verbose = 10,
              combine.quick=FALSE,
              data = df_all_ag_firm)

df_all_ag_firm <- df_all_ag_firm %>%
  .[!(geog %in% fit3$fixef_removed$geog) & !(bgt_occ6 %in% fit3$fixef_removed$bgt_occ6) & !(employer %in% fit3$fixef_removed$employer)]

# Fit final model
fit1 <- feols(fml = wfh ~ 1 | bgt_occ6 + country + state + geog + year_month,
              #weights = ~ tot_emp_ad,
              fixef.rm = "both",
              nthreads = 8,
              verbose = 10,
              combine.quick=FALSE,
              data = df_all_ag)

fit2 <- feols(fml = wfh ~ 1 | bgt_occ6 + country + state + geog + year_month + sector_clustered,
              #weights = ~ tot_emp_ad,
              fixef.rm = "both",
              nthreads = 8,
              verbose = 10,
              combine.quick=FALSE,
              data = df_all_ag_ind)

fit3 <- feols(fml = wfh ~ 1 | bgt_occ6 + country + state + geog + year_month + employer,
              #weights = ~ tot_emp_ad,
              fixef.rm = "both",
              nthreads = 8,
              verbose = 10,
              combine.quick=FALSE,
              data = df_all_ag_firm)

df_all_ag$pred <- predict(fit1)
df_all_ag_ind$pred <- predict(fit2)
df_all_ag_firm$pred <- predict(fit3)

df_all_ag$resid <- fit1$residuals
df_all_ag_ind$resid <- fit2$residuals
df_all_ag_firm$resid <- fit3$residuals

mean(df_all_ag$pred>0 & df_all_ag$pred<1) # 0.8517116
mean(df_all_ag_ind$pred>0 & df_all_ag_ind$pred<1) # 0.8256983
mean(df_all_ag_firm$pred>0 & df_all_ag_firm$pred<1) # 0.6748202

fe_df1 <- predict(fit1, fixef = TRUE)
fe_df2 <- predict(fit2, fixef = TRUE)
fe_df3 <- predict(fit3, fixef = TRUE)

colnames(fe_df1) <- paste0(colnames(fe_df1), "_fe")
colnames(fe_df2) <- paste0(colnames(fe_df2), "_fe")
colnames(fe_df3) <- paste0(colnames(fe_df3), "_fe")

df_all_ag <- bind_cols(df_all_ag, fe_df1)
df_all_ag_ind <- bind_cols(df_all_ag_ind, fe_df2)
df_all_ag_firm <- bind_cols(df_all_ag_firm, fe_df3)

df_all_ag_vd <- df_all_ag %>%
  summarise(bgt_occ6_fe = var(bgt_occ6_fe)/var(wfh),
            geog_fe = var(geog_fe)/var(wfh),
            year_month_fe = var(year_month_fe)/var(wfh),
            resid = var(resid)/var(wfh)) %>%
  mutate(hot = 1 - rowSums(.))

df_all_ag_ind_vd <- df_all_ag_ind %>%
  summarise(bgt_occ6_fe = var(bgt_occ6_fe)/var(wfh),
            geog_fe = var(geog_fe)/var(wfh),
            year_month_fe = var(year_month_fe)/var(wfh),
            sector_clustered_fe = var(sector_clustered_fe)/var(wfh),
            resid = var(resid)/var(wfh)) %>%
  mutate(hot = 1 - rowSums(.))

df_all_ag_firm_vd <- df_all_ag_firm %>%
  summarise(bgt_occ6_fe = var(bgt_occ6_fe)/var(wfh),
            geog_fe = var(geog_fe)/var(wfh),
            year_month_fe = var(year_month_fe)/var(wfh),
            employer_fe = var(employer_fe)/var(wfh),
            resid = var(resid)/var(wfh)) %>%
  mutate(hot = 1 - rowSums(.))

vd_df <- bind_rows(df_all_ag_vd, df_all_ag_ind_vd, df_all_ag_firm_vd) %>%
  select(bgt_occ6_fe, geog_fe, year_month_fe, sector_clustered_fe, employer_fe, resid) %>%
  rename("Occupation (6-digit)" = bgt_occ6_fe,
         "Local Geography" = geog_fe,
         "Year$\times$Month" = year_month_fe,
         "Industry Sector" = sector_clustered_fe,
         "Employer" = employer_fe,
         "Residual" = resid)

stargazer(round(t(vd_df),3), summary = F, title = "Variance Decomposition of Remote Work Share")

# Function to convert linear predictions to the probability scale:
c <- 0
k <- length(fit$residuals) / fit$ssr
m <- mean(df_all_mod$wfh)
a <- log(m/(1-m) ) + k*( c-.5 ) + .5*(1/m - 1/(1-m) )
lin_preds <- predict(fit)
my_preds_logit <- k*(lin_preds-c) + a
my_preds <- 1/(1 + exp(-my_preds_logit))
names(my_preds) <- "ldm_preds"

library(caret)
cm <- confusionMatrix(data = as.factor(as.numeric(my_preds>0.5)), reference = as.factor(df_all_mod$wfh))
cm$byClass["F1"]
table(data.frame("pred" = as.numeric(my_preds>0.5), "real" = df_all_mod$wfh))/length(df_all_mod$wfh)
table(data.frame("pred" = as.numeric(lin_preds>0.5), "real" = df_all_mod$wfh))/length(df_all_mod$wfh)

# Variance Decomposition
df_all_mod <- df_all %>%
  filter(employer != "" & !is.na(employer)) %>%
  filter(year == 2021) %>%
  group_by(bgt_occ6) %>%
  mutate(keep1 = (n()>10)) %>%
  group_by(month) %>%
  mutate(keep2 = (n()>10)) %>%
  group_by(geog) %>%
  mutate(keep3 = (n()>10)) %>%
  group_by(employer) %>%
  mutate(keep4 = (n()>10)) %>%
  ungroup()
nrow(df_all_mod) # 5,371,742
df_all_mod <- df_all_mod %>%
  group_by(bgt_occ6, month, geog, employer) %>%
  filter(!any(keep1==FALSE) & !any(keep2==FALSE) & !any(keep3==FALSE) & !any(keep4==FALSE)) %>%
  ungroup()
nrow(df_all_mod) # 4,620,806

remove(list = setdiff(ls(), c("df_all_mod", "df_all")))
mod_1 <- feols(fml = wfh ~ as.numeric(job_hours=="fulltime" | is.na(job_hours)) | employer + bgt_occ6 + month + geog,
               weights = ~ tot_emp_ad,
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               combine.quick=FALSE,
               data = df_all_mod)

fe_df <- predict(mod_1, fixef = TRUE)
colnames(fe_df) <- paste0(colnames(fe_df), "_fe")
df_all_mod <- bind_cols(df_all_mod, fe_df)


df_all_mod <- df_all_mod %>% filter(!(geog %in% mod_1$fixef_removed$geog))

etable(mod_1)

df_all_mod$pred <- predict(mod_1)
df_all_mod$pred_binary <- as.numeric(df_all_mod$pred>=0.5)
table(df_all_mod$pred_binary, df_all_mod$wfh)
summary(df_all_mod$pred)
table(df_all_mod$pred_binary==df_all_mod$wfh)/nrow(df_all_mod)
fe_df <- predict(mod_1, fixef = TRUE)
colnames(fe_df) <- paste0(colnames(fe_df), "_fe")
df_all_mod <- bind_cols(df_all_mod, fe_df)
df_all_mod$job_hours_fe <- ifelse(df_all_mod$job_hours=="fulltime" | is.na(df_all_mod$job_hours), as.numeric(mod_1$coefficients), 0)
df_all_mod$resid <- mod_1$residuals
summary(df_all_mod$resid)

library(Hmisc)
var_decomp <- df_all_mod
var_decomp <- setDT(var_decomp)
var_decomp <- var_decomp %>%
  group_by(year) %>%
  summarise(pred = wtd.var(pred, weights = tot_emp_ad, na.rm = T),
            wfh = wtd.var(wfh, weights = tot_emp_ad, na.rm = T),
            employer_fe = wtd.var(employer_fe, weights = tot_emp_ad, na.rm = T),
            month_fe = wtd.var(month_fe, weights = tot_emp_ad, na.rm = T),
            bgt_occ6_fe = wtd.var(bgt_occ6_fe, weights = tot_emp_ad, na.rm = T),
            state_fe = wtd.var(state_fe, weights = tot_emp_ad, na.rm = T),
            job_hours_fe = wtd.var(job_hours_fe, weights = tot_emp_ad, na.rm = T),
            resid = wtd.var(resid, weights = tot_emp_ad, na.rm = T))
var_decomp <- var_decomp %>%
  mutate(month_cont = month_fe/pred,
         employer_cont = employer_fe/pred,
         bgt_occ6_cont = bgt_occ6_fe/pred,
         state_cont = state_fe/pred,
         job_hours_cont = job_hours_fe/pred)

head(var_decomp)

var(df_all_mod$bgt_occ6_fe)/var(df_all_mod$wfh)
var(df_all_mod$job_hours_fe)/var(df_all_mod$wfh)
var(df_all_mod$state_fe)/var(df_all_mod$wfh)








# 
#### END ####

#### FIRM-SPECIFIC PLOTS ####
remove(list = ls())
load(file = "./int_data/bgt_structured/us_stru_wfh.RData")
df_us <- df_all_us
rm(df_all_us)
df_us$wfh <- as.numeric(df_us$wfh_prob>0.5)
df_us$bgt_occ6 <- str_sub(df_us$bgt_occ, 1, 7)
df_us$bgt_occ2 <- as.numeric(str_sub(df_us$bgt_occ, 1, 2))
df_us$job_ymd <- ymd(df_us$job_date)
df_us$year_quarter <- as.yearqtr(df_us$job_ymd)
df_us$year_month <- as.yearmon(df_us$job_ymd)
df_us <- setDT(df_us)
df_us$year<-substr(df_us$job_date, 1, 4)

df_us <- df_us[year %in% c(2019, 2021)]

remove(list = setdiff(ls(),"df_us"))
# BAR PLOT 2019, 2021

####### Plot within-industry and occupational change  #######

## Information & Mgmt Occ ##
info<-df_us[employer != "" &  naics3 %in% c("511","519"),
            .(n_posts = .N), 
            by = list(employer, year)]
info<-pivot_wider(info, names_from = year, values_from = n_posts)
info<-na.omit(info)
info$min_posts<-apply(info[,c(2:3)], 1, min)
info_top<-info %>% slice_max(order_by = min_posts, n = 25)
info_top
info_firm<-c("Microsoft Corporation", "Vmware Incorporated", "Salesforce", "Pearson")

df_info_mgmt <- df_us %>%
  filter(employer %in% info_firm & naics3 %in% c("511","519") & bgt_occ2 == "11") %>%
  setDT(.) %>%
  .[, .(wfh_post = sum(wfh), n_post = .N,
        wfh_share = sum(wfh)/.N), by = .(year, employer)] %>%
  filter(n_post >=45) %>%
  setDT(.) %>%
  .[, n_year := .N, by = employer] %>%
  filter(n_year == 2)

df_info_comp <- df_us %>%
  filter(employer %in% info_firm & naics3 %in% c("511","519") & bgt_occ2 == "15") %>%
  setDT(.) %>%
  .[, .(wfh_post = sum(wfh), n_post = .N,
        wfh_share = sum(wfh)/.N), by = .(year, employer)] %>%
  filter(n_post >=45) %>%
  setDT(.) %>%
  .[, n_year := .N, by = employer] %>%
  filter(n_year == 2)

info_firm<-unique(df_info_mgmt$employer[df_info_mgmt$employer %in% df_info_comp$employer])

info_firm
df_info_mgmt
df_info_mgmt$employer<-ifelse(df_info_mgmt$employer == "Adobe Systems", "Adobe",
                              ifelse(df_info_mgmt$employer == "Google Inc.", "Google",
                                     ifelse(df_info_mgmt$employer == "Microsoft Corporation", "Microsoft",
                                            ifelse(df_info_mgmt$employer == "Vmware Incorporated", "VMware", df_info_mgmt$employer))))
df_info_mgmt

sel_order <- 
  df_info_mgmt %>% 
  filter(year == "2021") %>% 
  arrange(wfh_share) %>% 
  mutate(employer = factor(employer))

p_info_mgmt = df_info_mgmt %>%
  mutate(employer = factor(employer, levels = sel_order$employer, ordered = TRUE)) %>% 
  ggplot(aes(x = employer, y = wfh_share, fill = as.factor(year)), group = employer) +
  geom_bar(stat = "identity", width=0.7, position = position_dodge(width=0.7))  +
  ylab("Share (%)") +
  labs(title = "Share of RW Vacancy Postings by Firm", subtitle = "USA IT Sector, Management Occupations" ) +
  scale_y_continuous(breaks = seq(0,4,0.2), limits = c(0,1)) +
  scale_fill_manual(values = c("#000000", "#CC6600")) +
  theme(
    axis.title.y=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 0),
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm"),
        legend.position = c(.7, .2)) +
  guides(fill = guide_legend(ncol = 2)) +
  coord_flip() +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 60))

p_info_mgmt

p_egg <- set_panel_size(p = p_info_mgmt,
                        width = unit(3.5, "in"),
                        height = unit(4*0.3, "in"))

ggsave(p_egg, filename = "./plots/top_us_firms_it_sector_management_occ.pdf", width = 8, height = 4*0.3+3)

## Information & Comp, Math Occ ##
df_info_comp <- df_us %>%
  filter(employer %in% info_firm & naics3 %in% c("511","519") & bgt_occ2 == "15") %>%
  setDT(.) %>%
  .[, .(wfh_post = sum(wfh), n_post = .N,
        wfh_share = sum(wfh)/.N), by = .(year, employer)] %>%
  setDT(.)

df_info_comp$employer<-ifelse(df_info_comp$employer == "Apple Inc.", "Apple",
                              ifelse(df_info_comp$employer == "Google Inc.", "Google",
                                     ifelse(df_info_comp$employer == "Microsoft Corporation", "Microsoft",
                                            ifelse(df_info_comp$employer == "Vmware Incorporated", "VMware", df_info_comp$employer))))

p_info_comp = df_info_comp %>%
  mutate(employer = factor(employer, levels = sel_order$employer, ordered = TRUE)) %>% 
  ggplot(aes(x = employer, y = wfh_share, fill = as.factor(year)), group = employer) +
  geom_bar(stat = "identity", width=0.7, position = position_dodge(width=0.7))  +
  ylab("Share (%)") +
  labs(title = "Share of RW Vacancy Postings by Firm", subtitle = "USA IT Sector, Computer Occupations" ) +
  scale_y_continuous(breaks = seq(0,4,0.2), limits = c(0,1)) +
  scale_fill_manual(values = c("#000000", "#CC6666")) +
  theme(
    axis.title.y=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 0),
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm"),
        legend.position = c(.7, .2)) +
  guides(fill = guide_legend(ncol = 2)) +
  coord_flip() +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 60))

p_info_comp

p_egg <- set_panel_size(p = p_info_comp,
                        width = unit(3.5, "in"),
                        height = unit(4*0.3, "in"))

ggsave(p_egg, filename = "./plots/top_us_firms_it_sector_comp_occ.pdf", width = 12, height = 4*0.3 + 3)


## Finance & Biz Occ ##
fin<-df_us[employer != "" &  sector == "52", .
           (n_posts = .N), 
           by = list(employer, year)]
fin<-pivot_wider(fin, names_from = year, values_from = n_posts)
fin<-na.omit(fin)
fin <- fin[fin$employer != "State Farm Insurance Companies",]
fin$min_posts<-apply(fin[,c(2:3)], 1, min)
fin_top<-fin %>% slice_max(order_by = min_posts, n = 10)
fin_firm<-fin_top$employer

fin_firm

df_fin_biz <- df_us %>%
  filter(employer %in% fin_firm & sector == "52" & bgt_occ2 == "13") %>%
  setDT(.) %>%
  .[, .(wfh_post = sum(wfh), n_post = .N,
        wfh_share = sum(wfh)/.N), by = .(year, employer)] %>%
  filter(n_post >=45) %>%
  setDT(.) %>%
  .[, n_year := .N, by = employer] %>%
  filter(n_year == 2)

df_fin_mgmt <- df_us %>%
  filter(employer %in% fin_firm & sector == "52" & bgt_occ2 == "11") %>%
  setDT(.) %>%
  .[, .(wfh_post = sum(wfh), n_post = .N,
        wfh_share = sum(wfh)/.N), by = .(year, employer)] %>%
  filter(n_post >=45) %>%
  setDT(.) %>%
  .[, n_year := .N, by = employer] %>%
  filter(n_year == 2)

fin_firm<-unique(df_fin_biz$employer[df_fin_biz$employer %in% df_fin_mgmt$employer])

df_fin_biz$employer<-ifelse(df_fin_biz$employer == "JP Morgan Chase Company", "JP Morgan",
                            ifelse(df_fin_biz$employer == "The PNC Financial Services Group, Inc.", "PNC Financial",
                                   ifelse(df_fin_biz$employer == "State Farm Insurance Companies", "State Farm",df_fin_biz$employer)))

sel_order_fin <- 
  df_fin_biz %>% 
  filter(year == "2021") %>% 
  arrange(wfh_share) %>% 
  mutate(employer = factor(employer))

p_fin_biz = df_fin_biz %>%
  mutate(employer = factor(employer, levels = sel_order_fin$employer, ordered = TRUE)) %>% 
  ggplot(aes(x = employer, y = wfh_share, fill = as.factor(year)), group = employer) +
  geom_bar(stat = "identity", width=0.7, position = position_dodge(width=0.7))  +
  ylab("Share (%)") +
  labs(title = "Share of RW Vacancy Postings by Firm", subtitle = "USA Finance/Insurance Sector, Business/Finance Occupations" ) +
  scale_y_continuous(breaks = seq(0,4,0.2), limits = c(0,0.9)) +
  scale_fill_manual(values = c("#000000", "#33CC33")) +
  theme(
    axis.title.y=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 0),
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm"),
        legend.position = c(.7, .1)) +
  guides(fill = guide_legend(ncol = 2)) +
  coord_flip() +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 60))

p_fin_biz

p_egg <- set_panel_size(p = p_fin_biz,
                        width = unit(3.5, "in"),
                        height = unit(10*0.3, "in"))

ggsave(p_egg, filename = "./plots/top_us_firms_fin_sector_busfin_occ.pdf", width = 12, height = 10*0.3 + 3)

### fin & mgmt ###

df_fin_mgmt <- df_us %>%
  filter(employer %in% fin_firm & sector == "52" & bgt_occ2 == "11") %>%
  setDT(.) %>%
  .[, .(wfh_post = sum(wfh), n_post = .N,
        wfh_share = sum(wfh)/.N), by = .(year, employer)] %>%
  setDT(.)

df_fin_mgmt$employer<-ifelse(df_fin_mgmt$employer == "JP Morgan Chase Company", "JP Morgan",
                             ifelse(df_fin_mgmt$employer == "The PNC Financial Services Group, Inc.", "PNC Financial",
                                    ifelse(df_fin_mgmt$employer == "State Farm Insurance Companies", "State Farm",df_fin_mgmt$employer)))


p_fin_mgmt = df_fin_mgmt %>%
  mutate(employer = factor(employer, levels = sel_order_fin$employer, ordered = TRUE)) %>% 
  ggplot(aes(x = employer, y = wfh_share, fill = as.factor(year)), group = employer) +
  geom_bar(stat = "identity", width=0.7, position = position_dodge(width=0.7))  +
  ylab("Share (%)") +
  labs(title = "Share of RW Vacancy Postings by Firm", subtitle = "USA Finance/Insurance Sector, Management Occupations" ) +
  scale_y_continuous(breaks = seq(0,4,0.2), limits = c(0,0.8)) +
  scale_fill_manual(values = c("#000000", "#CC6600")) +
  theme(
    axis.title.y=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 0),
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm"),
        legend.position = c(.7, .1)) +
  guides(fill = guide_legend(ncol = 2)) +
  coord_flip() +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 60))

p_fin_mgmt

p_egg <- set_panel_size(p = p_fin_mgmt,
                        width = unit(3.5, "in"),
                        height = unit(10*0.3, "in"))

ggsave(p_egg, filename = "./plots/top_us_firms_fin_sector_man_occ.pdf", width = 12, height = 10*0.3+3)

remove(list = setdiff(ls(),"df_us"))

## Edu & Mgmt, Occ ##
edu_all_us<-df_us[employer != "" &  sector == "61" & bgt_occ2 %in% c(11,14,31,33),
                  .(n_posts = .N), 
                  by = list(employer, year)]
edu_all_us<-pivot_wider(edu_all_us, names_from = year, values_from = n_posts)
edu_all_us<-na.omit(edu_all_us)
edu_all_us$min_posts<-apply(edu_all_us[,c(2:3)], 1, min)
edu_top<-edu_all_us %>% slice_max(order_by = min_posts, n = 14)
edu_firm<-edu_top$employer
edu_firm
us_edu<-c("Stanford University", "University of Chicago", "Harvard University")
edu_firm<-append(edu_firm, us_edu)

df_edu_us <- df_us %>%
  filter(employer %in% edu_firm  & sector == "61" & bgt_occ2 %in% c(11,14,31,33)) %>%
  setDT(.) %>%
  .[, .(wfh_post = sum(wfh), n_post = .N,
        wfh_share = sum(wfh)/.N), 
    by = .(year, employer)] %>%
  setDT(.)

df_edu_us$employer<-str_replace_all(df_edu_us$employer, "The ", "")
df_edu_us$employer<-str_replace_all(df_edu_us$employer, "University", "U")
df_edu_us$employer<-str_replace_all(df_edu_us$employer, "Pennsylvania", "Penn")

sel_order_edu <- 
  df_edu_us %>% 
  filter(year == 2021) %>% 
  arrange(wfh_share) %>% 
  mutate(employer = factor(employer))

p_edu = df_edu_us %>%
  mutate(employer = factor(employer, levels = sel_order_edu$employer, ordered = TRUE)) %>% 
  ggplot(aes(x = employer, y = wfh_share, fill = as.factor(year)), group = employer) +
  geom_bar(stat = "identity", width=0.7, position = position_dodge(width=0.7))  +
  ylab("Share (%)") +
  labs(title = "Share of RW Vacancy Postings by Firm", subtitle = "USA Tertiary Education Sector, Office Occupations" ) +
  scale_y_continuous(breaks = seq(0,4,0.2), limits = c(0,1)) +
  scale_fill_manual(values = c("#000000", "#FFCC33")) +
  theme(
    axis.title.y=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 0),
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm"),
        legend.position = c(.7, .1)) +
  guides(fill = guide_legend(ncol = 2)) +
  coord_flip() +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 60))

p_edu

p_egg <- set_panel_size(p = p_edu,
                        width = unit(3.5, "in"),
                        height = unit(16*0.3, "in"))

ggsave(p_egg, filename = "./plots/top_us_firms_tertiary_educ_sector_wc_occ.pdf", width = 12, height = 16*0.3 + 3)

#### government ####

df_gov <- df_us %>%
  filter(employer != "" & sector == "92" ) %>%
  setDT(.) %>%
  .[, .(n_posts = .N), 
    by = .(year, employer)] 
gov_us<-pivot_wider(df_gov, names_from = year, values_from = n_posts)
gov_us<-na.omit(gov_us)
gov_us$min_posts<-apply(gov_us[,c(2:3)], 1, min)
gov_top<-gov_us %>% slice_max(order_by = min_posts, n = 14)
gov_firm<-gov_top$employer

df_gov<-pivot_wider(df_gov, names_from = year, values_from = n_posts)
df_gov<-na.omit(df_gov)
df_gov$min_posts<-apply(df_gov[,c(2:3)], 1, min)

df_gov_us <- df_us %>%
  filter(employer %in% gov_firm  & sector == "92") %>%
  setDT(.) %>%
  .[, .(wfh_post = sum(wfh), n_post = .N,
        wfh_share = sum(wfh)/.N), 
    by = .(year, employer)] %>%
  setDT(.)

sel_order_gov <- 
  df_gov_us %>% 
  filter(year == "2021") %>% 
  arrange(wfh_share) %>% 
  mutate(employer = factor(employer))

p_gov = df_gov_us %>%
  mutate(employer = factor(employer, levels = sel_order_gov$employer, ordered = TRUE)) %>% 
  ggplot(aes(x = employer, y = wfh_share, fill = as.factor(year)), group = employer) +
  geom_bar(stat = "identity", width=0.7, position = position_dodge(width=0.7))  +
  ylab("Share (%)") +
  labs(title = "Share of RW Vacancy Postings by Firm", subtitle = "USA Government Sector, All Occupations" ) +
  scale_y_continuous(breaks = seq(0,4,0.2), limits = c(0,1)) +
  scale_fill_manual(values = c("#000000", "#CC0000")) +
  theme(
    axis.title.y=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 0),
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm"),
        legend.position = c(.7, .1)) +
  guides(fill = guide_legend(ncol = 2)) +
  coord_flip() +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 60))

p_gov

p_egg <- set_panel_size(p = p_gov,
                        width = unit(3.5, "in"),
                        height = unit(14*0.3, "in"))

ggsave(p_egg, filename = "./plots/top_us_firms_gov_sector.pdf", width = 12, height = 14*0.3 + 3)

remove(list = setdiff(ls(),"df_us"))

#### END ####

#### FIRM LEVEL ANALYSIS ####
remove(list = ls())
remove(list = setdiff(ls(), "df_all"))
df_all <- fread(file = "./int_data/df_all_standardised.csv", nThread = 8)
df_all$month <- factor(df_all$month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))
df_all$wfh <- as.numeric(df_all$wfh_prob>0.5)
df_all$bgt_occ6 <- str_sub(df_all$bgt_occ, 1, 7)
df_all$bgt_occ4 <- str_sub(df_all$bgt_occ, 1, 5)
df_all$bgt_occ2 <- as.numeric(str_sub(df_all$bgt_occ, 1, 2))
df_all$job_ymd <- ymd(df_all$job_date)
df_all$year_quarter <- as.yearqtr(df_all$job_ymd)
df_all$year_month <- as.yearmon(df_all$job_ymd)
df_all <- setDT(df_all)

keep_firms <- df_all %>%
  filter(!is.na(employer) & employer != "") %>%
  filter(year %in% c(2019, 2021)) %>%
  mutate(employer_country = paste0(employer," (", country,")")) %>%
  group_by(country, employer_country, year) %>%
  summarise(n_posts = sum(job_id_weight)) %>%
  group_by(employer_country) %>%
  summarise(min_n_posts = min(n_posts),
            gm_n_posts = exp((log(n_posts) + log(lag(n_posts)))/2)) %>%
  ungroup() %>%
  filter(!is.na(gm_n_posts)) %>%
  arrange(desc(min_n_posts)) %>%
  filter(min_n_posts > 10)

# WFH vs Potential
df_all_us <- df_all %>%
  filter(year %in% c(2019, 2021)) %>%
  mutate(employer_country = ifelse(employer_country %in% keep_firms$employer_country, employer_country, "1smallnone")) %>%
  filter(!is.na(bgt_occ4)) %>%
  setDT(.) %>%
  .[, .(job_id_weight = sum(job_id_weight),
        wfh_share = sum(wfh*job_id_weight, na.rm = T)/sum(job_id_weight, na.rm = T)), by = .(country, year, bgt_occ4, employer_country)] %>%
  setDT(.) %>%
  group_by(country, bgt_occ4, employer_country) %>%
  filter(n() == 2) %>%
  mutate(gm_job_id_weight = exp((log(job_id_weight) + log(lag(job_id_weight)))/2)) %>%
  arrange(employer_country,bgt_occ4, year) %>%
  group_by(year) %>%
  mutate(wfh_share_win = Winsorize(wfh_share, probs = c(0.05, 0.95), na.rm = T)) %>%
  ungroup() %>%
  group_by(bgt_occ4, employer_country) %>%
  mutate(wfh_share_change = (wfh_share_win - lag(wfh_share_win))/lag(wfh_share_win)) %>%
  mutate(wfh_share_change_win = Winsorize(wfh_share_change, probs = c(0.05, 0.95), na.rm = T)) %>%
  mutate(wfh_share_lchange = (wfh_share_win - lag(wfh_share_win))) %>%
  mutate(wfh_share_lchange_win = Winsorize(wfh_share_lchange, probs = c(0.05, 0.95), na.rm = T))

mod1 <- feols(data = df_all_us,
              fml = wfh_share_change_win ~ i(employer_country, ref = c("1smallnone (Canada)", "1smallnone (Australia)", "1smallnone (US)", "1smallnone (UK)", "1smallnone (NZ)")) | country^bgt_occ4, weights = ~ gm_job_id_weight)

mod2 <- feols(data = df_all_us,
              fml = wfh_share_lchange_win ~ i(employer_country, ref = c("1smallnone (Canada)", "1smallnone (Australia)", "1smallnone (US)", "1smallnone (UK)", "1smallnone (NZ)")) | country^bgt_occ4, weights = ~ gm_job_id_weight)

df_coefs1 <- as.data.frame(mod1$coeftable) %>% clean_names %>% rownames_to_column(var = "employer_country") %>%
  mutate(employer_country = gsub("employer_country::", "", employer_country, fixed = T)) %>%
  left_join(keep_firms)

df_coefs2 <- as.data.frame(mod2$coeftable) %>% clean_names %>% rownames_to_column(var = "employer_country") %>%
  mutate(employer_country = gsub("employer_country::", "", employer_country, fixed = T)) %>%
  left_join(keep_firms)

df_coefs2_sig <- df_coefs2 %>%
  filter(pr_t < 0.05)

table(df_all$bgt_occ2)



# Occupational Changes
df_all_us <- df_all %>%
  filter(year %in% c(2019, 2021)) %>%
  mutate(employer_country = paste0(employer," (", country,")")) %>%
  filter(employer_country %in% keep_firms$employer_country) %>%
  filter(!is.na(bgt_occ2)) %>%
  setDT(.) %>%
  .[, .(job_id_weight = sum(job_id_weight, na.rm = T),
        wfh_sum = sum(wfh*job_id_weight, na.rm = T)),
    by = .(bgt_occ2, country, year, employer_country)] %>%
  setDT(.) %>%
  .[, .(job_id_weight = sum(job_id_weight, na.rm = T),
        wfh_share = sum(wfh_sum)/sum(job_id_weight),
        share_11 = sum(job_id_weight[bgt_occ2 == 11], na.rm = T)/sum(job_id_weight, na.rm = T),
        share_13 = sum(job_id_weight[bgt_occ2 == 13], na.rm = T)/sum(job_id_weight, na.rm = T),
        share_15 = sum(job_id_weight[bgt_occ2 == 15], na.rm = T)/sum(job_id_weight, na.rm = T),
        share_17 = sum(job_id_weight[bgt_occ2 == 17], na.rm = T)/sum(job_id_weight, na.rm = T),
        share_19 = sum(job_id_weight[bgt_occ2 == 19], na.rm = T)/sum(job_id_weight, na.rm = T),
        share_21 = sum(job_id_weight[bgt_occ2 == 21], na.rm = T)/sum(job_id_weight, na.rm = T),
        share_23 = sum(job_id_weight[bgt_occ2 == 23], na.rm = T)/sum(job_id_weight, na.rm = T),
        share_25 = sum(job_id_weight[bgt_occ2 == 25], na.rm = T)/sum(job_id_weight, na.rm = T),
        share_27 = sum(job_id_weight[bgt_occ2 == 27], na.rm = T)/sum(job_id_weight, na.rm = T),
        share_29 = sum(job_id_weight[bgt_occ2 == 29], na.rm = T)/sum(job_id_weight, na.rm = T),
        share_31 = sum(job_id_weight[bgt_occ2 == 31], na.rm = T)/sum(job_id_weight, na.rm = T),
        share_33 = sum(job_id_weight[bgt_occ2 == 33], na.rm = T)/sum(job_id_weight, na.rm = T),
        share_35 = sum(job_id_weight[bgt_occ2 == 35], na.rm = T)/sum(job_id_weight, na.rm = T),
        share_37 = sum(job_id_weight[bgt_occ2 == 37], na.rm = T)/sum(job_id_weight, na.rm = T),
        share_39 = sum(job_id_weight[bgt_occ2 == 39], na.rm = T)/sum(job_id_weight, na.rm = T),
        share_41 = sum(job_id_weight[bgt_occ2 == 41], na.rm = T)/sum(job_id_weight, na.rm = T),
        share_43 = sum(job_id_weight[bgt_occ2 == 43], na.rm = T)/sum(job_id_weight, na.rm = T),
        share_45 = sum(job_id_weight[bgt_occ2 == 45], na.rm = T)/sum(job_id_weight, na.rm = T),
        share_47 = sum(job_id_weight[bgt_occ2 == 47], na.rm = T)/sum(job_id_weight, na.rm = T),
        share_49 = sum(job_id_weight[bgt_occ2 == 49], na.rm = T)/sum(job_id_weight, na.rm = T),
        share_51 = sum(job_id_weight[bgt_occ2 == 51], na.rm = T)/sum(job_id_weight, na.rm = T),
        share_53 = sum(job_id_weight[bgt_occ2 == 53], na.rm = T)/sum(job_id_weight, na.rm = T)),
    by = .(country, year, employer_country)] %>%
  setDT(.)

mod_11 <- feols(data = df_all_us, fml = share_11 ~ wfh_share | employer_country + year, weights = ~ job_id_weight)
mod_13 <- feols(data = df_all_us, fml = share_13 ~ wfh_share | employer_country + year, weights = ~ job_id_weight)
mod_15 <- feols(data = df_all_us, fml = share_15 ~ wfh_share | employer_country + year, weights = ~ job_id_weight)
mod_17 <- feols(data = df_all_us, fml = share_17 ~ wfh_share | employer_country + year, weights = ~ job_id_weight)
mod_19 <- feols(data = df_all_us, fml = share_19 ~ wfh_share | employer_country + year, weights = ~ job_id_weight)
mod_21 <- feols(data = df_all_us, fml = share_21 ~ wfh_share | employer_country + year, weights = ~ job_id_weight)
mod_23 <- feols(data = df_all_us, fml = share_23 ~ wfh_share | employer_country + year, weights = ~ job_id_weight)
mod_25 <- feols(data = df_all_us, fml = share_25 ~ wfh_share | employer_country + year, weights = ~ job_id_weight)
mod_27 <- feols(data = df_all_us, fml = share_27 ~ wfh_share | employer_country + year, weights = ~ job_id_weight)
mod_29 <- feols(data = df_all_us, fml = share_29 ~ wfh_share | employer_country + year, weights = ~ job_id_weight)
mod_31 <- feols(data = df_all_us, fml = share_31 ~ wfh_share | employer_country + year, weights = ~ job_id_weight)
mod_33 <- feols(data = df_all_us, fml = share_33 ~ wfh_share | employer_country + year, weights = ~ job_id_weight)
mod_35 <- feols(data = df_all_us, fml = share_35 ~ wfh_share | employer_country + year, weights = ~ job_id_weight)
mod_37 <- feols(data = df_all_us, fml = share_37 ~ wfh_share | employer_country + year, weights = ~ job_id_weight)
mod_39 <- feols(data = df_all_us, fml = share_39 ~ wfh_share | employer_country + year, weights = ~ job_id_weight)
mod_41 <- feols(data = df_all_us, fml = share_41 ~ wfh_share | employer_country + year, weights = ~ job_id_weight)
mod_43 <- feols(data = df_all_us, fml = share_43 ~ wfh_share | employer_country + year, weights = ~ job_id_weight)
mod_45 <- feols(data = df_all_us, fml = share_45 ~ wfh_share | employer_country + year, weights = ~ job_id_weight)
mod_47 <- feols(data = df_all_us, fml = share_47 ~ wfh_share | employer_country + year, weights = ~ job_id_weight)
mod_49 <- feols(data = df_all_us, fml = share_49 ~ wfh_share | employer_country + year, weights = ~ job_id_weight)
mod_51 <- feols(data = df_all_us, fml = share_51 ~ wfh_share | employer_country + year, weights = ~ job_id_weight)
mod_53 <- feols(data = df_all_us, fml = share_53 ~ wfh_share | employer_country + year, weights = ~ job_id_weight)


etable(mod_11,mod_13,mod_15,mod_53, tex = T)


#### END ####




