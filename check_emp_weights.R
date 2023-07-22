#### MAKE MONTHLY x COUNTRY SERIES, 2019 EMPLOYMENT WEIGHTS ####
remove(list = setdiff(ls(), "df_all_list"))
dropped_days <- readRDS(file = "./int_data/dropped_days_from_jkf.rds")
dropped_days
# Make Month x BGT Occ 5 digits x Country series (dropping days which are filtered by the Jackknife filter)
df_us <- df_all_list[[5]]
nrow(df_us)

df_us <- df_us %>% select(country, wfh_wham, job_date, bgt_occ, month, job_id_weight, tot_emp_ad)
df_us <- df_us[!is.na(wfh_wham)]
df_us <- df_us[!is.na(bgt_occ)]
df_us <- df_us[, bgt_occ5 := str_sub(bgt_occ, 1, 6)]
df_us <- df_us[, month := factor(month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))]
df_us <- df_us[, job_ymd := ymd(job_date)]
df_us <- df_us[, year_month := as.yearmon(job_ymd)]
df_us <- df_us[, year := year(job_ymd)]
df_us <- setDT(df_us)

nrow(df_us[year_month == as.yearmon(ymd("20230101"))]) #
nrow(df_us[year_month == as.yearmon(ymd("20221201"))]) #

sum(df_us[year_month == as.yearmon(ymd("20230101"))]$tot_emp_ad)
sum(df_us[year_month == as.yearmon(ymd("20221201"))]$tot_emp_ad)
sum(df_us[year_month == as.yearmon(ymd("20221101"))]$tot_emp_ad)

sum(df_us[year_month == as.yearmon(ymd("20230101"))]$tot_emp_ad*df_us[year_month == as.yearmon(ymd("20230101"))]$job_id_weight)
sum(df_us[year_month == as.yearmon(ymd("20221201"))]$tot_emp_ad*df_us[year_month == as.yearmon(ymd("20221201"))]$job_id_weight)
sum(df_us[year_month == as.yearmon(ymd("20221101"))]$tot_emp_ad*df_us[year_month == as.yearmon(ymd("20221101"))]$job_id_weight)

mean(df_us[year_month == as.yearmon(ymd("20230101"))]$wfh_wham*df_us[year_month == as.yearmon(ymd("20230101"))]$job_id_weight)
mean(df_us[year_month == as.yearmon(ymd("20221201"))]$wfh_wham*df_us[year_month == as.yearmon(ymd("20221201"))]$job_id_weight)
mean(df_us[year_month == as.yearmon(ymd("20221101"))]$wfh_wham*df_us[year_month == as.yearmon(ymd("20221101"))]$job_id_weight)

summary(df_us[year_month == as.yearmon(ymd("20230101"))]$tot_emp_ad)
summary(df_us[year_month == as.yearmon(ymd("20220101"))]$tot_emp_ad)

sum(df_us[year_month == as.yearmon(ymd("20230101"))]$wfh_wham*df_us[year_month == as.yearmon(ymd("20230101"))]$tot_emp)/sum(df_us[year_month == as.yearmon(ymd("20230101"))]$tot_emp)
sum(df_us[year_month == as.yearmon(ymd("20221201"))]$wfh_wham*df_us[year_month == as.yearmon(ymd("20221201"))]$tot_emp)/sum(df_us[year_month == as.yearmon(ymd("20221201"))]$tot_emp)
sum(df_us[year_month == as.yearmon(ymd("20221101"))]$wfh_wham*df_us[year_month == as.yearmon(ymd("20221101"))]$tot_emp)/sum(df_us[year_month == as.yearmon(ymd("20221101"))]$tot_emp)

mean(df_us[year_month == as.yearmon(ymd("20230101"))]$wfh_wham)
mean(df_us[year_month == as.yearmon(ymd("20221201"))]$wfh_wham)
mean(df_us[year_month == as.yearmon(ymd("20221101"))]$wfh_wham)
head(df_us)

df_us <- df_us %>% .[, bgt_occ2 := str_sub(bgt_occ, 1, 2)]
df_us <- df_us %>% .[!is.na(bgt_occ2) & bgt_occ2 != ""]

check1 <- df_us[year_month == as.yearmon(ymd("20230101"))] %>% .[, .(emp_jan23 = sum(tot_emp_ad)), by = bgt_occ2] %>% .[, emp_share_jan23 := 100*round(emp_jan23/sum(emp_jan23), 2)]
check2 <- df_us[year_month == as.yearmon(ymd("20221201"))] %>% .[, .(emp_dec22 = sum(tot_emp_ad)), by = bgt_occ2] %>% .[, emp_share_dec22 := 100*round(emp_dec22/sum(emp_dec22), 2)]

check <- check1 %>% left_join(check2)


nrow(df_us)
df_us <- merge(x = df_us, y = dropped_days, all.x = TRUE, all.y = FALSE, by = c("country", "job_date"))
df_us <- df_us[is.na(drop)]
nrow(df_us)
df_us_occ_ag <- df_us %>%
  select(country, bgt_occ5, year_month, year, wfh_wham, job_id_weight, tot_emp_ad) %>%
  setDT(.) %>%
  .[, .(wfh_sum_emp = sum(wfh_wham*tot_emp_ad, na.rm = T),
        emp_sum = sum(tot_emp_ad, na.rm = T)),
    by = .(country, bgt_occ5, year_month, year)] %>%
  .[, wfh_share := wfh_sum / job_ads_sum] %>%
  setDT(.)
  
df_us_occ_ag <- df_us_occ_ag %>%
  rename(monthly_share = wfh_share, N = job_ads_sum)

# Make BGT Occ 5 weights, based on 2019 US employment shares
head(df_us)
shares_df <- df_us_occ_ag %>%
  .[bgt_occ5 != "" & !is.na(bgt_occ5)] %>%
  .[year == 2019] %>%
  .[, .(N = sum(N, na.rm = T)), by = .(bgt_occ5, country)] %>%
  .[, share := N/sum(N, na.rm = T)] %>%
  select(bgt_occ5, share, country)

# Aggregate Monthly x BGT Occ 5 x Country series, using emp weights bsased on 2019 US employment shares
shares_df <- as.data.frame(shares_df) %>% as.data.table(.)
df_us_occ_ag <- as.data.frame(df_us_occ_ag) %>% as.data.table(.)

df_us_occ_ag <- df_us_occ_ag %>%
  .[bgt_occ5 != "" & !is.na(bgt_occ5)] %>%
  left_join(shares_df) %>%
  setDT(.)

sum(df_us_occ_ag[year_month == as.yearmon(ymd("20230101"))]$share*df_us_occ_ag[year_month == as.yearmon(ymd("20230101"))]$monthly_share)
sum(df_us_occ_ag[year_month == as.yearmon(ymd("20221201"))]$share*df_us_occ_ag[year_month == as.yearmon(ymd("20221201"))]$monthly_share)

check <- left_join(df_us_occ_ag[year_month == as.yearmon(ymd("20230101"))] %>% select(bgt_occ5, monthly_share, share) %>% rename(wfh_share_jan23 = monthly_share),
                   df_us_occ_ag[year_month == as.yearmon(ymd("20221201"))] %>% select(bgt_occ5, monthly_share) %>% rename(wfh_share_dec22 = monthly_share))
                   
check$bgt_occ2 <- str_sub(check$bgt_occ5, 1, 2)

check1 <- check %>%
  .[, .(wfh_share_jan23 = sum(wfh_share_jan23*share)/sum(share),
        wfh_share_dec22 = sum(wfh_share_dec22*share)/sum(share),
        share = sum(share)),
    by = bgt_occ2]

View(check1)

View(df_us_occ_ag_2023_jan)

df_us_occ_ag_2023_jan <- df_us_occ_ag %>% .[year_month == as.yearmon(ymd("20230101"))]

df_us_occ_ag <- df_us_occ_ag %>%
  .[, .(monthly_mean = sum(monthly_share*(share/sum(share, na.rm = T)), na.rm = T)), by = .(country, year_month)]

ts_for_plot_glob_emp_weight <- wfh_monthly_bgt_occ_share
saveRDS(ts_for_plot_glob_emp_weight, file = "./int_data/wfh_across_countries_monthly_jkf_2019_global_emp_w.rds")

#### END ####