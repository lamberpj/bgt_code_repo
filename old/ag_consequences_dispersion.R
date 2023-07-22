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


#### NOMINAL WAGE DISPERTION ####

colnames(fread("./int_data/df_us_2019_standardised.csv", nrow = 100))

df_us_2019 <- fread("./int_data/df_us_2019_standardised.csv", nThread = 8, integer64 = "numeric",
                    select = c("job_id", "country","state","msa","county","fips","city",  "job_url", "job_date", "wfh_wham", "narrow_result", "neg_narrow_result",
                               "bgt_occ", "sector_clustered", "employer", "job_id_weight", "job_hours", "disjoint_salary")) %>%
  .[!grepl("careerbuilder", job_url)] %>%
  .[, year_quarter := as.yearqtr(job_date)] %>%
  .[, bgt_occ2 := str_sub(bgt_occ, 1, 2)]

df_us_2019 <- df_us_2019 %>%
  .[bgt_occ != ""]

df_us_2019 <- df_us_2019 %>%
  .[, bgt_occ5 := str_sub(bgt_occ, 1, 6)]

df_us_2019 <- df_us_2019 %>%
  .[, log_sal := log(as.numeric(disjoint_salary))] %>%
  .[, msa := as.character(msa)] %>%
  .[!is.na(log_sal) & !is.na(bgt_occ5) & !is.na(msa)]

df_us_2022 <- fread("./int_data/df_us_2022_standardised.csv", nThread = 8, integer64 = "numeric",
                    select = c("job_id", "country","state","msa","county","fips","city",  "job_url", "job_date", "wfh_wham", "narrow_result", "neg_narrow_result",
                               "bgt_occ", "sector_clustered", "employer", "job_id_weight", "job_hours", "disjoint_salary")) %>%
  .[!grepl("careerbuilder", job_url)] %>%
  .[, year_quarter := as.yearqtr(job_date)] %>%
  .[, bgt_occ2 := str_sub(bgt_occ, 1, 2)]

df_us_2022 <- df_us_2022 %>%
  .[bgt_occ != ""]

df_us_2022 <- df_us_2022 %>%
  .[, bgt_occ5 := str_sub(bgt_occ, 1, 6)]

df_us_2022 <- df_us_2022 %>%
  .[, log_sal := log(as.numeric(disjoint_salary))] %>%
  .[, msa := as.character(msa)] %>%
  .[!is.na(log_sal) & !is.na(bgt_occ5) & !is.na(msa)]

#### VAR DECMOP ####
mod1 <- feols(data = df_us_2019, fml = log_sal ~ -1 | bgt_occ2 + msa)

fixedEffects = fixef(mod1)
fe_bgt_occ2 <- as.data.table(fixedEffects$bgt_occ, keep.rownames = T) %>% rename(bgt_occ2 = V1, bgt_occ2_fe = V2)
fe_bgt_msa <- as.data.table(fixedEffects$msa, keep.rownames = T) %>% rename(msa = V1, msa_fe = V2)

df_us_2019 <- df_us_2019 %>%
  mutate(msa = as.character(msa)) %>%
  left_join(., fe_bgt_occ2) %>%
  left_join(., fe_bgt_msa) %>%
  mutate(resid = mod1$residuals) %>%
  setDT(.)

View(head(df_us_2019, 1000))

(var_log_sal <- sd(df_us_2019$log_sal)^2)
(var_fe_bgt_occ2 <- sd(df_us_2019$bgt_occ2_fe, na.rm = T)^2)
(var_fe_msa <- sd(df_us_2019$msa_fe)^2)
(var_resid <- sd(df_us_2019$resid)^2)
(cov_log_sal <- cov(x = as.vector(df_us_2019$bgt_occ2_fe), y = as.vector(df_us_2019$msa_fe)))

var_fe_bgt_occ2/var_log_sal # 0.4112532
var_fe_msa/var_log_sal # 0.02136849
var_resid/var_log_sal # 0.5553906
cov_log_sal/var_log_sal # 0.005993822

#### END ####

# SPATIAL DISPERSION #
df_us_2019_msa_soc2 <- df_us_2019 %>%
  .[, .(.N,
        wfh_wham_share = mean(wfh_wham, na.rm = T),
        has_salary = sum(!is.na(as.numeric(disjoint_salary))),
        mean_salary = mean(as.numeric(disjoint_salary), na.rm = T),
        med_salary = median(as.numeric(disjoint_salary), na.rm = T)),
    by = .(bgt_occ5, msa)]

df_us_2019_soc <- df_us_2019_msa_soc2 %>%
  .[has_salary > 100] %>%
  .[, .(posts = sum(N),
        wfh_wham_share = sum(N*wfh_wham_share)/sum(N),
        iqr_mean_salary = IQR(mean_salary),
        mm_mean_salary = max(mean_salary) - min(mean_salary),
        sd_mean_salary = sd(mean_salary),
        gmean_salary = mean(mean_salary),
        count_msa = .N),
    by = bgt_occ5] %>%
  .[count_msa > 50]

df_us_2022_msa_soc2 <- df_us_2022 %>%
  .[, .(.N,
        wfh_wham_share = mean(wfh_wham, na.rm = T),
        has_salary = sum(!is.na(as.numeric(disjoint_salary))),
        mean_salary = mean(as.numeric(disjoint_salary), na.rm = T),
        med_salary = median(as.numeric(disjoint_salary), na.rm = T)),
    by = .(bgt_occ5, msa)]

df_us_2022_soc <- df_us_2022_msa_soc2 %>%
  .[has_salary > 100] %>%
  .[, .(posts = sum(N),
        wfh_wham_share = sum(N*wfh_wham_share)/sum(N),
        iqr_mean_salary = IQR(mean_salary),
        mm_mean_salary = max(mean_salary) - min(mean_salary),
        sd_mean_salary = sd(mean_salary),
        gmean_salary = mean(mean_salary),
        count_msa = .N),
    by = bgt_occ5] %>%
  .[count_msa > 50]

feols(data = df_us_2019_soc,
      fml = log(sd_mean_salary) ~ log(wfh_wham_share))

feols(data = df_us_2019_soc,
      fml = log(sd_mean_salary) ~ log(wfh_wham_share) + log(gmean_salary))

feols(data = df_us_2022_soc,
      fml = log(sd_mean_salary) ~ log(wfh_wham_share))

feols(data = df_us_2022_soc,
      fml = log(sd_mean_salary) ~ log(wfh_wham_share) + log(gmean_salary))

df_us_soc <- df_us_2019_soc %>%
  left_join(., df_us_2022_soc, by = "bgt_occ5", suffix = c("_2019", "_2022"))

head(df_us_soc %>% select(bgt_occ5, iqr_mean_salary_2019, iqr_mean_salary_2022) %>% arrange(desc(iqr_mean_salary_2019)))

tail(df_us_soc %>% select(bgt_occ5, iqr_mean_salary_2019, iqr_mean_salary_2022) %>% arrange(desc(iqr_mean_salary_2019)))

p = ggplot(data = )

summary(df_us_soc[, c("bgt_occ5", "iqr_mean_salary_2019", "iqr_mean_salary_2022")])

df_us_soc <- df_us_soc %>%
  mutate(diff_sd_mean_salary = sd_mean_salary_2022 - sd_mean_salary_2019,
         diff_iqr_mean_salary = iqr_mean_salary_2022 - iqr_mean_salary_2019,
         diff_wfh_wham_share = wfh_wham_share_2022 - wfh_wham_share_2019,
         diff_gmean_salary = gmean_salary_2022 - gmean_salary_2019,
         dhs_sd_mean_salary = (sd_mean_salary_2022 - sd_mean_salary_2019)/(0.5*(sd_mean_salary_2022 + sd_mean_salary_2019)),
         dhs_iqr_mean_salary = (iqr_mean_salary_2022 - iqr_mean_salary_2019)/(0.5*(iqr_mean_salary_2022 + iqr_mean_salary_2019)),
         dhs_wfh_wham_share = (wfh_wham_share_2022 - wfh_wham_share_2019)/(0.5*(wfh_wham_share_2022 + wfh_wham_share_2019)),
         dhs_gmean_salary = (gmean_salary_2022 - gmean_salary_2019)/(0.5*(gmean_salary_2022 + gmean_salary_2019)),
         pdiff_sd_mean_salary = log(sd_mean_salary_2022) - log(sd_mean_salary_2019),
         pdiff_iqr_mean_salary = log(iqr_mean_salary_2022) - log(iqr_mean_salary_2019),
         pdiff_wfh_wham_share = log(wfh_wham_share_2022) - log(wfh_wham_share_2019),
         pdiff_gmean_salary = log(gmean_salary_2022) - log(gmean_salary_2019))


library(stargazer)
df <- as.data.frame(df_us_soc[, c("diff_iqr_mean_salary", "pdiff_iqr_mean_salary", "diff_sd_mean_salary", "pdiff_sd_mean_salary")])

stargazer(df, digits = 1, median = TRUE)

df_us_msa_soc2 <- df_us_2019_msa_soc2 %>%
  left_join(., df_us_2022_msa_soc2, by = c("msa", "bgt_occ5"), suffix = c("_2019", "_2022"))

colnames(df_us_msa_soc2)

df_us_msa_soc2 <- df_us_msa_soc2 %>%
  mutate(diff_wfh_wham_share = wfh_wham_share_2022 - wfh_wham_share_2019,
         diff_mean_salary = mean_salary_2022 - mean_salary_2019,
         pdiff_wfh_wham_share = log(wfh_wham_share_2022) - log(wfh_wham_share_2019),
         pdiff_mean_salary = log(mean_salary_2022) - log(mean_salary_2019))

feols(data = df_us_msa_soc2,
      fml = pdiff_mean_salary ~ pdiff_wfh_wham_share)


#### FIRMS ####
remove(list = ls())

df_us_2019 <- fread("./int_data/df_us_2019_standardised.csv", nThread = 8, integer64 = "numeric",
                    select = c("job_id", "country","state","msa","county","fips","city",  "job_url", "job_date", "wfh_wham", "narrow_result", "neg_narrow_result",
                               "bgt_occ", "sector_clustered", "employer", "job_id_weight", "job_hours", "disjoint_salary")) %>%
  .[!grepl("careerbuilder", job_url)] %>%
  .[, year_quarter := as.yearqtr(job_date)] %>%
  .[, bgt_occ2 := str_sub(bgt_occ, 1, 2)] %>%
  .[bgt_occ != ""] %>%
  .[, bgt_occ5 := str_sub(bgt_occ, 1, 6)] %>%
  .[, year := 2019] %>%
  .[as.yearqtr(job_date) %in% as.yearqtr(c("2019 Q1", "2019 Q2", "2019 Q3", "2019 Q4", "2021 Q3", "2021 Q4", "2022 Q1", "2022 Q2"))]

df_us_2021 <- fread("./int_data/df_us_2021_standardised.csv", nThread = 8, integer64 = "numeric",
                    select = c("job_id", "country","state","msa","county","fips","city",  "job_url", "job_date", "wfh_wham", "narrow_result", "neg_narrow_result",
                               "bgt_occ", "sector_clustered", "employer", "job_id_weight", "job_hours", "disjoint_salary")) %>%
  .[!grepl("careerbuilder", job_url)] %>%
  .[, year_quarter := as.yearqtr(job_date)] %>%
  .[, bgt_occ2 := str_sub(bgt_occ, 1, 2)] %>%
  .[bgt_occ != ""] %>%
  .[, bgt_occ5 := str_sub(bgt_occ, 1, 6)] %>%
  .[, year := 2021] %>%
  .[as.yearqtr(job_date) %in% as.yearqtr(c("2019 Q1", "2019 Q2", "2019 Q3", "2019 Q4", "2021 Q3", "2021 Q4", "2022 Q1", "2022 Q2"))]

df_us_2022 <- fread("./int_data/df_us_2022_standardised.csv", nThread = 8, integer64 = "numeric",
                    select = c("job_id", "country","state","msa","county","fips","city",  "job_url", "job_date", "wfh_wham", "narrow_result", "neg_narrow_result",
                               "bgt_occ", "sector_clustered", "employer", "job_id_weight", "job_hours", "disjoint_salary")) %>%
  .[!grepl("careerbuilder", job_url)] %>%
  .[, year_quarter := as.yearqtr(job_date)] %>%
  .[, bgt_occ2 := str_sub(bgt_occ, 1, 2)] %>%
  .[bgt_occ != ""] %>%
  .[, bgt_occ5 := str_sub(bgt_occ, 1, 6)] %>%
  .[, year := 2022] %>%
  .[as.yearqtr(job_date) %in% as.yearqtr(c("2019 Q1", "2019 Q2", "2019 Q3", "2019 Q4", "2021 Q3", "2021 Q4", "2022 Q1", "2022 Q2"))]

df_us <- bind_rows(df_us_2019, df_us_2021, df_us_2022)

remove(list = setdiff(ls(), "df_us"))

df_us <- df_us %>%
  .[, year := ifelse(year == 2021, 2022, year)]

sort(unique(df_us$bgt_occ2))

df_us_firm_year <- df_us %>%
  .[, .(sector_clustered = sector_clustered[1],
        n_msa = uniqueN(msa),
        n_posts = .N,
        wfh_wham_share = mean(wfh_wham, na.rm = T),
        share_11 = sum(bgt_occ2 == 11) / .N,
        share_13 = sum(bgt_occ2 == 13) / .N,
        share_15 = sum(bgt_occ2 == 15) / .N,
        share_17 = sum(bgt_occ2 == 17) / .N,
        share_19 = sum(bgt_occ2 == 19) / .N,
        share_21 = sum(bgt_occ2 == 21) / .N,
        share_23 = sum(bgt_occ2 == 23) / .N,
        share_25 = sum(bgt_occ2 == 25) / .N,
        share_27 = sum(bgt_occ2 == 27) / .N,
        share_29 = sum(bgt_occ2 == 29) / .N,
        share_31 = sum(bgt_occ2 == 31) / .N,
        share_33 = sum(bgt_occ2 == 33) / .N,
        share_35 = sum(bgt_occ2 == 35) / .N,
        share_37 = sum(bgt_occ2 == 37) / .N,
        share_39 = sum(bgt_occ2 == 39) / .N,
        share_41 = sum(bgt_occ2 == 41) / .N,
        share_43 = sum(bgt_occ2 == 43) / .N,
        share_45 = sum(bgt_occ2 == 45) / .N,
        share_47 = sum(bgt_occ2 == 47) / .N,
        share_49 = sum(bgt_occ2 == 49) / .N,
        share_51 = sum(bgt_occ2 == 51) / .N,
        share_53 = sum(bgt_occ2 == 53) / .N,
        share_55 = sum(bgt_occ2 == 55) / .N),
    by = .(year, employer)
    ] %>%
  group_by(employer) %>%
  pivot_wider(., names_from = year, values_from = n_msa:share_55) %>%
  filter(n_posts_2022 > 100 & n_posts_2019 > 100)

df_us_firm_year <- df_us_firm_year %>%
  mutate(ad_n_msa = n_msa_2022 - n_msa_2019,
         ad_n_posts = n_posts_2022 - n_posts_2019,
         ad_wfh_wham_share = wfh_wham_share_2022 - wfh_wham_share_2019,
         ad_share_11 = share_11_2022 - share_11_2019,
         ad_share_13 = share_13_2022 - share_13_2019,
         ad_share_15 = share_15_2022 - share_15_2019,
         ad_share_17 = share_17_2022 - share_17_2019,
         ad_share_19 = share_19_2022 - share_19_2019,
         ad_share_21 = share_21_2022 - share_21_2019,
         ad_share_23 = share_23_2022 - share_23_2019,
         ad_share_25 = share_25_2022 - share_25_2019,
         ad_share_27 = share_27_2022 - share_27_2019,
         ad_share_29 = share_29_2022 - share_29_2019,
         ad_share_31 = share_31_2022 - share_31_2019,
         ad_share_33 = share_33_2022 - share_33_2019,
         ad_share_35 = share_35_2022 - share_35_2019,
         ad_share_37 = share_37_2022 - share_37_2019,
         ad_share_39 = share_39_2022 - share_39_2019,
         ad_share_41 = share_41_2022 - share_41_2019,
         ad_share_43 = share_43_2022 - share_43_2019,
         ad_share_45 = share_45_2022 - share_45_2019,
         ad_share_47 = share_47_2022 - share_47_2019,
         ad_share_49 = share_49_2022 - share_49_2019,
         ad_share_51 = share_51_2022 - share_51_2019,
         ad_share_53 = share_53_2022 - share_53_2019,
         ad_share_55 = share_55_2022 - share_55_2019,
         dhs_n_msa = n_msa_2022 - n_msa_2019/(0.5*(n_msa_2022 + n_msa_2019)),
         dhs_n_posts = n_posts_2022 - n_posts_2019/(0.5*(n_posts_2022 + n_posts_2019)),
         dhs_wfh_wham_share = wfh_wham_share_2022 - wfh_wham_share_2019/(0.5*(wfh_wham_share_2022 + wfh_wham_share_2019)),
         dhs_share_11 = share_11_2022 - share_11_2019/(0.5*(share_11_2022 + share_11_2019)),
         dhs_share_13 = share_13_2022 - share_13_2019/(0.5*(share_13_2022 + share_13_2019)),
         dhs_share_15 = share_15_2022 - share_15_2019/(0.5*(share_15_2022 + share_15_2019)),
         dhs_share_17 = share_17_2022 - share_17_2019/(0.5*(share_17_2022 + share_17_2019)),
         dhs_share_19 = share_19_2022 - share_19_2019/(0.5*(share_19_2022 + share_19_2019)),
         dhs_share_21 = share_21_2022 - share_21_2019/(0.5*(share_21_2022 + share_21_2019)),
         dhs_share_23 = share_23_2022 - share_23_2019/(0.5*(share_23_2022 + share_23_2019)),
         dhs_share_25 = share_25_2022 - share_25_2019/(0.5*(share_25_2022 + share_25_2019)),
         dhs_share_27 = share_27_2022 - share_27_2019/(0.5*(share_27_2022 + share_27_2019)),
         dhs_share_29 = share_29_2022 - share_29_2019/(0.5*(share_29_2022 + share_29_2019)),
         dhs_share_31 = share_31_2022 - share_31_2019/(0.5*(share_31_2022 + share_31_2019)),
         dhs_share_33 = share_33_2022 - share_33_2019/(0.5*(share_33_2022 + share_33_2019)),
         dhs_share_35 = share_35_2022 - share_35_2019/(0.5*(share_35_2022 + share_35_2019)),
         dhs_share_37 = share_37_2022 - share_37_2019/(0.5*(share_37_2022 + share_37_2019)),
         dhs_share_39 = share_39_2022 - share_39_2019/(0.5*(share_39_2022 + share_39_2019)),
         dhs_share_41 = share_41_2022 - share_41_2019/(0.5*(share_41_2022 + share_41_2019)),
         dhs_share_43 = share_43_2022 - share_43_2019/(0.5*(share_43_2022 + share_43_2019)),
         dhs_share_45 = share_45_2022 - share_45_2019/(0.5*(share_45_2022 + share_45_2019)),
         dhs_share_47 = share_47_2022 - share_47_2019/(0.5*(share_47_2022 + share_47_2019)),
         dhs_share_49 = share_49_2022 - share_49_2019/(0.5*(share_49_2022 + share_49_2019)),
         dhs_share_51 = share_51_2022 - share_51_2019/(0.5*(share_51_2022 + share_51_2019)),
         dhs_share_53 = share_53_2022 - share_53_2019/(0.5*(share_53_2022 + share_53_2019)),
         dhs_share_55 = share_55_2022 - share_55_2019/(0.5*(share_55_2022 + share_55_2019)))

df_us_firm_year <- df_us_firm_year %>%
  mutate(
    e_share_11 = as.numeric(share_11_2022>0) - as.numeric(share_11_2019>0),
    e_share_13 = as.numeric(share_13_2022>0) - as.numeric(share_13_2019>0),
    e_share_15 = as.numeric(share_15_2022>0) - as.numeric(share_15_2019>0),
    e_share_17 = as.numeric(share_17_2022>0) - as.numeric(share_17_2019>0),
    e_share_19 = as.numeric(share_19_2022>0) - as.numeric(share_19_2019>0),
    e_share_21 = as.numeric(share_21_2022>0) - as.numeric(share_21_2019>0),
    e_share_23 = as.numeric(share_23_2022>0) - as.numeric(share_23_2019>0),
    e_share_25 = as.numeric(share_25_2022>0) - as.numeric(share_25_2019>0),
    e_share_27 = as.numeric(share_27_2022>0) - as.numeric(share_27_2019>0),
    e_share_29 = as.numeric(share_29_2022>0) - as.numeric(share_29_2019>0),
    e_share_31 = as.numeric(share_31_2022>0) - as.numeric(share_31_2019>0),
    e_share_33 = as.numeric(share_33_2022>0) - as.numeric(share_33_2019>0),
    e_share_35 = as.numeric(share_35_2022>0) - as.numeric(share_35_2019>0),
    e_share_37 = as.numeric(share_37_2022>0) - as.numeric(share_37_2019>0),
    e_share_39 = as.numeric(share_39_2022>0) - as.numeric(share_39_2019>0),
    e_share_41 = as.numeric(share_41_2022>0) - as.numeric(share_41_2019>0),
    e_share_43 = as.numeric(share_43_2022>0) - as.numeric(share_43_2019>0),
    e_share_45 = as.numeric(share_45_2022>0) - as.numeric(share_45_2019>0),
    e_share_47 = as.numeric(share_47_2022>0) - as.numeric(share_47_2019>0),
    e_share_49 = as.numeric(share_49_2022>0) - as.numeric(share_49_2019>0),
    e_share_51 = as.numeric(share_51_2022>0) - as.numeric(share_51_2019>0),
    e_share_53 = as.numeric(share_53_2022>0) - as.numeric(share_53_2019>0),
    e_share_55 = as.numeric(share_55_2022>0) - as.numeric(share_55_2019>0))

df_us_firm_year <- df_us_firm_year %>%
  mutate(
    n1_wfh_wham_share = as.numeric(wfh_wham_share_2022>0) - as.numeric(wfh_wham_share_2019==0))

i_mod11 <- feols(data = df_us_firm_year, fml = dhs_share_11 ~ dhs_wfh_wham_share | sector_clustered)
i_mod13 <- feols(data = df_us_firm_year, fml = dhs_share_13 ~ dhs_wfh_wham_share | sector_clustered)
i_mod15 <- feols(data = df_us_firm_year, fml = dhs_share_15 ~ dhs_wfh_wham_share | sector_clustered)
i_mod17 <- feols(data = df_us_firm_year, fml = dhs_share_17 ~ dhs_wfh_wham_share | sector_clustered)
i_mod19 <- feols(data = df_us_firm_year, fml = dhs_share_19 ~ dhs_wfh_wham_share | sector_clustered)
i_mod21 <- feols(data = df_us_firm_year, fml = dhs_share_21 ~ dhs_wfh_wham_share | sector_clustered)
i_mod23 <- feols(data = df_us_firm_year, fml = dhs_share_23 ~ dhs_wfh_wham_share | sector_clustered)
i_mod25 <- feols(data = df_us_firm_year, fml = dhs_share_25 ~ dhs_wfh_wham_share | sector_clustered)
i_mod27 <- feols(data = df_us_firm_year, fml = dhs_share_27 ~ dhs_wfh_wham_share | sector_clustered)
i_mod29 <- feols(data = df_us_firm_year, fml = dhs_share_29 ~ dhs_wfh_wham_share | sector_clustered)
i_mod31 <- feols(data = df_us_firm_year, fml = dhs_share_31 ~ dhs_wfh_wham_share | sector_clustered)
i_mod33 <- feols(data = df_us_firm_year, fml = dhs_share_33 ~ dhs_wfh_wham_share | sector_clustered)
i_mod35 <- feols(data = df_us_firm_year, fml = dhs_share_35 ~ dhs_wfh_wham_share | sector_clustered)
i_mod37 <- feols(data = df_us_firm_year, fml = dhs_share_37 ~ dhs_wfh_wham_share | sector_clustered)
i_mod39 <- feols(data = df_us_firm_year, fml = dhs_share_39 ~ dhs_wfh_wham_share | sector_clustered)
i_mod41 <- feols(data = df_us_firm_year, fml = dhs_share_41 ~ dhs_wfh_wham_share | sector_clustered)
i_mod43 <- feols(data = df_us_firm_year, fml = dhs_share_43 ~ dhs_wfh_wham_share | sector_clustered)
i_mod45 <- feols(data = df_us_firm_year, fml = dhs_share_45 ~ dhs_wfh_wham_share | sector_clustered)
i_mod47 <- feols(data = df_us_firm_year, fml = dhs_share_47 ~ dhs_wfh_wham_share | sector_clustered)
i_mod49 <- feols(data = df_us_firm_year, fml = dhs_share_49 ~ dhs_wfh_wham_share | sector_clustered)
i_mod51 <- feols(data = df_us_firm_year, fml = dhs_share_51 ~ dhs_wfh_wham_share | sector_clustered)
i_mod53 <- feols(data = df_us_firm_year, fml = dhs_share_53 ~ dhs_wfh_wham_share | sector_clustered)

etable(i_mod11,i_mod13,i_mod15,i_mod17,i_mod19,i_mod21,i_mod23,i_mod25,i_mod27,i_mod29,i_mod31,i_mod33,i_mod35,i_mod37,i_mod39,i_mod41,i_mod43,i_mod45,i_mod47,i_mod49,i_mod51,i_mod53)

e_mod11 <- feols(data = df_us_firm_year, fml = e_share_11 ~ dhs_wfh_wham_share | sector_clustered)
e_mod13 <- feols(data = df_us_firm_year, fml = e_share_13 ~ dhs_wfh_wham_share | sector_clustered)
e_mod15 <- feols(data = df_us_firm_year, fml = e_share_15 ~ dhs_wfh_wham_share | sector_clustered)
e_mod17 <- feols(data = df_us_firm_year, fml = e_share_17 ~ dhs_wfh_wham_share | sector_clustered)
e_mod19 <- feols(data = df_us_firm_year, fml = e_share_19 ~ dhs_wfh_wham_share | sector_clustered)
e_mod21 <- feols(data = df_us_firm_year, fml = e_share_21 ~ dhs_wfh_wham_share | sector_clustered)
e_mod23 <- feols(data = df_us_firm_year, fml = e_share_23 ~ dhs_wfh_wham_share | sector_clustered)
e_mod25 <- feols(data = df_us_firm_year, fml = e_share_25 ~ dhs_wfh_wham_share | sector_clustered)
e_mod27 <- feols(data = df_us_firm_year, fml = e_share_27 ~ dhs_wfh_wham_share | sector_clustered)
e_mod29 <- feols(data = df_us_firm_year, fml = e_share_29 ~ dhs_wfh_wham_share | sector_clustered)
e_mod31 <- feols(data = df_us_firm_year, fml = e_share_31 ~ dhs_wfh_wham_share | sector_clustered)
e_mod33 <- feols(data = df_us_firm_year, fml = e_share_33 ~ dhs_wfh_wham_share | sector_clustered)
e_mod35 <- feols(data = df_us_firm_year, fml = e_share_35 ~ dhs_wfh_wham_share | sector_clustered)
e_mod37 <- feols(data = df_us_firm_year, fml = e_share_37 ~ dhs_wfh_wham_share | sector_clustered)
e_mod39 <- feols(data = df_us_firm_year, fml = e_share_39 ~ dhs_wfh_wham_share | sector_clustered)
e_mod41 <- feols(data = df_us_firm_year, fml = e_share_41 ~ dhs_wfh_wham_share | sector_clustered)
e_mod43 <- feols(data = df_us_firm_year, fml = e_share_43 ~ dhs_wfh_wham_share | sector_clustered)
e_mod45 <- feols(data = df_us_firm_year, fml = e_share_45 ~ dhs_wfh_wham_share | sector_clustered)
e_mod47 <- feols(data = df_us_firm_year, fml = e_share_47 ~ dhs_wfh_wham_share | sector_clustered)
e_mod49 <- feols(data = df_us_firm_year, fml = e_share_49 ~ dhs_wfh_wham_share | sector_clustered)
e_mod51 <- feols(data = df_us_firm_year, fml = e_share_51 ~ dhs_wfh_wham_share | sector_clustered)
e_mod53 <- feols(data = df_us_firm_year, fml = e_share_53 ~ dhs_wfh_wham_share | sector_clustered)

etable(e_mod11,e_mod13,e_mod15,e_mod17,e_mod19,e_mod21,e_mod23,e_mod25,e_mod27,e_mod29,e_mod31,e_mod33,e_mod35,e_mod37,e_mod39,e_mod41,e_mod43,e_mod45,e_mod47,e_mod49,e_mod51,e_mod53)

#### SALARY DIFFERENCES ####
remove(list = setdiff(ls(),"df_us"))

df_us$year_fac <- as.factor(df_us$year)

class(df_us$bgt_occ2)

df_us_soc13 <- df_us %>% filter(bgt_occ2 == "13")

df_us_soc13 <- df_us_soc13 %>% mutate(year_month = as.yearmon(job_date) & job_hours == "fulltime")

mod1 <- feols(data = df_us_soc13, lean = TRUE, fixef.rm = "both", fml = log(disjoint_salary) ~ wfh_wham:year_fac | year_month, cluster = ~ bgt_occ)
mod2 <- feols(data = df_us_soc13, lean = TRUE, fixef.rm = "both", fml = log(disjoint_salary) ~ wfh_wham:year_fac | year_month + msa, cluster = ~ bgt_occ)
mod3 <- feols(data = df_us_soc13, lean = TRUE, fixef.rm = "both", fml = log(disjoint_salary) ~ wfh_wham:year_fac | year_month + bgt_occ, cluster = ~ bgt_occ)
mod4 <- feols(data = df_us_soc13, lean = TRUE, fixef.rm = "both", fml = log(disjoint_salary) ~ wfh_wham:year_fac | year_month + bgt_occ^msa, cluster = ~ bgt_occ)
mod5 <- feols(data = df_us_soc13, lean = TRUE, fixef.rm = "both", fml = log(disjoint_salary) ~ wfh_wham:year_fac | year_month + employer, cluster = ~ bgt_occ)
mod6 <- feols(data = df_us_soc13, lean = TRUE, fixef.rm = "both", fml = log(disjoint_salary) ~ wfh_wham:year_fac | year_month + employer^bgt_occ, cluster = ~ bgt_occ)

etable(mod1,mod2,mod3,mod4,mod5,mod6, title = "Salary Premium to Remote Work, Pre- and Post-COVID", tex = TRUE, digits = 2, digits.stats = 3, di)

#### END ####

# 11-0000  Management Occupations
# 13-0000  Business and Financial Operations Occupations
# 15-0000  Computer and Mathematical Occupations
# 17-0000  Architecture and Engineering Occupations
# 19-0000  Life, Physical, and Social Science Occupations
# 21-0000  Community and Social Service Occupations
# 23-0000  Legal Occupations
# 25-0000  Education, Training, and Library Occupations
# 27-0000  Arts, Design, Entertainment, Sports, and Media Occupations
# 29-0000  Healthcare Practitioners and Technical Occupations
# 31-0000  Healthcare Support Occupations
# 33-0000  Protective Service Occupations
# 35-0000  Food Preparation and Serving Related Occupations
# 37-0000  Building and Grounds Cleaning and Maintenance Occupations
# 39-0000  Personal Care and Service Occupations
# 41-0000  Sales and Related Occupations
# 43-0000  Office and Administrative Support Occupations
# 45-0000  Farming, Fishing, and Forestry Occupations
# 47-0000  Construction and Extraction Occupations
# 49-0000  Installation, Maintenance, and Repair Occupations
# 51-0000  Production Occupations
# 53-0000  Transportation and Material Moving Occupations
# 55-0000  Military Specific Occupations



