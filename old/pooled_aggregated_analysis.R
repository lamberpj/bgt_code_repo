#### SETUP ####
options(repos = c(CRAN = "https://mran.revolutionanalytics.com/snapshot/2021-04-01"))

library("data.table")
library("tidyverse")
library(magrittr)
library("janitor")
library("lubridate")
library("doParallel")
#library("lsa")
#library("fuzzyjoin")
#library("quanteda")
library("refinr")
#library("FactoMineR")
library("ggpubr")
library("scales")
#install.packages("ggthemr")
#ggthemr('flat')
library("fixest")
library("lfe")
library("stargazer")
library("texreg")
library("sjPlot")
library("margins")
library("DescTools")
#library("bit64")

setwd("/mnt/disks/pdisk/bg-us/")

#### SUMMARY STATISTICS ####
remove(list = ls())

load("./pooled_data_mm/college_share_US_EU.RData")

df <- share_US_EU %>%
  mutate(year = as.numeric(str_sub(year_month, 1, 4)),
         month = as.numeric(str_sub(year_month, 6, 7))) %>%
  mutate(idesco_level_3 = str_sub(idesco_level_4, 1, 3),
         idesco_level_2 = str_sub(idesco_level_4, 1, 2)) %>%
  select(-c(idmacro_sector))
  
remove(share_US_EU)

# ESCO 4 KDENSITY
colnames(df)

df_region <- df %>%
  group_by(year_month, idesco_level_4, region, year, month) %>%
  summarise(count_total = sum(count_total, na.rm = T), count_college = sum(count_college, na.rm = T)) %>%
  mutate(college_share = count_college/count_total) %>%
  ungroup()

df_region <- df_region %>%
  group_by(idesco_level_4) %>%
  mutate(n = n()) %>%
  ungroup()

df_region <- df_region %>% mutate(region_year = paste0(region,"_",year))

df_region_wide <- df_region %>%
  select(-c(count_total, count_college, region_year)) %>%
  group_by(year_month, idesco_level_4, year, month) %>%
  pivot_wider(., names_from = region, values_from = college_share, names_prefix = "college_share_") %>%
  filter(!is.na(college_share_EU) * !is.na(college_share_EU))

df_region_wide %$% cor(college_share_EU, college_share_US, use = "pairwise.complete") # 0.5718987
df_region_wide %>% filter(year == 2019) %$% cor(college_share_EU, college_share_US, use = "pairwise.complete") # 0.5434565
df_region_wide %>% filter(year == 2020) %$% cor(college_share_EU, college_share_US, use = "pairwise.complete") # 0.6116556

df_region_wide <- df_region_wide %>% arrange(college_share_US)

summary(df_region_wide$college_share_EU) # q3 0.18972
summary(df_region_wide$college_share_US) # q3 0.35765

df_region_wide <- df_region_wide %>%
  mutate(diff = college_share_EU - college_share_US)

model <- df_region_wide %>% feols(data = ., fml = diff ~ eval((college_share_EU + college_share_US)/2) | idesco_level_4)

summary(model)

df_region_wide %>% filter(year == 2019) %$% cor(diff, college_share_EU, use = "pairwise.complete") # -0.1643854
df_region_wide %>% filter(year == 2019) %$% cor(diff, college_share_US, use = "pairwise.complete") # -0.9173541

df_region_wide %>% filter(year == 2019) %$% summary(diff)

df_region <- df_region %>% mutate(region_year = paste0(region,"_",year))

df_region_long <- df_region %>%
  mutate(covid = case_when(
    year == 2019 | year == 2020 & month < 3 ~ "precovid",
    year == 2020 & month > 2 & month < 9 ~ "earlycovid",
    TRUE ~ "latecovid"))

df_region_long <- df_region_long %>% mutate(region_covid = paste0(region,"_",covid))

colnames(df_region_long)

p = ggplot(df_region_long %>% filter(year == 2019), aes(x = college_share, color = region, fill = region)) +
  geom_density(size = 1, alpha = 0.05, adjust = 0.8, aes(weight = count_total), ) +
  ggtitle("KDensity of College Share 2019 (Weighted)") +
  scale_x_continuous(breaks = seq(0,1,0.1)) +
  ylab("Density") +
  xlab("Share of Postings Requiring College or Higher") +
  scale_y_continuous() +
  theme(legend.position = "bottom") +
  labs(caption = "Note: Each Data Point is a (Month x ESCO 4 Digit Occupation x Region) cell.\nData is weighted by posting-count in cell.")
p
ggexport(p, filename = "./plots/kdensity_by_region_2019_weighted.pdf")
remove(p)

p = ggplot(df_region_long %>% filter(year == 2019), aes(x = college_share, color = region, fill = region)) +
  geom_density(size = 1, alpha = 0.05, adjust = 0.8) +
  ggtitle("KDensity of College Share 2019 (Unweighted)") +
  scale_x_continuous(breaks = seq(0,1,0.1)) +
  ylab("Density") +
  xlab("Share of Postings Requiring College or Higher") +
  scale_y_continuous() +
  theme(legend.position = "bottom") +
  labs(caption = "Note: Each Data Point is a (Month x ESCO 4 Digit Occupation x Region) cell.\nData is unweighted by posting-count in cell.")
p
ggexport(p, filename = "./plots/kdensity_by_region_2019_unweighted.pdf")
remove(p)

p = ggplot(df_region_long, aes(x = college_share, color = covid, fill = covid, linetype = region)) +
  geom_density(size = 1, alpha = 0.05, adjust = 1.75, aes(weight = count_total), ) +
  ggtitle("KDensity of College Share (Weighted)") +
  scale_x_continuous(breaks = seq(0,1,0.1)) +
  ylab("Density") +
  xlab("Share of Postings Requiring College or Higher") +
  scale_y_continuous() +
  theme(legend.position = "bottom") +
  labs(caption = "Note: Each Data Point is a (Month x ESCO 4 Digit Occupation x Region) cell.\nData is weighted by posting-count in cell.
       \nPRe-COVID = Jan 2019 to Feb 2020.  Early-COVID = Mar 2020 to Aug 2020.  Late-COVID = Sep 2020 - Jan 2021.")
p
ggexport(p, filename = "./plots/kdensity_by_region_covid_weighted.pdf")
remove(p)

p = ggplot(df_region_long, aes(x = college_share, color = covid, fill = covid, linetype = region)) +
  geom_density(size = 1, alpha = 0.05, adjust = 1.75) +
  ggtitle("KDensity of College Share (Unweighted)") +
  scale_x_continuous(breaks = seq(0,1,0.1)) +
  ylab("Density") +
  xlab("Share of Postings Requiring College or Higher") +
  scale_y_continuous() +
  theme(legend.position = "bottom") +
  labs(caption = "Note: Each Data Point is a (Month x ESCO 4 Digit Occupation x Region) cell.\nData is unweighted by posting-count in cell.
       \nPRe-COVID = Jan 2019 to Feb 2020.  Early-COVID = Mar 2020 to Aug 2020.  Late-COVID = Sep 2020 - Jan 2021.")
p
ggexport(p, filename = "./plots/kdensity_by_region_covid_unweighted.pdf")
remove(p)

#### CHANGES ####



