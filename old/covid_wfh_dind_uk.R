#### SETUP ####
remove(list = ls())

options(repos = c(CRAN = "https://mran.revolutionanalytics.com/snapshot/2021-09-01"))

options(scipen=999)

#install.packages("corpus")
library("data.table")
library("tidyverse")
library("janitor")
library("lubridate")
#library("dplyr")
#library("stringr")
library("doParallel")
library("textclean")
#library("quanteda")
#library("readtext")
#library("rvest")
library("xml2")
#library("DescTools")
library("zoo")
#library("lsa")
#library("fuzzyjoin")
library("quanteda")
library("refinr")
#library("FactoMineR")
#library("ggpubr")
#library("scales")
#install.packages("ggthemr")
#ggthemr('flat')
library("fixest")
#library("lfe")
library("stargazer")
#library("texreg")
#library("sjPlot")
#library("margins")
#library("DescTools")
#library("fuzzyjoin")
library("readxl")
# Translation packages
#library("rvest")
#library("googleLanguageR")
#library("cld2")
#library("datasets")
#library("vroom")

library(ggplot2)
library(scales)
library("ggpubr")
#library("devtools")
#devtools::install_github('Mikata-Project/ggthemr')
library("ggthemr")
ggthemr('flat')
library(ggrepel)

# quanteda_options(threads = 16)
setwd("/mnt/disks/pdisk/bg_combined/")
setDTthreads(1)

remove(list = ls())
#### end ####

#### LOAD DATA ####
remove(list = ls())
df_all <- fread(file = "./int_data/df_all_standardised.csv", nThread = 4)
df_all$month <- factor(df_all$month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))
df_all$wfh <- as.numeric(df_all$wfh_prob>0.5)
df_all$bgt_occ6 <- str_sub(df_all$bgt_occ, 1, 7)
df_all$bgt_occ2 <- as.numeric(str_sub(df_all$bgt_occ, 1, 2))
df_all$job_ymd <- ymd(df_all$job_date)
df_all$year_quarter <- as.yearqtr(df_all$job_ymd)
df_all$year_month <- as.yearmon(df_all$job_ymd)

df_all <- df_all %>% filter(year >= 2019)

df_all <- df_all %>%
  mutate(state = ifelse(country == "NZ", "New Zealand", state)) %>%
  mutate(state = toupper(state),
         country = toupper(country))

df_all <- df_all %>%
  mutate(state = str_squish(state)) %>%
  mutate(state = case_when(
    state == "ACT" & country == "AUSTRALIA" ~ "AUSTRALIAN CAPITAL TERRITORY",
    state == "NSW" & country == "AUSTRALIA" ~ "NEW SOUTH WALES",
    state == "NT" & country == "AUSTRALIA" ~ "NORTHERN TERRITORY",
    state == "QLD" & country == "AUSTRALIA" ~ "QUEENSLAND",
    state == "SA" & country == "AUSTRALIA" ~ "SOUTH AUSTRALIA",
    state == "TAS" & country == "AUSTRALIA" ~ "TASMANIA",
    state == "VIC" & country == "AUSTRALIA" ~ "VICTORIA",
    state == "WA" & country == "AUSTRALIA" ~ "WESTERN AUSTRALIA",
    state == "DISTRICT OF COLUMBIA"  & country == "US" ~ "WASHINGTON DC",
    state == "AB" & country == "CANADA" ~ "ALBERTA",
    state == "BC" & country == "CANADA" ~ "BRITISH COLUMBIA",
    state == "MB" & country == "CANADA" ~ "MANITOBA",
    state == "NB" & country == "CANADA" ~ "NEW BRUNSWICK",
    state == "NL" & country == "CANADA" ~ "NEWFOUNDLAND AND LABRADOR",
    state == "NS" & country == "CANADA" ~ "NOVA SCOTIA",
    state == "NT" & country == "CANADA" ~ "NORTHWEST TERRITORIES",
    state == "NU" & country == "CANADA" ~ "NUNAVUT",
    state == "ON" & country == "CANADA" ~ "ONTARIO",
    state == "PE" & country == "CANADA" ~ "PRINCE EDWARD ISLAND",
    state == "QC" & country == "CANADA" ~ "QUEBEC",
    state == "SK" & country == "CANADA" ~ "SASKATCHEWAN",
    state == "YT" & country == "CANADA" ~ "YUKON",
    TRUE ~ state
  ))

df_all <- df_all %>%
  mutate(country = str_squish(country)) %>%
  mutate(country = case_when(
    country == "NZ" ~ "NEW ZEALAND",
    country == "UK" ~ "UNITED KINGDOM",
    country == "US" ~ "UNITED STATES",
    country == "CANADA" ~ "CANADA",
    TRUE ~ country
  ))

all_year_month <- df_all %>%
  group_by(year_month) %>%
  summarise(year_month_date = min(job_date)) %>%
  ungroup() %>%
  arrange(year_month)

all_year_month$t <- 1:nrow(all_year_month)

max_t <- max(all_year_month$t, na.rm = T)

#### LOAD COVID STRINGENCY ####
stringency_ts <- fread("./aux_data/geog/OxCGRT_latest.csv") %>% clean_names %>%
  filter(grepl("United Kingdom|United States$|Canada|Australia|New Zealand", country_name)) %>%
  mutate(date = ymd(date)) %>%
  filter(jurisdiction== "STATE_TOTAL" | country_name == "New Zealand") %>%
  mutate(region_name = ifelse(country_name == "New Zealand", "New Zealand", region_name)) %>%
  select(country_name, country_code, region_name, region_code, date, c1_school_closing, c2_workplace_closing, c6_stay_at_home_requirements, c8_international_travel_controls,
         confirmed_cases, confirmed_deaths, stringency_index, containment_health_index, economic_support_index) %>%
  mutate(year_month = as.yearmon(date)) %>%
  group_by(country_name, country_code, region_name, year_month) %>%
  summarise(across(c1_school_closing:economic_support_index, ~ mean(.x, na.rm = T))) %>%
  mutate(region_name = toupper(region_name)) %>%
  mutate(country_name = toupper(country_name))

stringency_ts_all <- stringency_ts %>%
  group_by(country_name, country_code, region_name) %>%
  expand(t = 1:max_t) %>%
  ungroup() %>%
  arrange(country_name, country_code, region_name, t) %>%
  left_join(all_year_month, by = "t") %>%
  select(country_name, country_code, region_name, year_month, t)

stringency_ts_all <- stringency_ts_all %>%
  left_join(stringency_ts)

stringency_ts_all <- stringency_ts_all %>%
  left_join(all_year_month)

stringency_ts_all <- stringency_ts_all %>%
  select(country_name, country_code, region_name, year_month, year_month_date, everything()) %>%
  arrange(country_name, country_code, region_name, year_month)

stringency_ts_all[is.na(stringency_ts_all)] <- 0

stringency_ts_all <- stringency_ts_all %>% select(-country_code)

stringency_ts_all <- stringency_ts_all %>%
  group_by(country_name, region_name) %>%
  mutate(across(c1_school_closing:economic_support_index, ~ cumsum(.x), .names = "cum_{.col}"))

stringency_ts_all <- stringency_ts_all %>%
  group_by(country_name, region_name) %>%
  mutate(across(c1_school_closing:economic_support_index, ~ cummax(.x), .names = "cummax_{.col}"))

stringency_ts_all <- stringency_ts_all %>%
  group_by(country_name, region_name) %>%
  mutate(across(c1_school_closing:economic_support_index, ~ .x + (0.5^1)*lag(.x, n = 1) + (0.5^2)*lag(.x, n = 2) + (0.5^3)*lag(.x, n = 3) + (0.5^4)*lag(.x, n = 4) +
                  + (0.5^5)*lag(.x, n = 5) + (0.5^6)*lag(.x, n = 6) + (0.5^7)*lag(.x, n = 7) + (0.5^8)*lag(.x, n = 8) + (0.5^9)*lag(.x, n = 9) + (0.5^10)*lag(.x, n = 10),
                .names = "pim50_{.col}"))

stringency_ts_all <- stringency_ts_all %>%
  group_by(country_name, region_name) %>%
  mutate(across(c1_school_closing:economic_support_index, ~ .x + (0.75^1)*lag(.x, n = 1) + (0.75^2)*lag(.x, n = 2) + (0.75^3)*lag(.x, n = 3) + (0.75^4)*lag(.x, n = 4) +
                  + (0.75^5)*lag(.x, n = 5) + (0.75^6)*lag(.x, n = 6) + (0.75^7)*lag(.x, n = 7) + (0.75^8)*lag(.x, n = 8) + (0.75^9)*lag(.x, n = 9) + (0.75^10)*lag(.x, n = 10),
                .names = "pim75_{.col}"))

df_stru_covid <- df_all %>%
  left_join(stringency_ts_all, by = c("country" = "country_name", "state" = "region_name", "year_month" = "year_month"))

# Plot cumulative COVID policy stringency
cbbPalette <- c("#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00")
p = stringency_ts_all %>%
  ggplot(., aes(x = as.Date(year_month_date), y = cum_stringency_index, colour = str_to_title(country_name), group = str_to_title(region_name))) +
  geom_line(size = 1.75, alpha = 0.5) +
  ylab("Cumulative Index") +
  xlab("Week") +
  labs(title = "Cumulative COVID Policy Stringency") +
  #scale_x_yearmon(format = '%Y-%-m', breaks = seq(from = as.yearmon("2014-1"), to = as.yearmon("2021-9"), by = 1),  limits = c(as.yearmon("2014-1"), as.yearmon("2021-9"))) +
  scale_x_date(labels = date_format("%b %Y"), breaks = date_breaks("3 months")) +
  #scale_y_continuous(labels = scales::number_format(accuracy = 0.025),  breaks = seq(0,0.3,0.05)) +
  #coord_cartesian(ylim = c(0, 0.18)) +
  scale_colour_manual(values=cbbPalette) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(ncol = 3)) +
  theme(text = element_text(size=18, family="serif"),
        panel.background = element_rect(fill = "white"))
p
p_egg <- set_panel_size(p = p,
                        width = unit(6, "in"),
                        height = unit(4, "in"))
ggsave(p_egg, filename = "./plots/cumulative_covid_stringency.pdf", width = 8, height = 8)

system2(command = "pdfcrop", 
        args    = c("./plots/cumulative_covid_stringency.pdf", 
                    "./plots/cumulative_covid_stringency.pdf")
)

remove(list = c("p", "p_egg"))

# Plot pim50 COVID policy stringency
cbbPalette <- c("#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00")
p = stringency_ts_all %>%
  ggplot(., aes(x = as.Date(year_month_date), y = pim50_stringency_index, colour = str_to_title(country_name), group = str_to_title(region_name))) +
  geom_line(size = 1.75, alpha = 0.5) +
  ylab("PIM 0.5") +
  xlab("Week") +
  labs(title = "PIM 0.5 COVID Policy Stringency") +
  #scale_x_yearmon(format = '%Y-%-m', breaks = seq(from = as.yearmon("2014-1"), to = as.yearmon("2021-9"), by = 1),  limits = c(as.yearmon("2014-1"), as.yearmon("2021-9"))) +
  scale_x_date(labels = date_format("%b %Y"), breaks = date_breaks("3 months")) +
  #scale_y_continuous(labels = scales::number_format(accuracy = 0.025),  breaks = seq(0,0.3,0.05)) +
  #coord_cartesian(ylim = c(0, 0.18)) +
  scale_colour_manual(values=cbbPalette) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(ncol = 3)) +
  theme(text = element_text(size=18, family="serif"),
        panel.background = element_rect(fill = "white"))
p
p_egg <- set_panel_size(p = p,
                        width = unit(6, "in"),
                        height = unit(4, "in"))
ggsave(p_egg, filename = "./plots/cumulative_covid_stringency.pdf", width = 8, height = 8)

system2(command = "pdfcrop", 
        args    = c("./plots/cumulative_covid_stringency.pdf", 
                    "./plots/cumulative_covid_stringency.pdf")
)

remove(list = c("p", "p_egg"))

# Plot Cummax COVID policy stringency
cbbPalette <- c("#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00")
p = stringency_ts_all %>%
  ggplot(., aes(x = as.Date(year_month_date), y = cummax_stringency_index, colour = str_to_title(country_name), group = str_to_title(region_name))) +
  geom_line(size = 1.75, alpha = 0.5) +
  ylab("Cumulative Max Index") +
  xlab("Week") +
  labs(title = "Cumulative Max COVID Policy Stringency") +
  #scale_x_yearmon(format = '%Y-%-m', breaks = seq(from = as.yearmon("2014-1"), to = as.yearmon("2021-9"), by = 1),  limits = c(as.yearmon("2014-1"), as.yearmon("2021-9"))) +
  scale_x_date(labels = date_format("%b %Y"), breaks = date_breaks("3 months")) +
  #scale_y_continuous(labels = scales::number_format(accuracy = 0.025),  breaks = seq(0,0.3,0.05)) +
  #coord_cartesian(ylim = c(0, 0.18)) +
  scale_colour_manual(values=cbbPalette) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(ncol = 3)) +
  theme(text = element_text(size=18, family="serif"),
        panel.background = element_rect(fill = "white"))
p
p_egg <- set_panel_size(p = p,
                        width = unit(6, "in"),
                        height = unit(4, "in"))
ggsave(p_egg, filename = "./plots/cumulativemax_covid_stringency.pdf", width = 8, height = 8)

system2(command = "pdfcrop", 
        args    = c("./plots/cumulativemax_covid_stringency.pdf", 
                    "./plots/cumulativemax_covid_stringency.pdf")
)

remove(list = c("p", "p_egg"))


# Need to make sure there are no NAs after COVID started!!!

sd(df_stru_covid$cum_stringency_index[df_stru_covid$year == 2022], na.rm = T)
mean(df_stru_covid$cum_stringency_index[df_stru_covid$year == 2022], na.rm = T)

df_stru_covid$zscore_cum_string <- (df_stru_covid$cum_stringency_index-5383.191)/782.8836

ihs <- function(x) {
  y <- log(x + sqrt(x ^ 2 + 1))
  return(y)
}

head(df_stru_covid)

# Non-parametric
np1 <- feols(df_stru_covid %>% filter(year >= 2021),
             fixef.rm = "both",
             lean = T,
             nthreads = 8,
             verbose = 10,
             fml = wfh ~ i(year_month, state) | year_month^bgt_occ6,
             cluster = ~ year_month)

np2 <- feols(df_stru_covid %>% filter(year >= 2021),
             fixef.rm = "both",
             lean = T,
             nthreads = 8,
             verbose = 10,
             fml = wfh ~1 | year_month^bgt_occ6^state,
             cluster = ~ year_month)

etable(np2)

strd0 <- feols(df_stru_covid %>% filter(year >= 2019),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ c2_workplace_closing,
               cluster = ~ state)

strd1 <- feols(df_stru_covid %>% filter(year >= 2019),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ c2_workplace_closing,
               cluster = ~ state)

strd2 <- feols(df_stru_covid %>% filter(year >= 2019),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ pim75_c2_workplace_closing,
               cluster = ~ state)

strd3 <- feols(df_stru_covid %>% filter(year >= 2019),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ pim50_c2_workplace_closing,
               cluster = ~ state)

strd4 <- feols(df_stru_covid %>% filter(year >= 2019),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ cum_c2_workplace_closing,
               cluster = ~ state)

etable(strd0, strd1, strd2, strd3, strd4)

summary(df_stru_covid$c2_workplace_closing)

# Cumulative Stringency
strd1 <- feols(df_stru_covid %>% filter(year >= 2021),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ cum_stringency_index | state + bgt_occ6,
               cluster = ~ state)

strd2 <- feols(df_stru_covid %>% filter(year >= 2021),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ cum_stringency_index | year_month + state + bgt_occ6,
               cluster = ~ state)

strd3 <- feols(df_stru_covid %>% filter(year >= 2021),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ cum_stringency_index | year_month^bgt_occ6 + state,
               cluster = ~ state)

strd4 <- feols(df_stru_covid %>% filter(year >= 2021),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ cum_stringency_index | state + employer,
               cluster = ~ state)

strd5 <- feols(df_stru_covid %>% filter(year >= 2021),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ cum_stringency_index | year_month + state + employer,
               cluster = ~ state)

etable(strd1, strd2, strd3, strd4, strd5, title = "DiD: WFH and COVID Policy Stringency Index (CPSI)", tex = F)

# Cumulative Stingency
strd1 <- feols(df_stru_covid %>% filter(year >= 2019),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ cummax_stringency_index | state + bgt_occ6,
               cluster = ~ state)

strd2 <- feols(df_stru_covid %>% filter(year >= 2019),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ cummax_stringency_index | year_month + state + bgt_occ6,
               cluster = ~ state)

strd3 <- feols(df_stru_covid %>% filter(year >= 2019),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ cummax_stringency_index | year_month^bgt_occ6 + state,
               cluster = ~ state)

strd4 <- feols(df_stru_covid %>% filter(year >= 2019),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ cummax_stringency_index | state + employer,
               cluster = ~ state)

strd5 <- feols(df_stru_covid %>% filter(year >= 2019),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ cummax_stringency_index | year_month + state + employer,
               cluster = ~ state)

etable(strd1, strd2, strd3, strd4, strd5, title = "DiD: WFH and COVID Policy Stringency Index (CPSI)", tex = F)

# Stingency
strd1 <- feols(df_stru_covid %>% filter(year >= 2021),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ stringency_index | state + bgt_occ6,
               cluster = ~ state)

strd2 <- feols(df_stru_covid %>% filter(year >= 2021),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ stringency_index | year_month + state + bgt_occ6,
               cluster = ~ state)

strd3 <- feols(df_stru_covid %>% filter(year >= 2021),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ stringency_index | year_month^bgt_occ6 + state,
               cluster = ~ state)

strd4 <- feols(df_stru_covid %>% filter(year >= 2021),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ stringency_index | state + employer,
               cluster = ~ state)

strd5 <- feols(df_stru_covid %>% filter(year >= 2021),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ stringency_index | year_month + state + employer,
               cluster = ~ state)

strd6 <- feols(df_stru_covid %>% filter(year == 2021),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ stringency_index | year_month + state + employer + bgt_occ6,
               cluster = ~ state)

etable(strd1, strd2, strd3, strd4, strd5, strd6, title = "DiD: WFH and COVID Policy Stringency Index (CPSI)", tex = F)

# SQRT Stingency
strd1 <- feols(df_stru_covid %>% filter(year >= 2019),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ sqrt(cum_stringency_index) | state + bgt_occ6,
               cluster = ~ state)

strd2 <- feols(df_stru_covid %>% filter(year >= 2019),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ sqrt(cum_stringency_index) | year_month + state + bgt_occ6,
               cluster = ~ state)

strd3 <- feols(df_stru_covid %>% filter(year >= 2019),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ sqrt(cum_stringency_index) | year_month^bgt_occ6 + state,
               cluster = ~ state)

strd4 <- feols(df_stru_covid %>% filter(year >= 2019),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ sqrt(cum_stringency_index) | state + employer,
               cluster = ~ state)

strd5 <- feols(df_stru_covid %>% filter(year >= 2019),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ sqrt(cum_stringency_index) | year_month + state + employer,
               cluster = ~ state)

etable(strd1, strd2, strd3, strd4, strd5, title = "DiD: WFH and COVID Policy Stringency Index (CPSI)", tex = F)

# Poly Stingency
strd1 <- feols(df_stru_covid %>% filter(year >= 2019) %>% filter(!is.na(cum_stringency_index)),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ poly(cum_stringency_index, 4) | state + bgt_occ6,
               cluster = ~ state)

strd2 <- feols(df_stru_covid %>% filter(year >= 2019) %>% filter(!is.na(cum_stringency_index)),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ poly(cum_stringency_index, 4) | year_month + state + bgt_occ6,
               cluster = ~ state)

strd3 <- feols(df_stru_covid %>% filter(year >= 2019) %>% filter(!is.na(cum_stringency_index)),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ poly(cum_stringency_index, 4) | year_month^bgt_occ6 + state,
               cluster = ~ state)

strd4 <- feols(df_stru_covid %>% filter(year >= 2019) %>% filter(!is.na(cum_stringency_index)),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ poly(cum_stringency_index, 4) | state + employer,
               cluster = ~ state)

strd5 <- feols(df_stru_covid %>% filter(year >= 2019) %>% filter(!is.na(cum_stringency_index)),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ poly(cum_stringency_index, 4) | year_month + state + employer,
               cluster = ~ state)

etable(strd1, strd2, strd3, strd4, strd5, title = "DiD: WFH and COVID Policy Stringency Index (CPSI)", tex = F)

# Best Quarter
strd1 <- feols(df_stru_covid %>% filter(year_quarter == "2021 Q3"),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ cum_stringency_index | state + bgt_occ6,
               cluster = ~ state)

strd2 <- feols(df_stru_covid %>% filter(year_quarter == "2021 Q3"),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ cum_stringency_index | year_month + state + bgt_occ6,
               cluster = ~ state)

strd3 <- feols(df_stru_covid %>% filter(year_quarter == "2021 Q3"),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ cum_stringency_index | year_month^bgt_occ6 + state,
               cluster = ~ state)

strd4 <- feols(df_stru_covid %>% filter(year_quarter == "2021 Q3"),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ cum_stringency_index | state + employer,
               cluster = ~ state)

strd5 <- feols(df_stru_covid %>% filter(year_quarter == "2021 Q3"),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ cum_stringency_index | year_month + state + employer,
               cluster = ~ state)

etable(strd1, strd2, strd3, strd4, strd5, title = "DiD: WFH and COVID Policy Stringency Index (CPSI)", tex = F)

# Cross-section
strdx1 <- feols(df_stru_covid %>% filter(year_quarter == "2020 Q1"),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ cum_stringency_index | state + bgt_occ6,
               cluster = ~ state)

strdx2 <- feols(df_stru_covid %>% filter(year_quarter == "2020 Q2"),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ cum_stringency_index | state + bgt_occ6,
               cluster = ~ state)

strdx3 <- feols(df_stru_covid %>% filter(year_quarter == "2020 Q3"),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ cum_stringency_index | state + bgt_occ6,
               cluster = ~ state)

strdx4 <- feols(df_stru_covid %>% filter(year_quarter == "2020 Q4"),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ cum_stringency_index | state + bgt_occ6,
               cluster = ~ state)

strdx5 <- feols(df_stru_covid %>% filter(year_quarter == "2021 Q1"),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ cum_stringency_index | state + bgt_occ6,
               cluster = ~ state)

strdx6 <- feols(df_stru_covid %>% filter(year_quarter == "2021 Q2"),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ cum_stringency_index | state + bgt_occ6,
               cluster = ~ state)

strdx7 <- feols(df_stru_covid %>% filter(year_quarter == "2021 Q3"),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ cum_stringency_index | state + bgt_occ6,
               cluster = ~ state)

strdx8 <- feols(df_stru_covid %>% filter(year_quarter == "2021 Q4"),
               fixef.rm = "both",
               nthreads = 8,
               verbose = 10,
               fml = wfh ~ cum_stringency_index | state + bgt_occ6,
               cluster = ~ state)

etable(strdx1, strdx2, strdx3, strdx4, strdx5, strdx6, strdx7, strdx8, title = "DiD: WFH and COVID Policy Stringency Index (CPSI)", tex = F)

### ZSCORE ###
strdxz1 <- feols(df_stru_covid %>% filter(year_quarter == "2020 Q1") %>% mutate(cum_stringency_index = (cum_stringency_index-mean(cum_stringency_index, na.rm = T))/sd(cum_stringency_index, na.rm = T)),
                fixef.rm = "both",
                nthreads = 8,
                verbose = 10,
                fml = wfh ~ cum_stringency_index | state + bgt_occ6,
                cluster = ~ state)

strdxz2 <- feols(df_stru_covid %>% filter(year_quarter == "2020 Q2") %>% mutate(cum_stringency_index = (cum_stringency_index-mean(cum_stringency_index, na.rm = T))/sd(cum_stringency_index, na.rm = T)),
                fixef.rm = "both",
                nthreads = 8,
                verbose = 10,
                fml = wfh ~ cum_stringency_index | state + bgt_occ6,
                cluster = ~ state)

strdxz3 <- feols(df_stru_covid %>% filter(year_quarter == "2020 Q3") %>% mutate(cum_stringency_index = (cum_stringency_index-mean(cum_stringency_index, na.rm = T))/sd(cum_stringency_index, na.rm = T)),
                fixef.rm = "both",
                nthreads = 8,
                verbose = 10,
                fml = wfh ~ cum_stringency_index | state + bgt_occ6,
                cluster = ~ state)

strdxz4 <- feols(df_stru_covid %>% filter(year_quarter == "2020 Q4") %>% mutate(cum_stringency_index = (cum_stringency_index-mean(cum_stringency_index, na.rm = T))/sd(cum_stringency_index, na.rm = T)),
                fixef.rm = "both",
                nthreads = 8,
                verbose = 10,
                fml = wfh ~ cum_stringency_index | state + bgt_occ6,
                cluster = ~ state)

strdxz5 <- feols(df_stru_covid %>% filter(year_quarter == "2021 Q1") %>% mutate(cum_stringency_index = (cum_stringency_index-mean(cum_stringency_index, na.rm = T))/sd(cum_stringency_index, na.rm = T)),
                fixef.rm = "both",
                nthreads = 8,
                verbose = 10,
                fml = wfh ~ cum_stringency_index | state + bgt_occ6,
                cluster = ~ state)

strdxz6 <- feols(df_stru_covid %>% filter(year_quarter == "2021 Q2") %>% mutate(cum_stringency_index = (cum_stringency_index-mean(cum_stringency_index, na.rm = T))/sd(cum_stringency_index, na.rm = T)),
                fixef.rm = "both",
                nthreads = 8,
                verbose = 10,
                fml = wfh ~ cum_stringency_index | state + bgt_occ6,
                cluster = ~ state)

strdxz7 <- feols(df_stru_covid %>% filter(year_quarter == "2021 Q3") %>% mutate(cum_stringency_index = (cum_stringency_index-mean(cum_stringency_index, na.rm = T))/sd(cum_stringency_index, na.rm = T)),
                fixef.rm = "both",
                nthreads = 8,
                verbose = 10,
                fml = wfh ~ cum_stringency_index | state + bgt_occ6,
                cluster = ~ state)

strdxz8 <- feols(df_stru_covid %>% filter(year_quarter == "2021 Q4") %>% mutate(cum_stringency_index = (cum_stringency_index-mean(cum_stringency_index, na.rm = T))/sd(cum_stringency_index, na.rm = T)),
                fixef.rm = "both",
                nthreads = 8,
                verbose = 10,
                fml = wfh ~ cum_stringency_index | state + bgt_occ6,
                cluster = ~ state)

strdxz9 <- feols(df_stru_covid %>% filter(year_quarter == "2022 Q1") %>% mutate(cum_stringency_index = (cum_stringency_index-mean(cum_stringency_index, na.rm = T))/sd(cum_stringency_index, na.rm = T)),
                 fixef.rm = "both",
                 nthreads = 8,
                 verbose = 10,
                 fml = wfh ~ cum_stringency_index | state + bgt_occ6,
                 cluster = ~ state)

etable(strdxz1, strdxz2, strdxz3, strdxz4, strdxz5, strdxz6, strdxz7, strdxz8, strdxz9, title = "DiD: WFH and COVID Policy Stringency Index (CPSI)", tex = F)

