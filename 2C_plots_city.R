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


#### LOAD DATA ####
# 
# df_nz <- fread("./int_data/df_nz_standardised.csv", nThread = 8, integer64 = "numeric") %>% select(job_id_weight, country, state, city, year, job_date, wfh_wham, job_url)
# df_aus <- fread("./int_data/df_aus_standardised.csv", nThread = 8, integer64 = "numeric") %>% select(job_id_weight, country, state, city, year, job_date, wfh_wham, job_url)
# df_can <- fread("./int_data/df_can_standardised.csv", nThread = 8, integer64 = "numeric") %>% select(job_id_weight, country, state, city, year, job_date, wfh_wham, job_url)
# df_uk <- fread("./int_data/df_uk_standardised.csv", nThread = 8, integer64 = "numeric") %>% select(job_id_weight, country, state, city, year, job_date, wfh_wham, job_url)
# df_us_2019 <- fread("./int_data/df_us_2019_standardised.csv", nThread = 8, integer64 = "numeric") %>% select(job_id_weight, country, state, city, year, job_date, wfh_wham, job_url)
# df_us_2021 <- fread("./int_data/df_us_2021_standardised.csv", nThread = 8, integer64 = "numeric") %>% select(job_id_weight, country, state, city, year, job_date, wfh_wham, job_url)
# df_us_2022 <- fread("./int_data/df_us_2022_standardised.csv", nThread = 8, integer64 = "numeric") %>% select(job_id_weight, country, state, city, year, job_date, wfh_wham, job_url)
# 
# df_nz <- df_nz %>% .[!grepl("mercadojobs", job_url)]
# df_can <- df_can %>% .[!grepl("workopolis", job_url)]
# df_can <- df_can %>% .[!grepl("careerjet", job_url)]
# df_uk <- df_uk %>% .[!grepl("jobisjob", job_url)]
# df_us_2019 <- df_us_2019 %>% .[!grepl("careerbuilder", job_url)]
# df_us_2021 <- df_us_2021 %>% .[!grepl("careerbuilder", job_url)]
# df_us_2022 <- df_us_2022 %>% .[!grepl("careerbuilder", job_url)]
# 
# df_nz <- df_nz %>% .[, city_state := paste0(city,"_",state)]
# df_aus <- df_aus %>% .[, city_state := paste0(city,"_",state)]
# df_can <- df_can %>% .[, city_state := paste0(city,"_",state)]
# df_uk <- df_uk %>% .[, city_state := paste0(city,"_",state)]
# df_us_2019 <- df_us_2019 %>% .[, city_state := paste0(city,"_",state)]
# df_us_2021 <- df_us_2021 %>% .[, city_state := paste0(city,"_",state)]
# df_us_2022 <- df_us_2022 %>% .[, city_state := paste0(city,"_",state)]
# 
# df <- rbindlist(list(df_nz,df_aus,df_can,df_uk,df_us_2019,df_us_2021,df_us_2022))
# ls()
# remove(list = setdiff(ls(), "df"))
# 
# df <- df %>% .[, year_quarter := as.yearqtr(job_date)]
# 
# table(df[year >= 2021]$country, df[year >= 2021]$year_quarter)
# 
# df_cit <- df %>%
#   .[year_quarter %in% as.yearqtr(c("2019 Q1", "2019 Q2", "2019 Q3", "2019 Q4", "2021 Q3", "2021 Q4", "2022 Q1", "2022 Q2"))] %>%
#   .[!is.na(city) & city != ""] %>%
#   .[!is.na(wfh_wham) & wfh_wham != ""] %>%
#   .[, year := year(job_date)] %>%
#   .[, year := ifelse(year == 2021, 2022, year)] %>%
#   setDT(.) %>%
#   select(year, wfh_wham, city, state, country, job_id_weight) %>%
#   setDT(.) %>%
#   .[, .(N = sum(job_id_weight),
#         wfh_share = sum(wfh_wham*job_id_weight)/sum(job_id_weight)),
#     by = .(year, city, state, country)] %>%
#   setDT(.)
# 
# fwrite(df_cit, file = "./int_data/df_cit_2019_2022.csv")
df_cit <- fread(file = "./int_data/df_cit_2019_2022.csv")

df_cit$wfh_share <- 100*df_cit$wfh_share

df_cit$city[df_cit$city == "Washington" & df_cit$state == "District of Columbia"] <- "Washington, D.C."

df_cit <- df_cit %>%
  .[!is.na(country) & !is.na(state) & !is.na(city)] %>%
  .[order(country, state, city, year)] %>%
  .[, prop_growth := paste0(round(wfh_share/shift(wfh_share),1),"X"), by = .(country, state, city)] %>%
  .[, prop_growth := ifelse(prop_growth == "NAX", "", prop_growth)] %>%
  setDT(.)

#### END #####

#### BAR PLOT 2022 vs 2019 ####
remove(list = setdiff(ls(),c("df", "df_cit")))

df_cit_large <- df_cit %>%
  .[N > 5000] %>%
  .[(city == "Auckland" & country == "NZ") |
      (city == "New York" & country == "US") |
      (city == "Los Angeles" & country == "US") |
      (city == "San Francisco" & country == "US") |
      (city == "Atlanta" & country == "US") |
      (city == "Washington, D.C." & country == "US") |
      (city == "Miami" & country == "US") |
      (city == "Dallas" & country == "US") |
      (city == "Chicago" & country == "US") |
      (city == "Phoenix" & country == "US") |
      (city == "Houston" & country == "US") |
      (city == "Philadelphia" & country == "US") |
      (city == "London" & country == "UK") |
      (city == "Manchester" & country == "UK") |
      (city == "Birmingham" & country == "UK") |
      (city == "Glasgow" & country == "UK") |
      (city == "Sydney" & country == "Australia") |
      (city == "Melbourne" & country == "Australia") |
      (city == "Brisbane" & country == "Australia") |
      (city == "Adelaide" & country == "Australia") |
      (city == "Perth" & country == "Australia") |
      (city == "Toronto" & country == "Canada") |
      (city == "Montreal" & country == "Canada") |
      (city == "Ottowa" & country == "Canada")] %>%
  .[order(country, state, city, year)]
  
#cbbPalette <- c("#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00")
#cbbPalette_oc <- c("#000000", "#56B4E9")
#cbbPalette_ind <- c("#000000", "#F0E442")
cbbPalette_cit <- c("#000000", "#A020F0")

df_cit_large$country <- ifelse(df_cit_large$country=="Australia", "AUS", df_cit_large$country)
df_cit_large$country <- ifelse(df_cit_large$country=="Canada", "CAN", df_cit_large$country)
df_cit_large$city_country <- paste0(df_cit_large$city, " (",df_cit_large$country,")")

df_cit_large <- df_cit_large %>% mutate(city_country_fac = fct_reorder(city_country, wfh_share, .fun = max, .desc = FALSE)) %>% ungroup()

a <- ifelse(grepl("(AUS)", as.vector(levels(df_cit_large$city_country_fac))), "red", "black")

df_cit_large <- df_cit_large %>%
  mutate(year = ifelse(year == 2019, "2019", "2021-22"))
df_cit_large
p = ggplot(df_cit_large, aes(x = city_country_fac, y = wfh_share, fill = as.factor(year))) +
  geom_bar(stat = "identity", width=1, position = position_dodge(width=0.8))  +
  geom_text(aes(label = prop_growth, family = "serif"), size = 5, vjust = 0, colour = "black", hjust = -0.5) +
  ylab("Share (%)") +
  scale_y_continuous(breaks = seq(0,100,5), limits = c(0, 32)) +
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
        axis.text.y = element_text(hjust=0.5, colour = "black"),
        legend.position = c(0.80, 0.125)) +
  guides(fill = guide_legend(ncol = 1)) +
  coord_flip() +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 60)) +
  theme(aspect.ratio=4/4)
p
save(p, file = "./ppt/ggplots/city_dist_alt.RData")
remove(p)

#### END ####

#### GLOBAL CITY SCATTER PLOTS ####
library(ggrepel)
set.seed(999)
df_cit
df_cit_scat <- df_cit %>%
  select(-prop_growth) %>%
  .[, size_keep := ifelse(any(N < 10000) | any(wfh_share == 0), 0, 1), by = .(city, state, country)] %>%
  .[size_keep == 1] %>%
  select(-size_keep) %>%
  .[,  title_keep := ifelse((city == "Auckland" & country == "NZ") | 
                              (city == "New York" & country == "US") | 
                              (city == "Los Angeles" & country == "US") | 
                              (city == "San Francisco" & country == "US" & state == "California") | 
                              #(city == "Atlanta" & country == "US" & state == "Georgia") | 
                              (city == "Washington, D.C." & country == "US") | 
                              (city == "Memphis" & country == "US" & state == "Tennessee") | 
                              (city == "New Orleans" & country == "US" & state == "Louisiana") | 
                              (city == "Birmingham" & country == "US" & state == "Alabama") | 
                              (city == "Baton Rouge" & country == "US" & state == "Louisiana") | 
                              (city == "Savannah" & country == "US" & state == "Georgia") | 
                              #(city == "Miami" & country == "US") | 
                              #(city == "Dallas" & country == "US") | 
                              (city == "Chicago" & country == "US") | 
                              #(city == "Phoenix" & country == "US") | 
                              #(city == "Houston" & country == "US") | 
                              #(city == "Philadelphia" & country == "US") | 
                              (city == "London" & country == "UK") | 
                              (city == "Manchester" & country == "UK") |
                              #(city == "Birmingham" & country == "UK") | 
                              #(city == "Glasgow" & country == "UK") | 
                              (city == "Sydney" & country == "Australia") | 
                              (city == "Melbourne" & country == "Australia") | 
                              #(city == "Brisbane" & country == "Australia") | 
                              #(city == "Adelaide" & country == "Australia") | 
                              #(city == "Perth" & country == "Australia") | 
                              (city == "Toronto" & country == "Canada") | 
                              (city == "Montreal" & country == "Canada"),
                            city, NA)] %>%
  group_by(city, state, country) %>%
  pivot_wider(., names_from = year, values_from = c("wfh_share", "N")) %>%
  rename(n_post_2019 = N_2019,
         n_post_2022 = N_2022) %>%
  setDT(.)

df_cit_scat[title_keep != ""]

# Scatter plot (Log-log)
cbbPalette <- c("#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00")

summary(feols(data = df_cit_scat %>%
                filter(n_post_2019 > 250 & n_post_2022 > 250),
              fml = log(wfh_share_2022) ~ 1 + log(wfh_share_2019)))

p_in <- df_cit_scat %>%
  filter(n_post_2019 > 1000 & n_post_2022 > 1000) %>%
  mutate(pos = ifelse(log(wfh_share_2022) < 1.86 + 0.387*log(wfh_share_2019), "below", "above"))

scaleFUN <- function(x) {paste0(sprintf('%.2f',round(x, 2)))}

log(32)

p = p_in %>%
  ggplot(., aes(x = wfh_share_2019, y = wfh_share_2022,
                color = `country`, shape = `country`)) +
  scale_color_manual(values = cbbPalette) +
  scale_y_continuous(trans = log_trans(), 
                     labels=scaleFUN,
                     breaks = c(0.25, 0.5, 1, 2, 4, 8, 16, 32, 64)) +
  scale_x_continuous(trans = log_trans(), 
                     labels=scaleFUN,
                     breaks = c(0.25, 0.5, 1, 2, 4, 8, 16, 32, 64)) +
  geom_point(data = p_in[is.na(title_keep) & wfh_share_2019 > 0.3 & wfh_share_2022 > 2], aes(x = wfh_share_2019, y = wfh_share_2022), size = 1.5, stroke = 1, alpha = 0.3)  +
  geom_smooth(method=lm, se=FALSE, aes(group=1), colour = "blue", size = 0.8) +
  geom_point(data = p_in[!is.na(title_keep) & wfh_share_2019 > 0.3 & wfh_share_2022 > 2], aes(x = wfh_share_2019, y = wfh_share_2022), size = 3, stroke = 2) +
  stat_poly_eq(aes(group=1, label=paste(..eq.label.., sep = "~~~")),geom="label",alpha=1,method = lm,label.y = log(3), label.x = log(14),
               eq.with.lhs = "plain(log)(y)~`=`~",
               eq.x.rhs = "~plain(log)(italic(x))", colour = "blue", size = 4.5) +
  stat_poly_eq(aes(group=1, label=paste(..rr.label.., sep = "~~~")),geom="label",alpha=1,method = lm,label.y = log(2.2), label.x = log(14),
               colour = "blue", size = 4.5) +
  ylab("Share (%) (2021-22) (Logscale)") +
  xlab("Share (%) (2019)  (Logscale)") +
  #scale_y_continuous(breaks = c(0,1,2,3,4,5)) +
  #scale_x_continuous(breaks = c(0,1,2,3,4,5)) +
  coord_cartesian(ylim = c(2, 32), xlim = c(0.5, 26)) +
  guides(size = "none") +
  theme(
    legend.position="bottom"
  ) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm"),
        legend.title = element_blank()) +
  scale_shape_manual(values=c(1, 3, 4, 5, 6)) +
  guides(colour = guide_legend(nrow = 1)) +
  geom_text_repel(data = p_in[pos == "above"], aes(label = title_keep), 
                  fontface = "bold", size = 4, max.overlaps = 1000, force_pull = 1, force = 1, box.padding = 0.5,
                  bg.color = "white", nudge_y = 0.2, nudge_x = -0.2,
                  bg.r = 0.15, seed = 1234, show.legend = FALSE) +
  geom_text_repel(data = p_in[pos == "below"], aes(label = title_keep[pos == "below"]), 
                  fontface = "bold", size = 4, max.overlaps = 1000, force_pull = 1, force = 1, box.padding = 0.5,
                  bg.color = "white", nudge_y = -0.2, nudge_x = -0.2,
                  bg.r = 0.15, seed = 1234, show.legend = FALSE) +
  theme(aspect.ratio=3/5)
p
save(p, file = "./ppt/ggplots/wfh_pre_post_by_city.RData")
#### END ####


#### CITY TIME SERIES PLOTS - US ####
# City-level comparisons

# load city plots
remove(list = ls())

ts_for_plot <- fread(file = "./aux_data/city_level_ts.csv")

us_monthly_series <- fread(file = "./aux_data/us_monthly_ts.csv")

head(ts_for_plot)

ts_for_plot_cit <- ts_for_plot %>%
  #.[city_state %in% us_city_list] %>%
  .[name == "monthly_mean_3ma_l1o"] %>%
  .[country == "US"] %>%
  .[city_state != "Columbus, Georgia"]

sort(unique(ts_for_plot_cit$city_state))

remove(ts_for_plot)

head(ts_for_plot_cit)

divisions <- fread(file = "./aux_data/us census bureau regions and divisions.csv") %>% clean_names

nrow(ts_for_plot_cit)
ts_for_plot_cit <- ts_for_plot_cit %>%
  left_join(divisions)
nrow(ts_for_plot_cit)

table(ts_for_plot_cit$region[!duplicated(ts_for_plot_cit$city_state)])

ts_for_plot_cit[region == "West"][order(-N)]$city_state

us_city_list <- c("Boston, Massachusetts","Cleveland, Ohio",
                  "Columbus, Ohio","Des Moines, Iowa","Houston, Texas","Indianapolis, Indiana",
                  "Jacksonville, Florida","Louisville, Kentucky","Memphis, Tennessee",
                  "New York, New York","Des Moines, Iowa","San Francisco, California",
                  "Savannah, Georgia","Wichita, Kansas","Miami Beach, Florida",
                  "Houston, Texas", "Phoenix, Arizona","Denver, Colorado")

ts_for_plot_cit <- ts_for_plot_cit %>%
  setDT(.) %>%
  .[city_state %in% us_city_list]

table(ts_for_plot_cit$region[!duplicated(ts_for_plot_cit$city_state)])

head(us_monthly_series)
head(ts_for_plot_cit)

us_monthly_series <- us_monthly_series %>% mutate(city_state = " US National", city = "US") %>% select(year_month, city_state, city, name, value)
us_monthly_series
ts_for_plot_cit <- bind_rows(ts_for_plot_cit, us_monthly_series)

ts_for_plot_cit$city <- as.character(ts_for_plot_cit$city)

library("RColorBrewer")
(pal <- c(brewer.pal(8, "Dark2"), brewer.pal(4, "Dark2")))

pal[1] <- "black"

p = ts_for_plot_cit %>%
  mutate(lab = ifelse(year_month == max(year_month), paste0("bold(",gsub(" ","",city),")"), NA_character_)) %>%
  .[year(as.Date(as.yearmon(year_month)))>= 2019] %>%
  filter(region %in% c("Midwest") | city_state == " US National") %>%
  ggplot(., aes(x = as.Date(as.yearmon(year_month)), y = 100*value, colour = city_state, shape = city_state)) +
  #stat_smooth (geom="line", alpha=0.8, size=2, span=0.2) +
  #stat_smooth(span = 0.1, alpha=0.5, se=FALSE, size = 2) +
  geom_point(size = 4) +
  geom_line(size = 1.5, alpha = 0.5) +
  ylab("Share (%)") +
  xlab("Date") +
  #labs(title = "Share of Postings Advertising Remote Work (%)", subtitle = paste0("Unweighted, Outliers Removed. Removed ")) +
  scale_x_date(breaks = as.Date(c("2019-01-01", "2019-04-01", "2019-07-01", "2019-10-01","2020-01-01", "2020-04-01", "2020-07-01", "2020-10-01",
                                  "2021-01-01", "2021-04-01", "2021-07-01", "2021-10-01", "2022-01-01", "2022-04-01", "2022-07-01")),
               date_labels = '%Y-%m',
               limits = as.Date(c("2019-01-01", "2022-07-01"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,2)) +
  #coord_cartesian(ylim = c(0, 20)) +
  scale_shape_manual(values=c(15,16,17,18,19,15,16,17,18,19)) +
  scale_color_manual(values = pal) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=15, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=15, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  guides(col = guide_legend(ncol = 3), shape = guide_legend(ncol = 3)) +
  labs(color  = "Guide name", shape = "Guide name") +
  # geom_label_repel(aes(family = c("serif"), label = lab, x = as.Date(as.yearmon(year_month)) %m+% months(1),
  #                      y = 100*value), force = 0.25, force_pull = 4, hjust = "left", vjust = "top", direction = "y", parse=TRUE,
  #                  label.padding = 0, label.size = NA, size = 5, show.legend = FALSE, segment.color = "transparent") +
  theme(aspect.ratio=3/5)
p
save(p, file = "./ppt/ggplots/ts_cities_mw.RData")

remove(list = c("p", "p_egg"))

p = ts_for_plot_cit %>%
  mutate(lab = ifelse(year_month == max(year_month), paste0("bold(",gsub(" ","",city),")"), NA_character_)) %>%
  .[year(as.Date(as.yearmon(year_month)))>= 2019] %>%
  filter(region %in% c("Northeast", "West") | city_state == " US National") %>%
  ggplot(., aes(x = as.Date(as.yearmon(year_month)), y = 100*value, colour = city_state, shape = city_state)) +
  #stat_smooth (geom="line", alpha=0.8, size=2, span=0.2) +
  #stat_smooth(span = 0.1, alpha=0.5, se=FALSE, size = 2) +
  geom_point(size = 4) +
  geom_line(size = 1.5, alpha = 0.5) +
  ylab("Share (%)") +
  xlab("Date") +
  #labs(title = "Share of Postings Advertising Remote Work (%)", subtitle = paste0("Unweighted, Outliers Removed. Removed ")) +
  scale_x_date(breaks = as.Date(c("2019-01-01", "2019-04-01", "2019-07-01", "2019-10-01","2020-01-01", "2020-04-01", "2020-07-01", "2020-10-01",
                                  "2021-01-01", "2021-04-01", "2021-07-01", "2021-10-01", "2022-01-01", "2022-04-01", "2022-07-01")),
               date_labels = '%Y-%m',
               limits = as.Date(c("2019-01-01", "2022-07-01"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,2)) +
  #coord_cartesian(ylim = c(0, 20)) +
  scale_shape_manual(values=c(15,16,17,18,19,15,16,17,18,19)) +
  scale_color_manual(values = pal) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  guides(col = guide_legend(ncol = 3), shape = guide_legend(ncol = 2)) +
  labs(color  = "Guide name", shape = "Guide name") +
  # geom_label_repel(aes(family = c("serif"), label = lab, x = as.Date(as.yearmon(year_month)) %m+% months(1),
  #                      y = 100*value), force = 0.25, force_pull = 4, hjust = "left", vjust = "top", direction = "y", parse=TRUE,
  #                  label.padding = 0, label.size = NA, size = 5, show.legend = FALSE, segment.color = "transparent") +
  theme(aspect.ratio=3/5)
p
save(p, file = "./ppt/ggplots/ts_cities_ne_w.RData")

remove(list = c("p", "p_egg"))

p = ts_for_plot_cit %>%
  mutate(lab = ifelse(year_month == max(year_month), paste0("bold(",gsub(" ","",city),")"), NA_character_)) %>%
  .[year(as.Date(as.yearmon(year_month)))>= 2019] %>%
  filter(region %in% c("South") | city_state == " US National") %>%
  ggplot(., aes(x = as.Date(as.yearmon(year_month)), y = 100*value, colour = city_state, shape = city_state)) +
  #stat_smooth (geom="line", alpha=0.8, size=2, span=0.2) +
  #stat_smooth(span = 0.1, alpha=0.5, se=FALSE, size = 2) +
  geom_point(size = 4) +
  geom_line(size = 1.5, alpha = 0.5) +
  ylab("Share (%)") +
  xlab("Date") +
  #labs(title = "Share of Postings Advertising Remote Work (%)", subtitle = paste0("Unweighted, Outliers Removed. Removed ")) +
  scale_x_date(breaks = as.Date(c("2019-01-01", "2019-04-01", "2019-07-01", "2019-10-01","2020-01-01", "2020-04-01", "2020-07-01", "2020-10-01",
                                  "2021-01-01", "2021-04-01", "2021-07-01", "2021-10-01", "2022-01-01", "2022-04-01", "2022-07-01")),
               date_labels = '%Y-%m',
               limits = as.Date(c("2019-01-01", "2022-07-01"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,2)) +
  #coord_cartesian(ylim = c(0, 20)) +
  scale_shape_manual(values=c(15,16,17,18,19,15,16,17,18,19)) +
  scale_color_manual(values = pal) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  guides(col = guide_legend(ncol = 3), shape = guide_legend(ncol = 3)) +
  labs(color  = "Guide name", shape = "Guide name") +
  # geom_label_repel(aes(family = c("serif"), label = lab, x = as.Date(as.yearmon(year_month)) %m+% months(1),
  #                      y = 100*value), force = 0.25, force_pull = 4, hjust = "left", vjust = "top", direction = "y", parse=TRUE,
  #                  label.padding = 0, label.size = NA, size = 5, show.legend = FALSE, segment.color = "transparent") +
  theme(aspect.ratio=3/5)
p
save(p, file = "./ppt/ggplots/ts_cities_s.RData")

#### END ####

#### CITY TIME SERIES PLOTS - UK ####
# City-level comparisons

# load city plots
library(ggrepel)
remove(list = ls())

ts_for_plot <- fread(file = "./aux_data/city_level_ts.csv")
uk_monthly_series <- fread(file = "./aux_data/uk_monthly_ts.csv")
head(ts_for_plot)

ts_for_plot_cit <- ts_for_plot %>%
  #.[city_state %in% us_city_list] %>%
  .[name == "monthly_mean_3ma_l1o"] %>%
  .[country == "UK"] %>%
  .[city_state != "Columbus, Georgia"]

sort(unique(ts_for_plot_cit$city_state))

remove(ts_for_plot)

uk_city_list <- c("Manchester, England",
                  "London, England",
                  "Glasgow, Scotland",
                  "Edinburgh, Scotland",
                  "Cardiff, Wales",
                  "Aberdeen, Scotland")

ts_for_plot_cit <- ts_for_plot_cit %>%
  setDT(.) %>%
  .[city_state %in% uk_city_list]

uk_monthly_series <- uk_monthly_series %>% mutate(city_state = " UK National", city = "UK") %>% select(year_month, city_state, city, name, value)
ts_for_plot_cit <- bind_rows(ts_for_plot_cit, uk_monthly_series)

library("RColorBrewer")
(pal <- brewer.pal(10, "Dark2"))

pal[1] <- "black"

p = ts_for_plot_cit %>%
  mutate(lab = ifelse(year_month == max(year_month), paste0("bold(",gsub(" ","",city),")"), NA_character_)) %>%
  .[year(as.Date(as.yearmon(year_month)))>= 2019] %>%
  ggplot(., aes(x = as.Date(as.yearmon(year_month)), y = 100*value, colour = city_state, shape = city_state)) +
  #stat_smooth (geom="line", alpha=0.8, size=2, span=0.2) +
  #stat_smooth(span = 0.1, alpha=0.5, se=FALSE, size = 2) +
  geom_point(size = 4) +
  geom_line(size = 1.5, alpha = 0.5) +
  ylab("Share (%)") +
  xlab("Date") +
  #labs(title = "Share of Postings Advertising Remote Work (%)", subtitle = paste0("Unweighted, Outliers Removed. Removed ")) +
  scale_x_date(breaks = as.Date(c("2019-01-01", "2019-04-01", "2019-07-01", "2019-10-01","2020-01-01", "2020-04-01", "2020-07-01", "2020-10-01",
                                  "2021-01-01", "2021-04-01", "2021-07-01", "2021-10-01", "2022-01-01", "2022-04-01", "2022-07-01")),
               date_labels = '%Y-%m',
               limits = as.Date(c("2019-01-01", "2022-07-01"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,5)) +
  #coord_cartesian(ylim = c(0, 20)) +
  scale_shape_manual(values=c(15,16,17,18,19,15,16,17,18,19)) +
  scale_color_manual(values = pal) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  guides(col = guide_legend(ncol = 3), shape = guide_legend(ncol = 3)) +
  labs(color  = "Guide name", shape = "Guide name") +
  theme(aspect.ratio=3/5)
p
save(p, file = "./ppt/ggplots/ts_cities_uk.RData")

#### END ####


#### ACS vs WHAM - by MSA and Broad Industry ####
remove(list = ls())
library(vroom)
df <- vroom("./aux_data/acs/B08526_mot_workplace_industry_msa/ACSDT1Y2021.B08526-Data.csv", skip = 1) %>% clean_names
colnames(df) <- gsub("estimate_total_", "tot_", colnames(df))
colnames(df) <- gsub("worked_from_home", "wfh", colnames(df))

uniqueN(df$geography)

df[df == "null"] <- NA

df <- df %>% select(geography,geographic_area_name,estimate_total,tot_construction,tot_manufacturing,tot_wholesale_trade,tot_retail_trade,tot_transportation_and_warehousing_and_utilities,tot_information,tot_finance_and_insurance_and_real_estate_and_rental_and_leasing,tot_professional_scientific_and_management_and_administrative_and_waste_management_services,tot_educational_services_and_health_care_and_social_assistance,tot_arts_entertainment_and_recreation_and_accommodation_and_food_services,tot_other_services_except_public_administration,tot_public_administration,tot_armed_forces,tot_wfh,tot_wfh_agriculture_forestry_fishing_and_hunting_and_mining,tot_wfh_construction,tot_wfh_manufacturing,tot_wfh_wholesale_trade,tot_wfh_retail_trade,tot_wfh_transportation_and_warehousing_and_utilities,tot_wfh_information,tot_wfh_finance_and_insurance_and_real_estate_and_rental_and_leasing,tot_wfh_professional_scientific_and_management_and_administrative_and_waste_management_services,tot_wfh_educational_services_and_health_care_and_social_assistance,tot_wfh_arts_entertainment_and_recreation_and_accommodation_and_food_services,tot_wfh_other_services_except_public_administration,tot_wfh_public_administration,tot_wfh_armed_forces)

df <- df %>%
  mutate(across(3:31, as.numeric))

uniqueN(df$geography)

df_acs <- df %>%
  group_by(geography,geographic_area_name,estimate_total) %>%
  pivot_longer(cols = tot_construction:tot_wfh_armed_forces) %>%
  mutate(type = ifelse(grepl("wfh", name), "wfh", "tot")) %>%
  mutate(industry = gsub("tot_wfh_|tot_", "", name)) %>%
  mutate(industry = ifelse(industry == "wfh", "all", industry)) %>%
  select(geography,geographic_area_name,estimate_total, industry, type, value) %>%
  group_by(geography,geographic_area_name,estimate_total, industry) %>%
  pivot_wider(names_from = type, values_from = value) %>%
  mutate(wfh_share = wfh/tot) %>%
  filter(!is.na(wfh_share))

uniqueN(df_acs$geography)

df_acs <- df_acs %>%
  rename(acs_ind = industry)

unique(df_acs$acs_ind)

remove(list = setdiff(ls(), "df_acs"))

df <- fread(file = "../bg-us/int_data/us_stru_2022_wfh.csv", select = c("job_id", "job_date","soc","sector_name","city","state","county","fips","msa","wfh_wham","job_url"))
df <- df %>% .[!is.na(soc) & !is.na(sector_name) & !is.na(wfh_wham) & !is.na(msa)]
df <- df %>% .[soc != "" & sector_name != "" & wfh_wham  != "" & msa != ""]
df <- df %>% .[soc != "" & sector_name != "" & wfh_wham  != "" & msa != ""]
df <- df %>% .[!grepl("careerbuilder", job_url)]

df <- df %>%
  mutate(acs_ind = case_when(
    sector_name == "Accommodation and Food Services" ~ "arts_entertainment_and_recreation_and_accommodation_and_food_services",
    sector_name == "Administrative and Support and Waste Management and Remediation Services" ~ "professional_scientific_and_management_and_administrative_and_waste_management_services",
    sector_name == "Agriculture, Forestry, Fishing and Hunting" ~ "agriculture_forestry_fishing_and_hunting_and_mining",
    sector_name == "Arts, Entertainment, and Recreation" ~ "arts_entertainment_and_recreation_and_accommodation_and_food_services",
    sector_name == "Construction" ~ "construction",
    sector_name == "Educational Services" ~ "educational_services_and_health_care_and_social_assistance",
    sector_name == "Finance and Insurance" ~ "finance_and_insurance_and_real_estate_and_rental_and_leasing",
    sector_name == "Health Care and Social Assistance" ~ "educational_services_and_health_care_and_social_assistance",
    sector_name == "Information" ~ "information",
    sector_name == "Management of Companies and Enterprises" ~ "professional_scientific_and_management_and_administrative_and_waste_management_services",
    sector_name == "Manufacturing" ~ "manufacturing",
    sector_name == "Mining, Quarrying, and Oil and Gas Extraction" ~ "agriculture_forestry_fishing_and_hunting_and_mining",
    sector_name == "Other Services (except Public Administration)" ~ "other_services_except_public_administration",
    sector_name == "Professional, Scientific, and Technical Services" ~ "professional_scientific_and_management_and_administrative_and_waste_management_services",
    sector_name == "Public Administration" ~ "public_administration",
    sector_name == "Real Estate and Rental and Leasing" ~ "finance_and_insurance_and_real_estate_and_rental_and_leasing",
    sector_name == "Retail Trade" ~ "retail_trade",
    sector_name == "Transportation and Warehousing" ~ "transportation_and_warehousing_and_utilities",
    sector_name == "Utilities" ~ "transportation_and_warehousing_and_utilities",
    sector_name == "Wholesale Trade" ~ "wholesale_trade",
    TRUE ~ ""
  )) %>%
  setDT(.)

df_wham_msa_bls_ind <- df %>%
  select(msa, wfh_wham, acs_ind) %>%
  setDT(.) %>%
  .[, .(N = .N,
        wfh_wham_share = mean(wfh_wham, na.rm = T)),
    by = .(msa, acs_ind)] %>%
  setDT(.)

head(df_wham_msa_bls_ind)

df_acs_wham <- df_acs %>%
  ungroup() %>%
  mutate(msa = as.numeric(gsub("310M600US", "", geography))) %>%
  left_join(df_wham_msa_bls_ind)

colnames(df_acs_wham)

feols(data = df_acs_wham, fml = wfh_wham_share ~ wfh_share, cluster = ~ acs_ind + msa) %>% etable()

uniqueN(df_acs_wham$geography) # 161

df_acs_wham_ag <- df_acs_wham %>%
  filter(!is.na(wfh_share) & !is.na(wfh_wham_share)) %>%
  setDT(.) %>%
  .[, .(tot = tot[1],
        wfh_acs_share = sum(wfh_share*(tot/sum(tot, na.rm = T)), na.rm = T),
        wfh_wham_share = sum(wfh_wham_share*(tot/sum(tot, na.rm = T)), na.rm = T)),
    by = .(geography, geographic_area_name)]

head(df_acs_wham_ag)

feols(data = df_acs_wham_ag,
      fml = wfh_wham_share ~ wfh_acs_share, weights = ~tot) %>% etable()

df_acs_wham_ag <- df_acs_wham_ag %>%
  .[, name_keep := ifelse(geographic_area_name %in% c("Boston-Cambridge-Newton, MA-NH Metro Area","Chattanooga, TN-GA Metro Area",
                                                      "Chicago-Naperville-Elgin, IL-IN-WI Metro Area","Corpus Christi, TX Metro Area",
                                                      "Houston-The Woodlands-Sugar Land, TX Metro Area","Jacksonville, FL Metro Area",
                                                      "Memphis, TN-MS-AR Metro Area","New York-Newark-Jersey City, NY-NJ-PA Metro Area",
                                                      "San Jose-Sunnyvale-Santa Clara, CA Metro Area", "Washington-Arlington-Alexandria, DC-VA-MD-WV Metro Area",
                                                      "Little Rock-North Little Rock-Conway, AR Metro Area", "Durham-Chapel Hill, NC Metro Area"),
                          geographic_area_name, NA)]

df_acs_wham_ag$name_keep[df_acs_wham_ag$name_keep == "Chicago-Naperville-Elgin, IL-IN-WI Metro Area"] <- "Chicago\n Naperville\n Elgin"
df_acs_wham_ag$name_keep[df_acs_wham_ag$name_keep == "Corpus Christi, TX Metro Area"] <- "Corpus Christi"
df_acs_wham_ag$name_keep[df_acs_wham_ag$name_keep == "Houston-The Woodlands-Sugar Land, TX Metro Area"] <- "Houston"
df_acs_wham_ag$name_keep[df_acs_wham_ag$name_keep == "Jacksonville, FL Metro Area"] <- "Jacksonville"
df_acs_wham_ag$name_keep[df_acs_wham_ag$name_keep == "Memphis, TN-MS-AR Metro Area"] <- "Memphis"
df_acs_wham_ag$name_keep[df_acs_wham_ag$name_keep == "New York-Newark-Jersey City, NY-NJ-PA Metro Area"] <- "New York\n Newark\n Jersey City"
df_acs_wham_ag$name_keep[df_acs_wham_ag$name_keep == "Washington-Arlington-Alexandria, DC-VA-MD-WV Metro Area"] <- "Washington\n Arlington\nAlexandria"
df_acs_wham_ag$name_keep[df_acs_wham_ag$name_keep == "Chattanooga, TN-GA Metro Area"] <- "Chattanooga"
df_acs_wham_ag$name_keep[df_acs_wham_ag$name_keep == "Boston-Cambridge-Newton, MA-NH Metro Area"] <- "Boston\n Cambridge\n Newton"
df_acs_wham_ag$name_keep[df_acs_wham_ag$name_keep == "San Jose-Sunnyvale-Santa Clara, CA Metro Area"] <- "San Jose\n Sunnyvale\n Santa Clara"
df_acs_wham_ag$name_keep[df_acs_wham_ag$name_keep == "Little Rock-North Little Rock-Conway, AR Metro Area"] <- "Little Rock"
df_acs_wham_ag$name_keep[df_acs_wham_ag$name_keep == "Durham-Chapel Hill, NC Metro Area"] <- "Durham-Chapel Hill"

set.seed(999)
library(ggrepel)


df_acs_wham_ag$diff <- df_acs_wham_ag$wfh_acs_share - df_acs_wham_ag$wfh_wham_share

p = df_acs_wham_ag %>%
  ggplot(., aes(x = 100*wfh_acs_share, y = 100*wfh_wham_share)) +
  geom_smooth(method = "lm", mapping = aes(weight = tot), colour = "blue", se = FALSE) +
  geom_point(data = df_acs_wham_ag[is.na(name_keep)], aes(size = tot), alpha = 0.5, stroke=1, colour = "grey") +
  geom_point(data = df_acs_wham_ag[!is.na(name_keep)], colour = "darkred", aes(size = tot), alpha = 1, stroke=1) +
  stat_poly_eq(aes(weight = tot, group=1, label=paste(..eq.label.., sep = "~~~")),geom="label",
               alpha=1,method = lm, label.y = 18, label.x = 5,
               eq.with.lhs = "y~`=`~",
               eq.x.rhs = "~italic(x)") +
  stat_poly_eq(aes(weight = tot, group=1, label=paste(..rr.label.., sep = "~~~")),geom="label",
               alpha=1,method = lm, label.y = 20, label.x = 5,
               eq.with.lhs = "plain(log)(y)~`=`~",
               eq.x.rhs = "~plain(log)(italic(x))") +
  ylab("") + xlab("") +
  ylab("Share of Vacancies Advertising Remote Work (2022)") +
  xlab('Share of Employed who are "Mostly Working from Home" (2021 ACS)') +
  scale_color_brewer(palette = "Set3") +
  scale_x_continuous(breaks = seq(0, 100, 5)) +
  scale_y_continuous(breaks = seq(0, 100, 5)) +
  coord_cartesian(xlim = c(0, 35), ylim = c(0, 20)) +
  theme(
    #axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 0)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=15, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=15, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  guides(colour = guide_legend(ncol = 1)) +
  guides(size = "none") +
  geom_text_repel(aes(label = name_keep), 
                  fontface = "bold", size = 3.5, max.overlaps = 500, force_pull = 1, force = 10, box.padding = 1,
                  bg.color = "white",
                  bg.r = 0.15, seed = 1234, show.legend = FALSE, colour = "darkred") +
  theme(aspect.ratio=3/5)

p
save(p, file = "./ppt/ggplots/wham_vs_acs.RData")

#### END ####


