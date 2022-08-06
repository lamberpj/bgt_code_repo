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

setDTthreads(4)
getDTthreads()
#quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg-us")

#### LOAD DATA AND COLLAPSE BY CITY/STATE x QUARTER ####
paths <- list.files("/mnt/disks/pdisk/bg-us/int_data", pattern = "_wfh.csv", full.names = T)
paths

df_ag_2019 <- fread(paths[1], nThread = 8) %>%
  .[!is.na(wfh_wham)] %>%
  .[, city := str_to_title(city)] %>%
  .[, state := str_to_title(state)] %>%
  .[!is.na(city) & !is.na(soc) & !is.na(state),
    .(.N, wfh_share = mean(wfh_wham, na.rm = T)),
    by = .(year_quarter, city, state)] %>%
  .[N > 250]

df_ag_2020 <- fread(paths[2], nThread = 8) %>%
  .[!is.na(wfh_wham)] %>%
  .[, city := str_to_title(city)] %>%
  .[, state := str_to_title(state)] %>%
  .[!is.na(city) & !is.na(soc) & !is.na(state),
    .(.N, wfh_share = mean(wfh_wham, na.rm = T)),
    by = .(year_quarter, city, state)] %>%
  .[N > 250]

df_ag_2021 <- fread(paths[3], nThread = 8) %>%
  .[!is.na(wfh_wham)] %>%
  .[, city := str_to_title(city)] %>%
  .[, state := str_to_title(state)] %>%
  .[!is.na(city) & !is.na(soc) & !is.na(state),
    .(.N, wfh_share = mean(wfh_wham, na.rm = T)),
    by = .(year_quarter, city, state)] %>%
  .[N > 250]

df_ag_2022 <- fread(paths[4], nThread = 8) %>%
  .[ymd(job_date) <= as.Date("2022-05-18")] %>%
  .[!is.na(wfh_wham)] %>%
  .[, city := str_to_title(city)] %>%
  .[, state := str_to_title(state)] %>%
  .[!is.na(city) & !is.na(soc) & !is.na(state),
    .(.N, wfh_share = mean(wfh_wham, na.rm = T)),
    by = .(year_quarter, city, state)] %>%
  .[N > 250]

df_ag <- bind_rows(df_ag_2019, df_ag_2020, df_ag_2021, df_ag_2022) %>% setDT(.)

table(df_ag$year_quarter)

remove(list = setdiff(ls(), "df_ag"))

df_ag$year_quarter <- as.character(as.yearqtr(df_ag$year_quarter))

# Check coverage
#df_ag_check <- df_ag %>%
#  .[, .(N = sum(N)), by = year_quarter]
#View(df_ag_check)
#rm(df_ag_check)

#View(df_ag)

# Seattle and Portland

df_ag_c <- df_ag %>%
  .[, city_state := paste0(city,"--",state)] %>%
  .[city_state %in% c("Seattle--Washington", "Portland--Oregon", "San Francisco--California")]

df_ag_c <- df_ag_c %>%
  .[, .(N = sum(N), wfh_share = sum(wfh_share * (N/sum(N)))), by = .(city, state, year_quarter)]

df_ag_c$wfh_share <- 100*round(df_ag_c$wfh_share, 3)

df_ag_usa <- df_ag %>%
  .[, .(N = sum(N), wfh_share = sum(wfh_share * (N/sum(N)))), by = .(year_quarter)] %>%
  .[, city := ""] %>%
  .[, state := "USA"]

df_ag_usa$wfh_share <- 100*round(df_ag_usa$wfh_share, 3)

df <- bind_rows(df_ag_c, df_ag_usa)

df <- df %>%
  .[, city_state := paste0(city,", ",state)] %>%
  .[, city_state := ifelse(city_state == ", USA", "USA", city_state)]

#### PLOT FOR NICK ####
cbbPalette <- c("#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00")
1+1

head(df)

as.Date(df$year_quarter[10])

as.Date(as.yearqtr(df$year_quarter[10]))

p = df %>%
  ggplot(., aes(x = as.Date(as.yearqtr(year_quarter)), y = wfh_share, colour = city_state)) +
  #stat_smooth (geom="line", alpha=0.8, size=2, span=0.2) +
  #stat_smooth(span = 0.1, alpha=0.5, se=FALSE, size = 2) +
  geom_point(size = 2.25) +
  geom_line(size = 1.25) +
  ylab("Share (%)") +
  xlab("Date") +
  labs(title = "Share of Remote Work Vacancy Postings (%)", subtitle = "All online job postings which offer remote work in the text.") +
  scale_x_date(breaks = as.Date(c("2019-01-01", "2019-04-01", "2019-07-01", "2019-10-01",
                                  "2020-01-01", "2020-04-01", "2020-07-01", "2020-10-01",
                                  "2021-01-01", "2021-04-01", "2021-07-01", "2021-10-01",
                                  "2022-01-01", "2022-04-01", "2022-07-01", "2022-10-01")), date_labels = "%b %y") +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,5)) +
  coord_cartesian(ylim = c(0, 27)) +
  scale_colour_manual(values=cbbPalette) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(nrow = 2, reverse = T)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"))
p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/nb_all.pdf", width = 8, height = 6)

remove(list = c("p", "p_egg"))

table(df$city_state)

p = df %>%
  filter(city_state %in% c("USA", "San Francisco, California")) %>%
  ggplot(., aes(x = as.Date(as.yearqtr(year_quarter)), y = wfh_share, colour = city_state)) +
  #stat_smooth (geom="line", alpha=0.8, size=2, span=0.2) +
  #stat_smooth(span = 0.1, alpha=0.5, se=FALSE, size = 2) +
  geom_point(size = 2.25) +
  geom_line(size = 1.25) +
  ylab("Share (%)") +
  xlab("Date") +
  labs(title = "Share of Remote Work Vacancy Postings (%)", subtitle = "All online job postings which offer remote work in the text.") +
  scale_x_date(breaks = as.Date(c("2019-01-01", "2019-04-01", "2019-07-01", "2019-10-01",
                                  "2020-01-01", "2020-04-01", "2020-07-01", "2020-10-01",
                                  "2021-01-01", "2021-04-01", "2021-07-01", "2021-10-01",
                                  "2022-01-01", "2022-04-01", "2022-07-01", "2022-10-01")), date_labels = "%b %y") +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,5)) +
  coord_cartesian(ylim = c(0, 27)) +
  scale_colour_manual(values=cbbPalette) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  guides(col = guide_legend(nrow = 2, reverse = T)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"))
p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/nb_us_sf.pdf", width = 8, height = 6)


p = df %>%
  filter(state != "Washington") %>%
  ggplot(., aes(x = as.Date(as.yearqtr(year_quarter)), y = wfh_share, colour = city_state)) +
  #stat_smooth (geom="line", alpha=0.8, size=2, span=0.2) +
  #stat_smooth(span = 0.1, alpha=0.5, se=FALSE, size = 2) +
  geom_point(size = 2.25) +
  geom_line(size = 1.25) +
  ylab("Share (%)") +
  xlab("Date") +
  labs(title = "Share of Remote Work Vacancy Postings (%)", subtitle = "All online job postings which offer remote work in the text.") +
  scale_x_date(breaks = as.Date(c("2019-01-01", "2019-04-01", "2019-07-01", "2019-10-01",
                                  "2020-01-01", "2020-04-01", "2020-07-01", "2020-10-01",
                                  "2021-01-01", "2021-04-01", "2021-07-01", "2021-10-01",
                                  "2022-01-01", "2022-04-01", "2022-07-01", "2022-10-01")), date_labels = "%b %y") +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,5)) +
  coord_cartesian(ylim = c(0, 22)) +
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
ggsave(p_egg, filename = "./plots/nb_us_portland.pdf", width = 8, height = 6)

p = df %>%
  filter(state != "Oregon") %>%
  ggplot(., aes(x = as.Date(as.yearqtr(year_quarter)), y = wfh_share, colour = city_state)) +
  #stat_smooth (geom="line", alpha=0.8, size=2, span=0.2) +
  #stat_smooth(span = 0.1, alpha=0.5, se=FALSE, size = 2) +
  geom_point(size = 2.25) +
  geom_line(size = 1.25) +
  ylab("Share (%)") +
  xlab("Date") +
  labs(title = "Share of Remote Work Vacancy Postings (%)", subtitle = "All online job postings which offer remote work in the text.") +
  scale_x_date(breaks = as.Date(c("2019-01-01", "2019-04-01", "2019-07-01", "2019-10-01",
                                  "2020-01-01", "2020-04-01", "2020-07-01", "2020-10-01",
                                  "2021-01-01", "2021-04-01", "2021-07-01", "2021-10-01",
                                  "2022-01-01", "2022-04-01", "2022-07-01", "2022-10-01")), date_labels = "%b %y") +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,30,5)) +
  coord_cartesian(ylim = c(0, 22)) +
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
ggsave(p_egg, filename = "./plots/nb_us_seattle.pdf", width = 8, height = 6)



remove(list = c("p", "p_egg"))



#### END ####






#### CHECK SPIKE ####

df_2022 <- fread(paths[4], nThread = 8) %>%
  select(city, state, wfh_wham, job_date, employer, soc) %>%
  .[, year_quarter := as.character(as.yearqtr(ymd(job_date)))] %>%
  .[!is.na(wfh_wham)] %>%
  .[, city := str_to_title(city)] %>%
  .[, state := str_to_title(state)] %>%
  .[!is.na(city) & !is.na(state)]

# Check weeks

head(df_2022)

df_2022_weekly_3c <- df_2022 %>%
  filter(job_date != "2022-05-19" & job_date != "2022-05-20" & job_date != "2022-05-21") %>%
  filter((city == "Kansas City" & state == "Missouri") | (city == "Columbus" & state == "Ohio")) %>%
  .[, year_week := paste0(year(ymd(job_date)),"-",sprintf("%02d",week(ymd(job_date))))] %>%
  #.[ year_week %in% c("2022-20", "2022-18")] %>%
  .[, .(.N, wfh_share = mean(wfh_wham)), by = .(year_week, city, state)]

df_2022_daily_3c <- df_2022 %>%
  filter((city == "Kansas City" & state == "Missouri") | (city == "Columbus" & state == "Ohio")) %>%
  .[, year_week := paste0(year(ymd(job_date)),"-",sprintf("%02d",week(ymd(job_date))))] %>%
  .[year(ymd(job_date)) == 2022] %>%
  .[ year_week %in% c("2022-18", "2022-19", "2022-20", "2022-21", "2022-22")] %>%
  .[, .(.N, wfh_share = mean(wfh_wham)), by = .(year_week, job_date, city, state)]


df_2022_check_days <- df_2022 %>%
  filter((city == "Columbus" & state == "Ohio")) %>%
  filter(year_quarter == "2022 Q2") %>%
  filter(job_date != "2022-05-20") %>%
  .[, .(.N, wfh_share = mean(wfh_wham)), by = .(job_date)]

max(df_2022_check_days$N)
sd(df_2022_check_days$N)
median(df_2022_check_days$N)
mean(df_2022_check_days$N)


# Check occs

df_2022_weekly_3c_soc15_vs_not <- df_2022 %>%
  #filter(job_date != "2022-05-20" & job_date != "2022-05-21") %>%
  filter((city == "Kansas City" & state == "Missouri") | (city == "Columbus" & state == "Ohio")) %>%
  .[, year_week := paste0(year(ymd(job_date)),"-",sprintf("%02d",week(ymd(job_date))))] %>%
  .[, soc2 := str_sub(soc, 1, 2)] %>%
  .[, soc2_111523 := ifelse(soc2 %in% c(11,15,23), "YES", "NOT")] %>%
  #.[ year_week %in% c("2022-20", "2022-18")] %>%
  .[, .(.N, wfh_share = mean(wfh_wham)), by = .(year_week, city, state)] %>%
  .[, share := N/sum(N), by = .(year_week, city, state)]

df_2022_daily_3c_soc15_vs_not <- df_2022 %>%
  #filter(job_date != "2022-05-20" & job_date != "2022-05-21") %>%
  filter((city == "Kansas City" & state == "Missouri") | (city == "Columbus" & state == "Ohio")) %>%
  .[, year_week := paste0(year(ymd(job_date)),"-",sprintf("%02d",week(ymd(job_date))))] %>%
  .[, soc2 := str_sub(soc, 1, 2)] %>%
  .[, soc2_111523 := ifelse(soc2 %in% c(11,15,23), "YES", "NOT")] %>%
  .[year(ymd(job_date)) == 2022] %>%
  .[ year_week %in% c("2022-18", "2022-19", "2022-20", "2022-21", "2022-22")] %>%
  .[, .(.N, wfh_share = mean(wfh_wham)), by = .(job_date, year_week, city, state, soc2_111523)] %>%
  .[, share := N/sum(N), by = .(job_date, year_week, city, state)]



####  FIRM LEVEL ####

#### LOAD DATA AND COLLAPSE BY CITY/STATE x QUARTER ####
remove(list = ls())
paths <- list.files("/mnt/disks/pdisk/bg-us/int_data", pattern = "_wfh.csv", full.names = T)
paths

df_ag_2019 <- fread(paths[1], nThread = 8) %>%
  .[!is.na(wfh_wham)] %>%
  .[, city := str_to_title(city)] %>%
  .[, state := str_to_title(state)] %>%
  .[grepl("Schwabe", employer, ignore.case = T)] %>%
  .[!is.na(city) & !is.na(soc) & !is.na(state),
    .(.N, wfh_share = mean(wfh_wham, na.rm = T)),
    by = .(year_quarter, city, state, employer)]

df_ag_2020 <- fread(paths[2], nThread = 8) %>%
  .[!is.na(wfh_wham)] %>%
  .[, city := str_to_title(city)] %>%
  .[, state := str_to_title(state)] %>%
  .[grepl("Schwabe", employer, ignore.case = T)] %>%
  .[!is.na(city) & !is.na(soc) & !is.na(state),
    .(.N, wfh_share = mean(wfh_wham, na.rm = T)),
    by = .(year_quarter, city, state, employer)]

df_ag_2021 <- fread(paths[3], nThread = 8) %>%
  .[!is.na(wfh_wham)] %>%
  .[, city := str_to_title(city)] %>%
  .[, state := str_to_title(state)] %>%
  .[grepl("Schwabe", employer, ignore.case = T)] %>%
  .[!is.na(city) & !is.na(soc) & !is.na(state),
    .(.N, wfh_share = mean(wfh_wham, na.rm = T)),
    by = .(year_quarter, city, state, employer)]

df_ag_2022 <- fread(paths[4], nThread = 8) %>%
  .[!is.na(wfh_wham)] %>%
  .[, city := str_to_title(city)] %>%
  .[, state := str_to_title(state)] %>%
  .[grepl("Schwabe", employer, ignore.case = T)] %>%
  .[, city := str_to_title(city)] %>%
  .[, state := str_to_title(state)] %>%
  .[!is.na(city) & !is.na(soc) & !is.na(state),
    .(.N, wfh_share = mean(wfh_wham, na.rm = T)),
    by = .(year_quarter, city, state, employer)]

df_ag <- bind_rows(df_ag_2019, df_ag_2020, df_ag_2021, df_ag_2022) %>% setDT(.)

df_ag_all_cit <- df_ag %>%
  .[,
    .(.N, wfh_share = sum(wfh_share*(N/sum(N)), na.rm = T)),
    by = .(year_quarter, employer)]

fwrite(df_ag_all_cit, "all_schwabe_usa_quarterly.csv")


table(df_ag$year_quarter)

remove(list = setdiff(ls(), "df_ag"))

df_ag$year_quarter <- as.character(as.yearqtr(df_ag$year_quarter))


