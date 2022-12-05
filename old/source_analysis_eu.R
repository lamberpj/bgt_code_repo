#### SETUP ####
options(repos = c(CRAN = "https://mran.revolutionanalytics.com/snapshot/2021-04-01"))

library("data.table")
library("tidyverse")
library("janitor")
library("lubridate")
library("doParallel")
library("lsa")
library("fuzzyjoin")
library("quanteda")
library("refinr")
library("FactoMineR")
library("ggpubr")
library("scales")
library(remotes)

#install.packages('ggthemr')
#install.packages("ggthemr")
#ggthemr('flat')

setwd("/mnt/disks/pdisk/bg-eu/")

#### DAVIS APPROACH #####

#### file paths ####
remove(list = ls())
file_names_structured <- list.files("./bg-eu-bucket/new/structured", "*.csv", full.names = F)
file_names_sources <- list.files("./bg-eu-bucket/new/sources", "*.csv", full.names = F)
file_names_sources

file_names_structured <- as.data.frame(file_names_structured) %>% separate(file_names_structured, sep = " ", c("type", "country", "file"), remove = F)
file_names_sources <- as.data.frame(file_names_sources) %>% separate(file_names_sources, sep = " ", c("type", "country", "file"), remove = F)

file_names_structured$file_names <- as.data.frame(paths <- list.files("./bg-eu-bucket/new/structured", "*.csv", full.names = T))$paths
file_names_sources$file_names <- as.data.frame(paths <- list.files("./bg-eu-bucket/new/sources", "*.csv", full.names = T))$paths


file_names_structured <- file_names_structured %>% filter(!grepl("2020Q3|202101", file_names))

file_names_structured <- file_names_structured %>% filter(grepl("DE|FR|NL|BE|IT|IE|SE|PL", file_names_structured))
file_names_sources <- file_names_sources %>% filter(grepl("DE|FR|NL|BE|IT|IE|SE|PL", file_names_sources))

countries <- file_names_structured$country %>% unique()
m <- length(countries)

#### /end ####

#### compile data ####
setMKLthreads(1)
getMKLthreads()

df_list <- lapply(1:m, function(i) {
  print(i)
  y1 <- lapply(file_names_structured$file_names[file_names_structured$type == "postings" & file_names_structured$country == countries[[i]]], function(x) {
    fread(paste0(x), data.table = F, nThread = 8, select = c("general_id","year_grab_date","month_grab_date","day_grab_date", "idesco_level_4", "idcountry"))}) %>% bind_rows %>% clean_names
  
  y2 <- lapply(file_names_sources$file_names[file_names_sources$type == "mapping_source" & file_names_sources$country == countries[[i]]], function(x) {
    fread(paste0(x), data.table = F, nThread = 8)}) %>% bind_rows %>% clean_names %>%
    select(general_id, source)
  
  y2[y2 == ""] <- NA
  
  y2 <- y2 %>%
    filter(!is.na(source)) %>%
    arrange(general_id)
  
  y1 <- y1 %>%
    mutate(grab_date = dmy(paste0(day_grab_date,"/",month_grab_date,"/",year_grab_date))) %>%
    mutate(year_month = paste0(isoyear(grab_date),".", sprintf("%02d", month(grab_date)))) %>%
    mutate(year_week = paste0(isoyear(grab_date),".", sprintf("%02d", isoweek(grab_date)))) %>%
    select(-c(day_grab_date, month_grab_date, year_grab_date)) %>%
    distinct(general_id, .keep_all = T) %>%
    filter(general_id %in% y2$general_id)
  
  y2 <- y2 %>%
    filter(general_id %in% y1$general_id)
  
  y1 <- y1 %>%
    left_join(y2)
  
  remove("y2")
  
  y1 <- y1 %>%
    group_by(idcountry, year_month, year_week, source) %>%
    summarise(n = n())
  print(i)
  return(y1)
})

#### end ####

#### ANALYSIS ####
#saveRDS(df_list, "./int_data/df_list.rds")
remove(list = ls())
df_list <- readRDS("./int_data/df_list.rds")
setMKLthreads(8)
getMKLthreads()

df <- df_list %>% bind_rows %>%
  filter(grepl("2019|2020|2021", year_week)) %>%
  ungroup() %>%
  select(-year_month)

df <- df %>%
  complete(year_week, idcountry, source) %>%
  ungroup() %>%
  arrange(idcountry, source, year_week) %>%
  group_by(idcountry, source) %>%
  filter(any(!is.na(n))) %>%
  ungroup()

df_na_rm <- df %>%
  arrange(idcountry, source, year_week) %>%
  group_by(idcountry, source) %>%
  arrange(year_week) %>%
  mutate(n_fill = n) %>%
  mutate(n_fill = ifelse(is.na(n_fill) & (!is.na(lag(n_fill, n = 1)) | !is.na(lag(n_fill, n = 2)) | !is.na(lag(n_fill, n = 3)) | !is.na(lag(n_fill, n = 4)) | !is.na(lag(n_fill, n = 5)) |
                           !is.na(lag(n_fill, n = 6)) | !is.na(lag(n_fill, n = 7)) | !is.na(lag(n_fill, n = 8)) | !is.na(lag(n_fill, n = 9)) | !is.na(lag(n_fill, n = 10)) |
                           !is.na(lag(n_fill, n = 11)) | !is.na(lag(n_fill, n = 12)) | !is.na(lag(n_fill, n = 13)) | !is.na(lag(n_fill, n = 14))), 0, n_fill)) %>%
  mutate(n_fill = ifelse(is.na(n_fill) & (!is.na(lag(n_fill, n = 1)) | !is.na(lag(n_fill, n = 2)) | !is.na(lag(n_fill, n = 3)) | !is.na(lag(n_fill, n = 4)) | !is.na(lag(n_fill, n = 5)) |
                                            !is.na(lag(n_fill, n = 6)) | !is.na(lag(n_fill, n = 7)) | !is.na(lag(n_fill, n = 8)) | !is.na(lag(n_fill, n = 9)) | !is.na(lag(n_fill, n = 10)) |
                                            !is.na(lag(n_fill, n = 11)) | !is.na(lag(n_fill, n = 12)) | !is.na(lag(n_fill, n = 13)) | !is.na(lag(n_fill, n = 14))), 0, n_fill)) %>%
  mutate(n_fill = ifelse(is.na(n_fill) & (!is.na(lag(n_fill, n = 1)) | !is.na(lag(n_fill, n = 2)) | !is.na(lag(n_fill, n = 3)) | !is.na(lag(n_fill, n = 4)) | !is.na(lag(n_fill, n = 5)) |
                                            !is.na(lag(n_fill, n = 6)) | !is.na(lag(n_fill, n = 7)) | !is.na(lag(n_fill, n = 8)) | !is.na(lag(n_fill, n = 9)) | !is.na(lag(n_fill, n = 10)) |
                                            !is.na(lag(n_fill, n = 11)) | !is.na(lag(n_fill, n = 12)) | !is.na(lag(n_fill, n = 13)) | !is.na(lag(n_fill, n = 14))), 0, n_fill))  %>%
  mutate(n_fill = ifelse(is.na(n_fill) & (!is.na(lag(n_fill, n = 1)) | !is.na(lag(n_fill, n = 2)) | !is.na(lag(n_fill, n = 3)) | !is.na(lag(n_fill, n = 4)) | !is.na(lag(n_fill, n = 5)) |
                                            !is.na(lag(n_fill, n = 6)) | !is.na(lag(n_fill, n = 7)) | !is.na(lag(n_fill, n = 8)) | !is.na(lag(n_fill, n = 9)) | !is.na(lag(n_fill, n = 10)) |
                                            !is.na(lag(n_fill, n = 11)) | !is.na(lag(n_fill, n = 12)) | !is.na(lag(n_fill, n = 13)) | !is.na(lag(n_fill, n = 14))), 0, n_fill)) %>%
  mutate(n_fill = ifelse(is.na(n_fill) & (!is.na(lag(n_fill, n = 1)) | !is.na(lag(n_fill, n = 2)) | !is.na(lag(n_fill, n = 3)) | !is.na(lag(n_fill, n = 4)) | !is.na(lag(n_fill, n = 5)) |
                                            !is.na(lag(n_fill, n = 6)) | !is.na(lag(n_fill, n = 7)) | !is.na(lag(n_fill, n = 8)) | !is.na(lag(n_fill, n = 9)) | !is.na(lag(n_fill, n = 10)) |
                                            !is.na(lag(n_fill, n = 11)) | !is.na(lag(n_fill, n = 12)) | !is.na(lag(n_fill, n = 13)) | !is.na(lag(n_fill, n = 14))), 0, n_fill))  %>%
  mutate(n_fill = ifelse(is.na(n_fill) & (!is.na(lag(n_fill, n = 1)) | !is.na(lag(n_fill, n = 2)) | !is.na(lag(n_fill, n = 3)) | !is.na(lag(n_fill, n = 4)) | !is.na(lag(n_fill, n = 5)) |
                                            !is.na(lag(n_fill, n = 6)) | !is.na(lag(n_fill, n = 7)) | !is.na(lag(n_fill, n = 8)) | !is.na(lag(n_fill, n = 9)) | !is.na(lag(n_fill, n = 10)) |
                                            !is.na(lag(n_fill, n = 11)) | !is.na(lag(n_fill, n = 12)) | !is.na(lag(n_fill, n = 13)) | !is.na(lag(n_fill, n = 14))), 0, n_fill)) %>%
  mutate(n_fill = ifelse(is.na(n_fill) & (!is.na(lag(n_fill, n = 1)) | !is.na(lag(n_fill, n = 2)) | !is.na(lag(n_fill, n = 3)) | !is.na(lag(n_fill, n = 4)) | !is.na(lag(n_fill, n = 5)) |
                                            !is.na(lag(n_fill, n = 6)) | !is.na(lag(n_fill, n = 7)) | !is.na(lag(n_fill, n = 8)) | !is.na(lag(n_fill, n = 9)) | !is.na(lag(n_fill, n = 10)) |
                                            !is.na(lag(n_fill, n = 11)) | !is.na(lag(n_fill, n = 12)) | !is.na(lag(n_fill, n = 13)) | !is.na(lag(n_fill, n = 14))), 0, n_fill))  %>%
  mutate(n_fill = ifelse(is.na(n_fill) & (!is.na(lag(n_fill, n = 1)) | !is.na(lag(n_fill, n = 2)) | !is.na(lag(n_fill, n = 3)) | !is.na(lag(n_fill, n = 4)) | !is.na(lag(n_fill, n = 5)) |
                                            !is.na(lag(n_fill, n = 6)) | !is.na(lag(n_fill, n = 7)) | !is.na(lag(n_fill, n = 8)) | !is.na(lag(n_fill, n = 9)) | !is.na(lag(n_fill, n = 10)) |
                                            !is.na(lag(n_fill, n = 11)) | !is.na(lag(n_fill, n = 12)) | !is.na(lag(n_fill, n = 13)) | !is.na(lag(n_fill, n = 14))), 0, n_fill)) %>%
  mutate(n_fill = ifelse(is.na(n_fill) & (!is.na(lag(n_fill, n = 1)) | !is.na(lag(n_fill, n = 2)) | !is.na(lag(n_fill, n = 3)) | !is.na(lag(n_fill, n = 4)) | !is.na(lag(n_fill, n = 5)) |
                                            !is.na(lag(n_fill, n = 6)) | !is.na(lag(n_fill, n = 7)) | !is.na(lag(n_fill, n = 8)) | !is.na(lag(n_fill, n = 9)) | !is.na(lag(n_fill, n = 10)) |
                                            !is.na(lag(n_fill, n = 11)) | !is.na(lag(n_fill, n = 12)) | !is.na(lag(n_fill, n = 13)) | !is.na(lag(n_fill, n = 14))), 0, n_fill))  %>%
  mutate(n_fill = ifelse(is.na(n_fill) & (!is.na(lag(n_fill, n = 1)) | !is.na(lag(n_fill, n = 2)) | !is.na(lag(n_fill, n = 3)) | !is.na(lag(n_fill, n = 4)) | !is.na(lag(n_fill, n = 5)) |
                                            !is.na(lag(n_fill, n = 6)) | !is.na(lag(n_fill, n = 7)) | !is.na(lag(n_fill, n = 8)) | !is.na(lag(n_fill, n = 9)) | !is.na(lag(n_fill, n = 10)) |
                                            !is.na(lag(n_fill, n = 11)) | !is.na(lag(n_fill, n = 12)) | !is.na(lag(n_fill, n = 13)) | !is.na(lag(n_fill, n = 14))), 0, n_fill)) %>%
  mutate(n_fill = ifelse(is.na(n_fill) & (!is.na(lag(n_fill, n = 1)) | !is.na(lag(n_fill, n = 2)) | !is.na(lag(n_fill, n = 3)) | !is.na(lag(n_fill, n = 4)) | !is.na(lag(n_fill, n = 5)) |
                                            !is.na(lag(n_fill, n = 6)) | !is.na(lag(n_fill, n = 7)) | !is.na(lag(n_fill, n = 8)) | !is.na(lag(n_fill, n = 9)) | !is.na(lag(n_fill, n = 10)) |
                                            !is.na(lag(n_fill, n = 11)) | !is.na(lag(n_fill, n = 12)) | !is.na(lag(n_fill, n = 13)) | !is.na(lag(n_fill, n = 14))), 0, n_fill))  %>%
  mutate(n_fill = ifelse(is.na(n_fill) & (!is.na(lag(n_fill, n = 1)) | !is.na(lag(n_fill, n = 2)) | !is.na(lag(n_fill, n = 3)) | !is.na(lag(n_fill, n = 4)) | !is.na(lag(n_fill, n = 5)) |
                                            !is.na(lag(n_fill, n = 6)) | !is.na(lag(n_fill, n = 7)) | !is.na(lag(n_fill, n = 8)) | !is.na(lag(n_fill, n = 9)) | !is.na(lag(n_fill, n = 10)) |
                                            !is.na(lag(n_fill, n = 11)) | !is.na(lag(n_fill, n = 12)) | !is.na(lag(n_fill, n = 13)) | !is.na(lag(n_fill, n = 14))), 0, n_fill)) %>%
  mutate(n_fill = ifelse(is.na(n_fill) & (!is.na(lag(n_fill, n = 1)) | !is.na(lag(n_fill, n = 2)) | !is.na(lag(n_fill, n = 3)) | !is.na(lag(n_fill, n = 4)) | !is.na(lag(n_fill, n = 5)) |
                                            !is.na(lag(n_fill, n = 6)) | !is.na(lag(n_fill, n = 7)) | !is.na(lag(n_fill, n = 8)) | !is.na(lag(n_fill, n = 9)) | !is.na(lag(n_fill, n = 10)) |
                                            !is.na(lag(n_fill, n = 11)) | !is.na(lag(n_fill, n = 12)) | !is.na(lag(n_fill, n = 13)) | !is.na(lag(n_fill, n = 14))), 0, n_fill)) 


df_na_rm <- df_na_rm %>%
  arrange(idcountry, source, year_week) %>%
  mutate(n = n_fill) %>%
  select(-n_fill)

df_ss <- df_na_rm %>%
  group_by(source) %>%
  summarise(sum = sum(n, na.rm = T), mean = mean(n, na.rm = T), sd = sd(n, na.rm = T), median = median(n, na.rm = T), min = min(n, na.rm = T), max = max(n, na.rm = T)) %>%
  mutate(mean_by_sd = mean/sd) %>%
  ungroup()

df_hhi <- df_na_rm %>%
  group_by(source, year_week) %>%
  summarise(sum = sum(n, na.rm = T)) %>%
  group_by(year_week) %>%
  summarise(hhi = sum((sum/sum(sum))^2)) %>%
  ungroup() %>%
  mutate(date = ISOweek::ISOweek2date(paste0(str_sub(year_week,1,4),"-W",str_sub(year_week,6,7),"-1")))

### ACROSS SOURCES ###

# PLOT ALL POSTINGS
df_plot1_n <- df_na_rm %>%
  group_by(year_week) %>%
  summarise(n = sum(n, na.rm = T)) %>%
  ungroup() %>%
  mutate(n = n/427641) %>%
  mutate(date = ISOweek::ISOweek2date(paste0(str_sub(year_week,1,4),"-W",str_sub(year_week,6,7),"-1"))) %>%
  mutate(group = "Unadjusted")

#### FISCHER APPROACH ####
library("IndexNumR")

df_na_rm_w <- df_na_rm %>%
  group_by(year_week, source) %>%
  summarise(n = sum(n, na.rm = T))

df_na_rm_w <- df_na_rm_w %>%
  group_by(year_week) %>%
  mutate(n_src_share = n/sum(n, na.rm = T)) %>%
  ungroup() %>%
  mutate(one = 1)

time_int <- as.data.frame(sort(unique(df_na_rm_w$year_week))) %>% mutate(int = 1:n())
colnames(time_int) <- c("year_week", "time_int")
df_na_rm_w <- df_na_rm_w %>% left_join(time_int)

# LET THE "PRICE" BE THE SHARE OF TOTAL POSTINGS
output_list <- c("chained")

df_list_methods_chain <- lapply(1:1, 
                                function(i) {
                                  x <- priceIndex(df_na_rm_w,
                                                  pvar = "n", 
                                                  qvar = "n_src_share", 
                                                  pervar = "time_int", 
                                                  prodID = "source", 
                                                  indexMethod = "fisher", 
                                                  output = output_list[[i]],
                                                  window=5, 
                                                  splice = "window")
                                  as.data.frame(x) %>%
                                    mutate(method = output_list[[i]],
                                           year_week = time_int$year_week)}) %>%
  bind_rows %>%
  mutate(date = ISOweek::ISOweek2date(paste0(str_sub(year_week,1,4),"-W",str_sub(year_week,6,7),"-1"))) %>%
  mutate(n = V1)

df_plot1_final <- df_plot1_n %>% 
  mutate(method = "unadjusted") %>%
  select(n, date, method) %>%
  bind_rows(., df_list_methods_chain %>% select(n, date, method))

p = ggplot(df_plot1_final, aes(y = n, x = date, color = method)) +
  #geom_smooth(span = 0.05) +
  geom_line() +
  scale_x_date(date_labels = "%b'%y", breaks = date_breaks("3 months"), minor_breaks = date_breaks("months")) +
  ggtitle("Weekly Postings: Unadjusted vs Fisher Chain Weighted") +
  ylab("Index (Base Week 1 2019)") +
  scale_y_continuous() +
  labs(subtitle = "Countries = DE,FR,NL,BE,IT,IE,SE,PL") + 
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        legend.position = "bottom",
        axis.title.x=element_blank()) +
  guides(col = guide_legend(ncol = 3))
ggexport(p, filename = "./plots/ts/fcw_vs_unadjusted.pdf")

#### WITHIN SOURCES ####

df_plot1_n <- df_na_rm %>%
  group_by(year_week) %>%
  summarise(n = sum(n, na.rm = T)) %>%
  ungroup() %>%
  mutate(n = (n - mean(n, na.rm = T))/sd(n, na.rm = T)) %>%
  mutate(date = ISOweek::ISOweek2date(paste0(str_sub(year_week,1,4),"-W",str_sub(year_week,6,7),"-1"))) %>%
  mutate(group = "Z-Scored Pooled")

#  WITHIN SOURCE Z-SCORED
df_plot2 <- df_na_rm %>%
  group_by(source) %>%
  mutate(n = (n - mean(n, na.rm = T))/sd(n, na.rm = T)) %>%
  ungroup() %>%
  group_by(year_week) %>%
  summarise(n = mean(n, na.rm = T)) %>%
  mutate(date = ISOweek::ISOweek2date(paste0(str_sub(year_week,1,4),"-W",str_sub(year_week,6,7),"-1"))) %>%
  mutate(n = 5*n) %>%
  ungroup() %>%
  mutate(group = "Z-Score within Sources, uniform weights")

p = ggplot(bind_rows(df_plot1_n, df_plot2), aes(y = n, x = date, color = group)) +
  #geom_smooth(span = 0.05) +
  geom_line() +
  scale_x_date(date_labels = "%b'%y", breaks = date_breaks("3 months"), minor_breaks = date_breaks("months")) +
  ggtitle("Weekly Postings: ZScoring Within vs Across Sources") +
  ylab("Index (Base Week 1 2019)") +
  scale_y_continuous() +
  labs(subtitle = "Countries = DE,FR,NL,BE,IT,IE,SE,PL") + 
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        legend.position = "bottom",
        axis.title.x=element_blank()) +
  guides(col = guide_legend(ncol = 3))
p
ggexport(p, filename = "./plots/ts/zscore_within_vs_across_sources.pdf")

##### MONTHLY TIME ####
remove(list = setdiff(ls(), "df_list"))
df <- df_list %>% bind_rows %>%
  filter(grepl("2019|2020|2021", year_week)) %>%
  ungroup() %>%
  select(-year_week)

df <- df %>%
  group_by(year_month, idcountry, source) %>%
  summarise(n = sum(n, na.rm = T)) %>%
  complete(year_month, idcountry, source) %>%
  ungroup() %>%
  arrange(idcountry, source, year_month) %>%
  group_by(idcountry, source) %>%
  filter(any(!is.na(n))) %>%
  ungroup()

# Price Index
df_w <- df %>%
  group_by(year_month, source) %>%
  summarise(n = sum(n, na.rm = T))

df_w <- df_w %>%
  group_by(year_month) %>%
  mutate(n_src_share = n/sum(n, na.rm = T)) %>%
  ungroup() %>%
  mutate(one = 1)

time_int <- as.data.frame(sort(unique(df_w$year_month))) %>% mutate(int = 1:n())
colnames(time_int) <- c("year_month", "time_int")
df_w <- df_w %>% left_join(time_int)

# LET THE "PRICE" BE THE SHARE OF TOTAL POSTINGS
methods <- c("laspeyres","paasche","fisher")

df_list_methods_chain <- lapply(1:3, 
                                function(i) {
                                  x <- priceIndex(df_w,
                                                  pvar = "n", 
                                                  qvar = "n_src_share", 
                                                  pervar = "time_int", 
                                                  prodID = "source", 
                                                  indexMethod = methods[i], 
                                                  output = "chained")
                                  as.data.frame(x) %>%
                                    mutate(method = methods[i],
                                           year_month = time_int$year_month)}) %>%
  bind_rows %>%
  mutate(date = as.Date(parse_date_time(year_month, "ym")))

p = ggplot(df_list_methods_chain, aes(y = V1, x = date, color = method)) +
  #geom_smooth(span = 0.05) +
  geom_line() +
  scale_x_date(date_labels = "%b'%y", breaks = date_breaks("months"), minor_breaks = date_breaks("months")) +
  ggtitle("Price Index Methods") +
  ylab("Index") +
  scale_y_continuous() +
  labs(subtitle = "Countries = DE,FR,NL,BE,IT,IE,SE,PL") + 
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        legend.position = "bottom",
        axis.title.x=element_blank()) +
  guides(col = guide_legend(ncol = 3))
p
  

