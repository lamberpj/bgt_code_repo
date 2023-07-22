#### SETUP ####
options(repos = c(CRAN = "https://mran.revolutionanalytics.com/snapshot/2021-04-01"))

library("data.table")
library("tidyverse")
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
library("bit64")

setwd("/mnt/disks/pdisk/bg-us/")

#### IMPORT DATA #####

#### file paths ####
remove(list = ls())
file_names <- list.files("./raw_data/main", "*.txt", full.names = F)
file_names <- as.data.frame(file_names) %>% separate(file_names, sep = "_", c("type", "year-month"), remove = F)
file_names$file_names <- as.data.frame(paths <- list.files("./raw_data/main", "*.txt", full.names = T))$paths
colnames(file_names)
m <- length(file_names$file_names)
#### /end ####

i = 10

#### compile data ####
df <- lapply(1:m, function(i) {
  y1_lines <- read_lines(paste0(file_names$file_names[i]))
  
  n <- length(y1_lines)
  
  y1 <- fread(paste0(file_names$file_names[i]), data.table = F, nThread = 8, integer64 = "double", na.strings = c("",NA,"na"), stringsAsFactors = FALSE, colClasses = 'character')
  
  y1[y1 == "na"] <- NA
  
  
  
          select = c("general_id","year_grab_date","month_grab_date","day_grab_date", "salary", "idcountry",
                     "idesco_level_2", "idesco_level_4", "ideducational_level", "educational_level", "idsector", "idmacro_sector",
                     "idcategory_sector", "companyname"))}) %>% bind_rows %>% clean_names
  print(countries[[i]])
  print(n - nrow(y1))
  
  y2 <- lapply(file_names$file_names[file_names$type == "skills" & file_names$country == countries[[i]]], function(x) {
    fread(paste0(x), data.table = F, nThread = 8)}) %>% bind_rows %>% clean_names %>%
    select(general_id, escoskill_level_3) %>%
    mutate(escoskill_level_3 = str_squish(toupper(escoskill_level_3)))
  
  y2 <- y2 %>%
    filter(!is.na(escoskill_level_3) & escoskill_level_3 != "") %>%
    distinct(., .keep_all = T) %>%
    group_by(escoskill_level_3) %>%
    mutate(n = n() + runif(1, min = 0, max = 0.1)) %>%
    ungroup() %>%
    group_by(general_id) %>%
    filter(n == min(n) | n == max(n)) %>%
    arrange(n) %>%
    mutate(n = 1:n()) %>%
    ungroup() %>%
    group_by(general_id) %>%
    pivot_wider(., names_from = n, names_prefix = "skill_", values_from = escoskill_level_3) %>%
    ungroup() %>%
    mutate(skill_2 = ifelse(is.na(skill_2), skill_1, skill_2))
  
  y1 <- y1 %>%
    mutate(grab_date = dmy(paste0(day_grab_date,"/",month_grab_date,"/",year_grab_date))) %>%
    mutate(year_month = paste0(year(grab_date),".", sprintf("%02d", month(grab_date)))) %>%
    select(-c(day_grab_date, month_grab_date, year_grab_date)) %>%
    distinct(general_id, .keep_all = T)
  
  y1 <- y1 %>%
    left_join(y2)
  
  remove(y2)
  
  return(y1)
})

remove(list = setdiff(ls(), "df"))

#saveRDS(df, "./int_data/educ_regs/all_EU_alt_data_list.rds")
df <- readRDS("./int_data/educ_regs/all_EU_alt_data_list.rds")
df <- df %>% bind_rows


#### /end ####

#### PREPARE FOR REGS ####


df$ideducational_level <- as.numeric(df$ideducational_level)
df$ideducational_level[is.na(df$ideducational_level)] <- 0
df$bach_or_higher <- as.numeric(df$ideducational_level >= 6)

df_ts_sum <- df %>%
  group_by(year_month, ideducational_level) %>%
  summarise(n = n()) %>%
  ungroup() %>%
  group_by(year_month) %>%
  mutate(prop = n/n())

df <- df %>%
  mutate(year = as.numeric(substring(year_month, 1, 4)),
         month = as.numeric(substring(year_month, 6, 7)))

time_trends <- df %>% select(year_month) %>% unique %>% arrange(year_month) %>% mutate(time_trend = 1:n())

df <- df %>%
  left_join(time_trends)

df$idsector[df$idsector == ""] <- NA
df$idmacro_sector[df$idmacro_sector == ""] <- NA
df$idcategory_sector[df$idcategory_sector == ""] <- NA
df$companyname[df$companyname == ""] <- NA

df <- df %>%
  select(-idcategory_sector)

df$idsector[df$idsector == df$idmacro_sector] <- "UP"

colnames(df)

df <- df %>%
  mutate_at(c("idcountry", "idesco_level_2", "idesco_level_4", "idsector", "idmacro_sector", "year_month", "skill_1", "skill_2", "month"), as.factor)

#### /end ####

#### SUMMARY STATISTICS ####

nrow(df) # 110,143,224
df <- df %>%
  mutate(date = as.Date(parse_date_time(year_month, "ym"))) %>%
  mutate(quarter = as.factor(quarter(date)))

sum(is.na(df$idcountry))
df <- df %>% filter(!is.na(idesco_level_4))

df$bach_or_higher_adj <- df$bach_or_higher * 100

lmf.1 <- fixest::feols(bach_or_higher_adj ~ 0 | idcountry + month + idesco_level_4,
                       data = df)
lmf.2 <- fixest::feols(bach_or_higher_adj ~ 0 | idcountry + as.factor(paste0(month,"_",idesco_level_4)),
                       data = df)
lmf.3 <- fixest::feols(bach_or_higher_adj ~ 0 | as.factor(paste0(idcountry,"_",month,"_",idesco_level_4)),
                       data = df)

df$resid1 <- Winsorize(resid(lmf.1), probs = c(0.05, 0.95))
df$resid2 <- Winsorize(resid(lmf.2), probs = c(0.05, 0.95))
df$resid3 <- Winsorize(resid(lmf.3), probs = c(0.05, 0.95))

#### AGGREGATE MOVEMENT ####
df_ts <- df %>%
  filter(as.character(idcountry) %in% c("FR")) %>%
  mutate(date = as.Date(parse_date_time(year_month, "ym"))) %>%
  select(date, resid1, resid2, resid3) %>%
  group_by(date) %>%
  pivot_longer(., cols = c(resid1, resid2, resid3)) %>%
  group_by(date, name) %>%
  summarise(mean_resid = mean(value), sd_resid = sd(value)) %>%
  ungroup()

head(df_ts)

p = ggplot(df_ts, aes(x = date, color = as.factor(name))) +
  geom_line(aes(y = mean_resid)) +
  scale_x_date(date_labels = "%b'%y", breaks = date_breaks("3 months"), minor_breaks = date_breaks("months")) +
  ggtitle("Bachelor Requirement or Higher by Year-Month") +
  ylab("Bachelor Requirement or Higher") +
  scale_y_continuous() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        legend.position = "bottom",
        axis.title.x=element_blank()) +
  guides(col = guide_legend(ncol = 3))
p

df_ts_country <- df %>%
  mutate(date = as.Date(parse_date_time(year_month, "ym"))) %>%
  select(idcountry, date, resid1) %>%
  group_by(idcountry , date) %>%
  summarise(mean_resid = mean(resid1), sd_resid = sd(resid1)) %>%
  ungroup()

p = ggplot(df_ts_country, aes(x = date, color = as.factor(idcountry))) +
  geom_line(aes(y = mean_resid)) +
  scale_x_date(date_labels = "%b'%y", breaks = date_breaks("3 months"), minor_breaks = date_breaks("months")) +
  ggtitle("Bachelor Requirement or Higher by Year-Month") +
  ylab("Bachelor Requirement or Higher") +
  scale_y_continuous() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        legend.position = "bottom",
        axis.title.x=element_blank()) +
  guides(col = guide_legend(ncol = 3))
p


#### AGGREGATE MOVEMENT ####
df_ts_esco1 <- df %>%
  mutate(idesco_level_1 = as.factor(str_sub(as.character(idesco_level_4), 1, 1))) %>%
  group_by(year_month, idesco_level_1) %>%
  summarise(mean = mean(bach_or_higher_resid_win), sd = sd(bach_or_higher_resid_win), median = median(bach_or_higher_resid_win)) %>%
  ungroup() %>%
  mutate(date = as.Date(parse_date_time(year_month, "ym"))) %>%
  select(date, idesco_level_1, mean, sd) %>%
  ungroup()

p = ggplot(df_ts_esco1, aes(x = date, y = mean, color = idesco_level_1)) +
  geom_smooth(span = 0.1) +
  scale_x_date(date_labels = "%b'%y", breaks = date_breaks("3 months"), minor_breaks = date_breaks("months")) +
  ggtitle("At-least Bachelor Requirement by Year-Month") +
  ylab("At-least Bachelors") +
  scale_y_continuous() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        legend.position = "bottom",
        axis.title.x=element_blank()) +
  guides(col = guide_legend(ncol = 3)) +
  labs(subtitle = "Residuals after projecting out Country, ESCO 4-digit, NACE Rev.2 Section, \nMonthly and Quarterly Fixed Effects")
ggexport(p, filename = "./plots/educ_inflation/ts_residuals_bach_or_higher_by_esco1.pdf")

ggplot(cdat, aes(x = xvals)) + 
  geom_line(aes(y = yvals)) +
  geom_line(aes(y = upper), linetype = 2) +
  geom_line(aes(y = lower), linetype = 2) +
  geom_hline(yintercept = 0) +
  ggtitle("Predicted Fuel Economy (mpg) by Weight") +
  xlab("Weight (1000 lbs)") + ylab("Predicted Value")



#### /end ####

#### NO FIRM FIXED EFFECTS ####

# Single FEs with and without country FEs

model1.fixest <- lfe::felm(bach_or_higher ~ early_covd + recovery_covd + late_covd | idcountry | 0 | year_month,
                           data = df,
                           cmethod = "reghdfe")

model2.fixest <- lfe::felm(bach_or_higher ~ early_covd + recovery_covd + late_covd + time_trend | idcountry + month | 0 | year_month,
                           data = df,
                           cmethod = "reghdfe")

model3.fixest <- lfe::felm(bach_or_higher ~ early_covd + recovery_covd + late_covd + time_trend | idcountry + month + idesco_level_2 | 0 | year_month,
                           data = df,
                           cmethod = "reghdfe")

model4.fixest <- lfe::felm(bach_or_higher ~ early_covd + recovery_covd + late_covd + time_trend | idcountry + month + idesco_level_4 | 0 | year_month,
                           data = df,
                           cmethod = "reghdfe")

model5.fixest <- lfe::felm(bach_or_higher ~ early_covd + recovery_covd + late_covd + time_trend | idcountry + month + idmacro_sector | 0 | year_month,
                           data = df,
                           cmethod = "reghdfe")

model6.fixest <- lfe::felm(bach_or_higher ~ early_covd + recovery_covd + late_covd + time_trend | idcountry + month + idesco_level_4 + idmacro_sector | 0 | year_month,
                           data = df,
                           cmethod = "reghdfe")

model7.fixest <- lfe::felm(bach_or_higher ~ early_covd + recovery_covd + late_covd + time_trend | idcountry + month + idesco_level_4 + idmacro_sector + skill_1 | 0 | year_month,
                           data = df,
                           cmethod = "reghdfe")

model8.fixest <- lfe::felm(bach_or_higher ~ early_covd + recovery_covd + late_covd + time_trend | idcountry + month + idesco_level_4 + idmacro_sector + skill_2 | 0 | year_month,
                           data = df,
                           cmethod = "reghdfe")

model9.fixest <- lfe::felm(bach_or_higher ~ early_covd + recovery_covd + late_covd + time_trend | idcountry + month + idesco_level_4 + idmacro_sector + skill_1  + skill_2 | 0 | year_month,
                           data = df,
                           cmethod = "reghdfe")

stargazer(list(model1.fixest, model2.fixest, model3.fixest, model4.fixest, model5.fixest, model6.fixest, model7.fixest, model8.fixest, model9.fixest),
          add.lines=list(c('Time Controls:', '-', 'Yes','Yes', 'Yes', 'Yes', 'Yes','Yes', 'Yes', 'Yes'),
                         c('Country FE:', '-', 'Yes','Yes', 'Yes', 'Yes', 'Yes','Yes', 'Yes', 'Yes'),
                         c('ESCO 2-Digit FE:', '-', '-','Yes', '-', '-', '-','Yes', 'Yes', 'Yes'),
                         c('ESCO 4-Digit FE:', '-', '-','-', 'Yes', '-', 'Yes','Yes', 'Yes', 'Yes'),
                         c('NACE Rev2 Section FE:', '-', '-','-', '-', 'Yes', 'Yes','Yes', 'Yes', 'Yes'),
                         c('Skill - Least Abundant:', '-', '-','-', '-', '-', '-','Yes', '-', 'Yes'),
                         c('Skill - Most Abundant:', '-', '-','-', '-', '-', '-','-', 'Yes', 'Yes')))

#### /end ####

#### VISUALISE THE RESIDUALS #####
# Once we take out the industry, month, occupation, country FE what do the residuals look like? 

#### WITH FIRMS ####
setMKLthreads(1)
df_firms <- df %>%
  mutate(year = as.numeric(substring(year_month, 1, 4)),
         month = as.numeric(substring(year_month, 6, 7))) %>%
  filter(companyname != "" & companyname != "NA" & !is.na(companyname)) %>%
  mutate(firm_selection1 = ifelse(year < 2020 | year == 2020 & month < 3, 1, 0)) %>%
  mutate(firm_selection2 = ifelse(year == 2020 & month %in% c(6,7,8,9,10), 1, 0)) %>%
  group_by(companyname, firm_selection1, firm_selection2) %>%
  mutate(n = n()) %>%
  ungroup() %>%
  mutate(firm_selection1 = as.numeric(firm_selection1 == 1 & n >= 50),
         firm_selection2 = as.numeric(firm_selection2 == 1 & n >= 10)) %>%
  group_by(companyname) %>%
  filter(any(firm_selection1 == 1) & any(firm_selection2 == 1)) %>%
  ungroup() %>%
  mutate(companyname = as.factor(companyname),
         month = as.factor(month))

remove(list = setdiff(ls(),c("df_firms")))

table(df_firms$year_month)

model1.firm_fixest <- lfe::felm(bach_or_higher ~ early_covd + recovery_covd + late_covd | idcountry + month + companyname | 0 | year_month,
                                data = df_firms,
                                cmethod = "reghdfe")

model2.firm_fixest <- lfe::felm(bach_or_higher ~ early_covd + recovery_covd + late_covd + time_trend | idcountry + month + companyname | 0 | year_month,
                                data = df_firms,
                                cmethod = "reghdfe")

model3.firm_fixest <- lfe::felm(bach_or_higher ~ early_covd + recovery_covd + late_covd + time_trend | idcountry + month + idesco_level_2 + companyname | 0 | year_month,
                                data = df_firms,
                                cmethod = "reghdfe")

model4.firm_fixest <- lfe::felm(bach_or_higher ~ early_covd + recovery_covd + late_covd + time_trend | idcountry + month + idesco_level_4 + companyname | 0 | year_month,
                                data = df_firms,
                                cmethod = "reghdfe")

summary(model4.firm_fixest)

stargazer(list(model1.firm_fixest, model2.firm_fixest, model3.firm_fixest, model4.firm_fixest),
          add.lines=list(c('Time Controls:', 'Yes', 'Yes','Yes', 'Yes'),
                         c('Country FE:', 'Yes', 'Yes','Yes', 'Yes'),
                         c('ESCO 2-Digit FE:', '-', '-','Yes', '-'),
                         c('ESCO 4-Digit FE:', '-', '-','-', 'Yes'),
                         c('Firm FE:', 'Yes', 'Yes','Yes', 'Yes')))

#### /end ####

#### VISUALISE RESIDUALS ####


#### /end ####












