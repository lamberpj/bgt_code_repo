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
library('officer')
library('rvg')
library('here')

library("plotly")
library("ggplot2")
library("dplyr")
library("car")
library("babynames")
library("gapminder")

library("highcharter")
# Set highcharter options
options(highcharter.theme = hc_theme_smpl(tooltip = list(valueDecimals = 2)))

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

##################################################
# RESULT
##################################################

#### 1 REMOTE WORK ACROSS COUNTRIES ####
remove(list = ls())

# Plot US Vacancy Weighted
ts_for_plot <- readRDS(file = "./int_data/country_ts_combined_weights.rds")

unique(ts_for_plot$weight)

ts_for_plot_2 <- ts_for_plot %>%
  ungroup() %>%
  filter(weight == "US 2019 Vacancy") %>%
  mutate(Date = as.Date(as.yearmon(year_month)),
         Percent = round(100*value_3ma, 2),
         Country = country) %>%
  mutate(Weights = "Reweighed by US Vacancies in 2019")

cbbPalette <- c("#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00")

hc <- ts_for_plot_2 %>%
  hchart(object = .,
    "line", 
    hcaes(x = Date, y = Percent, group = Country)
  ) %>%
  #hc_title(text = 'Figure 1: Share of New Job Vacancy Postings \n which Advertise Remote Work') %>% 
  hc_yAxis(title = list(text = '', style = list(fontSize = 0)), labels = list(style = list(fontSize = 14))) %>%
  hc_yAxis(title = list(text = 'Percent', style = list(fontSize = 16)), labels = list(style = list(fontSize = 14))) %>%
  hc_colors(cbbPalette) %>%
  hc_legend(itemStyle = list(fontSize = 16),
            layout = 'horizontal',
            align = 'center',
            verticalAlign = 'top',
            itemMarginTop = 5,
            itemMarginLeft = 10,
            itemMarginBottom = 10,
            floating = TRUE) # Centre legend

hc
#### END ####

#### ACROSS OCCUPATIONS ####
remove(list = ls())
df_us_oc <- readRDS(file = "./int_data/us_onet_wham_2019_vs_2022.rds")

df_us_oc <- df_us_oc %>%
  mutate(ONET = paste0(onet,": ",title)) %>%
  select(ONET, year, wfh_share, N) %>%
  filter(year == 2022) %>%
  select(ONET, wfh_share, N) %>%
  rename(Percent = wfh_share) %>%
  mutate(SOC2 = as.numeric(str_sub(ONET, 1, 2))) %>%
  left_join(
    data.table(SOC2 = c(11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47,49,51,53),
               Group = c("Management","Business & Financial","Computer & Math","Architecture & Engineering","Life, Physical, & Social Science","Community & Social","Legal","Education, Training, & Library","Arts, Design, Entertainment, Sports, & Media","Healthcare Practitioners","Healthcare Support","Protective Service","Food Preparation & Serving","Cleaning & Maintenance","Personal Care & Service","Sales and Related","Office & Admin","Farming, Fishing, & Forestry","Construction & Extraction","Installation, Maintenance, & Repair","Production","Transportation"))
  ) %>%
  select(ONET, Group, Percent, N) %>%
  group_by(Group) %>%
  top_n(n = 10, wt = N) %>%
  ungroup() %>%
  group_by(Group) %>%
  mutate(max_Group = max(Percent)) %>%
  ungroup() %>%
  arrange(desc(Percent))

df_us_oc <- df_us_oc %>%
  filter(!(Group %in% c("Production","Installation, Maintenance, & Repair","Protective Service","Education, Training, & Library","Healthcare Support","Food Preparation & Serving","Personal Care & Service","Construction & Extraction","Transportation","Cleaning & Maintenance","Arts, Design, Entertainment, Sports, & Media", "Farming, Fishing, & Forestry")))

df_us_oc %>% select(Group, max_Group) %>% distinct() %>% arrange(desc(max_Group))

df_us_oc <- df_us_oc %>%
  mutate(Group = factor(Group, levels = c("Computer & Math","Business & Financial","Sales and Related","Life, Physical, & Social Science","Management","Legal","Architecture & Engineering","Healthcare Practitioners","Community & Social","Office & Admin")))


library("RColorBrewer")
(pal <- brewer.pal(11, "Dark2"))

hc <- df_us_oc %>% 
  hchart('column', hcaes(x = ONET, y = Percent, group = Group)) %>%
  hc_yAxis(title = list(text = 'Percent', style = list(fontSize = 16)), labels = list(style = list(fontSize = 14))) %>%
  hc_xAxis(title = list(text = "ONET Occupations (click each bar for more info)", style = list(fontSize = 16)), labels = list(enabled = FALSE)) %>%
  hc_legend(width = 400, itemWidth = 200) %>%
  hc_colors(pal) %>%
  #hc_yAxis(title = "Percent", labels = list(style = list(fontSize = 12))) %>%
  hc_legend(width = 450, itemWidth = 225,
            itemStyle = list(fontSize = 14),
            align = 'center')
hc
#### END ####

#### 3 REMOTE WORK ACROSS CITIES ####
remove(list = ls())

ts_for_plot_cit <- readRDS(file = "./int_data/city_ts_for_plotly.rds") %>%
  mutate(lab = ifelse(year_month == max(year_month), paste0("bold(",gsub(" ","",city),")"), NA_character_)) %>%
  .[year(as.Date(as.yearmon(year_month)))>= 2019] %>%
  filter(region %in% c("Northeast", "West", "South") | city_state == "08 US National")

library("RColorBrewer")
(pal <- brewer.pal(11, "Dark2"))
pal[8] <- "black"

ts_for_plot_cit <- ts_for_plot_cit %>%
  select(city, state_code, year_month, value) %>%
  mutate(Date = as.Date(as.yearmon(year_month)),
         Percent = round(100*value, 2),
         City = paste0(city,", ",state_code)) %>%
  mutate(City = ifelse("US, NA" == City, "US (all)", City))

hc <- ts_for_plot_cit %>%
  hchart(object = .,
         "line", 
         hcaes(x = Date, y = Percent, group = City)
  ) %>%
  hc_yAxis(labels = list(style = list(fontSize = 14))) %>%
  hc_yAxis(title = list(text = 'Percent', style = list(fontSize = 16)), labels = list(style = list(fontSize = 14))) %>%
  hc_colors(pal) %>%
  hc_legend(width = 540, itemWidth = 180,
            itemStyle = list(fontSize = 14),
            align = 'center')

hc
#### END ####







