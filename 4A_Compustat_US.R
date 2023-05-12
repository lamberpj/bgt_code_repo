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
library("devtools")
#install_github("markwestcott34/stargazer-booktabs")
library("zoo")
library("stargazer")
library("tsibble")
library("readxl")
library("ggplot2")
library("scales")
library("ggpubr")
library("ggthemr")
library("ggpmisc")
library("RColorBrewer")

ggthemr('flat')
library(egg)
library(extrafont)
library(fixest)
library(stats)

setwd("/mnt/disks/pdisk/bg_combined/")

#### COMPARE US STATES TO GMD ####
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
library(haven)
# font_import()
#loadfonts(device="postscript")
#fonts()

setDTthreads(8)
getDTthreads()
#quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg-us/")

#### LOAD "US BGT" ####
df_us_2019 <- fread("./int_data/us_stru_2019_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "US") %>% setDT()
df_us_2020 <- fread("./int_data/us_stru_2020_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "US") %>% setDT()
df_us_2021 <- fread("./int_data/us_stru_2021_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "US") %>% setDT()
df_us_2022 <- fread("./int_data/us_stru_2022_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "US") %>% setDT()
df_us_2023 <- fread("./int_data/us_stru_2023_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "US") %>% setDT()

df_us <- rbindlist(list(df_us_2019,df_us_2020,df_us_2021,df_us_2022,df_us_2023))
remove(list = c("df_us_2019","df_us_2020","df_us_2021","df_us_2022","df_us_2023"))

setwd("/mnt/disks/pdisk/bg_combined/")

df_us <- df_us %>% .[, city_state := paste0(city,"_",state)]

uniqueN(df_us$city_state) # 31,568

df_us <- df_us[!is.na(job_domain) & job_domain != ""]
df_us <- df_us %>% .[!grepl("careerbuilder", job_domain)]

remove(list = setdiff(ls(), "df_us"))

colnames(df_us)

#### END ####

Mode <- function(x) {
  ux <- unique(x[!is.na(x)])
  ux[which.max(tabulate(match(x, ux)))]
}

#### CHECK EMPLOYMENT CONCENTRATION ####
df_us <- df_us %>%
  .[!is.na(employer) & employer != ""] %>%
  .[, keep := ifelse(any(!is.na(naics3)), 1, 0), by = employer] %>%
  .[keep == 1] %>%
  select(-keep)

colnames(df_us)

employer_naics3 <- df_us %>%
  .[, .(naics3 = Mode(naics3),
        sector_name = Mode(sector_name)), by = employer]

df_soc2 <- df_us %>%
  .[, soc2 := str_sub(soc, 1, 2)] %>%
  .[soc2 != ""] %>%
  .[year == 2022] %>%
  .[, .(.N, wfh_share = 100*mean(wfh_wham, na.rm = T)), by = soc2] %>%
  .[order(soc2)] %>%
  .[wfh_share > 20]

df_soc2

df_us_firm_annual <- df_us %>%
  .[, soc2 := str_sub(soc, 1, 2)] %>%
  .[soc2 %in% df_soc2$soc2] %>%
  .[, year := year(job_date)] %>%
  .[, .(n_postings = .N, n_wfh = sum(wfh_wham)), by = .(year, employer, city_state)] %>%
  .[, share_postings := n_postings/sum(n_postings), by = .(year, employer)] %>%
  .[, .(n_postings = sum(n_postings),
        wfh_share = 100*sum(n_wfh)/sum(n_postings),
        n_city_state = uniqueN(city_state),
        n_city_state_5 = uniqueN(city_state[n_postings>=5]),
        n_city_state_10 = uniqueN(city_state[n_postings>=10]),
        n_city_state_20 = uniqueN(city_state[n_postings>=20]),
        hhi_city_state = sum(share_postings^2)),
    by = .(year, employer)] %>%
  arrange(desc(n_postings)) %>%
  left_join(employer_naics3) %>%
  setDT(.)

head(df_us_firm_annual)

df_us_firm_annual_2022 <- df_us_firm_annual[year %in% c(2022)]
df_us_firm_annual_2019_2022 <- df_us_firm_annual[year %in% c(2019,2022)] %>%
  .[, keep := ifelse(.N == 2, 1, 0), by = employer] %>%
  .[keep == 1] %>%
  select(-keep) %>%
  .[order(employer, year)] %>%
  .[, n_city_state_ld := log(n_city_state) - log(shift(n_city_state)), by = employer] %>%
  .[, n_city_state_ad := n_city_state - shift(n_city_state), by = employer] %>%
  .[year == 2022]

quantile(df_us_firm_annual_2019_2022$n_city_state, probs = c(0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1))
head(df_us_firm_annual_2019_2022)

m1 <- feols(df_us_firm_annual_2019_2022[n_city_state >= 10 & wfh_share > 50], fml = log(n_city_state_ld) ~ log(wfh_share), fixef.rm = "both")

etable(m1)

nrow(df_us_firm_annual_2019_2022[n_city_state >= 10 & wfh_share > 15])

p = df_us_firm_annual_2019_2022[n_city_state >= 10 & wfh_share > 5] %>%
  ggplot(., aes(x = wfh_share, y = n_city_state_ld)) +
  stat_summary_bin(bins=15, fun = "median", color='orange', size=2, geom='point') +
  ylab("Change in Number of Cities \n(2022 vs 2019)") +
  xlab("Remote/Hybrid Posting Share (%)") +
  scale_y_continuous(breaks = seq(0,3,0.25)) +
  scale_x_continuous(limits = c(0, 100)) +
  #coord_cartesian(ylim = c(2, 32), xlim = c(0.5, 26)) +
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
  theme(aspect.ratio=3/5)
p

save(p, file = "./ppt/ggplots/city_expansion.RData")


#### END ####