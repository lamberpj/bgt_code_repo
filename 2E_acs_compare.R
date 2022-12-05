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
library(ggrepel)
set.seed(999)
# font_import()
#loadfonts(device="postscript")
#fonts()

setDTthreads(4)
getDTthreads()
#quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg_combined/")

#### LOAD "ALL" ####
df_nz <- fread("./int_data/df_nz_standardised.csv", nThread = 8, integer64 = "numeric")
df_aus <- fread("./int_data/df_aus_standardised.csv", nThread = 8, integer64 = "numeric")
df_can <- fread("./int_data/df_can_standardised.csv", nThread = 8, integer64 = "numeric")
df_uk <- fread("./int_data/df_uk_standardised.csv", nThread = 8, integer64 = "numeric")
df_us_2014 <- fread("./int_data/df_us_2014_standardised.csv", nThread = 8, integer64 = "numeric")
df_us_2015 <- fread("./int_data/df_us_2015_standardised.csv", nThread = 8, integer64 = "numeric")
df_us_2016 <- fread("./int_data/df_us_2016_standardised.csv", nThread = 8, integer64 = "numeric")
df_us_2017 <- fread("./int_data/df_us_2017_standardised.csv", nThread = 8, integer64 = "numeric")
df_us_2018 <- fread("./int_data/df_us_2018_standardised.csv", nThread = 8, integer64 = "numeric")
df_us_2019 <- fread("./int_data/df_us_2019_standardised.csv", nThread = 8, integer64 = "numeric")
df_us_2020 <- fread("./int_data/df_us_2020_standardised.csv", nThread = 8, integer64 = "numeric")
df_us_2021 <- fread("./int_data/df_us_2021_standardised.csv", nThread = 8, integer64 = "numeric")
df_us_2022 <- fread("./int_data/df_us_2022_standardised.csv", nThread = 8, integer64 = "numeric")

df_us <- rbindlist(list(df_us_2014, df_us_2015, df_us_2016, df_us_2017, df_us_2018,df_us_2019,df_us_2020,df_us_2021,df_us_2022))
remove(list = c("df_us_2014", "df_us_2015", "df_us_2016", "df_us_2017", "df_us_2018","df_us_2019","df_us_2020","df_us_2021","df_us_2022"))

df_nz <- df_nz %>% .[!grepl("mercadojobs", job_url)]
df_can <- df_can %>% .[!grepl("workopolis", job_url)]
df_can <- df_can %>% .[!grepl("careerjet", job_url)]
df_uk <- df_uk %>% .[!grepl("jobisjob", job_url)]
df_us <- df_us %>% .[!grepl("careerbuilder", job_url)]

df_all_list <- list(df_nz,df_aus,df_can,df_uk,df_us)
remove(list = setdiff(ls(), "df_all_list"))

df_all_monthly_state_list <- lapply(1:length(df_all_list), function(i) {
  df_all_list[[i]] %>%
    .[year %in% c(2019,2020,2021,2022)] %>%
    .[, .(.N, year_mean_wham = mean(wfh_wham, na.rm = T)), by = .(state, year, year_month)]
})

df_all_monthly_state <- rbindlist(df_all_monthly_state_list)

df_all_monthly_state <- df_all_monthly_state[state != ""]

fwrite(df_all_monthly_state, file = "./int_data/state_by_month_shares.csv")

#### END ####

# US State Level - ACS
rm(list = ls())
df_us_monthly_state <- fread(file = "./int_data/state_by_month_shares.csv") %>%
  .[year %in% c(2019,2022)] %>%
  .[, .(wfh_share = sum(N*year_mean_wham, na.rm = T)/sum(N), N = sum(N)), by = .(state, year)] %>%
  .[order(state, year)] %>%
  .[, diff_wfh_share := wfh_share - lag(wfh_share), by = state] %>%
  .[, dhs_diff_wfh_share := (wfh_share - lag(wfh_share))/(0.5*(wfh_share + lag(wfh_share))), by = state] %>%
  .[, pc_diff_wfh_share := log(wfh_share) - log(lag(wfh_share)), by = state] %>%
  .[year == 2022]

wfh_2021_by_state_acs <- fread(file = "./aux_data/wfh_2021_by_state_acs.csv")
wfh_2021_by_state_acs
wfh_2021_by_state_acs$state[!(wfh_2021_by_state_acs$state %in% df_us_monthly_state$state)]
df_us_monthly_state$state[!(df_us_monthly_state$state %in% wfh_2021_by_state_acs$state)]

wfh_2021_by_state_acs <- wfh_2021_by_state_acs %>%
  inner_join(df_us_monthly_state)

wfh_2021_by_state_acs <- wfh_2021_by_state_acs %>%
  setDT(.) %>%
  .[, acs_wfh_share := worked_from_home/pop]

feols(data = wfh_2021_by_state_acs,
      fml = acs_wfh_share ~ wfh_share) %>% etable()

feols(data = wfh_2021_by_state_acs,
      fml = wfh_share ~ acs_wfh_share) %>% etable()

p = wfh_2021_by_state_acs %>%
  #.[!(state %in% c("Vermont", "District of Columbia"))] %>%
  ggplot(., aes(x = wfh_share, y = acs_wfh_share)) +
  geom_point(aes(size = pop))  +
  geom_smooth(method=lm, colour = "grey", se=FALSE, fullrange=TRUE) +
  ylab("Share (2022)") +
  xlab('Share "Mostly Working from Home" (ACS)') +
  labs(title = "Remote Work Vacancies (WHAM) vs Travel Method (ACS)", subtitle = "No reweighting of vacancies.  ACS measure of workers mostly working from home.") +
  #coord_cartesian(xlim = c(10, 30), ylim = c(0.1, 0.2)) +
  theme(
    #axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 0)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  guides(colour = guide_legend(ncol = 3)) +
  geom_text_repel(aes(label = state),
                  fontface = "bold", size = 3, max.overlaps = 100,
                  bg.color = "white", seed = 4321)
p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/acs_vs_wham_level_diff.pdf", width = 8, height = 6)

p = wfh_2021_by_state_acs %>%
  .[!(state %in% c("Vermont", "District of Columbia"))] %>%
  ggplot(., aes(x = wfh_share, y = acs_wfh_share)) +
  geom_point(aes(size = pop))  +
  geom_smooth(method=lm, colour = "grey", se=FALSE, fullrange=TRUE) +
  ylab("Share (2022)") +
  xlab('Share "Mostly Working from Home" (ACS)') +
  labs(title = "Remote Work Vacancies (WHAM) vs Travel Method (ACS)", subtitle = "No reweighting of vacancies.  ACS measure of workers mostly working from home.") +
  #coord_cartesian(xlim = c(10, 30), ylim = c(0.1, 0.2)) +
  theme(
    #axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 0)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  guides(colour = guide_legend(ncol = 3)) +
  geom_text_repel(aes(label = state),
                  fontface = "bold", size = 3, max.overlaps = 100,
                  bg.color = "white", seed = 4321)
p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/acs_vs_wham_level_diff_no_outlier.pdf", width = 8, height = 6)

# US State Level with Reweighting - ACS
remove(list = ls())
df <- fread(file = "../bg-us/int_data/us_stru_2022_wfh.csv", select = c("job_id", "job_date","soc","sector_name","city","state","county","fips","msa","wfh_wham","job_url"))
df <- df %>% .[!is.na(soc) & !is.na(sector_name) & !is.na(wfh_wham) & !is.na(state)]
df <- df %>% .[soc != "" & sector_name != "" & wfh_wham  != "" & state != ""]
df <- df %>% .[!grepl("careerbuilder", job_url)]

df <- df %>%
  .[, soc2 := as.numeric(str_sub(soc, 1, 2))] %>%
  mutate(acs_occ = case_when(
    soc2 %in% c(11:29) ~ "management_business_science_and_arts_occupations",
    soc2 %in% c(31:39) ~ "service_occupations",
    soc2 %in% c(41:43) ~ "sales_and_office_occupations",
    soc2 %in% c(45:49) ~ "natural_resources_construction_and_maintenance_occupations",
    soc2 %in% c(51:53) ~ "production_transportation_and_material_moving_occupations",
    soc2 %in% c(55) ~ "military",
    TRUE ~ ""
  )) %>%
  setDT(.)

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

# Import ACS weights
df_w <- fread(file = "./aux_data/ind_occ_dist_2021_by_state_acs.csv") %>% clean_names %>% setDT(.)

df_w <- df_w %>%
  group_by(state_code, state, pop, emp_occ) %>%
  pivot_longer(cols = occ_management_business_science_and_arts_occupations:ind_public_administration)

df_w <- df_w %>%
  setDT(.) %>%
  .[, category := ifelse(grepl("ind_", name), "industry", "occupation")] %>%
  .[, name := str_sub(name, 5, -1)]

# Make weighted average over industries
df_ind <- df %>%
  unique(., by = "job_id") %>%
  .[, .(.N, wfh_share = mean(wfh_wham)), by = .(state, acs_ind)]

df_w_ind <- df_w %>% filter(category == "industry") %>% rename(acs_ind = name) %>% mutate(share = value/emp_occ) %>% select(-state_code)

df_w_emp_state <- df_w_ind %>% select(state, emp_occ) %>% distinct(.)

nrow(df_ind)
df_ind <- df_ind %>% inner_join(df_w_ind) %>% setDT(.)
nrow(df_ind)

df_ag <- df_ind %>%
  .[, .(emp_occ = emp_occ[1], wfh_share = sum(share*wfh_share)), by = state]

wfh_2021_by_state_acs <- fread(file = "./aux_data/wfh_2021_by_state_acs.csv")

wfh_2021_by_state_acs <- wfh_2021_by_state_acs %>%
  inner_join(df_ag)

wfh_2021_by_state_acs <- wfh_2021_by_state_acs %>%
  setDT(.) %>%
  .[, acs_wfh_share := worked_from_home/emp_occ]

feols(data = wfh_2021_by_state_acs %>% filter(!(state %in% c("District of Columbia"))),
      fml = acs_wfh_share ~ wfh_share) %>% etable()

feols(data = wfh_2021_by_state_acs,
      fml = acs_wfh_share ~ wfh_share) %>% etable()

p = wfh_2021_by_state_acs %>%
  #.[!(state %in% c("District of Columbia"))] %>%
  ggplot(., aes(x = wfh_share, y = acs_wfh_share)) +
  geom_point(aes(size = emp_occ))  +
  geom_smooth(method=lm, colour = "grey", se=FALSE, fullrange=TRUE) +
  ylab("Share (2022)") +
  xlab('Share "Mostly Working from Home" (ACS)') +
  labs(title = "Remote Work Vacancies (WHAM) vs Travel Method (ACS)", subtitle = "Reweighting vacancies to match Industry-employment share from ACS.  ACS measure of workers mostly working from home.") +
  #coord_cartesian(xlim = c(10, 30), ylim = c(0.1, 0.2)) +
  theme(
    #axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 0)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  guides(colour = guide_legend(ncol = 3)) +
  geom_text_repel(aes(label = state),
                  fontface = "bold", size = 3, max.overlaps = 100,
                  bg.color = "white", seed = 4321)

p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/wham_vs_acs_level.pdf", width = 8, height = 6)
remove(list = c("p", "p_egg"))

p = wfh_2021_by_state_acs %>%
  .[!(state %in% c("District of Columbia"))] %>%
  ggplot(., aes(x = wfh_share, y = acs_wfh_share)) +
  geom_point(aes(size = emp_occ))  +
  geom_smooth(method=lm, colour = "grey", se=FALSE, fullrange=TRUE) +
  ylab("Share (2022)") +
  xlab('Share "Mostly Working from Home" (ACS)') +
  labs(title = "Remote Work Vacancies (WHAM) vs Travel Method (ACS)", subtitle = "Reweighting vacancies to match Industry-employment share from ACS.  ACS measure of workers mostly working from home.") +
  #coord_cartesian(xlim = c(10, 30), ylim = c(0.1, 0.2)) +
  theme(
    #axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 0)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  guides(colour = guide_legend(ncol = 3)) +
  geom_text_repel(aes(label = state),
                  fontface = "bold", size = 3, max.overlaps = 100,
                  bg.color = "white", seed = 4321)
p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/wham_vs_acs_level_no_dc.pdf", width = 8, height = 6)
remove(list = c("p", "p_egg"))

# US MSA - ACS
remove(list = ls())
colnames(fread(file = "../bg-us/int_data/us_stru_2022_wfh.csv", nrows = 100))
df <- fread(file = "../bg-us/int_data/us_stru_2022_wfh.csv", select = c("job_id", "job_date","soc","sector_name","city","state","county","fips","msa","wfh_wham","job_url"))
df <- df %>% .[!is.na(soc) & !is.na(sector_name) & !is.na(wfh_wham) & !is.na(state)]
df <- df %>% .[soc != "" & sector_name != "" & wfh_wham  != "" & state != ""]
df <- df %>% .[!grepl("careerbuilder", job_url)]

df_msa <- df %>%
  unique(., by = "job_id") %>%
  .[, .(.N, wfh_share = mean(wfh_wham)), by = .(state, msa)] %>%
  .[, msa := as.numeric(msa)]

df_acs <- fread(file = "./aux_data/wfh_by_msa_acs.csv") %>%
  .[, msa := as.numeric(gsub("310M600US", "", msa_code))] %>%
  select(msa, emp, wfh) %>%
  .[, wfh_share_acs := as.numeric(wfh)/as.numeric(emp)]

df_acs <- df_acs %>%
  left_join(df_msa)

feols(data = df_acs,
      fml = wfh_share_acs ~ wfh_share) %>% etable()

p = df_acs %>%
  ggplot(., aes(x = log(100*wfh_share), y = log(100*wfh_share_acs))) +
  geom_point(aes(size = emp), alpha = 0.7, stroke=1) +
  #geom_point(aes(size = emp), alpha = 0, stroke=1) +
  geom_smooth(method=lm, colour = "grey", se=FALSE, fullrange=TRUE) +
  ylab("Share (2022) (Logscale)") +
  xlab('Share "Mostly Working from Home" (ACS) (Logscale)') +
  labs(title = "Remote Work Vacancies (WHAM) vs Travel Method (ACS)", subtitle = "No reweighting.  ACS measure of workers mostly working from home.") +
  #coord_cartesian(xlim = c(10, 30), ylim = c(0.1, 0.2)) +
  #coord_cartesian(xlim = c(10, 30), ylim = c(0.1, 0.2)) +
  theme(
    #axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 0)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  guides(colour = guide_legend(ncol = 3))

p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/wham_vs_acs_msa_loglevel.pdf", width = 8, height = 6)
remove(list = c("p", "p_egg"))

p = df_acs %>%
  ggplot(., aes(x = 100*wfh_share, y = 100*wfh_share_acs)) +
  geom_point(aes(size = emp), alpha = 0.7, stroke=1) +
  #geom_point(aes(size = emp), alpha = 0, stroke=1) +
  geom_smooth(method=lm, colour = "grey", se=FALSE, fullrange=TRUE) +
  ylab("Share (2022)") +
  xlab('Share "Mostly Working from Home" (ACS)') +
  labs(title = "Remote Work Vacancies (WHAM) vs Travel Method (ACS)", subtitle = "No reweighting.  ACS measure of workers mostly working from home.") +
  coord_cartesian(xlim = c(0, 40), ylim = c(0, 40)) +
  theme(
    #axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 0)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  guides(colour = guide_legend(ncol = 3))

p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/wham_vs_acs_msa_level.pdf", width = 8, height = 6)
remove(list = c("p", "p_egg"))


