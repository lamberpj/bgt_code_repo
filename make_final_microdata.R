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
ggthemr('flat')
library(egg)
library(extrafont)
library(fixest)
# font_import()
#loadfonts(device="postscript")
#fonts()

setDTthreads(1)
getDTthreads()
#quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg_combined/")

#### PREPARE WEIGHTED DATA #### ####
remove(list = ls())

w_aus_2019 <- fread("./aux_data/emp_weights/w_aus_2019.csv")
w_nz_2019 <- fread("./aux_data/emp_weights/w_nz_2019.csv") %>%
  .[, anzsco_code := as.numeric(str_sub(anzsco_code, 1, 4))] %>%
  .[, .(tot_emp = sum(as.numeric(tot_emp), na.rm = T)), by = anzsco_code] %>%
  .[, emp_share := tot_emp / sum(tot_emp, na.rm = T)]
w_can_2019 <- fread("./aux_data/emp_weights/w_can_2019.csv") %>%
  .[, can_noc := as.numeric(can_noc)]
w_uk_2019 <- fread("./aux_data/emp_weights/w_uk_2019.csv") %>%
  .[, emp_share := ifelse(is.na(emp_share), 0, emp_share)]
w_us_2019 <- fread("./aux_data/emp_weights/w_us_2019.csv") %>%
  .[, us_soc18 := str_sub(us_soc18, 1, 5)] %>%
  .[, .(tot_emp = sum(tot_emp, na.rm = T)), by = us_soc18] %>%
  .[, emp_share := tot_emp / sum(tot_emp, na.rm = T)]

sum(w_nz_2019$tot_emp)/1000000 # 2.445144
sum(w_aus_2019$tot_emp)/1000000 # 12.89455
sum(w_can_2019$tot_emp)/1000000 # 18.9824
sum(w_uk_2019$tot_emp, na.rm = T)/1000000 # 32.1755
sum(w_us_2019$tot_emp, na.rm = T)/1000000 # 146.8735

d_and_m_wfh_onet <- fread(file = "./aux_data/occupations_workathome.csv") %>%
  select(onetsoccode, teleworkable) %>%
  rename(onet = onetsoccode)

d_and_m_wfh_onet6 <- d_and_m_wfh_onet %>%
  mutate(onet = str_sub(onet, 1, 7)) %>%
  group_by(onet) %>%
  mutate(teleworkable = mean(teleworkable, na.rm = T)) %>%
  ungroup() %>%
  distinct(.)

d_and_m_wfh_onet3 <- d_and_m_wfh_onet %>%
  mutate(onet = str_sub(onet, 1, 4)) %>%
  group_by(onet) %>%
  mutate(teleworkable = mean(teleworkable, na.rm = T)) %>%
  ungroup() %>%
  distinct(.)

### END ####

#### MAKE UNIFIED MICRODATA ####

#### US ####
df_us_stru_2019 <- fread("../bg-us/int_data/us_stru_2019_wfh.csv")
df_us_stru_2020 <- fread("../bg-us/int_data/us_stru_2020_wfh.csv")
df_us_stru_2021 <- fread("../bg-us/int_data/us_stru_2021_wfh.csv")
df_us_stru_2022 <- fread("../bg-us/int_data/us_stru_2022_wfh.csv")

df_all_us <- rbindlist(df_us_stru_2019,df_us_stru_2020,df_us_stru_2021,df_us_stru_2022)

df_all_us <- df_all_us %>%
  .[!is.na(soc)]
nrow(df_all_us) # 11,608,001
soc10_soc18_xwalk <- readxl::read_xlsx("./aux_data/emp_weights/us_weights/soc_2010_to_2018_crosswalk.xlsx", skip = 8) %>%
  clean_names %>%
  select(x2010_soc_code, x2018_soc_code)

soc10_soc18_xwalk

nrow(df_all_us) # 11,574,877
df_all_us <- df_all_us %>%
  left_join(., soc10_soc18_xwalk, by = c("soc" = "x2010_soc_code"))
df_all_us <- df_all_us %>%
  .[, us_soc18 := str_sub(x2018_soc_code, 1, 5)] %>%
  .[!is.na(us_soc18)] %>%
  .[, country := "US"] %>%
  left_join(w_us_2019) %>%
  setDT(.) %>%
  .[!is.na(emp_share) & !is.na(tot_emp)]

# Apportion employment across (weighted) vacancies
df_all_us <- df_all_us %>%
  setDT(.) %>%
  .[, job_id_weight := 1/.N, by = job_id] %>%
  setDT(.) %>%
  .[, tot_emp_ad := tot_emp/sum(job_id_weight), by = .(year_month, us_soc18)]

# Check how problematic the extensive margin is
#check_us <- df_all_us %>%
#  group_by(year_month) %>%
#  summarise(sum(tot_emp_ad))
#View(check_us)
colnames(df_all_us)

df_all_us <- df_all_us %>%
  select(job_id,country,year_month,job_date,wfh_prob,neg,dict,oecd_dict,dict_weight,state,city,msa,msa_name,fips,county,
         employer,exp, max_exp,degree,min_salary,job_hours,bgt_occ,bgt_occ_name,
         onet,naics4,sector,sector_name,tot_emp_ad,job_id_weight)

df_all_us <- df_all_us %>%
  left_join(d_and_m_wfh_onet)

df_all_us <- df_all_us %>%
  mutate(onet6 = str_sub(onet, 1, 7)) %>%
  left_join(d_and_m_wfh_onet6, by = c("onet6" = "onet"))

df_all_us <- df_all_us %>%
  mutate(teleworkable = ifelse(!is.na(teleworkable.x), teleworkable.x, teleworkable.y)) %>%
  select(-c(onet6, teleworkable.x, teleworkable.y))

df_all_us <- df_all_us %>%
  mutate(onet3 = str_sub(onet, 1, 4)) %>%
  left_join(d_and_m_wfh_onet3, by = c("onet3" = "onet"))

df_all_us <- df_all_us %>%
  mutate(teleworkable = ifelse(!is.na(teleworkable.x), teleworkable.x, teleworkable.y)) %>%
  select(-c(onet3, teleworkable.x, teleworkable.y))

df_all_us <- setDT(df_all_us)

#### end ####

#### Canada ####
load("./int_data/bgt_structured/can_stru_wfh.RData")
df_all_can <- df_all_can %>% setDT(.) %>% .[!is.na(noc_code)]

cops_noc_concordance <- fread("./aux_data/emp_weights/can_weights/cops_noc_concordance.csv") %>% clean_names %>%
  select(cops_occupational_groupings_codes, noc_4_digit_codes) %>%
  rename(can_noc = noc_4_digit_codes, cops = cops_occupational_groupings_codes) %>%
  .[, can_noc := as.numeric(can_noc)] %>%
  .[, cops := as.numeric(cops)]
nrow(df_all_can) # 3,609,187
df_all_can<-df_all_can %>%
  .[, can_noc := as.numeric(str_sub(noc_code, 1, 4))] %>%
  .[!is.na(can_noc)] %>%
  left_join(cops_noc_concordance) %>%
  setDT(.) %>%
  .[, can_noc := ifelse(can_noc %in% w_can_2019$can_noc, can_noc, cops)] %>%
  .[!is.na(can_noc)] %>%
  .[, country := "Canada"] %>%
  left_join(w_can_2019) %>%
  setDT(.) %>%
  .[!is.na(emp_share) & !is.na(tot_emp)]
nrow(df_all_can) # 3,529,039

# Apportion employment across (weighted) vacancies
df_all_can <- df_all_can %>%
  setDT(.) %>%
  .[, job_id_weight := 1/.N, by = job_id] %>%
  setDT(.) %>%
  .[, tot_emp_ad := tot_emp/sum(job_id_weight), by = .(year_month, can_noc)]

colnames(df_all_can)

# Check how problematic the extensive margin is
#check_can <- df_all_can %>%
#  group_by(year_month) %>%
#  summarise(sum(tot_emp_ad))
#View(check_can)

colnames(df_all_can)

df_all_can <- df_all_can %>%
  select(job_id,country,year_month,job_date,wfh_prob,neg,dict,oecd_dict,dict_weight,canon_state,canon_city,
         canon_employer,min_experience,max_experience,canon_minimum_degree,min_degree_level,min_annual_salary,canon_job_hours,clean_job_title,bgt_occ,bgt_occ_name,bgt_occ_group_name,consolidated_inferred_naics,industry_name,
         industry_group_name,sub_sector_name,sector_name,tot_emp_ad,job_id_weight)

nrow(df_all_can)
df_all_can <- df_all_can %>%
  mutate(onet6 = str_sub(bgt_occ, 1, 7)) %>%
  left_join(d_and_m_wfh_onet6, by = c("onet6" = "onet"))

df_all_can <- df_all_can %>%
  mutate(onet3 = str_sub(bgt_occ, 1, 4)) %>%
  left_join(d_and_m_wfh_onet3, by = c("onet3" = "onet"))

df_all_can <- df_all_can %>%
  mutate(teleworkable = ifelse(!is.na(teleworkable.x), teleworkable.x, teleworkable.y)) %>%
  select(-c(onet3, teleworkable.x, teleworkable.y))

df_all_can <- setDT(df_all_can)
nrow(df_all_can)

#### end ####

#### Australia ####
load("./int_data/bgt_structured/anz_stru_wfh.RData")
df_all_anz <- df_all_anz %>% setDT(.) %>% .[!is.na(anzsco_code)]
df_all_aus <- df_all_anz %>%
  filter(canon_country == "AUS")

nrow(df_all_aus) # 2,381,602
df_all_aus<-setDT(df_all_aus) %>%
  .[, anzsco_code := as.numeric(str_sub(anzsco_code, 1, 4))] %>%
  .[!is.na(anzsco_code)] %>%
  .[, country := "Australia"] %>%
  left_join(w_aus_2019) %>%
  setDT(.) %>%
  .[!is.na(emp_share) & !is.na(tot_emp)]
nrow(df_all_aus) # 2,381,602

# Apportion employment across (weighted) vacancies
df_all_aus <- df_all_aus %>%
  setDT(.) %>%
  .[, job_id_weight := 1/.N, by = job_id] %>%
  setDT(.) %>%
  .[, tot_emp_ad := tot_emp/sum(job_id_weight), by = .(year_month, anzsco_code)]

# Check how problematic the extensive margin is
#check_aus <- df_all_aus %>%
#  group_by(year_month) %>%
#  summarise(sum(tot_emp_ad))
#View(check_aus)
#colnames(df_all_aus)
colnames(df_all_aus)

df_all_aus <- df_all_aus %>%
  select(job_id,country,year_month,job_date,wfh_prob,neg,dict,oecd_dict,dict_weight,canon_state,canon_city,
         canon_employer,min_experience,max_experience,canon_minimum_degree,min_degree_level,canon_job_hours,min_annual_salary,clean_job_title,bgt_occ,bgt_occ_name,bgt_occ_group_name,anzsic_code,anzsic_class,anzsic_group,anzsic_subdivision,anzsic_division,
         tot_emp_ad,job_id_weight)

nrow(df_all_aus) # 628,935
df_all_aus <- df_all_aus %>%
  mutate(onet6 = str_sub(bgt_occ, 1, 7)) %>%
  left_join(d_and_m_wfh_onet6, by = c("onet6" = "onet"))

df_all_aus <- df_all_aus %>%
  select(-c(onet6))

df_all_aus <- df_all_aus %>%
  mutate(onet3 = str_sub(bgt_occ, 1, 4)) %>%
  left_join(d_and_m_wfh_onet3, by = c("onet3" = "onet"))

df_all_aus <- df_all_aus %>%
  mutate(teleworkable = ifelse(!is.na(teleworkable.x), teleworkable.x, teleworkable.y)) %>%
  select(-c(onet3, teleworkable.x, teleworkable.y))

df_all_aus <- setDT(df_all_aus)
nrow(df_all_aus) # 628,935

### end ####

#### NZ ####
load("./int_data/bgt_structured/anz_stru_wfh.RData")
df_all_anz <- df_all_anz %>% setDT(.) %>% .[!is.na(anzsco_sub_major_group)]
df_all_nz <- df_all_anz %>%
  filter(canon_country == "NZL")
nrow(df_all_nz) # 631935
df_all_nz<-setDT(df_all_nz) %>%
  .[, anzsco_code := as.numeric(str_sub(anzsco_code, 1, 4))] %>%
  .[, country := "NZ"] %>%
  left_join(w_nz_2019) %>%
  setDT(.) %>%
  .[!is.na(emp_share) & !is.na(tot_emp)]
colnames(df_all_nz)

# Apportion employment across (weighted) vacancies
df_all_nz <- df_all_nz %>%
  setDT(.) %>%
  .[, job_id_weight := 1/.N, by = job_id] %>%
  setDT(.) %>%
  .[, tot_emp_ad := tot_emp/sum(job_id_weight), by = .(year_month, anzsco_code)]

colnames(df_all_nz)
# Check how problematic the extensive margin is
#check_nz <- df_all_nz %>%
#  group_by(year_month) %>%
#  summarise(sum(tot_emp_ad))
#View(check_nz)

df_all_nz <- df_all_nz %>%
  select(job_id,country,year_month,job_date,wfh_prob,neg,dict,oecd_dict,dict_weight,canon_state,canon_city,
         canon_employer,min_experience,max_experience,canon_minimum_degree,min_degree_level,canon_job_hours,min_annual_salary,clean_job_title,bgt_occ,bgt_occ_name,bgt_occ_group_name,anzsic_code,anzsic_class,anzsic_group,anzsic_subdivision,anzsic_division,
         tot_emp_ad,job_id_weight)

nrow(df_all_nz) # 628,935
df_all_nz <- df_all_nz %>%
  mutate(onet6 = str_sub(bgt_occ, 1, 7)) %>%
  left_join(d_and_m_wfh_onet6, by = c("onet6" = "onet"))

df_all_nz <- df_all_nz %>%
  select(-c(onet6))

df_all_nz <- df_all_nz %>%
  mutate(onet3 = str_sub(bgt_occ, 1, 4)) %>%
  left_join(d_and_m_wfh_onet3, by = c("onet3" = "onet"))

df_all_nz <- df_all_nz %>%
  mutate(teleworkable = ifelse(!is.na(teleworkable.x), teleworkable.x, teleworkable.y)) %>%
  select(-c(onet3, teleworkable.x, teleworkable.y))

df_all_nz <- setDT(df_all_nz)
nrow(df_all_nz) # 628,935

### end ####

#### UK ####
load("./int_data/bgt_structured/uk_stru_wfh.RData")

df_all_uk <- df_all_uk %>% setDT(.) %>% .[!is.na(uksoc_code)]

df_all_uk <- df_all_uk %>%
  .[, uk_soc10 := as.numeric(str_sub(uksoc_code, 1, 4))] %>%
  .[!is.na(uk_soc10)] %>%
  .[, country := "UK"] %>%
  left_join(w_uk_2019) %>%
  setDT(.) %>%
  .[!is.na(emp_share) & !is.na(tot_emp)]

# Apportion employment across (weighted) vacancies
df_all_uk <- df_all_uk %>%
  setDT(.) %>%
  .[, job_id_weight := 1/.N, by = job_id] %>%
  setDT(.) %>%
  .[, tot_emp_ad := tot_emp/sum(job_id_weight), by = .(year_month, uk_soc10)]

# Check how problematic the extensive margin is
#check_uk <- df_all_uk %>%
#  group_by(year_month) %>%
#  summarise(sum(tot_emp_ad))
#View(check_uk)
colnames(df_all_uk)

df_all_uk <- df_all_uk %>%
  select(job_id,country,year_month,job_date,wfh_prob,neg,dict,oecd_dict,dict_weight,nation,region,canon_county,canon_city,ttwa,
         canon_employer,min_experience,max_experience,canon_minimum_degree,min_degree_level,min_annual_salary,canon_job_hours,clean_job_title,bgt_occ,bgt_occ_name,bgt_occ_group_name,sic_code,sic_class,
         sic_group,sic_division,sic_section,tot_emp_ad,job_id_weight)

nrow(df_all_uk) # 628,935
df_all_uk <- df_all_uk %>%
  mutate(onet6 = str_sub(bgt_occ, 1, 7)) %>%
  left_join(d_and_m_wfh_onet6, by = c("onet6" = "onet"))

df_all_uk <- df_all_uk %>%
  select(-c(onet6))

df_all_uk <- df_all_uk %>%
  mutate(onet3 = str_sub(bgt_occ, 1, 4)) %>%
  left_join(d_and_m_wfh_onet3, by = c("onet3" = "onet"))

df_all_uk <- df_all_uk %>%
  mutate(teleworkable = ifelse(!is.na(teleworkable.x), teleworkable.x, teleworkable.y)) %>%
  select(-c(onet3, teleworkable.x, teleworkable.y))

df_all_uk <- setDT(df_all_uk)
nrow(df_all_uk) # 628,935

#### end ####

#### Standardise covariaties including NAs / -999s ####
ls()
remove(list = setdiff(ls(),c("df_all_nz", "df_all_aus", "df_all_can", "df_all_uk", "df_all_us")))

# Remove Cannon
colnames(df_all_nz) <- gsub("canon_","", colnames(df_all_nz))
colnames(df_all_aus) <- gsub("canon_","", colnames(df_all_aus))
colnames(df_all_can) <- gsub("canon_","", colnames(df_all_can))
colnames(df_all_uk) <- gsub("canon_","", colnames(df_all_uk))
colnames(df_all_us) <- gsub("canon_","", colnames(df_all_us))

# Job Title
colnames(df_all_nz) # 
colnames(df_all_aus) # 
colnames(df_all_can) # 
colnames(df_all_uk) # 
colnames(df_all_us) # 

df_all_nz <- df_all_nz %>% rename(job_title = clean_job_title)
df_all_aus <- df_all_aus %>% rename(job_title = clean_job_title)
df_all_can <- df_all_can %>% rename(job_title = clean_job_title)
df_all_uk <- df_all_uk %>% rename(job_title = clean_job_title)
df_all_us <- df_all_us %>% rename(job_title = clean_title)

# Geography
colnames(df_all_nz) # state
colnames(df_all_aus) # state
colnames(df_all_can) # state
colnames(df_all_uk) # nation
colnames(df_all_us) # state

df_all_nz$city[1:5]
df_all_aus$city[1:5]
df_all_can$city <- str_to_title(df_all_can$city)
df_all_can$city[1:5]
df_all_uk$city[1:5]
df_all_us$city[1:5]

df_all_uk <- df_all_uk %>% rename(state = nation)

# Experience - call this disjoint_exp_max, and disjoint_degree_level

df_all_nz <- df_all_nz %>% rename(disjoint_exp_max = max_experience, disjoint_exp_min = min_experience)
df_all_aus <- df_all_aus %>% rename(disjoint_exp_max = max_experience, disjoint_exp_min = min_experience)
df_all_can <- df_all_can %>% rename(disjoint_exp_max = max_experience, disjoint_exp_min = min_experience)
df_all_uk <- df_all_uk %>% rename(disjoint_exp_max = max_experience, disjoint_exp_min = min_experience)
df_all_us <- df_all_us %>% rename(disjoint_exp_max = max_exp, disjoint_exp_min = exp)

# Min Degree - call this disjoint_degree_name, and disjoint_degree_level
table(df_all_nz$minimum_degree)
table(df_all_aus$min_degree_level)
table(df_all_can$min_degree_level)
table(df_all_uk$min_degree_level)
table(df_all_us$degree)

df_all_nz <- df_all_nz %>% rename(disjoint_degree_level = min_degree_level, disjoint_degree_name = minimum_degree)
df_all_aus <- df_all_aus %>% rename(disjoint_degree_level = min_degree_level, disjoint_degree_name = minimum_degree)
df_all_can <- df_all_can %>% rename(disjoint_degree_level = min_degree_level, disjoint_degree_name = minimum_degree)
df_all_uk <- df_all_uk %>% rename(disjoint_degree_level = min_degree_level, disjoint_degree_name = minimum_degree)
df_all_us <- df_all_us %>% rename(disjoint_degree_name = degree)

# Go back and check if other vars

# Industry
table(df_all_nz$anzsic_division)
table(df_all_aus$anzsic_division)
table(df_all_can$sector_name)
table(df_all_uk$sic_section)
table(df_all_us$sector_name)

uniqueN(df_all_nz$anzsic_division) # 20
uniqueN(df_all_aus$anzsic_division) # 20
uniqueN(df_all_can$sector_name) # 21
uniqueN(df_all_uk$sic_section) # 22
uniqueN(df_all_us$sector_name) # 21

# Need to cluster properly, for now let's just make them all "disjoint_sector"
df_all_nz <- df_all_nz %>% rename(disjoint_sector = anzsic_division)
df_all_aus <- df_all_aus %>% rename(disjoint_sector = anzsic_division)
df_all_can <- df_all_can %>% rename(disjoint_sector = sector_name)
df_all_uk <- df_all_uk %>% rename(disjoint_sector = sic_section)
df_all_us <- df_all_us %>% rename(disjoint_sector = sector_name)

# Min Salary (call this disjoint_salary")
colnames(df_all_nz) # min_annual_salary
colnames(df_all_aus) # min_annual_salary
colnames(df_all_can) # min_annual_salary
colnames(df_all_uk) # min_annual_salary
colnames(df_all_us) # min_salary

df_all_nz <- df_all_nz %>% rename(disjoint_salary = min_annual_salary)
df_all_aus <- df_all_aus %>% rename(disjoint_salary = min_annual_salary)
df_all_can <- df_all_can %>% rename(disjoint_salary = min_annual_salary)
df_all_uk <- df_all_uk %>% rename(disjoint_salary = min_annual_salary)
df_all_us <- df_all_us %>% rename(disjoint_salary = min_salary)

# Job hours
colnames(df_all_nz) # job_hours
colnames(df_all_aus) # job_hours
colnames(df_all_can) # job_hours
colnames(df_all_uk) # job_hours
colnames(df_all_us) # job_hours

table(df_all_nz$job_hours)
table(df_all_aus$job_hours)
table(df_all_can$job_hours)
table(df_all_uk$job_hours)
table(df_all_us$job_hours)

# Firm
colnames(df_all_nz) # employer
colnames(df_all_aus) # employer
colnames(df_all_can) # employer
colnames(df_all_uk) # employer
colnames(df_all_us) # employer

# Date
colnames(df_all_nz) # employer
colnames(df_all_aus) # employer
colnames(df_all_can) # employer
colnames(df_all_uk) # employer
colnames(df_all_us) # employer

#
df_all_nz <- df_all_nz %>% select(job_id, country, state, city, year_month, job_date, wfh_prob, neg, dict, oecd_dict, dict_weight, teleworkable, employer, job_title, bgt_occ, bgt_occ_name, disjoint_exp_max, disjoint_exp_min, job_hours, disjoint_sector, disjoint_degree_level, disjoint_degree_name, disjoint_salary, tot_emp_ad, job_id_weight)
df_all_aus <- df_all_aus %>% select(job_id, country, state, city, year_month, job_date, wfh_prob, neg, dict, oecd_dict, dict_weight, teleworkable, employer, job_title, bgt_occ, bgt_occ_name,  disjoint_exp_max, disjoint_exp_min, job_hours, disjoint_sector, disjoint_degree_level, disjoint_degree_name, disjoint_salary, tot_emp_ad, job_id_weight)
df_all_can <- df_all_can %>% select(job_id, country, state, city, year_month, job_date, wfh_prob, neg, dict, oecd_dict, dict_weight, teleworkable, employer, job_title, bgt_occ, bgt_occ_name,  disjoint_exp_max, disjoint_exp_min, job_hours, disjoint_sector, disjoint_degree_level, disjoint_degree_name, disjoint_salary, tot_emp_ad, job_id_weight)
df_all_uk <- df_all_uk %>% select(job_id, country, state, region, county, ttwa, city, year_month, job_date, wfh_prob, neg, dict, oecd_dict, dict_weight, teleworkable, employer, job_title, bgt_occ, bgt_occ_name,  disjoint_exp_max, disjoint_exp_min, job_hours, disjoint_sector, disjoint_degree_level, disjoint_degree_name, disjoint_salary, tot_emp_ad, job_id_weight)
df_all_us <- df_all_us %>% select(job_id, country, state, msa, msa_name, county, fips, city, year_month, job_date, wfh_prob, neg, dict, oecd_dict, dict_weight, teleworkable, employer, job_title, bgt_occ, bgt_occ_name, onet,  disjoint_exp_max, disjoint_exp_min, job_hours, disjoint_sector, disjoint_degree_name, disjoint_salary, tot_emp_ad, job_id_weight)

df_all <- bind_rows(df_all_nz,df_all_aus,df_all_can,df_all_uk,df_all_us) %>% setDT(.) %>%  .[year_month <= as.yearmon("Feb 2022")]

colnames(df_all)

df_all <- df_all %>% select(job_id, country, state, region, county, ttwa, msa, msa_name, county, fips, city, everything())

table(df_all$country)

remove(list = setdiff(ls(), "df_all"))

df_all$year <- year(df_all$year_month)
df_all$month <- str_sub(as.character(df_all$year_month), 1, 3)
df_all$month <- factor(df_all$month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))

df_all$disjoint_salary <- as.numeric(df_all$disjoint_salary)
df_all$disjoint_salary <- ifelse(df_all$disjoint_salary<0, NA, df_all$disjoint_salary)

df_all <- df_all %>%
  mutate(across(where(is.character), ~ifelse(.=="", NA, as.character(.))))
df_all <- setDT(df_all)

#### MZ CODE 
df_all$bach_or_higher<-grepl("bachelor|master|doctor|PhD", df_all$disjoint_degree_name, ignore.case = T)
df_all$bach_or_higher<-ifelse(df_all$bach_or_higher==FALSE & 
                                df_all$disjoint_degree_level >=16 & ! is.na(df_all$disjoint_degree_level), 
                              TRUE, df_all$bach_or_higher)
df_all$bach_or_higher<-ifelse(is.na(df_all$disjoint_degree_level) & df_all$disjoint_degree_name == "",
                              NA, df_all$bach_or_higher)
df_all$bach_or_higher<-ifelse(df_all$bach_or_higher == TRUE, 1, ifelse(df_all$bach_or_higher == FALSE, 0, NA))

df_all$sector_clustered<-ifelse(df_all$disjoint_sector %in% c("Wholesale Trade", "Retail Trade", "WHOLESALE AND RETAIL TRADE; REPAIR OF MOTOR VEHICLES AND MOTORCYCLES"), "Wholesale and Retail Trade",
                                ifelse(df_all$disjoint_sector %in% c("Accommodation and Food Services","ACCOMMODATION AND FOOD SERVICE ACTIVITIES"),"Accomodation and Food Services",
                                       ifelse(df_all$disjoint_sector %in% c("Electricity, Gas, Water and Waste Services","WATER SUPPLY; SEWERAGE, WASTE MANAGEMENT AND REMEDIATION ACTIVITIES", "Utilities", "ELECTRICITY, GAS, STEAM AND AIR CONDITIONING SUPPLY"), "Utility Services",
                                              ifelse(df_all$disjoint_sector %in% c("Administrative and Support Services","Administrative and Support and Waste Management and Remediation Services","ADMINISTRATIVE AND SUPPORT SERVICE ACTIVITIES"),"Administrative and Support",
                                                     ifelse(df_all$disjoint_sector %in% c("Professional, Scientific and Technical Services","Professional, Scientific, and Technical Services", "PROFESSIONAL, SCIENTIFIC AND TECHNICAL ACTIVITIES"),"Technical Services",
                                                            ifelse(df_all$disjoint_sector %in% c("Other Services", "Other Services (except Public Administration)", "OTHER SERVICE ACTIVITIES"), "Other Services",
                                                                   ifelse(df_all$disjoint_sector %in% c("Education and Training", "Educational Services", "EDUCATION"), "Education",
                                                                          ifelse(df_all$disjoint_sector %in% c("Health Care and Social Assistance", "HUMAN HEALTH AND SOCIAL WORK ACTIVITIES"), "Healthcare",
                                                                                 ifelse(df_all$disjoint_sector %in% c("Public Administration and Safety", "Public Administration", "PUBLIC ADMINISTRATION AND DEFENCE; COMPULSORY SOCIAL SECURITY"), "Public Administration",
                                                                                        ifelse(df_all$disjoint_sector %in% c("Financial and Insurance Services", "Financial and Insurance", "FINANCIAL AND INSURANCE ACTIVITIES"), "Finance and Insurance",
                                                                                               ifelse(df_all$disjoint_sector %in% c("Information Media and Telecommunications", "Information", "INFORMATION AND COMMUNICATION"), "Information and Communication",
                                                                                                      ifelse(df_all$disjoint_sector %in% c("Manufacturing", "MANUFACTURING"), "Manufacturing",
                                                                                                             ifelse(df_all$disjoint_sector %in% c("Construction", "CONSTRUCTION"), "Construction",
                                                                                                                    ifelse(df_all$disjoint_sector %in% c("Rental, Hiring and Real Estate Services","Real Estate and Rental and Leasing",  "REAL ESTATE ACTIVITIES"), "Real Estate",
                                                                                                                           ifelse(df_all$disjoint_sector %in% c("Agriculture, Forestry and Fishing",  "Agriculture, Forestry, Fishing and Hunting","AGRICULTURE, FORESTRY AND FISHING"),"Agriculture",
                                                                                                                                  ifelse(df_all$disjoint_sector %in% c("Transport, Postal and Warehousing", "Transportation and Warehousing","TRANSPORTATION AND STORAGE"), "Transportation",
                                                                                                                                         ifelse(df_all$disjoint_sector %in% c("Arts and Recreation Services","Arts, Entertainment, and Recreation",  "ARTS, ENTERTAINMENT AND RECREATION"), "Arts and Entertainment",
                                                                                                                                                ifelse(df_all$disjoint_sector %in% c("Mining", "Mining, Quarrying, and Oil and Gas Extraction","MINING AND QUARRYING"), "Mining", df_all$disjoint_sector))))))))))))))))))

df_all[df_all==""] <- NA
check <- as.data.frame(table(df_all$sector_clustered, df_all$disjoint_sector)) %>% filter(Freq > 0)
fwrite(df_all, file = "./int_data/df_all_standardised.csv")

# REFINE FURTHER
remove(list = ls())
df_all <- fread(file = "./int_data/df_all_standardised.csv", nThread = 8)
df_all$month <- factor(df_all$month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))
df_all$wfh <- as.numeric(df_all$wfh_prob>0.5)
df_all$bgt_occ6 <- str_sub(df_all$bgt_occ, 1, 7)
df_all$bgt_occ2 <- as.numeric(str_sub(df_all$bgt_occ, 1, 2))
df_all$job_ymd <- ymd(df_all$job_date)
df_all$year_quarter <- as.yearqtr(df_all$job_ymd)
df_all$year_month <- as.yearmon(df_all$job_ymd)
df_all <- setDT(df_all)

df_all <- df_all %>%
  mutate(sector_clustered = ifelse(sector_clustered %in% c("",
                                                           "ACTIVITIES OF EXTRATERRITORIAL ORGANISATIONS AND BODIES",
                                                           "ACTIVITIES OF HOUSEHOLDS AS EMPLOYERS; UNDIFFERENTIATED GOODS-AND SERVICES-PRODUCING ACTIVITIES OF HOUSEHOLDS FOR OWN USE"),
                                   NA, sector_clustered)) %>%
  setDT(.)

# CLUSTER EXP #

table(df_all$disjoint_exp_min)
table(df_all$disjoint_exp_max)

df_all <- df_all %>%
  .[, disjoint_exp_min := ifelse(disjoint_exp_min == -999, NA, disjoint_exp_min)] %>%
  .[, disjoint_exp_max := ifelse(disjoint_exp_max == -999, NA, disjoint_exp_max)]

df_all <- df_all %>%
  .[, exp_max := disjoint_exp_max] %>%
  select(-c(disjoint_exp_min, disjoint_exp_max))

# ARRANGE #
df_all <- df_all %>%
  setDT(.) %>%
  .[order(country, job_id)]

# SAVE #
fwrite(df_all, file = "./int_data/df_all_standardised.csv")

#### END ####