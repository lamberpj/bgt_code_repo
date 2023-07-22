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

setDTthreads(8)
getDTthreads()
#quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg_combined/")

colnames(fread("../bg-us/int_data/us_stru_2019_wfh.csv", nThread = 8, nrows = 1000))

df_us_2019 <- fread("../bg-us/int_data/us_stru_2019_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_id","job_date","soc","employer","sector","sector_name","naics5","naics4","state", "city", "wfh_wham","job_domain"))
df_us_2022 <- fread("../bg-us/int_data/us_stru_2022_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_id","job_date","soc","employer","sector","sector_name","naics5","naics4","state", "city", "wfh_wham","job_domain"))

df_us <- rbindlist(list(df_us_2019,df_us_2022))

remove(list = setdiff(ls(), "df_us"))

df_us <- df_us %>%
  .[employer != "" & !is.na(employer)] %>%
  .[year(job_date) %in% c(2019, 2022)]

df_us <- df_us %>%
  .[, period := ifelse(year(job_date) == 2019, "2019", "2022")]

nrow(df_us) # 71,348,406
df_us <- df_us %>%
  .[!grepl("careerbuilder", job_domain)]
nrow(df_us) # 65,938,081

# Boeing, Lockheed Martin, Northrop Grumman, SpaceX.
#### SPECIFIC EXAMPLES - AEROSPACE MANUF SECTOR MANAGEMENT OCCS ####
df_air_man <- df_us %>%
  .[, naics4 := str_sub(naics5, 1, 4)] %>%
  .[, soc2 := str_sub(soc, 1, 2)] %>%
  .[naics4 == "3364" & soc2 == "11"] %>%
  setDT(.) %>%
  .[, .(.N, wfh_share = round(100*mean(wfh_wham, na.rm = T),2)), by = .(employer, period)] %>%
  .[, sumsqrt_N := sum(N^0.5), by = employer] %>%
  .[order(-sumsqrt_N)] %>%
  .[employer %in% c("Northrop Grumman", "Lockheed Martin Corporation", "The Boeing Company", "Spacex")] %>%
  .[, employer_ord :=
      factor(employer,
             levels = rev(c("The Boeing Company","Lockheed Martin Corporation","Northrop Grumman","Spacex")))]

colnames(df_air_man)
for_wsj <- df_air_man %>% select(employer, period, wfh_share)

fwrite(for_wsj, "./aux_data/aerospace_employers_for_wsj.csv")

p = df_air_man %>%
  ggplot(aes(x = employer_ord, y = wfh_share+0.2, fill = as.factor(period)), group = employer_ord) +
  geom_bar(stat = "identity", width=0.7, position = position_dodge(width=0.7))  +
  ylab("Share (%)") +
  scale_y_continuous(breaks = seq(0,100,25)) +
  scale_fill_manual(values = c("#000000", "#CC6600")) +
  theme(
    axis.title.y=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 0),
    panel.grid.major.y = element_blank(),
    panel.grid.minor.y = element_blank()
  ) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm"),
        legend.position = c(.7, .2)) +
  guides(fill = guide_legend(ncol = 1)) +
  coord_flip() +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 60)) +
  theme(aspect.ratio=3/1)
p
save(p, file = "./ppt/ggplots/top_us_firms_soc2_11_naics4_3364_airspace_management.RData")
#### END ####

#Mutual of Omaha, UnitedHealth Group, Humana.
#### SPECIFIC EXAMPLES - INSURANCE SECTOR MATH OCCS ####
df_ins_math <- df_us %>%
  .[, naics4 := str_sub(naics5, 1, 4)] %>%
  .[, soc4 := str_sub(soc, 1, 5)] %>%
  .[naics4 == "5241" & soc4 == "15-20"] %>%
  setDT(.) %>%
  .[, .(.N, wfh_share = round(100*mean(wfh_wham, na.rm = T),2)), by = .(employer, period)] %>%
  .[, sumsqrt_N := sum(N^0.5), by = employer] %>%
  .[order(-sumsqrt_N)] %>%
  .[employer %in% c("Mutual of Omaha Company","UnitedHealth Group","Humana")] %>%
  .[, employer := ifelse(employer == "American International Group Incorporated", "AIG", employer)] %>%
  .[, employer_ord :=
      factor(employer,
             levels = rev(c("Mutual of Omaha Company","UnitedHealth Group","Humana")))]

p = df_ins_math %>%
  ggplot(aes(x = employer_ord, y = wfh_share+0.2, fill = as.factor(period)), group = employer_ord) +
  geom_bar(stat = "identity", width=0.7, position = position_dodge(width=0.7))  +
  ylab("Share (%)") +
  scale_y_continuous(breaks = seq(0,100,25)) +
  scale_fill_manual(values = c("#000000", "#FFCC33")) +
  theme(
    axis.title.y=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 0),
    panel.grid.major.y = element_blank(),
    panel.grid.minor.y = element_blank()
  ) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm"),
        legend.position = c(.7, .2)) +
  guides(fill = guide_legend(ncol = 1)) +
  coord_flip() +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 60)) +
  theme(aspect.ratio=3/1)
p
save(p, file = "./ppt/ggplots/top_us_firms_soc2_15-20_naics4_5241_insurance_math_occs.RData")
#### END ####

# Auto manufacturers

#### SPECIFIC EXAMPLES - AUTO MANUF ####
df_auto <- df_us %>%
  .[, naics4 := str_sub(naics5, 1, 4)] %>%
  .[, soc3 := str_sub(soc, 1, 4)] %>%
  .[soc3 == "17-2"] %>%
  setDT(.) %>%
  .[employer %in% c("General Motors", "Honda", "Ford Motor Company", "Tesla Motors", "Tesla")] %>%
  .[, employer := ifelse(employer %in% c("Tesla Motors", "Tesla"), "Tesla", employer)] %>%
  .[, .(.N, wfh_share = round(100*mean(wfh_wham, na.rm = T),2)), by = .(employer, period, soc3)] %>%
  .[, sumsqrt_N := sum(N^0.5), by = employer] %>%
  .[order(-sumsqrt_N)] %>%
  .[, employer_ord :=
      factor(employer,
             levels = rev(c("Honda","General Motors","Ford Motor Company","Tesla")))]

p = df_auto %>%
  ggplot(aes(x = employer_ord, y = wfh_share+0.2, fill = as.factor(period)), group = employer_ord) +
  geom_bar(stat = "identity", width=0.7, position = position_dodge(width=0.7))  +
  ylab("Share (%)") +
  scale_y_continuous(breaks = seq(0,100,20)) +
  scale_fill_manual(values = c("#000000", "#33CC33")) +
  theme(
    axis.title.y=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 0),
    panel.grid.major.y = element_blank(),
    panel.grid.minor.y = element_blank()
  ) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm"),
        legend.position = c(.7, .2)) +
  guides(fill = guide_legend(ncol = 1)) +
  coord_flip() +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 60)) +
  theme(aspect.ratio=3/1)
p
save(p, file = "./ppt/ggplots/top_us_firms_soc2_17-21_auto_naics4_3361_eng.RData")
#### END ####

# PriceWaterhousCoopers, GrantThornton, H&R Block
#### SPECIFIC EXAMPLES - ACCOUNTING SERVICES INDUSTRY, FINANCIAL SPECIALIST OCCUPATIONS  ####
df_accounting_fin_spec_occs <- df_us %>%
  .[, naics4 := str_sub(naics5, 1, 4)] %>%
  .[, soc4 := str_sub(soc, 1, 5)] %>%
  .[naics4 == "5412" & soc4 == "13-20"] %>%
  setDT(.) %>%
  .[, .(.N, wfh_share = round(100*mean(wfh_wham, na.rm = T),2)), by = .(employer, period)] %>%
  .[, sumsqrt_N := sum(N^0.5), by = employer] %>%
  .[order(-sumsqrt_N)] %>%
  .[employer %in% c("PricewaterhouseCoopers","Grant Thornton","H&R Block")] %>%
  .[, employer_ord :=
      factor(employer,
             levels = rev(c("PricewaterhouseCoopers","Grant Thornton","H&R Block")))]

p = df_accounting_fin_spec_occs %>%
  ggplot(aes(x = employer_ord, y = wfh_share+0.2, fill = as.factor(period)), group = employer_ord) +
  geom_bar(stat = "identity", width=0.7, position = position_dodge(width=0.7))  +
  ylab("Share (%)") +
  scale_y_continuous(breaks = seq(0,100,25)) +
  scale_fill_manual(values = c("#000000", "#33CC33")) +
  theme(
    axis.title.y=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 0),
    panel.grid.major.y = element_blank(),
    panel.grid.minor.y = element_blank()
  ) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm"),
        legend.position = c(.7, .2)) +
  guides(fill = guide_legend(ncol = 1)) +
  coord_flip() +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 60)) +
  theme(aspect.ratio=3/1)
p
save(p, file = "./ppt/ggplots/top_us_firms_soc2_13-20_naics4_5412_accounting_services_ind_fin_spec_occs.RData")
#### END ####

# Not included
#### SPECIFIC EXAMPLES - UNIVERSITIES ADMIN STAFF ####
df_sunis_admin <- df_us %>%
  .[, naics4 := str_sub(naics5, 1, 4)] %>%
  .[, soc4 := str_sub(soc, 1, 5)] %>%
  .[naics4 == "6113" & soc4 == "43-60"] %>%
  setDT(.) %>%
  .[, .(.N, wfh_share = round(100*mean(wfh_wham, na.rm = T),2)), by = .(employer, period)] %>%
  .[, sumsqrt_N := sum(N^0.5), by = employer] %>%
  .[order(-sumsqrt_N)] %>%
  .[employer %in% c("University of Texas", "Stanford University", "University of Florida", "University of Michigan", "University of Utah", "California State University", "Pennsylvania State University", "Cornell University", "Harvard University", "University of Wisconsin")] %>%
  .[, employer_ord :=
      factor(employer,
             levels = rev(c("University of Michigan","Stanford University","Harvard University","University of Wisconsin","University of Texas","Cornell University","University of Utah","California State University","Pennsylvania State University","University of Florida")))]

View(df_sunis_admin)

p = df_sunis_admin %>%
  ggplot(aes(x = employer_ord, y = wfh_share+0.2, fill = as.factor(period)), group = employer_ord) +
  geom_bar(stat = "identity", width=0.7, position = position_dodge(width=0.7))  +
  ylab("Share (%)") +
  scale_y_continuous(breaks = seq(0,100,10)) +
  scale_fill_manual(values = c("#000000", "#FFCC33")) +
  theme(
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
        legend.position = c(.7, .2)) +
  guides(fill = guide_legend(ncol = 1)) +
  coord_flip() +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 60)) +
  theme(aspect.ratio=3/5)
p
save(p, file = "./ppt/ggplots/top_us_firms_soc2_43-60_naics4_6113_universities_sec_and_admin_assistant_occs.RData")
#### END ####

#### SPECIFIC EXAMPLES - CONSULTANCIES, COMPUTER OCCUPATIONS  ####
df_man_consult_comp_occs <- df_us %>%
  .[, soc3 := str_sub(soc, 1, 4)] %>%
  .[soc3 == "13-1"] %>%
  .[, employer := ifelse(employer %in% c("Bain Company", "Bain Company Incorporated"), "Bain Company", employer)] %>%
  .[, employer := ifelse(employer %in% c("Boston Consulting Group Bcg", "Boston Consulting Group", "Boston Consulting Group Incorporated"), "Boston Consulting Group", employer)] %>%
  .[, employer := ifelse(employer %in% c("Bain Company", "Bain Company Incorporated"), "Bain Company", employer)] %>%
  .[, employer := ifelse(employer %in% c("PricewaterhouseCoopers", "Price Waterhouse Coopers", "Pricewaterhouse Coopers"), "PricewaterhouseCoopers", employer)] %>%
  setDT(.) %>%
  .[, .(.N, wfh_share = round(100*mean(wfh_wham, na.rm = T),2)), by = .(employer, period)] %>%
  .[, sumsqrt_N := sum(N^0.5), by = employer] %>%
  .[order(-sumsqrt_N)] %>%
  .[employer %in% c("McKinsey & Company","Boston Consulting Group","Accenture","Booz Allen Hamilton Inc.","KPMG","Bain Company","Boston Consulting Group","Deloitte","PricewaterhouseCoopers","Ernst & Young")] %>%
  .[, employer_ord :=
      factor(employer,
             levels = rev(c("PricewaterhouseCoopers","Accenture","Booz Allen Hamilton Inc.","Bain Company","KPMG","Ernst & Young","Deloitte","Boston Consulting Group","McKinsey & Company")))]

View(df_man_consult_comp_occs)

p = df_man_consult_comp_occs %>%
  ggplot(aes(x = employer_ord, y = wfh_share+0.2, fill = as.factor(period)), group = employer_ord) +
  geom_bar(stat = "identity", width=0.7, position = position_dodge(width=0.7))  +
  ylab("Share (%)") +
  scale_y_continuous(breaks = seq(0,100,5)) +
  scale_fill_manual(values = c("#000000", "#33CC33")) +
  theme(
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
        legend.position = c(.7, .2)) +
  guides(fill = guide_legend(ncol = 1)) +
  coord_flip() +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 60)) +
  theme(aspect.ratio=3/5)
p
save(p, file = "./ppt/ggplots/top_us_firms_soc3_13-1_busops_specialist_consulting_firms.RData")
#### END ####

#### SPECIFIC EXAMPLES - GOVERNMENT AGENCIES ####
df_man_gov_ind_other_man_occs <- df_us %>%
  .[, soc3 := str_sub(soc, 1, 4)] %>%
  .[soc3 == "11-9" & naics4 == "9211"] %>%
  setDT(.) %>%
  .[, .(.N, wfh_share = round(100*mean(wfh_wham, na.rm = T),2)), by = .(employer, period)] %>%
  .[, sumsqrt_N := sum(N^0.5), by = employer] %>%
  .[order(-sumsqrt_N)] %>%
  .[sumsqrt_N > 29] %>%
  .[employer %in% c("Internal Revenue Service","US Government","King County","Washington Department Of Health","State of Arizona","colorado state government","Commonwealth of Kentucky","State of Nebraska","State Of Massachusetts","State of New York","City of Philadelphia")] %>%
  .[, employer_ord :=
      factor(employer,
             levels = rev(c("Internal Revenue Service","US Government","King County","Washington Department Of Health","State of Arizona","colorado state government","Commonwealth of Kentucky","State of Nebraska","State Of Massachusetts","State of New York","City of Philadelphia")))]

View(df_man_gov_ind_other_man_occs)

p = df_man_gov_ind_other_man_occs %>%
  ggplot(aes(x = employer_ord, y = wfh_share+0.2, fill = as.factor(period)), group = employer_ord) +
  geom_bar(stat = "identity", width=0.7, position = position_dodge(width=0.7))  +
  ylab("Share (%)") +
  scale_y_continuous(breaks = seq(0,100,10)) +
  scale_fill_manual(values = c("#000000", "deeppink")) +
  theme(
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
        legend.position = c(.7, .2)) +
  guides(fill = guide_legend(ncol = 1)) +
  coord_flip() +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 60)) +
  theme(aspect.ratio=3/5)
p
save(p, file = "./ppt/ggplots/top_us_firms_soc2_11-9_naics4_9211_other_man__occs.RData")
#### END ####

#### FIRM SIZE and REMOTE WORK ####
remove(list = setdiff(ls(), "df_us"))

check <- df_us %>%
  .[period == "2022"] %>%
  .[, firm_posting_count := .N, by = employer]

check <- check %>% .[employer != "" & !is.na(employer)]

length(check[firm_posting_count < 10]$employer)/length(check$employer) # 8.39%
length(check[firm_posting_count > 1666]$employer)/length(check$employer) # 47.8%

df_us_top_employers_cells <- df_us %>%
  .[, keep := .N >= 10, by = .(period,employer)] %>%
  .[keep == TRUE] %>%
  .[, .(.N, wfh_share = 100*mean(wfh_wham, na.rm = T)), by = .(period, employer)] %>%
  .[!is.na(wfh_share)] %>%
  .[order(employer, period)] %>%
  .[, Np := .N, by = employer] %>%
  .[, dhs_change_wfh := (wfh_share - shift(wfh_share))/(0.5*(wfh_share + shift(wfh_share))), by = employer] %>%
  .[, dhs_change_N := (N - shift(N))/(0.5*(N + shift(N))), by = employer] %>%
  .[, pc_change_wfh := (wfh_share - shift(wfh_share))/(wfh_share), by = employer] %>%
  .[, pc_change_N := (N - shift(N))/(N), by = employer] %>%
  .[, obs := .N, by = employer]

nrow(df_us_top_employers_cells) # 449,399 unbalanced, 198,248 balanced

quantile(df_us_top_employers_cells[period == "2022"]$N, probs = c(0.25, 0.5, 0.75, 0.9, 0.95, 0.98, 0.99, 0.995, 0.999, 0.9999, 1))
quantile(df_us_top_employers_cells[period == "2019"]$N, probs = c(0.25, 0.5, 0.75, 0.9, 0.95, 0.98, 0.99, 0.995, 0.999, 0.9999, 1))

(ub_N <- quantile(df_us_top_employers_cells[period == "2019"]$N, probs = c(0.95), na.rm = T))

head(df_us_top_employers_cells)

scaleFUN <- function(x) {paste0(round(x, 0))}

p = df_us_top_employers_cells %>%
  #filter(period == "2019") %>%
  filter(N <= 365.65) %>%
  ggplot(., aes(y = wfh_share, x = N, shape = as.character(period), colour = as.character(period))) +
  scale_x_continuous(trans = log_trans(), 
                     labels=scaleFUN,
                     breaks = c(8, 16, 32, 64, 128, 256, 512)) +
  scale_y_continuous(breaks = seq(0, 20, 2.5)) +
  stat_summary_bin(bins=30, size=4, geom='point') +
  ylab("Percent") +
  xlab('Number of Postings') +
  #labs(title = "Binscatter WFH vs Posting Counts", subtitle = "No reweighting, no residualising") +
  coord_cartesian(xlim = c(8, 400), ylim = c(2, 13), expand = FALSE) +
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
        legend.key.width = unit(1,"cm"),
        legend.position = c(0.085, 0.865)) +
  guides(colour = guide_legend(ncol = 1)) +
  scale_shape_manual(values=c(17, 16)) +
  theme(aspect.ratio=3/5)
p
save(p, file = "./ppt/ggplots/wfh_share_vs_firm_size_bs1.RData")


p = df_us_top_employers_cells %>%
  #filter(period == "2019") %>%
  filter(N <= 1666) %>%
  ggplot(., aes(y = wfh_share, x = N, shape = as.character(period), colour = as.character(period))) +
  scale_x_continuous(trans = log_trans(), 
                     labels=scaleFUN,
                     breaks = c(8, 16, 32, 64, 128, 256, 512, 1024, 2048, 2*2048, 4*2048, 8*2048, 16*2048)) +
  scale_y_continuous(breaks = seq(0, 20, 2.5)) +
  stat_summary_bin(bins=30, size=4, geom='point') +
  ylab("Percent") +
  xlab('Number of Postings') +
  #labs(title = "Binscatter WFH vs Posting Counts", subtitle = "No reweighting, no residualising") +
  coord_cartesian(xlim = c(8, 2250), ylim = c(1.8, 12.6), expand = FALSE) +
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
        legend.key.width = unit(1,"cm"),
        legend.position = c(0.085, 0.865)) +
  guides(colour = guide_legend(ncol = 1)) +
  scale_shape_manual(values=c(17, 16)) +
  theme(aspect.ratio=3/5)
p
save(p, file = "./ppt/ggplots/wfh_share_vs_firm_size_bs2.RData")

#### END ####

#### OLD ####


df_us_top_employers_cells <- df_us %>%
  .[period == "2022"] %>%
  .[city != "" & !is.na(city) & soc != "" & !is.na(soc) & naics5 != "" & !is.na(naics5)] %>%
  .[, keep := .N > 20, by = employer] %>%
  .[keep == TRUE] %>%
  .[, keep := .N > 2, by = .(employer, naics5, soc, city)] %>%
  .[keep == TRUE] %>%
  select(-keep) %>%
  .[, .(.N, wfh_share = 100*mean(wfh_wham, na.rm = T)), by = .(employer, naics5, city, soc)] %>%
  .[!is.na(wfh_share)] %>%
  .[N > 0] %>%
  mutate(naics5 = as.character(naics5),
         city = as.character(city),
         soc = as.character(soc)) %>%
  setDT(.)

mod1 <- feols(data = df_us_top_employers_cells,
              fml = wfh_share ~ -1 | naics5 + city + soc, cluster = ~ employer)

fe <- fixef(mod1)
fe_naics5 <- as.data.frame(fe$naics5) %>% mutate(naics5 = rownames(.)) %>% rename(fev_naics = colnames(.)[1])
fe_city <- as.data.frame(fe$city) %>% mutate(city = rownames(.)) %>% rename(fev_city = colnames(.)[1])
fe_soc <- as.data.frame(fe$soc) %>% mutate(soc = rownames(.)) %>% rename(fev_soc = colnames(.)[1])

df_us_top_employers_cells <- df_us_top_employers_cells %>%
  left_join(fe_naics5) %>%
  left_join(fe_city) %>%
  left_join(fe_soc) %>%
  setDT(.) %>%
  .[, mean_naics5 := mean(wfh_share), by = naics5] %>%
  .[, mean_city := mean(wfh_share), by = city] %>%
  .[, mean_soc := mean(wfh_share), by = soc] %>%
  .[, gmean_naics5 := mean(mean_naics5)] %>%
  .[, gmean_city := mean(mean_city)] %>%
  .[, gmean_soc := mean(mean_soc)] %>%
  .[, wfh_share_resid := wfh_share + fev_naics + fev_city + fev_soc + gmean_naics5 + gmean_city + gmean_soc]

library(splitstackshape)

df_us_top_employers_cells_expanded <- df_us_top_employers_cells %>%
  select(wfh_share_resid, employer, N) %>%
  group_by(employer) %>%
  expandRows('N') %>%
  ungroup() %>%
  setDT() %>%
  .[, .(.N, wfh_share_resid = mean(wfh_share_resid, na.rm = T)), by = .(employer)] %>%
  .[!is.na(wfh_share_resid)]

quantile(df_us_top_employers_cells_expanded$wfh_share_resid, probs = c(0, 0.05))
quantile(df_us_top_employers_cells_expanded$wfh_share_resid, probs = c(0.9, 0.95, 0.98, 0.99, 1))

df_us_top_employers_cells_expanded <- df_us_top_employers_cells_expanded %>%
  .[, wfh_share_resid := pmin(100, pmax(0, wfh_share_resid))]

summary(df_us_top_employers_cells_expanded)

p = df_us_top_employers_cells_expanded %>%
  .[N < 500] %>%
  ggplot(., aes(y = wfh_share_resid, x = N)) +
  stat_summary_bin(bins=10,
                   color='orange', size=4, geom='point', ) +
  stat_summary_bin(bins=500,
                   color='grey', size=2, geom='point', alpha = 0.5) +
  ylab("Share (2022)") +
  xlab('Number of Postings in 2022') +
  labs(title = "Binscatter WFH vs Posting Counts", subtitle = "No reweighting, residualising wfh share on NAICS5, city and SOC") +
  coord_cartesian(xlim = c(0, 500), ylim = c(30, 70)) +
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
ggsave(p_egg, filename = "./plots/wfh_share_vs_firm_size_bs_resid.pdf", width = 8, height = 6)
remove(list = c("p", "p_egg"))


# Look at variation across firms, with and without residualising

# with residulaising
r_by_soc2 <- df_us_top_employers_cells %>%
  setDT(.) %>%
  .[, soc2 := str_sub(soc, 1, 2)] %>%
  select(wfh_share_resid, employer, N, soc2) %>%
  group_by(employer, soc2) %>%
  expandRows('N') %>%
  ungroup() %>%
  setDT() %>%
  .[, .(.N, wfh_share_resid = mean(wfh_share_resid, na.rm = T)), by = .(employer, soc2)] %>%
  .[!is.na(wfh_share_resid)] %>%
  .[, wfh_share_resid := pmin(100, pmax(0, wfh_share_resid))] %>%
  .[, .(.N, iqr = IQR(wfh_share_resid), min_max = max(wfh_share_resid) - min(wfh_share_resid), sd = sd(wfh_share_resid)),
    by = soc2]

r_by_soc2 <- r_by_soc2 %>%
  mutate(iqr = round(iqr),
         min_max = round(min_max),
         sd = round(sd))

soc2010_names <- fread(file = "./aux_data/us_soc_2010_names.csv")
soc2010_names
soc2010_names$soc10_2d <- as.numeric(soc2010_names$soc10_2d)
r_by_soc2$soc2 <- as.numeric(r_by_soc2$soc2)
nrow(r_by_soc2)
r_by_soc2 <- r_by_soc2 %>%
  left_join(., soc2010_names, by = c("soc2" = "soc10_2d")) %>%
  setDT(.)
nrow(r_by_soc2)
rm(soc2010_names)
r_by_soc2$name <- gsub("and", "&", r_by_soc2$name, fixed = T)
r_by_soc2$name <- gsub(" Occupations", "", r_by_soc2$name)
r_by_soc2$name <- gsub(", Sports, & Media| & Technical|, & Repair|Cleaning &|& Serving Related", "", r_by_soc2$name)

r_by_soc2 <- r_by_soc2 %>% select(soc2, name, everything())

r_by_soc2$name <- gsub("/", "and", r_by_soc2$name)

stargazer(r_by_soc2, summary = FALSE, title = "Spread of Advertised Remote Work Shares, across Firms within SOC2 Occupations (Residualized using NAICS5 + City + SOC Fixed Effects)", rownames = FALSE)

#### END ####
