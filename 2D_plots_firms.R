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

df_us_2019 <- fread("../bg-us/int_data/us_stru_2019_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_id","job_date","soc","employer","sector","sector_name","naics5","naics4","state", "city", "wfh_wham","job_domain","job_url"))
df_us_2021 <- fread("../bg-us/int_data/us_stru_2021_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_id","job_date","soc","employer","sector","sector_name","naics5","naics4","state", "city", "wfh_wham","job_domain","job_url"))
df_us_2022 <- fread("../bg-us/int_data/us_stru_2022_wfh.csv", nThread = 8, integer64 = "numeric", select = c("job_id","job_date","soc","employer","sector","sector_name","naics5","naics4","state", "city", "wfh_wham","job_domain","job_url"))

df_us <- rbindlist(list(df_us_2019,df_us_2021,df_us_2022))

remove(list = setdiff(ls(), "df_us"))

df_us <- df_us %>%
  .[employer != "" & !is.na(employer)]

df_us <- df_us %>%
  .[year(job_date) == 2019 | year(job_date) == 2021 & job_date >= ymd("2021-07-01")| year(job_date) == 2022 & job_date < ymd("2022-07-01")] %>%
  .[, period := ifelse(year(job_date) == 2019, "2019", "2021-22")]

nrow(df_us) # 70,405,086
df_us <- df_us %>%
  .[!grepl("careerbuilder", job_domain)]
nrow(df_us) # 65,938,081
ls()
#### FIRM SIZE and REMOTE WORK ####
remove(list = setdiff(ls(), "df_us"))

df_us_top_employers_cells <- df_us %>%
  .[period == "2021-22"] %>%
  .[city != "" & !is.na(city) & soc != "" & !is.na(soc) & naics5 != "" & !is.na(naics5)] %>%
  .[, keep := .N > 20, by = employer] %>%
  .[keep == TRUE] %>%
  .[, keep := .N > 2, by = .(employer, naics5, soc, city)] %>%
  .[keep == TRUE] %>%
  select(-keep) %>%
  .[, .(.N, wfh_share = 100*mean(wfh_wham, na.rm = T)), by = .(employer)] %>%
  .[!is.na(wfh_share)]

quantile(df_us_top_employers_cells$N, probs = c(0, 0.25, 0.5, 0.75, 0.9))

p = df_us_top_employers_cells %>%
  .[N < 500] %>%
  ggplot(., aes(y = wfh_share, x = N)) +
  stat_summary_bin(bins=10,
                   color='orange', size=4, geom='point', ) +
  stat_summary_bin(bins=250,
                   color='grey', size=2, geom='point', alpha = 0.5) +
  ylab("Share (2022)") +
  xlab('Number of Postings in 2021-22') +
  labs(title = "Binscatter WFH vs Posting Counts", subtitle = "No reweighting, no residualising") +
  coord_cartesian(xlim = c(0, 500), ylim = c(0, 20)) +
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
ggsave(p_egg, filename = "./plots/wfh_share_vs_firm_size_bs1.pdf", width = 8, height = 6)
remove(list = c("p", "p_egg"))

df_us_top_employers_cells <- df_us %>%
  .[period == "2021-22"] %>%
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
  xlab('Number of Postings in 2021-22') +
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

# Specific Case Studies
good_soc2_11_naics5 <- c(33641,52411, 54121, 52211, 33611, 33441, 42361, 52393, 61131, 52311, 81341, 81219, 92111, 92119, 32561, 72251)
good_soc2_15_naics5 <- c(33641,52411,45411,54161,52211,51913,44821,33422)

# Largest firms by NAICS 4
df_us_top_employers <- df_us %>%
  .[, soc2 := str_sub(soc, 1, 2)] %>%
  .[period == "2021-22" & !is.na(naics5)] %>%
  .[soc2 == "19"] %>%
  .[, .(.N, wfh_share = round(100*mean(wfh_wham, na.rm = T),2)), by = .(employer, naics5)] %>%
  .[order(naics5, -N)] %>%
  .[, rank := c(1:.N), by = naics5] %>%
  .[rank <= 5] %>%
  .[, keep := ifelse(any(rank == 5 & N > 25), 1, 0), by = naics5] %>%
  .[keep == 1] %>%
  select(-keep)





#### Make other data



View(as.data.frame(table(as.yearmon(df_us$job_date), df_us$period)))

df_us_firms <- df_us %>%
  .[, soc2 := str_sub(soc, 1, 2)] %>%
  .[, .(wfh_wham = mean(wfh_wham, na.rm = T),
        .N),
    by = .(soc2, period, employer)]

nrow(df_us_firms) # 6,701,753
df_us_firms <- df_us_firms %>%
  .[, keep := all(N>10), by = .(soc2, employer)] %>%
  .[keep == TRUE] %>%
  select(-keep)
nrow(df_us_firms) # 421,710



# BAR PLOT 2019, 2021

####### Plot within-industry and occupational change  #######

## Information & Mgmt Occ ##
info<-df_us[employer != "" &  naics3 %in% c("511","519"),
            .(n_posts = .N), 
            by = list(employer, year)]
info<-pivot_wider(info, names_from = year, values_from = n_posts)
info<-na.omit(info)
info$min_posts<-apply(info[,c(2:3)], 1, min)
info_top<-info %>% slice_max(order_by = min_posts, n = 25)
info_top
info_firm<-c("Microsoft Corporation", "Vmware Incorporated", "Salesforce", "Pearson")

df_info_mgmt <- df_us %>%
  filter(employer %in% info_firm & naics3 %in% c("511","519") & bgt_occ2 == "11") %>%
  setDT(.) %>%
  .[, .(wfh_post = sum(wfh), n_post = .N,
        wfh_share = sum(wfh)/.N), by = .(year, employer)] %>%
  filter(n_post >=45) %>%
  setDT(.) %>%
  .[, n_year := .N, by = employer] %>%
  filter(n_year == 2)

df_info_comp <- df_us %>%
  filter(employer %in% info_firm & naics3 %in% c("511","519") & bgt_occ2 == "15") %>%
  setDT(.) %>%
  .[, .(wfh_post = sum(wfh), n_post = .N,
        wfh_share = sum(wfh)/.N), by = .(year, employer)] %>%
  filter(n_post >=45) %>%
  setDT(.) %>%
  .[, n_year := .N, by = employer] %>%
  filter(n_year == 2)

info_firm<-unique(df_info_mgmt$employer[df_info_mgmt$employer %in% df_info_comp$employer])

info_firm
df_info_mgmt
df_info_mgmt$employer<-ifelse(df_info_mgmt$employer == "Adobe Systems", "Adobe",
                              ifelse(df_info_mgmt$employer == "Google Inc.", "Google",
                                     ifelse(df_info_mgmt$employer == "Microsoft Corporation", "Microsoft",
                                            ifelse(df_info_mgmt$employer == "Vmware Incorporated", "VMware", df_info_mgmt$employer))))
df_info_mgmt

sel_order <- 
  df_info_mgmt %>% 
  filter(year == "2021") %>% 
  arrange(wfh_share) %>% 
  mutate(employer = factor(employer))

p_info_mgmt = df_info_mgmt %>%
  mutate(employer = factor(employer, levels = sel_order$employer, ordered = TRUE)) %>% 
  ggplot(aes(x = employer, y = wfh_share, fill = as.factor(year)), group = employer) +
  geom_bar(stat = "identity", width=0.7, position = position_dodge(width=0.7))  +
  ylab("Share (%)") +
  labs(title = "Share of RW Vacancy Postings by Firm", subtitle = "USA IT Sector, Management Occupations" ) +
  scale_y_continuous(breaks = seq(0,4,0.2), limits = c(0,1)) +
  scale_fill_manual(values = c("#000000", "#CC6600")) +
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
  guides(fill = guide_legend(ncol = 2)) +
  coord_flip() +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 60))

p_info_mgmt

p_egg <- set_panel_size(p = p_info_mgmt,
                        width = unit(3.5, "in"),
                        height = unit(4*0.3, "in"))

ggsave(p_egg, filename = "./plots/top_us_firms_it_sector_management_occ.pdf", width = 8, height = 4*0.3+3)

## Information & Comp, Math Occ ##
df_info_comp <- df_us %>%
  filter(employer %in% info_firm & naics3 %in% c("511","519") & bgt_occ2 == "15") %>%
  setDT(.) %>%
  .[, .(wfh_post = sum(wfh), n_post = .N,
        wfh_share = sum(wfh)/.N), by = .(year, employer)] %>%
  setDT(.)

df_info_comp$employer<-ifelse(df_info_comp$employer == "Apple Inc.", "Apple",
                              ifelse(df_info_comp$employer == "Google Inc.", "Google",
                                     ifelse(df_info_comp$employer == "Microsoft Corporation", "Microsoft",
                                            ifelse(df_info_comp$employer == "Vmware Incorporated", "VMware", df_info_comp$employer))))

p_info_comp = df_info_comp %>%
  mutate(employer = factor(employer, levels = sel_order$employer, ordered = TRUE)) %>% 
  ggplot(aes(x = employer, y = wfh_share, fill = as.factor(year)), group = employer) +
  geom_bar(stat = "identity", width=0.7, position = position_dodge(width=0.7))  +
  ylab("Share (%)") +
  labs(title = "Share of RW Vacancy Postings by Firm", subtitle = "USA IT Sector, Computer Occupations" ) +
  scale_y_continuous(breaks = seq(0,4,0.2), limits = c(0,1)) +
  scale_fill_manual(values = c("#000000", "#CC6666")) +
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
  guides(fill = guide_legend(ncol = 2)) +
  coord_flip() +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 60))

p_info_comp

p_egg <- set_panel_size(p = p_info_comp,
                        width = unit(3.5, "in"),
                        height = unit(4*0.3, "in"))

ggsave(p_egg, filename = "./plots/top_us_firms_it_sector_comp_occ.pdf", width = 12, height = 4*0.3 + 3)


## Finance & Biz Occ ##
fin<-df_us[employer != "" &  sector == "52", .
           (n_posts = .N), 
           by = list(employer, year)]
fin<-pivot_wider(fin, names_from = year, values_from = n_posts)
fin<-na.omit(fin)
fin <- fin[fin$employer != "State Farm Insurance Companies",]
fin$min_posts<-apply(fin[,c(2:3)], 1, min)
fin_top<-fin %>% slice_max(order_by = min_posts, n = 10)
fin_firm<-fin_top$employer

fin_firm

df_fin_biz <- df_us %>%
  filter(employer %in% fin_firm & sector == "52" & bgt_occ2 == "13") %>%
  setDT(.) %>%
  .[, .(wfh_post = sum(wfh), n_post = .N,
        wfh_share = sum(wfh)/.N), by = .(year, employer)] %>%
  filter(n_post >=45) %>%
  setDT(.) %>%
  .[, n_year := .N, by = employer] %>%
  filter(n_year == 2)

df_fin_mgmt <- df_us %>%
  filter(employer %in% fin_firm & sector == "52" & bgt_occ2 == "11") %>%
  setDT(.) %>%
  .[, .(wfh_post = sum(wfh), n_post = .N,
        wfh_share = sum(wfh)/.N), by = .(year, employer)] %>%
  filter(n_post >=45) %>%
  setDT(.) %>%
  .[, n_year := .N, by = employer] %>%
  filter(n_year == 2)

fin_firm<-unique(df_fin_biz$employer[df_fin_biz$employer %in% df_fin_mgmt$employer])

df_fin_biz$employer<-ifelse(df_fin_biz$employer == "JP Morgan Chase Company", "JP Morgan",
                            ifelse(df_fin_biz$employer == "The PNC Financial Services Group, Inc.", "PNC Financial",
                                   ifelse(df_fin_biz$employer == "State Farm Insurance Companies", "State Farm",df_fin_biz$employer)))

sel_order_fin <- 
  df_fin_biz %>% 
  filter(year == "2021") %>% 
  arrange(wfh_share) %>% 
  mutate(employer = factor(employer))

p_fin_biz = df_fin_biz %>%
  mutate(employer = factor(employer, levels = sel_order_fin$employer, ordered = TRUE)) %>% 
  ggplot(aes(x = employer, y = wfh_share, fill = as.factor(year)), group = employer) +
  geom_bar(stat = "identity", width=0.7, position = position_dodge(width=0.7))  +
  ylab("Share (%)") +
  labs(title = "Share of RW Vacancy Postings by Firm", subtitle = "USA Finance/Insurance Sector, Business/Finance Occupations" ) +
  scale_y_continuous(breaks = seq(0,4,0.2), limits = c(0,0.9)) +
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
        legend.position = c(.7, .1)) +
  guides(fill = guide_legend(ncol = 2)) +
  coord_flip() +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 60))

p_fin_biz

p_egg <- set_panel_size(p = p_fin_biz,
                        width = unit(3.5, "in"),
                        height = unit(10*0.3, "in"))

ggsave(p_egg, filename = "./plots/top_us_firms_fin_sector_busfin_occ.pdf", width = 12, height = 10*0.3 + 3)

### fin & mgmt ###

df_fin_mgmt <- df_us %>%
  filter(employer %in% fin_firm & sector == "52" & bgt_occ2 == "11") %>%
  setDT(.) %>%
  .[, .(wfh_post = sum(wfh), n_post = .N,
        wfh_share = sum(wfh)/.N), by = .(year, employer)] %>%
  setDT(.)

df_fin_mgmt$employer<-ifelse(df_fin_mgmt$employer == "JP Morgan Chase Company", "JP Morgan",
                             ifelse(df_fin_mgmt$employer == "The PNC Financial Services Group, Inc.", "PNC Financial",
                                    ifelse(df_fin_mgmt$employer == "State Farm Insurance Companies", "State Farm",df_fin_mgmt$employer)))


p_fin_mgmt = df_fin_mgmt %>%
  mutate(employer = factor(employer, levels = sel_order_fin$employer, ordered = TRUE)) %>% 
  ggplot(aes(x = employer, y = wfh_share, fill = as.factor(year)), group = employer) +
  geom_bar(stat = "identity", width=0.7, position = position_dodge(width=0.7))  +
  ylab("Share (%)") +
  labs(title = "Share of RW Vacancy Postings by Firm", subtitle = "USA Finance/Insurance Sector, Management Occupations" ) +
  scale_y_continuous(breaks = seq(0,4,0.2), limits = c(0,0.8)) +
  scale_fill_manual(values = c("#000000", "#CC6600")) +
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
        legend.position = c(.7, .1)) +
  guides(fill = guide_legend(ncol = 2)) +
  coord_flip() +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 60))

p_fin_mgmt

p_egg <- set_panel_size(p = p_fin_mgmt,
                        width = unit(3.5, "in"),
                        height = unit(10*0.3, "in"))

ggsave(p_egg, filename = "./plots/top_us_firms_fin_sector_man_occ.pdf", width = 12, height = 10*0.3+3)

remove(list = setdiff(ls(),"df_us"))

## Edu & Mgmt, Occ ##
edu_all_us<-df_us[employer != "" &  sector == "61" & bgt_occ2 %in% c(11,14,31,33),
                  .(n_posts = .N), 
                  by = list(employer, year)]
edu_all_us<-pivot_wider(edu_all_us, names_from = year, values_from = n_posts)
edu_all_us<-na.omit(edu_all_us)
edu_all_us$min_posts<-apply(edu_all_us[,c(2:3)], 1, min)
edu_top<-edu_all_us %>% slice_max(order_by = min_posts, n = 14)
edu_firm<-edu_top$employer
edu_firm
us_edu<-c("Stanford University", "University of Chicago", "Harvard University")
edu_firm<-append(edu_firm, us_edu)

df_edu_us <- df_us %>%
  filter(employer %in% edu_firm  & sector == "61" & bgt_occ2 %in% c(11,14,31,33)) %>%
  setDT(.) %>%
  .[, .(wfh_post = sum(wfh), n_post = .N,
        wfh_share = sum(wfh)/.N), 
    by = .(year, employer)] %>%
  setDT(.)

df_edu_us$employer<-str_replace_all(df_edu_us$employer, "The ", "")
df_edu_us$employer<-str_replace_all(df_edu_us$employer, "University", "U")
df_edu_us$employer<-str_replace_all(df_edu_us$employer, "Pennsylvania", "Penn")

sel_order_edu <- 
  df_edu_us %>% 
  filter(year == 2021) %>% 
  arrange(wfh_share) %>% 
  mutate(employer = factor(employer))

p_edu = df_edu_us %>%
  mutate(employer = factor(employer, levels = sel_order_edu$employer, ordered = TRUE)) %>% 
  ggplot(aes(x = employer, y = wfh_share, fill = as.factor(year)), group = employer) +
  geom_bar(stat = "identity", width=0.7, position = position_dodge(width=0.7))  +
  ylab("Share (%)") +
  labs(title = "Share of RW Vacancy Postings by Firm", subtitle = "USA Tertiary Education Sector, Office Occupations" ) +
  scale_y_continuous(breaks = seq(0,4,0.2), limits = c(0,1)) +
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
        legend.position = c(.7, .1)) +
  guides(fill = guide_legend(ncol = 2)) +
  coord_flip() +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 60))

p_edu

p_egg <- set_panel_size(p = p_edu,
                        width = unit(3.5, "in"),
                        height = unit(16*0.3, "in"))

ggsave(p_egg, filename = "./plots/top_us_firms_tertiary_educ_sector_wc_occ.pdf", width = 12, height = 16*0.3 + 3)

#### government ####

df_gov <- df_us %>%
  filter(employer != "" & sector == "92" ) %>%
  setDT(.) %>%
  .[, .(n_posts = .N), 
    by = .(year, employer)] 
gov_us<-pivot_wider(df_gov, names_from = year, values_from = n_posts)
gov_us<-na.omit(gov_us)
gov_us$min_posts<-apply(gov_us[,c(2:3)], 1, min)
gov_top<-gov_us %>% slice_max(order_by = min_posts, n = 14)
gov_firm<-gov_top$employer

df_gov<-pivot_wider(df_gov, names_from = year, values_from = n_posts)
df_gov<-na.omit(df_gov)
df_gov$min_posts<-apply(df_gov[,c(2:3)], 1, min)

df_gov_us <- df_us %>%
  filter(employer %in% gov_firm  & sector == "92") %>%
  setDT(.) %>%
  .[, .(wfh_post = sum(wfh), n_post = .N,
        wfh_share = sum(wfh)/.N), 
    by = .(year, employer)] %>%
  setDT(.)

sel_order_gov <- 
  df_gov_us %>% 
  filter(year == "2021") %>% 
  arrange(wfh_share) %>% 
  mutate(employer = factor(employer))

p_gov = df_gov_us %>%
  mutate(employer = factor(employer, levels = sel_order_gov$employer, ordered = TRUE)) %>% 
  ggplot(aes(x = employer, y = wfh_share, fill = as.factor(year)), group = employer) +
  geom_bar(stat = "identity", width=0.7, position = position_dodge(width=0.7))  +
  ylab("Share (%)") +
  labs(title = "Share of RW Vacancy Postings by Firm", subtitle = "USA Government Sector, All Occupations" ) +
  scale_y_continuous(breaks = seq(0,4,0.2), limits = c(0,1)) +
  scale_fill_manual(values = c("#000000", "#CC0000")) +
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
        legend.position = c(.7, .1)) +
  guides(fill = guide_legend(ncol = 2)) +
  coord_flip() +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 60))

p_gov

p_egg <- set_panel_size(p = p_gov,
                        width = unit(3.5, "in"),
                        height = unit(14*0.3, "in"))

ggsave(p_egg, filename = "./plots/top_us_firms_gov_sector.pdf", width = 12, height = 14*0.3 + 3)

remove(list = setdiff(ls(),"df_us"))

#### END ####


