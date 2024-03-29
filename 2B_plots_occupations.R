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

#### END ####

#### LOAD DATA ####
# colnames(fread("../bg-us/int_data/us_stru_2019_wfh.csv", nrows = 100))
# 
# df_us_2019 <- fread("../bg-us/int_data/us_stru_2019_wfh.csv", nThread = 4, integer64 = "numeric", select = c("job_id","job_date","soc","onet","year_quarter","wfh_wham","job_domain"))
# df_us_2022 <- fread("../bg-us/int_data/us_stru_2022_wfh.csv", nThread = 4, integer64 = "numeric", select = c("job_id","job_date","soc","onet","year_quarter","wfh_wham","job_domain"))
# 
# df_us <- rbindlist(list(df_us_2019,df_us_2022))
# ls()
# remove(list = c("df_us_2019","df_us_2022"))
# df_us <- df_us %>% .[!grepl("careerbuilder", job_domain)]
# df_us <- df_us[!is.na(job_domain) & job_domain != ""]
# nrow(df_us)
# df_us <- df_us %>% .[!(onet %in% c("19-2099.01","19-4099.03"))]
# nrow(df_us)
# 
# #### END ####
# remove(list = setdiff(ls(), "df_us"))
# df_us$year_quarter <- as.yearqtr(df_us$job_date)
# df_us$year_month <- as.yearmon(df_us$job_date)
# 
# colnames(df_us)
# 
# df_us_oc <- df_us %>%
#   .[!is.na(soc) & soc != ""] %>%
#   .[!is.na(wfh_wham) & wfh_wham != ""] %>%
#   .[, soc2 := str_sub(soc, 1, 2)] %>%
#   .[, year := year(job_date)] %>%
#   .[year %in% c(2019, 2022)] %>%
#   setDT(.) %>%
#   select(year, wfh_wham, soc2) %>%
#   setDT(.) %>%
#   .[, .(wfh_share = mean(wfh_wham)),
#     by = .(year, soc2)] %>%
#   setDT(.)
# 
# soc2010_names <- fread(file = "./aux_data/us_soc_2010_names.csv")
# soc2010_names$soc10_2d <- as.numeric(soc2010_names$soc10_2d)
# df_us_oc$soc2 <- as.numeric(df_us_oc$soc2)
# nrow(df_us_oc)
# df_us_oc <- df_us_oc %>%
#   left_join(., soc2010_names, by = c("soc2" = "soc10_2d")) %>%
#   setDT(.)
# nrow(df_us_oc)
# rm(soc2010_names)
# df_us_oc$name <- gsub("and", "&", df_us_oc$name, fixed = T)
# df_us_oc$name <- gsub(" Occupations", "", df_us_oc$name)
# df_us_oc$name <- gsub(", Sports, & Media| & Technical|, & Repair|Cleaning &|& Serving Related", "", df_us_oc$name)
# df_us_oc <- df_us_oc %>% filter(!is.na(df_us_oc$name))
# df_us_oc <- setDT(df_us_oc)
# df_us_oc$ussoc_2d_wn <- paste0(df_us_oc$name)
# 
# saveRDS(df_us_oc, file = "./int_data/df_occ_2019_2022.csv")
df_us_oc <- readRDS(file = "./int_data/df_occ_2019_2022.csv")

df_us_oc$wfh_share <- round(df_us_oc$wfh_share*100, 1)

fwrite(df_us_oc, file = "./occ_group_for_cesifo.csv")

#### END ####

#### BAR PLOT 2022 vs 2019 ####
df_us_oc <- df_us_oc %>%
  group_by(ussoc_2d_wn) %>%
  mutate(prop_growth = ifelse(!is.na(lag(wfh_share)), paste0(round((wfh_share)/lag(wfh_share),1),"X"), NA)) %>%
  ungroup() %>%
  setDT(.)
df_us_oc
cbbPalette <- c("#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00")
cbbPalette_oc <- c("#000000", "darkorange")
cbbPalette_ind <- c("#000000", "#F0E442")
df_us_oc$prop_growth[is.na(df_us_oc$prop_growth)] <- ""
df_us_oc <- df_us_oc %>%
  mutate(year = ifelse(year == 2019, "2019", "2022"))

df_us_oc <- df_us_oc %>%
  mutate(ussoc_2d_wn_fac = factor(ussoc_2d_wn, levels = df_us_oc[year==2022][order(wfh_share)]$ussoc_2d_wn,  ordered = T)) %>% ungroup()

p = df_us_oc %>%
  ggplot(., aes(x = ussoc_2d_wn_fac, y = 100*wfh_share, fill = as.factor(year))) +
  geom_bar(stat = "identity", width=1, position = position_dodge(width=0.8))  +
  geom_text(aes(label = prop_growth, family = "serif"), size = 5, vjust = 0, colour = "black", hjust = -0.5) +
  ylab("Share (%)") +
  scale_y_continuous(breaks = seq(0,100,5), limits = c(0, 37)) +
  scale_fill_manual(values = cbbPalette_oc) +
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
        axis.text.y = element_text(hjust=1),
        legend.position = c(0.80, 0.125)) +
  guides(fill = guide_legend(ncol = 1)) +
  coord_flip() +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 60)) +
  theme(aspect.ratio=4/4)
p
save(p, file = "./ppt/ggplots/occ_dist_alt.RData")
remove(p)

#### END ####

#### OCCUPATION SCATTER PLOTS ####
# Prepare Data
remove(list = setdiff(ls(), "df_us"))

df_us_oc <- df_us %>%
  .[!is.na(onet) & onet != ""] %>%
  .[!is.na(wfh_wham) & wfh_wham != ""] %>%
  .[, year := year(job_date)] %>%
  setDT(.) %>%
  select(year, wfh_wham, onet) %>%
  setDT(.) %>%
  .[, .(.N, wfh_share = 100*round(mean(wfh_wham),4)),
    by = .(year, onet)] %>%
  .[, share := N/sum(N), by = year] %>%
  setDT(.)

remove(list = setdiff(ls(), c("df_us", "df_us_oc")))
occupations_workathome <- read_csv("./aux_data/occupations_workathome.csv") %>% rename(onet = onetsoccode)
df_us_oc
occupations_workathome
sum(!(occupations_workathome$onet %in% df_us_oc$onet))
sum(df_us_oc[year == 2022][!(df_us_oc[year == 2022]$onet %in% occupations_workathome$onet)]$share) # 5.7% of vacs not matches - who cares!

df_us_oc <- df_us_oc %>%
  left_join(occupations_workathome)

df_us_oc <- df_us_oc %>% .[!is.na(teleworkable)]

remove(list = setdiff(ls(), c("df_us", "df_us_oc")))

saveRDS(df_us_oc, file = "./int_data/us_onet_wham_2019_vs_2022.rds")

# outliers

library(ggrepel)
set.seed(999)

df_us_oc$`D&N Classification:` <- ifelse(df_us_oc$teleworkable == 1, "Teleworkable", "Not Teleworkable")

# Scatter plot (Log-log)
df_us_oc_wide <- df_us_oc %>%
  select(-share) %>%
  group_by(onet, teleworkable, `D&N Classification:`) %>%
  pivot_wider(., names_from = year, values_from = c("wfh_share", "N")) %>%
  rename(n_post_2019 = N_2019,
         n_post_2022 = N_2022) %>%
  setDT(.)
nrow(df_us_oc_wide)
# Outliers
class(df_us_oc_wide$teleworkable)
class(df_us_oc_wide$year)
(check2 <- df_us_oc_wide %>% .[teleworkable == 0 & n_post_2022 > median(n_post_2022)] %>% .[order(desc(wfh_share_2022))] %>% .[1:50])
(check1 <- df_us_oc_wide %>% .[teleworkable == 1 & n_post_2022 > median(n_post_2022)] %>% .[order(wfh_share_2022)] %>% .[1:20])

df_us_oc_wide <- df_us_oc_wide %>%
  .[, title_keepA := ifelse(title %in%
                              c("Software Developers, Applications", "Telemarketers", "Advertising Sales Agents", "Loan Officers",
                                "Mental Health and Substance Abuse Social Workers",
                                "Gas Plant Operators"),
                            title, NA)] %>%
  .[, title_keepA := gsub(" and ", " & ", title_keepA)] %>%
  .[, title_keepA := gsub(", Applications", "", title_keepA)] %>%
  .[, title_keepA := gsub(", Except Special Education", "", title_keepA)] %>%
  .[, title_keepA := gsub(" & Substance Abuse Social", "", title_keepA)]

df_us_oc_wide <- df_us_oc_wide %>%
  .[, title_keepB := ifelse(title %in%
                              c("Registered Nurses", "Pharmacy Technicians", "Roofers", "Travel Agents", "Kindergarten Teachers, Except Special Education",
                                "Medical Secretaries", "Legal Secretaries", "Interpreters and Translators"),
                            title, NA)] %>%
  .[, title_keepB := gsub(" and ", " & ", title_keepB)] %>%
  .[, title_keepB := gsub(", Applications", "", title_keepB)] %>%
  .[, title_keepB := gsub(", Except Special Education", "", title_keepB)] %>%
  .[, title_keepB := gsub(" & Substance Abuse Social", "", title_keepB)]

cbbPalette_d_and_n <- c("#000000", "darkorange")

summary(feols(data = df_us_oc_wide %>%
                filter(n_post_2019 > 250 & n_post_2022 > 250),
              fml = log(wfh_share_2022) ~ 1 + log(wfh_share_2019)))

p_in <- df_us_oc_wide %>%
  filter(n_post_2019 > 250 & n_post_2022 > 250)

head(p_in)

max(p_in[`D&N Classification:` != "Teleworkable"]$wfh_share_2022) # 50.71
min(p_in[`D&N Classification:` != "Teleworkable"]$wfh_share_2022) # 0
mean(p_in[`D&N Classification:` != "Teleworkable"]$wfh_share_2022) # 4.924618
sd(p_in[`D&N Classification:` != "Teleworkable"]$wfh_share_2022) # 6.757661
max(p_in[`D&N Classification:` == "Teleworkable"]$wfh_share_2022) # 73.5
min(p_in[`D&N Classification:` == "Teleworkable"]$wfh_share_2022) # 0.48
mean(p_in[`D&N Classification:` == "Teleworkable"]$wfh_share_2022) # 18.10762
sd(p_in[`D&N Classification:` == "Teleworkable"]$wfh_share_2022) # 12.32669

scaleFUN <- function(x) {paste0(sprintf('%.2f',round(x, 2)))}

summary(p_in[wfh_share_2019 > 0.1 & wfh_share_2022 > 0.1]$wfh_share_2019)
#    Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
# 0.110   0.710   2.030   3.730   4.822  40.050 
sd(p_in[wfh_share_2019 > 0.1 & wfh_share_2022 > 0.1]$wfh_share_2019)
# 4.865454
summary(p_in$wfh_share_2019)
# Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
# .000   0.550   1.720   3.502   4.660  40.050 
sd(p_in$wfh_share_2019)
# 4.798223
summary(p_in[wfh_share_2019 > 0.1 & wfh_share_2022 > 0.1]$wfh_share_2022)
# Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
# 0.14    1.89    5.74   10.58   16.48   73.50 
sd(p_in[wfh_share_2019 > 0.1 & wfh_share_2022 > 0.1]$wfh_share_2022)
# 11.39738
summary(p_in$wfh_share_2022)
# Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
# 0.000   1.560   4.840   9.998  15.270  73.500 
sd(p_in$wfh_share_2022)
# 11.29761
# Scatter plot - logscale
p = ggplot(data = p_in, aes(x = wfh_share_2019, y = wfh_share_2022,
                            color = `D&N Classification:`, shape = `D&N Classification:`)) +
  scale_color_manual(values = cbbPalette_d_and_n) +
  scale_y_continuous(trans = log_trans(), 
                     labels=scaleFUN,
                     breaks = c(0.125, 0.25, 0.5, 1, 2, 4, 8, 16, 32, 64)) +
  scale_x_continuous(trans = log_trans(),
                     labels=scaleFUN,
                     breaks = c(0.125, 0.25, 0.5, 1, 2, 4, 8, 16, 32, 64)) +
  geom_point(data = p_in[is.na(title_keepA) & is.na(title_keepB) & wfh_share_2019 > 0.1 & wfh_share_2022 > 0.1], aes(x = wfh_share_2019, y = wfh_share_2022), size = 1.5, stroke = 1, alpha = 0.3)  +
  geom_smooth(method=lm, se=FALSE, aes(group=1), colour = "blue", size = 0.8) +
  geom_point(data = p_in[!is.na(title_keepA) | !is.na(title_keepB) & wfh_share_2019 > 0.1 & wfh_share_2022 > 0.1], aes(x = wfh_share_2019, y = wfh_share_2022), size = 3, stroke = 2) +
  stat_poly_eq(aes(group=1, label=paste(..eq.label.., sep = "~~~")),geom="label",alpha=1,method = lm,label.y = log(0.425), label.x = 3.5,
               eq.with.lhs = "plain(log)(y)~`=`~",
               eq.x.rhs = "~plain(log)(italic(x))", colour = "blue", size = 4.5) +
  stat_poly_eq(aes(group=1, label=paste(..rr.label.., sep = "~~~")),geom="label",alpha=1,method = lm,label.y = log(0.23), label.x = 3.5,
               colour = "blue", size = 4.5) +
  ylab("Share (%) (2022)") +
  xlab("Share (%) (2019)") +
  #scale_y_continuous(breaks = c(0,1,2,3,4,5)) +
  #scale_x_continuous(breaks = c(0,1,2,3,4,5)) +
  coord_cartesian(ylim = c(exp(-2.1), exp(4.5)), xlim = c(exp(-2.1), exp(4.5))) +
  theme(
    #axis.title.x=element_blank(),
    legend.position="bottom",
    axis.text.x = element_text(angle = 0)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  guides(size = "none") +
  scale_shape_manual(values=c(1, 2)) +
  guides(colour = guide_legend(override.aes = list(size=3))) +
  geom_text_repel(aes(label = title_keepA),
                  fontface = "bold", size = 4, max.overlaps = 1000, point.padding = 0, box.padding = 0.5, nudge_x = -0.4, nudge_y = 0.4, force = 10, force_pull = 1,
                  bg.color = "white",
                  bg.r = 0.15, seed = 1234, show.legend = FALSE) +
  geom_text_repel(aes(label = title_keepB),
                  fontface = "bold", size = 4, max.overlaps = 1000, point.padding = 0, box.padding = 0.5, nudge_x = 0.4, nudge_y = -0.4, force = 10, force_pull = 1,
                  bg.color = "white",
                  bg.r = 0.15, seed = 1234, show.legend = FALSE) +
  theme(aspect.ratio=3/5)
p
save(p, file = "./ppt/ggplots/wfh_pre_post_by_tele.RData")
remove(p)

#### END ####




