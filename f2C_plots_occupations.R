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
# font_import()
#loadfonts(device="postscript")
#fonts()

setDTthreads(4)
getDTthreads()
#quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg_combined/")

#### END ####

#### LOAD DATA ####
colnames(fread("../bg-us/int_data/us_stru_2019_wfh.csv", nrows = 100))

df_us_2019 <- fread("../bg-us/int_data/us_stru_2019_wfh.csv", nThread = 4, integer64 = "numeric", select = c("job_id","job_date","soc","onet","year_quarter","wfh_wham","job_domain","job_url"))
df_us_2021 <- fread("../bg-us/int_data/us_stru_2021_wfh.csv", nThread = 4, integer64 = "numeric", select = c("job_id","job_date","soc","onet","year_quarter","wfh_wham","job_domain","job_url"))
df_us_2022 <- fread("../bg-us/int_data/us_stru_2022_wfh.csv", nThread = 4, integer64 = "numeric", select = c("job_id","job_date","soc","onet","year_quarter","wfh_wham","job_domain","job_url"))

df_us <- rbindlist(list(df_us_2019,df_us_2021,df_us_2022))
ls()
remove(list = c("df_us_2019","df_us_2021","df_us_2022"))
df_us <- df_us %>% .[!grepl("careerbuilder", job_url)]
nrow(df_us)
df_us <- df_us %>% .[!(onet %in% c("19-2099.01","19-4099.03"))]
nrow(df_us)


#### END ####
remove(list = setdiff(ls(), "df_us"))

df_us$year_quarter <- as.yearqtr(df_us$job_date)

colnames(df_us)

df_us_oc <- df_us %>%
  .[year_quarter %in% as.yearqtr(c("2019 Q1", "2019 Q2", "2019 Q3", "2019 Q4", "2021 Q3", "2021 Q4", "2022 Q1", "2022 Q2"))] %>%
  .[!is.na(soc) & soc != ""] %>%
  .[!is.na(wfh_wham) & wfh_wham != ""] %>%
  .[, soc2 := str_sub(soc, 1, 2)] %>%
  .[, year := year(job_date)] %>%
  .[, year := ifelse(year == 2021, 2022, year)] %>%
  setDT(.) %>%
  select(year, wfh_wham, soc2) %>%
  setDT(.) %>%
  .[, .(wfh_share = mean(wfh_wham)),
    by = .(year, soc2)] %>%
  setDT(.)

soc2010_names <- fread(file = "./aux_data/us_soc_2010_names.csv")
soc2010_names
soc2010_names$soc10_2d <- as.numeric(soc2010_names$soc10_2d)
df_us_oc$soc2 <- as.numeric(df_us_oc$soc2)
nrow(df_us_oc)
df_us_oc <- df_us_oc %>%
  left_join(., soc2010_names, by = c("soc2" = "soc10_2d")) %>%
  setDT(.)
nrow(df_us_oc)
rm(soc2010_names)
df_us_oc$name <- gsub("and", "&", df_us_oc$name, fixed = T)
df_us_oc$name <- gsub(" Occupations", "", df_us_oc$name)
df_us_oc$name <- gsub(", Sports, & Media| & Technical|, & Repair|Cleaning &|& Serving Related", "", df_us_oc$name)
df_us_oc <- df_us_oc %>% filter(!is.na(df_us_oc$name))
df_us_oc <- setDT(df_us_oc)
df_us_oc$ussoc_2d_wn <- paste0(df_us_oc$name)

# df_us_top <- df_us[onet %in% c("19-2099.01","19-4099.03","15-2021.00","27-3043.05","15-2041.02")]
# fwrite(df_us_top, "./aux_data/check_top_5_onets.csv")

#### BAR PLOT 2021 vs 2019 ####
df_us_oc <- df_us_oc %>%
  group_by(ussoc_2d_wn) %>%
  mutate(prop_growth = ifelse(!is.na(lag(wfh_share)), paste0(round((wfh_share)/lag(wfh_share),1),"X"), NA)) %>%
  ungroup() %>%
  setDT(.)

df_us_oc

df_us_oc$wfh_share_index <- 100*df_us_oc$wfh_share_index

cbbPalette <- c("#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00")
cbbPalette_oc <- c("#000000", "darkorange")
cbbPalette_ind <- c("#000000", "#F0E442")

df_us_oc$prop_growth[is.na(df_us_oc$prop_growth)] <- ""

df_us_oc <- df_us_oc %>%
  mutate(year = ifelse(year == 2019, "2019", "2021-22"))

p = df_us_oc %>%
  #filter(ussoc_2d_wn < 27) %>%
  mutate(ussoc_2d_wn = fct_reorder(ussoc_2d_wn, 100*wfh_share, .desc = FALSE)) %>%
  ggplot(., aes(x = ussoc_2d_wn, y = 100*wfh_share, fill = as.factor(year))) +
  geom_bar(stat = "identity", width=1, position = position_dodge(width=0.8))  +
  geom_text(aes(label = prop_growth, family = "serif"), size = 5, vjust = 0, colour = "black", hjust = -0.5) +
  ylab("Share (%)") +
  labs(title = "WFH Share Distribution by Occupation / Year", subtitle = "Employment Weighted, Global") +
  scale_y_continuous(breaks = seq(0,100,5), limits = c(0, 35)) +
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
        axis.text.y = element_text(hjust=0),
        legend.position = c(0.65, 0.05)) +
  guides(fill = guide_legend(ncol = 2)) +
  coord_flip() +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 60))

p
p_egg <- set_panel_size(p = p,
                        width = unit(3.5, "in"),
                        height = unit(22*0.65, "cm"))
ggsave(p_egg, filename = "./plots/occ_dist_alt.pdf", width = 9, height = 22*0.65+3)

#### END ####

#### OCCUPATION SCATTER PLOTS ####
# Prepare Data
remove(list = setdiff(ls(), "df_us"))

df_us_oc <- df_us %>%
  .[year_quarter %in% as.yearqtr(c("2019 Q1", "2019 Q2", "2019 Q3", "2019 Q4", "2021 Q3", "2021 Q4", "2022 Q1", "2022 Q2"))] %>%
  .[!is.na(onet) & onet != ""] %>%
  .[!is.na(wfh_wham) & wfh_wham != ""] %>%
  .[, year := year(job_date)] %>%
  .[, year := ifelse(year == 2021, 2022, year)] %>%
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
sum(df_us_oc[year == 2022][!(df_us_oc[year == 2022]$onet %in% occupations_workathome$onet)]$share) # 5.6% of vacs not matches - who cares!

df_us_oc <- df_us_oc %>%
  left_join(occupations_workathome)

df_us_oc <- df_us_oc %>% .[!is.na(teleworkable)]

remove(list = setdiff(ls(), c("df_us", "df_us_oc")))

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
                        "Loan Counselors", "Gas Plant Operators"),
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

max(p_in[`D&N Classification:` != "Teleworkable"]$wfh_share_2022)
min(p_in[`D&N Classification:` != "Teleworkable"]$wfh_share_2022)
mean(p_in[`D&N Classification:` != "Teleworkable"]$wfh_share_2022)
sd(p_in[`D&N Classification:` != "Teleworkable"]$wfh_share_2022)
max(p_in[`D&N Classification:` == "Teleworkable"]$wfh_share_2022)
min(p_in[`D&N Classification:` == "Teleworkable"]$wfh_share_2022)
mean(p_in[`D&N Classification:` == "Teleworkable"]$wfh_share_2022)
sd(p_in[`D&N Classification:` == "Teleworkable"]$wfh_share_2022)

scaleFUN <- function(x) {paste0(sprintf('%.2f',round(x, 2)), "%")}

# Scatter plot - logscale
p = ggplot(data = p_in, aes(x = wfh_share_2019, y = wfh_share_2022,
                color = `D&N Classification:`, shape = `D&N Classification:`)) +
  scale_color_manual(values = cbbPalette_d_and_n) +
  scale_y_continuous(trans = log_trans(), 
                     labels=scaleFUN,
                     breaks = c(0.25, 0.5, 1, 2, 4, 8, 16, 32, 64)) +
  scale_x_continuous(trans = log_trans(),
                     labels=scaleFUN,
                     breaks = c(0.25, 0.5, 1, 2, 4, 8, 16, 32, 64)) +
  geom_point(data = p_in[is.na(title_keepA) & is.na(title_keepB)], aes(x = wfh_share_2019, y = wfh_share_2022), size = 1.5, stroke = 1, alpha = 0.3)  +
  geom_smooth(method=lm, se=FALSE, aes(group=1), colour = "blue", size = 0.8) +
  geom_point(data = p_in[!is.na(title_keepA) | !is.na(title_keepB)], aes(x = wfh_share_2019, y = wfh_share_2022), size = 3, stroke = 2) +
  stat_poly_eq(aes(group=1, label=paste(..eq.label.., sep = "~~~")),geom="label",alpha=1,method = lm,label.y = 0, label.x = 3.5,
               eq.with.lhs = "plain(log)(y)~`=`~",
               eq.x.rhs = "~plain(log)(italic(x))", colour = "blue", size = 4.5) +
  stat_poly_eq(aes(group=1, label=paste(..rr.label.., sep = "~~~")),geom="label",alpha=1,method = lm,label.y = -0.6, label.x = 3.5,
               colour = "blue", size = 4.5) +
  ylab("Share (%) (2021-22) (Logscale)") +
  xlab("Share (%) (2019)  (Logscale)") +
  #scale_y_continuous(breaks = c(0,1,2,3,4,5)) +
  #scale_x_continuous(breaks = c(0,1,2,3,4,5)) +
  coord_cartesian(ylim = c(exp(-1.5), exp(4.5)), xlim = c(exp(-1.5), exp(4.5))) +
  labs(title = "Pre- and Post-Pandemic WFH Share by ONET",
       subtitle = paste0("Vacancy Weighted, USA. ",
       "2019/2022 Mean (SD) = ", round(mean(p_in$wfh_share_2019),3), "(", round(sd(p_in$wfh_share_2019),3), ") / ",
       round(mean(p_in$wfh_share_2022),3), "(", round(sd(p_in$wfh_share_2022),3), "). ")) +
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
                  bg.r = 0.15, seed = 1234, show.legend = FALSE)
  
p

p_egg <- set_panel_size(p = p,
                        width = unit(6, "in"),
                        height = unit(4, "in"))
ggsave(p_egg, filename = "./plots/wfh_pre_post_by_tele.pdf", width = 9, height = 7)

remove(list = c("p", "p_egg"))

# Scatter Plot (Levels)
p = ggplot(data = p_in, aes(x = wfh_share_2019, y = wfh_share_2022,
                            color = `D&N Classification:`, shape = `D&N Classification:`)) +
  scale_color_manual(values = cbbPalette_d_and_n) +
  #geom_abline(intercept = 0, slope = 1, size = 0.5, colour = "grey", linetype = "dashed") +
  geom_point(data = p_in[title_keep == ""], aes(x = wfh_share_2019, y = wfh_share_2022), size = 1.5, stroke = 1, alpha = 0.5)  +
  geom_smooth(method=lm, se=FALSE, aes(group=1), colour = "blue", size = 0.8) +
  geom_point(data = p_in[title_keep != ""], aes(x = wfh_share_2019, y = wfh_share_2022), size = 3, stroke = 2)  +
  stat_poly_eq(aes(group=1, label=paste(..eq.label.., sep = "~~~")),geom="label",alpha=1,method = lm,label.y = 13, label.x = 30,
               eq.with.lhs = "(y)~`=`~",
               eq.x.rhs = "~(italic(x))", colour = "blue", size = 4.5) +
  stat_poly_eq(aes(group=1, label=paste(..rr.label.., sep = "~~~")),geom="label",alpha=1,method = lm,label.y = 5, label.x = 30,
               eq.with.lhs = "(y)~`=`~",
               eq.x.rhs = "~(italic(x))", colour = "blue", size = 4.5) +
  ylab("Share (%) (2021-22)") +
  xlab("Share (%) (2019)") +
  scale_y_continuous(breaks = c(0,10,20,30,40,50,60,70,80,90,100)) +
  scale_x_continuous(breaks = c(0,10,20,30,40,50,60,70,80,90,100)) +
  coord_cartesian(ylim = c(0, 55), xlim = c(0, 35)) +
  labs(title = "Pre- and Post-Pandemic WFH Share by ONET", subtitle = "Vacancy Weighted, USA") +
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
  geom_text_repel(aes(label = title_keep),
                  fontface = "bold", size = 3.5, max.overlaps = 1000, point.padding = 0, box.padding = 0.5, min.segment.length = 0, force = 2,
                  bg.color = "white", nudge_y = 1,
                  bg.r = 0.15, seed = 1548, show.legend = FALSE)

p
p_egg <- set_panel_size(p = p,
                        width = unit(6, "in"),
                        height = unit(4, "in"))
ggsave(p_egg, filename = "./plots/level_wfh_pre_post_by_tele.pdf", width = 8, height = 6)

dist_feats <- df %>%
  filter(n_post_2019 > 250)
mean(dist_feats$wfh_share_2019) # 0.03647199
sd(dist_feats$wfh_share_2019) # 0.0487183
mean(dist_feats$wfh_share_2022) # 0.09670947
sd(dist_feats$wfh_share_2022) # 0.1033183

#### END ####




