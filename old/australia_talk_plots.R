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
# font_import()
#loadfonts(device="postscript")
#fonts()

setDTthreads(1)
getDTthreads()
#quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg_combined/")

#### OCCUPATION BREAK DOWN #### ####
remove(list = ls())
df_all <- fread(file = "./int_data/df_all_standardised.csv", nThread = 8)
df_all$month <- factor(df_all$month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))
df_all$wfh <- as.numeric(df_all$wfh_prob>0.5)
df_all$bgt_occ6 <- str_sub(df_all$bgt_occ, 1, 7)
df_all$bgt_occ2 <- as.numeric(str_sub(df_all$bgt_occ, 1, 2))
df_all$job_ymd <- ymd(df_all$job_date)
df_all$year_quarter <- as.yearqtr(df_all$job_ymd)
df_all$year_month <- as.yearmon(df_all$job_ymd)

soc2010_names <- fread(file = "./aux_data/us_soc_2010_names.csv")
soc2010_names$soc10_2d <- as.numeric(soc2010_names$soc10_2d)

nrow(df_all)
df_all <- df_all %>%
  left_join(., soc2010_names, by = c("bgt_occ2" = "soc10_2d")) %>%
  setDT(.)
nrow(df_all)
rm(soc2010_names)
df_all$name <- gsub("and", "&", df_all$name, fixed = T)
df_all$name <- gsub(" Occupations", "", df_all$name)
df_all <- df_all %>% filter(!is.na(df_all$name))
df_all <- setDT(df_all)
df_all$ussoc_2d_wn <- paste0(df_all$name)

# BAR PLOT BY COUNTRY 2019
# Plot Occupational Change
df_all_occ <- df_all %>%
  filter(year %in% c(2019, 2021, 2022)) %>%
  filter(bgt_occ2 %in% c(43, 41, 31, 29, 25, 23, 19, 17, 15, 13, 11)) %>%
  mutate(post = ifelse(year == 2019, 0, 1)) %>%
  setDT(.) %>%
  .[, .(wfh_share = sum(wfh*tot_emp_ad, na.rm = T)/sum(tot_emp_ad, na.rm = T)), by = .(ussoc_2d_wn, bgt_occ2, post, country)] %>%
  .[order(country, ussoc_2d_wn, bgt_occ2, post)]

df_all_occ <- df_all_occ %>%
  group_by(country, post) %>%
  mutate(wfh_share_norm = wfh_share/sum(wfh_share, na.rm = T))

cbbPalette <- c("#E69F00", "#009E73", "#CC79A7", "#0072B2", "#D55E00")
cbbPalette_oc <- c("#000000", "#56B4E9")
cbbPalette_ind <- c("#000000", "#F0E442")

df_all_occ$ussoc_2d_wn <- gsub("& Administrative Support", "& Admin", df_all_occ$ussoc_2d_wn)
df_all_occ$ussoc_2d_wn <- gsub("& Related", "", df_all_occ$ussoc_2d_wn)
df_all_occ$ussoc_2d_wn <- gsub("& Technical", "", df_all_occ$ussoc_2d_wn)
df_all_occ$ussoc_2d_wn <- gsub(", Training, & Library", "", df_all_occ$ussoc_2d_wn)
df_all_occ$ussoc_2d_wn <- gsub("Life, Physical, & Social ", "", df_all_occ$ussoc_2d_wn)
df_all_occ$ussoc_2d_wn <- gsub("Financial Operations", "Finance", df_all_occ$ussoc_2d_wn)
df_all_occ$ussoc_2d_wn <- gsub("Mathematical", "Math", df_all_occ$ussoc_2d_wn)
df_all_occ$ussoc_2d_wn <- gsub("Engineering", "Eng.", df_all_occ$ussoc_2d_wn)

# Raw post-pandemic RW share
p = df_all_occ %>%
  filter(bgt_occ2 %in% c(43, 41, 31, 29, 25, 23, 19, 17, 15, 13, 11)) %>%
  filter(post == 1) %>%
  ggplot(., aes(x = ussoc_2d_wn, y = wfh_share, fill = country)) +
  geom_bar(stat = "identity", width=1, position = position_dodge(width=0.8))  +
  #geom_text(aes(label = prop_growth, family = "serif"), size = 5, vjust = 0, colour = "black", hjust = -0.5) +
  ylab("Share") +
  labs(title = "WFH Share Distribution by Occupation / Year", subtitle = "Employment Weighted, Global") +
  scale_y_continuous(breaks = seq(0,4,0.05), limits = c(0,0.3)) +
  #             labels=format(df_all_occ$date_fake,
  #                           format="%Y")) +
  #             minor_breaks = as.Date(c("2019-01-01","2019-07-01","2020-01-01","2020-07-01","2021-01-01","2021-07-01", "2022-01-01")),
  #             date_labels = '%Y') +
  #scale_y_continuous(labels = scales::number_format(accuracy = 0.025),  breaks = seq(0,0.3,0.05)) +
  #coord_cartesian(ylim = c(0, 0.24)) +
  scale_fill_manual(values = cbbPalette) +
  #scale_shape_manual(values=rep(0:4, 3)) +
  theme(
    #axis.title.x=element_blank(),
    axis.title.y=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 45, hjust = 1),
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()) +
  theme(text = element_text(size=18, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  guides(fill = guide_legend(ncol = 6)) +
  #coord_flip() +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 60))
p
p_egg <- set_panel_size(p = p,
                        width = unit(8, "in"),
                        height = unit(4, "in"))
ggsave(p, filename = "./plots/occ_dist_alt_country_postcovid.pdf", width = 10, height = 9)

# Normalised
p = df_all_occ %>%
  filter(bgt_occ2 %in% c(43, 41, 31, 29, 25, 23, 19, 17, 15, 13, 11)) %>%
  filter(post == 1) %>%
  ggplot(., aes(x = ussoc_2d_wn, y = wfh_share_norm, fill = country)) +
  geom_bar(stat = "identity", width=1, position = position_dodge(width=0.8))  +
  #geom_text(aes(label = prop_growth, family = "serif"), size = 5, vjust = 0, colour = "black", hjust = -0.5) +
  ylab("Share") +
  labs(title = "WFH Share Distribution by Occupation / Year", subtitle = "Employment Weighted, Global") +
  scale_y_continuous(breaks = seq(0,4,0.05), limits = c(0,0.3)) +
  #             labels=format(df_all_occ$date_fake,
  #                           format="%Y")) +
  #             minor_breaks = as.Date(c("2019-01-01","2019-07-01","2020-01-01","2020-07-01","2021-01-01","2021-07-01", "2022-01-01")),
  #             date_labels = '%Y') +
  #scale_y_continuous(labels = scales::number_format(accuracy = 0.025),  breaks = seq(0,0.3,0.05)) +
  #coord_cartesian(ylim = c(0, 0.24)) +
  scale_fill_manual(values = cbbPalette) +
  #scale_shape_manual(values=rep(0:4, 3)) +
  theme(
    #axis.title.x=element_blank(),
    axis.title.y=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 45, hjust = 1),
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()) +
  theme(text = element_text(size=18, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  guides(fill = guide_legend(ncol = 6)) +
  #coord_flip() +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 60))
p
p_egg <- set_panel_size(p = p,
                        width = unit(8, "in"),
                        height = unit(4, "in"))
ggsave(p, filename = "./plots/occ_dist_alt_country_postcovid_norm.pdf", width = 10, height = 9)

# Raw post-pandemic RW share
p = df_all_occ %>%
  filter(bgt_occ2 %in% c(43, 41, 31, 29, 25, 23, 19, 17, 15, 13, 11)) %>%
  filter(post == 0) %>%
  ggplot(., aes(x = ussoc_2d_wn, y = wfh_share, fill = country)) +
  geom_bar(stat = "identity", width=1, position = position_dodge(width=0.8))  +
  #geom_text(aes(label = prop_growth, family = "serif"), size = 5, vjust = 0, colour = "black", hjust = -0.5) +
  ylab("Share") +
  labs(title = "WFH Share Distribution by Occupation / Year", subtitle = "Employment Weighted, Global") +
  scale_y_continuous(breaks = seq(0,4,0.05), limits = c(0,0.3)) +
  #             labels=format(df_all_occ$date_fake,
  #                           format="%Y")) +
  #             minor_breaks = as.Date(c("2019-01-01","2019-07-01","2020-01-01","2020-07-01","2021-01-01","2021-07-01", "2022-01-01")),
  #             date_labels = '%Y') +
  #scale_y_continuous(labels = scales::number_format(accuracy = 0.025),  breaks = seq(0,0.3,0.05)) +
  #coord_cartesian(ylim = c(0, 0.24)) +
  scale_fill_manual(values = cbbPalette) +
  #scale_shape_manual(values=rep(0:4, 3)) +
  theme(
    #axis.title.x=element_blank(),
    axis.title.y=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 45, hjust = 1),
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()) +
  theme(text = element_text(size=18, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  guides(fill = guide_legend(ncol = 6)) +
  #coord_flip() +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 60))
p
p_egg <- set_panel_size(p = p,
                        width = unit(8, "in"),
                        height = unit(4, "in"))
ggsave(p, filename = "./plots/occ_dist_alt_country_precovid.pdf", width = 10, height = 9)

# Normalised
p = df_all_occ %>%
  filter(bgt_occ2 %in% c(43, 41, 31, 29, 25, 23, 19, 17, 15, 13, 11)) %>%
  filter(post == 0) %>%
  ggplot(., aes(x = ussoc_2d_wn, y = wfh_share_norm, fill = country)) +
  geom_bar(stat = "identity", width=1, position = position_dodge(width=0.8))  +
  #geom_text(aes(label = prop_growth, family = "serif"), size = 5, vjust = 0, colour = "black", hjust = -0.5) +
  ylab("Share") +
  labs(title = "WFH Share Distribution by Occupation / Year", subtitle = "Employment Weighted, Global") +
  scale_y_continuous(breaks = seq(0,4,0.05), limits = c(0,0.3)) +
  #             labels=format(df_all_occ$date_fake,
  #                           format="%Y")) +
  #             minor_breaks = as.Date(c("2019-01-01","2019-07-01","2020-01-01","2020-07-01","2021-01-01","2021-07-01", "2022-01-01")),
  #             date_labels = '%Y') +
  #scale_y_continuous(labels = scales::number_format(accuracy = 0.025),  breaks = seq(0,0.3,0.05)) +
  #coord_cartesian(ylim = c(0, 0.24)) +
  scale_fill_manual(values = cbbPalette) +
  #scale_shape_manual(values=rep(0:4, 3)) +
  theme(
    #axis.title.x=element_blank(),
    axis.title.y=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 45, hjust = 1),
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()) +
  theme(text = element_text(size=18, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  guides(fill = guide_legend(ncol = 6)) +
  #coord_flip() +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 60))
p
p_egg <- set_panel_size(p = p,
                        width = unit(8, "in"),
                        height = unit(4, "in"))
ggsave(p, filename = "./plots/occ_dist_alt_country_precovid_norm.pdf", width = 10, height = 9)



# Tables Compare our Measure to D&N
check <- df_all %>%
  filter(year > 2021) %>%
  setDT(.) %>%
  .[, .(n = .N,
        wfh_share = sum(wfh*tot_emp_ad, na.rm = T)/sum(tot_emp_ad, na.rm = T),
        teleworkable_share = sum(teleworkable*tot_emp_ad, na.rm = T)/sum(tot_emp_ad, na.rm = T)),
    by = .(bgt_occ, bgt_occ_name)] %>%
  .[, diff := wfh_share - teleworkable_share] %>%
  filter(teleworkable_share == 0 | teleworkable_share == 1) %>%
  filter(n > 500) %>%
  arrange(desc(diff)) %>%
  select(-diff) %>%
  select(-n) %>%
  select(-bgt_occ) %>%
  mutate(teleworkable_share = ifelse(teleworkable_share==1, "yes", "no")) %>%
  rename("ONET Occupation" = bgt_occ_name,
         "WFH Share" = wfh_share,
         "Teleworkable" = teleworkable_share)

stargazer(head(check, 20), type = "latex", title = "Top WHAM Occupations not `Teleworkable'", summary = F, rownames = F, digits = 2, font.size = "footnotesize")
stargazer(tail(check, 20), type = "latex", title = "Top WHAM Occupations not `Teleworkable'", summary = F, rownames = F, digits = 2, font.size = "footnotesize")
# End

# Tables Compare our Measure to D&N
check <- df_all %>%
  filter(year > 2021) %>%
  setDT(.) %>%
  .[, .(n = .N,
        wfh_share = sum(wfh*tot_emp_ad, na.rm = T)/sum(tot_emp_ad, na.rm = T),
        teleworkable_share = sum(teleworkable*tot_emp_ad, na.rm = T)/sum(tot_emp_ad, na.rm = T)),
    by = .(bgt_occ, bgt_occ_name)] %>%
  mutate(teleworkable_share = as.numeric(teleworkable_share>0.5)) %>%
  filter(n > 500) %>%
  select(-n) %>%
  select(-bgt_occ) %>%
  mutate(teleworkable_share = ifelse(teleworkable_share==1, "yes", "no")) %>%
  rename("ONET Occupation" = bgt_occ_name,
         "WFH Share" = wfh_share,
         "Teleworkable" = teleworkable_share)

check$Teleworkable

stargazer(head(check %>% filter(Teleworkable == "no") %>% arrange(desc(`WFH Share`)), 20), type = "latex", title = "Top WHAM Occupations not `Teleworkable'", summary = F, rownames = F, digits = 2, font.size = "footnotesize")
stargazer(head(check %>% filter(Teleworkable == "yes") %>% arrange(`WFH Share`), 20), type = "latex", title = "Bottom WHAM Occupations which are `Teleworkable'", summary = F, rownames = F, digits = 2, font.size = "footnotesize")

# End

# Scatter Plot
df <- df_all %>% select(c("country", "onet", "teleworkable","wfh", "job_id_weight", "tot_emp_ad", "year")) %>%
  filter(country == "US" & year %in% c(2019, 2021)) %>%
  setDT(.) %>%
  .[ , .(n_post = sum(job_id_weight, na.rm = T),
         n_emp = sum(tot_emp_ad, na.rm = T),
         d_n_teleworkable = sum(teleworkable*tot_emp_ad, na.rm = T)/sum(tot_emp_ad*as.numeric(!is.na(teleworkable)), na.rm = T),
         wfh_share = sum(wfh*tot_emp_ad, na.rm = T)/(sum(tot_emp_ad, na.rm = T))), 
     by = .(onet, year)] %>%
  group_by(onet) %>%
  pivot_wider(names_from = year, values_from = c(n_post, n_emp, d_n_teleworkable, wfh_share)) %>%
  ungroup %>% mutate("D&N (2020) Teleworkable" = ifelse(d_n_teleworkable_2019>0.5, "Yes", "No"))

occupations_workathome <- read_csv("./aux_data/occupations_workathome.csv") %>% rename(onet = onetsoccode)

df <- df %>%
  left_join(occupations_workathome) %>%
  filter(!is.na(title))

df$title <- gsub("and", "&", df$title, fixed = T)
df$title <- gsub("(.*?),.*", "\\1", df$title)

df <- df %>%
  setDT(.) %>%
  .[, keep := as.numeric(max(n_post_2019, na.rm = T) == n_post_2019), by = title] %>%
  .[keep == 1] %>%
  select(-keep)

df$wfh_share_diff <- df$wfh_share_2021 - df$wfh_share_2019

library(ggrepel)
set.seed(999)

df$title_keep <- ifelse(df$title %in% c("Parking Lot Attendants", "Bakers", "Baristas", "Bartenders", "Cashiers", "Insurance Sales Agents", "Telemarketers",
                                        "Registered Nurses", "Software Developers", "Sales Representatives", "Retail Salesperson", "Secretaries & Administrative Assistants")
                        ,
                        df$title, "")
# LOG LOG
quantile(df$n_post_2019+df$n_post_2021, probs = 0.9, na.rm = T)
cbbPalette_d_and_n <- c("#FF9100", "#0800FF")
summary(lm(data = df %>%
             filter(n_post_2019 > 500 & n_post_2021 > 500) %>%
             filter(wfh_share_2019 > 0.00001 & wfh_share_2021 > 0.00001), formula = log(wfh_share_2021, base=10) ~ 1 + log(wfh_share_2019, base = 10)))
p = df %>%
  filter(n_post_2019 > 500 & n_post_2021 > 500) %>%
  filter(wfh_share_2019 > 0.00001 & wfh_share_2021 > 0.00001) %>%
  ggplot(., aes(x = wfh_share_2019, y = wfh_share_2021,
                color = `D&N (2020) Teleworkable`, shape = `D&N (2020) Teleworkable`)) +
  scale_color_manual(values = cbbPalette_d_and_n) +
  scale_y_log10(limits = c(0.0001,1)) +
  scale_x_log10(limits = c(0.0001,1)) +
  geom_abline(intercept = 0, slope = 1, size = 0.5, colour = "grey", linetype = "dashed") +
  geom_smooth(method=lm, colour = "grey", se=FALSE, aes(group=1), fullrange=TRUE) +
  geom_point(aes(size = (n_post_2019+n_post_2021)^2), stroke = 1)  +
  stat_poly_eq(aes(group=1, label=paste(..eq.label.., sep = "~~~")),geom="label",alpha=1,method = lm,label.y = log(0.0001, base = 10), label.x = log(0.247, base = 10),
               eq.with.lhs = "plain(log)(y)~`=`~",
               eq.x.rhs = "~plain(log)(italic(x))") +
  ylab("WFH Share (2021) (Logscale)") +
  xlab("WFH Share (2019) (Logscale)") +
  labs(title = "Pre- and Post-Pandemic WFH Share by ONET", subtitle = "Vacancy Weighted, USA") +
  theme(
    #axis.title.x=element_blank(),
    legend.position="bottom",
    axis.text.x = element_text(angle = 0)) +
  theme(text = element_text(size=14, family="serif"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  guides(size = "none") +
  scale_shape_manual(values=c(1, 2)) +
  geom_text_repel(aes(label = title_keep), size = 4, max.time = 4, max.overlaps = 120)
p
p_egg <- set_panel_size(p = p,
                        width = unit(6, "in"),
                        height = unit(4, "in"))
ggsave(p_egg, filename = "./plots/wfh_pre_post_by_tele.pdf", width = 8, height = 8)

system2(command = "pdfcrop", 
        args    = c("./plots/wfh_pre_post_by_tele.pdf", 
                    "./plots/wfh_pre_post_by_tele.pdf")
)

remove(list = c("p", "p_egg"))

### LEVELS
p = df %>%
  filter(n_post_2019 > 500 & n_post_2021 > 500) %>%
  ggplot(., aes(x = wfh_share_2019, y = wfh_share_2021,
                color = `D&N (2020) Teleworkable`, shape = `D&N (2020) Teleworkable`)) +
  scale_color_manual(values = cbbPalette_d_and_n) +
  geom_abline(intercept = 0, slope = 1, size = 0.5, colour = "grey", linetype = "dashed") +
  geom_smooth(method=lm, colour = "grey", se=FALSE, aes(group=1), fullrange=TRUE) +
  geom_point(aes(size = (n_post_2019+n_post_2021)^2), stroke = 1)  +
  stat_poly_eq(aes(group=1, label=paste(..eq.label.., sep = "~~~")),geom="label",alpha=1,method="lm",label.y = 0, label.x = 0.247,
               eq.with.lhs = "y~`=`~",
               eq.x.rhs = "~italic(x)") +
  ylab("WFH Share (2021)") +
  xlab("WFH Share (2019)") +
  scale_x_continuous(breaks = seq(0,1,0.1)) +
  scale_y_continuous(breaks = seq(0,1,0.1)) +
  labs(title = "Pre- and Post-Pandemic WFH Share by ONET", subtitle = "Vacancy Weighted, USA") +
  theme(
    #axis.title.x=element_blank(),
    legend.position="bottom",
    axis.text.x = element_text(angle = 0)) +
  theme(text = element_text(size=14, family="serif"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  guides(size = "none") +
  scale_shape_manual(values=c(1, 2)) +
  geom_text_repel(aes(label = title_keep), size = 4, max.time = 4, max.overlaps = 120)
p
p_egg <- set_panel_size(p = p,
                        width = unit(6, "in"),
                        height = unit(4, "in"))
ggsave(p_egg, filename = "./plots/level_wfh_pre_post_by_tele.pdf", width = 8, height = 8)

system2(command = "pdfcrop", 
        args    = c("./plots/level_wfh_pre_post_by_tele.pdf", 
                    "./plots/level_wfh_pre_post_by_tele.pdf")
)

remove(list = c("p", "p_egg"))

df$wfh_share_diff <- df$wfh_share_2021 - df$wfh_share_2019

p = df %>%
  filter(n_post_2019 > 300 & n_post_2021 > 300) %>%
  ggplot(., aes(x = wfh_share_2019, y = wfh_share_diff, label = title_keep,
                colour = `D&N (2020) Teleworkable`, shape = `D&N (2020) Teleworkable`)) +
  scale_color_manual(values = cbbPalette_d_and_n) +
  geom_point(aes(size = (n_post_2019+n_post_2021)^2), stroke = 1)  +
  ylab("WFH Share Level Change (2021 vs 2019)") +
  xlab("WFH Share (2019)") +
  labs(title = "Pre- and Post-Pandemic WFH Share by ONET", subtitle = "Vacancy Weighted, USA") +
  theme(
    #axis.title.x=element_blank(),
    legend.position="bottom",
    axis.text.x = element_text(angle = 0)) +
  theme(text = element_text(size=18, family="serif"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  guides(size = "none",
         colour = guide_legend(ncol = 3)) +
  scale_shape_manual(values=c(1, 2)) +
  geom_text_repel(aes(label = title_keep), size = 4, max.time = 4, max.overlaps = 120)
p
p_egg <- set_panel_size(p = p,
                        width = unit(6, "in"),
                        height = unit(4, "in"))
ggsave(p_egg, filename = "./plots/level_wfh_pre_growth_by_tele.pdf", width = 8, height = 8)

system2(command = "pdfcrop", 
        args    = c("./plots/level_wfh_pre_growth_by_tele.pdf", 
                    "./plots/level_wfh_pre_growth_by_tele.pdf")
)

remove(list = c("p", "p_egg"))







## Distributions 2021 vs 2019
library(RColorBrewer)
mycolors <- colorRampPalette(brewer.pal(8, "Set2"))(22)

p = df_all_occ %>%
  #filter(ussoc_2d_wn < 27) %>%
  filter(year == 2019 | year == 2021) %>%
  mutate(ussoc_2d_wn = fct_reorder(ussoc_2d_wn, wfh_share, .desc = FALSE)) %>%
  ggplot(., aes(x = as.factor(year), y = wfh_share, fill = ussoc_2d_wn)) +
  geom_bar(stat = "identity", position = position_dodge(), alpha = 0.9)  +
  ylab("Share") +
  xlab("Date") +
  labs(title = "WFH Share Distribution by Occupation / Year", subtitle = "Employment Weighted, Global") +
  scale_y_continuous(breaks = seq(0,3,0.05)) +
  #             labels=format(df_all_occ$date_fake,
  #                           format="%Y")) +
  #             minor_breaks = as.Date(c("2019-01-01","2019-07-01","2020-01-01","2020-07-01","2021-01-01","2021-07-01", "2022-01-01")),
  #             date_labels = '%Y') +
  #scale_y_continuous(labels = scales::number_format(accuracy = 0.025),  breaks = seq(0,0.3,0.05)) +
  #coord_cartesian(ylim = c(0, 0.24)) +
  scale_fill_manual(values = mycolors) +
  #scale_shape_manual(values=rep(0:4, 3)) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 0)) +
  theme(text = element_text(size=18, family="serif"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  guides(fill = guide_legend(ncol = 3))
p
p_egg <- set_panel_size(p = p,
                        width = unit(12, "in"),
                        height = unit(4, "in"))
ggsave(p_egg, filename = "./plots/occ_dist.pdf", width = 14, height = 8)

system2(command = "pdfcrop", 
        args    = c("./plots/occ_dist.pdf", 
                    "./plots/occ_dist.pdf")
)



#### END ####



#### OCC FES BY COUNTRY #### ####
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

df_all$is_aus <- ifelse(df_all$country == "Australia", 1, 0)

table(df_all$state)

df_all$is_vic <- ifelse(df_all$country == "Australia" & df_all$state == "VIC", 1, 0)

df_all$post_covid <- ifelse(df_all$year >= 2021, 1, 0)

colnames(df_all)

table(df_all$country)

df_all[year %in% c(2019,2021,2022) & country == "AUSTRALIA" & state != "OT"]

# Basic correlations
mod1 <- feols(fml = wfh ~ i(state, post_covid),
              data = df_all[year %in% c(2019,2021,2022) & country == "AUSTRALIA" & state != "Ot"],
              #weights = ~ tot_emp_ad,
              fixef.rm = "both",
              nthreads = 8,
              verbose = 10, 
              lean = T,
              cluster = ~ year_month^state
)

mod2 <- feols(fml = wfh ~ i(state, post_covid) | bgt_occ2^post_covid,
              data = df_all[year %in% c(2019,2021,2022) & country == "AUSTRALIA" & state != "Ot"],
              #weights = ~ tot_emp_ad,
              fixef.rm = "both",
              nthreads = 8,
              verbose = 10, 
              lean = T,
              cluster = ~ year_month^state
)

mod3 <- feols(fml = wfh ~ i(state, post_covid) | bgt_occ6^post_covid,
              data = df_all[year %in% c(2019,2021,2022) & country == "AUSTRALIA" & state != "Ot"],
              #weights = ~ tot_emp_ad,
              fixef.rm = "both",
              nthreads = 8,
              verbose = 10, 
              lean = T,
              cluster = ~ year_month^state
)

mod4 <- feols(fml = wfh ~ i(state, post_covid) | employer,
              data = df_all[year %in% c(2019,2021,2022) & country == "AUSTRALIA" & state != "Ot"],
              #weights = ~ tot_emp_ad,
              fixef.rm = "both",
              nthreads = 8,
              verbose = 10,
              lean = T,
              cluster = ~ year_month^state
)

etable(mod1, mod2, mod3, mod4)

#### END ####