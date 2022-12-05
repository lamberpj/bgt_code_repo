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
library(ggrepel)
# system("/mnt/disks/pdisk/bgt_code_repo")
# system("git commit -a --allow-empty-message -m ''")
# system("git push origin master")

setDTthreads(2)
getDTthreads()
#quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg_combined/")

df_us_2022 <- fread("../bg-us/int_data/df_us_2022_standardised.csv", nThread = 16)
firm_audit <- fread("./aux_data/firm_audit_direct_comp_PL.csv") %>%
  mutate(wfh_share = as.numeric(n_remote) / as.numeric(n_total)) %>%
  mutate(date = dmy(date)) %>%
  select(employer, date, n_remote, n_total)


colnames(df_us_2022_audit)

df_us_2022_audit <- df_us_2022 %>%
  .[employer %in% firm_audit$employer] %>%
  setDT(.) %>%
  .[as.character(as.yearmon(year_month)) == "Jun 2022"] %>%
  #.[job_date <= ymd("2022-06-12")] %>%
  .[, .(n_wfh_bgt = sum(job_id_weight*wfh_wham, na.rm = T),
        n_total_bgt = sum(job_id_weight, na.rm = T)), by = .(employer, disjoint_sector)] %>%
  left_join(firm_audit) %>%
  setDT(.) %>%
  .[, n_total_clean := ifelse(grepl(">", n_total), NA, as.numeric(gsub("[^[:digit:]]", "", n_total)))] %>%
  .[, n_remote_clean := as.numeric(gsub("[^[:digit:]]", "", n_remote))] %>%
  .[, wfh_share_bgt := n_wfh_bgt/n_total_bgt] %>%
  .[, wfh_share_audit := n_remote_clean/n_total_clean] %>%
  .[, adiff := abs(wfh_share_bgt - wfh_share_audit)] %>%
  .[!is.na(adiff)] %>%
  .[, min_adiff := as.numeric(adiff == min(adiff)), by = employer] %>%
  .[min_adiff == 1]

feols(data = df_us_2022_audit, fml = log(wfh_share_audit) ~ log(wfh_share_bgt), weights = ~ n_total_bgt) %>% etable()
feols(data = df_us_2022_audit, fml = wfh_share_audit ~ wfh_share_bgt) %>% etable()

p = ggplot(data = df_us_2022_audit, aes(y = wfh_share_audit, x = wfh_share_bgt))

p = df_us_2022_audit %>%
  ggplot(., aes(x = 100*wfh_share_bgt, y = 100*wfh_share_audit, colour = disjoint_sector)) +
  scale_x_continuous(breaks = seq(0, 100, 10)) +
  scale_y_continuous(breaks = seq(0, 100, 10)) +
  # scale_x_log10(limits = c(0.01,100)) +
  geom_abline(intercept = 0, slope = 1, size = 0.5, colour = "grey", linetype = "dashed") +
  geom_smooth(method=lm, colour = "grey", se=FALSE, aes(group=1), fullrange=TRUE) +
  geom_point(aes(size = n_total_bgt))  +
  stat_poly_eq(size = 5, aes(group=1, label=paste(..eq.label.., sep = "~~~")),geom="label",alpha=1,method = lm, label.y = 5, label.x = 50,
               eq.with.lhs = "y~`=`~",
               eq.x.rhs = "~italic(x)") +
  #stat_poly_eq(aes(group=1, label=paste(..rr.label.., sep = "~~~")),geom="label",alpha=1,method = lm,#label.y = log(0.013, base = 10), label.x = log(18, base = 10),
  #             ) +
  ylab("RW Share (Website Audit) (%)") +
  xlab("RW Share (WHAM) (%)") +
  labs(title = "WHAM Measure vs Firms' Website Audit", subtitle = "") +
  theme(
    #axis.title.x=element_blank(),
    legend.position="bottom",
    axis.text.x = element_text(angle = 0)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        panel.background = element_rect(fill = "white"),
        legend.position="none") +
  guides(size = "none") +
  geom_text_repel(aes(label = gsub(" ", "\n", employer)), size = 4)
p
p_egg <- set_panel_size(p = p,
                        width = unit(5, "in"),
                        height = unit(3, "in"))
ggsave(p_egg, filename = "./plots/wham_vs_audit.pdf", width = 8, height = 6)

remove(list = c("p", "p_egg"))

















