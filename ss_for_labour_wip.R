#### SETUP ####
remove(list = ls())

options(repos = c(CRAN = "https://mran.revolutionanalytics.com/snapshot/2021-09-01"))

options(scipen=999)

#install.packages("corpus")
library("data.table")
library("tidyverse")
library("janitor")
library("lubridate")
#library("dplyr")
#library("stringr")
library("doParallel")
library("textclean")
library("quanteda")
#library("readtext")
#library("rvest")
library("xml2")
#library("DescTools")
library("zoo")
#library("lsa")
#library("fuzzyjoin")
library("quanteda")
library("refinr")
#library("FactoMineR")
#library("ggpubr")
#library("scales")
#install.packages("ggthemr")
#ggthemr('flat')
library("fixest")
#library("lfe")
library("stargazer")
#library("texreg")
#library("sjPlot")
#library("margins")
#library("DescTools")
#library("fuzzyjoin")
library("readxl")
# Translation packages
#library("rvest")
#library("googleLanguageR")
#library("cld2")
#library("datasets")
#library("vroom")
library("Hmisc")

library(ggplot2)
library(scales)
library("ggpubr")
#library("devtools")
#devtools::install_github('Mikata-Project/ggthemr')
library("ggthemr")
ggthemr('flat')
library("stats")

quanteda_options(threads = 16)
setwd("/mnt/disks/pdisk/bg-uk/")
setDTthreads(1)

remove(list = ls())

#### LAOD DATA ####
remove(list = ls())
setwd("/mnt/disks/pdisk/bg-us/")

df_stru_us_sum <- fread("./int_data/df_stru.csv", select = c("job_date","bgt_occ","state", "county", "wfh_nn","wfh_w_nn"), nThread = 6) %>%
  mutate(year_quarter = as.yearqtr(ymd(job_date))) %>%
  mutate(soc5_us = str_sub(bgt_occ, 1, 6)) %>%
  group_by(year_quarter, soc5_us, state, county) %>%
  summarise(n = n(),
            wfh_nn = sum(wfh_nn)/n(),
            wfh_w_nn = sum(wfh_w_nn)/n())
df_stru_us_sum <- df_stru_us_sum %>% rename(region = state)
df_stru_us_sum <- df_stru_us_sum %>% mutate(source = "USA")

setwd("/mnt/disks/pdisk/bg-uk/")
colnames(fread("./int_data/df_stru.csv", nrow = 100))
df_stru_uk_sum <- fread("./int_data/df_stru.csv", select = c("job_date","bgt_occ","nation", "canon_county", "wfh_nn","wfh_w_nn"), nThread = 6) %>%
  rename(county = canon_county) %>%
  mutate(year_quarter = as.yearqtr(ymd(job_date))) %>%
  mutate(soc5_us = str_sub(bgt_occ, 1, 6)) %>%
  group_by(year_quarter, soc5_us, nation, county) %>%
  summarise(n = n(),
            wfh_nn = sum(wfh_nn)/n(),
            wfh_w_nn = sum(wfh_w_nn)/n())
df_stru_uk_sum <- df_stru_uk_sum %>% rename(region = nation)
df_stru_uk_sum <- df_stru_uk_sum %>% mutate(source = "UK")

df_stru_sum <- bind_rows(df_stru_uk_sum, df_stru_us_sum)

df_stru_sum <- df_stru_sum %>% mutate(year = as.numeric(str_sub(year_quarter, 1, 4)))

#### UK vs USA ####
df_stru_sum_ag <- df_stru_sum %>%
  group_by(year_quarter, source) %>%
  summarise(wfh_nn_uw = sum(wfh_nn*(n/sum(n, na.rm = T)), na.rm = T))

p = df_stru_sum_ag %>%
  ggplot(data = ., aes(x = as.Date(as.yearqtr(year_quarter)), y = wfh_nn_uw, colour = source)) +
  geom_line(size = 1.5) +
  geom_point(size = 2) +
  ggtitle("WFH Share") +
  ylab("WFH Share") +
  scale_x_date(date_breaks = "6 months", date_labels = "%b%y") +
  scale_y_continuous(breaks = seq(0,0.14,0.02), limits = c(0,0.14)) +
  #coord_cartesian(ylim = c(0, 0.195)) +
  scale_colour_ggthemr_d() +
  #labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 45, vjust = 1.25, hjust=1.1, size = 16),
    legend.key.width=unit(2.5,"cm"),
    legend.text=element_text(size=14),
    axis.text = element_text(size = 16),
    text = element_text(size = 16),
    plot.title = element_text(hjust = 0.5, size = 16))
p
ggexport(p, filename = "./plots/wfh_ts_by_country.pdf")

#### HYSTERISIS COUNTY ####

summary(df_stru_sum_ag$wfh_nn_uw)

df_stru_sum_ag <- df_stru_sum %>%
  mutate(wfh_nn = wfh_nn*n) %>%
  group_by(year, county, source) %>%
  summarise(n = sum(n),
            wfh_nn_uw = sum(wfh_nn, na.rm = T)/sum(n, na.rm = T)) %>%
  mutate(ln_wfh_nn_uw = log(wfh_nn_uw+0.001)) %>%
  ungroup

summary(df_stru_sum_ag$wfh_nn_uw)

df_stru_sum_ag <- df_stru_sum_ag %>% filter(year %in% c(2019,2021))%>%
  group_by(source, county) %>%
  pivot_wider(., names_from = year, values_from = c(wfh_nn_uw, ln_wfh_nn_uw, n))

df_stru_sum_ag <- df_stru_sum_ag %>% mutate(n = 0.5*(n_2019+n_2021)) %>%
  filter(n > 500 & wfh_nn_uw_2021 > 0 & wfh_nn_uw_2019>0)

quantile(df_stru_sum_ag$n, probs = seq(0, 1, by = .05))

deciles(df_stru_sum_ag$n)
df_stru_sum_ag$labels <- ifelse(30249.85 < df_stru_sum_ag$n, df_stru_sum_ag$county, NA)

View(df_stru_sum_ag)

p = df_stru_sum_ag %>% filter(n > 100 & wfh_nn_uw_2021 > 0 & wfh_nn_uw_2019>0) %>%
  ggplot(data = ., aes(x = wfh_nn_uw_2019, y = wfh_nn_uw_2021)) +
  geom_abline(intercept = 0, slope = 1, colour = "black", alpha = 0.6) +
  geom_smooth(aes(weight = n), alpha = 0.05, method=lm, color = "darkgrey", se = F) +
  geom_point(aes(size = n^2, colour = source), alpha = 0.6) +
  #ggrepel::geom_text_repel(aes(label=labels, size = n+5), colour = "black", max.overlaps = 10, segment.color = 'transparent') +
  ggtitle("WFH Share 2021 vs 2019 by County") +
  xlab("WFH Share 2019") +
  ylab("WFH Share 2021") +
  scale_x_continuous(limits = c(0, 0.1), breaks = seq(0, 0.1, 0.025)) +
  scale_y_continuous(limits = c(0, 0.2), breaks = seq(0, 0.2, 0.025)) +
  scale_colour_manual(values=rep(brewer.pal(8,"Dark2"),times=200)) +
  theme(
    legend.position="bottom",
    legend.title = element_blank(),
    text = element_text(size = 16))+
  guides(size=FALSE)
p

ggexport(p, filename = "./plots/sp_pre_post_county.pdf")

#### HYSTERISIS OCCUPATION ####

summary(df_stru_sum_ag$wfh_nn_uw)

df_stru_sum_ag <- df_stru_sum %>%
  mutate(wfh_nn = wfh_nn*n) %>%
  group_by(year, soc5_us, source) %>%
  summarise(n = sum(n),
            wfh_nn_uw = sum(wfh_nn, na.rm = T)/sum(n, na.rm = T)) %>%
  mutate(ln_wfh_nn_uw = log(wfh_nn_uw+0.001)) %>%
  ungroup

summary(df_stru_sum_ag$wfh_nn_uw)

df_stru_sum_ag <- df_stru_sum_ag %>% filter(year %in% c(2019,2021))%>%
  group_by(source, soc5_us) %>%
  pivot_wider(., names_from = year, values_from = c(wfh_nn_uw, ln_wfh_nn_uw, n))

df_stru_sum_ag <- df_stru_sum_ag %>% mutate(n = 0.5*(n_2019+n_2021)) %>%
  filter(n > 10 & wfh_nn_uw_2021 > 0 & wfh_nn_uw_2019>0)

p = df_stru_sum_ag %>% filter(n > 100 & wfh_nn_uw_2021 > 0 & wfh_nn_uw_2019>0) %>%
  ggplot(data = ., aes(x = wfh_nn_uw_2019, y = wfh_nn_uw_2021)) +
  geom_abline(intercept = 0, slope = 1, colour = "black", alpha = 0.6) +
  geom_smooth(aes(weight = n), alpha = 0.05, method=lm, color = "darkgrey", se = F) +
  geom_point(aes(size = n^2+5, colour = source), alpha = 0.6) +
  #ggrepel::geom_text_repel(aes(label=labels, size = n+5), colour = "black", max.overlaps = 10, segment.color = 'transparent') +
  ggtitle("WFH Share 2021 vs 2019 by Occupation") +
  xlab("WFH Share 2019") +
  ylab("WFH Share 2021") +
  scale_x_continuous(limits = c(0, 0.1), breaks = seq(0, 0.1, 0.025)) +
  scale_y_continuous(limits = c(0, 0.2), breaks = seq(0, 0.2, 0.025)) +
  scale_colour_manual(values=rep(brewer.pal(8,"Dark2"),times=200)) +
  theme(
    legend.position="bottom",
    legend.title = element_blank(),
    text = element_text(size = 16))+
  guides(size=FALSE)
p

ggexport(p, filename = "./plots/sp_pre_post_occupation.pdf")

    

p
    #scale_y_continuous(trans = log_trans()) +
    #scale_x_continuous(trans = log_trans()) +
    
p
  
ggexport(p, filename = "./plots/wfh_ts_by_country.pdf")

#### STATIONARY WEIGHTS ####

df_stru_sum <- df_stru_sum %>%
  group_by(soc5_us, county) %>%
  mutate(n_2019 = mean(n[year == 2019], na.rm = T)) %>%
  mutate(n_2021 = mean(n[year == 2021], na.rm = T))

df_stru_sum_ag <- df_stru_sum %>%
  group_by(year_quarter) %>%
  summarise(wfh_nn_uw = sum(wfh_nn*(n/sum(n, na.rm = T)), na.rm = T),
            wfh_nn_2019w = sum(wfh_nn*(n_2019/sum(n_2019, na.rm = T)), na.rm = T),
            wfh_nn_2021w = sum(wfh_nn*(n_2021/sum(n_2019, na.rm = T)), na.rm = T))

df_stru_sum_ag <- df_stru_sum_ag %>%
  group_by(year_quarter) %>%
  pivot_longer(., cols = c(wfh_nn_uw, wfh_nn_2019w, wfh_nn_2021w))
head(df_stru_sum_ag)
df_stru_sum_ag <- df_stru_sum_ag %>%
  mutate(name = case_when(
    name == "wfh_nn_uw" ~ "Unweighted",
    name == "wfh_nn_2019w" ~ "2019 Weights",
    name == "wfh_nn_2021w" ~ "2021 Weights"
  ))

head(df_stru_sum_ag)

p = df_stru_sum_ag %>%
  ggplot(data = ., aes(x = as.Date(as.yearqtr(year_quarter)), y = value, colour = name)) +
  geom_line(size = 1.5) +
  geom_point(size = 2) +
  ggtitle("WFH Share by Employment Weights") +
  ylab("WFH Share") +
  scale_x_date(date_breaks = "6 months", date_labels = "%b%y") +
  scale_y_continuous(breaks = seq(0,0.1,0.02), limits = c(0,0.10)) +
  #coord_cartesian(ylim = c(0, 0.195)) +
  scale_colour_ggthemr_d() +
  #labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 45, vjust = 1.25, hjust=1.1, size = 16),
    legend.key.width=unit(2.5,"cm"),
    legend.text=element_text(size=14),
    axis.text = element_text(size = 16),
    text = element_text(size = 16),
    plot.title = element_text(hjust = 0.5, size = 16)) +
  guides(col = guide_legend(nrow = 2))

ggexport(p, filename = "./plots/wfh_ts_weights.pdf")

#### COEF PLOTS ####
df_stru_sum <- df_stru_sum %>% ungroup

head(df_stru_sum)

table(df_stru_sum$year_quarter)

df_stru_sum <- df_stru_sum %>% mutate(year_quarter_dum = ifelse(year < 2019, "1pre", year_quarter))
df_stru_sum <- df_stru_sum %>% mutate(soc5_us_post = ifelse(year < 2019, "1pre", soc5_us))
df_stru_sum <- df_stru_sum %>% mutate(county_post = ifelse(year < 2019, "1pre", county))

cp1 <- feols(data = df_stru_sum, collin.tol = 1e-300, weights = ~ log(n),
                  fml = wfh_nn ~ -1 + year_quarter_dum)
df1 <- data.frame("var" = parse_number(names(cp1$coefficients)[2:16]),  "estimate" = cp1$coeftable$Estimate[2:16],
                  "se" = cp1$coeftable$`Std. Error`[2:16]) %>% mutate(fes = "None")

cp2 <- feols(data = df_stru_sum, collin.tol = 1e-300, weights = ~ log(n),
             fml = wfh_nn ~ year_quarter_dum | county_post)
df2 <- data.frame("var" = parse_number(names(cp2$coefficients)),  "estimate" = cp2$coeftable$Estimate, "se" = cp2$coeftable$`Std. Error`) %>%
  mutate(fes = "County")

cp3 <- feols(data = df_stru_sum, collin.tol = 1e-300, weights = ~ log(n),
             fml = wfh_nn ~ year_quarter_dum | soc5_us_post)
df3 <- data.frame("var" = parse_number(names(cp3$coefficients)),  "estimate" = cp3$coeftable$Estimate, "se" = cp3$coeftable$`Std. Error`) %>%
  mutate(fes = "Occupation (US SOC5)")

cp4 <- feols(data = df_stru_sum, collin.tol = 1e-300, weights = ~ log(n),
             fml = wfh_nn ~ year_quarter_dum | soc5_us + county_post)
df4 <- data.frame("var" = parse_number(names(cp4$coefficients)),  "estimate" = cp4$coeftable$Estimate, "se" = cp4$coeftable$`Std. Error`) %>%
  mutate(fes = "County + Occupation (US SOC5)")

etable(cp1, cp2, cp3, cp4, cp4)

df <- bind_rows(df1, df2, df3, df4, df4) %>% filter(var != "1") %>%
  mutate(var = as.Date(as.yearqtr(var)))

df <- df %>%
  group_by(fes) %>%
  mutate(estimate = estimate - mean(estimate[1:10], na.rm = T)) %>%
  ungroup

df

p = ggplot(df, aes(x=var, y = estimate, color = fes)) +
  geom_line(size = 1.5) +
  ggtitle("Coefficient Plot of Quarter Dummies") +
  xlab("Year / Quarter") +
  ylab("WFH Share") +
  scale_x_date(date_breaks = "6 months", date_labels = "%b%y") +
  scale_colour_ggthemr_d() +
  #labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 45, vjust = 1.25, hjust=1.1, size = 16),
    legend.key.width=unit(2.5,"cm"),
    legend.text=element_text(size=14),
    axis.text = element_text(size = 16),
    text = element_text(size = 16),
    plot.title = element_text(hjust = 0.5, size = 16)) +
  guides(col = guide_legend(nrow = 2))
p

#### TECHNOLOGY ####

vd1_2019 <- feols(data = df_stru_sum %>% filter(year == 2019) ,
      fml = wfh_nn ~ -1 | county + soc5_us)

df_county <- fixef(vd1_2019)[[1]] %>% as.data.frame() %>% rownames_to_column("county") %>% rename(county_fe = ".")
df_soc5_us <- fixef(vd1_2019)[[2]] %>% as.data.frame() %>% rownames_to_column("soc5_us") %>% rename(soc5_us_fe = ".")

df <- df_stru_sum %>% filter(year == 2019) %>%
  left_join(df_county) %>%
  left_join(df_soc5_us) %>%
  mutate(residuals = vd1_2019$residuals)

wtd.var(df$county_fe, df$n)/wtd.var(df$wfh_nn, df$n) # 0.0877882
wtd.var(df$soc5_us_fe, df$n)/wtd.var(df$wfh_nn, df$n) # 0.1289386
stats::cov.wt(x = data.frame("1" = df$soc5_us_fe, "2" = df$county_fe), wt = df$n)$cov[1,2]/ wtd.var(df$wfh_nn, df$n) # 0.004249734

vd1_2021 <- feols(data = df_stru_sum %>% filter(year == 2021),
                  fml = wfh_nn ~ -1 | county + soc5_us, weights = ~ n)

df_county <- fixef(vd1_2021)[[1]] %>% as.data.frame() %>% rownames_to_column("county") %>% rename(county_fe = ".")
df_soc5_us <- fixef(vd1_2021)[[2]] %>% as.data.frame() %>% rownames_to_column("soc5_us") %>% rename(soc5_us_fe = ".")

df <- df_stru_sum %>% filter(year == 2021) %>%
  left_join(df_county) %>%
  left_join(df_soc5_us) %>%
  mutate(residuals = vd1_2021$residuals)

wtd.var(df$county_fe, df$n)/wtd.var(df$wfh_nn, df$n) # 0.07322126
wtd.var(df$soc5_us_fe, df$n)/wtd.var(df$wfh_nn, df$n) # 0.3452601
stats::cov.wt(x = data.frame("1" = df$soc5_us_fe, "2" = df$county_fe), wt = df$n)$cov[1,2]/ wtd.var(df$wfh_nn, df$n) # 0.01799172

ss <- data.frame("Year" = c(2019, 2021), "County FEs" = 100*c(0.0877882, 0.07322126), "SOC5 FEs" = 100*c(0.07322126, 0.3452601))

stargazer(ss, summary = F, title = "Variance Decomposition Pre- and Post-COVID", rownames = F)


