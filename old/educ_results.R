#### SETUP ####
options(repos = c(CRAN = "https://mran.revolutionanalytics.com/snapshot/2021-04-01"))
remove(list = ls())
options(scipen=999)

library("data.table")
library("tidyverse")
library("janitor")
library("lubridate")
library("doParallel")
#library("lsa")
#library("fuzzyjoin")
#library("quanteda")
#library("refinr")
#library("FactoMineR")
#library("ggpubr")
#library("scales")
#install.packages("ggthemr")
#ggthemr('flat')
library("fixest")
#library("lfe")
#library("stargazer")
#library("texreg")
#library("sjPlot")
#library("margins")
#library("DescTools")
#library("fuzzyjoin")

# Translation packages
#library("rvest")
#library("googleLanguageR")
#library("cld2")
#library("datasets")
#library("vroom")
#library("refinr")
#library(stringi)

library(zoo)
library(ggplot2)
library(scales)
library("ggpubr")
devtools::install_github('Mikata-Project/ggthemr')
library(ggthemr)
ggthemr('flat')

setwd("/mnt/disks/pdisk/")

#### EDUC SHARE RESULTS ####

# Within-firm educ share
regs_us <- readRDS("./bg-us/col_share_bgt_regs.rds")
regs_eu <- readRDS("./bg-eu/col_share_bgt_regs.rds")
length(regs)

lapply(regs, summary)

print("test")

fe <- c("Month.", "Month, Occupation.", "Month, Region.", "Month, Occupation, Region.", "Month, Firm.")

i = 5

lapply(1:5, function(i) {
  test_us <- regs_us[[i]]$coeftable %>%
    as_tibble %>%
    mutate(quarter = c("2019Q1","2019Q2","2019Q3","2019Q4","2020Q1","2020Q2","2020Q3","2020Q4","2021Q1","2021Q2")) %>%
    mutate(quarter = as.yearqtr(yq(.$quarter))) %>%
    mutate(region = "US")
  
  test_eu <- regs_eu[[i]]$coeftable %>%
    as_tibble %>%
    mutate(quarter = c("2019Q1","2019Q2","2019Q3","2019Q4","2020Q1","2020Q2","2020Q3","2020Q4","2021Q1")) %>%
    mutate(quarter = as.yearqtr(yq(.$quarter))) %>%
    mutate(region = "EU")
  
  test <- bind_rows(test_us, test_eu)
  
  p = ggplot(test %>% ungroup(), aes(x = quarter, y = Estimate, color = region)) +
    geom_line(size = 1) +
    geom_point(size = 2) +
    geom_ribbon(aes(ymin = Estimate-`Std. Error`, ymax = Estimate+`Std. Error`),alpha = 0.15, fill = "Black", linetype=0) +
    geom_ribbon(aes(ymin = Estimate-2*`Std. Error`, ymax = Estimate+2*`Std. Error`),alpha = 0.15, fill = "Black", linetype=0) +
    ylab("Coefficient on Quarter Dummies") +
    xlab("Quarter") +
    scale_x_yearqtr(breaks = seq(from = min(test$quarter), to = max(test$quarter), by = 0.25),
                    format = "%YQ%q") +
    scale_colour_ggthemr_d() +
    ggtitle("Demand for Skilled Workers: Quarterly Coefficients (USA)") +
    labs(subtitle = paste0("Obs (USA/EU) = ",formatC(regs_us[[i]]$nobs, format="f", big.mark=",", digits=0),
                           " / ",
                           formatC(regs_eu[[i]]$nobs, format="f", big.mark=",", digits=0),
                           ". Fixed Effects: ",fe[[i]]),
         caption = "+/- 1 SD in dark grey.  +/- 2 SD in light grey.") +
    theme(legend.title=element_blank(),
          axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))
  
  ggexport(p, filename = paste0("./bg-us/plots/skilled_worker_demand/coeffplot_",i,".pdf"))
})


#### SKILL CLUSTER RESULTS - FIRM FEs ####
regs_us <- readRDS("./bg-us/sc_bgt_regs.rds")
regs_eu <- readRDS("./bg-eu/sc_bgt_regs.rds")

lapply(1:44, function(i) {
  
  test_us <- regs_us[[i]]$coeftable %>%
    rownames_to_column(., var = "quarter") %>%
    mutate(quarter = str_sub(quarter, -6, -1)) %>%
    mutate(region = "US") %>%
    mutate(quarter = as.yearqtr(yq(quarter)))
  
  test_eu <- regs_eu[[i]]$coeftable %>%
    rownames_to_column(., var = "quarter") %>%
    mutate(quarter = str_sub(quarter, -6, -1)) %>%
    mutate(region = "EU") %>%
    mutate(quarter = as.yearqtr(yq(quarter)))
  
  (regs_us[[i]]$fml_all$linear == regs_eu[[i]]$fml_all$linear)
  
  gsub("\\(|\\)", "", regs_us[[i]]$fml_all$fixef[2])
  
  test <- bind_rows(test_us, test_eu)
  
  p = ggplot(test %>% ungroup(), aes(x = quarter, y = Estimate, color = region)) +
    geom_line(size = 1) +
    geom_point(size = 2) +
    geom_ribbon(aes(ymin = Estimate-`Std. Error`, ymax = Estimate+`Std. Error`),alpha = 0.15, fill = "Black", linetype=0) +
    geom_ribbon(aes(ymin = Estimate-2*`Std. Error`, ymax = Estimate+2*`Std. Error`),alpha = 0.15, fill = "Black", linetype=0) +
    ylab("Coefficient on Quarter Dummies") +
    xlab("Quarter") +
    scale_x_yearqtr(breaks = seq(from = min(test$quarter), to = max(test$quarter), by = 0.25),
                    format = "%YQ%q") +
    scale_colour_ggthemr_d() +
    ggtitle(paste0('Demand for "',gsub("\\(\\)", "", regs_us[[i]]$fml_all$linear[2]),'": Quarterly Coefficients (USA)')) +
    labs(subtitle = paste0("Obs (USA/EU) = ",formatC(regs_us[[i]]$nobs, format="f", big.mark=",", digits=0),
                           " / ",
                           formatC(regs_eu[[i]]$nobs, format="f", big.mark=",", digits=0),
                           ". FE = ",gsub("\\(|\\)", "", regs_us[[i]]$fml_all$fixef[2]),"."),
         caption = "+/- 1 SD in dark grey.  +/- 2 SD in light grey.") +
    theme(legend.title=element_blank(),
          axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))
  p
  ggexport(p, filename = paste0("./bg-us/plots/all/n",i,"_",gsub("\\(\\)", "", regs_us[[i]]$fml_all$linear[2]),"_coeffplot.pdf"))
})


lapply(1:44, function(i) {
  
  print(regs_us[[i]]$fml_all$linear[2] == regs_eu[[i]]$fml_all$linear[2])
  
})
