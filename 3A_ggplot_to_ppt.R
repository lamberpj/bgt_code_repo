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
library('officer')
library('rvg')
library('here')

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

#### 1 REMOTE WORK ACROSS COUNTRIES - UNWEIGHTED ####
remove(list = ls())
load(file = "./ppt/ggplots/rwa_country_ts_w_unweighted_month.RData")
fig_number <- 1
fig_number_title <- paste0("Figure ", fig_number, ": ")
fig_short_name <- "wham_countries_unw"
fig_title <- "Share of Online Job Vacancy Postings which Advertise Remote Work, by Country"
fig_footnote <- "This figure present a monthly time series of the share of all online job vacancy postings which explicitly advertise remote work arrangements. Prior to aggregation at the monthly level, we employ a jackknife filter to remove a small number of outlier days (see Appendix XXXX: Data for further details). This figure reports the raw (unweighted) share of all new online job vacancies which advertised remote work, divided by the total number of new online job vacancies. Total number of job vacancies is 198,913,314.  Appendix XXX: Results provides alternative specifications of this figure using different re-weighted methodologies."
p_dml <- rvg::dml(ggobj = p)
ppt <- officer::read_pptx() %>%
  officer::add_slide(layout = "Title and Content", master = "Office Theme")
s_s <- slide_size(ppt)
(s_w <- s_s$width) # width of slides
(s_h <- s_s$height) # height of slides
f_w <- 7 # width of figure
f_h <- 5 # height of figure
left_f <- (s_w/2) - (f_w/2) 
top_f <- (s_h/2) - (f_h/2)
cap_w <- 9
cap_h <- 3
left_cap <-  (s_w/2) - (cap_w/2) 
top_cap <- (s_h/2) + (f_h/2) - 1
ppt <- ppt %>%
  ph_with(p_dml, ph_location(left = left_f, top = top_f + 0.15, width = f_w, height = f_h)) %>%
  ph_with(
    block_list(
      fpar(
        ftext(fig_number_title, prop = fp_text(bold = TRUE, font.size = 20, color = "black")),
        ftext(fig_title, prop = fp_text(font.size = 20, color = "black")),
        fp_p = fp_par(text.align = "justify")
      )
    ),
    location = ph_location_type(type = "title")
  ) %>%
  ph_with(
    block_list(
      fpar(
        ftext("Note: ", prop = fp_text(bold = TRUE, font.size = 12, color = "black")),
        ftext(fig_footnote, prop = fp_text(font.size = 12, color = "black")),
        fp_p = fp_par(text.align = "justify")
      )
    ),
    location = ph_location(left = left_cap, top = top_cap, width = cap_w, height = cap_h))
try(unlink(paste0("ppt/f",fig_number,"_",fig_short_name,".pptx")))
base::print(ppt, target = paste0("ppt/f",fig_number,"_",fig_short_name,".pptx"))
#### END ####

#### 2 REMOTE WORK ACROSS COUNTRIES - US VAC WEIGHTED 2019 ####
remove(list = ls())
load(file = "./ppt/ggplots/rwa_country_ts_w_us_vac_month.RData")
p
fig_number <- 2
fig_number_title <- paste0("Figure ", fig_number, ": ")
fig_short_name <- "wham_countries_usw"
fig_title <- "Share of Online Job Vacancy Postings which Advertise Remote Work, by Country"
fig_footnote <- "This figure present a monthly time series of the share of all online job vacancy postings which explicitly advertise remote work arrangements. The aggregate monthly share in each country is constructed as the weighted-mean across the share of advertised remote work in each granular occupation group (akin to six-digit SOC codes). The weights used to aggregate from the occupation-level to the national-level in each month come from the fraction of all 2019 vacancies in the USA belonging to this occupation group. Prior to aggregation at the monthly level, we employ a jackknife filter to remove a small number of outlier days (see Appendix XXXX: Data for further details). Total number of job vacancies is 198,913,314.  Appendix XXXX: Results) provides alternative specifications of this figure using different re-weighted methodologies."
p_dml <- rvg::dml(ggobj = p)
ppt <- officer::read_pptx() %>%
  officer::add_slide(layout = "Title and Content", master = "Office Theme")
s_s <- slide_size(ppt)
(s_w <- s_s$width) # width of slides
(s_h <- s_s$height) # height of slides
f_w <- 7 # width of figure
f_h <- 5 # height of figure
left_f <- (s_w/2) - (f_w/2) 
top_f <- (s_h/2) - (f_h/2)
cap_w <- 9
cap_h <- 3
left_cap <-  (s_w/2) - (cap_w/2) 
top_cap <- (s_h/2) + (f_h/2) - 1
ppt <- ppt %>%
  ph_with(p_dml, ph_location(left = left_f, top = top_f + 0.15, width = f_w, height = f_h)) %>%
  ph_with(
    block_list(
      fpar(
        ftext(fig_number_title, prop = fp_text(bold = TRUE, font.size = 20, color = "black")),
        ftext(fig_title, prop = fp_text(font.size = 20, color = "black")),
        fp_p = fp_par(text.align = "justify")
      )
    ),
    location = ph_location_type(type = "title")
  ) %>%
  ph_with(
    block_list(
      fpar(
        ftext("Note: ", prop = fp_text(bold = TRUE, font.size = 12, color = "black")),
        ftext(fig_footnote, prop = fp_text(font.size = 12, color = "black")),
        fp_p = fp_par(text.align = "justify")
      )
    ),
    location = ph_location(left = left_cap, top = top_cap, width = cap_w, height = cap_h))
try(unlink(paste0("ppt/f",fig_number,"_",fig_short_name,".pptx")))
base::print(ppt, target = paste0("ppt/f",fig_number,"_",fig_short_name,".pptx"))
#### END ####

#### 3 REMOTE WORK ACROSS OCCUPATIONS - BAR PLOT ####
remove(list = ls())
load(file = "./ppt/ggplots/occ_dist_alt.RData")
fig_number <- 3
fig_number_title <- paste0("Figure ", fig_number, ": ")
fig_short_name <- "wham_soc2_bar"
fig_title <- "Share of Online Job Vacancy Postings which Advertise Remote Work, by Occupation Group"
fig_footnote <- "We group all job vacancy postings by broad occupation codes and calculate the share of US job postings in each occupation which advertise remote work (i.e. that allow one or more days of work from home).  Each occupation group is based on a two-digit SOC US 2010 occupation group (https://www.bls.gov/soc/2010/2010_major_groups.htm). The pre-pandemic period includes all vacancies posted in calendar year 2019.  The post-pandemic period includes all vacancies posted between 2021Q3 and 2022Q2 (inclusive). "
p
p_dml <- rvg::dml(ggobj = p, pointsize = 10, editable = TRUE)
p_dml
ppt <- officer::read_pptx() %>%
  officer::add_slide(layout = "Title and Content", master = "Office Theme")
s_s <- slide_size(ppt)
(s_w <- s_s$width) # width of slides
(s_h <- s_s$height) # height of slides
f_w <- 9 # width of figure
f_h <- 5 # height of figure
left_f <- (s_w/2) - (f_w/2) 
top_f <- (s_h/2) - (f_h/2)
cap_w <- 9
cap_h <- 3
left_cap <-  (s_w/2) - (cap_w/2) 
top_cap <- (s_h/2) + (f_h/2) - 1
ppt <- ppt %>%
  ph_with(p_dml, ph_location(left = left_f, top = top_f + 0.15, width = f_w, height = f_h)) %>%
  ph_with(
    block_list(
      fpar(
        ftext(fig_number_title, prop = fp_text(bold = TRUE, font.size = 20, color = "black")),
        ftext(fig_title, prop = fp_text(font.size = 20, color = "black")),
        fp_p = fp_par(text.align = "justify")
      )
    ),
    location = ph_location_type(type = "title")
  ) %>%
  ph_with(
    block_list(
      fpar(
        ftext("Note: ", prop = fp_text(bold = TRUE, font.size = 12, color = "black")),
        ftext(fig_footnote, prop = fp_text(font.size = 12, color = "black")),
        fp_p = fp_par(text.align = "justify")
      )
    ),
    location = ph_location(left = left_cap, top = top_cap, width = cap_w, height = cap_h))
try(unlink(paste0("ppt/f",fig_number,"_",fig_short_name,".pptx")))
base::print(ppt, target = paste0("ppt/f",fig_number,"_",fig_short_name,".pptx"))
#### END ####

#### 4 REMOTE WORK ACROSS OCCUPATIONS - SCATTER PLOT ####
remove(list = ls())
load(file = "./ppt/ggplots/wfh_pre_post_by_tele.RData")
fig_number <- 4
fig_number_title <- paste0("Figure ", fig_number, ": ")
fig_short_name <- "wham_onet_scatter"
fig_title <- "Share of Online Job Vacancy Postings which Advertise Remote Work Pre- and Post-COVID, by `ONET' Occupation"
fig_footnote <- "We group all job vacancy postings by ONET occupation codes and calculate the share of US job postings in each occupation which advertise remote work (i.e. that allow one or more days of work from home).  We drop ONET codes with fewer than 250 posts in 2019, leaving a total of 875. The x-axis reports each occupation's share from all new postings in calendar year 2019, the y-axis uses job ads posted from 2021Q3 to 2022Q2, inclusive.  Blue triangles denote occupations which Dingle & Neimann (2020) classify as feasible for full-time teleworking, the orange circles show those which they classify as not suitable. The blue solid line is the unweighted OLS fit, with coefficients and R^2 reported alongside. On a level-scale, the mean (sd) for the x-axis is 4(5.905), and for the y-axis is 9.597(11.157)."
p
p_dml <- rvg::dml(ggobj = p)
ppt <- officer::read_pptx() %>%
  officer::add_slide(layout = "Title and Content", master = "Office Theme")
s_s <- slide_size(ppt)
(s_w <- s_s$width) # width of slides
(s_h <- s_s$height) # height of slides
f_w <- 7 # width of figure
f_h <- 5 # height of figure
left_f <- (s_w/2) - (f_w/2) 
top_f <- (s_h/2) - (f_h/2)
cap_w <- 9
cap_h <- 3
left_cap <-  (s_w/2) - (cap_w/2) 
top_cap <- (s_h/2) + (f_h/2) - 1
ppt <- ppt %>%
  ph_with(p_dml, ph_location(left = left_f, top = top_f, width = f_w, height = f_h)) %>%
  ph_with(
    block_list(
      fpar(
        ftext(fig_number_title, prop = fp_text(bold = TRUE, font.size = 20, color = "black")),
        ftext(fig_title, prop = fp_text(font.size = 20, color = "black")),
        fp_p = fp_par(text.align = "justify")
      )
    ),
    location = ph_location_type(type = "title")
  ) %>%
  ph_with(
    block_list(
      fpar(
        ftext("Note: ", prop = fp_text(bold = TRUE, font.size = 12, color = "black")),
        ftext(fig_footnote, prop = fp_text(font.size = 12, color = "black")),
        fp_p = fp_par(text.align = "justify")
      )
    ),
    location = ph_location(left = left_cap, top = top_cap, width = cap_w, height = cap_h))
try(unlink(paste0("ppt/f",fig_number,"_",fig_short_name,".pptx")))
base::print(ppt, target = paste0("ppt/f",fig_number,"_",fig_short_name,".pptx"))
#### END ####

#### 5 REMOTE WORK ACROSS CITIES - BAR PLOT ####
remove(list = ls())
load(file = "./ppt/ggplots/city_dist_alt.RData")
fig_number <- 5
fig_number_title <- paste0("Figure ", fig_number, ": ")
fig_short_name <- "wham_city_bar"
fig_title <- "Share of Online Job Vacancy Postings which Advertise Remote Work, by Large City"
fig_footnote <- "We calculate the share of all job vacancy postings which explicitly advertise remote working arrangements, by large cities across our five countries.  We compare calendar year 2019 to the period 2021 Q3 to 2022 Q2, inclusive.  The location of each job ad is based on the establishment/firm which is hiring."
p
p_dml <- rvg::dml(ggobj = p, pointsize = 10, editable = TRUE)
p_dml
ppt <- officer::read_pptx() %>%
  officer::add_slide(layout = "Title and Content", master = "Office Theme")
s_s <- slide_size(ppt)
(s_w <- s_s$width) # width of slides
(s_h <- s_s$height) # height of slides
f_w <- 9 # width of figure
f_h <- 5 # height of figure
left_f <- (s_w/2) - (f_w/2) 
top_f <- (s_h/2) - (f_h/2)
cap_w <- 9
cap_h <- 3
left_cap <-  (s_w/2) - (cap_w/2) 
top_cap <- (s_h/2) + (f_h/2) - 1
ppt <- ppt %>%
  ph_with(p_dml, ph_location(left = left_f, top = top_f + 0.15, width = f_w, height = f_h)) %>%
  ph_with(
    block_list(
      fpar(
        ftext(fig_number_title, prop = fp_text(bold = TRUE, font.size = 20, color = "black")),
        ftext(fig_title, prop = fp_text(font.size = 20, color = "black")),
        fp_p = fp_par(text.align = "justify")
      )
    ),
    location = ph_location_type(type = "title")
  ) %>%
  ph_with(
    block_list(
      fpar(
        ftext("Note: ", prop = fp_text(bold = TRUE, font.size = 12, color = "black")),
        ftext(fig_footnote, prop = fp_text(font.size = 12, color = "black")),
        fp_p = fp_par(text.align = "justify")
      )
    ),
    location = ph_location(left = left_cap, top = top_cap, width = cap_w, height = cap_h))
try(unlink(paste0("ppt/f",fig_number,"_",fig_short_name,".pptx")))
base::print(ppt, target = paste0("ppt/f",fig_number,"_",fig_short_name,".pptx"))
#### END ####

#### 6 REMOTE WORK ACROSS CITIES - SCATTER PLOT ####
remove(list = ls())
load(file = "./ppt/ggplots/wfh_pre_post_by_city.RData")
fig_number <- 6
fig_number_title <- paste0("Figure ", fig_number, ": ")
fig_short_name <- "wham_city_scatter"
fig_title <- "Share of Online Job Vacancy Postings which Advertise Remote Work Pre- and Post-COVID, by City"
fig_footnote <- "We calculate the share of all job vacancy postings which explicitly advertise remote working arrangements, by large cities across our five countries. The x-axis reports information from calendar year 2019, the y-axis reports information from the period 2021Q3 to 2022Q2, inclusive.  Panel (b) applies the inverse-hyperbolic-sine transformation.  The blue solid line is a least-squares regression fit, with the linear coefficients and the coefficient of determination in the bottom right of each panel. Vacancy Weighted, USA. 2019/2022 Mean (SD) = 3.573(2.135) / 11.08(5.276)"
p
p_dml <- rvg::dml(ggobj = p)
ppt <- officer::read_pptx() %>%
  officer::add_slide(layout = "Title and Content", master = "Office Theme")
s_s <- slide_size(ppt)
(s_w <- s_s$width) # width of slides
(s_h <- s_s$height) # height of slides
f_w <- 7 # width of figure
f_h <- 5 # height of figure
left_f <- (s_w/2) - (f_w/2) 
top_f <- (s_h/2) - (f_h/2)
cap_w <- 9
cap_h <- 3
left_cap <-  (s_w/2) - (cap_w/2) 
top_cap <- (s_h/2) + (f_h/2) - 1
ppt <- ppt %>%
  ph_with(p_dml, ph_location(left = left_f, top = top_f, width = f_w, height = f_h)) %>%
  ph_with(
    block_list(
      fpar(
        ftext(fig_number_title, prop = fp_text(bold = TRUE, font.size = 20, color = "black")),
        ftext(fig_title, prop = fp_text(font.size = 20, color = "black")),
        fp_p = fp_par(text.align = "justify")
      )
    ),
    location = ph_location_type(type = "title")
  ) %>%
  ph_with(
    block_list(
      fpar(
        ftext("Note: ", prop = fp_text(bold = TRUE, font.size = 12, color = "black")),
        ftext(fig_footnote, prop = fp_text(font.size = 12, color = "black")),
        fp_p = fp_par(text.align = "justify")
      )
    ),
    location = ph_location(left = left_cap, top = top_cap, width = cap_w, height = cap_h))
try(unlink(paste0("ppt/f",fig_number,"_",fig_short_name,".pptx")))
base::print(ppt, target = paste0("ppt/f",fig_number,"_",fig_short_name,".pptx"))
#### END ####

#### 7 REMOTE WORK ACROSS CITIES - NORTHEAST, WEST and SOUTH ####
remove(list = ls())
load(file = "./ppt/ggplots/ts_cities_ne_w_s.RData")
p
fig_number <- 7
fig_number_title <- paste0("Figure ", fig_number, ": ")
fig_short_name <- "city_ts_northeast_west_south"
fig_title <- "Share of Online Job Vacancy Postings which Advertise Remote Work, by Selected Cities from the North-East, West and South US Regions"
fig_footnote <- "We calculate the monthly share of all new job vacancy postings which explicitly advertise remote working arrangements, by selected cities. Prior to aggregation at the monthly level, we employ a jackknife filter to remove a small number of outlier days (see Appendix XXX: Data for further details). This figure shows the 3-month moving average, except for March-May 2020, where the monthly level is shown."
p_dml <- rvg::dml(ggobj = p)
ppt <- officer::read_pptx() %>%
  officer::add_slide(layout = "Title and Content", master = "Office Theme")
s_s <- slide_size(ppt)
(s_w <- s_s$width) # width of slides
(s_h <- s_s$height) # height of slides
f_w <- 7 # width of figure
f_h <- 5 # height of figure
left_f <- (s_w/2) - (f_w/2) 
top_f <- (s_h/2) - (f_h/2)
cap_w <- 9
cap_h <- 3
left_cap <-  (s_w/2) - (cap_w/2) 
top_cap <- (s_h/2) + (f_h/2) - 1
ppt <- ppt %>%
  ph_with(p_dml, ph_location(left = left_f, top = top_f + 0.15, width = f_w, height = f_h)) %>%
  ph_with(
    block_list(
      fpar(
        ftext(fig_number_title, prop = fp_text(bold = TRUE, font.size = 20, color = "black")),
        ftext(fig_title, prop = fp_text(font.size = 20, color = "black")),
        fp_p = fp_par(text.align = "justify")
      )
    ),
    location = ph_location_type(type = "title")
  ) %>%
  ph_with(
    block_list(
      fpar(
        ftext("Note: ", prop = fp_text(bold = TRUE, font.size = 12, color = "black")),
        ftext(fig_footnote, prop = fp_text(font.size = 12, color = "black")),
        fp_p = fp_par(text.align = "justify")
      )
    ),
    location = ph_location(left = left_cap, top = top_cap, width = cap_w, height = cap_h))
try(unlink(paste0("ppt/f",fig_number,"_",fig_short_name,".pptx")))
base::print(ppt, target = paste0("ppt/f",fig_number,"_",fig_short_name,".pptx"))
#### END ####

#### 8 REMOTE WORK ACROSS CITIES - UK ####
remove(list = ls())
load(file = "./ppt/ggplots/ts_cities_uk.RData")
fig_number <- 8
fig_number_title <- paste0("Figure ", fig_number, ": ")
fig_short_name <- "city_ts_uk"
fig_title <- "Share of Online Job Vacancy Postings which Advertise Remote Work, by Selected Cities from the United Kingdom"
fig_footnote <- "We calculate the monthly share of all new job vacancy postings which explicitly advertise remote working arrangements, by selected cities. Prior to aggregation at the monthly level, we employ a jackknife filter to remove a small number of outlier days (see Appendix XXX: Data for further details). This figure shows the 3-month moving average, except for March-May 2020, where the monthly level is shown."
p_dml <- rvg::dml(ggobj = p)
ppt <- officer::read_pptx() %>%
  officer::add_slide(layout = "Title and Content", master = "Office Theme")
s_s <- slide_size(ppt)
(s_w <- s_s$width) # width of slides
(s_h <- s_s$height) # height of slides
f_w <- 7 # width of figure
f_h <- 5 # height of figure
left_f <- (s_w/2) - (f_w/2) 
top_f <- (s_h/2) - (f_h/2)
cap_w <- 9
cap_h <- 3
left_cap <-  (s_w/2) - (cap_w/2) 
top_cap <- (s_h/2) + (f_h/2) - 1
ppt <- ppt %>%
  ph_with(p_dml, ph_location(left = left_f, top = top_f + 0.15, width = f_w, height = f_h)) %>%
  ph_with(
    block_list(
      fpar(
        ftext(fig_number_title, prop = fp_text(bold = TRUE, font.size = 20, color = "black")),
        ftext(fig_title, prop = fp_text(font.size = 20, color = "black")),
        fp_p = fp_par(text.align = "justify")
      )
    ),
    location = ph_location_type(type = "title")
  ) %>%
  ph_with(
    block_list(
      fpar(
        ftext("Note: ", prop = fp_text(bold = TRUE, font.size = 12, color = "black")),
        ftext(fig_footnote, prop = fp_text(font.size = 12, color = "black")),
        fp_p = fp_par(text.align = "justify")
      )
    ),
    location = ph_location(left = left_cap, top = top_cap, width = cap_w, height = cap_h))
try(unlink(paste0("ppt/f",fig_number,"_",fig_short_name,".pptx")))
base::print(ppt, target = paste0("ppt/f",fig_number,"_",fig_short_name,".pptx"))
#### END ####

#### 9 ACS vs WHAM ####
remove(list = ls())
load(file = "./ppt/ggplots/wham_vs_acs.RData")
fig_number <- 9
fig_number_title <- paste0("Figure ", fig_number, ": ")
fig_short_name <- "wham_vs_acs"
fig_title <- "Share of Online Job Vacancy Postings which Advertise Remote Work (2022) vs Share of Employees who reported ‘Mostly Working from Home’ in the 2021 American Community Survey (ACS), by MSA"
fig_footnote <- "The y-axis shows our measurement of the share of all new job vacancy postings which explicitly advertise remote working arrangements, by MSA. The x-axis shows the share of employees in the American Communities Survey (ACS) who responded that they `mostly worked from home'.  This latter series is constructed using sampling weights, and in all cases we take the ACS' best effort to calculate the point-estimate for the population based on their survey data.  We cut off one substantial outlier, but do not remove any outliers when calculating fit statistics.  Roughly half of all MSAs were removed due to missing values in the ACS."
p_dml <- rvg::dml(ggobj = p)
ppt <- officer::read_pptx() %>%
  officer::add_slide(layout = "Title and Content", master = "Office Theme")
layout_properties(ppt, layout = "Title and Content")
s_s <- slide_size(ppt)
(s_w <- s_s$width) # width of slides
(s_h <- s_s$height) # height of slides
f_w <- 7 # width of figure
f_h <- 5 # height of figure
left_f <- (s_w/2) - (f_w/2) 
top_f <- (s_h/2) - (f_h/2)
cap_w <- 9
cap_h <- 3
left_cap <-  (s_w/2) - (cap_w/2) 
top_cap <- (s_h/2) + (f_h/2) - 1
ppt <- ppt %>%
  ph_with(p_dml, ph_location(left = left_f, top = top_f + 0.15, width = f_w, height = f_h)) %>%
  ph_with(
    block_list(
      fpar(
        ftext(fig_number_title, prop = fp_text(bold = TRUE, font.size = 20, color = "black")),
        ftext(fig_title, prop = fp_text(font.size = 20, color = "black")),
        fp_p = fp_par(text.align = "justify")
      )
    ),
    location = ph_location_type(type = "title")
  ) %>%
  ph_with(
    block_list(
      fpar(
        ftext("Note: ", prop = fp_text(bold = TRUE, font.size = 12, color = "black")),
        ftext(fig_footnote, prop = fp_text(font.size = 12, color = "black")),
        fp_p = fp_par(text.align = "justify")
      )
    ),
    location = ph_location(left = left_cap, top = top_cap, width = cap_w, height = cap_h))

try(unlink(paste0("ppt/f",fig_number,"_",fig_short_name,".pptx")))
base::print(ppt, target = paste0("ppt/f",fig_number,"_",fig_short_name,".pptx"))
#### END ####

#### 10 REMOTE WORK ACROSS SELECTED FIRMS - SPACE/AERO MANAGEMENT OCCS - BAR PLOT ####
remove(list = ls())
load(file = "./ppt/ggplots/top_us_firms_soc2_11_naics4_3364_airspace_management.RData")
fig_number <- 10
fig_number_title <- paste0("Figure ", fig_number, ": ")
fig_short_name <- "selected_firms_aerospace_man_bar"
fig_title <- "Share of Online Job Vacancy Postings which Advertise Remote Work, by Selected Employers in Aerospace Manufacturing Industries (NAICS 3364) (Management Occupations Only, SOC 11) "
fig_footnote <- "We calculate the share of all job vacancy postings which explicitly advertise remote working arrangements, by selected employers in the US.  We compare calendar year 2019 to the period 2021 Q3 to 2022 Q2, inclusive.  We subset these data to consider only `Management Occupations` (i.e. the SOC 2-digit code 11).  Note that we add a small amount of jitter to help with visualisations, for example in both periods SpaceX had exactly zero job ads advertising remote work.  All companies posted at least 20 job ads in each period, with the lowest being 21 posts (United Technologies Corporation in 2021-22) and the highest being 8,130 (The Boeing Company in 2021-22)."
p
p_dml <- rvg::dml(ggobj = p, pointsize = 10, editable = TRUE)
p_dml
ppt <- officer::read_pptx() %>%
  officer::add_slide(layout = "Title and Content", master = "Office Theme")
s_s <- slide_size(ppt)
(s_w <- s_s$width) # width of slides
(s_h <- s_s$height) # height of slides
f_w <- 9 # width of figure
f_h <- 5 # height of figure
left_f <- (s_w/2) - (f_w/2) 
top_f <- (s_h/2) - (f_h/2)
cap_w <- 9
cap_h <- 3
left_cap <-  (s_w/2) - (cap_w/2) 
top_cap <- (s_h/2) + (f_h/2) - 1
ppt <- ppt %>%
  ph_with(p_dml, ph_location(left = left_f, top = top_f + 0.15, width = f_w, height = f_h)) %>%
  ph_with(
    block_list(
      fpar(
        ftext(fig_number_title, prop = fp_text(bold = TRUE, font.size = 20, color = "black")),
        ftext(fig_title, prop = fp_text(font.size = 20, color = "black")),
        fp_p = fp_par(text.align = "justify")
      )
    ),
    location = ph_location_type(type = "title")
  ) %>%
  ph_with(
    block_list(
      fpar(
        ftext("Note: ", prop = fp_text(bold = TRUE, font.size = 12, color = "black")),
        ftext(fig_footnote, prop = fp_text(font.size = 12, color = "black")),
        fp_p = fp_par(text.align = "justify")
      )
    ),
    location = ph_location(left = left_cap, top = top_cap, width = cap_w, height = cap_h))
try(unlink(paste0("ppt/f",fig_number,"_",fig_short_name,".pptx")))
base::print(ppt, target = paste0("ppt/f",fig_number,"_",fig_short_name,".pptx"))
#### END ####

#### 11 REMOTE WORK ACROSS SELECTED FIRMS - INSURANCE - MATHEMATICAL SCIENCE OCCUPATIONS - BAR PLOT ####
remove(list = ls())
load(file = "./ppt/ggplots/top_us_firms_soc2_15-20_naics4_5241_insurance_math_occs.RData")
fig_number <- 11
fig_number_title <- paste0("Figure ", fig_number, ": ")
fig_short_name <- "selected_firms_insurance_math_bar"
fig_title <- "Share of Online Job Vacancy Postings which Advertise Remote Work, by Selected Employers in Insurance Industry (NAICS 5241) (Mathematical Science Occupations Only, SOC 15-20) "
fig_footnote <- "We calculate the share of all job vacancy postings which explicitly advertise remote working arrangements, by selected employers in the US.  We compare calendar year 2019 to the period 2021 Q3 to 2022 Q2, inclusive.  We subset these data to consider only `Mathematical Science Occupations` (i.e. the SOC 4-digit code 15-20).  All companies posted at least 20 job ads in each period, with the lowest being 71 posts (Mutual of Omaha Company in 2019) and the highest being 7,302 (Anthem Blue Cross in 2019)."
p
p_dml <- rvg::dml(ggobj = p, pointsize = 10, editable = TRUE)
p_dml
ppt <- officer::read_pptx() %>%
  officer::add_slide(layout = "Title and Content", master = "Office Theme")
s_s <- slide_size(ppt)
(s_w <- s_s$width) # width of slides
(s_h <- s_s$height) # height of slides
f_w <- 9 # width of figure
f_h <- 5 # height of figure
left_f <- (s_w/2) - (f_w/2) 
top_f <- (s_h/2) - (f_h/2)
cap_w <- 9
cap_h <- 3
left_cap <-  (s_w/2) - (cap_w/2) 
top_cap <- (s_h/2) + (f_h/2) - 1
ppt <- ppt %>%
  ph_with(p_dml, ph_location(left = left_f, top = top_f + 0.15, width = f_w, height = f_h)) %>%
  ph_with(
    block_list(
      fpar(
        ftext(fig_number_title, prop = fp_text(bold = TRUE, font.size = 20, color = "black")),
        ftext(fig_title, prop = fp_text(font.size = 20, color = "black")),
        fp_p = fp_par(text.align = "justify")
      )
    ),
    location = ph_location_type(type = "title")
  ) %>%
  ph_with(
    block_list(
      fpar(
        ftext("Note: ", prop = fp_text(bold = TRUE, font.size = 12, color = "black")),
        ftext(fig_footnote, prop = fp_text(font.size = 12, color = "black")),
        fp_p = fp_par(text.align = "justify")
      )
    ),
    location = ph_location(left = left_cap, top = top_cap, width = cap_w, height = cap_h))
try(unlink(paste0("ppt/f",fig_number,"_",fig_short_name,".pptx")))
base::print(ppt, target = paste0("ppt/f",fig_number,"_",fig_short_name,".pptx"))
#### END ####

#### 12 REMOTE WORK ACROSS SELECTED FIRMS - ACCOUNTING SERVICES - FINANCIAL SPECIALIST OCCUPATIONS - BAR PLOT ####
# remove(list = ls())
# load(file = "./ppt/ggplots/top_us_firms_soc2_13-20_naics4_5412_accounting_services_ind_fin_spec_occs.RData")
# fig_number <- 12
# fig_number_title <- paste0("Figure ", fig_number, ": ")
# fig_short_name <- "selected_firms_accounting_serv_ind_fin_spec_occ_bar"
# fig_title <- "Share of Online Job Vacancy Postings which Advertise Remote Work, by Selected Employers in Accounting Services industry (NAICS 5412) (Financial Specialist occupations only, SOC 13-20) "
# fig_footnote <- "We calculate the share of all job vacancy postings which explicitly advertise remote working arrangements, by selected employers in the US.  We compare calendar year 2019 to the period 2021 Q3 to 2022 Q2, inclusive.  We subset these data to consider only `Financial Specialists` occupations (i.e. the SOC 4-digit code 13-20).  Each group has a subtantial number of vacancy postings, with the lowest being 203 posts (Eide Bailly in 2019) and the highest being 28,726 (KPMG in 2021-22)."
# p
# p_dml <- rvg::dml(ggobj = p, pointsize = 10, editable = TRUE)
# p_dml
# ppt <- officer::read_pptx() %>%
#   officer::add_slide(layout = "Title and Content", master = "Office Theme")
# s_s <- slide_size(ppt)
# (s_w <- s_s$width) # width of slides
# (s_h <- s_s$height) # height of slides
# f_w <- 9 # width of figure
# f_h <- 5 # height of figure
# left_f <- (s_w/2) - (f_w/2) 
# top_f <- (s_h/2) - (f_h/2)
# cap_w <- 9
# cap_h <- 3
# left_cap <-  (s_w/2) - (cap_w/2) 
# top_cap <- (s_h/2) + (f_h/2) - 1
# ppt <- ppt %>%
#   ph_with(p_dml, ph_location(left = left_f, top = top_f + 0.15, width = f_w, height = f_h)) %>%
#   ph_with(
#     block_list(
#       fpar(
#         ftext(fig_number_title, prop = fp_text(bold = TRUE, font.size = 20, color = "black")),
#         ftext(fig_title, prop = fp_text(font.size = 20, color = "black")),
#         fp_p = fp_par(text.align = "justify")
#       )
#     ),
#     location = ph_location_type(type = "title")
#   ) %>%
#   ph_with(
#     block_list(
#       fpar(
#         ftext("Note: ", prop = fp_text(bold = TRUE, font.size = 12, color = "black")),
#         ftext(fig_footnote, prop = fp_text(font.size = 12, color = "black")),
#         fp_p = fp_par(text.align = "justify")
#       )
#     ),
#     location = ph_location(left = left_cap, top = top_cap, width = cap_w, height = cap_h))
# try(unlink(paste0("ppt/f",fig_number,"_",fig_short_name,".pptx")))
# base::print(ppt, target = paste0("ppt/f",fig_number,"_",fig_short_name,".pptx"))
#### END ####


#### 13 REMOTE WORK ACROSS SELECTED FIRMS - AUTO FIRMS - BAR PLOT ####
remove(list = setdiff(ls(), "df_us"))
load(file = "./ppt/ggplots/top_us_firms_soc2_17-21_auto_naics4_3361_eng.RData")
fig_number <- 13
fig_number_title <- paste0("Figure ", fig_number, ": ")
fig_short_name <- "selected_firms_auto_eng_bar"
fig_title <- "Share of Online Job Vacancy Postings which Advertise Remote Work, by Selected Employers in Auto Industry (NAICS 3361) (Engineers only, SOC 43-2) "
fig_footnote <- "We calculate the share of all job vacancy postings which explicitly advertise remote working arrangements, by selected employers in the US.  We compare calendar year 2019 to the period 2021 Q3 to 2022 Q2, inclusive.  We subset these data to consider only `Engineering` occupations (i.e. the SOC 4-digit code 17-2).  Each group has a subtantial number of vacancy postings, with the lowest being 187 posts (University of Wisconsin in 2019) and the highest being 887 (Pennsylvania State University in 2021-22)."
p
p_dml <- rvg::dml(ggobj = p, pointsize = 10, editable = TRUE)
p_dml
ppt <- officer::read_pptx() %>%
  officer::add_slide(layout = "Title and Content", master = "Office Theme")
s_s <- slide_size(ppt)
(s_w <- s_s$width) # width of slides
(s_h <- s_s$height) # height of slides
f_w <- 9 # width of figure
f_h <- 5 # height of figure
left_f <- (s_w/2) - (f_w/2)
top_f <- (s_h/2) - (f_h/2)
cap_w <- 9
cap_h <- 3
left_cap <-  (s_w/2) - (cap_w/2)
top_cap <- (s_h/2) + (f_h/2) - 1
ppt <- ppt %>%
  ph_with(p_dml, ph_location(left = left_f, top = top_f + 0.15, width = f_w, height = f_h)) %>%
  ph_with(
    block_list(
      fpar(
        ftext(fig_number_title, prop = fp_text(bold = TRUE, font.size = 20, color = "black")),
        ftext(fig_title, prop = fp_text(font.size = 20, color = "black")),
        fp_p = fp_par(text.align = "justify")
      )
    ),
    location = ph_location_type(type = "title")
  ) %>%
  ph_with(
    block_list(
      fpar(
        ftext("Note: ", prop = fp_text(bold = TRUE, font.size = 12, color = "black")),
        ftext(fig_footnote, prop = fp_text(font.size = 12, color = "black")),
        fp_p = fp_par(text.align = "justify")
      )
    ),
    location = ph_location(left = left_cap, top = top_cap, width = cap_w, height = cap_h))
try(unlink(paste0("ppt/f",fig_number,"_",fig_short_name,".pptx")))
base::print(ppt, target = paste0("ppt/f",fig_number,"_",fig_short_name,".pptx"))
#### END ####


#### 14 WHAM vs Log N - 2019 vs 2022 ####
remove(list = ls())
load(file = "./ppt/ggplots/wfh_share_vs_firm_size_bs2.RData")
fig_number <- 14
fig_number_title <- paste0("Figure ", fig_number, ": ")
fig_short_name <- "wham_vs_N"
fig_title <- "Binscatter of Share of Online Job Vacancy Postings which Advertise Remote Work vs Number of New Job Vacancies, by Employers (2019 vs 2022)"
fig_footnote <- "The y-axis shows our measurement of the share of all new job vacancy postings which explicitly advertise remote working arrangements, calculated for each employer with 10 or more vacancies during the period 2021 Q3 to 2022 Q2, inclusive. The x-axis shows the number of new vacancy postings for each employer, during the period 2021 Q3 to 2022 Q2, inclusive. Data is trimmed at the 5/95 percentiles along both dimensions. The large orange circles depict a bin-scatter across 10 bins. The small grey circles depict a bin-scatter across 150 bins.  Standard errors for orange points are present, but not discernable."
p_dml <- rvg::dml(ggobj = p)
ppt <- officer::read_pptx() %>%
  officer::add_slide(layout = "Title and Content", master = "Office Theme")
layout_properties(ppt, layout = "Title and Content")
s_s <- slide_size(ppt)
(s_w <- s_s$width) # width of slides
(s_h <- s_s$height) # height of slides
f_w <- 7 # width of figure
f_h <- 5 # height of figure
left_f <- (s_w/2) - (f_w/2) 
top_f <- (s_h/2) - (f_h/2)
cap_w <- 9
cap_h <- 3
left_cap <-  (s_w/2) - (cap_w/2) 
top_cap <- (s_h/2) + (f_h/2) - 1
ppt <- ppt %>%
  ph_with(p_dml, ph_location(left = left_f, top = top_f + 0.15, width = f_w, height = f_h)) %>%
  ph_with(
    block_list(
      fpar(
        ftext(fig_number_title, prop = fp_text(bold = TRUE, font.size = 20, color = "black")),
        ftext(fig_title, prop = fp_text(font.size = 20, color = "black")),
        fp_p = fp_par(text.align = "justify")
      )
    ),
    location = ph_location_type(type = "title")
  ) %>%
  ph_with(
    block_list(
      fpar(
        ftext("Note: ", prop = fp_text(bold = TRUE, font.size = 12, color = "black")),
        ftext(fig_footnote, prop = fp_text(font.size = 12, color = "black")),
        fp_p = fp_par(text.align = "justify")
      )
    ),
    location = ph_location(left = left_cap, top = top_cap, width = cap_w, height = cap_h))

try(unlink(paste0("ppt/f",fig_number,"_",fig_short_name,".pptx")))
base::print(ppt, target = paste0("ppt/f",fig_number,"_",fig_short_name,".pptx"))
#### END ####

#### 16 WHAM vs N DHS DIFF ####
# remove(list = ls())
# load(file = "./ppt/ggplots/wfh_share_vs_firm_size_bs2.RData")
# fig_number <- 16
# fig_number_title <- paste0("Figure ", fig_number, ": ")
# fig_short_name <- "wham_vs_N_diff"
# fig_title <- "Binscatter of Absolute Change in the Share of Online Job Vacancy Postings which Advertise Remote Work (2019 vs 2021-22) vs Absolute Change in the Number of New Job Vacancies, by Employers (2019 vs 2021-22)"
# fig_footnote <- "The y-axis shows the absolute difference between the share of all new job vacancy postings which explicitly advertise remote working arrangements, calculated for each employer with 10 or more vacancies in both 2019 and the period 2021 Q3 to 2022 Q2, inclusive. The x-axis shows the absolute difference in the 2019 and 2021-22 number of new vacancy postings for each employer, during the period 2021 Q3 to 2022 Q2, inclusive. Data is trimmed at the 5/95 percentiles along both dimensions. The large orange circles depict a bin-scatter across 10 bins. The small grey circles depict a bin-scatter across 150 bins.  Standard errors for orange points are present, but not discernable."
# p_dml <- rvg::dml(ggobj = p)
# ppt <- officer::read_pptx() %>%
#   officer::add_slide(layout = "Title and Content", master = "Office Theme")
# layout_properties(ppt, layout = "Title and Content")
# s_s <- slide_size(ppt)
# (s_w <- s_s$width) # width of slides
# (s_h <- s_s$height) # height of slides
# f_w <- 7 # width of figure
# f_h <- 5 # height of figure
# left_f <- (s_w/2) - (f_w/2) 
# top_f <- (s_h/2) - (f_h/2)
# cap_w <- 9
# cap_h <- 3
# left_cap <-  (s_w/2) - (cap_w/2) 
# top_cap <- (s_h/2) + (f_h/2) - 1
# ppt <- ppt %>%
#   ph_with(p_dml, ph_location(left = left_f, top = top_f + 0.15, width = f_w, height = f_h)) %>%
#   ph_with(
#     block_list(
#       fpar(
#         ftext(fig_number_title, prop = fp_text(bold = TRUE, font.size = 20, color = "black")),
#         ftext(fig_title, prop = fp_text(font.size = 20, color = "black")),
#         fp_p = fp_par(text.align = "justify")
#       )
#     ),
#     location = ph_location_type(type = "title")
#   ) %>%
#   ph_with(
#     block_list(
#       fpar(
#         ftext("Note: ", prop = fp_text(bold = TRUE, font.size = 12, color = "black")),
#         ftext(fig_footnote, prop = fp_text(font.size = 12, color = "black")),
#         fp_p = fp_par(text.align = "justify")
#       )
#     ),
#     location = ph_location(left = left_cap, top = top_cap, width = cap_w, height = cap_h))
# 
# try(unlink(paste0("ppt/f",fig_number,"_",fig_short_name,".pptx")))
# base::print(ppt, target = paste0("ppt/f",fig_number,"_",fig_short_name,".pptx"))
#### END ####


#### 17 WHAM vs N DHS DIFF ####
# remove(list = ls())
# load(file = "./ppt/ggplots/wfh_share_vs_firm_size_bs3.RData")
# fig_number <- 17
# fig_number_title <- paste0("Figure ", fig_number, ": ")
# fig_short_name <- "wham_vs_N_diff"
# fig_title <- "Binscatter of Absolute Change in the Share of Online Job Vacancy Postings which Advertise Remote Work (2019 vs 2021-22) vs Absolute Change in the Number of New Job Vacancies, by Employers (2019 vs 2021-22)"
# fig_footnote <- "The y-axis shows the absolute difference between the share of all new job vacancy postings which explicitly advertise remote working arrangements, calculated for each employer with 10 or more vacancies in both 2019 and the period 2021 Q3 to 2022 Q2, inclusive. The x-axis shows the absolute difference in the 2019 and 2021-22 number of new vacancy postings for each employer, during the period 2021 Q3 to 2022 Q2, inclusive. Data is trimmed at the 5/95 percentiles along both dimensions. The large orange circles depict a bin-scatter across 10 bins. The small grey circles depict a bin-scatter across 150 bins.  Standard errors for orange points are present, but not discernable."
# p_dml <- rvg::dml(ggobj = p)
# ppt <- officer::read_pptx() %>%
#   officer::add_slide(layout = "Title and Content", master = "Office Theme")
# layout_properties(ppt, layout = "Title and Content")
# s_s <- slide_size(ppt)
# (s_w <- s_s$width) # width of slides
# (s_h <- s_s$height) # height of slides
# f_w <- 7 # width of figure
# f_h <- 5 # height of figure
# left_f <- (s_w/2) - (f_w/2) 
# top_f <- (s_h/2) - (f_h/2)
# cap_w <- 9
# cap_h <- 3
# left_cap <-  (s_w/2) - (cap_w/2) 
# top_cap <- (s_h/2) + (f_h/2) - 1
# ppt <- ppt %>%
#   ph_with(p_dml, ph_location(left = left_f, top = top_f + 0.15, width = f_w, height = f_h)) %>%
#   ph_with(
#     block_list(
#       fpar(
#         ftext(fig_number_title, prop = fp_text(bold = TRUE, font.size = 20, color = "black")),
#         ftext(fig_title, prop = fp_text(font.size = 20, color = "black")),
#         fp_p = fp_par(text.align = "justify")
#       )
#     ),
#     location = ph_location_type(type = "title")
#   ) %>%
#   ph_with(
#     block_list(
#       fpar(
#         ftext("Note: ", prop = fp_text(bold = TRUE, font.size = 12, color = "black")),
#         ftext(fig_footnote, prop = fp_text(font.size = 12, color = "black")),
#         fp_p = fp_par(text.align = "justify")
#       )
#     ),
#     location = ph_location(left = left_cap, top = top_cap, width = cap_w, height = cap_h))
# 
# try(unlink(paste0("ppt/f",fig_number,"_",fig_short_name,".pptx")))
# base::print(ppt, target = paste0("ppt/f",fig_number,"_",fig_short_name,".pptx"))
# #### END ####


##################################################
# APPENDIX
##################################################

#### REMOTE WORK ACROSS COUNTRIES - UNWEIGHTED ####
remove(list = ls())
load(file = "./ppt/ggplots/rwa_country_ts_w_unweighted_month.RData")
fig_number <- "A1"
fig_number_title <- paste0("Figure ", fig_number, ": ")
fig_short_name <- "wham_by_country_unweighted"
fig_title <- "Appendix: Raw Share of Remote Work Vacancies across Countries"
fig_footnote <- "BLah Blah Blah BLah Blah Blah BLah Blah Blah BLah Blah Blah BLah Blah Blah BLah Blah Blah"
p_dml <- rvg::dml(ggobj = p)
ppt <- officer::read_pptx() %>%
  officer::add_slide(layout = "Title and Content", master = "Office Theme")
s_s <- slide_size(ppt)
(s_w <- s_s$width) # width of slides
(s_h <- s_s$height) # height of slides
f_w <- 7 # width of figure
f_h <- 5 # height of figure
left_f <- (s_w/2) - (f_w/2) 
top_f <- (s_h/2) - (f_h/2)
cap_w <- 9
cap_h <- 3
left_cap <-  (s_w/2) - (cap_w/2) 
top_cap <- (s_h/2) + (f_h/2) - 1
ppt <- ppt %>%
  ph_with(p_dml, ph_location(left = left_f, top = top_f + 0.15, width = f_w, height = f_h)) %>%
  ph_with(
    block_list(
      fpar(
        ftext(fig_number_title, prop = fp_text(bold = TRUE, font.size = 20, color = "black")),
        ftext(fig_title, prop = fp_text(font.size = 20, color = "black")),
        fp_p = fp_par(text.align = "justify")
      )
    ),
    location = ph_location_type(type = "title")
  ) %>%
  ph_with(
    block_list(
      fpar(
        ftext("Note: ", prop = fp_text(bold = TRUE, font.size = 12, color = "black")),
        ftext(fig_footnote, prop = fp_text(font.size = 12, color = "black")),
        fp_p = fp_par(text.align = "justify")
      )
    ),
    location = ph_location(left = left_cap, top = top_cap, width = cap_w, height = cap_h))
try(unlink(paste0("ppt/f",fig_number,"_",fig_short_name,".pptx")))
base::print(ppt, target = paste0("ppt/f",fig_number,"_",fig_short_name,".pptx"))
#### END ####

#### REMOTE WORK ACROSS COUNTRIES - JOB WEIGHTED ####
# remove(list = ls())
# load(file = "./ppt/ggplots/rwa_country_ts_w_us_emp_month.RData")
# fig_number <- "A2"
# fig_number_title <- paste0("Figure ", fig_number, ": ")
# fig_short_name <- "wham_by_country_us_emp_weight"
# fig_title <- "Appendix: Share of Remote Work Vacancies across Countries, Reweighted to Match US Employment Occ Dist"
# fig_footnote <- "BLah Blah Blah BLah Blah Blah BLah Blah Blah BLah Blah Blah BLah Blah Blah BLah Blah Blah"
# p_dml <- rvg::dml(ggobj = p)
# ppt <- officer::read_pptx() %>%
#   officer::add_slide(layout = "Title and Content", master = "Office Theme")
# s_s <- slide_size(ppt)
# (s_w <- s_s$width) # width of slides
# (s_h <- s_s$height) # height of slides
# f_w <- 7 # width of figure
# f_h <- 5 # height of figure
# left_f <- (s_w/2) - (f_w/2) 
# top_f <- (s_h/2) - (f_h/2)
# cap_w <- 9
# cap_h <- 3
# left_cap <-  (s_w/2) - (cap_w/2) 
# top_cap <- (s_h/2) + (f_h/2) - 1
# ppt <- ppt %>%
#   ph_with(p_dml, ph_location(left = left_f, top = top_f + 0.15, width = f_w, height = f_h)) %>%
#   ph_with(
#     block_list(
#       fpar(
#         ftext(fig_number_title, prop = fp_text(bold = TRUE, font.size = 20, color = "black")),
#         ftext(fig_title, prop = fp_text(font.size = 20, color = "black")),
#         fp_p = fp_par(text.align = "justify")
#       )
#     ),
#     location = ph_location_type(type = "title")
#   ) %>%
#   ph_with(
#     block_list(
#       fpar(
#         ftext("Note: ", prop = fp_text(bold = TRUE, font.size = 12, color = "black")),
#         ftext(fig_footnote, prop = fp_text(font.size = 12, color = "black")),
#         fp_p = fp_par(text.align = "justify")
#       )
#     ),
#     location = ph_location(left = left_cap, top = top_cap, width = cap_w, height = cap_h))
# try(unlink(paste0("ppt/f",fig_number,"_",fig_short_name,".pptx")))
# base::print(ppt, target = paste0("ppt/f",fig_number,"_",fig_short_name,".pptx"))
#### END ####






