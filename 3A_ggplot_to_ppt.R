#### SETUP ####
#remove(list = ls())
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
remove(list = setdiff(ls(), "df_acs_wham_ag"))

##################################################
# RESULT
##################################################

#### 1 REMOTE WORK ACROSS COUNTRIES - UNWEIGHTED ####
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
load(file = "./ppt/ggplots/occ_dist_alt.pdf.RData")
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

#### ACS vs WHAM ####
load(file = "./ppt/ggplots/wham_vs_acs.RData")

fig_number <- 10
fig_number_title <- paste0("Figure ", fig_number, ": ")

fig_short_name <- "wham_vs_acs"

fig_title <- "Share of Online Job Vacancy Postings which Advertise Remote Work (2022) vs Share of Employees who reported ‘Mostly Working from Home’ in the 2021 American Community Survey (ACS), by MSA"

fig_footnote <- "The y-axis shows our measurement of the share of all new job vacancy postings which explicitly advertise remote working arrangements, by MSA. The x-axis shows the share of employees in the American Communities Survey (ACS) who responded that they `mostly worked from home'.  This latter series is constructed using sampling weights, and in all cases we take the ACS' best effort to calculate the point-estimate for the population based on their survey data.  We cut off one substantial outlier, but do not remove any outliers when calculating fit statistics.  Roughly half of all MSAs were removed due to missing values in the ACS."

p
p_dml <- rvg::dml(ggobj = p)

p_dml

# initialize PowerPoint slide ----
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

# Print PPT
try(unlink(paste0("ppt/f",fig_number,"_",fig_short_name,".pptx")))
base::print(ppt, target = paste0("ppt/f",fig_number,"_",fig_short_name,".pptx"))
#### END ####












