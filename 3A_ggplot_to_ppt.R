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
# font_import()
#loadfonts(device="postscript")
#fonts()
library('officer')
library('rvg')
library('here')

system("cd /mnt/disks/pdisk/bgt_code_repo/")
system("git add .")
system("git commit -a --allow-empty-message -m ''")
system("git push origin master")

setDTthreads(4)
getDTthreads()
#quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg_combined/")

remove(list = setdiff(ls(), "df_acs_wham_ag"))

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













