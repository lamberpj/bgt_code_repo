#### SETUP ####
options(repos = c(CRAN = "https://mran.revolutionanalytics.com/snapshot/2021-04-01"))



options(scipen=999)

library("data.table")
library("tidyverse")
library("janitor")
library("lubridate")
library("doParallel")
#library("lsa")
#library("fuzzyjoin")
#library("quanteda")
library("refinr")
#library("FactoMineR")
library("ggpubr")
library("scales")
#install.packages("ggthemr")
#ggthemr('flat')
library("fixest")
library("lfe")
library("stargazer")
library("texreg")
library("sjPlot")
library("margins")
library("DescTools")
library("fuzzyjoin")

# Clean html packages
library("htmltidy")

# Translation packages
library("rvest")
library("googleLanguageR")
library("cld2")
library("cld3")
library("datasets")
library("vroom")

#options(repos = c(CRAN = "https://cran.microsoft.com/snapshot/2021-09-05/"))
#install.packages("cld3")
#options(repos = c(CRAN = "https://mran.revolutionanalytics.com/snapshot/2021-04-01"))
library("cld3")

?cld3::detect_language

setwd("/mnt/disks/pdisk/bg-eu/translate/")

#### LOADS DATA from ML FALSE NEGATIVES ####

df <- fread("./test_IT_Jobs_ML_text_noML_skills.csv")

colnames(df)

df <- df %>%
  select(-escoskill_level_3) %>%
  group_by(general_id) %>%
  filter(row_number() == 1) %>%
  ungroup()
#### end ####

#### CLEAN FOR TRANSLATE ####
tf <- function(frag) {
  tidy_html(frag) %>%   
    read_html() %>% 
    html_nodes(xpath="//*[not(self::script)]/text()") %>% 
    html_text() %>% 
    paste0(collapse = " ") %>% 
    str_replace_all(pattern = "\n", replacement = " ") %>%
    str_replace_all(pattern = "[\\^]", replacement = " ") %>%
    str_replace_all(pattern = "\"", replacement = " ") %>%
    str_replace_all(pattern = "\\s+", replacement = " ") %>%
    str_trim(side = "both")
}

df$description_clean <- lapply(1:nrow(df), function(i) {tf(df$description[i])})
df$title_clean <- lapply(1:nrow(df), function(i) {tf(df$title[i])})

df$title_clean[1]
df$title[1]

#### DETERMINE LANGUAGES ####
df <- df %>%
  mutate(title_lang = detect_language(title_clean, plain_text = F),
         description_lang = detect_language(description_clean, plain_text = F))

df <- df %>%
  mutate(title_lang_cld3 = cld3::detect_language(title_clean),
         description_lang_cld3 = cld3::detect_language(description_clean))
#### END ####

#### TRANSLATE 100 JOB TITLES ####
gl_auth("burning-glass-eu-1e673ec779c7.json")

df$title_english <- ""

#Check cost
sum(nchar(df$title_clean))
sum(nchar(df$description_clean))
df$title_clean <- as.character(df$title_clean)
df$description_clean <- as.character(df$description_clean)

df$title_english <- gl_translate(df$title_clean, target = "en")$translatedText
df$description_english <- gl_translate(df$description_clean, target = "en")$translatedText

fwrite(df, file = "./test_IT_Jobs_ML_text_noML_skills_translated.csv")

summary(nchar(df$title_clean))

head(df$title_clean)

#### END ####


#### SANDBOX ####





