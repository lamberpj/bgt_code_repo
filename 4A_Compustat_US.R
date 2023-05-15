#### COMPARE US STATES TO GMD ####
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
library(haven)
library(phonics)
library(PGRdup)
library(qdapDictionaries)
# font_import()
#loadfonts(device="postscript")
#fonts()

setDTthreads(2)
getDTthreads()
#quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg-us/")

#### LOAD "US BGT" ####
df_us_2019 <- fread("./int_data/us_stru_2019_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "US") %>% setDT() %>% .[!is.na(employer) & employer != ""]
df_us_2020 <- fread("./int_data/us_stru_2020_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "US") %>% setDT() %>% .[!is.na(employer) & employer != ""]
df_us_2021 <- fread("./int_data/us_stru_2021_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "US") %>% setDT() %>% .[!is.na(employer) & employer != ""]
df_us_2022 <- fread("./int_data/us_stru_2022_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "US") %>% setDT() %>% .[!is.na(employer) & employer != ""]
df_us_2023 <- fread("./int_data/us_stru_2023_wfh.csv", nThread = 8, integer64 = "numeric") %>% .[!is.na(bgt_occ) & bgt_occ != ""] %>% .[!is.na(wfh_wham) & wfh_wham != ""] %>% mutate(country = "US") %>% setDT() %>% .[!is.na(employer) & employer != ""]

df_us_2019 <- df_us_2019 %>% select(-year)
df_us_2020 <- df_us_2020 %>% select(-year)
df_us_2021 <- df_us_2021 %>% select(-year)
df_us_2022 <- df_us_2022 %>% select(-year)
#df_us_2023 <- df_us_2023 %>% select(-year)

df_us <- rbindlist(list(df_us_2019,df_us_2020,df_us_2021,df_us_2022,df_us_2023), use.names = T)
remove(list = c("df_us_2019","df_us_2020","df_us_2021","df_us_2022","df_us_2023"))

setwd("/mnt/disks/pdisk/bg-us/")

df_us <- df_us %>% .[, city_state := paste0(city,"_",state)]

uniqueN(df_us$city_state) # 31,568

df_us <- df_us[!is.na(job_domain) & job_domain != ""]
df_us <- df_us %>% .[!grepl("careerbuilder", job_domain)]

remove(list = setdiff(ls(), "df_us"))
ls()
colnames(df_us)

df_us <- df_us %>% select(job_id, job_date, employer, sector, naics3, naics4, city, state, city_state, county, fips, msa)
df_us <- df_us %>% mutate(naics2 = str_sub(sector, 1, 2))
df_us <- df_us %>% select(job_id, job_date, employer, naics2, naics3, naics4, city, state, city_state, county, fips, msa)

df_us <- df_us %>% .[!is.na(employer) & employer != ""]

df_us[df_us == ""] <- NA
df_us[, c(4:12)][df_us[, c(4:12)] == "NA"] <- NA

head(df_us)

Mode <- function(x) {
  ux <- unique(x[!is.na(x)])
  ux[which.max(tabulate(match(x, ux)))]
}

sapply(df_us[, c("naics2", "naics3", "naics4")], function(x) {mean(is.na(x))})

table(nchar(df_us$naics2))
table(nchar(df_us$naics3))
table(nchar(df_us$naics4))

df_us <- df_us %>%
  .[, `:=`(naics2_m = Mode(naics2), naics3_m = Mode(naics3), naics4_m=Mode(naics4)),
    by = .(employer)]

sapply(df_us[, c("naics2_m", "naics3_m", "naics4_m")], function(x) {mean(is.na(x))})

df_us <- df_us %>%
  .[, `:=`(naics2_cm = Mode(naics2), naics3_cm = Mode(naics3), naics4_cm=Mode(naics4)),
    by = .(county, employer)]

sapply(df_us[, c("naics2_cm", "naics3_cm", "naics4_cm")], function(x) {mean(is.na(x))})

colnames(df_us)

df_us <- df_us %>% select(job_id, job_date, employer, naics2, naics2_m, naics2_cm, naics3, naics3_m, naics3_cm, naics4, naics4_m, naics4_cm, state, county, fips)

df_us_firm_state <- df_us %>%
  .[, year := year(job_date)] %>%
  .[, .N, by = .(employer, state, year)] %>%
  .[N >= 10]

nrow(df_us) # 120,435,466
uniqueN(df_us$employer) # 3,296,967
df_us_large_emp <- df_us %>% .[employer %in% df_us_firm_state$employer]
nrow(df_us_large_emp) # 109,593,805
uniqueN(df_us_large_emp$employer) # 368,381

fwrite(df_us_large_emp, file = "./int_data/lc_usa_wfh_bigfirms_for_match.csv")

#### END ####

#### MATCH ####
remove(list = ls())
df_us_large_emp <- fread(file = "./int_data/lc_usa_wfh_bigfirms_for_match.csv")

head(df_us_large_emp)

nrow(df_us_large_emp) # 109,593,805
df_us_large_emp <- df_us_large_emp %>%
  .[, .N, by = .(employer, naics2, naics2_m, naics2_cm, naics3, naics3_m, naics3_cm, naics4, naics4_m, naics4_cm, fips)]
nrow(df_us_large_emp) # 9,163,351

db_cs_2021 <- read_dta("./int_data/d_and_b/DB-Compustat_data_2021.dta") %>% clean_names() %>%
  setDT(.) %>%
  .[, primarysic := as.numeric(primarysic)]

df_sic_naics <- fread("./aux_data/sic_to_naics.csv") %>% clean_names() %>% rename(primarysic = sic_code) %>%
  .[, primarysic := as.numeric(primarysic)] %>%
  .[, naics4 := str_sub(naics_code, 1, 4)] %>%
  select(primarysic, naics4) %>%
  unique(.)

nrow(db_cs_2021) # 511,693
db_cs_2021 <- db_cs_2021 %>%
  left_join(df_sic_naics, multiple = "all") %>%
  setDT(.)
nrow(db_cs_2021) # 1,042,164

db_cs_2021 <- db_cs_2021 %>% select(dbrowid, companyname, secondaryname, physicalstatecode, dbstatecode, dbcountycode, physicalstreetaddress, naics4)
db_cs_2021 <- db_cs_2021 %>% .[, dbcountycode_full := paste0(dbstatecode,dbcountycode)]

fips_us_county_codes <- read_xls("./aux_data/fips_us_county_codes.xls", skip = 4) %>% clean_names() %>%
  mutate(fips = paste0(state_code, code_value)) %>% select(-c(state_code, code_value))
dnb_us_county_codes <- read_xls("./aux_data/dnb_us_county_codes.xls", skip = 4) %>% clean_names() %>%
  mutate(dbcountycode_full = paste0(state_code, code_value)) %>% select(-c(state_code, code_value))

nrow(fips_us_county_codes) # 3219
fips_us_county_codes <- fips_us_county_codes %>%
  left_join(dnb_us_county_codes)
nrow(fips_us_county_codes) # 3219

rm(dnb_us_county_codes)

df_us_large_emp$fips <- as.numeric(df_us_large_emp$fips)
fips_us_county_codes$fips <- as.numeric(fips_us_county_codes$fips)

nrow(df_us_large_emp) # 109,593,805
df_us_large_emp <- df_us_large_emp %>%
  left_join(fips_us_county_codes)
nrow(df_us_large_emp) # 109,593,805

# CLEAN STRING NAMES
clean_business_name <- function(business_names) {
  # List of common words to remove
  common_words <- c("inc", "llc", "corp", "company", "ltd", "co", "plc", "gmbh", "sa", "pte", "nv", "ag", "srl", "kgaa", "og", "eg", "ab", "oyj", "as", "asa", "sl", "rl", "bv")
  business_names <- iconv(business_names, to = "UTF-8") # Convert to UTF-8
  cleaned_names <- tolower(business_names) # Convert to lower case
  cleaned_names <- gsub("[\'-]", "", cleaned_names) # Remove apostrophes and hyphens
  cleaned_names <- gsub("[[:punct:]]", "", cleaned_names) # Remove punctuation
  #cleaned_names <- gsub("\\d", "", cleaned_names) # Remove digits
  cleaned_names <- stri_trim(cleaned_names)
  cleaned_names <- gsub("\\s+", " ", cleaned_names)
  # Remove common words
  for(word in common_words) {
    cleaned_names <- gsub(paste0("\\b", word, "\\b"), "", cleaned_names)
  }
  cleaned_names <- trimws(cleaned_names) # Remove leading and trailing white spaces
  return(cleaned_names)
}

# Clean strings
clean_unqiue_dt <- df_us_large_emp %>% select(employer) %>% unique(.)
clean_unqiue_dt$clean_name <- mclapply(clean_unqiue_dt$employer, clean_business_name, mc.cores = 8)
df_us_large_emp <- df_us_large_emp %>%
  left_join(clean_unqiue_dt) %>% setDT(.)
rm(clean_unqiue_dt)

clean_unqiue_dt <- db_cs_2021 %>% select(companyname) %>% unique(.)
clean_unqiue_dt$clean_name <- mclapply(clean_unqiue_dt$companyname, clean_business_name, mc.cores = 8)
db_cs_2021 <- db_cs_2021 %>%
  left_join(clean_unqiue_dt) %>% setDT(.)
rm(clean_unqiue_dt)

# Add spaces between letters in non-words with 2-3 characters
replace_non_dictionary_words <- function(input_string) {
  english_words <- tolower(GradyAugmented[nchar(GradyAugmented)<=3]) # Get the list of English words from the GradyAugmented dataset
  words <- unlist(strsplit(input_string, " ")) # Split the input string into individual words
  # Process each word
  for (i in seq_along(words)) {
    word <- words[i]
    # If the word is 3 characters or less and not an English word,
    # replace it with the same word but with spaces between each letter
    if (!is.na(word) & ((nchar(word) == 3 & !(tolower(word) %in% c(english_words, "usa"))) | nchar(word) == 2)) {
      words[i] <- paste(unlist(strsplit(word, "")), collapse = " ")
    }}
  output_string <- paste(words, collapse = " ") # Concatenate the words back into a single string
  output_string <- stri_trim(output_string)
  output_string <- gsub("\\s+", " ", output_string)
  return(output_string)
}

# replace_non_dictionary_words("special ed therapy")

abrv_unqiue_dt <- df_us_large_emp %>% select(clean_name) %>% distinct(clean_name)
abrv_unqiue_dt$clean_name_abv <- mclapply(abrv_unqiue_dt$clean_name, replace_non_dictionary_words, mc.cores = 8)
df_us_large_emp <- df_us_large_emp %>%
  left_join(abrv_unqiue_dt) %>% setDT(.)
rm(abrv_unqiue_dt)

abrv_unqiue_dt <- db_cs_2021 %>% select(clean_name) %>% distinct(clean_name)
abrv_unqiue_dt$clean_name_abv <- mclapply(abrv_unqiue_dt$clean_name, replace_non_dictionary_words, mc.cores = 8)
db_cs_2021 <- db_cs_2021 %>%
  left_join(abrv_unqiue_dt) %>% setDT(.)
rm(abrv_unqiue_dt)

# Check exact matches
mean(unique(db_cs_2021$clean_name_abv) %in% unique(df_us_large_emp$clean_name_abv))*100 # 4.824431
mean(unique(df_us_large_emp$clean_name_abv) %in% unique(db_cs_2021$clean_name_abv))*100 # 1.490731

mean(is.na(df_us_large_emp$clean_name_abv))
mean(is.na(db_cs_2021$clean_name_abv))

# Remove strings where it will be impossible to do fuzzy matching
df_us_large_emp <- df_us_large_emp %>% .[!is.na(clean_name_abv)]
db_cs_2021 <- db_cs_2021 %>% .[!is.na(clean_name_abv)]

df_us_large_emp <- df_us_large_emp %>% .[clean_name_abv!=""]
db_cs_2021 <- db_cs_2021 %>% .[clean_name_abv!=""]

df_us_large_emp <- df_us_large_emp %>% .[nchar(clean_name_abv)>2]
db_cs_2021 <- db_cs_2021 %>% .[nchar(clean_name_abv)>2]

# Fingerprint
generate_2gram_fingerprint <- function(input_string) {
  input_string <- tolower(input_string) # Convert to lower case
  bigrams <- substring(input_string, seq(1, nchar(input_string) - 1), seq(2, nchar(input_string))) # Generate 2-grams
  fingerprint <- paste(sort(unique(bigrams)), collapse = " ") # Sort the 2-grams and concatenate them to form the fingerprint
  return(fingerprint)
}

fp_unique_dt <- df_us_large_emp %>% select(clean_name_abv) %>% distinct(clean_name_abv)
fp_unique_dt$fp2 <- mclapply(fp_unique_dt$clean_name_abv, generate_2gram_fingerprint, mc.cores = 8)
df_us_large_emp <- df_us_large_emp %>%
  left_join(fp_unique_dt) %>% setDT(.)
rm(fp_unique_dt)

fp_unique_dt <- db_cs_2021 %>% select(clean_name_abv) %>% distinct(clean_name_abv)
fp_unique_dt$fp2 <- mclapply(fp_unique_dt$clean_name_abv, generate_2gram_fingerprint, mc.cores = 8)
db_cs_2021 <- db_cs_2021 %>%
  left_join(fp_unique_dt) %>% setDT(.)
rm(fp_unique_dt)

# Metaphone
double_metaphone_multi_word <- function(input_string) {
  words <- unlist(strsplit(input_string, " ")) # Split the input string into individual words
  encoded_words <- sapply(words, function(x) {DoubleMetaphone(x)}) # Apply Double Metaphone encoding to each word
  encoded_string <- paste(encoded_words, collapse = " ") # Concatenate the encoded words with spaces
  return(encoded_string)
}

metaph_unique_dt <- df_us_large_emp %>% select(clean_name_abv) %>% distinct(clean_name_abv)
metaph_unique_dt$double_metaphone <- mclapply(metaph_unique_dt$clean_name_abv, double_metaphone_multi_word, mc.cores = 8)
df_us_large_emp <- df_us_large_emp %>%
  left_join(metaph_unique_dt) %>% setDT(.)
rm(metaph_unique_dt)

metaph_unique_dt <- db_cs_2021 %>% select(clean_name_abv) %>% distinct(clean_name_abv)
metaph_unique_dt$double_metaphone <- mclapply(metaph_unique_dt$clean_name_abv, double_metaphone_multi_word, mc.cores = 8)
db_cs_2021 <- db_cs_2021 %>%
  left_join(metaph_unique_dt) %>% setDT(.)
rm(metaph_unique_dt)

fwrite(df_us_large_emp, "./int_data/us_stru_2019_2023_clean_names.csv")
fwrite(db_cs_2021, "./int_data/db_compustat_data_2021_clean_names.csv")

View(tail(df_us_large_emp, 10000))

#### END ####










