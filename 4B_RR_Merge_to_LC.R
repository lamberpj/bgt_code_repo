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
library("egg")
library("extrafont")
library("fixest")
library("haven")
library("phonics")
library("PGRdup")
library("quanteda")
library("qdapDictionaries")
# font_import()
#loadfonts(device="postscript")
#fonts()
gc()
setDTthreads(2)
getDTthreads()
#quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg-us/")

# system("gsutil -m cp gs://for_transfer/data_ingest/bgt_upload/US_stru/* /mnt/disks/pdisk/bg-us/raw_data/main/")
# system("gsutil -m cp gs://for_transfer/data_ingest/rr_d_and_b/* /mnt/disks/pdisk/bg-us/raw_data/rr_dandb/")

#### LOAD "US BGT" ####
paths <- list.files("/mnt/disks/pdisk/bg-us/raw_data/main", pattern = ".zip", full.names = T)
paths
source("/mnt/disks/pdisk/bgt_code_repo/old/safe_mclapply.R")

df_stru <- safe_mclapply(1:length(paths), function(i) {
  warning(paste0("\nSTART: ",i,"\n"))
  system(paste0("unzip -o ",paths[i]," -d ./raw_data/main/"))
  path_csv <- gsub(".zip", ".txt", paths[i])
  df <- fread(path_csv, nThread = 1, colClasses = "character", stringsAsFactors = FALSE,
              select = c("BGTJobId", "JobDate", "Employer", "Sector",
                          "NAICS3", "NAICS4", "State", "City", "County", "FIPS")) %>%
    clean_names %>%
    setDT(.) %>%
    rename(job_id = bgt_job_id) %>%
    .[, job_id := as.numeric(job_id)]
  
  unlink(path_csv)
  
  df[df == "na"] <- NA
  df[df == "-999"] <- NA
  df[df == "-999.00"] <- NA
  df[df == ""] <- NA
  df <- df[employer != "" & !is.na(employer)]

  df <- df %>% .[, job_ymd := ymd(job_date)]
  df <- df %>% .[, naics2 := str_sub(sector, 1, 2)]
  df <- df %>% .[, year := year(job_ymd)]

  df <- df %>% select(year, employer, naics2, naics3, naics4, state, city, county, fips)

  warning(paste0("\nDONE: ",i))
  return(df)
}, mc.cores = 6)

df_stru <- rbindlist(df_stru)
df_stru <- setDT(df_stru)

nrow(df_stru) # 298,934,832
df_stru <- df_stru %>%
  .[, (.N), by = .(year, employer, naics2, naics3, naics4, state, city, county, fips)]
df_stru <- df_stru %>% rename(n = V1)
nrow(df_stru) # 47,212,803
nrow(df_stru[n > 10]) # 4,286,675
df_stru <- df_stru %>% .[employer %in% df_stru[n > 10]$employer]
nrow(df_stru) # 39,361,748

fwrite(df_stru, "./aux_data/lc_usa_wfh_bigfirms_for_match.csv")

#### END ####

#### LOAD IN RR ####
remove(list = ls())
paths <- list.files("./raw_data/rr_dandb/", pattern = ".dta", full.names = T)
paths
years <- as.numeric(str_sub(paths, -8, -5))
years

df_rr_list <- mclapply(seq_along(paths), function(i) {
  read_dta(paths[[i]]) %>% setDT() %>% .[, year := years[i]]
}, mc.cores = 4)

# What are the columns common to all years?
common_cols <- Reduce(intersect, lapply(df_rr_list, colnames))

common_cols

df_rr_list <- lapply(df_rr_list, function(x) {x %>% select(all_of(common_cols))})

df_rr <- rbindlist(df_rr_list)

nrow(df_rr)

df_rr_primary <- df_rr %>% select(-secondaryname) %>% .[!is.na(companyname) & companyname != ""] %>% .[, name_type := "primary"]
df_rr_secondary <- df_rr %>% .[companyname != secondaryname] %>% select(-companyname) %>% rename(companyname = secondaryname) %>% .[!is.na(companyname) & companyname != ""] %>% .[, name_type := "secondary"]

df_rr <- rbindlist(list(df_rr_primary, df_rr_secondary))

nrow(df_rr) # 4,658,098

# Expand SIC codes
df_rr <- df_rr %>%
  .[, `:=`(primarysic = str_sub(primarysic, 1, 4), sic2 = str_sub(sic2, 1, 4), sic3 = str_sub(sic3, 1, 4), sic4 = str_sub(sic4, 1, 4), sic5 = str_sub(sic5, 1, 4), sic6 = str_sub(sic6, 1, 3))]

df_rr_primary_primarysic <- df_rr %>% setDT(.) %>% select(-c(sic2, sic3, sic4, sic5, sic6)) %>% rename(sic = primarysic) %>% .[!is.na(sic) & sic != ""] %>% .[, sic_type := "primary"]
df_rr_primary_sic2 <- df_rr %>% setDT(.) %>% select(-c(primarysic, sic3, sic4, sic5, sic6)) %>% rename(sic = sic2) %>% .[!is.na(sic) & sic != ""] %>% .[, sic_type := "sic2"]
df_rr_primary_sic3 <- df_rr %>% setDT(.) %>% select(-c(primarysic, sic2, sic4, sic5, sic6)) %>% rename(sic = sic3) %>% .[!is.na(sic) & sic != ""] %>% .[, sic_type := "sic3"]
df_rr_primary_sic4 <- df_rr %>% setDT(.) %>% select(-c(primarysic, sic2, sic3, sic5, sic6)) %>% rename(sic = sic4) %>% .[!is.na(sic) & sic != ""] %>% .[, sic_type := "sic4"]
df_rr_primary_sic5 <- df_rr %>% setDT(.) %>% select(-c(primarysic, sic2, sic3, sic4, sic6)) %>% rename(sic = sic5) %>% .[!is.na(sic) & sic != ""] %>% .[, sic_type := "sic5"]
df_rr_primary_sic6 <- df_rr %>% setDT(.) %>% select(-c(primarysic, sic2, sic3, sic4, sic5)) %>% rename(sic = sic6) %>% .[!is.na(sic) & sic != ""] %>% .[, sic_type := "sic6"]

df_rr <- rbindlist(list(df_rr_primary_primarysic, df_rr_primary_sic2, df_rr_primary_sic3, df_rr_primary_sic4, df_rr_primary_sic5, df_rr_primary_sic6))

remove(list = setdiff(ls(), "df_rr"))
nrow(df_rr) # 5,608,604

df_rr <- df_rr %>%
  .[, drop := ifelse(duplicated(sic), 1, 0), by = .(subsid_dunsnumber, year, name_type, companyname, dbrowid)] %>%
  .[drop == 0] %>%
  select(-drop)

nrow(df_rr) # 5,608,604
colnames(df_rr)
df_rr <- df_rr %>% select(year, rr_company_id1, rr_company_id2, rr_company_id3, subsid_dunsnumber, companyname, dbcountycode, dbstatecode, sic, salesvolume, employeeshere, employeestotal, name_type, sic_type)

df_rr <- df_rr %>% .[order(subsid_dunsnumber, year)]

nrow(df_rr) # 4,658,098
df_rr <- df_rr %>%
  setDT(.) %>%
  .[, .(
    n = .N, 
    employeeshere = sum(employeeshere, na.rm = T),
    employeestotal = sum(employeestotal, na.rm = T),
    salesvolume = sum(salesvolume, na.rm = T)),
  by = .(year, rr_company_id1, rr_company_id2, rr_company_id3, subsid_dunsnumber, companyname, dbcountycode, dbstatecode, sic, name_type, sic_type)]
nrow(df_rr) # 5,460,978

fwrite(df_rr, "./aux_data/rr_for_match.csv")

#### END ####

#### PREPARE FOR MATCH ####
remove(list = ls())
df_us_large_emp <- fread(file = "./aux_data/lc_usa_wfh_bigfirms_for_match.csv", integer64 = "numeric") %>% .[year >= 2010 & year <= 2021]
df_rr <- fread(file = "./aux_data/rr_for_match.csv", integer64 = "numeric") %>% .[year >= 2010 & year <= 2021]

df_sic_naics <- fread("./aux_data/sic_to_naics.csv") %>% clean_names() %>% rename(primarysic = sic_code) %>%
  .[, primarysic := as.numeric(primarysic)] %>%
  .[, naics4 := str_sub(naics_code, 1, 4)] %>%
  select(primarysic, naics4) %>%
  rename(sic = primarysic) %>%
  unique(.) %>%
  .[!is.na(sic) & !is.na(naics4)]

nrow(df_rr) # 2,595,441
df_rr <- df_rr %>%
  left_join(df_sic_naics, multiple = "all") %>%
  setDT(.)
nrow(df_rr) # 10,040,898
colnames(df_rr)

#head(df_rr)

df_rr <- df_rr %>% .[, dbcountycode_full := paste0(dbstatecode,dbcountycode)]

fips_us_county_codes <- read_xls("./aux_data/fips_us_county_codes.xls", skip = 4) %>% clean_names() %>%
  mutate(fips = paste0(state_code, code_value)) %>% select(-c(state_code, code_value))
dnb_us_county_codes <- read_xls("./aux_data/dnb_us_county_codes.xls", skip = 4) %>% clean_names() %>%
  mutate(dbcountycode_full = paste0(state_code, code_value)) %>% select(-c(state_code, code_value)) %>%
  mutate(county = str_sub(definition, 1, -6))

head(fips_us_county_codes)
head(dnb_us_county_codes)

nrow(fips_us_county_codes) # 3082
fips_us_county_codes <- fips_us_county_codes %>%
  inner_join(dnb_us_county_codes)
nrow(fips_us_county_codes) # 3082

rm(dnb_us_county_codes)

fips_us_county_codes <- fips_us_county_codes %>% select(-definition)

df_us_large_emp$fips <- as.numeric(df_us_large_emp$fips)
fips_us_county_codes$fips <- as.numeric(fips_us_county_codes$fips)

#head(df_rr)
#head(fips_us_county_codes)

df_rr$dbcountycode_full <- as.numeric(df_rr$dbcountycode_full)
fips_us_county_codes$dbcountycode_full <- as.numeric(fips_us_county_codes$dbcountycode_full)

nrow(df_rr) # 10,040,898
df_rr <- df_rr %>%
  left_join(fips_us_county_codes, by = "dbcountycode_full")
nrow(df_rr) # 10,040,898

nrow(df_us_large_emp) # 30,163,493
df_us_large_emp <- df_us_large_emp %>%
  left_join(fips_us_county_codes)
nrow(df_us_large_emp) # 30,163,493

# Fix earlier codes issue with D&B
#head(df_rr)
mean(is.na(df_rr$fips)) # 0.2132435
mean(is.na(df_rr$dbcountycode)) # 0.009508612

#head(df_rr[is.na(fips) & !is.na(dbcountycode)])

us_state_codes <- read_xls("./aux_data/dnb_numeric_state_codes.xls", skip = 2) %>% clean_names() %>%
  rename(dbstatecode = code_value, state = definition) %>%
  mutate(dbstatecode = as.numeric(dbstatecode))

df_rr <- df_rr %>% .[, dbstatecode := as.numeric(dbstatecode)]

nrow(df_rr) # 10,040,898
df_rr <- df_rr %>%
  left_join(us_state_codes)
nrow(df_rr) # 10,040,898

df_rr <- df_rr %>% setDT(.)

# Create a named vector that includes the full state names and abbreviations
full_states <- c(state.name, "District of Columbia", "Puerto Rico", "Virgin Islands", "Virgin Islands of the U.S.", "North Caorlina")
abbreviations <- c(state.abb, "DC", "PR", "VI", "VI", "NC")

# Function to convert state names to their codes
convert_state_name_to_code <- function(state_name){
    state_name <- tolower(state_name)
    states_lower <- tolower(full_states)
    state_code <- abbreviations[match(state_name, states_lower)]
    return(state_code)
}

df_rr$state_code <- convert_state_name_to_code(df_rr$state)
df_us_large_emp$state_code <- convert_state_name_to_code(df_us_large_emp$state)

unique(df_us_large_emp[is.na(state_code) & !is.na(state)]$state)

#head(df_rr)
#head(df_us_large_emp)

df_rr <- df_rr %>% select(-c(dbcountycode, dbstatecode, dbcountycode_full, state, sic))
df_us_large_emp <- df_us_large_emp %>% select(-c(city, state, dbcountycode_full))

df_rr <- df_rr %>% select(year, everything(), naics4, fips, county, state_code)
df_us_large_emp <- df_us_large_emp %>% select(year, everything(), naics4, fips, county, state_code)

head(df_rr)
head(df_us_large_emp)

# CLEAN STRING NAMES
clean_business_name <- function(business_names) {
  # List of common words to remove
  common_words <- c("inc", "llc", "corp", "corp", "corporation", "company", "ltd", "co", "plc", "gmbh", "sa", "pte", "nv", "ag", "srl", "kgaa", "og", "eg", "ab", "oyj", "as", "asa", "sl", "rl", "bv")
  business_names <- iconv(business_names, to = "UTF-8") # Convert to UTF-8
  cleaned_names <- tolower(business_names) # Convert to lower case
  cleaned_names <- gsub("[\'-]", "", cleaned_names) # Remove apostrophes and hyphens
  cleaned_names <- gsub("[[:punct:]]", " ", cleaned_names) # Remove punctuation
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
clean_unqiue_dt$clean_name <- as.character(mclapply(clean_unqiue_dt$employer, clean_business_name, mc.cores = 8))
df_us_large_emp <- df_us_large_emp %>%
  left_join(clean_unqiue_dt) %>% setDT(.)
rm(clean_unqiue_dt)

clean_unqiue_dt <- df_rr %>% select(companyname) %>% unique(.)
clean_unqiue_dt$clean_name <- as.character(mclapply(clean_unqiue_dt$companyname, clean_business_name, mc.cores = 8))
df_rr <- df_rr %>%
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
    if (!is.na(word) & (nchar(word) == 3 | nchar(word) == 2)) {
      words[i] <- paste(unlist(strsplit(word, "")), collapse = " ")
    }}
  output_string <- paste(words, collapse = " ") # Concatenate the words back into a single string
  output_string <- stri_trim(output_string)
  output_string <- gsub("\\s+", " ", output_string)
  return(output_string)
}

abrv_unqiue_dt <- df_us_large_emp %>% select(clean_name) %>% distinct(clean_name)
abrv_unqiue_dt$clean_name_abv <- as.character(mclapply(abrv_unqiue_dt$clean_name, replace_non_dictionary_words, mc.cores = 8))
df_us_large_emp <- df_us_large_emp %>%
  left_join(abrv_unqiue_dt) %>% setDT(.)
rm(abrv_unqiue_dt)

abrv_unqiue_dt <- df_rr %>% select(clean_name) %>% distinct(clean_name)
abrv_unqiue_dt$clean_name_abv <- as.character(mclapply(abrv_unqiue_dt$clean_name, replace_non_dictionary_words, mc.cores = 8))
df_rr <- df_rr %>%
  left_join(abrv_unqiue_dt) %>% setDT(.)
rm(abrv_unqiue_dt)

# Check exact matches
mean(unique(df_rr$clean_name) %in% unique(df_us_large_emp$clean_name))*100 # 4.311791
mean(unique(df_rr$clean_name_abv) %in% unique(df_us_large_emp$clean_name_abv))*100 # 4.408843
mean(unique(df_us_large_emp$clean_name_abv) %in% unique(df_rr$clean_name_abv))*100 # 2.377276

mean(is.na(df_us_large_emp$clean_name_abv))
mean(is.na(df_rr$clean_name_abv))

# Remove strings where it will be impossible to do fuzzy matching
df_us_large_emp <- df_us_large_emp %>% .[!is.na(clean_name_abv)]
df_rr <- df_rr %>% .[!is.na(clean_name_abv)]

df_us_large_emp <- df_us_large_emp %>% .[clean_name_abv!=""]
df_rr <- df_rr %>% .[clean_name_abv!=""]

df_us_large_emp <- df_us_large_emp %>% .[nchar(clean_name_abv)>2]
df_rr <- df_rr %>% .[nchar(clean_name_abv)>2]

# Create some alternative string representations
# Fingerprint
generate_2gram_fingerprint <- function(input_string) {
    input_string <- iconv(input_string, "latin1", "ASCII", sub=" ") # Convert to UFT-8
    if(nchar(input_string) == 0 | is.na(input_string)) return("") # If the string is empty, return an empty string
    input_string <- tolower(input_string) # Convert to lower case
    bigrams <- substring(input_string, seq(1, nchar(input_string) - 1), seq(2, nchar(input_string))) # Generate 2-grams
    fingerprint <- paste(sort(unique(bigrams)), collapse = " ") # Sort the 2-grams and concatenate them to form the fingerprint
    return(fingerprint)
}
# Double Metaphone
double_metaphone_multi_word <- function(input_string) {
    input_string <- iconv(input_string, "latin1", "ASCII", sub=" ") # Convert to UFT-8
    if(nchar(input_string) == 0 | is.na(input_string)) return("") # If the string is empty, return an empty string
    words <- unlist(strsplit(input_string, " ")) # Split the input string into individual words
    encoded_words <- sapply(words, function(x) {DoubleMetaphone(x)$primary}) # Apply Double Metaphone encoding to each word
    encoded_string <- paste(encoded_words, collapse = " ") # Concatenate the encoded words with spaces
    return(encoded_string)
}

fp_unique_dt <- df_us_large_emp %>% select(clean_name_abv) %>% distinct(clean_name_abv)
fp_unique_dt$fp2 <- as.character(mclapply(fp_unique_dt$clean_name_abv, generate_2gram_fingerprint, mc.cores = 8))
df_us_large_emp <- df_us_large_emp %>%
  left_join(fp_unique_dt) %>% setDT(.)
rm(fp_unique_dt)

fp_unique_dt <- df_rr %>% select(clean_name_abv) %>% distinct(clean_name_abv)
fp_unique_dt$fp2 <- as.character(mclapply(fp_unique_dt$clean_name_abv, generate_2gram_fingerprint, mc.cores = 8))
df_rr <- df_rr %>%
  left_join(fp_unique_dt) %>% setDT(.)
rm(fp_unique_dt)

metaph_unique_dt <- df_us_large_emp %>% select(clean_name_abv) %>% distinct(clean_name_abv)
metaph_unique_dt$double_metaphone <- as.character(mclapply(metaph_unique_dt$clean_name_abv, double_metaphone_multi_word, mc.cores = 8))
df_us_large_emp <- df_us_large_emp %>%
  left_join(metaph_unique_dt) %>% setDT(.)
rm(metaph_unique_dt)

metaph_unique_dt <- df_rr %>% select(clean_name_abv) %>% distinct(clean_name_abv)
metaph_unique_dt$double_metaphone <- as.character(mclapply(metaph_unique_dt$clean_name_abv, double_metaphone_multi_word, mc.cores = 8))
df_rr <- df_rr %>%
  left_join(metaph_unique_dt) %>% setDT(.)
rm(metaph_unique_dt)

# Find list of 250 common words in name_clean, and then create name_clean with only these, and name_clean without any of these
dfm_rr_clean_names <- corpus(unique(df_rr$clean_name)) %>% tokens() %>% dfm()
top_features <- as.data.frame(topfeatures(dfm_rr_clean_names, 250), row_names = "name") %>% rownames_to_column("names") %>% rename(count = colnames(.)[2])
top_features
# Make top_features terms into a call to remove for stringi
top_features$names <- paste0("\\b", top_features$names, "\\b")
terms <- paste0(top_features$names, collapse = "|")

# Remove the top 250 terms from the name_clean column

narrow_unique_dt <- df_us_large_emp %>% select(clean_name) %>% distinct(clean_name)
narrow_unique_dt$name_clean_narrow <- as.character(mclapply(narrow_unique_dt$clean_name, function(x) {str_squish(str_trim(str_replace_all(x, terms, " ")))}, mc.cores = 8))
df_us_large_emp <- df_us_large_emp %>%
  left_join(narrow_unique_dt) %>% setDT(.)
rm(narrow_unique_dt)

narrow_unique_dt <- df_rr %>% select(clean_name) %>% distinct(clean_name)
narrow_unique_dt$name_clean_narrow <- as.character(mclapply(narrow_unique_dt$clean_name, function(x) {str_squish(str_trim(str_replace_all(x, terms, " ")))}, mc.cores = 8))
df_rr <- df_rr %>%
  left_join(narrow_unique_dt) %>% setDT(.)
rm(narrow_unique_dt)

# Keep only these terms using string extract
broad_unique_dt <- df_us_large_emp %>% select(clean_name) %>% distinct(clean_name)
broad_unique_dt$name_clean_broad <- as.character(lapply(broad_unique_dt$clean_name, function(x) {paste0(unlist(str_extract_all(x, terms)), collapse = " ")}))
df_us_large_emp <- df_us_large_emp %>%
  left_join(broad_unique_dt) %>% setDT(.)
rm(broad_unique_dt)

broad_unique_dt <- df_rr %>% select(clean_name) %>% distinct(clean_name)
broad_unique_dt$name_clean_broad <- as.character(lapply(broad_unique_dt$clean_name, function(x) {paste0(unlist(str_extract_all(x, terms)), collapse = " ")}))
df_rr <- df_rr %>%
  left_join(broad_unique_dt) %>% setDT(.)
rm(broad_unique_dt)

View(tail(df_us_large_emp, 10000))
View(tail(df_rr, 10000))

fwrite(df_us_large_emp, "./int_data/lc_us_large_emp_clean_expanded.csv")
fwrite(df_rr, "./int_data/rr_us_large_emp_clean_expanded.csv")

library("arrow")

write_parquet(df_us_large_emp, "./int_data/lc_us_large_emp_clean_expanded.parquet")
write_parquet(df_rr, "./int_data/rr_us_large_emp_clean_expanded.parquet")

system("gsutil cp /mnt/disks/pdisk/bg-us/int_data/lc_us_large_emp_clean_expanded.parquet  gs://for_transfer/data_ingest/rr_d_and_b/")
system("gsutil cp /mnt/disks/pdisk/bg-us/int_data/rr_us_large_emp_clean_expanded.parquet  gs://for_transfer/data_ingest/rr_d_and_b/")

#### END ####

#### MATCH USING RECLIN" ####
remove(list = ls())
library("reclin2")

df_us_large_emp <- fread("./int_data/lc_us_large_emp_clean_expanded.csv")
df_rr <- fread("./int_data/rr_us_large_emp_clean_expanded.csv")

head(df_us_large_emp)

nrow(df_us_large_emp) # 30,161,312
uniqueN(df_us_large_emp$employer) # 275,730

head(df_rr)

colnames(df_us_large_emp)
colnames(df_rr)

df_us_large_emp <- df_us_large_emp %>% select(employer, naics3_m, fips, clean_name, clean_name_abv, fp2, double_metaphone) %>% distinct() %>% setDT(.)
df_rr <- df_rr %>% mutate(naics3_m = str_sub(naics4, 1, 3)) %>% select(companyname, subsid_dunsnumber, naics3_m, fips, clean_name, clean_name_abv, fp2, double_metaphone)  %>% distinct() %>% setDT(.)

df_us_large_emp <- df_us_large_emp %>% .[, fips_naics := paste0(fips,"_",naics3_m)]
df_rr <- df_rr %>% .[, fips_naics := paste0(fips,"_",naics3_m)]

df_us_large_emp <- df_us_large_emp %>% .[, c("clean_name2", "clean_name_abv2", "fp22", "double_metaphone2") := .(clean_name, clean_name_abv, fp2, double_metaphone)]
df_rr <- df_rr %>% .[, c("clean_name2", "clean_name_abv2", "fp22", "double_metaphone2") := .(clean_name, clean_name_abv, fp2, double_metaphone)]

colnames(df_us_large_emp)

nrow(df_us_large_emp) # 5,413,307
nrow(df_rr) # 1,060,545

df_us_large_emp <- df_us_large_emp[fips_naics %in% df_rr$fips_naics]
df_rr <- df_rr[fips_naics %in% df_us_large_emp$fips_naics]

nrow(df_us_large_emp) # 4,368,254
nrow(df_rr) # 1,020,063

uniqueN(df_us_large_emp$employer) # 344,228
uniqueN(df_rr$companyname) # 106,752

setDTthreads(2)
cl <- makeCluster(8)
pairs <- cluster_pair_blocking(cl, df_us_large_emp, df_rr, "fips_naics")

compare_pairs(pairs, on = c("clean_name", "clean_name_abv",  "fp2", "double_metaphone", "clean_name2", "clean_name_abv2", "fp22", "double_metaphone2"), 
              comparators = list(
                "clean_name" = jaro_winkler(0.75),
                "clean_name_abv" = jaro_winkler(0.75),
                "fp2" = jaro_winkler(0.75),
                "double_metaphone" = jaro_winkler(0.75),
                "clean_name2" = lcs(0.5),
                "clean_name_abv2" = lcs(0.5),
                "fp22" = lcs(0.5),
                "double_metaphone2" = lcs(0.5)
              ), inplace = TRUE)

m <- problink_em(~clean_name + clean_name_abv + fp2 + double_metaphone + clean_name2 + clean_name_abv2 + fp22 + double_metaphone2, data = pairs)

pairs <- predict(m, pairs = pairs, add = TRUE)

pairs <- select_threshold(pairs, "threshold", score = "weights",  threshold = 3)
pairs <- cluster_collect(pairs, "threshold", clear = TRUE)

quantile(pairs$weights, probs = c(0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 1))

pairs <- select_greedy(pairs, "weights", variable = "greedy", threshold = 0)
pairs <- pairs %>% setDT(.) %>% .[greedy == 1]
linked_data_set <- link(pairs)

nrow(linked_data_set) # 239,812

head(linked_data_set)
linked_data_set <- linked_data_set %>% setDT() %>% select(".x", ".y", employer, companyname, subsid_dunsnumber)
linked_data_set <- linked_data_set %>%
  left_join(pairs %>% select(".x", ".y", "weights")) %>%
  setDT()
fwrite(linked_data_set, paste0("./int_data/linked_data_set.csv"))
stopCluster(cl)

View(linked_data_set)



















#### Extract for DAVID AUTOR ####
df_us_autor <- df_us %>%
  setDT(.) %>%
  .[, year_month := as.yearmon(job_date)] %>%
  .[, .(wfh_share = mean(wfh_wham), n = .N), by = .(naics2, soc2, county, fips, year_month)] %>%
  .[, year := year(year_month)] %>%
  .[, month := month(year_month)] %>%
  select(year, month, year_month, soc2, naics2, county, fips, wfh_share, n)
  
df_us_autor <- df_us_autor %>%
  .[order(year, month, year_month, soc2, naics2, county, fips)]

fwrite(df_us_autor, "./int_data/extract_autor_et_al.csv")

View(df_us_autor)

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
