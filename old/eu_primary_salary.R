#### SETUP ####
options(repos = c(CRAN = "https://mran.revolutionanalytics.com/snapshot/2021-04-01"))

library("data.table")
library("tidyverse")
library("janitor")
library("lubridate")
library("doParallel")
library("lsa")
library("fuzzyjoin")
library("quanteda")
library("refinr")

library("FactoMineR")
library("ggpubr")
library("scales")
#install.packages("ggthemr")
#ggthemr('flat')

setwd("/mnt/disks/pdisk/bg-eu/")

#### DAVIS APPROACH #####

#### file paths ####
remove(list = ls())
file_names <- list.files("./bg-eu-bucket/new/structured", "*.csv", full.names = F)
file_names <- as.data.frame(file_names) %>% separate(file_names, sep = " ", c("type", "country", "file"), remove = F)
file_names$file_names <- as.data.frame(paths <- list.files("./bg-eu-bucket/new/structured", "*.csv", full.names = T))$paths
colnames(file_names)
file_names <- file_names %>% filter(!grepl("2020Q3|202101", file_names))
file_names <- file_names %>% filter(grepl("DE|FR|NL|BE|IT|IE|SE|PL", country))
countries <- file_names$country %>% unique()

m <- length(countries)
i = 1
#### /end ####

#### compile data ####
mclapply(1:m, function(i) {
  y1 <- lapply(file_names$file_names[file_names$type == "postings" & file_names$country == countries[[i]]], function(x) {
    fread(paste0(x), data.table = F, nThread = 1, select = c("general_id","year_grab_date","month_grab_date","day_grab_date",
                                                             "salary", "idesco_level_4", "idcountry"))}) %>% bind_rows %>% clean_names
  
  y2 <- lapply(file_names$file_names[file_names$type == "skills" & file_names$country == countries[[i]]], function(x) {
    fread(paste0(x), data.table = F, nThread = 1)}) %>% bind_rows %>% clean_names %>%
    select(general_id, escoskill_level_3) %>%
    mutate(escoskill_level_3 = str_squish(toupper(escoskill_level_3)))
  
  y2[y2 == ""] <- NA
  
  y2 <- y2 %>%
    filter(!is.na(escoskill_level_3)) %>%
    arrange(general_id)
  
  y1 <- y1 %>%
    mutate(grab_date = dmy(paste0(day_grab_date,"/",month_grab_date,"/",year_grab_date))) %>%
    mutate(year_month = paste0(year(grab_date),".", sprintf("%02d", month(grab_date)))) %>%
    select(-c(day_grab_date, month_grab_date, year_grab_date)) %>%
    distinct(general_id, .keep_all = T) %>%
    filter(!is.na(salary) & salary != "")
  
  y2 <- y2 %>%
    filter(general_id %in% y1$general_id) %>%
    distinct(., .keep_all = T)
  
  y2$group <- group_indices(y2, general_id)
  
  y2 <- y2 %>%
    mutate(group = ntile(group, 10))
  
  y2 <- y2 %>%
    group_by(group) %>%
    group_split()
  
  y2 <- lapply(y2, function(x) {
    x %>%
      ungroup() %>%
      select(-group) %>%
      mutate(value = 1) %>%
      group_by(general_id) %>%
      pivot_wider(., names_from = escoskill_level_3, values_from = value) %>%
      ungroup()
  }) %>% bind_rows
  
  y1 <- y1 %>%
    left_join(y2)
  
  remove("y2")
  
  saveRDS(y1, file = paste0("./int_data/skill_project/", countries[[i]], "_all_postings.rds"))
  
  remove("y1")
  
  return("")
}, mc.cores = 8)

#system("echo sci2007! | sudo -S shutdown -h now")

#### end ####

#### analysis and cluster ####
remove(list = ls())
paths <- list.files("./int_data/skill_project", "*.rds", full.names = T)
paths <- paths[grepl("DE|FR|NL|BE|IT|IE|SE|PL", paths)]
paths

setMKLthreads(1)

df <- lapply(1:8, function(i){
  print(i)
  x <- readRDS(paths[[i]]) %>%
    filter(str_sub(year_month, 1, 4) %in% c("2018","2019","2020", "2021")) %>%
    mutate(across(7:ncol(.), ~ as.numeric(.x)))
  x1 <- x[,1:6]
  x2 <- x[,7:ncol(x)]
  remove(x)
  setnafill(x2, fill=0)
  x1 <- as.data.frame(x1)
  x2 <- as.data.frame(x2)
  print(paths[[i]])
  return(bind_cols(x1, x2))
}) %>%
  bind_rows() %>%
  clean_names

# Countries missing skills
df_missing <- df %>% group_by(idcountry) %>% mutate(n_postings = n()) %>% filter(row_number() == 1) %>%
  select(-c(general_id,salary,idesco_level_4,grab_date,year_month)) %>%
  select(idcountry, n_postings, everything())

colnames(df_missing)[1:10]

df_missing[,3:ncol(df_missing)] <- !is.na(df_missing[,3:ncol(df_missing)])
df_missing$skill_sum <- rowSums(df_missing[,3:ncol(df_missing)])
df_missing <- df_missing %>% select(idcountry, skill_sum, n_postings, everything())

colnames(df)[1:10]

# Skill counts
df_sum <- df %>%
  select(4,7:ncol(.)) %>%
  group_by(idcountry) %>%
  summarise(across(everything(), ~ sum(.x, na.rm = TRUE)))

# Skill correlation
no_na <- function(x) {!any(is.na(x))}
df <- df %>% select_if(no_na)
df <- df %>% clean_names
df$row_sum <- rowSums(df[,7:ncol(df)])
df <- df %>% filter(row_sum > 0) %>% select(-row_sum)
df <- as.data.frame(df)

#mat_cor <- cor(df[,6:ncol(df)])
#saveRDS(mat_cor, "./int_data/mat_cor.rds")
#system("echo sci2007! | sudo -S shutdown -h now")
mat_cor <- readRDS("./int_data/mat_cor.rds")

mat_dist <- as.dist(1 - mat_cor)
skill.tree <- hclust(mat_dist, method="complete")

clusters <- as.data.frame(cutree(skill.tree, k = 400)) %>% rownames_to_column(., var = "skill")
colnames(clusters) <- c("skill", "cluster")

clusters <- clusters %>%
  arrange(cluster, skill) %>% 
  group_by(cluster) %>%
  mutate(cluster_name = skill[1])

head(clusters)

saveRDS(clusters, "./int_data/skill_clusters400.rds")

skill.pca <- PCA(df[,6:ncol(df)],  graph = FALSE)
saveRDS(skill.pca, "./int_data/skill.pca.rds")

?prcomp

# Visualize eigenvalues/variances
fviz_screeplot(res.pca, addlabels = TRUE, ylim = c(0, 50))


#### end ####

#### REIMPORT AND USE CLUSTERS ####
#### file paths ####
remove(list = ls())
file_names <- list.files("./bg-eu-bucket/new/structured", "*.csv", full.names = F)
file_names <- as.data.frame(file_names) %>% separate(file_names, sep = " ", c("type", "country", "file"), remove = F)
file_names$file_names <- as.data.frame(paths <- list.files("./bg-eu-bucket/new/structured", "*.csv", full.names = T))$paths
colnames(file_names)
file_names <- file_names %>% filter(!grepl("2020Q3|202101", file_names))
file_names <- file_names %>% filter(grepl("DE|FR|NL|BE|IT|IE|SE|PL", country))
countries <- file_names$country %>% unique()

m <- length(countries)
i = 4

clusters <- readRDS("./int_data/skill_clusters400.rds") %>% ungroup()

head(clusters)

#### /end ####

#### compile data ####
lapply(1:m, function(i) {
  y1 <- lapply(file_names$file_names[file_names$type == "postings" & file_names$country == countries[[i]]], function(x) {
    fread(paste0(x), data.table = F, nThread = 8, select = c("general_id","year_grab_date","month_grab_date","day_grab_date",
                                                             "salary", "idesco_level_4", "idcountry"))}) %>% bind_rows %>% clean_names
  
  y2 <- lapply(file_names$file_names[file_names$type == "skills" & file_names$country == countries[[i]]], function(x) {
    fread(paste0(x), data.table = F, nThread = 8)}) %>% bind_rows %>% clean_names %>%
    select(general_id, escoskill_level_3) %>%
    mutate(escoskill_level_3 = str_squish(toupper(escoskill_level_3))) %>%
    filter(!is.na(escoskill_level_3) & escoskill_level_3 != "")
  
  y2_merge <- y2 %>% select(escoskill_level_3) %>% distinct() %>% mutate(skill = janitor::make_clean_names(escoskill_level_3))
  
  y2 <- y2 %>%
    left_join(y2_merge) %>%
    select(-escoskill_level_3)
  
  y2 <- y2 %>%
    filter(skill %in% clusters$skill) %>%
    left_join(clusters %>% select(skill, cluster_name)) %>%
    select(general_id, cluster_name) %>%
    distinct(., .keep_all = T)
  
  
  y2[y2 == ""] <- NA
  
  y2 <- y2 %>%
    filter(!is.na(cluster_name)) %>%
    arrange(general_id)
  
  y1 <- y1 %>%
    mutate(grab_date = dmy(paste0(day_grab_date,"/",month_grab_date,"/",year_grab_date))) %>%
    mutate(year_month = paste0(year(grab_date),".", sprintf("%02d", month(grab_date)))) %>%
    select(-c(day_grab_date, month_grab_date, year_grab_date)) %>%
    distinct(general_id, .keep_all = T) %>%
    filter(!is.na(salary) & salary != "")
  
  y2 <- y2 %>%
    filter(general_id %in% y1$general_id) %>%
    distinct(., .keep_all = T)
  
  y2$group <- group_indices(y2, general_id)
  
  y2 <- y2 %>%
    mutate(group = ntile(group, 10))
  
  y2 <- y2 %>%
    group_by(group) %>%
    group_split()
  
  y2 <- lapply(y2, function(x) {
    x %>%
      ungroup() %>%
      select(-group) %>%
      mutate(value = 1) %>%
      group_by(general_id) %>%
      pivot_wider(., names_from = cluster_name, values_from = value) %>%
      ungroup()
  }) %>% bind_rows
  
  y1 <- y1 %>%
    left_join(y2)
  
  remove("y2")
  
  saveRDS(y1, file = paste0("./int_data/skill_project/", countries[[i]], "_all_postings_clustered.rds"))
  
  remove("y1")
  
  return("")
})

#### end ####

#### LOAD DATA ####
remove(list = ls())
paths <- list.files("./int_data/skill_project", "clustered.rds", full.names = T)
paths <- paths[grepl("DE|FR|NL|BE|IT|IE|SE|PL", paths)]
paths

df <- lapply(1:8, function(i){
  print(i)
  x <- readRDS(paths[[i]]) %>%
    filter(str_sub(year_month, 1, 4) %in% c("2018","2019","2020", "2021")) %>%
    mutate(across(7:ncol(.), ~ as.numeric(.x)))
  x1 <- x[,1:6]
  x2 <- x[,7:ncol(x)]
  remove(x)
  setnafill(x2, fill=0)
  x1 <- as.data.frame(x1)
  x2 <- as.data.frame(x2)
  print(paths[[i]])
  return(bind_cols(x1, x2))
}) %>%
  bind_rows() %>%
  clean_names

colnames(df)[1:15]

# Remove obs with no skills
no_na <- function(x) {!any(is.na(x))}
df[7:ncol(df)] <- df[7:ncol(df)] %>% select_if(no_na)
df <- df %>% clean_names
colnames(df)[1:15]
df$row_sum <- rowSums(df[,7:ncol(df)])
df <- df %>% filter(row_sum > 0) %>% select(-row_sum)
df <- as.data.frame(df)

#### end ####

#### SALARY ANALYSIS ####
unique(df$salary)

df <- df %>%
  mutate(salary_num = case_when(
    salary == "0 - 6.000 EUR" ~ 1,
    salary == "6.001 - 12.000 EUR" ~ 6,
    salary == "12.001 - 18.000 EUR" ~ 12,
    salary == "18.001 - 24.000 EUR" ~ 18,
    salary == "24.001 - 30.000 EUR" ~ 24,
    salary == "30.001 - 36.000 EUR" ~ 30,
    salary == "36.001 - 42.000 EUR" ~ 36,
    salary == "42.001 - 48.000 EUR" ~ 42,
    salary == "48.001 - 54.000 EUR" ~ 48,
    salary == "54.001 - 66.000 EUR" ~ 54,
    salary == "66.001 - 78.000 EUR" ~ 66,
    salary == "78.001 - 90.000 EUR" ~ 78,
    salary == "> 90.001 EUR"        ~ 90
  ))

df <- df %>% mutate(ln_salary_num = log(salary_num))
df <- df %>% mutate(idesco_level_3 = str_sub(idesco_level_4, 1, 3))
df <- df %>% mutate(idesco_level_2 = str_sub(idesco_level_4, 1, 2))

df <- df %>%
  select(general_id, salary, salary_num, ln_salary_num, idesco_level_4, idesco_level_3, idesco_level_2, idcountry, grab_date, year_month, everything())

#### Income Regressions ####
library(fixest)
colnames(df)[1:12]

fe1.model <- feols(fml = ln_salary_num ~ i(year_month, ref = "2019.06") | as.factor(idcountry) + as.factor(idesco_level_4),
                   data = df)

fe2.model <- feols(fml = ln_salary_num ~ i(year_month, ref = "2019.06") | factor(paste0(idcountry,"_",idesco_level_3)),
                   data = df)

fe3.model <- feols(fml = ln_salary_num ~ i(year_month, ref = "2019.06") | factor(paste0(idcountry,"_",idesco_level_4)),
                   data = df)

summary(fe1.model)
summary(fe2.model)
summary(fe3.model)

coefs.1 <- as.data.frame(summary(fe1.model)$coeftable) %>% rownames_to_column(var="names") %>% mutate(names = str_sub(names, -7, -1)) %>% clean_names %>%
  bind_rows(., data.frame("names" = "2019.06", "estimate" = 0, "std_error" = 0)) %>% arrange(names) %>% mutate(time = 1:n()) %>%
  mutate(date = as.Date(parse_date_time(names, "ym")))

p.1 = ggplot(coefs.1, aes(y = estimate, x = date)) +
  geom_ribbon(aes(ymin = estimate - std_error, ymax = estimate + std_error, alpha = 0.1)) + geom_line(color = "black") +
  scale_x_date(date_labels = "%b'%y", breaks = date_breaks("3 months"), minor_breaks = date_breaks("months")) +
  ggtitle("Year-Month Coefficients on Log(Salary)") +
  ylab("Coefficient") +
  labs(subtitle = "FE: ESCO4 + Country") + 
  scale_colour_ggthemr_d() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        legend.position = "none",
        axis.title.x=element_blank())

coefs.2 <- as.data.frame(summary(fe2.model)$coeftable) %>% rownames_to_column(var="names") %>% mutate(names = str_sub(names, -7, -1)) %>% clean_names %>%
  bind_rows(., data.frame("names" = "2019.06", "estimate" = 0, "std_error" = 0)) %>% arrange(names) %>% mutate(time = 1:n()) %>%
  mutate(date = as.Date(parse_date_time(names, "ym")))

p.2 = ggplot(coefs.2, aes(y = estimate, x = date)) +
  geom_ribbon(aes(ymin = estimate - std_error, ymax = estimate + std_error, alpha = 0.1)) + geom_line(color = "black") +
  scale_x_date(date_labels = "%b'%y", breaks = date_breaks("3 months"), minor_breaks = date_breaks("months")) +
  ggtitle("Year-Month Coefficients on Log(Salary)") +
  ylab("Coefficient") +
  labs(subtitle = "FE: ESCO3 x Country") + 
  scale_colour_ggthemr_d() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        legend.position = "none",
        axis.title.x=element_blank())

coefs.3 <- as.data.frame(summary(fe2.model)$coeftable) %>% rownames_to_column(var="names") %>% mutate(names = str_sub(names, -7, -1)) %>% clean_names %>%
  bind_rows(., data.frame("names" = "2019.06", "estimate" = 0, "std_error" = 0)) %>% arrange(names) %>% mutate(time = 1:n()) %>%
  mutate(date = as.Date(parse_date_time(names, "ym")))

p.3 = ggplot(coefs.3, aes(y = estimate, x = date)) +
  geom_ribbon(aes(ymin = estimate - std_error, ymax = estimate + std_error, alpha = 0.1)) + geom_line(color = "black") +
  scale_x_date(date_labels = "%b'%y", breaks = date_breaks("3 months"), minor_breaks = date_breaks("months")) +
  ggtitle("Year-Month Coefficients on Log(Salary)") +
  ylab("Coefficient") +
  labs(subtitle = "FE: ESCO4 x Country") + 
  scale_colour_ggthemr_d() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        legend.position = "none",
        axis.title.x=element_blank())

ggexport(p.1, filename = paste0("./plots/salary/eu_year_month_fe_esco4_country.pdf"))
ggexport(p.2, filename = paste0("./plots/salary/eu_year_month_esco3_x_country.pdf"))
ggexport(p.3, filename = paste0("./plots/salary/eu_year_month_esco4_x_country.pdf"))

#### end ####

#### Skill Premia ####
library("biglm")
library("fixest")

colnames(df)[1:15]

FF1 <- formula(paste("ln_salary_num ~ ", 
                    paste(colnames(df)[11:ncol(df)], collapse=" + ")," | as.factor(idcountry)"))

sp1.model <- feols(fml = FF1,
                   data = df)

unique(df$year_month)

df <- df %>%
  mutate(
    dum_clean_surfaces_2018_06 = ifelse(year_month == "2018.06", clean_surfaces, 0),
    dum_clean_surfaces_2018_03 = ifelse(year_month == "2018.03", clean_surfaces, 0),
    dum_clean_surfaces_2018_10 = ifelse(year_month == "2018.10", clean_surfaces, 0),
    dum_clean_surfaces_2018_08 = ifelse(year_month == "2018.08", clean_surfaces, 0),
    dum_clean_surfaces_2018_05 = ifelse(year_month == "2018.05", clean_surfaces, 0),
    dum_clean_surfaces_2018_09 = ifelse(year_month == "2018.09", clean_surfaces, 0),
    dum_clean_surfaces_2018_07 = ifelse(year_month == "2018.07", clean_surfaces, 0),
    dum_clean_surfaces_2018_12 = ifelse(year_month == "2018.12", clean_surfaces, 0),
    dum_clean_surfaces_2018_04 = ifelse(year_month == "2018.04", clean_surfaces, 0),
    dum_clean_surfaces_2018_11 = ifelse(year_month == "2018.11", clean_surfaces, 0),
    dum_clean_surfaces_2018_02 = ifelse(year_month == "2018.02", clean_surfaces, 0),
    dum_clean_surfaces_2018_01 = ifelse(year_month == "2018.01", clean_surfaces, 0),
    dum_clean_surfaces_2019_02 = ifelse(year_month == "2019.02", clean_surfaces, 0),
    dum_clean_surfaces_2019_01 = ifelse(year_month == "2019.01", clean_surfaces, 0),
    dum_clean_surfaces_2019_07 = ifelse(year_month == "2019.07", clean_surfaces, 0),
    dum_clean_surfaces_2019_09 = ifelse(year_month == "2019.09", clean_surfaces, 0),
    dum_clean_surfaces_2019_08 = ifelse(year_month == "2019.08", clean_surfaces, 0),
    dum_clean_surfaces_2019_06 = ifelse(year_month == "2019.06", clean_surfaces, 0),
    dum_clean_surfaces_2019_04 = ifelse(year_month == "2019.04", clean_surfaces, 0),
    dum_clean_surfaces_2019_05 = ifelse(year_month == "2019.05", clean_surfaces, 0),
    dum_clean_surfaces_2019_12 = ifelse(year_month == "2019.12", clean_surfaces, 0),
    dum_clean_surfaces_2019_11 = ifelse(year_month == "2019.11", clean_surfaces, 0),
    dum_clean_surfaces_2019_03 = ifelse(year_month == "2019.03", clean_surfaces, 0),
    dum_clean_surfaces_2019_10 = ifelse(year_month == "2019.10", clean_surfaces, 0),
    dum_clean_surfaces_2020_07 = ifelse(year_month == "2020.07", clean_surfaces, 0),
    dum_clean_surfaces_2020_10 = ifelse(year_month == "2020.10", clean_surfaces, 0),
    dum_clean_surfaces_2020_09 = ifelse(year_month == "2020.09", clean_surfaces, 0),
    dum_clean_surfaces_2020_01 = ifelse(year_month == "2020.01", clean_surfaces, 0),
    dum_clean_surfaces_2020_11 = ifelse(year_month == "2020.11", clean_surfaces, 0),
    dum_clean_surfaces_2020_08 = ifelse(year_month == "2020.08", clean_surfaces, 0),
    dum_clean_surfaces_2020_06 = ifelse(year_month == "2020.06", clean_surfaces, 0),
    dum_clean_surfaces_2020_03 = ifelse(year_month == "2020.03", clean_surfaces, 0),
    dum_clean_surfaces_2020_05 = ifelse(year_month == "2020.05", clean_surfaces, 0),
    dum_clean_surfaces_2020_02 = ifelse(year_month == "2020.02", clean_surfaces, 0),
    dum_clean_surfaces_2020_12 = ifelse(year_month == "2020.12", clean_surfaces, 0),
    dum_clean_surfaces_2020_04 = ifelse(year_month == "2020.04", clean_surfaces, 0),
    dum_clean_surfaces_2021_02 = ifelse(year_month == "2021.02", clean_surfaces, 0),
    dum_clean_surfaces_2021_03 = ifelse(year_month == "2021.03", clean_surfaces, 0),
    dum_clean_surfaces_2021_01 = ifelse(year_month == "2021.01", clean_surfaces, 0))
  
View(as.data.frame(df_wide))

colnames(df)[11:ncol(df)][colnames(df)[11:ncol(df)] != "clean_surfaces"]

# Skill Premia Time Varying "clean_surfaces"
FF2 <- formula(paste("ln_salary_num ~ ", 
                    paste(colnames(df)[11:ncol(df)][colnames(df)[11:ncol(df)] != "clean_surfaces"], collapse=" + ")," | as.factor(idcountry)"))

sp2.model <- feols(fml = FF2,
                   data = df)

# Plot Coeffs
coefs.sp2 <- as.data.frame(summary(sp2.model)$coeftable) %>% rownames_to_column(var="names") %>% clean_names %>% filter(grepl("dum_clean", names)) %>%
  mutate(year_month = str_sub(names, -7, -1)) %>%
  mutate(date = as.Date(parse_date_time(year_month, "ym"))) %>%
  filter(!grepl("2018", year_month))

head(coefs.sp2)

ggp <- ggplot(coefs.sp2,aes(x=date,y=estimate)) + 
  geom_ribbon(aes(ymin = estimate - std_error, ymax = estimate + std_error), fill = "grey70") +
  geom_line(color = "blue") +
  scale_x_date(date_labels = "%b'%y", breaks = date_breaks("3 months"), minor_breaks = date_breaks("months")) +
  ylab("Coefficient ~ Marginal Effect on Salary (£)") + xlab("Month Interaction") +
  ggtitle('Skill-Premia "clean_surfaces" " interacted by year-month') +
  labs(subtitle = "Countries = DE,FR,NL,BE,IT,IE,SE,PL, Obs = 7,298,490")

ggp

ggsave(ggp, filename = "./plots/skill_premia/clean_surfaces_int_year_month.png")

View(coefs.sp1)
  
head(clusters)

View(coefs.sp1)
%>%
  bind_rows(., data.frame("names" = "2019.06", "estimate" = 0, "std_error" = 0)) %>% arrange(names) %>% mutate(time = 1:n()) %>%
  mutate(date = as.Date(parse_date_time(names, "ym")))

p.1 = ggplot(coefs.1, aes(y = estimate, x = date)) +
  geom_ribbon(aes(ymin = estimate - std_error, ymax = estimate + std_error, alpha = 0.1)) + geom_line(color = "black") +
  scale_x_date(date_labels = "%b'%y", breaks = date_breaks("3 months"), minor_breaks = date_breaks("months")) +
  ggtitle("Year-Month Coefficients on Log(Salary)") +
  ylab("Coefficient") +
  labs(subtitle = "FE: ESCO4 + Country") + 
  scale_colour_ggthemr_d() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        legend.position = "none",
        axis.title.x=element_blank())

?feols



df <- df %>%
  mutate(rand = runif(n(), min = 0, max = 100)) %>%
  mutate(group = ntiles(rand, 20)) %>%
  group_by(group) %>%
  group_split()

lm1.skills <- biglm(FF,df[[1]])
lm1.skills <- update(lm1.skills,df[[2]])
lm1.skills <- update(lm1.skills,df[[3]])
lm1.skills <- update(lm1.skills,df[[4]])
lm1.skills <- update(lm1.skills,df[[5]])
lm1.skills <- update(lm1.skills,df[[6]])
lm1.skills <- update(lm1.skills,df[[7]])
lm1.skills <- update(lm1.skills,df[[8]])
lm1.skills <- update(lm1.skills,df[[9]])
lm1.skills <- update(lm1.skills,df[[10]])

saveRDS(lm1.skills, "/int_data/skill_premia/lm1_skills.rds")


#### end ####


#### SOURCE FIXED EFFECTS - DO THEY MATTER FOR INCOME ####


