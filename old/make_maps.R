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
ggthemr('flat')
library(egg)
library(extrafont)
library(fixest)
# font_import()
#loadfonts(device="postscript")
#fonts()

setDTthreads(1)
getDTthreads()
#quanteda_options(threads = 1)
setwd("/mnt/disks/pdisk/bg_combined/")

#### IMPORT CLEAN ####
remove(list = ls())
remove(list = setdiff(ls(),"df_all"))

unique(df_all$region)

df_all <- fread(file = "./int_data/df_all_standardised.csv", nThread = 8)
df_all$month <- factor(df_all$month, levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))
df_all$wfh <- as.numeric(df_all$wfh_prob>0.5)
df_all$bgt_occ6 <- str_sub(df_all$bgt_occ, 1, 7)
df_all$bgt_occ2 <- as.numeric(str_sub(df_all$bgt_occ, 1, 2))
df_all$job_ymd <- ymd(df_all$job_date)
df_all$year_quarter <- as.yearqtr(df_all$job_ymd)
df_all$year_month <- as.yearmon(df_all$job_ymd)
df_all <- setDT(df_all)

colnames(df_all)

#### END ####

#### GLOBAL STATE MAPS ####
remove(list = setdiff(ls(),"df_all"))
df_state <- df_all[year >= 2021] %>%
  group_by(country, state) %>%
  summarise(wfh = weighted.mean(x = wfh, w = job_id_weight),
            job_id_weight = sum(job_id_weight))

df_state <- df_state %>%
  filter(state != "" & !is.na(state))

df_state <- df_state %>%
  mutate(state = case_when(
    country == "Canada" & state == "AB" ~ "Alberta",
    country == "Canada" & state == "BC" ~ "British Columbia",
    country == "Canada" & state == "MB" ~ "Manitoba",
    country == "Canada" & state == "NB" ~ "New Brunswick",
    country == "Canada" & state == "NL" ~ "Newfoundland and Labrador",
    country == "Canada" & state == "NS" ~ "Northwest Territories",
    country == "Canada" & state == "NT" ~ "Nova Scotia",
    country == "Canada" & state == "NU" ~ "Nunavut",
    country == "Canada" & state == "ON" ~ "Ontario",
    country == "Canada" & state == "PE" ~ "Prince Edward Island",
    country == "Canada" & state == "QC" ~ "Québec",
    country == "Canada" & state == "SK" ~ "Saskatchewan",
    country == "Canada" & state == "YT" ~ "Yukon",
    
    country == "Australia" & state == "ACT" ~ "Australian Capital Territory",
    country == "Australia" & state == "NSW" ~ "New South Wales",
    country == "Australia" & state == "NT" ~ "Northern Territory",
    country == "Australia" & state == "OT" ~ "OT",
    country == "Australia" & state == "QLD" ~ "Queensland",
    country == "Australia" & state == "SA" ~ "South Australia",
    country == "Australia" & state == "TAS" ~ "Tasmania",
    country == "Australia" & state == "VIC" ~ "Victoria",
    country == "Australia" & state == "WA"  ~ "Western Australia",
    
    country == "UK" & state == "wales"  ~ "Wales",
    
    country == "NZ" ~ "New Zealand",
    
    TRUE ~ state
  ))
#reg_df <- df_all[year >= 2021 & country == "US"] %>% select(job_id_weight, wfh, bgt_occ6, year_month, state) %>% group_by(bgt_occ6) %>% filter(n()>2) %>% setDT(.)

# Remove Occupation FEs
#mod1 <- feols(fml = wfh ~ 1 | bgt_occ6,
#              data = reg_df,
#              weights = ~ job_id_weight,
#              fixef.rm = "both",
#              nthreads = 8,
#              verbose = 10,
#              cluster = ~ year_month)
#
#summary(mod1)
#
#fe_df <- as.data.table(as.data.frame(fixef(mod1)), keep.rownames = TRUE) %>% rename(bgt_occ6_coeff = bgt_occ6) %>% rename(bgt_occ6 = rn)
#
#reg_df <- reg_df %>%
#  left_join(fe_df)
#
#weighted.mean(x = reg_df$wfh, w = reg_df$job_id_weight) # 0.0932998
#
#(grand_mean <- weighted.mean(x = reg_df$bgt_occ6_coeff, w = reg_df$job_id_weight))
#
#weighted.mean(x = reg_df$wfh - reg_df$bgt_occ6_coeff, w = reg_df$job_id_weight) + grand_mean
#
#reg_df$adj_wfh <- reg_df$wfh - reg_df$bgt_occ6_coeff + grand_mean
#
#reg_df_state <- reg_df %>%
#  group_by(state) %>%
#  summarise(adj_wfh = weighted.mean(x = adj_wfh, w = job_id_weight))
#
#df_state <- df_state %>%
#  left_join(reg_df_state) %>%
#  mutate(state = tolower(state))
#
#remove(list = setdiff(ls(),c("df_all", "df_state")))

# GADM for US and Canada states
#us_states <- raster::getData(country="USA", level=1) %>% sf::st_as_sf()
#can_provinces <- raster::getData(country="CAN", level=1) %>% sf::st_as_sf()
#uk_states <- raster::getData(country="GBR", level=1) %>% sf::st_as_sf()
#aus_states <- raster::getData(country="AUS", level=1) %>% sf::st_as_sf()
#nz_states <- raster::getData(country="NZL", level=0) %>% sf::st_as_sf()
#pri_states <- raster::getData(country="PRI", level=0) %>% sf::st_as_sf()
#pri_states$NAME_1 <- "Puerto Rico"
#nz_states$NAME_1 <- nz_states$NAME_0
#
#sf_states_all <- bind_rows(us_states, pri_states, can_provinces, uk_states, aus_states, nz_states)
#
#saveRDS(sf_states_all, "./aux_data/shape_files_states_all.rds")
sf_states_all <- readRDS("./aux_data/shape_files_states_all.rds")

# Merge in Covariates
sf_states_all <- merge(sf_states_all, df_state, by.x = "NAME_1", by.y = "state")

mean(sf_states_all$NAME_1 %in% df_state$state)
df_state[!(df_state$state %in% sf_states_all$NAME_1),]

#### COUNTRY MAPS by STATE/PROVINCE/DEVOLVED NATION ####
p = ggplot(sf_states_all[sf_states_all$NAME_0 == "Canada",], aes(group = NAME_1, fill = wfh*100)) +
  geom_sf(colour = "white", size = 0.1) +
  coord_sf() +
  #scale_fill_gradientn(colours=rev(heat.colors(10)),na.value="grey90") +
  #scale_fill_viridis_c(values = ) +
  #scale_fill_continuous(type = "viridis") +
  scale_fill_gradient(low="#DEEAF6", high="#072F6B", name = "WFH Share of Vacancy Postings (%)",
                      breaks = c(0,5,10,15,20), labels = c(0,5,10,15,20), 
                      aesthetics = "fill", expand = TRUE, limits = c(0,20),
                      na.value="white") +
  #continuous_scale()
  labs(title = "WFH Share by State/Province", subtitle = "Vacancy weighted") +
  theme(legend.position = "bottom")+
  #theme(panel.background = element_rect(fill = 'skyblue')) +    
  theme(axis.title.x=element_blank(),
        axis.text.x=element_blank(),
        axis.ticks.x=element_blank(),
        axis.title.y=element_blank(),
        axis.text.y=element_blank(),
        axis.ticks.y=element_blank(),
        axis.line = element_blank(),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        text = element_text(size=18, family="serif"),
        legend.text=element_text(size=14)) +
  guides(fill = guide_colourbar(barwidth = 20, draw.ulim = TRUE, draw.llim = TRUE,
                                barheight = 1, title.position = "top", title.hjust = 0.5,
                                ticks = T, label.position = "bottom", 
  ))

p
p_egg <- set_panel_size(p = p,
                        width = unit(6, "in"),
                        height = unit(4, "in"))
ggsave(p_egg, filename = "./plots/can_map_states.png", width = 8, height = 8)

p = ggplot(sf_states_all[sf_states_all$NAME_0 == "Australia",], aes(group = NAME_1, fill = wfh*100)) +
  geom_sf(colour = "white", size = 0.1) +
  coord_sf() +
  #scale_fill_gradientn(colours=rev(heat.colors(10)),na.value="grey90") +
  #scale_fill_viridis_c(values = ) +
  #scale_fill_continuous(type = "viridis") +
  scale_fill_gradient(low="#DEEAF6", high="#072F6B", name = "WFH Share of Vacancy Postings (%)",
                      breaks = c(0,5,10,15,20), labels = c(0,5,10,15,20), 
                      aesthetics = "fill", expand = TRUE, limits = c(0,20),
                      na.value="white") +
  #continuous_scale()
  labs(title = "WFH Share by State/Province", subtitle = "Vacancy weighted") +
  theme(legend.position = "bottom")+
  #theme(panel.background = element_rect(fill = 'skyblue')) +    
  theme(axis.title.x=element_blank(),
        axis.text.x=element_blank(),
        axis.ticks.x=element_blank(),
        axis.title.y=element_blank(),
        axis.text.y=element_blank(),
        axis.ticks.y=element_blank(),
        axis.line = element_blank(),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        text = element_text(size=18, family="serif"),
        legend.text=element_text(size=14)) +
  guides(fill = guide_colourbar(barwidth = 20, draw.ulim = TRUE, draw.llim = TRUE,
                                barheight = 1, title.position = "top", title.hjust = 0.5,
                                ticks = T, label.position = "bottom", 
  ))

p
p_egg <- set_panel_size(p = p,
                        width = unit(6, "in"),
                        height = unit(4, "in"))
ggsave(p_egg, filename = "./plots/aus_map_states.png", width = 8, height = 8)

p = ggplot(sf_states_all[sf_states_all$NAME_0 == "United States" & !(sf_states_all$NAME_1 %in% c("Alaska", "Hawaii")),], aes(group = NAME_1, fill = wfh*100)) +
  geom_sf(colour = "white", size = 0.1) +
  coord_sf() +
  #scale_fill_gradientn(colours=rev(heat.colors(10)),na.value="grey90") +
  #scale_fill_viridis_c(values = ) +
  #scale_fill_continuous(type = "viridis") +
  scale_fill_gradient(low="#DEEAF6", high="#072F6B", name = "WFH Share of Vacancy Postings (%)",
                      breaks = c(0,5,10,15,20), labels = c(0,5,10,15,20), 
                      aesthetics = "fill", expand = TRUE, limits = c(0,20),
                      na.value="white") +
  #continuous_scale()
  labs(title = "WFH Share by State/Province", subtitle = "Vacancy weighted") +
  theme(legend.position = "bottom")+
  #theme(panel.background = element_rect(fill = 'skyblue')) +    
  theme(axis.title.x=element_blank(),
        axis.text.x=element_blank(),
        axis.ticks.x=element_blank(),
        axis.title.y=element_blank(),
        axis.text.y=element_blank(),
        axis.ticks.y=element_blank(),
        axis.line = element_blank(),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        text = element_text(size=18, family="serif"),
        legend.text=element_text(size=14)) +
  guides(fill = guide_colourbar(barwidth = 20, draw.ulim = TRUE, draw.llim = TRUE,
                                barheight = 1, title.position = "top", title.hjust = 0.5,
                                ticks = T, label.position = "bottom", 
  ))

p
p_egg <- set_panel_size(p = p,
                        width = unit(6, "in"),
                        height = unit(4, "in"))
ggsave(p_egg, filename = "./plots/us_map_states.png", width = 8, height = 8)

p = ggplot(sf_states_all[sf_states_all$NAME_0 == "United Kingdom",], aes(group = NAME_1, fill = wfh*100)) +
  geom_sf(colour = "white", size = 0.1) +
  coord_sf() +
  #scale_fill_gradientn(colours=rev(heat.colors(10)),na.value="grey90") +
  #scale_fill_viridis_c(values = ) +
  #scale_fill_continuous(type = "viridis") +
  scale_fill_gradient(low="#DEEAF6", high="#072F6B", name = "WFH Share of Vacancy Postings (%)",
                      breaks = c(0,5,10,15,20), labels = c(0,5,10,15,20), 
                      aesthetics = "fill", expand = TRUE, limits = c(0,20),
                      na.value="white") +
  #continuous_scale()
  labs(title = "WFH Share by State/Province", subtitle = "Vacancy weighted") +
  theme(legend.position = "bottom")+
  #theme(panel.background = element_rect(fill = 'skyblue')) +    
  theme(axis.title.x=element_blank(),
        axis.text.x=element_blank(),
        axis.ticks.x=element_blank(),
        axis.title.y=element_blank(),
        axis.text.y=element_blank(),
        axis.ticks.y=element_blank(),
        axis.line = element_blank(),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        text = element_text(size=18, family="serif"),
        legend.text=element_text(size=14)) +
  guides(fill = guide_colourbar(barwidth = 20, draw.ulim = TRUE, draw.llim = TRUE,
                                barheight = 1, title.position = "top", title.hjust = 0.5,
                                ticks = T, label.position = "bottom", 
  ))

p
p_egg <- set_panel_size(p = p,
                        width = unit(6, "in"),
                        height = unit(4, "in"))
ggsave(p_egg, filename = "./plots/uk_map_states.png", width = 8, height = 8)
#### END ####

#### MERGE DENSITY ####

remove(list = setdiff(ls(),"df_all"))

df_ttwa <- df_all[year >= 2021 & country == "UK"] %>%
  rename(ttwa = geog) %>%
  group_by(ttwa) %>%
  summarise(n_vacs = sum(job_id_weight, na.rm = T),
            wfh = weighted.mean(x = wfh, w = job_id_weight),
            density_per_km2 = density_per_km2[1]) %>%
  ungroup() %>%
  setDT(.)

df_fips <- df_all[year >= 2021 & country == "US"] %>%
  rename(fips = geog) %>%
  group_by(fips) %>%
  summarise(n_vacs = sum(job_id_weight, na.rm = T),
            wfh = weighted.mean(x = wfh, w = job_id_weight),
            density_per_km2 = density_per_km2[1]) %>%
  ungroup() %>%
  setDT(.) %>%
  .[, fips := as.integer(as.numeric(fips))]

colnames(fips_census)

# US pop density census
fips_census <- fread(input = "https://raw.githubusercontent.com/ykzeng/covid-19/master/data/census-landarea-all.csv", colClasses=c("character")) %>% clean_names %>%
  setDT(.) %>%
  .[, fips := as.integer(as.numeric(fips))] %>%
  select(fips, pop060210) %>%
  rename(pop_density = pop060210)

# UK pop density
pop_uk_ttwa <- fread(input = "./aux_data/geog/pop_uk_raw.csv", colClasses=c("character")) %>% clean_names %>%
  setDT(.) %>%
  .[, pop := as.numeric(gsub(",", "", local_authority_population_by_ttwa, fixed = T))] %>%
  setDT(.) %>%
  .[, .(pop = sum(pop, na.rm = T)),  by = .(ttwa_code, ttwa_name)] %>%
  filter(ttwa_name != "" & !is.na(ttwa_name)) %>%
  .[, ttwa_name := tolower(gsub("&", "and", str_squish(ttwa_name)))]

area_uk_ttwa <- fread(input = "./aux_data/geog/area_uk.csv", colClasses=c("character")) %>% clean_names %>%
  rename(ttwa_name = ttwa) %>%
  .[, area := as.numeric(area)] %>%
  setDT(.) %>%
  .[, .(area = sum(area, na.rm = T)),  by = .(ttwa11cd, ttwa_name)] %>%
  filter(ttwa_name != "" & !is.na(ttwa_name)) %>%
  .[, ttwa_name := tolower(gsub("&", "and", str_squish(ttwa_name)))]

View(area_uk_ttwa[!(area_uk_ttwa$ttwa_name %in% pop_uk_ttwa$ttwa_name)])
View(pop_uk_ttwa[!(pop_uk_ttwa$ttwa_name %in% area_uk_ttwa$ttwa_name)])
mean()

gsub("[^0-9.]", "", "100,000.01")

zip_df <- fread(input = "https://raw.githubusercontent.com/arjunramani3/donut-effect/main/data/zip_all_chars_cbd.csv", colClasses=c("character")) %>% clean_names

zip_df$county_fips = as.integer(as.numeric(paste0(zip_df$state, zip_df$county)))

colnames(zip_df)

fips_acs <- zip_df %>%
  .[, .(land_area = sum(as.numeric(land_area), na.rm = T),
        population = sum(as.numeric(x2019_population), na.rm = T)),
    by = county_fips] %>%
  .[, density := population/land_area]

View(df_fips[!(df_fips$fips %in% fips_acs$county_fips)])




mean(fips_acs$county_fips %in% df_fips$fips)

#### END ####

#### FIPS COUNTY LEVEL PLOTS ####
remove(list = setdiff(ls(),"df_all"))

df_fips <- df_all[year >= 2021 & country == "US"] %>%
  rename(fips = geog) %>%
  group_by(fips) %>%
  summarise(n_vacs = sum(job_id_weight, na.rm = T),
            wfh = weighted.mean(x = wfh, w = job_id_weight),
            density_per_km2 = density_per_km2[1]) %>%
  ungroup() %>%
  setDT(.) %>%
  .[, fips := as.integer(as.numeric(fips))]

df_fips <- df_fips %>%
  .[, wfh_ptile := as.numeric(cut(wfh, breaks=c(quantile(wfh, probs = seq(0, 1, by = 0.10), na.rm = T)), include.lowest=TRUE))*10]

df_fips <- df_fips %>%
  .[, density_quint := as.numeric(cut(density_per_km2, breaks=c(quantile(density_per_km2, probs = seq(0, 1, by = 0.20), na.rm = T)), include.lowest=TRUE))]

## some lonely cows

## all Fips
dfips <- maps::county.fips %>%
  as_tibble %>% 
  extract(polyname, c("region", "subregion"), "^([^,]+),([^,]+)$") %>%
  mutate(region = str_to_title(region),
         subregion = str_to_title(subregion))



## some county maps & left_join
data <- usmap::us_map(regions = "counties") %>%
  mutate(fips = as.numeric(fips)) %>%
  left_join(dfips)

us_counties <- raster::getData(country="USA", level=2) %>% sf::st_as_sf()

mean(dfips$subregion %in% us_counties$NAME_2) # WE drop some! Investigate

us_counties_fips <- us_counties %>%
  merge(., dfips, by.x = c("NAME_1", "NAME_2"), by.y = c("region", "subregion"), all.x = TRUE, all.y = FALSE)

us_counties_fips <- us_counties_fips %>%
  merge(., df_fips, by.x = c("fips"), by.y = c("fips"), all.x = TRUE, all.y = FALSE)

#us_counties_fips_cent <- sf::st_centroid(us_counties_fips)

us_states <- raster::getData(country="USA", level=1) %>% sf::st_as_sf()

p = ggplot(data = us_counties_fips) +
  geom_sf(size = 0.1, color = "grey", fill = "black") +
  geom_sf(data = us_states, size = 0.5, color = "lightgrey", fill = NA) +
  geom_point(
    aes(colour = wfh_ptile, size = n_vacs/1000, geometry = geometry),
    stat = "sf_coordinates",
    alpha = 0.7
  ) +
  coord_sf(xlim = c(-127, -64), ylim = c(24, 50), expand = FALSE) +
  scale_colour_gradient2(aesthetics = "colour", high="red", name = "WFH Share Decile",
                         breaks = c(0,25,50,75,100), labels = c(0,25,50,75,100),
                         expand = TRUE, limits = c(0,100)) +
  scale_size(name="Vacancy Postings ('000):", breaks = c(1,10, 20, 30), range = c(1, 6), limits = c(0.1,NA)) +
  labs(title = "WFH Share Distribution across the US", subtitle = "Vacancy weighted") +
  theme(legend.position = "bottom")+
  #theme(panel.background = element_rect(fill = 'skyblue')) +    
  theme(axis.title.x=element_blank(),
        axis.text.x=element_blank(),
        axis.ticks.x=element_blank(),
        axis.title.y=element_blank(),
        axis.text.y=element_blank(),
        axis.ticks.y=element_blank(),
        axis.line = element_blank(),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        text = element_text(size=12, family="serif"),
        legend.text=element_text(size=12)) +
  guides() +
  guides(size = guide_legend(override.aes=list(colour="darkgrey")),
         color = guide_colourbar(barwidth = 15, draw.ulim = TRUE, draw.llim = TRUE,
                                barheight = 0.8, title.position = "top", title.hjust = 0.5,
                                ticks = T, label.position = "bottom")) +
  theme(legend.direction = "horizontal", legend.box = "vertical")
p
p_egg <- set_panel_size(p = p,
                        width = unit(6, "in"),
                        height = unit(4, "in"))
p_egg
ggsave(p_egg, filename = "./plots/us_fips_wfh_heatmap.png", width = 6, height = 8)

# MSA~ xwalk https://www.bls.gov/cew/classifications/areas/county-msa-csa-crosswalk.htm




## more left_join
data<-left_join(data,df_fips)

#install.packages("usmap")
states <- usmap::us_map()

## map with random, fictive cows
p = ggplot() +
  #geom_polygon(data = states, aes(x = x, y = y, group = group), fill = "grey", size = 0.2) +
  geom_point(data = data, aes(x, y, group = group, color=wfh*100, size = n_vacs)) +
  coord_equal() +
  scale_color_gradient(low="navy", high="red", name = "WFH Share of Vacancy Postings (%)",
                    breaks = c(0,10,20,30,40,50), labels = c(0,10,20,30,40,50), 
                    aesthetics = "fill", expand = TRUE, limits = c(0,45),
                    na.value="black")
p

p = ggplot() +
  geom_polygon(data = data, aes(x, y, group = group, fill=wfh*100), colour = "white", size = 0.02) +
  geom_polygon(data = states, aes(x = x, y = y, group = group), fill = NA, color = "grey", size = 0.2) +
  coord_equal() +
  #scale_fill_gradientn(colours=rev(heat.colors(10)),na.value="grey90") +
  #scale_fill_viridis_c(values = ) +
  #scale_fill_continuous(type = "viridis") +
  
  #continuous_scale()
  labs(title = "WFH Share by US State", subtitle = "Vacancy weighted") +
  theme(legend.position = "bottom")+
  #theme(panel.background = element_rect(fill = 'skyblue')) +    
  theme(axis.title.x=element_blank(),
        axis.text.x=element_blank(),
        axis.ticks.x=element_blank(),
        axis.title.y=element_blank(),
        axis.text.y=element_blank(),
        axis.ticks.y=element_blank(),
        axis.line = element_blank(),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        text = element_text(size=18, family="serif")) +
  guides(fill = guide_colourbar(barwidth = 20, draw.ulim = TRUE, draw.llim = TRUE,
                                barheight = 1, title.position = "top", title.hjust = 0.5,
                                ticks = T, label.position = "bottom", 
  ))
p
p_egg <- set_panel_size(p = p,
                        width = unit(6, "in"),
                        height = unit(4, "in"))
p_egg
ggsave(p_egg, filename = "./plots/us_fips.png", width = 8, height = 8, )

#### END ####

#### SANDBOX ####


devtools::install_github("ropensci/rnaturalearthhires")

state_prov <- rnaturalearth::ne_states(c("united states of america", "canada"))

