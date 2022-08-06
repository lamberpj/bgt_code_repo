#### B. SHORT PLOTS ####

# 1. TIME SERIES PLOTS BY PARSING
# Load Data
remove(list = setdiff(ls(),"df_stru"))
load(file = "./int_data/df_stru_wfh_eu_alt.RData")
obs <- nrow(df_stru)

df_levels <- df_stru %>%
  group_by(year_quarter) %>%
  summarise(n_posts = n(),
            wfh_raw = sum(wfh_raw),
            wfh_ustring = sum(wfh_ustring),
            wfh_non_neg = sum(wfh_non_neg),
            wfh_raw_body = sum(wfh_raw_body),
            wfh_ustring_body = sum(wfh_ustring_body),
            wfh_non_neg_body = sum(wfh_non_neg_body)
  ) %>%
  mutate(across(wfh_raw:wfh_non_neg_body, ~ .x / n_posts, .names = "{.col}_prop")) %>%
  mutate(across(wfh_raw:wfh_non_neg_body, ~ .x / .x[year_quarter == "2019 Q1"], .names = "{.col}_index")) %>%
  mutate(across(wfh_raw_prop:wfh_non_neg_body_prop, ~ .x / .x[year_quarter == "2019 Q1"], .names = "{.col}_index"))

# 1.1 By Count
df_ts_posts <- df_levels %>%
  select(year_quarter, wfh_raw, wfh_non_neg, wfh_non_neg_body) %>%
  group_by(year_quarter) %>%
  pivot_longer(., cols = c(wfh_raw, wfh_non_neg, wfh_non_neg_body)) %>%
  ungroup() %>%
  mutate(name = case_when(
    name == "wfh_raw" ~ "Raw",
    name == "wfh_non_neg" ~ "Non-negated",
    name == "wfh_non_neg_body" ~ "Non-negated, body"))

p1 = df_ts_posts %>%
  ggplot(., aes(x = year_quarter, y = value, colour = name, linetype = name)) +
  geom_line(size = 2) +
  ##geom_point(size = 3) +
  ggtitle("Count of WFH Job Ads in BGT EU (Alt)") +
  ylab("Count of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25),  limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(label=comma, limits = c(0,NA), n.breaks = 10) + 
  scale_colour_ggthemr_d() +
  labs(subtitle = paste0("n = ",format(obs, big.mark=",")),
       caption = "Note: Pools data from BE, DE, FR, LU and NE (unweighted)") +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm"),
    text = element_text(size = 16))
p1
ggexport(p1, filename = "./plots/text_based/eu_alt_wfh_level_by_parsing_sr.pdf")
remove(p1)

# 1.2 By Share
df_ts_share <- df_levels %>%
  select(year_quarter, wfh_raw_prop, wfh_non_neg_prop, wfh_non_neg_body_prop) %>%
  group_by(year_quarter) %>%
  pivot_longer(., cols = c(wfh_raw_prop, wfh_non_neg_prop, wfh_non_neg_body_prop)) %>%
  ungroup() %>%
  mutate(name = case_when(
    name == "wfh_raw_prop" ~ "Raw",
    name == "wfh_non_neg_prop" ~ "Non-negated",
    name == "wfh_non_neg_body_prop" ~ "Non-negated, body"))

p2 = df_ts_share %>%
  ggplot(., aes(x = year_quarter, y = value, colour = name, linetype = name)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT EU (Alt)") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01),  breaks = seq(0,1,0.01)) +
  coord_cartesian(ylim = c(0, 0.195)) +
  scale_colour_ggthemr_d() +
  labs(subtitle = paste0("n = ",format(obs, big.mark=",")),
       caption = "Note: Pools data from BE, DE, FR, LU and NE (unweighted)") +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm"),
    text = element_text(size = 16))
p2
ggexport(p2, filename = "./plots/text_based/eu_alt_wfh_share_by_parsing_20_sr.pdf")
remove(p2)

p2 = df_ts_share %>%
  ggplot(., aes(x = year_quarter, y = value, colour = name, linetype = name)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT EU (Alt)") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01),  breaks = seq(0,1,0.01)) +
  coord_cartesian(ylim = c(0, 0.10)) +
  scale_colour_ggthemr_d() +
  labs(subtitle = paste0("n = ",format(obs, big.mark=",")),
       caption = "Note: Pools data from BE, DE, FR, LU and NE (unweighted)") +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm"),
    text = element_text(size = 16))
p2
ggexport(p2, filename = "./plots/text_based/eu_alt_wfh_share_by_parsing_10_sr.pdf")
remove(p2)

p2 = df_ts_share %>%
  ggplot(., aes(x = year_quarter, y = value, colour = name, linetype = name)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT EU (Alt)") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01),  breaks = seq(0,1,0.01)) +
  coord_cartesian(ylim = c(0, 0.05)) +
  scale_colour_ggthemr_d() +
  labs(subtitle = paste0("n = ",format(obs, big.mark=",")),
       caption = "Note: Pools data from BE, DE, FR, LU and NE (unweighted)") +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm"),
    text = element_text(size = 16))
p2
ggexport(p2, filename = "./plots/text_based/eu_alt_wfh_share_by_parsing_5_sr.pdf")
remove(p2)

# 2. TIME SERIES PLOTS BY EU COUNTRY
# Load Data
remove(list = setdiff(ls(),"df_stru"))
#load(file = "./int_data/df_stru_wfh_eu_alt.RData")
obs <- nrow(df_stru)

df_levels_country <- df_stru %>%
  group_by(year_quarter, idcountry) %>%
  summarise(n_posts = n(),
            wfh_non_neg = sum(wfh_non_neg)
  ) %>%
  mutate(across(wfh_non_neg, ~ .x / n_posts, .names = "{.col}_prop")) %>%
  ungroup() %>%
  group_by(idcountry) %>%
  mutate(across(wfh_non_neg, ~ .x / .x[year_quarter == "2019 Q1"], .names = "{.col}_index")) %>%
  mutate(across(wfh_non_neg_prop, ~ .x / .x[year_quarter == "2019 Q1"], .names = "{.col}_index"))

# 2.1 By Count
p3 = df_levels_country %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg, colour = idcountry, linetype = idcountry)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Count of WFH Job Ads in BGT EU (Alt)") +
  ylab("Count of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(label=comma, limits = c(0,NA), n.breaks = 10) + 
  scale_colour_ggthemr_d() +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(nrow = 1)) +
  theme(text = element_text(size = 16))
p3
ggexport(p3, filename = "./plots/text_based/eu_alt_wfh_level_by_country_sr.pdf")
remove(p3)

# 2.2 By Share
p3 = df_levels_country %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop, colour = idcountry, linetype = idcountry)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT EU (Alt)") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01),  breaks = seq(0,1,0.01)) +
  coord_cartesian(ylim = c(0, 0.195)) +
  scale_colour_ggthemr_d() +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(nrow = 1)) +
  theme(text = element_text(size = 16))
p3
ggexport(p3, filename = "./plots/text_based/eu_alt_wfh_share_by_country_20_sr.pdf")
remove(p3)

p3 = df_levels_country %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop, colour = idcountry, linetype = idcountry)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT EU (Alt)") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01),  breaks = seq(0,1,0.01)) +
  coord_cartesian(ylim = c(0, 0.10)) +
  scale_colour_ggthemr_d() +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(nrow = 1)) +
  theme(text = element_text(size = 16))
p3
ggexport(p3, filename = "./plots/text_based/eu_alt_wfh_share_by_country_10_sr.pdf")
remove(p3)

p3 = df_levels_country %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop, colour = idcountry, linetype = idcountry)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT EU (Alt)") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01),  breaks = seq(0,1,0.01)) +
  coord_cartesian(ylim = c(0, 0.05)) +
  scale_colour_ggthemr_d() +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(nrow = 1)) +
  theme(text = element_text(size = 16))
p3
ggexport(p3, filename = "./plots/text_based/eu_alt_wfh_share_by_country_5_sr.pdf")
remove(p3)

# 2.3 By Index (Count)
p4 = df_levels_country %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_index, colour = idcountry, linetype = idcountry)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT EU (Alt) (Count)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,1)) +
  coord_cartesian(ylim = c(0, 19.5)) +
  scale_colour_ggthemr_d() +
  
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(nrow = 1)) +
  theme(text = element_text(size = 16))
p4
ggexport(p4, filename = "./plots/text_based/eu_alt_wfh_index_count_by_country_20_sr.pdf")
remove(p4)

p4 = df_levels_country %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_index, colour = idcountry, linetype = idcountry)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT EU (Alt) (Count)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,1)) +
  coord_cartesian(ylim = c(0, 10)) +
  scale_colour_ggthemr_d() +
  
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(nrow = 1)) +
  theme(text = element_text(size = 16))
p4
ggexport(p4, filename = "./plots/text_based/eu_alt_wfh_index_count_by_country_10_sr.pdf")
remove(p4)

# 2.3 By Index (Share)
p4 = df_levels_country %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop_index, colour = idcountry, linetype = idcountry)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT EU (Alt) (Share)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,1)) +
  coord_cartesian(ylim = c(0, 10)) +
  scale_colour_ggthemr_d() +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(nrow = 1)) +
  theme(text = element_text(size = 16))
p4
ggexport(p4, filename = "./plots/text_based/eu_alt_wfh_index_share_by_country_10_sr.pdf")
remove(p4)

p4 = df_levels_country %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop_index, colour = idcountry, linetype = idcountry)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT EU (Alt) (Share)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,1)) +
  coord_cartesian(ylim = c(0, 5)) +
  scale_colour_ggthemr_d() +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(nrow = 1)) +
  theme(text = element_text(size = 16))
p4
ggexport(p4, filename = "./plots/text_based/eu_alt_wfh_index_share_by_country_5_sr.pdf")
remove(p4)

# 3. TIME SERIES PLOTS BY ESCO MAJOR GROUP
# Load Data
remove(list = setdiff(ls(),"df_stru"))
#load(file = "./int_data/df_stru_wfh_eu_alt.RData")
obs <- nrow(df_stru)
df_stru <- df_stru %>% as_tibble
head(df_stru)
df_levels_esco1 <- df_stru %>%
  mutate(idesco_level_1 = str_sub(idesco_level_2, 1, 1)) %>%
  filter(idesco_level_1 != "") %>%
  filter(!is.na(idesco_level_1)) %>%
  group_by(year_quarter, idesco_level_1) %>%
  summarise(n_posts = n(),
            wfh_non_neg = sum(wfh_non_neg)
  ) %>%
  mutate(across(wfh_non_neg, ~ .x / n_posts, .names = "{.col}_prop")) %>%
  ungroup() %>%
  group_by(idesco_level_1) %>%
  mutate(across(wfh_non_neg, ~ .x / .x[year_quarter == "2019 Q1"], .names = "{.col}_index")) %>%
  mutate(across(wfh_non_neg_prop, ~ .x / .x[year_quarter == "2019 Q1"], .names = "{.col}_index"))

# Names
df_levels_esco1 <- df_levels_esco1 %>%
  mutate(esco1 = case_when(
    idesco_level_1 == "0" ~ "0. Armed forces",
    idesco_level_1 == "1" ~ "1. Managers",
    idesco_level_1 == "2" ~ "2. Professionals",
    idesco_level_1 == "3" ~ "3. Technicians",
    idesco_level_1 == "4" ~ "4. Clerical support",
    idesco_level_1 == "5" ~ "5. Service & sales",
    idesco_level_1 == "6" ~ "6. Skilled agricultural",
    idesco_level_1 == "7" ~ "7. Craft & related trades",
    idesco_level_1 == "8" ~ "8. Plant & machine operators",
    idesco_level_1 == "9" ~ "9. Elementary"))

# 3.1 By Count
p3 = df_levels_esco1 %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg, colour = esco1, linetype = esco1)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Count of WFH Job Ads in BGT EU (Alt)") +
  ylab("Count of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(label=comma, limits = c(0,NA), n.breaks = 10) + 
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(ncol = 2)) +
  theme(text = element_text(size = 16))
p3
ggexport(p3, filename = "./plots/text_based/eu_alt_wfh_level_by_esco1_sr.pdf")
remove(p3)

# 3.2 By Share
p3 = df_levels_esco1 %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop, colour = esco1, linetype = esco1)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT EU (Alt)") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01),  breaks = seq(0,1,0.01)) +
  coord_cartesian(ylim = c(0, 0.20)) +
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(ncol = 2)) +
  theme(text = element_text(size = 16))
p3
ggexport(p3, filename = "./plots/text_based/eu_alt_wfh_share_by_esco1_20_sr.pdf")
remove(p3)

p3 = df_levels_esco1 %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop, colour = esco1, linetype = esco1)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT EU (Alt)") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01),  breaks = seq(0,1,0.01)) +
  coord_cartesian(ylim = c(0, 0.10)) +
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(ncol = 2)) +
  theme(text = element_text(size = 16))
p3
ggexport(p3, filename = "./plots/text_based/eu_alt_wfh_share_by_esco1_10_sr.pdf")
remove(p3)

p3 = df_levels_esco1 %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop, colour = esco1, linetype = esco1)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT EU (Alt)") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01),  breaks = seq(0,1,0.01)) +
  coord_cartesian(ylim = c(0, 0.05)) +
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(ncol = 2)) +
  theme(text = element_text(size = 16))
p3
ggexport(p3, filename = "./plots/text_based/eu_alt_wfh_share_by_esco1_5_sr.pdf")
remove(p3)

# 3.3 By Index (Count)
p4 = df_levels_esco1 %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_index, colour = esco1, linetype = esco1)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT EU (Alt) (Count)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,1)) +
  coord_cartesian(ylim = c(0, 19.5)) +
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(ncol = 2)) +
  theme(text = element_text(size = 16))
p4
ggexport(p4, filename = "./plots/text_based/eu_alt_wfh_index_count_by_esco1_20_sr.pdf")
remove(p4)

p4 = df_levels_esco1 %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_index, colour = esco1, linetype = esco1)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT EU (Alt) (Count)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,1)) +
  coord_cartesian(ylim = c(0, 10)) +
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(ncol = 2)) +
  theme(text = element_text(size = 16))
p4
ggexport(p4, filename = "./plots/text_based/eu_alt_wfh_index_count_by_esco1_10_sr.pdf")
remove(p4)

# 3.4 By Index (Share)
p4 = df_levels_esco1 %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop_index, colour = esco1, linetype = esco1)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT EU (Alt) (Share)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,1)) +
  coord_cartesian(ylim = c(0, 10)) +
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(ncol = 2)) +
  theme(text = element_text(size = 16))
p4
ggexport(p4, filename = "./plots/text_based/eu_alt_wfh_index_share_by_esco1_10_sr.pdf")
remove(p4)

p4 = df_levels_esco1 %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop_index, colour = esco1, linetype = esco1)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT EU (Alt) (Share)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,1)) +
  coord_cartesian(ylim = c(0, 5)) +
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  guides(col = guide_legend(ncol = 2)) +
  theme(text = element_text(size = 16))
p4
ggexport(p4, filename = "./plots/text_based/eu_alt_wfh_index_share_by_esco1_5_sr.pdf")
remove(p4)

# 4. TIME SERIES PLOTS BY DN TELEWORK COMPARISON
# Load Data
remove(list = setdiff(ls(),"df_stru"))
#load(file = "./int_data/df_stru_wfh_eu_alt.RData")
obs <- nrow(df_stru)
df_stru <- df_stru %>% as_tibble

dn <- haven::read_dta("./aux_data/country_isco08_telework.dta") %>%
  clean_names %>%
  filter(country_code %in% c("BEL", "DEU", "FRA", "ITA", "LUX", "NLD")) %>%
  mutate(idcountry = str_sub(country_code, 1, 2)) %>%
  select(idcountry, isco08_code_2digit, teleworkable) %>%
  as_tibble

nrow(df_stru) # 130,994,648
df_stru <- df_stru %>%
  left_join(dn, by = c("idcountry" = "idcountry", "idesco_level_2" = "isco08_code_2digit"))
nrow(df_stru) #  130,994,648
colnames(df_stru)

df_stru <- df_stru %>%
  mutate(teleworkable_bin = ifelse(teleworkable>=0.5, "Teleworkable", "Not"))

df_levels_telework <- df_stru %>%
  filter(!is.na(teleworkable_bin)) %>%
  group_by(year_quarter, teleworkable_bin) %>%
  summarise(n_posts = n(),
            wfh_non_neg = sum(wfh_non_neg)
  ) %>%
  mutate(across(wfh_non_neg, ~ .x / n_posts, .names = "{.col}_prop")) %>%
  ungroup() %>%
  group_by(teleworkable_bin) %>%
  mutate(across(wfh_non_neg, ~ .x / .x[year_quarter == "2019 Q1"], .names = "{.col}_index")) %>%
  mutate(across(wfh_non_neg_prop, ~ .x / .x[year_quarter == "2019 Q1"], .names = "{.col}_index"))

# 4.1 By Count
p3 = df_levels_telework %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg, colour = teleworkable_bin, linetype = teleworkable_bin)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Count of WFH Job Ads in BGT EU (Alt)") +
  ylab("Count of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(label=comma, limits = c(0,NA), n.breaks = 10) + 
  scale_colour_ggthemr_d() +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  theme(text = element_text(size = 16))
p3
ggexport(p3, filename = "./plots/text_based/eu_alt_wfh_level_by_telework_sr.pdf")
remove(p3)

# 4.2 By Share
p3 = df_levels_telework %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop, colour = teleworkable_bin, linetype = teleworkable_bin)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT EU (Alt)") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01),  breaks = seq(0,1,0.01)) +
  coord_cartesian(ylim = c(0, 0.195)) +
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  theme(text = element_text(size = 16))
p3
ggexport(p3, filename = "./plots/text_based/eu_alt_wfh_share_by_telework_20_sr.pdf")
remove(p3)

p3 = df_levels_telework %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop, colour = teleworkable_bin, linetype = teleworkable_bin)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT EU (Alt)") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01),  breaks = seq(0,1,0.01)) +
  coord_cartesian(ylim = c(0, 0.10)) +
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  theme(text = element_text(size = 16))
p3
ggexport(p3, filename = "./plots/text_based/eu_alt_wfh_share_by_telework_10_sr.pdf")
remove(p3)

p3 = df_levels_telework %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop, colour = teleworkable_bin, linetype = teleworkable_bin)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Share of WFH Job Ads in BGT EU (Alt)") +
  ylab("Share of WFH Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 0.01),  breaks = seq(0,1,0.01)) +
  coord_cartesian(ylim = c(0, 0.05)) +
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  theme(text = element_text(size = 16))
p3
ggexport(p3, filename = "./plots/text_based/eu_alt_wfh_share_by_telework_5_sr.pdf")
remove(p3)

# 4.3 By Index (Count)
p4 = df_levels_telework %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_index, colour = teleworkable_bin, linetype = teleworkable_bin)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT EU (Alt) (Count)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,1)) +
  coord_cartesian(ylim = c(0, 19.5)) +
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  theme(text = element_text(size = 16))
p4
ggexport(p4, filename = "./plots/text_based/eu_alt_wfh_index_count_by_telework_20_sr.pdf")
remove(p4)

p4 = df_levels_telework %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_index, colour = teleworkable_bin, linetype = teleworkable_bin)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT EU (Alt) (Count)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,1)) +
  coord_cartesian(ylim = c(0, 10)) +
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  theme(text = element_text(size = 16))
p4
ggexport(p4, filename = "./plots/text_based/eu_alt_wfh_index_count_by_telework_10_sr.pdf")
remove(p4)

# 4.4 By Index (Share)
p4 = df_levels_telework %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop_index, colour = teleworkable_bin, linetype = teleworkable_bin)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT EU (Alt) (Share)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,1)) +
  coord_cartesian(ylim = c(0, 10)) +
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  theme(text = element_text(size = 16))
p4
ggexport(p4, filename = "./plots/text_based/eu_alt_wfh_index_share_by_telework_10_sr.pdf")
remove(p4)

p4 = df_levels_telework %>%
  ggplot(., aes(x = year_quarter, y = wfh_non_neg_prop_index, colour = teleworkable_bin, linetype = teleworkable_bin)) +
  geom_line(size = 2) +
  #geom_point(size = 3) +
  ggtitle("Index of WFH Job Ads in BGT EU (Alt) (Share)") +
  ylab("Index of WFH Job Ads (2019 Q1 = 1)") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q', breaks = seq(from = as.yearqtr("2019 Q1"), to = as.yearqtr("2021 Q2"), by = 0.25), limits = c(as.yearqtr("2019 Q1"), as.yearqtr("2021 Q2"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,1)) +
  coord_cartesian(ylim = c(0, 5)) +
  scale_color_manual(values = c(swatch(), "grey")) +
  labs(subtitle = paste0("n = ",format(obs, big.mark=","))) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm")) +
  theme(text = element_text(size = 16))
p4
ggexport(p4, filename = "./plots/text_based/eu_alt_wfh_index_share_by_telework_5_sr.pdf")
remove(p4)

#### /end ####