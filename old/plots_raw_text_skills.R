
#### PLOTS ####
remove(list = ls())
load(file = "./int_data/df_stru_us.RData")

colnames(df_stru)

obs <- nrow(df_stru)

df_stru <- df_stru %>% filter(as.numeric(quarter(ymd(job_date))) %in% c(2,4))

# Make Quarter
df_stru <- df_stru %>%
  mutate(year_quarter = as.yearqtr(ymd(job_date), with_year = T))

# Hit Miss plot
df_hm <- df_stru %>%
  mutate(ai_s0_t0 = ifelse(ai_skill == 0 & ai_cluster_text == 0, 1, 0),
         ai_s1_t0 = ifelse(ai_skill == 1 & ai_cluster_text == 0, 1, 0),
         ai_s0_t1 = ifelse(ai_skill == 0 & ai_cluster_text == 1, 1, 0),
         ai_s1_t1 = ifelse(ai_skill == 1 & ai_cluster_text == 1, 1, 0),
         ml_s0_t0 = ifelse(ml_skill == 0 & ml_cluster_text == 0, 1, 0),
         ml_s1_t0 = ifelse(ml_skill == 1 & ml_cluster_text == 0, 1, 0),
         ml_s0_t1 = ifelse(ml_skill == 0 & ml_cluster_text == 1, 1, 0),
         ml_s1_t1 = ifelse(ml_skill == 1 & ml_cluster_text == 1, 1, 0)) %>%
  summarise("AI s=1 t = 0" = sum(ai_s1_t0),
            "AI s=0 t = 1" = sum(ai_s0_t1),
            "AI s=1 t = 1" = sum(ai_s1_t1),
            "ML s=1 t = 0" = sum(ml_s1_t0),
            "ML s=0 t = 1" = sum(ml_s0_t1),
            "ML s=1 t = 1" = sum(ml_s1_t1))

df_hm_sum <- data.frame("names" = rownames(t(df_hm)), "count" = t(df_hm)[,1]) %>%
  mutate(cluster = str_sub(names, 1, 2)) %>%
  group_by(cluster) %>%
  mutate(prop = round(count/sum(count), 3)) %>%
  ungroup() %>%
  mutate(count = format(count, big.mark=",")) %>%
  select(names, count, prop)

stargazer(df_hm_sum, summary=FALSE, rownames=FALSE, title="Compare Text to Skills: US Data Hit/Miss", align=TRUE)

# Levels and Changes
df_levels <- df_stru %>%
  group_by(year_quarter) %>%
  summarise(n_posts = n(),
            ai_cluster_text_posts = sum(ai_cluster_text),
            ml_cluster_text_posts = sum(ml_cluster_text),
            ai_skill_posts = sum(ai_skill),
            ml_skill_posts = sum(ml_skill),
            ai_cluster_text_prop = mean(ai_cluster_text, na.rm = T),
            ml_cluster_text_prop = mean(ml_cluster_text, na.rm = T),
            ai_skill_prop = mean(ai_skill, na.rm = T),
            ml_skill_prop = mean(ml_skill, na.rm = T),
  )

# Plot Posts
df_ts_posts <- df_levels %>%
  select(year_quarter, ai_skill_posts, ml_skill_posts, ai_cluster_text_posts, ml_cluster_text_posts) %>%
  group_by(year_quarter) %>%
  pivot_longer(., cols = c(ai_skill_posts, ml_skill_posts, ai_cluster_text_posts, ml_cluster_text_posts)) %>%
  ungroup() %>%
  mutate(name = case_when(
    name == "ai_skill_posts" ~ "AI (skills)",
    name == "ml_skill_posts" ~ "ML (skills)",
    name == "ai_cluster_text_posts" ~ "AI (text)",
    name == "ml_cluster_text_posts" ~ "ML (text)")) %>%
  mutate(Cluster = str_sub(name, 1, 2),
         Method = str_sub(gsub("[\\(\\)]", "", name), 4, -1))

head(df_ts_posts)

p1 = df_ts_posts %>%
  ggplot(., aes(x = year_quarter, y = value, colour = Cluster, linetype = Method)) +
  geom_line(size = 2) +
  geom_point(size = 3) +
  ylab("Count of Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q',n = 28) +
  scale_y_continuous(label=comma) +
  scale_colour_ggthemr_d() +
  #ggtitle("Count of US Job Ads requiring AI and ML Skills") +
  #labs(subtitle = paste0("n = ",format(obs, big.mark=","),". Comparing Raw Text to BGT Skill Cluster results"),
  #     caption = "Note: Using BGT US Job Ads, Q2/Q4 only.") +
  theme(
    legend.position="bottom",
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm"))

#ggexport(p1, filename = "./plots/text_based/us_level_ai_ml_skills_vs_text.pdf")

# Proportions
df_ts_prop <- df_levels %>%
  select(year_quarter, ai_skill_prop, ml_skill_prop, ai_cluster_text_prop, ml_cluster_text_prop) %>%
  group_by(year_quarter) %>%
  pivot_longer(., cols = c(ai_skill_prop, ml_skill_prop, ai_cluster_text_prop, ml_cluster_text_prop)) %>%
  ungroup() %>%
  mutate(name = case_when(
    name == "ai_skill_prop" ~ "AI (skills)",
    name == "ml_skill_prop" ~ "ML (skills)",
    name == "ai_cluster_text_prop" ~ "AI (text)",
    name == "ml_cluster_text_prop" ~ "ML (text)")) %>%
  mutate(Cluster = str_sub(name, 1, 2),
         Method = str_sub(gsub("[\\(\\)]", "", name), 4, -1))

p2 = df_ts_prop %>%
  ggplot(., aes(x = year_quarter, y = value, colour = Cluster, linetype = Method)) +
  geom_line(size = 2) +
  geom_point(size = 3) +
  ylab("Proportion of Job Postings") +
  xlab("Year / Quarter") +
  scale_x_yearqtr(format = '%YQ%q',n = 28) +
  scale_y_continuous(label=comma, limits = c(0,0.01)) +
  scale_colour_ggthemr_d() +
  #ggtitle("Proportion of US Job Ads requiring AI and ML Skills") +
  #labs(subtitle = paste0("n = ",format(obs, big.mark=","),". Comparing Raw Text to BGT Skill Cluster results"),
  #     caption = "Note: Using BGT US Job Ads, Q2/Q4 only.") +
  theme(
    legend.position="bottom",
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
    legend.key.width=unit(1.5,"cm"))

a1 = ggarrange(p1, p2, ncol = 1, nrow = 2, legend = "bottom", common.legend = T, align = "v") %>%
  annotate_figure(.,
                  top = text_grob("Figure X: ML/AI in United States Job Ads"))

ggexport(a1, filename = "./plots/text_based/us_level_prop_ai_ml.pdf")
remove(list = c("p1","p2","a1"))

#### COEF PLOTS ####
head(df_stru)

df_stru <- df_stru %>%
  mutate(year_month = paste0(year(ymd(job_date)),"_",month(ymd(job_date)))) %>%
  mutate(year_quarter_text = paste0(year(ymd(job_date)), ".", quarter(ymd(job_date)))) %>%
  mutate(month = month(ymd(job_date)))

head(df_stru)

df_stru <- df_stru %>%
  mutate(later_quarters = case_when(
    year_quarter_text %in% c("2018.1") ~ "2018Q1",
    year_quarter_text %in% c("2018.2") ~ "2018Q2",
    year_quarter_text %in% c("2018.3") ~ "2018Q3",
    year_quarter_text %in% c("2018.4") ~ "2018Q4",
    year_quarter_text %in% c("2019.1") ~ "2019Q1",
    year_quarter_text %in% c("2019.2") ~ "2019Q2",
    year_quarter_text %in% c("2019.3") ~ "2019Q3",
    year_quarter_text %in% c("2019.4") ~ "2019Q4",
    year_quarter_text %in% c("2020.1") ~ "2020Q1",
    year_quarter_text %in% c("2020.2") ~ "2020Q2",
    year_quarter_text %in% c("2020.3") ~ "2020Q3",
    year_quarter_text %in% c("2020.4") ~ "2020Q4",
    year_quarter_text %in% c("2021.1") ~ "2021Q1",
    year_quarter_text %in% c("2021.2") ~ "2021Q2",
    year_quarter_text %in% c("2021.3") ~ "2021Q3",
    year_quarter_text %in% c("2021.4") ~ "2021Q4",
    TRUE ~ "1pre"
  ))

regs_ml <- safe_mclapply(1:4, function(i) {
  if(i == 1) {lm <- feols(ml_skill ~ later_quarters | month, df_stru, cluster = ~ year_month, lean = T)}
  if(i == 2) {lm <- feols(ml_cluster_text ~ later_quarters | month, df_stru, cluster = ~ year_month, lean = T)}
  if(i == 3) {lm <- feols(ml_skill ~ later_quarters | month + msa + soc, df_stru, cluster = ~ year_month, lean = T)}
  if(i == 4) {lm <- feols(ml_cluster_text ~ later_quarters | month + msa + soc, df_stru, cluster = ~ year_month, lean = T)}
  warning("\n\n############# ",i," #############\n\n")
  return(lm)
}, mc.cores = 4)

regs_ai <- safe_mclapply(1:4, function(i) {
  if(i == 1) {lm <- feols(ai_skill ~ later_quarters | month, df_stru, cluster = ~ year_month, lean = T)}
  if(i == 2) {lm <- feols(ai_cluster_text ~ later_quarters | month, df_stru, cluster = ~ year_month, lean = T)}
  if(i == 3) {lm <- feols(ai_skill ~ later_quarters | month + msa + soc, df_stru, cluster = ~ year_month, lean = T)}
  if(i == 4) {lm <- feols(ai_cluster_text ~ later_quarters | month + msa + soc, df_stru, cluster = ~ year_month, lean = T)}
  warning("\n\n############# ",i," #############\n\n")
  return(lm)
}, mc.cores = 4)

regs_ml[[1]]$coeftable

for_plot_ml <- lapply(1:4, function(i) {
  regs_ml[[i]]$coeftable %>%
    as_tibble %>%
    mutate(estimate = Estimate, lb = (Estimate - 2*`Std. Error`), ub = (Estimate + 2*`Std. Error`)) %>%
    select(estimate, lb, ub) %>%
    as_tibble %>%
    mutate(quarter = as.yearqtr(c("2018 Q2","2018 Q4","2019 Q2","2019 Q4","2020 Q2","2020 Q4","2021 Q2")))
})

for_plot_ai <- lapply(1:4, function(i) {
  regs_ai[[i]]$coeftable %>%
    as_tibble %>%
    mutate(estimate = Estimate, lb = (Estimate - 2*`Std. Error`), ub = (Estimate + 2*`Std. Error`)) %>%
    select(estimate, lb, ub) %>%
    as_tibble %>%
    mutate(quarter = as.yearqtr(c("2018 Q2","2018 Q4","2019 Q2","2019 Q4","2020 Q2","2020 Q4","2021 Q2")))
})

for_plot_long_ml <- bind_rows(for_plot_ml[[1]] %>% mutate(group = "1", Method = "Skill", FEs = "Month"),
                              for_plot_ml[[2]] %>% mutate(group = "2", Method = "Text", FEs = "Month"),
                              for_plot_ml[[3]] %>% mutate(group = "3", Method = "Skill", FEs = "Month x Occ x Geog"),
                              for_plot_ml[[4]] %>% mutate(group = "4", Method = "Text", FEs = "Month x Occ x Geog"))

for_plot_long_ai <- bind_rows(for_plot_ai[[1]] %>% mutate(group = "1", Method = "Skill", FEs = "Month"),
                              for_plot_ai[[2]] %>% mutate(group = "2", Method = "Text", FEs = "Month"),
                              for_plot_ai[[3]] %>% mutate(group = "3", Method = "Skill", FEs = "Month x Occ x Geog"),
                              for_plot_ai[[4]] %>% mutate(group = "4", Method = "Text", FEs = "Month x Occ x Geog"))

p1 = for_plot_long_ml %>%
  filter(Method == "Text") %>%
  ggplot(., aes(x = quarter, y = estimate, linetype = FEs, color = "#00d166")) +
  geom_line(size = 2) +
  geom_point(size = 3) +
  geom_ribbon(aes(ymin = lb, ymax = ub), alpha = 0.10, fill = "Black", size = 0) +
  ylab("Quarter Dummy Coefficient") +
  xlab("Year") +
  scale_x_yearqtr(format = '%YQ%q',n = 28) +
  scale_y_continuous(label=comma, limits = c(0, 0.01)) +
  scale_colour_manual(values="#00d166") +
  #ggtitle("Coeff Plot ML Skills") +
  #labs(subtitle = paste0("n = ",obs,". Fixed Effects: Month, Occupation, Region"),
  #     caption = "Note: Using BGT US Job Ads.\n+/- 2 SD in dark grey.  +/- 2 SD in light grey.") +
  theme(legend.position="bottom",
        axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
        legend.key.width=unit(1.5,"cm"))

p2 = for_plot_long_ai %>%
  filter(Method == "Text") %>%
  filter() %>%
  ggplot(., aes(x = quarter, y = estimate, linetype = FEs)) +
  geom_line(size = 2) +
  geom_point(size = 3) +
  geom_ribbon(aes(ymin = lb, ymax = ub), alpha = 0.10, fill = "Black", size = 0) +
  ylab("Quarter Dummy Coefficient") +
  xlab("Year") +
  scale_x_yearqtr(format = '%YQ%q',n = 28) +
  scale_y_continuous(label=comma, limits = c(0, 0.01)) +
  scale_colour_ggthemr_d() +
  #ggtitle("Coeff Plot ML Skills") +
  #labs(subtitle = paste0("n = ",obs,". Fixed Effects: Month, Occupation, Region"),
  #     caption = "Note: Using BGT US Job Ads.\n+/- 2 SD in dark grey.  +/- 2 SD in light grey.") +
  theme(legend.position="bottom",
        axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
        legend.key.width=unit(1.5,"cm"))
p2

p_leg = bind_rows(for_plot_long_ml %>% mutate(Cluster = "ML"), for_plot_long_ai %>% mutate(Cluster = "AI")) %>%
  filter() %>%
  ggplot(., aes(x = quarter, y = estimate, color = Cluster, linetype = FEs)) +
  scale_colour_ggthemr_d() +
  geom_line(size = 2) +
  theme(legend.position="bottom",
        axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
        legend.key.width=unit(1.5,"cm"))
p_leg

a1 = ggarrange(p1, p2, ncol = 1, nrow = 2, legend = "bottom", legend.grob = get_legend(p_leg), align = "v") %>%
  annotate_figure(.,
                  top = text_grob("Figure X: Coefficient Plots of ML (top) and AI (bottom) in United States"),
  )
a1
ggexport(a1, filename = "./plots/text_based/us_cp_ai_ml.pdf")
#remove(list = c("p1","p2","a1"))

