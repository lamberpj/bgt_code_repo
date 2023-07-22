#### CITY TIME SERIES PLOTS - US ####
# City-level comparisons

  

# load city plots
remove(list = ls())
ts_for_plot <- fread(file = "./aux_data/city_level_ts.csv")
us_monthly_series <- ts_for_plot %>% .[city_state == "US"] %>% .[name == "monthly_mean_3ma_l1o"] %>% .[country == "US"]

check <- ts_for_plot %>% .[, year := year(job_date)] %>%
  .[year == 2022] %>% .[, .(N = sum(N)), by = city_state] %>% .[N > 250000]
unique(ts_for_plot$name)
ts_for_plot <- ts_for_plot %>% .[country == "US" & city_state %in% check$city_state] %>% .[name == "monthly_mean_3ma_l1o"] %>% .[country == "US"]
ts_for_plot_cit <- ts_for_plot %>%
  #.[city_state %in% us_city_list] %>%
  .[name == "monthly_mean_l1o"] %>%
  .[country == "US"] %>%
  .[city_state != "Columbus, Georgia"]

ts_for_plot_cit

divisions <- fread(file = "./aux_data/us census bureau regions and divisions.csv") %>% clean_names
nrow(ts_for_plot_cit)
ts_for_plot_cit <- ts_for_plot_cit %>%
  left_join(divisions)
nrow(ts_for_plot_cit)

ts_for_plot_cit <- ts_for_plot_cit %>%
  .[city_state %in% c("Fort Worth, Texas","Raleigh, North Carolina","Nashville, Tennessee", "Philadelphia, Pennsylvania","Sacramento, California",
                      "Cleveland, Ohio", "Detroit, Michigan","Las Vegas, Nevada","Kansas City, Missouri","Rochester, New York")]

us_monthly_series <- us_monthly_series %>% mutate(city_state = " US National", city = "US") %>% select(year_month, city_state, city, name, value)

ts_for_plot_cit <- bind_rows(ts_for_plot_cit, us_monthly_series)

ts_for_plot_cit$city <- as.character(ts_for_plot_cit$city)

unique(ts_for_plot_cit$city_state)

library("RColorBrewer")
(pal <- c("black", "darkgrey", "brown", brewer.pal(8, "Dark2")))

ts_for_plot_cit_filter <- ts_for_plot_cit %>%
  mutate(lab = ifelse(year_month == max(year_month), paste0("bold(",gsub(" ","",city),")"), NA_character_)) %>%
  .[year(as.Date(as.yearmon(year_month)))>= 2019]

p = ts_for_plot_cit_filter %>%
  ggplot(., aes(x = as.Date(as.yearmon(year_month)), y = 100*value, colour = city_state)) +
  #stat_smooth (geom="line", alpha=0.8, size=2, span=0.2) +
  #stat_smooth(span = 0.1, alpha=0.5, se=FALSE, size = 2) +
  geom_point(size = 2) +
  geom_line(size = 1, alpha = 0.5) +
  ylab("Percentage") +
  xlab("Date") +
  #labs(title = "Share of Postings Advertising Remote Work (%)", subtitle = paste0("Unweighted, Outliers Removed. Removed ")) +
  scale_x_date(breaks = as.Date(c("2020-01-01", "2020-04-01", "2020-07-01", "2020-10-01",
                                  "2021-01-01", "2021-04-01", "2021-07-01", "2021-10-01", "2022-01-01", "2022-04-01", "2022-07-01", "2022-10-01",
                                  "2023-01-01", "2023-04-01")),
               date_labels = '%Y-%m',
               limits = as.Date(c("2022-04-01", "2023-05-01"))) +
  scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,5)) +
  coord_cartesian(ylim = c(0, 22)) +
  scale_color_manual(values = pal) +
  theme(
    axis.title.x=element_blank(),
    legend.position="bottom",
    legend.title = element_blank(),
    axis.text.x = element_text(angle = 45, vjust = 1, hjust=0.9)) +
  theme(text = element_text(size=15, family="serif", colour = "black"),
        axis.text = element_text(size=14, family="serif", colour = "black"),
        axis.title = element_text(size=15, family="serif", colour = "black"),
        legend.text = element_text(size=14, family="serif"),
        panel.background = element_rect(fill = "white"),
        legend.key.width = unit(1,"cm")) +
  guides(col = guide_legend(ncol = 2)) +
  labs(color  = "Guide name", shape = "Guide name") +
  # geom_label_repel(aes(family = c("serif"), label = lab, x = as.Date(as.yearmon(year_month)) %m+% months(1),
  #                      y = 100*value), force = 0.25, force_pull = 4, hjust = "left", vjust = "top", direction = "y", parse=TRUE,
  #                  label.padding = 0, label.size = NA, size = 5, show.legend = FALSE, segment.color = "transparent") +
  theme(aspect.ratio=4/5)
p

# p = ts_for_plot_cit %>%
#   mutate(lab = ifelse(year_month == max(year_month), paste0("bold(",gsub(" ","",city),")"), NA_character_)) %>%
#   .[year(as.Date(as.yearmon(year_month)))>= 2019] %>%
#   filter(region %in% c("South") | city_state == " US National") %>%
#   ggplot(., aes(x = as.Date(as.yearmon(year_month)), y = 100*value, colour = city_state)) +
#   #stat_smooth (geom="line", alpha=0.8, size=2, span=0.2) +
#   #stat_smooth(span = 0.1, alpha=0.5, se=FALSE, size = 2) +
#   geom_point(size = 2) +
#   geom_line(size = 1, alpha = 0.5) +
#   ylab("Percent") +
#   xlab("Date") +
#   #labs(title = "Share of Postings Advertising Remote Work (%)", subtitle = paste0("Unweighted, Outliers Removed. Removed ")) +
#   scale_x_date(breaks = as.Date(c("2019-01-01", "2019-04-01", "2019-07-01", "2019-10-01","2020-01-01", "2020-04-01", "2020-07-01", "2020-10-01",
#                                   "2021-01-01", "2021-04-01", "2021-07-01", "2021-10-01", "2022-01-01", "2022-04-01", "2022-07-01")),
#                date_labels = '%Y-%m',
#                limits = as.Date(c("2019-01-01", "2022-07-01"))) +
#   scale_y_continuous(labels = scales::number_format(accuracy = 1),  breaks = seq(0,100,5)) +
#   #coord_cartesian(ylim = c(0, 20)) +
#   scale_shape_manual(values=c(15,16,17,18,19,15,16,17,18,19)) +
#   scale_color_manual(values = pal) +
#   theme(
#     axis.title.x=element_blank(),
#     legend.position="bottom",
#     legend.title = element_blank(),
#     axis.text.x = element_text(angle = 45, vjust = 1, hjust=0.9)) +
#   theme(text = element_text(size=15, family="serif", colour = "black"),
#         axis.text = element_text(size=14, family="serif", colour = "black"),
#         axis.title = element_text(size=15, family="serif", colour = "black"),
#         legend.text = element_text(size=14, family="serif", colour = "black"),
#         panel.background = element_rect(fill = "white"),
#         legend.key.width = unit(1,"cm")) +
#   guides(col = guide_legend(ncol = 2)) +
#   labs(color  = "Guide name", shape = "Guide name") +
#   # geom_label_repel(aes(family = c("serif"), label = lab, x = as.Date(as.yearmon(year_month)) %m+% months(1),
#   #                      y = 100*value), force = 0.25, force_pull = 4, hjust = "left", vjust = "top", direction = "y", parse=TRUE,
#   #                  label.padding = 0, label.size = NA, size = 5, show.legend = FALSE, segment.color = "transparent") +
#   theme(aspect.ratio=1.5/5)
# p
# save(p, file = "./ppt/ggplots/ts_cities_s.RData")

#### END ####