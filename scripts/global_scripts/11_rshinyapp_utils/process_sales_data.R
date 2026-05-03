process_sales_data <- function(SalesPattern, time_scale_profile) {
  SalesPattern %>% rename_with(make_names) %>% 
    group_by(time_interval = floor_date(time_scale, unit = time_scale_profile)) %>% 
    dplyr::summarise(total = sum(total),
                    num_customers = sum(num_customers)) %>% 
    mutate(Difference = round(total - lag(total, default = NA), 2),
           Average = round(total / num_customers, 2)) %>%
    mutate(time_interval = formattime(time_interval, time_scale_profile)) %>% 
    dplyr::select(time_interval, total, Difference, num_customers, Average)
}
