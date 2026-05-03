library(tidyverse)
library(lares)


#data <- date_vector
Holidays_Recode<- function(data,time_loc="time",Yearnow = as.numeric(format(now(), "%Y")), 
                           Yearstart = as.numeric((format(now(), "%Y")))-2){
  
  if (is.data.frame(data)){
    timeVec <- data$time
    Result <- data
  }else{
    Result <- data.frame(time = data)
    timeVec <- data
  }

  importance_set <- c("Christmas Day","New Year's Eve")
  mk_importance_set <- make.names(importance_set)
  set_period_s <- c(-40,-6)
  set_period_e <- c(6,5)
  
  holiset <- holidays(years = c(Yearstart:Yearnow),countries = "US")
  holiday_data <- holiset %>% filter(holiday_name %in% importance_set) %>% 
  dplyr::select(all_of(c("holiday","holiday_name")))
  
  holiday_data$holiday_name <- make.names(holiday_data$holiday_name)
  
  for (i in 1:nrow(holiday_data)){
  holiday.row <- holiday_data[i,]$holiday
  holiday.name <- holiday_data[i,]$holiday_name
  start_p <- set_period_s[ which(holiday.name==mk_importance_set)]
  end_p <- set_period_e[ which(holiday.name==mk_importance_set)]
  diffdays <- as.Date(timeVec)-holiday.row
  
  is.holiday_period<- (diffdays>=(start_p) & diffdays<=end_p)
  # is.holiday_period <- ifelse(is.infinite(is.holiday_period),0,result$Is_Holiday_Period)
  
  if (is.null(Result[[holiday.name]])) {Result[[holiday.name]] <- FALSE}
  
  Result[[holiday.name]] <- (Result[[holiday.name]]|is.holiday_period)
  
}
  Result
}
# 
# start_date <- as.Date("2024-01-01")
# end_date <- as.Date("2024-12-31")
# date_vector <- seq.Date(from = start_date, to = end_date, by = "day")
# 
# 
# test <- Holidays_Recode(date_vector)
