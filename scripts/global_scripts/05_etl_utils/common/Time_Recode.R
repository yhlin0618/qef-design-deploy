select <- dplyr::select
# 
# time_Recode <- function(data,time_loc="time",ID_col="transaction_id"){
#   #details: see generate_dummy_matrix.R
#    # data <- officialwebsite_sales_zip_dta
#    # time_loc="time_Local"
#   flag <- 0
#   if (is.null(dim(data))) {
#     data <- tibble(idx=c(1:length(data)),time=data)
#     data[[ID_col]] <- data$idx
#     flag <- 1
#   }
#   data <- data %>% mutate(key=1:n())
#   outdata <- data %>%  select(key,!!sym(ID_col),!!sym(time_loc)  )%>% mutate(
#     year = year( as.Date(!!sym(time_loc))),
#     month = month( as.Date(!!sym(time_loc))),
#     day = day( as.Date(!!sym(time_loc)))) %>%
#     mutate(month = as.factor(month)) %>%
#     mutate(month_dummy = 1) %>%
#     pivot_wider(
#       names_from = month,
#       names_prefix = "month_",  # 添加前缀
#       values_from = month_dummy,
#       values_fill = list(month_dummy = 0)
#     ) %>% mutate(WEEK_DAY=weekdays(as.Date(!!sym(time_loc)))) %>%
#     mutate(WEEK_DAY = as.factor(WEEK_DAY)) %>%
#     mutate(week_dummy = 1) %>%
#     pivot_wider(
#       names_from = WEEK_DAY,
#       values_from = week_dummy,
#       values_fill = list(week_dummy = 0)) %>% select(-!!sym(ID_col),-!!sym(time_loc)  )
#   outdata <- left_join(data,outdata,by="key") %>% select(-key)
# 
#   if(flag==1){
#     outdata <- outdata %>% dplyr::select(-idx,-!!sym(ID_col))
#   }
#   
#   return(outdata)
# }




time_Recode <- function(data,time_loc="time"){
  #details: see generate_dummy_matrix.R
  # data <- officialwebsite_sales_zip_dta
  # time_loc="time_Local"
  os_type <- Sys.info()["sysname"]
  
  # Set locale based on OS
  if (os_type == "Windows") {
    locale <- "English_United States"
  } else if (os_type == "Darwin") { # macOS
    locale <- "en_US.UTF-8"
  } else if (os_type == "Linux") {
    locale <- "en_US.UTF-8"
  } else {
    stop("Unsupported operating system. Cannot set locale.")
  }
  
  
  flag <- 0
  if (is.null(dim(data))) {
    data <- tibble(idx=c(1:length(data)),time=data)
    flag <- 1
  }
  data <- data %>% mutate(key=1:n())
  outdata <- data %>%  select(key,!!sym(time_loc)  )%>% mutate(
    year = year( as.Date(!!sym(time_loc))),
    month = month( as.Date(!!sym(time_loc))),
    day = day( as.Date(!!sym(time_loc)))) %>%
    mutate(month = as.factor(month)) %>%
    mutate(month_dummy = 1) %>%
    pivot_wider(
      names_from = month,
      names_prefix = "month_",  # 添加前缀
      values_from = month_dummy,
      values_fill = list(month_dummy = 0)
    ) %>% mutate(WEEK_DAY=weekdays(as.Date(!!sym(time_loc)))) %>%
    mutate(WEEK_DAY = as.factor(WEEK_DAY)) %>%
    mutate(week_dummy = 1) %>%
    pivot_wider(
      names_from = WEEK_DAY,
      values_from = week_dummy,
      values_fill = list(week_dummy = 0)) %>% select(-!!sym(time_loc)  )
  outdata <- left_join(data,outdata,by="key") %>% select(-key)
  
  if(flag==1){
    outdata <- outdata %>% dplyr::select(-idx)
  }
  
  return(outdata)
}




######Example

# start_date <- as.Date("2024-01-01")
# end_date <- as.Date("2024-12-31")
# date_vector <- seq.Date(from = start_date, to = end_date, by = "day")
# 
# 
# test <- time_Recode(date_vector)


############
##Needto fix
# officialwebsite_sales_zip_dta <- readRDS(file.path(Data_folder,"officialwebsite_sales_zip_dta.rds"))
# a <- time_Recode(officialwebsite_sales_zip_dta,time_loc="time_Local")
# a[a$`NA`==1,]

#time_Local 是當地時間的文字檔
#time 有換算成時間的格式但好像也不能跑