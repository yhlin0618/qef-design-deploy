# Helper functions for KitchenMAMA Precision Marketing Platform

#' Safe Get Function
#'
#' Safely loads an RDS file, returning NULL if the file doesn't exist
#'
#' @param name The name of the RDS file (without extension)
#' @param path The path to the RDS files directory
#'
#' @return The loaded object or NULL
#'
safe_get <- function(name, path = "app_data") {
  file_path <- file.path(path, paste0(name, ".rds"))
  if (file.exists(file_path)) {
    tryCatch({
      readRDS(file_path)
    }, error = function(e) {
      warning(paste("Error reading file:", file_path, "-", e$message))
      NULL
    })
  } else {
    NULL
  }
}

#' Format Time Function
#'
#' Formats a date object based on the specified time scale
#'
#' @param time_scale The date to format
#' @param case The time scale to use (year, quarter, month)
#'
#' @return A formatted string
#'
formattime <- function(time_scale, case) {
  if (case == "quarter") {
    return(gsub("\\.", ".Q", lubridate::quarter(time_scale, type = "year.quarter")))
  }
  if (case == "year") {
    return(format(time_scale, "%Y"))
  }
  if (case == "month") {
    return(format(time_scale, "%Y/%m"))
  }
}

#' Clean Column Names Function
#'
#' Removes English text from Chinese column names
#'
#' @param column_names A character vector of column names
#'
#' @return A cleaned character vector
#'
clean_column_names_remove_english <- function(column_names) {
  # Ensure input is a character vector
  column_names <- as.character(column_names)
  
  # Detect Chinese and English/underscore characters
  contains_chinese <- str_detect(column_names, "[\\u4e00-\\u9fa5]")
  contains_english_or_underscore <- str_detect(column_names, "[A-Za-z_]")
  
  # Apply cleaning only to columns with both Chinese and English/underscore
  result <- ifelse(
    contains_chinese & contains_english_or_underscore,
    str_remove_all(column_names, "[A-Za-z_]+"),
    column_names
  )
  
  return(result)
}

#' Get Dynamic Options Function
#'
#' Gets unique values from a data frame based on a filter
#'
#' @param list Filter values
#' @param dta Data frame
#' @param invariable Variable to filter on
#' @param outvariable Variable to extract values from
#'
#' @return A sorted vector of unique values
#'
getDynamicOptions <- function(list, dta, invariable, outvariable) {
  dta %>%
    dplyr::filter({{invariable}} %in% list) %>%
    dplyr::pull({{outvariable}}) %>%
    unique() %>%
    sort()
}

#' Create Choices Function
#'
#' Creates a list of unique values from a data frame column
#'
#' @param dta Data frame
#' @param variable Variable to extract values from
#'
#' @return A sorted vector of unique values
#'
CreateChoices <- function(dta, variable) {
  dta %>%
    dplyr::select({{ variable }}) %>%
    dplyr::pull() %>%
    unique() %>% 
    sort()
}

#' Remove Elements Function
#'
#' Removes specified elements from a vector, ignoring case
#'
#' @param vector The input vector
#' @param elements Elements to remove
#'
#' @return The filtered vector
#'
remove_elements <- function(vector, elements) {
  vector <- vector[!tolower(vector) %in% tolower(elements)]
  return(vector)
}

#' Recode Time TraceBack Function
#'
#' Converts a time scale to the corresponding historical time scale
#'
#' @param profile The time scale to convert
#'
#' @return The corresponding historical time scale
#'
Recode_time_TraceBack <- function(profile) {
  switch(profile,
         "m1quarter" = "m1quarter",
         "m1year" = "m1year",
         "m1month" = "m1month",
         "quarter" = "m1quarter",
         "year" = "m1year",
         "month" = "m1month",
         NA)  # If no match, return NA
}

#' Process Sales Data Function
#'
#' Process and summarize sales data by time interval
#'
#' @param SalesPattern Sales data frame
#' @param time_scale_profile Time scale to use (year, quarter, month)
#'
#' @return Processed sales data frame
#'
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