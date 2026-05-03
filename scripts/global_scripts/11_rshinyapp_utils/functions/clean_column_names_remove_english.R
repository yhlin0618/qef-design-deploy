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