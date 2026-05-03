#' Extract Numeric Rating from String
#'
#' @title Find Number or Return NaN
#' @description Extracts numeric ratings (0-10) from character strings, handling
#' various formats including NaN values. This function is designed to parse
#' ratings that may be embedded in text or have inconsistent formatting.
#' It specifically looks for numbers in the 0-10 range, which is common for
#' rating scales.
#'
#' @param s Character string potentially containing a numeric rating (0-10)
#'        or NaN/NA indicators
#'        
#' @return Integer value between 0-10 if found, NA_integer_ if not found or
#'         if input contains NaN/nan/NA
#'         
#' @examples
#' # Extract rating from clean numeric string
#' find_number_or_nan("8")        # Returns 8L
#' find_number_or_nan("10")       # Returns 10L
#' 
#' # Handle NaN and NA cases
#' find_number_or_nan("NaN")      # Returns NA_integer_
#' find_number_or_nan("nan")      # Returns NA_integer_
#' find_number_or_nan(NA)         # Returns NA_integer_
#' 
#' # Extract from text
#' find_number_or_nan("Rating: 7") # Returns 7L
#' find_number_or_nan("Score is 10 out of 10") # Returns 10L
#' 
#' # No valid rating found
#' find_number_or_nan("excellent") # Returns NA_integer_
#' find_number_or_nan("15")        # Returns NA_integer_ (out of 0-10 range)
#' 
#' @export
#' @principle R094 Roxygen2 documentation standard
#' @principle MP047 Functional programming
#' @note Requires stringr package for str_match function
find_number_or_nan <- function(s) {
  # 检查是否为 NA 或包含 'NaN'
  if (is.na(s) || grepl("NaN|nan", s, ignore.case = TRUE)) {
    return(NA_integer_)
  }
  
  # 匹配 0-10 的数字
  match <- str_match(s, "\\b(10|[0-9])\\b")[, 1]
  if (!is.na(match)) {
    return(as.integer(match))
  } else {
    return(NA_integer_)
  }
}

# 定义函数：处理单个元素
process_element <- function(x) {
  if (is.character(x)) {
    # 分割字符串
    first_element <- str_split(x, ",|，", simplify = TRUE)[, 1]
    # 提取第一个数字
    return(find_number_or_nan(first_element))
  } else {
    return(NA_integer_)
  }
}

# 对指定列范围应用转换
process_columns <- function(df, start_column) {
  df %>%
    mutate(across(start_column:ncol(df), 
                  ~ ifelse(is.character(.x), sapply(.x, process_element), NA_integer_)))
}
