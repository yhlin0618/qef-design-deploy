#' Clean Column Names by Removing English Characters
#'
#' Removes all English characters from column names, keeping only non-English characters
#'
#' @param column_names Character vector of column names to clean
#' @return Character vector with English characters removed
#'
#' @examples
#' cols <- c('價格Price', '材質Material')
#' clean_column_names_remove_english(cols)
clean_column_names_remove_english <- function(column_names) {
  # 使用正則表達式來移除所有英文字符（大小寫）
  cleaned_names <- gsub("[A-Za-z]", "", column_names)
  return(cleaned_names)
}
