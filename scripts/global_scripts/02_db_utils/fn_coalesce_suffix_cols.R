#' Merge suffix columns back to original columns
#'
#' @description
#' Merges columns with a specific suffix (e.g., "_1") back to their corresponding 
#' base columns using coalesce. If the base column doesn't exist, the suffix column
#' is renamed to become the base column. After merging, all suffix columns are removed.
#' This is particularly useful when handling join results with duplicate column names.
#'
#' @param df A data frame or tibble
#' @param suffix A string suffix to identify columns to be merged (default: "_1")
#' @return A data frame with suffix columns merged back to base columns
#'
#' @examples
#' # Create sample data
#' df <- data.frame(
#'   id = 1:3,
#'   name = c("A", NA, "C"),
#'   age = c(25, 30, NA),
#'   name_1 = c(NA, "B", "CC"),
#'   age_1 = c(NA, NA, 35)
#' )
#'
#' # Merge suffix columns
#' coalesce_suffix_cols(df)
#' # Result:
#' #   id name age
#' #   1  A   25
#' #   2  B   30
#' #   3  C   35
#'
#' @importFrom dplyr coalesce select all_of
#' @importFrom stringr str_ends str_remove
#' @export
#' @implements MP035 Null Special Treatment

library(dplyr)
library(stringr)

# 合併 *_1 欄位回原始欄位
coalesce_suffix_cols <- function(df, suffix = "_1") {
  suffix_cols <- names(df)[str_ends(names(df), fixed(suffix))]
  
  for (col1 in suffix_cols) {
    base_col <- str_remove(col1, fixed(suffix))
    
    # 如果 base 欄位存在，就用 coalesce 合併
    if (base_col %in% names(df)) {
      df[[base_col]] <- dplyr::coalesce(df[[base_col]], df[[col1]])
    } else {
      # 若 base 欄位不存在，就改名為 base 欄位
      names(df)[names(df) == col1] <- base_col
    }
  }
  
  # 移除所有 *_1 欄位（已合併）
  df <- df %>% select(-all_of(suffix_cols))
  
  return(df)
}