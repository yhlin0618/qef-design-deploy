#' Clean Column Names by Removing Suffixes
#'
#' @title Remove Language Suffixes from Column Names
#' @description Cleans column names by removing language suffixes or annotations
#' that appear after a specified separator pattern. This is particularly useful
#' for bilingual datasets where column names contain both English and translated
#' versions separated by a delimiter (e.g., "Price..價格" becomes "Price").
#' 
#' Following principles:
#' - R094: Roxygen2 documentation standard
#' - MP047: Functional programming (pure function)
#' - R019: Object naming convention
#'
#' @param column_names Character vector of column names to clean
#' @param separator Character string or regex pattern marking where to cut the
#'        column name. Default is "\\.\\." (two dots). Everything after and
#'        including this separator will be removed.
#'        
#' @return Character vector of cleaned column names with suffixes removed.
#'         Original names are returned if separator is not found.
#'         
#' @examples
#' # Clean bilingual column names with default separator
#' cols <- c("Price..價格", "Volume..體積", "Color..顏色")
#' clean_column_names(cols)
#' # Returns: c("Price", "Volume", "Color")
#' 
#' # Use custom separator
#' cols <- c("name_en", "price_zh", "date_jp")
#' clean_column_names(cols, separator = "_")
#' # Returns: c("name", "price", "date")
#' 
#' # Handle columns without separator
#' cols <- c("ProductID", "Rating", "Comment..評論")
#' clean_column_names(cols)
#' # Returns: c("ProductID", "Rating", "Comment")
#' 
#' @export
#' @principle R094 Roxygen2 documentation standard
#' @principle R019 Object naming convention (clean column names)
clean_column_names <- function(column_names, separator = "\\.\\.") {
  pattern <- paste0(separator, ".*")
  cleaned_names <- gsub(pattern, "", column_names)
  return(cleaned_names)
}

# # 測試數據
# column_names <- c("product_Index", "AmazonReviewRating", "材質..Material.", "體積..Volume.", "顏色..Color.", "價格..Price.", "設計..Design.", "質量..Quality.")
# 
# # 使用函數清理列名
# cleaned_names <- clean_column_names(column_names)
# 
# # 打印結果
# print(cleaned_names)


