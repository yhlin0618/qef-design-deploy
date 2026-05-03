#' Parse and standardize datetime columns in a data frame
#'
#' This function parses datetime columns that might be in different formats
#' and standardizes them to POSIXct with a specified timezone.
#'
#' @param df A data frame containing the datetime columns to be parsed
#' @param cols A character vector of column names to be parsed
#' @param all_force_tz A character string specifying the timezone to force (default: "Asia/Taipei")
#'
#' @return A data frame with the specified columns converted to POSIXct with the specified timezone
#'
#' @examples
#' \dontrun{
#' # Convert order_date and ship_date columns to datetime with Taipei timezone
#' orders_df <- parse_datetime_columns(orders_df, 
#'                                    cols = c("order_date", "ship_date"),
#'                                    all_force_tz = "Asia/Taipei")
#' }
#'
#' @importFrom dplyr mutate across all_of
#' @importFrom lubridate parse_date_time force_tz
#'
#' @export
parse_datetime_columns <- function(df, cols, all_force_tz = "Asia/Taipei") {
  # 載入必要套件
  require(dplyr)
  require(lubridate)
  
  df %>% mutate(across(all_of(cols), ~ {
    # 如果欄位是 factor，先轉成 character
    if (is.factor(.x)) {
      .x <- as.character(.x)
    }
    # 如果尚未轉成日期時間格式，則嘗試解析
    if (!inherits(.x, "POSIXct")) {
      # 嘗試以多種常見的日期時間格式解析，這裡的 orders 可依實際資料調整
      parsed <- parse_date_time(.x, orders = c("ymd HMS", "ymd HM", "ymd"), tz = "UTC")
      # 將解析後的日期時間強制設定為指定的時區
      force_tz(parsed, tzone = all_force_tz)
    } else {
      # 已是日期時間格式，直接調整時區
      force_tz(.x, tzone = all_force_tz)
    }
  }))
}
