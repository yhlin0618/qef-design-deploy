#' Make Names Function
#'
#' Converts strings to valid R variable names with customizable case formatting
#'
#' @param x Character vector to be converted
#' @param case Case format to use. Options: "snake_case" (default), "original", "lower", "upper"
#' @return Character vector with cleaned names
#'
#' @examples
#' make_names(c("My Variable", "Another.Name"))
#' make_names(c("My Variable", "Another.Name"), case = "original")
#' make_names(c("My Variable", "Another.Name"), case = "upper")

make_names <- function(x, case = "snake_case") {
  if (!is.character(x)) stop("Input must be a character vector") # 防錯處理
  if (!case %in% c("snake_case", "original", "lower", "upper")) {
    stop("case must be one of: 'snake_case', 'original', 'lower', 'upper'")
  }
  
  # Basic cleaning - convert to legal R names and replace dots with underscores
  cleaned <- x %>%
    make.names() %>%                        # 轉換為合法的 R 名稱
    gsub("\\.", "_", .) %>%                 # 將所有 . 替換為 _
    gsub("__+", "_", .) %>%                 # 替換連續的下劃線為單個下劃線
    gsub("^_+|_+$", "", .)                  # 去除開頭和結尾的下劃線
  
  # Apply case formatting
  result <- switch(case,
    "snake_case" = tolower(cleaned),        # 預設：轉換為小寫 snake_case
    "original" = cleaned,                   # 保持原始大小寫
    "lower" = tolower(cleaned),             # 全部小寫
    "upper" = toupper(cleaned),             # 全部大寫
    cleaned                                 # 預設回傳
  )
  
  return(result)
}
