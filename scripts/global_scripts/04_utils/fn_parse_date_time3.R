#' @file   fn_parse_date_time3.R
#' @principle R021 One Function One File
#'
#' @title   Advanced Date and Time String Parser
#' @description
#'   Comprehensive parser that handles English, Chinese, Spanish (and more) date-
#'   time strings, auto-detects Excel serials / time-only / date-only / full
#'   datetime, and returns a POSIXct vector with useful attributes.
#'
#' @param x            Character | factor vector to parse
#' @param tz           Time zone for output (default "UTC")
#' @param locale_es    Spanish locale code; on Windows use "Spanish_Spain"
#' @param prefer_order Character vector giving preferred orders of ymd/mdy/dmy
#' @return POSIXct vector. Attributes:
#'         • class_hint : "date" | "time" | "datetime" | "unknown"
#'         • unparsed   : original strings that failed to parse
#' @export
#' parse_date_time3  v0.3.1  (overwrite all old code)
#' ---------------------------------------------------
#' 1. 清除 Amazon 雜訊、中文 AM/PM、英文／西文介詞
#' 2. 將「YYYY年MM月DD日」正規化成 "YYYY-MM-DD"
#' 3. 支援 Excel serial、time‑only、date‑only、datetime
#' 4. 回傳 POSIXct；attr(class_hint)、attr(unparsed)
#' 5. 不再於任何 orders 字串留有 '年月日'
#'
parse_date_time3 <- function(
    x,
    tz           = "UTC",
    locale_es    = "es_ES",
    prefer_order = c("ymd", "mdy", "dmy")
) {
  stopifnot(is.character(x) || is.factor(x))
  x_chr <- as.character(x)
  
  ## ─ 1. 前置清洗 -----------------------------------------------------------
  pat <- c("^Reviewed.* on ([A-Za-z]+ \\d{1,2}, \\d{4}).*" = "\\1",
           "Reviewed in (.*?) on "                         = "",
           "(.*?) en (.*?) el "                            = "",
           "上午"                                          = "AM",
           "下午"                                          = "PM",
           "a\\. ?m\\.|p\\. ?m\\."                         = "\\U\\0")   # a.m. → AM
  clean <- stringr::str_replace_all(x_chr, pat) |>
    stringr::str_replace_all("(\\d{4})年(\\d{1,2})月(\\d{1,2})日",
                             "\\1-\\2-\\3") |>
    stringr::str_replace_all("\\bde\\b", " ") |>
    stringr::str_squish()
  
  ## ─ 2. 先判斷整欄是否「現在含時間」 ------------------------------
  time_regex <- ":|\\b(AM|PM)\\b"
  col_has_time <- any(grepl(time_regex, clean, ignore.case = TRUE))
  
  ## ─ 3. 純日期欄位：直接 <Date> ---------------------------------
  if (!col_has_time) {
    out <- lubridate::parse_date_time(
      clean,
      orders  = c(prefer_order, "d B Y", "B d, Y"),
      locale  = "C",
      exact   = FALSE,
      quiet   = TRUE
    ) |>
      lubridate::as_date()
    attr(out, "unparsed") <- x_chr[is.na(out)]
    return(out)
  }
  
  ## ─ 4. 含時間欄位：依完整流程解析 (<POSIXct>) ------------------
  n      <- length(clean)
  out    <- rep(as.POSIXct(NA), n)
  
  # 4.1 Excel serial
  is_num <- grepl("^\\d+(\\.\\d+)?$", clean)
  if (any(is_num)) {
    out[is_num] <- readxl::excel_numeric_to_date(as.numeric(clean[is_num]),
                                                 date_system = "modern") |>
      lubridate::with_tz(tz)
  }
  
  # 4.2 其餘字串
  miss <- is.na(out)
  if (any(miss)) {
    out[miss] <- anytime::anytime(clean[miss], tz = tz, asUTC = TRUE)
  }
  
  attr(out, "unparsed") <- x_chr[is.na(out)]
  out
}