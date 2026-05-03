library(lubridate)

parse_datetime_columns <- function(data, cols, default_tz = "UTC") {
  # 內建時間格式設定
  orders <- c("%Y/%m/%d %I:%M:%S %p",
              "%Y/%m/%d %p%I:%M:%S",
              "%Y/%m/%d %I:%M:%S",
              "%Y-%m-%d %I:%M:%S",
              "%B %d, %Y",
              "%Y年%m月%d日",
              "%Y-%m-%d %H:%M:%S %z",
              "%m/%d/%Y %H:%M")
  
  quiet <- TRUE
  exact <- TRUE
  
  # 檢查所有指定的欄位是否存在於資料中
  if (!all(cols %in% names(data))) {
    stop("資料集中不存在部分指定的欄位。")
  }
  
  # 將形如 "+0800" 或 "-0800" 的 offset 轉換成 R 可識別的時區名稱
  convert_offset_to_tz <- function(offset_str) {
    sign <- substr(offset_str, 1, 1)
    hour <- as.numeric(substr(offset_str, 2, 3))
    minute <- as.numeric(substr(offset_str, 4, 5))
    if (minute != 0) {
      warning("僅支援整點時區，分鐘部分將被忽略。")
    }
    # 在 Etc/GMT 區域中，符號需要反向：
    # 例如 "-0800" 對應 "Etc/GMT+8"，"+0800" 對應 "Etc/GMT-8"
    if (sign == "-") {
      tzname <- paste0("Etc/GMT+", hour)
    } else {
      tzname <- paste0("Etc/GMT-", hour)
    }
    return(tzname)
  }
  
  # 掃描所有欄位，找出第一個含有時區資訊的字串（例如 "-0800"）
  found_tz <- NULL
  for (col in cols) {
    for (val in data[[col]]) {
      if (!is.na(val) && grepl("[+-][0-9]{4}", val)) {
        tz_match <- regmatches(val, regexpr("[+-][0-9]{4}", val))
        found_tz <- tz_match
        break
      }
    }
    if (!is.null(found_tz)) break
  }
  
  # 若找不到，則使用預設時區
  if (is.null(found_tz)) {
    found_tz <- default_tz
  } else {
    # 若找到的是 offset 格式，則轉換成合法的時區名稱
    if (grepl("^[+-][0-9]{4}$", found_tz)) {
      found_tz <- convert_offset_to_tz(found_tz)
    }
  }
  
  # 逐欄解析時間，並根據是否含有時區資訊來決定使用 with_tz 或 force_tz
  for (col in cols) {
    original <- data[[col]]
    parsed <- parse_date_time(original, orders = orders, quiet = quiet, exact = exact)
    
    # 建立一個全 NA 的 POSIXct 向量
    adjusted <- as.POSIXct(rep(NA, length(parsed)), origin = "1970-01-01", tz = found_tz)
    
    for (i in seq_along(parsed)) {
      if (is.na(parsed[i])) {
        adjusted[i] <- NA
      } else {
        # 若原始字串中包含時區資訊，則使用 with_tz() 保留同一個時刻的轉換
        if (!is.na(original[i]) && grepl("[+-][0-9]{4}", original[i])) {
          adjusted[i] <- with_tz(parsed[i], tzone = found_tz)
        } else {
          # 若沒有時區資訊，則認定其屬於 found_tz
          adjusted[i] <- force_tz(parsed[i], tzone = found_tz)
        }
      }
    }
    data[[col]] <- adjusted
  }
  
  return(data)
}