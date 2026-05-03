library(dplyr)
library(stringr)

# 调用函数进行转换

convert_units_to_smallest <- function(df, unit_equation) {
  units_info <- str_split(unit_equation, ";")[[1]]
  conversion_list <- lapply(units_info, function(group) {
    parts <- str_split(group, "=")[[1]]
    units <- str_split(parts[1], ":")[[1]]
    factors <- as.numeric(str_split(parts[2], ":")[[1]])
    data.frame(unit = units, factor = factors, stringsAsFactors = FALSE)
  })
  
  # 应用转换
  df_converted <- df %>%
    mutate(across(where(is.character), ~ {
      sapply(.x, function(value) {
        # 如果值为NA，则直接跳过
        if (is.na(value)) return(NA_real_)
        
        for (conversion in conversion_list) {
          matched <- FALSE
          for (i in 1:nrow(conversion)) {
            if (str_detect(value, fixed(conversion$unit[i]))) {
              number <- as.numeric(str_extract(value, "\\d+\\.?\\d*"))
              smallest_factor <- min(conversion$factor)
              converted_value <- number * conversion$factor[i] / smallest_factor
              matched <- TRUE
              return(converted_value)
            }
          }
          if (matched) break
        }
        return(NA_real_)
      })
    }, .names = "{.col}_united"))
  
  # 移除全为NA的"_united"结尾的列
  cols_to_remove <- sapply(df_converted, function(col) all(is.na(col)))
  cols_to_remove <- names(cols_to_remove[cols_to_remove & str_detect(names(cols_to_remove), "united")])
  df_filtered <- df_converted %>% select(-all_of(cols_to_remove))
  
  return(df_filtered)
}