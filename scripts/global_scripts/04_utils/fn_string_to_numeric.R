#' String to Numeric Function
#'
#' Brief description of what this function does
#'
#' @param params Description of parameters
#' @return Description of return value
#'
#' @examples
#' string_to_numeric()
# 定义函数将字符串转换为数值并生成对照表
string_to_numeric <- function(dt, col_name) {
  # 创建对照表
  unique_strings <- unique(dt[[col_name]])
  mapping_table <- data.table(
    original = unique_strings,
    numeric = seq_along(unique_strings)
  )
  
  # 创建转换后的 data.table
  dt_transformed <- copy(dt)
  dt_transformed[[col_name]] <- mapping_table$numeric[match(dt[[col_name]], mapping_table$original)]
  
  return(list(transformed_data = dt_transformed, mapping_table = mapping_table))
}

# 定义函数将数值转换回字符串
numeric_to_string <- function(dt, col_name, mapping_table) {
  dt_transformed <- copy(dt)
  dt_transformed[[col_name]] <- mapping_table$original[match(dt[[col_name]], mapping_table$numeric)]
  
  return(dt_transformed)
}
