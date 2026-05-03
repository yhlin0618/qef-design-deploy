#' Generate Dummy Variable Matrix from Categorical Data
#'
#' @title Convert Categorical Variables to Dummy Matrix
#' @description Transforms categorical columns in a data frame into dummy variables
#' (one-hot encoding), handling multiple values per cell separated by specified
#' delimiters. This function is particularly useful for data containing multi-valued
#' categorical fields (e.g., "red/blue/green" becomes separate dummy columns).
#' The function preserves numeric columns and maintains the ID column structure.
#'
#' Following principles:
#' - R094: Roxygen2 documentation standard
#' - MP047: Functional programming
#' - R047: Aggregate variable naming
#' - R050: Data.table vectorization where applicable
#'
#' @param data Data frame to transform. Must contain at least one ID column and
#'        categorical columns to convert.
#' @param id_column_name Character string specifying the name of the ID column
#'        that uniquely identifies each row. This column will be preserved.
#' @param separator Regular expression pattern for splitting multi-valued cells.
#'        Default is "[、/,]" which splits on Chinese comma, forward slash, or
#'        English comma. Can be customized for different delimiters.
#' @param remain_name Optional character vector of column names to preserve without
#'        dummy encoding. These columns will be kept in their original form.
#'        Default is NULL (no additional columns preserved).
#'        
#' @return Data frame with:
#'   - Original ID column
#'   - All numeric columns preserved
#'   - Categorical columns converted to dummy variables (0/1)
#'   - Column names formatted as "feature_value"
#'   - Any columns specified in remain_name preserved
#'   
#' @examples
#' # Basic usage with product data
#' df <- data.frame(
#'   id = 1:3,
#'   color = c("red/blue", "green", "red/green/blue"),
#'   size = c("S,M", "L", "M,L"),
#'   price = c(10, 20, 15)
#' )
#' result <- generate_dummy_matrix(df, "id")
#' # Creates dummy columns: color_red, color_blue, color_green, size_S, size_M, size_L
#' 
#' # Preserve certain columns without encoding
#' result <- generate_dummy_matrix(df, "id", remain_name = "size")
#' # Size column remains unchanged, only color is dummy encoded
#' 
#' # Custom separator for different delimiter
#' df2 <- data.frame(
#'   ASIN = c("B001", "B002"),
#'   features = c("waterproof;durable", "lightweight;portable")
#' )
#' result <- generate_dummy_matrix(df2, "ASIN", separator = ";")
#' 
#' @export
#' @note Missing values (NA) are handled by creating a special "_NA" dummy column
#' @principle R094 Roxygen2 documentation standard
#' @principle R047 Aggregate variable naming (feature_value pattern)
library(dplyr)
library(tidyr)

# ## 考量、和/ 的，但可能有duplicate issue 應該已經修正，但之後保險起見可以檢查
# generate_dummy_matrix <- function(data, id_column_name,separator="[、/,]") {
#   id_column_name <- as.character(id_column_name)
#   
#   # 选取数值型和ID列以外的所有列进行转换
#   target_columns <- names(data)[!names(data) %in% id_column_name & !sapply(data, is.numeric)]
#   
#   # 长格式转换，处理包含NA的行和多种分隔符
#   data_long <- data %>%
#     pivot_longer(cols = target_columns, names_to = "feature", values_to = "value") %>%
#     separate_rows(value, sep = separator) %>%
#     drop_na(value) %>%
#     # 为每个特征和其值生成一个唯一的列名
#     mutate(feature_value = paste(feature, value, sep = "_")) %>%
#     # 创建dummy变量
#     mutate(dummy = 1) %>%
#     distinct() %>% 
#     pivot_wider(id_cols = id_column_name, names_from = feature_value, 
#                 values_from = dummy, values_fill = list(dummy = 0)) # 正确使用values_fill
#   
#   # 合并数值型列
#   num_columns <- select(data, !!sym(id_column_name), where(is.numeric))
#   
#   # 合并数值型列和转换后的数据
#   final_data <- left_join(num_columns, data_long, by = id_column_name)
#   
#   ordered_cols <- c(id_column_name, names(num_columns)[-1], 
#                     sort(setdiff(names(final_data), c(id_column_name, names(num_columns)[-1]))))
#   
#   final_data <- final_data %>%
#     select(all_of(ordered_cols))
#   
#   
#   return(final_data)
# }
# 
# 
# library(tidyverse)
# 
# generate_dummy_matrix <- function(data, id_column_name, separator="[、/,]") {
#   id_column_name <- as.character(id_column_name)
#   
#   # 选取数值型和ID列以外的所有列进行转换
#   target_columns <- names(data)[!names(data) %in% id_column_name & !sapply(data, is.numeric)]
#   
#   # 长格式转换，不处理包含NA的行，以适应多种分隔符
#   data_long <- data %>%
#     pivot_longer(cols = target_columns, names_to = "feature", values_to = "value") %>%
#     separate_rows(value, sep = separator) %>%
#     # 不再在这里删除NA值
#     mutate(feature_value = paste(feature, value, sep = "_"),
#            # 创建dummy变量，这里处理NA值
#            dummy = if_else(is.na(value), NA_integer_, 1)) %>%
#     distinct() %>%
#     pivot_wider(id_cols = id_column_name, names_from = feature_value, 
#                 values_from = dummy, values_fill = list(dummy = 0))  # values_fill只应用于非NA的情况
#   
#   # 合并数值型列
#   num_columns <- select(data, !!sym(id_column_name), where(is.numeric))
#   
#   # 合并数值型列和转换后的数据
#   final_data <- left_join(num_columns, data_long, by = id_column_name)
#   
#   ordered_cols <- c(id_column_name, names(num_columns)[-1], 
#                     sort(setdiff(names(final_data), c(id_column_name, names(num_columns)[-1]))))
#   
#   final_data <- final_data %>%
#     select(all_of(ordered_cols))
#   
#   return(final_data)
# }
# data <- sub_res
# id_column_name <- "key"
#data <-a# product_Property_001_Electric_Can_Opener
#id_column_name <- "ASIN"
generate_dummy_matrix <- function(data, id_column_name, separator="[、/,]",remain_name=NULL) {
  id_column_name <- as.character(id_column_name)
  # id_column_name <- "ASIN"
  # remain_name <- "品牌"
  # unit_equation = "克:盎司:公斤:Kilograms=1:28.3:1000:1000;毫升:公升=1:1000"
  # data <- d %>% select(.,-any_of(remove_name)) %>% 
  #   convert_units_to_smallest(., unit_equation) %>%
  #   select(-"重量")
  data <- data %>%
    mutate(across(where(~is.list(.x)), ~unlist(.x)))
  # 选取数值型和ID列以外的所有列进行转换
  if (is.null(remain_name)){
    target_columns <- names(data)[!names(data) %in% id_column_name & !sapply(data, is.numeric)]
  }else{
    target_columns <- names(data)[!names(data) %in% id_column_name & !sapply(data, is.numeric)& !names(data) %in% remain_name]
  }

  
  # 长格式转换
  data_long <- data %>%
    pivot_longer(cols = target_columns, names_to = "feature", values_to = "value") %>%
    separate_rows(value, sep = separator) %>%
    # 处理NA值，为每个特征和其值生成一个唯一的列名，同时处理NA情况
    mutate(feature_value = if_else(is.na(value), paste(feature, "NA", sep = "_"), paste(feature, value, sep = "_")),
           dummy = if_else(is.na(value), 1, 1)) %>%
    distinct() %>%
    pivot_wider(id_cols = id_column_name, names_from = feature_value, 
                values_from = dummy, values_fill = list(dummy = 0)) 
  
  # 合并数值型列
  num_columns <- select(data, any_of(c(id_column_name)),where(is.numeric))
  REMAIN_COL <- select(data,any_of(c(id_column_name, remain_name)))
  # 合并数值型列和转换后的数据
  final_data <- left_join(num_columns, data_long, by = id_column_name)
  final_data <- left_join(final_data, REMAIN_COL, by = id_column_name)

  if(is.null(remain_name)){
    ordered_cols <- c(id_column_name, names(num_columns)[-1], 
                      sort(setdiff(names(final_data), c(id_column_name, names(num_columns)[-1]))))
  }else{
    ordered_cols <- c(id_column_name, names(num_columns)[-1],remain_name, 
                      sort(setdiff(names(final_data), c(id_column_name, names(num_columns)[-1]))))
  }
  final_data <- final_data %>%
    select(all_of(ordered_cols))

  return(final_data)
}

