library(dplyr)
library(tidyr)

update_amazon_poisson_precision_marketing_dummy <- function(asin_table, product_property_dictionary) {
  # 取得 asin_table 中所有欄位名稱符合 "001_asin", "002_asin", ... 的欄位
  asin_columns <- grep("^[0-9]{3}_asin$", names(asin_table), value = TRUE)
  
  # 將這些欄位轉成長格式，並過濾掉 NA 與空字串，取出所有不重複的 asin
  asin_union <- asin_table %>%
    select(all_of(asin_columns)) %>%
    pivot_longer(cols = everything(),
                 names_to = "asin_col",
                 values_to = "asin") %>%
    filter(!is.na(asin) & asin != "") %>%
    distinct(asin) %>%
    pull(asin)
  
  # 在 product_property_dictionary 中新增 amazon_poisson_precision_marketing 欄位，
  # 若該筆 asin 有出現在 asin_union 中，則設定為 1，否則設定為 0
  product_property_dictionary <- product_property_dictionary %>%
    mutate(amazon_poisson_precision_marketing = if_else(asin %in% asin_union, 1, 0))
  
  return(product_property_dictionary)
}
