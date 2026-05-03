process_amazon_sales <- function(raw_data, Data) {
  library(dplyr)
  library(dbplyr)
  
  amazon_sales_dta <- tbl(raw_data, "amazon_sales_dta")
  product_property_dictionary <- tbl(raw_data, "product_property_dictionary")
  
  result <- amazon_sales_dta %>% 
    filter(str_detect(buyer_email, "@")) %>%
    mutate(
      customer_id = sql("LOWER(SUBSTR(buyer_email, 1, POSITION('@' IN buyer_email) - 1))"),
      time = purchase_date
    ) %>%
    rename(
      lineproduct_price = product_price,
      zip_code = shipping_postal_code
    ) %>%
    left_join(product_property_dictionary, by = join_by(sku)) %>%
    filter(
      !is.na(customer_id) & !is.na(time) & 
        !is.na(sku) & !is.na(asin) & !is.na(product_line_id)
    ) %>% 
    filter(shipping_country_code == "US") %>% 
    select(customer_id, time, sku, lineproduct_price, everything()) %>% 
    collect()
  
  dbWriteTable(Data, "amazon_sales_dta", result, overwrite = TRUE, temporary = FALSE)
  message("Amazon 銷售資料處理完成並寫入 Data 資料庫。")
}