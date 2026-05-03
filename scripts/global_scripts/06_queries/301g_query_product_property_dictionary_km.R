query_product_property_dictionary_km <- function(df, product_line_id_vec_token=product_line_id_vec, 
                                     brand_name_token=brand_name) {
  # 重新命名欄位，請依照原始資料的欄位順序設定
  colnames(df) <- c("product_name", "sku", "product_line", "asin", "成本", "利潤")
  
  # 先過濾掉所有 NA 的列與 sku 為 "SKU" 的資料，再根據 product_line 做轉換
  df <- df %>% 
    filter_all(~ !is.na(.)) %>% 
    filter(sku != "SKU") %>% 
    mutate(product_line = case_when(
      product_line == "Salt & Pepper Grinders" ~ "003_Salt_and_Pepper_Grinder",
      product_line == "Grinders" ~ "003_Salt_and_Pepper_Grinder",
      product_line == "Milk Frother" ~ "002_Milk_Frother",
      product_line == "Small Kitchen Gadgets" ~ "005_Meat_Claw",
      product_line == "Silicone Spatulas" ~ "004_Silicone_Spatula",
      product_line == "Silicone Products" ~ "004_Silicone_Spatula",
      product_line == "Manual Opener" ~ "007_Bottle_Openers",
      product_line == "Electric Can Opener" ~ "001_Electric_Can_Opener",
      # 若有重複定義 "Small Kitchen Gadgets"，可依需求保留其中一個規則
      TRUE ~ product_line
    ))
  
  # (可選) 查看轉換後的 product_line 值
  print(unique(df$product_line))
  
  # 進一步處理資料：
  # 1. convert_list_columns()：假設此函數已定義，可處理清單型別欄位
  # 2. 從 product_line 中取出前 3 個字元作為 product_line_id
  # 3. 過濾 product_line_id 需存在於 product_line_id_vec 中
  # 4. 新增 brand 欄位，並將 product_line_id_name 轉成小寫後存入 product_line_id_name
  # 5. 將 "成本" 與 "利潤" 分別重新命名為 cost 與 profit
  # 6. 最後選取指定欄位順序
  df2 <- df %>% 
    convert_list_columns() %>% 
    mutate(product_line_id = substr(product_line, 1, 3)) %>%
    filter(product_line_id %in% product_line_id_vec_token) %>% 
    mutate(brand = brand_name_token) %>%
    select(-product_line) %>% 
    rename(cost = `成本`,
           profit = `利潤`)
  
  return(df2)
}