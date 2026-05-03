Sort_Optimal_Pricing_Formula <- function(formula) {
  formula2 <- gsub("\n", "", formula) 
  formula3 <- gsub("_logprice", ":logprice", formula2)
  
  # 去掉公式的左侧部分 (例如 "Sales ~ ")
  response_variable <- sub(" ~.*", "", formula3)
  independent_variables <- sub(".* ~ ", "", formula3)
  
  independent_variables2 <- strsplit(as.character(independent_variables), " \\+ ")[[1]]
  
  
  independent_variables3 <- trimws(independent_variables2)
  
  # 提取公式中的變數
 
  # 分類變數
  no_logprice <- grep("logprice|logprice2", independent_variables3, invert = TRUE, value = TRUE)
  logprice <- grep("logprice$", independent_variables3, value = TRUE)
  logprice2 <- grep("logprice2$", independent_variables3, value = TRUE)
  
  # 排序後的變數
  sorted_variables <- c(no_logprice, logprice, logprice2)
  
  # 重新組合成公式
  sorted_formula <- paste(sorted_variables, collapse = " + ")
  
  # 組合並返回完整的公式
  full_formula <- paste(response_variable, "~", sorted_formula)
  
  return(full_formula)
}

# 範例使用
formula <- "Sales ~ logprice + logprice2 + month_1 + 客戶服務 + month_5 + 
  month_6 + month_7 + RatingNum + month_8 + month_9 + month_10 + 
  year + Charging_type_4 + 星期二 + 星期一 + 星期三 + 
  Revenue + 星期四 + 星期六 + is_pre7day_holiday + 顏色_橘_logprice + 
  month_1_logprice + month_2_logprice + 材質_不鏽鋼 + 
  Rating + month_4_logprice + 顏色_白 + month_5_logprice + 
  month_6_logprice + month_7_logprice + 可靠性_logprice + 
  month_8_logprice + month_9_logprice + month_10_logprice + 
  month_11_logprice + month_12_logprice + year_logprice + AmazonReviewRating + 
  顏色_綠 + 顏色_海軍藍 + month_12 + 是否可開多種罐型_4.5 + 
  星期一_logprice + 是否有保固 + 星期二_logprice + 
  星期日_logprice + 星期三_logprice + 星期四_logprice + 
  is_pre7day_holiday_logprice + month_1_logprice2 + month_2_logprice2 + 
  month_3_logprice2 + month_4_logprice2 + month_5_logprice2 + 
  month_6_logprice2 + month_7_logprice2 + month_8_logprice2 + 
  month_9_logprice2 + month_10_logprice2 + month_11_logprice2 + 
  year_logprice2 + 星期日_logprice2 + 星期五_logprice"

sorted_formula <- Sort_Optimal_Pricing_Formula(formula)
cat(sorted_formula)
