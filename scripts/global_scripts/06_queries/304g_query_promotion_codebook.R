query_promotion_codebook <- function(Promotion_Codebook, officialwebsite_sales_dta4) {
  # 建立 recode 字典
  Shipping_Method_CodeDict <- c(Promotion_Codebook$Shipping_Method_Coding)
  names(Shipping_Method_CodeDict) <- Promotion_Codebook$Shipping_Method
  
  Payment_Method_CodeDict <- c(Promotion_Codebook$Payment_Method_Coding)
  names(Payment_Method_CodeDict) <- Promotion_Codebook$Payment_Method
  
  Source_CodeDict <- c(Promotion_Codebook$Source_Coding)
  names(Source_CodeDict) <- Promotion_Codebook$Source
  
  officialwebsite_sales_dta4$shipping_method_recode <- Shipping_Method_CodeDict[officialwebsite_sales_dta4$shipping_method]
  officialwebsite_sales_dta4$payment_method_recode <- Payment_Method_CodeDict[officialwebsite_sales_dta4$payment_method]
  officialwebsite_sales_dta4$source_recode <- Source_CodeDict[officialwebsite_sales_dta4$source]
  
  message("促銷碼及 recode 處理完成。")
  return(officialwebsite_sales_dta4)
}