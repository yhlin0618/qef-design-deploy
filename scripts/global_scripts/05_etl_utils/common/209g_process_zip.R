process_zip <- function(Dta, zip_code_db_ver2, source_i = "amazon") {
  # 依據資料中的 zip_code 欄位，利用 reverse_zipcode_fast 取得處理結果
  result <- reverse_zipcode_fast(Dta[["zip_code"]], zip_code_db_ver2)
  Dta2 <- cbind(Dta, result)
  return(Dta2)
}