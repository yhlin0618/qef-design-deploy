process_by_customer_time <- function(dt) {
  result <- dt[, `:=`(date = floor_date(time, unit = "day"))]
  
  
  # 計算每個customer_id和time的總消費金額和消費次數
  result <- dt[, .(
    total = sum(lineproduct_price, na.rm = TRUE),  # 總消費金額
    amount = .N,                                # 每月的消費次數
    time = first(time),        # 將所有時間連接起來
    zipcode = first(zipcode), 
    state = first(state),
    lat = first(lat), 
    lng = first(lng)
  ), by = .(customer_id, date)]
  
  # 計算每個customer_id的購買總次數和訂單順序
  result[, `:=`(
    ni = .N,                                    # 總購買次數
    times = seq_len(.N)                         # 訂單順序
  ), by = customer_id]
  
  # 計算兩次訂單之間的天數差異 (IPT)
  result[, IPT := as.numeric(difftime(time, shift(time), units = "days")), by = customer_id]
  
  
  # 按照customer_id和time排序
  setorder(result, customer_id, date)
  
  return(result)
}