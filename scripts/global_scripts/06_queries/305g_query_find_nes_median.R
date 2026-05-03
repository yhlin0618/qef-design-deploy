query_find_nes_median <- function(con,con_pre, source) {
  library(dplyr)
  library(data.table)
  library(dbplyr)
  library(lubridate)
  library(glue)
  library(stringr)
  
  # 根據 source 自動產生來源表名稱，格式: "<source>_000_ALL_now"
  source_table_name <- paste0(source, "_000_ALL_now")
  
  # 讀取來源表 (例如 "amazon_000_ALL_now")
  dt <- tbl(con_pre, source_table_name) %>% collect() %>% as_tibble()
  
  # 以 customer_id 分組，取得目前最大時間作為參考
  dt_grouped <- dt %>% group_by(customer_id)
  time_now <- max(dt$time, na.rm = TRUE)
  
  # 1. 計算 IPTmean_table：針對每個 customer_id 計算平均 IPT、平均購物金額等指標
  IPTmean_table <- dt_grouped %>% 
    summarise(
      IPT_mean = mean(IPT, na.rm = TRUE),
      Mvalue = mean(total, na.rm = TRUE),
      ni = n(),
      sigma_HNorm_mle = sqrt(mean(IPT^2, na.rm = TRUE)),
      sigma_HNorm_bcmle = sigma_HNorm_mle + (sigma_HNorm_mle / (4 * (ni - 1))),
      .groups = "drop"
    ) %>% 
    mutate(
      M_ecdf = cume_dist(Mvalue),
      Mlabel = cut(M_ecdf, breaks = Mbreaks, labels = textMlabel, right = TRUE, ordered_result = TRUE)
    )
  
  # 2. 將原始資料與 IPTmean_table 合併，並選取每個 customer_id 中 times 最高的那筆資料
  NEStable <- left_join(dt, IPTmean_table, by = "customer_id") %>%
    group_by(customer_id) %>%
    slice(which.max(times)) %>%
    ungroup() %>% as_tibble()
  
  # 3. 將 IPT_mean 轉換為 difftime (單位 days)
  NEStable <- NEStable %>% mutate(IPT_mean = as.difftime(IPT_mean, units = "days"))
  
  # 4. 計算 Difftime：time_now - time（以 days 計）
  NEStable <- NEStable %>% mutate(Difftime = difftime(time_now, time, units = "days"))
  
  # 5. 計算 NESratio：Difftime / IPT_mean
  NEStable <- NEStable %>% mutate(NESratio = as.numeric(Difftime) / as.numeric(IPT_mean))
  
  # 6. 計算 NES 中位數
  nes_median <- median(NEStable$NESratio, na.rm = TRUE)
  message("For source '", source_table_name, "', computed NES median is: ", nes_median)
  
  # 7. 建立或更新 nes_median_dictionary 表
  if (!("nes_median_dictionary" %in% dbListTables(con))) {
    create_sql <- "
      CREATE TABLE nes_median_dictionary (
        source VARCHAR PRIMARY KEY,
        nes_median DOUBLE
      );
    "
    dbExecute(con, create_sql)
  }
  
  # 使用 source 字串作為來源標籤
  new_record <- tibble(source = source, nes_median = nes_median)
  
  # 若該來源已存在，先刪除再插入
  existing <- dbGetQuery(con, glue("SELECT * FROM nes_median_dictionary WHERE source = '{source}'"))
  if(nrow(existing) > 0) {
    dbExecute(con, glue("DELETE FROM nes_median_dictionary WHERE source = '{source}'"))
  }
  
  dbWriteTable(con, "nes_median_dictionary", new_record, append = TRUE, row.names = FALSE)
  message("nes_median_dictionary updated for source: ", source)
  
  return(new_record)
}