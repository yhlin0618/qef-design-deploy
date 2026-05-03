patch_amazon_reviews <- function(comment_property_rating, 
                                 vec_product_line_id_noall, 
                                 key_cols = c("asin", "title", "body", "time"),
                                 threshold = 0.3) {
  library(DBI)
  library(dplyr)
  library(dbplyr)
  library(stringdist)
  
  for (product_line_id in vec_product_line_id_noall) {
    message("=== 處理產品線：", product_line_id, " ===")
    
    # 定義表名稱
    raw_table_name   <- paste0("comment_property_rating_", product_line_id, "_raw")
    main_table_name  <- paste0("comment_property_rating_", product_line_id)
    rated_table_name <- paste0("comment_property_rating_", product_line_id, "_rated")
    
    # 讀取 _raw 表（必須存在，並作為欄位名稱基準）
    if (!dbExistsTable(comment_property_rating, raw_table_name)) {
      message("產品線 ", product_line_id, ": 找不到原始表 ", raw_table_name, "，跳過。")
      next
    }
    raw_data <- tbl(comment_property_rating, raw_table_name) %>% collect()
    message("產品線 ", product_line_id, ": _raw 表筆數：", nrow(raw_data))
    
    # 備份 _raw 表
    time_stamp_raw <- format(Sys.time(), "%Y%m%d%H%M%S")
    backup_raw_name <- paste0(raw_table_name, "_backup_", time_stamp_raw)
    backup_sql <- paste("CREATE TABLE", backup_raw_name, "AS SELECT * FROM", raw_table_name)
    dbExecute(comment_property_rating, backup_sql)
    message("產品線 ", product_line_id, ": 已備份 _raw 表為 ", backup_raw_name)
    
    # 讀取主表；若不存在，則以 _raw 資料建立主表
    if (dbExistsTable(comment_property_rating, main_table_name)) {
      main_data <- tbl(comment_property_rating, main_table_name) %>% collect()
      message("產品線 ", product_line_id, ": 主表筆數：", nrow(main_data))
    } else {
      message("產品線 ", product_line_id, ": 主表不存在，使用 _raw 資料建立主表。")
      main_data <- raw_data
    }
    
    # 備份主表（如果存在）
    if (dbExistsTable(comment_property_rating, main_table_name)) {
      time_stamp_main <- format(Sys.time(), "%Y%m%d%H%M%S")
      backup_main <- paste0(main_table_name, "_backup_", time_stamp_main)
      sql_backup_main <- paste("CREATE TABLE", backup_main, "AS SELECT * FROM", main_table_name)
      dbExecute(comment_property_rating, sql_backup_main)
      message("產品線 ", product_line_id, ": 已備份主表為 ", backup_main)
    }
    
    ### 第一階段 patch：以 _rated 表更新主表
    if (dbExistsTable(comment_property_rating, rated_table_name)) {
      rated_data <- tbl(comment_property_rating, rated_table_name) %>% collect()
      message("產品線 ", product_line_id, ": _rated 表筆數：", nrow(rated_data))
      
      # 備份 _rated 表
      time_stamp_rated <- format(Sys.time(), "%Y%m%d%H%M%S")
      backup_rated <- paste0(rated_table_name, "_backup_", time_stamp_rated)
      sql_backup_rated <- paste("CREATE TABLE", backup_rated, "AS SELECT * FROM", rated_table_name)
      dbExecute(comment_property_rating, sql_backup_rated)
      message("產品線 ", product_line_id, ": 已備份 _rated 表為 ", backup_rated)
      
      # 建立 mapping：_rated 表 → 主表
      target_prop_cols <- setdiff(colnames(main_data), key_cols)
      source_prop_cols <- setdiff(colnames(rated_data), key_cols)
      mapping_rated <- list()
      for (i in seq_along(source_prop_cols)) {
        current_source <- source_prop_cols[i]
        distances <- stringdist::stringdist(current_source, target_prop_cols, method = "jw")
        d_min <- min(distances)
        best_match <- target_prop_cols[which.min(distances)]
        if (d_min == 0) {
          mapping_rated[[current_source]] <- current_source
        } else if (d_min < threshold) {
          mapping_rated[[current_source]] <- best_match
          message("產品線 ", product_line_id, ": 將 _rated 表欄位 '", current_source,
                  "' 映射為 '", best_match, "' (距離=", round(d_min, 3), ").")
        } else {
          message("產品線 ", product_line_id, ": 捨棄 _rated 表欄位 '", current_source,
                  "' (最小距離=", round(d_min, 3), ").")
        }
      }
      mapped_targets <- unlist(mapping_rated)
      if (length(unique(mapped_targets)) != length(mapped_targets)) {
        dup_names <- names(which(table(mapped_targets) > 1))
        stop("產品線 ", product_line_id, ": _rated 表映射中發現重複，主表欄位被多個來源對應：", 
             paste(dup_names, collapse = ", "))
      }
      for (source_name in names(mapping_rated)) {
        target_name <- mapping_rated[[source_name]]
        if (source_name != target_name) {
          names(rated_data)[names(rated_data) == source_name] <- target_name
        }
      }
      cols_to_keep <- c(key_cols, unname(unlist(mapping_rated)))
      rated_data <- rated_data[, intersect(colnames(rated_data), cols_to_keep)]
      rated_data <- rated_data %>% mutate(across(where(is.character), ~ ifelse(. == "", NA_character_, .)))
      
      updated_main <- rows_patch(main_data, rated_data, by = key_cols)
      message("產品線 ", product_line_id, ": 已用 _rated 資料 patch 主表。")
    } else {
      message("產品線 ", product_line_id, ": _rated 表不存在，主表維持原狀。")
      updated_main <- main_data
    }
    
    ### 第二階段 patch：以 updated_main 更新 _raw 表
    # 建立 mapping：_raw 表 → updated_main，並以 _raw 表欄位名稱為準
    raw_prop_cols <- setdiff(colnames(raw_data), key_cols)
    main_prop_cols_updated <- setdiff(colnames(updated_main), key_cols)
    mapping_raw <- list()
    for (i in seq_along(raw_prop_cols)) {
      current_raw <- raw_prop_cols[i]
      distances <- stringdist::stringdist(current_raw, main_prop_cols_updated, method = "jw")
      d_min <- min(distances)
      best_match <- main_prop_cols_updated[which.min(distances)]
      if (d_min == 0) {
        mapping_raw[[current_raw]] <- current_raw
      } else if (d_min < threshold) {
        mapping_raw[[current_raw]] <- best_match
        message("產品線 ", product_line_id, ": 將 _raw 表欄位 '", current_raw,
                "' 映射為 '", best_match, "' (距離=", round(d_min, 3), ").")
      } else {
        message("產品線 ", product_line_id, ": 捨棄 _raw 表欄位 '", current_raw,
                "' (最小距離=", round(d_min, 3), ").")
      }
    }
    mapped_targets_raw <- unlist(mapping_raw)
    if (length(unique(mapped_targets_raw)) != length(mapped_targets_raw)) {
      dup_names <- names(which(table(mapped_targets_raw) > 1))
      stop("產品線 ", product_line_id, ": _raw 表映射中發現重複：", paste(dup_names, collapse = ", "))
    }
    for (source_name in names(mapping_raw)) {
      target_name <- mapping_raw[[source_name]]
      if (source_name != target_name) {
        names(raw_data)[names(raw_data) == source_name] <- target_name
      }
    }
    
    
    cols_to_keep <- c(key_cols, unname(unlist(mapping_raw)))
    updated_main <- updated_main[, intersect(colnames(updated_main), cols_to_keep)]
    updated_main <- updated_main %>% mutate(across(where(is.character), ~ ifelse(. == "", NA_character_, .)))
    
    # 第二階段 patch：以 updated_main 來 patch _raw 表
    final_data <- rows_patch(raw_data, updated_main, by = key_cols)
    message("產品線 ", product_line_id, ": 已以 updated_main 資料 patch 更新 _raw 表。")
    
    # 刪除原 _raw 表，將更新後的 final_data 寫入，並命名為主表名稱（移除 _raw 後綴）
    dbWriteTable(comment_property_rating, main_table_name, final_data, overwrite = TRUE)
    message("產品線 ", product_line_id, ": 主表 ", main_table_name, " 已由更新後的 _raw 建立。")
    
    # 如果 _rated 表存在，則刪除之
    if (dbExistsTable(comment_property_rating, rated_table_name)) {
      dbRemoveTable(comment_property_rating, rated_table_name)
      message("產品線 ", product_line_id, ": _rated 表 ", rated_table_name, " 已刪除。")
    }
    
    message("產品線 ", product_line_id, " 處理完成。")
  }
}
