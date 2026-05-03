poisson_regression <- function(var_name, Dta) {
  # 檢查變數是否全為 NA
  if (all(is.na(Dta[[var_name]]))) {
    return(list(
      marginal_effect_pct = NA,
      coefficient = NA,
      interpretation = "資料不足",
      practical_meaning = "無法分析"
    ))
  }
  
  # 檢查並處理 sales 欄位
  if (!"sales" %in% names(Dta)) {
    warning("找不到 'sales' 欄位")
    return(list(
      marginal_effect_pct = NA,
      coefficient = NA,
      interpretation = "缺少銷量資料",
      practical_meaning = "無法分析"
    ))
  }
  
  # 將 sales 轉換為整數，並確保非負
  Dta$sales <- round(abs(as.numeric(Dta$sales)))
  
  # 檢查處理後的 sales 是否有效
  if (all(is.na(Dta$sales)) || all(Dta$sales == 0)) {
    warning("處理後的 sales 資料無效")
    return(list(
      marginal_effect_pct = NA,
      coefficient = NA,
      interpretation = "銷量資料無效",
      practical_meaning = "無法分析"
    ))
  }
  
  # 進行 Poisson 回歸
  formula <- as.formula(paste("sales ~", var_name))
  poisson_model <- glm(formula, data = Dta, family = poisson())
  
  # 獲取指定變數的係數
  coef_var <- poisson_model$coefficients[var_name]
  
  # 檢查係數是否存在
  if (is.na(coef_var)) {
    return(list(
      marginal_effect_pct = NA,
      coefficient = NA,
      interpretation = "模型擬合失敗",
      practical_meaning = "無法分析"
    ))
  }
  
  # 計算更容易理解的指標
  
  # 1. 邊際效應百分比（每增加1單位的影響）
  marginal_effect_pct <- (exp(coef_var) - 1) * 100
  
  # 2. 計算賽道倍數（從最低到最高的總影響）
  var_range <- range(Dta[[var_name]], na.rm = TRUE)
  track_width <- diff(var_range)  # 賽道寬度
  # 賽道倍數：考慮係數方向，正係數表示高值有利，負係數表示低值有利
  track_multiplier <- exp(track_width * abs(coef_var))  # 賽道的影響倍數大小
  
  # 3. 生成解釋文字
  direction <- if(coef_var > 0) "正面影響" else "負面影響"
  strength <- if(abs(marginal_effect_pct) > 50) "強烈" else 
             if(abs(marginal_effect_pct) > 20) "中等" else "微弱"
  
  interpretation <- paste0(var_name, "對銷量有", strength, direction)
  
  # 4. 實際商業意義（考慮賽道倍數）
  if (track_multiplier < 1.2) {
    practical_meaning <- "影響很小，不是關鍵因素"
  } else if (track_multiplier < 2.0) {
    practical_meaning <- "有一定影響，可考慮優化"
  } else if (track_multiplier < 3.0) {
    practical_meaning <- "重要影響因素，應重點關注"
  } else {
    practical_meaning <- "極重要因素，是核心競爭力"
  }
  
  # 5. 賽道說明
  track_explanation <- paste0("從最", ifelse(coef_var > 0, "低", "高"), "到最", 
                             ifelse(coef_var > 0, "高", "低"), "，銷量可相差", 
                             round(track_multiplier, 1), "倍")
  
  return(list(
    marginal_effect_pct = round(marginal_effect_pct, 1),
    track_multiplier = round(track_multiplier, 1),
    track_width = round(track_width, 2),
    coefficient = round(coef_var, 3),
    interpretation = interpretation,
    practical_meaning = practical_meaning,
    unit_explanation = paste0("每提升1單位", var_name, "，銷量變化", 
                             round(abs(marginal_effect_pct), 1), "%"),
    track_explanation = track_explanation
  ))
}

# 批量分析函數
analyze_all_attributes <- function(data, attribute_cols) {
  results <- list()
  
  cat("=== 產品屬性影響力分析報告 ===\n\n")
  
  for (attr in attribute_cols) {
    if (attr %in% names(data) && is.numeric(data[[attr]])) {
      result <- poisson_regression(attr, data)
      results[[attr]] <- result
      
      cat("📊", attr, ":\n")
      cat("   影響程度:", result$unit_explanation, "\n")
      cat("   賽道效應:", result$track_explanation, "\n")
      cat("   商業意義:", result$practical_meaning, "\n")
      cat("   統計結論:", result$interpretation, "\n\n")
    }
  }
  
  # 排序並顯示重要性排名（以賽道倍數為主要指標）
  track_scores <- sapply(results, function(x) x$track_multiplier)
  track_scores <- track_scores[!is.na(track_scores)]
  
  if (length(track_scores) > 0) {
    sorted_attrs <- names(sort(track_scores, decreasing = TRUE))
    
    cat("🏆 屬性重要性排名 (按賽道倍數):\n")
    for (i in seq_along(sorted_attrs)) {
      attr <- sorted_attrs[i]
      multiplier <- results[[attr]]$track_multiplier
      effect <- results[[attr]]$marginal_effect_pct
      cat("   ", i, ".", attr, "- 賽道倍數:", multiplier, "倍 (邊際效應:", effect, "%)\n")
    }
  }
  
  return(results)
}
