# ============================================================================
# Poisson Regression Analysis - InsightForge Version
# 從 InsightForge 移植過來的 Poisson 迴歸分析函數
# 用於分析產品屬性對銷量的影響
# ============================================================================

#' Poisson 迴歸分析函數
#' 
#' @param var_name 字符串，要分析的變數名稱（如 "品質", "價格" 等）
#' @param Dta 數據框，必須包含 'sales' 欄位和指定的變數
#' @return 列表，包含邊際效應、賽道倍數、係數等分析結果
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

#' 批量分析所有屬性
#' 
#' @param data 數據框，包含銷量和各個屬性
#' @param attribute_cols 字符向量，要分析的屬性欄位名稱
#' @return 列表，包含所有屬性的分析結果
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

#' 生成 Poisson 分析的解讀報告
#' 
#' @param results_df 數據框，包含分析結果
#' @return 字符串，格式化的解讀報告
generate_poisson_interpretation <- function(results_df) {
  if (nrow(results_df) == 0) {
    return("📊 分析結果：沒有足夠的資料進行分析")
  }
  
  # 找出最重要的屬性
  top_track_attr <- results_df$屬性[1]
  top_track_multiplier <- results_df$賽道倍數[1]
  top_marginal_idx <- which.max(results_df$邊際效應百分比)
  top_marginal_attr <- results_df$屬性[top_marginal_idx]
  top_marginal_effect <- results_df$邊際效應百分比[top_marginal_idx]
  
  interpretation <- paste0(
    "🎯 產品屬性正向影響力分析報告\n\n",
    "📊 分析概要：\n",
    "• 成功分析了 ", nrow(results_df), " 個具有正向影響的評分屬性\n",
    "• 所有係數均為正值，表示這些屬性對銷售有促進作用\n\n",
    
    "🏁 賽道冠軍（戰略重點）：\n",
    "• ", top_track_attr, " - 賽道倍數 ", top_track_multiplier, " 倍\n",
    "• 意義：從最低到最高的總體正向影響潛力最大\n",
    "• 建議：適合制定長期戰略改進計劃，提升此屬性將帶來最大收益\n\n",
    
    "⚡ 邊際效應冠軍（日常優化）：\n",
    "• ", top_marginal_attr, " - 每提升1單位促進銷售 ", top_marginal_effect, "%\n",
    "• 意義：小幅改進就能看到明顯的正向效果\n",
    "• 建議：適合日常運營的快速優化，投報率高\n\n",
    
    "📈 決策指南（針對正向影響屬性）：\n",
    "• 賽道倍數 > 2.0：極重要正向因素，核心競爭優勢\n",
    "• 賽道倍數 1.2-2.0：重要促進因素，應重點關注\n",
    "• 邊際效應 > 50%：強烈正向影響，小改進大效果\n",
    "• 邊際效應 20-50%：中等正向影響，穩定提升策略\n\n",
    "💡 行動建議：專注於正向係數屬性的提升，制定「戰略+戰術」雙重優化策略，確保投資回報最大化"
  )
  
  return(interpretation)
}