# positionStrategy Component Documentation

## 概述

`positionStrategy` 是一個 R Shiny 模組，用於進行產品的策略定位分析。該組件基於關鍵因素分析，將產品策略劃分為四個象限，並提供 AI 驅動的策略建議。

## 主要功能

### 1. 四象限策略視覺化
- **訴求 (Argument)**: 展示產品在關鍵因素上表現優異的領域
- **改善 (Improvement)**: 顯示非關鍵因素中表現良好的項目
- **劣勢 (Weakness)**: 標示非關鍵因素中表現不佳的方面
- **改變 (Change)**: 指出關鍵因素中需要改進的領域

### 2. 動態字體大小調整
- 根據內容長度自動調整文字大小 (10-24px)
- 確保四象限中的文字清晰可讀
- 考慮字符數和行數進行智能縮放

### 3. AI 策略分析
- 整合 OpenAI API 生成深度策略建議
- 基於四象限分析結果提供具體的行銷策略
- 自動跳過空白象限，只顯示有內容的部分
- 生成中文繁體的專業行銷建議（限制 300 字內）

### 4. 產品選擇與過濾
- 支援通過 Item ID 選擇特定產品
- 自動排除特殊行（Ideal、Rating、Revenue）
- 顯示品牌和產品 ID 的組合選項

## 技術架構

### 遵循的原則
- **MP56**: Connected Component Principle - 組件結構設計
- **MP81**: Explicit Parameter Specification - 明確參數規範
- **R116**: Enhanced Data Access with tbl2 - 增強的數據訪問
- **R09**: UI-Server-Defaults Triple - UI/Server/預設值三元組
- **MP88**: Immediate Feedback - 即時回饋（無需 Apply 按鈕）
- **MP47**: Functional Programming - 函數式編程
- **MP73**: Interactive Visualization Preference - 互動式視覺化

### 依賴套件
- `shiny`: 核心框架
- `plotly`: 互動式圖表
- `dplyr`: 數據處理
- `httr2`: API 調用（可選）
- `jsonlite`: JSON 處理
- `stringr`: 字串處理（可選）
- `markdown`: Markdown 轉 HTML（可選）
- `shinycssloaders`: 載入動畫（可選）

## 數據處理流程

### 1. 數據載入與篩選
```r
position_data <- reactive({
  fn_get_position_complete_case(
    app_data_connection = app_data_connection,
    product_line_id = prod_line,
    include_special_rows = TRUE,  # 需要 Ideal 行來識別關鍵因素
    apply_type_filter = TRUE
  )
})
```

#### 篩選機制詳解
1. **產品線篩選**: 使用 `product_line_id` 參數篩選特定產品線
2. **特殊行處理**: `include_special_rows = TRUE` 確保包含 "Ideal"、"Rating"、"Revenue" 等特殊行
3. **類型篩選**: `apply_type_filter = TRUE` 應用資料類型篩選

### 2. 關鍵因素識別

#### 識別流程
```r
key_factors <- reactive({
  data <- position_data()
  
  # 步驟 1: 處理平台特定的產品 ID 欄位
  if (!"product_id" %in% names(data)) {
    item_col <- switch(platform_id(),
      "amz" = "asin",            # Amazon
      "eby" = "ebay_item_number",# eBay
      "product_id"               # 預設
    )
    data <- data %>% dplyr::rename(product_id = !!sym(item_col))
  }
  
  # 步驟 2: 篩選出分析資料（排除 Rating、Revenue）
  df_analysis <- data %>%
    dplyr::filter(!product_id %in% c("Rating", "Revenue"))
  
  # 步驟 3: 提取 Ideal 行
  ideal_row <- df_analysis %>% dplyr::filter(product_id == "Ideal")
  
  # 步驟 4: 排除系統變數
  exclude_vars <- c("product_line_id", "platform_id", "rating", "sales", "revenue", "product_id", "brand")
  ideal_row <- ideal_row %>% dplyr::select(-dplyr::any_of(exclude_vars))
  
  # 步驟 5: 識別關鍵因素（Ideal 行中數值 > 0 的欄位）
  key_factors <- character(0)
  for (col in names(ideal_row)) {
    ideal_val <- ideal_row[[col]][1]
    if (!is.na(ideal_val) && is.numeric(ideal_val) && is.finite(ideal_val) && ideal_val > 0) {
      key_factors <- c(key_factors, col)
    }
  }
})
```

#### 關鍵因素定義
- 從 Ideal 行中提取關鍵因素
- 排除系統變數（product_id、brand、rating、sales、revenue、product_line_id、platform_id）
- 只保留數值型且在 Ideal 行中有正值的因素

### 3. 策略分析演算法

#### 核心演算法實作
```r
perform_strategy_analysis <- function(data, selected_item_id, key_factors, exclude_vars) {
  # 1. 資料篩選 - 選擇特定產品
  sub_sa <- sa_token %>% 
    dplyr::filter(product_id == selected_item_id)
  
  # 2. 排除系統欄位，只保留數值型欄位
  exclude_all <- c("product_id", "brand", "product_line_id", "platform_id", exclude_vars)
  numeric_data <- sub_sa %>% 
    dplyr::select(-dplyr::any_of(exclude_all)) %>%
    dplyr::select_if(is.numeric)
  
  # 3. 分離關鍵因素與非關鍵因素
  key_factors_present <- intersect(key_factors, names(numeric_data))
  non_key_factors <- setdiff(names(numeric_data), key_factors_present)
  
  # 4. 計算關鍵因素的平均值
  if (length(key_factors_present) > 0) {
    sub_dir <- colSums(numeric_data %>% dplyr::select(dplyr::all_of(key_factors_present)), na.rm = TRUE)
    key_mean <- mean(sub_dir, na.rm = TRUE)
  } else {
    key_mean <- 0
  }
  
  # 5. 計算非關鍵因素的平均值
  if (length(non_key_factors) > 0) {
    sub_dir_not_key <- colSums(numeric_data %>% dplyr::select(dplyr::all_of(non_key_factors)), na.rm = TRUE)
    non_key_mean <- mean(sub_dir_not_key, na.rm = TRUE)
  } else {
    non_key_mean <- 0
  }
  
  # 6. 四象限分類（完整實作）
  argument_factors <- names(sub_dir[sub_dir > key_mean])              # 訴求: 關鍵因素 > key_mean
  changing_factors <- names(sub_dir[sub_dir <= key_mean])             # 改變: 關鍵因素 <= key_mean
  improvement_factors <- names(sub_dir_not_key[sub_dir_not_key > non_key_mean])   # 改善: 非關鍵因素 > non_key_mean
  weakness_factors <- names(sub_dir_not_key[sub_dir_not_key <= non_key_mean])     # 劣勢: 非關鍵因素 <= non_key_mean
}
```

#### 平均值計算細節
- **key_mean**: 所有關鍵因素的 colSums 的平均值
- **non_key_mean**: 所有非關鍵因素的 colSums 的平均值
- 使用 `na.rm = TRUE` 處理缺失值

## UI 組件結構

### FilterUI
- 產品選擇下拉選單（支援 1000 個選項）
- 隱藏的視覺化設定（字體大小、語言選擇）
- AI 分析按鈕
- 組件狀態顯示

### DisplayUI
1. 標題區：說明四象限策略視覺化
2. 策略矩陣：600px 高的 Plotly 互動圖表
3. 產品資訊：顯示選中產品的詳細信息
4. AI 分析結果：展示 AI 生成的策略建議

## API 整合

### OpenAI 配置
- 使用環境變數 `OPENAI_API_KEY`
- 預設模型：`o4-mini`
- 最大 token 數：4000
- 回應語言：繁體中文

### API 錯誤處理
- 檢查 API 金鑰格式（需以 'sk-' 開頭）
- 捕獲網路錯誤並顯示友善訊息
- 提供降級方案（無 API 時仍可使用基本功能）

## 使用範例

```r
# 基本使用
strategyComp <- positionStrategyComponent("strategy_analysis")

# 帶資料庫連接
strategyComp <- positionStrategyComponent(
  id = "strategy_analysis",
  app_data_connection = app_conn,
  config = list(platform_id = "amz")  # Amazon
)

# 響應式配置
strategyComp <- positionStrategyComponent(
  id = "strategy_analysis",
  app_data_connection = app_conn,
  config = reactive({ 
    list(filters = list(
      platform_id = input$platform,
      product_line_id = input$product_line
    ))
  })
)
```

## 輸出內容

### 反應式值
組件返回以下反應式值：
- `position_data`: 定位數據
- `strategy_result`: 策略分析結果
- `key_factors`: 識別的關鍵因素
- `component_status`: 組件狀態（idle/loading/ready/computing/error）
- `ai_analysis_result`: AI 分析結果文本

## 特色功能

### 1. 智能文字佈局
- 每行最多 2 個項目，自動換行
- 使用 Tab 分隔同行項目
- 動態計算最佳字體大小

### 2. 平台適應性
- 自動偵測平台特定的產品 ID 欄位
- 支援 Amazon (asin) 和 eBay (ebay_item_number)
- 統一轉換為 product_id 進行處理

### 3. 進度回饋
- 使用 `withProgress` 顯示 AI 分析進度
- 分階段更新：準備數據 → 分析策略 → 調用 API → 處理結果

### 4. 視覺化控制
- 隱藏 Plotly 工具列以簡化界面
- 禁用滾動縮放和雙擊縮放
- 固定座標軸範圍 (-10 到 10)

## 與其他實作的比較

### BrandEdge 自定義版本對比
BrandEdge 應用程式（l1_basic/BrandEdge）有自己的策略分析實作，與本全域組件的算法核心相同：

#### 算法一致性
```r
# BrandEdge 版本
訴求 = feats_key[sums_key > mean(sums_key)]      # 關鍵因素 > 平均值
改變 = feats_key[sums_key <= mean(sums_key)]     # 關鍵因素 <= 平均值
改善 = feats_non[sums_non > mean(sums_non)]      # 非關鍵因素 > 平均值
劣勢 = feats_non[sums_non <= mean(sums_non)]     # 非關鍵因素 <= 平均值

# 全域 positionStrategy 版本（本組件）
argument_factors = names(sub_dir[sub_dir > key_mean])                    # 關鍵因素 > 平均值
changing_factors = names(sub_dir[sub_dir <= key_mean])                   # 關鍵因素 <= 平均值
improvement_factors = names(sub_dir_not_key[sub_dir_not_key > non_key_mean])  # 非關鍵因素 > 平均值
weakness_factors = names(sub_dir_not_key[sub_dir_not_key <= non_key_mean])    # 非關鍵因素 <= 平均值
```

#### 主要差異
1. **變數命名**：BrandEdge 使用中文命名，全域組件使用英文
2. **錯誤處理**：全域組件有更完善的錯誤處理和邊界條件檢查
3. **功能完整性**：全域組件支援更多功能（動態字體、平台適應、快取等）
4. **資料處理**：全域組件使用 `fn_get_position_complete_case` 進行更複雜的資料篩選

## 注意事項

1. **數據要求**：需要包含 Ideal 行的定位數據才能正確識別關鍵因素
2. **API 限制**：需要有效的 OpenAI API 金鑰才能使用 AI 分析功能
3. **性能考量**：大量產品時建議使用 `maxOptions` 限制選項數量
4. **瀏覽器相容性**：Plotly 圖表需要現代瀏覽器支援
5. **平均值計算**：`key_mean` 和 `non_key_mean` 是基於各因素的 colSums 計算的平均值，而非原始數值的平均
