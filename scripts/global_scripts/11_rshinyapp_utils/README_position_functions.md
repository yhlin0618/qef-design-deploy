# Position Table 統一資料存取函數

## 概述

為了統一 Position Table 的資料存取模式，我們創建了兩個標準化函數來處理不同使用情境的資料需求。這些函數統一處理了產品線篩選、type 篩選、以及特殊行的處理。

## 函數說明

### 1. `fn_get_position_complete_case()` - 完整案例資料存取

**用途**: 用於需要完整資料進行分析的模組，包含特殊行 (Ideal, Rating, Revenue)

**使用場景**:
- 需要 Ideal 行進行基準比較的分析
- 完整的表格展示
- 關鍵因子分析 (KFE)
- 理想率分析 (IdealRate)  
- 策略分析 (Strategy) - 需要 Ideal 行計算關鍵因子

**參數**:
- `app_data_connection`: 資料庫連接物件
- `product_line_id`: 產品線 ID 篩選
- `include_special_rows`: 是否包含特殊行 (預設 TRUE)
- `apply_type_filter`: 是否應用 type = "屬性" 篩選 (預設 TRUE)
- `type_values`: 要包含的 type 值 (預設 "屬性")

**範例**:
```r
# 獲取完整的 Position Table 資料
data <- fn_get_position_complete_case(
  app_data_connection = conn,
  product_line_id = "tur",
  include_special_rows = TRUE,
  apply_type_filter = TRUE
)
```

### 2. `fn_get_position_demonstrate_case()` - 展示案例資料存取

**用途**: 用於展示實際市場資料的模組，排除特殊行，專注於真實競品資料

**使用場景**:
- 市場區隔分析 (MS Plotly)
- DNA 視覺化分析
- 競品比較分析
- 不需要理想值的視覺化展示

**參數**:
- `app_data_connection`: 資料庫連接物件
- `product_line_id`: 產品線 ID 篩選
- `apply_iterative_filter`: 是否應用迭代篩選 (預設 TRUE)
- `apply_type_filter`: 是否應用 type = "屬性" 篩選 (預設 TRUE)
- `type_values`: 要包含的 type 值 (預設 "屬性")
- `iterative_threshold`: 迭代篩選閾值 (預設 0.5)

**範例**:
```r
# 獲取展示用的 Position Table 資料
data <- fn_get_position_demonstrate_case(
  app_data_connection = conn,
  product_line_id = "tur",
  apply_iterative_filter = TRUE,
  apply_type_filter = TRUE
)
```

## 模組使用分類

### Complete Case 模組 (需要特殊行)

1. **positionTable** - 完整表格展示
   - 需要 Ideal 行作為 footer 顯示
   - 用戶可選擇是否顯示 Ideal 行

2. **positionKFE** - 關鍵因子評估
   - 必須有 Ideal 行才能進行分析
   - 基於理想值識別關鍵成功因子

3. **positionIdealRate** - 理想率分析
   - 需要 Ideal 行計算理想率分數
   - 排名產品與理想值的接近程度

4. **positionStrategy** - 策略分析
   - 需要 Ideal 行計算關鍵因子
   - 四象限策略分析需要基準值

### Demonstrate Case 模組 (排除特殊行)

1. **positionMSPlotly** - 市場區隔分析
   - 專注於實際競品的市場定位
   - 不需要理想值干擾聚類分析

2. **positionDNAPlotly** - DNA 視覺化
   - 展示真實品牌的屬性比較
   - 避免理想值影響視覺化效果

## Type 篩選功能

兩個函數都支援 type 篩選功能，只保留 `type = "屬性"` 的欄位：

- **自動欄位匹配**: 從 `df_all_comment_property` 表格獲取屬性資訊
- **動態欄位選擇**: 根據產品線自動篩選相關屬性欄位
- **基本欄位保護**: 始終保留 `product_id`, `brand`, `product_line_id`, `rating`, `sales`

## 錯誤處理

- **連接驗證**: 檢查資料庫連接有效性
- **參數驗證**: 驗證必要參數是否提供
- **容錯機制**: type 篩選失敗時自動回退到原始資料
- **詳細日誌**: 提供詳細的處理過程資訊

## 遷移指南

### 舊代碼模式:
```r
# 舊的資料存取方式
tbl <- tbl2(app_data_connection, "df_position")
tbl <- tbl %>% dplyr::filter(product_line_id == prod_line)
filtered_data <- tbl %>% collect()
filtered_data <- filtered_data %>% 
  dplyr::filter(!product_id %in% c("Ideal", "Rating", "Revenue"))
```

### 新代碼模式:
```r
# 新的統一資料存取方式
filtered_data <- fn_get_position_demonstrate_case(
  app_data_connection = app_data_connection,
  product_line_id = prod_line,
  apply_type_filter = TRUE
)
```

## 優勢

1. **統一介面**: 所有 Position 模組使用相同的資料存取模式
2. **Type 篩選**: 自動處理屬性類型篩選，避免無關欄位
3. **特殊行處理**: 統一處理 Ideal, Rating, Revenue 行的邏輯
4. **錯誤處理**: 統一的錯誤處理和日誌記錄
5. **維護性**: 集中化的邏輯，便於後續維護和更新 