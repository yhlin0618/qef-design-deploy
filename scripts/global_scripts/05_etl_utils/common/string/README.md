# ETL String Utilities (05_etl_utils/common/string)

ETL 專用的字串處理工具，專門為資料處理流程設計。

## 函數列表

### `fn_convert_all_columns_to_utf8.R`
- **功能**：智能轉換資料框所有欄位為 UTF-8 編碼
- **特色**：自動檢測編碼或指定來源編碼
- **依賴**：stringi 套件（用於編碼檢測）
- **適用場景**：ETL 匯入階段的編碼標準化

## 使用方式

```r
# 載入函數
source("scripts/global_scripts/05_etl_utils/common/string/fn_convert_all_columns_to_utf8.R")

# 自動檢測編碼並轉換
utf8_df <- convert_all_columns_to_utf8(raw_df)

# 指定來源編碼
utf8_df <- convert_all_columns_to_utf8(raw_df, from_encoding = "Big5")
```

## 設計原則

這個目錄下的函數應該：
- 專門為 ETL 流程設計
- 可以依賴專業套件（如 stringi）
- 處理複雜的資料轉換需求
- 支援批量資料處理