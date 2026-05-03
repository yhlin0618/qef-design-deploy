# String Utilities (04_utils/string)

通用字串處理工具函數，可在任何地方使用。

## 函數列表

### `fn_remove_illegal_utf8.R`
- **功能**：移除資料框中所有非法的 UTF-8 字符
- **用途**：通用資料清理工具
- **依賴**：無外部套件依賴
- **適用場景**：任何需要清理字串編碼問題的地方

## 使用方式

```r
# 載入函數
source("scripts/global_scripts/04_utils/string/fn_remove_illegal_utf8.R")

# 使用函數
clean_df <- remove_illegal_utf8(messy_df)
```

## 設計原則

這個目錄下的函數應該：
- 通用性強，不限於特定業務場景
- 依賴最小化，避免過多外部套件
- 可重複使用，適合放在工具函數庫