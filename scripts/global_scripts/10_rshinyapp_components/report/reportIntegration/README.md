# 報告整合模組 (Report Integration Module)

## 問題背景
MAMBA 系統目前有四大分析模組，每個模組都有獨立的 AI 分析功能，但缺乏整合機制：
- 老闆需要看完整的分析報告，而不是分散的片段
- 各模組的分析結果無法互相參照
- 缺乏統一的報告輸出格式

## 解決方案概覽

### 核心功能
1. **自動收集** - 從四大模組自動收集 AI 分析結果
2. **智能整合** - 使用 AI 生成整合性洞察和建議
3. **彈性輸出** - 支援 HTML、PDF、Word 多種格式
4. **報告類型** - 執行摘要、完整報告、技術報告

### 系統架構
```
┌─────────────────────────────────────────────┐
│           報告整合模組 (Report Hub)          │
├─────────────────────────────────────────────┤
│  收集層 (Collection Layer)                   │
│  ├─ Marketing Vital-Signs → 宏觀指標        │
│  ├─ TagPilot → 顧客分析                     │
│  ├─ BrandEdge → 品牌定位                    │
│  └─ InsightForge 360 → 市場洞察             │
├─────────────────────────────────────────────┤
│  整合層 (Integration Layer)                  │
│  └─ OpenAI GPT → 生成整合性策略建議          │
├─────────────────────────────────────────────┤
│  輸出層 (Output Layer)                       │
│  └─ HTML / PDF / Word 報告生成               │
└─────────────────────────────────────────────┘
```

## 快速測試

### 1. 運行測試應用
```r
# 在 MAMBA 專案根目錄執行
source("test_report_integration.R")
```

### 2. 測試步驟
1. 選擇要包含的分析模組（預設全選）
2. 選擇報告類型（執行摘要/完整報告/技術報告）
3. 點擊「生成整合報告」
4. 預覽並下載報告

## 正式整合指南

### 步驟 1: 載入模組
在 `union_production_test.R` 的元件載入區（約第 60 行）加入：
```r
# Load report integration module
source("scripts/global_scripts/10_rshinyapp_components/report/reportIntegration/reportIntegration.R")
source("scripts/global_scripts/10_rshinyapp_components/report/reportIntegration/report_integration_addon.R")
```

### 步驟 2: 更新 UI
在 sidebar menu 中加入報告選項：
```r
# 在 sidebarMenu 的最後加入
bs4SidebarMenuItem(
  text = "整合報告",
  icon = icon("file-export"),
  startExpanded = FALSE,
  bs4SidebarMenuItem("AI 分析報告", tabName="report", icon=icon("magic"))
)
```

在 bs4TabItems 中加入對應頁面：
```r
bs4TabItem(
  tabName = "report",
  fluidRow(
    column(12,
      bs4Card(
        title = "AI 整合分析報告",
        status = "warning",
        width = 12,
        solidHeader = TRUE,
        elevation = 3,
        uiOutput("report_display")
      )
    )
  )
)
```

### 步驟 3: 更新 Server
在 server function 中，組件初始化後加入：
```r
# 收集 AI 分析結果的 reactive
ai_results_collected <- reactive({
  collect_ai_results(
    position_strategy_res = position_strategy_res,
    poisson_comment_res = poisson_comment_res,
    poisson_time_res = poisson_time_res,
    poisson_feature_res = poisson_feature_res
  )
})

# 初始化報告整合元件
report_comp <- reportIntegrationInitialize(
  "report",
  app_data_connection = app_connection,
  ai_results = ai_results_collected
)

# 渲染報告 UI
output$report_display <- renderUI2(
  current_tab = input$sidebar_menu,
  target_tab = "report",
  ui_component = report_comp$ui,
  loading_icon = "file-export"
)

# 啟動報告 server
report_res <- report_comp$server(input, output, session)
```

## 使用說明

### 報告類型說明
- **執行摘要** - 1-2 頁精簡報告，適合高階主管快速瀏覽
- **完整報告** - 包含所有分析細節，適合深入研究
- **技術報告** - 包含統計數據和技術細節，適合技術團隊

### 輸出格式
- **HTML** - 網頁格式，可直接在瀏覽器查看
- **PDF** - 需安裝 `pagedown` 套件：`install.packages("pagedown")`
- **Word** - 目前輸出為 Markdown 格式，可用 Word 開啟

### 注意事項
1. 確保環境變數 `OPENAI_API_KEY` 已設定
2. PDF 輸出需要 Chrome/Chromium 瀏覽器
3. 各模組必須先生成 AI 分析才能整合

## 客製化擴充

### 新增分析模組
在 `collect_ai_results` 函數中加入：
```r
if (!is.null(new_module_res$ai_result)) {
  ai_results$new_module <- new_module_res$ai_result()
}
```

### 自訂報告樣式
修改 `reportIntegration.R` 中的 CSS：
```css
body {
  font-family: 'Your Font', sans-serif;
  color: #your-color;
}
```

### 新增報告區段
在 `report_sections` 中加入：
```r
report_sections$custom <- paste0(
  "## 自訂區段\n\n",
  "您的內容...\n\n"
)
```

## 常見問題

### Q: 報告生成失敗
A: 檢查：
1. OPENAI_API_KEY 是否設定
2. 網路連線是否正常
3. 各模組是否有生成 AI 分析

### Q: PDF 輸出失敗
A: 安裝必要套件：
```r
install.packages("pagedown")
pagedown::find_chrome()  # 檢查 Chrome
```

### Q: 報告內容不完整
A: 確保在生成報告前，已經在各模組執行過 AI 分析

## 技術規格

### 依賴套件
- shiny (核心)
- bs4Dash (UI 框架)
- markdown (Markdown 轉 HTML)
- pagedown (PDF 輸出，選用)

### 遵循原則
- MP56: Connected Component Principle
- R091: Universal Data Access Pattern
- MP81: Explicit Parameter Specification
- R76: Module Data Connection Rule

## 聯絡支援
如有問題或建議，請聯繫技術團隊。

---
*最後更新: 2025-09-23*
*版本: 1.0.0*