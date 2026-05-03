# Company Information Directory Documentation
## 27_company_info 使用指南

---

## 📁 目錄結構

```
27_company_info/
├── peakededges/          # 祈鋒行銷科技公司資訊
│   ├── about.md         # 公司簡介
│   ├── contacts.md      # 聯絡資訊
│   ├── services.md      # 服務項目
│   ├── testimonials.md  # 客戶見證
│   ├── branding/        # 品牌素材 (logos, colors)
│   └── docs/            # 額外文件
├── shared/              # 共用資源
│   ├── disclaimers.md   # 法律聲明
│   ├── privacy.md       # 隱私政策
│   └── terms.md         # 服務條款
└── templates/           # 範本與文件
    ├── README.md        # 本文件
    └── company_template/ # 新公司資訊範本
```

---

## 🚀 在 Shiny Apps 中使用

### 基本使用方法

```r
# 在 Shiny app 中載入公司資訊
library(shiny)
library(markdown)

# 方法 1: 直接載入 Markdown 檔案
ui <- fluidPage(
  includeMarkdown("scripts/global_scripts/27_company_info/peakededges/about.md")
)

# 方法 2: 在 modalDialog 中顯示
showModal(modalDialog(
  title = "關於我們",
  includeMarkdown("scripts/global_scripts/27_company_info/peakededges/about.md"),
  size = "l",
  easyClose = TRUE
))

# 方法 3: 在 tabPanel 中組織多個文件
ui <- navbarPage(
  "祈鋒行銷科技",
  tabPanel("關於我們", 
    includeMarkdown("scripts/global_scripts/27_company_info/peakededges/about.md")
  ),
  tabPanel("服務項目", 
    includeMarkdown("scripts/global_scripts/27_company_info/peakededges/services.md")
  ),
  tabPanel("聯絡我們", 
    includeMarkdown("scripts/global_scripts/27_company_info/peakededges/contacts.md")
  )
)
```

### 進階使用：動態載入

```r
# 根據設定動態載入公司資訊
get_company_info <- function(company_id, info_type) {
  base_path <- "scripts/global_scripts/27_company_info"
  file_path <- file.path(base_path, company_id, paste0(info_type, ".md"))
  
  if (file.exists(file_path)) {
    return(includeMarkdown(file_path))
  } else {
    return(p("資訊載入中..."))
  }
}

# 在 UI 中使用
ui <- fluidPage(
  uiOutput("company_info")
)

server <- function(input, output, session) {
  output$company_info <- renderUI({
    get_company_info("peakededges", "about")
  })
}
```

### 整合到現有應用程式

```r
# 在 app_config.yaml 中設定
company_info:
  id: "peakededges"
  display_sections:
    - about
    - services
    - contacts
    - testimonials

# 在應用程式中讀取設定
config <- yaml::read_yaml("app_config.yaml")
company_id <- config$company_info$id
sections <- config$company_info$display_sections

# 動態生成 UI
ui_sections <- lapply(sections, function(section) {
  tabPanel(
    switch(section,
      "about" = "關於我們",
      "services" = "服務項目",
      "contacts" = "聯絡我們",
      "testimonials" = "客戶見證"
    ),
    get_company_info(company_id, section)
  )
})
```

---

## 📝 新增公司資訊

### 步驟 1：建立公司目錄

```bash
# 建立新公司目錄結構
mkdir -p 27_company_info/[company_name]/{branding,docs}
```

### 步驟 2：建立必要檔案

每個公司目錄應包含以下基本檔案：

1. **about.md** - 公司簡介
   - 公司願景與使命
   - 核心價值
   - 技術優勢
   - 產業應用

2. **contacts.md** - 聯絡資訊
   - Email 聯絡方式
   - 服務區域
   - 支援語言
   - 營業時間

3. **services.md** - 服務項目
   - 核心服務詳細說明
   - 技術特色
   - 應用場景
   - 交付成果

4. **testimonials.md** - 客戶見證（選擇性）
   - 成功案例
   - 客戶評價
   - 量化成果
   - 產業應用成效

### 步驟 3：遵循內容格式規範

```markdown
# 主標題
## 副標題

---  <!-- 分隔線 -->

### 章節標題

內容段落...

- 項目列表
- 使用一致的 emoji 圖示
- 保持專業語氣

**重點強調**使用粗體

*次要說明*使用斜體

[連結文字](mailto:email@domain.com)
```

---

## 🎨 最佳實踐

### 內容撰寫原則

1. **專業性**
   - 使用正式商業語言
   - 避免過度技術性描述
   - 平衡中英文專有名詞

2. **一致性**
   - 統一的格式與排版
   - 一致的品牌聲音
   - 標準化的聯絡資訊格式

3. **可讀性**
   - 使用清晰的章節結構
   - 適當使用列表與表格
   - 重要資訊使用視覺強調

4. **維護性**
   - 避免硬編碼時效性資訊
   - 使用相對路徑引用
   - 定期檢查更新內容

### 檔案命名規範

- 使用小寫英文字母
- 單字間使用底線分隔
- 避免使用特殊字元
- 保持簡短但具描述性

### 版本控制

```bash
# 更新內容時記錄變更
git add 27_company_info/[company_name]/*
git commit -m "Update [company_name] company information: [specific changes]"
```

---

## 🔧 工具函數

### 載入公司資訊的輔助函數

```r
# utils/company_info_loader.R

#' Load company information
#' @param company_id Company identifier
#' @param info_type Type of information (about, contacts, services, testimonials)
#' @param base_path Base path to company_info directory
#' @return Rendered markdown content or error message
load_company_info <- function(company_id, info_type, 
                             base_path = "scripts/global_scripts/27_company_info") {
  
  valid_types <- c("about", "contacts", "services", "testimonials")
  
  if (!info_type %in% valid_types) {
    stop("Invalid info_type. Must be one of: ", paste(valid_types, collapse = ", "))
  }
  
  file_path <- file.path(base_path, company_id, paste0(info_type, ".md"))
  
  if (!file.exists(file_path)) {
    return(p(class = "text-muted", "資訊尚未提供"))
  }
  
  tryCatch({
    includeMarkdown(file_path)
  }, error = function(e) {
    p(class = "text-danger", "載入資訊時發生錯誤")
  })
}

#' Check available company information
#' @param company_id Company identifier
#' @param base_path Base path to company_info directory
#' @return Named logical vector of available files
check_company_info <- function(company_id, 
                              base_path = "scripts/global_scripts/27_company_info") {
  
  company_path <- file.path(base_path, company_id)
  
  if (!dir.exists(company_path)) {
    return(NULL)
  }
  
  files <- c("about.md", "contacts.md", "services.md", "testimonials.md")
  available <- file.exists(file.path(company_path, files))
  names(available) <- gsub("\\.md$", "", files)
  
  return(available)
}
```

---

## 🔒 安全性考量

1. **避免敏感資訊**
   - 不要包含內部系統資訊
   - 避免具體的財務數據
   - 不公開員工個人資訊

2. **聯絡資訊保護**
   - 使用公司統一對外信箱
   - 避免直接電話號碼
   - 考慮使用表單連結替代

3. **版權聲明**
   - 確保所有內容擁有使用權
   - 標註商標與版權資訊
   - 遵守隱私法規要求

---

## 📚 相關原則參考

根據 MAMBA 框架原則：

- **MP014 - Company Centered Design**: 所有內容以公司需求為中心設計
- **MP012 - Information Flow Transparency**: 資訊流程保持透明
- **MP030 - Archive Immutability**: 歷史資料保持不變性

---

## 🤝 貢獻指南

1. 新增或修改公司資訊前，請先確認：
   - 內容的準確性與時效性
   - 符合品牌形象與聲音
   - 遵循既定格式規範

2. 提交變更時：
   - 使用清晰的 commit message
   - 在 PR 中說明變更原因
   - 確保通過相關測試

3. 定期維護：
   - 每季檢查資訊時效性
   - 更新客戶案例與成就
   - 同步最新服務內容

---

*最後更新：2025-09-09*

**祈鋒行銷科技有限公司**  
**PeakedEdges Marketing Technology Co., Ltd.**