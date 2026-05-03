# Password-Only Login Module

簡化版的密碼登入模組，只需要輸入密碼即可進入系統，適合內部使用或簡單的存取控制需求。

## 特色功能

- 🔐 **彈性密碼驗證** - 支援單一或多個密碼登入
- 🔑 **多密碼支援** - 可設定多組密碼供不同使用者使用
- 🔒 **環境變數配置** - 密碼儲存在環境變數中，安全且易於管理
- 🚫 **登入嘗試限制** - 防止暴力破解，超過次數自動鎖定
- ⏱️ **自動解鎖** - 鎖定期間過後自動解除限制
- 🎨 **自訂外觀** - 可自訂顏色、標題、圖示等UI元素

## 使用方法

### 1. 基本使用

```r
library(shiny)
library(shinyjs)

# 設定密碼 (在部署環境中設定)
# 單一密碼
Sys.setenv(APP_PASSWORD = "your_secure_password")

# 或設定多個密碼（三種格式都支援）
# 格式1: R 向量格式
Sys.setenv(APP_PASSWORD = 'c("123","1234","admin")')

# 格式2: 逗號分隔
Sys.setenv(APP_PASSWORD = "123,1234,admin")

# 格式3: JSON 陣列（需要 jsonlite 套件）
Sys.setenv(APP_PASSWORD = '["123","1234","admin"]')

# UI
ui <- fluidPage(
  useShinyjs(),
  passwordOnlyUI(
    id = "login",
    app_title = "我的應用程式"
  ),
  hidden(
    div(id = "main_content",
        h1("受保護的內容")
    )
  )
)

# Server
server <- function(input, output, session) {
  auth <- passwordOnlyServer("login")
  
  observe({
    if (auth$logged_in()) {
      shinyjs::show("main_content")
    } else {
      shinyjs::hide("main_content")
    }
  })
}

shinyApp(ui, server)
```

### 2. 進階設定

```r
# UI 自訂選項
passwordOnlyUI(
  id = "login",
  app_title = "精準行銷平台",
  app_icon = "www/logo.png",
  password_label = "請輸入系統密碼",
  submit_label = "進入系統",
  background_color = "#f0f4f8",
  primary_color = "#17a2b8",
  card_width = "400px"
)

# Server 自訂選項
passwordOnlyServer(
  id = "login",
  password_env_var = "MY_APP_PASSWORD",  # 自訂環境變數名稱
  max_attempts = 5,                      # 最大嘗試次數
  lockout_duration = 600,                # 鎖定時間（秒）
  success_message = "歡迎回來！",
  error_message = "密碼不正確",
  lockout_message = "請稍後再試"
)
```

## 環境變數設定

### 本地開發
```r
# 在 R 中設定單一密碼
Sys.setenv(APP_PASSWORD = "development_password")

# 設定多個密碼
Sys.setenv(APP_PASSWORD = 'c("pass1","pass2","pass3")')
# 或
Sys.setenv(APP_PASSWORD = "pass1,pass2,pass3")

# 在 .Renviron 檔案中
APP_PASSWORD=development_password
# 或多個密碼
APP_PASSWORD=c("pass1","pass2","pass3")
APP_PASSWORD=pass1,pass2,pass3
```

### Posit Connect 部署
1. 在 Posit Connect 管理介面中
2. 進入 Applications > Your App > Vars
3. 新增環境變數：`APP_PASSWORD`
4. 設定密碼值：
   - 單一密碼：`your_password`
   - 多個密碼（R 向量）：`c("pass1","pass2")`
   - 多個密碼（逗號分隔）：`pass1,pass2,pass3`
   - 多個密碼（JSON）：`["pass1","pass2","pass3"]`

### 使用 Variable Sets (推薦)
如果有多個應用程式需要不同密碼：
1. 建立 Variable Set
2. 為每個應用程式設定不同的 `APP_PASSWORD` 值（可以是單一或多個密碼）
3. 將 Variable Set 套用到對應的應用程式

### 多密碼使用場景
- **不同權限等級**：為不同使用者群組設定不同密碼
- **臨時存取**：可以新增臨時密碼，之後移除
- **密碼輪替**：在更換密碼期間，新舊密碼都可使用

## API 參考

### passwordOnlyUI

| 參數 | 說明 | 預設值 |
|------|------|--------|
| `id` | 模組命名空間ID | 必填 |
| `app_title` | 應用程式標題 | "應用程式" |
| `app_icon` | 圖示路徑 | NULL |
| `password_label` | 密碼輸入欄位標籤 | "請輸入密碼" |
| `submit_label` | 送出按鈕標籤 | "進入" |
| `background_color` | 背景顏色 | "#f5f6fa" |
| `primary_color` | 主要顏色 | "#007bff" |
| `card_width` | 登入卡片最大寬度 | "350px" |

### passwordOnlyServer

| 參數 | 說明 | 預設值 |
|------|------|--------|
| `id` | 模組命名空間ID | 必填 |
| `password_env_var` | 環境變數名稱，支援單一或多個密碼<br>格式：<br>- 單一密碼：`"password"`<br>- R 向量：`c("p1","p2")`<br>- 逗號分隔：`"p1,p2"`<br>- JSON：`["p1","p2"]` | "APP_PASSWORD" |
| `max_attempts` | 最大嘗試次數 | 3 |
| `lockout_duration` | 鎖定時間（秒） | 300 |
| `success_message` | 成功訊息 | "✅ 登入成功" |
| `error_message` | 錯誤訊息 | "❌ 密碼錯誤" |
| `lockout_message` | 鎖定訊息 | "⛔ 登入失敗次數過多..." |

### 回傳值

`passwordOnlyServer` 回傳一個 list，包含：
- `logged_in`: reactive 值，表示是否已登入
- `logout`: 函數，用於登出

## 完整範例

查看 `passwordOnly_example.R` 以了解完整的使用範例，包含：
- 登入/登出功能
- 主要內容的顯示/隱藏
- bs4Dash 整合
- 自訂樣式

## 安全建議

1. **永遠不要** 在程式碼中硬編碼密碼
2. 使用強密碼（建議至少 12 個字元）
3. 定期更換密碼
4. 在生產環境中使用 HTTPS
5. 考慮結合其他安全措施（如 IP 白名單）

## 重要：資源路徑設定

當使用圖示或其他靜態資源時，必須在應用程式中設定資源路徑：

```r
# 在 UI 定義之前設定資源路徑
# 例如：使用 global_scripts 的資源
addResourcePath("assets", "scripts/global_scripts/24_assets")

# 或設定本地資源路徑
if (dir.exists("www/images")) {
  addResourcePath("images", "www/images")
}

# 然後才建立 UI
ui <- fluidPage(
  passwordOnlyUI("login", 
    app_icon = "images/logo.png"  # 使用設定的資源路徑
  ),
  ...
)
```

⚠️ **注意事項**：
- 必須在 UI 定義**之前**呼叫 `addResourcePath()`
- 資源路徑名稱（第一個參數）將成為 URL 路徑的一部分
- 實際路徑（第二個參數）是相對於應用程式根目錄的路徑

## 與 union_production_test.R 整合

可以輕鬆整合到現有的應用程式中：

```r
# 設定資源路徑（如果有使用圖示）
addResourcePath("assets", "scripts/global_scripts/24_assets")

# 在 UI 中加入
ui <- bs4DashPage(
  ...,
  body = bs4DashBody(
    useShinyjs(),
    passwordOnlyUI("login", 
      app_title = "AI Marketing Platform",
      app_icon = "assets/icons/app_icon.png"  # 使用資源路徑
    ),
    hidden(
      div(id = "main_app",
          # 原本的 UI 內容
          bs4TabItems(...)
      )
    )
  )
)

# 在 Server 中加入
server <- function(input, output, session) {
  # 設定資源路徑（Server 端）
  addResourcePath("images", "www/images")
  
  auth <- passwordOnlyServer("login")
  
  observe({
    if (auth$logged_in()) {
      shinyjs::show("main_app")
      # 初始化主要應用程式邏輯
    } else {
      shinyjs::hide("main_app")
    }
  })
  
  # 原本的 server 邏輯...
}
```