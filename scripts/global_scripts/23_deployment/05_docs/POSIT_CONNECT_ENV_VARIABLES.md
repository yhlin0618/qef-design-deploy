# Posit Connect Cloud 環境變數設定指南

您的 positioning_app 包含敏感資訊（資料庫密碼、API Key），需要透過環境變數安全處理。

## 📋 現有環境變數

從您的 `.env` 檔案中，需要在 Posit Connect Cloud 設定的變數：

### PostgreSQL 資料庫
- `PGHOST`
- `PGPORT`
- `PGUSER`
- `PGPASSWORD`
- `PGDATABASE`
- `PGSSLMODE`

### OpenAI API Keys
- `OPENAI_API_KEY`
- `OPENAI_API_KEY_LIN`

## 🔐 安全部署步驟

### 步驟 1：創建 .Renviron 檔案（本地測試用）

創建 `.Renviron` 檔案：
```r
# PostgreSQL
PGHOST='db-postgresql-sgp1-73173-do-user-18877526-0.f.db.ondigitalocean.com'
PGPORT='25060'
PGUSER='doadmin'
PGPASSWORD='YOUR_PASSWORD_HERE'
PGDATABASE='positioning'
PGSSLMODE='require'

# OpenAI
OPENAI_API_KEY='YOUR_API_KEY_HERE'
OPENAI_API_KEY_LIN='YOUR_API_KEY_HERE'
```

### 步驟 2：確保 .gitignore 包含敏感檔案

```gitignore
# 環境變數檔案（絕對不要上傳）
.env
.env.*
.Renviron
```

### 步驟 3：在 Posit Connect Cloud 設定環境變數

1. 登入 https://connect.posit.cloud
2. 點擊 **Publish** → **Shiny**
3. 選擇您的 GitHub repository
4. 選擇 **app.R** 作為主檔案
5. **點擊 Advanced settings**
6. 在 **Configure variables** 下，逐一添加環境變數：

   對每個變數：
   - 點擊 **Add variable**
   - **Name**: 輸入變數名稱（如 `PGHOST`）
   - **Value**: 輸入變數值
   
7. 添加所有必要的環境變數後，點擊 **Publish**

## 🔧 在應用程式中使用環境變數

確保您的 R 程式碼使用 `Sys.getenv()` 讀取環境變數：

```r
# 資料庫連線
db_config <- list(
  host = Sys.getenv("PGHOST"),
  port = as.numeric(Sys.getenv("PGPORT", "5432")),
  user = Sys.getenv("PGUSER"),
  password = Sys.getenv("PGPASSWORD"),
  dbname = Sys.getenv("PGDATABASE"),
  sslmode = Sys.getenv("PGSSLMODE", "require")
)

# OpenAI API
openai_key <- Sys.getenv("OPENAI_API_KEY")
```

## 📝 檢查清單

部署前確認：
- [ ] `.env` 和 `.Renviron` 都在 `.gitignore` 中
- [ ] 沒有將任何密碼或 API Key 寫死在程式碼中
- [ ] 所有敏感資訊都透過 `Sys.getenv()` 讀取
- [ ] 已準備好所有環境變數的名稱和值

## ⚠️ 重要提醒

1. **永遠不要**將包含真實密碼的檔案提交到 GitHub
2. 環境變數值在 Posit Connect Cloud 中是加密儲存的
3. 更新環境變數後需要重新部署應用程式
4. 建議為不同環境（開發/生產）使用不同的資料庫和 API Key

## 🔄 更新環境變數

> 提醒：程式碼部署採 Git 自動部署；環境變數更新仍需在 Connect 內保存並觸發一次部署。

如需更新環境變數：
1. 在 Posit Connect Cloud 的 Content List 中找到您的應用
2. 進入應用設定
3. 更新環境變數
4. 重新部署應用

---
最後更新：2024-01-15 
