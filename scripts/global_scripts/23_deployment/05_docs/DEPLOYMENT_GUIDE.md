# Positioning App 部署指南

本指南說明如何使用 global_scripts 中的標準部署工具來部署 positioning_app。

## 🚀 快速開始

### 方法 1：使用便捷腳本（推薦）

```bash
cd /Users/che/Library/CloudStorage/Dropbox/ai_martech/l1_basic/positioning_app
Rscript deploy_using_global_scripts.R
```

### 方法 2：直接執行 global_scripts 部署腳本

```r
# 在 R 中執行
setwd("/Users/che/Library/CloudStorage/Dropbox/ai_martech/l1_basic/positioning_app")
source("scripts/global_scripts/21_rshinyapp_templates/rsconnect/deployment_positioning_app.R")
```

## 📝 環境設定

### 1. 創建 .env 檔案

在 positioning_app 目錄下創建 `.env` 檔案：

```bash
# 部署目標：connect 或 shinyapps
DEPLOY_TARGET=shinyapps

# ShinyApps.io 設定
SHINYAPPS_APP_NAME=positioning_app
SHINYAPPS_ACCOUNT=kyle-lin
APP_TITLE=Product Positioning Analysis

# 如果使用 Posit Connect（以 Git 自動部署為主；CONNECT_API_KEY 僅手動 rsconnect 時需要）
# DEPLOY_TARGET=connect
# CONNECT_SERVER=https://your-connect-server.com
# CONNECT_API_KEY=your-api-key-here
```

### 2. 設定 rsconnect 帳號

如果尚未設定 rsconnect 帳號：

```r
rsconnect::setAccountInfo(
  name = "kyle-lin",
  token = "從 shinyapps.io 獲取",
  secret = "從 shinyapps.io 獲取"
)
```

## 🔧 部署腳本說明

global_scripts 的部署腳本提供了以下功能：

1. **自動尋找專案根目錄**：使用 `here` 或 `rprojroot` 套件
2. **智能檔案選擇**：自動排除不必要的檔案
3. **環境變數支援**：從 .env 檔案讀取配置
4. **雙平台支援**：可部署到 ShinyApps.io 或 Posit Connect

## 📦 檔案結構

部署腳本會自動包含以下檔案：
- `app.R` 或 `full_app_v17.R`（主應用程式）
- `www/`（靜態資源）
- `icons/`（圖標）
- `scripts/`（必要的腳本）
- `manifest.json`（依賴清單）

自動排除：
- `.git`、`.Rproj`、`.Rhistory`
- `test/`、`tests/`、`archive/`
- `.env`（環境變數）
- `data/raw`、`data/test`（原始資料）

## 🐛 疑難排解

### 問題 1：找不到專案根目錄

如果遇到錯誤訊息：
```
Cannot find l1_basic directory in project root
```

解決方法：
```r
# 手動設定專案根目錄
Sys.setenv(HERE_ROOT = "/Users/che/Library/CloudStorage/Dropbox/ai_martech")
```

### 問題 2：here 套件未安裝

```r
install.packages("here")
# 或
install.packages("rprojroot")
```

### 問題 3：部署到 Posit Connect 失敗

如果使用 Git 自動部署，確認：
- Connect 內的 repo、branch、app path 設定正確
- Git push 已完成且有新的提交
若使用手動 rsconnect，再確認環境變數：
```r
# 檢查環境變數
Sys.getenv("DEPLOY_TARGET")
Sys.getenv("CONNECT_SERVER")
Sys.getenv("CONNECT_API_KEY")
```

### 問題 4：檔案大小超過限制

global_scripts 的部署工具已設定最大檔案大小為 4GB：
```r
options(rsconnect.max.bundle.size = 4 * 1024^3)
```

## 📊 檢查部署狀態

部署完成後，可以檢查應用狀態：

```r
# 查看應用列表
rsconnect::applications()

# 查看部署日誌
rsconnect::showLogs(appName = "positioning_app")

# 檢查應用配置
rsconnect::appDependencies()
```

## 🔄 更新部署

如果需要更新已部署的應用：

1. 更新 manifest.json：
```r
rsconnect::writeManifest(appPrimaryDoc = "full_app_v17.R")
```

2. 重新執行部署腳本（ShinyApps.io）

如果部署目標為 Posit Connect Cloud，更新後只需 `git push`，Connect 會自動部署。

## 📞 支援

如有問題，請參考：
- [ShinyApps.io 文件](https://docs.rstudio.com/shinyapps.io/)
- [Posit Connect 文件](https://docs.posit.co/connect/)
- 專案維護者：kyle-lin

最後更新：2024-01-15 
