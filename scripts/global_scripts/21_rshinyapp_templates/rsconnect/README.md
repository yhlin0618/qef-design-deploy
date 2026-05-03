# 部署腳本使用指南

## 使用原則

**每個 app 應該在自己的根目錄下執行部署**

## 使用方式

### 1. 進入 app 目錄
```bash
cd l1_basic/positioning_app
```

### 2. 設定環境變數（如果需要）
確保 `.env` 檔案包含必要的設定：
```bash
# 部署到 Posit Connect（預設）
CONNECT_SERVER=https://your-connect-server.com
CONNECT_API_KEY=your-api-key

# 或部署到 ShinyApps.io
DEPLOY_TARGET=shinyapps
SHINYAPPS_ACCOUNT=your-account
```

### 3. 執行部署
```bash
# 從 app 目錄執行
Rscript scripts/global_scripts/21_rshinyapp_templates/rsconnect/deployment_positioning_app.R
```

## 範例

### 部署 positioning_app
```bash
cd l1_basic/positioning_app
Rscript scripts/global_scripts/21_rshinyapp_templates/rsconnect/deployment_positioning_app.R
```

### 部署 VitalSigns
```bash
cd l1_basic/VitalSigns
Rscript scripts/global_scripts/21_rshinyapp_templates/rsconnect/deployment_VitalSigns.R
```

### 部署 InsightForge
```bash
cd l1_basic/InsightForge
Rscript scripts/global_scripts/21_rshinyapp_templates/rsconnect/deployment_InsightForge.R
```

## 優點

1. **清晰的工作目錄**：每個 app 在自己的目錄下部署
2. **獨立的環境設定**：每個 app 有自己的 `.env`
3. **簡單的路徑**：不需要記住複雜的相對路徑

## 注意事項

- 腳本會驗證你是否在正確的 app 目錄
- 如果不在正確目錄，會顯示錯誤訊息並指導如何修正
- `.env` 檔案會從當前目錄（app 根目錄）載入 