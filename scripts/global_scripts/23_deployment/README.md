# 部署工具集

本目錄包含通用的 Shiny 應用程式部署工具，可用於任何應用程式。

## 目錄結構

- **01_checks/** - 部署前檢查和診斷工具
- **02_setup/** - 環境設置和準備工具  
- **03_deploy/** - 部署執行腳本
- **04_update/** - 應用程式更新管理
- **05_docs/** - 部署相關文檔
- **06_env/** - 環境變數管理工具

## 🚀 快速開始

### 方法 1：一鍵部署（推薦）

使用配置檔案驅動的部署：

1. **在您的應用程式目錄創建 `app_config.yaml`**：
   ```bash
   cp scripts/global_scripts/23_deployment/app_config_template.yaml app_config.yaml
   ```

2. **編輯配置檔案**

3. **執行部署**：
   ```bash
   Rscript scripts/global_scripts/23_deployment/sc_deployment_config.R
   ```

詳細說明請參考 [配置驅動部署指南](README_CONFIG.md)

> **注意**：Posit Connect Cloud 一律使用 GitHub 整合部署，推送到 Git 後自動部署；此目錄的腳本主要用於部署前檢查與 ShinyApps.io 流程。

### 方法 2：直接使用工具

1. **檢查部署狀態**：
   ```r
   source("01_checks/check_deployment_improved.R")
   ```

2. **更新應用程式**：
   ```bash
   ./04_update/update_app.sh --latest
   ```

3. **部署應用程式**：
   ```r
   source("03_deploy/deploy_app.R")
   ```

## ⚠️ 常見問題

### 語法錯誤警告
如果看到 archive 目錄中的語法錯誤，這是正常的。部署腳本已配置為：
- 使用 `.renvignore` 排除這些目錄
- 自動過濾問題檔案

### 路徑偵測問題
部署腳本包含智能路徑偵測，可從任何目錄執行。

## 📚 相關文檔

- [配置驅動部署系統](README_CONFIG.md) - **推薦使用**
- [完整部署指南](05_docs/COMPLETE_DEPLOYMENT_GUIDE.md)
- [Posit Connect Cloud 部署](05_docs/POSIT_CONNECT_CLOUD_GITHUB_DEPLOYMENT.md)
