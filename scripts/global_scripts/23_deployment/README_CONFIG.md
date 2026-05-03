# 配置驅動的部署系統

## 概述

這個部署系統允許每個應用程式使用自己的 `app_config.yaml` 檔案來定義應用程式配置和部署參數，避免需要修改部署腳本。

## 🚀 快速開始

1. **複製配置模板到您的應用程式目錄**：
   ```bash
   cp scripts/global_scripts/23_deployment/app_config_template.yaml app_config.yaml
   ```

2. **編輯 `app_config.yaml`** 來符合您的應用程式

3. **執行部署**：
   ```bash
   # 互動式部署
   Rscript scripts/global_scripts/23_deployment/sc_deployment_config.R
   
   # 自動模式
   Rscript scripts/global_scripts/23_deployment/sc_deployment_config.R --auto
   ```

## 📁 配置檔案結構

```yaml
# 基本應用程式資訊
app_info:
  name: "Your App Name"              # 應用程式名稱
  description: "Brief description"   # 簡短描述
  version: "1.0.0"                  # 版本號
  language: "en_US"                 # 語言設定

# UI/主題設定（可選）
theme:
  version: 5
  bootswatch: cosmo

# 資料設定（可選）
data:
  raw_data_dir: "../data"
  cache_dir: "./cache"

# 部署配置
deployment:
  target: "connect"                 # 部署目標: "shinyapps" 或 "connect"
  
  github_repo: "owner/repo"          # GitHub repository
  app_path: "path/to/app"           # 從 repo 根目錄到 app 的路徑
  main_file: "app.R"                # 主要的 R 檔案
  branch: "main"                    # Git 分支
  
  # ShinyApps.io 設定（當 target=shinyapps）
  shinyapps:
    account: "your-account"          # ShinyApps.io 帳號
    app_name: "app-name"            # 應用名稱
  
  version_files:                    # 可選：版本檔案列表
    - "app_v2.R"
    - "app_v1.R"
  
  required_files:                   # 部署前要檢查的檔案
    - "app.R"
    - "manifest.json"
  
  required_dirs:                    # 部署前要檢查的目錄
    - "www"
    - "data"
  
  env_vars:                        # 需要的環境變數（只列名稱）
    - "DATABASE_URL"
    - "API_KEY"
  
  special_instructions: |          # 特殊說明
    Any special notes for deployment

# 組件配置（可選）
components:
  # 定義應用程式組件
```

## 🔧 進階用法

### 使用不同的配置檔案

```bash
# 使用自訂配置檔案
Rscript scripts/global_scripts/23_deployment/sc_deployment_config.R my_config.yaml

# 使用自訂配置檔案 + 自動模式
Rscript scripts/global_scripts/23_deployment/sc_deployment_config.R my_config.yaml --auto
```

### 不同應用程式的配置範例

#### VitalSigns 範例
```yaml
app_info:
  name: "VitalSigns"
  description: "Health monitoring dashboard"
  version: "2.0.0"
  
deployment:
  target: "shinyapps"  # 部署到 ShinyApps.io
  
  github_repo: "kiki830621/ai_martech"
  app_path: "l1_basic/VitalSigns"
  main_file: "app.R"
  branch: "main"
  
  shinyapps:
    account: "my-shinyapps-account"
    app_name: "vitalsigns-dashboard"
  
  required_dirs:
    - "www"
    - "modules"
    - "data"
```

#### InsightForge 範例
```yaml
app_info:
  name: "InsightForge"
  description: "Data insights application"
  version: "1.5.0"
  
deployment:
  github_repo: "kiki830621/ai_martech"
  app_path: "l1_basic/InsightForge"
  main_file: "app.R"
  branch: "main"
  
  env_vars:
    - "DB_CONNECTION_STRING"
    - "INSIGHT_API_KEY"
```

## 📋 部署流程

1. **載入配置**：讀取 `deployment_config.yaml`
2. **確認配置**：顯示部署參數供確認
3. **檢查主檔案**：確保 app.R 存在（或從版本檔案創建）
4. **更新 manifest.json**：更新套件依賴清單
5. **檢查檔案和目錄**：確認所有必要項目存在
6. **環境變數提醒**：列出需要設定的環境變數
7. **顯示部署指示**：提供詳細的部署步驟

## ✨ 優點

- **可重用**：同一個腳本可用於所有應用程式
- **易維護**：配置與程式碼分離
- **版本控制**：每個 app 的配置可以獨立管理
- **團隊協作**：清楚記錄每個 app 的部署需求

## 🔍 故障排除

### 找不到 yaml 套件
```r
install.packages("yaml")
```

### 配置檔案格式錯誤
確保 YAML 格式正確，特別注意縮排（使用空格，不要用 Tab）

### 路徑問題
- `app_path` 應該是從 Git repository 根目錄開始的相對路徑
- 不要以 `/` 開頭
- 使用正斜線 `/`，不要用反斜線 `\` 