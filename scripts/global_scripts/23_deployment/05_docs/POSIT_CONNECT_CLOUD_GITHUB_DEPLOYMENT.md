# Posit Connect Cloud 部署指南（GitHub 方式）

根據官方文檔，Posit Connect Cloud 使用 GitHub 整合進行部署。首次設定完成後，部署會自動由 Git 觸發，不需要手動 republish。

## 📋 部署前準備

### 1. 確認檔案結構
```
{app_name}/
├── app.R              # 主應用程式（必須）
├── manifest.json      # 依賴清單（必須）
├── data/              # 資料檔案
├── www/               # 靜態資源
├── icons/             # 圖標
└── scripts/           # 相關腳本（真實檔案，非 symlinks）
```

### 2. 創建 manifest.json
```r
# 在應用目錄下執行
library(rsconnect)
rsconnect::writeManifest()
```

這會創建 `manifest.json` 檔案，記錄：
- R 版本
- 所需套件及版本

## 🏗️ 部署架構：Dual-Repo Pattern

### 為什麼需要 Dual-Repo？

本專案使用 **symlink 架構**（MP122）：開發時公司專案透過 symlink 引用 `shared/` 中的共用程式碼。但 Posit Connect Cloud 從 GitHub clone repo 時**無法解析 symlinks**，因此需要一個獨立的 deploy repo 存放解析後的真實檔案。

### 架構概覽

```
Dev repo (symlinks)           Deploy repo (real files)
ai_martech_l4_QEF_DESIGN     ai_martech_l4_QEF_DESIGN_deploy
├── scripts/                  ├── scripts/
│   ├── global_scripts -> ../../shared/...  │   ├── global_scripts/   ← 真實檔案
│   └── update_scripts -> ../../shared/...  │   └── update_scripts/   ← 真實檔案
└── app.R                     └── app.R
        │                             │
        └── make deploy-sync ────────→┘
              (rsync -avL)
```

### 工作流程

```bash
# 日常部署（在 dev repo 根目錄）
make deploy-sync    # rsync -avL 解析 symlinks → deploy repo
make deploy-diff    # 檢查變更內容
make deploy-push    # commit + push → 自動觸發 Posit Connect 部署

# 首次設定
make deploy-init    # 建立 deploy repo、設定 .gitignore、連結 GitHub remote
```

## 🚀 首次部署步驟

### 步驟 1：準備 Deploy Repo

```bash
# 在 dev repo 根目錄
make deploy-init    # 建立 deployment/{app-name}/ 並初始化 git
make deploy-sync    # 首次同步所有檔案
make deploy-push    # 推送到 GitHub
```

### 步驟 2：在 Posit Connect Cloud 設定

1. 登入 [Posit Connect Cloud](https://connect.posit.cloud)
2. 點擊頁面頂部的 **Publish** 按鈕
3. 選擇 **Shiny**
4. 選擇 **deploy repo**（名稱含 `_deploy` 後綴）
5. 確認分支（通常是 `main`）
6. 選擇 **app.R** 作為主要檔案
7. 點擊 **Publish**
8. 確認已啟用 Git 變更自動部署

> **Note**: Posit Connect Cloud 支援 **private repositories**。不需要將 repo 設為公開。

### 步驟 3：監控部署

- 部署過程中會顯示狀態更新
- 底部會顯示建構日誌
- 部署完成後會獲得應用程式連結

## 📝 重要注意事項

### manifest.json 必須包含在 Git 中
修改 `.gitignore`，確保 `manifest.json` **不被**排除：
```bash
# 在 .gitignore 中，移除或註解這行
# manifest.json
```

### 確保 app.R 是主檔案
Deploy repo 的根目錄必須有 `app.R`。

### Private Repos 完全支援
Posit Connect Cloud 透過 GitHub OAuth 整合，可以存取你授權的 private repositories。不再需要將 repo 設為公開。

## 🔄 更新應用程式

日常更新流程：

```bash
# 1. 在 dev repo 完成程式碼變更
# 2. 同步到 deploy repo
make deploy-sync

# 3. 檢查變更
make deploy-diff

# 4. 推送（自動觸發重新部署）
make deploy-push
```

Posit Connect Cloud 會自動偵測 Git push 並重新部署（不需要手動點 republish）。

## 🐛 疑難排解

### 問題：找不到 app.R
確保 deploy repo 根目錄有 `app.R`，且 `make deploy-sync` 有正確同步。

### 問題：找不到 scripts/ 中的模組
很可能 deploy repo 中仍有 symlinks 而非真實檔案。確認使用 `rsync -avL`（`-L` flag 解析 symlinks）。

```bash
# 檢查 deploy repo 中是否有殘留 symlinks
find deployment/{app-name}/ -type l
```

### 問題：套件版本衝突
重新生成 manifest.json：
```r
# 刪除舊的
file.remove("manifest.json")

# 創建新的
rsconnect::writeManifest()
```

### 問題：deploy-sync 後檔案不完整
```bash
# 手動執行 rsync 觀察同步過程
rsync -avL --dry-run . deployment/{app-name}/ --exclude='.git' --exclude='deployment/'
```

## 📦 Deploy Repo .gitignore 範本

```gitignore
# R 相關
.Rproj.user
.Rhistory
.RData
.Ruserdata

# 環境變數（保持私密）
.env
.env.*

# 資料檔案（視需求）
*.csv
*.xlsx
*.sqlite
*.duckdb

# 但不要排除 manifest.json！
# manifest.json  <- 確保這行被註解或移除

# 暫存
cache/
temp/
```

## 🎯 完整檢查清單

部署前確認：
- [ ] `app.R` 存在於 deploy repo 根目錄
- [ ] `manifest.json` 已創建且是最新的
- [ ] 所有必要的資料檔案都已包含
- [ ] `.gitignore` 沒有排除 `manifest.json`
- [ ] Deploy repo 中無殘留 symlinks（`find . -type l` 確認）
- [ ] `make deploy-sync` 已執行
- [ ] 所有變更都已提交並推送

## 🔗 相關原則

- **MP122**: Quad-Track Shared Symlink Architecture（Section 13: Deployment）
- **TD_P001**: Deployment Patterns
- **SO_P016**: Configuration Scope Hierarchy

---
最後更新：2026-03-04
