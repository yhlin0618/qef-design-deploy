# Posit Connect Cloud 完整部署指南

## 🎯 部署概覽

Posit Connect Cloud 使用 **GitHub 整合 + 環境變數** 的方式部署需要敏感資訊的 Shiny 應用。

## 📋 部署前檢查清單

- [ ] `app.R` 存在且是最新版本
- [ ] `manifest.json` 已創建
- [ ] `.gitignore` 包含 `.env`、`.Renviron` 和其他敏感檔案
- [ ] 所有敏感資訊都透過 `Sys.getenv()` 讀取
- [ ] GitHub repository 是公開的（或您有 Posit Connect Cloud Pro）

## 🚀 完整部署流程

### 第 1 步：準備檔案

```bash
# 執行準備腳本
Rscript prepare_for_posit_connect.R

# 這會：
# - 確認 app.R 存在
# - 確認 manifest.json 存在
# - 修正 .gitignore（不排除 manifest.json）
```

### 第 2 步：準備環境變數清單

```bash
# 執行環境變數準備工具
Rscript prepare_env_for_posit_connect.R

# 這會列出所有需要在 Posit Connect Cloud 設定的環境變數
```

### 第 3 步：提交到 GitHub

```bash
# 確認敏感檔案不會被提交
git status

# 添加檔案（排除 .env 和 .Renviron）
git add .
git commit -m "Prepare for Posit Connect Cloud deployment"
git push origin main
```

### 第 4 步：在 Posit Connect Cloud 部署（首次設定）

1. 登入 https://connect.posit.cloud

2. 點擊 **Publish** → **Shiny**

3. 選擇您的 GitHub repository

4. 選擇分支和 **app.R**

5. **重要**：點擊 **Advanced settings**

6. 在 **Configure variables** 區域添加環境變數：

   **PostgreSQL 變數**：
   - `PGHOST` → `db-postgresql-sgp1-73173-do-user-18877526-0.f.db.ondigitalocean.com`
   - `PGPORT` → `25060`
   - `PGUSER` → `doadmin`
   - `PGPASSWORD` → （從 .env 複製）
   - `PGDATABASE` → `positioning`
   - `PGSSLMODE` → `require`

   **OpenAI 變數**：
   - `OPENAI_API_KEY` → （從 .env 複製）
   - `OPENAI_API_KEY_LIN` → （從 .env 複製）

7. 點擊 **Publish**
8. 確認已啟用 Git 變更自動部署（名稱依 UI 顯示為準）

## 🔄 更新應用程式

### 程式碼更新
```bash
# 修改程式碼後
git add .
git commit -m "Update application"
git push
```
推送後會自動觸發部署，不需要手動 republish。

### 環境變數更新
1. 在 Content List 找到應用
2. 進入設定
3. 更新環境變數
4. 重新部署

## 🐛 常見問題

### 問題：找不到環境變數
**解決**：確認使用 `Sys.getenv("VARIABLE_NAME")` 而非其他方式讀取

### 問題：資料庫連線失敗
**解決**：
- 檢查所有 PG 開頭的變數都已設定
- 確認密碼沒有特殊字元問題
- 確認資料庫允許外部連線

### 問題：OpenAI API 錯誤
**解決**：
- 確認 API Key 有效
- 檢查是否有額度限制
- 使用正確的環境變數名稱

## 📦 檔案結構確認

```
positioning_app/
├── app.R                    ✅ 必須
├── manifest.json            ✅ 必須（不在 .gitignore）
├── .env                     ❌ 不要提交
├── .Renviron               ❌ 不要提交
├── .gitignore              ✅ 必須
├── data/                   ✅ 應用資料
├── www/                    ✅ 靜態資源
└── scripts/                ✅ 相關腳本
```

## 🔐 安全最佳實踐

1. **分離環境**：開發和生產使用不同的資料庫/API Key
2. **定期輪換**：定期更新密碼和 API Key
3. **最小權限**：資料庫用戶只給必要權限
4. **監控使用**：定期檢查 API 使用量和資料庫存取日誌

## 📚 相關文件

- `POSIT_CONNECT_CLOUD_GITHUB_DEPLOYMENT.md` - GitHub 部署詳細說明
- `POSIT_CONNECT_ENV_VARIABLES.md` - 環境變數處理指南
- [官方文檔](https://docs.posit.co/connect-cloud/)

---
最後更新：2024-01-15 
