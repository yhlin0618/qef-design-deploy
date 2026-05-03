# Posit Connect Cloud 部署指南（Git 自動部署）

## 🎯 原則

- 一律使用 GitHub 整合部署
- 推送到指定分支會自動觸發部署，不需要手動 republish
- 不使用 rsconnect API Key 的手動部署流程

## 🚀 快速開始

1. 確認 `app.R` 與 `manifest.json` 已存在且最新
2. 將應用程式推送到 GitHub
3. 在 Posit Connect Cloud 完成首次 Publish（一次性設定）
4. 在 Advanced settings 設定環境變數
5. 後續更新只需 `git push`，部署會自動觸發

## 📋 必要檔案

- `app.R`
- `manifest.json`
- `.gitignore`（不可排除 `manifest.json`，必須排除 `.env` / `.Renviron`）

## 🔄 更新流程（自動）

```bash
git add .
git commit -m "Update app"
git push
```

Connect 會自動建置並部署最新版本。

## 🐛 疑難排解

### 問題 1：自動部署未觸發

- 確認 push 到正確分支（如 `main`）
- 在 Connect 應用設定確認 Git 自動部署已啟用
- 檢查 Content List 的建置日誌

### 問題 2：找不到 app.R 或 manifest.json

- 確認檔案在 repo 指定的 app path 內
- 重新生成 manifest.json：`rsconnect::writeManifest()`

### 問題 3：敏感資訊暴露

- 確認 `.env` / `.Renviron` 未提交
- 使用 `Sys.getenv()` 讀取環境變數

## 📚 相關文件

- `POSIT_CONNECT_CLOUD_GITHUB_DEPLOYMENT.md`
- `POSIT_CONNECT_ENV_VARIABLES.md`

---
最後更新：2024-01-15
