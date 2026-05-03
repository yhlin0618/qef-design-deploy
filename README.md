# QEF DESIGN — Posit Connect Deploy

L4 Enterprise Shiny app bundle for **向創設計（QEF DESIGN）**.

## Deploy

Posit Connect Cloud reads this repo and starts Shiny via `app.R`, which sources the
union file `scripts/global_scripts/10_rshinyapp_components/unions/union_production_test.R`.

### Required environment variables (Connect → Content → Variables)

| Name | Purpose | Secret |
|------|---------|--------|
| `SUPABASE_URL` | `https://<project_ref>.supabase.co` | ⬜ |
| `SUPABASE_ANON_KEY` | Supabase anon JWT — used by login RPC | ✅ |
| `SUPABASE_DB_HOST` | `db.<project_ref>.supabase.co` | ⬜ |
| `SUPABASE_DB_PORT` | `5432` | ⬜ |
| `SUPABASE_DB_USER` | `postgres` | ⬜ |
| `SUPABASE_DB_PASSWORD` | Postgres password | ✅ |
| `SUPABASE_DB_NAME` | `postgres` | ⬜ |
| `OPENAI_API_KEY` | OpenAI key | ✅ |
| `APP_PASSWORD` | Optional — login password override | ✅ |

## Notes

- `app_config.yaml` 裡 `database.mode: supabase` + `startup.allow_missing_metadata: true`，
  允許在 ETL 完成前先讓 UI 啟動並走 stub 資料。等 Supabase 上 `df_platform` /
  `df_product_line_profile` 等表都建好後，把 `allow_missing_metadata` 改回 `false`。
- 登入採 Supabase RPC (`verify_password`) — 預設 admin / admin123。
