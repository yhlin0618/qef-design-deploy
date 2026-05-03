# MAMBA

MAMBA 精密汽車零件品牌,L4 Enterprise tier,跨平台電商(Cyberbiz 官網 + eBay + 部分 Amazon 測試)。

## Brand info

- **Display name**: MAMBA
- **L-tier**: l4_enterprise
- **Business domain**: 精密汽車零件(渦輪、排氣、引擎週邊)
- **Main product lines**: 跨品牌 aftermarket auto parts(多個 product line IDs)
- **Primary platforms**: `cbz`(Cyberbiz 官網), `eby`(eBay USA via MAMBATEK ERP), `amz`(Amazon 測試)
- **Dashboard URL**: Posit Connect Cloud (deploy repo: `kiki830621/ai_martech_l4_MAMBA_deploy`)

## Special environment

MAMBA 是 L4 tier 中環境最複雜的公司,跟一般 Cyberbiz-only 或 Amazon-only 的公司差別大:

- **Database backend**: **混合** — 本地開發用 DuckDB,Posit Connect production 用 Supabase PostgreSQL
- **External data sources**:
  - **Cyberbiz API**(官網 orders/sales/customers/products)— `.env` 中 `CBZ_API_TOKEN`
  - **eBay via MAMBATEK ERP**(MSSQL via SSH tunnel)— 最複雜的 integration
- **SSH tunnel**(for eBay ERP):
  - Jump host: `kylelin@220.128.138.146` (`.env`: `EBY_SSH_HOST`, `EBY_SSH_USER`, `EBY_SSH_PASSWORD`)
  - Target: `125.227.84.85:1433` MSSQL(ODBC Driver 18 for SQL Server)
  - Database: `MAMBATEK`
  - Local port forward: `1433 → 125.227.84.85:1433`
  - Helper: `02_db_utils/fn_ensure_tunnel.R`(本 namespace)
- **Required env vars**:
  - `OPENAI_API_KEY`, `APP_PASSWORD` (Shiny 登入)
  - `CBZ_API_TOKEN`
  - `EBY_SSH_*`, `EBY_SQL_*`(connect ERP)
  - `SUPABASE_DB_*`(Posit Connect production backend)
- **API credentials**: 本地 `.env`(`.gitignore` 排除),Posit Connect 用 Variable Set

## Known issues and lessons learned

| 日期 | 主題 | GitHub Issue | 筆記 |
|---|---|---|---|
| 2026-04-13 | MAMBA Supabase schema drift + cross-driver query layer fix | [#365](https://github.com/kiki830621/ai_martech_global_scripts/issues/365), [#371](https://github.com/kiki830621/ai_martech_global_scripts/issues/371) | `notes/2026-04-13_supabase_schema_drift.md`(待寫) |
| 2026-04-13 | eby BAYORE column_mapping 隱私風險 + 付款狀態 column 消失 | [#373](https://github.com/kiki830621/ai_martech_global_scripts/issues/373) | `notes/2026-04-13_bayore_dictionary.md`(待寫) |
| 2026-03-03 | BG/NBD P(alive) integration | [#211](https://github.com/kiki830621/ai_martech_global_scripts/issues/211) | `notes/2026-03-03_btyd_palive.md`(待寫) |

## File overview

### `02_db_utils/`

`fn_ensure_tunnel.R` — 確保 SSH tunnel 到 `220.128.138.146` 建立後再連 MSSQL。從根層 `02_db_utils/fn_ensure_mamba_tunnel.R` 搬遷(#371 unify-cross-driver-query-layer 之後的 follow-up refactor)。任何公司需要 SSH tunnel to MSSQL 可 copy + adapt SSH host/port/port-forward。

`fn_ensure_tunnel_enhanced.R` — 同上的 enhanced 版,有 retry + better error reporting。

### `notes/`

Chronological lessons-learned。新 notes 用 `YYYY-MM-DD_topic.md` 命名。目前內容見 "Known issues" 表格。

## 參考時請注意

**這個 namespace 的檔案是 read-and-adapt,不是 source()**。

- 其他公司(例如 D_RACING 之後也接 MSSQL)要跟進時,**複製**檔案到自己 namespace(e.g., `29_company_examples/d_racing/02_db_utils/fn_ensure_tunnel.R`),不是直接 `source("29_company_examples/mamba/...")`
- 當 2 家公司都有類似 helper 時,走 **rule of three** promote 到 generic chapter(`02_db_utils/`),兩個 namespace 的 copy 都改 reference 新 generic location

詳見 [chapter 29 README](../README.md) 的「Cookbook vs Library」段落。

## Related

- [#211 BTYD P(alive)](https://github.com/kiki830621/ai_martech_global_scripts/issues/211)
- [#365 MAMBA Posit Connect PostgreSQL failure (original)](https://github.com/kiki830621/ai_martech_global_scripts/issues/365)
- [#369 unify-cross-driver-query-layer Spectra change](https://github.com/kiki830621/ai_martech_global_scripts/issues/369)
- [#371 MAMBA Supabase schema drift](https://github.com/kiki830621/ai_martech_global_scripts/issues/371)
- [#373 eby column_mapping audit](https://github.com/kiki830621/ai_martech_global_scripts/issues/373)
