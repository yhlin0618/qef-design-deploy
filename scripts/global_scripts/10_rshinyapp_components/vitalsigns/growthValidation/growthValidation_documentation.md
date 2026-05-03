# growthValidation — Growth Validation Component

**Issue**: [#416](https://github.com/kiki830621/ai_martech_global_scripts/issues/416)
**Source**: 向創建議_20260411 item #13
**Tier**: VitalSigns(Marketing Vital-Signs module)
**Last updated**: 2026-05-02

---

## Purpose

驗證導入儀表板後業績是否比品類平均成長快。回答客戶問題:

> 「我這個月成長 +X%,品類平均成長 +Y%。我比品類好(壞)Z 個百分點。」

---

## Data Source

| 來源 | 說明 |
|---|---|
| 表名 | `df_macro_monthly_summary` |
| 產生來源 | `D05_01_core` per-platform aggregate + `D05_01_finalize_category` cross-platform finalize |
| 分流 key | `platform_id` × `product_line_id_filter` |
| 必要欄位 | `total_revenue`, `mom_revenue_pct`, `yoy_revenue_pct`, `qoq_revenue_pct`, `category_revenue`, `category_mom_pct`, `category_yoy_pct`, `category_qoq_pct`, `excess_growth_mom`, `excess_growth_yoy`, `excess_growth_qoq` |

如果 `qoq_revenue_pct` / `category_*` / `excess_growth_*` 欄位缺失,component 顯示 empty-state「需 ≥ 24 個月歷史資料」並 log "Missing columns (run D05_01 + finalize)"。

---

## UI Layout(UI_R026 / UI_R028)

### `ui$filter`(sidebar layer ③)
- `radioButtons` period selector(YoY 預設 / MoM / QoQ)
- AI Insight button(底部,UX_P002)

### `ui$display`(main panel)
- **3 valueBox** 並排:
  - **品牌成長率**(藍 primary) — `<period>_revenue_pct`
  - **品類平均成長率**(灰 secondary) — `category_<period>_pct`
  - **超額成長率**(綠 success / 紅 danger) — `excess_growth_<period>`
- **Plotly bar chart** — 每月 excess_growth 時序,正值綠 / 負值紅
- **DT table** — 每月 brand_revenue / category_revenue / brand_pct / category_pct / excess_pp
- **AI Insight 結果**(底部)

---

## Period Definitions

| Period | Lag | 公式(brand,類比 category)|
|---|---|---|
| MoM | 1 month | `(current_month - prev_month) / prev_month * 100` |
| QoQ | 3 months | `(current_month - month_3_ago) / month_3_ago * 100`(parallel to YoY,non-calendar)|
| YoY | 12 months | `(current_month - same_month_last_year) / same_month_last_year * 100` |

QoQ 使用 lag-3 same-month 而非 calendar quarter sum,以維持與 MoM(lag-1)/ YoY(lag-12)一致的 per-month 唯一值,避免同一季 3 個月共享同一 QoQ 值的解讀困難。

---

## Decision Points(Plan-tier locked, 2026-05-02)

| Decision | Option | Locked As |
|---|---|---|
| 1.「品類」定義 | A: SUM total_revenue across platforms within same product_line | Locked |
| 2. excess_growth 公式 | A: brand_pct − category_pct(差值,pp) | Locked |
| 3. Period selector UI | A: radioButton + 3 valueBox showing active period | Locked |

完整 trade-off 見 [issue #416 Implementation Plan comment](https://github.com/kiki830621/ai_martech_global_scripts/issues/416)。

---

## Wiring(union_production_test.R)

| 階段 | Snippet |
|---|---|
| Source | `source_once("scripts/global_scripts/10_rshinyapp_components/vitalsigns/growthValidation/growthValidation.R")` |
| Sidebar menu | `bs4SidebarMenuItem(translate("Growth Validation"), tabName="growthValidation", icon=icon("trophy"))` 在 VitalSigns 區塊內 |
| TabItem | `bs4TabItem(tabName="growthValidation", uiOutput("growth_validation_display"))` |
| Component init | `growth_validation_comp <- growthValidationComponent("growth_validation", app_connection, comp_config, translate)` |
| dynamic_filter | `"growthValidation" = growth_validation_comp$ui$filter` |
| Display | `output$growth_validation_display <- renderUI2(current_tab=input$sidebar_menu, target_tab="growthValidation", ui_component=growth_validation_comp$ui$display, loading_icon="trophy")` |
| Lazy init | `growth_validation_res <- init_on_tab("growthValidation", function() growth_validation_comp$server(input, output, session))` |

---

## Translation Entries(`ui_terminology.csv`)

| English | Traditional Chinese |
|---|---|
| Growth Validation | 業績成長驗證(YoY/MoM/QoQ) |
| Brand Growth Rate | 品牌成長率 |
| Category Average Growth | 品類平均成長率 |
| Excess Growth Rate | 超額成長率 |
| Excess Growth Trend | 超額成長率趨勢 |
| Period: YoY / MoM / QoQ | 週期:年 / 月 / 季同期比 |
| Brand minus Category | 品牌減品類 |
| Need >= 24 months of historical data | 需 ≥ 24 個月歷史資料 |
| Latest month | 最新月份 |
| Growth Validation Detail | 業績成長驗證明細 |
| Brand Revenue | 品牌營收 |
| Category Revenue | 品類營收 |
| Brand | 品牌 |
| Category | 品類 |
| Excess | 超額 |
| Month | 月份 |

---

## Empty-state Behavior

- < 24 月歷史 → YoY 不可算 → valueBox 顯示 `-`,footer 顯示「需 ≥ 24 個月歷史資料」
- < 12 月歷史 → 連 MoM 趨勢圖也 sparse → plotly chart 顯示 placeholder
- 無 source data → 整個 component degrade 為 empty-state(同 customerAcquisition pattern)

不算為 bug — 為新公司 onboarding 早期合理狀態。

---

## AI Insight Prompt Key

`vitalsigns_analysis.growth_insights`(尚需在 `30_global_data/parameters/scd_type1/ai_prompts.yaml` 新增,當 AI 報告 prompt 確定後再加;目前缺則 button click 顯示「Prompt 未配置」)。

Template variables:
- `filter_context` — Platform / Product line / Period
- `latest_month` — 最新可算月份
- `brand_pct`, `category_pct`, `excess_pp` — 該月三個值
- `outperform_summary` — 全期間 outperform 月數 / 有效月數
- `underperform_count` — 全期間 underperform 月數

---

## Related Principles

- **MP064**: ETL/DRV separation(計算在 D05 衍生,UI 只讀)
- **MP029**: No fake data(reads real `df_macro_monthly_summary`)
- **UI_R001 / UI_R026 / UI_R028**: Component triple + sidebar 3-layer + filter exclusivity
- **DEV_R052**: Business logic uses English keys, UI translates
- **DEV_R051**: CSV download with UTF-8 BOM
- **UX_P002**: Progressive disclosure(AI insight 為 summary,放底部)

---

## Verification

```bash
# Unit tests for D05 growth rate helpers
cd <COMPANY>
Rscript scripts/global_scripts/98_test/general/test_D05_01_growth_rates.R

# Pipeline run (rebuild df_macro_monthly_summary with new columns)
cd scripts/update_scripts
make run TARGET=cbz_D05_01

# Live UI smoke test
cd <COMPANY>
Rscript app.R   # http://localhost:3838
# Login → 業績成長驗證 tab → verify valueBox + plotly + radioButton
```
