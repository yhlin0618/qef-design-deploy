# =============================================================
# union_production_test.R
# 2025‑04‑19 – 更新為使用 tbl2 進行資料存取 (R116 Enhanced Data Access)
# 2025‑04‑17 – 清理結構 + 動態 Filter 區塊 (dynamic_filter)
# 2025-04-24 – 修正 Platform 選項處理邏輯 (按照 MP081 原則)
# 2025-05-16 – 新增 positionTable 組件 (按照 MP56、MP081 原則)
# 2025-05-22 – 新增 positionDNAPlotly、positionCSAPlotly、positionKFE、positionIdealRate、positionStrategy、microMacroKPI 組件
# =============================================================

# IMPORTANT CODE MAINTENANCE NOTES:
# 1. radioButtons parameters MUST be named explicitly (inputId, label, choices, selected) per MP081
# 2. Platform switch handling MUST use consistent variable names throughout the observer
# 3. Any variables used in notification must also be updated in the logging section
# 4. Currently supported platforms follow app_configs$platforms plus "all"
# 5. Position components include table, DNA plotly, CSA plotly, and KFE analysis
# =============================================================

# ---- 0. 初始化 ------------------------------------------------------------
# Disable all package installation prompts and auto-answer y/n questions
options(install.packages.ask = FALSE)
options(repos = c(CRAN = "https://cran.rstudio.com/"))
options(menu.graphics = FALSE)
# Auto-answer all y/n prompts with "y"
options(pkgType = "binary")
# Set environment variable to avoid interactive prompts
Sys.setenv(R_INSTALL_STAGED = FALSE)
# Force non-interactive mode
options(warn = 1)  # Show warnings immediately but don't stop

# Restore project root: shiny::runApp() changes wd to the file's directory,
# but autoinit() needs the company project root (e.g., D_RACING/, MAMBA/).
# app.R saves the correct root in PROJECT_ROOT env var before launching.
if (nzchar(Sys.getenv("PROJECT_ROOT"))) {
  setwd(Sys.getenv("PROJECT_ROOT"))
}
message("Working directory: ", getwd())


# Union files live under global_scripts/ but are Shiny Apps — force APP_MODE
OPERATION_MODE <- "APP_MODE"
autoinit()

# Async AI insight support (GUIDE03: ExtendedTask for >5s operations)
# TD_P004 compliant: single worker, NOT furrr batch parallel
library(future)
future::plan(future::multisession, workers = 1)

# Ensure app_configs and df_platform are available before UI defaults
if (!exists("app_configs", inherits = TRUE)) {
  if (!requireNamespace("yaml", quietly = TRUE)) {
    stop("Please install the 'yaml' package to read app_config.yaml")
  }
  app_configs <- yaml::read_yaml("app_config.yaml")
}
if (is.null(app_configs$platforms)) {
  app_configs$platforms <- list()
}

# DM_R054 v2.1 (hotfix 2026-04-20, #424):
# Runtime reads metadata from the canonical source selected by
# app_config.yaml > database.mode. In local dev (`duckdb`) this is
# `meta_data.duckdb`; in Posit Connect (`supabase`) this is the live Supabase
# PostgreSQL. There is still NO CSV fallback at runtime (§6); CSV is
# bootstrap seed only, consumed by `all_ETL_meta_init_0IM.R` (DuckDB mode)
# or by the equivalent ETL that populates Supabase.

if (!exists("df_platform", inherits = TRUE) ||
    !exists("df_product_line", inherits = TRUE)) {
  # Delegate to dbConnectAppData (MP142, DM_R023) so mode selection is
  # configuration-driven and a single connection serves both reads.
  .meta_app_con <- dbConnectAppData(verbose = TRUE)
  .meta_conn_type <- attr(.meta_app_con, "connection_type")
  .meta_existing <- try(DBI::dbListTables(.meta_app_con), silent = TRUE)

  if (inherits(.meta_existing, "try-error")) {
    try(DBI::dbDisconnect(.meta_app_con), silent = TRUE)
    stop("Shiny startup: could not list tables on canonical metadata backend ",
         "(connection_type=",
         if (is.null(.meta_conn_type)) "unknown" else .meta_conn_type,
         "). Check database.mode in app_config.yaml and that the metadata ",
         "source (meta_data.duckdb or Supabase) is reachable.",
         call. = FALSE)
  }

  if (!exists("df_platform", inherits = TRUE)) {
    if (!("df_platform" %in% .meta_existing)) {
      try(DBI::dbDisconnect(.meta_app_con), silent = TRUE)
      stop("Shiny startup: df_platform not found on canonical metadata backend ",
           "(connection_type=",
           if (is.null(.meta_conn_type)) "unknown" else .meta_conn_type,
           "). For DuckDB mode, run `all_ETL_meta_init_0IM.R` to populate ",
           "meta_data.duckdb. For Supabase mode, run the deployment ETL that ",
           "populates public.df_platform.",
           call. = FALSE)
    }
    df_platform <- DBI::dbReadTable(.meta_app_con, "df_platform")
  }

  if (!exists("df_product_line", inherits = TRUE)) {
    # Route through the canonical reader helper so schema validation + logging
    # stay centralized. Pass the active connection down via `mode="auto"` and
    # a NULL meta_data_path (Supabase fallback path inside load_product_lines).
    .scratch_con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
    df_product_line <- load_product_lines(
      conn           = .scratch_con,
      meta_data_path = if (exists("db_path_list", inherits = TRUE))
                         db_path_list$meta_data else NULL,
      mode           = "auto"
    )
    try(DBI::dbDisconnect(.scratch_con), silent = TRUE)
  }

  try(DBI::dbDisconnect(.meta_app_con), silent = TRUE)
  rm(.meta_app_con, .meta_conn_type, .meta_existing)
}

# R68: Object Initialization - Ensure required variables exist
if (!exists("PACKAGES_INITIALIZED")) {
  PACKAGES_INITIALIZED <- list(status = "minimal")
}

# UX_P001: Use source_once() to prevent duplicate loading if autoinit already sourced
# Load password-only login module
source_once("scripts/global_scripts/10_rshinyapp_components/passwordOnly/passwordOnlyUI.R")
source_once("scripts/global_scripts/10_rshinyapp_components/passwordOnly/passwordOnlyServer.R")

# Load report integration module - Following R09: UI-Server-Defaults triple
# MP56: Connected Component Principle - Enable cross-module data sharing
source_once("scripts/global_scripts/10_rshinyapp_components/report/reportIntegration/reportIntegration.R")

# Load Dashboard Overview component
source_once("scripts/global_scripts/10_rshinyapp_components/macro/dashboardOverview/dashboardOverview.R")

# Shared chart utilities — DP_R001 histogram winsorization (MP032 DRY)
source_once("scripts/global_scripts/11_rshinyapp_utils/fn_render_histogram.R")

# Load TagPilot components
source_once("scripts/global_scripts/10_rshinyapp_components/tagpilot/fn_rsv_classification.R")
source_once("scripts/global_scripts/10_rshinyapp_components/tagpilot/fn_dna_marketing_utils.R")
source_once("scripts/global_scripts/10_rshinyapp_components/tagpilot/customerValue/customerValue.R")
source_once("scripts/global_scripts/10_rshinyapp_components/tagpilot/customerActivity/customerActivity.R")
source_once("scripts/global_scripts/10_rshinyapp_components/tagpilot/customerStatus/customerStatus.R")
source_once("scripts/global_scripts/10_rshinyapp_components/tagpilot/customerStructure/customerStructure.R")
source_once("scripts/global_scripts/10_rshinyapp_components/tagpilot/customerLifecycle/customerLifecycle.R")
source_once("scripts/global_scripts/10_rshinyapp_components/tagpilot/rsvMatrix/rsvMatrix.R")
source_once("scripts/global_scripts/10_rshinyapp_components/tagpilot/marketingDecision/marketingDecision.R")
source_once("scripts/global_scripts/10_rshinyapp_components/tagpilot/customerExport/customerExport.R")
source_once("scripts/global_scripts/10_rshinyapp_components/tagpilot/comprehensiveDiagnosis/comprehensiveDiagnosis.R")

# Load VitalSigns components (Phase 2: Revenue Pulse, Engagement, Retention, Acquisition)
source_once("scripts/global_scripts/10_rshinyapp_components/vitalsigns/revenuePulse/revenuePulse.R")
source_once("scripts/global_scripts/10_rshinyapp_components/vitalsigns/customerEngagement/customerEngagement.R")
source_once("scripts/global_scripts/10_rshinyapp_components/vitalsigns/customerRetention/customerRetention.R")
source_once("scripts/global_scripts/10_rshinyapp_components/vitalsigns/customerAcquisition/customerAcquisition.R")
source_once("scripts/global_scripts/04_utils/fn_country_name_zh.R")  # #330: Chinese country names for worldMap
source_once("scripts/global_scripts/10_rshinyapp_components/vitalsigns/worldMap/worldMap.R")
source_once("scripts/global_scripts/10_rshinyapp_components/vitalsigns/comprehensiveDiagnosis/comprehensiveDiagnosis.R")
source_once("scripts/global_scripts/10_rshinyapp_components/vitalsigns/macroTrends/macroTrends.R")
source_once("scripts/global_scripts/10_rshinyapp_components/vitalsigns/growthValidation/growthValidation.R")  # #416

# Load micro components
source_once("scripts/global_scripts/10_rshinyapp_components/micro/microCustomer/microCustomer2.R")

# Load position components
source_once("scripts/global_scripts/10_rshinyapp_components/position/positionTable/positionTable.R")
source_once("scripts/global_scripts/10_rshinyapp_components/position/positionDNAPlotly/positionDNAPlotly.R")
source_once("scripts/global_scripts/10_rshinyapp_components/position/positionMSPlotly/positionMSPlotly.R")
source_once("scripts/global_scripts/10_rshinyapp_components/position/positionKFE/positionKFE.R")
source_once("scripts/global_scripts/10_rshinyapp_components/position/positionIdealRate/positionIdealRate.R")
source_once("scripts/global_scripts/10_rshinyapp_components/position/positionStrategy/positionStrategy.R")

# Load poisson components
source_once("scripts/global_scripts/10_rshinyapp_components/poisson/poissonTimeAnalysis/poissonTimeAnalysis.R")
source_once("scripts/global_scripts/10_rshinyapp_components/poisson/poissonFeatureAnalysis/poissonFeatureAnalysis.R")
source_once("scripts/global_scripts/10_rshinyapp_components/poisson/poissonCommentAnalysis/poissonCommentAnalysis.R")


# ---- 2. 連線 --------------------------------------------------------------
# Database connection will be established inside server function to avoid lifecycle issues


# ---- 3. Component 載入 ----------------------------------------------------
# 確保載入 position 相關元件
# 這些都會initialize不需要額外載入
# 翻譯函數 - 設為全域以確保所有模組都能存取
# ---- Translation System Initialization (UI_R022) ----
translate <<- function(x) x   # Safe fallback
tryCatch({
  source_once(file.path("scripts", "global_scripts", "04_utils", "fn_translation.R"))
  source_once(file.path("scripts", "global_scripts", "04_utils", "fn_initialize_ui_translation.R"))
  source_once(file.path("scripts", "global_scripts", "04_utils", "fn_get_language_scope.R"))
  ui_lang <- get_language_scope("ui_text")
  translate <<- initialize_ui_translation(language = ui_lang)
  message("[Translation] Initialized: ui_text=", ui_lang)
}, error = function(e) {
  message("[Translation] Init failed, using identity: ", e$message)
})

# ---- 3.5 Resource Path Setup (重要：必須在 UI 定義前) ---------------------
# MP119: UI Block Separation Principle - External CSS file organization
# R03: Error Handling - Robust path detection for deployment environments
# MP47: Functional Programming - Safe project root detection

# Set up resource paths for static assets
if (dir.exists("scripts/global_scripts/24_assets")) {
  addResourcePath("assets", "scripts/global_scripts/24_assets")
}
if (dir.exists("www")) {
  addResourcePath("www", "www")
}

# Add resource path for CSS files (following MP119 and UI_R014)
# CSS files are centralized in 19_CSS directory
css_path <- "scripts/global_scripts/19_CSS"
if (dir.exists(css_path)) {
  addResourcePath("css", css_path)
  message("Resource path 'css' added for: ", css_path)
} else {
  warning("CSS directory not found at: ", css_path)
}

# Ensure tags resolves to htmltools before UI assembly.
if (!exists("tags", inherits = TRUE) || is.function(tags)) {
  tags <- htmltools::tags
}

# ---- 4. CSS ---------------------------------------------------------------
# MP119: UI Block Separation Principle - External CSS file organization by block type
# The styles.css file organizes styles by block type (Control, Display, Navigation, etc.)
css_deps <- tags$head(
  # Include external CSS files from centralized 19_CSS directory (UI_R014)
  # union_block_separation.css removed - conflicts with bs4Dash native styles
  # tags$link(rel = "stylesheet", type = "text/css", href = "css/union_block_separation.css"),
  tags$link(rel = "stylesheet", type = "text/css", href = "css/union_component_visibility.css"),
  tags$link(rel = "stylesheet", type = "text/css", href = "css/dynamic_filter_theme.css"),
  
  # Keep inline only the critical DataTables-specific styles
  tags$style(HTML(
    ".sidebar .selectize-dropdown{width:auto!important;min-width:100%!important}.sidebar .selectize-dropdown-content{min-width:100%}
    /* 確保 DataTables 的滾動條始終顯示 */
    .dataTables_scrollBody { overflow-x: scroll !important; }
    /* macOS 滾動條始終顯示的全域設定 */
    .dataTables_scrollBody::-webkit-scrollbar { -webkit-appearance: none; }
    .dataTables_scrollBody::-webkit-scrollbar:horizontal { height: 10px; }
    .dataTables_scrollBody::-webkit-scrollbar-thumb { border-radius: 5px; background-color: rgba(0,0,0,.3); }
    .dataTables_scrollBody::-webkit-scrollbar-track { background-color: rgba(0,0,0,.1); border-radius: 5px; }
  /* 確保 filter panel 中的按鈕有適當的左右邊距 */
  .well .btn-block { margin-left: 0 !important; margin-right: 0 !important; width: 100% !important; }
  .wellPanel .btn-block { margin-left: 0 !important; margin-right: 0 !important; width: 100% !important; }
  /* 調整 sidebar accordion 樣式 */
  .sidebar-section .accordion { margin: 0 -15px; }
  .sidebar-section .accordion .card { 
    border: none; 
    background-color: transparent;
    margin-bottom: 0;  /* Remove spacing between accordion items */
  }
  .sidebar-section .accordion .card-header { 
    padding: 0.5rem 1rem;
    color: #fff !important;
  }
  /* Platform & Product line (info) 樣式 */
  .sidebar-section .accordion .card-header.bg-info { 
    background-color: #17a2b8 !important;
    border: 1px solid #17a2b8 !important;
  }
  .sidebar-section .accordion .card-header.bg-info:hover { 
    background-color: #138496 !important;
    filter: brightness(1.1);
  }
  /* 其他 primary 樣式保持不變 */
  .sidebar-section .accordion .card-header.bg-primary { 
    background-color: var(--primary) !important; 
    border: 1px solid var(--primary) !important;
  }
  .sidebar-section .accordion .card-header.bg-primary:hover { 
    background-color: var(--primary) !important; 
    filter: brightness(1.1);
  }
  /* 確保 accordion 標題文字可見 */
  .sidebar-section .accordion .card-header .btn { 
    color: #fff !important;
    font-size: 1.1rem;
  }
  .sidebar-section .accordion .card-header h2 { 
    margin: 0; 
    font-size: 1rem;
  }
  .sidebar-section .accordion .btn-link { 
    color: #fff !important; 
    text-decoration: none;
    font-weight: 500;
    width: 100%;
    text-align: left;
    padding: 0;
    display: flex;
    justify-content: space-between;
    align-items: center;
  }
  .sidebar-section .accordion .btn-link:hover { 
    color: #fff !important; 
    text-decoration: none;
  }
  .sidebar-section .accordion .btn-link::after {
    content: '';
    display: inline-block;
    margin-left: auto;
    transition: transform 0.2s;
  }
  .sidebar-section .accordion .card-body { 
    padding: 0.25rem 0.75rem;  /* 大幅減少上下 padding */
    background-color: transparent;
  }
  /* Platform & Product line (info) card body */
  .sidebar-section .accordion .card.bg-info .card-body { 
    border-left: 1px solid #17a2b8;
    border-right: 1px solid #17a2b8;
    border-bottom: 1px solid #17a2b8;
  }
  /* Primary card body (保持原樣) */
  .sidebar-section .accordion .card.bg-primary .card-body { 
    border-left: 1px solid var(--primary);
    border-right: 1px solid var(--primary);
    border-bottom: 1px solid var(--primary);
  }
  /* 調整 radioButtons 選項間距 - 讓選項更緊密 */
  .sidebar-section .radio { 
    margin: 0 !important; 
    padding: 0 !important;  /* 完全移除間距 */
    line-height: 1.2;  /* 調整行高讓選項更緊密 */
  }
  .sidebar-section .shiny-options-group { 
    margin: 0 !important;
    padding: 0 !important;
  }
  /* 確保 radioButtons 容器本身也沒有額外間距 */
  #sidebar_accordion .shiny-input-radiogroup {
    margin: 0 !important;
    padding: 0 !important;
  }
  .sidebar-section .shiny-options-group .radio:first-child {
    padding-top: 0;  /* 第一個選項無上方間距 */
  }
  .sidebar-section .shiny-options-group .radio:last-child {
    margin-bottom: 0;
    padding-bottom: 0;  /* 最後一個選項無下方間距 */
  }
  .sidebar-section .shiny-options-group .radio {
    margin: 0 !important;  /* 強制移除所有 margin */
    padding: 0 !important;  /* 強制移除所有 padding */
  }
  /* 特別針對 accordion 內的 radio buttons */
  #sidebar_accordion .radio {
    margin: 0 !important;
    padding: 0 !important;
    line-height: 1.4;  /* 稍微增加一點行高保持可讀性 */
  }"
)))

# Suppress AdminLTE CardWidget.minimize() style error on tab switch (Issue #239)
# Root cause: AdminLTE 3.1.0-pre accesses this._parent[0].style on detached DOM cards
# during resize cascade triggered by sidebar CSS transitionend → $(window).trigger("resize")
js_adminlte_fix <- tags$head(tags$script(HTML("
(function(){
  var _orig = window.onerror;
  window.onerror = function(msg, src, line, col, err){
    if(msg && msg.indexOf('Cannot read properties of undefined')!==-1 &&
       msg.indexOf('style')!==-1) return true;
    if(_orig) return _orig.apply(this, arguments);
    return false;
  };
})();
")))

# ---- 4.5 Default UI selections --------------------------------------------
# Avoid hard-coded defaults; choose from configured/available options.
platform_choices_df <- tryCatch({
  df_platform[df_platform$platform_id %in% c("all", names(app_configs$platforms)), , drop = FALSE]
}, error = function(e) {
  df_platform
})

platform_choice_vec <- tryCatch({
  setNames(platform_choices_df$platform_id, platform_choices_df$platform_name_english)
}, error = function(e) {
  as.character(platform_choices_df$platform_id)
})

platform_ids <- tryCatch(as.character(platform_choices_df$platform_id), error = function(e) character(0))
default_platform_id <- tryCatch({
  enabled <- setdiff(intersect(names(app_configs$platforms), platform_ids), "all")
  if (!is.null(app_configs$default_platform_id) && app_configs$default_platform_id %in% platform_ids) {
    as.character(app_configs$default_platform_id)
  } else if (length(enabled) == 1) {
    enabled[[1]]
  } else if ("all" %in% platform_ids) {
    "all"
  } else if (length(platform_ids) >= 1) {
    platform_ids[[1]]
  } else {
    NULL
  }
}, error = function(e) {
  if ("all" %in% platform_ids) "all" else if (length(platform_ids) >= 1) platform_ids[[1]] else NULL
})

product_line_choices_df <- tryCatch({
  rbind(
    df_product_line[df_product_line$product_line_id == "all", , drop = FALSE],
    get_active_product_lines()
  )
}, error = function(e) {
  df_product_line
})

pl_lang <- get_language_scope("product_line_labels")
pl_label_col <- if (grepl("^zh", tolower(pl_lang)) &&
  "product_line_name_chinese" %in% names(product_line_choices_df)
) "product_line_name_chinese" else "product_line_name_english"

product_line_choice_vec <- tryCatch({
  setNames(product_line_choices_df$product_line_id,
           product_line_choices_df[[pl_label_col]])
}, error = function(e) {
  as.character(product_line_choices_df$product_line_id)
})

product_line_ids <- tryCatch(as.character(product_line_choices_df$product_line_id), error = function(e) character(0))
default_product_line_id <- tryCatch({
  if (!is.null(app_configs$default_product_line_id) && app_configs$default_product_line_id %in% product_line_ids) {
    as.character(app_configs$default_product_line_id)
  } else if (length(product_line_ids) >= 1) {
    product_line_ids[[1]]
  } else {
    NULL
  }
}, error = function(e) {
  if (length(product_line_ids) >= 1) product_line_ids[[1]] else NULL
})

default_sidebar_tab <- tryCatch({
  tab <- NULL
  if (!is.null(app_configs$default_sidebar_tab)) tab <- app_configs$default_sidebar_tab
  if (is.null(tab) && !is.null(app_configs$app) && !is.null(app_configs$app$default_sidebar_tab)) tab <- app_configs$app$default_sidebar_tab
  if (is.null(tab) && !is.null(app_configs$app) && !is.null(app_configs$app$default_tab)) tab <- app_configs$app$default_tab
  if (!is.null(tab) && nzchar(as.character(tab))) as.character(tab) else "rsvMatrix"
}, error = function(e) {
  "rsvMatrix"
})

# ---- 5. UI ---------------------------------------------------------------
# Main app UI (will be shown after login)
main_app_ui <- bs4DashPage(
  title = translate("AI Marketing Platform"), fullscreen = TRUE,
  header = bs4DashNavbar(title = bs4DashBrand(translate("AI Marketing Platform")), skin="light", status="primary"),
  sidebar = bs4DashSidebar(status="primary", width="300px", elevation = 3,minified  = FALSE,
                           # 共用 Platform Filter - 移到最上面
                           div(class="sidebar-section p-3 mt-2",
                               bs4Accordion(
                                 id = "sidebar_accordion",
                                 bs4AccordionItem(
                                   title = translate("Platform"),
                                   status = "info",  # 使用info色（淺藍色）更顯眼
                                   collapsed = TRUE,  # 預設收起
                                   # Following MP081: Explicit Parameter Specification Metaprinciple
                                   radioButtons(
                                     inputId = "platform",
                                     label = NULL,
                                     choices = platform_choice_vec,
                                     selected = default_platform_id
                                   )
                                 ),
                                 bs4AccordionItem(
                                   title = translate("Product line"),
                                   status = "info",  # 使用info色（淺藍色）與Platform統一
                                   collapsed = TRUE,  # 預設收起
                                   radioButtons(
                                     inputId = "product_line",
                                     label = NULL,
                                     choices = product_line_choice_vec,
                                     selected = default_product_line_id
                                   )
                                 )
                               )),
                           # Horizontal separator line between global filters and tab menu
                           tags$hr(style = "border: none; border-top: 3px solid #007bff; margin: 15px -15px;"),
                           sidebarMenu(id="sidebar_menu",
                                      # 0. Dashboard Overview
                                      bs4SidebarMenuItem(
                                        translate("Dashboard Overview"),
                                        tabName = "dashboardOverview",
                                        icon = icon("tachometer-alt")
                                      ),
                                       # 1. TagPilot - 顧客分析與標籤管理
                                       bs4SidebarMenuItem(
                                         text = translate("TagPilot"),
                                         icon = icon("tag"),
                                         startExpanded = TRUE,
                                         bs4SidebarMenuItem(translate("Customer Value (RFM)"), tabName="customerValue", icon=icon("chart-pie")),
                                         bs4SidebarMenuItem(translate("Customer Activity (CAI)"), tabName="customerActivity", icon=icon("bolt")),
                                         bs4SidebarMenuItem(translate("Customer Status (NES)"), tabName="customerStatus", icon=icon("heartbeat")),
                                         bs4SidebarMenuItem(translate("Customer Structure"), tabName="customerStructure", icon=icon("cubes")),
                                         bs4SidebarMenuItem(translate("Lifecycle Prediction"), tabName="customerLifecycle", icon=icon("chart-line")),
                                         bs4SidebarMenuItem(translate("RSV Matrix"), tabName="rsvMatrix", icon=icon("th")),
                                         bs4SidebarMenuItem(translate("Marketing Decision Table"), tabName="marketingDecision", icon=icon("bullhorn")),
                                         bs4SidebarMenuItem(translate("Customer Tag Export"), tabName="customerExport", icon=icon("file-export")),
                                         bs4SidebarMenuItem(translate("Comprehensive Diagnosis"), tabName="tpComprehensiveDiagnosis", icon=icon("stethoscope"))
                                       ),
                                       # 2. Marketing Vital-Signs - 營收與顧客健康
                                       bs4SidebarMenuItem(
                                         text = translate("Marketing Vital-Signs"),
                                         icon = icon("chart-line"),
                                         startExpanded = FALSE,
                                         bs4SidebarMenuItem(translate("Revenue Pulse"), tabName="revenuePulse", icon=icon("dollar-sign")),
                                         bs4SidebarMenuItem(translate("Growth Validation"), tabName="growthValidation", icon=icon("trophy")),
                                         bs4SidebarMenuItem(translate("Customer Growth"), tabName="customerAcquisition", icon=icon("user-plus")),
                                         bs4SidebarMenuItem(translate("Customer Retention"), tabName="customerRetention", icon=icon("user-shield")),
                                         bs4SidebarMenuItem(translate("Active Conversion"), tabName="customerEngagement", icon=icon("heartbeat")),
                                         bs4SidebarMenuItem(translate("Macro Trends"), tabName="macroTrends", icon=icon("chart-bar")),
                                         bs4SidebarMenuItem(translate("World Market Map"), tabName="worldMap", icon=icon("globe")),
                                         bs4SidebarMenuItem(translate("Comprehensive Diagnosis"), tabName="vsComprehensiveDiagnosis", icon=icon("stethoscope"))
                                       ),
                                       # 3. BrandEdge - 品牌競爭分析
                                       bs4SidebarMenuItem(
                                         text = translate("BrandEdge"),
                                         icon = icon("gem"),
                                         startExpanded = FALSE,
                                         bs4SidebarMenuItem(translate("Brand Attribute Evaluation"), tabName="position", icon=icon("table")),
                                         bs4SidebarMenuItem(translate("Brand DNA"), tabName="positionDNA", icon=icon("chart-line")),
                                        bs4SidebarMenuItem(translate("Market Segmentation and Target Market Analysis"), tabName="positionMS", icon=icon("crosshairs")),
                                         bs4SidebarMenuItem(translate("Key Factor Analysis"), tabName="positionKFE", icon=icon("key")),
                                         bs4SidebarMenuItem(translate("Ideal Rate Analysis"), tabName="positionIdealRate", icon=icon("star")),
                                         bs4SidebarMenuItem(translate("Brand Positioning Strategy Recommendations"), tabName="positionStrategy", icon=icon("compass"))
                                       ),
                                       # 4. InsightForge 360 - 高級分析與預測
                                       bs4SidebarMenuItem(
                                         text = translate("InsightForge 360"),
                                         icon = icon("lightbulb"),
                                         startExpanded = FALSE,
                                         bs4SidebarMenuItem(translate("Market Track"), tabName="poissonComment", icon=icon("trophy")),
                                         bs4SidebarMenuItem(translate("Time Analysis"), tabName="poissonTime", icon=icon("clock")),
                                         bs4SidebarMenuItem(translate("Precision Marketing"), tabName="poissonFeature", icon=icon("bullseye"))
                                       ),
                                       # 5. Report Center - 報告生成中心 (MP88: Immediate Feedback)
                                       bs4SidebarMenuItem(
                                         text = translate("Report Center"),
                                         icon = icon("file-alt"),
                                         tabName = "reportCenter"
                                       ) ),
                           # Horizontal separator between tab menu (layer 2) and component filters (layer 3)
                           tags$hr(style = "border: none; border-top: 1px solid #dee2e6; margin: 10px -15px;"),
                           # UI_R026 layer 3: Component filter (all filters via ui$filter, UI_R028)
                           uiOutput("dynamic_filter") ),
  
  body = bs4DashBody(css_deps,
                     bs4TabItems(
                       # Dashboard Overview tab
                       bs4TabItem(tabName="dashboardOverview", uiOutput("dashboard_overview_display")),
                       bs4TabItem(tabName="position",      fluidRow(column(12, bs4Card(title=translate("Position Analysis"),    status="primary", width=12, solidHeader=TRUE, elevation=3, uiOutput("position_display"))))),
                       bs4TabItem(tabName="positionDNA",   fluidRow(column(12, bs4Card(title=translate("Position DNA Visualization"), status="primary", width=12, solidHeader=TRUE, elevation=3, uiOutput("position_dna_display"))))),
                       bs4TabItem(tabName="positionMS",    fluidRow(
                         column(12, bs4Card(title=translate("Market Segmentation and Target Market Analysis"), status="primary", width=12, solidHeader=TRUE, elevation=3, uiOutput("position_ms_display")))
                       )),
                       bs4TabItem(tabName="positionKFE",   fluidRow(column(12, bs4Card(title=translate("Key Factor Analysis"), status="success", width=12, solidHeader=TRUE, elevation=3, uiOutput("position_kfe_full_display"))))),
                       bs4TabItem(tabName="positionIdealRate", fluidRow(column(12, bs4Card(title=translate("Ideal Rate Analysis"), status="warning", width=12, solidHeader=TRUE, elevation=3, uiOutput("position_ideal_rate_display"))))),
                       bs4TabItem(tabName="positionStrategy", fluidRow(column(12, bs4Card(title=translate("Strategic Position Analysis"), status="danger", width=12, solidHeader=TRUE, elevation=3, uiOutput("position_strategy_display"))))),
                       bs4TabItem(tabName="poissonTime", fluidRow(column(12, bs4Card(title=translate("Time Segment Analysis"), status="primary", width=12, solidHeader=TRUE, elevation=3, uiOutput("poisson_time_display"))))),
                       bs4TabItem(tabName="poissonFeature", fluidRow(column(12, bs4Card(title=translate("Precision Model Analysis"), status="success", width=12, solidHeader=TRUE, elevation=3, uiOutput("poisson_feature_display"))))),
                       bs4TabItem(tabName="poissonComment", fluidRow(column(12, bs4Card(title=translate("Product Track Analysis"), status="info", width=12, solidHeader=TRUE, elevation=3, uiOutput("poisson_comment_display"))))),
                       # TagPilot tabs (Phase 2: DNA analysis)
                       bs4TabItem(tabName="customerValue", uiOutput("customer_value_display")),
                       bs4TabItem(tabName="customerActivity", uiOutput("customer_activity_display")),
                       bs4TabItem(tabName="customerStatus", uiOutput("customer_status_display")),
                       bs4TabItem(tabName="customerStructure", uiOutput("customer_structure_display")),
                       bs4TabItem(tabName="customerLifecycle", uiOutput("customer_lifecycle_display")),
                       # TagPilot tabs (Phase 1: RSV, Marketing, Export)
                       bs4TabItem(tabName="rsvMatrix", uiOutput("rsv_matrix_display")),
                       bs4TabItem(tabName="marketingDecision", uiOutput("marketing_decision_display")),
                       bs4TabItem(tabName="customerExport", uiOutput("customer_export_display")),
                       # VitalSigns tabs
                       bs4TabItem(tabName="revenuePulse", uiOutput("revenue_pulse_display")),
                       bs4TabItem(tabName="growthValidation", uiOutput("growth_validation_display")),
                       bs4TabItem(tabName="customerAcquisition", uiOutput("customer_acquisition_display")),
                       bs4TabItem(tabName="customerEngagement", uiOutput("customer_engagement_display")),
                       bs4TabItem(tabName="customerRetention", uiOutput("customer_retention_display")),
                       bs4TabItem(tabName="macroTrends", uiOutput("macro_trends_display")),
                       bs4TabItem(tabName="worldMap", uiOutput("world_map_display")),
                       bs4TabItem(tabName="vsComprehensiveDiagnosis", uiOutput("vs_comprehensive_diagnosis_display")),
                       bs4TabItem(tabName="tpComprehensiveDiagnosis", uiOutput("tp_comprehensive_diagnosis_display")),
                       # Report Center tab - Following MP88: Immediate Feedback
                       bs4TabItem(tabName="reportCenter", fluidRow(column(12, bs4Card(title=translate("Report Generation Center"), status="danger", width=12, solidHeader=TRUE, elevation=3, uiOutput("report_display"))))) ) ),

  footer = dashboardFooter(fixed=TRUE, right=paste(translate("Version"), "1.0.0 | 2025")) )

# ---- 5.5 Complete UI with Login --------------------------------------------
ui <- tagList(
  useShinyjs(),
  css_deps,
  js_adminlte_fix,
  
  # Password login interface
  div(id = "login_page",
    passwordOnlyUI(
      id = "auth",
      app_title = translate("AI Marketing Platform"),
      app_icon = "assets/icons/app_icon.png",  # Optional: add your icon
      password_label = translate("Enter system password"),
      submit_label = translate("Enter System"),
      primary_color = "#007bff",
      translate = translate
    )
  ),
  
  # Main application interface
  hidden(
    div(id = "main_app",
      main_app_ui
    )
  )
)

# ---- 6. Server -----------------------------------------------------------
server <- function(input, output, session){
  # ---- 6.0 登入驗證 ------------------------------------------------------
  # Initialize password authentication
  auth <- passwordOnlyServer(
    id = "auth",
    password_env_var = "APP_PASSWORD",  # Set this in your deployment environment
    max_attempts = 3,
    lockout_duration = 300,  # 5 minutes
    translate = translate
  )

  # ---- 6.0.1 Initialize Default Tab --------------------------------------
  # Set default tab on app startup to prevent NULL sidebar_menu errors
  # This ensures input$sidebar_menu always has a valid value
  # ENHANCED: Use isolate and once flag to prevent race conditions
  # Principle: MP031/MP033 - Proper initialization patterns
  observe({
    if (is.null(input$sidebar_menu) || length(input$sidebar_menu) == 0) {
      isolate({
        # Use isolate to prevent reactive loops during initialization
        updateTabItems(session, "sidebar_menu", selected = default_sidebar_tab)
      })
    }
  }) |> bindEvent(TRUE, once = TRUE)  # Execute only once at startup

  # Monitor login status
  observe({
    if (auth$logged_in()) {
      # Hide login and show main app
      shinyjs::hide("login_page")
      shinyjs::show("main_app")
      
      # Show success notification
      showNotification(
        "歡迎使用 AI Marketing Platform",
        type = "message",
        duration = 5
      )
    }
  })
  
  # ---- 6.1 資料庫連接 ----------------------------------------------------
  # Connect to the app database using dual-mode approach (DuckDB local, Supabase on deployment)
  # DM_R056: Posit Connect Deployment Assets - auto-detect data source
  # Create connection inside server to ensure proper lifecycle management
  app_connection <- tryCatch(
    dbConnectAppData(db_path = db_path_list$app_data),
    error = function(e) {
      message("[union_production_test] DB connection failed: ", e$message)
      message("[union_production_test] App will start in no-data mode. Components will show empty-state messages.")
      NULL
    }
  )

  # Create a reactive value to track connection status
  connection_active <- reactiveVal(!is.null(app_connection))
  
  # Ensure connection is closed when session ends
  session$onSessionEnded(function(){
    # Mark connection as inactive first
    connection_active(FALSE)
    
    # Then close the connection
    if(!is.null(app_connection) && inherits(app_connection, "DBIConnection")) {
      try(DBI::dbDisconnect(app_connection), silent = TRUE)
    }
  })
  
  # ---- 6.2 共享設定 ------------------------------------------------------
  comp_config <- reactive({
    selected_product_line <- input$product_line
    if (is.null(selected_product_line) || !nzchar(as.character(selected_product_line))) {
      selected_product_line <- "all"
    }

    # UI_R028: country filter removed from comp_config — now owned by worldMap component (#349)
    list(
      filters    = list(platform_id = input$platform,
                        product_line_id_chosen = selected_product_line,
                        product_line_id_sliced = selected_product_line),
      active_tab = input$sidebar_menu
    )
  })

  # ---- 6.2b Country filter removed from union (UI_R028) --------------------
  # Country filter is now owned by worldMap component's ui$filter (#349, #350)
  # Previously: uiOutput("country_filter_sidebar") injected at union level

  # ---- 6.3 Component instances -----------------------------------------
  # Dashboard Overview component
  dashboard_overview_comp <- dashboardOverviewComponent("dashboard_overview", app_connection, comp_config, translate)

  customer_comp <- microCustomerComponent("cust", app_connection, comp_config, translate)
  position_comp <- positionTableComponent("position", app_connection, comp_config, translate)
  position_dna_comp <- positionDNAPlotlyComponent("position_dna", app_connection, comp_config, translate)
  position_ms_comp <- positionMSPlotlyComponent("position_ms", app_connection, comp_config, translate)
  position_kfe_comp <- positionKFEComponent("position_kfe", app_connection, comp_config, translate, display_mode = "compact")
  position_kfe_full_comp <- positionKFEComponent("position_kfe_full", app_connection, comp_config, translate, display_mode = "full")
  position_ideal_rate_comp <- positionIdealRateComponent("position_ideal_rate", app_connection, comp_config, translate)
  position_strategy_comp <- positionStrategyComponent("position_strategy", app_connection, comp_config, translate)
  # Poisson analysis components
  poisson_time_comp <- poissonTimeAnalysisComponent("poisson_time", app_connection, comp_config, translate)

  # 使用 Poisson 特徵分析元件
  poisson_feature_comp <- poissonFeatureAnalysisComponent("poisson_feature", app_connection, comp_config, translate)

  # 使用 Poisson 評論分析元件
  poisson_comment_comp <- poissonCommentAnalysisComponent("poisson_comment", app_connection, comp_config, translate)

  # Report Integration Component - Following MP56: Connected Component Principle
  # R116: Enhanced Data Access with tbl2
  report_comp <- reportIntegrationComponent("report", app_connection, comp_config, translate)

  # UX_P002: Validate pre-computed tables exist at startup
  if (!is.null(app_connection)) {
    required_precomputed <- c("df_rsv_classified", "df_dna_plot_data",
                              "df_dna_category_counts", "df_dna_summary_stats")
    existing_tables <- tryCatch(DBI::dbListTables(app_connection), error = function(e) character(0))
    missing_tables <- setdiff(required_precomputed, existing_tables)
    if (length(missing_tables) > 0) {
      message("[UX_P002] WARNING: Missing pre-computed tables: ", paste(missing_tables, collapse = ", "),
              "\nRun: make run (from update_scripts/) to generate them.")
    }
  }

  # TagPilot components (Phase 2: DNA analysis)
  customer_value_comp <- customerValueComponent("customer_value", app_connection, comp_config, translate)
  customer_activity_comp <- customerActivityComponent("customer_activity", app_connection, comp_config, translate)
  customer_status_comp <- customerStatusComponent("customer_status", app_connection, comp_config, translate)
  customer_structure_comp <- customerStructureComponent("customer_structure", app_connection, comp_config, translate)
  customer_lifecycle_comp <- customerLifecycleComponent("customer_lifecycle", app_connection, comp_config, translate)

  # TagPilot components (Phase 1)
  rsv_matrix_comp <- rsvMatrixComponent("rsv_matrix", app_connection, comp_config, translate)
  marketing_decision_comp <- marketingDecisionComponent("marketing_decision", app_connection, comp_config, translate)
  customer_export_comp <- customerExportComponent("customer_export", app_connection, comp_config, translate)

  # VitalSigns components (Phase 2)
  revenue_pulse_comp <- revenuePulseComponent("revenue_pulse", app_connection, comp_config, translate)
  customer_engagement_comp <- customerEngagementComponent("customer_engagement", app_connection, comp_config, translate)
  customer_retention_comp <- customerRetentionComponent("customer_retention", app_connection, comp_config, translate)
  customer_acquisition_comp <- customerAcquisitionComponent("customer_acquisition", app_connection, comp_config, translate)
  growth_validation_comp <- growthValidationComponent("growth_validation", app_connection, comp_config, translate)  # #416
  macro_trends_comp <- macroTrendsComponent("macro_trends", app_connection, comp_config, translate)
  world_map_comp <- worldMapComponent("world_map", app_connection, comp_config, translate)
  vs_comprehensive_diagnosis_comp <- vsComprehensiveDiagnosisComponent("vs_comprehensive_diagnosis", app_connection, comp_config, translate)

  # TagPilot Comprehensive Diagnosis (Issue #271)
  tp_comprehensive_diagnosis_comp <- comprehensiveDiagnosisComponent("tp_comprehensive_diagnosis", app_connection, comp_config, translate)

  # ---- 6.4 動態 Filter 注入 --------------------------------------------
  # Apply defensive programming: check if sidebar_menu is NULL, empty, or not scalar
  # FIX: Ensure input$sidebar_menu is scalar (length 1) before using in switch
  # Principle: MP031/MP033 - Proper initialization patterns
  # Principle: Defensive Programming - Handle vector inputs gracefully
  output$dynamic_filter <- renderUI({
    # Defensive check: Return NULL if sidebar_menu is not yet initialized
    if (is.null(input$sidebar_menu) || length(input$sidebar_menu) == 0) {
      return(NULL)
    }

    # CRITICAL FIX: Ensure scalar input for switch statement
    # If input contains multiple values, take the first one
    sidebar_value <- if(length(input$sidebar_menu) > 1) {
      warning("sidebar_menu contains multiple values, using first: ", paste(input$sidebar_menu, collapse=", "))
      input$sidebar_menu[1]
    } else {
      input$sidebar_menu
    }

    switch(sidebar_value,
           "dashboardOverview" = dashboard_overview_comp$ui$filter,
           "position"      = position_comp$ui$filter,
           "positionDNA"   = position_dna_comp$ui$filter,
           "positionMS"    = position_ms_comp$ui$filter,
           "positionKFE"   = position_kfe_full_comp$ui$filter,
           "positionIdealRate" = position_ideal_rate_comp$ui$filter,
           "positionStrategy" = position_strategy_comp$ui$filter,
           "poissonTime"   = poisson_time_comp$ui$filter,
           "poissonFeature" = poisson_feature_comp$ui$filter,
           "poissonComment" = poisson_comment_comp$ui$filter,
           "reportCenter" = report_comp$ui$filter,
           # TagPilot components (Phase 2: DNA analysis)
           "customerValue" = customer_value_comp$ui$filter,
           "customerActivity" = customer_activity_comp$ui$filter,
           "customerStatus" = customer_status_comp$ui$filter,
           "customerStructure" = customer_structure_comp$ui$filter,
           "customerLifecycle" = customer_lifecycle_comp$ui$filter,
           # TagPilot components (Phase 1)
           "rsvMatrix" = rsv_matrix_comp$ui$filter,
           "marketingDecision" = marketing_decision_comp$ui$filter,
           "customerExport" = customer_export_comp$ui$filter,
           # VitalSigns components (Phase 2)
           "revenuePulse" = revenue_pulse_comp$ui$filter,
           "growthValidation" = growth_validation_comp$ui$filter,
           "customerAcquisition" = customer_acquisition_comp$ui$filter,
           "customerEngagement" = customer_engagement_comp$ui$filter,
           "customerRetention" = customer_retention_comp$ui$filter,
           "macroTrends" = macro_trends_comp$ui$filter,
           "worldMap" = world_map_comp$ui$filter,
           "vsComprehensiveDiagnosis" = vs_comprehensive_diagnosis_comp$ui$filter,
           # TagPilot Comprehensive Diagnosis
           "tpComprehensiveDiagnosis" = tp_comprehensive_diagnosis_comp$ui$filter,
           NULL)  # Default case when no match
  })
  
  # ---- 6.5 Component UI (using renderUI2 for conditional rendering) -------
  output$position_display <- renderUI2(
    current_tab = input$sidebar_menu,
    target_tab = "position", 
    ui_component = position_comp$ui$display
  )
  
  output$position_dna_display <- renderUI2(
    current_tab = input$sidebar_menu,
    target_tab = "positionDNA", 
    ui_component = position_dna_comp$ui$display
  )
  
  output$position_ms_display <- renderUI2(
    current_tab = input$sidebar_menu,
    target_tab = "positionMS", 
    ui_component = position_ms_comp$ui$display,
    loading_icon = "crosshairs"
  )
  
  
  
  output$position_kfe_full_display <- renderUI2(
    current_tab = input$sidebar_menu,
    target_tab = "positionKFE", 
    ui_component = position_kfe_full_comp$ui$display,
    loading_icon = "key"
  )
  
  output$position_ideal_rate_display <- renderUI2(
    current_tab = input$sidebar_menu,
    target_tab = "positionIdealRate", 
    ui_component = position_ideal_rate_comp$ui$display,
    loading_icon = "star"
  )
  
  output$position_strategy_display <- renderUI2(
    current_tab = input$sidebar_menu,
    target_tab = "positionStrategy", 
    ui_component = position_strategy_comp$ui$display,
    loading_icon = "compass"
  )
  
  # Poisson component UI outputs
  output$poisson_time_display <- renderUI2(
    current_tab = input$sidebar_menu,
    target_tab = "poissonTime", 
    ui_component = poisson_time_comp$ui$display,
    loading_icon = "clock"
  )
  
  output$poisson_feature_display <- renderUI2(
    current_tab = input$sidebar_menu,
    target_tab = "poissonFeature", 
    ui_component = poisson_feature_comp$ui$display,
    loading_icon = "bullseye"
  )
  
  output$poisson_comment_display <- renderUI2(
    current_tab = input$sidebar_menu,
    target_tab = "poissonComment",
    ui_component = poisson_comment_comp$ui$display,
    loading_icon = "trophy"
  )

  # Report Center UI output - Following MP88: Immediate Feedback
  output$report_display <- renderUI2(
    current_tab = input$sidebar_menu,
    target_tab = "reportCenter",
    ui_component = report_comp$ui$display,
    loading_icon = "file-alt"
  )

  # Dashboard Overview UI output
  output$dashboard_overview_display <- renderUI2(
    current_tab = input$sidebar_menu,
    target_tab = "dashboardOverview",
    ui_component = dashboard_overview_comp$ui$display,
    loading_icon = "tachometer-alt"
  )

  # TagPilot components (Phase 2: DNA analysis) UI outputs
  output$customer_value_display <- renderUI2(
    current_tab = input$sidebar_menu,
    target_tab = "customerValue",
    ui_component = customer_value_comp$ui$display,
    loading_icon = "chart-pie"
  )

  output$customer_activity_display <- renderUI2(
    current_tab = input$sidebar_menu,
    target_tab = "customerActivity",
    ui_component = customer_activity_comp$ui$display,
    loading_icon = "bolt"
  )

  output$customer_status_display <- renderUI2(
    current_tab = input$sidebar_menu,
    target_tab = "customerStatus",
    ui_component = customer_status_comp$ui$display,
    loading_icon = "heartbeat"
  )

  output$customer_structure_display <- renderUI2(
    current_tab = input$sidebar_menu,
    target_tab = "customerStructure",
    ui_component = customer_structure_comp$ui$display,
    loading_icon = "cubes"
  )

  output$customer_lifecycle_display <- renderUI2(
    current_tab = input$sidebar_menu,
    target_tab = "customerLifecycle",
    ui_component = customer_lifecycle_comp$ui$display,
    loading_icon = "chart-line"
  )

  # TagPilot components (Phase 1) UI outputs
  output$rsv_matrix_display <- renderUI2(
    current_tab = input$sidebar_menu,
    target_tab = "rsvMatrix",
    ui_component = rsv_matrix_comp$ui$display,
    loading_icon = "th"
  )

  output$marketing_decision_display <- renderUI2(
    current_tab = input$sidebar_menu,
    target_tab = "marketingDecision",
    ui_component = marketing_decision_comp$ui$display,
    loading_icon = "bullhorn"
  )

  output$customer_export_display <- renderUI2(
    current_tab = input$sidebar_menu,
    target_tab = "customerExport",
    ui_component = customer_export_comp$ui$display,
    loading_icon = "file-export"
  )

  # VitalSigns components (Phase 2) UI outputs
  output$revenue_pulse_display <- renderUI2(
    current_tab = input$sidebar_menu,
    target_tab = "revenuePulse",
    ui_component = revenue_pulse_comp$ui$display,
    loading_icon = "dollar-sign"
  )

  output$customer_acquisition_display <- renderUI2(
    current_tab = input$sidebar_menu,
    target_tab = "customerAcquisition",
    ui_component = customer_acquisition_comp$ui$display,
    loading_icon = "user-plus"
  )

  # #416: Growth Validation
  output$growth_validation_display <- renderUI2(
    current_tab = input$sidebar_menu,
    target_tab = "growthValidation",
    ui_component = growth_validation_comp$ui$display,
    loading_icon = "trophy"
  )

  output$customer_engagement_display <- renderUI2(
    current_tab = input$sidebar_menu,
    target_tab = "customerEngagement",
    ui_component = customer_engagement_comp$ui$display,
    loading_icon = "heartbeat"
  )

  output$customer_retention_display <- renderUI2(
    current_tab = input$sidebar_menu,
    target_tab = "customerRetention",
    ui_component = customer_retention_comp$ui$display,
    loading_icon = "user-shield"
  )

  output$macro_trends_display <- renderUI2(
    current_tab = input$sidebar_menu,
    target_tab = "macroTrends",
    ui_component = macro_trends_comp$ui$display,
    loading_icon = "chart-bar"
  )

  output$world_map_display <- renderUI2(
    current_tab = input$sidebar_menu,
    target_tab = "worldMap",
    ui_component = world_map_comp$ui$display,
    loading_icon = "globe"
  )

  output$vs_comprehensive_diagnosis_display <- renderUI2(
    current_tab = input$sidebar_menu,
    target_tab = "vsComprehensiveDiagnosis",
    ui_component = vs_comprehensive_diagnosis_comp$ui$display,
    loading_icon = "stethoscope"
  )

  # TagPilot Comprehensive Diagnosis
  output$tp_comprehensive_diagnosis_display <- renderUI2(
    current_tab = input$sidebar_menu,
    target_tab = "tpComprehensiveDiagnosis",
    ui_component = tp_comprehensive_diagnosis_comp$ui$display,
    loading_icon = "stethoscope"
  )

  # ---- 6.6 啟動 Component server ---------------------------------------
  # PERFORMANCE NOTE (2026-01-26):
  # - Component servers initialize on first tab activation (lazy init)
  # - Reduces startup work while keeping UI placeholders via renderUI2
  init_on_tab <- function(tab, init_fn, init_immediately = FALSE) {
    res <- reactiveVal(NULL)
    initialized <- reactiveVal(FALSE)

    run_init <- function() {
      # Use isolate() to safely read reactive value outside reactive context
      # This is needed when init_immediately = TRUE (called during server init)
      if (!isolate(initialized())) {
        res(init_fn())
        initialized(TRUE)
      }
    }

    if (init_immediately) {
      run_init()
    }

    observeEvent(input$sidebar_menu, {
      tab_value <- input$sidebar_menu
      if (length(tab_value) > 1) {
        tab_value <- tab_value[1]
      }
      if (identical(tab_value, tab)) {
        run_init()
      }
    }, ignoreInit = TRUE)

    res
  }

  get_res <- function(x) if (is.function(x)) x() else x

  # Dashboard Overview - lazy init
  dashboard_overview_res <- init_on_tab("dashboardOverview", function() dashboard_overview_comp$server(input, output, session), init_immediately = TRUE)

  # Core component for default tab
  cust_res     <- init_on_tab("microCustomer", function() customer_comp$server(input, output, session), init_immediately = TRUE)

  # Lazy-initialized components
  position_res <- init_on_tab("position", function() position_comp$server(input, output, session))
  position_dna_res <- init_on_tab("positionDNA", function() position_dna_comp$server(input, output, session))
  position_ms_res <- init_on_tab(
    "positionMS",
    function() positionMSPlotlyServer("position_ms", app_connection, comp_config, session, active_tab = reactive(input$sidebar_menu))
  )
  position_kfe_res <- init_on_tab("positionKFE", function() position_kfe_comp$server(input, output, session))
  position_kfe_full_res <- init_on_tab("positionKFE", function() position_kfe_full_comp$server(input, output, session))
  position_ideal_rate_res <- init_on_tab("positionIdealRate", function() position_ideal_rate_comp$server(input, output, session))
  position_strategy_res <- init_on_tab("positionStrategy", function() position_strategy_comp$server(input, output, session))

  poisson_time_res <- init_on_tab("poissonTime", function() poisson_time_comp$server(input, output, session))
  poisson_feature_res <- init_on_tab("poissonFeature", function() poisson_feature_comp$server(input, output, session))
  poisson_comment_res <- init_on_tab("poissonComment", function() poisson_comment_comp$server(input, output, session))

  # TagPilot components (Phase 2: DNA analysis) - lazy init
  customer_value_res <- init_on_tab("customerValue", function() customer_value_comp$server(input, output, session))
  customer_activity_res <- init_on_tab("customerActivity", function() customer_activity_comp$server(input, output, session))
  customer_status_res <- init_on_tab("customerStatus", function() customer_status_comp$server(input, output, session))
  customer_structure_res <- init_on_tab("customerStructure", function() customer_structure_comp$server(input, output, session))
  customer_lifecycle_res <- init_on_tab("customerLifecycle", function() customer_lifecycle_comp$server(input, output, session))

  # TagPilot components (Phase 1)
  # FIX: init_on_tab with ignoreInit=TRUE skips the default tab's first value,
  # so the default tab's server never initializes. Use init_immediately for it.
  rsv_matrix_res <- init_on_tab("rsvMatrix", function() rsv_matrix_comp$server(input, output, session),
                                init_immediately = identical(default_sidebar_tab, "rsvMatrix"))
  marketing_decision_res <- init_on_tab("marketingDecision", function() marketing_decision_comp$server(input, output, session),
                                        init_immediately = identical(default_sidebar_tab, "marketingDecision"))
  customer_export_res <- init_on_tab("customerExport", function() customer_export_comp$server(input, output, session),
                                     init_immediately = identical(default_sidebar_tab, "customerExport"))

  # VitalSigns components (Phase 2) - lazy init
  revenue_pulse_res <- init_on_tab("revenuePulse", function() revenue_pulse_comp$server(input, output, session))
  customer_engagement_res <- init_on_tab("customerEngagement", function() customer_engagement_comp$server(input, output, session))
  customer_retention_res <- init_on_tab("customerRetention", function() customer_retention_comp$server(input, output, session))
  customer_acquisition_res <- init_on_tab("customerAcquisition", function() customer_acquisition_comp$server(input, output, session))
  growth_validation_res <- init_on_tab("growthValidation", function() growth_validation_comp$server(input, output, session))  # #416
  macro_trends_res <- init_on_tab("macroTrends", function() macro_trends_comp$server(input, output, session))
  world_map_res <- init_on_tab("worldMap", function() world_map_comp$server(input, output, session))
  vs_comprehensive_diagnosis_res <- init_on_tab("vsComprehensiveDiagnosis", function() vs_comprehensive_diagnosis_comp$server(input, output, session))

  # TagPilot Comprehensive Diagnosis
  tp_comprehensive_diagnosis_res <- init_on_tab("tpComprehensiveDiagnosis", function() tp_comprehensive_diagnosis_comp$server(input, output, session))

  # Initialize Report Integration Server - Following MP56: Connected Component Principle
  # Collect all module results for report generation
  module_results <- reactive({
    list(
      vital_signs = list(
        revenue_pulse = get_res(revenue_pulse_res),
        customer_engagement = get_res(customer_engagement_res),
        customer_retention = get_res(customer_retention_res),
        customer_acquisition = get_res(customer_acquisition_res),
        world_map = get_res(world_map_res),
        macro_trends = get_res(macro_trends_res)
      ),
      tagpilot = list(
        customer_dna = get_res(cust_res),
        customer_value = get_res(customer_value_res),
        customer_activity = get_res(customer_activity_res),
        customer_status = get_res(customer_status_res),
        customer_structure = get_res(customer_structure_res),
        customer_lifecycle = get_res(customer_lifecycle_res),
        rsv_matrix = get_res(rsv_matrix_res),
        marketing_decision = get_res(marketing_decision_res),
        customer_export = get_res(customer_export_res)
      ),
      brandedge = list(
        position_table = get_res(position_res),
        position_dna = get_res(position_dna_res),
        position_ms = get_res(position_ms_res),
        position_kfe = get_res(position_kfe_full_res),
        position_ideal = get_res(position_ideal_rate_res),
        position_strategy = get_res(position_strategy_res)
      ),
      insightforge = list(
        poisson_comment = get_res(poisson_comment_res),
        poisson_time = get_res(poisson_time_res),
        poisson_feature = get_res(poisson_feature_res)
      )
    )
  })

  # Pass module results to report component
  report_res <- init_on_tab(
    "reportCenter",
    function() report_comp$server(input, output, session, module_results)
  )
  
  # ---- 6.7  切換分頁時的跨元件協調 ---------------------------
  observeEvent(input$sidebar_menu, {
    # 切換分頁時重置 microCustomer filter（元件在背景執行，供 Report Center 使用）
    session$sendCustomMessage(
      "shiny.button.click",
      list(id = "cust-clear_filter")
    )
    
    # 當切換到 Position 相關分頁時，顯示通知訊息
    if (input$sidebar_menu == "position") {
      showNotification(translate("Position data shows competitive keyword ranking performance"), 
                       type = "message", duration = 5)
    }
    
    # 當切換到 Position DNA 分頁時，顯示通知訊息
    if (input$sidebar_menu == "positionDNA") {
      showNotification(translate("Interactive DNA visualization shows multi-dimensional brand positioning"), 
                       type = "message", duration = 5)
    }
    
    # 當切換到 Position MS (Market Segmentation) 分頁時，顯示通知訊息
    if (input$sidebar_menu == "positionMS") {
      showNotification(translate("Market Segmentation and Target Market Analysis: identify key market segments through MDS analysis"),
                       type = "message", duration = 5)
    }
    
    # 當切換到 Position KFE 分頁時，顯示通知訊息
    if (input$sidebar_menu == "positionKFE") {
      showNotification(translate("Key Factor Evaluation identifies critical success factors"), 
                       type = "message", duration = 5)
    }
    
    # 當切換到 Position Ideal Rate 分頁時，顯示通知訊息
    if (input$sidebar_menu == "positionIdealRate") {
      showNotification(translate("Ideal Rate Analysis provides product ranking based on key factor performance"), 
                       type = "message", duration = 5)
    }
    
    # 當切換到 Position Strategy 分頁時，顯示通知訊息
    if (input$sidebar_menu == "positionStrategy") {
      showNotification(translate("Strategy Analysis provides four-quadrant strategic positioning insights"), 
                       type = "message", duration = 5)
    }
    
    # 當切換到 Poisson 時間分析分頁時，顯示通知訊息
    if (input$sidebar_menu == "poissonTime") {
      showNotification(translate("Time Segment Analysis: analyze the impact of time factors on sales"), 
                       type = "message", duration = 5)
    }
    
    # 當切換到 Poisson 精準模型分頁時，顯示通知訊息
    if (input$sidebar_menu == "poissonFeature") {
      showNotification(translate("Precision Model: comprehensively analyze the impact of product attributes on sales"), 
                       type = "message", duration = 5)
    }
    
    # 當切換到 Poisson 產品賽道分析分頁時，顯示通知訊息
    if (input$sidebar_menu == "poissonComment") {
      showNotification(translate("Product Track Analysis: competitiveness analysis based on ratings and reviews"),
                       type = "message", duration = 5)
    }

    # 當切換到 Report Center 分頁時，顯示通知訊息 - MP88: Immediate Feedback
    if (input$sidebar_menu == "reportCenter") {
      showNotification(translate("Report Generation Center: generate comprehensive analysis reports"),
                       type = "message", duration = 5)
    }
  }, ignoreInit = TRUE)
  
  # ---- 6.8 通用通知 / 狀態 ---------------------------------------------
  observeEvent(input$platform, {
    # Use df_platform to get the correct platform name
    platform_id <- as.character(input$platform)
    
    # Find the platform name from df_platform
    platform_name <- tryCatch({
      if (exists("df_platform") && !is.null(df_platform)) {
        platform_row <- df_platform %>% 
          dplyr::filter(platform_id == !!platform_id)
        
        if (nrow(platform_row) > 0) {
          platform_row$platform_name_english[1]
        } else {
          platform_id  # Fallback to platform_id if not found
        }
      } else {
        platform_id  # Fallback if df_platform not available
      }
    }, error = function(e) {
      platform_id  # Fallback on any error
    })
    
    # Show notification with the platform name
    showNotification(paste("Switched to", platform_name), type="message", duration=3)
    
    # Log platform switch using tbl2-compatible approach
    tryCatch({
      if (!is.null(app_connection)) {
        # Example of using tbl2 for logging (if log table exists)
        if (any(grepl("system_log", DBI::dbListTables(app_connection)))) {
          log_data <- data.frame(
            timestamp = Sys.time(),
            event = "platform_switch",
            details = platform_name,
            user_id = session$user
          )
          tbl2(log_data) # Would typically be collected and saved to db
        }
      }
    }, error = function(e) {
      # Silently handle any logging errors
    })
  }, ignoreInit = TRUE)
}

# ---- 7. Run --------------------------------------------------------------
shinyApp(ui, server)
