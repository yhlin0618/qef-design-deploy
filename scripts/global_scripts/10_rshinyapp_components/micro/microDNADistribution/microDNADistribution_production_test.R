# =============================================================
# customer_dna_production_app_refactored.R
# 2025‑04‑19 – 更新為使用 tbl2 進行數據存取 (R116 Enhanced Data Access)
# 2025‑04‑17 – 清理結構 + 動態 Filter 區塊 (dynamic_filter)
# 2025-04-24 – 修正 Platform 選項處理邏輯 (按照 MP081 原則)
# =============================================================

# IMPORTANT CODE MAINTENANCE NOTES:
# 1. radioButtons parameters MUST be named explicitly (inputId, label, choices, selected) per MP081
# 2. Platform switch handling MUST use consistent variable names throughout the observer
# 3. Any variables used in notification must also be updated in the logging section
# 4. Currently supported platforms: Amazon (amz) and All Platforms (all)
# =============================================================

# ---- 0. 初始化 ------------------------------------------------------------
find_and_set_project_root <- function(){
  p <- getwd();
  while(!endsWith(p,"precision_marketing_app") && !file.exists(file.path(p,"precision_marketing_app"))){
    nxt <- dirname(p); if(nxt==p){ warning("找不到 precision_marketing_app 目錄"); return(FALSE) }; p <- nxt }
  if(!endsWith(p,"precision_marketing_app")) p <- file.path(p,"precision_marketing_app")
  if(dir.exists(p)){ setwd(p); message("工作目錄已設定為: ", p); TRUE } else FALSE }
find_and_set_project_root()
autoinit()
stopifnot(exists("PACKAGES_INITIALIZED"), is.list(PACKAGES_INITIALIZED))

# ---- 1. 套件 --------------------------------------------------------------
library(shiny); library(bs4Dash); library(plotly); library(dplyr); library(shinyjs); library(DBI)

# ---- 2. 連線 --------------------------------------------------------------
# Connect to the app database using the Enhanced Data Access approach (R116)
connect_to_app_database <- function(db_path = "app_data/app_data.duckdb") {
  tryCatch({
    if (!requireNamespace("DBI", quietly = TRUE)) {
      message("Installing DBI package...")
      install.packages("DBI")
    }
    
    if (!requireNamespace("duckdb", quietly = TRUE)) {
      message("Installing duckdb package...")
      install.packages("duckdb")
    }
    
    # Check if file exists
    if (!file.exists(db_path)) {
      stop("Database file does not exist at path: ", db_path)
    }
    
    # 1. 關閉所有先前的連線（如果有）
    #    並且釋放掉該 driver instance  
    if (exists("conn")) {
      DBI::dbDisconnect(conn, shutdown = TRUE)
    }
    
    # 2. 建立一個新的 DuckDB driver，並在這裡指定 dbdir 與 read_only = TRUE
    drv_ro <- duckdb::duckdb(
      dbdir     = db_path,     # 你的 .duckdb 檔案路徑
      read_only = TRUE         # 唯讀模式
    )
    
    # 3. 用這個唯讀 driver 建立連線
    conn_ro <- DBI::dbConnect(drv_ro)
    
    # 4. 驗證
    conn_ro
    # 應該會看到類似：
    # <duckdb_connection ... read_only=TRUE ...>
    
    return(conn_ro)
  }, error = function(e) {
    message("Error connecting to database: ", e$message)
    return(NULL)
  })
}

app_connection <- connect_to_app_database()

# ---- 3. Component 載入 ----------------------------------------------------
translate <- function(x) x   # 佔位翻譯函數

# ---- 4. CSS ---------------------------------------------------------------
css_deps <- tags$head(tags$style(HTML(
  ".sidebar .selectize-dropdown{width:auto!important;min-width:100%!important}.sidebar .selectize-dropdown-content{min-width:100%}"
)))

# ---- 5. UI ---------------------------------------------------------------
ui <- bs4DashPage(
  title = translate("AI Marketing Platform"), fullscreen = TRUE,
  header = bs4DashNavbar(title = translate("AI Marketing Platform"), skin="light", status="primary"),
  sidebar = bs4DashSidebar(status="primary", width="300px", elevation = 3,minified  = FALSE,
                           sidebarMenu(id="sidebar_menu",
                                       bs4SidebarHeader("Application Settings"),
                                       bs4SidebarMenuItem("Customer DNA",   tabName="microCustomer", icon=icon("users")),
                                       bs4SidebarMenuItem("DNA Distribution", tabName="dna",          icon=icon("chart-bar")) ),
                           # 共用 Platform Filter
                           div(class="sidebar-section p-3 mt-2",
                               h5("Platform", class="mb-3"),
                               # Following MP081: Explicit Parameter Specification Metaprinciple
                               radioButtons(
                                 inputId = "platform",
                                 label = NULL,
                                 choices = df_platform %>%
                                   filter(platform_id %in% c("all", names(app_configs$platforms))) %>%
                                   { setNames(.$platform_id, .$platform_name_english) },
                                 selected = "all"
                               ),
                               h5("Product line", class="mb-3"),
                               radioButtons(
                                 inputId = "product_line",
                                 label = NULL,
                                 choices = rbind(
                                   df_product_line[df_product_line$product_line_id == "all", , drop = FALSE],
                                   get_active_product_lines()
                                 ) %>% { setNames(.$product_line_id, .$product_line_name_english) },
                                 selected = "all"
                               )),
                           # 動態 filter 容器
                           uiOutput("dynamic_filter") ),
  
  body = bs4DashBody(css_deps, useShinyjs(),
                     bs4TabItems(
                       bs4TabItem(tabName="microCustomer", fluidRow(column(12, bs4Card(title="Customer DNA Analysis", status="primary", width=12, solidHeader=TRUE, elevation=3, uiOutput("cust_display"))))),
                       bs4TabItem(tabName="dna",          fluidRow(column(12, bs4Card(title="DNA Distribution",     status="primary", width=12, solidHeader=TRUE, elevation=3, uiOutput("dna_display"))))) ) ),
  
  footer = dashboardFooter(fixed=TRUE, right="Version 1.0.0 | 2025") )

# ---- 6. Server -----------------------------------------------------------
server <- function(input, output, session){
  # ---- 6.1 共享設定 ------------------------------------------------------
  comp_config <- reactive({
    list(
      filters    = list(platform_id = input$platform),
      active_tab = input$sidebar_menu        # ← 新增
    )
  })
  
  # ---- 6.2 Component instances -----------------------------------------
  customer_comp <- microCustomerComponent("cust", app_connection, comp_config, translate)
  dna_comp      <- microDNADistributionComponent("dna", app_connection, comp_config, translate)
  
  # ---- 6.3 動態 Filter 注入 --------------------------------------------
  output$dynamic_filter <- renderUI({ switch(input$sidebar_menu,
                                             "microCustomer" = customer_comp$ui$filter,
                                             "dna"           = dna_comp$ui$filter, NULL) })
  
  # ---- 6.4 Component UI -------------------------------------------------
  output$cust_display <- renderUI(customer_comp$ui$display)
  output$dna_display  <- renderUI(dna_comp$ui$display)
  
  # ---- 6.5 啟動 Component server ---------------------------------------
  cust_res <- customer_comp$server(input, output, session)
  dna_res  <- dna_comp$server(input, output, session)
  
  # ---- 6.55  切到 DNA 分頁時重置 microCustomer ---------------------------
  observeEvent(input$sidebar_menu, {
    if (input$sidebar_menu == "dna") {
      # 透過自訂訊息「點擊」 cust 模組裡的 clear_filter 按鈕
      session$sendCustomMessage(
        "shiny.button.click",
        list(id = "cust-clear_filter")  # cust = 模組 ID
      )
    }
  }, ignoreInit = TRUE)
  
  # ---- 6.6 通用通知 / 狀態 ---------------------------------------------
  observeEvent(input$platform, {
    # Map platform ID to platform name
    platform_map <- setNames(df_platform$platform_name_english, df_platform$platform_id)

    # Get the platform name using the platform ID as key
    platform_id <- as.character(input$platform)
    platform_name <- platform_map[platform_id]
    if (is.na(platform_name)) {
      platform_name <- platform_id
    }
    
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
  
  session$onSessionEnded(function(){ if(!is.null(app_connection)&&inherits(app_connection,"DBIConnection")) try(DBI::dbDisconnect(app_connection), silent=TRUE) })
}

# ---- 7. Run --------------------------------------------------------------
shinyApp(ui, server)
