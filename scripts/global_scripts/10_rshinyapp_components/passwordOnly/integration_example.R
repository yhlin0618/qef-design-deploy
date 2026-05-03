#!/usr/bin/env Rscript
#' Integration Example: passwordOnly with union_production_test.R
#' 
#' This example shows how to add password protection to an existing app
#' Based on the union_production_test.R structure

# ---- 0. 初始化和套件載入 ----------------------------------------------------
library(shiny)
library(bs4Dash)
library(shinyjs)
library(DBI)
library(dplyr)

# 設定密碼（部署時使用環境變數）
Sys.setenv(APP_PASSWORD = "mamba2025")

# ---- 1. 資源路徑設定（重要：必須在 UI 定義前） -------------------------------
# 設定 global_scripts 資源路徑
if (dir.exists("scripts/global_scripts/24_assets")) {
  addResourcePath("assets", "scripts/global_scripts/24_assets")
}

# 設定本地資源路徑
if (dir.exists("www")) {
  addResourcePath("www", "www")
}

# ---- 2. 載入必要模組 -------------------------------------------------------
# 載入密碼登入模組
source("scripts/global_scripts/10_rshinyapp_components/passwordOnly/passwordOnlyUI.R")
source("scripts/global_scripts/10_rshinyapp_components/passwordOnly/passwordOnlyServer.R")

# 載入其他必要的模組（根據您的應用需求）
# source("scripts/global_scripts/02_db_utils/fn_tbl2.R")
# source("scripts/global_scripts/10_rshinyapp_components/micro/microCustomer/microCustomer2.R")

# ---- 3. CSS 樣式 -----------------------------------------------------------
css_deps <- tags$head(
  tags$style(HTML("
    /* 確保登入後的內容填滿整個畫面 */
    #main_app_content {
      width: 100%;
      height: 100vh;
    }
    
    /* 登入成功後的過渡動畫 */
    .fade-in {
      animation: fadeIn 0.5s ease-in;
    }
    
    @keyframes fadeIn {
      from { opacity: 0; }
      to { opacity: 1; }
    }
  "))
)

# ---- 4. UI 定義 ------------------------------------------------------------
ui <- tagList(
  useShinyjs(),
  css_deps,
  
  # 密碼登入介面
  passwordOnlyUI(
    id = "auth",
    app_title = "AI Marketing Platform - MAMBA",
    app_icon = "assets/icons/mamba_icon.png",
    password_label = "請輸入系統密碼",
    submit_label = "進入系統",
    primary_color = "#17a2b8"
  ),
  
  # 主要應用程式內容（初始隱藏）
  hidden(
    div(
      id = "main_app_content",
      class = "fade-in",
      
      # 這裡放入原本 union_production_test.R 的 UI
      bs4DashPage(
        title = "AI Marketing Platform",
        fullscreen = TRUE,
        
        header = bs4DashNavbar(
          title = bs4DashBrand("AI Marketing Platform"),
          skin = "light",
          status = "primary",
          rightUi = tagList(
            # 登出按鈕
            actionButton("logout_btn", "登出", 
                        class = "btn-sm btn-danger",
                        style = "margin-right: 10px;")
          )
        ),
        
        sidebar = bs4DashSidebar(
          status = "primary",
          width = "300px",
          sidebarMenu(
            id = "sidebar_menu",
            bs4SidebarHeader("Precision Marketing Suite"),
            bs4SidebarMenuItem("Dashboard", tabName = "dashboard", icon = icon("dashboard")),
            bs4SidebarMenuItem("Customer DNA", tabName = "customer", icon = icon("users")),
            bs4SidebarMenuItem("Analytics", tabName = "analytics", icon = icon("chart-line"))
          )
        ),
        
        body = bs4DashBody(
          bs4TabItems(
            bs4TabItem(
              tabName = "dashboard",
              fluidRow(
                column(12,
                  bs4Card(
                    title = "歡迎使用 MAMBA 系統",
                    status = "primary",
                    width = 12,
                    solidHeader = TRUE,
                    h3("您已成功登入系統"),
                    p("這是受密碼保護的精準行銷平台。")
                  )
                )
              )
            ),
            
            bs4TabItem(
              tabName = "customer",
              fluidRow(
                column(12,
                  bs4Card(
                    title = "Customer DNA Analysis",
                    status = "primary",
                    width = 12,
                    solidHeader = TRUE,
                    p("顧客 DNA 分析功能")
                  )
                )
              )
            ),
            
            bs4TabItem(
              tabName = "analytics",
              fluidRow(
                column(12,
                  bs4Card(
                    title = "Analytics Dashboard",
                    status = "info",
                    width = 12,
                    solidHeader = TRUE,
                    p("數據分析儀表板")
                  )
                )
              )
            )
          )
        ),
        
        footer = dashboardFooter(
          fixed = TRUE,
          right = "Version 1.0.0 | 2025"
        )
      )
    )
  )
)

# ---- 5. Server 邏輯 --------------------------------------------------------
server <- function(input, output, session) {
  
  # 初始化密碼驗證模組
  auth <- passwordOnlyServer(
    id = "auth",
    password_env_var = "APP_PASSWORD",
    max_attempts = 3,
    lockout_duration = 300
  )
  
  # 監控登入狀態
  observe({
    if (auth$logged_in()) {
      # 登入成功：顯示主要應用程式
      shinyjs::show("main_app_content", anim = TRUE)
      
      # 顯示歡迎通知
      showNotification(
        "歡迎使用 MAMBA 精準行銷平台",
        type = "success",
        duration = 5
      )
      
      # 這裡可以初始化資料庫連接等
      # app_connection <- dbConnect(...)
      
    } else {
      # 未登入：隱藏主要應用程式
      shinyjs::hide("main_app_content")
    }
  })
  
  # 處理登出
  observeEvent(input$logout_btn, {
    # 呼叫登出函數
    auth$logout()
    
    # 顯示登出通知
    showNotification(
      "您已成功登出系統",
      type = "message",
      duration = 3
    )
    
    # 清理資源（如果有的話）
    # if (exists("app_connection")) {
    #   dbDisconnect(app_connection)
    # }
  })
  
  # 這裡放入原本的 server 邏輯
  # 例如：component instances, reactive values, etc.
  
}

# ---- 6. 執行應用程式 -------------------------------------------------------
shinyApp(ui = ui, server = server)