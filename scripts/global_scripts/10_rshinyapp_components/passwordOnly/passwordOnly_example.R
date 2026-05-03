#!/usr/bin/env Rscript
#' Password-Only Login Module Example
#' 
#' This example demonstrates how to use the passwordOnly module
#' 
#' Setup:
#' 1. Set environment variable: Sys.setenv(APP_PASSWORD = "your_password")
#' 2. Run this script
#' 
#' Features demonstrated:
#' - Simple password authentication
#' - Login attempt limiting and lockout
#' - Environment variable configuration
#' - Logout functionality

library(shiny)
library(bs4Dash)
library(shinyjs)

# Source the module files
source("passwordOnlyUI.R")
source("passwordOnlyServer.R")

# Set password via environment variable (for demo purposes)
# In production, set this in your deployment environment

# Example 1: Single password (backward compatible)
# Sys.setenv(APP_PASSWORD = "demo123")

# Example 2: Multiple passwords using R vector format
Sys.setenv(APP_PASSWORD = 'c("demo123","admin","guest")')

# Example 3: Multiple passwords using comma-separated format
# Sys.setenv(APP_PASSWORD = "demo123,admin,guest")

# Example 4: Multiple passwords using JSON format (requires jsonlite)
# Sys.setenv(APP_PASSWORD = '["demo123","admin","guest"]')

# Set up resource paths (IMPORTANT: must be done before UI definition)
# This allows serving static files like images, CSS, etc.
if (dir.exists("www")) {
  addResourcePath("www", "www")
}

# If using global_scripts assets
if (dir.exists("scripts/global_scripts/24_assets")) {
  addResourcePath("assets", "scripts/global_scripts/24_assets")
}

# Example Shiny App
ui <- bs4DashPage(
  title = "Password Protected App",
  
  # Include shinyjs
  header = bs4DashNavbar(
    title = "Protected Application",
    skin = "light",
    status = "primary"
  ),
  
  sidebar = bs4DashSidebar(disable = TRUE),
  
  body = bs4DashBody(
    useShinyjs(),
    
    # Login UI
    passwordOnlyUI(
      id = "login",
      app_title = "精準行銷平台 - 多密碼登入範例",
      app_icon = NULL,  # You can add your icon path here
      password_label = "請輸入系統密碼 (demo123/admin/guest)",
      submit_label = "登入系統",
      primary_color = "#17a2b8"
    ),
    
    # Main content (hidden until logged in)
    hidden(
      div(id = "main_content",
          fluidRow(
            column(12,
                   bs4Card(
                     title = "歡迎使用系統",
                     status = "primary",
                     solidHeader = TRUE,
                     width = 12,
                     h3("您已成功登入！"),
                     p("這是受密碼保護的內容區域。"),
                     br(),
                     actionButton("logout_btn", "登出", class = "btn-danger")
                   )
            )
          ),
          
          fluidRow(
            column(6,
                   bs4Card(
                     title = "系統資訊",
                     status = "info",
                     solidHeader = TRUE,
                     p("登入時間：", textOutput("login_time", inline = TRUE)),
                     p("系統版本：1.0.0")
                   )
            ),
            column(6,
                   bs4Card(
                     title = "功能區塊",
                     status = "success",
                     solidHeader = TRUE,
                     p("在這裡可以放置您的應用程式主要功能。"),
                     actionButton("demo_btn", "示範按鈕", class = "btn-success")
                   )
            )
          )
      )
    )
  )
)

server <- function(input, output, session) {
  
  # Initialize login module
  auth <- passwordOnlyServer(
    id = "login",
    password_env_var = "APP_PASSWORD",
    max_attempts = 3,
    lockout_duration = 300  # 5 minutes
  )
  
  # Show/hide main content based on login status
  observe({
    if (auth$logged_in()) {
      shinyjs::show("main_content")
      output$login_time <- renderText({
        format(Sys.time(), "%Y-%m-%d %H:%M:%S")
      })
    } else {
      shinyjs::hide("main_content")
    }
  })
  
  # Handle logout
  observeEvent(input$logout_btn, {
    auth$logout()
    showNotification("您已成功登出", type = "message")
  })
  
  # Demo button
  observeEvent(input$demo_btn, {
    showNotification("這是一個示範功能", type = "success")
  })
}

# Run the app
shinyApp(ui = ui, server = server)