#' Target Section Sidebar UI Component
#'
#' This component provides sidebar UI elements specific to the target marketing
#' section of the application, focused on campaign creation and management.
#'
#' IMPORTANT: According to the UI-Server Pairing Rule, this UI component MUST be used with
#' its corresponding server component sidebarTargetServer(). All outputs defined here
#' must be fulfilled by the server component to avoid broken displays.
#'
#' @param id The module ID
#'
#' @return A sidebar UI component
#' @export
sidebarTargetUI <- function(id) {
  ns <- NS(id)
  
  sidebar(
    title = "行銷活動規劃",
    
    # Campaign selection
    selectInput(
      inputId = ns("campaign_selector"),
      label = "選擇活動",
      choices = NULL,  # Will be populated by server
      width = "100%"
    ),
    
    # New campaign button
    actionButton(
      inputId = ns("new_campaign"),
      label = "建立新活動",
      icon = icon("plus"),
      width = "100%",
      class = "btn-success mb-3"
    ),
    
    # Target audience selection
    selectInput(
      inputId = ns("target_audience"),
      label = "目標客群",
      choices = NULL,  # Will be populated by server
      width = "100%"
    ),
    
    # Campaign type
    radioButtons(
      inputId = ns("campaign_type"),
      label = "活動類型",
      choices = list(
        "促銷折扣 (Promotional)" = "promotional",
        "新產品發佈 (New Product)" = "new_product",
        "會員專屬 (Loyalty)" = "loyalty",
        "重新吸引 (Reengagement)" = "reengagement",
        "季節性 (Seasonal)" = "seasonal"
      ),
      selected = "promotional"
    ),
    
    # Channel selection
    checkboxGroupInput(
      inputId = ns("campaign_channels"),
      label = "行銷渠道",
      choices = list(
        "電子郵件 (Email)" = "email",
        "簡訊 (SMS)" = "sms",
        "推播通知 (Push)" = "push",
        "社群媒體 (Social)" = "social",
        "網頁橫幅 (Web)" = "web"
      ),
      selected = c("email")
    ),
    
    # Campaign budget slider
    sliderInput(
      inputId = ns("campaign_budget"),
      label = "預算 (美元)",
      min = 1000,
      max = 100000,
      value = 10000,
      step = 1000,
      pre = "$"
    ),
    
    # Campaign dates
    dateRangeInput(
      inputId = ns("campaign_dates"),
      label = "活動日期",
      start = Sys.Date() + 7,  # Default to start in one week
      end = Sys.Date() + 21,   # Default to two-week campaign
      separator = " to "
    ),
    
    # Projected metrics
    conditionalPanel(
      condition = "input.campaign_selector != 'new'",
      wellPanel(
        h4("預估指標", style = "font-size: 16px; margin-top: 0;"),
        uiOutput(ns("projected_metrics"))
      )
    ),
    
    # Save/update button
    actionButton(
      inputId = ns("save_campaign"),
      label = "儲存活動設定",
      width = "100%",
      class = "btn-primary"
    )
  )
}