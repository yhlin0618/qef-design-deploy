#' Micro Section Sidebar UI Component
#'
#' This component provides sidebar UI elements specific to the micro-level customer 
#' analysis section of the application.
#'
#' IMPORTANT: According to the UI-Server Pairing Rule, this UI component MUST be used with
#' its corresponding server component sidebarMicroServer(). All outputs defined here
#' must be fulfilled by the server component to avoid broken displays.
#'
#' @param id The module ID
#'
#' @return A sidebar UI component
#' @export
sidebarMicroUI <- function(id) {
  ns <- NS(id)
  
  sidebar(
    title = "顧客篩選",
    
    # Customer search
    textInput(
      inputId = ns("customer_search"),
      label = "顧客搜尋",
      placeholder = "輸入顧客 ID 或名稱"
    ),
    
    # Customer segment selection
    selectInput(
      inputId = ns("customer_segment"),
      label = "顧客區隔",
      choices = NULL, # Will be populated by server
      multiple = TRUE,
      width = "100%"
    ),
    
    # RFM filters
    sliderInput(
      inputId = ns("recency_filter"),
      label = "最近購買 (R)",
      min = 0,
      max = 365,
      value = c(0, 365),
      step = 1
    ),
    
    sliderInput(
      inputId = ns("frequency_filter"),
      label = "購買頻率 (F)",
      min = 0,
      max = 50,
      value = c(0, 50),
      step = 1
    ),
    
    sliderInput(
      inputId = ns("monetary_filter"),
      label = "購買金額 (M)",
      min = 0,
      max = 10000,
      value = c(0, 10000),
      step = 100
    ),
    
    # Customer lifecycle stage
    checkboxGroupInput(
      inputId = ns("lifecycle_stage"),
      label = "生命週期階段",
      choices = NULL, # Will be populated by server
      width = "100%"
    ),
    
    # Reset filters button
    actionButton(
      inputId = ns("reset_filters"),
      label = "重設篩選條件",
      width = "100%",
      class = "btn-secondary"
    ),
    
    # Filter status
    uiOutput(ns("filter_status"))
  )
}