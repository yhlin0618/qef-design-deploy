#' Main Sidebar UI Component
#'
#' This component provides the main sidebar UI elements with common filters 
#' used across different app sections.
#'
#' IMPORTANT: According to the UI-Server Pairing Rule, this UI component MUST be used with
#' its corresponding server component sidebarMainServer(). All outputs defined here
#' must be fulfilled by the server component to avoid broken displays.
#'
#' @param id The module ID
#'
#' @return A sidebar UI component
#' @export
sidebarMainUI <- function(id) {
  ns <- NS(id)
  
  sidebar(
    title = "基本選項",
    
    # Distribution channel selection
    radioButtons(
      inputId = ns("distribution_channel"),
      label = "行銷通路",
      choices = list(
        "Amazon" = "amazon",
        "Official Website" = "officialwebsite"
      ),
      selected = "officialwebsite",
      width = "100%"
    ),
    
    # Product category selection
    selectInput(
      inputId = ns("product_category"),
      label = "商品種類",
      choices = NULL, # Will be populated by server
      width = "100%"
    ),
    
    # Time scale selection
    conditionalPanel(
      condition = "input.nav !== 'micro'",
      selectInput(
        inputId = ns("time_scale"),
        label = "時間尺度",
        choices = list(
          "Year" = "year", 
          "Quarter" = "quarter", 
          "Month" = "month"
        ),
        selected = "quarter"
      )
    ),
    
    # Geographic filter
    conditionalPanel(
      condition = "input.nav !== 'micro'",
      selectizeInput(
        inputId = ns("geographic_region"), 
        label = "地區",
        choices = NULL, # Will be populated by server
        multiple = FALSE,
        options = list(plugins = list('remove_button', 'drag_drop'))
      )
    ),
    
    # Date range input
    dateRangeInput(
      inputId = ns("date_range"),
      label = "日期範圍",
      start = Sys.Date() - 365,
      end = Sys.Date(),
      separator = " to "
    ),
    
    # Status indicator for filters
    uiOutput(ns("filter_status"))
  )
}