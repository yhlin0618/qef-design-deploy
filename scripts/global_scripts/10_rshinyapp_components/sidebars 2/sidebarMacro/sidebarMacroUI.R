#' Macro Section Sidebar UI Component
#'
#' This component provides sidebar UI elements specific to the macro-level analysis
#' section of the application, including aggregation and comparison options.
#'
#' IMPORTANT: According to the UI-Server Pairing Rule, this UI component MUST be used with
#' its corresponding server component sidebarMacroServer(). All outputs defined here
#' must be fulfilled by the server component to avoid broken displays.
#'
#' @param id The module ID
#'
#' @return A sidebar UI component
#' @export
sidebarMacroUI <- function(id) {
  ns <- NS(id)
  
  sidebar(
    title = "分析選項",
    
    # Metric selection
    selectInput(
      inputId = ns("primary_metric"),
      label = "主要指標",
      choices = NULL,  # Will be populated by server
      width = "100%"
    ),
    
    # Aggregation level
    radioButtons(
      inputId = ns("aggregation_level"),
      label = "資料彙總層級",
      choices = list(
        "產品類別 (Product Category)" = "product_category",
        "地區 (Region)" = "region",
        "通路 (Channel)" = "channel",
        "客戶區隔 (Customer Segment)" = "customer_segment"
      ),
      selected = "product_category"
    ),
    
    # Comparison options
    checkboxInput(
      inputId = ns("enable_comparison"),
      label = "啟用比較",
      value = FALSE
    ),
    
    # Comparison type (conditionally shown)
    conditionalPanel(
      condition = "input.enable_comparison == true",
      ns = NS(ns("enable_comparison")),
      
      selectInput(
        inputId = ns("comparison_type"),
        label = "比較類型",
        choices = list(
          "同期比較 (Year-over-Year)" = "yoy",
          "上期比較 (Period-over-Period)" = "pop",
          "目標比較 (Target Comparison)" = "target"
        ),
        selected = "yoy"
      )
    ),
    
    # Target value (conditionally shown)
    conditionalPanel(
      condition = "input.enable_comparison == true && input.comparison_type == 'target'",
      ns = NS(ns("comparison_type")),
      
      numericInput(
        inputId = ns("target_value"),
        label = "目標值",
        value = 100000,
        min = 0
      )
    ),
    
    # Chart type selection
    selectInput(
      inputId = ns("chart_type"),
      label = "圖表類型",
      choices = list(
        "長條圖 (Bar Chart)" = "bar",
        "折線圖 (Line Chart)" = "line",
        "圓餅圖 (Pie Chart)" = "pie",
        "熱力圖 (Heat Map)" = "heatmap",
        "散佈圖 (Scatter Plot)" = "scatter"
      ),
      selected = "bar"
    ),
    
    # Data display options
    checkboxGroupInput(
      inputId = ns("display_options"),
      label = "顯示選項",
      choices = list(
        "顯示數據標籤 (Show Data Labels)" = "show_labels",
        "顯示趨勢線 (Show Trend Line)" = "show_trend",
        "顯示平均線 (Show Average Line)" = "show_average",
        "包含前一期 (Include Previous Period)" = "include_previous"
      ),
      selected = c("show_labels")
    ),
    
    # Apply button
    actionButton(
      inputId = ns("apply_settings"),
      label = "套用設定",
      width = "100%",
      class = "btn-primary"
    )
  )
}