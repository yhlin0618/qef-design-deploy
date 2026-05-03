# ============================================================
# periodComparisonUI.R
# Period Comparison UI Component for MAMBA
# Created: 2025-09-23
# Principle: R09 (UI-Server-Defaults Triple)
# ============================================================

#' Period Comparison UI Module
#' 
#' @param id Module namespace ID
#' @param width Width of the component (default "100%")
#' @return Shiny UI component for period comparison
#' 
#' @description
#' Provides UI for selecting and comparing different time periods.
#' Implements rolling period comparison with YoY and MoM options.
#' 
#' @export
periodComparisonUI <- function(id, width = "100%") {
  ns <- NS(id)
  
  tagList(
    # CSS for period comparison styling
    tags$style(HTML(sprintf("
      #%s .period-selector {
        padding: 10px;
        background-color: #f8f9fa;
        border-radius: 5px;
        margin-bottom: 15px;
      }
      #%s .period-badge {
        font-size: 0.875rem;
        padding: 0.25rem 0.5rem;
        margin-left: 0.5rem;
      }
      #%s .trend-up { color: #28a745; }
      #%s .trend-down { color: #dc3545; }
      #%s .trend-neutral { color: #6c757d; }
    ", ns(""), ns(""), ns(""), ns(""), ns("")))),
    
    # Period selection controls
    div(class = "period-selector",
      fluidRow(
        column(4,
          selectInput(
            inputId = ns("period_type"),
            label = "時間段類型",
            choices = list(
              "每日" = "daily",
              "每週" = "weekly",
              "每月" = "monthly",
              "每季" = "quarterly",
              "每年" = "yearly"
            ),
            selected = "monthly",
            width = "100%"
          )
        ),
        column(4,
          selectInput(
            inputId = ns("comparison_type"),
            label = "比較方式",
            choices = list(
              "環比（與上期）" = "period_over_period",
              "同比（與去年同期）" = "year_over_year",
              "自訂比較" = "custom"
            ),
            selected = "period_over_period",
            width = "100%"
          )
        ),
        column(4,
          conditionalPanel(
            condition = sprintf("input['%s'] == 'custom'", ns("comparison_type")),
            numericInput(
              inputId = ns("periods_back"),
              label = "往回滾動期數",
              value = 1,
              min = 1,
              max = 12,
              step = 1,
              width = "100%"
            )
          )
        )
      ),
      
      # Date range selector
      fluidRow(
        column(6,
          dateRangeInput(
            inputId = ns("date_range"),
            label = "選擇日期範圍",
            start = Sys.Date() - 90,
            end = Sys.Date(),
            format = "yyyy-mm-dd",
            language = "zh-TW",
            width = "100%"
          )
        ),
        column(6,
          br(),
          actionButton(
            inputId = ns("apply_comparison"),
            label = "套用比較",
            icon = icon("sync"),
            class = "btn-primary btn-block"
          )
        )
      )
    ),
    
    # Comparison results display
    div(
      id = ns("comparison_results"),
      
      # Summary cards
      fluidRow(
        column(4,
          uiOutput(ns("current_period_card"))
        ),
        column(4,
          uiOutput(ns("previous_period_card"))
        ),
        column(4,
          uiOutput(ns("change_card"))
        )
      ),
      
      # Detailed comparison table
      br(),
      DT::dataTableOutput(ns("comparison_table"), width = width),
      
      # Trend visualization
      br(),
      plotlyOutput(ns("trend_chart"), width = width, height = "400px")
    )
  )
}