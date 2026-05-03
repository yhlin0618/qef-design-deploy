# UI Module for Union Sidebar
# Following P21 component n-tuple pattern and NSQL set theoretical naming

#' Sidebar UI with Union of Common and Tab-Specific Filters
#'
#' Creates a sidebar UI component that unifies common filters with tab-specific filter sections.
#' Using set theoretical "Union" concept to represent combination of multiple filter sets.
#'
#' @param id The module ID
#' @return A dashboardSidebar component with common and tab-specific filter sections
#' @export
sidebarUnionUI <- function(id) {
  ns <- NS(id)
  
  bs4Dash::dashboardSidebar(
    fixed = TRUE,
    skin = "light",
    status = "primary",
    elevation = 3,
    
    # Filters header
    div(
      class = "filters-header",
      style = "padding: 15px; border-bottom: 1px solid rgba(60, 141, 188, 0.2); margin-bottom: 15px;",
      h3(
        style = "margin: 0; font-size: 18px; font-weight: 600;",
        translate("Application Settings")
      )
    ),
    
    # Common filters that appear in all tabs
    div(
      id = ns("common_filters"),
      class = "common-filters",
      style = "padding: 10px 15px; margin-bottom: 10px; border-bottom: 1px dashed #eee;",
      
      # Marketing Channel filter - common to all tabs
      h4(translate("Marketing Channel"), style = "color: #007bff;"),
      radioButtons(
        ns("common_channel"), 
        translate("Select Channel:"), 
        choices = c("Amazon" = "amazon", "Official Website" = "officialwebsite"),
        selected = "amazon"
      ),
      
      # Product Category filter - common to all tabs
      h4(translate("Product Category"), style = "color: #007bff;"),
      selectInput(
        ns("common_category"), 
        translate("Select Category:"), 
        choices = c("All Products" = "000", "Kitchen" = "001", "Home" = "002", "Electronics" = "003"),
        selected = "000"
      )
    ),
    
    # Filters for Micro tab (specific to this tab)
    div(
      id = ns("micro_filters"),
      class = "sidebar-filters",
      style = "padding: 10px 15px;",
      
      # Tab header in bold
      tags$div(
        tags$strong(paste0(translate("Micro Analysis Filters:"), " "), style = "color: #007bff; font-size: 14px;"),
        style = "margin-bottom: 10px;"
      ),
      
      # Tab-specific filters
      h4(translate("Region"), style = "color: #007bff;"),
      selectInput(
        ns("micro_region"), 
        translate("Select Region:"), 
        choices = c("All Regions" = "000", "North America" = "001", "Europe" = "002", "Asia" = "003"),
        selected = "000"
      ),
      
      # Small note to explain automatic filtering
      helpText(translate("Filters apply automatically"), 
               style = "font-style: italic; color: #6c757d; margin-top: 15px; text-align: center;")
    ),
    
    # Filters for Macro tab (initially hidden)
    # Using CSS display property per P22: CSS Controls Over Shiny Conditionals
    div(
      id = ns("macro_filters"),
      class = "sidebar-filters",
      style = "padding: 10px 15px; display: none;",
      
      # Tab header in bold
      tags$div(
        tags$strong(paste0(translate("Macro Analysis Filters:"), " "), style = "color: #28a745; font-size: 14px;"),
        style = "margin-bottom: 10px;"
      ),
      
      # Tab-specific filters
      h4(translate("Aggregation"), style = "color: #28a745;"),
      selectInput(
        ns("macro_aggregation"), 
        translate("Aggregation Level:"), 
        choices = c("Product Category", "Region", "Channel", "Customer Segment"),
        selected = "Product Category"
      ),
      
      h4(translate("Comparison"), style = "color: #28a745;"),
      checkboxInput(
        ns("macro_comparison"),
        translate("Enable Comparison"),
        value = FALSE
      ),
      
      h4(translate("Time Range"), style = "color: #28a745;"),
      dateRangeInput(
        ns("macro_daterange"),
        translate("Select Period:"),
        start = Sys.Date() - 90,
        end = Sys.Date()
      ),
      
      # Small note to explain automatic filtering
      helpText(translate("Changes apply automatically"), 
               style = "font-style: italic; color: #6c757d; margin-top: 15px; text-align: center;")
    ),
    
    # Filters for Target tab (initially hidden)
    # Using CSS display property per P22: CSS Controls Over Shiny Conditionals
    div(
      id = ns("target_filters"),
      class = "sidebar-filters",
      style = "padding: 10px 15px; display: none;",
      
      # Tab header in bold
      tags$div(
        tags$strong(paste0(translate("Target Marketing Filters:"), " "), style = "color: #dc3545; font-size: 14px;"),
        style = "margin-bottom: 10px;"
      ),
      
      # Tab-specific filters
      h4(translate("Campaign"), style = "color: #dc3545;"),
      selectInput(
        ns("target_campaign"), 
        translate("Select Campaign:"), 
        choices = c("Summer Promotion", "New Product Launch", "Black Friday"),
        selected = "Summer Promotion"
      ),
      
      h4(translate("Target Audience"), style = "color: #dc3545;"),
      checkboxGroupInput(
        ns("target_audience"),
        translate("Select Segments:"),
        choices = c("New Customers", "Returning Customers", "VIP"),
        selected = "New Customers"
      ),
      
      # Small note to explain automatic filtering
      helpText(translate("Filters apply instantly"), 
               style = "font-style: italic; color: #6c757d; margin-top: 15px; text-align: center;")
    ),
    
    # HIDDEN-FEATURE (2025-04-07): Settings button at bottom of sidebar
    # This feature is hidden until user testing confirms sidebar settings approach
    # Will be enabled after UX review determines if settings should be in sidebar or main navbar
    # Expected implementation date: 2025-04-30
    # div(
    #   style = "position: absolute; bottom: 0; width: 100%; padding: 15px;",
    #   actionButton(
    #     inputId = ns("sidebar_settings"),
    #     label = translate("Settings"),
    #     icon = icon("gear"),
    #     width = "100%",
    #     class = "btn-primary"
    #   )
    # )
  )
}