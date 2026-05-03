# Server Module for Common Sidebar
# Following P21 component n-tuple pattern

#' Sidebar Server Function
#'
#' Server logic for the sidebar with common filters across tabs
#'
#' @param id The module ID
#' @param active_tab Reactive expression that returns the currently active tab
#' @return A module server function
#' @export
sidebarCommonServer <- function(id, active_tab) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns
    
    # Activate the appropriate filter section based on active tab
    observe({
      current_tab <- active_tab()
      
      # Hide all tab-specific filters
      shinyjs::hide(selector = paste0("#", ns("micro_filters")))
      shinyjs::hide(selector = paste0("#", ns("macro_filters")))
      shinyjs::hide(selector = paste0("#", ns("target_filters")))
      
      # Show the appropriate filter section based on active tab
      if (current_tab == "micro_tab") {
        shinyjs::show(id = "micro_filters")
      } else if (current_tab == "macro_tab") {
        shinyjs::show(id = "macro_filters")
      } else if (current_tab == "target_tab") {
        shinyjs::show(id = "target_filters")
      }
    })
    
    # Return reactive values for all filter inputs
    return(reactive({
      # Common filters
      common_filters <- list(
        channel = input$common_channel,
        category = input$common_category
      )
      
      # Tab-specific filters - include all even though only one set is active at a time
      tab_filters <- list(
        micro = list(
          region = input$micro_region
        ),
        macro = list(
          aggregation = input$macro_aggregation,
          comparison = input$macro_comparison,
          date_range = input$macro_daterange
        ),
        target = list(
          campaign = input$target_campaign,
          audience = input$target_audience
        )
      )
      
      # Return combined filters
      list(
        common = common_filters,
        tab_specific = tab_filters
      )
    }))
  })
}