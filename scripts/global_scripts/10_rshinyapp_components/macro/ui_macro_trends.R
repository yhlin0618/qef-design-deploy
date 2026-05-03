#' Macro Trends UI Component
#'
#' @param id The ID of the module
#' @return A UI element
#' @export
macroTrendsUI <- function(id) {
  ns <- NS(id)
  
  nav_panel(
    title = "Trends",
    icon = icon("chart-area"),
    h2("Market Trends"),
    fluidRow(
      column(
        width = 12,
        card(
          card_header("Purchase Timing"),
          plotOutput(ns("time_plot"))
        )
      )
    )
  )
}