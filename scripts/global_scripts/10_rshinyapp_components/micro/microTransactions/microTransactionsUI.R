#' Micro Transactions UI Component
#'
#' @param id The ID of the module
#' @return A UI element
#' @export
microTransactionsUI <- function(id) {
  ns <- NS(id)
  
  nav_panel(
    title = "Transactions",
    icon = icon("credit-card"),
    h2("Transaction Analysis"),
    fluidRow(
      column(
        width = 4,
        card(
          card_header("Filter"),
          sliderInput(ns("revenue_filter"), "Min Revenue", min = 0, max = 10000, value = 0)
        )
      ),
      column(
        width = 8,
        card(
          card_header("Transaction Data"),
          plotOutput(ns("transaction_scatter"))
        )
      )
    )
  )
}