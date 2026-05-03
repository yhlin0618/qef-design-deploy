#' @principle R91 Universal Data Access Pattern
#' @principle R92 Universal DBI Approach
#' @principle P77 Performance Optimization
#' @principle R94 Roxygen2 Function Examples Standard
#'
#' Test script for microTransactions module demonstrating the Universal Data Access Pattern (R91)
#' and Universal DBI Approach (R92).

library(shiny)
library(dplyr)
library(ggplot2)
library(DBI)

# Load required functions and module components
source("update_scripts/global_scripts/00_principles/02_db_utils/fn_universal_data_accessor.R")
source("update_scripts/global_scripts/10_rshinyapp_components/micro/microTransactions/microTransactionsUI.R")
source("update_scripts/global_scripts/10_rshinyapp_components/micro/microTransactions/microTransactionsServer.R")

# Create example data for testing
test_data <- list(
  transactions = data.frame(
    customer_id = rep(1:10, each = 3),
    transaction_id = 1:30,
    revenue = round(runif(30, min = 100, max = 1000), 2),
    transactions = sample(1:10, 30, replace = TRUE),
    segment = sample(c("New", "Regular", "VIP"), 30, replace = TRUE),
    date = sample(seq(as.Date("2023-01-01"), as.Date("2023-12-31"), by = "day"), 30)
  )
)

# Create a UI with input controls to demonstrate module functionality
ui <- fluidPage(
  titlePanel("microTransactions Module Test"),
  
  sidebarLayout(
    sidebarPanel(
      # Connection type selector
      selectInput("connection_type", "Data Connection Type:", 
                 choices = c("Direct List" = "list", 
                             "Function-based" = "function",
                             "Mock DBI" = "dbi")),
      
      # Revenue filter (used by the module)
      sliderInput("revenue_filter", "Minimum Revenue Filter:",
                 min = 100, max = 1000, value = 300),
      
      # Display information about the active connection
      verbatimTextOutput("connection_info")
    ),
    
    mainPanel(
      # Place the microTransactions module UI here
      plotOutput("transaction_scatter")
    )
  )
)

# Server logic including the module implementation
server <- function(input, output, session) {
  # Create dynamic data connection based on user selection
  data_connection <- reactive({
    # Get the selected connection type
    conn_type <- input$connection_type
    
    # Create different connection types for demonstration
    switch(conn_type,
           "list" = {
             # Direct list connection - simplest case
             test_data
           },
           "function" = {
             # Function-based connection - data accessed through functions
             func_conn <- list(
               get_transactions = function() {
                 test_data$transactions
               }
             )
             func_conn
           },
           "dbi" = {
             # Mock DBI connection - simulates database access
             create_mock_connection(test_data, "dbi") 
           },
           # Default fallback
           test_data
    )
  })
  
  # Display information about the current connection
  output$connection_info <- renderPrint({
    conn <- data_connection()
    cat("Current connection type:", input$connection_type, "\n")
    cat("Connection class:", class(conn)[1], "\n")
    
    if (input$connection_type == "dbi") {
      cat("Available tables:", paste(conn$tables, collapse = ", "), "\n")
    } else if (input$connection_type == "function") {
      cat("Available functions:", paste(names(conn), collapse = ", "), "\n")
    } else {
      cat("Available data:", paste(names(conn), collapse = ", "), "\n")
    }
  })
  
  # Initialize the microTransactions module with the selected connection
  microTransactionsServer("transactions", data_connection)
}

# Run the test application
shinyApp(ui, server)