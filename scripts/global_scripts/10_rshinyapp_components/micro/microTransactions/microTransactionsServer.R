#' Micro Transactions Server Component
#'
#' @param id The ID of the module
#' @param app_data_connection The data connection (supports Universal Data Access Pattern)
#' @return The module server function
#' @export
#' @implements R76 Module Data Connection Rule
#' @implements R91 Universal Data Access Pattern
#' @implements R92 Universal DBI Approach
#' @implements P76 Error Handling Patterns
#' @implements P77 Performance Optimization
microTransactionsServer <- function(id, app_data_connection) {
  # Load the universal data accessor if not already loaded
  if (!exists("universal_data_accessor")) {
    source("update_scripts/global_scripts/00_principles/02_db_utils/fn_universal_data_accessor.R")
  }
  
  moduleServer(id, function(input, output, session) {
    # Get transactions data using Universal Data Access Pattern
    transactions_data <- reactive({
      # P76: Error handling - ensure connection exists
      req(app_data_connection)
      
      # R91: Use universal data accessor to handle different connection types
      data <- universal_data_accessor(
        data_connection = app_data_connection,
        data_name = "transactions",
        log_level = 3
      )
      
      if (is.null(data) || !is.data.frame(data) || nrow(data) == 0) {
        cat("WARNING: No transactions data available or returned empty dataset\n")
        return(NULL)
      }
      
      return(data)
    })
    
    # Filtered data based on revenue slider
    filtered_data <- reactive({
      # P76: Error handling with improved validation
      req(transactions_data(), input$revenue_filter)
      df <- transactions_data()
      
      # Validate required columns exist
      if (!all(c("revenue", "transactions", "segment") %in% colnames(df))) {
        cat("WARNING: Transactions data missing required columns\n")
        cat("Available columns:", paste(colnames(df), collapse=", "), "\n")
        return(NULL)
      }
      
      # P77: Use tidyverse for consistent filtering
      filtered <- df %>%
        dplyr::filter(revenue >= input$revenue_filter)
        
      if (nrow(filtered) == 0) {
        cat("INFO: No transactions match the revenue filter criteria\n")
      }
      
      return(filtered)
    })
    
    # Create transaction scatter plot
    output$transaction_scatter <- renderPlot({
      # P76: Error handling
      req(filtered_data())
      df <- filtered_data()
      
      # P77: Performance optimization - only render when we have data
      if (nrow(df) == 0) {
        # Return empty plot with message when no data matches filter
        return(
          ggplot() + 
            annotate("text", x = 0.5, y = 0.5, label = "No data matching current filter") +
            theme_void()
        )
      }
      
      # Create the plot with improved styling
      ggplot(df, aes(x = transactions, y = revenue, color = segment)) +
        geom_point(size = 3, alpha = 0.7) +
        scale_color_brewer(palette = "Set1") +
        theme_minimal() +
        labs(
          title = "Revenue vs Transactions", 
          x = "Number of Transactions", 
          y = "Revenue",
          caption = paste("Showing", nrow(df), "transactions")
        ) +
        theme(
          plot.title = element_text(hjust = 0.5, face = "bold"),
          legend.position = "bottom"
        )
    })
  })
}