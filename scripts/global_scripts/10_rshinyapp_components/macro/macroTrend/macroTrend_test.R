#' @title Test Script for macroTrend Module
#' @description This script provides a simple Shiny app to test the macroTrend module
#' @principle P16 Component Testing
#' @principle R75 Test Script Initialization
#' @principle R91 Universal Data Access Pattern
#' @principle MP31 Initialization First

# Set APP_MODE for proper initialization 
OPERATION_MODE <- "APP_MODE"
if(!exists("INITIALIZATION_COMPLETED")) {
  INITIALIZATION_COMPLETED <- FALSE
}

init_script_path <- file.path("update_scripts", "global_scripts", "00_principles", 
                              "sc_initialization_app_mode.R")
source(init_script_path)

# Generate sample sales trend data
set.seed(123)
generate_sample_data <- function() {
  # Create date range for the past 2 years
  end_date <- Sys.Date()
  start_date <- end_date - 365 * 2
  dates <- seq.Date(from = start_date, to = end_date, by = "day")
  
  # Define categories
  categories <- c("Electronics", "Clothing", "Home Goods", "Sports", "Books", "Food", "Health")
  
  # Create seasonal pattern
  seasonal_factor <- function(date) {
    month <- month(date)
    # Higher sales in Nov-Dec (holiday season) and summer
    if (month %in% c(11, 12)) return(1.5)  # Holiday season
    if (month %in% c(6, 7, 8)) return(1.2)  # Summer
    if (month %in% c(1, 2)) return(0.8)    # Post-holiday slump
    return(1.0)  # Normal months
  }
  
  # Create weekend effect
  weekend_factor <- function(date) {
    wday <- wday(date)
    if (wday %in% c(1, 7)) return(1.3)  # Weekend
    return(1.0)  # Weekday
  }
  
  # Create long-term trend (growing)
  trend_factor <- function(date, start_date) {
    days_since_start <- as.numeric(difftime(date, start_date, units = "days"))
    1 + (days_since_start / 365) * 0.1  # 10% growth per year
  }
  
  # Create data frame with multiple rows per date (multiple transactions)
  n_transactions <- length(dates) * 15  # ~15 transactions per day
  
  transaction_dates <- sample(dates, n_transactions, replace = TRUE)
  transaction_dates <- sort(transaction_dates)  # Sort for realism
  
  # Add some randomness to transaction count (more transactions on busy days)
  daily_count <- table(as.character(transaction_dates))
  seasonal_effect <- sapply(as.Date(names(daily_count)), seasonal_factor)
  weekend_effect <- sapply(as.Date(names(daily_count)), weekend_factor)
  
  # Generate sales data
  sales_data <- data.frame(
    date = transaction_dates,
    category = sample(categories, n_transactions, replace = TRUE, 
                     prob = c(0.25, 0.2, 0.15, 0.1, 0.1, 0.1, 0.1)),  # Different category frequencies
    stringsAsFactors = FALSE
  )
  
  # Generate realistic sales amounts
  base_amount <- function(category) {
    switch(category,
           "Electronics" = runif(1, 100, 1000),
           "Clothing" = runif(1, 20, 200),
           "Home Goods" = runif(1, 50, 500),
           "Sports" = runif(1, 30, 300),
           "Books" = runif(1, 10, 50),
           "Food" = runif(1, 15, 100),
           "Health" = runif(1, 20, 150),
           runif(1, 20, 200)  # Default
    )
  }
  
  # Apply all factors to generate sales amount
  sales_data$sales_amount <- mapply(function(date, category) {
    amount <- base_amount(category)
    amount <- amount * seasonal_factor(date)
    amount <- amount * weekend_factor(date)
    amount <- amount * trend_factor(date, start_date)
    amount <- amount * runif(1, 0.8, 1.2)  # Random noise
    return(round(amount, 2))
  }, sales_data$date, sales_data$category)
  
  # Add transaction IDs and customer IDs
  sales_data$transaction_id <- sprintf("TXN-%06d", 1:nrow(sales_data))
  sales_data$customer_id <- sample(1:1000, nrow(sales_data), replace = TRUE)
  
  return(sales_data)
}

# Create test app
test_app <- function() {
  # Verify APP_MODE is set (should already be set by initialization script)
  if (!exists("OPERATION_MODE") || OPERATION_MODE != "APP_MODE") {
    message("OPERATION_MODE not set to APP_MODE. Re-initializing...")
    init_script_path <- file.path("update_scripts", "global_scripts", "00_principles", 
                                "sc_initialization_app_mode.R")
    source(init_script_path)
  }
  
  # Generate sample data
  message("Generating sample data for macroTrend component test...")
  sample_data <- generate_sample_data()
  
  # Create a reactive data connection (list format) following R91 Universal Data Access Pattern
  data_connection <- reactive({
    # In a real application, this would be a connection to a database or other data source
    # For testing, we use a simple list with the sample data
    list(
      sales_trend_data = sample_data
    )
  })
  
  # Define UI
  ui <- bs4Dash::dashboardPage(
    title = "macroTrend Test",
    header = bs4Dash::dashboardHeader(
      title = "macroTrend Module Test"
    ),
    sidebar = bs4Dash::dashboardSidebar(
      bs4Dash::sidebarMenu(
        id = "menu",
        bs4Dash::menuItem(
          text = "Test macroTrend",
          tabName = "test"
        )
      )
    ),
    body = bs4Dash::dashboardBody(
      bs4Dash::tabItems(
        bs4Dash::tabItem(
          tabName = "test",
          fluidRow(
            column(
              width = 3,
              # Filter UI
              macroTrendFilterUI("test_module")
            ),
            column(
              width = 9,
              # Display UI
              macroTrendUI("test_module")
            )
          )
        )
      )
    )
  )
  
  # Define server
  server <- function(input, output, session) {
    # Initialize and run the module
    filtered_data <- macroTrendServer(
      "test_module", 
      data_connection
    )
    
    # Add observeEvent to see when data changes
    observeEvent(filtered_data(), {
      cat("Filtered data updated, row count:", 
          if(is.null(filtered_data())) "NULL" else nrow(filtered_data()),
          "\n")
    })
  }
  
  # Initialization should already be complete from the initialization script
  message("Using APP_MODE initialization for the test app")
  
  # Return the Shiny app
  shinyApp(ui, server)
}

# Run the test app when this script is executed directly
if (interactive()) {
  # Set verbose initialization mode for better debugging
  VERBOSE_INITIALIZATION <- TRUE
  
  # Log application start
  message("Starting macroTrend test application in APP_MODE...")
  
  # Run the app
  test_app()
}