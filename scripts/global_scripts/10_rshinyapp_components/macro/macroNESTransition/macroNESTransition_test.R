# Test script for macroNESTransition component
# This script demonstrates how to use the macroNESTransition component in a Shiny app

# Load required libraries
library(shiny)
library(shinydashboard)
library(plotly)
library(dplyr)
library(RColorBrewer)

# Source the component
source("macroNESTransition.R")

# Mock data connection (for testing without database)
mock_data_connection <- local({
  # Create mock sales data
  set.seed(123)
  n <- 1000
  
  # Previous period data
  sales_past <- data.frame(
    customer_id = 1:n,
    nesstatus = sample(c("N", "E0", "S1", "S2", "S3"), n, replace = TRUE, 
                      prob = c(0.2, 0.5, 0.1, 0.1, 0.1)),
    source = sample(c("amazon", "retail", "web"), n, replace = TRUE),
    product_line_id = sample(c("001", "002", "003"), n, replace = TRUE),
    state = sample(c("CA", "NY", "TX", "FL"), n, replace = TRUE),
    platform_id = "amz",
    time_condition = "m1quarter"
  )
  
  # Current period data - simulate some transitions
  transition_matrix <- matrix(
    c(0.6, 0.2, 0.1, 0.05, 0.05,  # N to X
      0.1, 0.7, 0.1, 0.05, 0.05,  # E0 to X
      0.2, 0.3, 0.3, 0.1, 0.1,    # S1 to X
      0.1, 0.2, 0.2, 0.3, 0.2,    # S2 to X
      0.05, 0.15, 0.1, 0.2, 0.5), # S3 to X
    nrow = 5, byrow = TRUE
  )
  
  # Function to sample new status based on previous status
  get_new_status <- function(old_status) {
    old_idx <- match(old_status, c("N", "E0", "S1", "S2", "S3"))
    probs <- transition_matrix[old_idx, ]
    sample(c("N", "E0", "S1", "S2", "S3"), 1, prob = probs)
  }
  
  # Apply transitions
  sales_now <- sales_past
  sales_now$nesstatus <- sapply(sales_past$nesstatus, get_new_status)
  sales_now$time_condition <- "now"
  
  # Combine data
  all_sales <- rbind(sales_past, sales_now)
  
  # Create product categories
  product_categories <- data.frame(
    id = c("001", "002", "003"),
    name = c("Kitchen", "Bathroom", "Living Room"),
    platform_id = "amz"
  )
  
  # Create mock database connection
  list(
    tbl2 = function(conn, table_name) {
      if (table_name == "sales_by_customer") {
        return(all_sales)
      } else if (table_name == "product_categories") {
        return(product_categories)
      }
      data.frame()
    }
  )
})

# Create Shiny app with the component
ui <- fluidPage(
  titlePanel("macroNESTransition Component Test"),
  fluidRow(
    column(width = 3,
           wellPanel(
             h3("Component Documentation"),
             p("This is a test application demonstrating the macroNESTransition component."),
             p("It displays customer transitions between NES status categories."),
             p("The data shown is simulated for demonstration purposes.")
           )
    ),
    column(width = 9,
           fluidRow(
             column(width = 3, 
                    macroNESTransitionComponent("test_nes_transition", 
                                              mock_data_connection)$ui$filter),
             column(width = 9, 
                    macroNESTransitionComponent("test_nes_transition", 
                                              mock_data_connection)$ui$display)
           )
    )
  )
)

server <- function(input, output, session) {
  # Initialize the component
  macroNESTransitionComponent("test_nes_transition", 
                            mock_data_connection, 
                            list(platform_id = "amz"))$server(input, output, session)
}

# Run the app
shinyApp(ui, server)
