#' @file microCustomer_test.R
#' @principle MP51 Test Data Design
#' @principle R74 Shiny Test Data
#' @principle R75 Test Script Initialization
#' @principle R76 Module Data Connection Rule
#' @principle R91 Universal Data Access Pattern
#' @principle P76 Error Handling Patterns
#' @principle P77 Performance Optimization
#' @author Analytics Team
#' @date 2025-04-10
#' @modified 2025-04-10
#'
#' @title Test Application for microCustomer Module with Universal Data Access
#' @description This test script demonstrates how the microCustomer module can work with 
#' different connection types using the universal_data_accessor pattern.

#LOCK FILE

# Set to TRUE to skip tests and just launch the app
DEMO_MODE <- TRUE

# 1. Environment Setup
source_find_root <- function(start_path = getwd()) {
  current_path <- normalizePath(start_path, winslash = "/")
  while (!file.exists(file.path(current_path, "app.R"))) {
    parent_path <- dirname(current_path)
    if (parent_path == current_path) {
      stop("Cannot find project root directory containing app.R")
    }
    current_path <- parent_path
  }
  return(current_path)
}

if (!exists("script_triggered_from_app_mode") || !script_triggered_from_app_mode) {
  root_path <- source_find_root()
  setwd(root_path)
  
  # 2. Mode Initialization
  source(file.path("update_scripts", "global_scripts", "00_principles", 
                   "sc_initialization_app_mode.R"))
}

# 3. Required Libraries
library(shiny)
library(bs4Dash)
library(dplyr)

# 5. Test Data
customer_test_data <- list(
  # Complete customer data
  df_profile_by_customer = data.frame(
    customer_id = 1:5,
    buyer_name = c("Zhang San", "Li Si", "Wang Wu", "Zhao Liu", "Chen Qi"),
    email = c("zhang@example.com", "li@example.com", "wang@example.com", 
              "zhao@example.com", "chen@example.com"),
    stringsAsFactors = FALSE
  ),
  
  # Partial DNA data (only for customers 1-3)
  df_dna_by_customer = data.frame(
    customer_id = 1:3,
    time_first = as.Date(c("2022-01-15", "2021-03-20", "2020-11-05")),
    time_first_to_now = c(450, 780, 920),
    r_label = c("極近", "一般", "非常久遠"),
    r_value = c(5, 45, 120),
    f_label = c("非常高", "一般", "低"),
    f_value = c(12, 4, 8),
    m_label = c("非常高", "一般", "低"),
    m_value = c(15000, 3500, 7800),
    cai_label = c("非常活躍", "一般活躍", "不活躍"),
    cai = c(0.85, 0.35, 0.15),
    ipt_mean = c(38.5, 97.2, 65.8),
    pcv = c(180000, 15000, 62400),
    clv = c(250000, 25000, 89000),
    cri = c(0.92, 0.45, 0.72),
    nrec = c(0.75, 0.35, 0.55),
    nes_status = c("主力型", "成長型", "沉睡型"),
    nt = c(1500, 900, 1200),
    e0t = c(2500, 1500, 1800),
    stringsAsFactors = FALSE
  ),
  
  # Test scenarios
  scenarios = list(
    complete = list(
      df_profile_by_customer = data.frame(
        customer_id = 1:5,
        buyer_name = c("Zhang San", "Li Si", "Wang Wu", "Zhao Liu", "Chen Qi"),
        email = c("zhang@example.com", "li@example.com", "wang@example.com", 
                  "zhao@example.com", "chen@example.com"),
        stringsAsFactors = FALSE
      ),
      df_dna_by_customer = data.frame(
        customer_id = 1:3,
        time_first = as.Date(c("2022-01-15", "2021-03-20", "2020-11-05")),
        time_first_to_now = c(450, 780, 920),
        r_label = c("極近", "一般", "非常久遠"),
        r_value = c(5, 45, 120),
        f_label = c("非常高", "一般", "低"),
        f_value = c(12, 4, 8),
        m_label = c("非常高", "一般", "低"),
        m_value = c(15000, 3500, 7800),
        cai_label = c("非常活躍", "一般活躍", "不活躍"),
        cai = c(0.85, 0.35, 0.15),
        ipt_mean = c(38.5, 97.2, 65.8),
        pcv = c(180000, 15000, 62400),
        clv = c(250000, 25000, 89000),
        cri = c(0.92, 0.45, 0.72),
        nrec = c(0.75, 0.35, 0.55),
        nes_status = c("主力型", "成長型", "沉睡型"),
        nt = c(1500, 900, 1200),
        e0t = c(2500, 1500, 1800),
        stringsAsFactors = FALSE
      )
    ),
    
    incomplete = list(
      df_profile_by_customer = data.frame(
        customer_id = 1:5,
        buyer_name = c("Zhang San", "Li Si", "Wang Wu", "Zhao Liu", "Chen Qi"),
        email = c("zhang@example.com", "li@example.com", "wang@example.com", 
                  "zhao@example.com", "chen@example.com"),
        stringsAsFactors = FALSE
      ),
      df_dna_by_customer = data.frame(
        customer_id = 1:2,
        time_first = as.Date(c("2022-01-15", "2021-03-20")),
        time_first_to_now = c(450, 780),
        r_label = c("極近", "一般"),
        r_value = c(5, 45),
        f_label = c("非常高", "一般"),
        f_value = c(12, 4),
        m_label = c("非常高", "一般"),
        m_value = c(15000, 3500),
        cai_label = c("非常活躍", "一般活躍"),
        cai = c(0.85, 0.35),
        ipt_mean = c(38.5, 97.2),
        pcv = c(180000, 15000),
        clv = c(250000, 25000),
        cri = c(0.92, 0.45),
        nrec = c(0.75, 0.35),
        nes_status = c("主力型", "成長型"),
        nt = c(1500, 900),
        e0t = c(2500, 1500),
        stringsAsFactors = FALSE
      )
    ),
    
    empty = list(
      df_profile_by_customer = data.frame(
        customer_id = integer(0),
        buyer_name = character(0),
        email = character(0),
        stringsAsFactors = FALSE
      ),
      df_dna_by_customer = data.frame(
        customer_id = integer(0),
        time_first = as.Date(character(0)),
        time_first_to_now = numeric(0),
        r_label = character(0),
        r_value = numeric(0),
        f_label = character(0),
        f_value = numeric(0),
        m_label = character(0),
        m_value = numeric(0),
        cai_label = character(0),
        cai = numeric(0),
        ipt_mean = numeric(0),
        pcv = numeric(0),
        clv = numeric(0),
        cri = numeric(0),
        nrec = numeric(0),
        nes_status = character(0),
        nt = numeric(0),
        e0t = numeric(0),
        stringsAsFactors = FALSE
      )
    )
  ),
  
  # Metadata about this test data
  metadata = list(
    description = "Standard customer test data for microCustomer module",
    customer_count = 5,
    dna_coverage_pct = 60,
    includes_edge_cases = TRUE,
    key_relationship = "df_profile_by_customer (key = customer_id) is complete, df_dna_by_customer (key = customer_id) is partial"
  )
)

# 6. Create test connections using different methods
create_test_connection <- function(scenario_data, connection_type = "list") {
  if (connection_type == "mock_dbi") {
    # Create mock DBI connection
    connection <- create_mock_connection(scenario_data, "dbi")
  } else if (connection_type == "function") {
    # Create function-based connection
    connection <- list(
      get_customer_profile = function() {
        return(scenario_data$df_profile_by_customer)
      },
      get_dna_by_customer = function() {
        return(scenario_data$df_dna_by_customer)
      }
    )
    class(connection) <- c("function_connection", "list")
  } else if (connection_type == "mixed") {
    # Mixed connection type (some direct access, some functions)
    connection <- list(
      df_profile_by_customer = scenario_data$df_profile_by_customer,
      get_dna_by_customer = function() {
        return(scenario_data$df_dna_by_customer)
      }
    )
    class(connection) <- c("mixed_connection", "list")
  } else {
    # Default to simple list
    connection <- scenario_data
    class(connection) <- c("list_connection", "list")
  }
  
  return(connection)
}

# 7. Test Functions

#' Validate Data Structure
#'
#' Checks if a data structure meets requirements for testing.
#'
#' @param data Data to validate
#' @param required_fields Required fields to check for
#' @param data_name Name to use in messages
#' @return TRUE if valid, FALSE otherwise
validate_data <- function(data, required_fields = NULL, data_name = "Data") {
  if (is.null(data)) {
    message(data_name, " is NULL")
    return(FALSE)
  }
  
  if (!is.data.frame(data)) {
    message(data_name, " is not a data frame")
    return(FALSE)
  }
  
  if (nrow(data) == 0) {
    message(data_name, " has 0 rows")
    return(FALSE)
  }
  
  if (!is.null(required_fields) && !all(required_fields %in% colnames(data))) {
    missing_fields <- setdiff(required_fields, colnames(data))
    message(data_name, " is missing required fields: ", 
            paste(missing_fields, collapse = ", "))
    return(FALSE)
  }
  
  return(TRUE)
}

#' Create Universal Test App
#'
#' Creates a test application using the universal microCustomer module.
#'
#' @param connection_type Type of connection to use
#' @param test_data Test data to use in the application
#' @return A Shiny application
create_test_app <- function(connection_type = "list") {
  ui <- fluidPage(
    title = paste("microCustomer Module Test with", connection_type, "connection"),
    
    # CSS styles
    tags$head(
      tags$style(HTML("
        .test-control-panel {
          background-color: #f8f9fa;
          padding: 15px;
          margin-bottom: 20px;
          border-radius: 5px;
          border: 1px solid #dee2e6;
        }
        .test-header {
          margin-bottom: 20px;
          padding-bottom: 10px;
          border-bottom: 1px solid #dee2e6;
        }
        .test-section {
          border: 1px solid #dee2e6;
          border-radius: 5px;
          padding: 15px;
          margin-top: 20px;
        }
        .connection-type {
          background-color: #e2f3f5;
          padding: 5px 10px;
          font-weight: bold;
          border-radius: 3px;
          display: inline-block;
        }
      "))
    ),
    
    # Test header
    div(class = "test-header",
        h1(paste("microCustomer Module Test with", toupper(connection_type), "Connection")),
        p("This test application demonstrates the universal data accessor pattern with different connection types"),
        div(class = "connection-type", 
            paste("Using Connection Type:", connection_type))
    ),
    
    # Test control panel
    div(class = "test-control-panel",
        fluidRow(
          column(12,
                 h3("Test Controls"),
                 selectInput("test_scenario", "Test Scenario:", 
                             choices = c(
                               "Complete Data" = "complete",
                               "Incomplete Data" = "incomplete",
                               "Empty Data" = "empty"
                             ),
                             selected = "complete"
                 ),
                 hr(),
                 selectInput("connection_type", "Connection Type:", 
                             choices = c(
                               "List (direct)" = "list",
                               "Functions" = "function",
                               "Mock DBI" = "mock_dbi",
                               "Mixed" = "mixed"
                             ),
                             selected = connection_type
                 ),
                 checkboxInput("show_filter", "Show Filter Interface", value = TRUE),
                 checkboxInput("show_debug", "Show Debug Information", value = TRUE)
          )
        )
    ),
    
    # Test results area
    fluidRow(
      column(12,
             h3("Module Test Results"),
             conditionalPanel(
               condition = "input.show_filter == true",
               div(class = "test-section",
                   h4("Filter Component"),
                   microCustomerFilterUI("test_module")
               )
             ),
             div(class = "test-section",
                 h4("Customer Profile Component"),
                 microCustomerUI("test_module")
             )
      )
    ),
    
    # Debug information
    conditionalPanel(
      condition = "input.show_debug == true",
      fluidRow(
        column(12,
               div(class = "test-section",
                   h3("Debug Information"),
                   h4("Connection Details"),
                   verbatimTextOutput("connection_info"),
                   h4("Data Status"),
                   verbatimTextOutput("data_info"),
                   h4("Module Response"),
                   verbatimTextOutput("module_info")
               )
        )
      )
    )
  )
  
  server <- function(input, output, session) {
    # Create a reactive data connection based on the selected scenario and type
    data_connection <- reactive({
      # Get connection type
      conn_type <- input$connection_type
      
      # Get scenario data
      scenario <- input$test_scenario
      
      if (scenario %in% names(customer_test_data$scenarios)) {
        scenario_data <- customer_test_data$scenarios[[scenario]]
      } else {
        # Default to complete data if scenario not found
        scenario_data <- customer_test_data$scenarios$complete
      }
      
      # Create the appropriate connection type
      connection <- create_test_connection(scenario_data, conn_type)
      
      # Log connection creation
      cat("Created", conn_type, "connection for scenario:", scenario, "\n")
      
      return(connection)
    })
    
    # Create a wrapper connection that's properly reactive for the module
    module_connection <- reactive({
      # This reactive expression wraps data_connection()
      # Use universal_data_accessor for actual data access
      data_connection()
    })
    
    # Initialize the module with the universal data accessor pattern
    filtered_data <- microCustomerServer(
      "test_module", 
      module_connection
    )
    
    # Connection info debug output
    output$connection_info <- renderPrint({
      conn <- data_connection()
      
      # Show connection class and type
      list(
        "Connection Class" = class(conn),
        "Connection Type" = input$connection_type,
        "Scenario" = input$test_scenario
      )
    })
    
    # Data info debug output
    output$data_info <- renderPrint({
      # Extract data directly for debugging
      tryCatch({
        conn <- data_connection()
        
        # Use universal_data_accessor to get both datasets
        customer_data <- universal_data_accessor(conn, "customer_profile", log_level = 0)
        dna_data <- universal_data_accessor(conn, "dna_by_customer", log_level = 0)
        
        list(
          customer_profile = list(
            success = !is.null(customer_data),
            rows = if(!is.null(customer_data)) nrow(customer_data) else 0,
            columns = if(!is.null(customer_data)) ncol(customer_data) else 0
          ),
          dna_by_customer = list(
            success = !is.null(dna_data),
            rows = if(!is.null(dna_data)) nrow(dna_data) else 0,
            columns = if(!is.null(dna_data)) ncol(dna_data) else 0
          )
        )
      }, error = function(e) {
        list(error = e$message)
      })
    })
    
    # Module info debug output
    output$module_info <- renderPrint({
      # Show information about the module's output
      list(
        filtered_data = list(
          success = !is.null(filtered_data()),
          rows = if(!is.null(filtered_data())) nrow(filtered_data()) else 0
        ),
        module_status = "Using Universal Data Accessor Pattern"
      )
    })
  }
  
  # Launch the app with specific options to ensure it opens in the browser
  shinyApp(ui, server, options = list(
    port = 5432,         # Use a specific port
    host = "127.0.0.1"   # Listen on localhost only
  ))
}

# 8. Test Execution

# Run tests or launch demo app
if (DEMO_MODE) {
  message("DEMO_MODE is enabled - launching the app with list connection type")
  
  # Set only the global launch.browser option to avoid recursion
  options(shiny.launch.browser = TRUE)
  
  # Launch with the list connection type by default
  create_test_app("list")
} else {
  # Run formal tests when not in DEMO_MODE
  test_that("Universal data accessor works with different connection types", {
    # Create test connections
    list_conn <- create_test_connection(customer_test_data$scenarios$complete, "list")
    func_conn <- create_test_connection(customer_test_data$scenarios$complete, "function")
    
    # Test with list connection
    customer_data_list <- universal_data_accessor(list_conn, "customer_profile", log_level = 0)
    expect_true(is.data.frame(customer_data_list))
    expect_true(nrow(customer_data_list) > 0)
    
    # Test with function connection
    customer_data_func <- universal_data_accessor(func_conn, "customer_profile", log_level = 0)
    expect_true(is.data.frame(customer_data_func))
    expect_true(nrow(customer_data_func) > 0)
    
    # Verify data is identical regardless of connection type
    expect_equal(nrow(customer_data_list), nrow(customer_data_func))
  })
  
  if (interactive() && !exists("skip_interactive") && !isTRUE(getOption("test.mode"))) {
    message("Starting interactive test app...")
    create_test_app("list")  # Launch app with list connection
  } else {
    message("Test validation complete. Run in interactive mode to see the test app.")
  }
}