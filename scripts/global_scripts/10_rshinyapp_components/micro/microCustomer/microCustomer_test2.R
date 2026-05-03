#' @file microCustomer_test2.R
#' @principle MP51 Test Data Design
#' @principle R74 Shiny Test Data
#' @principle R75 Test Script Initialization
#' @principle R76 Module Data Connection Rule
#' @principle R91 Universal Data Access Pattern
#' @principle R99 Test App Building Principles
#' @principle R100 Database Access via tbl() Rule
#' @principle R101 Unified tbl-like Data Access Pattern
#' @principle R102 Shiny Reactive Observation Rule
#' @principle P76 Error Handling Patterns
#' @principle P77 Performance Optimization
#' @principle R93 Function Location Rule
#' @author Claude
#' @date 2025-04-13
#' @modified 2025-04-13
#'
#' @title Advanced Test Script for microCustomer Module with Universal Data Access
#' @description 
#' This test script tests all components and functions from microCustomer.R:
#' - microCustomerDefaults
#' - microCustomerFilterUI
#' - microCustomerDisplayUI
#' - microCustomerServer
#' - microCustomerComponent
#' - microCustomerInitialize (deprecated alias)
#' Testing focuses on the universal_data_accessor pattern with different connection types.

# Set to TRUE to skip tests and just launch the app in interactive mode
DEMO_MODE <- TRUE

# User Validation Note:
# According to R99_test_app_building.md (User Validation as Ultimate Success Criterion),
# the final determination of component functionality must be made by a human user through
# interactive testing. While automated tests provide confidence, visual inspection and
# manual verification of all connection types is required for final validation.
#
# Implementation Note:
# This app now implements the R102 Shiny Reactive Observation Rule, which ensures that 
# critical reactive expressions are always executed regardless of UI visibility. This
# fixes a common issue where filters appear but do not work because their server-side
# logic is never triggered. Without R102, the filter would only work when "Show Technical
# Details" was checked, since that was the only thing observing the customer_data() reactive.
#
# When testing this app:
# 1. Verify each connection type works as expected
# 2. Confirm error states are handled gracefully
# 3. Test all user interactions and UI elements
# 4. Verify data is displayed correctly
# 5. Confirm filters work regardless of whether debug panels are visible (R102)

# 1. Helper Functions (setting up the environment)
find_project_root <- function() {
  # Start from current directory
  current_path <- getwd()
  
  # Check for app.R or a known project structure file
  while (!file.exists(file.path(current_path, "app.R")) && 
         !file.exists(file.path(current_path, "precision_marketing_app"))) {
    # Go up one level
    parent_path <- dirname(current_path)
    
    # If we've reached the root without finding the project root
    if (parent_path == current_path) {
      # Fall back to the current working directory
      message("Could not find project root, using current directory.")
      return(getwd())
    }
    
    current_path <- parent_path
  }
  
  # If we found precision_marketing_app folder, go into it
  if (file.exists(file.path(current_path, "precision_marketing_app"))) {
    return(file.path(current_path, "precision_marketing_app"))
  }
  
  # Otherwise return the directory with app.R
  return(current_path)
}

# Find the project root
project_root <- find_project_root()
message("Project root: ", project_root)

# 2. Load Required Libraries
suppressPackageStartupMessages({
  library(shiny)
  library(bs4Dash)
  library(dplyr)
  library(testthat)
  
  # Add bslib which provides nav_panel function
  if (requireNamespace("bslib", quietly = TRUE)) {
    library(bslib)
    message("Loaded bslib package for nav_panel function")
  } else {
    # If bslib is not available, create a temporary nav_panel function
    # that mimics the behavior enough for testing
    if (!exists("nav_panel")) {
      nav_panel <- function(title, ...) {
        message("Using nav_panel compatibility function")
        shiny::tagList(...)
      }
      message("Created compatibility nav_panel function")
    }
  }
  
  # Add shinyjs for interactive UI elements
  if (requireNamespace("shinyjs", quietly = TRUE)) {
    library(shinyjs)
    message("Loaded shinyjs package for interactive UI")
  } else {
    message("Note: shinyjs package is not available. Some UI features may not work correctly.")
    # Create stub functions if shinyjs is not available
    if (!exists("addClass")) {
      shinyjs <- list(
        addClass = function(selector, class) {
          message(paste("Adding class", class, "to", selector))
        },
        removeClass = function(selector, class) {
          message(paste("Removing class", class, "from", selector))
        }
      )
      assign("shinyjs", shinyjs, envir = .GlobalEnv)
    }
  }
})

# Note: nav_panel compatibility function has been added to support the original functions

# 3. Source Required Files
# Determine the correct path to the universal_data_accessor
db_utils_path <- file.path(project_root, "update_scripts", "global_scripts", "02_db_utils", 
                           "fn_universal_data_accessor.R")

# Check if the file exists at the expected location
if (!file.exists(db_utils_path)) {
  # Try alternate path (from previous location)
  old_path <- file.path(project_root, "update_scripts", "global_scripts", "00_principles", 
                         "02_db_utils", "fn_universal_data_accessor.R")
  if (file.exists(old_path)) {
    db_utils_path <- old_path
    message("Using legacy path for universal_data_accessor")
  } else {
    message("Warning: Could not find universal_data_accessor at expected paths")
  }
}

# Source universal_data_accessor
message("Sourcing universal_data_accessor from: ", db_utils_path)
source(db_utils_path)

# Source microCustomer module
micro_customer_path <- file.path(project_root, "update_scripts", "global_scripts", 
                                "10_rshinyapp_components", "micro", "microCustomer", "microCustomer.R")
message("Sourcing microCustomer from: ", micro_customer_path)
source(micro_customer_path)

# 4. Generate Test Data (with multiple variations for different tests)
generate_test_data <- function() {
  # Main customer data
  customer_data <- data.frame(
    customer_id = 1:5,
    buyer_name = c("Alex Wang", "Betty Chen", "Charlie Zhang", "David Li", "Eva Liu"),
    email = c("alex@example.com", "betty@example.com", "charlie@example.com", 
              "david@example.com", "eva@example.com"),
    registration_date = as.Date(c("2022-01-15", "2021-03-20", "2023-05-10", 
                                 "2020-11-05", "2022-07-30")),
    stringsAsFactors = FALSE
  )
  
  # Complete DNA data for all customers
  dna_complete <- data.frame(
    customer_id = 1:5,
    time_first = as.Date(c("2022-01-15", "2021-03-20", "2023-05-10", 
                          "2020-11-05", "2022-07-30")),
    time_first_to_now = c(450, 780, 340, 920, 380),
    r_label = c("極近", "一般", "極近", "非常久遠", "一般"),
    r_value = c(5, 45, 2, 120, 30),
    f_label = c("非常高", "一般", "低", "高", "一般"),
    f_value = c(12, 4, 2, 8, 5),
    m_label = c("非常高", "一般", "低", "高", "一般"),
    m_value = c(15000, 3500, 1200, 7800, 4200),
    cai_label = c("非常活躍", "一般活躍", "低度活躍", "不活躍", "一般活躍"),
    cai = c(0.85, 0.35, 0.15, 0.10, 0.40),
    ipt_mean = c(38.5, 97.2, 145.8, 65.8, 82.5),
    pcv = c(180000, 15000, 2400, 62400, 22000),
    clv = c(250000, 25000, 3500, 89000, 38000),
    cri = c(0.92, 0.45, 0.28, 0.72, 0.55),
    nrec = c(0.75, 0.35, 0.20, 0.55, 0.40),
    nes_status = c("主力型", "成長型", "潛力型", "沉睡型", "成長型"),
    nt = c(1500, 900, 1200, 1200, 1100),
    e0t = c(2500, 1500, 800, 1800, 1600),
    stringsAsFactors = FALSE
  )
  
  # Partial DNA data (only for first 3 customers)
  dna_partial <- data.frame(
    customer_id = 1:3,
    time_first = as.Date(c("2022-01-15", "2021-03-20", "2023-05-10")),
    time_first_to_now = c(450, 780, 340),
    r_label = c("極近", "一般", "極近"),
    r_value = c(5, 45, 2),
    f_label = c("非常高", "一般", "低"),
    f_value = c(12, 4, 2),
    m_label = c("非常高", "一般", "低"),
    m_value = c(15000, 3500, 1200),
    cai_label = c("非常活躍", "一般活躍", "低度活躍"),
    cai = c(0.85, 0.35, 0.15),
    ipt_mean = c(38.5, 97.2, 145.8),
    pcv = c(180000, 15000, 2400),
    clv = c(250000, 25000, 3500),
    cri = c(0.92, 0.45, 0.28),
    nrec = c(0.75, 0.35, 0.20),
    nes_status = c("主力型", "成長型", "潛力型"),
    nt = c(1500, 900, 1200),
    e0t = c(2500, 1500, 800),
    stringsAsFactors = FALSE
  )
  
  # DNA data with missing fields (to test error handling)
  dna_incomplete <- data.frame(
    customer_id = 1:3,
    time_first = as.Date(c("2022-01-15", "2021-03-20", "2023-05-10")),
    r_value = c(5, 45, 2),
    f_value = c(12, 4, 2),
    m_value = c(15000, 3500, 1200),
    # Missing many fields that should be in DNA data
    stringsAsFactors = FALSE
  )
  
  # Empty DNA data (for edge case testing)
  dna_empty <- data.frame(
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
  
  # Customer data with different field names (to test field name detection)
  customer_alt_fields <- data.frame(
    id = 1:5,  # using 'id' instead of 'customer_id'
    name = c("Alex Wang", "Betty Chen", "Charlie Zhang", "David Li", "Eva Liu"),
    customer_email = c("alex@example.com", "betty@example.com", "charlie@example.com", 
                      "david@example.com", "eva@example.com"),
    reg_date = as.Date(c("2022-01-15", "2021-03-20", "2023-05-10", 
                        "2020-11-05", "2022-07-30")),
    stringsAsFactors = FALSE
  )
  
  # Organize test data into scenarios
  return(list(
    # Core data
    customer_profile = customer_data,
    dna_by_customer = dna_complete,
    
    # Specific test scenarios
    scenarios = list(
      complete = list(
        customer_profile = customer_data,
        dna_by_customer = dna_complete
      ),
      partial = list(
        customer_profile = customer_data,
        dna_by_customer = dna_partial
      ),
      incomplete_fields = list(
        customer_profile = customer_data,
        dna_by_customer = dna_incomplete
      ),
      alt_field_names = list(
        customer_profile = customer_alt_fields,
        dna_by_customer = dna_complete
      ),
      empty = list(
        customer_profile = customer_data,
        dna_by_customer = dna_empty
      ),
      null_data = list(
        customer_profile = NULL,
        dna_by_customer = NULL
      )
    )
  ))
}

# Generate test data
test_data <- generate_test_data()

# 5. Test Connection Factories

#' Create test connections using different methods
#' @param scenario_data Data to include in the connection
#' @param connection_type Type of connection to create
#' @return Connection object of the specified type
create_test_connection <- function(scenario_data, connection_type = "list") {
  if (connection_type == "db") {
    # Verify DBI package is available
    if (!requireNamespace("DBI", quietly = TRUE) || 
        !requireNamespace("duckdb", quietly = TRUE)) {
      message("DBI or duckdb package missing. Using list connection instead.")
      connection_type <- "list"
    } else {
      # Create an in-memory DuckDB connection
      message("Creating in-memory DuckDB connection...")
      conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
      
      # Add tables to the database with appropriate names for universal_data_accessor
      for (name in names(scenario_data)) {
        if (!is.null(scenario_data[[name]]) && is.data.frame(scenario_data[[name]])) {
          # Write the table with its direct name
          tryCatch({
            DBI::dbWriteTable(conn, name, scenario_data[[name]])
            message(paste("Created table:", name))
          }, error = function(e) {
            message(paste("Error creating table", name, ":", e$message))
          })
        }
      }
      
      # Check tables in database
      tables <- DBI::dbListTables(conn)
      message(paste("Tables in database:", paste(tables, collapse=", ")))
      
      return(conn)
    }
  }
  
  if (connection_type == "function_based") {
    # Function-based connection
    connection <- list()
    
    # Create getter functions for each data frame
    for (name in names(scenario_data)) {
      if (!is.null(scenario_data[[name]]) && is.data.frame(scenario_data[[name]])) {
        # Create the function dynamically
        fn_name <- paste0("get_", name)
        connection[[fn_name]] <- local({
          data <- scenario_data[[name]]
          function() data
        })
      }
    }
    
    # Add generic data accessor
    connection$get_data <- function(data_name) {
      if (data_name %in% names(scenario_data) && 
          !is.null(scenario_data[[data_name]])) {
        return(scenario_data[[data_name]])
      }
      stop(paste("Data not found:", data_name))
    }
    
    class(connection) <- c("function_connection", "list")
    return(connection)
    
  } else if (connection_type == "mixed") {
    # Mixed connection (both direct data and functions)
    connection <- list()
    
    # Add direct references to some data
    if (!is.null(scenario_data$customer_profile)) {
      connection$customer_profile <- scenario_data$customer_profile
    }
    
    # Add function access to other data
    if (!is.null(scenario_data$dna_by_customer)) {
      connection$get_dna_by_customer <- local({
        data <- scenario_data$dna_by_customer
        function() data
      })
    }
    
    class(connection) <- c("mixed_connection", "list")
    return(connection)
    
  } else if (connection_type == "reactive") {
    # Create a reactive wrapper around the list data
    if (!requireNamespace("shiny", quietly = TRUE)) {
      message("shiny package missing. Using list connection instead.")
      connection_type <- "list"
    } else {
      # Create a reactive wrapper that returns a list
      message("Creating reactive connection wrapper...")
      # This only works within a running Shiny app
      connection <- shiny::reactiveVal(scenario_data)
      return(connection)
    }
  } 
  
  # Default: return a simple list connection
  connection <- scenario_data
  class(connection) <- c("list_connection", "list")
  return(connection)
}

# 6. Unit Test Functions

#' Test the microCustomerDefaults function
test_defaults <- function() {
  cat("\n=== Testing microCustomerDefaults ===\n")
  
  # Get the defaults
  defaults <- microCustomerDefaults()
  
  # Validate defaults structure
  test_that("Defaults function returns a properly structured list", {
    expect_true(is.list(defaults))
    expect_true(length(defaults) > 0)
    expect_true(all(c("dna_time_first", "dna_r_label", "dna_f_label") %in% names(defaults)))
  })
  
  # Check content of some default values
  test_that("Default values have expected content", {
    expect_equal(defaults$dna_time_first, "N/A")
    expect_equal(defaults$dna_r_value, "0")
    expect_equal(defaults$dna_f_value, "0")
    expect_equal(defaults$dna_m_value, "0.00")
  })
  
  cat("   ✓ microCustomerDefaults tests passed\n")
}

#' Test microCustomerFilterUI and microCustomerDisplayUI functions
test_ui_components <- function() {
  cat("\n=== Testing UI Components ===\n")
  
  # Test filter UI
  filter_ui <- microCustomerFilterUI("test_id")
  
  test_that("Filter UI is created correctly", {
    expect_true(inherits(filter_ui, "shiny.tag"))
    expect_true(grepl("customer-filter-container", as.character(filter_ui)))
    expect_true(grepl("test_id-customer_filter", as.character(filter_ui)))
  })
  
  # Test display UI
  display_ui <- microCustomerDisplayUI("test_id")
  
  test_that("Display UI is created correctly", {
    expect_true(inherits(display_ui, "shiny.tag.list") || 
                inherits(display_ui, "shiny.tag"))
    expect_true(grepl("test_id-dna_time_first", as.character(display_ui)))
    expect_true(grepl("test_id-customer_name", as.character(display_ui)))
  })
  
  cat("   ✓ UI component tests passed\n")
}

#' Test microCustomerComponent and microCustomerInitialize functions
test_component_factory <- function() {
  cat("\n=== Testing Component Factory Functions ===\n")
  
  # Create a connection
  conn <- create_test_connection(test_data$scenarios$complete)
  
  # Test microCustomerComponent
  component <- microCustomerComponent("test_id", conn)
  
  test_that("Component factory returns proper structure", {
    expect_true(is.list(component))
    expect_true(all(c("ui", "server") %in% names(component)))
    expect_true(is.list(component$ui))
    expect_true(all(c("filter", "display") %in% names(component$ui)))
    expect_true(is.function(component$server))
  })
  
  # Test deprecated initialize function
  component2 <- microCustomerInitialize("test_id2", conn)
  
  test_that("Deprecated initialize function works", {
    expect_true(is.list(component2))
    expect_true(all(c("ui", "server") %in% names(component2)))
  })
  
  cat("   ✓ Component factory tests passed\n")
}

# 7. Create Test App with All Connection Types

create_universal_test_app <- function() {
  # Initialize connections for each type
  connections <- list(
    list = create_test_connection(test_data$scenarios$complete, "list"),
    func = create_test_connection(test_data$scenarios$complete, "function_based"),
    mixed = create_test_connection(test_data$scenarios$complete, "mixed")
  )
  
  # Try to create DB connection if packages available
  if (requireNamespace("DBI", quietly = TRUE) && 
      requireNamespace("duckdb", quietly = TRUE)) {
    connections$db <- create_test_connection(test_data$scenarios$complete, "db")
  }
  
  ui <- fluidPage(
    # Enable shinyjs
    if (requireNamespace("shinyjs", quietly = TRUE)) shinyjs::useShinyjs(),
    
    # App header and styles
    tags$head(
      tags$style(HTML("
        .connection-panel {
          margin-bottom: 20px;
          border: 1px solid #ddd;
          border-radius: 5px;
          padding: 15px;
          background-color: #f9f9f9;
        }
        .test-controls {
          margin-bottom: 20px;
          padding: 15px;
          background-color: #e9ecef;
          border-radius: 5px;
        }
        .scenario-info {
          padding: 10px;
          margin-bottom: 15px;
          background-color: #f1f8ff;
          border-left: 4px solid #0366d6;
          border-radius: 3px;
        }
        .test-header {
          margin-bottom: 20px;
          padding-bottom: 10px;
          border-bottom: 1px solid #eee;
          text-align: center;
        }
        .validation-panel {
          margin-top: 20px;
          padding: 15px;
          background-color: #fff8dc;
          border: 1px solid #f0e68c;
          border-radius: 5px;
        }
        .validation-success {
          background-color: #dff0d8;
          border-color: #d6e9c6;
        }
        .validation-pending {
          background-color: #fcf8e3;
          border-color: #faebcc;
        }
        .validation-failed {
          background-color: #f2dede;
          border-color: #ebccd1;
        }
        h1, h2, h3 {
          color: #333;
        }
        pre {
          background-color: #f8f9fa;
          padding: 10px;
          border-radius: 5px;
          font-size: 0.9em;
        }
      "))
    ),
    
    # App header
    div(class = "test-header",
        h1("Universal Data Accessor with microCustomer Test"),
        p("This test app demonstrates how the microCustomer component works with different connection types"),
        p("Complies with R99 Test App Building Principles")
    ),
    
    # Test controls 
    div(class = "test-controls",
        fluidRow(
          column(4, 
                 selectInput("connection_type", "Connection Type:",
                             choices = names(connections),
                             selected = "list")
          ),
          column(4,
                 selectInput("scenario", "Test Scenario:",
                             choices = c(
                               "Complete Data" = "complete",
                               "Partial Data (3/5 customers)" = "partial",
                               "Incomplete Fields" = "incomplete_fields",
                               "Alternative Field Names" = "alt_field_names",
                               "Empty DNA Data" = "empty",
                               "NULL Data" = "null_data"
                             ),
                             selected = "complete")
          ),
          column(4,
                 checkboxInput("show_details", "Show Technical Details", value = FALSE),
                 actionButton("refresh_connection", "Refresh Connection", class = "btn-primary")
          )
        )
    ),
    
    # Information about the current connection
    conditionalPanel(
      condition = "input.show_details == true",
      div(class = "connection-panel",
          h3("Connection Information"),
          verbatimTextOutput("connection_info")
      )
    ),
    
    # Customer component
    fluidRow(
      column(4,
             # Use the filter UI from microCustomer
             wellPanel(
               h3("Customer Filter"),
               uiOutput("customer_filter_container")
             )
      ),
      column(8,
             # Use the display UI from microCustomer
             uiOutput("customer_profile_container")
      )
    ),
    
    # Debug info
    conditionalPanel(
      condition = "input.show_details == true",
      div(class = "connection-panel",
          h3("Data Details"),
          verbatimTextOutput("data_details")
      )
    ),
    
    # User Validation Panel (R99 Requirement)
    div(id = "user_validation_panel", class = "validation-panel validation-pending",
        h3("User Validation (R99 Requirement)"),
        p("According to R99, human validation is the ultimate criterion for success."),
        
        div(style = "margin-bottom: 15px;",
            strong("Current Connection Type: "), textOutput("validation_connection_type", inline = TRUE)
        ),
        
        checkboxInput("validation_ui_renders", "UI renders correctly", FALSE),
        checkboxInput("validation_data_displays", "Data displays correctly", FALSE),
        checkboxInput("validation_filter_works", "Filter functionality works", FALSE),
        checkboxInput("validation_errors_handled", "Error states handled appropriately", FALSE),
        
        div(style = "margin-top: 15px;",
            actionButton("validation_pass", "Test PASSED", class = "btn-success", width = "48%"),
            actionButton("validation_fail", "Test FAILED", class = "btn-danger", width = "48%")
        ),
        
        div(style = "margin-top: 15px;",
            textInput("validation_notes", "Notes", placeholder = "Add any notes about this test...")
        ),
        
        div(style = "margin-top: 15px; font-style: italic;",
            "Remember to test all connection types and scenarios for complete validation."
        )
    )
  )
  
  server <- function(input, output, session) {
    # Create connection based on selected type and scenario
    current_connection <- reactiveVal()
    
    # Initialize the connection
    observe({
      conn_type <- input$connection_type
      scenario_name <- input$scenario
      
      validate(
        need(conn_type, "Please select a connection type"),
        need(scenario_name, "Please select a test scenario")
      )
      
      # Get the selected scenario data
      scenario_data <- test_data$scenarios[[scenario_name]]
      
      # For function_based connection type, use the correct parameter
      connection_type <- ifelse(conn_type == "func", "function_based", conn_type)
      
      # Create a new connection with the selected type and data
      new_conn <- create_test_connection(scenario_data, connection_type)
      
      # Update the connection
      current_connection(new_conn)
      
      # Log connection creation
      message("Created ", conn_type, " connection for scenario: ", scenario_name)
    })
    
    # Refresh connection when button clicked
    observeEvent(input$refresh_connection, {
      conn_type <- input$connection_type
      scenario_name <- input$scenario
      
      # Get the selected scenario data
      scenario_data <- test_data$scenarios[[scenario_name]]
      
      # For function_based connection type, use the correct parameter
      connection_type <- ifelse(conn_type == "func", "function_based", conn_type)
      
      # Create a new connection with the selected type and data
      new_conn <- create_test_connection(scenario_data, connection_type)
      
      # Update the connection
      current_connection(new_conn)
      
      # Show notification
      showNotification(paste("Connection refreshed:", conn_type), type = "message")
    })
    
    # Display connection information
    output$connection_info <- renderPrint({
      conn <- current_connection()
      
      # Basic info about the connection
      if (is.null(conn)) {
        return("No connection available")
      }
      
      # Return information about the connection
      list(
        "Connection Class" = class(conn),
        "Connection Type" = input$connection_type,
        "Scenario" = input$scenario,
        "Available Data" = if (is.list(conn) && !is.function(conn)) names(conn) else "Dynamic data"
      )
    })
    
    # Create component (microCustomerComponent uses both UI and server parts)
    customer_component <- reactive({
      # Get the current connection
      conn <- current_connection()
      
      # Create the component
      microCustomerComponent("customer_module", conn)
    })
    
    # Render the filter UI
    output$customer_filter_container <- renderUI({
      component <- customer_component()
      component$ui$filter
    })
    
    # Render the profile UI
    output$customer_profile_container <- renderUI({
      component <- customer_component()
      component$ui$display
    })
    
    # Initialize the component server
    customer_data <- reactive({
      component <- customer_component()
      
      # This connects the microCustomerServer function
      if (is.function(component$server)) {
        component$server(input, output, session)
      } else {
        # Return NULL if no server function
        NULL
      }
    })
    
    # CRITICAL: R102 Shiny Reactive Observation Rule
    # Ensure the server function is always executed regardless of UI visibility
    observe({
      # Force customer_data() to execute even when debug panels are hidden
      customer_data()
    })
    
    # Display data details
    output$data_details <- renderPrint({
      # Get the selected customer data for debugging
      data <- customer_data()
      
      if (is.null(data) || length(data) == 0) {
        return("No data available")
      }
      
      if (is.function(data)) {
        data <- data()
      }
      
      if (is.null(data) || !is.data.frame(data) || nrow(data) == 0) {
        return("Empty data frame returned")
      }
      
      # Return basic info about the data
      list(
        "Data Dimensions" = dim(data),
        "Column Names" = colnames(data),
        "Sample Data (1st row)" = data[1, ]
      )
    })
    
    # User Validation Panel logic
    
    # Show current connection type for validation
    output$validation_connection_type <- renderText({
      input$connection_type
    })
    
    # Update validation panel class based on user validation
    observe({
      # If all items are checked and the pass button is clicked, show success
      if (input$validation_pass > 0) {
        shinyjs::addClass(selector = "#user_validation_panel", class = "validation-success")
        shinyjs::removeClass(selector = "#user_validation_panel", class = "validation-pending")
        shinyjs::removeClass(selector = "#user_validation_panel", class = "validation-failed")
        
        # Log the validation status
        message(paste("User VALIDATED connection type:", input$connection_type))
        message(paste("Validation notes:", input$validation_notes))
      } else if (input$validation_fail > 0) {
        shinyjs::addClass(selector = "#user_validation_panel", class = "validation-failed")
        shinyjs::removeClass(selector = "#user_validation_panel", class = "validation-pending")
        shinyjs::removeClass(selector = "#user_validation_panel", class = "validation-success")
        
        # Log the validation status
        message(paste("User FAILED connection type:", input$connection_type))
        message(paste("Validation notes:", input$validation_notes))
      }
    })
    
    # Reset validation panel when connection type changes
    observeEvent(input$connection_type, {
      # Reset checkboxes
      updateCheckboxInput(session, "validation_ui_renders", value = FALSE)
      updateCheckboxInput(session, "validation_data_displays", value = FALSE)
      updateCheckboxInput(session, "validation_filter_works", value = FALSE)
      updateCheckboxInput(session, "validation_errors_handled", value = FALSE)
      
      # Reset notes
      updateTextInput(session, "validation_notes", value = "")
      
      # Reset panel class
      shinyjs::addClass(selector = "#user_validation_panel", class = "validation-pending")
      shinyjs::removeClass(selector = "#user_validation_panel", class = "validation-success")
      shinyjs::removeClass(selector = "#user_validation_panel", class = "validation-failed")
    })
  }
  
  # Launch app with browser
  options(shiny.launch.browser = TRUE)
  shinyApp(ui, server, options = list(port = 5434))
}

# 8. Run Tests And Launch App

# Run unit tests if not in DEMO_MODE
if (!DEMO_MODE) {
  # Run unit tests
  test_defaults()
  test_ui_components()
  test_component_factory()
  
  # Test universal_data_accessor with different connection types
  cat("\n=== Testing universal_data_accessor with Different Connection Types ===\n")
  
  # Test list connection
  list_conn <- create_test_connection(test_data$scenarios$complete, "list")
  list_data <- universal_data_accessor(list_conn, "customer_profile", log_level = 0)
  
  test_that("universal_data_accessor works with list connection", {
    expect_true(!is.null(list_data))
    expect_true(is.data.frame(list_data))
    expect_true(nrow(list_data) > 0)
  })
  
  # Test function connection
  func_conn <- create_test_connection(test_data$scenarios$complete, "function_based")
  func_data <- universal_data_accessor(func_conn, "customer_profile", log_level = 0)
  
  test_that("universal_data_accessor works with function connection", {
    expect_true(!is.null(func_data))
    expect_true(is.data.frame(func_data))
    expect_true(nrow(func_data) > 0)
  })
  
  cat("All tests completed successfully.\n")
}

# Launch the app for interactive testing
if (DEMO_MODE || interactive()) {
  message("Launching the test app...")
  create_universal_test_app()
} else {
  message("Test script completed. Run in interactive mode to launch the test app.")
}