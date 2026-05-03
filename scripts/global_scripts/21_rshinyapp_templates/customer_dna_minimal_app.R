#' @file customer_dna_minimal_app.R
#' @principle R102 Shiny Reactive Observation Rule
#' @principle R105 Shiny App Templates Rule
#' @principle MP51 Test Data Design
#' @principle P76 Error Handling Patterns
#' 
#' @title Customer DNA Analysis Dashboard Template
#' @description
#' A minimal Shiny application that displays customer data and DNA analysis using the bs4Dash framework.
#' This template demonstrates proper modularization, reactive data flow, and adherence to project principles.
#'
#' @required_libraries shiny, bs4Dash, dplyr
#'
#' @features
#' - Modular UI and server components
#' - Customer filtering and selection
#' - RFM analysis display
#' - Proper implementation of R102 (reactive observation)
#' - Error handling with fallback values
#' - Responsive bs4Dash layout
#'
#' @usage
#' 1. Copy this template to your project
#' 2. Modify the data source to connect to your actual data
#' 3. Customize UI components as needed
#' 4. Add additional features while maintaining the modular structure

# Load required libraries
library(shiny)
library(bs4Dash)
library(dplyr)

# ======================================================
# DATA GENERATION AND CONNECTION
# ======================================================
# In a production app, this would be replaced with actual
# database connections using the universal_data_accessor
# ======================================================
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
  
  # DNA data for all customers
  dna_data <- data.frame(
    customer_id = 1:5,
    time_first = as.Date(c("2022-01-15", "2021-03-20", "2023-05-10", 
                          "2020-11-05", "2022-07-30")),
    r_label = c("極近", "一般", "極近", "非常久遠", "一般"),
    r_value = c(5, 45, 2, 120, 30),
    f_label = c("非常高", "一般", "低", "高", "一般"),
    f_value = c(12, 4, 2, 8, 5),
    m_label = c("非常高", "一般", "低", "高", "一般"),
    m_value = c(15000, 3500, 1200, 7800, 4200),
    cai_label = c("非常活躍", "一般活躍", "低度活躍", "不活躍", "一般活躍"),
    cai = c(0.85, 0.35, 0.15, 0.10, 0.40),
    nes_status = c("主力型", "成長型", "潛力型", "沉睡型", "成長型"),
    stringsAsFactors = FALSE
  )
  
  return(list(
    customer_profile = customer_data,
    dna_by_customer = dna_data
  ))
}

# Generate the data
test_data <- generate_test_data()

# Define defaults for display
get_defaults <- function() {
  list(
    dna_time_first = "N/A",
    dna_r_label = "N/A",
    dna_r_value = "0",
    dna_f_label = "N/A",
    dna_f_value = "0",
    dna_m_label = "N/A",
    dna_m_value = "0.00",
    cai_label = "N/A",
    cai = "0.00",
    nes_status = "未知"
  )
}

# ======================================================
# UI COMPONENTS
# ======================================================
# Modular UI components for reusability and maintenance
# Follows P78 Component Composition pattern
# ======================================================
create_filter_ui <- function(id) {
  ns <- NS(id)
  div(
    class = "customer-filter-container",
    selectInput(ns("customer_filter"), "選擇顧客:", 
                choices = c("全部顧客" = "", 
                            setNames(test_data$customer_profile$customer_id, 
                                    test_data$customer_profile$buyer_name)))
  )
}

create_display_ui <- function(id) {
  ns <- NS(id)
  div(
    class = "customer-display-container",
    fluidRow(
      column(6, 
             h4("顧客基本資料"),
             div(
               tags$strong("顧客ID: "),
               textOutput(ns("customer_id"), inline = TRUE)
             ),
             div(
               tags$strong("顧客姓名: "),
               textOutput(ns("customer_name"), inline = TRUE)
             ),
             div(
               tags$strong("電子郵件: "),
               textOutput(ns("customer_email"), inline = TRUE)
             ),
             div(
               tags$strong("註冊日期: "),
               textOutput(ns("registration_date"), inline = TRUE)
             )
      ),
      column(6,
             h4("顧客DNA分析"),
             div(
               tags$strong("第一次購買時間: "),
               textOutput(ns("dna_time_first"), inline = TRUE)
             ),
             div(
               tags$strong("R值 (近度): "),
               textOutput(ns("dna_r_label"), inline = TRUE),
               " (",
               textOutput(ns("dna_r_value"), inline = TRUE),
               "天)"
             ),
             div(
               tags$strong("F值 (頻度): "),
               textOutput(ns("dna_f_label"), inline = TRUE),
               " (",
               textOutput(ns("dna_f_value"), inline = TRUE),
               "次)"
             ),
             div(
               tags$strong("M值 (金額): "),
               textOutput(ns("dna_m_label"), inline = TRUE),
               " (",
               textOutput(ns("dna_m_value"), inline = TRUE),
               "元)"
             ),
             div(
               tags$strong("顧客活躍指數: "),
               textOutput(ns("cai_label"), inline = TRUE),
               " (",
               textOutput(ns("cai"), inline = TRUE),
               ")"
             ),
             div(
               tags$strong("NES顧客狀態: "),
               textOutput(ns("nes_status"), inline = TRUE)
             )
      )
    )
  )
}

# ======================================================
# SERVER LOGIC
# ======================================================
# Implements R102 Shiny Reactive Observation Rule
# Uses proper error handling with fallback values
# ======================================================
create_server <- function(id) {
  moduleServer(id, function(input, output, session) {
    # Get customer data based on filter selection
    customer_data <- reactive({
      req(test_data$customer_profile)
      
      # If no filter is selected, return all customers
      if (input$customer_filter == "") {
        return(test_data$customer_profile)
      }
      
      # Filter customers based on selection
      customer_id <- as.numeric(input$customer_filter)
      filtered <- test_data$customer_profile %>%
        filter(customer_id == !!customer_id)
      
      return(filtered)
    })
    
    # Get DNA data for the selected customer
    dna_data <- reactive({
      req(test_data$dna_by_customer, customer_data())
      
      if (nrow(customer_data()) != 1) {
        return(NULL)
      }
      
      customer_id <- customer_data()$customer_id[1]
      
      filtered <- test_data$dna_by_customer %>%
        filter(customer_id == !!customer_id)
      
      if (nrow(filtered) == 0) {
        return(NULL)
      }
      
      return(filtered)
    })
    
    # Apply R102 Shiny Reactive Observation Rule
    # Ensuring reactive expressions are always executed
    observe({
      customer_data()
      dna_data()
    })
    
    # Render basic customer info
    output$customer_id <- renderText({
      if (nrow(customer_data()) != 1) return("N/A")
      customer_data()$customer_id[1]
    })
    
    output$customer_name <- renderText({
      if (nrow(customer_data()) != 1) return("N/A")
      customer_data()$buyer_name[1]
    })
    
    output$customer_email <- renderText({
      if (nrow(customer_data()) != 1) return("N/A")
      customer_data()$email[1]
    })
    
    output$registration_date <- renderText({
      if (nrow(customer_data()) != 1) return("N/A")
      as.character(customer_data()$registration_date[1])
    })
    
    # Render DNA data with default fallbacks
    defaults <- get_defaults()
    
    output$dna_time_first <- renderText({
      if (is.null(dna_data()) || nrow(dna_data()) == 0) return(defaults$dna_time_first)
      as.character(dna_data()$time_first[1])
    })
    
    output$dna_r_label <- renderText({
      if (is.null(dna_data()) || nrow(dna_data()) == 0) return(defaults$dna_r_label)
      dna_data()$r_label[1]
    })
    
    output$dna_r_value <- renderText({
      if (is.null(dna_data()) || nrow(dna_data()) == 0) return(defaults$dna_r_value)
      dna_data()$r_value[1]
    })
    
    output$dna_f_label <- renderText({
      if (is.null(dna_data()) || nrow(dna_data()) == 0) return(defaults$dna_f_label)
      dna_data()$f_label[1]
    })
    
    output$dna_f_value <- renderText({
      if (is.null(dna_data()) || nrow(dna_data()) == 0) return(defaults$dna_f_value)
      dna_data()$f_value[1]
    })
    
    output$dna_m_label <- renderText({
      if (is.null(dna_data()) || nrow(dna_data()) == 0) return(defaults$dna_m_label)
      dna_data()$m_label[1]
    })
    
    output$dna_m_value <- renderText({
      if (is.null(dna_data()) || nrow(dna_data()) == 0) return(defaults$dna_m_value)
      dna_data()$m_value[1]
    })
    
    output$cai_label <- renderText({
      if (is.null(dna_data()) || nrow(dna_data()) == 0) return(defaults$cai_label)
      dna_data()$cai_label[1]
    })
    
    output$cai <- renderText({
      if (is.null(dna_data()) || nrow(dna_data()) == 0) return(defaults$cai)
      format(dna_data()$cai[1], digits = 2)
    })
    
    output$nes_status <- renderText({
      if (is.null(dna_data()) || nrow(dna_data()) == 0) return(defaults$nes_status)
      dna_data()$nes_status[1]
    })
  })
}

# ======================================================
# APPLICATION LAYOUT
# ======================================================
# Uses bs4Dash framework for consistent application layout
# Follows the standard bs4Dash components structure
# ======================================================
ui <- bs4DashPage(
  title = "顧客DNA分析應用",
  header = bs4DashNavbar(
    title = "顧客DNA分析系統"
  ),
  sidebar = bs4DashSidebar(
    bs4SidebarMenu(
      bs4SidebarMenuproduct(
        "顧客分析", 
        tabName = "customer", 
        icon = icon("user")
      )
    )
  ),
  body = bs4DashBody(
    bs4Tabproducts(
      bs4Tabproduct(
        tabName = "customer",
        h2("顧客DNA分析儀表板"),
        p("選擇顧客以查看其DNA分析資料"),
        fluidRow(
          column(4, 
                 bs4Card(
                   title = "顧客過濾器",
                   create_filter_ui("customer_module")
                 )
          ),
          column(8,
                 bs4Card(
                   title = "顧客詳細資料",
                   create_display_ui("customer_module")
                 )
          )
        )
      )
    )
  )
)

# ======================================================
# MAIN SERVER FUNCTION
# ======================================================
# Connects modules and initializes the application
# ======================================================
server <- function(input, output, session) {
  # Initialize customer module
  create_server("customer_module")
}

# ======================================================
# APPLICATION INITIALIZATION
# ======================================================
# Launch the Shiny application with browser option
# In production, this would be part of app.R
# ======================================================
options(shiny.launch.browser = TRUE)
shinyApp(ui, server)