#' @principle P15 Debug Efficiency Exception
#' @r21_exception This file contains the UI-server-defaults triple for customer_profile module
#' @justification These components are frequently debugged together during UI adjustments
#' @refactor_plan To be refactored after the UI design is finalized (est. Q3 2025)

#' Customer Profile UI Function
#'
#' Creates the UI elements for the customer profile module
#'
#' @param id Module ID
#' @return UI elements for customer profile
#'
customer_profileUI <- function(id) {
  ns <- shiny::NS(id)
  
  shiny::tagList(
    shiny::fluidRow(
      shiny::column(
        width = 12,
        shiny::h3("Customer Profile"),
        shiny::tabsetPanel(
          id = ns("profile_tabs"),
          shiny::tabPanel(
            "Overview",
            shiny::br(),
            shiny::fluidRow(
              shiny::column(
                width = 6,
                shiny::h4("Basic Information"),
                shiny::uiOutput(ns("basic_info"))
              ),
              shiny::column(
                width = 6,
                shiny::h4("Purchase History"),
                shiny::plotOutput(ns("purchase_plot"))
              )
            )
          ),
          shiny::tabPanel(
            "Detailed Analysis",
            shiny::br(),
            shiny::fluidRow(
              shiny::column(
                width = 4,
                shiny::selectInput(
                  ns("time_scale"),
                  "Time Scale",
                  choices = c("Month", "Quarter", "Year"),
                  selected = "Quarter"
                )
              ),
              shiny::column(
                width = 8,
                shiny::plotOutput(ns("detailed_plot"))
              )
            ),
            shiny::fluidRow(
              shiny::column(
                width = 12,
                shiny::dataTableOutput(ns("detailed_table"))
              )
            )
          )
        )
      )
    )
  )
}

#' Customer Profile Server Function
#'
#' Implements the server logic for the customer profile module
#'
#' @param id Module ID
#' @param customer_data Reactive expression returning customer data
#' @return Server logic for customer profile
#'
customer_profile <- function(id, customer_data) {
  shiny::moduleServer(id, function(input, output, session) {
    ns <- session$ns
    
    # Get default values
    defaults <- customer_profile_defaults()
    
    # Process customer data
    processed_data <- shiny::reactive({
      req(customer_data())
      
      # Apply data transformation
      data <- customer_data()
      data$purchase_amount <- as.numeric(data$purchase_amount)
      data$purchase_date <- as.Date(data$purchase_date)
      
      # Add calculated fields
      data$month <- format(data$purchase_date, "%Y-%m")
      data$quarter <- formattime(data$purchase_date, "quarter")
      data$year <- format(data$purchase_date, "%Y")
      
      return(data)
    })
    
    # Basic information output
    output$basic_info <- shiny::renderUI({
      req(processed_data())
      data <- processed_data()
      
      # Extract customer information
      customer_id <- unique(data$customer_id)[1]
      first_purchase <- min(data$purchase_date)
      total_purchases <- nrow(data)
      total_spent <- sum(data$purchase_amount)
      
      shiny::tagList(
        shiny::p(shiny::strong("Customer ID: "), customer_id),
        shiny::p(shiny::strong("First Purchase: "), format(first_purchase, "%Y-%m-%d")),
        shiny::p(shiny::strong("Total Purchases: "), total_purchases),
        shiny::p(shiny::strong("Total Spent: "), sprintf("$%.2f", total_spent))
      )
    })
    
    # Purchase history plot
    output$purchase_plot <- shiny::renderPlot({
      req(processed_data())
      data <- processed_data()
      
      # Create time series of purchases
      ggplot2::ggplot(data, ggplot2::aes(x = purchase_date, y = purchase_amount)) +
        ggplot2::geom_line() +
        ggplot2::geom_point() +
        ggplot2::labs(title = "Purchase History", 
                     x = "Date", 
                     y = "Amount ($)") +
        ggplot2::theme_minimal()
    })
    
    # Prepare detailed data based on selected time scale
    detailed_data <- shiny::reactive({
      req(processed_data(), input$time_scale)
      data <- processed_data()
      
      # Group by selected time scale
      time_var <- switch(tolower(input$time_scale),
                         "month" = "month",
                         "quarter" = "quarter",
                         "year" = "year")
      
      # Aggregate data
      result <- data %>%
        dplyr::group_by(time_period = .data[[time_var]]) %>%
        dplyr::summarise(
          total_amount = sum(purchase_amount),
          avg_amount = mean(purchase_amount),
          num_purchases = dplyr::n(),
          .groups = 'drop'
        ) %>%
        dplyr::arrange(time_period)
      
      return(result)
    })
    
    # Detailed plot
    output$detailed_plot <- shiny::renderPlot({
      req(detailed_data())
      data <- detailed_data()
      
      # Create bar chart
      ggplot2::ggplot(data, ggplot2::aes(x = time_period, y = total_amount)) +
        ggplot2::geom_col(fill = defaults$plot_color) +
        ggplot2::labs(title = paste("Sales by", input$time_scale), 
                     x = input$time_scale, 
                     y = "Total Amount ($)") +
        ggplot2::theme_minimal() +
        ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 45, hjust = 1))
    })
    
    # Detailed table
    output$detailed_table <- shiny::renderDataTable({
      req(detailed_data())
      data <- detailed_data()
      
      # Format columns
      data$total_amount <- sprintf("$%.2f", data$total_amount)
      data$avg_amount <- sprintf("$%.2f", data$avg_amount)
      
      # Rename columns
      data <- data %>%
        dplyr::rename(
          "Time Period" = time_period,
          "Total Amount" = total_amount,
          "Average Amount" = avg_amount,
          "Number of Purchases" = num_purchases
        )
      
      return(data)
    }, options = list(
      pageLength = 10,
      searching = FALSE
    ))
    
    # Return reactive expressions for testing
    return(list(
      processed_data = processed_data,
      detailed_data = detailed_data
    ))
  })
}

#' Customer Profile Defaults
#'
#' Provides default values for the customer profile module
#'
#' @return List of default values
#'
customer_profile_defaults <- function() {
  list(
    default_time_scale = "Quarter",
    plot_color = "#1E88E5",
    date_format = "%Y-%m-%d",
    currency_format = "$%.2f"
  )
}