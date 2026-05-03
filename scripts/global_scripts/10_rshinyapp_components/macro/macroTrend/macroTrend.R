#' @principle P15 Debug Efficiency Exception
#' @principle P76 Error Handling Patterns
#' @principle P77 Performance Optimization
#' @principle P80 Integer ID Consistency
#' @principle P81 Tidyverse-Shiny Terminology Alignment
#' @principle R76 Module Data Connection Rule
#' @principle R88 Shiny Module ID Handling
#' @principle MP52 Unidirectional Data Flow
#' @principle MP53 Feedback Loop
#' @principle MP54 UI-Server Correspondence
#' @principle R91 Universal Data Access Pattern
#' @r21_exception This file contains the UI-server-defaults triple for macroTrend module with Universal Data Access
#' @justification These components are frequently debugged together during UI development and data integration
#' @refactor_plan To be refactored back into separate files once macro-level component design is finalized (est. Q3 2025)

# universal_data_accessor is loaded during initialization, no need to load again

# NSQL Data Flow Documentation
# DATA_FLOW(component: macro_trend) {
#   SOURCE: app_data_connection
#   INITIALIZE: {
#     EXTRACT(app_data_connection → GET sales_trend_data → trend_data)
#     AGGREGATE(trend_data → GROUP BY date → daily_trend)
#     AGGREGATE(trend_data → GROUP BY date, category → category_trend)
#   }
#   ON_FILTER_CHANGE: {
#     filter = macro_filter.selected
#     FILTER(trend_data → WHERE date BETWEEN filter.start_date AND filter.end_date → filtered_trend)
#     FILTER(trend_data → WHERE category IN filter.categories → filtered_category_trend)
#     AGGREGATE(filtered_trend → GROUP BY date → display_daily_trend)
#     AGGREGATE(filtered_trend → GROUP BY date, category → display_category_trend)
#   }
# }

####macroTrendDefaults####

#' Default Values for Macro Trend Component
#'
#' This function provides standard default values for the macro trend component.
#' These defaults ensure that all UI outputs have appropriate values even when
#' data is unavailable or invalid, implementing the UI-Server Pairing Rule.
#'
#' @return Named list of output IDs and their default values
#' @export
macroTrendDefaults <- function() {
  list(
    # Trend metrics
    trend_title = "Sales Trend Analysis",
    trend_subtitle = "No data available",
    trend_period = "N/A - N/A",
    
    # Summary metrics
    total_sales = "0.00",
    average_daily_sales = "0.00",
    growth_rate = "0%",
    
    # Peak metrics
    peak_day = "N/A",
    peak_day_sales = "0.00",
    lowest_day = "N/A",
    lowest_day_sales = "0.00",
    
    # Category metrics
    top_category = "N/A",
    top_category_sales = "0.00",
    top_category_percentage = "0%",
    
    # Growth metrics
    trend_direction = "Neutral",
    momentum_score = "0.00"
  )
}

####macroTrendFilterUI####

#' Macro Trend Filter UI Component
#'
#' This component provides a filter interface for selecting date ranges, categories,
#' and other parameters for the macro-level trend analysis view.
#' Follows P75: Search Input Optimization pattern and P81: Tidyverse-Shiny Terminology Alignment.
#'
#' @param id The module ID
#' @param translate Translation function, defaults to identity function
#'
#' @return A UI component for filtering trend data
#' @export
macroTrendFilterUI <- function(id, translate = function(x) x) {
  ns <- NS(id)
  
  div(
    class = "trend-filter-container",
    style = "padding: 15px; background-color: #f8f9fa; border-radius: 5px; margin-bottom: 20px;",
    fluidRow(
      column(
        width = 12,
        h4(translate("Trend Filters"), style = "margin-bottom: 15px;"),
        
        # Date range filter
        dateRangeInput(
          inputId = ns("date_range"),
          label = translate("Select Period:"),
          start = Sys.Date() - 90,
          end = Sys.Date(),
          separator = translate(" to ")
        ),
        
        # Category filter
        selectizeInput(
          inputId = ns("category_filter"),
          label = translate("Select Categories:"),
          choices = c("All Categories" = "all"),
          multiple = TRUE,
          selected = "all",
          options = list(
            placeholder = translate("Select categories...")
          )
        ),
        
        # Granularity selector
        radioButtons(
          inputId = ns("granularity"),
          label = translate("Time Granularity:"),
          choices = c(
            "Daily" = "daily",
            "Weekly" = "weekly",
            "Monthly" = "monthly"
          ),
          selected = "daily"
        ),
        
        # View Options
        checkboxGroupInput(
          inputId = ns("view_options"),
          label = translate("View Options:"),
          choices = c(
            "Show Trend Line" = "trend_line",
            "Show Categories" = "categories",
            "Show Year-over-Year" = "yoy"
          ),
          selected = c("trend_line", "categories")
        ),
        
        # Reset button
        div(
          style = "margin-top: 10px;",
          actionButton(
            inputId = ns("reset_filters"),
            label = translate("Reset Filters"),
            icon = icon("undo"),
            class = "btn-outline-secondary",
            width = "100%"
          )
        )
      )
    )
  )
}

####macroTrendUI####

#' Macro Trend UI Component
#'
#' This component provides the UI elements for displaying detailed trend analytics
#' in the macro-level view of the application.
#'
#' IMPORTANT: According to the UI-Server Pairing Rule, this UI component MUST be used with
#' its corresponding server component macroTrendServer(). All outputs defined here
#' must be fulfilled by the server component to avoid broken displays.
#'
#' @param id The module ID
#'
#' @return A UI component
#' @export
macroTrendUI <- function(id) {
  ns <- NS(id)
  
  div(
    fluidRow(
      column(12,
        # Header section with trend overview
        div(
          class = "trend-header",
          style = "margin-bottom: 20px; padding: 15px; background-color: #f8f9fc; border-radius: 5px;",
          fluidRow(
            column(8,
              h3(textOutput(ns("trend_title"))),
              p(textOutput(ns("trend_subtitle"))),
              p(textOutput(ns("trend_period")), style = "font-style: italic;")
            ),
            column(4,
              div(
                style = "text-align: right;",
                downloadButton(ns("download_data"), translate("Download"), class = "btn-sm")
              )
            )
          )
        )
      )
    ),
    
    # Key metrics row
    fluidRow(
      column(3, bs4Dash::valueBoxOutput(ns("total_sales"), width = 12)),
      column(3, bs4Dash::valueBoxOutput(ns("average_daily_sales"), width = 12)),
      column(3, bs4Dash::valueBoxOutput(ns("growth_rate"), width = 12)),
      column(3, bs4Dash::valueBoxOutput(ns("trend_direction"), width = 12))
    ),
    
    # Main trend visualization
    fluidRow(
      column(
        width = 12,
        div(
          class = "card",
          style = "width: 100%; margin-bottom: 20px;",
          div(
            class = "card-header",
            h4(translate("Sales Trend Visualization")),
            div(
              class = "card-tools",
              div(
                class = "btn-group btn-group-sm",
                actionButton(ns("view_line"), translate("Line"), class = "btn-outline-primary"),
                actionButton(ns("view_bar"), translate("Bar"), class = "btn-outline-primary"),
                actionButton(ns("view_area"), translate("Area"), class = "btn-outline-primary")
              )
            )
          ),
          div(
            class = "card-body",
            plotOutput(ns("main_trend_plot"), height = "300px")
          )
        )
      )
    ),
    
    # Category distribution and details
    fluidRow(
      column(
        width = 6,
        div(
          class = "card",
          style = "width: 100%; margin-bottom: 20px;",
          div(
            class = "card-header",
            h4(translate("Category Distribution"))
          ),
          div(
            class = "card-body",
            plotOutput(ns("category_distribution_plot"), height = "250px")
          )
        )
      ),
      column(
        width = 6,
        div(
          class = "card",
          style = "width: 100%; margin-bottom: 20px;",
          div(
            class = "card-header",
            h4(translate("Top Categories"))
          ),
          div(
            class = "card-body",
            DT::dataTableOutput(ns("category_table"))
          )
        )
      )
    ),
    
    # Comparative analysis section
    fluidRow(
      column(
        width = 12,
        div(
          class = "card",
          style = "width: 100%; margin-bottom: 20px;",
          div(
            class = "card-header",
            h4(translate("Comparative Analysis")),
            radioButtons(
              inputId = ns("comparison_type"),
              label = NULL,
              choices = c(
                "Year-over-Year" = "yoy",
                "Month-over-Month" = "mom",
                "Category Comparison" = "category"
              ),
              selected = "yoy",
              inline = TRUE
            )
          ),
          div(
            class = "card-body",
            plotOutput(ns("comparison_plot"), height = "300px")
          )
        )
      )
    )
  )
}

####macroTrendServer####

#' Unified Macro Trend Server Component with Universal Data Access
#'
#' This component integrates both filtering and display logic for the macro trend
#' analytics view. It handles date filtering, category selection, and data visualization.
#' Implements R91 Universal Data Access Pattern for compatibility with any connection type.
#'
#' @param id The module ID
#' @param app_data_connection App data connection object providing access to data sources.
#'        This can be any of: DBI connection, list with data frames, list with functions,
#'        reactive expression, or direct data frame.
#' @param session Current Shiny session, used for registerDataObj
#'
#' @return Reactive expression that returns filtered data
#' @export
#' @implements R91 Universal Data Access Pattern
macroTrendServer <- function(id, 
                           app_data_connection = NULL,
                           session = getDefaultReactiveDomain()) {
  # MP27 Data Flow Documentation:
  # REACTIVE(trend_data) {
  #   DEPENDS_ON: [app_data_connection]
  #   SOURCE: app_data_connection
  #   TRANSFORM: EXTRACT(app_data_connection → trend_data)
  # }
  
  # Create integrated module server
  moduleServer(id, function(input, output, session) {
    # Log module initialization
    cat("INFO: Initializing macroTrend module with ID:", id, "\n")
    
    # == Set defaults ==
    defaults <- macroTrendDefaults()
    
    # == Reactive data sources - using R91 Universal Data Access Pattern ==
    
    # Get sales trend data from connection
    sales_trend_data <- reactive({
      # P76: Error handling - verify connection exists
      req(app_data_connection)
      
      # R91: Use Universal Data Access Pattern to handle different connection types
      data <- universal_data_accessor(
        data_connection = app_data_connection,
        data_name = "sales_trend_data",
        log_level = 3
      )
      
      if (is.null(data) || !is.data.frame(data) || nrow(data) == 0) {
        cat("WARNING: No sales trend data available or returned empty dataset\n")
        return(NULL)
      }
      
      # Check required columns
      required_cols <- c("date", "sales_amount", "category")
      if (!all(required_cols %in% colnames(data))) {
        cat("WARNING: Sales trend data missing required columns. Required:", 
            paste(required_cols, collapse=", "), 
            "Found:", paste(colnames(data), collapse=", "), "\n")
        return(NULL)
      }
      
      # Ensure date column is properly formatted
      if ("date" %in% colnames(data) && !is.Date(data$date)) {
        data$date <- as.Date(data$date)
      }
      
      return(data)
    })
    
    # Get available categories
    available_categories <- reactive({
      trend_data <- sales_trend_data()
      if (is.null(trend_data) || !is.data.frame(trend_data) || nrow(trend_data) == 0) {
        return(character(0))
      }
      
      if (!"category" %in% colnames(trend_data)) {
        return(character(0))
      }
      
      # Return unique categories
      unique(trend_data$category)
    })
    
    # Update category filter choices with available categories
    observe({
      categories <- available_categories()
      if (length(categories) == 0) {
        updateSelectizeInput(
          session = session,
          inputId = "category_filter",
          choices = c("All Categories" = "all"),
          selected = "all"
        )
        return()
      }
      
      # Create choices with All Categories as first option
      choices <- c("All Categories" = "all", setNames(categories, categories))
      
      updateSelectizeInput(
        session = session,
        inputId = "category_filter",
        choices = choices,
        selected = "all"
      )
    })
    
    # Filter data based on user selections
    filtered_data <- reactive({
      trend_data <- sales_trend_data()
      if (is.null(trend_data) || !is.data.frame(trend_data) || nrow(trend_data) == 0) {
        return(NULL)
      }
      
      # Start with all data
      filtered <- trend_data
      
      # Apply date filter if available
      if (!is.null(input$date_range)) {
        start_date <- input$date_range[1]
        end_date <- input$date_range[2]
        
        filtered <- filtered %>%
          dplyr::filter(date >= start_date & date <= end_date)
      }
      
      # Apply category filter if not set to "all"
      if (!is.null(input$category_filter) && length(input$category_filter) > 0 && 
          !("all" %in% input$category_filter)) {
        filtered <- filtered %>%
          dplyr::filter(category %in% input$category_filter)
      }
      
      # Return filtered data
      return(filtered)
    })
    
    # Generate aggregated data based on selected granularity
    aggregated_data <- reactive({
      data <- filtered_data()
      if (is.null(data) || nrow(data) == 0) {
        return(NULL)
      }
      
      # Define date grouping function based on granularity
      date_group <- switch(input$granularity,
        daily = function(date) date,
        weekly = function(date) {
          # Get first day of the week (Monday)
          date - lubridate::wday(date, week_start = 1) + 1
        },
        monthly = function(date) {
          # Get first day of the month
          lubridate::floor_date(date, "month")
        },
        function(date) date  # Default to daily
      )
      
      # Apply date grouping
      data$group_date <- date_group(data$date)
      
      # Aggregate by grouped date
      agg_data <- data %>%
        dplyr::group_by(group_date) %>%
        dplyr::summarize(
          total_sales = sum(sales_amount, na.rm = TRUE),
          avg_sales = mean(sales_amount, na.rm = TRUE),
          transaction_count = n(),
          .groups = "drop"
        ) %>%
        dplyr::arrange(group_date)
      
      return(agg_data)
    })
    
    # Generate category aggregation
    category_data <- reactive({
      data <- filtered_data()
      if (is.null(data) || nrow(data) == 0) {
        return(NULL)
      }
      
      # Aggregate by category
      cat_data <- data %>%
        dplyr::group_by(category) %>%
        dplyr::summarize(
          total_sales = sum(sales_amount, na.rm = TRUE),
          transaction_count = n(),
          avg_sales = mean(sales_amount, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        dplyr::arrange(desc(total_sales))
      
      # Calculate percentage
      cat_data$percentage <- cat_data$total_sales / sum(cat_data$total_sales) * 100
      
      return(cat_data)
    })
    
    # Calculate summary metrics
    summary_metrics <- reactive({
      data <- filtered_data()
      agg_data <- aggregated_data()
      
      if (is.null(data) || nrow(data) == 0 || is.null(agg_data) || nrow(agg_data) == 0) {
        return(list(
          total_sales = NA,
          avg_daily_sales = NA,
          growth_rate = NA,
          trend_direction = NA,
          period_start = NA,
          period_end = NA
        ))
      }
      
      # Calculate period boundaries
      period_start <- min(data$date, na.rm = TRUE)
      period_end <- max(data$date, na.rm = TRUE)
      period_days <- as.numeric(difftime(period_end, period_start, units = "days")) + 1
      
      # Calculate basic metrics
      total_sales <- sum(data$sales_amount, na.rm = TRUE)
      avg_daily_sales <- total_sales / period_days
      
      # Calculate trend direction
      if (nrow(agg_data) > 1) {
        # Simple linear model for trend
        trend_model <- lm(total_sales ~ as.numeric(group_date), data = agg_data)
        trend_coefficient <- coef(trend_model)[2]
        trend_direction <- if (is.na(trend_coefficient)) "Neutral" else
                           if (trend_coefficient > 0) "Positive" else 
                           if (trend_coefficient < 0) "Negative" else "Neutral"
        
        # Calculate growth rate - compare first half to second half of period
        mid_point <- period_start + (period_end - period_start)/2
        first_half <- sum(data$sales_amount[data$date <= mid_point], na.rm = TRUE)
        second_half <- sum(data$sales_amount[data$date > mid_point], na.rm = TRUE)
        
        # Avoid division by zero
        if (first_half > 0) {
          growth_rate <- (second_half - first_half) / first_half * 100
        } else {
          growth_rate <- 0
        }
      } else {
        trend_direction <- "Neutral"
        growth_rate <- 0
      }
      
      # Return metrics
      list(
        total_sales = total_sales,
        avg_daily_sales = avg_daily_sales,
        growth_rate = growth_rate,
        trend_direction = trend_direction,
        period_start = period_start,
        period_end = period_end
      )
    })
    
    # == UI Output Rendering ==
    
    # Render trend title and subtitle
    output$trend_title <- renderText({
      metrics <- summary_metrics()
      if (is.null(metrics) || is.na(metrics$total_sales)) {
        return(defaults$trend_title)
      }
      
      # Create title with category information
      if (!is.null(input$category_filter) && length(input$category_filter) > 0 && 
          !("all" %in% input$category_filter)) {
        categories <- input$category_filter
        if (length(categories) == 1) {
          return(paste("Sales Trend Analysis -", categories))
        } else {
          return(paste("Sales Trend Analysis -", length(categories), "Categories"))
        }
      }
      
      return("Sales Trend Analysis - All Categories")
    })
    
    output$trend_subtitle <- renderText({
      metrics <- summary_metrics()
      if (is.null(metrics) || is.na(metrics$total_sales)) {
        return(defaults$trend_subtitle)
      }
      
      # Create directional subtitle
      direction <- metrics$trend_direction
      growth <- metrics$growth_rate
      
      if (direction == "Positive") {
        return(paste("Sales showing upward trend with", round(growth, 1), "% growth"))
      } else if (direction == "Negative") {
        return(paste("Sales showing downward trend with", round(growth, 1), "% decline"))
      } else {
        return("Sales showing stable trend")
      }
    })
    
    output$trend_period <- renderText({
      metrics <- summary_metrics()
      if (is.null(metrics) || is.na(metrics$period_start) || is.na(metrics$period_end)) {
        return(defaults$trend_period)
      }
      
      # Format period display
      start_formatted <- format(metrics$period_start, "%b %d, %Y")
      end_formatted <- format(metrics$period_end, "%b %d, %Y")
      return(paste("Period:", start_formatted, "to", end_formatted))
    })
    
    # Render value boxes
    output$total_sales <- renderValueBox({
      metrics <- summary_metrics()
      if (is.null(metrics) || is.na(metrics$total_sales)) {
        value <- defaults$total_sales
      } else {
        value <- round(metrics$total_sales, 2)
      }
      
      bs4Dash::valueBox(
        value = value,
        subtitle = "Total Sales",
        icon = icon("dollar-sign"),
        color = "primary"
      )
    })
    
    output$average_daily_sales <- renderValueBox({
      metrics <- summary_metrics()
      if (is.null(metrics) || is.na(metrics$avg_daily_sales)) {
        value <- defaults$average_daily_sales
      } else {
        value <- round(metrics$avg_daily_sales, 2)
      }
      
      bs4Dash::valueBox(
        value = value,
        subtitle = "Average Daily Sales",
        icon = icon("chart-bar"),
        color = "info"
      )
    })
    
    output$growth_rate <- renderValueBox({
      metrics <- summary_metrics()
      if (is.null(metrics) || is.na(metrics$growth_rate)) {
        value <- defaults$growth_rate
        color <- "secondary"
      } else {
        growth <- metrics$growth_rate
        value <- paste0(round(growth, 1), "%")
        
        # Set color based on growth direction
        if (growth > 5) {
          color <- "success"
        } else if (growth < -5) {
          color <- "danger"
        } else {
          color <- "warning"
        }
      }
      
      bs4Dash::valueBox(
        value = value,
        subtitle = "Growth Rate",
        icon = icon("percentage"),
        color = color
      )
    })
    
    output$trend_direction <- renderValueBox({
      metrics <- summary_metrics()
      if (is.null(metrics) || is.na(metrics$trend_direction)) {
        value <- defaults$trend_direction
        color <- "secondary"
      } else {
        direction <- metrics$trend_direction
        value <- direction
        
        # Set color based on direction
        if (direction == "Positive") {
          color <- "success"
          icon_name <- "arrow-trend-up"
        } else if (direction == "Negative") {
          color <- "danger"
          icon_name <- "arrow-trend-down"
        } else {
          color <- "warning"
          icon_name <- "equals"
        }
      }
      
      bs4Dash::valueBox(
        value = value,
        subtitle = "Trend Direction",
        icon = icon(icon_name),
        color = color
      )
    })
    
    # Render main trend plot
    output$main_trend_plot <- renderPlot({
      data <- aggregated_data()
      if (is.null(data) || nrow(data) == 0) {
        # Create empty plot with message
        return(
          ggplot() + 
            annotate("text", x = 0, y = 0, label = "No data available") + 
            theme_minimal() +
            theme(
              axis.title = element_blank(),
              axis.text = element_blank(),
              axis.ticks = element_blank(),
              panel.grid = element_blank()
            )
        )
      }
      
      # Determine plot type based on button clicks
      plot_type <- "line"  # Default
      if (!is.null(input$view_bar) && input$view_bar > 0) {
        plot_type <- "bar"
      } else if (!is.null(input$view_area) && input$view_area > 0) {
        plot_type <- "area"
      }
      
      # Create base plot
      p <- ggplot(data, aes(x = group_date, y = total_sales)) +
        labs(
          title = "Sales Trend",
          x = "Date",
          y = "Sales Amount"
        ) +
        theme_minimal()
      
      # Add appropriate geom based on plot type
      if (plot_type == "bar") {
        p <- p + geom_col(fill = "steelblue", alpha = 0.8)
      } else if (plot_type == "area") {
        p <- p + geom_area(fill = "steelblue", alpha = 0.6)
      } else {  # line
        p <- p + 
          geom_line(color = "steelblue", size = 1) +
          geom_point(color = "steelblue", size = 2)
      }
      
      # Add trend line if requested
      if (is.null(input$view_options) || "trend_line" %in% input$view_options) {
        p <- p + geom_smooth(method = "loess", se = FALSE, color = "red", linetype = "dashed")
      }
      
      # Format axes
      p <- p + 
        scale_y_continuous(labels = scales::comma) +
        theme(
          plot.title = element_text(face = "bold"),
          axis.title = element_text(face = "bold"),
          legend.position = "bottom"
        )
      
      return(p)
    })
    
    # Render category distribution plot
    output$category_distribution_plot <- renderPlot({
      data <- category_data()
      if (is.null(data) || nrow(data) == 0) {
        # Create empty plot with message
        return(
          ggplot() + 
            annotate("text", x = 0, y = 0, label = "No category data available") + 
            theme_minimal() +
            theme(
              axis.title = element_blank(),
              axis.text = element_blank(),
              axis.ticks = element_blank(),
              panel.grid = element_blank()
            )
        )
      }
      
      # Limit to top 10 categories for readability
      if (nrow(data) > 10) {
        data <- data[1:10,]
      }
      
      # Create pie chart for category distribution
      ggplot(data, aes(x = "", y = total_sales, fill = category)) +
        geom_bar(stat = "identity", width = 1) +
        coord_polar("y", start = 0) +
        labs(
          title = "Category Distribution",
          fill = "Category"
        ) +
        scale_fill_brewer(palette = "Paired") +
        theme_minimal() +
        theme(
          axis.title = element_blank(),
          axis.text = element_blank(),
          axis.ticks = element_blank(),
          panel.grid = element_blank(),
          plot.title = element_text(face = "bold", hjust = 0.5),
          legend.position = "right"
        )
    })
    
    # Render category table
    output$category_table <- DT::renderDataTable({
      data <- category_data()
      if (is.null(data) || nrow(data) == 0) {
        return(data.frame(
          Category = character(0),
          Sales = numeric(0),
          Percentage = numeric(0)
        ))
      }
      
      # Format the data for display
      display_data <- data.frame(
        Category = data$category,
        Sales = round(data$total_sales, 2),
        Percentage = paste0(round(data$percentage, 1), "%"),
        Transactions = data$transaction_count,
        "Avg Sale" = round(data$avg_sales, 2)
      )
      
      # Create the datatable
      DT::datatable(
        display_data,
        options = list(
          pageLength = 5,
          lengthMenu = c(5, 10, 15),
          dom = "tp",
          ordering = TRUE,
          scrollY = "200px"
        ),
        rownames = FALSE,
        selection = "single"
      )
    })
    
    # Handle comparison plot
    output$comparison_plot <- renderPlot({
      data <- filtered_data()
      if (is.null(data) || nrow(data) == 0) {
        # Create empty plot with message
        return(
          ggplot() + 
            annotate("text", x = 0, y = 0, label = "No comparison data available") + 
            theme_minimal() +
            theme(
              axis.title = element_blank(),
              axis.text = element_blank(),
              axis.ticks = element_blank(),
              panel.grid = element_blank()
            )
        )
      }
      
      # Get comparison type
      comp_type <- if (!is.null(input$comparison_type)) input$comparison_type else "yoy"
      
      # Create different plots based on comparison type
      if (comp_type == "yoy") {
        # Create Year-over-Year comparison
        data$year <- lubridate::year(data$date)
        data$month_day <- format(data$date, "%m-%d")
        
        # Aggregate by year and month-day
        yoy_data <- data %>%
          dplyr::group_by(year, month_day) %>%
          dplyr::summarize(
            total_sales = sum(sales_amount, na.rm = TRUE),
            .groups = "drop"
          )
        
        # Plot YoY comparison
        ggplot(yoy_data, aes(x = month_day, y = total_sales, color = factor(year), group = factor(year))) +
          geom_line() +
          geom_point() +
          labs(
            title = "Year-over-Year Comparison",
            x = "Month-Day",
            y = "Sales Amount",
            color = "Year"
          ) +
          scale_y_continuous(labels = scales::comma) +
          theme_minimal() +
          theme(
            plot.title = element_text(face = "bold"),
            axis.title = element_text(face = "bold"),
            legend.position = "bottom",
            axis.text.x = element_text(angle = 45, hjust = 1)
          )
      } else if (comp_type == "mom") {
        # Create Month-over-Month comparison
        data$year_month <- format(data$date, "%Y-%m")
        data$day <- lubridate::day(data$date)
        
        # Aggregate by year-month and day
        mom_data <- data %>%
          dplyr::group_by(year_month, day) %>%
          dplyr::summarize(
            total_sales = sum(sales_amount, na.rm = TRUE),
            .groups = "drop"
          )
        
        # Plot MoM comparison
        ggplot(mom_data, aes(x = day, y = total_sales, color = year_month, group = year_month)) +
          geom_line() +
          geom_point() +
          labs(
            title = "Month-over-Month Comparison",
            x = "Day of Month",
            y = "Sales Amount",
            color = "Year-Month"
          ) +
          scale_y_continuous(labels = scales::comma) +
          theme_minimal() +
          theme(
            plot.title = element_text(face = "bold"),
            axis.title = element_text(face = "bold"),
            legend.position = "bottom"
          )
      } else {
        # Category comparison
        if (!"category" %in% colnames(data)) {
          return(
            ggplot() + 
              annotate("text", x = 0, y = 0, label = "No category data available") + 
              theme_minimal() +
              theme(
                axis.title = element_blank(),
                axis.text = element_blank(),
                axis.ticks = element_blank(),
                panel.grid = element_blank()
              )
          )
        }
        
        # Aggregate by date and category
        cat_time_data <- data %>%
          dplyr::group_by(date, category) %>%
          dplyr::summarize(
            total_sales = sum(sales_amount, na.rm = TRUE),
            .groups = "drop"
          )
        
        # Limit to top 5 categories if there are many
        top_categories <- names(sort(tapply(cat_time_data$total_sales, cat_time_data$category, sum), decreasing = TRUE))
        if (length(top_categories) > 5) {
          top_categories <- top_categories[1:5]
          cat_time_data <- cat_time_data %>%
            dplyr::filter(category %in% top_categories)
        }
        
        # Plot category comparison over time
        ggplot(cat_time_data, aes(x = date, y = total_sales, color = category, group = category)) +
          geom_line() +
          geom_point(size = 1) +
          labs(
            title = "Category Performance Over Time",
            x = "Date",
            y = "Sales Amount",
            color = "Category"
          ) +
          scale_y_continuous(labels = scales::comma) +
          theme_minimal() +
          theme(
            plot.title = element_text(face = "bold"),
            axis.title = element_text(face = "bold"),
            legend.position = "bottom"
          )
      }
    })
    
    # Handle download button
    output$download_data <- downloadHandler(
      filename = function() {
        paste("sales_trend_data_", format(Sys.Date(), "%Y-%m-%d"), ".csv", sep = "")
      },
      content = function(file) {
        data <- filtered_data()
        if (is.null(data) || nrow(data) == 0) {
          # Create empty data frame with headers
          empty_data <- data.frame(
            date = character(),
            category = character(),
            sales_amount = numeric()
          )
          write.csv(empty_data, file, row.names = FALSE)
        } else {
          write.csv(data, file, row.names = FALSE)
        }
      }
    )
    
    # Reset filters button handler
    observeEvent(input$reset_filters, {
      # Reset date range
      updateDateRangeInput(
        session = session,
        inputId = "date_range",
        start = Sys.Date() - 90,
        end = Sys.Date()
      )
      
      # Reset category filter
      updateSelectizeInput(
        session = session,
        inputId = "category_filter",
        selected = "all"
      )
      
      # Reset granularity
      updateRadioButtons(
        session = session,
        inputId = "granularity",
        selected = "daily"
      )
      
      # Reset view options
      updateCheckboxGroupInput(
        session = session,
        inputId = "view_options",
        selected = c("trend_line", "categories")
      )
    })
    
    # Reset view type when switching tabs
    observeEvent(input$view_line, {
      # Nothing to do, as this is the default
    })
    
    # Return filtered data
    return(filtered_data)
  })
}

####macroTrendInitialize####

#' Initialize the Macro Trend components with Universal Data Access
#'
#' Integrates the filter and display components following R91 Universal Data Access Pattern.
#'
#' @param id Module ID
#' @param app_data_connection App data connection (any supported connection type)
#' @param config Optional configuration parameters
#'
#' @return A list containing UI elements and server function
#' @export
macroTrendInitialize <- function(id, app_data_connection = NULL, config = NULL) {
  # R88: Don't use custom namespace suffix to prevent double namespace issues
  # Use Shiny's built-in namespace mechanism
  
  # Return the components
  list(
    ui = list(
      filter = macroTrendFilterUI(id),
      display = macroTrendUI(id)
    ),
    server = function(input, output, session) {
      # Use unified ID
      filtered_data <- macroTrendServer(
        id, 
        app_data_connection
      )
      
      # Return the filtered data for potential outside use
      return(filtered_data)
    }
  )
}