#LOCK FILE
#
# macroNESTransition.R
#
# Following principles:
# - MP56: Connected Component Principle (component structure)
# - MP55: Computation Allocation Principle (pre-computation)
# - MP73: Interactive Visualization Preference (plotly for visualizations)
# - MP81: Explicit Parameter Specification (function arguments)
# - R116: Enhanced Data Access with tbl2 (data access)
# - R91: Universal Data Access Pattern (data access)
# - R09: UI-Server-Defaults Triple (component organization)
# - P006: Data Visualization (visualization standards)
#

# helper ----------------------------------------------------------------------
#' Paste operator for string concatenation
#' @param x Character string. First string to concatenate.
#' @param y Character string. Second string to concatenate.
#' @return Character string. The concatenated result of x and y.
`%+%` <- function(x, y) paste0(x, y)

#' Recode time values to their past-period equivalents
#' @param profile Character string. The time profile to recode.
#' @return Character string. The recoded time profile.
Recode_time_TraceBack <- function(profile) {
  switch(profile,
         "m1quarter" = "m1quarter",
         "m1year" = "m1year",
         "m1month" = "m1month",
         "quarter" = "m1quarter",
         "year" = "m1year",
         "month" = "m1month",
         NA)  # Return NA if no match found
}

# Filter UI -------------------------------------------------------------------
#' macroNESTransitionFilterUI
#' @param id Character string. The module ID used for namespacing inputs and outputs.
#' @param translate Function. Translation function for UI text elements (defaults to identity function).
#'        Should accept a string and return a translated string.
#' @return shiny.tag. A Shiny UI component containing the filter controls for the NES transition component.
macroNESTransitionFilterUI <- function(id, translate = identity) {
  ns <- NS(id)
  wellPanel(
    style = "padding:15px;",
    h4(translate("Time Period")),
    selectInput(ns("time_scale_profile"), translate("Time Scale"),
                choices = c("Month" = "month", "Quarter" = "quarter", "Year" = "year"),
                selected = "quarter"),
    selectInput(ns("distribution_channel"), translate("Distribution Channel"),
                choices = c("All" = "all"), selected = "all"),
    selectInput(ns("product_category"), translate("Product Category"),
                choices = c("All" = "000"), selected = "000"),
    selectInput(ns("geo"), translate("Geography"),
                choices = c("All" = "all"), selected = "all"),
    hr(),
    actionButton(ns("refresh_data"), translate("Refresh Data"), class = "btn-primary"),
    hr(),
    textOutput(ns("component_status"))
  )
}

# Display UI ------------------------------------------------------------------
#' macroNESTransitionDisplayUI
#' @param id Character string. The module ID used for namespacing inputs and outputs.
#' @return shiny.tag. A Shiny UI component containing the display elements for the NES transition visualizations.
macroNESTransitionDisplayUI <- function(id) {
  ns <- NS(id)
  tagList(
    div(class = "component-header mb-3 text-center",
        h3("Customer NES Status Transition Analysis"),
        p("Visualize how customers transition between New/Existing/Sleeping status")),
    div(class = "component-output",
        plotlyOutput(ns("nes_transition_plot"), height = "450px"),
        uiOutput(ns("transition_stats")))
  )
}

# Server ----------------------------------------------------------------------
#' macroNESTransitionServer
#' @param id Character string. The module ID used for namespacing inputs and outputs.
#' @param app_data_connection Database connection object or list. Any connection type supported by tbl2.
#'        Can be a DBI connection, a list with getter functions, a file path, or NULL if no database access is needed.
#' @param config List or reactive expression. Optional configuration settings that can customize behavior.
#'        If reactive, will be re-evaluated when dependencies change.
#' @param session Shiny session object. The current Shiny session (defaults to getDefaultReactiveDomain()).
#' @return list. A list of reactive values providing access to component state and data:
#'        - component_status: reactive value indicating the current status (idle, loading, ready, etc.)
#'        - nes_transition_data: reactive containing the processed NES transition data
macroNESTransitionServer <- function(id, app_data_connection = NULL, config = NULL,
                                    session = getDefaultReactiveDomain()) {
  moduleServer(id, function(input, output, session) {
    
    # ------------ 狀態 ---------------------------------------------
    #' Reactive value for tracking component status
    #' @description Tracks the current status of the component (loading, ready, etc.)
    #' @return Character string. One of "idle", "loading", "ready", "computing", or "error"
    component_status <- reactiveVal("idle")
    
    # ------------ 解析 platform_id ----------------------------------
    #' Extract platform_id from configuration
    #' 
    #' Parses the provided configuration (reactive or static) to extract platform_id.
    #' Handles both direct platform_id and nested platform_id in filters.
    #' 
    #' @return Character or NULL. The extracted platform_id or NULL if not found.
    platform_id <- reactive({
      # Handle both reactive and non-reactive config
      cfg <- if (is.function(config) && "reactive" %in% class(config)) config() else config
      
      # Extract platform_id from config if available
      if (!is.null(cfg)) {
        # Check for direct platform_id
        if (!is.null(cfg[["platform_id"]])) {
          return(cfg[["platform_id"]])
        }
        # Check for nested platform_id in filters
        if (!is.null(cfg[["filters"]]) && !is.null(cfg[["filters"]][["platform_id"]])) {
          return(cfg[["filters"]][["platform_id"]])
        }
      }
      
      # Return NULL if no platform_id found
      NULL
    })

    # ------------ 選項載入 ----------------------------------
    #' Update distribution channel select input options based on available data
    observe({
      req(app_data_connection)
      
      # Update component status to show loading
      component_status("loading")
      
      # Get platform ID from config
      plat <- platform_id()
      
      # Get unique distribution channels
      tryCatch({
        # Access data directly using tbl2 (R116)
        if (!is.null(plat) && nzchar(plat) && plat != "all") {
          # Apply platform filter when a valid platform_id is provided
          platform_id <- as.character(plat)
          # SLN08: Use !! to force evaluation of variables before SQL translation
          tbl <- tbl2(app_data_connection, "sales_by_customer") %>%
            dplyr::filter(platform_id == !!platform_id)
        } else {
          # Use tbl2 without filter
          tbl <- tbl2(app_data_connection, "sales_by_customer")
        }
        
        # Get unique distribution sources
        channels <- tbl %>%
          dplyr::distinct(source) %>%
          dplyr::collect() %>%
          dplyr::pull(source)
        
        # Add 'all' option and update
        channels <- c("All" = "all", setNames(channels, channels))
        updateSelectInput(session, "distribution_channel", choices = channels)
        
        # Update component status 
        component_status("ready")
      }, error = function(e) {
        message("Error fetching channel data: ", e$message)
        component_status("error")
      })
    })
    
    #' Update product category select input options based on available data  
    observe({
      req(app_data_connection)
      
      tryCatch({
        # Access product categories data
        plat <- platform_id()
        
        # Access data using tbl2 (R116)
        if (!is.null(plat) && nzchar(plat) && plat != "all") {
          platform_id <- as.character(plat)
          tbl <- tbl2(app_data_connection, "product_categories") %>%
            dplyr::filter(platform_id == !!platform_id)
        } else {
          tbl <- tbl2(app_data_connection, "product_categories")
        }
        
        # Get categories with their names
        categories <- tbl %>%
          dplyr::select(id, name) %>%
          dplyr::collect() %>%
          dplyr::mutate(label = paste0(name, " (", id, ")"))
        
        # Create a named vector and update
        if (nrow(categories) > 0) {
          cat_choices <- c("All" = "000", setNames(categories$id, categories$label))
          updateSelectInput(session, "product_category", choices = cat_choices)
        }
      }, error = function(e) {
        message("Error fetching category data: ", e$message)
      })
    })
    
    #' Update geography select input options
    observe({
      req(app_data_connection)
      
      tryCatch({
        # Access geography data
        plat <- platform_id()
        
        # Access data using tbl2 (R116)
        if (!is.null(plat) && nzchar(plat) && plat != "all") {
          platform_id <- as.character(plat)
          tbl <- tbl2(app_data_connection, "sales_by_customer") %>%
            dplyr::filter(platform_id == !!platform_id)
        } else {
          tbl <- tbl2(app_data_connection, "sales_by_customer")
        }
        
        # Get unique geographies
        geos <- tbl %>%
          dplyr::distinct(state) %>%
          dplyr::collect() %>%
          dplyr::pull(state)
        
        # Add 'all' option and update
        geos <- c("All" = "all", setNames(geos, geos))
        updateSelectInput(session, "geo", choices = geos)
      }, error = function(e) {
        message("Error fetching geography data: ", e$message)
      })
    })
    
    # ------------ 資料處理 ----------------------------------------
    #' Process NES transition data
    #' 
    #' Creates a data frame with NES transition information between current and past periods
    nes_transition_data <- reactive({
      req(app_data_connection)
      req(input$distribution_channel)
      req(input$product_category)
      req(input$geo)
      req(input$time_scale_profile)
      
      # Update component status to show computing
      component_status("computing")
      
      # Get platform ID from config
      plat <- platform_id()
      
      # Get selected filters
      distribution_channel <- input$distribution_channel
      category <- input$product_category
      geo <- input$geo
      time_scale <- input$time_scale_profile
      
      # Calculate previous time period
      time_scale_past <- Recode_time_TraceBack(time_scale)
      
      # Extract current period data
      sales_by_customer_now <- tbl2(app_data_connection, "sales_by_customer")
      
      # Apply filters
      if (!is.null(plat) && nzchar(plat) && plat != "all") {
        sales_by_customer_now <- sales_by_customer_now %>% 
          dplyr::filter(platform_id == !!as.character(plat))
      }
      
      if (distribution_channel != "all") {
        sales_by_customer_now <- sales_by_customer_now %>% 
          dplyr::filter(source == !!distribution_channel)
      }
      
      if (category != "000") {
        sales_by_customer_now <- sales_by_customer_now %>% 
          dplyr::filter(product_line_id == !!category)
      }
      
      if (geo != "all") {
        sales_by_customer_now <- sales_by_customer_now %>% 
          dplyr::filter(state == !!geo)
      }
      
      sales_by_customer_now <- sales_by_customer_now %>% 
        dplyr::filter(time_condition == "now") %>%
        dplyr::select(customer_id, nesstatus)
      
      # Extract past period data (with same filters but different time condition)
      sales_by_customer_past <- tbl2(app_data_connection, "sales_by_customer")
      
      # Apply filters
      if (!is.null(plat) && nzchar(plat) && plat != "all") {
        sales_by_customer_past <- sales_by_customer_past %>% 
          dplyr::filter(platform_id == !!as.character(plat))
      }
      
      if (distribution_channel != "all") {
        sales_by_customer_past <- sales_by_customer_past %>% 
          dplyr::filter(source == !!distribution_channel)
      }
      
      if (category != "000") {
        sales_by_customer_past <- sales_by_customer_past %>% 
          dplyr::filter(product_line_id == !!category)
      }
      
      if (geo != "all") {
        sales_by_customer_past <- sales_by_customer_past %>% 
          dplyr::filter(state == !!geo)
      }
      
      sales_by_customer_past <- sales_by_customer_past %>% 
        dplyr::filter(time_condition == !!time_scale_past) %>%
        dplyr::select(customer_id, nesstatus)
      
      # Join current and past data to trace transitions
      tryCatch({
        transition_data <- dplyr::left_join(
          sales_by_customer_past, 
          sales_by_customer_now,
          by = "customer_id",
          suffix = c("_pre", "_now")
        ) %>% 
          dplyr::collect()
        
        # If we have data, create the contingency table
        if (nrow(transition_data) > 0) {
          # Create contingency table
          transition_table <- table(transition_data$nesstatus_pre, transition_data$nesstatus_now, 
                                  useNA = "ifany")
          
          # Convert to data frame
          df_long <- as.data.frame(transition_table)
          colnames(df_long) <- c("nesstatus_pre", "nesstatus_now", "count")
          
          # Calculate transition rates
          df_long <- df_long %>%
            dplyr::group_by(nesstatus_pre) %>%
            dplyr::mutate(activation_rate = count / sum(count))
          
          # Update component status
          component_status("ready")
          return(df_long)
        } else {
          # No data available
          component_status("ready")
          return(NULL)
        }
      }, error = function(e) {
        message("Error processing transition data: ", e$message)
        component_status("error")
        return(NULL)
      })
    })
    
    # Update data when refresh button is clicked
    observeEvent(input$refresh_data, {
      # Trigger recalculation of transition data
      nes_transition_data()
    })
    
    # ------------ 視覺化 ----------------------------------------  
    #' Render the NES transition plot
    output$nes_transition_plot <- renderPlotly({
      # Get transition data
      transition_data <- req(nes_transition_data())
      
      # Check if data exists
      if (is.null(transition_data) || nrow(transition_data) == 0) {
        return(plotly::plotly_empty(type = "bar") %>%
                 plotly::add_annotations(text = "No data available", showarrow = FALSE))
      }
      
      # Create the stacked bar chart
      tryCatch({
        # Color palette for NES status
        status_colors <- RColorBrewer::brewer.pal(5, "OrRd")
        
        # Create plotly object
        p <- plotly::plot_ly(
          transition_data, 
          x = ~nesstatus_pre, 
          y = ~activation_rate, 
          type = "bar", 
          color = ~nesstatus_now, 
          colors = rev(status_colors),
          text = ~paste("Count:", count, 
                      "<br>Percentage:", round(activation_rate * 100, 1), "%"),
          hoverinfo = "text",
          marker = list(line = list(width = 1))
        ) %>%
          plotly::layout(
            barmode = "stack", 
            title = list(text = "NES Status Transitions", font = list(size = 16)),
            yaxis = list(title = "Transition Rate", tickformat = ".0%", fixedrange = TRUE),
            xaxis = list(title = "Previous NES Status", fixedrange = TRUE),
            legend = list(title = list(text = "Current NES Status"))
          )
        
        return(p)
      }, error = function(e) {
        message("Error creating transition plot: ", e$message)
        return(plotly::plotly_empty(type = "bar") %>%
                 plotly::add_annotations(text = "Error creating visualization", showarrow = FALSE))
      })
    })
    
    # Display transition statistics
    output$transition_stats <- renderUI({
      transition_data <- nes_transition_data()
      
      if (is.null(transition_data) || nrow(transition_data) == 0) {
        return(p("No transition data available"))
      }
      
      # Calculate summary statistics
      total_customers <- sum(transition_data$count)
      activation_count <- transition_data %>%
        dplyr::filter(nesstatus_pre %in% c("S1", "S2", "S3") & nesstatus_now %in% c("N", "E0")) %>%
        dplyr::summarise(sum(count)) %>%
        dplyr::pull()
      
      sleeping_count <- transition_data %>%
        dplyr::filter(nesstatus_pre %in% c("N", "E0") & nesstatus_now %in% c("S1", "S2", "S3")) %>%
        dplyr::summarise(sum(count)) %>%
        dplyr::pull()
      
      # Create stats boxes for counts
      div(
        class = "stats-container",
        style = "display:flex; justify-content:space-around; flex-wrap:wrap; margin-top:20px;",
        div(class = "stat-box", style = "padding:10px; text-align:center; min-width:150px;",
            h5("Total Customers"),
            p(format(total_customers, big.mark = ","))),
        div(class = "stat-box", style = "padding:10px; text-align:center; min-width:150px;",
            h5("Reactivated Customers"),
            p(format(activation_count, big.mark = ","))),
        div(class = "stat-box", style = "padding:10px; text-align:center; min-width:150px;",
            h5("Newly Sleeping Customers"),
            p(format(sleeping_count, big.mark = ",")))
      )
    })
    
    # Display component status
    output$component_status <- renderText({
      switch(component_status(),
             idle = "Ready to load data", 
             loading = "Loading data...",
             computing = "Computing transitions...",
             ready = "Data ready",
             error = "Error loading data",
             component_status())
    })
    
    # Return reactive values for external access
    list(
      component_status = component_status,
      nes_transition_data = nes_transition_data
    )
  })
}

# Component wrapper -----------------------------------------------------------
#' macroNESTransitionComponent
#' 
#' Implements a visualization component for customer NES status transitions
#' following the Connected Component principle and using plotly for visualizations.
#' 
#' @param id Character string. The module ID used for namespacing inputs and outputs.
#' @param app_data_connection Database connection object or list. The data connection supporting Enhanced Data Access pattern (R116).
#'        Can be a DBI connection, a list with getter functions, a file path, or NULL if no database access is needed.
#' @param config List or reactive expression. Configuration parameters for customizing component behavior (optional).
#'        If reactive, will be re-evaluated when dependencies change.
#' @param translate Function. Translation function for UI text elements (defaults to identity function).
#'        Should accept a string and return a translated string.
#' @return A list containing UI and server functions structured according to the Connected Component Principle (MP56).
#'         The UI element contains 'filter' and 'display' components, and the server function initializes component functionality.
#' @examples
#' # Basic usage with default settings
#' nesComponent <- macroNESTransitionComponent("nes_transitions")
#' 
#' # Usage with database connection
#' nesComponent <- macroNESTransitionComponent(
#'   id = "nes_transitions",
#'   app_data_connection = app_conn, 
#'   config = list(platform_id = "amz")
#' )
#' @export
macroNESTransitionComponent <- function(id, app_data_connection = NULL, config = NULL, translate = identity) {
  list(
    ui = list(filter = macroNESTransitionFilterUI(id, translate),
              display = macroNESTransitionDisplayUI(id)),
    server = function(input, output, session) {
      macroNESTransitionServer(id, app_data_connection, config, session)
    }
  )
}

# For backwards compatibility - assigns the component function to the old name
#' @rdname macroNESTransitionComponent
#' @usage macroNESTransition(id, app_data_connection = NULL, config = NULL, translate = identity)
#' @description Alias for macroNESTransitionComponent. Provided for backward compatibility.
macroNESTransition <- macroNESTransitionComponent
