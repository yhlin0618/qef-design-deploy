#LOCK FILE
#
# positionDNAPlotly.R
#
# Following principles:
# - MP56: Connected Component Principle (component structure)
# - MP73: Interactive Visualization Preference (Plotly for interactive visualizations)
# - MP81: Explicit Parameter Specification (function arguments)
# - R116: Enhanced Data Access with tbl2 (data access)
# - R09: UI-Server-Defaults Triple (component organization)
# - MP88: Immediate Feedback (real-time filtering without Apply button)
# - MP47: Functional Programming (data transformation functions)
#

#
# Features:
#   • Interactive DNA visualization with Plotly
#   • Multi-brand comparison with line charts
#   • Attribute-based positioning analysis
#   • Dynamic filtering and legend controls
#   • Brand-specific hover information
# -----------------------------------------------------------------------------

# helper ----------------------------------------------------------------------
#' Paste operator for string concatenation
#' @param x Character string. First string to concatenate.
#' @param y Character string. Second string to concatenate.
#' @return Character string. The concatenated result of x and y.
`%+%` <- function(x, y) paste0(x, y)

#' NULL coalescing operator
#' @param x Any value. The value to use if not NULL.
#' @param y Any value. The fallback value to use if x is NULL.
#' @return Either x or y. Returns x if it's not NULL, otherwise returns y.
`%||%` <- function(x, y) if (is.null(x)) y else x

# Data transformation functions (MP47) ----------------------------------------
#' Transform position data to DNA plot format
#' @param data data.frame. Position data with brand, product_id, and attribute columns
#' @param exclude_vars character vector. Variables to exclude from the plot
#' @return data.frame. Transformed data in long format suitable for plotting
transform_position_to_dna_format <- function(data, exclude_vars = NULL) {
  # Remove excluded variables if specified
  if (!is.null(exclude_vars)) {
    data <- data %>% dplyr::select(-dplyr::any_of(exclude_vars))
  }
  
  # Transform to long format for plotting
  dna_data <- data %>%
    tidyr::pivot_longer(
      cols = -c("product_id", "brand"),
      names_to = "attribute",
      values_to = "score"
    ) %>%
    dplyr::arrange(product_id, attribute) %>%
    dplyr::mutate(attribute = as.factor(attribute))
  
  return(dna_data)
}

#' Create brand groups for plotly visualization
#' @param dna_data data.frame. DNA data in long format
#' @return list. Named list of data frames, one for each brand
create_brand_groups <- function(dna_data) {
  product_id_groups <- split(dna_data, dna_data$product_id)
  return(product_id_groups)
}

# Filter UI -------------------------------------------------------------------
#' positionDNAPlotlyFilterUI
#' @param id Character string. The module ID used for namespacing inputs and outputs.
#' @param translate Function. Translation function for UI text elements (defaults to identity function).
#'        Should accept a string and return a translated string.
#' @return shiny.tag. A Shiny UI component containing the filter controls for the position DNA plotly component.
positionDNAPlotlyFilterUI <- function(id, translate = identity) {
  ns <- NS(id)
  
  wellPanel(
    style = "padding:15px;",
    h4(translate("DNA Visualization Filters")),
    
    # Brand selection
    selectizeInput(
      inputId = ns("selected_brands"),
      label = translate("Select Brands to Show"),
      choices = NULL,
      multiple = TRUE,
      options = list(plugins = list('remove_button', 'drag_drop'))
    ),
    
    # Attribute selection
    selectizeInput(
      inputId = ns("selected_attributes"),
      label = translate("Select Attributes"),
      choices = NULL,
      multiple = TRUE,
      options = list(plugins = list('remove_button', 'drag_drop'))
    ),
    
    # Display options
    hr(),
    h4(translate("Display Options")),
    
    # Show all brands initially
    checkboxInput(
      inputId = ns("show_all_initially"),
      label = translate("Show all brands initially"),
      value = FALSE
    ),
    
    # Line style options
    sliderInput(
      inputId = ns("line_width"),
      label = translate("Line Width"),
      min = 1,
      max = 5,
      value = 2,
      step = 1
    ),
    
    sliderInput(
      inputId = ns("marker_size"),
      label = translate("Marker Size"),
      min = 5,
      max = 20,
      value = 10,
      step = 1
    ),
    
    # Reset button
    actionButton(
      inputId = ns("reset_filters"),
      label = translate("Reset Filters"),
      class = "btn-outline-secondary btn-block mt-3"
    ),
    
    hr(),
    textOutput(ns("component_status"))
  )
}

# Display UI ------------------------------------------------------------------
#' positionDNAPlotlyDisplayUI
#' @param id Character string. The module ID used for namespacing inputs and outputs.
#' @param translate Function. Translation function for UI text elements (defaults to identity function).
#' @return shiny.tag. A Shiny UI component containing the display elements for the position DNA plotly.
positionDNAPlotlyDisplayUI <- function(id, translate = identity) {
  ns <- NS(id)
  
  tagList(
    div(class = "component-header mb-3 text-center",
        h3(translate("Brand DNA Visualization")),
        p(translate("Interactive multi-dimensional brand positioning analysis"))),
    div(class = "component-output p-3",
        plotlyOutput(ns("dna_plot"), width = "100%", height = "600px"))
  )
}

# Server ----------------------------------------------------------------------
#' positionDNAPlotlyServer
#' @param id Character string. The module ID used for namespacing inputs and outputs.
#' @param app_data_connection Database connection object or list. Any connection type supported by tbl2.
#'        Can be a DBI connection, a list with getter functions, a file path, or NULL if no database access is needed.
#' @param config List or reactive expression. Optional configuration settings that can customize behavior.
#'        If reactive, will be re-evaluated when dependencies change.
#' @param session Shiny session object. The current Shiny session (defaults to getDefaultReactiveDomain()).
#' @return list. A list of reactive values providing access to component state and data.
positionDNAPlotlyServer <- function(id, app_data_connection = NULL, config = NULL,
                                    session = getDefaultReactiveDomain()) {
  moduleServer(id, function(input, output, session) {
    
    # ------------ Status tracking ----------------------------------
    #' Reactive value for tracking component status
    #' @description Tracks the current status of the component (loading, ready, etc.)
    #' @return Character string. One of "idle", "loading", "ready", "computing", or "error"
    component_status <- reactiveVal("idle")
    
    # ------------ Extract configuration parameters -----------------
    #' Extract platform_id from configuration
    platform_id <- reactive({
      tryCatch({
        if (is.null(config)) return(NULL)
        
        cfg <- if (is.function(config)) config() else config
        
        if (!is.null(cfg[["platform_id"]])) {
          return(as.character(cfg[["platform_id"]]))
        }
        if (!is.null(cfg[["filters"]]) && !is.null(cfg[["filters"]][["platform_id"]])) {
          return(as.character(cfg[["filters"]][["platform_id"]]))
        }
        
        NULL
      }, error = function(e) {
        warning("Error extracting platform_id from config: ", e$message)
        NULL
      })
    })
    
    #' Extract product_line_id from configuration
    product_line_id <- reactive({
      tryCatch({
        if (is.null(config)) return("all")
        
        cfg <- if (is.function(config)) config() else config
        
        if (!is.null(cfg[["filters"]]) && !is.null(cfg[["filters"]][["product_line_id_chosen"]])) {
          return(as.character(cfg[["filters"]][["product_line_id_chosen"]]))
        }
        
        "all"
      }, error = function(e) {
        warning("Error extracting product_line_id: ", e$message)
        "all"
      })
    })
    
    # ------------ Data access (R116) -----------------------------------
    #' Reactive data accessor for position data
    position_data <- reactive({
      if (product_line_id() == "all") {
        component_status("idle")
        return(data.frame())
      }
      
      component_status("loading")
      
      prod_line <- product_line_id()
      
      result <- tryCatch({
        if (is.null(app_data_connection)) {
          warning("No valid database connection available")
          return(data.frame())
        }
        
        # Use demonstrate case function to get position data with type filtering
        filtered_data <- fn_get_position_demonstrate_case(
          app_data_connection = app_data_connection,
          product_line_id = prod_line,
          apply_iterative_filter = FALSE,  # No iterative filtering for DNA visualization
          apply_type_filter = TRUE
        )
        
        # Check if product_id column exists, if not try to find platform-specific column
        if (!"product_id" %in% names(filtered_data) && nrow(filtered_data) > 0) {
          platform <- platform_id()

          # Ensure platform is a scalar value for switch statement
          if (is.null(platform) || length(platform) == 0) {
            platform <- "default"
          } else if (length(platform) > 1) {
            warning("platform_id() returned multiple values, using first: ", paste(platform, collapse=", "))
            platform <- as.character(platform[1])
          } else {
            platform <- as.character(platform)
          }

          item_col <- switch(platform,
            "amz" = "asin",  # Amazon
            "eby" = "ebay_item_number",  # eBay
            "product_id"  # Default fallback
          )
          
          if (item_col %in% names(filtered_data)) {
            message("DEBUG: Renaming '", item_col, "' to 'product_id' in positionDNAPlotly")
            filtered_data <- filtered_data %>% dplyr::rename(product_id = !!sym(item_col))
          } else {
            warning("No product identifier column found in DNA position data. Available columns: ", paste(names(filtered_data), collapse = ", "))
          }
        }
        
        component_status("ready")
        return(filtered_data)
      }, error = function(e) {
        warning("Error fetching position data: ", e$message)
        component_status("error")
        data.frame()
      })
      
      return(result)
    })
    
    # ------------ Data transformation -----------------------------------
    #' Transform position data to DNA format
    dna_data <- reactive({
      data <- position_data()
      if (is.null(data) || nrow(data) == 0) return(data.frame())
      
      # Define variables to exclude (similar to original Exclude_Variable$Not_GPTRating)
      exclude_vars <- c("product_line_id", "platform_id", "rating", "sales", "revenue")
      
      # Transform data using the functional approach
      transform_position_to_dna_format(data, exclude_vars)
    })
    
    # ------------ Filter Options -----------------------------------------
    # Update brand filter choices when DNA data changes
    observe({
      data <- dna_data()
      
      if (is.null(data) || nrow(data) == 0) {
        updateSelectizeInput(session, "selected_brands",
                          choices = c("No brands available" = ""),
                          selected = character(0))
        return()
      }
      
      # Get unique brands
      brands <- data %>%
        dplyr::pull(brand) %>%
        unique() %>%
        sort()
      
      # Maintain current selections if possible
      selected <- intersect(input$selected_brands, brands)
      
      updateSelectizeInput(session, "selected_brands",
                        choices = brands,
                        selected = selected)
    })
    
    # Update attribute filter choices when DNA data changes
    observe({
      data <- dna_data()
      
      if (is.null(data) || nrow(data) == 0) {
        updateSelectizeInput(session, "selected_attributes",
                          choices = c("No attributes available" = ""),
                          selected = character(0))
        return()
      }
      
      # Get unique attributes
      attributes <- data %>%
        dplyr::pull(attribute) %>%
        levels() %>%
        sort()
      
      # Maintain current selections if possible
      selected <- intersect(input$selected_attributes, attributes)
      
      updateSelectizeInput(session, "selected_attributes",
                        choices = attributes,
                        selected = selected)
    })
    
    # ------------ Filtered Data ----------------------------------------
    filtered_dna_data <- reactive({
      data <- dna_data()
      
      # Apply brand filter if selected
      if (length(input$selected_brands) > 0) {
        data <- data %>% dplyr::filter(brand %in% input$selected_brands)
      }
      
      # Apply attribute filter if selected
      if (length(input$selected_attributes) > 0) {
        data <- data %>% dplyr::filter(attribute %in% input$selected_attributes)
      }
      
      return(data)
    })
    
    # ------------ Reset filters ------------------------------------
    observeEvent(input$reset_filters, {
      updateSelectizeInput(session, "selected_brands", selected = character(0))
      updateSelectizeInput(session, "selected_attributes", selected = character(0))
      updateCheckboxInput(session, "show_all_initially", value = FALSE)
      updateSliderInput(session, "line_width", value = 2)
      updateSliderInput(session, "marker_size", value = 10)
      
      message("DNA plot filters reset")
    })
    
    # ------------ Plot Rendering ------------------------------------
    output$dna_plot <- renderPlotly({
      data <- filtered_dna_data()
      
      if (is.null(data) || nrow(data) == 0) {
        return(plotly_empty() %>% 
               layout(title = "No data available for visualization"))
      }
      
      # Create brand groups
      product_id_groups <- create_brand_groups(data)
      
      # Get factor levels for x-axis
      fac_levels <- levels(data$attribute)
      
      # Initialize plotly object
      p <- plot_ly()
      
      # Add traces for each brand/product_id group
      for (i in seq_along(product_id_groups)) {
        product_id_data <- product_id_groups[[i]]
        brand <- unique(product_id_data$brand)
        
        # Determine visibility based on settings
        visibility <- if (input$show_all_initially) TRUE else "legendonly"
        
        p <- add_trace(
          p,
          data = product_id_data,
          x = ~levels(data$attribute),
          y = ~score,
          color = ~brand,
          type = 'scatter',
          mode = 'lines+markers',
          name = brand,
          text = ~product_id,
          hoverinfo = 'text',
          marker = list(size = input$marker_size),
          line = list(width = input$line_width),
          visible = visibility
        )
      }
      
      # Configure layout
      p <- layout(
        p,
        xaxis = list(
          title = list(
            text = "Attribute",
            font = list(size = 20)
          ),
          tickangle = -45
        ),
        yaxis = list(
          title = "Score",
          font = list(size = 20)
        ),
        hovermode = 'closest',
        showlegend = TRUE
      )
      
      return(p)
    })
    
    # Display component status
    output$component_status <- renderText({
      # MP031: Defensive programming - check for NULL/empty values before switch
      # R113: Error handling for reactive expressions
      status_val <- tryCatch({
        component_status()
      }, error = function(e) {
        warning("Error getting component status: ", e$message)
        "idle"
      })

      # MP099: Defensive check for NULL or empty status
      if (is.null(status_val) || length(status_val) == 0 || status_val == "") {
        return("Ready for position analysis")
      }

      # Ensure status_val is character and length 1 for switch
      status_val <- as.character(status_val)[1]

      switch(status_val,
             idle = "Ready for position analysis",
             loading = "Loading position data...",
             ready = paste0("Position data loaded: ", nrow(position_data()), " records"),
             computing = "Computing position metrics...",
             error = "Error loading position data",
             status_val)  # Default: return the status value itself
    })
    
    # Return reactive values for external use
    return(list(
      position_data = position_data,
      dna_data = dna_data,
      filtered_dna_data = filtered_dna_data,
      component_status = component_status
    ))
  })
}

# Component wrapper -----------------------------------------------------------
#' positionDNAPlotlyComponent
#' 
#' Implements an interactive DNA visualization component for position analysis
#' following the Connected Component principle.
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
#' dnaComp <- positionDNAPlotlyComponent("dna_plot")
#' 
#' # Usage with database connection
#' dnaComp <- positionDNAPlotlyComponent(
#'   id = "dna_plot",
#'   app_data_connection = app_conn, 
#'   config = list(platform_id = "amz")
#' )
#'
#' # Usage with reactive configuration
#' dnaComp <- positionDNAPlotlyComponent(
#'   id = "dna_plot",
#'   app_data_connection = app_conn,
#'   config = reactive({ list(filters = list(platform_id = input$platform)) })
#' )
#' @export
positionDNAPlotlyComponent <- function(id, app_data_connection = NULL, config = NULL, translate = identity) {
  list(
    ui = list(filter = positionDNAPlotlyFilterUI(id, translate),
              display = positionDNAPlotlyDisplayUI(id, translate)),
    server = function(input, output, session) {
      positionDNAPlotlyServer(id, app_data_connection, config, session)
    }
  )
}
