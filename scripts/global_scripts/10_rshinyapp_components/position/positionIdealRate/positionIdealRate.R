#LOCK FILE
#
# positionIdealRate.R
#
# Following principles:
# - MP56: Connected Component Principle (component structure)
# - MP81: Explicit Parameter Specification (function arguments)
# - R116: Enhanced Data Access with tbl2 (data access)
# - R09: UI-Server-Defaults Triple (component organization)
# - MP88: Immediate Feedback (real-time filtering without Apply button)
# - MP47: Functional Programming (data transformation functions)
#

#
# Features:
#   • Ideal Rate Analysis based on key factors
#   • Interactive data table display with scoring
#   • Brand and Item ID ranking by ideal distance
#   • Automatic key factor identification
#   • Real-time filtering and sorting capabilities
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
#' Perform ideal rate analysis following MK03 principle
#' @param data data.frame. Position data with numerical attributes
#' @param exclude_vars character vector. Variables to exclude from analysis
#' @param threshold_multiplier numeric. DEPRECATED - kept for backward compatibility
#' @param n_key_factors numeric. Number of key factors to select when using "top_n" method (default: 8)
#' @param selection_method character. Method for selecting key factors: "cross_average" (MK03 principle) or "top_n" (default: "cross_average")
#' @return list. Contains ideal analysis results with scores and rankings
perform_ideal_rate_analysis <- function(data, exclude_vars = NULL,
                                       threshold_multiplier = 0.3,  # DEPRECATED
                                       n_key_factors = 8,
                                       selection_method = "cross_average") {
  # Check if product_id column exists, if not look for platform-specific columns
  if (!"product_id" %in% names(data)) {
    # Try common platform-specific columns
    if ("asin" %in% names(data)) {
      data <- data %>% dplyr::rename(product_id = asin)
    } else if ("ebay_item_number" %in% names(data)) {
      data <- data %>% dplyr::rename(product_id = ebay_item_number)
    } else {
      warning("No product identifier column found in ideal rate analysis data")
      return(list(
        ideal_analysis = data.frame(),
        key_factors = character(0),
        indicators = data.frame()
      ))
    }
  }
  
  # Remove excluded variables and special rows
  df_analysis <- data %>%
    dplyr::filter(!product_id %in% c("Rating", "Revenue")) %>%
    dplyr::select(-dplyr::any_of(exclude_vars))
  
  # Extract Ideal row if it exists
  ideal_row <- df_analysis %>% dplyr::filter(product_id == "Ideal")
  
  if (nrow(ideal_row) == 0) {
    warning("No Ideal row found for ideal rate analysis")
    return(list(
      ideal_analysis = data.frame(),
      key_factors = character(0),
      indicators = data.frame()
    ))
  }
  
  # Get numeric columns for analysis
  key_cols <- c("product_id", "brand", "product_line_id", "platform_id")
  numeric_cols <- df_analysis %>% 
    dplyr::select(-dplyr::any_of(key_cols)) %>%
    dplyr::select_if(is.numeric) %>%
    names()
  
  if (length(numeric_cols) == 0) {
    warning("No numeric columns found for ideal rate analysis")
    return(list(
      ideal_analysis = data.frame(),
      key_factors = character(0),
      indicators = data.frame()
    ))
  }
  
  # Create indicators matrix
  df_no_ideal <- df_analysis %>% dplyr::filter(product_id != "Ideal")
  
  # CORRECTED ALGORITHM following MK03 principle
  # Step 1: Extract the ideal point vector (single m-dimensional vector)
  ideal_point_vector <- as.numeric(ideal_row[numeric_cols])
  names(ideal_point_vector) <- numeric_cols

  # Remove any NA values from ideal point vector
  valid_ideal <- ideal_point_vector[!is.na(ideal_point_vector)]

  if (length(valid_ideal) == 0) {
    warning("No valid ideal values found")
    return(list(
      ideal_analysis = data.frame(),
      key_factors = character(0),
      indicators = data.frame()
    ))
  }

  # Step 2: Identify key factors using MK03 principle
  # Key factors are attributes where ideal score > cross-attribute average
  if (selection_method == "cross_average") {
    # Method 1: Cross-attribute average threshold (MK03 original method)
    cross_attr_avg <- mean(valid_ideal, na.rm = TRUE)
    key_factors <- names(valid_ideal[valid_ideal > cross_attr_avg])
  } else {
    # Method 2: Select top N factors by score (ensures exactly N factors)
    # Sort ideal point values in descending order and take top N
    sorted_factors <- names(sort(valid_ideal, decreasing = TRUE))
    n_to_select <- min(n_key_factors, length(sorted_factors))
    key_factors <- sorted_factors[1:n_to_select]
  }

  # Step 3: Create indicators matrix for scoring products
  # This compares each product to ideal values ONLY for key factors
  indicators <- data.frame(matrix(0, nrow = nrow(df_no_ideal), ncol = length(numeric_cols)))
  colnames(indicators) <- numeric_cols

  for (col in numeric_cols) {
    ideal_val <- ideal_row[[col]][1]
    if (!is.na(ideal_val) && is.numeric(ideal_val) && is.finite(ideal_val)) {
      col_values <- df_no_ideal[[col]]
      # Products achieve ideal if they meet or exceed the ideal value
      comparison_result <- ifelse(is.na(col_values), 0, ifelse(col_values >= ideal_val, 1, 0))
      indicators[[col]] <- comparison_result
    } else {
      indicators[[col]] <- rep(0, nrow(df_no_ideal))
    }
  }
  
  # Create ideal analysis (IA) - following KitchenMAMA methodology
  if (length(key_factors) > 0) {
    # Select only key factors from indicators
    IA <- indicators %>% dplyr::select(dplyr::any_of(key_factors))
    
    # Calculate score as row sum
    IA <- IA %>% dplyr::mutate(Score = rowSums(IA, na.rm = TRUE))
    
    # Add brand and item_id information
    IA <- IA %>%
      dplyr::bind_cols(
        brand = df_no_ideal$brand,
        product_id = df_no_ideal$product_id
      )
    
    # Arrange by score descending (IA_lis equivalent)
    ideal_analysis <- IA %>% 
      dplyr::arrange(dplyr::desc(Score)) %>%
      dplyr::filter(product_id != "Ideal") %>%
      dplyr::select(Score, brand, product_id, dplyr::everything())
    
  } else {
    ideal_analysis <- data.frame()
  }
  
  return(list(
    ideal_analysis = ideal_analysis,
    key_factors = key_factors,
    indicators = indicators,
    ideal_point_vector = valid_ideal,  # Include the actual ideal point vector
    cross_attr_avg = if (exists("cross_attr_avg")) cross_attr_avg else mean(valid_ideal),
    n_key_factors = length(key_factors),
    selection_method = selection_method
  ))
}

# Filter UI -------------------------------------------------------------------
#' positionIdealRateFilterUI
#' @param id Character string. The module ID used for namespacing inputs and outputs.
#' @param translate Function. Translation function for UI text elements (defaults to identity function).
#'        Should accept a string and return a translated string.
#' @return shiny.tag. A Shiny UI component containing the filter controls for the IdealRate component.
positionIdealRateFilterUI <- function(id, translate = identity) {
  ns <- NS(id)
  
  wellPanel(
    style = "padding:15px;",
    h4(translate("Ideal Rate Analysis Settings")),
    
    # Display options
    hr(),
    h4(translate("Display Options")),
    
    # Show all columns
    checkboxInput(
      inputId = ns("show_all_columns"),
      label = translate("Show all factor columns"),
      value = FALSE
    ),
    
    # Reset button
    actionButton(
      inputId = ns("reset_filters"),
      label = translate("Reset Settings"),
      class = "btn-outline-secondary btn-block mt-3"
    ),
    
    hr(),
    textOutput(ns("component_status"))
  )
}

# Display UI ------------------------------------------------------------------
#' positionIdealRateDisplayUI
#' @param id Character string. The module ID used for namespacing inputs and outputs.
#' @param translate Function. Translation function for UI text elements (defaults to identity function).
#' @return shiny.tag. A Shiny UI component containing the display elements for the IdealRate component.
positionIdealRateDisplayUI <- function(id, translate = identity) {
  ns <- NS(id)
  
  tagList(
    div(class = "component-header mb-3 text-center",
        h3(translate("Ideal Rate Analysis")),
        p(translate("Product ranking based on ideal point distance and key factor performance"))),
    div(class = "component-output p-3",
        div(class = "ideal-rate-table",
            DT::DTOutput(ns("ideal_rate_table")))
    )
  )
}

# Server ----------------------------------------------------------------------
#' positionIdealRateServer
#' @param id Character string. The module ID used for namespacing inputs and outputs.
#' @param app_data_connection Database connection object or list. Any connection type supported by tbl2.
#'        Can be a DBI connection, a list with getter functions, a file path, or NULL if no database access is needed.
#' @param config List or reactive expression. Optional configuration settings that can customize behavior.
#'        If reactive, will be re-evaluated when dependencies change.
#' @param session Shiny session object. The current Shiny session (defaults to getDefaultReactiveDomain()).
#' @return list. A list of reactive values providing access to component state and data.
positionIdealRateServer <- function(id, app_data_connection = NULL, config = NULL,
                             session = getDefaultReactiveDomain()) {
  moduleServer(id, function(input, output, session) {
    
    # ------------ Status tracking ----------------------------------
    component_status <- reactiveVal("idle")
    
    # ------------ Extract configuration parameters -----------------
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
        
        # Use complete case function to get position data with type filtering (IdealRate needs Ideal row)
        filtered_data <- fn_get_position_complete_case(
          app_data_connection = app_data_connection,
          product_line_id = prod_line,
          include_special_rows = TRUE,  # IdealRate analysis needs Ideal row
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
            message("DEBUG: Renaming '", item_col, "' to 'product_id' in positionIdealRate")
            filtered_data <- filtered_data %>% dplyr::rename(product_id = !!sym(item_col))
          } else {
            warning("No product identifier column found in IdealRate position data. Available columns: ", paste(names(filtered_data), collapse = ", "))
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
    
    # ------------ Ideal Rate Analysis -----------------------------------
    ideal_rate_result <- reactive({
      data <- position_data()
      if (is.null(data) || nrow(data) == 0) return(NULL)
      
      component_status("computing")
      
      # Define variables to exclude from analysis
      exclude_vars <- c("product_line_id", "platform_id", "rating", "sales", "revenue")
      
      # Following MK03 principle - use cross-attribute average method
      result <- perform_ideal_rate_analysis(
        data = data,
        exclude_vars = exclude_vars,
        n_key_factors = 8,  # Only used if selection_method is "top_n"
        selection_method = "cross_average"  # Use MK03 principle: I_j > mean(I)
      )
      
      if (nrow(result$ideal_analysis) > 0) {
        component_status("ready")
      } else {
        component_status("idle")
      }
      
      return(result)
    })
    
    # ------------ Reset filters ------------------------------------
    observeEvent(input$reset_filters, {
      updateCheckboxInput(session, "show_all_columns", value = FALSE)
      
      message("Ideal Rate settings reset")
    })
    
    # ------------ Output Rendering ------------------------------------
    # Render the main data table
    output$ideal_rate_table <- DT::renderDT({
      result <- ideal_rate_result()
      
      if (is.null(result) || nrow(result$ideal_analysis) == 0) {
        return(data.frame(Message = "No data available for analysis"))
      }
      
      # Prepare data for display
      display_data <- result$ideal_analysis
      
      # Select columns based on display options
      if (input$show_all_columns %||% FALSE) {
        # Show all columns
        final_data <- display_data
      } else {
        # Show only essential columns (Score, brand, product_id)
        final_data <- display_data %>%
          dplyr::select(Score, brand, product_id)
      }
      
      return(final_data)
    }, options = list(
      pageLength = -1,  # Show all rows
      searching = FALSE,  # Disable search box
      lengthChange = FALSE,  # Don't show "Show [number] entries" option
      info = FALSE,  # Don't show "Showing 1 to X of X entries" info
      paging = FALSE,  # Disable pagination
      scrollX = TRUE,  # Enable horizontal scrolling
      columnDefs = list(
        list(visible = FALSE, targets = 0)  # Hide row numbers
      )
    ))
    
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
      ideal_rate_result = ideal_rate_result,
      component_status = component_status
    ))
  })
}

# Component wrapper -----------------------------------------------------------
#' positionIdealRateComponent
#' 
#' Implements an ideal rate analysis component for position analysis
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
#' idealRateComp <- positionIdealRateComponent("ideal_rate_analysis")
#' 
#' # Usage with database connection
#' idealRateComp <- positionIdealRateComponent(
#'   id = "ideal_rate_analysis",
#'   app_data_connection = app_conn, 
#'   config = list(platform_id = "amz")
#' )
#'
#' # Usage with reactive configuration
#' idealRateComp <- positionIdealRateComponent(
#'   id = "ideal_rate_analysis",
#'   app_data_connection = app_conn,
#'   config = reactive({ list(filters = list(platform_id = input$platform)) })
#' )
#' @export
positionIdealRateComponent <- function(id, app_data_connection = NULL, config = NULL, translate = identity) {
  list(
    ui = list(filter = positionIdealRateFilterUI(id, translate),
              display = positionIdealRateDisplayUI(id, translate)),
    server = function(input, output, session) {
      positionIdealRateServer(id, app_data_connection, config, session)
    }
  )
}
