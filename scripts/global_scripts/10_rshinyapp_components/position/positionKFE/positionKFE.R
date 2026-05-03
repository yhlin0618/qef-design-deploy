#LOCK FILE
#
# positionKFE.R
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
#   • Key Factor Evaluation (KFE) analysis
#   • Automatic identification of critical success factors
#   • Benchmark analysis for each key factor
#   • Ideal point analysis and scoring
#   • Interactive display of key factors and recommendations
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
#' Perform key factor evaluation analysis
#' @param data data.frame. Position data with numerical attributes
#' @param exclude_vars character vector. Variables to exclude from analysis
#' @param threshold_multiplier numeric. Multiplier for the threshold (default: 1.0)
#' @return list. Contains key factors, indicators, and benchmarks
perform_kfe_analysis <- function(data, exclude_vars = NULL, threshold_multiplier = 1.0) {
  # Check if product_id column exists, if not look for platform-specific columns
  if (!"product_id" %in% names(data)) {
    # Try common platform-specific columns
    if ("asin" %in% names(data)) {
      data <- data %>% dplyr::rename(product_id = asin)
    } else if ("ebay_item_number" %in% names(data)) {
      data <- data %>% dplyr::rename(product_id = ebay_item_number)
    } else {
      warning("No product identifier column found in KFE analysis data")
      return(list(
        key_factors = character(0),
        indicators = data.frame(),
        benchmarks = list(),
        ideal_analysis = data.frame()
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
    warning("No Ideal row found for KFE analysis")
    return(list(
      key_factors = character(0),
      indicators = data.frame(),
      benchmarks = list(),
      ideal_analysis = data.frame()
    ))
  }
  
  # Get numeric columns for analysis
  key_cols <- c("product_id", "brand", "product_line_id", "platform_id")
  numeric_cols <- df_analysis %>% 
    dplyr::select(-dplyr::any_of(key_cols)) %>%
    dplyr::select_if(is.numeric) %>%
    names()
  
  if (length(numeric_cols) == 0) {
    warning("No numeric columns found for KFE analysis")
    return(list(
      key_factors = character(0),
      indicators = data.frame(),
      benchmarks = list(),
      ideal_analysis = data.frame()
    ))
  }
  
  # Create indicators matrix
  df_no_ideal <- df_analysis %>% dplyr::filter(product_id != "Ideal")
  
  # Calculate ideal comparison for each numeric column
  indicators <- data.frame(matrix(0, nrow = nrow(df_no_ideal), ncol = length(numeric_cols)))
  colnames(indicators) <- numeric_cols
  
  for (col in numeric_cols) {
    ideal_val <- ideal_row[[col]][1]
    if (!is.na(ideal_val) && is.numeric(ideal_val) && is.finite(ideal_val)) {
      # Compare each value to ideal, handling NA values explicitly
      col_values <- df_no_ideal[[col]]
      # Only include non-NA values in comparison, set NA values to 0
      comparison_result <- ifelse(is.na(col_values), 0, ifelse(col_values >= ideal_val, 1, 0))
      indicators[[col]] <- comparison_result
    } else {
      # If ideal value is NA or invalid, set all indicators to 0
      indicators[[col]] <- rep(0, nrow(df_no_ideal))
    }
  }
  
  # Calculate gate (threshold) for key factor identification
  gate <- rowSums(indicators, na.rm = TRUE) / ncol(indicators) * threshold_multiplier
  
  # Identify key factors (columns where ideal comparison > gate)
  col_sums <- colSums(indicators, na.rm = TRUE)
  key_factors <- names(col_sums[col_sums > mean(gate, na.rm = TRUE)])
  
  # Create benchmarks for each key factor
  benchmarks <- list()
  for (factor in key_factors) {
    if (factor %in% colnames(indicators)) {
      # Get indices where the factor equals 1
      factor_indices <- which(indicators[[factor]] == 1)
      
      if (length(factor_indices) > 0) {
        # Get Product IDs for those indices, filtering out NA values
        benchmark_item_ids <- df_no_ideal$product_id[factor_indices]
        benchmark_item_ids <- benchmark_item_ids[!is.na(benchmark_item_ids)]
        benchmark_item_ids <- unique(benchmark_item_ids)
        
        # Only include if we have valid Product IDs
        if (length(benchmark_item_ids) > 0) {
          benchmarks[[factor]] <- benchmark_item_ids
        }
      }
    }
  }
  
  # Create ideal analysis (scoring based on key factors)
  if (length(key_factors) > 0) {
    ideal_analysis <- df_no_ideal %>%
      dplyr::select(product_id, brand, dplyr::all_of(key_factors)) %>%
      dplyr::mutate(
        Score = rowSums(dplyr::select(., dplyr::all_of(key_factors)), na.rm = TRUE)
      ) %>%
      dplyr::arrange(dplyr::desc(Score))
  } else {
    ideal_analysis <- data.frame()
  }
  
  return(list(
    key_factors = key_factors,
    indicators = indicators,
    benchmarks = benchmarks,
    ideal_analysis = ideal_analysis,
    gate_threshold = mean(gate, na.rm = TRUE)
  ))
}

#' Format key factors for display
#' @param factors character vector. List of key factors
#' @return character. Formatted string of key factors
format_key_factors <- function(factors) {
  if (length(factors) == 0) {
    return("No key factors identified")
  }
  
  # Clean up factor names for display
  clean_factors <- factors %>%
    stringr::str_replace_all("_", " ") %>%
    stringr::str_to_title()
  
  # Use bullet points for better readability and wrapping
  paste(paste("•", clean_factors), collapse = "\n")
}

# Filter UI -------------------------------------------------------------------
#' positionKFEFilterUI
#' @param id Character string. The module ID used for namespacing inputs and outputs.
#' @param translate Function. Translation function for UI text elements (defaults to identity function).
#'        Should accept a string and return a translated string.
#' @return shiny.tag. A Shiny UI component containing the filter controls for the KFE component.
positionKFEFilterUI <- function(id, translate = identity) {
  ns <- NS(id)
  
  wellPanel(
    style = "padding:15px;",
    h4(translate("Key Factor Analysis Settings")),
    
    # Display options
    hr(),
    h4(translate("Display Options")),
    
    # Show benchmark products
    checkboxInput(
      inputId = ns("show_benchmarks"),
      label = translate("Show Benchmark Products by Factor"),
      value = TRUE
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
#' positionKFEDisplayUI
#' @param id Character string. The module ID used for namespacing inputs and outputs.
#' @param translate Function. Translation function for UI text elements (defaults to identity function).
#' @param mode Character string. Display mode - "compact" for summary view, "full" for detailed view (default: "full").
#' @return shiny.tag. A Shiny UI component containing the display elements for the KFE component.
positionKFEDisplayUI <- function(id, translate = identity, mode = "full") {
  ns <- NS(id)
  
  if (mode == "compact") {
    # Compact mode for use alongside other components
    tagList(
      div(class = "component-output p-2",
          div(class = "kfe-main-output",
              h5(translate("Key Factors:")),
              div(class = "kfe-factors-display",
                  style = "white-space: pre-wrap; word-wrap: break-word; overflow-wrap: break-word;",
                  verbatimTextOutput(ns("key_factors_text")))
          )
      )
    )
  } else {
    # Full mode for dedicated view
    tagList(
      div(class = "component-header mb-3 text-center",
          h3(translate("Key Factor Evaluation")),
          p(translate("Automatic identification and analysis of critical success factors"))),
      div(class = "component-output p-3",
          div(class = "kfe-main-output",
              h4(translate("Key Factors")),
              div(class = "kfe-factors-display",
                  style = "white-space: pre-wrap; word-wrap: break-word; overflow-wrap: break-word;",
                  verbatimTextOutput(ns("key_factors_text")))
          ),
          conditionalPanel(
            condition = paste0("input['", ns("show_benchmarks"), "']"),
            div(class = "kfe-benchmarks mt-4",
                h4(translate("Benchmark Products by Factor")),
                div(style = "white-space: pre-wrap; word-wrap: break-word; overflow-wrap: break-word; max-width: 100%;",
                    verbatimTextOutput(ns("benchmark_details")))
            )
          )
      )
    )
  }
}

# Server ----------------------------------------------------------------------
#' positionKFEServer
#' @param id Character string. The module ID used for namespacing inputs and outputs.
#' @param app_data_connection Database connection object or list. Any connection type supported by tbl2.
#'        Can be a DBI connection, a list with getter functions, a file path, or NULL if no database access is needed.
#' @param config List or reactive expression. Optional configuration settings that can customize behavior.
#'        If reactive, will be re-evaluated when dependencies change.
#' @param session Shiny session object. The current Shiny session (defaults to getDefaultReactiveDomain()).
#' @param display_mode Character string. Display mode - "compact" or "full" (default: "full").
#' @return list. A list of reactive values providing access to component state and data.
positionKFEServer <- function(id, app_data_connection = NULL, config = NULL,
                              session = getDefaultReactiveDomain(), display_mode = "full") {
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
        
        # Use complete case function to get position data with type filtering (KFE needs Ideal row)
        filtered_data <- fn_get_position_complete_case(
          app_data_connection = app_data_connection,
          product_line_id = prod_line,
          include_special_rows = TRUE,  # KFE analysis needs Ideal row
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
            message("DEBUG: Renaming '", item_col, "' to 'product_id' in positionKFE")
            filtered_data <- filtered_data %>% dplyr::rename(product_id = !!sym(item_col))
          } else {
            warning("No product identifier column found in KFE position data. Available columns: ", paste(names(filtered_data), collapse = ", "))
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
    
    # ------------ KFE Analysis -----------------------------------
    kfe_result <- reactive({
      data <- position_data()
      if (is.null(data) || nrow(data) == 0) return(NULL)
      
      component_status("computing")
      
      # Define variables to exclude from KFE analysis
      exclude_vars <- c("product_line_id", "platform_id", "rating", "sales", "revenue")
      
      # Always use default threshold multiplier of 1.0
      threshold_mult <- 1.0
      
      result <- perform_kfe_analysis(
        data = data,
        exclude_vars = exclude_vars,
        threshold_multiplier = threshold_mult
      )
      
      if (length(result$key_factors) > 0) {
        component_status("ready")
      } else {
        component_status("idle")
      }
      
      return(result)
    })
    
    # ------------ Reset filters ------------------------------------
    # Only set up reset observer in full mode
    if (display_mode == "full") {
      observeEvent(input$reset_filters, {
        updateCheckboxInput(session, "show_benchmarks", value = TRUE)
        
        message("KFE settings reset")
      })
    }
    
    # ------------ Output Rendering ------------------------------------
    output$key_factors_text <- renderText({
      result <- kfe_result()
      
      if (is.null(result) || length(result$key_factors) == 0) {
        return("No key factors identified.")
      }
      
      # Show all factors without limitation
      formatted_factors <- format_key_factors(result$key_factors)
      
      return(formatted_factors)
    })
    
    output$benchmark_details <- renderText({
      result <- kfe_result()
      
      if (is.null(result) || length(result$benchmarks) == 0) {
        return("")
      }
      
      benchmark_text <- ""
      factors_with_benchmarks <- 0
      
      for (factor in names(result$benchmarks)) {
        products <- result$benchmarks[[factor]]
        
        # Only process factors that have valid benchmark products
        if (!is.null(products) && length(products) > 0 && !all(is.na(products))) {
          # Filter out any NA values that might have slipped through
          valid_products <- products[!is.na(products) & nchar(products) > 0]
          
          if (length(valid_products) > 0) {
            clean_factor <- stringr::str_replace_all(factor, "_", " ") %>% 
                           stringr::str_to_title()
            # Show all products without limitation
            benchmark_text <- paste0(
              benchmark_text,
              clean_factor, ": ", 
              paste(valid_products, collapse = ", "),
              "\n"
            )
            factors_with_benchmarks <- factors_with_benchmarks + 1
          }
        }
      }
      
      if (factors_with_benchmarks == 0) {
        return("")
      }
      
      return(benchmark_text)
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
      kfe_result = kfe_result,
      component_status = component_status
    ))
  })
}

# Component wrapper -----------------------------------------------------------
#' positionKFEComponent
#' 
#' Implements a key factor evaluation component for position analysis
#' following the Connected Component principle.
#' 
#' @param id Character string. The module ID used for namespacing inputs and outputs.
#' @param app_data_connection Database connection object or list. The data connection supporting Enhanced Data Access pattern (R116).
#'        Can be a DBI connection, a list with getter functions, a file path, or NULL if no database access is needed.
#' @param config List or reactive expression. Configuration parameters for customizing component behavior (optional).
#'        If reactive, will be re-evaluated when dependencies change.
#' @param translate Function. Translation function for UI text elements (defaults to identity function).
#'        Should accept a string and return a translated string.
#' @param display_mode Character string. Display mode - "compact" for summary view, "full" for detailed view (default: "full").
#' @return A list containing UI and server functions structured according to the Connected Component Principle (MP56).
#'         The UI element contains 'filter' and 'display' components, and the server function initializes component functionality.
#' @examples
#' # Basic usage with default settings
#' kfeComp <- positionKFEComponent("kfe_analysis")
#' 
#' # Usage with database connection
#' kfeComp <- positionKFEComponent(
#'   id = "kfe_analysis",
#'   app_data_connection = app_conn, 
#'   config = list(platform_id = "amz")
#' )
#'
#' # Usage with reactive configuration
#' kfeComp <- positionKFEComponent(
#'   id = "kfe_analysis",
#'   app_data_connection = app_conn,
#'   config = reactive({ list(filters = list(platform_id = input$platform)) })
#' )
#' @export
positionKFEComponent <- function(id, app_data_connection = NULL, config = NULL, translate = identity, display_mode = "full") {
  list(
    ui = list(filter = if (display_mode == "full") positionKFEFilterUI(id, translate) else NULL,
              display = positionKFEDisplayUI(id, translate, mode = display_mode)),
    server = function(input, output, session) {
      positionKFEServer(id, app_data_connection, config, session, display_mode = display_mode)
    }
  )
}
