#' Initialize data availability registry and detection
#'
#' @description
#' This function creates and initializes the data availability registry,
#' connects to the database, and performs availability detection for various 
#' data dimensions following MP45 (Automatic Data Availability Detection).
#' Also sets up periodic refresh if in Shiny context.
#'
#' @param conn Optional DBI connection to the app database. If NULL, a connection will be established.
#' @return List containing the availability registry
#' @export
#' @implements MP45 Automatic Data Availability Detection
initialize_data_availability <- function(conn = NULL) {
  # Initialize centralized availability registry (following MP45 pattern)
  availability_registry <- list()
  
  # Initialize database connection if not provided
  if (is.null(conn)) {
    tryCatch({
      conn <- connect_to_app_database("app_data/app_data.duckdb")
      message("Connected to app_data.duckdb database")
      
      # Register database availability in registry
      availability_registry[["database"]] <- list(
        app_data = TRUE,
        connection_valid = TRUE
      )
    }, error = function(e) {
      warning("Failed to connect to app_data.duckdb: ", e$message)
      
      # Register database unavailability
      availability_registry[["database"]] <- list(
        app_data = FALSE,
        connection_valid = FALSE
      )
      
      # Return early if we can't connect to the database
      return(availability_registry)
    })
  } else {
    # Register database availability in registry with provided connection
    availability_registry[["database"]] <- list(
      app_data = TRUE,
      connection_valid = !is.null(conn)
    )
  }
  
  # Load platform dictionary
  if (!exists("config") || is.null(config)) {
    warning("Config object not found. This may cause issues with platform detection.")
    platform_dict <- tryCatch({
      load_platform_dictionary()
    }, error = function(e) {
      warning("Failed to load platform dictionary: ", e$message)
      return(NULL)
    })
  } else {
    # Use config if available (from first implementation)
    platform_dict <- names(config$parameters$platform$platform_name_english)
  }
  
  # Initialize data availability detection with deep inspection
  channel_availability <- tryCatch({
    detect_marketing_channel_availability(conn, platform_dict)
  }, error = function(e) {
    warning("Failed to detect marketing channel availability: ", e$message)
    return(NULL)
  })
  
  # Register channel availability in registry
  availability_registry[["channel"]] <- channel_availability
  
  # Store channel availability in a global variable for compatibility
  assign("channel_availability", channel_availability, envir = .GlobalEnv)
  
  # Initialize dimension availability registry
  availability_registry[["dimension"]] <- tryCatch({
    list(
      customer = check_data_dimension(conn, "customer", "df_profile_by_customer"),
      dna = check_data_dimension(conn, "dna", "df_dna_by_customer"),
      region = FALSE,  # Will be checked once customer data is loaded
      category = check_data_dimension(conn, "category", "df_product_category")
    )
  }, error = function(e) {
    warning("Failed to check data dimensions: ", e$message)
    return(list(
      customer = FALSE,
      dna = FALSE,
      region = FALSE,
      category = FALSE
    ))
  })
  
  # Log the detected platform availability in a safer way
  tryCatch({
    if (!is.null(channel_availability)) {
      message("Platform availability detected:")
      
      # Get valid channel IDs only
      channel_ids <- names(channel_availability)
      if (length(channel_ids) > 0) {
        valid_ids <- channel_ids[nzchar(channel_ids)]
        
        # Log each valid channel
        for (id in valid_ids) {
          if (nzchar(id) && !is.null(channel_availability[[id]])) {
            message("  ", id, ": ", channel_availability[[id]])
          }
        }
      }
    } else {
      message("No platform availability data detected")
    }
  }, error = function(e) {
    message("Error processing platform availability: ", e$message)
  })
  
  # Schedule periodic refresh if shiny is available (from second implementation)
  if (requireNamespace("shiny", quietly = TRUE)) {
    # Set up a global timer flag to prevent multiple timers
    if (!exists("availability_refresh_active", envir = .GlobalEnv)) {
      assign("availability_refresh_active", TRUE, envir = .GlobalEnv)
      
      # Set up automatic refresh mechanism (5 minutes)
      shiny::observe({
        shiny::invalidateLater(300000)
        
        # Refresh channel availability
        updated_channel_availability <- tryCatch({
          detect_marketing_channel_availability(conn, platform_dict)
        }, error = function(e) {
          warning("Failed to refresh marketing channel availability: ", e$message)
          return(channel_availability)  # Keep old values on error
        })
        
        # Update global variable
        assign("channel_availability", updated_channel_availability, envir = .GlobalEnv)
        
        # Update registry as well
        tryCatch({
          availability_registry[["channel"]] <- updated_channel_availability
        }, error = function(e) {
          warning("Failed to update availability registry: ", e$message)
        })
        
        message("Channel availability refreshed")
      })
    }
  }
  
  # Make the registry global for easy access elsewhere
  assign("availability_registry", availability_registry, envir = .GlobalEnv)
  
  # Return the availability registry
  return(availability_registry)
}