#' Get Customers from Cyberbiz
#'
#' Retrieves customer data from the Cyberbiz API with automatic pagination
#' and rate limiting following MP094 standards.
#'
#' @param connection API connection object from fn_cbz_connect()
#' @param page Starting page number (default: 1)
#' @param per_page Results per page (default: 50)
#' @param all_pages Logical, whether to fetch all pages (default: TRUE)
#' @param filters Optional list of filters to apply
#' @param verbose Logical, whether to print progress messages
#' 
#' @return Data frame with customer information
#' 
#' @examples
#' \dontrun{
#' conn <- fn_cbz_connect()
#' 
#' # Get all customers
#' customers <- fn_cbz_get_customers(conn)
#' 
#' # Get first page only
#' first_page <- fn_cbz_get_customers(conn, all_pages = FALSE)
#' 
#' # Get with pagination control
#' customers <- fn_cbz_get_customers(conn, per_page = 100)
#' 
#' fn_cbz_disconnect(conn)
#' }
#' 
#' @export
fn_cbz_get_customers <- function(
  connection = NULL,
  page = 1,
  per_page = 50,
  all_pages = TRUE,
  filters = list(),
  verbose = TRUE
) {
  # Dependencies
  if (!requireNamespace("httr", quietly = TRUE)) {
    stop("Package 'httr' is required but not installed")
  }
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    stop("Package 'jsonlite' is required but not installed")
  }
  
  # Auto-connect if no connection provided
  auto_disconnect <- FALSE
  if (is.null(connection)) {
    connection <- fn_cbz_connect(verbose = verbose)
    auto_disconnect <- TRUE
  }
  
  # Validate connection
  if (!inherits(connection, "cbz_connection")) {
    stop("Invalid Cyberbiz connection object")
  }
  
  # Load dependencies
  source("scripts/global_scripts/26_platform_apis/common/fn_api_rate_limiter.R")
  source("scripts/global_scripts/26_platform_apis/common/fn_api_error_handler.R")
  
  # Get endpoint configuration
  endpoint_config <- connection$config$api$endpoints$customers
  url <- paste0(connection$base_url, endpoint_config$path)
  
  # Initialize collection
  all_customers <- list()
  has_data <- TRUE
  total_records <- 0
  
  if (verbose) {
    message("Fetching Cyberbiz customers...")
  }
  
  # Pagination loop
  while (has_data) {
    # Apply rate limiting
    connection$rate_limiter <- fn_check_rate_limit(
      connection$rate_limiter, 
      verbose = verbose
    )
    
    # Build query parameters
    query_params <- list(
      page = page,
      per_page = per_page
    )
    
    # Add filters if provided
    if (length(filters) > 0) {
      query_params <- c(query_params, filters)
    }
    
    # Make API request with retry logic
    attempt <- 1
    max_attempts <- connection$config$error_handling$retry_count %||% 3
    
    repeat {
      response <- tryCatch({
        httr::GET(
          url = url,
          query = query_params,
          httr::add_headers(
            Authorization = connection$auth_header,
            Accept = "application/json"
          ),
          httr::timeout(connection$config$api$timeout %||% 30)
        )
      }, error = function(e) {
        error_result <- fn_handle_api_error(
          error = e,
          connection = connection,
          request = list(url = url, endpoint = endpoint_config$path),
          attempt = attempt
        )
        
        if (error_result$retry) {
          attempt <- error_result$attempt
          NULL  # Signal to retry
        } else {
          stop(e)
        }
      })
      
      # Break if successful or max attempts reached
      if (!is.null(response) || attempt > max_attempts) {
        break
      }
    }
    
    # Update request count
    connection$request_count <- connection$request_count + 1
    
    # Check HTTP status
    if (httr::http_error(response)) {
      error_details <- fn_parse_http_error(response)
      
      if (verbose) {
        warning(sprintf(
          "Page %d failed with HTTP %d: %s",
          page, 
          error_details$status_code,
          error_details$message %||% error_details$status_message
        ))
      }
      
      # Check if this is a critical error that should stop pagination
      if (error_details$status_code %in% c(401, 403)) {
        stop(fn_format_api_error(
          error = sprintf("HTTP %d", error_details$status_code),
          connection = connection,
          request = list(endpoint = endpoint_config$path)
        ))
      }
      
      break
    }
    
    # Parse response content
    content <- tryCatch({
      httr::content(response, "parsed", simplifyVector = TRUE)
    }, error = function(e) {
      warning("Failed to parse response content: ", e$message)
      NULL
    })
    
    # Check if we have data
    if (is.null(content) || length(content) == 0) {
      has_data <- FALSE
    } else {
      # Convert to data frame if necessary
      if (is.list(content) && !is.data.frame(content)) {
        content <- as.data.frame(content)
      }
      
      # Store page data
      all_customers[[length(all_customers) + 1]] <- content
      records_in_page <- nrow(content)
      total_records <- total_records + records_in_page
      
      if (verbose) {
        message(sprintf(
          "Retrieved page %d with %d records (Total: %d)",
          page, records_in_page, total_records
        ))
      }
      
      # Check if we should continue
      if (!all_pages) {
        break
      }
      
      # Check if this was the last page
      if (records_in_page < per_page) {
        has_data <- FALSE
      } else {
        page <- page + 1
      }
    }
  }
  
  # Combine all pages
  if (length(all_customers) > 0) {
    # Combine data frames
    df_customers <- tryCatch({
      do.call(rbind, all_customers)
    }, error = function(e) {
      warning("Failed to combine pages, returning list: ", e$message)
      all_customers
    })
    
    # Apply data mapping if configured and data is a data frame
    if (is.data.frame(df_customers) && 
        !is.null(connection$config$data_mapping$customers)) {
      mapping <- connection$config$data_mapping$customers
      
      for (old_name in names(mapping)) {
        new_name <- mapping[[old_name]]
        if (old_name %in% names(df_customers)) {
          names(df_customers)[names(df_customers) == old_name] <- new_name
        }
      }
    }
    
    if (verbose) {
      message(sprintf(
        "✓ Total customers retrieved: %d",
        if (is.data.frame(df_customers)) nrow(df_customers) else length(df_customers)
      ))
    }
    
  } else {
    if (verbose) {
      warning("No customer data retrieved")
    }
    df_customers <- data.frame()
  }
  
  # Auto-disconnect if we created the connection
  if (auto_disconnect) {
    fn_cbz_disconnect(connection, verbose = verbose)
  }
  
  return(df_customers)
}

# Helper operator for NULL default values
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}