#' Get Orders from Cyberbiz
#'
#' Retrieves order data from the Cyberbiz API with automatic pagination
#' and rate limiting following MP094 standards.
#'
#' @param connection API connection object from fn_cbz_connect()
#' @param page Starting page number (default: 1)
#' @param per_page Results per page (default: 50)
#' @param all_pages Logical, whether to fetch all pages (default: TRUE)
#' @param filters Optional list of filters (e.g., list(status = "completed", created_after = "2024-01-01"))
#' @param verbose Logical, whether to print progress messages
#' 
#' @return Data frame with order information
#' 
#' @examples
#' \dontrun{
#' conn <- fn_cbz_connect()
#' 
#' # Get all orders
#' orders <- fn_cbz_get_orders(conn)
#' 
#' # Get orders with filters
#' recent_orders <- fn_cbz_get_orders(
#'   conn,
#'   filters = list(created_after = "2024-01-01")
#' )
#' 
#' # Get completed orders only
#' completed <- fn_cbz_get_orders(
#'   conn,
#'   filters = list(status = "completed")
#' )
#' 
#' fn_cbz_disconnect(conn)
#' }
#' 
#' @export
fn_cbz_get_orders <- function(
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
  endpoint_config <- connection$config$api$endpoints$orders
  url <- paste0(connection$base_url, endpoint_config$path)
  
  # Initialize collection
  all_orders <- list()
  has_data <- TRUE
  total_records <- 0
  
  if (verbose) {
    message("Fetching Cyberbiz orders...")
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
        # Handle nested list structures (common in order data)
        content <- tryCatch({
          if ("data" %in% names(content)) {
            # API might return {data: [...], meta: {...}}
            as.data.frame(content$data)
          } else {
            as.data.frame(content)
          }
        }, error = function(e) {
          # If conversion fails, try jsonlite
          jsonlite::fromJSON(jsonlite::toJSON(content), flatten = TRUE)
        })
      }
      
      # Store page data
      all_orders[[length(all_orders) + 1]] <- content
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
  if (length(all_orders) > 0) {
    # Combine data frames with fill = TRUE for inconsistent columns
    df_orders <- tryCatch({
      if (requireNamespace("data.table", quietly = TRUE)) {
        data.table::rbindlist(all_orders, fill = TRUE)
      } else {
        do.call(rbind, all_orders)
      }
    }, error = function(e) {
      warning("Failed to combine pages, returning list: ", e$message)
      all_orders
    })
    
    # Convert back to data.frame if using data.table
    if ("data.table" %in% class(df_orders)) {
      df_orders <- as.data.frame(df_orders)
    }
    
    # Apply data mapping if configured and data is a data frame
    if (is.data.frame(df_orders) && 
        !is.null(connection$config$data_mapping$orders)) {
      mapping <- connection$config$data_mapping$orders
      
      for (old_name in names(mapping)) {
        new_name <- mapping[[old_name]]
        if (old_name %in% names(df_orders)) {
          names(df_orders)[names(df_orders) == old_name] <- new_name
        }
      }
    }
    
    # Process order-specific fields
    if (is.data.frame(df_orders)) {
      # Ensure date fields are properly formatted
      date_fields <- c("order_date", "created_at", "updated_at", "shipped_at")
      for (field in date_fields) {
        if (field %in% names(df_orders)) {
          df_orders[[field]] <- as.POSIXct(
            df_orders[[field]], 
            format = "%Y-%m-%dT%H:%M:%S",
            tz = "UTC"
          )
        }
      }
      
      # Ensure numeric fields are properly typed
      numeric_fields <- c("total_amount", "subtotal", "tax", "shipping", "discount")
      for (field in numeric_fields) {
        if (field %in% names(df_orders)) {
          df_orders[[field]] <- as.numeric(df_orders[[field]])
        }
      }
    }
    
    if (verbose) {
      message(sprintf(
        "✓ Total orders retrieved: %d",
        if (is.data.frame(df_orders)) nrow(df_orders) else length(df_orders)
      ))
    }
    
  } else {
    if (verbose) {
      warning("No order data retrieved")
    }
    df_orders <- data.frame()
  }
  
  # Auto-disconnect if we created the connection
  if (auto_disconnect) {
    fn_cbz_disconnect(connection, verbose = verbose)
  }
  
  return(df_orders)
}

# Helper operator for NULL default values
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}