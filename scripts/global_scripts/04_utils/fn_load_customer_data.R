#' Load and Combine Customer Data Efficiently
#'
#' Following P071, P074, and R092 principles
load_customer_data <- function(conn, platform_filter = NULL, customer_filter = NULL, 
                              value_threshold = NULL, limit = NULL, use_cache = TRUE) {
  # Create a cache environment if it doesn't exist
  if (!exists("customer_data_cache", envir = .GlobalEnv) && use_cache) {
    assign("customer_data_cache", new.env(), envir = .GlobalEnv)
  }
  
  # Generate cache key based on all parameters
  cache_key <- paste0(
    "platform_", ifelse(is.null(platform_filter), "all", platform_filter),
    "_customers_", ifelse(is.null(customer_filter), "all", paste0(length(customer_filter), "filtered")),
    "_value_", ifelse(is.null(value_threshold), "all", value_threshold),
    "_limit_", ifelse(is.null(limit), "none", limit)
  )
  
  # Check if we have a valid connection
  if (is.null(conn) || !DBI::dbIsValid(conn)) {
    stop("Invalid database connection")
  }
  
  # Check if result is in cache
  if (use_cache && exists("customer_data_cache", envir = .GlobalEnv) && 
      exists(cache_key, envir = get("customer_data_cache", envir = .GlobalEnv))) {
    message("Using cached customer data for ", cache_key)
    return(get(cache_key, envir = get("customer_data_cache", envir = .GlobalEnv)))
  }
  
  # Construct query with filters (implementing P74: Reactive Data Filtering)
  profile_query <- "SELECT * FROM df_profile_by_customer"
  dna_query <- "SELECT * FROM df_dna_by_customer"
  
  # Build WHERE clauses
  profile_where <- NULL
  dna_where <- NULL
  
  # Platform filter (platform_id uses three-letter codes)
  if (!is.null(platform_filter)) {
    platform_id <- tolower(as.character(platform_filter))
    if (platform_id == "all") {
      platform_id <- NULL
    } else {
      platform_df <- NULL
      if (exists("df_platform", envir = .GlobalEnv)) {
        platform_df <- get("df_platform", envir = .GlobalEnv)
      } else {
        csv_path <- file.path("scripts", "global_scripts", "30_global_data", "parameters", "scd_type1", "df_platform.csv")
        if (file.exists(csv_path)) {
          platform_df <- utils::read.csv(csv_path, stringsAsFactors = FALSE)
        }
      }

      if (!is.null(platform_df) && nrow(platform_df) > 0 && "platform_id" %in% names(platform_df)) {
        names(platform_df) <- sub("^\\ufeff", "", names(platform_df))
        platform_df$platform_id <- tolower(as.character(platform_df$platform_id))

        if (!(platform_id %in% platform_df$platform_id)) {
          name_lookup <- tolower(gsub(" ", "", as.character(platform_df$platform_name_english)))
          name_lookup_cn <- tolower(gsub(" ", "", as.character(platform_df$platform_name_chinese)))
          match_idx <- which(name_lookup == gsub(" ", "", platform_id) | name_lookup_cn == gsub(" ", "", platform_id))
          if (length(match_idx) > 0) {
            platform_id <- platform_df$platform_id[match_idx[1]]
          } else {
            warning("No matching platform_id for filter: ", platform_filter)
            platform_id <- NULL
          }
        }
      } else {
        warning("Platform metadata not available for filter: ", platform_filter)
        platform_id <- NULL
      }
    }

    if (!is.null(platform_id)) {
      profile_where <- c(profile_where, paste0("platform_id = '", platform_id, "'"))
      dna_where <- c(dna_where, paste0("platform_id = '", platform_id, "'"))
    } else if (!is.null(platform_filter) && platform_filter != "all") {
      profile_where <- c(profile_where, "1 = 0")
      dna_where <- c(dna_where, "1 = 0")
    }
  }
  
  # Customer ID filter (using SQL IN clause for efficiency)
  if (!is.null(customer_filter) && length(customer_filter) > 0) {
    customer_list <- paste0("'", customer_filter, "'", collapse = ",")
    profile_where <- c(profile_where, paste0("customer_id IN (", customer_list, ")"))
    dna_where <- c(dna_where, paste0("customer_id IN (", customer_list, ")"))
  }
  
  # Value threshold filter
  if (!is.null(value_threshold) && !is.na(value_threshold) && value_threshold > 0) {
    dna_where <- c(dna_where, paste0("m_value >= ", value_threshold))
  }
  
  # Combine WHERE clauses if any exist
  if (length(profile_where) > 0) {
    profile_query <- paste0(profile_query, " WHERE ", paste(profile_where, collapse = " AND "))
  }
  
  if (length(dna_where) > 0) {
    dna_query <- paste0(dna_query, " WHERE ", paste(dna_where, collapse = " AND "))
  }
  
  # Add ORDER BY to ensure consistent ordering
  profile_query <- paste0(profile_query, " ORDER BY customer_id")
  dna_query <- paste0(dna_query, " ORDER BY customer_id")
  
  # Add LIMIT if specified
  if (!is.null(limit) && !is.na(limit) && limit > 0) {
    profile_query <- paste0(profile_query, " LIMIT ", limit)
    dna_query <- paste0(dna_query, " LIMIT ", limit)
  }
  
  # Log queries if in debug mode
  if (exists("debug_mode") && debug_mode) {
    message("Profile query: ", profile_query)
    message("DNA query: ", dna_query)
  }
  
  # Load data
  tryCatch({
    profile_data <- DBI::dbGetQuery(conn, profile_query)
    dna_data <- DBI::dbGetQuery(conn, dna_query)
    
    # Check if we got any data
    if (nrow(profile_data) == 0 || nrow(dna_data) == 0) {
      message("No customer data found with the current filters")
      return(data.frame())
    }
    
    # Combine data efficiently using P71 (Row-Aligned Tables)
    combined_data <- combine_aligned_tables(
      profile_data, 
      dna_data, 
      key_column = "customer_id", 
      verify = TRUE,
      fallback_to_join = TRUE,
      handle_duplicates = "suffix"
    )
    
    # Cache the result if caching is enabled
    if (use_cache && exists("customer_data_cache", envir = .GlobalEnv)) {
      assign(cache_key, combined_data, envir = get("customer_data_cache", envir = .GlobalEnv))
    }
    
    return(combined_data)
  }, error = function(e) {
    message("Error loading customer data: ", e$message)
    return(data.frame())
  })
}
