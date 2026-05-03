#' Detect marketing channel availability
#'
#' @description
#' Uses R38 (Platform Numbering Convention) to map between platform numbers and names
#' and detect which marketing channels have data available in the database.
#'
#' @param conn A DBI connection to the app database
#' @param platform_dict A data frame mapping platform numbers to names
#' @return A list of channel availability by channel ID
#' @export
#' @implements MP45 Automatic Data Availability Detection
detect_marketing_channel_availability <- function(conn, platform_dict = NULL) {
  # Initialize availability registry
  availability <- list()
  
  # If platform dictionary is not provided, try to load it
  if (is.null(platform_dict)) {
    platform_dict <- load_platform_dictionary()
  }
  
  # If we still don't have a platform dictionary, use default mapping from R38
  if (is.null(platform_dict)) {
    # Default mapping based on R38
    platform_dict <- data.frame(
      platform_number = c("1", "2", "3", "4", "5", "6", "7", "9"),
      platform_name_english = c("Amazon", "Official Website", "Retail Store", 
                              "Distributor", "Social Media", "eBay", "Cyberbiz", 
                              "Multi-platform"),
      code_alias = c("AMZ", "WEB", "RET", "DST", "SOC", "EBY", "CBZ", "MPT"),
      stringsAsFactors = FALSE
    )
  }
  
  # Check if we have a valid connection
  if (is.null(conn) || !DBI::dbIsValid(conn)) {
    message("Invalid database connection. Cannot detect channel availability.")
    
    # Set all channels as unavailable
    for (i in 1:nrow(platform_dict)) {
      channel_id <- tolower(gsub(" ", "", platform_dict$platform_name_english[i]))
      availability[[channel_id]] <- FALSE
    }
    
    return(availability)
  }
  
  # Get all tables in the database
  tables <- DBI::dbListTables(conn)
  message("Found ", length(tables), " tables in database")
  
  # Check each channel for data
  for (i in 1:nrow(platform_dict)) {
    platform_num <- platform_dict$platform_number[i]
    platform_name <- platform_dict$platform_name_english[i]
    channel_id <- tolower(gsub(" ", "", platform_name))
    
    # Initially mark as unavailable
    availability[[channel_id]] <- FALSE
    
    # Check for channel-specific tables using numeric prefixes (per R38)
    platform_tables <- grep(paste0("^p", platform_num, "_"), tables, value = TRUE)
    
    # Also check for tables with platform name
    name_tables <- grep(tolower(channel_id), tolower(tables), value = TRUE)
    
    # If we found any tables, check if they have data
    all_tables <- unique(c(platform_tables, name_tables))
    
    if (length(all_tables) > 0) {
      message("Found ", length(all_tables), " tables for platform: ", platform_name)
      
      # Check if any table has data
      for (table in all_tables) {
        tryCatch({
          query <- paste0("SELECT COUNT(*) as count FROM \"", table, "\"")
          result <- DBI::dbGetQuery(conn, query)
          
          if (result$count[1] > 0) {
            availability[[channel_id]] <- TRUE
            message("Platform ", platform_name, " (", channel_id, ") has data in table: ", table)
            break  # Found data, no need to check other tables
          }
        }, error = function(e) {
          message("Error checking table ", table, " for platform ", platform_name, ": ", e$message)
        })
      }
    } 
    
    # If still not available, check for platform ID in customer profile or dna tables
    if (!availability[[channel_id]]) {
      tryCatch({
        # Check in df_profile_by_customer
        if ("df_profile_by_customer" %in% tables) {
          query <- paste0("SELECT COUNT(*) as count FROM df_profile_by_customer WHERE platform_id = ", platform_num)
          result <- DBI::dbGetQuery(conn, query)
          
          if (result$count[1] > 0) {
            availability[[channel_id]] <- TRUE
            message("Platform ", platform_name, " (", channel_id, ") has data in df_profile_by_customer table")
          }
        }
        
        # Check in df_dna_by_customer if still not available
        if (!availability[[channel_id]] && "df_dna_by_customer" %in% tables) {
          query <- paste0("SELECT COUNT(*) as count FROM df_dna_by_customer WHERE platform = ", platform_num)
          result <- DBI::dbGetQuery(conn, query)
          
          if (result$count[1] > 0) {
            availability[[channel_id]] <- TRUE
            message("Platform ", platform_name, " (", channel_id, ") has data in df_dna_by_customer table")
          }
        }
      }, error = function(e) {
        message("Error checking profile/dna tables for platform ", platform_name, ": ", e$message)
      })
    }
    
    # If still not available, check if there's a sales table with a platform/channel column
    if (!availability[[channel_id]]) {
      tryCatch({
        # Try to find a general sales table
        sales_tables <- grep("sales", tolower(tables), value = TRUE)
        
        for (sales_table in sales_tables) {
          # Check if this table has a channel or platform column
          cols_query <- paste0("SELECT * FROM \"", sales_table, "\" LIMIT 0")
          cols_result <- DBI::dbGetQuery(conn, cols_query)
          cols <- names(cols_result)
          
          channel_cols <- grep("channel|platform", tolower(cols), value = TRUE)
          if (length(channel_cols) > 0) {
            # Check if there's data for this platform
            for (col in channel_cols) {
              # Try with platform number
              num_query <- paste0("SELECT COUNT(*) as count FROM \"", sales_table, 
                               "\" WHERE ", col, " = '", platform_num, "'")
              
              tryCatch({
                num_result <- DBI::dbGetQuery(conn, num_query)
                if (num_result$count[1] > 0) {
                  availability[[channel_id]] <- TRUE
                  message("Platform ", platform_name, " (", channel_id, 
                       ") has data in table: ", sales_table, " with platform number")
                  break
                }
              }, error = function(e) {
                # Skip this query if it fails
              })
              
              # Try with platform name
              name_query <- paste0("SELECT COUNT(*) as count FROM \"", sales_table, 
                                "\" WHERE LOWER(", col, ") = '", tolower(platform_name), "'")
              
              tryCatch({
                name_result <- DBI::dbGetQuery(conn, name_query)
                if (name_result$count[1] > 0) {
                  availability[[channel_id]] <- TRUE
                  message("Platform ", platform_name, " (", channel_id, 
                       ") has data in table: ", sales_table, " with platform name")
                  break
                }
              }, error = function(e) {
                # Skip this query if it fails
              })
              
              # Try with channel ID
              id_query <- paste0("SELECT COUNT(*) as count FROM \"", sales_table, 
                             "\" WHERE LOWER(", col, ") = '", channel_id, "'")
              
              tryCatch({
                id_result <- DBI::dbGetQuery(conn, id_query)
                if (id_result$count[1] > 0) {
                  availability[[channel_id]] <- TRUE
                  message("Platform ", platform_name, " (", channel_id, 
                       ") has data in table: ", sales_table, " with channel ID")
                  break
                }
              }, error = function(e) {
                # Skip this query if it fails
              })
            }
            
            if (availability[[channel_id]]) {
              break  # Found data, no need to check other tables
            }
          }
        }
      }, error = function(e) {
        message("Error checking sales tables for platform ", platform_name, ": ", e$message)
      })
    }
    
    message("Platform ", platform_name, " (", channel_id, ") availability: ", 
           availability[[channel_id]])
  }
  
  return(availability)
}