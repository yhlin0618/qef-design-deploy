#' Advanced Metrics Availability Detection
#'
#' This file implements MP45 (Automatic Data Availability Detection Metaprinciple)
#' to detect whether sufficient data is available to calculate advanced marketing metrics.
#'
#' @author Claude
#' @date 2025-04-08
#' @implements MP45 Automatic Data Availability Detection
#' @implements R21 One Function One File
#' @implements R19 Object Naming Convention

#' Detect which advanced marketing metrics can be calculated
#'
#' This function analyzes the database to determine which advanced marketing metrics
#' can be calculated based on available data. It follows MP45 by performing
#' runtime data inspection to determine data availability, rather than relying
#' on configuration.
#'
#' @param conn A DBI connection to the app database
#' @param metrics_list Optional list of specific metrics to check. If NULL, all metrics are checked.
#' @return A list of metrics with boolean values indicating their availability
#' @export
detect_advanced_metrics_availability <- function(conn, metrics_list = NULL) {
  # Initialize availability registry
  availability <- list()
  
  # Define default metrics if none provided
  if (is.null(metrics_list)) {
    metrics_list <- c(
      "customer_lifetime_value",
      "customer_acquisition_cost",
      "retention_rate",
      "purchase_frequency",
      "average_order_value",
      "repeat_purchase_rate",
      "net_promoter_score",
      "customer_satisfaction",
      "churn_rate",
      "market_reach",
      "conversion_rate"
    )
  }
  
  # Set all metrics as unavailable initially (Default Deny principle - MP02)
  for (metric in metrics_list) {
    availability[[metric]] <- FALSE
  }
  
  # Check if we have a valid connection
  if (is.null(conn) || !DBI::dbIsValid(conn)) {
    message("Invalid database connection. Cannot detect metric availability.")
    return(availability)
  }
  
  # Get all tables in the database
  tables <- NULL
  tryCatch({
    tables <- DBI::dbListTables(conn)
    message("Found ", length(tables), " tables in database")
  }, error = function(e) {
    message("Error listing tables: ", e$message)
    return(availability)
  })
  
  if (is.null(tables) || length(tables) == 0) {
    message("No tables found in database")
    return(availability)
  }
  
  # Check each metric's required data
  message("Checking advanced metrics availability...")
  
  # 1. Customer Lifetime Value (CLV)
  if ("customer_lifetime_value" %in% metrics_list) {
    clv_available <- check_clv_availability(conn, tables)
    availability[["customer_lifetime_value"]] <- clv_available
    message("CLV metric availability: ", clv_available)
  }
  
  # 2. Customer Acquisition Cost (CAC)
  if ("customer_acquisition_cost" %in% metrics_list) {
    cac_available <- check_cac_availability(conn, tables)
    availability[["customer_acquisition_cost"]] <- cac_available
    message("CAC metric availability: ", cac_available)
  }
  
  # 3. Retention Rate
  if ("retention_rate" %in% metrics_list) {
    retention_available <- check_retention_availability(conn, tables)
    availability[["retention_rate"]] <- retention_available
    message("Retention rate metric availability: ", retention_available)
  }
  
  # 4. Purchase Frequency
  if ("purchase_frequency" %in% metrics_list) {
    frequency_available <- check_frequency_availability(conn, tables)
    availability[["purchase_frequency"]] <- frequency_available
    message("Purchase frequency metric availability: ", frequency_available)
  }
  
  # 5. Average Order Value (AOV)
  if ("average_order_value" %in% metrics_list) {
    aov_available <- check_aov_availability(conn, tables)
    availability[["average_order_value"]] <- aov_available
    message("AOV metric availability: ", aov_available)
  }
  
  # 6. Repeat Purchase Rate
  if ("repeat_purchase_rate" %in% metrics_list) {
    repeat_available <- check_repeat_purchase_availability(conn, tables)
    availability[["repeat_purchase_rate"]] <- repeat_available
    message("Repeat purchase rate metric availability: ", repeat_available)
  }
  
  # 7. Net Promoter Score (NPS)
  if ("net_promoter_score" %in% metrics_list) {
    nps_available <- check_nps_availability(conn, tables)
    availability[["net_promoter_score"]] <- nps_available
    message("NPS metric availability: ", nps_available)
  }
  
  # 8. Customer Satisfaction
  if ("customer_satisfaction" %in% metrics_list) {
    csat_available <- check_csat_availability(conn, tables)
    availability[["customer_satisfaction"]] <- csat_available
    message("CSAT metric availability: ", csat_available)
  }
  
  # 9. Churn Rate
  if ("churn_rate" %in% metrics_list) {
    churn_available <- check_churn_availability(conn, tables)
    availability[["churn_rate"]] <- churn_available
    message("Churn rate metric availability: ", churn_available)
  }
  
  # 10. Market Reach
  if ("market_reach" %in% metrics_list) {
    reach_available <- check_market_reach_availability(conn, tables)
    availability[["market_reach"]] <- reach_available
    message("Market reach metric availability: ", reach_available)
  }
  
  # 11. Conversion Rate
  if ("conversion_rate" %in% metrics_list) {
    conversion_available <- check_conversion_rate_availability(conn, tables)
    availability[["conversion_rate"]] <- conversion_available
    message("Conversion rate metric availability: ", conversion_available)
  }
  
  return(availability)
}

#' Check if Customer Lifetime Value can be calculated
#'
#' Implements the deep inspection pattern from MP45
#'
#' @param conn A DBI connection to the app database
#' @param tables List of available tables
#' @return Boolean indicating if CLV can be calculated
#' @noexport
check_clv_availability <- function(conn, tables) {
  # CLV requires customer purchase history with values and dates
  required_tables <- has_any_of(tables, c("sales", "orders", "transactions", "df_profile_by_customer_dna"))
  if (!required_tables$has_any) {
    return(FALSE)
  }
  
  # If we have customer_profile_dna with clv field, it's the easiest path
  if ("df_profile_by_customer_dna" %in% tables) {
    has_clv_column <- check_column_exists(conn, "df_profile_by_customer_dna", "clv")
    if (has_clv_column) {
      return(TRUE)
    }
  }
  
  # Otherwise, we need sales data with customer_id, date, and value
  sales_table <- required_tables$found_table
  
  # Check if required columns exist in the sales table
  required_columns <- c("customer_id", "sale_date", "total_amount", "grand_total")
  if (check_columns_exist(conn, sales_table, required_columns, any_of = c("total_amount", "grand_total"))) {
    # Check if there's enough data (multiple purchases per customer)
    query <- paste0("SELECT COUNT(*) as count FROM (
                      SELECT customer_id, COUNT(*) as purchase_count 
                      FROM ", sales_table, " 
                      GROUP BY customer_id 
                      HAVING COUNT(*) > 1
                    ) as repeat_customers")
    
    result <- fn_safe_query(conn, query)
    
    return(!is.null(result) && result$count[1] > 0)
  }
  
  return(FALSE)
}

#' Check if Customer Acquisition Cost can be calculated
#'
#' @param conn A DBI connection to the app database
#' @param tables List of available tables
#' @return Boolean indicating if CAC can be calculated
#' @noexport
check_cac_availability <- function(conn, tables) {
  # CAC requires marketing campaign cost data and new customers by campaign
  required_campaign_tables <- has_any_of(tables, c("campaigns", "marketing_campaigns", "campaign_costs"))
  required_customer_tables <- has_any_of(tables, c("customers", "df_profile_by_customer", "new_customers"))
  
  if (!required_campaign_tables$has_any || !required_customer_tables$has_any) {
    return(FALSE)
  }
  
  # Check if campaign table has cost/budget column
  campaign_table <- required_campaign_tables$found_table
  budget_columns <- c("budget", "cost", "campaign_cost", "spend", "campaign_spend")
  has_budget <- check_columns_exist(conn, campaign_table, budget_columns, any_of = TRUE)
  
  if (!has_budget) {
    return(FALSE)
  }
  
  # Check if customers table has signup_date or can be linked to campaigns
  customer_table <- required_customer_tables$found_table
  date_columns <- c("signup_date", "acquisition_date", "registration_date", "created_at")
  has_date <- check_columns_exist(conn, customer_table, date_columns, any_of = TRUE)
  
  return(has_date)
}

#' Check if Retention Rate can be calculated
#'
#' @param conn A DBI connection to the app database
#' @param tables List of available tables
#' @return Boolean indicating if retention rate can be calculated
#' @noexport
check_retention_availability <- function(conn, tables) {
  # Retention rate requires purchase history with dates and customer IDs
  required_tables <- has_any_of(tables, c("sales", "orders", "transactions", "df_profile_by_customer_dna"))
  
  if (!required_tables$has_any) {
    return(FALSE)
  }
  
  # If we have customer_profile_dna with retention info, it's the easiest path
  if ("df_profile_by_customer_dna" %in% tables) {
    retention_columns <- c("retention_rate", "nes_status", "customer_status")
    has_retention_column <- check_columns_exist(conn, "df_profile_by_customer_dna", retention_columns, any_of = TRUE)
    if (has_retention_column) {
      return(TRUE)
    }
  }
  
  # Otherwise, we need sales data with customer_id and date spanning multiple periods
  sales_table <- required_tables$found_table
  
  # Check if required columns exist
  required_columns <- c("customer_id", "sale_date", "date", "transaction_date")
  has_columns <- check_columns_exist(conn, sales_table, required_columns, any_of = c("sale_date", "date", "transaction_date"))
  
  if (has_columns) {
    # Check date range for sufficient history
    date_col <- get_first_existing_column(conn, sales_table, c("sale_date", "date", "transaction_date"))
    
    if (!is.null(date_col)) {
      query <- paste0("SELECT MIN(", date_col, ") as min_date, MAX(", date_col, ") as max_date FROM ", sales_table)
      result <- safe_query(conn, query)
      
      if (!is.null(result) && !is.na(result$min_date) && !is.na(result$max_date)) {
        min_date <- as.Date(result$min_date)
        max_date <- as.Date(result$max_date)
        
        # Need at least 30 days of data to calculate meaningful retention
        return(difftime(max_date, min_date, units = "days") >= 30)
      }
    }
  }
  
  return(FALSE)
}

#' Check if Purchase Frequency can be calculated
#'
#' @param conn A DBI connection to the app database
#' @param tables List of available tables
#' @return Boolean indicating if purchase frequency can be calculated
#' @noexport
check_frequency_availability <- function(conn, tables) {
  # Similar requirements to retention rate
  required_tables <- has_any_of(tables, c("sales", "orders", "transactions", "df_profile_by_customer_dna"))
  
  if (!required_tables$has_any) {
    return(FALSE)
  }
  
  # If we have customer_profile_dna with frequency info, it's the easiest path
  if ("df_profile_by_customer_dna" %in% tables) {
    frequency_columns <- c("f_value", "frequency", "purchase_count", "ipt", "ipt_mean")
    has_frequency_column <- check_columns_exist(conn, "df_profile_by_customer_dna", frequency_columns, any_of = TRUE)
    if (has_frequency_column) {
      return(TRUE)
    }
  }
  
  # Otherwise, we need sales data with customer_id
  sales_table <- required_tables$found_table
  
  # Check if customer_id column exists
  if (check_column_exists(conn, sales_table, "customer_id")) {
    # Check if there are customers with multiple purchases
    query <- paste0("SELECT COUNT(*) as count FROM (
                      SELECT customer_id, COUNT(*) as purchase_count 
                      FROM ", sales_table, " 
                      GROUP BY customer_id 
                      HAVING COUNT(*) > 1
                    ) as repeat_customers")
    
    result <- fn_safe_query(conn, query)
    
    return(!is.null(result) && result$count[1] > 0)
  }
  
  return(FALSE)
}

#' Check if Average Order Value can be calculated
#'
#' @param conn A DBI connection to the app database
#' @param tables List of available tables
#' @return Boolean indicating if AOV can be calculated
#' @noexport
check_aov_availability <- function(conn, tables) {
  # AOV requires sales/order data with monetary values
  required_tables <- fn_has_any_of(tables, c("sales", "orders", "transactions"))
  
  if (!required_tables$has_any) {
    return(FALSE)
  }
  
  sales_table <- required_tables$found_table
  
  # Check for monetary value columns
  value_columns <- c("total_amount", "grand_total", "order_value", "sale_amount", "revenue")
  has_value_column <- check_columns_exist(conn, sales_table, value_columns, any_of = TRUE)
  
  return(has_value_column)
}

#' Check if Repeat Purchase Rate can be calculated
#'
#' @param conn A DBI connection to the app database
#' @param tables List of available tables
#' @return Boolean indicating if repeat purchase rate can be calculated
#' @noexport
check_repeat_purchase_availability <- function(conn, tables) {
  # Very similar to purchase frequency check
  required_tables <- has_any_of(tables, c("sales", "orders", "transactions", "df_profile_by_customer_dna"))
  
  if (!required_tables$has_any) {
    return(FALSE)
  }
  
  # If we have customer_profile_dna with repeat purchase info, it's the easiest path
  if ("df_profile_by_customer_dna" %in% tables) {
    repeat_columns <- c("repeat_purchase_rate", "repeat_rate", "f_value", "frequency")
    has_repeat_column <- fn_check_columns_exist(conn, "df_profile_by_customer_dna", repeat_columns, any_of = TRUE)
    if (has_repeat_column) {
      return(TRUE)
    }
  }
  
  # Otherwise, we need sales data with customer_id
  sales_table <- required_tables$found_table
  
  # Check if customer_id column exists
  if (check_column_exists(conn, sales_table, "customer_id")) {
    # Check if we have enough unique customers
    query <- paste0("SELECT COUNT(DISTINCT customer_id) as count FROM ", sales_table)
    result <- fn_safe_query(conn, query)
    
    if (!is.null(result) && result$count[1] > 0) {
      # Check if there are customers with multiple purchases
      query <- paste0("SELECT COUNT(*) as count FROM (
                        SELECT customer_id, COUNT(*) as purchase_count 
                        FROM ", sales_table, " 
                        GROUP BY customer_id 
                        HAVING COUNT(*) > 1
                      ) as repeat_customers")
      
      result <- safe_query(conn, query)
      
      return(!is.null(result) && result$count[1] > 0)
    }
  }
  
  return(FALSE)
}

#' Check if Net Promoter Score can be calculated
#'
#' @param conn A DBI connection to the app database
#' @param tables List of available tables
#' @return Boolean indicating if NPS can be calculated
#' @noexport
check_nps_availability <- function(conn, tables) {
  # NPS requires survey or feedback data
  required_tables <- fn_has_any_of(tables, c("surveys", "feedback", "nps_data", "customer_feedback"))
  
  if (!required_tables$has_any) {
    return(FALSE)
  }
  
  feedback_table <- required_tables$found_table
  
  # Check for NPS score column
  nps_columns <- c("nps_score", "score", "rating", "recommendation_score", "promoter_score")
  has_nps_column <- fn_check_columns_exist(conn, feedback_table, nps_columns, any_of = TRUE)
  
  if (has_nps_column) {
    # Check if we have enough ratings
    nps_col <- fn_get_first_existing_column(conn, feedback_table, nps_columns)
    
    if (!is.null(nps_col)) {
      query <- paste0("SELECT COUNT(*) as count FROM ", feedback_table, 
                     " WHERE ", nps_col, " IS NOT NULL")
      result <- safe_query(conn, query)
      
      return(!is.null(result) && result$count[1] >= 10)  # Need at least 10 ratings
    }
  }
  
  return(FALSE)
}

#' Check if Customer Satisfaction can be calculated
#'
#' @param conn A DBI connection to the app database
#' @param tables List of available tables
#' @return Boolean indicating if CSAT can be calculated
#' @noexport
check_csat_availability <- function(conn, tables) {
  # CSAT requires survey or feedback data
  required_tables <- fn_has_any_of(tables, c("surveys", "feedback", "csat_data", "customer_feedback"))
  
  if (!required_tables$has_any) {
    return(FALSE)
  }
  
  feedback_table <- required_tables$found_table
  
  # Check for satisfaction score column
  csat_columns <- c("csat_score", "satisfaction_score", "satisfaction", "satisfaction_rating")
  has_csat_column <- fn_check_columns_exist(conn, feedback_table, csat_columns, any_of = TRUE)
  
  if (has_csat_column) {
    # Check if we have enough ratings
    csat_col <- fn_get_first_existing_column(conn, feedback_table, csat_columns)
    
    if (!is.null(csat_col)) {
      query <- paste0("SELECT COUNT(*) as count FROM ", feedback_table, 
                     " WHERE ", csat_col, " IS NOT NULL")
      result <- safe_query(conn, query)
      
      return(!is.null(result) && result$count[1] >= 10)  # Need at least 10 ratings
    }
  }
  
  return(FALSE)
}

#' Check if Churn Rate can be calculated
#'
#' @param conn A DBI connection to the app database
#' @param tables List of available tables
#' @return Boolean indicating if churn rate can be calculated
#' @noexport
check_churn_availability <- function(conn, tables) {
  # Churn requires customer status or purchase history with dates
  required_tables <- fn_has_any_of(tables, c("customers", "df_profile_by_customer", 
                                         "df_profile_by_customer_dna", "sales", "orders"))
  
  if (!required_tables$has_any) {
    return(FALSE)
  }
  
  # If we have customer_profile_dna, check for status indicators
  if ("df_profile_by_customer_dna" %in% tables) {
    status_columns <- c("nes_status", "customer_status", "churn_status", "is_churned", "churn_rate")
    has_status_column <- fn_check_columns_exist(conn, "df_profile_by_customer_dna", status_columns, any_of = TRUE)
    if (has_status_column) {
      return(TRUE)
    }
  }
  
  # If we have customers table, check for status
  if ("customers" %in% tables || "df_profile_by_customer" %in% tables) {
    customer_table <- if ("customers" %in% tables) "customers" else "df_profile_by_customer"
    status_columns <- c("status", "customer_status", "is_active", "is_churned", "churn_status")
    has_status_column <- fn_check_columns_exist(conn, customer_table, status_columns, any_of = TRUE)
    if (has_status_column) {
      return(TRUE)
    }
  }
  
  # If we have sales data with dates, we can calculate churn based on inactivity
  if ("sales" %in% tables || "orders" %in% tables) {
    sales_table <- if ("sales" %in% tables) "sales" else "orders"
    
    # Check if required columns exist
    required_columns <- c("customer_id", "sale_date", "date", "transaction_date")
    date_columns <- c("sale_date", "date", "transaction_date")
    has_columns <- fn_check_columns_exist(conn, sales_table, required_columns, 
                                      any_of = date_columns)
    
    if (has_columns) {
      # Check date range for sufficient history
      date_col <- fn_get_first_existing_column(conn, sales_table, date_columns)
      
      if (!is.null(date_col)) {
        query <- paste0("SELECT MIN(", date_col, ") as min_date, MAX(", date_col, ") as max_date FROM ", sales_table)
        result <- safe_query(conn, query)
        
        if (!is.null(result) && !is.na(result$min_date) && !is.na(result$max_date)) {
          min_date <- as.Date(result$min_date)
          max_date <- as.Date(result$max_date)
          
          # Need at least 30 days of data to calculate meaningful churn
          return(difftime(max_date, min_date, units = "days") >= 90)
        }
      }
    }
  }
  
  return(FALSE)
}

#' Check if Market Reach can be calculated
#'
#' @param conn A DBI connection to the app database
#' @param tables List of available tables
#' @return Boolean indicating if market reach can be calculated
#' @noexport
check_market_reach_availability <- function(conn, tables) {
  # Market reach requires customer data with location or campaign performance metrics
  required_customer_tables <- fn_has_any_of(tables, c("customers", "df_profile_by_customer", "df_profile_by_customer_dna"))
  required_campaign_tables <- fn_has_any_of(tables, c("campaigns", "campaign_performance", "marketing_reach"))
  
  # Approach 1: Check if we have customer location data
  if (required_customer_tables$has_any) {
    customer_table <- required_customer_tables$found_table
    location_columns <- c("state", "region", "country", "zip", "city", "location")
    has_location <- fn_check_columns_exist(conn, customer_table, location_columns, any_of = TRUE)
    
    if (has_location) {
      return(TRUE)
    }
  }
  
  # Approach 2: Check if we have campaign performance data with impressions/reach
  if (required_campaign_tables$has_any) {
    campaign_table <- required_campaign_tables$found_table
    reach_columns <- c("impressions", "reach", "audience_size", "views", "exposure")
    has_reach <- fn_check_columns_exist(conn, campaign_table, reach_columns, any_of = TRUE)
    
    if (has_reach) {
      return(TRUE)
    }
  }
  
  return(FALSE)
}

# ===== Utility functions =====

#' Check if a column exists in a table
#'
#' @param conn A DBI connection
#' @param table_name The table to check
#' @param column_name The column to check for
#' @return Boolean indicating if the column exists
#' @noexport
check_column_exists <- function(conn, table_name, column_name) {
  tryCatch({
    # Get column names from the table
    query <- paste0("SELECT * FROM ", table_name, " LIMIT 0")
    result <- DBI::dbGetQuery(conn, query)
    
    # Check if the column exists
    return(column_name %in% colnames(result))
  }, error = function(e) {
    message("Error checking column '", column_name, "' in table '", table_name, "': ", e$message)
    return(FALSE)
  })
}

#' Check if multiple columns exist in a table
#'
#' @param conn A DBI connection
#' @param table_name The table to check
#' @param column_names Vector of column names to check for
#' @param any_of If TRUE, returns TRUE if any column exists; if FALSE, all must exist
#' @return Boolean indicating if the columns exist according to the any_of parameter
#' @noexport
check_columns_exist <- function(conn, table_name, column_names, any_of = FALSE) {
  tryCatch({
    # Get column names from the table
    query <- paste0("SELECT * FROM ", table_name, " LIMIT 0")
    result <- DBI::dbGetQuery(conn, query)
    table_columns <- colnames(result)
    
    # Check if the specified columns exist
    columns_exist <- column_names %in% table_columns
    
    # Return based on any_of parameter
    if (any_of) {
      return(any(columns_exist))
    } else {
      return(all(columns_exist))
    }
  }, error = function(e) {
    message("Error checking columns in table '", table_name, "': ", e$message)
    return(FALSE)
  })
}

#' Get the first column that exists in a table from a list of possibilities
#'
#' @param conn A DBI connection
#' @param table_name The table to check
#' @param column_names Vector of column names to check
#' @return The name of the first existing column, or NULL if none exist
#' @noexport
get_first_existing_column <- function(conn, table_name, column_names) {
  tryCatch({
    # Get column names from the table
    query <- paste0("SELECT * FROM ", table_name, " LIMIT 0")
    result <- DBI::dbGetQuery(conn, query)
    table_columns <- colnames(result)
    
    # Find the first matching column
    for (col in column_names) {
      if (col %in% table_columns) {
        return(col)
      }
    }
    
    return(NULL)
  }, error = function(e) {
    message("Error getting columns from table '", table_name, "': ", e$message)
    return(NULL)
  })
}

#' Check if any of the specified tables exist
#'
#' @param available_tables List of all tables in the database
#' @param required_tables Vector of table names to check for
#' @return List with has_any (boolean) and found_table (string) elements
#' @noexport
has_any_of <- function(available_tables, required_tables) {
  for (table in required_tables) {
    if (table %in% available_tables) {
      return(list(has_any = TRUE, found_table = table))
    }
  }
  return(list(has_any = FALSE, found_table = NULL))
}

#' Execute a SQL query safely
#'
#' @param conn A DBI connection
#' @param query SQL query to execute
#' @return Query result or NULL if the query fails
#' @noexport
safe_query <- function(conn, query) {
  tryCatch({
    DBI::dbGetQuery(conn, query)
  }, error = function(e) {
    message("Error executing query: ", e$message)
    return(NULL)
  })
}

#' Check if Conversion Rate can be calculated
#'
#' @param conn A DBI connection to the app database
#' @param tables List of available tables
#' @return Boolean indicating if conversion rate can be calculated
#' @implements MP45 Automatic Data Availability Detection
#' @implements MP02 Default Deny
#' @noexport
check_conversion_rate_availability <- function(conn, tables) {
  # Conversion rate requires impression/view data and sales/conversion data
  # Or explicit conversion rate data in customer profile
  
  # First check: if we have direct conversion rate data
  if ("df_profile_by_customer_dna" %in% tables) {
    conversion_columns <- c("conversion_rate", "cvr", "conversion_ratio", "conv_rate")
    has_conversion_column <- fn_check_columns_exist(conn, "df_profile_by_customer_dna", conversion_columns, any_of = TRUE)
    if (has_conversion_column) {
      return(TRUE)
    }
  }
  
  # Second check: if we have analytics data with impressions and conversions
  analytics_tables <- fn_has_any_of(tables, c("marketing_analytics", "campaign_performance", "ad_performance", "campaign_metrics"))
  
  if (analytics_tables$has_any) {
    analytics_table <- analytics_tables$found_table
    
    # Check for both impression/view columns and conversion columns
    impression_columns <- c("impressions", "views", "audience_reached", "reach", "ad_views")
    conversion_columns <- c("conversions", "sales", "purchases", "transactions", "orders")
    
    has_impression_data <- fn_check_columns_exist(conn, analytics_table, impression_columns, any_of = TRUE)
    has_conversion_data <- fn_check_columns_exist(conn, analytics_table, conversion_columns, any_of = TRUE)
    
    if (has_impression_data && has_conversion_data) {
      # Check if we have enough data to calculate meaningful rates
      impression_col <- fn_get_first_existing_column(conn, analytics_table, impression_columns)
      conversion_col <- fn_get_first_existing_column(conn, analytics_table, conversion_columns)
      
      if (!is.null(impression_col) && !is.null(conversion_col)) {
        query <- paste0("SELECT SUM(", impression_col, ") as total_impressions, ",
                       "SUM(", conversion_col, ") as total_conversions ",
                       "FROM ", analytics_table)
        result <- safe_query(conn, query)
        
        return(!is.null(result) && 
               !is.na(result$total_impressions) && 
               !is.na(result$total_conversions) && 
               result$total_impressions > 0)
      }
    }
  }
  
  # Third check: if we have separate impression and conversion data in different tables
  impression_tables <- fn_has_any_of(tables, c("impressions", "views", "ad_views", "marketing_reach"))
  conversion_tables <- fn_has_any_of(tables, c("conversions", "sales", "orders", "transactions"))
  
  if (impression_tables$has_any && conversion_tables$has_any) {
    impression_table <- impression_tables$found_table
    conversion_table <- conversion_tables$found_table
    
    # Check if we can join these tables on common keys
    impression_id_columns <- c("campaign_id", "ad_id", "marketing_id", "date", "customer_id")
    conversion_id_columns <- c("campaign_id", "ad_id", "marketing_id", "date", "customer_id")
    
    has_impression_id <- fn_check_columns_exist(conn, impression_table, impression_id_columns, any_of = TRUE)
    has_conversion_id <- fn_check_columns_exist(conn, conversion_table, conversion_id_columns, any_of = TRUE)
    
    # Check if both tables have at least one common column that can be used for joining
    if (has_impression_id && has_conversion_id) {
      # Find a common column for joining
      common_column <- NULL
      for (col in impression_id_columns) {
        if (fn_check_column_exists(conn, impression_table, col) && 
            fn_check_column_exists(conn, conversion_table, col)) {
          common_column <- col
          break
        }
      }
      
      if (!is.null(common_column)) {
        # Check if there's enough data to calculate meaningful conversion rates
        impression_query <- paste0("SELECT COUNT(*) as count FROM ", impression_table)
        conversion_query <- paste0("SELECT COUNT(*) as count FROM ", conversion_table)
        
        impression_result <- fn_safe_query(conn, impression_query)
        conversion_result <- fn_safe_query(conn, conversion_query)
        
        return(!is.null(impression_result) && !is.null(conversion_result) && 
               impression_result$count > 0 && conversion_result$count > 0)
      }
    }
  }
  
  return(FALSE)
}