# DuckDB MAMBA Integration Patterns
# MAMBA Framework
# Date: 2025-08-28
# Purpose: MAMBA-specific DuckDB patterns and utilities

#' MAMBA ETL pipeline for JSON API data
#' 
#' @param con DuckDB connection
#' @param api_response API response data
#' @param target_table Target table name
#' @return Number of rows processed
#' @export
mamba_etl_json_pipeline <- function(con, api_response, target_table) {
  # Convert API response to data frame
  df <- jsonlite::fromJSON(api_response, flatten = TRUE)
  
  # Handle nested structures
  df_processed <- df %>%
    # Flatten lists to JSON strings
    mutate(across(where(is.list), 
                 ~map_chr(., jsonlite::toJSON, auto_unbox = TRUE))) %>%
    # Clean strings
    mutate(across(where(is.character), trimws)) %>%
    # Ensure proper types
    mutate(across(where(~all(grepl("^\\d+$", .), na.rm = TRUE)), as.integer))
  
  # Load into DuckDB
  dbWriteTable(con, target_table, df_processed, append = TRUE)
  
  return(nrow(df_processed))
}

#' MAMBA sales data aggregation pattern
#' 
#' @param con DuckDB connection
#' @param start_date Start date for aggregation
#' @param end_date End date for aggregation
#' @return Aggregated sales data
#' @export
mamba_aggregate_sales <- function(con, start_date, end_date) {
  query <- glue::glue("
    WITH daily_sales AS (
      SELECT 
        DATE_TRUNC('day', order_date) as date,
        platform_id,
        COUNT(DISTINCT order_id) as orders,
        COUNT(DISTINCT customer_id) as customers,
        SUM(amount) as revenue,
        AVG(amount) as avg_order_value
      FROM sales_orders
      WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY 1, 2
    ),
    platform_totals AS (
      SELECT 
        platform_id,
        SUM(revenue) as total_revenue,
        SUM(orders) as total_orders,
        COUNT(DISTINCT date) as active_days
      FROM daily_sales
      GROUP BY platform_id
    )
    SELECT 
      ds.*,
      pt.total_revenue,
      pt.total_orders,
      pt.active_days,
      ds.revenue / pt.total_revenue as daily_share
    FROM daily_sales ds
    JOIN platform_totals pt USING (platform_id)
    ORDER BY date DESC, revenue DESC
  ")
  
  return(dbGetQuery(con, query))
}

#' MAMBA customer segmentation pattern
#' 
#' @param con DuckDB connection
#' @param recency_days Recency threshold in days
#' @return Customer segments
#' @export
mamba_customer_segmentation <- function(con, recency_days = 90) {
  query <- glue::glue("
    WITH customer_metrics AS (
      SELECT 
        customer_id,
        COUNT(DISTINCT order_id) as frequency,
        SUM(amount) as monetary,
        DATEDIFF('day', MAX(order_date), CURRENT_DATE) as recency,
        MIN(order_date) as first_order,
        MAX(order_date) as last_order
      FROM sales_orders
      GROUP BY customer_id
    ),
    customer_segments AS (
      SELECT 
        *,
        CASE 
          WHEN recency <= {recency_days/3} THEN 'Active'
          WHEN recency <= {recency_days} THEN 'At Risk'
          ELSE 'Churned'
        END as recency_segment,
        NTILE(3) OVER (ORDER BY frequency DESC) as frequency_tier,
        NTILE(3) OVER (ORDER BY monetary DESC) as monetary_tier
      FROM customer_metrics
    )
    SELECT 
      recency_segment,
      frequency_tier,
      monetary_tier,
      COUNT(*) as customers,
      AVG(monetary) as avg_ltv,
      AVG(frequency) as avg_orders
    FROM customer_segments
    GROUP BY recency_segment, frequency_tier, monetary_tier
    ORDER BY recency_segment, frequency_tier, monetary_tier
  ")
  
  return(dbGetQuery(con, query))
}

#' MAMBA inventory optimization pattern
#' 
#' @param con DuckDB connection
#' @param lookback_days Days to look back for analysis
#' @return Inventory optimization recommendations
#' @export
mamba_inventory_optimization <- function(con, lookback_days = 30) {
  query <- glue::glue("
    WITH sales_velocity AS (
      SELECT 
        product_id,
        platform_id,
        COUNT(DISTINCT order_date) as sale_days,
        SUM(quantity) as units_sold,
        SUM(quantity) / {lookback_days} as daily_velocity,
        STDDEV(quantity) as demand_variance
      FROM sales_items
      WHERE order_date >= CURRENT_DATE - INTERVAL '{lookback_days} days'
      GROUP BY product_id, platform_id
    ),
    inventory_levels AS (
      SELECT 
        product_id,
        platform_id,
        current_stock,
        reorder_point,
        max_stock
      FROM inventory
    )
    SELECT 
      il.*,
      sv.daily_velocity,
      sv.demand_variance,
      il.current_stock / NULLIF(sv.daily_velocity, 0) as days_of_supply,
      CASE
        WHEN il.current_stock < il.reorder_point THEN 'Reorder Now'
        WHEN il.current_stock / NULLIF(sv.daily_velocity, 0) < 7 THEN 'Reorder Soon'
        WHEN il.current_stock > il.max_stock THEN 'Overstock'
        ELSE 'Optimal'
      END as action_needed,
      GREATEST(0, il.reorder_point + (sv.demand_variance * 2) - il.current_stock) as suggested_order_qty
    FROM inventory_levels il
    LEFT JOIN sales_velocity sv USING (product_id, platform_id)
    ORDER BY action_needed, days_of_supply
  ")
  
  return(dbGetQuery(con, query))
}

#' MAMBA cross-platform comparison
#' 
#' @param con DuckDB connection
#' @param metric Metric to compare (revenue, orders, customers)
#' @param period Period for comparison (day, week, month)
#' @return Cross-platform comparison data
#' @export
mamba_cross_platform_compare <- function(con, metric = "revenue", period = "month") {
  metric_expr <- switch(metric,
    "revenue" = "SUM(amount)",
    "orders" = "COUNT(DISTINCT order_id)",
    "customers" = "COUNT(DISTINCT customer_id)",
    "SUM(amount)"  # default
  )
  
  query <- glue::glue("
    WITH platform_metrics AS (
      SELECT 
        DATE_TRUNC('{period}', order_date) as period_date,
        platform_id,
        {metric_expr} as metric_value
      FROM sales_orders
      GROUP BY 1, 2
    ),
    platform_ranks AS (
      SELECT 
        *,
        RANK() OVER (PARTITION BY period_date ORDER BY metric_value DESC) as rank,
        metric_value / SUM(metric_value) OVER (PARTITION BY period_date) as share
      FROM platform_metrics
    )
    SELECT 
      period_date,
      platform_id,
      metric_value,
      rank,
      ROUND(share * 100, 2) as market_share_pct,
      LAG(metric_value) OVER (PARTITION BY platform_id ORDER BY period_date) as prev_value,
      ((metric_value / NULLIF(LAG(metric_value) OVER (PARTITION BY platform_id ORDER BY period_date), 0)) - 1) * 100 as growth_pct
    FROM platform_ranks
    ORDER BY period_date DESC, rank
  ")
  
  return(dbGetQuery(con, query))
}

#' MAMBA data quality checks
#' 
#' @param con DuckDB connection
#' @param table_name Table to check
#' @return Data quality report
#' @export
mamba_data_quality_check <- function(con, table_name) {
  # Get table columns
  columns <- dbGetQuery(con, sprintf("
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = '%s'
  ", table_name))
  
  quality_checks <- list()
  
  # Check for nulls in each column
  for (i in 1:nrow(columns)) {
    col <- columns$column_name[i]
    null_check <- dbGetQuery(con, sprintf("
      SELECT 
        '%s' as column_name,
        COUNT(*) as total_rows,
        SUM(CASE WHEN %s IS NULL THEN 1 ELSE 0 END) as null_count,
        ROUND(100.0 * SUM(CASE WHEN %s IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as null_pct
      FROM %s
    ", col, col, col, table_name))
    
    quality_checks[[col]] <- null_check
  }
  
  # Combine results
  quality_report <- do.call(rbind, quality_checks)
  
  # Add duplicate check
  dup_check <- dbGetQuery(con, sprintf("
    SELECT COUNT(*) as total_rows, COUNT(DISTINCT *) as unique_rows
    FROM %s
  ", table_name))
  
  quality_report$duplicates <- dup_check$total_rows - dup_check$unique_rows
  
  return(quality_report)
}

#' MAMBA platform configuration loader
#' 
#' @param con DuckDB connection
#' @return Platform configuration data
#' @export
mamba_load_platform_config <- function(con) {
  # Create platform configuration table if not exists
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS platform_config (
      platform_id VARCHAR PRIMARY KEY,
      platform_name VARCHAR,
      api_endpoint VARCHAR,
      auth_type VARCHAR,
      rate_limit INTEGER,
      config_json JSON,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  ")
  
  # Load default configurations
  default_configs <- data.frame(
    platform_id = c("AMZ", "EBY", "SHP", "LAZ"),
    platform_name = c("Amazon", "eBay", "Shopee", "Lazada"),
    api_endpoint = c(
      "https://sellingpartnerapi.amazon.com",
      "https://api.ebay.com",
      "https://partner.shopeemobile.com",
      "https://open.lazada.com"
    ),
    auth_type = c("OAuth2", "OAuth2", "HMAC", "OAuth2"),
    rate_limit = c(10, 5, 20, 10),
    config_json = c("{}", "{}", "{}", "{}"),
    stringsAsFactors = FALSE
  )
  
  # Insert or update configurations
  for (i in 1:nrow(default_configs)) {
    dbExecute(con, sprintf("
      INSERT INTO platform_config VALUES ('%s', '%s', '%s', '%s', %d, '%s', CURRENT_TIMESTAMP)
      ON CONFLICT (platform_id) DO UPDATE SET
        updated_at = CURRENT_TIMESTAMP
    ", default_configs$platform_id[i],
       default_configs$platform_name[i],
       default_configs$api_endpoint[i],
       default_configs$auth_type[i],
       default_configs$rate_limit[i],
       default_configs$config_json[i]))
  }
  
  # Return current configuration
  return(dbGetQuery(con, "SELECT * FROM platform_config"))
}

#' MAMBA batch processing coordinator
#' 
#' @param con DuckDB connection
#' @param tasks List of processing tasks
#' @param parallel Enable parallel processing
#' @return Processing results
#' @export
mamba_batch_processor <- function(con, tasks, parallel = TRUE) {
  if (parallel) {
    # Configure parallel execution
    cores <- parallel::detectCores() - 1
    dbExecute(con, sprintf("SET threads=%d", cores))
    dbExecute(con, "SET enable_parallel=true")
  }
  
  results <- list()
  
  for (i in seq_along(tasks)) {
    task <- tasks[[i]]
    
    tryCatch({
      # Execute task
      start_time <- Sys.time()
      
      if (task$type == "query") {
        result <- dbGetQuery(con, task$sql)
      } else if (task$type == "execute") {
        dbExecute(con, task$sql)
        result <- list(status = "success", rows_affected = dbGetRowsAffected(con))
      } else if (task$type == "function") {
        result <- do.call(task$fn, task$args)
      }
      
      end_time <- Sys.time()
      
      results[[i]] <- list(
        task_id = task$id,
        status = "success",
        result = result,
        execution_time = as.numeric(end_time - start_time, units = "secs")
      )
      
    }, error = function(e) {
      results[[i]] <- list(
        task_id = task$id,
        status = "error",
        error = e$message
      )
    })
  }
  
  return(results)
}