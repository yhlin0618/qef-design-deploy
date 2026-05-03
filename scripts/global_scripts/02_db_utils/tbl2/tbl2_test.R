library(testthat)
library(dplyr)

# Required for DuckDB connection
if (!requireNamespace("DBI", quietly = TRUE)) {
  message("DBI package not available. Installing...")
  install.packages("DBI")
}
if (!requireNamespace("duckdb", quietly = TRUE)) {
  message("duckdb package not available. Installing...")
  install.packages("duckdb")
}

# Source the tbl2 function
source(here("update_scripts", "global_scripts", "02_db_utils", "fn_tbl2.R"))

# Test mock database connection
test_mock_db <- function() {
  # Path to mock database
  mock_db_path <- file.path("scripts", "global_scripts", "30_global_data", "mock_data.duckdb")
  
  if (!file.exists(mock_db_path)) {
    message("Mock database not found at: ", mock_db_path)
    return(FALSE)
  }
  
  tryCatch({
    # Connect to mock database
    cat("Connecting to mock database...\n")
    conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = mock_db_path, read_only = TRUE)
    
    # List available tables
    tables <- DBI::dbListTables(conn)
    cat("Available tables in mock database:", paste(tables, collapse = ", "), "\n")
    
    # Test tbl2 with the database connection
    if ("customer_profile" %in% tables) {
      cat("\nQuerying customer_profile table...\n")
      
      # Using tbl2 with pipe operations
      customers <- tbl2(conn, "customer_profile") %>%
        dplyr::select(id, name, signup_date) %>%
        dplyr::collect()
      
      cat("Retrieved", nrow(customers), "customers\n")
      print(customers)
      
      # Test joining tables if orders exists
      if ("orders" %in% tables) {
        cat("\nJoining customer_profile with orders...\n")
        
        # Complex query with join
        joined_data <- tbl2(conn, "customer_profile") %>%
          dplyr::left_join(
            tbl2(conn, "orders") %>%
              dplyr::group_by(customer_id) %>%
              dplyr::summarize(
                order_count = n(),
                total_amount = sum(amount, na.rm = TRUE)
              ),
            by = c("id" = "customer_id")
          ) %>%
          dplyr::collect()
        
        cat("Joined data result:\n")
        print(joined_data)
      }
    }
    
    # Disconnect from database
    DBI::dbDisconnect(conn, shutdown = TRUE)
    cat("\nTest completed successfully!\n")
    return(TRUE)
    
  }, error = function(e) {
    cat("Error testing mock database:", e$message, "\n")
    return(FALSE)
  })
}


# Test function
test_tbl2 <- function() {
  # Test data
  test_df <- data.frame(id = 1:3, name = c("A", "B", "C"))
  test_list <- list(
    customer_profile = test_df,
    get_orders = function() data.frame(order_id = 101:103, customer_id = 1:3)
  )
  
  # Test 1: Direct data frame
  result1 <- tbl2(test_df)
  print("Test 1: Data frame input")
  print(result1)
  
  # Test 2: List with named element
  result2 <- tbl2(test_list, "customer_profile")
  print("Test 2: List with named element")
  print(result2)
  
  # Test 3: List with get_ function
  result3 <- tbl2(test_list, "orders")
  print("Test 3: List with get_ function")
  print(result3)
  
  # Test 4: Convert vector to data frame
  test_vec <- c(1, 2, 3, 4, 5)
  result4 <- tbl2(test_vec)
  print("Test 4: Vector input")
  print(result4)
  
  # Test 5: Function that returns a data frame
  test_fn <- function() data.frame(x = 1:5, y = 6:10)
  result5 <- tbl2(test_fn)
  print("Test 5: Function input")
  print(result5)
  
  # Test 6: Pipe compatibility
  result6 <- test_df %>%
    tbl2() %>%
    filter(id > 1) %>%
    mutate(new_col = paste0(name, id)) %>%
    arrange(desc(id))
  
  print("Test 6: Pipe compatibility")
  print(result6)
  
  # Return TRUE if all tests passed
  return(TRUE)
}

# Run the standard tests
cat("\n=== RUNNING STANDARD TESTS ===\n")
test_results <- test_tbl2()
cat("\nStandard tests completed:", test_results, "\n")

# Run the mock database test
cat("\n=== RUNNING MOCK DATABASE TEST ===\n")
mock_db_results <- test_mock_db()
cat("\nMock database test completed:", mock_db_results, "\n")

# Example of how this would be used in real code:
cat("\nExample usage in real code:\n")
cat("
# Connect to mock database
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = \"path/to/mock_data.duckdb\")

# Use with pipe
results <- tbl2(conn, \"customer_profile\") %>%
  filter(signup_date > as.Date(\"2022-01-01\")) %>%
  left_join(
    tbl2(conn, \"orders\") %>%
      group_by(customer_id) %>%
      summarize(total_orders = n(), total_spent = sum(amount)),
    by = \"customer_id\"
  ) %>%
  collect()

# Using with other data sources
customer_list <- list(active = data.frame(id = 1:3, status = \"active\"))
combined <- tbl2(customer_list, \"active\") %>%
  left_join(
    tbl2(conn, \"customer_profile\"),
    by = c(\"id\" = \"customer_id\")
  ) %>%
  collect()
")
