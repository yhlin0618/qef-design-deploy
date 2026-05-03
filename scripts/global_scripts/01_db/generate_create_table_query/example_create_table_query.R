#' @file example_create_table_query.R
#' @requires DBI
#' @requires RSQLite
#' @requires 01_db/generate_create_table/fn_generate_create_table_query.R
#' @requires 02_db_utils/fn_print_query.R
#' 
#' @title Example Script for Generate Create Table Query Function
#' @description Demonstrates how to use the generate_create_table_query function
#' with various options, showcasing all supported DuckDB constraints including PRIMARY KEY,
#' NOT NULL, UNIQUE, CHECK, DEFAULT, REFERENCES (foreign keys), and COLLATE constraints.
#'

# Load required libraries
library(DBI)
library(RSQLite)

# Source the function
source(file.path("update_scripts", "global_scripts", "00_principles", "sc_initialization_update_mode.R"))


# Create a temporary database connection
conn <- dbConnect(RSQLite::SQLite(), ":memory:")

# Example 1: Simple table with single primary key
query1 <- generate_create_table_query(
  con = conn,
  source_table = NULL,
  target_table = "products",
  schema = "public",
  column_defs = list(
    list(name = "id", type = "INTEGER", not_null = TRUE),
    list(name = "name", type = "TEXT", not_null = TRUE),
    list(name = "description", type = "TEXT"),
    list(name = "price", type = "DECIMAL(10,2)", not_null = TRUE),
    list(name = "created_at", type = "TIMESTAMP", default = "CURRENT_TIMESTAMP")
  ),
  primary_key = "id",
  execute = TRUE
)
print_query(query1, "SIMPLE TABLE WITH SINGLE PRIMARY KEY")

# Example 2: Table with composite primary key
query2 <- generate_create_table_query(
  con = conn,
  source_table = NULL,
  target_table = "order_products",
  column_defs = list(
    list(name = "order_id", type = "INTEGER", not_null = TRUE),
    list(name = "product_id", type = "INTEGER", not_null = TRUE),
    list(name = "quantity", type = "INTEGER", not_null = TRUE),
    list(name = "unit_price", type = "DECIMAL(10,2)", not_null = TRUE),
    list(name = "discount", type = "DECIMAL(5,2)", default = "0.00")
  ),
  primary_key = c("order_id", "product_id")
)
print_query(query2, "TABLE WITH COMPOSITE PRIMARY KEY")

# Execute query to create the table
dbExecute(conn, query2)

# Example 3: Table with foreign keys and constraints 
# Note: This example also demonstrates how redundant indexes are avoided
query3 <- generate_create_table_query(
  con = conn,
  source_table = NULL,
  target_table = "orders",
  column_defs = list(
    list(name = "id", type = "INTEGER", not_null = TRUE),
    list(name = "customer_id", type = "INTEGER", not_null = TRUE),
    list(name = "order_date", type = "DATE", not_null = TRUE),
    list(name = "status", type = "TEXT", not_null = TRUE, 
         check = "status IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled')"),
    list(name = "total_amount", type = "DECIMAL(12,2)", not_null = TRUE),
    list(name = "shipping_address", type = "TEXT"),
    list(name = "created_at", type = "TIMESTAMP", default = "CURRENT_TIMESTAMP")
  ),
  primary_key = "id",
  foreign_keys = list(
    list(
      columns = "customer_id", 
      ref_table = "customers", 
      ref_columns = "id",
      on_delete = "CASCADE"
    )
  ),
  indexes = list(
    # The following indexes will be created with table-specific names
    # to avoid conflicts when the same column names exist in different tables
    # Format: idx_{table}_{column(s)}_{table}
    list(columns = "customer_id"),    # Creates idx_orders_customer_id_orders
    list(columns = "order_date"),     # Creates idx_orders_order_date_orders
    list(columns = "status"),         # Creates idx_orders_status_orders
    list(columns = c("customer_id", "order_date"), unique = TRUE),  # Creates idx_orders_customer_id_order_date_orders
    
    # This index would be redundant with the primary key and will be skipped
    list(columns = "id")  # Will be detected as redundant and not created
  )
)
print_query(query3, "TABLE WITH FOREIGN KEYS AND CONSTRAINTS")

# Example 4: Clone structure from existing table and add new constraints
# First create a sample table
dbExecute(conn, "CREATE TABLE customers (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT NOT NULL UNIQUE,
  phone TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)")

# Clone and modify
query4 <- generate_create_table_query(
  con = conn,
  source_table = "customers",
  target_table = "customer_archive",
  primary_key = c("id", "email"), # Override with composite key
  indexes = list(
    list(columns = "name"),
    list(columns = "phone")
  )
)
print_query(query4, "CLONED TABLE WITH MODIFIED CONSTRAINTS")

# Example 5: Comprehensive example demonstrating all DuckDB constraints
query5 <- generate_create_table_query(
  con = conn,
  source_table = NULL,
  target_table = "customer_profiles",
  column_defs = list(
    # PRIMARY KEY constraint
    list(name = "id", type = "INTEGER", not_null = TRUE),
    
    # NOT NULL constraint
    list(name = "full_name", type = "VARCHAR(100)", not_null = TRUE),
    
    # UNIQUE constraint
    list(name = "email", type = "VARCHAR(255)", not_null = TRUE, unique = TRUE),
    
    # CHECK constraint with simple validation
    list(name = "age", type = "INTEGER", not_null = TRUE, 
         check = "age >= 18 AND age < 120"),
    
    # DEFAULT constraint with literal value
    list(name = "status", type = "VARCHAR(20)", not_null = TRUE,
         default = "'active'", 
         check = "status IN ('active', 'inactive', 'suspended', 'pending')"),
    
    # COLLATE constraint for case-insensitive text comparisons
    list(name = "username", type = "VARCHAR(50)", not_null = TRUE, 
         unique = TRUE, collate = "NOCASE"),
    
    # Combination of constraints 
    list(name = "country_code", type = "CHAR(2)", not_null = TRUE,
         collate = "NOCASE",
         check = "length(country_code) = 2"),
    
    # DEFAULT with function reference
    list(name = "created_at", type = "TIMESTAMP", not_null = TRUE,
         default = "CURRENT_TIMESTAMP"),
         
    # Regular columns with type specifications
    list(name = "credit_score", type = "INTEGER",
         check = "credit_score IS NULL OR (credit_score >= 300 AND credit_score <= 850)"),
         
    list(name = "annual_income", type = "DECIMAL(12,2)"),
    
    # Binary collation for case-sensitive comparison
    list(name = "password_hash", type = "VARCHAR(255)", not_null = TRUE,
         collate = "BINARY")
  ),
  primary_key = "id",
  foreign_keys = list(
    list(
      columns = "country_code", 
      ref_table = "countries", 
      ref_columns = "code",
      on_update = "CASCADE"
    )
  ),
  indexes = list(
    list(columns = "email"),
    list(columns = "username"),
    list(columns = c("country_code", "status")),
    list(columns = "created_at")
  )
)
print_query(query5, "ALL DUCKDB CONSTRAINTS EXAMPLE")

# Example 6: Generate SQL for existing table
#
conn <- dbConnect_from_list("app_data")
sql <- generate_create_table_query(con=conn, source_table="df_profile_by_customer")
print_query(sql, "DF_PROFILE_BY_CUSTOMER TABLE SCHEMA")

# Example 7: Generate R code to recreate an existing table
# Demonstrate that the function captures the actual connection name
my_connection <- conn  # Use a different variable name to demonstrate
r_code <- generate_create_table_query(con=my_connection, source_table="df_profile_by_customer", output_format="r")
cat(r_code)

# Example 8: Generate R code for table with all constraints
r_code_all_constraints <- generate_create_table_query(
  con = conn,
  source_table = NULL,
  target_table = "customer_profiles",
  column_defs = list(
    list(name = "id", type = "INTEGER", not_null = TRUE),
    list(name = "email", type = "VARCHAR(255)", not_null = TRUE, unique = TRUE),
    list(name = "username", type = "VARCHAR(50)", not_null = TRUE, collate = "NOCASE"),
    list(name = "age", type = "INTEGER", check = "age >= 18"),
    list(name = "status", type = "VARCHAR(20)", default = "'active'")
  ),
  primary_key = "id",
  output_format = "r"
)
cat("\n\n# R code for table with all constraints:\n")
cat(r_code_all_constraints)

# Example 9: Generate R code with explicit connection name
r_code_explicit_conn <- generate_create_table_query(
  con = conn,
  source_table = NULL,
  target_table = "example_table",
  column_defs = list(
    list(name = "id", type = "INTEGER", not_null = TRUE),
    list(name = "name", type = "TEXT", collate = "NOCASE")
  ),
  primary_key = "id",
  output_format = "r",
  connection_name = "duckdb_connection"
)
cat("\n\n# R code with explicit connection name:\n")
cat(r_code_explicit_conn)

# Example 10: Table with Generated Columns
query_generated <- generate_create_table_query(
  con = conn,
  source_table = NULL,
  target_table = "customer_profile",
  column_defs = list(
    list(name = "customer_id", type = "INTEGER", not_null = TRUE),
    list(name = "buyer_name", type = "VARCHAR"),
    list(name = "email", type = "VARCHAR"),
    list(name = "platform_id", type = "INTEGER"),
    # Generated column example - STORED type
    list(name = "display_name", 
         generated_as = "buyer_name || ' (' || email || ')'",
         generated_type = "STORED")
  ),
  primary_key = c("customer_id", "platform_id")
)
print_query(query_generated, "TABLE WITH GENERATED COLUMNS")

# Example 11: Table with Generated Columns and Missing Type (inferred by DuckDB)
query_generated_2 <- generate_create_table_query(
  con = conn,
  source_table = NULL,
  target_table = "metrics",
  column_defs = list(
    list(name = "id", type = "INTEGER", not_null = TRUE),
    list(name = "price", type = "DECIMAL(10,2)", not_null = TRUE),
    list(name = "quantity", type = "INTEGER", not_null = TRUE),
    # Generated column without type (inferred) - default VIRTUAL type
    list(name = "total_value", 
         generated_as = "price * quantity")
  ),
  primary_key = "id"
)
print_query(query_generated_2, "TABLE WITH INFERRED TYPE GENERATED COLUMNS")

# Example 12: Using CREATE OR REPLACE TABLE
query_or_replace <- generate_create_table_query(
  con = conn,
  source_table = NULL,
  target_table = "logs",
  column_defs = list(
    list(name = "id", type = "INTEGER", not_null = TRUE),
    list(name = "timestamp", type = "TIMESTAMP", not_null = TRUE, 
         default = "CURRENT_TIMESTAMP"),
    list(name = "level", type = "VARCHAR", not_null = TRUE),
    list(name = "message", type = "TEXT", not_null = TRUE)
  ),
  primary_key = "id",
  or_replace = TRUE
)
print_query(query_or_replace, "CREATE OR REPLACE TABLE")

# Example 13: Using CREATE TEMPORARY TABLE
query_temp <- generate_create_table_query(
  con = conn,
  source_table = NULL,
  target_table = "temp_results",
  column_defs = list(
    list(name = "id", type = "INTEGER", not_null = TRUE),
    list(name = "result", type = "FLOAT"),
    list(name = "created_at", type = "TIMESTAMP", default = "CURRENT_TIMESTAMP")
  ),
  primary_key = "id",
  temp = TRUE
)
print_query(query_temp, "CREATE TEMPORARY TABLE")

# Example 14: Using CREATE TABLE IF NOT EXISTS
query_if_not_exists <- generate_create_table_query(
  con = conn,
  source_table = NULL,
  target_table = "config",
  column_defs = list(
    list(name = "key", type = "VARCHAR", not_null = TRUE),
    list(name = "value", type = "VARCHAR"),
    list(name = "updated_at", type = "TIMESTAMP", default = "CURRENT_TIMESTAMP")
  ),
  primary_key = "key",
  if_not_exists = TRUE
)
print_query(query_if_not_exists, "CREATE TABLE IF NOT EXISTS")

# Example 15: Combining multiple modifiers
query_all_modifiers <- generate_create_table_query(
  con = conn,
  source_table = NULL,
  target_table = "session_cache",
  column_defs = list(
    list(name = "session_id", type = "VARCHAR", not_null = TRUE),
    list(name = "data", type = "TEXT"),
    list(name = "expiry", type = "TIMESTAMP", not_null = TRUE)
  ),
  primary_key = "session_id",
  or_replace = TRUE,
  temp = TRUE,
  if_not_exists = TRUE
)
print_query(query_all_modifiers, "CREATE OR REPLACE TEMPORARY TABLE IF NOT EXISTS")

# Close connection
dbDisconnect(conn)

cat("\nExamples completed successfully.\n")

