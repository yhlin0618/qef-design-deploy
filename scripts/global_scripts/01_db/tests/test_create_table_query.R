# Test script for fn_create_table_query.R

# Load required libraries
library(testthat)
library(DBI)
library(RSQLite)

# Source the function to test
source("../generate_create_table_query/fn_generate_create_table_query.R")

# Define test cases
test_that("generate_create_table_query creates correct SQL for existing tables", {
  # Create temporary connection
  con <- dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(dbDisconnect(con))
  
  # Create test table
  dbExecute(con, "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT NOT NULL, value REAL)")
  
  # Test cloning a table
  query <- generate_create_table_query(con, "test_table", "test_table_copy")
  
  # Verify query contains correct elements
  expect_true(grepl("CREATE TABLE test_table_copy", query))
  expect_true(grepl("id INTEGER PRIMARY KEY", query))
  expect_true(grepl("name TEXT NOT NULL", query))
  expect_true(grepl("value REAL", query))
  
  # Test creating the table with the generated query
  result <- tryCatch({
    dbExecute(con, query)
    TRUE
  }, error = function(e) {
    FALSE
  })
  
  expect_true(result, "Table creation query should execute without errors")
  
  # Verify the table was created
  tables <- dbListTables(con)
  expect_true("test_table_copy" %in% tables)
})

test_that("generate_create_table_query supports composite primary keys", {
  # Create temporary connection
  con <- dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(dbDisconnect(con))
  
  # Test with composite primary key
  query <- generate_create_table_query(
    con = con,
    source_table = NULL,
    target_table = "composite_key_table",
    column_defs = list(
      list(name = "customer_id", type = "INTEGER", not_null = TRUE),
      list(name = "product_id", type = "INTEGER", not_null = TRUE),
      list(name = "order_date", type = "DATE", not_null = TRUE),
      list(name = "quantity", type = "INTEGER", not_null = TRUE)
    ),
    primary_key = c("customer_id", "product_id", "order_date")
  )
  
  # Verify composite primary key
  expect_true(grepl("PRIMARY KEY \\(customer_id, product_id, order_date\\)", query))
  
  # Test creating the table with the generated query
  result <- tryCatch({
    dbExecute(con, query)
    TRUE
  }, error = function(e) {
    FALSE
  })
  
  expect_true(result, "Table creation with composite primary key should execute without errors")
})

test_that("generate_create_table_query supports foreign keys", {
  # Create temporary connection
  con <- dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(dbDisconnect(con))
  
  # Create reference tables
  dbExecute(con, "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
  dbExecute(con, "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT)")
  
  # Test with foreign keys
  query <- generate_create_table_query(
    con = con,
    source_table = NULL,
    target_table = "sales",
    column_defs = list(
      list(name = "id", type = "INTEGER", not_null = TRUE),
      list(name = "customer_id", type = "INTEGER", not_null = TRUE),
      list(name = "product_id", type = "INTEGER", not_null = TRUE),
      list(name = "quantity", type = "INTEGER", not_null = TRUE)
    ),
    primary_key = "id",
    foreign_keys = list(
      list(columns = "customer_id", ref_table = "customers", ref_columns = "id"),
      list(columns = "product_id", ref_table = "products", ref_columns = "id")
    )
  )
  
  # Verify foreign keys
  expect_true(grepl("FOREIGN KEY \\(customer_id\\) REFERENCES customers \\(id\\)", query))
  expect_true(grepl("FOREIGN KEY \\(product_id\\) REFERENCES products \\(id\\)", query))
  
  # Enable foreign keys in SQLite
  dbExecute(con, "PRAGMA foreign_keys = ON;")
  
  # Test creating the table with the generated query
  result <- tryCatch({
    dbExecute(con, query)
    TRUE
  }, error = function(e) {
    FALSE
  })
  
  expect_true(result, "Table creation with foreign keys should execute without errors")
})

test_that("generate_create_table_query supports custom indexes", {
  # Create temporary connection
  con <- dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(dbDisconnect(con))
  
  # Test with indexes
  query <- generate_create_table_query(
    con = con,
    source_table = NULL,
    target_table = "indexed_table",
    column_defs = list(
      list(name = "id", type = "INTEGER", not_null = TRUE),
      list(name = "name", type = "TEXT", not_null = TRUE),
      list(name = "category", type = "TEXT"),
      list(name = "date_added", type = "DATE")
    ),
    primary_key = "id",
    indexes = list(
      list(columns = "name", unique = TRUE),
      list(columns = c("category", "date_added"))
    )
  )
  
  # Verify indexes
  expect_true(grepl("CREATE UNIQUE INDEX IF NOT EXISTS idx_indexed_table_name ON indexed_table\\(name\\);", query))
  expect_true(grepl("CREATE INDEX IF NOT EXISTS idx_indexed_table_category_date_added ON indexed_table\\(category, date_added\\);", query))
  
  # Test creating the table with the generated query
  result <- tryCatch({
    dbExecute(con, query)
    TRUE
  }, error = function(e) {
    FALSE
  })
  
  expect_true(result, "Table creation with custom indexes should execute without errors")
  
  # Verify the indexes were created
  indexes <- dbGetQuery(con, "PRAGMA index_list('indexed_table');")
  expect_equal(nrow(indexes), 3)  # 2 custom indexes plus 1 for primary key
})

test_that("generate_create_table_query handles all constraints", {
  # Create temporary connection
  con <- dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(dbDisconnect(con))
  
  # Test with various constraints
  query <- generate_create_table_query(
    con = con,
    source_table = NULL,
    target_table = "constraints_table",
    column_defs = list(
      list(name = "id", type = "INTEGER", not_null = TRUE),
      list(name = "name", type = "TEXT", not_null = TRUE, unique = TRUE),
      list(name = "age", type = "INTEGER", check = "age > 0 AND age < 150"),
      list(name = "status", type = "TEXT", default = "'active'"),
      list(name = "created_at", type = "TIMESTAMP", default = "CURRENT_TIMESTAMP")
    ),
    primary_key = "id"
  )
  
  # Verify constraints
  expect_true(grepl("name TEXT NOT NULL UNIQUE", query))
  expect_true(grepl("age INTEGER CHECK \\(age > 0 AND age < 150\\)", query))
  expect_true(grepl("status TEXT DEFAULT 'active'", query))
  expect_true(grepl("created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP", query))
  
  # Test creating the table with the generated query
  result <- tryCatch({
    dbExecute(con, query)
    TRUE
  }, error = function(e) {
    FALSE
  })
  
  expect_true(result, "Table creation with all constraints should execute without errors")
})

# Run tests
test_results <- test_file("test_create_table_query.R", reporter = "summary")
print(test_results)