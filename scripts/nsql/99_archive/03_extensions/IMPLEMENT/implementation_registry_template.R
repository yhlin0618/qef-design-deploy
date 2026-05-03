# Implementation Phrase Registry Generator
# This script creates a template for the implementation phrase registry in Excel format

# Required libraries
library(openxlsx)

# Create a new workbook
wb <- createWorkbook()

# Add a worksheet for implementation phrases
addWorksheet(wb, "Implementation Phrases")

# Define the column headers
headers <- c(
  "Phrase ID", 
  "Directive", 
  "Context",
  "Implementation Code",
  "Description",
  "Parameters",
  "Added Date",
  "Added By",
  "Status"
)

# Write the headers
writeData(wb, "Implementation Phrases", headers, startRow = 1)

# Add some example phrases
example_phrases <- data.frame(
  "Phrase ID" = c("INI001", "INI002", "INI003", "DB001", "DB002", "IMP001"),
  "Directive" = c("INITIALIZE", "INITIALIZE", "INITIALIZE", "CONNECT", "CONNECT", "IMPLEMENT"),
  "Context" = c("UPDATE_MODE", "APP_MODE", "DATABASE ONLY", "APP_DATA", "RAW_DATA", "TABLE CREATION"),
  "Implementation Code" = c(
    "source(file.path('update_scripts', 'global_scripts', '00_principles', 'sc_initialization_update_mode.R'))",
    "source(file.path('update_scripts', 'global_scripts', '00_principles', 'sc_initialization_app_mode.R'))",
    "source(file.path('update_scripts', 'global_scripts', '00_principles', 'sc_init_db_only.R'))",
    "app_data <- dbConnect_from_list('app_data')",
    "raw_data <- dbConnect_from_list('raw_data')",
    "create_table_query <- generate_create_table_query(\n  con = $connection,\n  or_replace = TRUE,\n  target_table = '$table_name',\n  source_table = NULL,\n  column_defs = $column_definitions,\n  primary_key = $primary_key,\n  indexes = $indexes\n)\ndbExecute($connection, create_table_query)"
  ),
  "Description" = c(
    "Initialize system in update mode for script development",
    "Initialize system in app mode for Shiny application",
    "Initialize database connections only",
    "Connect to the application database",
    "Connect to the raw data database",
    "Generate and execute a CREATE TABLE query"
  ),
  "Parameters" = c(
    "None",
    "None",
    "None",
    "None",
    "None",
    "$connection, $table_name, $column_definitions, $primary_key, $indexes"
  ),
  "Added Date" = rep(format(Sys.Date(), "%Y-%m-%d"), 6),
  "Added By" = rep("Claude", 6),
  "Status" = rep("Active", 6)
)

# Write the example phrases
writeData(wb, "Implementation Phrases", example_phrases, startRow = 2)

# Add a worksheet for implementation examples
addWorksheet(wb, "Implementation Examples")

# Define the column headers for examples
example_headers <- c(
  "Example ID",
  "Entity ID",
  "Location",
  "Options",
  "Implementation Content",
  "Description",
  "Status"
)

# Write the example headers
writeData(wb, "Implementation Examples", example_headers, startRow = 1)

# Add some example implementations
example_implementations <- data.frame(
  "Example ID" = c("EX001"),
  "Entity ID" = c("D00_01_00"),
  "Location" = c("update_scripts"),
  "Options" = c("automated_execution=TRUE"),
  "Implementation Content" = c("INITIALIZE IN UPDATE_MODE\nCONNECT TO APP_DATA\n\nIMPLEMENT TABLE CREATION\n  $connection = app_data\n  $table_name = df_customer_profile\n  $column_definitions = list(...)\n  $primary_key = c(\"customer_id\", \"platform_id\")\n  $indexes = list(list(columns = \"platform_id\"))"),
  "Description" = c("Implementation of customer profile table creation"),
  "Status" = c("Implemented")
)

# Write the example implementations
writeData(wb, "Implementation Examples", example_implementations, startRow = 2)

# Adjust column widths
setColWidths(wb, "Implementation Phrases", cols = 1:length(headers), widths = c(10, 15, 15, 50, 30, 30, 12, 12, 10))
setColWidths(wb, "Implementation Examples", cols = 1:length(example_headers), widths = c(10, 15, 15, 20, 50, 30, 15))

# Add text wrapping for the implementation code columns
style <- createStyle(wrapText = TRUE)
addStyle(wb, "Implementation Phrases", style, rows = 2:(nrow(example_phrases) + 1), cols = 4, gridExpand = TRUE)
addStyle(wb, "Implementation Examples", style, rows = 2:(nrow(example_implementations) + 1), cols = 5, gridExpand = TRUE)

# Save the workbook
registry_path <- file.path(getwd(), "scripts", "nsql", "extensions", "IMPLEMENT", "implementation_phrase_registry.xlsx")
saveWorkbook(wb, registry_path, overwrite = TRUE)

cat("Implementation phrase registry template created at:", registry_path, "\n")
cat("You can now open and modify this file to add more implementation phrases as needed.\n")