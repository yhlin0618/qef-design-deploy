# Implementation Phrase Registry Generator
# This script creates a comprehensive registry for all NSQL extension implementation phrases
# Following MP068: Language as Index Meta-Principle

# Required libraries
library(openxlsx)

# Create a new workbook
wb <- createWorkbook()

# ====================================================
# 1. Core Implementation Phrases
# ====================================================
addWorksheet(wb, "Core Implementation Phrases")

# Define the column headers
headers <- c(
  "Phrase ID", 
  "Directive", 
  "Context",
  "Implementation Code",
  "Description",
  "Parameters",
  "Extension",
  "Added Date",
  "Added By",
  "Status"
)

# Write the headers
writeData(wb, "Core Implementation Phrases", headers, startRow = 1)

# Add some example phrases
core_phrases <- data.frame(
  "Phrase ID" = c("INI001", "INI002", "INI003", "INI004", "DEI001", "DB001", "DB002", "IMP001"),
  "Directive" = c("INITIALIZE", "INITIALIZE", "INITIALIZE", "INITIALIZE_SYNTAX", "DEINITIALIZE_SYNTAX", "CONNECT", "CONNECT", "IMPLEMENT"),
  "Context" = c("UPDATE_MODE", "APP_MODE", "DATABASE ONLY", "", "", "APP_DATA", "RAW_DATA", "TABLE CREATION"),
  "Implementation Code" = c(
    "source(file.path('update_scripts', 'global_scripts', '00_principles', 'sc_initialization_update_mode.R'))",
    "source(file.path('update_scripts', 'global_scripts', '00_principles', 'sc_initialization_app_mode.R'))",
    "source(file.path('update_scripts', 'global_scripts', '00_principles', 'sc_init_db_only.R'))",
    "# Initialize required libraries and dependencies\ntryCatch({\n  # Load required libraries\n  suppressPackageStartupMessages({\n    $libraries\n  })\n  \n  # Set environment variables and parameters\n  $environment_setup\n  \n  message('Initialization completed successfully')\n}, error = function(e) {\n  message('Error during initialization: ', e$message)\n  return(FALSE)\n})",
    "# Clean up resources and connections\ntryCatch({\n  # Close open connections\n  $close_connections\n  \n  # Reset environment variables\n  $reset_environment\n  \n  message('Deinitialization completed successfully')\n}, error = function(e) {\n  message('Error during deinitialization: ', e$message)\n  return(FALSE)\n})",
    "app_data <- dbConnect_from_list('app_data')",
    "raw_data <- dbConnect_from_list('raw_data')",
    "create_table_query <- generate_create_table_query(\n  con = $connection,\n  or_replace = TRUE,\n  target_table = '$table_name',\n  source_table = NULL,\n  column_defs = $column_definitions,\n  primary_key = $primary_key,\n  indexes = $indexes\n)\ndbExecute($connection, create_table_query)"
  ),
  "Description" = c(
    "Initialize system in update mode for script development",
    "Initialize system in app mode for Shiny application",
    "Initialize database connections only",
    "Generic initialization syntax template with error handling",
    "Generic deinitialization syntax template with error handling",
    "Connect to the application database",
    "Connect to the raw data database",
    "Generate and execute a CREATE TABLE query"
  ),
  "Parameters" = c(
    "None",
    "None",
    "None",
    "$libraries, $environment_setup",
    "$close_connections, $reset_environment",
    "None",
    "None",
    "$connection, $table_name, $column_definitions, $primary_key, $indexes"
  ),
  "Extension" = c(
    "Core", "Core", "Core", "Core", "Core", "Core", "Core", "Core"
  ),
  "Added Date" = rep(format(Sys.Date(), "%Y-%m-%d"), 8),
  "Added By" = rep("Claude", 8),
  "Status" = rep("Active", 8)
)

# Write the core phrases
writeData(wb, "Core Implementation Phrases", core_phrases, startRow = 2)

# ====================================================
# 2. Table Creation Phrases
# ====================================================
addWorksheet(wb, "Table Creation Phrases")

# Write the headers
writeData(wb, "Table Creation Phrases", headers, startRow = 1)

# Add table creation extension phrases
table_creation_phrases <- data.frame(
  "Phrase ID" = c("TBL001", "TBL002", "TBL003", "TBL004", "TBL005"),
  "Directive" = c("CREATE", "CREATE", "CREATE", "ADD", "ALTER"),
  "Context" = c("TABLE", "TEMPORARY TABLE", "INDEX", "COLUMN", "TABLE"),
  "Implementation Code" = c(
    "create_table_query <- paste0('CREATE TABLE ', $table_name, ' (\\n  ', paste($column_definitions, collapse=',\\n  '), '\\n);')\ndbExecute($connection, create_table_query)",
    "create_temp_table_query <- paste0('CREATE TEMPORARY TABLE ', $table_name, ' (\\n  ', paste($column_definitions, collapse=',\\n  '), '\\n);')\ndbExecute($connection, create_temp_table_query)",
    "create_index_query <- paste0('CREATE ', if($unique) 'UNIQUE ' else '', 'INDEX ', $index_name, ' ON ', $table_name, '(', paste($columns, collapse=', '), ');')\ndbExecute($connection, create_index_query)",
    "alter_table_query <- paste0('ALTER TABLE ', $table_name, ' ADD COLUMN ', $column_definition, ';')\ndbExecute($connection, alter_table_query)",
    "alter_table_query <- paste0('ALTER TABLE ', $table_name, ' ', $alteration, ';')\ndbExecute($connection, alter_table_query)"
  ),
  "Description" = c(
    "Creates a new table with the specified columns",
    "Creates a temporary table that exists only for the current session",
    "Creates an index on specified columns",
    "Adds a new column to an existing table",
    "Alters an existing table structure"
  ),
  "Parameters" = c(
    "$connection, $table_name, $column_definitions",
    "$connection, $table_name, $column_definitions",
    "$connection, $table_name, $columns, $index_name, $unique",
    "$connection, $table_name, $column_definition",
    "$connection, $table_name, $alteration"
  ),
  "Extension" = c(
    "Table Creation", "Table Creation", "Table Creation", "Table Creation", "Table Creation"
  ),
  "Added Date" = rep(format(Sys.Date(), "%Y-%m-%d"), 5),
  "Added By" = rep("Claude", 5),
  "Status" = rep("Active", 5)
)

# Write the table creation phrases
writeData(wb, "Table Creation Phrases", table_creation_phrases, startRow = 2)

# ====================================================
# 3. Database Documentation Phrases
# ====================================================
addWorksheet(wb, "DB Documentation")

# Write the headers
writeData(wb, "DB Documentation", headers, startRow = 1)

# Add database documentation extension phrases
db_doc_phrases <- data.frame(
  "Phrase ID" = c("DOC001", "DOC002", "DOC003", "DOC004"),
  "Directive" = c("DOCUMENT", "DOCUMENT", "DOCUMENT", "GENERATE"),
  "Context" = c("TABLE", "DATABASE", "SCHEMA", "ER DIAGRAM"),
  "Implementation Code" = c(
    "table_info <- dbGetQuery($connection, paste0(\"PRAGMA table_info('\", $table_name, \"');\"))\ntable_doc <- data.frame(\n  Column = table_info$name,\n  Type = table_info$type,\n  NotNull = ifelse(table_info$notnull == 1, 'Yes', 'No'),\n  PrimaryKey = ifelse(table_info$pk > 0, 'Yes', 'No')\n)\nwrite.csv(table_doc, file.path($output_dir, paste0($table_name, '_documentation.csv')))",
    "tables <- dbListTables($connection)\ntable_docs <- lapply(tables, function(table) {\n  table_info <- dbGetQuery($connection, paste0(\"PRAGMA table_info('\", table, \"');\"))\n  data.frame(\n    Table = table,\n    Column = table_info$name,\n    Type = table_info$type,\n    NotNull = ifelse(table_info$notnull == 1, 'Yes', 'No'),\n    PrimaryKey = ifelse(table_info$pk > 0, 'Yes', 'No')\n  )\n})\nfull_doc <- do.call(rbind, table_docs)\nwrite.csv(full_doc, file.path($output_dir, 'database_documentation.csv'))",
    "schemas <- dbGetQuery($connection, \"SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'\")\nschema_doc <- data.frame(\n  Schema = schemas$name,\n  TableCount = sapply(schemas$name, function(schema) {\n    length(dbListTables($connection, schema))\n  })\n)\nwrite.csv(schema_doc, file.path($output_dir, 'schema_documentation.csv'))",
    "tables <- dbListTables($connection)\nrelations <- lapply(tables, function(table) {\n  fks <- dbGetQuery($connection, paste0(\"PRAGMA foreign_key_list('\", table, \"');\"))\n  if(nrow(fks) > 0) {\n    data.frame(\n      FromTable = table,\n      FromColumn = fks$from,\n      ToTable = fks$table,\n      ToColumn = fks$to\n    )\n  }\n})\nrelations <- do.call(rbind, relations[!sapply(relations, is.null)])\nwriteLines(paste0(\n  \"digraph ER {\\n\",\n  \"  node [shape=box];\\n\",\n  paste(\"  \\\"\", tables, \"\\\";\", sep = \"\", collapse = \"\\n\"),\n  \"\\n\",\n  paste(\"  \\\"\", relations$FromTable, \"\\\" -> \\\"\", relations$ToTable, \"\\\" [label=\\\"\", relations$FromColumn, \" -> \", relations$ToColumn, \"\\\"];\", sep = \"\", collapse = \"\\n\"),\n  \"\\n}\"\n), file.path($output_dir, 'er_diagram.dot'))"
  ),
  "Description" = c(
    "Documents a single table's structure",
    "Documents the entire database structure",
    "Documents all schemas in the database",
    "Generates an Entity-Relationship diagram in DOT format"
  ),
  "Parameters" = c(
    "$connection, $table_name, $output_dir",
    "$connection, $output_dir",
    "$connection, $output_dir",
    "$connection, $output_dir"
  ),
  "Extension" = c(
    "Database Documentation", "Database Documentation", "Database Documentation", "Database Documentation"
  ),
  "Added Date" = rep(format(Sys.Date(), "%Y-%m-%d"), 4),
  "Added By" = rep("Claude", 4),
  "Status" = rep("Active", 4)
)

# Write the database documentation phrases
writeData(wb, "DB Documentation", db_doc_phrases, startRow = 2)

# ====================================================
# 4. Graph Representation Phrases
# ====================================================
addWorksheet(wb, "Graph Representation")

# Write the headers
writeData(wb, "Graph Representation", headers, startRow = 1)

# Add graph representation extension phrases
graph_phrases <- data.frame(
  "Phrase ID" = c("GRP001", "GRP002", "GRP003"),
  "Directive" = c("CREATE", "ANALYZE", "VISUALIZE"),
  "Context" = c("GRAPH", "GRAPH METRICS", "GRAPH"),
  "Implementation Code" = c(
    "# Create a graph from nodes and edges\nedges_df <- data.frame(\n  from = $from_nodes,\n  to = $to_nodes,\n  weight = $weights\n)\nnodes_df <- data.frame(\n  id = unique(c($from_nodes, $to_nodes)),\n  label = $labels\n)\n\n# Create igraph object\ngraph <- igraph::graph_from_data_frame(edges_df, directed = $directed, vertices = nodes_df)\nsaveRDS(graph, file.path($output_dir, paste0($graph_name, '.rds')))",
    "# Analyze graph metrics\ngraph <- readRDS(file.path($input_dir, paste0($graph_name, '.rds')))\n\nmetrics <- data.frame(\n  node = igraph::V(graph)$name,\n  degree = igraph::degree(graph),\n  betweenness = igraph::betweenness(graph),\n  closeness = igraph::closeness(graph),\n  eigenvector = igraph::eigen_centrality(graph)$vector\n)\n\nwrite.csv(metrics, file.path($output_dir, paste0($graph_name, '_metrics.csv')))",
    "# Visualize graph\ngraph <- readRDS(file.path($input_dir, paste0($graph_name, '.rds')))\n\npng(file.path($output_dir, paste0($graph_name, '.png')), width = 1200, height = 1200, res = 150)\nigraph::plot.igraph(graph, \n  layout = igraph::layout_with_fr(graph),\n  vertex.size = 8,\n  vertex.label = igraph::V(graph)$label,\n  edge.arrow.size = 0.5,\n  vertex.label.cex = 0.8\n)\ndev.off()"
  ),
  "Description" = c(
    "Creates a graph structure from nodes and edges",
    "Analyzes graph metrics including centrality measures",
    "Visualizes a graph structure as an image"
  ),
  "Parameters" = c(
    "$from_nodes, $to_nodes, $weights, $labels, $directed, $output_dir, $graph_name",
    "$input_dir, $graph_name, $output_dir",
    "$input_dir, $graph_name, $output_dir"
  ),
  "Extension" = c(
    "Graph Representation", "Graph Representation", "Graph Representation"
  ),
  "Added Date" = rep(format(Sys.Date(), "%Y-%m-%d"), 3),
  "Added By" = rep("Claude", 3),
  "Status" = rep("Active", 3)
)

# Write the graph representation phrases
writeData(wb, "Graph Representation", graph_phrases, startRow = 2)

# ====================================================
# 5. Specialized Implementation Phrases
# ====================================================
addWorksheet(wb, "Specialized Phrases")

# Write the headers
writeData(wb, "Specialized Phrases", headers, startRow = 1)

# Add specialized implementation extension phrases
specialized_phrases <- data.frame(
  "Phrase ID" = c("SPC001", "SPC002", "SPC003", "SPC004"),
  "Directive" = c("TRANSFORM", "ANALYZE", "EXPORT", "VALIDATE"),
  "Context" = c("DATA", "RFM", "DATA", "SCHEMA"),
  "Implementation Code" = c(
    "# Transform data according to transformation rules\ntransformed_data <- $source_data %>%\n  dplyr::mutate_at($columns, $transform_function) %>%\n  dplyr::filter($filter_condition)\n\nif ($save_result) {\n  saveRDS(transformed_data, file.path($output_dir, paste0($output_name, '.rds')))\n}\n\ntransformed_data",
    "# Analyze RFM (Recency, Frequency, Monetary) metrics\nrfm_data <- $transaction_data %>%\n  dplyr::group_by($customer_id_col) %>%\n  dplyr::summarize(\n    recency = as.numeric(difftime(Sys.Date(), max($date_col), units = 'days')),\n    frequency = n(),\n    monetary = mean($amount_col)\n  ) %>%\n  dplyr::mutate(\n    r_score = ntile(recency, 5),\n    f_score = ntile(frequency, 5),\n    m_score = ntile(monetary, 5),\n    rfm_score = paste0(r_score, f_score, m_score)\n  )\n\nif ($save_result) {\n  saveRDS(rfm_data, file.path($output_dir, paste0($output_name, '.rds')))\n}\n\nrfm_data",
    "# Export data to various formats\ndata <- $source_data\n\nswitch($format,\n  csv = write.csv(data, file.path($output_dir, paste0($output_name, '.csv')), row.names = FALSE),\n  excel = writexl::write_xlsx(data, file.path($output_dir, paste0($output_name, '.xlsx'))),\n  json = jsonlite::write_json(data, file.path($output_dir, paste0($output_name, '.json'))),\n  parquet = arrow::write_parquet(data, file.path($output_dir, paste0($output_name, '.parquet'))),\n  feather = arrow::write_feather(data, file.path($output_dir, paste0($output_name, '.feather')))\n)",
    "# Validate data schema against expected schema\nactual_schema <- sapply($data, class)\nexpected_schema <- $expected_types\n\nmismatches <- which(actual_schema != expected_schema)\nif (length(mismatches) > 0) {\n  warning(paste0(\n    'Schema validation failed for columns: ',\n    paste(names(mismatches), collapse = ', '),\n    '. Expected: ',\n    paste(paste(names(mismatches), expected_schema[mismatches], sep = ': '), collapse = ', '),\n    '. Got: ',\n    paste(paste(names(mismatches), actual_schema[mismatches], sep = ': '), collapse = ', ')\n  ))\n  FALSE\n} else {\n  TRUE\n}"
  ),
  "Description" = c(
    "Transforms data according to specified rules",
    "Analyzes RFM (Recency, Frequency, Monetary) metrics",
    "Exports data to various file formats",
    "Validates data schema against expected types"
  ),
  "Parameters" = c(
    "$source_data, $columns, $transform_function, $filter_condition, $save_result, $output_dir, $output_name",
    "$transaction_data, $customer_id_col, $date_col, $amount_col, $save_result, $output_dir, $output_name",
    "$source_data, $format, $output_dir, $output_name",
    "$data, $expected_types"
  ),
  "Extension" = c(
    "Specialized", "Specialized", "Specialized", "Specialized"
  ),
  "Added Date" = rep(format(Sys.Date(), "%Y-%m-%d"), 4),
  "Added By" = rep("Claude", 4),
  "Status" = rep("Active", 4)
)

# Write the specialized implementation phrases
writeData(wb, "Specialized Phrases", specialized_phrases, startRow = 2)

# ====================================================
# 6. Implementation Examples
# ====================================================
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
  "Example ID" = c("EX001", "EX002", "EX003"),
  "Entity ID" = c("D00_01_00", "D00_02_P00", "S01_01_00"),
  "Location" = c("update_scripts", "update_scripts", "update_scripts"),
  "Options" = c("automated_execution=TRUE", "automated_execution=TRUE", "automated_execution=FALSE"),
  "Implementation Content" = c(
    "INITIALIZE IN UPDATE_MODE\nCONNECT TO APP_DATA\n\nIMPLEMENT TABLE CREATION\n  $connection = app_data\n  $table_name = df_customer_profile\n  $column_definitions = list(...)\n  $primary_key = c(\"customer_id\", \"platform_id\")\n  $indexes = list(list(columns = \"platform_id\"))",
    "INITIALIZE IN UPDATE_MODE\nCONNECT TO APP_DATA\n\nIMPLEMENT TABLE CREATION\n  $connection = app_data\n  $table_name = df_dna_by_customer\n  $column_definitions = list(...)\n  $primary_key = c(\"customer_id\", \"platform_id\")\n  $indexes = list(list(columns = \"platform_id\"))",
    "INITIALIZE_SYNTAX\n  $libraries = library(dplyr)\n  library(tidyr)\n  library(DBI)\n  library(duckdb)\n  $environment_setup = options(stringsAsFactors = FALSE)\n  options(scipen = 999)\n  DATA_PATH <- file.path(\"data\", \"processed\")\n\n# Data processing code here...\n\nDEINITIALIZE_SYNTAX\n  $close_connections = if(exists(\"con\") && inherits(con, \"DBIConnection\")) dbDisconnect(con)\n  $reset_environment = rm(list = setdiff(ls(), c(\"results\")))"
  ),
  "Description" = c(
    "Implementation of customer profile table creation",
    "Implementation of DNA by customer table creation",
    "Example of initialization and deinitialization syntax templates"
  ),
  "Status" = c("Implemented", "Implemented", "Example")
)

# Write the example implementations
writeData(wb, "Implementation Examples", example_implementations, startRow = 2)

# ====================================================
# 7. Format Workbook
# ====================================================
# Set column widths for each sheet individually
# Core Implementation Phrases
setColWidths(wb, "Core Implementation Phrases", cols = 1:length(headers), 
             widths = c(10, 15, 15, 50, 30, 30, 15, 12, 12, 10))
style <- createStyle(wrapText = TRUE)
addStyle(wb, "Core Implementation Phrases", style, rows = 2:(nrow(core_phrases) + 1), 
         cols = 4, gridExpand = TRUE)

# Table Creation Phrases
setColWidths(wb, "Table Creation Phrases", cols = 1:length(headers), 
             widths = c(10, 15, 15, 50, 30, 30, 15, 12, 12, 10))
addStyle(wb, "Table Creation Phrases", style, rows = 2:(nrow(table_creation_phrases) + 1), 
         cols = 4, gridExpand = TRUE)

# DB Documentation
setColWidths(wb, "DB Documentation", cols = 1:length(headers), 
             widths = c(10, 15, 15, 50, 30, 30, 15, 12, 12, 10))
addStyle(wb, "DB Documentation", style, rows = 2:(nrow(db_doc_phrases) + 1), 
         cols = 4, gridExpand = TRUE)

# Graph Representation
setColWidths(wb, "Graph Representation", cols = 1:length(headers), 
             widths = c(10, 15, 15, 50, 30, 30, 15, 12, 12, 10))
addStyle(wb, "Graph Representation", style, rows = 2:(nrow(graph_phrases) + 1), 
         cols = 4, gridExpand = TRUE)

# Specialized Phrases
setColWidths(wb, "Specialized Phrases", cols = 1:length(headers), 
             widths = c(10, 15, 15, 50, 30, 30, 15, 12, 12, 10))
addStyle(wb, "Specialized Phrases", style, rows = 2:(nrow(specialized_phrases) + 1), 
         cols = 4, gridExpand = TRUE)

# Implementation Examples
setColWidths(wb, "Implementation Examples", cols = 1:length(example_headers), 
             widths = c(10, 15, 15, 20, 50, 30, 15))
addStyle(wb, "Implementation Examples", style, rows = 2:(nrow(example_implementations) + 1), 
         cols = 5, gridExpand = TRUE)

# Add a Introduction sheet at the beginning
addWorksheet(wb, "Introduction", gridLines = TRUE)
intro_text <- paste("# NSQL Implementation Phrase Registry\n\n",
                   "This registry catalogs all implementation phrases used across NSQL extensions.\n\n",
                   "## What is a Phrase?\n\n",
                   "Implementation phrases are standardized linguistic patterns that map to code implementations.\n",
                   "They serve as an index to functionality while remaining human-readable.\n\n",
                   "## How to Use This Registry\n\n",
                   "1. Browse sheets for different extension domains\n",
                   "2. Reference phrases in your NSQL implementation directives\n",
                   "3. Use the phrase ID to uniquely identify a phrase\n",
                   "4. Use parameters to customize the implementation\n\n",
                   "## Meta-Principle\n\n",
                   "This registry implements the MP068: Language as Index Meta-Principle,\n",
                   "which recognizes that language constructs can serve as powerful indexing mechanisms\n",
                   "for knowledge, processes, and implementations.\n\n",
                   "## Example\n\n",
                   "```\n",
                   "IMPLEMENT D00_01_00 IN update_scripts\n",
                   "=== Implementation Details ===\n",
                   "INITIALIZE IN UPDATE_MODE\n",
                   "CONNECT TO APP_DATA\n\n",
                   "IMPLEMENT TABLE CREATION\n",
                   "  $connection = app_data\n",
                   "  $table_name = df_customer_profile\n",
                   "  $column_definitions = list(...)\n",
                   "```")
writeData(wb, "Introduction", intro_text, startRow = 1)
setColWidths(wb, "Introduction", cols = 1, width = 100)
intro_style <- createStyle(wrapText = TRUE)
addStyle(wb, "Introduction", intro_style, rows = 1, cols = 1, gridExpand = TRUE)

# Save the workbook
registry_path <- file.path(getwd(), "scripts", "nsql", "extensions", "implementation_phrase_registry.xlsx")
saveWorkbook(wb, registry_path, overwrite = TRUE)

cat("Comprehensive implementation phrase registry created at:", registry_path, "\n")
cat("This registry implements MP068: Language as Index Meta-Principle\n")
cat("All NSQL extensions are now indexed in the registry.\n")