# Simplified Database Initialization
# This script initializes only the database functions
# without going through the full initialization process

# Set environment
OPERATION_MODE <- "UPDATE_MODE"
VERBOSE_INITIALIZATION <- FALSE

# Load utilities and initialize packages
source(file.path("update_scripts", "global_scripts", "04_utils", "fn_source_with_verbose.R"))
source_with_verbose(file.path("update_scripts", "global_scripts", "04_utils", "base", "fn_library2.R"))
source_with_verbose(file.path("update_scripts", "global_scripts", "04_utils", "fn_initialize_packages.R"))
initialize_packages(
  mode = OPERATION_MODE,
  verbose = VERBOSE_INITIALIZATION,
  force_update = FALSE
)

# Source database functions
db_utils_dir <- file.path("update_scripts", "global_scripts", "02_db_utils")

# Source database path function
source_file <- function(file_path) {
  if (file.exists(file_path)) {
    source(file_path)
    message("Loaded: ", file_path)
    return(TRUE)
  } else {
    message("File not found: ", file_path)
    return(FALSE)
  }
}

# Source database functions
source_file(file.path(db_utils_dir, "duckdb", "fn_get_default_db_paths.R"))
source_file(file.path(db_utils_dir, "fn_dbConnect_from_list.R"))

# Initialize database paths
if (exists("get_default_db_paths") && !exists("db_path_list")) {
  db_path_list <<- get_default_db_paths()
  message("Database paths initialized")
}

# Create alias if needed
if (exists("fn_dbConnect_from_list") && !exists("dbConnect_from_list")) {
  dbConnect_from_list <<- fn_dbConnect_from_list
  message("Created dbConnect_from_list alias")
}

# Simple check for database functions
if (exists("db_path_list") && exists("dbConnect_from_list")) {
  message("Database system initialized successfully")
} else {
  message("WARNING: Database initialization incomplete")
}