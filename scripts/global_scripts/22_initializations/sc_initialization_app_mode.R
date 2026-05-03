# APP_MODE Initialization ------------------------------------------------------
# Minimal, deterministic initialization for Shiny application runtime.
# UX_P001: Startup timing measurement
.startup_time <- proc.time()

# Ensure autoinit() has been executed -----------------------------------------
if (!exists(".InitEnv") || !is.environment(.InitEnv)) {
  stop("autoinit() must be executed before sourcing sc_initialization_app_mode.R")
}

# Abort if already initialized -------------------------------------------------
if (exists("INITIALIZATION_COMPLETED") && INITIALIZATION_COMPLETED) {
  message("Initialization already completed – skipping.")
  return(invisible(NULL))
}

# -----------------------------------------------------------------------------
# 1. Environment flags
# -----------------------------------------------------------------------------
if (!exists("OPERATION_MODE", envir = .GlobalEnv)) {
  stop("OPERATION_MODE is not defined. Please set OPERATION_MODE before sourcing this script.")
}
options(app.verbose = getOption("app.verbose", FALSE))


# -----------------------------------------------------------------------------
# 2. Packages
# -----------------------------------------------------------------------------
source(file.path(GLOBAL_DIR, "04_utils", "fn_initialize_packages.R"))
source(file.path(GLOBAL_DIR, "04_utils", "base", "fn_library2.R"))
initialize_packages(mode = OPERATION_MODE,
                    verbose = getOption("app.verbose"),
                    force_update = FALSE)

# -----------------------------------------------------------------------------
# 3. Load environment variables from .env file
# -----------------------------------------------------------------------------
# Check for .env file in APP_DIR (project root)
env_file <- file.path(APP_DIR, ".env")
if (file.exists(env_file)) {
  # Use dotenv package if available, otherwise use readRenviron
  if (requireNamespace("dotenv", quietly = TRUE)) {
    dotenv::load_dot_env(file = env_file)
    message("Environment variables loaded from .env using dotenv package")
  } else {
    readRenviron(env_file)
    message("Environment variables loaded from .env using readRenviron")
  }
  
  # Verify critical environment variables
  if (nzchar(Sys.getenv("OPENAI_API_KEY"))) {
    message("✓ OPENAI_API_KEY is set")
  } else {
    warning("⚠ OPENAI_API_KEY is not set - AI features will be disabled")
  }
} else {
  message("No .env file found in ", APP_DIR)
}

# -----------------------------------------------------------------------------
# 4. Global script loading (only stable public APIs)
# -----------------------------------------------------------------------------
source(file.path(GLOBAL_DIR, "04_utils", "fn_get_r_files_recursive.R"))
load_dirs <- c(
  "02_db_utils",
  "04_utils",
  "03_config",
  "08_ai",  # AI-related functions including fn_chat_api
  # 10_rshinyapp_components: loaded on-demand by union file (UX_P001)
  "11_rshinyapp_utils"
)

for (d in load_dirs) {
  dir_path <- file.path(.InitEnv$GLOBAL_DIR, d)
  if (!dir.exists(dir_path)) next
  r_files <- sort(get_r_files_recursive(dir_path))
  lapply(r_files, source, local = FALSE)
}

# -----------------------------------------------------------------------------
# 4. Load application configuration (YAML/Excel)
# -----------------------------------------------------------------------------
if (exists("load_app_configs")) {
  try(load_app_configs(), silent = TRUE)
}

# Safely extract app-level variables (may not exist during initial ETL setup)
if (exists("app_configs") && !is.null(app_configs$RAW_DATA_DIR)) {
  RAW_DATA_DIR <- app_configs$RAW_DATA_DIR
}
if (exists("df_product_line") && is.data.frame(df_product_line)) {
  vec_product_line_id <- df_product_line$product_line_id
  vec_product_line_id_noall <- df_product_line$product_line_id[-1]
}


# -----------------------------------------------------------------------------
# 5. Finalize
# -----------------------------------------------------------------------------
INITIALIZATION_COMPLETED <- TRUE
paths_msg <- NULL
if (exists("db_path_list")) {
  paths_msg <- names(db_path_list)
} else if (exists(".InitEnv") && exists("db_path_list", envir = .InitEnv)) {
  paths_msg <- names(get("db_path_list", envir = .InitEnv))
}

if (!is.null(paths_msg)) {
  message("APP_MODE initialization finished. Databases available: ", paste(paths_msg, collapse = ", "))
} else {
  message("APP_MODE initialization finished. db_path_list not found")
}

# UX_P001: Report startup time
if (exists(".startup_time")) {
  message(sprintf("[UX_P001] APP_MODE init completed in %.1f seconds",
                  (proc.time() - .startup_time)[3]))
}

