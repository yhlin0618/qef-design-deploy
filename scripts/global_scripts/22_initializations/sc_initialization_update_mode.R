# UPDATE_MODE Initialization --------------------------------------------------
# Minimal initialization for update/batch scripts.

# Ensure autoinit() has been executed -----------------------------------------
if (!exists(".InitEnv") || !is.environment(.InitEnv)) {
  stop("autoinit() must be executed before sourcing sc_initialization_app_mode.R")
}

# Abort if already initialized -------------------------------------------------
if (exists("INITIALIZATION_COMPLETED") && INITIALIZATION_COMPLETED) {
  message("Initialization already completed – skipping.")
}

# -----------------------------------------------------------------------------
# 1. Environment flags
# -----------------------------------------------------------------------------
if (!exists("OPERATION_MODE", envir = .GlobalEnv)) {
  stop("OPERATION_MODE is not defined. Please set OPERATION_MODE before sourcing this script.")
}
options(app.verbose = getOption("app.verbose", FALSE))


# -----------------------------------------------------------------------------
# 2. Load environment variables from .env file
# -----------------------------------------------------------------------------
# Following MP099: Real-time progress reporting
message("UPDATE_MODE: Checking for .env file...")

# Check for .env file in APP_DIR (project root)
env_file <- file.path(APP_DIR, ".env")
if (!file.exists(env_file)) {
  stop("UPDATE_MODE: .env file not found at ", env_file)
}

# Use dotenv package if available, otherwise use readRenviron
if (requireNamespace("dotenv", quietly = TRUE)) {
  dotenv::load_dot_env(file = env_file)
  message("UPDATE_MODE: ✅ Environment variables loaded from .env using dotenv package")
} else {
  readRenviron(env_file)
  message("UPDATE_MODE: ✅ Environment variables loaded from .env using readRenviron")
}

# Verify critical environment variables for UPDATE_MODE
# Dynamically check env vars based on active platforms in app_config.yaml
# (Avoids hardcoding platform-specific vars like EBY_* in generic init script)
if (exists("app_configs") && is.list(app_configs) && !is.null(app_configs$platforms)) {
  active_platforms <- names(app_configs$platforms)
  if (length(active_platforms) > 0) {
    message(sprintf("UPDATE_MODE: Active platforms: %s", paste(active_platforms, collapse = ", ")))
  }
} else {
  message("UPDATE_MODE: No platforms section found in app_configs, skipping env var check")
}

# -----------------------------------------------------------------------------
# 3. Package initialization
# -----------------------------------------------------------------------------
source(file.path(GLOBAL_DIR, "04_utils", "fn_initialize_packages.R"))
source(file.path(GLOBAL_DIR, "04_utils", "base", "fn_library2.R"))
initialize_packages(mode = OPERATION_MODE,
                    verbose = getOption("update.verbose", FALSE),
                    force_update = FALSE)

# -----------------------------------------------------------------------------
# 3.1 App configuration bootstrap (for vector-based scripts)
# -----------------------------------------------------------------------------
if (!exists("load_app_configs", envir = .GlobalEnv, inherits = TRUE) ||
    !is.function(get("load_app_configs", envir = .GlobalEnv, inherits = TRUE))) {
  stop("UPDATE_MODE: load_app_configs() is required but not available.")
}

app_configs <- load_app_configs(verbose = FALSE)

# Normalize and cache data-root for scripts expecting RAW_DATA_DIR
if (!exists("RAW_DATA_DIR", envir = .GlobalEnv, inherits = TRUE) &&
    is.list(app_configs) &&
    !is.null(app_configs$RAW_DATA_DIR) &&
    nzchar(app_configs$RAW_DATA_DIR)) {
  RAW_DATA_DIR <- app_configs$RAW_DATA_DIR
  if (!startsWith(RAW_DATA_DIR, "/") &&
      file.exists(file.path(APP_DIR, RAW_DATA_DIR))) {
    RAW_DATA_DIR <- normalizePath(file.path(APP_DIR, RAW_DATA_DIR), mustWork = FALSE)
  } else {
    RAW_DATA_DIR <- normalizePath(RAW_DATA_DIR, mustWork = FALSE)
  }
}

# Ensure product line parameters are available in update mode.
#
# DM_R054 v2.1: runtime MUST read df_product_line from meta_data.duckdb via
# fn_load_product_lines(); direct read.csv(df_product_line.csv) is forbidden.
# The CSV at APP_PARAMETER_DIR/scd_type1/df_product_line.csv is a seed consumed
# only by all_ETL_meta_init_0IM.R. UPDATE_MODE's db_path_list$meta_data is
# populated by sc_Rprofile.R's UPDATE_MODE branch (all DB paths loaded).
if (!exists("df_product_line", envir = .GlobalEnv, inherits = TRUE) &&
    exists("db_path_list", envir = .GlobalEnv, inherits = TRUE) &&
    !is.null(db_path_list$meta_data)) {

  # Canonical reader — meta_data.duckdb (no CSV fallback per DM_R054 v2.1).
  # load_product_lines() requires a DBI conn to cache df_product_line_profile;
  # in UPDATE_MODE we do not need that side effect, so we open an in-memory
  # SQLite scratch connection and immediately disconnect it.
  suppressPackageStartupMessages({
    requireNamespace("DBI",     quietly = TRUE)
    requireNamespace("duckdb",  quietly = TRUE)
    requireNamespace("RSQLite", quietly = TRUE)
  })
  scratch_con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  df_product_line <- load_product_lines(
    conn           = scratch_con,
    meta_data_path = db_path_list$meta_data
  )
  try(DBI::dbDisconnect(scratch_con), silent = TRUE)
  assign("df_product_line", df_product_line, envir = .GlobalEnv)
}

if (exists("df_product_line", envir = .GlobalEnv, inherits = TRUE) &&
    is.data.frame(df_product_line) &&
    "product_line_id" %in% names(df_product_line)) {
  vec_product_line_id <- trimws(df_product_line$product_line_id)
  vec_product_line_id <- unique(vec_product_line_id[vec_product_line_id != ""])
  vec_product_line_id_noall <- vec_product_line_id[vec_product_line_id != "all"]
} else {
  stop("UPDATE_MODE: df_product_line is required and must include product_line_id")
}
# -----------------------------------------------------------------------------
# 4. Load global scripts in deterministic order
# -----------------------------------------------------------------------------
source(file.path(GLOBAL_DIR, "04_utils", "fn_get_r_files_recursive.R"))
load_dirs <- c(
  "14_sql_utils",
  "02_db_utils",
  "04_utils",
  "03_config",
  "01_db",
  "05_etl_utils",
  "06_queries",
  "05_data_processing",
  "07_models",
  "08_ai",
  "09_python_scripts",
  "10_rshinyapp_components",
  "11_rshinyapp_utils",
  "17_transform"
)

# Enhanced debugging for file loading
cat("🔧 Starting to load global scripts...\n")
total_files_loaded <- 0
total_errors <- 0

# NOTE (#510, Decision 2): The blanket-source loop below assumes every R file
# in load_dirs/ is "import-only" — i.e. defines functions without side effects
# at top level. Generator scripts (gen_*.R) and one-off fix scripts violate
# this assumption: they read yaml/csv at file load time, which halts autoinit
# when input data is missing. Per #510 fix (Strategy B, MP044 Functor-Module
# Correspondence), generators wrap their top-level into a generate_*() function
# with an entry guard. If this pattern keeps proliferating (≥ 5 generators or
# maintainer pain), consider promoting it to autoinit policy: skip files
# matching "gen_*.R" / "fix_*.R" patterns at sweep time. See follow-up issue
# referenced in #510 Closing Summary for the architectural cleanup proposal.

for (d in load_dirs) {
  dir_path <- file.path(GLOBAL_DIR, d)
  if (!dir.exists(dir_path)) {
    cat("⏭️  Skipping non-existent directory:", d, "\n")
    next
  }
  
  cat("📁 Loading directory:", d, "\n")
  r_files <- sort(get_r_files_recursive(dir_path))
  
  if (length(r_files) == 0) {
    cat("   ℹ️  No R files found in", d, "\n")
    next
  }
  
  cat("   📋 Found", length(r_files), "R files\n")
  
  # Load each file with individual error handling
  for (file_path in r_files) {
    cat("   🔄 Loading:", basename(file_path), "...")
    source(file_path, local = FALSE)
    cat(" ✅\n")
    total_files_loaded <- total_files_loaded + 1
  }
  cat("   ✅ Completed directory:", d, "\n\n")
}

cat("📊 Loading Summary:\n")
cat("   ✅ Successfully loaded:", total_files_loaded, "files\n")
cat("🎯 Global scripts loading completed!\n\n")

# -----------------------------------------------------------------------------
# 5. Finalize
# -----------------------------------------------------------------------------
INITIALIZATION_COMPLETED <- TRUE
message("UPDATE_MODE initialization finished. Databases available: ",
        paste(names(db_path_list), collapse = ", "))
