# Generic App Deployment Script Template
#
# This script should be run from the app's root directory
# e.g., cd l1_basic/positioning_app && Rscript scripts/global_scripts/21_rshinyapp_templates/rsconnect/deployment_positioning_app.R

library(rsconnect)

# -----------------------------------------------------------------------------
# CONFIGURATION - UPDATE THIS SECTION FOR EACH APP
# -----------------------------------------------------------------------------
APP_DIR_NAME <- "APP_NAME_HERE"  # e.g., "positioning_app", "VitalSigns", "InsightForge"

# -----------------------------------------------------------------------------
# Verify we're in the correct app directory
# -----------------------------------------------------------------------------
current_dir <- basename(getwd())
if (current_dir != APP_DIR_NAME) {
  stop("This script should be run from the ", APP_DIR_NAME, " directory.\n",
       "Current directory: ", getwd(), "\n",
       "Please run: cd l1_basic/", APP_DIR_NAME, " && Rscript <this script>")
}

# -----------------------------------------------------------------------------
# Load environment variables (.env) from current directory
# -----------------------------------------------------------------------------
if (file.exists(".env")) {
  readRenviron(".env")
  cat("✅ Loaded .env from", getwd(), "\n")
}

# -----------------------------------------------------------------------------
# Configuration (can be overridden via .env or shell vars)
# -----------------------------------------------------------------------------
APP_NAME  <- Sys.getenv("SHINYAPPS_APP_NAME", tolower(APP_DIR_NAME))
APP_TITLE <- Sys.getenv("APP_TITLE", APP_DIR_NAME)
ACCOUNT   <- Sys.getenv("SHINYAPPS_ACCOUNT", "kyle-lin")

# -----------------------------------------------------------------------------
# Source deployment utility from global_scripts
# -----------------------------------------------------------------------------
# Initialize with autoinit to get standard path constants
if (!exists("autoinit")) {
  # If autoinit is not available, try to source it
  rprofile_paths <- c(
    "scripts/global_scripts/22_initializations/sc_Rprofile.R",
    "../../global_scripts/22_initializations/sc_Rprofile.R",
    "../../../global_scripts/22_initializations/sc_Rprofile.R"
  )
  
  for (rprof_path in rprofile_paths) {
    if (file.exists(rprof_path)) {
      source(rprof_path)
      break
    }
  }
}

# Initialize environment to get path constants
if (exists("autoinit")) {
  autoinit()
  deploy_utils <- file.path(GLOBAL_DIR, "11_rshinyapp_utils", "fn_deploy_shiny_app.R")
  global_scripts_dir <- GLOBAL_DIR
} else {
  # Fallback to legacy path detection
  global_scripts_paths <- c(
    "scripts/global_scripts",  # Standard path within app
    "../../global_scripts",    # If running from l1_basic/app_name
    "../../../global_scripts"  # Other possible locations
  )
  
  deploy_utils <- NULL
  global_scripts_dir <- NULL
  for (path in global_scripts_paths) {
    test_path <- file.path(path, "11_rshinyapp_utils", "fn_deploy_shiny_app.R")
    if (file.exists(test_path)) {
      deploy_utils <- test_path
      global_scripts_dir <- path
      break
    }
  }
}

if (is.null(deploy_utils) || !file.exists(deploy_utils)) {
  stop("Cannot find fn_deploy_shiny_app.R. Please ensure autoinit() is available.")
}

cat("✅ Found deployment utility at:", deploy_utils, "\n")
source(deploy_utils)

# -----------------------------------------------------------------------------
# Run deployment (already in app directory)
# -----------------------------------------------------------------------------
cat("🚀 Deploying from:", getwd(), "\n")

# Pass the correct func_dir to fn_deploy_shiny_app
fn_deploy_shiny_app(
  app_name   = APP_NAME,
  app_title  = APP_TITLE,
  account    = ACCOUNT,
  func_dir   = global_scripts_dir  # Override the default path
)

# -----------------------------------------------------------------------------
# Post-deployment message
# -----------------------------------------------------------------------------
if (tolower(Sys.getenv("DEPLOY_TARGET", "connect")) == "shinyapps") {
  cat(sprintf("\n✅ Deployment complete!\nApplication URL: https://%s.shinyapps.io/%s/\n",
              ACCOUNT, APP_NAME))
} else {
  connect_server <- Sys.getenv("CONNECT_SERVER", "<your-connect-server>")
  cat("\n✅ Deployment complete!\n")
  cat(sprintf("Application URL: %s/%s/\n", connect_server, APP_NAME))
} 