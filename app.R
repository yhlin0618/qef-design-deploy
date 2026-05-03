# =============================================================
# app.R - Application Entry Point
# =============================================================

# Load .Rprofile to ensure autoinit() function is available
if (file.exists(".Rprofile")) {
  source(".Rprofile")
}

# Read configuration
if (!requireNamespace("yaml", quietly = TRUE)) {
  stop("Please install the 'yaml' package")
}

config <- yaml::read_yaml("app_config.yaml")
main_file <- config$deployment$main_file

if (!file.exists(main_file)) {
  stop("Cannot find main_file: ", main_file)
}

# Save project root so union files can restore it after shiny::runApp() changes wd
Sys.setenv(PROJECT_ROOT = normalizePath(getwd()))

# Detect deployment environment
on_connect <- identical(Sys.getenv("RSTUDIO_PRODUCT"), "CONNECT") ||
  nzchar(Sys.getenv("RSTUDIO_CONNECT_VERSION")) ||
  nzchar(Sys.getenv("CONNECT_SERVER")) ||
  !interactive()

# Start application
if (on_connect) {
  # On Posit Connect: source() and return shiny.appobj
  app <- source(main_file, local = FALSE)$value
  app
} else {
  # Local development: use runApp() for hot-reload
  library(shiny)
  shiny::runApp(main_file, launch.browser = TRUE)
}


