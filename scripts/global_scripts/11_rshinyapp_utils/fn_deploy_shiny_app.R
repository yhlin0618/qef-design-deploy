#' Deploy a Shiny application using rsconnect
#'
#' @file fn_deploy_shiny_app.R
#' @principle MP47 Functional Programming
#' @principle R21 One Function One File
#' @principle R95 Import Requirements Rule
#' @uses_package rsconnect See 20_R_packages/rsconnect.md
#' @uses_package stringr See 20_R_packages/stringr.md
#'
#' This function gathers the necessary files and deploys a Shiny
#' application to shinyapps.io. It encapsulates the logic from the
#' deployment templates so it can be reused for different apps.
#'
#' @param app_name   Application name on shinyapps.io
#' @param app_title  Title displayed for the application
#' @param account    Account name for deployment
#' @param app_file   Path to the main R file for the app (optional)
#' @param func_dir   Base directory containing global scripts
#' @param server     Deployment target: "shinyapps.io" or Posit Connect URL
#' @return           Invisible result of rsconnect::deployApp
#' @export
fn_deploy_shiny_app <- function(app_name,
                                app_title,
                                account,
                                app_file = NULL,
                                func_dir = file.path("update_scripts", "global_scripts"),
                                server = NULL) {
  if (!requireNamespace("rsconnect", quietly = TRUE)) {
    stop("Package 'rsconnect' is required")
  }
  if (!requireNamespace("stringr", quietly = TRUE)) {
    stop("Package 'stringr' is required")
  }
  
  # Determine deployment target from environment or parameter
  if (is.null(server)) {
    deploy_target <- Sys.getenv("DEPLOY_TARGET", "connect")  # 預設改為 connect
    if (deploy_target == "connect") {
      server <- Sys.getenv("CONNECT_SERVER")
      if (server == "") {
        stop("CONNECT_SERVER environment variable must be set when DEPLOY_TARGET=connect. 
Please set CONNECT_SERVER in your .env file or use DEPLOY_TARGET=shinyapps for shinyapps.io")
      }
    } else if (deploy_target == "shinyapps") {
      server <- "shinyapps.io"
    } else {
      stop("Invalid DEPLOY_TARGET. Must be 'connect' or 'shinyapps'")
    }
  }
  
  cat("Deploying to:", server, "\n")

  files_to_exclude <- c(
    ".RData", ".Rproj", ".Rhistory", ".Renviron",
    ".idea", ".vscode", ".atom",
    ".git", ".gitignore", ".svn",
    "README.md", "LICENSE", "DESCRIPTION", "doc", "docs",
    "documentation", "man", ".md",
    "test", "tests", "example", "examples",
    "deprecated", "archive", "app_archive", "99_archive", "old", "backup",
    "app_screenshots", "images", "figures", "output", "results",
    "tmp", "temp", "cache", ".cache",
    "data/raw", "data/unused", "data/archive", "data/test",
    "renv/library", "packrat/lib", "packrat/lib-ext", "packrat/lib-R", "packrat/src",
    "logs", "*.log",
    ".env", "dropbox"
  )

  if (!exists("get_r_files_recursive")) {
    util_path <- file.path(func_dir, "04_utils", "fn_get_r_files_recursive.R")
    if (file.exists(util_path)) {
      source(util_path)
    }
  }

  ordered_directories <- c(
    "02_db_utils",
    "04_utils",
    "03_config",
    "10_rshinyapp_components",
    "11_rshinyapp_utils"
  )

  process_dir <- function(dir_name) {
    dir_path <- file.path(func_dir, dir_name)
    if (!dir.exists(dir_path)) return(character(0))
    if (exists("get_r_files_recursive")) {
      get_r_files_recursive(dir_path)
    } else {
      recurse <- stringr::str_detect(dir_name, "10_rshinyapp")
      list.files(dir_path, pattern = "\\.R$", full.names = TRUE, recursive = recurse)
    }
  }

  add_file_path <- unique(unlist(lapply(ordered_directories, process_dir)))

  app_templates_dir <- file.path(func_dir, "21_rshinyapp_templates")

  
  options(rsconnect.max.bundle.size = 4 * 1024^3)
  all_files <- rsconnect::listDeploymentFiles(getwd())
  filtered_files <- all_files

  for (pattern in files_to_exclude) {
    if (stringr::str_detect(pattern, "\\*")) {
      safe_pattern <- gsub("\\.", "\\\\.", pattern)
      safe_pattern <- gsub("\\*", ".*", safe_pattern)
      filtered_files <- filtered_files[!grepl(safe_pattern, filtered_files)]
    } else {
      filtered_files <- filtered_files[!grepl(pattern, filtered_files)]
    }
  }

  update_scripts_pattern <- "^update_scripts/"
  global_scripts_pattern <- "^update_scripts/global_scripts/"
  update_scripts_files <- grep(update_scripts_pattern, filtered_files, value = TRUE)
  non_global_scripts_files <- update_scripts_files[!grepl(global_scripts_pattern, update_scripts_files)]
  if (length(non_global_scripts_files) > 0) {
    filtered_files <- filtered_files[!filtered_files %in% non_global_scripts_files]
  }

  problem_patterns <- c("app_archive/app", "00_principles/examples/nsql_integrated_example", "99_archive/")
  for (pattern in problem_patterns) {
    filtered_files <- filtered_files[!grepl(pattern, filtered_files, fixed = TRUE)]
  }

  deployment_files <- unique(c(filtered_files, add_file_path))

  # Handle custom app file if provided
  app_r_backup <- NULL
  if (!is.null(app_file)) {
    if (!file.exists(app_file)) {
      stop("App file not found: ", app_file)
    }
    
    cat("Using custom app file:", app_file, "\n")
    
    # Backup existing app.R if it exists
    app_r_path <- "app.R"
    if (file.exists(app_r_path)) {
      app_r_backup <- paste0("app.R.backup.", format(Sys.time(), "%Y%m%d_%H%M%S"))
      file.copy(app_r_path, app_r_backup)
      cat("Backed up existing app.R to:", app_r_backup, "\n")
    }
    
    # Copy the specified file to app.R
    file.copy(app_file, app_r_path, overwrite = TRUE)
    cat("Copied", app_file, "to app.R for deployment\n")
    
    # Ensure app.R is in deployment files
    if (!app_r_path %in% deployment_files) {
      deployment_files <- c(deployment_files, app_r_path)
    }
  }
  
  # R68: Object Initialization - Build deployApp arguments  
  deploy_args <- list(
    server = server,
    appName = app_name,
    appTitle = app_title,
    appFiles = deployment_files,
    account = account,
    forceUpdate = TRUE,
    logLevel = "normal"
  )
  
  # Add Posit Connect specific parameters if needed
  if (server != "shinyapps.io") {
    # For Posit Connect, we might need to add API key
    api_key <- Sys.getenv("CONNECT_API_KEY")
    if (api_key != "") {
      deploy_args$apiKey <- api_key
    }
  }
  
  # Deploy the app
  tryCatch({
    do.call(rsconnect::deployApp, deploy_args)
  }, finally = {
    # Restore original app.R if we made a backup
    if (!is.null(app_r_backup) && file.exists(app_r_backup)) {
      file.copy(app_r_backup, "app.R", overwrite = TRUE)
      file.remove(app_r_backup)
      cat("Restored original app.R from backup\n")
    }
  })
  
  # configureApp(
  #   appName = app_name,
  #   
  #   size      = "xxxlarge",      
  #   idleTimeout = 60,           # Instance Idle Timeout, 單位「分鐘」
  #   
  #   # ② Worker 設定
  #   appInstances = list(
  #     max_worker_processes = 2,  
  #     idle_timeout         = 30  # 每個 worker 無連線 30 秒即釋放
  #   ),
  #   
  #   # ③ 啟動 / 連線時限 (保持先前建議)
  #   startupTimeout = 300,
  #   connectionTimeout  = 60,
  #   readTimeout        = 120
  # )

  invisible(deployment_files)
}

