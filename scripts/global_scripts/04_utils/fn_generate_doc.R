#' Generate Documentation from Template
#'
#' Creates a new documentation file from a predefined template, following
#' SO_R032 (Documentation Template System) principle.
#'
#' Following principles:
#' - SO_R032: Documentation Template System
#' - R021: One function one file rule
#' - R069: Function file naming (fn_ prefix)
#' - R094: Roxygen2 documentation standard
#' - MP047: Functional programming (pure function)
#' - P076: Error handling patterns
#'
#' @param type Character: Template type. One of:
#'   - "implementation": Implementation report template
#'   - "debug": Debug report template
#'   - "fix": Fix report template
#'   - "test": Test report template
#'   - "strategy": Strategy document template
#' @param topic Character: Brief topic name (used in filename, use snake_case)
#' @param title Character: Full title for the document (optional, defaults to formatted topic)
#' @param author Character: Author name (default: Sys.info()["user"])
#' @param output_dir Character: Output directory path (default: CHANGELOG directory)
#' @param open Logical: Whether to open the file after creation (default: FALSE)
#' @param date Date: Document date (default: Sys.Date())
#'
#' @return Character: Path to the created file, or NULL if error
#' @export
#'
#' @examples
#' # Basic usage
#' fn_generate_doc("implementation", "user_auth")
#'
#' # With custom title
#' fn_generate_doc("implementation", "user_auth", "User Authentication System Implementation")
#'
#' # Debug report
#' fn_generate_doc("debug", "api_timeout", "API Timeout Issue Investigation")
#'
#' # Open file after creation
#' fn_generate_doc("fix", "memory_leak", open = TRUE)
#'
fn_generate_doc <- function(type = c("implementation", "debug", "fix", "test", "strategy"),
                            topic,
                            title = NULL,
                            author = NULL,
                            output_dir = NULL,
                            open = FALSE,
                            date = Sys.Date()) {

  # Following P076: Input validation
  type <- match.arg(type)

  if (missing(topic) || is.null(topic) || !nzchar(topic)) {
    stop("Topic is required and cannot be empty")
  }

  # Sanitize topic for filename (following R051: lowercase naming)
  topic_sanitized <- tolower(gsub("[^a-zA-Z0-9_]", "_", topic))
  topic_sanitized <- gsub("_+", "_", topic_sanitized)  # Remove consecutive underscores
  topic_sanitized <- gsub("^_|_$", "", topic_sanitized)  # Remove leading/trailing underscores

  # Set default author
  if (is.null(author)) {
    author <- Sys.info()["user"]
    if (is.na(author)) author <- "Unknown"
  }

  # Set default title (format topic into readable title)
  if (is.null(title)) {
    title <- gsub("_", " ", topic)
    title <- tools::toTitleCase(title)
  }

  # Determine template and output directories
  # Find the MAMBA root directory
  mamba_root <- find_mamba_root()
  if (is.null(mamba_root)) {
    stop("Could not find MAMBA project root. Ensure you are within the MAMBA project.")
  }

  template_dir <- file.path(mamba_root, "scripts", "global_scripts", "00_principles", "templates")

  if (is.null(output_dir)) {
    output_dir <- file.path(mamba_root, "scripts", "global_scripts", "00_principles", "CHANGELOG")
  }

  # Map type to template filename and suffix
  template_map <- list(
    implementation = list(
      template = "TEMPLATE_implementation_report.md",
      suffix = "implementation"
    ),
    debug = list(
      template = "TEMPLATE_debug_report.md",
      suffix = "debug"
    ),
    fix = list(
      template = "TEMPLATE_fix_report.md",
      suffix = "fix"
    ),
    test = list(
      template = "TEMPLATE_test_report.md",
      suffix = "test"
    ),
    strategy = list(
      template = "TEMPLATE_strategy_document.md",
      suffix = "strategy"
    )
  )

  template_info <- template_map[[type]]
  template_path <- file.path(template_dir, template_info$template)

  # Validate template exists
  if (!file.exists(template_path)) {
    stop(sprintf("Template not found: %s", template_path))
  }

  # Validate output directory exists
  if (!dir.exists(output_dir)) {
    message(sprintf("Creating output directory: %s", output_dir))
    dir.create(output_dir, recursive = TRUE)
  }

  # Format date
  date_str <- format(date, "%Y-%m-%d")

  # Generate output filename
  output_filename <- sprintf("%s_%s_%s.md", date_str, topic_sanitized, template_info$suffix)
  output_path <- file.path(output_dir, output_filename)

  # Check if file already exists
  if (file.exists(output_path)) {
    warning(sprintf("File already exists: %s. Use a different topic or delete the existing file.", output_path))
    return(NULL)
  }

  # Read template
  template_content <- tryCatch({
    readLines(template_path, encoding = "UTF-8", warn = FALSE)
  }, error = function(e) {
    stop(sprintf("Error reading template: %s", e$message))
  })

  # Replace placeholders
  content <- template_content
  content <- gsub("\\[Feature Name\\]", title, content)
  content <- gsub("\\[Issue\\]", title, content)
  content <- gsub("\\[Feature/Component\\]", title, content)
  content <- gsub("\\[Topic\\]", title, content)
  content <- gsub("YYYY-MM-DD", date_str, content)
  content <- gsub("\\[Author\\]", author, content)

  # Update file location in metadata
  content <- gsub(
    "scripts/global_scripts/00_principles/changelog/YYYY-MM-DD_[^.]+\\.md",
    gsub(mamba_root, "", output_path, fixed = TRUE),
    content
  )

  # Write output file
  tryCatch({
    writeLines(content, output_path, useBytes = TRUE)
    message(sprintf("Document created successfully: %s", output_path))
  }, error = function(e) {
    stop(sprintf("Error writing file: %s", e$message))
  })

  # Open file if requested
  if (open) {
    tryCatch({
      if (.Platform$OS.type == "windows") {
        shell.exec(output_path)
      } else if (Sys.info()["sysname"] == "Darwin") {
        system2("open", output_path)
      } else {
        system2("xdg-open", output_path)
      }
    }, error = function(e) {
      warning(sprintf("Could not open file: %s", e$message))
    })
  }

  return(output_path)
}


#' Find MAMBA Project Root Directory
#'
#' Traverses up the directory tree to find the MAMBA project root.
#' The root is identified by the presence of specific marker files/directories.
#'
#' @return Character: Path to MAMBA root, or NULL if not found
#'
find_mamba_root <- function() {
  # Start from current working directory
  current_dir <- getwd()

  # Maximum depth to search
 max_depth <- 10

  for (i in seq_len(max_depth)) {
    # Check for MAMBA markers
    markers <- c(
      file.path(current_dir, "scripts", "global_scripts", "00_principles"),
      file.path(current_dir, "app_config.yaml"),
      file.path(current_dir, "scripts", "global_scripts")
    )

    if (any(file.exists(markers))) {
      return(current_dir)
    }

    # Also check if we're inside scripts/global_scripts
    if (grepl("MAMBA[/\\\\]?$", current_dir) ||
        (dir.exists(file.path(current_dir, "scripts")) &&
         dir.exists(file.path(current_dir, "data")))) {
      return(current_dir)
    }

    # Move up one directory
    parent_dir <- dirname(current_dir)

    # Check if we've reached the root
    if (parent_dir == current_dir) {
      break
    }

    current_dir <- parent_dir
  }

  # Try environment variable as fallback
  mamba_root <- Sys.getenv("MAMBA_ROOT", unset = NA)
  if (!is.na(mamba_root) && dir.exists(mamba_root)) {
    return(mamba_root)
  }

  return(NULL)
}


#' List Available Document Templates
#'
#' Returns information about available documentation templates.
#'
#' @return Data.frame: Template information
#' @export
#'
#' @examples
#' fn_list_doc_templates()
#'
fn_list_doc_templates <- function() {
  templates <- data.frame(
    type = c("implementation", "debug", "fix", "test", "strategy"),
    description = c(
      "Implementation report for new features or changes",
      "Debug report for investigating issues",
      "Fix report documenting bug fixes",
      "Test report for testing activities",
      "Strategy document for planning and decisions"
    ),
    filename_pattern = c(
      "YYYY-MM-DD_topic_implementation.md",
      "YYYY-MM-DD_topic_debug.md",
      "YYYY-MM-DD_topic_fix.md",
      "YYYY-MM-DD_topic_test.md",
      "YYYY-MM-DD_topic_strategy.md"
    ),
    stringsAsFactors = FALSE
  )

  return(templates)
}
