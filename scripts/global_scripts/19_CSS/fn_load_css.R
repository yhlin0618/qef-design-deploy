# -*- coding: utf-8 -*-
#LOCK FILE

#' Load CSS files from the CSS directory
#'
#' This function creates HTML tags for CSS files in the specified directory
#' @param css_dir The directory containing CSS files
#' @param add_www_prefix Whether to add the www/ prefix for files in the www directory
#' @return A tagList of link tags for the CSS files
#' @export
load_css_files <- function(css_dir = NULL, add_www_prefix = FALSE) {
  # Default to the 19_CSS directory if not specified
  if (is.null(css_dir)) {
    css_dir <- file.path("update_scripts", "global_scripts", "19_CSS")
  }
  
  # Check if directory exists
  if (!dir.exists(css_dir)) {
    warning("CSS directory not found: ", css_dir)
    return(NULL)
  }
  
  # List all CSS files
  css_files <- list.files(css_dir, pattern = "\\.css$", full.names = FALSE)
  
  if (length(css_files) == 0) {
    warning("No CSS files found in directory: ", css_dir)
    return(NULL)
  }
  
  # Create path prefix based on whether to use www
  path_prefix <- if (add_www_prefix) "" else css_dir
  
  # Create links for each CSS file
  css_links <- lapply(css_files, function(file) {
    file_path <- if (add_www_prefix) file else file.path(path_prefix, file)
    tags$link(rel = "stylesheet", type = "text/css", href = file_path)
  })
  
  # Return a tagList of all CSS links
  do.call(tagList, css_links)
}

#' Copy CSS files to www directory
#'
#' Copies CSS files from the source directory to the www directory
#' @param source_dir The source directory containing CSS files
#' @param www_dir The target www directory
#' @return Boolean indicating success
#' @export
copy_css_to_www <- function(source_dir = NULL, www_dir = NULL) {
  # Default to the 19_CSS directory if not specified
  if (is.null(source_dir)) {
    source_dir <- file.path("update_scripts", "global_scripts", "19_CSS")
  }
  
  # Default to the www directory if not specified
  if (is.null(www_dir)) {
    www_dir <- "www"
  }
  
  # Check if directories exist
  if (!dir.exists(source_dir)) {
    warning("Source CSS directory not found: ", source_dir)
    return(FALSE)
  }
  
  # Create www directory if it doesn't exist
  if (!dir.exists(www_dir)) {
    dir.create(www_dir, recursive = TRUE)
  }
  
  # List all CSS files
  css_files <- list.files(source_dir, pattern = "\\.css$", full.names = TRUE)
  
  if (length(css_files) == 0) {
    warning("No CSS files found in directory: ", source_dir)
    return(FALSE)
  }
  
  # Copy each file to www directory
  success <- sapply(css_files, function(file) {
    target_file <- file.path(www_dir, basename(file))
    tryCatch({
      file.copy(file, target_file, overwrite = TRUE)
    }, error = function(e) {
      warning("Failed to copy file ", file, " to ", target_file, ": ", e$message)
      return(FALSE)
    })
  })
  
  # Return success status
  all(success)
}