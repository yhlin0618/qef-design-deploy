#' Source All R Files in a Directory
#'
#' Loads all R files from a directory and its subdirectories, following the
#' R56 principle (Folder-Based Sourcing) and P55 (N-tuple Independent Loading).
#'
#' @param dir_path String. Path to the directory containing R files to source.
#' @param pattern String. File pattern to match. Defaults to "\\.R$".
#' @param recursive Logical. Whether to include subdirectories. Defaults to TRUE.
#' @param verbose Logical. Whether to display progress messages. Defaults to TRUE.
#' @param exclude_pattern String. Pattern to exclude files. Defaults to NULL.
#' @param aspect_order Logical. Whether to enforce P55 loading order by aspect type. Defaults to FALSE.
#'
#' @return Character vector of sourced file paths.
#'
#' @examples
#' # Source all R files in the utils directory
#' sourced_files <- source_directory("utils")
#'
#' # Source only files with fn_ prefix
#' fn_files <- source_directory("utils", pattern = "^fn_.*\\.R$")
#'
#' # Exclude initialization files
#' files <- source_directory("utils", exclude_pattern = "initialization|init")
#'
#' # Source components by aspect type (P55)
#' # First UI components, then Server components, etc.
#' source_directory("components", pattern = "UI\\.R$", aspect_order = TRUE)
#' source_directory("components", pattern = "Server\\.R$", aspect_order = TRUE)
#' source_directory("components", pattern = "Filters\\.R$", aspect_order = TRUE)
#' source_directory("components", pattern = "Defaults\\.R$", aspect_order = TRUE)
#'
source_directory <- function(dir_path, pattern = "\\.R$", recursive = TRUE, 
                           verbose = TRUE, exclude_pattern = NULL, aspect_order = FALSE) {
  # Get list of R files based on which recursive function is available
  if (exists("get_r_files_recursive")) {
    # Use the existing function, but adapt parameters
    if ("include_pattern" %in% names(formals(get_r_files_recursive))) {
      # Use the full version from initialization script
      r_files <- get_r_files_recursive(dir_path, include_pattern = pattern)
    } else {
      # Fall back to the basic version
      r_files <- list.files(path = dir_path, pattern = pattern, 
                           full.names = TRUE, recursive = recursive)
    }
  } else {
    # Simple version if no recursive function is available
    r_files <- list.files(path = dir_path, pattern = pattern, 
                         full.names = TRUE, recursive = recursive)
  }
  
  # Apply exclude pattern if provided
  if (!is.null(exclude_pattern) && length(r_files) > 0) {
    r_files <- r_files[!grepl(exclude_pattern, r_files)]
  }
  
  # Following P55 (N-tuple Independent Loading)
  # When aspect_order is TRUE, sort files to put base components first
  # This ensures proper dependency loading (e.g., sidebarUI.R before marketingChannelUI.R)
  if (aspect_order && length(r_files) > 0) {
    # Extract filenames without path
    filenames <- basename(r_files)
    
    # Identify base components (e.g., sidebarUI.R vs marketingChannel/marketingChannelUI.R)
    is_base <- !grepl("/[^/]+/", r_files)
    
    # Create a priority score (lower = load earlier)
    priority_scores <- rep(2, length(r_files))  # Default priority
    priority_scores[is_base] <- 1              # Base components load first
    
    # Special case for main components like sidebarUI.R
    # These should load before specific components like marketingChannelUI.R
    main_components <- grepl("^sidebar(UI|Server|Filters|Defaults)\\.R$", filenames, ignore.case = TRUE)
    priority_scores[main_components] <- 0  # Main components load first
    
    # Sort by priority first, then alphabetically within each priority group
    r_files <- r_files[order(priority_scores, filenames)]
    
    if (verbose) {
      message("Sorting by aspect following P55 (N-tuple Independent Loading):")
      for (i in seq_along(r_files)) {
        message("  ", i, ". ", basename(r_files[i]), " (priority ", priority_scores[i], ")")
      }
    }
  } else {
    # Standard alphabetical sort for consistent loading order
    r_files <- sort(r_files)
  }
  
  # Initialize tracking variables
  loaded_files <- character(0)
  failed_files <- character(0)
  
  # If no files found, return
  if (length(r_files) == 0) {
    if (verbose) message("No matching files found in directory: ", dir_path)
    return(loaded_files)
  }
  
  if (verbose) message("Sourcing ", length(r_files), " files from ", dir_path, "...")
  
  # Source each file
  for (file in r_files) {
    tryCatch({
      source(file)
      loaded_files <- c(loaded_files, file)
      if (verbose) message("  Sourced: ", basename(file))
    }, error = function(e) {
      failed_files <- c(failed_files, file)
      warning("Error sourcing ", basename(file), ": ", e$message)
    })
  }
  
  # Report results
  if (verbose) {
    message("Successfully loaded ", length(loaded_files), " files.")
    
    if (length(failed_files) > 0) {
      message("WARNING: Failed to load ", length(failed_files), " files:")
      for (file in failed_files) {
        message("  - ", basename(file))
      }
    }
  }
  
  # Add loaded files to global tracking if it exists
  if (exists("loaded_files", envir = .GlobalEnv)) {
    assign("loaded_files", c(get("loaded_files", envir = .GlobalEnv), loaded_files), envir = .GlobalEnv)
  }
  
  return(loaded_files)
}