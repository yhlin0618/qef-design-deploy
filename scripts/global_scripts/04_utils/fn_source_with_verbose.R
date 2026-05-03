#' @file fn_source_with_verbose.R
#' @principle R108 Function Deployment Rule
#' @principle R33 Recursive Sourcing
#' @principle R56 Folder Based Sourcing
#' 
#' @title Enhanced Source Function with Dependency Management
#' @description 
#' Enhanced version of source() that handles dependency annotations,
#' displays verbose output, and tracks loaded files.

#' @title Source With Verbose
#' @description Source a file with verbose output and dependency resolution
#' @param file_path Path to the file to source
#' @param verbose Whether to show detailed loading information (default: VERBOSE_INITIALIZATION)
#' @param max_attempts Maximum number of attempts to load a file (default: 3)
#' @return TRUE if successful, FALSE otherwise
#' @export
source_with_verbose <- function(file_path, verbose = VERBOSE_INITIALIZATION, max_attempts = 3) {
  # Ensure we have a tracking list of loaded files
  if (!exists("LOADED_FILES", envir = .GlobalEnv)) {
    assign("LOADED_FILES", character(0), envir = .GlobalEnv)
  }
  already_loaded <- get("LOADED_FILES", envir = .GlobalEnv)
  
  # Track attempts to avoid infinite recursion
  if (!exists("CURRENT_SOURCE_ATTEMPTS", envir = .GlobalEnv)) {
    assign("CURRENT_SOURCE_ATTEMPTS", new.env(), envir = .GlobalEnv)
  }
  source_attempts <- get("CURRENT_SOURCE_ATTEMPTS", envir = .GlobalEnv)
  
  # Get normalized path
  norm_path <- normalizePath(file_path, mustWork = FALSE)
  
  # Check if already loaded
  if (norm_path %in% already_loaded) {
    if (verbose) message(paste("File already loaded:", file_path))
    return(TRUE)
  }
  
  # Check if we've tried this file too many times
  if (!is.null(source_attempts[[norm_path]]) && source_attempts[[norm_path]] >= max_attempts) {
    warning(paste("Maximum loading attempts reached for file:", file_path))
    return(FALSE)
  }
  
  # Update attempt counter
  source_attempts[[norm_path]] <- if(is.null(source_attempts[[norm_path]])) 1 else source_attempts[[norm_path]] + 1
  assign("CURRENT_SOURCE_ATTEMPTS", source_attempts, envir = .GlobalEnv)
  
  # Function to extract dependencies from file
  extract_dependencies <- function(file_path) {
    if (!file.exists(file_path)) return(list(files = character(0), packages = character(0)))
    
    tryCatch({
      lines <- readLines(file_path, n = 30, warn = FALSE)  # Read first 30 lines for annotations
      file_dependencies <- character(0)
      package_dependencies <- character(0)
      
      # Look for requires annotations: both #' @requires and # @requires formats
      file_req_patterns <- c(
        "#'\\s*@requires\\s+(.+)$", 
        "#\\s*@requires\\s+(.+)$",
        "#'\\s*@depends\\s+(.+)$",
        "#\\s*@depends\\s+(.+)$"
      )
      
      # Look for package dependencies
      pkg_req_patterns <- c(
        "#'\\s*@use_package\\s+(.+)$",
        "#\\s*@use_package\\s+(.+)$",
        "#'\\s*@uses_package\\s+(.+)$",
        "#\\s*@uses_package\\s+(.+)$",
        "#'\\s*@requires\\s+package:(.+)$",
        "#\\s*@requires\\s+package:(.+)$"
      )
      
      for (line in lines) {
        # Check for file dependencies
        for (pattern in file_req_patterns) {
          if (grepl(pattern, line)) {
            match <- regexpr(pattern, line)
            if (match != -1) {
              dep_name <- sub(pattern, "\\1", line)
              dep_name <- trimws(dep_name)
              
              # Skip if it looks like a package dependency
              if (!grepl("^package:", dep_name) && 
                  !dep_name %in% rownames(installed.packages())) {
                file_dependencies <- c(file_dependencies, dep_name)
              }
            }
          }
        }
        
        # Check for package dependencies
        for (pattern in pkg_req_patterns) {
          if (grepl(pattern, line)) {
            match <- regexpr(pattern, line)
            if (match != -1) {
              pkg_name <- sub(pattern, "\\1", line)
              pkg_name <- trimws(pkg_name)
              package_dependencies <- c(package_dependencies, pkg_name)
            }
          }
        }
      }
      
      return(list(
        files = unique(file_dependencies), 
        packages = unique(package_dependencies)
      ))
    }, error = function(e) {
      warning(paste("Error reading file for dependencies:", file_path, "\nError:", e$message))
      return(list(files = character(0), packages = character(0)))
    })
  }
  
  # Function to find file path for a dependency name
  find_dependency_path <- function(dep_name, current_file_path) {
    # If it's already a full path
    if (file.exists(dep_name)) {
      return(dep_name)
    }
    
    # Try in the same directory
    dir_path <- dirname(current_file_path)
    possible_dep <- file.path(dir_path, dep_name)
    if (file.exists(possible_dep)) {
      return(possible_dep)
    }
    
    # Try with .R extension if missing
    if (!grepl("\\.R$", possible_dep, ignore.case = TRUE)) {
      possible_dep_with_ext <- paste0(possible_dep, ".R")
      if (file.exists(possible_dep_with_ext)) {
        return(possible_dep_with_ext)
      }
    }
    
    # Search in common directories if ordered_directories exists
    if (exists("ordered_directories") && exists("Func_dir")) {
      for (search_dir in ordered_directories) {
        search_path <- file.path(Func_dir, search_dir)
        if (dir.exists(search_path)) {
          # Search for exact matches first
          dep_files <- list.files(search_path, pattern = paste0("^", dep_name, "$"), 
                                 recursive = TRUE, full.names = TRUE)
          
          # Search for file with .R extension
          if (length(dep_files) == 0 && !grepl("\\.R$", dep_name, ignore.case = TRUE)) {
            dep_files <- list.files(search_path, pattern = paste0("^", dep_name, "\\.R$"), 
                                   recursive = TRUE, full.names = TRUE)
          }
          
          # Search for name as part of filename
          if (length(dep_files) == 0) {
            dep_files <- list.files(search_path, pattern = dep_name, 
                                   recursive = TRUE, full.names = TRUE)
          }
          
          if (length(dep_files) > 0) {
            return(dep_files[1])
          }
        }
      }
    }
    
    # Nothing found
    return(NULL)
  }
  
  # Function to load a package dependency
  load_package_dependency <- function(pkg_name) {
    # Try to load the package
    if (requireNamespace(pkg_name, quietly = TRUE)) {
      if (verbose) message(paste("Package dependency satisfied:", pkg_name))
      return(TRUE)
    } else {
      # Try to install the package if not available
      if (interactive()) {
        answer <- readline(paste("Package", pkg_name, "is required but not installed. Install it now? (y/n): "))
        if (tolower(answer) == "y") {
          tryCatch({
            message(paste("Installing package:", pkg_name))
            install.packages(pkg_name)
            if (requireNamespace(pkg_name, quietly = TRUE)) {
              message(paste("Successfully installed and loaded package:", pkg_name))
              return(TRUE)
            } else {
              warning(paste("Failed to install package:", pkg_name))
              return(FALSE)
            }
          }, error = function(e) {
            warning(paste("Error installing package:", pkg_name, "-", e$message))
            return(FALSE)
          })
        } else {
          warning(paste("Package dependency not satisfied:", pkg_name))
          return(FALSE)
        }
      } else {
        warning(paste("Package dependency not satisfied:", pkg_name))
        return(FALSE)
      }
    }
  }
  
  # Try to source the file
  tryCatch({
    # Extract dependencies if the file exists
    if (file.exists(file_path)) {
      dependencies <- extract_dependencies(file_path)
      file_dependencies <- dependencies$files
      package_dependencies <- dependencies$packages
      
      if (length(file_dependencies) > 0 && verbose) {
        message(paste("Found", length(file_dependencies), "file dependencies for", file_path))
      }
      
      if (length(package_dependencies) > 0 && verbose) {
        message(paste("Found", length(package_dependencies), "package dependencies for", file_path))
      }
      
      # Process package dependencies first
      for (pkg_name in package_dependencies) {
        success <- load_package_dependency(pkg_name)
        if (!success && verbose) {
          warning(paste("Missing package dependency:", pkg_name, "for file:", file_path))
        }
      }
      
      # Process file dependencies
      for (dep_name in file_dependencies) {
        dep_path <- find_dependency_path(dep_name, file_path)
        
        if (is.null(dep_path)) {
          warning(paste("Could not find file dependency:", dep_name, "required by:", file_path))
          next
        }
        
        # Skip if already loaded
        if (dep_path %in% already_loaded) {
          if (verbose) message(paste("File dependency already loaded:", dep_path))
          next
        }
        
        # Source the dependency
        if (verbose) message(paste("Loading file dependency:", dep_path, "for", file_path))
        dep_success <- source_with_verbose(dep_path, verbose, max_attempts)
        
        if (!dep_success) {
          warning(paste("Failed to load file dependency:", dep_path, "required by:", file_path))
          # Continue anyway - the file might work without this dependency
        }
      }
    }
    
    # Now source the main file
    if (verbose) message(paste("Sourcing file:", file_path))
    
    source(file_path)
    
    # Mark as successfully loaded
    already_loaded <- c(already_loaded, norm_path)
    assign("LOADED_FILES", already_loaded, envir = .GlobalEnv)
    
    if (verbose) message(paste("Successfully loaded:", file_path))
    return(TRUE)
  }, error = function(e) {
    warning(paste("Error sourcing file:", file_path, "\nError:", e$message))
    return(FALSE)
  })
}

#' @title Get Loaded Files
#' @description Get a list of all files that have been loaded
#' @return Character vector of loaded file paths
#' @export
get_loaded_files <- function() {
  if (!exists("LOADED_FILES", envir = .GlobalEnv)) {
    return(character(0))
  }
  return(get("LOADED_FILES", envir = .GlobalEnv))
}

#' @title Reset Source Tracking
#' @description Reset the tracking of loaded files and attempts
#' @param clear_loaded Whether to clear the loaded files list
#' @return TRUE
#' @export
reset_source_tracking <- function(clear_loaded = TRUE) {
  # Reset attempt counter
  assign("CURRENT_SOURCE_ATTEMPTS", new.env(), envir = .GlobalEnv)
  
  # Optionally clear loaded files
  if (clear_loaded) {
    assign("LOADED_FILES", character(0), envir = .GlobalEnv)
  }
  
  return(TRUE)
}

# Example usage:
# 
# # 1. Add @requires annotations to your R files:
# 
# # At the top of component1.R:
# # @requires base_utilities.R
# # @requires data_functions.R
# # @use_package dplyr
# 
# # 2. Then use source_with_verbose to load files with dependency resolution:
# source_with_verbose("path/to/component1.R")