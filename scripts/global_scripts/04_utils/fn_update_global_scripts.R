#' @file fn_update_global_scripts.R
#' @principle MP14 Change Tracking Principle
#' @principle R08 Global Scripts Synchronization Rule
#' @principle R33 Recursive Sourcing Rule
#' @author Claude
#' @date 2025-04-14
#' @description Function to update and synchronize global scripts following MP14 and R08

#' Update Global Scripts Across Projects
#'
#' This function implements the Global Scripts Synchronization Rule (R08) by providing
#' a programmatic way to ensure all instances of global_scripts across different
#' company projects are synchronized with the latest version. It handles checking
#' for uncommitted changes, committing changes if requested, and pulling updates.
#'
#' @param auto_commit Logical. If TRUE, automatically commits any uncommitted changes before synchronizing.
#' @param verbose Logical. If TRUE, displays detailed information during the synchronization process.
#' @param create_missing Logical. If TRUE, creates missing global_scripts directories for companies listed in parameters.
#' @return Invisible list with synchronization results for each company project.
#'
#' @examples
#' # Update global scripts with automatic commit of changes
#' update_global_scripts(auto_commit = TRUE)
#'
#' # Update scripts with verbose output
#' update_global_scripts(verbose = TRUE)
update_global_scripts <- function(auto_commit = FALSE, verbose = TRUE, create_missing = FALSE) {
  # Initialize results tracking
  results <- list()
  
  # Check if in the right operating mode
  if (!exists("OPERATION_MODE") || OPERATION_MODE != "UPDATE_MODE") {
    warning("This function should be run in UPDATE_MODE. Current mode: ", 
            if(exists("OPERATION_MODE")) OPERATION_MODE else "Not set")
  }
  
  # Define base paths
  if (!exists("BASE_PATH")) {
    tryCatch({
      source(file.path("update_scripts", "global_scripts", "00_principles", "fn_root_path_config.R"))
      paths <- setup_project_paths()
      BASE_PATH <- paths$base_path
    }, error = function(e) {
      # Fallback to a standard location
      BASE_PATH <- file.path(Sys.getenv("HOME"), "Library", "CloudStorage", "Dropbox", "precision_marketing")
      warning("Could not determine base path from config. Using: ", BASE_PATH)
    })
  }
  
  # Get list of companies from parameters
  companies <- get_company_list()
  if (verbose) {
    message("Found companies: ", paste(companies, collapse = ", "))
  }
  
  # Check each company's global_scripts for uncommitted changes
  uncommitted_changes <- list()
  missing_directories <- character(0)
  
  for (company in companies) {
    global_scripts_path <- file.path(BASE_PATH, paste0("precision_marketing_", company), 
                                    "precision_marketing_app", "update_scripts", "global_scripts")
    
    if (!dir.exists(global_scripts_path)) {
      if (verbose) {
        message("Directory not found: ", global_scripts_path)
      }
      missing_directories <- c(missing_directories, company)
      next
    }
    
    # Change to the global_scripts directory
    original_dir <- getwd()
    setwd(global_scripts_path)
    
    # Check for uncommitted changes
    status_result <- system("git status --porcelain", intern = TRUE)
    
    if (length(status_result) > 0) {
      if (verbose) {
        message("Uncommitted changes found in ", company, " project:")
        for (line in status_result) {
          message("  ", line)
        }
      }
      uncommitted_changes[[company]] <- status_result
    }
    
    # Return to original directory
    setwd(original_dir)
  }
  
  # Handle uncommitted changes
  if (length(uncommitted_changes) > 0) {
    if (auto_commit) {
      if (verbose) {
        message("Auto-committing changes...")
      }
      
      for (company in names(uncommitted_changes)) {
        global_scripts_path <- file.path(BASE_PATH, paste0("precision_marketing_", company), 
                                        "precision_marketing_app", "update_scripts", "global_scripts")
        
        # Change to the global_scripts directory
        original_dir <- getwd()
        setwd(global_scripts_path)
        
        # Commit changes
        system("git add .")
        commit_result <- system(paste0('git commit -m "[R08] Auto-commit by update_global_scripts.R"'), intern = TRUE)
        
        if (verbose) {
          message("Committed changes in ", company, " project:")
          for (line in commit_result) {
            message("  ", line)
          }
        }
        
        results[[company]]$commit <- list(status = "success", message = commit_result)
        
        # Return to original directory
        setwd(original_dir)
      }
    } else {
      warning("Uncommitted changes found in the following projects: ", 
              paste(names(uncommitted_changes), collapse = ", "), 
              ". Use auto_commit=TRUE to automatically commit these changes.")
      return(invisible(list(status = "error", message = "Uncommitted changes found")))
    }
  }
  
  # Synchronize each company's global_scripts
  for (company in companies) {
    if (company %in% missing_directories) {
      if (create_missing) {
        # Find a valid source repository
        source_company <- NULL
        for (potential_source in companies) {
          if (!(potential_source %in% missing_directories)) {
            source_company <- potential_source
            break
          }
        }
        
        if (!is.null(source_company)) {
          if (verbose) {
            message("Creating global_scripts for ", company, " using ", source_company, " as source...")
          }
          
          # Create the directory structure
          target_dir <- file.path(BASE_PATH, paste0("precision_marketing_", company), 
                                 "precision_marketing_app", "update_scripts")
          global_scripts_path <- file.path(target_dir, "global_scripts")
          
          if (!dir.exists(target_dir)) {
            dir.create(target_dir, recursive = TRUE)
          }
          
          # Copy the source repository
          source_path <- file.path(BASE_PATH, paste0("precision_marketing_", source_company), 
                                  "precision_marketing_app", "update_scripts", "global_scripts")
          
          copy_result <- tryCatch({
            # Use file.copy with recursive=TRUE to copy the directory
            ok <- file.copy(source_path, target_dir, recursive = TRUE)
            if (!ok) stop("Failed to copy files")
            "Success"
          }, error = function(e) {
            paste("Error:", e$message)
          })
          
          if (verbose) {
            message("Result of creating global_scripts for ", company, ": ", copy_result)
          }
          
          # Initialize git repository if needed
          if (dir.exists(file.path(source_path, ".git"))) {
            original_dir <- getwd()
            setwd(global_scripts_path)
            
            system("git init")
            system("git add .")
            system('git commit -m "Initial commit for global_scripts"')
            
            if (verbose) {
              message("Initialized git repository for ", company)
            }
            
            setwd(original_dir)
          }
          
          results[[company]]$create <- list(status = "success", message = copy_result)
        } else {
          if (verbose) {
            message("Cannot create global_scripts for ", company, ": No valid source repository found")
          }
          results[[company]]$create <- list(status = "error", message = "No valid source repository")
        }
      } else {
        results[[company]] <- list(status = "skipped", message = "Directory not found and create_missing=FALSE")
      }
      next
    }
    
    global_scripts_path <- file.path(BASE_PATH, paste0("precision_marketing_", company), 
                                    "precision_marketing_app", "update_scripts", "global_scripts")
    
    # Change to the global_scripts directory
    original_dir <- getwd()
    setwd(global_scripts_path)
    
    # Fetch the latest changes
    if (verbose) {
      message("Syncing ", company, " project...")
    }
    
    # Make sure this is a git repository
    if (!dir.exists(".git")) {
      if (verbose) {
        message("Not a git repository: ", global_scripts_path)
      }
      results[[company]] <- list(status = "error", message = "Not a git repository")
      setwd(original_dir)
      next
    }
    
    # Fetch the latest changes
    system("git fetch origin")
    
    # Get current branch
    current_branch <- system("git rev-parse --abbrev-ref HEAD", intern = TRUE)
    
    # Check if we're behind the remote
    behind_count <- system(paste0("git rev-list --count HEAD..origin/", current_branch, " 2>/dev/null || echo 0"), intern = TRUE)
    local_behind <- as.integer(behind_count)
    
    if (local_behind == 0) {
      if (verbose) {
        message(company, " is already up to date")
      }
      results[[company]] <- list(status = "success", message = "Already up to date")
    } else {
      if (verbose) {
        message("Updating ", company, " (", local_behind, " commits behind)...")
      }
      
      # Stash any local changes
      status_result <- system("git status --porcelain", intern = TRUE)
      stashed <- FALSE
      
      if (length(status_result) > 0) {
        if (verbose) {
          message("  Stashing local changes...")
        }
        system('git stash save "Automatically stashed by update_global_scripts.R"')
        stashed <- TRUE
      }
      
      # Pull the latest changes
      if (verbose) {
        message("  Pulling latest changes...")
      }
      
      pull_result <- system(paste0("git pull origin ", current_branch), intern = TRUE)
      
      # Apply stashed changes if needed
      if (stashed) {
        if (verbose) {
          message("  Restoring local changes...")
        }
        stash_result <- system("git stash pop", intern = TRUE)
        
        if (any(grepl("conflict", stash_result, ignore.case = TRUE))) {
          if (verbose) {
            message("  There were conflicts when restoring local changes.")
            message("  Please resolve them manually. Your changes are in the stash.")
          }
          results[[company]]$stash <- list(status = "warning", message = "Conflicts when restoring stash")
        }
      }
      
      if (verbose) {
        message(company, " updated successfully")
      }
      results[[company]]$pull <- list(status = "success", message = pull_result)
    }
    
    # Return to original directory
    setwd(original_dir)
  }
  
  if (verbose) {
    message("Global scripts synchronization completed")
  }
  
  return(invisible(results))
}

#' Get List of Companies from Parameters
#'
#' Retrieves the list of companies from the parameters file.
#'
#' @return Character vector of company names
get_company_list <- function() {
  # Default companies
  default_companies <- c("KitchenMAMA", "WISER", "MAMBA")
  
  # Try to find parameters.yaml
  params_file <- file.path("update_scripts", "global_scripts", "00_principles", "parameters.yaml")
  
  if (!file.exists(params_file)) {
    warning("Parameters file not found, using default companies")
    return(default_companies)
  }
  
  # Try to read companies from parameters.yaml
  tryCatch({
    # Check if yaml package is available
    if (!requireNamespace("yaml", quietly = TRUE)) {
      # Fallback to regex approach
      params_content <- readLines(params_file)
      
      # Look for companies in YAML list format or COMPANY_LIST format
      companies_line <- params_content[grep("^companies:|^COMPANY_LIST:", params_content)]
      
      if (length(companies_line) > 0) {
        # Extract companies from the line
        if (grepl("^companies:", companies_line[1])) {
          # Parse companies list from indented YAML
          companies_index <- grep("^companies:", params_content)
          if (length(companies_index) > 0) {
            start_idx <- companies_index[1] + 1
            companies <- character(0)
            
            for (i in start_idx:length(params_content)) {
              line <- params_content[i]
              
              # Stop at first non-indented, non-empty line
              if (!grepl("^[[:space:]]", line) && !grepl("^$", line)) {
                break
              }
              
              # Extract company name from list product
              if (grepl("^[[:space:]]*-", line)) {
                company <- gsub("^[[:space:]]*-[[:space:]]*([^[:space:]#]+).*$", "\\1", line)
                if (nchar(company) > 0) {
                  companies <- c(companies, company)
                }
              }
            }
            
            if (length(companies) > 0) {
              return(companies)
            }
          }
        } else if (grepl("^COMPANY_LIST:", companies_line[1])) {
          # Parse companies from COMPANY_LIST format
          companies_str <- gsub("^COMPANY_LIST:[[:space:]]*", "", companies_line[1])
          companies <- strsplit(companies_str, "[[:space:],]+")[[1]]
          
          if (length(companies) > 0) {
            return(companies)
          }
        }
      }
      
      # Fallback to defaults if no companies found
      warning("No companies found in parameters file, using defaults")
      return(default_companies)
    } else {
      # Use yaml package to parse the file
      params <- yaml::read_yaml(params_file)
      
      if (!is.null(params$companies) && length(params$companies) > 0) {
        return(params$companies)
      } else if (!is.null(params$COMPANY_LIST)) {
        companies <- strsplit(params$COMPANY_LIST, "[[:space:],]+")[[1]]
        return(companies)
      } else {
        warning("No companies found in parameters file, using defaults")
        return(default_companies)
      }
    }
  }, error = function(e) {
    warning("Error reading parameters file: ", e$message, ". Using default companies")
    return(default_companies)
  })
}