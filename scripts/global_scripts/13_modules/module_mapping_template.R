# Module Mapping Template
#
# This file provides a template for mapping between conceptual modules and implementation scripts.
# Copy this template to your project's update_scripts directory and customize it for your specific project.
#
# Usage in a project:
#   source("path/to/project_module_mapping.R")
#   script_path <- module_map$get_script("1.1") # Returns path to Customer DNA implementation
#   all_db_scripts <- module_map$get_module_scripts("3") # Returns all Database Management scripts

# Create the module mapping structure
module_map <- list()

# Main mapping structure - organized by module
module_map$modules <- list(
  # Module 0: System Initialization and Configuration
  "0" = list(
    name = "System Initialization and Configuration",
    submodules = list(
      "0.1" = list(
        name = "Environment Setup",
        scripts = c(
          "global_scripts/00_principles/sc_initialization_update_mode.R",
          # Add project-specific initialization scripts here
          # "local_scripts/your_company/init.R"
        )
      ),
      "0.2" = list(
        name = "Configuration Management",
        scripts = c(
          "global_scripts/03_config/global_parameters.R",
          # Add project-specific configuration scripts here
          # "local_scripts/your_company/parameters.R"
        )
      ),
      "0.3" = list(
        name = "Database Connection",
        scripts = c(
          "global_scripts/02_db_utils/fn_database_connect.R",
          "global_scripts/02_db_utils/fn_database_disconnect.R"
          # Add project-specific database connection scripts here
        )
      )
    )
  ),
  
  # Module 1: Customer Analysis
  "1" = list(
    name = "Customer Analysis",
    submodules = list(
      "1.1" = list(
        name = "Customer DNA",
        scripts = c(
          "global_scripts/05_data_processing/common/DNA_Function_dplyr.R",
          # Add project-specific DNA implementation if any
          # "local_scripts/your_company/custom_dna.R"
        )
      ),
      "1.2" = list(
        name = "RFM Analysis",
        scripts = c(
          # Add project-specific RFM scripts here
        )
      ),
      "1.3" = list(
        name = "Customer Status (NES)",
        scripts = c(
          # Add project-specific NES scripts here
        )
      ),
      "1.4" = list(
        name = "Customer Lifetime Value",
        scripts = c(
          # Add project-specific CLV scripts here
        )
      )
    )
  ),
  
  # Module 2: Data Import and Processing
  "2" = list(
    name = "Data Import and Processing",
    submodules = list(
      "2.1" = list(
        name = "Sales Data Import",
        scripts = c(
          "global_scripts/05_data_processing/import_amazon_sales.R"
          # Add project-specific import scripts here
        )
      ),
      "2.2" = list(
        name = "Sales Data Processing",
        scripts = c(
          "global_scripts/05_data_processing/amazon/sc_process_amazon_sales.R"
          # Add project-specific processing scripts here
        )
      ),
      "2.3" = list(
        name = "Review Processing",
        scripts = c(
          # Add project-specific review processing scripts here
        )
      )
    )
  ),
  
  # Module 3: Database Management
  "3" = list(
    name = "Database Management",
    submodules = list(
      "3.1" = list(
        name = "Table Creation",
        scripts = c(
          # Add project-specific table creation scripts here
        )
      ),
      "3.2" = list(
        name = "Database Operations",
        scripts = c(
          "global_scripts/02_db_utils/fn_database_copy_temp.R",
          "global_scripts/02_db_utils/fn_database_copy_table.R",
          "global_scripts/02_db_utils/fn_database_overwrite.R"
          # Add project-specific database operation scripts here
        )
      )
    )
  ),
  
  # Module 4: Data Analysis and Modeling
  "4" = list(
    name = "Data Analysis and Modeling",
    submodules = list(
      "4.1" = list(
        name = "Statistical Models",
        scripts = c(
          # Add project-specific modeling scripts here
        )
      ),
      "4.2" = list(
        name = "Predictive Analytics",
        scripts = c(
          # Add project-specific prediction scripts here
        )
      )
    )
  ),
  
  # Module 5: Visualization and Reporting
  "5" = list(
    name = "Visualization and Reporting",
    submodules = list(
      "5.1" = list(
        name = "Dashboards",
        scripts = c(
          # Add project-specific dashboard scripts here
        )
      ),
      "5.2" = list(
        name = "Reports",
        scripts = c(
          # Add project-specific reporting scripts here
        )
      )
    )
  ),
  
  # Add more modules as needed for your project
  
  # Module X: Project-Specific Module
  # "X" = list(
  #   name = "Your Custom Module",
  #   submodules = list(
  #     "X.1" = list(
  #       name = "Your Custom Submodule",
  #       scripts = c(
  #         "path/to/implementation.R"
  #       )
  #     )
  #   )
  # )
)

# Utility function to get a specific script by module ID
module_map$get_script <- function(module_id) {
  # Split the module ID into main module and submodule
  parts <- strsplit(module_id, "\\.")[[1]]
  main_module <- parts[1]
  
  if (length(parts) == 1) {
    # If only the main module is specified, return all scripts in that module
    all_scripts <- c()
    submodules <- module_map$modules[[main_module]]$submodules
    for (submodule_id in names(submodules)) {
      all_scripts <- c(all_scripts, submodules[[submodule_id]]$scripts)
    }
    return(all_scripts)
  } else {
    # Return scripts for a specific submodule
    submodule <- parts[2]
    full_id <- paste(main_module, submodule, sep=".")
    return(module_map$modules[[main_module]]$submodules[[full_id]]$scripts)
  }
}

# Utility function to get module name by ID
module_map$get_module_name <- function(module_id) {
  # Split the module ID into main module and submodule
  parts <- strsplit(module_id, "\\.")[[1]]
  main_module <- parts[1]
  
  if (length(parts) == 1) {
    # Return the main module name
    return(module_map$modules[[main_module]]$name)
  } else {
    # Return the submodule name
    submodule <- parts[2]
    full_id <- paste(main_module, submodule, sep=".")
    return(module_map$modules[[main_module]]$submodules[[full_id]]$name)
  }
}

# Utility function to list all modules and submodules
module_map$list_modules <- function() {
  result <- list()
  
  for (module_id in names(module_map$modules)) {
    module <- module_map$modules[[module_id]]
    result[[module_id]] <- module$name
    
    for (submodule_id in names(module$submodules)) {
      submodule <- module$submodules[[submodule_id]]
      result[[submodule_id]] <- submodule$name
    }
  }
  
  return(result)
}

# Utility function to find which module a script belongs to
module_map$find_script_module <- function(script_path) {
  for (module_id in names(module_map$modules)) {
    module <- module_map$modules[[module_id]]
    
    for (submodule_id in names(module$submodules)) {
      submodule <- module$submodules[[submodule_id]]
      
      if (script_path %in% submodule$scripts) {
        return(list(
          module_id = module_id,
          module_name = module$name,
          submodule_id = submodule_id,
          submodule_name = submodule$name
        ))
      }
    }
  }
  
  return(NULL)  # Script not found in mapping
}

# Utility function to source all scripts in a module
module_map$source_module <- function(module_id, verbose = TRUE) {
  scripts <- module_map$get_script(module_id)
  
  if (length(scripts) == 0) {
    if (verbose) warning(paste("No scripts found for module", module_id))
    return(FALSE)
  }
  
  if (verbose) {
    module_name <- module_map$get_module_name(module_id)
    message(paste("Sourcing module", module_id, ":", module_name))
  }
  
  success <- TRUE
  for (script in scripts) {
    if (verbose) message(paste("  Sourcing", script))
    
    result <- tryCatch({
      source(script)
      TRUE
    }, error = function(e) {
      warning(paste("Error sourcing", script, ":", e$message))
      FALSE
    })
    
    success <- success && result
  }
  
  return(success)
}

# Example of how to use this module map in a project script:
if (FALSE) {  # This block won't execute, it's just an example
  # Source the module mapping
  source("path/to/your/project_module_mapping.R")
  
  # Get all Customer DNA scripts and source them
  dna_scripts <- module_map$get_script("1.1")
  for (script in dna_scripts) {
    source(script)
  }
  
  # Or use the convenience function to source all scripts in a module
  module_map$source_module("2.1", verbose = TRUE)
  
  # Find which module a specific script belongs to
  module_info <- module_map$find_script_module("global_scripts/05_data_processing/import_amazon_sales.R")
  if (!is.null(module_info)) {
    print(paste("Script belongs to module", module_info$module_id, "-", module_info$module_name))
  }
}

# Instructions for customizing this template:
# 1. Save this file to your project's update_scripts directory
# 2. Rename it to something like "project_module_mapping.R"
# 3. Update the module structure to reflect your specific project organization
# 4. Add actual script paths for each module and submodule
# 5. Add any project-specific utility functions as needed