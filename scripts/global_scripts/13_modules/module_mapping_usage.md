# Using the Module Mapping Template

This guide explains how to implement and use the module mapping template in your project.

## What is Module Mapping?

Module mapping creates a programmatic link between conceptual modules (what functionality does) and implementation scripts (where the code lives). This helps with:

1. **Organization**: Understanding the logical structure of your codebase
2. **Navigation**: Finding specific functionality across many scripts
3. **Documentation**: Automatically generating documentation
4. **Maintenance**: Updating related scripts as a group

## Implementation Steps

### 1. Copy the Template to Your Project

```r
# In R console or script
file.copy(
  from = "update_scripts/global_scripts/13_claude_prompts/module_mapping_template.R",
  to = "update_scripts/project_module_mapping.R"
)
```

### 2. Customize the Module Structure

Edit the file to match your project's organization:

```r
# Module 2: Data Import and Processing
"2" = list(
  name = "Data Import and Processing",
  submodules = list(
    "2.1" = list(
      name = "Your Company Sales Import",  # <-- Customize these names
      scripts = c(
        "global_scripts/05_data_processing/import_amazon_sales.R",
        "local_scripts/your_company/import_company_sales.R"  # <-- Add your scripts
      )
    ),
    # Add other submodules...
  )
)
```

### 3. Integrate with Your Project's Initialization

Add this to your project initialization script:

```r
# Load the module mapping
source("update_scripts/project_module_mapping.R")

# Optionally, make the module mapping available globally
assign("MODULE_MAP", module_map, envir = .GlobalEnv)
```

## Using the Module Map

### Basic Usage

```r
# Get paths to all Customer DNA scripts
dna_scripts <- module_map$get_script("1.1")

# Get the name of a module or submodule
module_name <- module_map$get_module_name("2")  # "Data Import and Processing"
submodule_name <- module_map$get_module_name("2.1")  # "Your Company Sales Import"

# List all modules and submodules
all_modules <- module_map$list_modules()
print(all_modules)
```

### Sourcing Scripts by Module

```r
# Source all scripts in the Customer DNA module
module_map$source_module("1.1")

# Source with verbose output disabled
module_map$source_module("2.1", verbose = FALSE)
```

### Finding Which Module a Script Belongs To

```r
# Find a script's module
module_info <- module_map$find_script_module("global_scripts/05_data_processing/import_amazon_sales.R")

# Display the result
if (!is.null(module_info)) {
  cat("Script belongs to:", module_info$module_id, "-", 
      module_info$module_name, "/", module_info$submodule_name, "\n")
}
```

## Advanced Usage

### Creating a Module Browser Function

```r
browse_modules <- function() {
  modules <- module_map$list_modules()
  
  cat("Available Modules:\n")
  for (id in names(modules)) {
    if (!grepl("\\.", id)) {  # Main modules only
      cat("\n", id, ": ", modules[[id]], "\n", sep="")
      
      # List submodules
      for (sub_id in names(modules)) {
        if (grepl(paste0("^", id, "\\."), sub_id)) {
          cat("  ", sub_id, ": ", modules[[sub_id]], "\n", sep="")
          
          # List scripts
          scripts <- module_map$get_script(sub_id)
          for (script in scripts) {
            cat("    - ", script, "\n", sep="")
          }
        }
      }
    }
  }
}
```

### Generating Documentation

```r
generate_module_docs <- function(output_file = "module_documentation.md") {
  modules <- module_map$list_modules()
  
  # Create header
  lines <- c(
    "# Project Module Documentation",
    "",
    "Generated on: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    "",
    "## Modules Overview",
    ""
  )
  
  # Add each module
  for (id in names(modules)) {
    if (!grepl("\\.", id)) {  # Main modules only
      lines <- c(lines, paste("### Module", id, ":", modules[[id]]), "")
      
      # Add submodules
      for (sub_id in names(modules)) {
        if (grepl(paste0("^", id, "\\."), sub_id)) {
          lines <- c(lines, paste("#### Module", sub_id, ":", modules[[sub_id]]), "")
          
          # Add script list
          lines <- c(lines, "Scripts:")
          scripts <- module_map$get_script(sub_id)
          for (script in scripts) {
            lines <- c(lines, paste("- `", script, "`", sep=""))
          }
          lines <- c(lines, "")
        }
      }
    }
  }
  
  # Write to file
  writeLines(lines, output_file)
  message("Documentation written to ", output_file)
}
```

### Checking for Missing or Redundant Scripts

```r
validate_module_map <- function() {
  # Get all scripts from module map
  all_mapped_scripts <- c()
  modules <- module_map$list_modules()
  
  for (id in names(modules)) {
    if (grepl("\\.", id)) {  # Submodules only
      scripts <- module_map$get_script(id)
      all_mapped_scripts <- c(all_mapped_scripts, scripts)
    }
  }
  
  # Get actual R scripts
  all_r_files <- list.files(
    path = c("update_scripts", "local_scripts"),
    pattern = "\\.R$",
    recursive = TRUE,
    full.names = TRUE
  )
  
  # Check for unmapped scripts
  unmapped <- setdiff(all_r_files, all_mapped_scripts)
  if (length(unmapped) > 0) {
    cat("Unmapped scripts:\n")
    cat(paste(" -", unmapped), sep="\n")
  }
  
  # Check for mapped scripts that don't exist
  missing <- setdiff(all_mapped_scripts, all_r_files)
  if (length(missing) > 0) {
    cat("\nMapped scripts that don't exist:\n")
    cat(paste(" -", missing), sep="\n")
  }
  
  # Check for redundantly mapped scripts
  duplicates <- all_mapped_scripts[duplicated(all_mapped_scripts)]
  if (length(duplicates) > 0) {
    cat("\nScripts mapped in multiple modules:\n")
    cat(paste(" -", duplicates), sep="\n")
  }
}
```

## Best Practices

1. **Keep the mapping up to date**: Update the mapping when adding or moving scripts
2. **Use consistent IDs**: Maintain the numeric hierarchy (1, 1.1, 1.2, etc.)
3. **Be specific about paths**: Use relative paths from the project root
4. **Consider script dependencies**: Order modules so that dependencies come first
5. **Document the module purpose**: Include clear descriptions for each module

## Troubleshooting

- **Script not found**: Ensure paths are correct and relative to where the module map is used
- **Duplicate scripts**: Each script should ideally belong to only one submodule
- **Module structure too complex**: Keep the hierarchy simple (max 2 levels)
- **Performance issues**: For very large projects, consider lazy-loading techniques