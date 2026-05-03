#!/usr/bin/env Rscript
#
# generate_function_docs.R
#
# This script automatically generates markdown documentation from roxygen2 comments
# in R source files. It extracts documentation for functions and creates organized
# markdown files that can be viewed directly or converted to HTML/PDF.
#
# Usage:
#   Rscript generate_function_docs.R [output_dir]
#
# If output_dir is not provided, documentation will be generated in "docs/functions/"

# Load required packages
required_packages <- c("knitr", "stringr")
for (pkg in required_packages) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    install.packages(pkg)
  }
  library(pkg, character.only = TRUE)
}

# Process command line arguments
args <- commandArgs(trailingOnly = TRUE)
base_dir <- getwd()

# Find global_scripts directory by walking up the directory tree
find_global_scripts <- function(start_dir) {
  # Try direct match for current directory
  if (basename(start_dir) == "global_scripts") {
    return(start_dir)
  }
  
  # Try one level down
  if (file.exists(file.path(start_dir, "global_scripts"))) {
    return(file.path(start_dir, "global_scripts"))
  }
  
  # Try in update_scripts
  if (file.exists(file.path(start_dir, "update_scripts", "global_scripts"))) {
    return(file.path(start_dir, "update_scripts", "global_scripts"))
  }
  
  # Check if we're in a subdirectory of global_scripts
  parts <- strsplit(start_dir, .Platform$file.sep)[[1]]
  for (i in seq_along(parts)) {
    if (parts[i] == "global_scripts") {
      # Reconstruct path up to global_scripts
      return(do.call(file.path, as.list(parts[1:i])))
    }
  }
  
  # No luck finding global_scripts, use start_dir as fallback
  return(start_dir)
}

# Get path to the global_scripts directory
global_scripts_dir <- find_global_scripts(base_dir)
cat("Located global_scripts directory at:", global_scripts_dir, "\n")

# Default output directory (in global_scripts)
output_dir <- file.path(global_scripts_dir, "docs", "functions")

# Override output directory if specified
if (length(args) > 0) {
  output_dir <- args[1]
}

# Default scripts directory (focus on global_scripts)
scripts_dir <- global_scripts_dir

# Create output directory if it doesn't exist
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

# Function to extract roxygen blocks and corresponding function names
extract_roxygen_blocks <- function(file_path) {
  tryCatch({
    # Get file contents
    lines <- readLines(file_path, warn = FALSE, encoding = "UTF-8")
    
    # Extract roxygen blocks and corresponding function names
    roxygen_blocks <- list()
    current_block <- character()
    in_roxygen <- FALSE
    
    for (i in seq_along(lines)) {
      line <- lines[i]
      
      # Start of roxygen block
      if (grepl("^#'", line) && !in_roxygen) {
        in_roxygen <- TRUE
        current_block <- line
      }
      # Continue roxygen block
      else if (grepl("^#'", line) && in_roxygen) {
        current_block <- c(current_block, line)
      }
      # End of roxygen block
      else if (!grepl("^#'", line) && in_roxygen) {
        in_roxygen <- FALSE
        
        # Find function name (look for next line with function definition)
        for (j in i:min(i+10, length(lines))) {
          if (j <= length(lines) && 
              (grepl("<- function", lines[j]) || 
               grepl("= function", lines[j]) || 
               grepl("^function", lines[j]))) {
            
            func_line <- lines[j]
            # Extract function name
            if (grepl("<- function", func_line)) {
              func_name <- trimws(gsub("<- function.*", "", func_line))
            } else if (grepl("= function", func_line)) {
              func_name <- trimws(gsub("= function.*", "", func_line))
            } else if (grepl("^function", func_line)) {
              # For functions defined directly (rare in R)
              # Try to find assignment on previous line
              if (j > 1 && grepl("<-$", lines[j-1])) {
                func_name <- trimws(gsub("<-$", "", lines[j-1]))
              } else {
                func_name <- paste0("anonymous_", i)  # Use line number as identifier
              }
            }
            
            # Store the roxygen block with the function name
            roxygen_blocks[[func_name]] <- current_block
            break
          }
        }
      }
    }
    
    return(roxygen_blocks)
  }, error = function(e) {
    warning("Error processing file ", file_path, ": ", e$message)
    return(list())
  })
}

# Function to convert roxygen block to markdown
roxygen_to_markdown <- function(block) {
  # Remove roxygen markers
  md_lines <- gsub("^#' ?", "", block)
  
  # Process @tags specially
  in_examples <- FALSE
  in_params <- FALSE
  result <- character()
  
  for (i in seq_along(md_lines)) {
    line <- md_lines[i]
    
    # Handle @param tag
    if (grepl("^@param", line)) {
      if (!in_params) {
        result <- c(result, "", "## Parameters", "")
        in_params <- TRUE
      }
      # Format parameter as list product with bold name
      param_parts <- strsplit(sub("^@param\\s+", "", line), "\\s+", 2)[[1]]
      if (length(param_parts) == 2) {
        param_name <- param_parts[1]
        param_desc <- param_parts[2]
        result <- c(result, paste0("- **", param_name, "**: ", param_desc))
      } else {
        result <- c(result, paste0("- **", param_parts[1], "**"))
      }
    }
    # Handle @return tag
    else if (grepl("^@return", line)) {
      return_text <- sub("^@return\\s*", "", line)
      result <- c(result, "", "## Return Value", "", return_text)
    }
    # Handle @examples tag
    else if (grepl("^@examples", line)) {
      in_examples <- TRUE
      result <- c(result, "", "## Examples", "")
      example_code <- sub("^@examples\\s*", "", line)
      if (nzchar(example_code)) {
        result <- c(result, "```r", example_code)
      } else {
        result <- c(result, "```r")
      }
    }
    # Handle continued example
    else if (in_examples) {
      if (grepl("^@", line)) {
        # End of examples section
        in_examples <- FALSE
        result <- c(result, "```")
        
        # Process the new tag (recursive call to avoid code duplication)
        result <- c(result, roxygen_to_markdown(paste0("#' ", line)))
      } else {
        result <- c(result, line)
      }
    }
    # Handle @note tag
    else if (grepl("^@note", line)) {
      note_text <- sub("^@note\\s*", "", line)
      result <- c(result, "", "## Note", "", note_text)
    }
    # Handle @details tag
    else if (grepl("^@details", line)) {
      details_text <- sub("^@details\\s*", "", line)
      result <- c(result, "", "## Details", "", details_text)
    }
    # Handle @section tag
    else if (grepl("^@section", line)) {
      section_parts <- strsplit(sub("^@section\\s+", "", line), ":")[[1]]
      section_title <- section_parts[1]
      section_content <- if (length(section_parts) > 1) section_parts[2] else ""
      result <- c(result, "", paste("##", section_title), "", section_content)
    }
    # Handle other @ tags generically
    else if (grepl("^@[a-zA-Z]", line)) {
      tag <- sub("^@([a-zA-Z]+).*", "\\1", line)
      tag_content <- sub(paste0("^@", tag, "\\s*"), "", line)
      result <- c(result, "", paste("##", stringr::str_to_title(tag)), "", tag_content)
    }
    # Regular documentation text
    else if (!in_examples && !grepl("^@", line)) {
      result <- c(result, line)
    }
  }
  
  # Close code block if we're still in examples
  if (in_examples) {
    result <- c(result, "```")
  }
  
  return(result)
}

# Find all R files, excluding 99_archive directory
cat("Searching for R files in", scripts_dir, "\n")
r_files <- list.files(scripts_dir, pattern = "\\.R$", recursive = TRUE, full.names = TRUE)
# Filter out files from 99_archive
r_files <- r_files[!grepl("99_archive", r_files)]
cat("Found", length(r_files), "R files (excluding 99_archive directory)\n")

# Map to store documentation by directory
dir_docs <- list()

# Process each file
for (file_path in r_files) {
  # Extract the subdirectory for organization
  rel_path <- gsub(paste0("^", gsub("([.\\\\+])", "\\\\\\1", scripts_dir), "/?"), "", file_path)
  dir_name <- dirname(rel_path)
  if (dir_name == ".") dir_name <- "root"
  
  if (!dir_name %in% names(dir_docs)) {
    dir_docs[[dir_name]] <- list()
  }
  
  # Extract roxygen blocks
  roxygen_blocks <- extract_roxygen_blocks(file_path)
  
  if (length(roxygen_blocks) > 0) {
    # Create a documentation object for this file
    doc_obj <- list(
      file_name = basename(file_path),
      rel_path = rel_path,
      functions = list()
    )
    
    # Process each roxygen block
    for (func_name in names(roxygen_blocks)) {
      block <- roxygen_blocks[[func_name]]
      
      # Convert to markdown
      md_content <- roxygen_to_markdown(block)
      
      # Store in the documentation object
      doc_obj$functions[[func_name]] <- md_content
    }
    
    # Add to the directory-specific list
    dir_docs[[dir_name]][[basename(file_path)]] <- doc_obj
  }
}

# Create directory for each module
module_index <- c(
  "# Function Documentation Index",
  "",
  "This document provides links to all documented modules.",
  "",
  "## Modules"
)

# Process each directory
for (dir_name in names(dir_docs)) {
  # Create directory for this module
  module_dir <- file.path(output_dir, dir_name)
  dir.create(module_dir, recursive = TRUE, showWarnings = FALSE)
  
  # Create index file for this module
  module_index_path <- file.path(module_dir, "index.md")
  module_index_content <- c(
    paste0("# ", dir_name, " Module Documentation"),
    "",
    paste0("This module contains ", length(dir_docs[[dir_name]]), " files with documented functions."),
    "",
    "## Files"
  )
  
  files_in_dir <- dir_docs[[dir_name]]
  
  # Add module to main index
  module_rel_path <- file.path(dir_name, "index.md")
  module_index <- c(module_index, paste0("- [", dir_name, "](", module_rel_path, ")"))
  
  # Process each file in this directory
  for (file_name in names(files_in_dir)) {
    doc_obj <- files_in_dir[[file_name]]
    
    # Skip if no functions
    if (length(doc_obj$functions) == 0) next
    
    # Create markdown file for this file
    file_id <- gsub("\\.[rR]$", "", file_name)
    md_file <- paste0(file_id, ".md")
    md_path <- file.path(module_dir, md_file)
    
    # Add to module index
    module_index_content <- c(
      module_index_content, 
      paste0("- [", file_id, "](", md_file, ") - ", length(doc_obj$functions), " functions")
    )
    
    # Create content for this file
    file_content <- c(
      paste0("# ", file_id),
      "",
      paste0("Source: `", doc_obj$rel_path, "`"),
      "",
      "## Functions"
    )
    
    # Add table of contents for functions
    file_content <- c(file_content, "", "**Function List:**")
    
    for (func_name in names(doc_obj$functions)) {
      file_content <- c(
        file_content,
        paste0("- [", func_name, "](#", tolower(gsub("[^a-zA-Z0-9]", "-", func_name)), ")")
      )
    }
    
    # Add function documentation
    for (func_name in names(doc_obj$functions)) {
      md_content <- doc_obj$functions[[func_name]]
      
      # Extract first line as title
      title <- if (length(md_content) > 0) md_content[1] else func_name
      
      file_content <- c(
        file_content,
        "",
        paste0("### ", func_name),
        ""
      )
      
      # Add documentation content
      file_content <- c(file_content, md_content)
      
      # Add separator
      file_content <- c(file_content, "", "---", "")
    }
    
    # Write the file
    writeLines(file_content, md_path, useBytes = TRUE)
    cat("Generated documentation for", file_name, "at", md_path, "\n")
  }
  
  # Write module index
  writeLines(module_index_content, module_index_path, useBytes = TRUE)
  cat("Generated index for", dir_name, "module at", module_index_path, "\n")
}

# Write main index
main_index_path <- file.path(output_dir, "index.md")
writeLines(module_index, main_index_path, useBytes = TRUE)
cat("Generated main index at", main_index_path, "\n")

# Generate HTML index if knitr and rmarkdown are available
if (requireNamespace("rmarkdown", quietly = TRUE)) {
  tryCatch({
    # Add YAML header to markdown files to fix title warnings
    add_yaml_header <- function(md_file, title) {
      content <- readLines(md_file)
      # Extract title from first header line if possible
      if (!missing(title)) {
        yaml_header <- c("---", paste0("title: \"", title, "\""), "---", "")
      } else if (length(content) > 0 && grepl("^# ", content[1])) {
        title <- gsub("^# ", "", content[1])
        yaml_header <- c("---", paste0("title: \"", title, "\""), "---", "")
      } else {
        dir_name <- basename(dirname(md_file))
        file_name <- tools::file_path_sans_ext(basename(md_file))
        yaml_header <- c("---", paste0("title: \"", dir_name, " - ", file_name, "\""), "---", "")
      }
      writeLines(c(yaml_header, content), md_file)
    }
    
    # Add title to main index
    add_yaml_header(main_index_path, "Function Documentation Index")
    
    # Render main index to HTML
    html_index <- file.path(output_dir, "index.html")
    rmarkdown::render(main_index_path, output_file = html_index, quiet = TRUE)
    cat("Generated HTML index at", html_index, "\n")
    
    # Try to generate HTML for each module too
    for (dir_name in names(dir_docs)) {
      module_index_path <- file.path(output_dir, dir_name, "index.md")
      if (file.exists(module_index_path)) {
        # Add title to module index
        add_yaml_header(module_index_path, paste(dir_name, "Module Documentation"))
        
        # Render module index to HTML
        html_path <- file.path(output_dir, dir_name, "index.html")
        rmarkdown::render(module_index_path, output_file = html_path, quiet = TRUE)
        cat("Generated HTML for", dir_name, "module\n")
        
        # Process individual function files in this module
        md_files <- list.files(file.path(output_dir, dir_name), pattern = "\\.md$", full.names = TRUE)
        md_files <- md_files[!grepl("index\\.md$", md_files)]
        
        for (md_file in md_files) {
          # Add title to function file
          add_yaml_header(md_file)
          
          # Render function file to HTML
          html_path <- gsub("\\.md$", ".html", md_file)
          rmarkdown::render(md_file, output_file = html_path, quiet = TRUE)
        }
      }
    }
  }, error = function(e) {
    warning("Could not generate HTML: ", e$message)
  })
}

cat("\nDocumentation generation complete!\n")
cat("Documentation is available at", output_dir, "\n")