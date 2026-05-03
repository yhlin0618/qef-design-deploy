#' Object Identification Utilities - Main Module
#'
#' This file serves as the main entry point for the object identification utilities.
#' It sources all individual function files to make them available.
#'
#' @author Claude
#' @date 2025-04-04

# Get the directory where this script is located
current_dir <- dirname(parent.frame(2)$ofile)
if (is.null(current_dir) || current_dir == "") {
  # Fallback if dirname doesn't work (e.g., in RStudio)
  current_dir <- "04_utils/object_id"
}

# Source all function files
function_dir <- file.path(current_dir, "functions")
function_files <- list.files(function_dir, pattern = "\\.R$", full.names = TRUE)

for (file in function_files) {
  source(file)
}

# Print a message to confirm loading
message("Object identification utilities loaded successfully.")