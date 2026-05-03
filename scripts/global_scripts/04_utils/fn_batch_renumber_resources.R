#' Batch Renumber Multiple Resources
#'
#' @description
#' Performs batch renumbering operations based on a mapping table.
#'
#' @param mapping_table A data frame with old_id, new_id, and name columns
#' @return A list with success status and details for each operation
#'
batch_renumber_resources <- function(mapping_table) {
  # Validate the mapping table
  required_cols <- c("old_id", "new_id", "name")
  missing_cols <- setdiff(required_cols, names(mapping_table))
  
  if (length(missing_cols) > 0) {
    return(list(
      success = FALSE,
      error = paste("Missing required columns in mapping table:", paste(missing_cols, collapse = ", "))
    ))
  }
  
  # Check for conflicts in the mapping
  if (any(duplicated(mapping_table$new_id))) {
    dup_ids <- mapping_table$new_id[duplicated(mapping_table$new_id)]
    return(list(
      success = FALSE,
      error = paste("Duplicate target IDs in mapping:", paste(dup_ids, collapse = ", "))
    ))
  }
  
  # Create a backup directory for the original files
  backup_dir <- "renaming_backup"
  if (!dir.exists(backup_dir)) {
    dir.create(backup_dir)
  }
  
  # Timestamp for the batch operation
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  
  # Initialize results
  results <- list()
  
  # First pass: Verify all operations
  for (i in 1:nrow(mapping_table)) {
    old_id <- mapping_table$old_id[i]
    new_id <- mapping_table$new_id[i]
    name <- mapping_table$name[i]
    
    old_filename <- paste0(old_id, "_", name, ".md")
    new_filename <- paste0(new_id, "_", name, ".md")
    
    # Check if the old file exists
    if (!file.exists(old_filename)) {
      results[[i]] <- list(
        success = FALSE,
        error = paste("Source file not found:", old_filename)
      )
      next
    }
    
    # Check if the new name is unique
    if (file.exists(new_filename) && old_filename != new_filename) {
      results[[i]] <- list(
        success = FALSE,
        error = paste("Target file already exists:", new_filename)
      )
      next
    }
  }
  
  # Check if any verification failed
  verification_failed <- sapply(results, function(r) !is.null(r) && !r$success)
  if (any(verification_failed)) {
    return(list(
      success = FALSE,
      error = "Verification failed for some operations",
      details = results[verification_failed]
    ))
  }
  
  # Backup all files before proceeding
  for (i in 1:nrow(mapping_table)) {
    old_id <- mapping_table$old_id[i]
    name <- mapping_table$name[i]
    
    old_filename <- paste0(old_id, "_", name, ".md")
    backup_filename <- file.path(backup_dir, paste0(old_filename, ".", timestamp, ".bak"))
    
    file.copy(old_filename, backup_filename, overwrite = TRUE)
  }
  
  # Second pass: Execute all renaming operations
  for (i in 1:nrow(mapping_table)) {
    old_id <- mapping_table$old_id[i]
    new_id <- mapping_table$new_id[i]
    name <- mapping_table$name[i]
    
    # Call the individual renumber function
    result <- renumber_resource(old_id, new_id, name)
    results[[i]] <- result
  }
  
  # Check if any operation failed
  operations_failed <- sapply(results, function(r) !r$success)
  if (any(operations_failed)) {
    # Attempt to rollback failed operations
    for (i in which(operations_failed)) {
      old_id <- mapping_table$old_id[i]
      name <- mapping_table$name[i]
      
      old_filename <- paste0(old_id, "_", name, ".md")
      backup_filename <- file.path(backup_dir, paste0(old_filename, ".", timestamp, ".bak"))
      
      if (file.exists(backup_filename)) {
        file.copy(backup_filename, old_filename, overwrite = TRUE)
      }
    }
    
    return(list(
      success = FALSE,
      error = "Some renaming operations failed",
      details = results[operations_failed],
      rolled_back = which(operations_failed)
    ))
  }
  
  # Check system consistency after all operations
  consistency_issues <- verify_consistency()
  
  # Return the results
  return(list(
    success = TRUE,
    operations = nrow(mapping_table),
    results = results,
    consistency_issues = consistency_issues
  ))
}
