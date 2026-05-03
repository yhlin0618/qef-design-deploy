#' Renumber a Sequenced Resource
#'
#' @description
#' Specifically for renumbering sequenced resources (MP, P, R).
#'
#' @param old_id The current id (e.g., "P16")
#' @param new_id The new id (e.g., "P07")
#' @param name The name part of the resource (e.g., "app_bottom_up_construction")
#' @return A list with success status and details
#'
renumber_resource <- function(old_id, new_id, name) {
  # Construct the filenames
  old_filename <- paste0(old_id, "_", name, ".md")
  new_filename <- paste0(new_id, "_", name, ".md")
  
  # Check if the old file exists
  if (!file.exists(old_filename)) {
    return(list(
      success = FALSE,
      error = paste("Source file not found:", old_filename)
    ))
  }
  
  # Check if the new name is unique
  if (file.exists(new_filename)) {
    return(list(
      success = FALSE,
      error = paste("Target file already exists:", new_filename)
    ))
  }
  
  # Create a backup of the original file
  backup_filename <- paste0(old_filename, ".bak")
  file.copy(old_filename, backup_filename, overwrite = TRUE)
  
  # Read the file content
  content <- readLines(old_filename)
  
  # Replace the id in the YAML frontmatter
  content <- gsub(paste0('id: "', old_id, '"'), paste0('id: "', new_id, '"'), content)
  
  # Replace any self-references in the content
  content <- gsub(paste0(old_id, " \\("), paste0(new_id, " ("), content)
  
  # Write the updated content to the new file
  writeLines(content, new_filename)
  
  # Find all references to the old ID in other files
  references <- system(paste0('grep -l "', old_id, '"', ' --include="*.md" .'), intern = TRUE)
  
  # Update references in other files
  for (file in references) {
    if (file != old_filename && file != new_filename && file != backup_filename) {
      # Read the file
      file_content <- readLines(file)
      
      # Replace references in YAML front matter
      file_content <- gsub(paste0('"', old_id, '": '), paste0('"', new_id, '": '), file_content)
      
      # Write the updated content
      writeLines(file_content, file)
    }
  }
  
  # Update README.md if it exists
  if (file.exists("README.md")) {
    readme_content <- readLines("README.md")
    readme_content <- gsub(old_filename, new_filename, readme_content)
    readme_content <- gsub(paste0("- ", old_filename), paste0("- ", new_filename), readme_content)
    writeLines(readme_content, "README.md")
  }
  
  # Remove the old file only after all references are updated
  if (file.exists(new_filename)) {
    file.remove(old_filename)
  }
  
  # Return success information
  return(list(
    success = TRUE,
    old_id = old_id,
    new_id = new_id,
    old_filename = old_filename,
    new_filename = new_filename,
    references_updated = length(references)
  ))
}
