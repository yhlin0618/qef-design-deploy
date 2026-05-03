#' Create Update Script from Template
#' 
#' Generates a new update script following R113 Four-Part Structure
#' 
#' @param script_name Name of the script (e.g., "amz_D03_01")
#' @param description Brief description of what the script does
#' @param derivation_id Derivation identifier (e.g., "D03_01")
#' @param derivation_desc Full description of the derivation step
#' @param main_operation Description of the main operation
#' @param verification_desc Description of what is being verified
#' @param target_dir Directory where the script should be created
#' @param overwrite Whether to overwrite existing file (default: FALSE)
#' 
#' @return Path to the created script file
#' 
#' @examples
#' create_update_script(
#'   script_name = "amz_D04_00",
#'   description = "Process Amazon customer segments",
#'   derivation_id = "D04_00",
#'   derivation_desc = "Customer Segmentation Analysis",
#'   main_operation = "customer segmentation processing",
#'   verification_desc = "customer segment results",
#'   target_dir = "scripts/update_scripts"
#' )
create_update_script <- function(script_name,
                                description,
                                derivation_id,
                                derivation_desc,
                                main_operation,
                                verification_desc,
                                target_dir = "scripts/update_scripts",
                                overwrite = FALSE) {
  
  # Load the template
  template_path <- file.path("scripts", "global_scripts", "00_principles", 
                            "principles", "R113_update_script_template.R")
  
  if (!file.exists(template_path)) {
    stop("Template file not found: ", template_path)
  }
  
  # Read template content
  template_content <- readLines(template_path)
  
  # Replace placeholders
  script_content <- template_content
  script_content <- gsub("\\[SCRIPT_NAME\\]", script_name, script_content)
  script_content <- gsub("\\[BRIEF_DESCRIPTION\\]", description, script_content)
  script_content <- gsub("\\[DERIVATION_ID\\]", derivation_id, script_content)
  script_content <- gsub("\\[DERIVATION_DESCRIPTION\\]", derivation_desc, 
                        script_content)
  script_content <- gsub("\\[MAIN_OPERATION_DESCRIPTION\\]", main_operation, 
                        script_content)
  script_content <- gsub("\\[VERIFICATION_DESCRIPTION\\]", verification_desc, 
                        script_content)
  
  # Create target directory if it doesn't exist
  if (!dir.exists(target_dir)) {
    dir.create(target_dir, recursive = TRUE)
  }
  
  # Define output file path
  output_file <- file.path(target_dir, paste0(script_name, ".R"))
  
  # Check if file exists and overwrite setting
  if (file.exists(output_file) && !overwrite) {
    stop("File already exists: ", output_file, 
         ". Use overwrite=TRUE to replace it.")
  }
  
  # Write the script file
  writeLines(script_content, output_file)
  
  message("Update script created: ", output_file)
  message("Remember to:")
  message("1. Review and customize the TODO sections")
  message("2. Add your specific business logic")
  message("3. Implement appropriate verification tests")
  message("4. Test the script before production use")
  
  return(output_file)
}

#' Create Update Script Interactively
#' 
#' Interactive version that prompts for all required information
#' 
#' @param target_dir Directory where the script should be created
#' 
#' @return Path to the created script file
create_update_script_interactive <- function(target_dir = "scripts/update_scripts") {
  
  cat("=== Update Script Generator ===\n")
  cat("Following R113: Four-Part Update Script Structure\n\n")
  
  # Collect information interactively
  script_name <- readline(prompt = "Script name (e.g., amz_D04_00): ")
  description <- readline(prompt = "Brief description: ")
  derivation_id <- readline(prompt = "Derivation ID (e.g., D04_00): ")
  derivation_desc <- readline(prompt = "Full derivation description: ")
  main_operation <- readline(prompt = "Main operation description: ")
  verification_desc <- readline(prompt = "Verification description: ")
  
  # Ask about overwrite
  overwrite_input <- readline(prompt = "Overwrite existing file? (y/N): ")
  overwrite <- tolower(overwrite_input) %in% c("y", "yes")
  
  # Create the script
  result <- create_update_script(
    script_name = script_name,
    description = description,
    derivation_id = derivation_id,
    derivation_desc = derivation_desc,
    main_operation = main_operation,
    verification_desc = verification_desc,
    target_dir = target_dir,
    overwrite = overwrite
  )
  
  return(result)
}