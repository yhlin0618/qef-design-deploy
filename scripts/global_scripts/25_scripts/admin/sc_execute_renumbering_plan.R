#' Execute Principles Renumbering Plan
#'
#' @description
#' This script implements the renumbering plan for principles as outlined in
#' 2025_04_04_principles_renumbering_plan.md
#'
#' @author Claude
#' @date 2025-04-04

# Set working directory to the 00_principles directory
principles_dir <- "/Users/che/Library/CloudStorage/Dropbox/precision_marketing/precision_marketing_WISER/precision_marketing_app/update_scripts/global_scripts/00_principles"
setwd(principles_dir)

# Load the renumbering module
source("M00_renumbering_principles/M05_fn_renumber_principles.R")

# Create backup directory if it doesn't exist
backup_dir <- "renumbering_backup"
if (!dir.exists(backup_dir)) {
  dir.create(backup_dir)
}

# Backup the entire directory before starting
timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
backup_cmd <- paste0("cp -r * ", backup_dir, "/backup_", timestamp)
system(backup_cmd)

# Mapping table for rules as defined in the plan
rules_mapping <- data.frame(
  old_id = c("R06", "R07", "R08", "R09", "R11", "R12", "R13", "R14", "R15", 
             "R17", "R18", "R19", "R20_language_standard_adherence", "R20_pseudocode_standard_adherence", 
             "R23", "R24", "R25", "R26", "R27", "R28", "R29", "R30", "R31", "R32"),
  new_id = c("R05", "R06", "R07", "R08", "R09", "R10", "R11", "R12", "R13", 
             "R14", "R15", "R16", "R17", "R18", "R19", "R20", "R21", "R22", 
             "R23", "R24", "R25", "R26", "R27", "R28"),
  name = c("temporary_file_handling", "module_naming_convention", "update_script_naming", 
           "global_scripts_synchronization", "ui_server_defaults_triple", 
           "package_consistency_naming", "hybrid_sidebar_pattern", "minimal_modification", 
           "initialization_sourcing", "ui_hierarchy", "defaults_from_triple", 
           "yaml_parameter_configuration", "language_standard_adherence", 
           "pseudocode_standard_adherence", "object_naming_convention", 
           "logic_document_creation", "one_function_one_file", "app_mode_naming_simplicity", 
           "object_file_name_translation", "type_token_naming", "terminology_synonym_mapping", 
           "type_token_distinction_application", "data_frame_creation_strategy", 
           "archiving_standard"),
  stringsAsFactors = FALSE
)

# Fix the R20 names by updating the IDs to match the filenames
r20_language_row <- which(rules_mapping$old_id == "R20_language_standard_adherence")
r20_pseudocode_row <- which(rules_mapping$old_id == "R20_pseudocode_standard_adherence")

# Update to actual filenames
rules_mapping$old_id[r20_language_row] <- "R20"
rules_mapping$old_id[r20_pseudocode_row] <- "R20"

cat("Starting rule renumbering...\n")

# Execute rule renumbering
rule_results <- M00_renumbering_principles$batch_renumber(rules_mapping)

if (!rule_results$success) {
  cat("Error in rule renumbering:\n")
  print(rule_results$error)
  if (!is.null(rule_results$details)) {
    print(rule_results$details)
  }
  cat("Renumbering aborted. Check the error messages.\n")
  quit(status = 1)
}

cat("Rule renumbering completed successfully.\n")
cat("Operations performed:", rule_results$operations, "\n")

# Now handle module renumbering (M05 to M00)
cat("\nStarting module renumbering...\n")
cat("Note: Module renumbering must be done carefully as the module can't rename itself while running\n")

# Create a new directory for M00
if (!dir.exists("M00_renumbering_principles")) {
  dir.create("M00_renumbering_principles")
}

# First copy all files to the new location
system("cp -r M00_renumbering_principles/* M00_renumbering_principles/")

# Update the module main file
m05_main_file <- readLines("M00_renumbering_principles/M05_fn_renumber_principles.R")
m00_main_file <- gsub("M00_renumbering_principles", "M00_renumbering_principles", m05_main_file)
writeLines(m00_main_file, "M00_renumbering_principles/M00_fn_renumber_principles.R")

# Remove the old main file
file.remove("M00_renumbering_principles/M05_fn_renumber_principles.R")

# Update references to M05 in other files
m00_files <- list.files("M00_renumbering_principles", recursive = TRUE, full.names = TRUE)
for (file in m00_files) {
  if (file.exists(file) && !dir.exists(file)) {
    content <- readLines(file)
    updated_content <- gsub("M00_renumbering_principles", "M00_renumbering_principles", content)
    writeLines(updated_content, file)
  }
}

# Update README.md in the module
readme_path <- "M00_renumbering_principles/README.md"
if (file.exists(readme_path)) {
  readme_content <- readLines(readme_path)
  updated_readme <- gsub("M00_renumbering_principles", "M00_renumbering_principles", readme_content)
  updated_readme <- gsub("M05 - Renumbering Principles", "M00 - Renumbering Principles", updated_readme)
  writeLines(updated_readme, readme_path)
}

cat("Module files updated. Original module is still intact at M00_renumbering_principles.\n")
cat("You will need to manually remove the old module directory after verifying the new one works correctly.\n")

# Update references to M05 in the main principles directory
principles_files <- list.files(pattern = "*.md", recursive = FALSE)
for (file in principles_files) {
  if (file.exists(file)) {
    content <- readLines(file)
    updated_content <- gsub("M00_renumbering_principles", "M00_renumbering_principles", content)
    updated_content <- gsub('"M05":', '"M00":', updated_content)
    updated_content <- gsub('"M05"', '"M00"', updated_content)
    writeLines(updated_content, file)
  }
}

# Update reference in record files
record_files <- list.files("records", pattern = "*.md", full.names = TRUE)
for (file in record_files) {
  if (file.exists(file)) {
    content <- readLines(file)
    updated_content <- gsub("M00_renumbering_principles", "M00_renumbering_principles", content)
    updated_content <- gsub('"M05":', '"M00":', updated_content)
    updated_content <- gsub('"M05"', '"M00"', updated_content)
    writeLines(updated_content, file)
  }
}

cat("\nVerifying system consistency...\n")

# Verify system consistency
issues <- M00_renumbering_principles$verify()
if (is.null(issues)) {
  cat("System is consistent!\n")
} else {
  cat("Issues found in system consistency check:\n")
  print(issues)
}

cat("\nRenumbering plan execution completed.\n")
cat("Next steps:\n")
cat("1. Verify all functionality\n")
cat("2. Update the README.md to reflect the new numbering system\n")
cat("3. Once verified, manually remove M00_renumbering_principles directory\n")
