#' Demonstration of M00_renumbering_principles module
#'
#' This script demonstrates the functionality of the M00_renumbering_principles module.
#' It simulates renumbering operations in a safe environment.
#'

# Load the module
source("M05_fn_renumber_principles.R")

# Create a temporary directory for our demonstration
demo_dir <- "demo_files"
if (!dir.exists(demo_dir)) {
  dir.create(demo_dir)
}

# Create some example files for the demonstration
cat('---
id: "P16"
title: "App Bottom Up Construction"
type: "principle"
---

# P16: App Bottom Up Construction

This is a sample principle.
', file = file.path(demo_dir, "P16_app_bottom_up_construction.md"))

cat('---
id: "P04"
title: "App Construction"
type: "principle"
---

# P04: App Construction

This principle references P16 (App Bottom Up Construction).
', file = file.path(demo_dir, "P04_app_construction.md"))

cat('# README

- P16_app_bottom_up_construction.md
- P04_app_construction.md
', file = file.path(demo_dir, "README.md"))

# Print the initial state
cat("===== Initial State =====\n")
cat("Files in demo directory:\n")
print(list.files(demo_dir))
cat("\n")
cat("File contents:\n")
for (file in list.files(demo_dir, full.names = TRUE)) {
  cat(paste0("--- ", basename(file), " ---\n"))
  cat(readLines(file), sep = "\n")
  cat("\n\n")
}

# Change to the demo directory
original_dir <- getwd()
setwd(demo_dir)

# Use the module to renumber a principle
cat("===== Renumbering P16 to P07 =====\n")
result <- M00_renumbering_principles$renumber("P16", "P07", "app_bottom_up_construction")
print(result)
cat("\n")

# Print the state after renumbering
cat("===== After Renumbering =====\n")
cat("Files in directory:\n")
print(list.files())
cat("\n")
cat("File contents:\n")
for (file in list.files()) {
  cat(paste0("--- ", file, " ---\n"))
  cat(readLines(file), sep = "\n")
  cat("\n\n")
}

# Verify system consistency
cat("===== Consistency Check =====\n")
issues <- M00_renumbering_principles$verify()
if (is.null(issues)) {
  cat("System is consistent.\n")
} else {
  cat("Consistency issues found:\n")
  print(issues)
}
cat("\n")

# Use the module to perform batch renumbering
cat("===== Batch Renumbering =====\n")
mapping_table <- data.frame(
  old_id = c("P07", "P04"),
  new_id = c("P05", "P12"),
  name = c("app_bottom_up_construction", "app_construction"),
  stringsAsFactors = FALSE
)
cat("Mapping table:\n")
print(mapping_table)
cat("\n")

result <- M00_renumbering_principles$batch_renumber(mapping_table)
print(result)
cat("\n")

# Print the final state
cat("===== Final State =====\n")
cat("Files in directory:\n")
print(list.files())
cat("\n")
cat("File contents:\n")
for (file in list.files()) {
  cat(paste0("--- ", file, " ---\n"))
  cat(readLines(file), sep = "\n")
  cat("\n\n")
}

# Clean up
setwd(original_dir)
unlink(demo_dir, recursive = TRUE)

cat("===== Demo completed =====\n")
