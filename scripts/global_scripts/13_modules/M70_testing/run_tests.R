# Test Application Runner
# This script loads the testing module functions and runs a complete verification and test

# Load testing functions
source(file.path("modules", "M70_testing", "M70_fn_verify_config.R"))
source(file.path("modules", "M70_testing", "M70_fn_test_app.R"))

# Configuration path
config_path <- "app_config.yaml"

# Run verification
cat("Starting configuration verification...\n")
verify_results <- M70_verify_config(config_path, verbose = TRUE)

# If verification succeeded, run the app test
if (verify_results$success) {
  cat("\nConfiguration is valid. Starting app test...\n")
  test_results <- M70_test_app(config_path, test_mode = "basic", timeout = 30)
  
  if (test_results$success) {
    cat("\nApp test completed successfully!\n")
    cat("Test report saved to:", test_results$info$report_path, "\n")
  } else {
    cat("\nApp test failed!\n")
    if (length(test_results$errors) > 0) {
      cat("Errors:\n")
      for (error_name in names(test_results$errors)) {
        cat("- ", error_name, ": ", test_results$errors[[error_name]], "\n", sep = "")
      }
    }
    cat("Test report saved to:", test_results$info$report_path, "\n")
  }
} else {
  cat("\nConfiguration verification failed. Not running app test.\n")
  cat("Verification report saved to:", verify_results$info$report_path, "\n")
}

cat("\nTest execution complete.\n")