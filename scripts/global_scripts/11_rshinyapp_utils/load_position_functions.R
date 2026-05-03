#' Load Position Table Access Functions
#' 
#' This script ensures that the unified position table access functions
#' are available in the current R session.
#' 
#' Usage:
#' source('update_scripts/global_scripts/11_rshinyapp_utils/load_position_functions.R')
#' 

cat("🔄 Loading Position Table Access Functions...\n")

# Load complete case function
if (!exists("fn_get_position_complete_case")) {
  source(file.path("scripts", "global_scripts", "11_rshinyapp_utils", "fn_get_position_complete_case.R"))
  if (exists("fn_get_position_complete_case")) {
    cat("✅ fn_get_position_complete_case loaded successfully\n")
  } else {
    cat("❌ Failed to load fn_get_position_complete_case\n")
  }
} else {
  cat("✅ fn_get_position_complete_case already available\n")
}

# Load demonstrate case function
if (!exists("fn_get_position_demonstrate_case")) {
  source(file.path("scripts", "global_scripts", "11_rshinyapp_utils", "fn_get_position_demonstrate_case.R"))
  if (exists("fn_get_position_demonstrate_case")) {
    cat("✅ fn_get_position_demonstrate_case loaded successfully\n")
  } else {
    cat("❌ Failed to load fn_get_position_demonstrate_case\n")
  }
} else {
  cat("✅ fn_get_position_demonstrate_case already available\n")
}

cat("🎯 Position Table Access Functions ready for use!\n")
cat("\n📋 Available functions:\n")
cat("   • fn_get_position_complete_case() - for modules needing Ideal/Rating/Revenue rows\n")
cat("   • fn_get_position_demonstrate_case() - for modules showing only market data\n")
cat("\n🔍 Both functions support type filtering to show only '屬性' columns\n") 