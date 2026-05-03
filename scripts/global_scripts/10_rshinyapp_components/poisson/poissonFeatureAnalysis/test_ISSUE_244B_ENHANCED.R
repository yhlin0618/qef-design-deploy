# Test Script for ISSUE_244B_ENHANCED
# Verify enhanced Chinese variable pattern detection in calculate_attribute_range()

# Source the main file to get the function
source("/Users/che/Library/CloudStorage/Dropbox/che_workspace/projects/ai_martech/l4_enterprise/MAMBA/scripts/global_scripts/10_rshinyapp_components/poisson/poissonFeatureAnalysis/poissonFeatureAnalysis.R")

# Test cases as specified in ISSUE_244B_ENHANCED
test_cases <- data.frame(
  variable_name = c(
    "配送快速",
    "完美匹配",
    "套件內容_45_度飽腹按摩",
    "客服品質",
    "special_price",
    "unknown_var",
    "評分高低",
    "數量多少",
    "is_premium",
    "product_type_A",
    "包含贈品",
    "星級評價"
  ),
  expected_range = c(
    1,  # Chinese dummy pattern
    1,  # Chinese dummy pattern
    1,  # Underscore categorical
    4,  # Chinese rating keyword
    1,  # English binary pattern (has underscore)
    2,  # Conservative default
    4,  # Chinese rating keyword
    10, # Chinese quantity keyword
    1,  # English binary pattern
    1,  # Underscore categorical
    1,  # Chinese dummy pattern
    4   # Chinese rating keyword
  ),
  reason = c(
    "Chinese dummy pattern",
    "Chinese dummy pattern",
    "Underscore categorical",
    "Chinese rating keyword",
    "Underscore categorical",
    "Conservative default",
    "Chinese rating keyword",
    "Chinese quantity keyword",
    "English binary pattern",
    "Underscore categorical",
    "Chinese dummy pattern",
    "Chinese rating keyword"
  ),
  stringsAsFactors = FALSE
)

# Run tests
cat("========================================\n")
cat("ISSUE_244B_ENHANCED Test Results\n")
cat("========================================\n\n")

all_passed <- TRUE

for (i in 1:nrow(test_cases)) {
  var_name <- test_cases$variable_name[i]
  expected <- test_cases$expected_range[i]
  reason <- test_cases$reason[i]

  actual <- calculate_attribute_range(var_name)

  passed <- actual == expected
  all_passed <- all_passed && passed

  status <- if (passed) "✅ PASS" else "❌ FAIL"

  cat(sprintf("%s | %s\n", status, var_name))
  cat(sprintf("   Expected: %d (%s)\n", expected, reason))
  cat(sprintf("   Actual:   %d\n", actual))

  if (!passed) {
    cat(sprintf("   >>> MISMATCH! Expected %d but got %d <<<\n", expected, actual))
  }
  cat("\n")
}

cat("========================================\n")
if (all_passed) {
  cat("✅ ALL TESTS PASSED!\n")
} else {
  cat("❌ SOME TESTS FAILED!\n")
}
cat("========================================\n")

# Return test results
invisible(all_passed)
