#
# test_chat_api.R
# 測試 chat_api 函數，特別是不同模型的支援
#
# Usage: source("test_chat_api.R")
# -----------------------------------------------------------------------------

# Test function
test_chat_api <- function() {
  cat("=== Testing chat_api function ===\n\n")
  
  # Check if API key exists
  api_key <- Sys.getenv("OPENAI_API_KEY")
  if (!nzchar(api_key)) {
    cat("❌ Error: OPENAI_API_KEY not found in environment variables\n")
    return(FALSE)
  }
  
  cat("✅ API key found\n")
  
  # Prepare test messages
  sys_msg <- list(role = "system", content = "你是一個測試助手，請簡短回答。")
  usr_msg <- list(role = "user", content = "請說 Hello，測試是否正常運作。")
  messages <- list(sys_msg, usr_msg)
  
  # Test 1: Default model (gpt-5-nano)
  cat("\n--- Test 1: Default model (gpt-5-nano) ---\n")
  tryCatch({
    result1 <- chat_api(messages, api_key)
    cat("✅ Default model test passed\n")
    cat("Response:", substr(result1, 1, 100), "...\n")
  }, error = function(e) {
    cat("❌ Default model test failed:", e$message, "\n")
  })
  
  # Test 2: gpt-5-nano explicitly
  cat("\n--- Test 2: gpt-5-nano explicitly ---\n")
  tryCatch({
    result2 <- chat_api(messages, api_key, model = "gpt-5-nano")
    cat("✅ gpt-5-nano test passed\n")
    cat("Response:", substr(result2, 1, 100), "...\n")
  }, error = function(e) {
    cat("❌ gpt-5-nano test failed:", e$message, "\n")
  })
  
  # Test 3: o4-mini-deep-research
  cat("\n--- Test 3: o4-mini-deep-research ---\n")
  tryCatch({
    result3 <- chat_api(messages, api_key, model = "o4-mini-deep-research")
    cat("✅ o4-mini-deep-research test passed\n")
    cat("Response:", substr(result3, 1, 100), "...\n")
  }, error = function(e) {
    cat("❌ o4-mini-deep-research test failed:", e$message, "\n")
    cat("Error details:", e$message, "\n")
  })
  
  # Test 4: o1-mini (if available)
  cat("\n--- Test 4: o1-mini ---\n")
  tryCatch({
    result4 <- chat_api(messages, api_key, model = "o1-mini")
    cat("✅ o1-mini test passed\n")
    cat("Response:", substr(result4, 1, 100), "...\n")
  }, error = function(e) {
    cat("❌ o1-mini test failed:", e$message, "\n")
  })
  
  # Test 5: Invalid model
  cat("\n--- Test 5: Invalid model (should fail) ---\n")
  tryCatch({
    result5 <- chat_api(messages, api_key, model = "invalid-model-name")
    cat("⚠️  Invalid model test unexpectedly passed\n")
  }, error = function(e) {
    cat("✅ Invalid model correctly failed:", e$message, "\n")
  })
  
  cat("\n=== Test completed ===\n")
}

# Test different parameter passing methods
test_parameter_methods <- function() {
  cat("\n=== Testing parameter passing methods ===\n")
  
  api_key <- Sys.getenv("OPENAI_API_KEY")
  if (!nzchar(api_key)) {
    cat("❌ API key not found\n")
    return(FALSE)
  }
  
  messages <- list(
    list(role = "system", content = "簡短回答測試。"),
    list(role = "user", content = "請說 Test。")
  )
  
  # Method 1: Named parameter
  cat("\n--- Method 1: Named parameter ---\n")
  tryCatch({
    result1 <- chat_api(messages, api_key, model = "gpt-5-nano")
    cat("✅ Named parameter method passed\n")
  }, error = function(e) {
    cat("❌ Named parameter method failed:", e$message, "\n")
  })
  
  # Method 2: Positional parameter
  cat("\n--- Method 2: Positional parameter ---\n")
  tryCatch({
    result2 <- chat_api(messages, api_key, "gpt-5-nano")
    cat("✅ Positional parameter method passed\n")
  }, error = function(e) {
    cat("❌ Positional parameter method failed:", e$message, "\n")
  })
  
  # Method 3: Mixed parameters (like in the original code)
  cat("\n--- Method 3: Mixed parameters ---\n")
  tryCatch({
    result3 <- chat_api(list(
      list(role = "system", content = "Test system"),
      list(role = "user", content = "Test user")
    ), api_key, model = "gpt-5-nano")
    cat("✅ Mixed parameters method passed\n")
  }, error = function(e) {
    cat("❌ Mixed parameters method failed:", e$message, "\n")
  })
}

# Helper function to check model availability
check_model_availability <- function() {
  cat("\n=== Checking model availability ===\n")
  
  models_to_test <- c(
    "gpt-5-nano",
    "gpt-4o", 
    "o4-mini-deep-research",
    "o1-mini",
    "o1-preview"
  )
  
  api_key <- Sys.getenv("OPENAI_API_KEY")
  if (!nzchar(api_key)) {
    cat("❌ API key not found\n")
    return(FALSE)
  }
  
  simple_message <- list(
    list(role = "user", content = "Hi")
  )
  
  for (model in models_to_test) {
    cat("Testing model:", model, "... ")
    tryCatch({
      result <- chat_api(simple_message, api_key, model = model)
      cat("✅ Available\n")
    }, error = function(e) {
      if (grepl("404", e$message)) {
        cat("❌ Not available (404)\n")
      } else if (grepl("401", e$message)) {
        cat("❌ Unauthorized (401)\n")
      } else {
        cat("❌ Error:", substr(e$message, 1, 50), "...\n")
      }
    })
  }
}

# Test explicit API family wrappers (chat vs responses)
test_api_families <- function() {
  cat("\n=== Testing explicit API family wrappers ===\n")
  
  api_key <- Sys.getenv("OPENAI_API_KEY")
  if (!nzchar(api_key)) {
    cat("❌ API key not found\n")
    return(FALSE)
  }
  
  test_message <- list(
    list(role = "system", content = "你是一個測試助手，請簡短回答。"),
    list(role = "user", content = "請回覆 Yes。")
  )
  
  tryCatch({
    result <- chat_api_chat_completions(test_message, api_key, model = "o4-mini")
    cat("✅ chat_api_chat_completions is callable\n")
    cat("Preview:", substr(result, 1, 80), "...\n")
  }, error = function(e) {
    cat("❌ chat_api_chat_completions failed:", e$message, "\n")
  })
  
  tryCatch({
    result <- chat_api_responses(test_message, api_key, model = "gpt-5-nano")
    cat("✅ chat_api_responses is callable\n")
    cat("Preview:", substr(result, 1, 80), "...\n")
  }, error = function(e) {
    cat("❌ chat_api_responses failed:", e$message, "\n")
  })
}

# Run all tests
run_all_tests <- function() {
  cat("Starting comprehensive chat_api tests...\n")
  cat("Time:", Sys.time(), "\n")
  cat("Working directory:", getwd(), "\n\n")
  
  test_chat_api()
  test_parameter_methods()
  check_model_availability()
  test_api_families()
  
  cat("\n✨ All tests completed!\n")
}

# Auto-run if sourced directly
if (length(commandArgs(trailingOnly = TRUE)) == 0) {
  run_all_tests()
}
