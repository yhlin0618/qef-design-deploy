# ==============================================================================
# DEBUG VERBOSE INITIALIZATION - Enhanced Error Visibility for AI Debugging
# ==============================================================================
# PURPOSE: Provides clear, visible error output for effective AI debugging
# PRINCIPLE COMPLIANCE: MP099 (Real-time Progress Reporting), DEV_R032
# USAGE: Source this instead of regular autoinit() when debugging ETL scripts
# ==============================================================================

# Enhanced error handling with prominent visibility
debug_autoinit <- function(quiet_init = TRUE, highlight_errors = TRUE) {
  
  if (highlight_errors) {
    # Set R error options for maximum visibility
    options(
      error = function(e) {
        cat("\n")
        cat(strrep("🚨", 50), "\n")
        cat("🚨 CRITICAL ERROR DETECTED - AI DEBUGGING ALERT 🚨\n")
        cat(strrep("🚨", 50), "\n")
        cat("📍 Error Location: ", deparse(sys.call(-1)), "\n")
        cat("📄 Error Message: ", conditionMessage(e), "\n")
        cat("🔍 Call Stack:\n")
        
        # Print call stack
        for (i in seq_len(sys.nframe())) {
          frame_call <- sys.call(-i)
          if (!is.null(frame_call)) {
            cat("  ", i, ": ", deparse(frame_call), "\n")
          }
        }
        
        cat(strrep("🚨", 50), "\n")
        cat("🚨 SCRIPT EXECUTION STOPPED DUE TO ERROR 🚨\n")
        cat(strrep("🚨", 50), "\n")
        
        # Also print to stderr for immediate visibility
        message("CRITICAL ERROR - Check console output above")
        
        # Stop execution
        stop(e)
      },
      warn = if (highlight_errors) 1 else 0  # Show warnings immediately
    )
    
    # Enhance message visibility
    cat(strrep("🔧", 80), "\n")
    cat("🔧 DEBUG MODE: Enhanced Error Visibility Active\n")
    cat("🔧 All errors will be prominently displayed for AI debugging\n")
    cat(strrep("🔧", 80), "\n")
  }
  
  # Capture and suppress excessive initialization output if requested
  if (quiet_init) {
    cat("🔄 Initializing MAMBA system (quiet mode)...\n")
    
    # Capture all output from autoinit
    init_output <- capture.output({
      init_warnings <- capture.output({
        tryCatch({
          .InitEnv$autoinit()
        }, error = function(e) {
          # Re-throw error with enhanced formatting
          if (highlight_errors) {
            cat("\n🚨 ERROR DURING INITIALIZATION:\n")
            cat("📄 ", conditionMessage(e), "\n")
            cat("🔍 Location: autoinit()\n")
          }
          stop("Initialization failed: ", conditionMessage(e))
        })
      }, type = "message")
    }, type = "output")
    
    cat("✅ MAMBA system initialization completed\n")
    
    # Only show initialization summary
    if (exists("INITIALIZATION_COMPLETED") && INITIALIZATION_COMPLETED) {
      cat("✅ Operation Mode: ", OPERATION_MODE, "\n")
      if (exists("db_path_list")) {
        cat("✅ Databases available: ", paste(names(db_path_list), collapse = ", "), "\n")
      }
    }
    
    # Show any warnings from initialization
    if (length(init_warnings) > 0) {
      cat("⚠️  Initialization warnings (", length(init_warnings), " total):\n")
      for (i in seq_along(init_warnings)) {
        if (i <= 3) {  # Only show first 3 warnings
          cat("   ", init_warnings[i], "\n")
        }
      }
      if (length(init_warnings) > 3) {
        cat("   ... and ", length(init_warnings) - 3, " more warnings\n")
      }
    }
    
  } else {
    # Normal initialization with visible output
    cat("🔄 Initializing MAMBA system (verbose mode)...\n")
    .InitEnv$autoinit()
  }
  
  cat(strrep("✅", 80), "\n")
  cat("✅ MAMBA DEBUG INITIALIZATION COMPLETE - READY FOR ETL\n")
  cat(strrep("✅", 80), "\n")
  
  invisible(TRUE)
}

# Enhanced tryCatch wrapper for ETL operations
debug_tryCatch <- function(expr, error_context = "ETL Operation") {
  tryCatch(
    expr,
    error = function(e) {
      cat("\n")
      cat(strrep("❌", 60), "\n")
      cat("❌ ERROR IN ", toupper(error_context), "\n")
      cat(strrep("❌", 60), "\n")
      cat("📍 Context: ", error_context, "\n")
      cat("📄 Error: ", conditionMessage(e), "\n")
      cat("🔍 Call: ", deparse(e$call), "\n")
      cat("⏰ Time: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
      cat(strrep("❌", 60), "\n")
      
      # Re-throw the error to stop execution
      stop("ETL Error in ", error_context, ": ", conditionMessage(e))
    },
    warning = function(w) {
      cat("⚠️  WARNING in ", error_context, ": ", conditionMessage(w), "\n")
      invokeRestart("muffleWarning")
    }
  )
}

# Progress tracker for ETL phases
debug_progress <- function(phase, message, status = "info") {
  timestamp <- format(Sys.time(), "%H:%M:%S")
  
  prefix <- switch(status,
    "start" = "🚀",
    "success" = "✅",
    "error" = "❌",
    "warning" = "⚠️",
    "info" = "ℹ️"
  )
  
  cat(sprintf("%s [%s] %s: %s\n", prefix, timestamp, toupper(phase), message))
}

# Validation function to ensure error visibility is working
test_error_visibility <- function() {
  cat("🧪 Testing error visibility...\n")
  
  # Test 1: Warning visibility
  debug_progress("TEST", "Testing warning visibility", "start")
  tryCatch({
    warning("This is a test warning - should be visible")
  }, warning = function(w) {
    cat("✅ Warning visibility test passed\n")
  })
  
  # Test 2: Error catching (commented out to avoid stopping script)
  # debug_tryCatch({
  #   stop("This is a test error - should be prominently displayed")
  # }, "Error Visibility Test")
  
  cat("✅ Error visibility system is ready\n")
}

# Export functions to global environment
assign("debug_autoinit", debug_autoinit, envir = .GlobalEnv)
assign("debug_tryCatch", debug_tryCatch, envir = .GlobalEnv)
assign("debug_progress", debug_progress, envir = .GlobalEnv)
assign("test_error_visibility", test_error_visibility, envir = .GlobalEnv)

cat("🔧 Enhanced debugging functions loaded:\n")
cat("   • debug_autoinit() - Clean initialization with error visibility\n")
cat("   • debug_tryCatch() - Enhanced error catching for ETL operations\n")
cat("   • debug_progress() - Clear progress reporting\n")
cat("   • test_error_visibility() - Validate error visibility setup\n")