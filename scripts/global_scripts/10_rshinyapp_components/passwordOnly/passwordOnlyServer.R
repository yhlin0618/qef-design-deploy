#' Password-Only Login Module - Server
#' 
#' @param id Module namespace id
#' @param password_env_var Environment variable name containing the password(s) (default: "APP_PASSWORD")
#'   Can be a single password or multiple passwords in various formats:
#'   - R vector: c("pass1","pass2")
#'   - Comma-separated: pass1,pass2
#'   - JSON array: ["pass1","pass2"]
#' @param max_attempts Maximum login attempts before lockout (default: 3)
#' @param lockout_duration Lockout duration in seconds (default: 300 = 5 minutes)
#' @param success_message Success message (default: translated "Login successful")
#' @param error_message Error message (default: translated "Incorrect password")
#' @param lockout_message Lockout message (default: translated
#'   "Too many failed login attempts, please try again later")
#' @param translate Translation function for user-visible status text
#' @return List with reactive values: logged_in (logical), logout (function)
passwordOnlyServer <- function(id,
                              password_env_var = "APP_PASSWORD",
                              max_attempts = 3,
                              lockout_duration = 300,
                              success_message = NULL,
                              error_message = NULL,
                              lockout_message = NULL,
                              translate = function(x) x) {

  if (is.null(success_message)) {
    success_message <- paste0("✅ ", translate("Login successful"))
  }
  if (is.null(error_message)) {
    error_message <- paste0("❌ ", translate("Incorrect password"))
  }
  if (is.null(lockout_message)) {
    lockout_message <- paste0("⛔ ", translate("Too many failed login attempts, please try again later"))
  }
  
  # Helper function to parse passwords from environment variable
  parse_passwords <- function(password_str) {
    if (is.null(password_str) || password_str == "") {
      return(NULL)
    }
    
    # Remove leading/trailing whitespace
    password_str <- trimws(password_str)
    
    # Try to parse as R vector (e.g., c("pass1","pass2"))
    if (grepl("^c\\(", password_str)) {
      tryCatch({
        passwords <- eval(parse(text = password_str))
        return(as.character(passwords))
      }, error = function(e) {
        # Continue to next format
      })
    }
    
    # Try to parse as JSON array (e.g., ["pass1","pass2"])
    if (grepl("^\\[", password_str)) {
      tryCatch({
        if (requireNamespace("jsonlite", quietly = TRUE)) {
          passwords <- jsonlite::fromJSON(password_str)
          return(as.character(passwords))
        }
      }, error = function(e) {
        # Continue to next format
      })
    }
    
    # Try to parse as comma-separated values (e.g., pass1,pass2)
    if (grepl(",", password_str)) {
      passwords <- trimws(strsplit(password_str, ",")[[1]])
      return(passwords)
    }
    
    # Return as single password
    return(password_str)
  }
  
  moduleServer(id, function(input, output, session) {
    ns <- session$ns
    
    # Get password(s) from environment variable
    password_str <- Sys.getenv(password_env_var)
    
    # Parse passwords
    correct_passwords <- parse_passwords(password_str)
    
    # Check if password is set
    if (is.null(correct_passwords) || length(correct_passwords) == 0) {
      warning(paste0("Environment variable '", password_env_var, "' is not set. Using default password 'admin'."))
      correct_passwords <- "admin"
    }
    
    # Reactive values
    values <- reactiveValues(
      logged_in = FALSE,
      attempts = 0,
      lockout_time = NULL
    )
    
    # Check if currently locked out
    is_locked_out <- reactive({
      if (!is.null(values$lockout_time)) {
        time_diff <- as.numeric(difftime(Sys.time(), values$lockout_time, units = "secs"))
        if (time_diff < lockout_duration) {
          return(TRUE)
        } else {
          # Reset lockout
          values$lockout_time <- NULL
          values$attempts <- 0
          return(FALSE)
        }
      }
      return(FALSE)
    })
    
    # Remaining lockout time
    remaining_lockout_time <- reactive({
      if (!is.null(values$lockout_time)) {
        time_diff <- as.numeric(difftime(Sys.time(), values$lockout_time, units = "secs"))
        remaining <- max(0, lockout_duration - time_diff)
        return(ceiling(remaining))
      }
      return(0)
    })
    
    # Handle password submission
    observeEvent(input$submit_btn, {
      req(input$password)
      
      # Check lockout
      if (is_locked_out()) {
        remaining_mins <- ceiling(remaining_lockout_time() / 60)
        output$login_msg <- renderUI({
          div(class = "password-only-error",
              sprintf(translate("%s (%d minutes remaining)"), lockout_message, remaining_mins))
        })
        return()
      }
      
      # Verify password (check if input password matches any of the valid passwords)
      if (input$password %in% correct_passwords) {
        values$logged_in <- TRUE
        values$attempts <- 0
        
        output$login_msg <- renderUI({
          div(class = "password-only-success", success_message)
        })
        
        # Hide login form after successful login
        shinyjs::hide("login_container")
        
      } else {
        values$attempts <- values$attempts + 1
        
        if (values$attempts >= max_attempts) {
          # Lock out user
          values$lockout_time <- Sys.time()
          output$login_msg <- renderUI({
            div(class = "password-only-error", lockout_message)
          })
        } else {
          # Show error with remaining attempts
          remaining_attempts <- max_attempts - values$attempts
          output$login_msg <- renderUI({
            div(class = "password-only-error",
                sprintf(translate("%s (Remaining %d attempts)"), error_message, remaining_attempts))
          })
        }
      }
      
      # Clear password field
      updateTextInput(session, "password", value = "")
    })
    
    # Update lockout message periodically
    observe({
      if (is_locked_out()) {
        invalidateLater(1000)  # Update every second
        remaining_mins <- ceiling(remaining_lockout_time() / 60)
        output$login_msg <- renderUI({
          div(class = "password-only-error",
              sprintf(translate("%s (%d minutes remaining)"), lockout_message, remaining_mins))
        })
      }
    })
    
    # Logout function
    logout <- function() {
      values$logged_in <- FALSE
      values$attempts <- 0
      values$lockout_time <- NULL
      
      # Show login form again
      shinyjs::show("login_container")
      
      # Clear messages
      output$login_msg <- renderUI({})
    }
    
    # Return reactive values and functions
    list(
      logged_in = reactive(values$logged_in),
      logout = logout
    )
  })
}
