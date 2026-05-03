#' Password-Only Login Module - Modern Minimal Design
#'
#' A simplified login module that only requires password authentication.
#' Uses external CSS for styling (login_password_only.css).
#' Supports multiple passwords, lockout mechanism, and customizable messages.
#'
#' @author AI MarTech Team
#' @version 1.0
#' @date 2025-01-20
#'
#' Following principles:
#' - UI_R001: UI-Server-Defaults Triple Pattern
#' - UI_R011: bs4Dash Structure Adherence

# ============================================================================
# UI COMPONENT
# ============================================================================

#' Password-Only Login UI
#'
#' @param id Module namespace id
#' @param app_title Application title (displayed below logo)
#' @param app_icon Path to app icon image (e.g., "assets/icons/app_icon.png")
#' @param password_label Label for password input field
#' @param submit_label Label for submit button
#' @param css_path Path to CSS file (default: "scripts/global_scripts/19_CSS/login_password_only.css")
#' @return Shiny tagList with password-only login UI
#' @export
loginPasswordOnlyUI <- function(id,
                                app_title = "Welcome",
                                app_icon = NULL,
                                password_label = "Password",
                                submit_label = "Enter",
                                css_path = "scripts/global_scripts/19_CSS/login_password_only.css") {

  ns <- NS(id)

  tagList(
    # Include external CSS
    tags$head(
      tags$link(rel = "stylesheet", type = "text/css", href = css_path)
    ),

    # Enable shinyjs for show/hide functionality
    shinyjs::useShinyjs(),

    # Main wrapper
    div(class = "pwonly-page-wrapper",

        # Login card
        div(id = ns("login_container"), class = "pwonly-card",

            # Logo (if provided)
            if (!is.null(app_icon)) {
              div(class = "pwonly-logo-container",
                  img(src = app_icon, alt = "App Logo")
              )
            },

            # Title
            div(class = "pwonly-title", app_title),

            # Password form
            div(class = "pwonly-form",

                # Password input
                passwordInput(
                  ns("password"),
                  label = password_label,
                  placeholder = "******",
                  width = "100%"
                ),

                # Submit button
                actionButton(
                  ns("submit_btn"),
                  submit_label,
                  class = "pwonly-btn",
                  width = "100%"
                ),

                # Message container
                div(class = "pwonly-message",
                    uiOutput(ns("login_msg"))
                )
            )
        )
    )
  )
}

# ============================================================================
# SERVER COMPONENT
# ============================================================================

#' Password-Only Login Server
#'
#' @param id Module namespace id
#' @param password_env_var Environment variable name containing the password(s)
#'   Can be a single password or multiple passwords in various formats:
#'   - R vector: c("pass1","pass2")
#'   - Comma-separated: pass1,pass2
#'   - JSON array: ["pass1","pass2"]
#' @param default_password Default password if env var not set (for development)
#' @param max_attempts Maximum login attempts before lockout
#' @param lockout_duration Lockout duration in seconds (default: 300 = 5 minutes)
#' @param success_message Success message displayed on login
#' @param error_message Error message displayed on wrong password
#' @param lockout_message Message displayed during lockout
#' @return List with reactive values: logged_in (logical), logout (function)
#' @export
loginPasswordOnlyServer <- function(id,
                                    password_env_var = "APP_PASSWORD",
                                    default_password = "admin",
                                    max_attempts = 3,
                                    lockout_duration = 300,
                                    success_message = "Login successful",
                                    error_message = "Incorrect password",
                                    lockout_message = "Too many failed attempts. Please try again later.") {

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
      warning(paste0(
        "Environment variable '", password_env_var, "' is not set. ",
        "Using default password '", default_password, "'."
      ))
      correct_passwords <- default_password
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
          div(class = "pwonly-error",
              paste0(lockout_message, " (", remaining_mins, " min)"))
        })
        return()
      }

      # Verify password
      if (input$password %in% correct_passwords) {
        values$logged_in <- TRUE
        values$attempts <- 0

        output$login_msg <- renderUI({
          div(class = "pwonly-success", success_message)
        })

        # Hide login form after successful login
        shinyjs::hide("login_container")

      } else {
        values$attempts <- values$attempts + 1

        if (values$attempts >= max_attempts) {
          # Lock out user
          values$lockout_time <- Sys.time()
          output$login_msg <- renderUI({
            div(class = "pwonly-error", lockout_message)
          })
        } else {
          # Show error with remaining attempts
          remaining_attempts <- max_attempts - values$attempts
          output$login_msg <- renderUI({
            div(class = "pwonly-error",
                paste0(error_message, " (", remaining_attempts, " attempts remaining)"))
          })
        }
      }

      # Clear password field
      updateTextInput(session, "password", value = "")
    })

    # Handle Enter key press
    observeEvent(input$password, {
      # This is triggered on Enter key by default in passwordInput
    }, ignoreInit = TRUE)

    # Update lockout message periodically
    observe({
      if (is_locked_out()) {
        invalidateLater(1000)  # Update every second
        remaining_mins <- ceiling(remaining_lockout_time() / 60)
        output$login_msg <- renderUI({
          div(class = "pwonly-error",
              paste0(lockout_message, " (", remaining_mins, " min)"))
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

# ============================================================================
# DEFAULTS COMPONENT
# ============================================================================

#' Password-Only Login Defaults
#'
#' Returns default configuration for the password-only login module.
#'
#' @return List of default configuration values
#' @export
loginPasswordOnlyDefaults <- function() {
  list(
    # UI defaults
    app_title = "Welcome",
    app_icon = NULL,
    password_label = "Password",
    submit_label = "Enter",
    css_path = "scripts/global_scripts/19_CSS/login_password_only.css",

    # Server defaults
    password_env_var = "APP_PASSWORD",
    default_password = "admin",
    max_attempts = 3,
    lockout_duration = 300,  # 5 minutes

    # Message defaults (English)
    success_message = "Login successful",
    error_message = "Incorrect password",
    lockout_message = "Too many failed attempts. Please try again later.",

    # Message defaults (Chinese)
    success_message_zh = "登入成功",
    error_message_zh = "密碼錯誤",
    lockout_message_zh = "登入失敗次數過多，請稍後再試"
  )
}
