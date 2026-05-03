#' Password-Only Login Module - UI
#' @param id Module namespace id
#' @param app_title Application title (default: translated "Application")
#' @param app_icon Icon path (optional)
#' @param password_label Label for password input (default: translated "Enter password")
#' @param submit_label Label for submit button (default: translated "Enter")
#' @param background_color Background color (default: "#f5f6fa")
#' @param primary_color Primary color (default: "#007bff")
#' @param card_width Maximum width of login card (default: "350px")
#' @param translate Translation function for user-visible labels
#' @return Shiny tagList with password-only login UI
passwordOnlyUI <- function(id,
                          app_title = NULL,
                          app_icon = NULL,
                          password_label = NULL,
                          submit_label = NULL,
                          background_color = "#f5f6fa",
                          primary_color = "#007bff",
                          card_width = "350px",
                          translate = function(x) x) {

  if (is.null(app_title)) {
    app_title <- translate("Application")
  }
  if (is.null(password_label)) {
    password_label <- translate("Enter password")
  }
  if (is.null(submit_label)) {
    submit_label <- translate("Enter")
  }
  
  ns <- NS(id)
  
  tagList(
    # Custom CSS
    tags$style(HTML(paste0(
      ".password-only-bg {",
      "  min-height: 100vh;",
      "  display: flex;",
      "  align-items: center;",
      "  justify-content: center;",
      "  background: ", background_color, ";",
      "}",
      ".password-only-card {",
      "  background: #fff;",
      "  border-radius: 20px;",
      "  box-shadow: 0 4px 24px rgba(0,0,0,0.08);",
      "  padding: 2.5rem 1.5rem 2rem 1.5rem;",
      "  max-width: ", card_width, ";",
      "  width: 100%;",
      "  margin: 0 auto;",
      "  text-align: center;",
      "}",
      ".password-only-icon {",
      "  margin-bottom: 1.5rem;",
      "}",
      ".password-only-title {",
      "  font-size: 1.5rem;",
      "  font-weight: 600;",
      "  color: #222;",
      "  margin-bottom: 1.5rem;",
      "}",
      ".password-only-btn {",
      "  width: 100%;",
      "  font-size: 1.1rem;",
      "  padding: 0.75rem;",
      "  border-radius: 8px;",
      "  background-color: ", primary_color, ";",
      "  border-color: ", primary_color, ";",
      "}",
      ".password-only-btn:hover {",
      "  opacity: 0.9;",
      "}",
      ".password-only-form .form-group {",
      "  text-align: left;",
      "  margin-bottom: 1.5rem;",
      "}",
      ".password-only-form label {",
      "  font-weight: 500;",
      "  color: #333;",
      "  margin-bottom: 0.5rem;",
      "}",
      ".password-only-msg {",
      "  margin-top: 1rem;",
      "  font-size: 0.9rem;",
      "}",
      ".password-only-error {",
      "  color: #dc3545;",
      "}",
      ".password-only-success {",
      "  color: #28a745;",
      "}"
    ))),
    
    # Main login container
    div(class = "password-only-bg",
        div(id = ns("login_container"), class = "password-only-card",
            # App icon (if provided)
            if (!is.null(app_icon)) {
              div(class = "password-only-icon",
                  img(src = app_icon, height = "90px")
              )
            },
            
            # App title
            div(class = "password-only-title", app_title),
            
            # Password form
            div(class = "password-only-form",
                passwordInput(
                  ns("password"), 
                  label = password_label,
                  placeholder = "******"
                ),
                
                actionButton(
                  ns("submit_btn"), 
                  submit_label, 
                  class = "btn-primary password-only-btn"
                ),
                
                # Message output
                div(id = ns("msg_container"), class = "password-only-msg",
                    uiOutput(ns("login_msg"))
                )
            )
        )
    )
  )
}
