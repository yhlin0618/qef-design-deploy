#' Password-Only Login Module - Defaults
#' 
#' This file defines default parameters for the passwordOnly module
#' Following R009 (UI-Server-Defaults Triple Rule)
#' 
#' @return List of default parameters for UI and Server components

passwordOnlyDefaults <- function() {
  list(
    # UI Defaults
    ui = list(
      app_title = "應用程式",
      app_icon = NULL,
      password_label = "請輸入密碼",
      submit_label = "進入",
      background_color = "#f5f6fa",
      primary_color = "#007bff",
      card_width = "350px"
    ),
    
    # Server Defaults
    server = list(
      password_env_var = "APP_PASSWORD",
      max_attempts = 3,
      lockout_duration = 300,  # 5 minutes
      success_message = "✅ 登入成功",
      error_message = "❌ 密碼錯誤",
      lockout_message = "⛔ 登入失敗次數過多，請稍後再試",
      default_password = "admin"  # Used when env var is not set
    ),
    
    # Password Parsing Options
    password_parsing = list(
      # Supported formats for multiple passwords
      formats = c("r_vector", "comma_separated", "json_array"),
      
      # Examples of each format
      examples = list(
        r_vector = 'c("pass1","pass2","pass3")',
        comma_separated = "pass1,pass2,pass3",
        json_array = '["pass1","pass2","pass3"]'
      ),
      
      # Whether to trim whitespace from passwords
      trim_whitespace = TRUE,
      
      # Whether to remove empty passwords
      remove_empty = TRUE
    ),
    
    # CSS Classes
    css_classes = list(
      background = "password-only-bg",
      card = "password-only-card",
      icon = "password-only-icon",
      title = "password-only-title",
      button = "password-only-btn",
      form = "password-only-form",
      message = "password-only-msg",
      error = "password-only-error",
      success = "password-only-success"
    )
  )
}

#' Get specific default value
#' 
#' @param category Category of defaults (ui, server, password_parsing, css_classes)
#' @param param Parameter name within the category
#' @return Default value for the specified parameter
passwordOnlyDefault <- function(category = NULL, param = NULL) {
  defaults <- passwordOnlyDefaults()
  
  if (is.null(category)) {
    return(defaults)
  }
  
  if (!category %in% names(defaults)) {
    warning(paste0("Category '", category, "' not found in defaults"))
    return(NULL)
  }
  
  if (is.null(param)) {
    return(defaults[[category]])
  }
  
  if (!param %in% names(defaults[[category]])) {
    warning(paste0("Parameter '", param, "' not found in category '", category, "'"))
    return(NULL)
  }
  
  return(defaults[[category]][[param]])
}