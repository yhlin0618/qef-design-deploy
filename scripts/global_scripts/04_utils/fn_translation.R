#' @file fn_translation.R
#' @principle R108 Function Deployment Rule
#' @principle R21 One Function One File
#' @principle MP31 Initialization First
#' 
#' @title Translation Utility Functions
#' @description 
#' Provides translation functionality for multilingual support in Shiny applications.
#' Must be loaded early in the application initialization process.

#' @title Initialize Translation System
#' @description 
#' Sets up the translation system by defining the translate function 
#' and making it available in the global environment.
#' @param language Character string defining the default language (default: "en")
#' @param translation_dict Optional list or data frame containing translations
#' @return The translate function, invisibly
#' @export
initialize_translation_system <- function(language = "en", translation_dict = NULL) {
  # Create the translation dictionary if provided
  dict <- translation_dict
  current_language <- language
  
  # Define the translate function with appropriate closure over dictionary
  translate <- function(text, lang = NULL) {
    # Use specified language or fall back to current language
    use_lang <- if (is.null(lang)) current_language else lang

    # If no dictionary available or language not found, return original text
    if (is.null(dict) || !is.list(dict) || is.null(dict[[use_lang]])) {
      return(text)
    }

    lang_dict <- dict[[use_lang]]

    # Guard: named vector uses %in% names() (not is.null) to avoid subscript out of bounds
    if (is.character(lang_dict)) {
      if (text %in% names(lang_dict)) return(unname(lang_dict[[text]]))
      return(text)
    }

    # For list-type dictionaries
    val <- lang_dict[[text]]
    if (is.null(val)) return(text)
    return(val)
  }
  
  # Function to change the current language
  set_language <- function(new_language) {
    if (!is.null(new_language) && is.character(new_language) && length(new_language) == 1) {
      current_language <<- new_language
      return(TRUE)
    }
    return(FALSE)
  }
  
  # Function to update or add translations
  update_translations <- function(new_dict) {
    if (!is.null(new_dict) && is.list(new_dict)) {
      if (is.null(dict)) {
        dict <<- new_dict
      } else {
        # Merge dictionaries
        for (lang in names(new_dict)) {
          if (is.null(dict[[lang]])) {
            dict[[lang]] <<- new_dict[[lang]]
          } else {
            dict[[lang]] <<- modifyList(dict[[lang]], new_dict[[lang]])
          }
        }
      }
      return(TRUE)
    }
    return(FALSE)
  }
  
  # Ensure global availability of these functions
  assign("translate", translate, envir = .GlobalEnv)
  assign("set_language", set_language, envir = .GlobalEnv)
  assign("update_translations", update_translations, envir = .GlobalEnv)
  
  # Notify initialization
  message("Translation system initialized with default language: ", language)
  
  # Return the translate function invisibly
  invisible(translate)
}

#' @title Get Current Translation Function
#' @description 
#' Ensures the translate function is available by checking for its existence
#' and providing a fallback if it's not found.
#' @return A function that can be used for translation
#' @export
get_translate_function <- function() {
  # Check if translate function exists in global environment
  if (!exists("translate", envir = .GlobalEnv) || 
      !is.function(get("translate", envir = .GlobalEnv))) {
    
    # Create a fallback function
    warning("Translate function not found. Using fallback implementation.")
    fallback_translate <- function(text, lang = NULL) {
      return(text)
    }
    
    # Make it available in global environment
    assign("translate", fallback_translate, envir = .GlobalEnv)
    
    return(fallback_translate)
  }
  
  # Return the existing translate function
  return(get("translate", envir = .GlobalEnv))
}

#' @title Create Sample Translation Dictionary
#' @description 
#' Creates a sample translation dictionary for demonstration purposes.
#' @return A nested list with translations
#' @export
create_sample_translation_dict <- function() {
  return(list(
    "en" = list(
      "Hello" = "Hello",
      "Goodbye" = "Goodbye",
      "Welcome" = "Welcome",
      "Customer" = "Customer",
      "Settings" = "Settings",
      "Dashboard" = "Dashboard",
      "AI Marketing Platform" = "AI Marketing Platform"
    ),
    "zh" = list(
      "Hello" = "您好",
      "Goodbye" = "再見",
      "Welcome" = "歡迎",
      "Customer" = "客戶",
      "Settings" = "設置",
      "Dashboard" = "儀表板",
      "AI Marketing Platform" = "AI行銷平台"
    )
  ))
}