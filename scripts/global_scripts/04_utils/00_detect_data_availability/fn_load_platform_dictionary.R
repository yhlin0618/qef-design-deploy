#' Load the platform dictionary from Excel file
#'
#' @description
#' Searches for and loads the platform dictionary which maps platform IDs to names.
#' Follows MP46 (Neighborhood Principle) when searching for the file.
#'
#' @param file_path Path to the platform dictionary Excel file
#' @return A data frame with platform mapping, or NULL if not found
#' @export
#' @implements MP45 Automatic Data Availability Detection
#' @implements MP46 Neighborhood Principle
load_platform_dictionary <- function(file_path = NULL) {
  # Try different potential locations if path not specified
  if (is.null(file_path)) {
    # Use file.path for cross-platform compatibility (MP46: Neighborhood Principle)
    potential_paths <- c(
      # Primary location - parameters subdirectory (neighborhood organization)
      file.path("update_scripts", "global_scripts", "30_global_data", "parameters", "platform_dictionary.xlsx"),
      # Fallback locations
      file.path("update_scripts", "global_scripts", "30_global_data", "platform_dictionary.xlsx"),
      file.path("30_global_data", "parameters", "platform_dictionary.xlsx"),
      file.path("app_data", "parameters", "platform.xlsx"),
      file.path("parameters", "platform_dictionary.xlsx"),
      file.path("parameters", "platform.xlsx")
    )
    
    for (path in potential_paths) {
      if (file.exists(path)) {
        message("Found platform dictionary at: ", path)
        file_path <- path
        break
      }
    }
  }
  
  # If we still don't have a file path or it doesn't exist, return NULL
  if (is.null(file_path) || !file.exists(file_path)) {
    message("Platform dictionary file not found")
    return(NULL)
  }
  
  # Try to load the Excel file
  tryCatch({
    if (!requireNamespace("readxl", quietly = TRUE)) {
      message("Installing readxl package...")
      install.packages("readxl")
    }
    
    platform_dict <- readxl::read_excel(file_path)
    
    # Check if it has the required columns
    required_cols <- c("platform_number", "platform_name_english")
    if (!all(required_cols %in% names(platform_dict))) {
      message("Platform dictionary file does not contain required columns: ", 
             paste(required_cols, collapse = ", "))
      return(NULL)
    }
    
    return(platform_dict)
  }, error = function(e) {
    message("Error loading platform dictionary: ", e$message)
    return(NULL)
  })
}