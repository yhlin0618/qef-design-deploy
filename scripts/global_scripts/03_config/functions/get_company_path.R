#' Get Company Path
#'
#' Gets the path to a specific company's directory
#'
#' @param company_name Name of the company
#'
#' @return Character string with the company's directory path
#'
get_company_path <- function(company_name) {
  # ROOT_PATH should be defined in the environment before calling this function
  if (!exists("ROOT_PATH")) {
    stop("ROOT_PATH is not defined. Source the root_path_config.R file first.")
  }
  
  file.path(ROOT_PATH, paste0("precision_marketing_", company_name))
}