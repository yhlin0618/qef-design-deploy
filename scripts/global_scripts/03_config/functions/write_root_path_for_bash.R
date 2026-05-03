#' Write Root Path For Bash
#'
#' Writes the root path to a bash script for use in shell scripts
#'
#' @return NULL invisibly
#'
write_root_path_for_bash <- function() {
  # ROOT_PATH should be defined in the environment before calling this function
  if (!exists("ROOT_PATH")) {
    stop("ROOT_PATH is not defined. Source the root_path_config.R file first.")
  }
  
  bash_path <- file.path(ROOT_PATH, "root_path.sh")
  writeLines(paste0("PRECISION_MARKETING_ROOT=", ROOT_PATH), bash_path)
  message("Root path exported to bash script: ", bash_path)
  
  invisible(NULL)
}