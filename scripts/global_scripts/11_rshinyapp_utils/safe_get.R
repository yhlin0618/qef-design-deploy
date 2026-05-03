safe_get <- function(name, path = "app_data") {
  file_path <- file.path(path, paste0(name, ".rds"))
  if (file.exists(file_path)) {
    tryCatch({
      readRDS(file_path)
    }, error = function(e) {
      warning(paste("Error reading file:", file_path, "-", e$message))
      NULL
    })
  } else {
    NULL
  }
}
