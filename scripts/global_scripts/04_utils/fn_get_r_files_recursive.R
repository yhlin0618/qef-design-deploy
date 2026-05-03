#' Get R Files Recursively
#'
#' Recursively retrieves all .R files in a directory and its subdirectories
#' with support for inclusion and exclusion patterns.
#' Implementation follows R33 Recursive Sourcing Rule.
#'
#' @param dir_path String. Path to the directory to search.
#' @param include_pattern String. Regex pattern for files to include. Defaults to "\\.R$" (all R files).
#' @param exclude_pattern String. Regex pattern for files to exclude, applied to both filenames and full paths. Defaults to "(initialization|deinitialization|test|demo|example|archive)".
#' @param max_depth Integer. Maximum depth of subdirectories to traverse. Defaults to Inf (no limit).
#'
#' @return Character vector of full paths to matching R files.
#'
#' @examples
#' # Get all R files in the utils directory
#' r_files <- get_r_files_recursive("utils")
#'
#' # Get only files with fn_ prefix
#' fn_files <- get_r_files_recursive("utils", include_pattern = "^fn_.*\\.R$")
#'
#' # Get function files but exclude test files and archive paths
#' fn_no_test <- get_r_files_recursive("utils", include_pattern = "^fn_.*\\.R$", exclude_pattern = "(test|archive)")
#'
#' # Exclude files in certain directories even if the filename doesn't match the pattern
#' prod_files <- get_r_files_recursive("src", exclude_pattern = "(test|dev|archive)")
#'
get_r_files_recursive <- function(dir_path, 
                                  include_pattern = "\\.R$", 
                                  exclude_pattern = "(initialization|deinitialization|test|demo|example|archive|deploy|99_archive)",
                                  max_depth = Inf) {
  
  # 檢查目錄是否存在
  if (!dir.exists(dir_path)) {
    warning("Directory does not exist: ", dir_path)
    return(character(0))
  }
  
  # 取得所有檔案及資料夾（非遞迴）
  all_contents <- list.files(dir_path, full.names = TRUE, recursive = FALSE, 
                             include.dirs = TRUE, all.files = FALSE)
  
  # 分離檔案與資料夾
  is_dir <- dir.exists(all_contents)
  files <- all_contents[!is_dir]
  subdirs <- all_contents[is_dir]
  
  # 依據 include_pattern 進行檔案篩選（只包含 .R 檔案）
  if (!is.null(include_pattern)) {
    files <- files[grepl(include_pattern, basename(files))]
  }
  
  # 依據 exclude_pattern 排除符合條件的檔案
  if (!is.null(exclude_pattern)) {
    # 排除檔案名稱符合條件的檔案
    files <- files[!grepl(exclude_pattern, basename(files))]
    
    # 排除路徑中包含 exclude_pattern 的檔案
    files <- files[!grepl(exclude_pattern, files, ignore.case = TRUE)]
  }
  
  # 如果尚未達到最大深度，遞迴處理資料夾
  if (length(subdirs) > 0 && max_depth > 1) {
    subdir_files <- lapply(subdirs, function(subdir) {
      get_r_files_recursive(subdir, 
                            include_pattern = include_pattern,
                            exclude_pattern = exclude_pattern,
                            max_depth = max_depth - 1)
    })
    
    # 合併檔案清單
    files <- c(files, unlist(subdir_files))
  }
  
  return(files)
}