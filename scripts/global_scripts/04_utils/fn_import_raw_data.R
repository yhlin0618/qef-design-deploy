#' Import Raw Data into DuckDB
#' 
#' @description 
#' A utility function that imports raw data files (CSV or Excel) into a DuckDB connection
#' 
#' @param folder_name Character string. Name of the folder containing the data files
#' @param data_dir Character string. Base directory containing raw data folders. Default is "data/raw"
#' @param src_pattern Character string. Optional pattern to match specific files. If NULL, first CSV/Excel file is used
#' @param dest_con DBI connection. The DuckDB connection to write to. Default is raw_data
#' @param overwrite Logical. Whether to overwrite existing table. Default is TRUE
#' 
#' @return Invisible TRUE on success
#' 
#' @importFrom DBI dbWriteTable
#' 
import_raw_data <- function(folder_name,
                            data_dir   = "data/raw",
                            src_pattern = NULL,
                            dest_con   = raw_data,
                            overwrite  = TRUE) {

  # 1. 找檔案 ---------------------------------------------------------------
  folder_path <- file.path(data_dir, folder_name)
  file_vec <- if (is.null(src_pattern)) {
                list.files(folder_path,
                           pattern = "\\.(csv|xlsx?)$", ignore.case = TRUE,
                           full.names = TRUE)
              } else {
                file.path(folder_path, src_pattern)
              }
  if (length(file_vec) == 0 || !file.exists(file_vec[1]))
    stop("找不到資料檔案：", folder_path)

  src_file <- file_vec[1]
  message("⇢ 讀取檔案：", src_file)

  # 2. 讀檔 ---------------------------------------------------------------
  df <- read_csvxlsx(src_file)

  # 3. 把日期 / 時間欄位轉文字 --------------------------------------------
  date_like <- vapply(df, inherits, logical(1), what = c("Date", "POSIXt"))
  if (any(date_like)) {
    df[date_like] <- lapply(df[date_like], as.character)
  }

  # 4. 寫入 DuckDB --------------------------------------------------------
  table_name <- sprintf("df_%s", folder_name)
  dbWriteTable(dest_con,
               name      = table_name,
               value     = df,
               overwrite = overwrite,   # TRUE: 直接覆蓋；FALSE: append
               temporary = FALSE)

  message("✓ 已匯入 ", nrow(df), " 筆資料到表 ", table_name)
  invisible(TRUE)
}