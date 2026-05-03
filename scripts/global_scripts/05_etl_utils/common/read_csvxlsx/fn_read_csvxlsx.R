#' Read CSV or Excel Files
#' 
#' @description 
#' A utility function that reads either CSV or Excel files based on file extension
#' 
#' @param file_name Character string. Path to the file to be read
#' @param engine Character string. Which engine to use: "auto", "readr", "vroom", "fread", or "openxlsx"
#' @param ... Additional parameters to pass to the underlying read function
#' 
#' @return A data frame containing the data from the file
#' 
#' @importFrom tools file_ext
#' @importFrom readr read_csv
#' @importFrom readxl read_excel
#' @importFrom dplyr case_when recode
#' @importFrom tibble as_tibble
#' 
#' Read CSV or Excel Files
#'
#' @description
#' A utility function that reads either CSV or Excel files based on file extension.
#'
#' @param file_name Character string. Path to the file to be read
#' @param engine Character string. Which engine to use:
#'        "auto", "readr", "vroom", "fread", or "openxlsx"
#' @param ... Additional parameters passed to the underlying read function
#'
#' @return A tibble containing the data from the file, with column types
#'         restricted to logical, integer, double, character.
#'

read_csvxlsx <- function(file_name,
                         engine = c("auto", "readr", "vroom", "fread", "openxlsx"),
                         ...) {
  
  ## ── 允許型別 ────────────────────────────────────────────────────────
  allowed <- c("logical", "integer", "double", "character")
  
  ## ── 選擇 reader ──────────────────────────────────────────────────────
  pick_reader <- function(path, eng) {
    ext <- tolower(tools::file_ext(path))
    is_csv   <- ext == "csv"
    is_excel <- ext %in% c("xlsx", "xls")
    if (!is_csv && !is_excel) stop("不支援的檔案格式：", path)
    
    if (eng == "auto") {
      size_mb <- file.info(path)$size / 2^20
      eng <- dplyr::case_when(
        is_csv   & size_mb > 100 ~ "fread",
        is_csv   & size_mb >  20 ~ "vroom",
        is_csv                    ~ "readr",
        is_excel & size_mb >  20 ~ "openxlsx",
        TRUE                       ~ "readr"
      )
    }
    list(kind = if (is_csv) "csv" else "excel", engine = eng)
  }
  
  ## ── 核心讀檔器 ────────────────────────────────────────────────────────
  read_raw <- function(path, rdr, spec = NULL, ...) {
    k <- rdr$kind
    e <- rdr$engine
    out <- switch(e,
                  readr = if (k == "csv") {
                    readr::read_csv(path, col_types = spec, show_col_types = FALSE, ...)
                  } else {
                    readxl::read_excel(path, col_types = spec, ...)
                  },
                  vroom = { if (k != "csv") stop("vroom 只能讀 CSV")
                    vroom::vroom(path, col_types = spec, progress = FALSE, ...) },
                  fread = { if (k != "csv") stop("fread 只能讀 CSV")
                    data.table::fread(path, colClasses = spec, data.table = FALSE,
                                      showProgress = FALSE, ...) },
                  openxlsx = { if (k != "excel") stop("openxlsx 只能讀 Excel")
                    openxlsx::read.xlsx(path, colTypes = spec, ...) },
                  stop("未知 reader")
    )
    tibble::as_tibble(out)
  }
  
  engine <- match.arg(engine)
  rdr    <- pick_reader(file_name, engine)
  
  ## ── 第一次讀檔 ────────────────────────────────────────────────────────
  df1 <- read_raw(file_name, rdr, spec = NULL, ...)
  
  ## 1a. 將純整數的 double 欄位轉成 integer -----------------------------
  dbl_cols <- names(df1)[vapply(df1, is.double, logical(1))]
  for (nm in dbl_cols) {
    col <- df1[[nm]]
    if (all(is.na(col) | (col == floor(col)))) {
      df1[[nm]] <- as.integer(col)
    }
  }
  
  ## 1b. 把非允許型別轉成 character --------------------------------------
  bad_cols <- names(df1)[!sapply(df1, \(x) class(x)[1] %in% allowed)]
  if (length(bad_cols)) {
    df1[bad_cols] <- lapply(df1[bad_cols], as.character)
    message("⚙️  已轉成 character：", paste(bad_cols, collapse = ", "))
  }
  
  ## ── 建立期望型別向量 ----------------------------------------------------
  expect_types <- vapply(df1, \(x) class(x)[1], character(1))
  
  ## ── 生成各 reader 需要的型別規格 ---------------------------------------
  make_spec <- function(rdr, types) {
    e <- rdr$engine
    if (e %in% c("readr", "vroom")) {
      code <- c(logical = "l", integer = "i", double = "d", character = "c")
      paste(code[types], collapse = "")
    } else if (e == "fread") {
      as.list(types)           # list(colA = "integer", ...)
    } else if (e == "openxlsx") {
      vapply(types, dplyr::recode,
             logical   = "logical",
             integer   = "numeric",   # openxlsx 無 integer，先讀 numeric
             double    = "numeric",
             character = "text",
             character(1))
    } else stop("未知 engine")
  }
  spec <- make_spec(rdr, expect_types)
  
  ## ── 第二次讀檔：強制型別 -------------------------------------------------
  df2 <- read_raw(file_name, rdr, spec = spec, ...)
  
  ## 再保險：確保所有欄型都屬於 allowed -----------------------------------
  df2[] <- Map(\(col) {
    cls <- class(col)[1]
    if (cls %in% allowed) col else as.character(col)
  }, df2)
  
  message("✅ 已完成，所有欄位型別符合規格。")
  df2
}