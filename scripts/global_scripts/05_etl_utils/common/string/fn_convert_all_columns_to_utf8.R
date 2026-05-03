#' Convert All Data Frame Columns to UTF-8 Encoding
#'
#' This ETL utility function intelligently converts all character and factor
#' columns in a data frame to UTF-8 encoding. It can auto-detect encoding
#' or use a specified encoding for conversion. After conversion, it optionally
#' removes any illegal UTF-8 characters.
#'
#' @param df A data frame containing columns to be converted
#' @param from_encoding Optional character string specifying the source encoding.
#'                     If NULL (default), the function will auto-detect encoding
#'                     using stringi::stri_enc_detect()
#' @param remove_illegal_chars Logical flag to remove illegal UTF-8 characters after conversion.
#'                            Default is TRUE.
#' @return A data frame with all text columns converted to UTF-8
#'
#' @details
#' The function processes each column in the data frame:
#' - Character columns: Detects encoding and converts to UTF-8
#' - Factor columns: Converts to character, processes, then converts back to factor
#' - Other columns: Left unchanged
#' 
#' Encoding detection is performed on the first non-NA value in each column.
#' If detection fails or all values are NA, UTF-8 is assumed.
#' 
#' After encoding conversion, the function can optionally remove illegal UTF-8
#' characters using the remove_illegal_utf8 function.
#'
#' @note This function requires the 'stringi' package for encoding detection.
#'
#' @examples
#' \dontrun{
#' # Auto-detect and convert encoding, with illegal character removal
#' utf8_df <- convert_all_columns_to_utf8(raw_df)
#' 
#' # Convert from specific encoding without removing illegal characters
#' utf8_df <- convert_all_columns_to_utf8(raw_df, from_encoding = "Big5", remove_illegal_chars = FALSE)
#' }
#'
#' @export
convert_all_columns_to_utf8 <- function(df, from_encoding = NULL, remove_illegal_chars = TRUE) {
  # Ensure stringi package is available
  if (!requireNamespace("stringi", quietly = TRUE)) {
    stop("Package 'stringi' is required for encoding detection")
  }
  
  # Source the remove_illegal_utf8 function if remove_illegal_chars is TRUE
  if (remove_illegal_chars) {
    if (!exists("remove_illegal_utf8", mode = "function")) {
      source(here::here("scripts", "global_scripts", "04_utils", "string", "fn_remove_illegal_utf8.R"))
    }
  }
  
  df[] <- lapply(df, function(x) {
    # 處理字串欄位
    if (is.character(x)) {
      enc <- from_encoding
      if (is.null(enc)) {
        # 取得第一個非 NA 值
        first_val <- x[which(!is.na(x))[1]]
        if (length(first_val) == 0 || is.na(first_val)) {
          enc <- "UTF-8"  # 若欄位內皆為 NA，就直接當作 UTF-8
        } else {
          detected <- stringi::stri_enc_detect(first_val)
          if (length(detected) > 0 && !is.null(detected[[1]]$Encoding)) {
            enc <- detected[[1]]$Encoding[1]
          } else {
            enc <- "UTF-8"
          }
        }
      }
      # 轉換為 UTF-8
      iconv(x, from = enc, to = "UTF-8", sub = "")
    } else if (is.factor(x)) {
      # 先轉成字串處理
      x_char <- as.character(x)
      enc <- from_encoding
      if (is.null(enc)) {
        first_val <- x_char[which(!is.na(x_char))[1]]
        if (length(first_val) == 0 || is.na(first_val)) {
          enc <- "UTF-8"
        } else {
          detected <- stringi::stri_enc_detect(first_val)
          if (length(detected) > 0 && !is.null(detected[[1]]$Encoding)) {
            enc <- detected[[1]]$Encoding[1]
          } else {
            enc <- "UTF-8"
          }
        }
      }
      x_utf8 <- iconv(x_char, from = enc, to = "UTF-8", sub = "")
      factor(x_utf8)
    } else {
      x
    }
  })
  
  # Apply illegal UTF-8 character removal if requested
  if (remove_illegal_chars) {
    df <- remove_illegal_utf8(df)
  }
  
  return(df)
}