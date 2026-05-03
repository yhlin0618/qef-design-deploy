#LOCK FILE
#
# fn_strip_code_fence.R
#
# Following principles:
# - R21: One Function One File
# - R69: Function File Naming (fn_ prefix)
# - MP47: Functional Programming
#
# Function to strip code fence markers from text
# -----------------------------------------------------------------------------

#' Strip code fence markers from text
#' @param txt Character string. The text potentially containing code fence markers.
#' @return Character string. The text with code fence markers removed.
#' @examples
#' strip_code_fence("```r\ncode here\n```")
#' strip_code_fence("```\nplain text\n```")
strip_code_fence <- function(txt) {
  if (!requireNamespace("stringr", quietly = TRUE)) {
    # Fallback to base R if stringr is not available
    gsub("^```[A-Za-z0-9]*[ \\t]*\\r?\\n|\\r?\\n```[ \\t]*$", "", txt)
  } else {
    stringr::str_replace_all(
      txt,
      stringr::regex("^```[A-Za-z0-9]*[ \\t]*\\r?\\n|\\r?\\n```[ \\t]*$", multiline = TRUE),
      ""
    )
  }
}