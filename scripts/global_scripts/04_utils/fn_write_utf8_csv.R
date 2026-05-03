#' Write CSV with UTF-8 BOM for Excel Compatibility
#'
#' @description
#' Writes a CSV file with UTF-8 encoding and BOM (Byte Order Mark).
#' The BOM ensures Excel can correctly identify and display Chinese characters.
#'
#' Excel on Windows defaults to system encoding (Big5 for Traditional Chinese).
#' Without BOM, Excel cannot auto-detect UTF-8, causing Chinese characters to
#' display as gibberish. Adding the 3-byte BOM (EF BB BF) at the start of the
#' file signals to Excel that the file is UTF-8 encoded.
#'
#' @principle UI_R020 UTF-8 BOM for Excel Compatibility
#' @principle MP114 Input Validation
#'
#' @param data data.frame. The data to export
#' @param file character. The output file path
#' @param na character. String to use for missing values (default: "")
#' @param row.names logical. Include row names? (default: FALSE)
#'
#' @return Invisible NULL. File is written as side effect.
#'
#' @details
#' Technical implementation:
#' 1. Writes CSV with UTF-8 encoding to temporary file
#' 2. Reads file as binary
#' 3. Prepends UTF-8 BOM (EF BB BF)
#' 4. Writes final file with BOM + content
#'
#' File size impact: +3 bytes (negligible)
#' Performance impact: < 100ms for typical datasets
#'
#' @examples
#' # Basic usage
#' write_utf8_csv_with_bom(mtcars, "output.csv")
#'
#' # In downloadHandler
#' output$download_data <- downloadHandler(
#'   filename = function() paste0("data_", Sys.Date(), ".csv"),
#'   content = function(file) {
#'     data <- prepare_data()
#'     write_utf8_csv_with_bom(data, file)
#'   }
#' )
#'
#' @export
write_utf8_csv_with_bom <- function(data, file, na = "", row.names = FALSE) {
  # MP114: Input validation
  if (!is.data.frame(data)) {
    stop("data must be a data.frame, got: ", class(data)[1])
  }

  if (nrow(data) == 0) {
    warning("Exporting empty data frame to: ", file)
  }

  if (!is.character(file) || length(file) != 1) {
    stop("file must be a single character string")
  }

  # Create temporary file
  temp_file <- tempfile(fileext = ".csv")
  on.exit(unlink(temp_file), add = TRUE)  # Ensure cleanup

  # Write CSV with UTF-8 encoding (without BOM)
  write.csv(data,
            file = temp_file,
            row.names = row.names,
            na = na,
            fileEncoding = "UTF-8")

  # Read the file as binary
  csv_content <- readBin(temp_file, "raw", file.info(temp_file)$size)

  # UTF-8 BOM: EF BB BF
  # This 3-byte sequence tells Excel "this file is UTF-8"
  bom <- as.raw(c(0xEF, 0xBB, 0xBF))

  # Write BOM + content to final file
  writeBin(c(bom, csv_content), file)

  invisible(NULL)
}


#' Create Standard Download Handler with UTF-8 BOM
#'
#' @description
#' Creates a standardized downloadHandler for CSV exports with UTF-8 BOM.
#' Ensures Excel compatibility for Chinese characters across all platforms.
#'
#' This is the recommended way to create download handlers in the MAMBA framework.
#' It handles edge cases like empty data, provides consistent filename formatting,
#' and ensures proper encoding for Chinese characters.
#'
#' @principle UI_R020 UTF-8 BOM for Excel Compatibility
#' @principle UI_R018 Download Button Placement (works with independent buttons)
#' @principle MP114 Input Validation
#' @principle MP122 Transparency (clear error messages)
#'
#' @param data_reactive reactive. Reactive expression returning data frame
#' @param filename_prefix character. Prefix for the filename (without extension)
#' @param date_format character. Format for date in filename (default: "%Y%m%d")
#' @param include_time logical. Include timestamp in filename? (default: FALSE)
#'
#' @return downloadHandler object for use in Shiny output
#'
#' @details
#' Filename format:
#' - Without time: {prefix}_{YYYYMMDD}.csv
#' - With time: {prefix}_{YYYYMMDD_HHMMSS}.csv
#'
#' Handles edge cases:
#' - NULL data: exports message "無可用資料"
#' - Empty data frame: exports message "無可用資料"
#' - Valid data: exports with UTF-8 BOM
#'
#' @examples
#' # Basic usage
#' output$download_data <- create_utf8_csv_download_handler(
#'   data_reactive = filtered_data,
#'   filename_prefix = "analysis_results"
#' )
#'
#' # With timestamp
#' output$download_data <- create_utf8_csv_download_handler(
#'   data_reactive = reactive({ get_current_data() }),
#'   filename_prefix = "position_data",
#'   include_time = TRUE
#' )
#'
#' # Custom date format
#' output$download_data <- create_utf8_csv_download_handler(
#'   data_reactive = results_data,
#'   filename_prefix = "InsightForge_分析結果",
#'   date_format = "%Y年%m月%d日"
#' )
#'
#' @export
create_utf8_csv_download_handler <- function(data_reactive,
                                               filename_prefix = "data",
                                               date_format = "%Y%m%d",
                                               include_time = FALSE) {
  # Input validation
  if (!is.reactive(data_reactive)) {
    stop("data_reactive must be a reactive expression")
  }

  if (!is.character(filename_prefix) || length(filename_prefix) != 1) {
    stop("filename_prefix must be a single character string")
  }

  downloadHandler(
    filename = function() {
      if (include_time) {
        timestamp <- format(Sys.time(), paste0(date_format, "_%H%M%S"))
      } else {
        timestamp <- format(Sys.Date(), date_format)
      }
      paste0(filename_prefix, "_", timestamp, ".csv")
    },

    content = function(file) {
      # Get data from reactive
      data <- tryCatch(
        data_reactive(),
        error = function(e) {
          warning("Error retrieving data: ", e$message)
          NULL
        }
      )

      # Validation and error handling
      if (is.null(data) || !is.data.frame(data) || nrow(data) == 0) {
        # Write empty message with UTF-8 BOM
        empty_df <- data.frame(
          訊息 = "無可用資料",
          Message = "No data available"
        )
        write_utf8_csv_with_bom(empty_df, file)
        return()
      }

      # Write data with UTF-8 BOM
      write_utf8_csv_with_bom(data, file)
    }
  )
}


#' Verify UTF-8 BOM in File
#'
#' @description
#' Utility function to check if a file has UTF-8 BOM.
#' Useful for debugging and testing.
#'
#' @param file character. Path to file to check
#'
#' @return logical. TRUE if file starts with UTF-8 BOM, FALSE otherwise
#'
#' @examples
#' write_utf8_csv_with_bom(mtcars, "test.csv")
#' verify_utf8_bom("test.csv")  # Should return TRUE
#'
#' write.csv(mtcars, "test_no_bom.csv", fileEncoding = "UTF-8")
#' verify_utf8_bom("test_no_bom.csv")  # Should return FALSE
#'
#' @export
verify_utf8_bom <- function(file) {
  if (!file.exists(file)) {
    stop("File does not exist: ", file)
  }

  # Read first 3 bytes
  first_bytes <- readBin(file, "raw", n = 3)

  # UTF-8 BOM is EF BB BF
  bom <- as.raw(c(0xEF, 0xBB, 0xBF))

  # Check if first 3 bytes match BOM
  identical(first_bytes, bom)
}
