# fn_import_adapter_csv.R - Generic CSV Import Adapter
# ==============================================================================
# Following MP064: ETL-Derivation Separation (adapter = pure data reading)
# Following DM_R037: Source-Type + Version ETL naming support
# Following SO_R007: One Function One File
#
# Adapter responsibility: Open file -> Read content -> Standardize column names
# NOT responsible for: Business logic, DB writes, validation
# ==============================================================================

#' Import Data from CSV Files
#'
#' Generic adapter for reading one or more CSV files into a single data.frame.
#' Uses data.table::fread for performance on large files.
#'
#' @param path Character. Path to a single .csv file OR a directory containing
#'   .csv files.
#' @param column_mapping Named character vector (optional). Maps source column
#'   names to target names, e.g. \code{c(target_col = "source_col")}.
#' @param recursive Logical. Scan subdirectories when \code{path} is a
#'   directory (default TRUE).
#' @param file_pattern Character. Regex pattern for file matching
#'   (default "\\\\.csv$").
#' @param encoding Character. File encoding (default "UTF-8").
#' @param add_source_path Logical. Add a \code{source_file} column with the
#'   originating file path (default TRUE).
#' @param verbose Logical. Print progress messages (default TRUE).
#' @param ... Additional arguments passed to \code{data.table::fread}.
#'
#' @return A \code{data.frame}. Returns an empty data.frame (0 rows) when no
#'   files are found.
import_from_csv <- function(path,
                            column_mapping = NULL,
                            recursive = TRUE,
                            file_pattern = "\\.csv$",
                            encoding = "UTF-8",
                            add_source_path = TRUE,
                            verbose = TRUE,
                            ...) {

  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Package 'data.table' is required. Install with: install.packages('data.table')")
  }

  # Resolve file list -----------------------------------------------------------
  if (dir.exists(path)) {
    files <- list.files(path, pattern = file_pattern,
                        recursive = recursive, full.names = TRUE,
                        ignore.case = TRUE)
    if (length(files) == 0) {
      if (verbose) message("import_from_csv: No CSV files found in ", path)
      return(data.frame())
    }
    if (verbose) message("import_from_csv: Found ", length(files), " CSV files")
  } else if (file.exists(path)) {
    files <- path
  } else {
    stop("import_from_csv: Path does not exist: ", path)
  }

  # Read each file --------------------------------------------------------------
  results <- list()
  for (i in seq_along(files)) {
    f <- files[i]
    tryCatch({
      if (verbose) message("  Reading [", i, "/", length(files), "]: ", basename(f))
      df <- as.data.frame(
        data.table::fread(f, encoding = encoding, data.table = FALSE,
                          showProgress = FALSE, ...)
      )

      if (nrow(df) == 0) {
        if (verbose) message("    Skipping empty file")
        next
      }

      # Standardise column names to snake_case
      names(df) <- tolower(names(df))
      names(df) <- gsub("[- ]+", "_", names(df))
      names(df) <- gsub("[^a-z0-9_]", "", names(df))

      # Apply column mapping
      if (!is.null(column_mapping)) {
        for (target in names(column_mapping)) {
          src <- column_mapping[[target]]
          if (src %in% names(df)) {
            names(df)[names(df) == src] <- target
          }
        }
      }

      # Add source file path
      if (add_source_path) {
        df$source_file <- basename(f)
      }

      results[[length(results) + 1]] <- df

    }, error = function(e) {
      warning("import_from_csv: Error reading ", basename(f), ": ", e$message)
    })
  }

  if (length(results) == 0) {
    if (verbose) message("import_from_csv: No data read from any file")
    return(data.frame())
  }

  # Bind rows (handle mismatched columns) --------------------------------------
  all_cols <- unique(unlist(lapply(results, names)))
  aligned <- lapply(results, function(df) {
    missing <- setdiff(all_cols, names(df))
    if (length(missing) > 0) {
      df[missing] <- NA
    }
    df[all_cols]
  })

  combined <- do.call(rbind, aligned)
  rownames(combined) <- NULL

  if (verbose) {
    message("import_from_csv: Combined ", nrow(combined), " rows, ",
            ncol(combined), " columns")
  }

  combined
}
