#' Build Condition Grids from a Dimension Manifest
#'
#' This utility loads dimension values from a YAML manifest and optional CSV
#' sources, then generates a Cartesian product grid for filtered derivations.
#'
#' @param manifest_path Optional path to dimension manifest YAML.
#' @param app_dir Optional app root. Defaults to APP_DIR when available.
#' @param verbose Logical. Print load details.
#'
#' @return A named list of dimension vectors (components) or a data frame grid.
#' @export

resolve_dimensions_manifest_path <- function(manifest_path = NULL, app_dir = NULL) {
  if (!is.null(manifest_path) && nzchar(manifest_path)) {
    return(manifest_path)
  }

  if (is.null(app_dir) || !nzchar(app_dir)) {
    if (exists("APP_DIR", inherits = TRUE)) {
      app_dir <- APP_DIR
    } else {
      app_dir <- getwd()
    }
  }

  file.path(app_dir, "data", "app_data", "parameters", "scd_type1", "list_dimensions_manifest.yaml")
}


read_dimensions_manifest <- function(manifest_path = NULL, app_dir = NULL) {
  if (!requireNamespace("yaml", quietly = TRUE)) {
    stop("Package 'yaml' is required to read dimension manifest.")
  }

  path <- resolve_dimensions_manifest_path(manifest_path = manifest_path, app_dir = app_dir)
  if (!file.exists(path)) {
    stop("Dimension manifest not found: ", path)
  }

  manifest <- yaml::read_yaml(path, eval.expr = FALSE)
  if (is.null(manifest$dimensions) || !is.list(manifest$dimensions)) {
    stop("Invalid manifest: missing 'dimensions' list in ", path)
  }

  manifest
}


load_dimension_components <- function(manifest_path = NULL,
                                      app_dir = NULL,
                                      verbose = FALSE) {
  manifest <- read_dimensions_manifest(manifest_path = manifest_path, app_dir = app_dir)

  if (is.null(app_dir) || !nzchar(app_dir)) {
    if (exists("APP_DIR", inherits = TRUE)) {
      app_dir <- APP_DIR
    } else {
      app_dir <- getwd()
    }
  }

  read_csv_table <- function(path) {
    if (exists("read_csvxlsx", mode = "function")) {
      return(as.data.frame(read_csvxlsx(path), stringsAsFactors = FALSE))
    }
    if (requireNamespace("readr", quietly = TRUE)) {
      return(as.data.frame(readr::read_csv(path, show_col_types = FALSE), stringsAsFactors = FALSE))
    }
    utils::read.csv(path, stringsAsFactors = FALSE)
  }

  normalize_values <- function(values, spec) {
    if (is.null(values)) values <- character(0)
    values <- as.character(values)
    values <- values[nzchar(trimws(values))]

    if (isTRUE(spec$tolower)) values <- tolower(values)
    if (isTRUE(spec$toupper)) values <- toupper(values)

    if (!is.null(spec$prepend_values)) {
      values <- c(as.character(spec$prepend_values), values)
    }
    if (!is.null(spec$append_values)) {
      values <- c(values, as.character(spec$append_values))
    }

    if (!identical(spec$unique_values, FALSE)) {
      values <- unique(values)
    }
    if (isTRUE(spec$sort_values)) {
      values <- sort(values)
    }

    values
  }

  resolve_csv_values <- function(spec) {
    if (is.null(spec$source_file) || !nzchar(spec$source_file)) {
      stop("csv_column source requires 'source_file'.")
    }
    if (is.null(spec$value_column) || !nzchar(spec$value_column)) {
      stop("csv_column source requires 'value_column'.")
    }

    source_file <- spec$source_file
    csv_path <- if (grepl("^/", source_file)) source_file else file.path(app_dir, source_file)
    if (!file.exists(csv_path)) {
      stop("CSV source not found: ", csv_path)
    }

    df <- read_csv_table(csv_path)
    names(df) <- sub("^\\ufeff", "", names(df))

    value_column <- as.character(spec$value_column)
    if (!value_column %in% names(df)) {
      stop("CSV source missing value_column '", value_column, "': ", csv_path)
    }

    if (!is.null(spec$include_column) && nzchar(as.character(spec$include_column))) {
      include_column <- as.character(spec$include_column)
      if (!include_column %in% names(df)) {
        stop("CSV source missing include_column '", include_column, "': ", csv_path)
      }

      include_values <- spec$include_values
      if (is.null(include_values) && !is.null(spec$include_value)) {
        include_values <- spec$include_value
      }
      if (is.null(include_values)) {
        include_values <- TRUE
      }

      include_values <- as.character(include_values)
      df <- df[as.character(df[[include_column]]) %in% include_values, , drop = FALSE]
    }

    as.character(df[[value_column]])
  }

  components <- list()
  for (dimension_name in names(manifest$dimensions)) {
    spec <- manifest$dimensions[[dimension_name]]
    if (isTRUE(spec$enabled == FALSE)) next

    source_type <- if (is.null(spec$source_type)) "" else as.character(spec$source_type)
    values <- NULL

    if (identical(source_type, "inline") || (!nzchar(source_type) && !is.null(spec$values))) {
      values <- spec$values
    } else if (identical(source_type, "csv_column")) {
      values <- resolve_csv_values(spec)
    } else {
      stop("Unsupported source_type for dimension '", dimension_name, "': ", source_type)
    }

    values <- normalize_values(values, spec)
    if (length(values) == 0) {
      warning("Dimension has no values and will be skipped: ", dimension_name)
      next
    }

    components[[dimension_name]] <- values
    if (isTRUE(verbose)) {
      message(sprintf("Loaded dimension '%s' with %d values", dimension_name, length(values)))
    }
  }

  if (length(components) == 0) {
    stop("No dimension components loaded from manifest.")
  }

  components
}


build_condition_grid <- function(components) {
  if (is.null(components) || !is.list(components) || length(components) == 0) {
    stop("components must be a non-empty list of dimension vectors.")
  }
  if (!requireNamespace("tidyr", quietly = TRUE)) {
    stop("Package 'tidyr' is required to build condition grid.")
  }

  do.call(tidyr::expand_grid, components)
}


filter_condition_components <- function(components,
                                        time_values = NULL,
                                        product_values = NULL,
                                        state_values = NULL,
                                        source_values = NULL,
                                        country_values = NULL,
                                        extra_filters = NULL) {
  if (is.null(components) || !is.list(components) || length(components) == 0) {
    stop("components must be a non-empty list.")
  }

  apply_named_filter <- function(target_names, filter_values) {
    if (is.null(filter_values)) return(invisible(NULL))
    idx <- which(names(components) %in% target_names)
    if (length(idx) == 0) return(invisible(NULL))
    target_name <- names(components)[idx[[1]]]
    components[[target_name]] <<- intersect(
      as.character(components[[target_name]]),
      as.character(filter_values)
    )
  }

  apply_named_filter(c("time_condition"), time_values)
  apply_named_filter(c("product_line_id_sliced"), product_values)
  apply_named_filter(c("state_filter"), state_values)
  apply_named_filter(c("source_filter"), source_values)
  apply_named_filter(c("country_filter"), country_values)

  if (!is.null(extra_filters)) {
    if (!is.list(extra_filters)) {
      stop("extra_filters must be NULL or a named list.")
    }
    for (nm in names(extra_filters)) {
      if (nm %in% names(components)) {
        components[[nm]] <- intersect(
          as.character(components[[nm]]),
          as.character(extra_filters[[nm]])
        )
      }
    }
  }

  empty_dims <- names(components)[vapply(components, function(x) length(x) == 0, logical(1))]
  if (length(empty_dims) > 0) {
    stop("Filtered components contain empty dimensions: ", paste(empty_dims, collapse = ", "))
  }

  components
}


# Backward-compatible aliases ---------------------------------------------------
get_condition_grid <- function(components = NULL, manifest_path = NULL, app_dir = NULL) {
  if (is.null(components)) {
    components <- load_dimension_components(manifest_path = manifest_path, app_dir = app_dir)
  }
  build_condition_grid(components)
}


get_filtered_condition_grid <- function(time_values = NULL,
                                        product_values = NULL,
                                        state_values = NULL,
                                        source_values = NULL,
                                        country_values = NULL,
                                        components = NULL,
                                        manifest_path = NULL,
                                        app_dir = NULL,
                                        extra_filters = NULL) {
  if (is.null(components)) {
    components <- load_dimension_components(manifest_path = manifest_path, app_dir = app_dir)
  }

  filtered_components <- filter_condition_components(
    components = components,
    time_values = time_values,
    product_values = product_values,
    state_values = state_values,
    source_values = source_values,
    country_values = country_values,
    extra_filters = extra_filters
  )

  build_condition_grid(filtered_components)
}
