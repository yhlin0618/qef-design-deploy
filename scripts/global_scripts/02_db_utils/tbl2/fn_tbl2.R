#' Enhanced tbl function with support for more data types
#'
#' @description
#' Extends the dplyr tbl function to support additional data types while
#' maintaining full compatibility with the dplyr ecosystem and pipe operator.
#' This function is a drop-in replacement for dplyr::tbl() with extended capabilities.
#'
#' @param src A data source object, extended to support more types
#' @param ... Additional arguments passed to specific methods
#'
#' @return A lazy tbl reference supporting dplyr verbs and collect()
#'
#' @examples
#' # Standard database usage (same as dplyr::tbl)
#' # conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
#' # tbl2(conn, "mtcars") %>% filter(cyl == 6) %>% collect()
#'
#' # Enhanced support for custom data types
#' # df_list <- list(customers = data.frame(id = 1:3, name = c("A", "B", "C")))
#' # tbl2(df_list, "customers") %>% filter(id > 1) %>% collect()
#'
#' @export
#' @importFrom dplyr tbl
#' @implements R91 Universal Data Access Pattern

tbl2 <- function(src, ...) {
  UseMethod("tbl2")
}

#' @export
#' @rdname tbl2
tbl2.default <- function(src, ...) {
  # Default method - try using dplyr::tbl
  tryCatch({
    dplyr::tbl(src, ...)
  }, error = function(e) {
    # Handle cases where dplyr::tbl would fail but we can convert
    if (can_convert_to_df(src)) {
      as_tbl_df(src)
    } else {
      stop("Unsupported data source type for tbl2: ", class(src)[1])
    }
  })
}

#' @export
#' @rdname tbl2
tbl2.DBIConnection <- function(src, from, ...) {
  # Pure passthrough to dplyr::tbl() — fully cross-driver via dbplyr.
  # For schema-qualified or attached-database access, pass
  # dbplyr::in_schema("schema", "table") explicitly as `from`.
  dplyr::tbl(src, from, ...)
}

#' @export
#' @rdname tbl2
tbl2.tbl_sql <- function(src, ...) {
  # Pass through for tbl_sql objects
  src
}

#' @export
#' @rdname tbl2
tbl2.data.frame <- function(src, ...) {
  # For data frames, return a tibble directly
  dplyr::as_tibble(src)
}

#' @export
#' @rdname tbl2
tbl2.tbl_df <- function(src, ...) {
  # For tibbles, return the tibble directly
  src
}

#' @export
#' @rdname tbl2
tbl2.list <- function(src, from = NULL, ...) {
  if (is.null(from)) {
    # No name provided - convert the entire list
    return(as_tbl_df(src))
  }
  
  # Try to find the data using multiple strategies
  
  # 1. Direct element access if it's a data frame
  if (from %in% names(src) && is.data.frame(src[[from]])) {
    return(dplyr::as_tibble(src[[from]]))
  }
  
  # 2. Try with "get_" prefix function
  getter <- paste0("get_", from)
  if (getter %in% names(src) && is.function(src[[getter]])) {
    result <- src[[getter]](...)
    return(dplyr::as_tibble(result))
  }
  
  # 3. Try a generic get_data method with the name parameter
  if (!is.null(src$get_data) && is.function(src$get_data)) {
    result <- src$get_data(from, ...)
    return(dplyr::as_tibble(result))
  }
  
  # 4. Try with pattern variations
  variants <- c(
    from,
    paste0("df_", from),
    paste0(from, "_df"),
    paste0(from, "_data"),
    paste0(from, "_tbl")
  )
  
  for (var in variants) {
    if (var %in% names(src) && is.data.frame(src[[var]])) {
      return(dplyr::as_tibble(src[[var]]))
    }
  }
  
  stop("No data found for '", from, "' in the list source")
}

#' @export
#' @rdname tbl2
tbl2.character <- function(src, ...) {
  # If src is a file path
  if (length(src) == 1 && file.exists(src)) {
    ext <- tolower(tools::file_ext(src))
    
    # Handle CSV files
    if (ext %in% c("csv", "tsv", "txt")) {
      if (!requireNamespace("readr", quietly = TRUE)) {
        stop("Package 'readr' is required to read CSV files.")
      }
      df <- readr::read_csv(src, ...)
      return(df)  # readr already returns tibbles
    }
    
    # Handle Excel files
    if (ext %in% c("xlsx", "xls")) {
      if (!requireNamespace("readxl", quietly = TRUE)) {
        stop("Package 'readxl' is required to read Excel files.")
      }
      df <- readxl::read_excel(src, ...)
      return(dplyr::as_tibble(df))
    }
    
    # Handle RDS files
    if (ext == "rds") {
      obj <- readRDS(src)
      return(as_tbl_df(obj))
    }
    
    # Handle RData files
    if (ext %in% c("rdata", "rda")) {
      e <- new.env()
      load(src, envir = e)
      objs <- ls(e)
      
      # If there's only one object, return it
      if (length(objs) == 1) {
        return(as_tbl_df(e[[objs[1]]]))
      }
      
      # If there are args that specify which object to get
      args <- list(...)
      if (!is.null(args$name) && args$name %in% objs) {
        return(as_tbl_df(e[[args$name]]))
      }
      
      # Otherwise collect all data frames into a list
      dfs <- list()
      for (obj in objs) {
        if (is.data.frame(e[[obj]])) {
          dfs[[obj]] <- e[[obj]]
        }
      }
      
      if (length(dfs) == 1) {
        return(dplyr::as_tibble(dfs[[1]]))
      } else if (length(dfs) > 0) {
        return(dfs)  # Return list of data frames
      }
      
      stop("No usable data frames found in '", src, "'")
    }
  }
  
  # For SQL queries or other character inputs, pass through to dplyr::tbl
  tryCatch({
    dplyr::tbl(src, ...)
  }, error = function(e) {
    stop("Cannot handle character input: ", e$message)
  })
}

#' @export
#' @rdname tbl2
tbl2.function <- function(src, ...) {
  # If src is a function, call it and convert result
  result <- src(...)
  return(as_tbl_df(result))
}

# Helper function to check if an object can be converted to a data frame
can_convert_to_df <- function(obj) {
  tryCatch({
    df <- as.data.frame(obj)
    return(TRUE)
  }, error = function(e) {
    return(FALSE)
  })
}

# Helper function to convert objects to tibbles
as_tbl_df <- function(obj) {
  if (is.data.frame(obj)) {
    return(dplyr::as_tibble(obj))
  } else {
    df <- tryCatch(as.data.frame(obj), error = function(e) {
      stop("Could not convert to data frame: ", e$message)
    })
    return(dplyr::as_tibble(df))
  }
}