# Universal SQL read helper using tbl2 (DM_R023)
# NOTE: Avoid DBI::dbGetQuery for reads. Use sql_read() instead.
sql_interpolate <- function(con, query, params = list()) {
  if (length(params) == 0) {
    return(query)
  }

  # Convert positional ? to named ?p1, ?p2 for DBI::sqlInterpolate
  idx <- seq_along(params)
  for (i in idx) {
    query <- sub("\\?", paste0("?p", i), query)
  }

  args <- setNames(params, paste0("p", idx))
  as.character(do.call(DBI::sqlInterpolate, c(list(con, query), args)))
}

sql_read <- function(con, query, params = list()) {
  if (missing(query) || is.null(query) || !nzchar(query)) {
    stop("sql_read(): query is missing or empty")
  }

  if (!exists("tbl2")) {
    tbl2_candidates <- c(
      file.path("scripts", "global_scripts", "02_db_utils", "tbl2", "fn_tbl2.R"),
      file.path("..", "global_scripts", "02_db_utils", "tbl2", "fn_tbl2.R"),
      file.path("..", "..", "global_scripts", "02_db_utils", "tbl2", "fn_tbl2.R"),
      file.path("..", "..", "..", "global_scripts", "02_db_utils", "tbl2", "fn_tbl2.R")
    )
    tbl2_path <- tbl2_candidates[file.exists(tbl2_candidates)][1]
    if (is.na(tbl2_path)) {
      stop("fn_tbl2.R not found in expected paths")
    }
    source(tbl2_path)
  }

  if (length(params) > 0) {
    query <- sql_interpolate(con, query, params)
  }

  dplyr::collect(tbl2(con, dplyr::sql(query)))
}
