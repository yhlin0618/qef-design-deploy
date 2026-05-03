#' @file fn_generate_create_table_from_yaml.R
#' @use yaml
#' @use DBI
#' @use duckdb
#' @requires fn_generate_create_table_query.R
#' @principle MP102 ETL Output Standardization (binds the authoring yaml files)
#' @principle MP156 Two-Tier Normativity (this file is a Tier-1 -> Tier-2 consumer)
#' @principle MP058 Database Table Creation Strategy
#' @principle SO_R007 One Function One File
#' @principle MP154 Side Effect Defense (sentinel-aware CHECK constraints)
#' @author Claude (spectra change `glue-layer-prerawdata-bridge`, #489 Phase 3)
#' @date 2026-04-27

#' Generate CREATE TABLE SQL from enriched authoring yaml
#'
#' Reads the canonical authoring schema yaml (per MP102 / MP156 binding) and
#' produces an engine-enforceable DuckDB CREATE TABLE statement. This is the
#' codegen entry point used by `01_db/raw_schema/_build.R` to populate
#' `01_db/raw_schema/_generated/`.
#'
#' Translation rules (yaml -> SQL):
#'   - `type:` mapped per `core_schemas.yaml#type_mapping`.
#'   - `required: true` -> NOT NULL constraint.
#'   - `pattern:` regex -> CHECK (col ~ pattern), with sentinel
#'      compatibility for optional fields: CHECK (col ~ pattern OR col = '').
#'   - `constraints:` literals translated to DuckDB-compatible CHECK fragments.
#'   - `coercion`, `aliases`, `fallback`, `description_zh` are NOT expressed
#'     in DDL (consumed at Phase 4 by `fn_glue_bridge`).
#'
#' Sentinel-aware patterns: For optional fields with `pattern:`, the empty
#' string sentinel ("") is allowed by adding an `OR col = ''` disjunct.
#' This preserves MP154 compliance: missing values use explicit sentinels
#' rather than NULL.
#'
#' @param schema_yaml_path Absolute path to `core_schemas.yaml`.
#' @param datatype Character: "sales" | "customers" | "orders" | "products" | "reviews".
#' @param platform Character platform_id (e.g., "amz", "official_website") or NULL
#'        for core-only DDL.
#' @param platform_extension_yaml_path Absolute path to platform extension yaml.
#'        Required when `platform` is non-NULL.
#' @param target_table Optional table name override.
#' @param con Optional DBI connection. If NULL, opens an in-memory DuckDB.
#' @return Character scalar with the CREATE TABLE SQL statement.
#' @export
generate_create_table_from_yaml <- function(schema_yaml_path,
                                            datatype,
                                            platform = NULL,
                                            platform_extension_yaml_path = NULL,
                                            target_table = NULL,
                                            con = NULL) {
  if (!file.exists(schema_yaml_path)) {
    stop("schema_yaml_path does not exist: ", schema_yaml_path)
  }
  if (!is.character(datatype) || length(datatype) != 1) {
    stop("datatype must be a single character string")
  }

  core <- yaml::read_yaml(schema_yaml_path)
  if (!datatype %in% names(core)) {
    stop("datatype '", datatype, "' not found. Available: ",
         paste(setdiff(names(core),
                       c("validation_rules", "type_mapping",
                         "bridge_mapping_notes")), collapse = ", "))
  }
  core_section <- core[[datatype]]
  required_fields <- core_section$required_fields
  if (is.null(required_fields) || length(required_fields) == 0) {
    stop("No required_fields defined for datatype: ", datatype)
  }

  ext_fields <- list()
  if (!is.null(platform)) {
    if (is.null(platform_extension_yaml_path)) {
      stop("platform_extension_yaml_path is required when platform is non-NULL")
    }
    if (!file.exists(platform_extension_yaml_path)) {
      stop("platform_extension_yaml_path does not exist: ",
           platform_extension_yaml_path)
    }
    ext <- yaml::read_yaml(platform_extension_yaml_path)
    if (datatype %in% names(ext) && !is.null(ext[[datatype]]$fields)) {
      ext_fields <- ext[[datatype]]$fields
    }
  }

  if (is.null(target_table)) {
    if (is.null(platform)) {
      target_table <- paste0("df_core_", datatype)
    } else {
      pattern_string <- core_section$table_pattern
      if (is.null(pattern_string)) {
        pattern_string <- paste0("df_{platform}_", datatype, "___raw")
      }
      target_table <- gsub("\\{platform\\}", platform, pattern_string,
                           fixed = FALSE)
    }
  }

  # Optional fields (added 2026-04-28 per qef-product-master-redesign 9.5 #460):
  # Some datatypes (company_product_extension, product_attributes_*) carry the
  # bulk of their schema in optional_fields. Generator now always emits them
  # alongside required_fields. Existing 5 core datatypes have no optional_fields
  # so this addition is a no-op for them — purely additive change.
  optional_fields <- core_section$optional_fields
  if (is.null(optional_fields)) optional_fields <- list()

  column_defs <- list()
  for (field_name in names(required_fields)) {
    field_spec <- required_fields[[field_name]]
    column_defs[[length(column_defs) + 1]] <- yaml_field_to_column_def(
      name = field_name, spec = field_spec)
  }
  for (field_name in names(optional_fields)) {
    field_spec <- optional_fields[[field_name]]
    column_defs[[length(column_defs) + 1]] <- yaml_field_to_column_def(
      name = field_name, spec = field_spec)
  }
  for (field_name in names(ext_fields)) {
    field_spec <- ext_fields[[field_name]]
    column_defs[[length(column_defs) + 1]] <- yaml_field_to_column_def(
      name = field_name, spec = field_spec)
  }

  owns_con <- FALSE
  if (is.null(con)) {
    if (!requireNamespace("duckdb", quietly = TRUE)) {
      stop("Package `duckdb` is required.")
    }
    con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
    owns_con <- TRUE
  }
  on.exit(if (owns_con) DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  if (!exists("generate_create_table_query")) {
    stop("generate_create_table_query() not loaded. ",
         "source('01_db/generate_create_table_query/fn_generate_create_table_query.R') first.")
  }

  sql <- generate_create_table_query(
    con = con,
    source_table = NULL,
    target_table = target_table,
    column_defs = column_defs,
    or_replace = FALSE,
    if_not_exists = TRUE,
    output_format = "sql"
  )

  header <- paste0(
    "-- Auto-generated by fn_generate_create_table_from_yaml.R\n",
    "-- Source: ", schema_yaml_path, "\n",
    if (!is.null(platform_extension_yaml_path)) {
      paste0("-- Platform extension: ", platform_extension_yaml_path, "\n")
    } else "",
    "-- Datatype: ", datatype,
    if (!is.null(platform)) paste0(", Platform: ", platform) else ", Core-only",
    "\n",
    "-- Bound by MP102 (v1.2) per MP156 Two-Tier Normativity (issue #489)\n",
    "-- DO NOT EDIT BY HAND. Edit the authoring yaml and re-run _build.R.\n",
    "\n"
  )
  paste0(header, sql)
}

#' Translate one yaml field spec to a column_def list element
#' @keywords internal
yaml_field_to_column_def <- function(name, spec) {
  type <- spec$type
  if (is.null(type)) {
    stop("Field '", name, "' missing type field")
  }
  duckdb_type <- switch(toupper(type),
    "VARCHAR"   = "VARCHAR",
    "TEXT"      = "VARCHAR",
    "INTEGER"   = "INTEGER",
    "NUMERIC"   = "DOUBLE",
    "BOOLEAN"   = "BOOLEAN",
    "TIMESTAMP" = "TIMESTAMP",
    {
      if (grepl("\\(", type)) type else type
    }
  )

  col_def <- list(name = name, type = duckdb_type)
  if (isTRUE(spec$required)) {
    col_def$not_null <- TRUE
  }

  check_clauses <- character(0)
  if (!is.null(spec$pattern) && nchar(spec$pattern) > 0) {
    pattern <- spec$pattern
    pattern_escaped <- gsub("'", "''", pattern, fixed = TRUE)
    if (isTRUE(spec$required)) {
      check_clauses <- c(check_clauses,
                         paste0(quote_identifier(name), " ~ '",
                                pattern_escaped, "'"))
    } else {
      check_clauses <- c(check_clauses,
                         paste0("(", quote_identifier(name), " ~ '",
                                pattern_escaped, "'", " OR ",
                                quote_identifier(name), " = '')"))
    }
  }

  raw_constraints <- spec$constraints
  if (is.null(raw_constraints)) raw_constraints <- character(0)
  for (raw_constraint in raw_constraints) {
    fragment <- translate_yaml_constraint(name, raw_constraint, type)
    if (!is.null(fragment) && nchar(fragment) > 0) {
      check_clauses <- c(check_clauses, fragment)
    }
  }

  if (length(check_clauses) > 0) {
    col_def$check <- paste(check_clauses, collapse = " AND ")
  }

  col_def
}

#' Translate a single yaml constraint string to a DuckDB CHECK fragment
#' @keywords internal
translate_yaml_constraint <- function(col_name, constraint, col_type) {
  constraint <- trimws(constraint)

  if (toupper(constraint) == "NOT NULL") return(NULL)
  if (toupper(constraint) == "LENGTH >= 0") return(NULL)

  if (grepl("^LENGTH\\s*>\\s*0$", constraint, ignore.case = TRUE)) {
    return(paste0("length(", quote_identifier(col_name), ") > 0"))
  }
  m <- regmatches(constraint,
                  regexec("^LENGTH\\s*=\\s*([0-9]+)$", constraint,
                          ignore.case = TRUE))[[1]]
  if (length(m) == 2) {
    return(paste0("length(", quote_identifier(col_name), ") = ", m[2]))
  }
  if (grepl("^LOWER\\s*\\(.*\\)\\s*=\\s*", constraint, ignore.case = TRUE)) {
    return(paste0("lower(", quote_identifier(col_name), ") = ",
                  quote_identifier(col_name)))
  }
  m <- regmatches(constraint,
                  regexec("^(>=|<=|>|<|=)\\s*([-0-9.]+)$", constraint))[[1]]
  if (length(m) == 3) {
    op <- m[2]
    val <- m[3]
    return(paste0(quote_identifier(col_name), " ", op, " ", val))
  }
  if (grepl(" OR ", constraint, ignore.case = TRUE)) {
    if (grepl("LENGTH", constraint, ignore.case = TRUE)) {
      expanded <- gsub("LENGTH\\s*=\\s*([0-9]+)",
                       paste0("length(", quote_identifier(col_name),
                              ") = \\1"),
                       constraint, ignore.case = TRUE)
      expanded <- gsub("LENGTH\\s*>\\s*0",
                       paste0("length(", quote_identifier(col_name),
                              ") > 0"),
                       expanded, ignore.case = TRUE)
      return(paste0("(", expanded, ")"))
    }
    return(paste0("(", constraint, ")"))
  }

  warning("Unrecognized constraint shape for column '", col_name,
          "': ", constraint, " (skipped)")
  NULL
}
