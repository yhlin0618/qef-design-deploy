#' Check DuckDB File Lock Holders
#'
#' Detects which external processes are holding an OS-level lock on the given
#' DuckDB files. Helps pipeline pre-flight surface the common case where a
#' local Shiny dev session (`R --file=app.R` / `Rscript app.R`) or an orphan
#' REPL is holding the write lock on `app_data.duckdb`, preventing pipeline
#' DRV targets from opening the file RW.
#'
#' Driven by #437. Used by:
#' - `22_initializations/sc_Rprofile.R` autoinit() lock precheck
#' - `02_db_utils/duckdb/fn_dbConnectDuckdb.R` error enrichment on lock failures
#'
#' On non-macOS without `lsof`, falls back to graceful skip (returns empty
#' list + single warning). Lock detection is best-effort, not a security
#' boundary — use it for **operator-friendly error messages**, not gating.
#'
#' @param paths Named list or named character vector of DuckDB file paths.
#'   Typically a subset of `db_path_list`. Non-existent paths are skipped.
#' @param exclude_self Logical. If TRUE (default), exclude the caller's own
#'   `Sys.getpid()` from the returned holders — a script that is itself
#'   RW-connected shouldn't flag itself.
#'
#' @return List of holder entries, each a list with fields:
#'   \describe{
#'     \item{db_name}{Character. Key from the input `paths` list.}
#'     \item{file}{Character. Absolute path to the DuckDB file.}
#'     \item{pid}{Integer. Process ID of the holder.}
#'     \item{command}{Character. Full command line (via `ps -p <pid> -o command=`),
#'       falls back to lsof's truncated command name if `ps` fails.}
#'     \item{user}{Character. Login name of the process owner.}
#'   }
#'   Empty list if no foreign holders, or if `lsof` is unavailable.
#'
#' @section Tested by:
#'   `shared/global_scripts/98_test/general/test_check_db_locks.R`
#'
#' @export
NULL

#' Check if a command line belongs to a macOS system daemon
#'
#' #452:CloudStorage paths (Dropbox / iCloud) reliably trigger false-positive
#' lock holders from macOS system daemons — Spotlight `mdworker_shared`,
#' Apple File Provider `fileproviderd`, Time Machine, Finder Quick Look.
#' These are **read-only** indexers/syncers, NOT competing RW writers; they
#' hold fd briefly but don't block DuckDB's actual write semantics.
#' `check_db_locks()` filters holders whose command starts with these
#' canonical system paths to avoid aborting pipeline mid-run.
#'
#' **Path prefixes excluded**(macOS system-reserved, cryptographic signature
#' required, not user-writable → low spoof risk):
#' - `/System/Library/`       — Apple frameworks + canonical daemons
#' - `/System/iOSSupport/`    — iOS emulation layer
#' - `/System/Applications/`  — Apple-bundled apps(Mail, Calendar, ...)
#' - `/usr/sbin/`             — BSD system daemons
#' - `/usr/libexec/`          — auxiliary system binaries(bluetoothd, etc.)
#'
#' Non-matching commands (user R session, RStudio, homebrew, etc.) are
#' preserved as holders so real conflicts still surface.
#'
#' @param command Character. Full command line from ps (or "").
#' @return TRUE if command is a canonical macOS system daemon path; FALSE
#'   otherwise. NA / NULL / empty string / whitespace → FALSE.
#' @keywords internal
.is_system_daemon_command <- function(command) {
  if (is.null(command)) return(FALSE)
  if (length(command) != 1L) return(FALSE)
  if (is.na(command)) return(FALSE)
  cmd <- trimws(command)
  if (!nzchar(cmd)) return(FALSE)

  daemon_prefixes <- c(
    "/System/Library/",
    "/System/iOSSupport/",
    "/System/Applications/",
    "/usr/sbin/",
    "/usr/libexec/"
  )
  any(startsWith(cmd, daemon_prefixes))
}

check_db_locks <- function(paths, exclude_self = TRUE) {
  if (is.null(paths)) return(list())
  if (is.character(paths)) {
    # Allow named character vector — convert to named list
    if (is.null(names(paths)) || any(!nzchar(names(paths)))) {
      stop("check_db_locks: `paths` must be named (key = short db name)",
           call. = FALSE)
    }
    paths <- as.list(paths)
  }
  if (!is.list(paths) || is.null(names(paths))) {
    stop("check_db_locks: `paths` must be a named list or named character vector",
         call. = FALSE)
  }

  lsof_bin <- Sys.which("lsof")
  if (!nzchar(lsof_bin)) {
    warning("check_db_locks: `lsof` not found on PATH; lock detection skipped. ",
            "Consider installing lsof for pipeline pre-flight lock diagnostics.",
            call. = FALSE)
    return(list())
  }

  self_pid <- Sys.getpid()

  get_full_command <- function(pid) {
    if (is.null(pid) || is.na(pid) || pid <= 0L) return("")
    # -ww (F10 from verify-437): force full untruncated command on Linux GNU ps
    # (macOS ps already unlimited by default; -ww is a no-op there).
    tryCatch({
      out <- suppressWarnings(system2(
        "ps", c("-ww", "-p", pid, "-o", "command="),
        stdout = TRUE, stderr = FALSE
      ))
      trimws(paste(out, collapse = " "))
    }, error = function(e) "")
  }

  holders <- list()
  for (name in names(paths)) {
    path <- paths[[name]]
    if (is.null(path) || is.na(path) || !nzchar(path) || !file.exists(path)) next

    # -F pcL: pid / command / login-name fields, prefixed one per line
    # Non-zero exit is expected when nothing holds the file — suppress stderr
    # and treat empty stdout as "no holders".
    output <- suppressWarnings(tryCatch(
      system2(lsof_bin, c("-F", "pcL", path),
              stdout = TRUE, stderr = FALSE),
      error = function(e) character()
    ))
    if (length(output) == 0L) next

    # lsof -F output format: one line per field, grouped per process.
    #   p<pid>
    #   c<command>
    #   L<login>
    # A new process block starts at the next 'p' line.
    current <- list()
    flush <- function() {
      # F7 from verify-437: reject NA / non-positive pid so malformed lsof
      # lines (empty `p\n`, `pabc\n`, permission errors) don't bleed into
      # stop() messages as phantom "pid=NA" holders.
      if (is.null(current$pid) || is.na(current$pid) || current$pid <= 0L) {
        return()
      }
      if (exclude_self && identical(current$pid, self_pid)) return()

      # Resolve full command line early — needed for system daemon detection.
      full_cmd <- get_full_command(current$pid)
      effective_cmd <- if (nzchar(full_cmd)) full_cmd else (current$command %||% "")

      # #452: Filter macOS system daemons (mdworker_shared, fileproviderd,
      # Time Machine, etc.) that commonly hold fd on Dropbox/iCloud
      # CloudStorage paths for indexing / syncing. They are NOT competing
      # RW writers, so treating them as lock holders causes spurious
      # pipeline aborts. See .is_system_daemon_command() for whitelist.
      if (.is_system_daemon_command(effective_cmd)) {
        if (nzchar(Sys.getenv("AUTOINIT_DEBUG", ""))) {
          message("[check_db_locks] excluding system daemon holder: pid=",
                  current$pid, " cmd=", substr(effective_cmd, 1, 120))
        }
        return()
      }

      entry <- list(
        db_name = name,
        file    = path,
        pid     = current$pid,
        command = effective_cmd,
        user    = current$user %||% ""
      )
      holders[[length(holders) + 1L]] <<- entry
    }
    for (line in output) {
      if (!nzchar(line)) next
      tag <- substr(line, 1L, 1L)
      val <- substr(line, 2L, nchar(line))
      if (tag == "p") {
        flush()
        current <- list(pid = suppressWarnings(as.integer(val)))
      } else if (tag == "c") {
        current$command <- val
      } else if (tag == "L") {
        current$user <- val
      }
    }
    flush()
  }
  holders
}

# %||% fallback for R < 4.4
if (!exists("%||%", mode = "function")) {
  `%||%` <- function(a, b) if (is.null(a)) b else a
}
