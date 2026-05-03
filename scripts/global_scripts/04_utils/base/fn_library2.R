# library2: Load packages, install if missing, optionally check for updates
#
# Simplified version: uses force_update parameter directly.
# ACCELERATION_LEVEL removed per #204 — future performance config
# will live in app_config.yaml (per-company), not global_scripts.

library2 <- function(..., force_update = FALSE) {
  pkg_names <- as.character(unlist(list(...)))

  # Only query CRAN when force_update is requested
  cran_pkgs <- if (force_update) {
    tryCatch(available.packages(), error = function(e) {
      warning("Could not reach CRAN. Skipping version checks.")
      NULL
    })
  } else {
    NULL
  }
  manual_update <- c()

  for (pkg in pkg_names) {
    # Validate package name
    if (!is.character(pkg) || nchar(pkg) == 0 || tolower(pkg) == "false") {
      warning(sprintf("Invalid package name: '%s'. Skipping.", pkg))
      next
    }

    ns_loaded <- pkg %in% loadedNamespaces()
    attached <- paste0("package:", pkg) %in% search()

    if (!requireNamespace(pkg, quietly = TRUE)) {
      # Package not installed — install it
      message(sprintf("Installing missing package: %s", pkg))
      do.call(install.packages, list(pkg, dependencies = TRUE))
    } else if (force_update && !is.null(cran_pkgs)) {
      # Package installed — check for updates if requested
      installed_version <- packageVersion(pkg)

      if (pkg %in% rownames(cran_pkgs)) {
        available_version <- package_version(cran_pkgs[pkg, "Version"])
        needs_update <- installed_version < available_version

        if (needs_update) {
          if (ns_loaded) {
            warning(sprintf(
              "Package '%s' is loaded (v%s) and cannot be auto-updated to v%s.\nPlease restart R and run: install.packages('%s')",
              pkg, installed_version, available_version, pkg
            ))
            manual_update <- c(manual_update, pkg)
          } else {
            message(sprintf("Updating '%s' from version %s to %s", pkg, installed_version, available_version))
            tryCatch({
              do.call(install.packages, list(pkg, dependencies = TRUE))
            }, error = function(e) {
              warning(sprintf("Failed to update '%s': %s", pkg, e$message))
              manual_update <- c(manual_update, pkg)
            })
          }
        }
      }
    }

    # Attach if not already attached
    if (!attached) {
      tryCatch({
        library(pkg, character.only = TRUE)
      }, error = function(e) {
        message(sprintf("Failed to load '%s': %s", pkg, e$message))
      })
    }
  }

  # Summary of packages that need manual update
  if (length(manual_update) > 0) {
    cat("\nThe following packages need manual update (restart R first):\n")
    for (p in manual_update) {
      cat(sprintf("   install.packages('%s')\n", p))
    }
  }
}

# Usage:
# library2("dplyr", "ggplot2", "readr")
# library2("dplyr", force_update = TRUE)  # check CRAN for updates
