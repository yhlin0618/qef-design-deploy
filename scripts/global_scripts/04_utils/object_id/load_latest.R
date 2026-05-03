#' Load the latest/default version of an object
#'
#' Loads the most appropriate version of an object based on its rigid identifier.
#' If multiple versions exist, loads the latest or default version based on system policy.
#'
#' @param rigid_id The rigid identifier of the object
#' @param default_policy How to choose default version: "latest" (default), "original", or "manual"
#' @return The loaded object, or NULL if not found
#' @examples
#' load_latest("df.amazon.sales.by_product_index.at_ALL.now.001")
#'
load_latest <- function(rigid_id, default_policy = "latest") {
  available_versions <- list_versions(rigid_id)
  
  if (length(available_versions) == 0) {
    warning(paste("No versions found for", rigid_id))
    return(NULL)
  }
  
  # If only one version exists, load that
  if (length(available_versions) == 1) {
    return(load_version(rigid_id, available_versions[1]))
  }
  
  # Otherwise, apply the default policy
  version_to_load <- switch(default_policy,
    "latest" = available_versions[length(available_versions)],
    "original" = available_versions[1],
    "manual" = {
      # Find manual version if it exists
      manual_idx <- grep("manual", available_versions)
      if (length(manual_idx) > 0) {
        available_versions[manual_idx[1]]
      } else {
        available_versions[length(available_versions)]  # Fall back to latest
      }
    },
    available_versions[length(available_versions)]  # Default to latest
  )
  
  return(load_version(rigid_id, version_to_load))
}