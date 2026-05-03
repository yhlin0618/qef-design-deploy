#' Verify System Consistency
#'
#' @description
#' Verifies naming consistency across the system.
#'
#' @return NULL if consistent, or a list of issues if inconsistent
#'
verify_consistency <- function() {
  issues <- list()
  
  # Check for duplicate MP numbers
  mp_files <- list.files(pattern = "^MP[0-9]+_.*\\.md$")
  mp_numbers <- gsub("^MP([0-9]+)_.*\\.md$", "\\1", mp_files)
  mp_duplicates <- mp_numbers[duplicated(mp_numbers)]
  
  if (length(mp_duplicates) > 0) {
    issues$mp_duplicates <- mp_files[mp_numbers %in% mp_duplicates]
  }
  
  # Check for duplicate P numbers
  p_files <- list.files(pattern = "^P[0-9]+_.*\\.md$")
  p_numbers <- gsub("^P([0-9]+)_.*\\.md$", "\\1", p_files)
  p_duplicates <- p_numbers[duplicated(p_numbers)]
  
  if (length(p_duplicates) > 0) {
    issues$p_duplicates <- p_files[p_numbers %in% p_duplicates]
  }
  
  # Check for duplicate R numbers
  r_files <- list.files(pattern = "^R[0-9]+_.*\\.md$")
  r_numbers <- gsub("^R([0-9]+)_.*\\.md$", "\\1", r_files)
  r_duplicates <- r_numbers[duplicated(r_numbers)]
  
  if (length(r_duplicates) > 0) {
    issues$r_duplicates <- r_files[r_numbers %in% r_duplicates]
  }
  
  # Extract all references to MP/P/R from files
  all_files <- list.files(pattern = "\\.(md|R)$", recursive = TRUE)
  all_references <- list()
  
  for (file in all_files) {
    if (file.exists(file)) {
      content <- readLines(file)
      
      # Extract references to MP numbers
      mp_refs <- unlist(regmatches(content, gregexpr('"MP[0-9]+"', content)))
      if (length(mp_refs) > 0) {
        all_references$mp <- c(all_references$mp, gsub('"(MP[0-9]+)"', "\\1", mp_refs))
      }
      
      # Extract references to P numbers
      p_refs <- unlist(regmatches(content, gregexpr('"P[0-9]+"', content)))
      if (length(p_refs) > 0) {
        all_references$p <- c(all_references$p, gsub('"(P[0-9]+)"', "\\1", p_refs))
      }
      
      # Extract references to R numbers
      r_refs <- unlist(regmatches(content, gregexpr('"R[0-9]+"', content)))
      if (length(r_refs) > 0) {
        all_references$r <- c(all_references$r, gsub('"(R[0-9]+)"', "\\1", r_refs))
      }
    }
  }
  
  # Check for references to non-existent MP files
  if (length(all_references$mp) > 0) {
    actual_mp <- gsub("_.*\\.md$", "", mp_files)
    missing_mp <- setdiff(unique(all_references$mp), actual_mp)
    
    if (length(missing_mp) > 0) {
      issues$missing_mp <- missing_mp
    }
  }
  
  # Check for references to non-existent P files
  if (length(all_references$p) > 0) {
    actual_p <- gsub("_.*\\.md$", "", p_files)
    missing_p <- setdiff(unique(all_references$p), actual_p)
    
    if (length(missing_p) > 0) {
      issues$missing_p <- missing_p
    }
  }
  
  # Check for references to non-existent R files
  if (length(all_references$r) > 0) {
    actual_r <- gsub("_.*\\.md$", "", r_files)
    missing_r <- setdiff(unique(all_references$r), actual_r)
    
    if (length(missing_r) > 0) {
      issues$missing_r <- missing_r
    }
  }
  
  # Check if README.md includes all principles
  if (file.exists("README.md")) {
    readme_content <- readLines("README.md")
    
    # Check for MP files not mentioned in README
    mp_in_readme <- unlist(lapply(mp_files, function(f) any(grepl(f, readme_content))))
    if (any(!mp_in_readme)) {
      issues$mp_not_in_readme <- mp_files[!mp_in_readme]
    }
    
    # Check for P files not mentioned in README
    p_in_readme <- unlist(lapply(p_files, function(f) any(grepl(f, readme_content))))
    if (any(!p_in_readme)) {
      issues$p_not_in_readme <- p_files[!p_in_readme]
    }
    
    # Check for R files not mentioned in README
    r_in_readme <- unlist(lapply(r_files, function(f) any(grepl(f, readme_content))))
    if (any(!r_in_readme)) {
      issues$r_not_in_readme <- r_files[!r_in_readme]
    }
  }
  
  # Return NULL if no issues found, otherwise return the list of issues
  if (length(issues) == 0) {
    return(NULL)
  } else {
    return(issues)
  }
}
