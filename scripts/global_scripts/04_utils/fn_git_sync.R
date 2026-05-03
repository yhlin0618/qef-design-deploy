#' Git Sync Utility
#'
#' This script provides functions to easily sync with GitHub from within R.
#' It allows you to pull updates from and push changes to the remote repository.
#'
#' @author Claude
#' @date 2025-03-25

# Set the repository path (change this if the script is moved to a different location)
default_repo_path <- file.path("update_scripts", "global_scripts")

#' Run a git command and print its output
#'
#' @param cmd The git command to run
#' @param repo_path The path to the git repository
#' @return The result of the command
run_git_cmd <- function(cmd, repo_path = default_repo_path) {
  # Get the full path to the repository
  full_path <- normalizePath(repo_path, mustWork = TRUE)
  
  # Change to the repository directory
  original_dir <- getwd()
  setwd(full_path)
  on.exit(setwd(original_dir), add = TRUE)
  
  # Run the git command
  result <- system(cmd, intern = TRUE)
  
  # Print and return the result
  cat(paste(result, collapse = "\n"), "\n")
  invisible(result)
}

#' Pull changes from the remote repository
#'
#' @param repo_path The path to the git repository
#' @param remote The name of the remote repository
#' @param branch The branch to pull from
#' @return The result of the pull command
git_pull <- function(repo_path = default_repo_path, remote = "origin", branch = "main") {
  cat("Pulling changes from GitHub...\n")
  cmd <- sprintf("git pull %s %s", remote, branch)
  run_git_cmd(cmd, repo_path)
}

#' Get the git status
#'
#' @param repo_path The path to the git repository
#' @return The result of the status command
git_status <- function(repo_path = default_repo_path) {
  cat("Checking git status...\n")
  run_git_cmd("git status", repo_path)
}

#' Stage all changes
#'
#' @param repo_path The path to the git repository
#' @return The result of the add command
git_add_all <- function(repo_path = default_repo_path) {
  cat("Staging all changes...\n")
  run_git_cmd("git add -A", repo_path)
}

#' Commit changes
#'
#' @param message The commit message
#' @param repo_path The path to the git repository
#' @return The result of the commit command
git_commit <- function(message, repo_path = default_repo_path) {
  if (missing(message) || message == "") {
    stop("Commit message cannot be empty")
  }
  
  cat("Committing changes...\n")
  cmd <- sprintf('git commit -m "%s"', message)
  run_git_cmd(cmd, repo_path)
}

#' Push changes to the remote repository
#'
#' @param repo_path The path to the git repository
#' @param remote The name of the remote repository
#' @param branch The branch to push to
#' @return The result of the push command
git_push <- function(repo_path = default_repo_path, remote = "origin", branch = "main") {
  cat("Pushing changes to GitHub...\n")
  cmd <- sprintf("git push %s %s", remote, branch)
  run_git_cmd(cmd, repo_path)
}

#' Full sync workflow: pull, add changes, commit with message, and push
#'
#' @param commit_message The commit message for staged changes
#' @param repo_path The path to the git repository
#' @param remote The name of the remote repository
#' @param branch The branch to work with
#' @param skip_pull Set to TRUE to skip pulling changes
#' @return Invisibly returns TRUE if successful
git_sync <- function(commit_message, repo_path = default_repo_path, 
                     remote = "origin", branch = "main", skip_pull = FALSE) {
  
  if (!skip_pull) {
    git_pull(repo_path, remote, branch)
  }
  
  git_status(repo_path)
  
  # Ask for confirmation before proceeding
  response <- readline(prompt = "Do you want to add and commit all changes? (y/n): ")
  if (tolower(response) != "y") {
    cat("Operation cancelled.\n")
    return(invisible(FALSE))
  }
  
  git_add_all(repo_path)
  
  # If no commit message was provided, ask for one
  if (missing(commit_message) || commit_message == "") {
    commit_message <- readline(prompt = "Enter commit message: ")
    if (commit_message == "") {
      stop("Commit message cannot be empty")
    }
  }
  
  git_commit(commit_message, repo_path)
  git_push(repo_path, remote, branch)
  
  cat("GitHub sync completed successfully!\n")
  invisible(TRUE)
}

# Example usage:
# 
# # Pull changes only
# git_pull()
# 
# # Check status
# git_status()
# 
# # Add, commit, and push all in one go
# git_sync("Update utility functions")
#
# # Skip pull and just commit and push changes
# git_sync("Fix bug in function", skip_pull = TRUE)