# Prompt for Checking Null References in Codebase

This prompt helps to check for null references in R code, such as function calls to functions that don't exist or have been renamed.

## Instructions for Claude

I need you to help me check for null references in my R codebase. Specifically, I need to verify that all function calls reference functions that actually exist in the codebase.

Please:

1. Search through all R files recursively in the specified directory
2. Identify any functions that are called but might not exist in the codebase
3. Pay special attention to recently renamed functions
4. Generate a report of potential null references

## Common Patterns to Check

1. Recursive functions that call themselves but with the wrong name
2. Functions recently renamed (camelCase to snake_case or vice versa)
3. Typos in function names
4. Functions moved to different files
5. Functions referenced without importing their package

## Example Commands to Help Find Issues

```bash
# Find all function definitions
grep -r "^[a-zA-Z0-9_.]+ <- function" --include="*.R" .

# Find specific function calls
grep -r "getRFilesRecursive" --include="*.R" .
grep -r "get_r_files_recursive" --include="*.R" .

# Look for potential recursive function calls
grep -r "function.*{" --include="*.R" . | grep -A 5 -B 5 "Recursive"
```

## Steps for Analysis

1. First, generate a list of all defined functions in the codebase
2. Then, extract all function calls from the codebase
3. Compare the lists to identify potential null references
4. Check for patterns that suggest references to renamed functions
5. Pay special attention to recursive functions that might call an old version of themselves

## Example Issues to Look For

```r
# Issue 1: Recursive function calls old name version of itself
get_r_files_recursive <- function(dir_path, pattern = "\\.R$") {
  # ...
  subdir_files <- getRFilesRecursive(subdir, pattern)  # Wrong! Should be get_r_files_recursive
  # ...
}

# Issue 2: Function renamed but not all references updated
# Old: calculateTotalSales()
# New: calculate_total_sales()
# But somewhere, code still calls: calculateTotalSales(data)

# Issue 3: Function moved to a package but called directly
# Code now requires: dplyr::filter()
# But somewhere still calls just: filter(data, condition)
```

## Output Format

Please provide your analysis in this format:

1. **Potential Null References**: List of functions called that might not exist
2. **Recently Renamed Functions**: Identify instances where old function names might still be in use
3. **File-by-File Analysis**: For each file with potential issues, show the line numbers and context
4. **Recommended Fixes**: Suggest code changes to resolve each potential null reference

## Specific Functions to Check

Please pay special attention to these recently renamed functions:

1. `getRFilesRecursive` → `get_r_files_recursive`
2. [Add other renamed functions here]

Thank you for helping ensure the codebase has no null references!