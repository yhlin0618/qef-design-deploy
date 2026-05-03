# Initialization and Deinitialization Syntax in NSQL

This document defines the syntax for initialization and deinitialization directives in NSQL.

## INITIALIZE Directive

The INITIALIZE directive is used to set up the environment for script execution.

### Mode-Based Initialization

```
INITIALIZE IN {mode}
```

Where:
- `{mode}` is the operation mode (UPDATE_MODE, APP_MODE, etc.)

### Examples

```
INITIALIZE IN UPDATE_MODE
```

```
INITIALIZE IN APP_MODE
```

```
INITIALIZE DATABASE ONLY
```

## INITIALIZE_SYNTAX Block

The INITIALIZE_SYNTAX block provides a templated initialization pattern with error handling.

### Syntax

```
INITIALIZE_SYNTAX
  $libraries = {library_imports}
  $environment_setup = {setup_code}
```

Where:
- `{library_imports}` is R code for importing required libraries
- `{setup_code}` is R code for setting up environment variables and parameters

### Example

```
INITIALIZE_SYNTAX
  $libraries = library(dplyr)
  library(tidyr)
  library(DBI)
  library(duckdb)
  
  $environment_setup = options(stringsAsFactors = FALSE)
  options(scipen = 999)
  DATA_PATH <- file.path("data", "processed")
```

## DEINITIALIZE_SYNTAX Block

The DEINITIALIZE_SYNTAX block provides a templated cleanup pattern with error handling.

### Syntax

```
DEINITIALIZE_SYNTAX
  $close_connections = {connection_cleanup}
  $reset_environment = {environment_cleanup}
```

Where:
- `{connection_cleanup}` is R code for closing database connections
- `{environment_cleanup}` is R code for cleaning up environment variables

### Example

```
DEINITIALIZE_SYNTAX
  $close_connections = if(exists("con") && inherits(con, "DBIConnection")) dbDisconnect(con)
  
  $reset_environment = rm(list = setdiff(ls(), c("results")))
```

## R Code Generation

The initialization directives translate to R code:

```r
# For INITIALIZE IN UPDATE_MODE
source(file.path("update_scripts", "global_scripts", "00_principles", "sc_initialization_update_mode.R"))

# For INITIALIZE_SYNTAX
tryCatch({
  # Load required libraries
  suppressPackageStartupMessages({
    library(dplyr)
    library(tidyr)
    library(DBI)
    library(duckdb)
  })
  
  # Set environment variables and parameters
  options(stringsAsFactors = FALSE)
  options(scipen = 999)
  DATA_PATH <- file.path("data", "processed")
  
  message("Initialization completed successfully")
}, error = function(e) {
  message("Error during initialization: ", e$message)
  return(FALSE)
})

# For DEINITIALIZE_SYNTAX
tryCatch({
  # Close open connections
  if(exists("con") && inherits(con, "DBIConnection")) dbDisconnect(con)
  
  # Reset environment variables
  rm(list = setdiff(ls(), c("results")))
  
  message("Deinitialization completed successfully")
}, error = function(e) {
  message("Error during deinitialization: ", e$message)
  return(FALSE)
})
```

## Grammar (EBNF)

```ebnf
initialization_directive ::= mode_initialization | syntax_initialization

mode_initialization ::= 'INITIALIZE' 'IN' mode

mode ::= 'UPDATE_MODE' | 'APP_MODE' | 'DATABASE' 'ONLY' | mode_name

syntax_initialization ::= initialize_syntax_block | deinitialize_syntax_block

initialize_syntax_block ::= 'INITIALIZE_SYNTAX' library_parameter environment_parameter

deinitialize_syntax_block ::= 'DEINITIALIZE_SYNTAX' connection_parameter environment_parameter

library_parameter ::= '$libraries' '=' r_code

environment_parameter ::= '$environment_setup' '=' r_code

connection_parameter ::= '$close_connections' '=' r_code

environment_parameter ::= '$reset_environment' '=' r_code

r_code ::= any_valid_r_code

mode_name ::= identifier
```