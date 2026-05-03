# Object Identification Utilities

This folder contains utilities for working with the object naming convention, which follows the pattern:

```
object_name = rigid_identifier___optional_descriptors
```

## Core Functions

The key functions in this module are:

### Identification Functions

- `get_rigid_id(object_name)` - Extract the rigid identifier from an object name
- `get_descriptors(object_name)` - Extract the optional descriptors from an object name

### Loading Functions

- `load_latest(rigid_id, default_policy = "latest")` - Load the latest/default version of an object
- `load_version(rigid_id, descriptor)` - Load a specific version of an object
- `list_versions(rigid_id)` - List all available versions of an object

### Saving Functions

- `save_version(object, rigid_id, descriptor = NULL, save_dir = "app_data", format = "rds")` - Save an object with version tracking

## Usage Examples

### Basic Usage

```r
# Extract components from an object name
rigid_id <- get_rigid_id("df.amazon.sales.by_product_index.at_ALL.now.001___manual")
# Returns: "df.amazon.sales.by_product_index.at_ALL.now.001"

descriptors <- get_descriptors("df.amazon.sales.by_product_index.at_ALL.now.001___manual")
# Returns: "manual"

# List available versions of an object
versions <- list_versions("df.amazon.sales.by_product_index.at_ALL.now.001")
# Might return: c("", "clean", "manual", "agg")

# Load the latest version (according to default policy)
data <- load_latest("df.amazon.sales.by_product_index.at_ALL.now.001")

# Load a specific version
manual_data <- load_version("df.amazon.sales.by_product_index.at_ALL.now.001", "manual")
```

### Saving Different Versions

```r
# Raw data
raw_data <- read.csv("sales_data.csv")
save_version(raw_data, "df.amazon.sales.by_product_index.at_ALL.now.001", NULL)

# After cleaning
clean_data <- clean_sales_data(raw_data)
save_version(clean_data, "df.amazon.sales.by_product_index.at_ALL.now.001", "clean")

# After manual adjustments
manual_data <- edit_problem_records(clean_data)
save_version(manual_data, "df.amazon.sales.by_product_index.at_ALL.now.001", "manual")

# After aggregation
agg_data <- aggregate_by_month(manual_data)
save_version(agg_data, "df.amazon.sales.by_product_index.at_ALL.now.001", "agg")
```

## Design Principles

This module implements the object naming convention established in R23 (Object Naming Convention) which defines the triple underscore (`___`) notation for separating the rigid identifier from optional descriptors.

The key principles are:

1. **Separation of Identification and Description**: The triple underscore clearly separates the part used for identification from the descriptive part.

2. **Version Management**: The module allows multiple versions of the same base object to exist with different descriptive tags.

3. **Consistent Access**: Objects can be referenced consistently by their rigid identifier, while still allowing specific versions to be loaded when needed.

4. **Persistence**: Objects can be saved with version information and loaded later, maintaining the version information.

5. **Discoverability**: Functions like `list_versions()` make it easy to discover what versions of an object exist.

## Relation to Naming Convention

This module directly implements the naming convention specified in R23 (Object Naming Convention) which defines:

```
df.platform.purpose.group.sliced.end_time.productlineid[___identifier]
```

Where the part before the triple underscore is the rigid identifier and the part after is the optional descriptor.