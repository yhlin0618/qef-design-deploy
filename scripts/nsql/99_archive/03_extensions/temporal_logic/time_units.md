# NSQL Temporal Units

NSQL defines a hierarchy of time units to describe application execution with mathematical precision. These units form the foundation for temporal reasoning about code execution and resource utilization.

## Fundamental Time Units

### Phase
A major time period in the application lifecycle with a distinct purpose and resource allocation.

**Characteristics of a Phase**:
- Encompasses a significant portion of application time
- Has a clear beginning and end
- Serves a distinct purpose in the application lifecycle
- Contains multiple stages that execute in sequence
- Allocates resources for a specific overall objective

**Examples**:
- Initialization Phase
- Runtime Phase
- Termination Phase

### Stage
A process that spends time and resources to accomplish a specific task.

**Characteristics of a Stage**:
- Consumes a measurable amount of time
- Uses computational resources (CPU, memory, I/O)
- Performs a well-defined set of operations
- Has clear inputs and outputs
- Represents a distinct step in a larger process

**Examples**:
- System Initialization Stage
- UI Construction Stage
- Data Preparation Stage
- Session Setup Stage

### Operation
A discrete unit of work that accomplishes a specific function.

**Characteristics of an Operation**:
- Atomic or near-atomic execution unit
- Focused on a single responsibility
- Has measurable execution time
- Often corresponds to a function call or block of code
- May be repeated at different times

**Examples**:
- Loading a library
- Creating a UI component
- Reading a file
- Calculating a value

## Temporal Relationships

### Sequential Relationship
- **Phase is a sequence of Stages**: Each phase consists of stages that execute in order
- **Stage is a sequence of Operations**: Each stage consists of operations that execute in order

### Temporal Composition
```
Application Lifecycle
├── Initialization Phase
│   ├── System Initialization Stage
│   │   ├── Load Library Operation
│   │   ├── Configure Environment Operation
│   │   └── Set Global Variables Operation
│   ├── UI Construction Stage
│   │   ├── Create Container Operation
│   │   ├── Create Component A Operation
│   │   └── Create Component B Operation
│   ├── Data Preparation Stage
│   │   ├── Load File Operation
│   │   └── Transform Data Operation
│   └── Session Setup Stage
│       ├── Initialize State Operation
│       └── Register Handlers Operation
├── Runtime Phase
│   ├── Event Handling Stage
│   ├── Data Processing Stage
│   └── Rendering Stage
└── Termination Phase
    ├── State Saving Stage
    └── Resource Cleanup Stage
```

## Measuring Temporal Units

Each temporal unit can be measured and analyzed:

```r
# Measuring a Phase
phase_start_time <- Sys.time()
run_initialization_phase()
phase_duration <- difftime(Sys.time(), phase_start_time, units = "secs")
log_timing("initialization_phase", phase_duration)

# Measuring a Stage
stage_start_time <- Sys.time()
run_ui_construction_stage()
stage_duration <- difftime(Sys.time(), stage_start_time, units = "secs")
log_timing("ui_construction_stage", stage_duration)

# Measuring an Operation
operation_start_time <- Sys.time()
create_component()
operation_duration <- difftime(Sys.time(), operation_start_time, units = "secs")
log_timing("create_component_operation", operation_duration)
```

## Time Unit Allocation Budget

Applications can establish time budgets for each unit:

```r
time_budgets <- list(
  # Phase budgets
  "initialization_phase" = 3.0,  # seconds
  "termination_phase" = 1.0,     # seconds
  
  # Stage budgets
  "system_initialization_stage" = 0.5,  # seconds
  "ui_construction_stage" = 1.0,        # seconds
  "data_preparation_stage" = 1.0,       # seconds
  "session_setup_stage" = 0.5,          # seconds
  
  # Operation budgets
  "load_library_operation" = 0.1,      # seconds
  "create_component_operation" = 0.05  # seconds
)
```

## Examples in Application Code

### Phase Example (Initialization Phase)

```r
# Initialization Phase
initialization_phase <- function() {
  # Measure and log the entire phase
  phase_start <- Sys.time()
  
  # Run each stage in sequence
  system_initialization_stage()
  ui_construction_stage()
  data_preparation_stage()
  session_setup_stage()
  
  # Calculate phase duration
  phase_duration <- difftime(Sys.time(), phase_start, units = "secs")
  log_timing("initialization_phase", phase_duration)
  
  # Check against budget
  if (phase_duration > time_budgets$initialization_phase) {
    warning("Initialization phase exceeded time budget")
  }
}
```

### Stage Example (UI Construction Stage)

```r
# UI Construction Stage
ui_construction_stage <- function() {
  # Measure and log the stage
  stage_start <- Sys.time()
  
  # Run operations in sequence
  create_container_operation()
  create_header_operation()
  create_sidebar_operation()
  create_main_content_operation()
  create_footer_operation()
  
  # Calculate stage duration
  stage_duration <- difftime(Sys.time(), stage_start, units = "secs")
  log_timing("ui_construction_stage", stage_duration)
  
  # Check against budget
  if (stage_duration > time_budgets$ui_construction_stage) {
    warning("UI construction stage exceeded time budget")
  }
}
```

### Operation Example (Create Component Operation)

```r
# Create Component Operation
create_component_operation <- function(component_type, properties) {
  # Measure and log the operation
  operation_start <- Sys.time()
  
  # Perform the operation
  component <- create_dom_element(component_type, properties)
  
  # Calculate operation duration
  operation_duration <- difftime(Sys.time(), operation_start, units = "secs")
  log_timing("create_component_operation", component_type, operation_duration)
  
  # Check against budget
  if (operation_duration > time_budgets$create_component_operation) {
    warning("Create component operation exceeded time budget for type: ", component_type)
  }
  
  return(component)
}
```

## Visualization of Time Unit Usage

Time units provide a natural hierarchy for visualizing time allocation:

```r
# Create a nested treemap of time allocation
plot_time_allocation_treemap <- function(timing_data) {
  # Group by phase, stage, and operation
  grouped_data <- timing_data %>%
    group_by(phase, stage, operation) %>%
    summarize(time = sum(time))
  
  # Plot as treemap
  treemap(grouped_data,
          index = c("phase", "stage", "operation"),
          vSize = "time",
          title = "Application Time Allocation",
          palette = "Set3")
}
```

## Related NSQL Concepts

- MP39: One-Time Operations During Initialization
- MP40: Time Allocation Decomposition
- ⚡¹: One-time execution adjective
- ⚡*: Many-time execution adjective