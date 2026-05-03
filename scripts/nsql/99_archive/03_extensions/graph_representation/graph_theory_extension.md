# NSQL Graph Theory Extension

## Overview

This extension adds formal graph theory notation to NSQL, enabling precise documentation of component relationships, system structure, and data flows. It provides a mathematical foundation for representing and analyzing the structure of applications.

## Graph Type Definitions

```
TYPE Vertex = Element ID and properties
TYPE Edge = Connection between vertices with optional direction and weight
TYPE Graph = Collection of vertices and edges
TYPE Path = Ordered sequence of vertices connected by edges
TYPE Component = Subgraph where all vertices are connected by some path
```

## Graph Definition Syntax

```
GRAPH(name) {
  VERTICES {
    name: properties,
    ...
  }
  EDGES {
    source -> destination [: properties],
    ...
  }
}
```

## Component Identification

```
COMPONENT(component_name) FROM graph_name {
  ENTRY_POINTS: [vertex_ids]
  EXIT_POINTS: [vertex_ids]
  INTERNAL_VERTICES: [vertex_ids]
}
```

## State Machine Documentation

```
STATE_MACHINE(name) {
  STATES {
    "idle": { type: "initial" },
    "processing": { type: "transient" },
    "success": { type: "terminal" },
    "error": { type: "terminal" }
  }
  
  TRANSITIONS {
    "idle" -> "processing": { event: "start_process", guard: "input_valid" },
    "processing" -> "success": { event: "process_complete", guard: "no_errors" },
    "processing" -> "error": { event: "process_error" },
    "error" -> "idle": { event: "reset" },
    "success" -> "idle": { event: "reset" }
  }
}
```

## Component Interconnection

```
SYSTEM(system_name) {
  COMPONENTS {
    "component_a": { type: "input_processor" },
    "component_b": { type: "data_transformer" },
    "component_c": { type: "output_generator" }
  }
  
  CONNECTIONS {
    "component_a.output" -> "component_b.input": { type: "data_flow" },
    "component_b.result" -> "component_c.data_source": { type: "data_flow" },
    "component_c.status" -> "component_a.feedback": { type: "event" }
  }
  
  // Cut sets between components
  CUT_SETS {
    "a_to_b": ["component_a.output", "component_b.input"],
    "b_to_c": ["component_b.result", "component_c.data_source"],
    "feedback_loop": ["component_c.status", "component_a.feedback"]
  }
}
```

## Graph Analysis Operations

```
// Find all paths between two vertices
PATHS = FIND_PATHS(graph, source, destination)

// Calculate graph metrics
METRICS = ANALYZE_GRAPH(graph) {
  "connectivity": CONNECTIVITY_SCORE,
  "cyclical": HAS_CYCLES,
  "diameter": MAX_PATH_LENGTH,
  "centrality": {
    vertex_id: centrality_score,
    ...
  }
}

// Identify connected components
COMPONENTS = FIND_COMPONENTS(graph)

// Find minimum cut set
CUT_SET = FIND_MIN_CUT(graph, source_set, destination_set)
```

## Integration with Core NSQL

```
// Data flow can reference graph components
DATA_FLOW(component: customer_filter) {
  GRAPH {
    VERTICES {
      "app_data_connection": { type: "data_source" },
      "dna_data": { type: "transformed_data" },
      "profiles": { type: "transformed_data" },
      "valid_ids": { type: "transformed_data" },
      "dropdown_options": { type: "ui_data" }
    }
    
    EDGES {
      "app_data_connection" -> "dna_data": { operation: "EXTRACT" },
      "app_data_connection" -> "profiles": { operation: "EXTRACT" },
      "dna_data" -> "valid_ids": { operation: "DISTINCT" },
      "profiles" -> "dropdown_options": { operation: "FILTER", condition: "customer_id IN valid_ids" }
    }
  }
  
  SOURCE: app_data_connection
  INITIALIZE: {
    EXTRACT(app_data_connection → GET dna_data → dna_data)
    EXTRACT(app_data_connection → GET customer_profiles → profiles)
    EXTRACT(dna_data → DISTINCT customer_id → valid_ids)
    FILTER(profiles → WHERE customer_id IN valid_ids → dropdown_options)
  }
  // ... rest of the data flow
}
```

## Benefits of Graph Theory Extensions

1. **Formal Component Definition**: Enables precise definition of component boundaries and interactions
2. **Flow Visualization**: Supports clear documentation of data and event flows
3. **Property Verification**: Allows verification of component properties like connectivity
4. **System Analysis**: Provides tools for reasoning about system-wide behaviors
5. **Cut-Set Identification**: Helps identify critical connections between system components
6. **State Modeling**: Provides notation for documenting state machines and transitions

## Implementation Examples

### App Structure Visualization

```
// Example: Visualization of a Shiny app structure
GRAPH("customer_dashboard_app") {
  VERTICES {
    "ui_inputs": { type: "input_group", components: ["date_filter", "segment_filter"] },
    "data_sources": { type: "data_group", sources: ["customers", "transactions"] },
    "reactive_elements": { type: "reactive_group", elements: ["filtered_data", "metrics"] },
    "outputs": { type: "output_group", components: ["summary_table", "charts"] }
  }
  
  EDGES {
    "ui_inputs" -> "reactive_elements": { type: "trigger" },
    "data_sources" -> "reactive_elements": { type: "data_source" },
    "reactive_elements" -> "outputs": { type: "data_flow" }
  }
}

// Analysis of component connectivity
METRICS = ANALYZE_GRAPH("customer_dashboard_app")
```

### Data Transformation Flow

```
// Example: Data transformation pipeline
GRAPH("etl_pipeline") {
  VERTICES {
    "raw_data": { type: "source" },
    "validation": { type: "transform" },
    "cleaning": { type: "transform" },
    "aggregation": { type: "transform" },
    "storage": { type: "destination" }
  }
  
  EDGES {
    "raw_data" -> "validation": { operation: "VALIDATE" },
    "validation" -> "cleaning": { operation: "CLEAN", condition: "is_valid = TRUE" },
    "validation" -> "error_log": { operation: "LOG", condition: "is_valid = FALSE" },
    "cleaning" -> "aggregation": { operation: "AGGREGATE" },
    "aggregation" -> "storage": { operation: "STORE" }
  }
}

// Find all successful paths through the pipeline
SUCCESS_PATHS = FIND_PATHS("etl_pipeline", "raw_data", "storage")
```

## Related Principles

- MP024: Natural SQL Language
- MP027: Integrated Natural SQL Language (NSQL)
- MP052: Unidirectional Data Flow
- MP056: Connected Component Principle
- MP059: App Dynamics