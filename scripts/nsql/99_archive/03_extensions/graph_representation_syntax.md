# Graph Representation Syntax in NSQL

This document defines the syntax for graph representation directives in NSQL.

## CREATE GRAPH Directive

The CREATE GRAPH directive is used to define and generate graph structures from data.

### Basic Syntax

```
CREATE GRAPH {graph_name} FROM {data_source} TO {output_path}
=== Graph Definition ===
{graph_definition}
```

Where:
- `{graph_name}` is the name of the graph to be created
- `{data_source}` is the data source (table, query, or file)
- `{output_path}` is the path where the graph representation will be saved
- `{graph_definition}` specifies how to construct the graph

### Example

```
CREATE GRAPH customer_relationships FROM app_data.df_transactions TO "graphs/customer"
=== Graph Definition ===
FROM_NODES = customer_id
TO_NODES = referred_by
WEIGHTS = transaction_amount
LABELS = customer_name
DIRECTED = TRUE
```

## ANALYZE GRAPH Directive

The ANALYZE GRAPH directive is used to compute metrics on existing graph structures.

### Syntax

```
ANALYZE GRAPH {graph_name} FROM {graph_path} TO {output_path}
=== Analysis Parameters ===
{analysis_parameters}
```

Where:
- `{graph_name}` is the name of the graph to analyze
- `{graph_path}` is the path to the stored graph object
- `{output_path}` is the path where analysis results will be saved
- `{analysis_parameters}` specifies the metrics to compute

### Example

```
ANALYZE GRAPH customer_relationships FROM "graphs/customer" TO "analysis/graph_metrics"
=== Analysis Parameters ===
METRICS = degree, betweenness, closeness, eigenvector
COMMUNITY_DETECTION = TRUE
NODE_IMPORTANCE = TRUE
```

## VISUALIZE GRAPH Directive

The VISUALIZE GRAPH directive is used to create visual representations of graphs.

### Syntax

```
VISUALIZE GRAPH {graph_name} FROM {graph_path} TO {output_path}
=== Visualization Parameters ===
{visualization_parameters}
```

Where:
- `{graph_name}` is the name of the graph to visualize
- `{graph_path}` is the path to the stored graph object
- `{output_path}` is the path where visualization will be saved
- `{visualization_parameters}` specifies visualization options

### Example

```
VISUALIZE GRAPH customer_relationships FROM "graphs/customer" TO "visualizations/graphs"
=== Visualization Parameters ===
FORMAT = png
LAYOUT = force_directed
NODE_SIZE = degree
NODE_COLOR = community
EDGE_WIDTH = weight
WIDTH = 1200
HEIGHT = 1200
RESOLUTION = 150
```

## R Code Generation

The graph directives translate to R code using the igraph package:

```r
# For CREATE GRAPH
edges_df <- data.frame(from = from_nodes, to = to_nodes, weight = weights)
nodes_df <- data.frame(id = unique(c(from_nodes, to_nodes)), label = labels)
graph <- igraph::graph_from_data_frame(edges_df, directed = directed, vertices = nodes_df)
saveRDS(graph, file.path(output_dir, paste0(graph_name, ".rds")))

# For ANALYZE GRAPH
graph <- readRDS(file.path(input_dir, paste0(graph_name, ".rds")))
metrics <- data.frame(
  node = igraph::V(graph)$name,
  degree = igraph::degree(graph),
  betweenness = igraph::betweenness(graph),
  closeness = igraph::closeness(graph),
  eigenvector = igraph::eigen_centrality(graph)$vector
)
write.csv(metrics, file.path(output_dir, paste0(graph_name, "_metrics.csv")))

# For VISUALIZE GRAPH
graph <- readRDS(file.path(input_dir, paste0(graph_name, ".rds")))
png(file.path(output_dir, paste0(graph_name, ".png")), width, height, res)
igraph::plot.igraph(graph, layout = layout, ...)
dev.off()
```

## Grammar (EBNF)

```ebnf
graph_directive ::= create_graph_directive | analyze_graph_directive | visualize_graph_directive

create_graph_directive ::= 'CREATE' 'GRAPH' graph_name 'FROM' data_source 'TO' output_path delimiter graph_definition

analyze_graph_directive ::= 'ANALYZE' 'GRAPH' graph_name 'FROM' graph_path 'TO' output_path delimiter analysis_parameters

visualize_graph_directive ::= 'VISUALIZE' 'GRAPH' graph_name 'FROM' graph_path 'TO' output_path delimiter visualization_parameters

graph_name ::= identifier

data_source ::= (table_reference | file_path)

table_reference ::= [connection_name '.'] table_name

graph_path ::= string_literal

output_path ::= string_literal

delimiter ::= '===' ('Graph' 'Definition' | 'Analysis' 'Parameters' | 'Visualization' 'Parameters') '==='

graph_definition ::= (graph_property_assignment)+

analysis_parameters ::= (analysis_property_assignment)+

visualization_parameters ::= (visualization_property_assignment)+

graph_property_assignment ::= property_name '=' property_value

property_name ::= 'FROM_NODES' | 'TO_NODES' | 'WEIGHTS' | 'LABELS' | 'DIRECTED' | identifier

property_value ::= identifier | string_literal | boolean | number
```