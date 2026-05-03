---
id: "NSQL_R07"
title: "NSQL Similarity Principle"
type: "rule"
date_created: "2025-04-03"
date_modified: "2025-12-24"
author: "Claude"
previous_id: "R62"
---

# NSQL_R07: NSQL Similarity Principle

> **Note**: This rule was previously R62 in the MAMBA principles system.

## Definition
The similarity between two processes or components is proportional to the similarity of their outputs when provided with identical inputs. Processes that consistently produce similar outputs for the same inputs are themselves similar, with the degree of similarity being measurable and quantifiable.

## Formal Expression
For processes P and Q, and similarity function sim():

sim(P, Q) ∝ Eₓ[sim(P(x), Q(x))]

Where:
- sim(P, Q) represents the similarity between processes P and Q
- P(x) and Q(x) represent the outputs of processes P and Q for input x
- Eₓ represents the expected value over all possible inputs x
- ∝ represents "proportional to"

## Measurement Scale
Similarity is expressed on a continuous scale from 0 to 1:
- 0 represents complete dissimilarity
- 1 represents perfect similarity (equivalent to extensional equality)
- Values between 0 and 1 represent degrees of similarity

## Explanation
While extensionality (R61) provides a binary notion of equality based on identical outputs, the similarity principle introduces a graduated measure of resemblance. This allows for nuanced comparison of processes, components, and datasets that may not be strictly equal but share important characteristics.

## Applications in NSQL

### Process Similarity
```nsql
# Define two data transformation processes
Process1 = TRANSFORM Sales USING moving_average(window=7)
Process2 = TRANSFORM Sales USING exponential_smoothing(alpha=0.3)

# Calculate similarity based on their outputs for the same inputs
ProcessSimilarity = AVERAGE(
  SIMILARITY(Process1(Sales), Process2(Sales))
  FOR ALL Sales IN historical_data
)
```

### Component Similarity
```nsql
# Compare similarity of two visualization components
Component1 = LineChart(data, {smooth: true, markers: false})
Component2 = AreaChart(data, {smooth: true, stacked: false})

# Visual similarity measured on test datasets
ComponentSimilarity = AVERAGE(
  VISUAL_SIMILARITY(Component1(dataset), Component2(dataset))
  FOR ALL dataset IN test_datasets
)
```

### Result Set Similarity
```nsql
# Compare similarity of query results
Query1 = SELECT * FROM Customers WHERE purchase_count > 10
Query2 = SELECT * FROM Customers WHERE lifetime_value > 1000

# Jaccard similarity of result sets
ResultSimilarity = COUNT(INTERSECTION(Query1, Query2)) / COUNT(UNION(Query1, Query2))
```

## Practical Implementation

### Similarity Metrics

Multiple similarity measures can be applied depending on the type of data:

#### Vector Similarity
```r
# Cosine similarity for numeric vectors
cosine_similarity <- function(v1, v2) {
  sum(v1 * v2) / (sqrt(sum(v1^2)) * sqrt(sum(v2^2)))
}

# Euclidean similarity for numeric vectors
euclidean_similarity <- function(v1, v2) {
  1 / (1 + sqrt(sum((v1 - v2)^2)))
}
```

#### String Similarity
```r
# Normalized Levenshtein similarity for strings
levenshtein_similarity <- function(s1, s2) {
  1 - stringdist::stringdist(s1, s2, method = "lv") / max(nchar(s1), nchar(s2))
}
```

#### Set Similarity
```r
# Jaccard similarity for sets
jaccard_similarity <- function(set1, set2) {
  length(intersect(set1, set2)) / length(union(set1, set2))
}
```

#### Component Output Similarity
```r
# Compare outputs of two components across test cases
component_similarity <- function(comp1, comp2, test_cases) {
  similarities <- sapply(test_cases, function(input) {
    output1 <- render_output(comp1, input)
    output2 <- render_output(comp2, input)
    output_similarity(output1, output2)
  })
  mean(similarities)
}
```

## Applications and Benefits

### 1. Recommendation Systems
```r
# Recommend similar components based on behavior
recommend_similar_components <- function(target_component, component_library) {
  similarities <- sapply(component_library, function(component) {
    component_similarity(target_component, component, standard_test_cases)
  })
  
  # Return components sorted by similarity
  component_library[order(similarities, decreasing = TRUE)]
}
```

### 2. Fuzzy Matching
```r
# Find processes with similar behavior to a target process
find_similar_processes <- function(target_process, process_library, threshold = 0.8) {
  process_library[sapply(process_library, function(process) {
    process_similarity(target_process, process) > threshold
  })]
}
```

### 3. Clustering and Classification
```r
# Group components by similarity
cluster_components <- function(components, similarity_threshold = 0.7) {
  # Calculate similarity matrix
  sim_matrix <- matrix(0, nrow=length(components), ncol=length(components))
  for (i in 1:length(components)) {
    for (j in 1:length(components)) {
      sim_matrix[i,j] <- component_similarity(components[i], components[j])
    }
  }
  
  # Perform hierarchical clustering
  clusters <- cutree(hclust(as.dist(1 - sim_matrix)), h = 1 - similarity_threshold)
  split(components, clusters)
}
```

### 4. Process Substitution with Quality Guarantee
```r
# Substitute a process with a similar but more efficient one
substitute_process <- function(original_process, candidate_processes, min_similarity = 0.95) {
  # Find most efficient process that maintains minimum similarity
  valid_substitutes <- Filter(function(p) {
    process_similarity(original_process, p) >= min_similarity
  }, candidate_processes)
  
  if (length(valid_substitutes) > 0) {
    # Return most efficient substitute
    valid_substitutes[[which.min(sapply(valid_substitutes, execution_cost))]]
  } else {
    # Fall back to original if no substitute meets similarity threshold
    original_process
  }
}
```

## Relationship to Extensionality

The similarity principle extends the binary notion of extensional equality to a continuous measure:

1. **Extensional Equality as Perfect Similarity**: When sim(P, Q) = 1, P and Q are extensionally equal
2. **Partial Similarity**: When 0 < sim(P, Q) < 1, P and Q share some but not all behavior
3. **Complete Dissimilarity**: When sim(P, Q) = 0, P and Q have no behavioral resemblance

## Related Concepts

- **Distance Metrics**: The inverse of similarity measures
- **Embedding Spaces**: Vector representations where distance corresponds to semantic similarity
- **Fuzzy Logic**: Many-valued logic where truth values range between 0 and 1
- **Cluster Analysis**: Grouping similar objects based on similarity measures

## Related Principles and Rules

- R61: NSQL Extensionality Principle
- MP28: NSQL Set Theory Foundations
- R59: Component Effect Propagation Rule
- R23: Mathematical Precision