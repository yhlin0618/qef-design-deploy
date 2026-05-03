---
id: "RSQL_P01"
title: "Explicit Evaluation Preference"
type: "principle"
date_created: "2025-04-03"
date_modified: "2025-12-24"
author: "Claude"
---

# RSQL_P01: Explicit Evaluation Preference

## Pragmatic Rule
In R-specific SQL (RSQL), explicit evaluation mechanisms like `do.call()` must be preferred over meta-programming operators like `!!!` when constructing function calls with dynamic arguments.

## Formal Definition
Let Δ be the domain of function call constructions, and let δ ∈ Δ be a specific function call.

Let E_explicit(δ) denote the explicit evaluation of δ using do.call(),
and let E_implicit(δ) denote the implicit evaluation of δ using !!! or similar operators.

For all δ ∈ Δ, the evaluation preferences are defined as:

E_explicit(δ) ≻ E_implicit(δ)

Where ≻ denotes strict preference in terms of robustness, type safety, and maintainability.

## Operational Semantics
For a function f: X → Y and a set of arguments A:

1. E_explicit(f, A) = do.call(f, A)
2. E_implicit(f, A) = f(!!!A)

The evaluation contexts differ:
- In E_explicit, arguments in A are evaluated within the context of do.call()
- In E_implicit, arguments in A may be evaluated before being passed to f

## Query Examples

### Example 1: UI Component Construction
```r
# Non-compliant RSQL:
SELECT * FROM ui_components
WHERE oneTimeUnionUI2(!!!filters, id=?id, initial_visibility=?visibility)

# Compliant RSQL:
SELECT * FROM ui_components
WHERE {
  args <- list(id=?id, initial_visibility=?visibility);
  FOREACH name IN KEYS(filters) {
    IF !IS_NULL(filters[name]) THEN
      args[name] <- filters[name];
    END IF;
  }
  do.call(oneTimeUnionUI2, args);
}
```

### Example 2: Data Transformation
```r
# Non-compliant RSQL:
UPDATE data_frame
SET transformed = transform_func(!!!params)

# Compliant RSQL:
UPDATE data_frame
SET transformed = {
  validated_params <- FILTER params WHERE !IS_NULL(?value);
  do.call(transform_func, validated_params);
}
```

## Verification Rules
A function call construction δ conforms to RSQL_P01 if and only if:

1. Dynamic argument lists are passed using do.call() or equivalent explicit evaluation
2. The !!! operator is not used for argument splicing in function calls
3. Arguments are validated before being passed to functions
4. Type checking is performed on argument lists before evaluation

## Implementation Notes
- When implementing this rule in RSQL processors:
  - Static analysis should flag uses of !!! in function calls
  - Runtime checks should verify argument types before function application
  - Optimization passes should convert implicit evaluations to explicit ones when possible

## Related Rules
- R64: Explicit Over Implicit Evaluation
- MP41: Type-Dependent Operations
- SLN01: Handling Non-Logical Data Types