# Reference Resolution Rule for NSQL

## Core Rule

**All references in NSQL must be unambiguous.** If any ambiguity exists in a reference (table, column, schema, etc.), the system MUST query the user for clarification until all ambiguity is resolved. No automatic resolution or assumptions about reference intent are permitted.

## Implementation Requirements

### 1. Ambiguity Detection

The system must detect the following types of ambiguities:

- **Entity Type Ambiguity**: When it's unclear if a reference refers to a table, view, schema, etc.
- **Path Ambiguity**: When an unqualified name could exist in multiple schemas
- **Column Ambiguity**: When a column name exists in multiple tables in the query context
- **Alias Ambiguity**: When aliases conflict with existing names or other aliases
- **Function Ambiguity**: When a function name could refer to multiple functions with different signatures

### 2. Resolution Process

When ambiguity is detected:

1. **Immediate Clarification**: Processing stops immediately upon detecting ambiguity
2. **Specific Questions**: The system asks specific, targeted questions about the ambiguous reference
3. **Clear Options**: All possible resolutions are presented to the user
4. **Context Information**: Sufficient context is provided to make an informed decision
5. **Persistent Resolution**: Once resolved, the resolution is remembered for the current session

### 3. User Interaction

The clarification interaction must:

1. **Be Explicit**: Clearly state what is ambiguous and why
2. **Provide Choices**: List all valid possible interpretations
3. **Accept Precise Input**: Allow the user to precisely specify their intent
4. **Allow Qualification**: Enable the user to qualify references (e.g., with schema prefixes)
5. **Support Learning**: Optionally remember resolutions for similar future ambiguities

### 4. Examples of Ambiguities and Resolutions

#### Entity Type Ambiguity

**Ambiguous Statement**:
```
import Sales to Analytics
```

**Resolution Query**:
```
'Sales' could refer to multiple entities:
1. Table 'Sales' in schema 'Public'
2. Schema 'Sales' containing multiple tables
3. Database 'Sales'

Please clarify which you mean:
- "Table Sales in Public schema"
- "Schema Sales"
- "Database Sales"
- Or provide a fully qualified reference
```

#### Path Ambiguity

**Ambiguous Statement**:
```
transform Customers to CustomerSummary as
  count(*) as customer_count
  grouped by region
```

**Resolution Query**:
```
'Customers' could refer to tables in multiple schemas:
1. Public.Customers
2. CRM.Customers
3. Analytics.Customers

Please clarify which you mean:
- "Public.Customers"
- "CRM.Customers"
- "Analytics.Customers"
- Or specify another location
```

#### Column Ambiguity

**Ambiguous Statement**:
```
transform Orders joined with Customers on id = customer_id to OrderSummary as
  sum(total) as total_sales
  grouped by region
```

**Resolution Query**:
```
The column 'region' is ambiguous:
1. Orders.region
2. Customers.region

Please clarify which you mean:
- "Orders.region"
- "Customers.region"
```

### 5. Caching and Context

While ambiguities must be resolved explicitly, the system may:

1. **Cache Resolutions**: Remember how specific ambiguities were resolved during a session
2. **Use Current Context**: Apply the current working context (e.g., current schema) for suggested resolutions
3. **Learn Preferences**: Optionally learn user resolution patterns to improve suggestions

## Benefits

This strict reference resolution approach ensures:

1. **Clarity**: All operations have clear, explicit meaning
2. **Correctness**: Prevents unintended operations on incorrect data
3. **User Intent**: Preserves the user's true intent without assumption
4. **Learning Opportunity**: Educates users about the data environment
5. **Error Prevention**: Catches potential errors before execution

## Conclusion

The Reference Resolution Rule ensures that all NSQL operations are performed with complete clarity of intent. By requiring explicit resolution of all ambiguities through user interaction, the system guarantees that operations match user expectations and prevents errors from incorrect assumptions about references.