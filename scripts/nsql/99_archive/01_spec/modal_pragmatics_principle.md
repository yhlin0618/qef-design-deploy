# Pragmatic Principle of Modal Words in NSQL

## Core Principle

In Natural SQL Language (NSQL), modal words carry pragmatic force that determines how statements are interpreted, validated, and executed. The pragmatic meaning of modal words must be consistent across the language and should encode both the strength of the directive and its enforcement behavior.

## Modal Hierarchy

NSQL implements a clear hierarchy of modal words across several dimensions, each with defined pragmatic interpretations:

### 1. Deontic Modals (Obligation/Permission)

| Modal Word | Pragmatic Force | Validation Rule | Implementation Pattern |
|------------|----------------|-----------------|------------------------|
| **MUST** | Absolute requirement | Hard constraint | Operation fails if violated |
| **SHOULD** | Strong recommendation | Soft constraint | Warning generated if violated |
| **MORALLY SHOULD** | Ethical recommendation | Ethical constraint | Warning with ethical dimension |
| **MAY** | Discretionary option | Preference | Logged but not enforced |
| **CAN** | Capability statement | Possibility | Informational only |

### 2. Epistemic Modals (Knowledge/Belief)

| Modal Word | Pragmatic Force | Knowledge Status | Implementation Pattern |
|------------|----------------|------------------|------------------------|
| **KNOW** | Certainty claim | Verified fact | Used for absolute knowledge assertions |
| **BELIEVE** | Confidence claim | Probable fact | Used for likely but not certain assertions |
| **POSSIBLY** | Possibility claim | Potential fact | Used for data exploration and hypotheses |
| **NECESSARILY** | Logical requirement | Logical entailment | Used for conclusions that must follow |

## Modal Word Definitions

### Deontic Modals

#### MUST
- **Pragmatic Definition**: An absolute requirement that MUST be satisfied for the operation to succeed
- **Validation Behavior**: Validates before execution and fails immediately if condition not met
- **Error Handling**: Returns explicit error with requirement details
- **Implementation**: Translates to database constraints, validation rules, or execution blockers

#### SHOULD
- **Pragmatic Definition**: A strong recommendation that SHOULD be followed unless there are justified reasons not to
- **Validation Behavior**: Validates before execution but proceeds with warnings if condition not met
- **Error Handling**: Generates non-blocking warnings with recommendation details
- **Implementation**: Translates to warning-level validations

#### MORALLY SHOULD
- **Pragmatic Definition**: An ethical recommendation based on moral considerations rather than just technical ones
- **Validation Behavior**: Validates with ethical dimension and proceeds with ethical warnings
- **Error Handling**: Generates warnings that explicitly reference ethical considerations
- **Implementation**: Translates to warnings with ethics tags and documentation of ethical implications

#### MAY
- **Pragmatic Definition**: A discretionary option where the action MAY be taken if deemed appropriate
- **Validation Behavior**: Checks condition but does not affect execution flow
- **Error Handling**: Logs preferences but doesn't generate user-facing messages
- **Implementation**: Translates to logging or audit implementations

#### CAN
- **Pragmatic Definition**: A statement of capability expressing that an action CAN be performed
- **Validation Behavior**: No validation performed
- **Error Handling**: No errors or warnings generated
- **Implementation**: Primarily used in documentation and possibility statements

### Epistemic Modals

#### KNOW
- **Pragmatic Definition**: Assertion of certainty about a fact or condition that is known to be true
- **Validation Behavior**: Treats condition as axiomatically true; will throw error if contradictory data found
- **Error Handling**: Generates errors when knowledge assertions are contradicted
- **Implementation**: Translates to axioms and baseline assertions in knowledge systems

#### BELIEVE
- **Pragmatic Definition**: Expression of confidence but not certainty about a fact or condition
- **Validation Behavior**: Treats condition as probable but verifiable; logs contradictions
- **Error Handling**: Logs contradictions to belief statements without failing
- **Implementation**: Translates to probabilistic assertions and confidence-weighted calculations

#### POSSIBLY
- **Pragmatic Definition**: Suggestion that a condition might be true without committing to its truth
- **Validation Behavior**: Considers condition as one of multiple alternatives
- **Error Handling**: No errors on contradictions, just probability adjustments
- **Implementation**: Translates to hypothesis generation and exploratory analysis paths

#### NECESSARILY
- **Pragmatic Definition**: Assertion that a conclusion logically follows from established premises
- **Validation Behavior**: Verifies logical entailment from premises
- **Error Handling**: Generates errors if logical inconsistencies are detected
- **Implementation**: Translates to inference rules and logical constraints

## Implementation Patterns

### 1. Constraint Definition

```
# MUST - Hard constraint with blocking
TABLE customer_data MUST HAVE COLUMN email 
  WHERE format MATCHES "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"

# SHOULD - Soft constraint with warning
REPORT financial_summary SHOULD INCLUDE timestamp 
  WHERE format = "YYYY-MM-DD HH:MM:SS"

# MAY - Preference with logging
QUERY customer_search MAY USE INDEX customer_email_idx
  WHERE search_field = "email"
```

### 2. Operation Rules

```
# MUST - Required operation behavior
BEFORE DELETE FROM customer_data 
  WHERE contains_pii = true
  CHANGES MUST BE LOGGED

# SHOULD - Recommended operation behavior
WHEN UPDATING sales_data
  TRANSACTION SHOULD USE ISOLATION LEVEL serializable

# MAY - Optional optimization
WHEN RETRIEVING large_results
  QUERY MAY USE PAGINATION
    WHERE page_size = 100
```

### 3. System Behaviors

```
# MUST - System requirement
SYSTEM BACKUP MUST RUN daily
  WHERE retention_period >= 30

# SHOULD - System recommendation
CACHE INVALIDATION SHOULD OCCUR
  WHERE data_change = true

# MAY - System option
ERROR REPORTS MAY INCLUDE debug_info
  WHERE environment = "development"
```

## Modal Composition Rules

When multiple modal statements apply to the same operation, the following composition rules apply:

1. **Highest Modal Wins**: When conflicting modals apply, the stronger modal takes precedence
   ```
   # MUST overrides SHOULD
   TABLE transactions MUST HAVE COLUMN amount WHERE amount > 0
   TABLE transactions SHOULD HAVE COLUMN amount WHERE amount > 10
   # The MUST constraint controls
   ```

2. **Conjunction for Same Modal**: Multiple constraints with the same modal combine as AND conditions
   ```
   TABLE customer MUST HAVE COLUMN id
   TABLE customer MUST HAVE COLUMN created_at
   # Both constraints must be satisfied
   ```

3. **Required Over Optional**: MUST and SHOULD constraints are evaluated before MAY and CAN statements
   ```
   # Order of evaluation:
   TABLE orders MUST HAVE COLUMN status
   TABLE orders SHOULD HAVE COLUMN updated_at
   TABLE orders MAY HAVE COLUMN notes
   ```

## Translation to Implementation Languages

The modal pragmatics translate to implementation languages as follows:

### SQL Translation

| Modal | SQL Translation |
|-------|----------------|
| MUST | CHECK constraints, NOT NULL, FOREIGN KEY constraints |
| SHOULD | Triggers that log warnings, comments, documentation |
| MAY | Comments, hints, optional configurations |
| CAN | Documentation only |

### R Translation

| Modal | R Translation |
|-------|---------------|
| MUST | `stopifnot()`, validation with `stop()` |
| SHOULD | `warning()` calls, warning-level validations |
| MAY | `message()` calls, logging statements |
| CAN | Comments, documentation |

## Benefits of Modal Pragmatics

1. **Clarity of Intent**: Clear distinction between requirements and recommendations
2. **Flexible Enforcement**: Different enforcement levels for different contexts
3. **Documentation Integration**: Self-documenting code with clear requirements
4. **Validation Framework**: Foundation for comprehensive data validation
5. **Business Rule Expression**: Natural language expression of business rules

## Examples in Context

### Example 1: Customer Data Requirements (Deontic Modals)

```
# Requirements for customer data handling
WHEN STORING customer_data:
  - PII_FIELDS MUST BE encrypted
  - RETENTION_PERIOD MUST BE <= 7 years
  - ACCESS MUST BE logged
  
  - DATA SHOULD BE normalized
  - UPDATES SHOULD BE versioned
  
  - RECORDS MAY INCLUDE marketing_preferences
  - EXPORT MAY USE anonymization
  
  - DATA MORALLY SHOULD NOT include sensitive_demographics WITHOUT explicit_consent
```

### Example 2: Financial Report Generation (Deontic Modals)

```
# Requirements for financial report generation
WHEN GENERATING financial_reports:
  - VALUES MUST BALANCE across ledgers
  - CALCULATIONS MUST USE standardized_formulas
  
  - REPORTS SHOULD INCLUDE timestamp
  - FORMATTING SHOULD FOLLOW corporate_style
  
  - FOOTNOTES MAY INCLUDE explanatory_text
  - VISUALIZATION MAY USE charts
```

### Example 3: Data Analysis Claims (Epistemic Modals)

```
# Knowledge assertions about customer behavior
REGARDING customer_segments:
  - WE KNOW high_value_customers HAVE lifetime_value > 1000
  - WE BELIEVE seasonal_buyers TEND TO purchase_in Q4
  - WE POSSIBLY SEE correlation BETWEEN weather AND purchase_rate
  - customer_churn NECESSARILY IMPACTS revenue_projections
```

### Example 4: Mixed Modal Types

```
# Comprehensive policy with multiple modal types
REGARDING pii_data_handling:
  # Deontic (obligation)
  - SYSTEM MUST ENCRYPT all_pii_fields
  - ADMINISTRATORS SHOULD REVIEW access_logs weekly
  
  # Epistemic (knowledge)
  - WE KNOW customer_email IS pii_data
  - WE BELIEVE anonymization PRESERVES analysis_value
  - data_breaches POSSIBLY OCCUR despite_safeguards
  - compliance_violations NECESSARILY TRIGGER audit_procedures
```

## Integration with NSQL Grammar

The modal pragmatics principle is integrated into the NSQL grammar through modal verb phrases:

```
<modal_statement> ::= <subject> <modal_verb> <predicate>

<modal_verb> ::= <deontic_modal> | <epistemic_modal>

<deontic_modal> ::= "MUST" | "SHOULD" | "MORALLY SHOULD" | "MAY" | "CAN"

<epistemic_modal> ::= 
    "KNOW" | 
    "BELIEVE" | 
    "POSSIBLY" | 
    "NECESSARILY"

<epistemic_subject> ::= "WE" <epistemic_modal> | <term> <epistemic_modal>
```

This grammar extension allows both deontic and epistemic modal statements to be formally parsed, validated, and executed within the NSQL processing pipeline. Epistemic modals typically take "WE" as a subject (for assertions about the system's knowledge state) but can also apply directly to terms for logical necessity.

## Conclusion

The Pragmatic Principle of Modal Words establishes a clear framework for expressing requirements, recommendations, and options in NSQL. By formalizing the pragmatic interpretation of modal words, NSQL gains significant expressive power for constraint definition, validation, and business rule expression while maintaining its natural language readability.

This principle bridges the gap between natural language principles and executable code, allowing organizational policies, business rules, and system requirements to be expressed directly in NSQL with appropriate enforcement mechanisms.