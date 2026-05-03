# Claude Configuration and Guidelines

## 🚨 PRINCIPLES-FIRST POLICY

**CRITICAL**: Before ANY code change, consult the MAMBA Principles System.

---

## 🗣️ NSQL Confirmation Protocol (v3.0)

**CRITICAL**: 當用戶請求涉及資料查詢或操作時，必須使用 NSQL 確認協議。

### 核心原則

1. **AI 寫，人讀**: 用戶只需閱讀和確認，不需學習任何語法
2. **操作順序明確**: 每個操作及其順序必須可見，防止隱含假設
3. **動態確認迴圈**: 透過對話達成共識，而非完美的一次性解析

### 確認格式

#### Pipeline 格式（預設）

最精確、操作順序明確、最少歧義：

```
我理解您要的是：

Orders
  -> filter(status = 'completed' AND date in [2024-11-01, 2024-11-30])
  -> group(customer_id)
  -> aggregate(total_spent = sum(amount))
  -> sort(total_spent desc)
  -> limit(10)

這樣對嗎？
```

#### SQL-like 格式（備用）

適合偏好 SQL 風格的用戶：

```
我理解您要的是：

transform Orders to CustomerRanking
as sum(amount) as total_spent
grouped by customer_id
where status = 'completed' AND date >= '2024-11-01'
ordered by total_spent desc
limit 10

這樣對嗎？
```

#### Operation 格式（寫入操作）

用於 CREATE, UPDATE, DELETE 等修改操作：

```
我將執行以下操作：

UPDATE on Customers
with status = 'inactive'
affecting customers where last_purchase < '2024-06-24'

⚠️ 此操作將影響約 1,247 筆資料
⚠️ 此操作無法復原

確認執行嗎？
```

### 格式選擇指南

| 場景 | 推薦格式 | 理由 |
|------|----------|------|
| 一般資料查詢 | **Pipeline** | 操作順序明確 |
| 多步驟轉換 | **Pipeline** | 鏈式操作清晰 |
| 用戶偏好 SQL | SQL-like | 更熟悉的格式 |
| 寫入/刪除操作 | **Operation** | 強調影響範圍和警告 |

### 歧義消解觸發點

遇到以下情況時，必須先釐清再確認：

1. **模糊時間**: "上個月"、"最近"、"去年" → 詢問具體範圍
2. **未定義業務術語**: "高價值客戶"、"活躍用戶" → 提供定義選項
3. **多義詞**: "銷售"（金額/數量）、"訂單"（筆數/金額）→ 確認語意
4. **隱含聚合**: "各區的銷售" → 確認是總和/平均/計數
5. **操作順序**: "篩選後加總" vs "加總後篩選" → 確認順序

### 回應處理

| 用戶回應 | AI 行動 |
|---------|---------|
| "對/是/確認" | 執行操作 |
| "不對，應該是..." | 更新確認並重新呈現 |
| "不是/錯了" | 請求釐清並重新開始 |
| "還要加上..." | 整合新資訊並更新 |

### Schema 意識確認

本專案的 schema 定義位於：
- **摘要版**: `00_principles/llm/nsql_schema_summary.yaml`（供 NSQL 確認協議使用）
- **完整版（authoring source-of-truth, 2026-04-27 起）**: `01_db/raw_schema/_authoring/core_schemas.yaml`
  - 搬遷自 `00_principles/docs/en/part2_implementations/CH10_database_specifications/etl_schemas/`，
    依 amended MP102 + 新 MP156 二層 normativity binding（spectra change `glue-layer-prerawdata-bridge`, #489）

**Schema-Aware 確認流程**：

1. **讀取 schema**：確認前先讀取 `nsql_schema_summary.yaml`
2. **對應表格**：將用戶的自然語言對應到實際表格名稱
3. **驗證欄位**：確保確認中的欄位存在於 schema
4. **顯示可用欄位**：在確認中列出相關表格的可用欄位

**Schema-Aware 確認範例**：

```
用戶：找評分最高的產品

AI 回應：根據 reviews 表，我理解您要的是：

reviews
  -> group(product_id)
  -> aggregate(avg_rating = mean(rating))
  -> sort(avg_rating desc)
  -> limit({N})

可用欄位：review_id, product_id, rating, review_date, review_text
（product_id 對應您說的「產品」）

N = ?（請指定數量）
```

### 參考資源

- **協議規範**: `scripts/nsql/protocol.yaml`
- **術語定義**: `scripts/nsql/dictionary.yaml`
- **Schema 摘要**: `00_principles/nsql_schema_summary.yaml`
- **詳細指南**: `scripts/nsql/docs/guide.md`

---

### Primary Principle Resources (READ THESE FIRST)

1. **For AI Agents** - LLM-optimized YAML format
   - Location: `00_principles/llm/index.yaml`
   - Purpose: Structured principles with scenarios
   - **START HERE** - load chapters on demand

2. **QUICK_REFERENCE.md** - Human-friendly lookup
   - Location: `00_principles/QUICK_REFERENCE.md`
   - Purpose: Fast lookup by number, topic, or scenario

3. **INDEX.md** - Overview and recent changes
   - Location: `00_principles/INDEX.md`
   - Purpose: Recent updates and high-level structure

4. **Principle Files (.qmd)** - Detailed specifications
   - Location: `00_principles/docs/en/part1_principles/`
   - Purpose: Full principle specifications
   - Read: When YAML summary insufficient

## 📋 PLAN MODE GUIDELINES

**When in Plan Mode, act as principle-explorer:**

1. **BEFORE planning**, read `00_principles/llm/index.yaml`
2. **Find relevant scenario** in the `scenarios:` section
3. **Identify applicable principles** (cite by ID: MP064, DM_R023, etc.)
4. **Include in plan file**:
   ```markdown
   ## Principles Compliance
   - Relevant principles: [list IDs]
   - Potential violations to avoid: [list]
   ```
5. **Design solutions** that align with principles
6. **Anticipate challenges** principle-coder might face

### How to Use the Principles System

**Standard Workflow:**
```
1. User gives task
   ↓
2. Open QUICK_REFERENCE.md
   ↓
3. Search by topic OR scenario OR number
   ↓
4. Identify 3-5 relevant principles
   ↓
5. Read those .qmd files for details
   ↓
6. Implement following principles
   ↓
7. Document principle compliance in code & CHANGELOG
```

**Quick Search Examples:**
- Building ETL? → QUICK_REFERENCE.md → "Scenario 1: Building ETL"
- Creating UI? → QUICK_REFERENCE.md → "Scenario 3: Shiny UI Component"
- Need principle MP029? → QUICK_REFERENCE.md → Ctrl+F "MP029"
- Topic "Security"? → QUICK_REFERENCE.md → "Security & Credentials"

### README-First Policy

When working with this codebase, always check the relevant README files before making structural changes:

- Main README: `/update_scripts/global_scripts/README.md`
- Principles README: `/update_scripts/global_scripts/00_principles/README.md`

README files serve as the authoritative documentation for the project structure, principles organization, and coding standards.

## Code Revision Guidelines

When revising code in this project:

1. **Principle-Based Approach**: Every modification MUST follow at least one principle (MP/P/R)
   - Consult QUICK_REFERENCE.md BEFORE coding
   - Document which principles in code comments
   - Cite principles in CHANGELOG entries

2. **Documentation**: Explicitly state which principles each change follows

3. **Functional Programming**: Prefer functional approaches over imperative coding (MP044)

4. **Vectorization**: Use vectorized operations when working with data (MP035, DEV_R001, DEV_R014)

5. **File Organization**: Follow the one-function-one-file rule (SO_R007) with proper naming (SO_R026)

## Project Structure

The README files document:
- Directory organization
- File naming conventions
- Principles categorization (MP, SO_R, DM_R, DEV_R, UI_R, TD_R, SEC_R, IC_R)
- Coding standards
- Module organization

## Working with Principles

### When Applying Existing Principles

**Every time you write code:**
1. Open QUICK_REFERENCE.md
2. Search for relevant topic/scenario
3. Identify applicable principles (3-5 typically)
4. Read those principle .qmd files
5. Apply principles to implementation
6. Document compliance in comments and CHANGELOG

**Example:**
```r
# This derivation follows:
# - DM_R044: Derivation Implementation Standard (template structure)
# - MP029: No Fake Data (uses real df_customer via tbl2)
# - DM_R002: Universal Data Access (tbl2 pattern)
# - R119: Universal df_ Prefix (output: df_customer_rfm)
```

### When Adding New Principles

1. **Check if principle already exists:**
   - Search QUICK_REFERENCE.md for similar concepts
   - Check principle relationships section

2. **Draft new principle:**
   - Use template from `docs/en/part1_principles/`
   - Assign next available number in category
   - Write clear title, subtitle, rationale, implementation

3. **Update index files:**
   - Add to QUICK_REFERENCE.md (all relevant sections)
   - Add to INDEX.md (Recent Changes)
   - Update count at top of QUICK_REFERENCE.md

4. **Create CHANGELOG entry:**
   - Location: `00_principles/changelog/YYYY-MM-DD_principle_addition.md`
   - Explain why new principle needed
   - Show examples of application

## Recent Important Principles

1. **MP49**: Docker-Based Deployment
2. **MP50**: Debug Code Tracing
3. **MP51**: Test Data Design
4. **R75**: Test Script Initialization
5. **R76**: Module Data Connection
6. **R77**: Supplemental Description Notation
7. **SLN04**: Shiny Namespace Collision Resolution

## Functor-Module Correspondence (MP44)

Follow the Functor-Module Correspondence Principle (MP44) when implementing new functionality:

1. Identify the command functor (verb-object pair)
2. Create a module in the format: `M{number}_{verb}ing_{object}`
3. Structure the module with:
   - `{verb}_{object}.R` - Main implementation
   - `sc_{verb}_{object}.R` - Execution script
   - `{verb}_{object}_utils.R` - Utility functions
   - `README.md` - Documentation

Example: The "summarize database" command is implemented in `M01_summarizing_database`.

## UI-Server-Defaults Triple (R09)

Shiny components must follow the UI-Server-Defaults triple rule:

1. Each component must have:
   - UI component in a file named with the component function (e.g., `componentNameUI.R`)
   - Server component in a file named with the component function (e.g., `componentNameServer.R`)
   - Defaults component in a file named with the component function (e.g., `componentNameDefaults.R`)

2. Preferred folder-based organization:
   ```
   componentName/
   ├── componentNameUI.R
   ├── componentNameServer.R
   └── componentNameDefaults.R
   ```

## Key Principles to Follow

1. `R21`: One Function One File
2. `R45`: Initialization Imports Only
3. `R46`: Source Directories Not Individual Files
4. `R49`: Apply Functions Over For Loops
5. `R50`: data.table Vectorized Operations
6. `R67`: Functional Encapsulation
7. `R68`: Object Initialization
8. `R69`: Function File Naming
9. `R70`: N-Tuple Delimiter
10. `R72`: Component ID Consistency
11. `R76`: Module Data Connection
12. `R91`: Universal Data Access Pattern
13. `R92`: Universal DBI Approach
14. `MP28`: Avoid Self-Reference
15. `MP29`: No Fake Data
16. `MP30`: Vectorization Principle
17. `MP44`: Functor-Module Correspondence
18. `MP47`: Functional Programming
19. `MP48`: Universal Initialization
20. `MP52`: Unidirectional Data Flow

## Initialization Path

Always use the correct path for initialization:
```r
source(file.path("update_scripts", "global_scripts", "00_principles", "sc_initialization_update_mode.R"))
```

Application mode initialization:
```r
source(file.path("update_scripts", "global_scripts", "00_principles", "sc_initialization_app_mode.R"))
```

Global mode initialization:
```r
source(file.path("update_scripts", "global_scripts", "00_principles", "sc_initialization_global_mode.R"))
```

Do not use deprecated paths:
```r
# DEPRECATED - Do not use
# source(file.path("update_scripts", "global_scripts", "000g_initialization_update_mode.R"))
```

## Testing

Set up proper test scripts following R75 (Test Script Initialization):

```r
# In your test script
options(shiny.launch.browser = TRUE)

# Initialize directory
current_dir <- getwd()
setwd(file.path(current_dir, "precision_marketing_app"))

# Initialize test environment
source(file.path("update_scripts", "global_scripts", "00_principles", "sc_initialization_app_mode.R"))

# Create test data following MP51
test_data <- create_test_data()

# Create and run test app
app <- create_test_app(test_data)
runApp(app, port = 8765) # Don't use launch.browser=TRUE here
```

## Common Mistakes to Avoid

1. Creating redundant index files (use README.md as the primary index)
2. Modifying initialization files directly (use utility files per R45)
3. Manually listing source files instead of sourcing directories (per R46)
4. Creating self-referential code structures (per MP28)
5. Generating fake data in production (per MP29)
6. Using loops instead of vectorized operations (per MP30, R49, R50)
7. Placing related functionality in separate modules (per MP44)
8. Using imperative programming when functional would be cleaner (per MP47)
9. Not properly initializing variables before use (per MP48 and R68)
10. Omitting the "fn_" prefix from function filenames (per R69)
11. Using inconsistent IDs across component parts (per R72)
12. Not separating UI-server-defaults components with delimiters (per R70)
13. Passing pre-filtered data to modules instead of connections (per R76)

## Functionalizing Code

Use the M48_functionalizing module to convert procedural code blocks to proper functions:

```r
source("update_scripts/global_scripts/00_principles/M48_functionalizing/M48_fn_functionalize.R")

# Convert a code block to a function
result <- functionalize(
  code_block = "# Your code here...",
  target_directory = "update_scripts/global_scripts/04_utils",
  function_name = "my_function"
)

# Use the convenient wrapper
run_functionalization(
  code_block = "# Your code here...",
  target_directory = "update_scripts/global_scripts/04_utils"
)
```

**Important scope considerations:**
- Use `<-` for local variables (function scope only)
- Use `<<-` for global variables (when modifying global state)
- Document global variables clearly in function documentation

## Temporary Code Management (MP37)

Follow MP37 (Comment Only for Temporary or Uncertain) when temporarily disabling code:

```r
# TEMP-DISABLED (2025-04-09): Authentication not working with new backend
# Will be fixed when backend team addresses issue #123 (expected by 2025-04-15)
# validate_auth(data)
```

Always include:
1. Standard prefix (TEMP-DISABLED, UNCERTAIN, EXPERIMENTAL, etc.)
2. Date when commented (YYYY-MM-DD)
3. Reason for commenting out
4. Expected resolution date or condition

## 🔄 Pipeline Orchestration (ETL + DRV)

The MAMBA pipeline system orchestrates ETL and DRV scripts using the {targets} R package with declarative dependency markers.

**New in 2025-12-24:** Three-Layer Configuration Architecture (MP140-MP142)

### Three-Layer Configuration (SO_P016)

```
Layer 1 (Universal):  global_scripts/templates/_targets_config.base.yaml
                      └─ Schema only, shared via shared/ symlink
                                   │
                                   ▼ (modifyList merge)
Layer 2 (Company):    app_config.yaml > pipeline: section
                      └─ Company-specific enabled platforms
                                   │
                                   ▼ (make config-merge)
Layer 3 (Generated):  {project_root}/_targets_config.yaml
                      └─ Final merged config for execution
```

### Location
```
update_scripts/
├── _targets.R               # Main orchestration
├── _targets_config.yaml     # Generated config (git-ignored)
├── Makefile                 # All commands
├── ETL/
│   ├── _targets_etl.R       # ETL layer definition
│   └── {platform}/          # ETL scripts per platform
├── DRV/
│   ├── _targets_drv.R       # DRV layer definition
│   └── {platform}/          # DRV scripts per platform
└── scheduling/              # launchd template for macOS
```

### Configuration Commands
```bash
cd scripts/update_scripts

# Configuration Management
make config-merge      # Merge base + company → generated config
make config-scan       # Scan ETL/DRV directories for scripts
make config-full       # config-merge + scan + validate
make config-validate   # Validate configuration integrity
```

### Execution Commands
```bash
# Pipeline Execution (MP140)
make run TARGET=cbz_D04_02   # Execute specific target with dependencies
make run PLATFORM=cbz        # Execute all targets for platform
make run                     # Execute all enabled platforms
make dry-run TARGET=cbz_D04_02  # Show what would execute

# Status & Visualization
make status            # Check pipeline progress
make vis               # Visualize full pipeline DAG
make vis TARGET=cbz_D04_02   # Visualize specific target subgraph
```

### Scheduling Commands (MP141)
```bash
# Scheduling (macOS launchd)
make schedule          # Set up weekly auto-execution (Sunday 2:00 AM)
make schedule-status   # Check if scheduled
make unschedule        # Disable auto-execution
make schedule-logs     # View scheduled job logs
```

### Dependency Markers (MP140)

Each DRV script MUST declare dependencies:
```r
#####
# CONSUMES: df_cbz_sales_transformed
# PRODUCES: df_cbz_poisson_analysis_all
# DEPENDS_ON_ETL: cbz_ETL_sales_2TR
# DEPENDS_ON_DRV: cbz_D04_01
#####
```

### Principles Compliance
- **MP140**: Pipeline Orchestration - Declarative dependencies, on-demand execution
- **MP141**: Scheduled Execution Pattern - Manual + scheduled modes
- **MP142**: Configuration-Driven Pipeline - Three-layer YAML configuration
- **SO_P016**: Configuration Scope Hierarchy - Universal/Company/Application
- **MP122**: Quad-Track Shared Symlink Architecture
- **MP064**: ETL-Derivation separation
- **DM_R041**: Directory structure (platform subdirectories)

---

## 🔄 AI Workflows Index

### WF001: Deployment Quickstart
- **Location**: `23_deployment/06_workflows/WF001_deployment_quickstart.md`
- **Purpose**: Quick deployment process for positioning app
- **Prerequisites**: App config ready, environment variables set

### WF002: Complete Deployment Workflow
- **Location**: `23_deployment/06_workflows/WF002_deployment_complete.md`
- **Purpose**: Full deployment workflow with all checks and documentation
- **Prerequisites**: Clean git status, all tests passing

When asked to deploy applications, refer to these WF-numbered workflow files for step-by-step guidance.

### Workflow Usage Pattern

1. User asks: "understand `/path/to/global_scripts/`"
2. AI reads this CLAUDE.md and discovers workflow locations
3. AI navigates to specific workflows based on task requirements
4. AI follows the structured workflows for consistent execution
