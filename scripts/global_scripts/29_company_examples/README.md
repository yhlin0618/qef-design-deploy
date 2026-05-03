# Chapter 29: Company Examples Cookbook

> **Cookbook,not Library** — files here are **read-and-adapted**,not `source()`-d
> across companies. See "Design philosophy" below.

## Purpose

This chapter collects **company-specific code patterns + lessons-learned notes**
from each onboarded company in the L4 Enterprise tier. New companies onboarding
to the framework can browse existing companies' namespaces to learn:

- How others solved company-specific integration problems (MSSQL, ERP, custom APIs)
- What schema drift / silent bugs / edge cases have been encountered
- Active code that would otherwise pollute the generic numbered chapters

## Design philosophy: Cookbook vs Library

| Aspect | Library (`02_db_utils/` etc.) | Cookbook (`29_company_examples/`) |
|---|---|---|
| Use mode | `source(...)` to import | Read, copy, adapt |
| Cross-company reuse | Yes — generic functions | **No** — namespace isolation |
| Audience | All companies | Other companies as reference |
| Code drift | Bad — DRY violation | Acceptable — independent evolution |
| Promotion path | — | Promoted to library when 2+ callers exist (rule of three) |

**Key constraint** (per `DEV_R0XX` and `cross-company-source` requirement):
R scripts for company X SHALL NOT `source()` files inside `29_company_examples/Y/`
where X != Y. If functionality is needed across companies, copy the code into
your namespace (with attribution comment) or promote to a generic chapter.

## Directory structure

Each company has its own namespace mirroring root-level numbered chapters:

```
29_company_examples/
├── README.md                        ← this file
├── _template/                       ← starter for new company onboarding
│   ├── README.md
│   ├── notes/
│   └── 02_db_utils/
├── mamba/                           ← MAMBA company namespace
│   ├── README.md                    ← MAMBA's brand info, env, known issues
│   ├── 02_db_utils/                 ← mirrors root 02_db_utils/
│   │   ├── fn_ensure_tunnel.R
│   │   └── fn_ensure_tunnel_enhanced.R
│   └── notes/                       ← lessons-learned (YYYY-MM-DD_topic.md)
└── {NEW_COMPANY}/                   ← created by /new-company skill
```

## Existing companies

(populated as new companies are onboarded)

| Company | Namespace | README |
|---|---|---|
| MAMBA | [`mamba/`](mamba/README.md) | MSSQL via SSH tunnel + ERP integration |

## Onboarding a new company

The `/new-company` skill (`.claude/skills/new-company/SKILL.md`) handles
namespace creation:

1. Run `/new-company` and follow prompts
2. The skill copies `_template/` to `{lowercase_company_code}/`
3. Fill the placeholder sections in the new company's `README.md`
4. As you encounter company-specific quirks, add notes to `notes/`

## Related principles

- **SO_P018**: Directory Governance — chapter 29 is the only allowed location
  for company-specific code in `shared/global_scripts/`
- **MP122**: Penta-Track Subrepo Architecture — chapter 29 lives in Track 1
  (Framework subrepo `global_scripts`)
- **DOC_R009**: Triple-Layer Sync — changes to chapter 29 governance must
  propagate to `llm/CH01_structure.yaml` and `.claude/rules/07-directory-governance.md`
