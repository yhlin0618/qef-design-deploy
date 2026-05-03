# {COMPANY_CODE} Company Namespace

<!-- TODO: Fill in this template when onboarding a new company via /new-company skill.
     Replace all {PLACEHOLDER} tokens with real values.
     Keep section structure intact so other companies can browse consistently. -->

## Brand info

<!-- TODO: Company display name, brand owner, business domain (e-commerce, services,
     education, etc.), L-tier (l1_basic / l2_pro / l3_premium / l4_enterprise),
     main product lines, primary geography. -->

- **Display name**: {COMPANY_DISPLAY_NAME}
- **L-tier**: {l1_basic | l2_pro | l3_premium | l4_enterprise}
- **Business domain**: {e.g., 精品電商, 餐飲服務, 汽車零件}
- **Main product lines**: {e.g., 技詮汽車零件、品牌配件、Amazon美國站}
- **Primary platforms**: {e.g., cbz, eby, amz, shp}
- **Dashboard URL**: {Posit Connect URL when deployed}

## Special environment

<!-- TODO: List company-specific integrations that differ from the generic framework.
     Examples: ERP connection via SSH tunnel + MSSQL, custom Shopify GraphQL endpoint,
     proprietary BI system integration, etc. Link to any relevant secret env vars. -->

- **Database backend**: {duckdb local | PostgreSQL Supabase | both}
- **External data sources**: {e.g., Cyberbiz API, eBay MSSQL via SSH tunnel, Google Sheets}
- **Required env vars**: {list from .env.template}
- **SSH tunnel**: {if applicable, describe host + port forwarding}
- **API credentials**: {where stored, e.g., .env file, Posit Connect Variable Set}

## Known issues and lessons learned

<!-- TODO: As you encounter company-specific quirks, schema drift, silent bugs,
     or edge cases, add them to notes/ with dated filenames (YYYY-MM-DD_topic.md)
     and cross-reference GitHub issues here.

     When starting fresh with no prior history, leave this section empty but keep
     the heading so later entries have a natural home. -->

| Date | Topic | GitHub Issue | Notes file |
|---|---|---|---|
| — | — | — | — |

## File overview

<!-- TODO: Brief description of each file in this namespace. Explain what problem
     it solves and under what conditions it should be copied/adapted to a new
     company (NOT source()-d — this is a cookbook, not a library). -->

### `02_db_utils/`

<!-- e.g., fn_ensure_tunnel.R — establishes SSH tunnel before MSSQL connection.
     Adapt for any company using MSSQL behind SSH. -->

### `notes/`

<!-- Chronological lessons-learned documents. See dated files. -->

## Cookbook usage reminder

**Files in this namespace are read-and-adapted, NOT source()-d by other companies.**

If another company needs similar functionality:

1. Copy the file to `29_company_examples/{other_company}/{chapter}/` with an
   attribution comment referencing this namespace
2. Or, if 2+ companies need the same helper, promote it to a generic chapter
   (e.g., `02_db_utils/`) via a separate Spectra change

See [chapter 29 README](../README.md) for the full cookbook vs library rationale.

## Related principles

- **SO_P018**: Directory Governance — Chapter 29 allow-list + cookbook semantics
- **MP122**: Penta-Track Subrepo Architecture — Track 1 (Framework)
- **DOC_R009**: Triple-Layer Sync — any governance change must propagate to llm/ + rules/
