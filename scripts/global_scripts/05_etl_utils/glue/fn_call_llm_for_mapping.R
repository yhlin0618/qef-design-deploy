#' @file fn_call_llm_for_mapping.R
#' @principle MP029 (no fake data); design D3 (LLM is codegen-time only)
#' @author Claude (#489 Phase 4)
#'
#' SKILL-ONLY HELPER — DO NOT CALL FROM PRODUCTION RUNTIME PATH.
#'
#' This function exists to be invoked by the `glue-bridge` Claude skill
#' during Step 2 (Generate). It encapsulates the contract between the
#' skill and an LLM-backed model service: structured input (canonical
#' schema yaml + prerawdata fingerprint + sample column metadata),
#' structured output (draft bridge yaml that a human reviewer must sign
#' off on before commit).
#'
#' At runtime, `fn_glue_bridge.R` reads the *committed* bridge yaml and
#' applies it deterministically. There is no path from runtime to this
#' file — runtime is interpreter-only (design D3).
#'
#' Marker for runtime audit: this file declares the variable
#' `LLM_CODEGEN_TIME_ONLY` so an automated audit can `grep` for any
#' source() of this file inside `05_etl_utils/glue/fn_glue_bridge.R`
#' and fail-build if found.

LLM_CODEGEN_TIME_ONLY <- TRUE  # nolint: object_name_linter

#' Build the LLM prompt for bridge mapping codegen
#'
#' Pure data transformation (no actual LLM invocation here — the skill
#' uses Claude Code's native tools to execute the prompt).
#'
#' @param company Character; e.g. "QEF_DESIGN"
#' @param platform Character; e.g. "amz"
#' @param datatype Character; one of sales/customers/orders/products/reviews
#' @param prerawdata_columns Named character vector: name -> inferred type
#' @param schema_fingerprint List from `hash_prerawdata_schema()`
#' @param canonical_schema_yaml Character: full content of core_schemas.yaml
#' @param platform_extension_yaml Character: full content of {platform}_extensions.yaml
#'        (or "" if no extension)
#' @return Character: the full prompt to send to an LLM
#' @export
build_bridge_codegen_prompt <- function(company, platform, datatype,
                                          prerawdata_columns,
                                          schema_fingerprint,
                                          canonical_schema_yaml,
                                          platform_extension_yaml = "") {
  paste0(
    "You are generating a bridge yaml for the glue layer (spectra change\n",
    "`glue-layer-prerawdata-bridge`, issue #489). The bridge maps a\n",
    "company's prerawdata source columns to canonical raw layer fields.\n",
    "\n",
    "Inputs:\n",
    "  Company: ", company, "\n",
    "  Platform: ", platform, "\n",
    "  Datatype: ", datatype, "\n",
    "  Schema fingerprint (sha256): ", schema_fingerprint$value, "\n",
    "  Prerawdata columns:\n",
    paste(sprintf("    - %s: %s",
                  names(prerawdata_columns),
                  unname(prerawdata_columns)),
          collapse = "\n"), "\n",
    "\n",
    "Canonical schema (core_schemas.yaml):\n",
    "```yaml\n", canonical_schema_yaml, "\n```\n",
    "\n",
    if (nzchar(platform_extension_yaml)) {
      paste0("Platform extension yaml:\n```yaml\n",
             platform_extension_yaml, "\n```\n\n")
    } else "",
    "Required output: a complete bridge yaml conforming to the schema in\n",
    "the glue-bridge SKILL.md. Specifically:\n",
    "  - Every required canonical field must have a column_mapping entry,\n",
    "    a derive_from rule, or apply_fallback: true.\n",
    "  - For every prerawdata column not used, list it in ignored_columns.\n",
    "  - Use the canonical schema's aliases to disambiguate column names.\n",
    "  - reviewed_by MUST be left as the placeholder \"REQUIRES_HUMAN_REVIEW\"\n",
    "    (a human reviewer fills this in before commit).\n",
    "  - generated_by MUST be \"glue-bridge skill v1.0\".\n",
    "  - generated_at MUST be the current ISO 8601 UTC timestamp.\n",
    "\n",
    "Do NOT invent columns that don't appear in the prerawdata schema above.\n",
    "Do NOT invent canonical fields not in the canonical schema yaml.\n",
    "If a required canonical field has no source column and no fallback in\n",
    "the canonical schema, mark it apply_fallback: true and emit a comment\n",
    "in review_notes explaining the gap for the human reviewer.\n"
  )
}
