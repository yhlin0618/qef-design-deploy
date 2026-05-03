
---

## Iteration 1 - 2026-04-29T01:37:17Z

**Reviewers**: codex, correctness, security, devils_advocate

**Findings**: 126 (30 CRITICAL / 34 HIGH / 25 MEDIUM / 27 LOW / 10 SUGGESTION)

**Findings hash**: 5f869a3611c6e7108fd339af70618edba50a9e45738738a0a215a1c1d2192d4a

| ID  | Severity   | Source              | Rule       | Summary                                |
|-----|------------|---------------------|------------|----------------------------------------|
|   1 | HIGH       | codex               |            | - Severity:                            |
|   2 | LOW        | codex               |            | - Severity:                            |
|   3 | CRITICAL   | correctness         |            | | | 2 |                                |
|   4 | HIGH       | correctness         |            | | | 2 |                                |
|   5 | MEDIUM     | correctness         |            | | | 1 |                                |
|   6 | LOW        | correctness         |            | | | 0 |                                |
|   7 | CRITICAL   | correctness         |            | ## findings (block bridge from being m |
|   8 | CRITICAL   | correctness         |            | ### -1: Schema declares `保固` as VARCHA |
|   9 | CRITICAL   | correctness         |            | **Severity**:                          |
|  10 | CRITICAL   | correctness         | MP159      | This is a because it represents a Laye |
|  11 | CRITICAL   | correctness         |            | ### -2: Schema declares `清潔方式` as VARC |
|  12 | CRITICAL   | correctness         |            | **Severity**:                          |
|  13 | CRITICAL   | correctness         |            | Schema says VARCHAR ("建議的清潔方式,如 '乾布擦拭' |
|  14 | CRITICAL   | correctness         |            | 3. 同 -1,需要走 schema change 修 type;不能在 b |
|  15 | HIGH       | correctness         |            | ## findings (data fidelity — should fi |
|  16 | HIGH       | correctness         |            | ### -1: BOOLEAN field with `'NA'` lite |
|  17 | HIGH       | correctness         |            | **Severity**:                          |
|  18 | HIGH       | correctness         |            | **Why this is not **:                  |
|  19 | HIGH       | correctness         |            | ### -2: DOUBLE fields with `'NA'` lite |
|  20 | HIGH       | correctness         |            | **Severity**:                          |
|  21 | HIGH       | correctness         |            | **Why this is "less wrong" than -1 but |
|  22 | HIGH       | correctness         | MP154      | 優先級比 -1 低,因為當前不會破壞資料,但顯性化是 MP154 的精神。 |
|  23 | MEDIUM     | correctness         |            | ## findings                            |
|  24 | MEDIUM     | correctness         |            | ### -1: Bridge column ordering mixes r |
|  25 | MEDIUM     | correctness         |            | **Severity**: (cosmetic / readability) |
|  26 | MEDIUM     | correctness         |            | **Why not **:                          |
|  27 | CRITICAL   | correctness         |            | - `保固`: "1" → `warranty`: VARCHAR "1"  |
|  28 | HIGH       | correctness         |            | - `鏡框最大寬度`: "NA" → `dimension_frame_ma |
|  29 | HIGH       | correctness         |            | - `高能可見光阻隔技術`: "NA" → `protection_hev_ |
|  30 | CRITICAL   | correctness         |            | 1. **Block -1 + -2** — verify with QEF |
|  31 | HIGH       | correctness         |            | 2. **Fix -1** — add `value_map: { "NA" |
|  32 | HIGH       | correctness         |            | 3. **Consider -2** — same value_map tr |
|  33 | MEDIUM     | correctness         |            | 4. **Defer -1** — generator-level impr |
|  34 | CRITICAL   | security            |            | | | 0 |                                |
|  35 | HIGH       | security            |            | | | 1 |                                |
|  36 | MEDIUM     | security            |            | | | 4 |                                |
|  37 | LOW        | security            |            | | | 4 |                                |
|  38 | SUGGESTION | security            |            | | | 2 |                                |
|  39 | HIGH       | security            |            | **Verdict**: Bridge is **NOT yet ship- |
|  40 | HIGH       | security            |            | - **Severity**:                        |
|  41 | HIGH       | security            |            | - **Why not **: requires source-side w |
|  42 | MEDIUM     | security            |            | - **Severity**:                        |
|  43 | CRITICAL   | security            |            | 1. `findings_summary$ == 0` and `$ ==  |
|  44 | CRITICAL   | security            |            | findings_summary: { : 0, : 0, : 0, : 0 |
|  45 | CRITICAL   | security            |            | and crafts a `.bridge.review.md` that  |
|  46 | LOW        | security            |            | No path-traversal guard. An attacker w |
|  47 | CRITICAL   | security            |            | : 0                                    |
|  48 | HIGH       | security            |            | : 0                                    |
|  49 | MEDIUM     | security            |            | : 1                                    |
|  50 | LOW        | security            |            | : 4                                    |
|  51 | SUGGESTION | security            |            | : 2                                    |
|  52 | CRITICAL   | security            |            | additions over current 6 fields: `sche |
|  53 | MEDIUM     | security            |            | - **Severity**:                        |
|  54 | LOW        | security            |            | - `fn_hash_prerawdata_schema.R:36-45`  |
|  55 | LOW        | security            |            | ## Finding 4 — `target_table` from yam |
|  56 | MEDIUM     | security            |            | - **Severity**:                        |
|  57 | MEDIUM     | security            |            | - **Severity**:                        |
|  58 | LOW        | security            |            | - Perl regex with attacker-controlled  |
|  59 | LOW        | security            |            | - **Severity**:                        |
|  60 | SUGGESTION | security            | CWE-200    | - **Rule**: SEC_R-secret-disclosure, C |
|  61 | LOW        | security            |            | - Per prior ensemble finding, the gshe |
|  62 | SUGGESTION | security            |            | - **Severity**: (clean)                |
|  63 | LOW        | security            |            | - `from_column` fs through `match(toer |
|  64 | LOW        | security            |            | ## Finding 8 — `pre_filter.exclude_val |
|  65 | LOW        | security            |            | - **Severity**:                        |
|  66 | LOW        | security            |            | - In future `glue-bridge` skill output |
|  67 | LOW        | security            |            | ## Finding 9 — Bridge file path resolu |
|  68 | LOW        | security            |            | - **Severity**:                        |
|  69 | SUGGESTION | security            |            | - **Severity**:                        |
|  70 | LOW        | security            |            | - **Severity**:                        |
|  71 | LOW        | security            |            | - **Severity**: (private-repo) / (if p |
|  72 | SUGGESTION | security            | CWE-209    | - **Rule**: CWE-209 (rmation Exposure  |
|  73 | CRITICAL   | security            |            | | `findings_summary.{,,,,}` | Aggregat |
|  74 | SUGGESTION | security            |            | n = file.(artifact_resolved)$size)     |
|  75 | HIGH       | security            |            | 1. ** leverage** — Add `constraints:`  |
|  76 | HIGH       | security            |            | 2. ** leverage** — Extend the planned  |
|  77 | MEDIUM     | security            |            | 3. ** leverage** — Add `target_table`  |
|  78 | MEDIUM     | security            |            | 4. ** leverage** — Add path-traversal  |
|  79 | LOW        | security            |            | 5. ** leverage** — Switch `dbWriteTabl |
|  80 | HIGH       | devils_advocate     |            | - **Severity**:                        |
|  81 | CRITICAL   | devils_advocate     |            | - **Recommendation**: This isn't a bug |
|  82 | CRITICAL   | devils_advocate     |            | - **Severity**:                        |
|  83 | CRITICAL   | devils_advocate     |            | - Same finding as correctness -1, -2.  |
|  84 | HIGH       | devils_advocate     |            | - **Severity**:                        |
|  85 | CRITICAL   | devils_advocate     |            | - **Recommendation**: Either (a) schem |
|  86 | MEDIUM     | devils_advocate     |            | - **Severity**:                        |
|  87 | HIGH       | devils_advocate     |            | - The generator silently skips canonic |
|  88 | MEDIUM     | devils_advocate     |            | - **Severity**:                        |
|  89 | HIGH       | devils_advocate     |            | ### IND-6: `as.numeric("NA")` leak     |
|  90 | LOW        | devils_advocate     |            | - **Severity**:                        |
|  91 | HIGH       | devils_advocate     |            | - Source CSV has literal `"NA"` string |
|  92 | HIGH       | devils_advocate     |            | - `as.numeric("NA")` in R returns `NA_ |
|  93 | HIGH       | devils_advocate     |            | - **Recommendation**: Wrap coercion wi |
|  94 | HIGH       | devils_advocate     |            | | 1 | : BOOLEAN `NA` coerced to FALSE  |
|  95 | LOW        | devils_advocate     |            | | 2 | : Mojibake `â` in product_name | |
|  96 | HIGH       | devils_advocate     | MP160      | | Checked, no finding (5 items) | code |
|  97 | CRITICAL   | devils_advocate     |            | | -1 | `保固` schema VARCHAR but source  |
|  98 | CRITICAL   | devils_advocate     |            | | -2 | `清潔方式` schema VARCHAR but sourc |
|  99 | CRITICAL   | devils_advocate     |            | | -3 (missed) | rating_* fields semant |
| 100 | HIGH       | devils_advocate     |            | | -1 | BOOLEAN `'NA'` → FALSE silent | |
| 101 | HIGH       | devils_advocate     | MP154      | | -2 | DOUBLE `'NA'` → NA_real_ "lucky |
| 102 | MEDIUM     | devils_advocate     |            | | -1 | Column ordering mixes required/ |
| 103 | HIGH       | devils_advocate     |            | | F1 | : No `constraints:` on numeric  |
| 104 | MEDIUM     | devils_advocate     |            | | F2 | : `reviewed_by` structured form |
| 105 | MEDIUM     | devils_advocate     |            | | F3 | : Fingerprint covers names+type |
| 106 | MEDIUM     | devils_advocate     |            | | F4 | : `target_table` from yaml not  |
| 107 | MEDIUM     | devils_advocate     |            | | F5 | : ReDoS in pattern field | **So |
| 108 | LOW        | devils_advocate     |            | | F6 | : source_uri leaks layout | **M |
| 109 | SUGGESTION | devils_advocate     |            | | F7 | : value_map clean | **Disagree  |
| 110 | LOW        | devils_advocate     |            | | F8 | : pre_filter abuse vector | **M |
| 111 | LOW        | devils_advocate     |            | | F9 | : Path traversal in bridges_roo |
| 112 | SUGGESTION | devils_advocate     |            | | F10 | : fingerprint stable for blb | |
| 113 | LOW        | devils_advocate     |            | | F11 | : dbWriteTable(append=TRUE) re |
| 114 | LOW        | devils_advocate     |            | | F12 | : Error messages leak filesyst |
| 115 | HIGH       | devils_advocate     |            | - **codex/correctness/security all rev |
| 116 | CRITICAL   | devils_advocate     |            | - **Concrete unknown**: Does the `保固`  |
| 117 | CRITICAL   | devils_advocate     |            | - **Recommendation**: Before declaring |
| 118 | LOW        | devils_advocate     |            | | Codex "no finding" downgrades | 1 (` |
| 119 | CRITICAL   | devils_advocate     |            | | Correctness new (was PASS) | 1 (rati |
| 120 | MEDIUM     | devils_advocate     |            | | Correctness severity downgrade | 1 ( |
| 121 | MEDIUM     | devils_advocate     |            | | Security severity escalation | 2 (F4 |
| 122 | MEDIUM     | devils_advocate     |            | | Security severity downgrade | 1 (F5  |
| 123 | SUGGESTION | devils_advocate     |            | | Security /PASS challenged | 1 (F7 ne |
| 124 | CRITICAL   | devils_advocate     |            | **Net escalations**: 4 (1 new , 2 seve |
| 125 | MEDIUM     | devils_advocate     |            | **Net downgrades**: 2 (correctness -1, |
| 126 | CRITICAL   | devils_advocate     |            | - Schema has at least 3 type-mismatch  |
---


## Iteration 2 - 2026-04-29T10:14:24Z

**Reviewers**: codex-cli@0.124.0, claude-opus-4-7-correctness, claude-opus-4-7-security, claude-opus-4-7-devils-advocate

**Run dir**: /tmp/blb_iter2_20260429_175936

**Findings**: 17 (0 CRITICAL / 0 HIGH / 5 MEDIUM / 7 LOW / 5 SUGGESTION) — strict canonical F-N parser

**Findings hash**: 5109d1ed2f2b2802b9cc0b854d9e5d15094b23c2116328f85ebd1d1e0f2bbb69

**Wave-1 reviewer verdicts**:
- codex-cli: PASS (0 CRITICAL / 0 HIGH / 1 MEDIUM)
- claude-correctness: PASS (Gap A + Gap B both CLOSED, max MEDIUM)
- claude-security: PASS (0 new HIGH+ from #502 changes)
- claude-devils-advocate: PASS (0 escalations of wave-1 findings; 5 new systemic blind spots, all MEDIUM/LOW/SUGGESTION)

**Iter 2 expectation**:
- Iter 1 max severity: CRITICAL (30 CRITICAL / 34 HIGH)
- Iter 2 max severity: MEDIUM (achieved expected drop to MEDIUM or lower)
- Gap A (Layer 1 BOOLEAN type fix): **CLOSED** (verified by codex + correctness; warranty/cleaning_method type=BOOLEAN with notes; cross-PL consistency verified)
- Gap B (Layer 0 generator NA-scanning): **CLOSED** (verified by codex + correctness; 15 value_map entries in blb; 414 total across 12 PLs; MP160 set equality holds)

**Findings table**:

| ID  | Severity   | Source              | Rule       | Summary                                                      |
|-----|------------|---------------------|------------|--------------------------------------------------------------|
|   1 | MEDIUM     | codex               |            | `reviewed_by` remains a placeholder and still blocks final b |
|   2 | LOW        | correctness         |            | Schema notes for `warranty` and `cleaning_method` claim "som |
|   3 | MEDIUM     | correctness         |            | Bridge file lacks structured `reviewed_by` block (still plac |
|   4 | LOW        | correctness         |            | Bridge file's leading comment line 13-14 still says "reviewe |
|   5 | SUGGESTION | correctness         |            | BOM-prefixed first column header observed in CSV bytes (`﻿"品 |
|   6 | SUGGESTION | correctness         |            | Field `quantity` (數量) is INTEGER and source values are all " |
|   7 | LOW        | security            |            | Two-pass CSV read doubles attack surface for malicious sourc |
|   8 | MEDIUM     | security            |            | `build_value_map_for_col()` keys are emitted to yaml without |
|   9 | SUGGESTION | security            |            | BOOLEAN cast path: value_map intermediate does not introduce |
|  10 | LOW        | security            |            | Two-pass read with `colClasses = "character"` does not open  |
|  11 | MEDIUM     | devils_advocate     |            | Unit tests cover helpers in isolation but provide ZERO integ |
|  12 | LOW        | devils_advocate     |            | Whitespace handling is asymmetric — value_map keys are trimm |
|  13 | LOW        | devils_advocate     |            | Lowercase `na`/`n/a` deliberately excluded but no validation |
|  14 | MEDIUM     | devils_advocate     |            | The committed bridge yaml ships with `reviewed_by: REQUIRES_ |
|  15 | LOW        | devils_advocate     |            | No idempotence test — running `gen_product_attribute_bridges |
|  16 | SUGGESTION | devils_advocate     |            | Cross-bridge target_table consistency NOT verified by any re |
|  17 | SUGGESTION | devils_advocate     |            | `protection_blue_light_rate` mixes percent strings (`"65%-98 |

**Notable structural concerns** (from devils-advocate, all MEDIUM/SUGGESTION; tracked separately):
- F-1 (MEDIUM): unit tests cover helpers in isolation only; no integration coverage of generator end-to-end
- F-2 (LOW): whitespace handling asymmetry between generator and runtime trim contracts
- F-3 (LOW): case-sensitivity choice of NA literals ("na"/"n/a" not in canonical set) not documented
- F-5 (LOW): wall-clock timestamps in generated_at/fingerprinted_at break idempotence checks
- F-7 (SUGGESTION): protection_blue_light_rate source has "65%-98%" percent-range that won't cast to DOUBLE (out-of-scope for #502)

**Out-of-scope re-acks** (not raised as new findings; tracked separately for future spectra changes):
- numeric range constraints (#502 follow-up scope 3)
- reviewed_by cryptographic binding (#502 follow-up scope 4)
- sales bridge dead coercion keys (#502 follow-up scope 5)
- target_table integrity check (#502 follow-up scope 6)

**Verdict**: 

## Verdict: CONVERGED at iteration 2 (pending findings_resolutions for 5 MEDIUM + 7 LOW)

Both Gap A and Gap B from iter 1 are demonstrably closed. The 4-reviewer ensemble independently verifies the bridge is ship-ready at the data-correctness layer. The 17 remaining findings (0 critical / 0 high / 5 medium / 7 low / 5 suggestion) split into:
- Out-of-scope follow-ups (#502 scopes 3-6): tracked separately, no resolution action needed in this iter
- Devil's advocate systemic concerns (process-timing, idempotence, test coverage): non-blocking; recommend separate hardening change

Step 6e prerequisite: human reviewer SHALL author findings_resolutions: block in bridge yaml for the 12 MEDIUM+LOW findings before structured reviewed_by is written. Until then bridge yaml retains placeholder reviewed_by: REQUIRES_HUMAN_REVIEW.
