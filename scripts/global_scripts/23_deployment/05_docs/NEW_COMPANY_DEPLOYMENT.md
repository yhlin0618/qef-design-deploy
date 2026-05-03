# New Company Deployment Guide

> **Based on MP122: Quad-Track Shared Symlink Architecture**
>
> This guide explains how to deploy a new company project using the standardized Shared Symlink architecture.

## Architecture Overview

```
{tier}/
├── shared/                          ← Standalone git repos (shared across companies)
│   ├── global_scripts/              ← Track 1: Framework (standalone git)
│   ├── update_scripts/              ← Track 2: Scripts (standalone git)
│   └── nsql/                        ← Track 4: NSQL (standalone git)
│
└── {project}/
    ├── app_config.yaml              ← Track 3: Config (NOT shared)
    ├── .env                         ← Track 3: Credentials (NOT shared)
    │
    └── scripts/
        ├── global_scripts -> ../../shared/global_scripts   ← symlink
        ├── update_scripts -> ../../shared/update_scripts   ← symlink
        └── nsql -> ../../shared/nsql                       ← symlink
```

### Remote Repositories

| Track | Remote URL | Branch | Storage |
|-------|------------|--------|---------|
| Track 1 (Framework) | `git@github.com:kiki830621/ai_martech_global_scripts.git` | main | `shared/global_scripts/` |
| Track 2 (Scripts) | `git@github.com:kiki830621/ai_martech_update_scripts.git` | main | `shared/update_scripts/` |
| Track 3 (Config) | NOT shared - project independent | - | `{project}/` |
| Track 4 (NSQL) | `git@github.com:kiki830621/nsql.git` | main | `shared/nsql/` |

---

## Deployment Options

### Option A: Minimal (Track 3 only)
**Use when**: New company can use generic scripts without customization
- Create Track 3 configuration only
- Symlink to shared repos
- Use generic scripts from Track 2 (no `___COMPANY` suffix)
- Fastest and simplest

### Option B: Standard (Track 3 + Track 2 variants)
**Use when**: Some platforms need company-specific customization
- Create Track 3 configuration
- Symlink to shared repos
- Add `___NEWCOMPANY.R` variant scripts in shared Track 2
- Other platforms use generic scripts
- **Recommended for most deployments**

### Option C: Full (New repo + symlinks)
**Use when**: Company needs independent Git management
- Create new Git repository
- Symlink to shared repos (global_scripts, update_scripts, nsql)
- Create complete Track 3 configuration

---

## Standard Deployment Flow (Option B)

### Step 0: Prerequisites Check

Before starting, verify these requirements:

```bash
# Check 1: GitHub CLI installed?
gh --version
# Expected: gh version 2.x.x or higher

# Check 2: GitHub CLI authenticated?
gh auth status
# Expected: "Logged in to github.com account..."

# Check 3: GitHub SSH access?
ssh -T git@github.com
# Expected: "Hi {username}! You've successfully authenticated..."
```

**If checks fail, fix first:**

```bash
# Install and authenticate GitHub CLI
brew install gh
gh auth login

# Set up SSH key (if missing)
ssh-keygen -t ed25519 -C "your_email@example.com"
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_ed25519

# Add public key to GitHub
cat ~/.ssh/id_ed25519.pub
# Copy output → GitHub → Settings → SSH and GPG keys → New SSH key

# Verify
ssh -T git@github.com
```

> ⚠️ **Do NOT proceed until checks pass!**

### Step 1: Create Project Directory

```bash
# Example: New company "ACME"
mkdir -p /path/to/ai_martech/l4_enterprise/ACME
cd /path/to/ai_martech/l4_enterprise/ACME

# Create basic directories (NOTE: rawdata is configured in Step 1.5)
mkdir -p scripts data/local_data data/app_data logs docs
```

### Step 1.5: Rawdata Setup (MANUAL - User Action Required)

> ⚠️ **IMPORTANT**: Do NOT use scripts to create or move rawdata folders!
> The automated skill only creates `data/local_data/`. User must manually set up rawdata.

**Why manual?** Rawdata folders are typically Dropbox-synced folders. Moving them programmatically can cause sync issues or data loss.

#### User Action: Choose and execute one option

**Option A: Symlink to Existing Dropbox Folder**
```bash
# When client data already exists elsewhere in Dropbox
ln -s /path/to/shared/client_data data/local_data/rawdata_ACME
```

**Option B: Move Dropbox Folder via Finder**
```
1. Open Finder
2. Drag the existing Dropbox data folder into data/local_data/
3. Rename to rawdata_ACME
```

**Option C: Create New Dropbox Shared Folder**
```bash
# Create folder, then set up Dropbox sharing via web interface
mkdir -p data/local_data/rawdata_ACME
# Then: Dropbox web → Share → Invite client
```

**Option D: API Download Only (No Dropbox)**
```bash
# Create empty directory, ETL 0IM scripts will download data
mkdir -p data/local_data/rawdata_ACME
```

> **After setup**: Verify `RAW_DATA_DIR` in `app_config.yaml` points to correct path!

### Step 2: Initialize Git, Create GitHub Repo, and Set Up Symlinks

```bash
# Stay at project ROOT (not scripts/)
cd /path/to/ai_martech/l4_enterprise/ACME

# Initialize Git at project root
git init

# Create GitHub repo via CLI (naming: ai_martech_{tier}_{company})
gh repo create kiki830621/ai_martech_l4_ACME --private --source=. --remote=origin

# Ensure shared repos exist at tier level (if not already cloned)
cd /path/to/ai_martech/l4_enterprise
[ -d shared/global_scripts/.git ] || git clone git@github.com:kiki830621/ai_martech_global_scripts.git shared/global_scripts
[ -d shared/update_scripts/.git ] || git clone git@github.com:kiki830621/ai_martech_update_scripts.git shared/update_scripts
[ -d shared/nsql/.git ] || git clone git@github.com:kiki830621/nsql.git shared/nsql

# Create symlinks from project scripts/ to shared/
cd /path/to/ai_martech/l4_enterprise/ACME
mkdir -p scripts
ln -s ../../shared/global_scripts scripts/global_scripts
ln -s ../../shared/update_scripts scripts/update_scripts
ln -s ../../shared/nsql scripts/nsql
```

**Naming Convention:**
| Tier | Repo Name |
|------|-----------|
| l1_basic | `ai_martech_l1_ACME` |
| l2_pro | `ai_martech_l2_ACME` |
| l3_premium | `ai_martech_l3_ACME` |
| l4_enterprise | `ai_martech_l4_ACME` |

### Step 3: Copy Track 3 Template Files

From MAMBA template:
```bash
TEMPLATE="/path/to/ai_martech/l4_enterprise/MAMBA"

cp $TEMPLATE/app_config.yaml .
cp $TEMPLATE/.env.template .
cp $TEMPLATE/.gitignore .
cp $TEMPLATE/app.R .
cp $TEMPLATE/.Rprofile .
cp $TEMPLATE/_targets.R .
cp $TEMPLATE/MAMBA.Rproj ./ACME.Rproj
```

### Step 4: Customize app_config.yaml

```yaml
app:
  name: "ACME Analytics Platform"
  version: "1.0.0"
  tier: "l4_enterprise"

brand_name: "ACME"
language: "zh_TW.UTF-8"
RAW_DATA_DIR: "./data/local_data/rawdata_ACME"

# Enabled platforms
platform:
  - cbz
  - eby

# Company variant settings
execution:
  company_variant: "ACME"
  use_company_specific_scripts: true

# Platform-specific settings
platforms:
  cbz:
    enabled: true
  eby:
    enabled: true
    variant: "ACME"  # Use eby_ETL_*___ACME.R

deployment:
  target: "posit_connect"
  account_name: "acme-team"
  app_name: "acme-analytics"
```

### Step 5: Configure Environment Variables

```bash
cp .env.template .env
chmod 600 .env

# Edit .env with ACME-specific credentials
# EBY_SSH_HOST=acme-ebay-server.example.com
# CBZ_API_TOKEN=acme_cyberbiz_token
# OPENAI_API_KEY=sk-xxx
```

### Step 6: Initial Commit and Push to GitHub

```bash
cd /path/to/ai_martech/l4_enterprise/ACME

# Add all files
git add .

# Initial commit
git commit -m "Initial setup: ACME analytics platform

- Track 3 configuration files
- Symlinks to shared repos: global_scripts, update_scripts, nsql
- Based on MAMBA template"

# Push to GitHub
git push -u origin main
```

### Step 7: Create Company-Specific Script Variants (if needed)

In **Track 2** (shared update_scripts):

```bash
cd scripts/update_scripts/ETL/eby/

# Copy generic version as base
cp eby_ETL_orders_0IM.R eby_ETL_orders_0IM___ACME.R

# Edit ACME-specific settings (SSH tunnel, DB connection, etc.)
```

**Important**: After modifying variant scripts, push changes in shared Track 2:
```bash
cd /path/to/ai_martech/l4_enterprise/shared/update_scripts
git add .
git commit -m "Add ACME ETL variants"
git pull && git push
```

### Step 8: Verify Deployment

```bash
# Test connections
Rscript scripts/global_scripts/98_test/test_connections.R

# Test script resolution
Rscript -e '
source("scripts/global_scripts/04_utils/fn_resolve_script_path.R")
company <- get_company_from_config()
print(paste("Company:", company))
script <- resolve_script_path("ETL/eby/eby_ETL_orders_0IM.R", company)
print(paste("Resolved:", script))
'
```

---

## File Checklist: New Company Minimum Requirements

### Track 3 (Project Root) - MUST create

| File | Action | Description |
|------|--------|-------------|
| `app_config.yaml` | Copy + customize | Company name, platforms, deployment settings |
| `.env` | Create | Company credentials (from .env.template) |
| `.gitignore` | Copy | Protect sensitive data |
| `app.R` | Copy | Entry point (usually no modification needed) |
| `.Rprofile` | Copy | Initialization settings |
| `ACME.Rproj` | Create | RStudio project file |

### Track 2 (Shared update_scripts) - Create as needed

| File | Action | Description |
|------|--------|-------------|
| `*___ACME.R` | Create | Only when platform needs customization |

### Track 1 (Shared global_scripts) - No modification needed

Framework layer remains unchanged, shared by all companies.

---

## Shared Repo Maintenance Commands

```bash
# Pull latest shared code
cd shared/global_scripts && git pull
cd shared/update_scripts && git pull
cd shared/nsql && git pull

# Push changes (normal workflow, direct push)
cd shared/update_scripts
git add . && git commit -m "[TYPE] description" && git pull && git push

# For tracked issues needing formal review
cd shared/global_scripts
git checkout -b fix/issue-description
# ... make changes ...
git add . && git commit -m "[FIX] description"
git push -u origin fix/issue-description
gh pr create --title "Fix: description" --body "Closes #123"

# Check shared repo status
cd shared/global_scripts && git status
cd shared/update_scripts && git status
```

---

## Deployment Checklist

### Prerequisites (Verify First!)
- [ ] `gh --version` → shows version 2.x.x+
- [ ] `gh auth status` → shows "Logged in to github.com"
- [ ] `ssh -T git@github.com` → shows "successfully authenticated"

### Automated Steps
- [ ] Create project directory structure
- [ ] Create `data/local_data/` parent directory
- [ ] Initialize git at project root
- [ ] Create GitHub repo: `ai_martech_{TIER}_{COMPANY}`
- [ ] Clone shared repos (if not already present)
- [ ] Create symlinks: `scripts/global_scripts`, `scripts/update_scripts`, `scripts/nsql`
- [ ] Copy and customize app_config.yaml
- [ ] Create .env.template
- [ ] Configure .gitignore
- [ ] Initial commit and push to GitHub

### Manual Steps (User Must Do)
- [ ] **⚠️ Set up rawdata folder manually** (see Step 1.5):
  - Option A: Create symlink to existing Dropbox folder
  - Option B: Move Dropbox folder via Finder
  - Option C: Create new Dropbox shared folder
  - Option D: Create empty folder for API-only
- [ ] **Verify `RAW_DATA_DIR` in app_config.yaml is correct**
- [ ] **Verify rawdata is accessible**: `ls data/local_data/rawdata_{COMPANY}`

### Verification Steps
- [ ] Test database connections
- [ ] Test script resolution (verify company variant)
- [ ] If needed, create ___COMPANY.R variant scripts
- [ ] Execute full ETL pipeline test
- [ ] Deploy to Posit Connect

---

## Quick Reference: Script Resolution Logic

```r
# Per MP122: Config → Scripts → Framework (unidirectional)

resolve_script_path <- function(base_script, company = NULL) {
  if (!is.null(company)) {
    variant <- gsub("\\.R$", paste0("___", company, ".R"), base_script)
    full_path <- file.path("scripts/update_scripts", variant)
    if (file.exists(full_path)) return(full_path)  # Use variant
  }
  return(file.path("scripts/update_scripts", base_script))  # Fallback to generic
}
```

---

## Related Principles

- **MP122**: Quad-Track Shared Symlink Architecture
- **SO_P016**: Configuration Scope Hierarchy
- **DM_R037**: Company-Specific ETL Naming (`___COMPANY` suffix)
- **DM_R038**: Company Suffix Scope Limitation

---

*Last updated: 2025-12-24*
