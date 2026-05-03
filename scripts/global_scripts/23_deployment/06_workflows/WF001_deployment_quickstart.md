# 🔄 WF001: Deployment Quickstart

## 🎯 Purpose
Quick deployment process for positioning app using automated scripts with path detection.

## 📋 Prerequisites
- App config ready (`app_config.yaml` configured)
- Environment variables set (database and API keys)
- Git repository accessible

## 🚀 Steps

### Method 1: Interactive Deployment (Recommended)
```bash
# Execute in project directory
Rscript deploy_now.R

# Or execute from anywhere (auto-detects project location)
Rscript /path/to/positioning_app/deploy_now.R
```

### Method 2: Automated Deployment
```bash
# Execute in project directory  
Rscript deploy_auto.R

# Or execute from anywhere (auto-detects project location)
Rscript /path/to/positioning_app/deploy_auto.R
```

### Deployment Process Flow
Both scripts will:
1. ✅ Update app.R to latest version
2. ✅ Update manifest.json
3. ✅ Check critical files
4. ✅ Display Git status
5. 📋 Provide Git auto-deploy setup notes (first-time only)

### Posit Connect Cloud Deployment (First-time setup)
After initial Publish, Git pushes trigger deployments automatically; no manual republish needed.
1. Login to https://connect.posit.cloud
2. Click Publish → Shiny
3. Fill in:
   - Repository: `kiki830621/ai_martech`
   - Application Path: `l1_basic/positioning_app`
   - Primary File: `app.R`
   - Branch: `main`
4. Confirm Git auto-deploy is enabled in Connect (label may vary)

## ✅ Verification
- [ ] Scripts execute without errors
- [ ] Git status is clean
- [ ] All required files present
- [ ] Application deploys successfully
- [ ] Application loads in browser

## 🔗 Related Workflows
- **Alternative**: WF002 for complete deployment with full verification
- **Next step**: Test deployed application functionality

## 📚 Additional Resources
- Complete tools: `scripts/global_scripts/23_deployment/`
- Full documentation: `23_deployment/05_docs/COMPLETE_DEPLOYMENT_GUIDE.md`
- Cloud deployment guide: `23_deployment/05_docs/POSIT_CONNECT_CLOUD_GITHUB_DEPLOYMENT.md` 
