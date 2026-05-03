# Platform APIs Architecture

This directory contains standardized API implementations for all e-commerce platforms and data sources following MP094 (Platform API Architecture).

## Directory Structure

```
26_platform_apis/
├── common/         # Shared utilities for all platforms
├── cbz/           # Cyberbiz API
├── eby/           # eBay API  
├── amz/           # Amazon API
├── web/           # Official website database
└── csv/           # CSV import handlers
```

## Platform IDs (MP092)

| Platform ID | Platform | Data Source | Status |
|------------|----------|-------------|---------|
| cbz | Cyberbiz | API + CSV | Active |
| eby | eBay | API | Active |
| amz | Amazon | CSV Export | Active |
| web | Official Website | Database | Active |
| csv | Generic CSV | File Import | Active |

## Usage Pattern

### Basic API Connection

```r
# Load platform API
source("scripts/global_scripts/26_platform_apis/cbz/fn_cbz_connect.R")
source("scripts/global_scripts/26_platform_apis/cbz/fn_cbz_get_orders.R")

# Connect to platform
connection <- fn_cbz_connect()

# Fetch data
orders <- fn_cbz_get_orders(connection)

# Disconnect
fn_cbz_disconnect(connection)
```

### Unified Platform Access

```r
source("scripts/global_scripts/26_platform_apis/common/fn_platform_get_data.R")

# Get data from any platform using platform_id
customers_cbz <- fn_platform_get_data("cbz", "customers")
orders_eby <- fn_platform_get_data("eby", "orders")
```

## Configuration

Each platform has an `api_config.yaml` file defining:

- API endpoints and base URLs
- Authentication methods
- Rate limiting settings
- Data field mappings
- Error handling strategies

Example configuration:

```yaml
platform:
  platform_id: "cbz"
  name: "Cyberbiz"
  
api:
  base_url: "https://api.cyberbiz.com/v1"
  auth_type: "bearer_token"
  
authentication:
  bearer_token:
    env_var: "CYBERBIZ_API_TOKEN"
```

## Environment Variables

Required environment variables for each platform:

- `CYBERBIZ_API_TOKEN` - Cyberbiz API authentication
- `EBAY_API_KEY` - eBay API key
- `EBAY_API_SECRET` - eBay API secret
- `AMAZON_MWS_KEY` - Amazon MWS key (if applicable)

## Common Utilities

The `common/` directory provides shared functions:

- `fn_api_rate_limiter.R` - Rate limiting for API calls
- `fn_api_error_handler.R` - Standardized error handling
- `fn_api_authenticator.R` - Authentication helpers
- `fn_api_cache_manager.R` - Response caching
- `fn_platform_get_data.R` - Unified data access

## Testing

Each platform includes tests in its `tests/` subdirectory:

```bash
# Run all platform API tests
Rscript -e "testthat::test_dir('scripts/global_scripts/26_platform_apis')"

# Test specific platform
Rscript -e "testthat::test_dir('scripts/global_scripts/26_platform_apis/cbz/tests')"
```

## Migration Status

| Platform | Migration Date | From | Status |
|----------|---------------|------|---------|
| Cyberbiz | 2025-08-27 | archive/MAMBA/cyberbiz_api | ⏳ In Progress |
| eBay | Pending | archive/MAMBA/*ebay*.R | 📋 Planned |
| Amazon | Pending | CSV imports | 📋 Planned |

## Adding New Platforms

1. Create platform directory: `mkdir -p {platform_id}/tests`
2. Create `api_config.yaml` from template
3. Implement required functions:
   - `fn_{platform_id}_connect.R`
   - `fn_{platform_id}_disconnect.R`
   - `fn_{platform_id}_authenticate.R`
   - `fn_{platform_id}_get_{object}.R`
4. Add tests in `tests/` directory
5. Update `api_registry.yaml`
6. Document in this README

## References

- [MP094: Platform API Architecture](../00_principles/docs/en/part1_principles/CH00_fundamental_principles/04_data_management/MP094_platform_api_architecture.qmd)
- [MP092: Platform ID Standard](../00_principles/docs/en/part1_principles/CH00_fundamental_principles/04_data_management/MP092_platform_id_standard.qmd)
- [MG01: Platform API Migration Guide](../00_principles/docs/en/part2_implementations/CH14_migration_guides/MG01_platform_api_migration.qmd)

## Support

For issues or questions about platform APIs:
1. Check the migration guide (MG01)
2. Review MP094 for architecture principles
3. Consult test files for usage examples
