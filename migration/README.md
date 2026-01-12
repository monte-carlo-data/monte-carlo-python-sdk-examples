# Migration Module

The `migration/` module provides a unified utility for exporting and importing Monte Carlo data observability configurations between environments. It supports migrating **domains**, **data products**, **blocklists**, **tags**, and **exclusion windows**.

## Overview

This module is designed to facilitate environment migrations (e.g. dev → prod or US workspace → EU workspace) by allowing you to:

- **Export** configurations from a source MC environment to CSV files
- **Validate** migration files before importing
- **Import** configurations to a target MC environment (with dry-run support)


### Key Components

| File | Description |
|------|-------------|
| `workspace_migrator.py` | Main entry point. Orchestrates export/import/validate operations across all entity types. |
| `base_migrator.py` | Abstract base class defining the interface (`export()`, `import_data()`, `validate()`) that all migrators implement. |
| `blocklist_migrator.py` | Handles blocklist entries (ingestion rules) that control which tables/datasets/projects are monitored. |
| `domain_migrator.py` | Handles domains—logical groupings of tables for asset organization. |
| `tag_migrator.py` | Handles object tags (properties)—key-value pairs attached to tables for organization. |
| `exclusion_window_migrator.py` | Handles exclusion windows (data maintenance entries)—time periods when anomaly detection is suppressed. |
| `data_product_migrator.py` | Handles data products—business-facing data assets for stakeholder monitoring. |

## Usage

### Export

Export all configurations from a source environment:

```bash
python migration/workspace_migrator.py export --profile source_env
```

Export specific entities:

```bash
python migration/workspace_migrator.py export --profile source_env --entities domains,blocklists
```

Export to a custom directory:

```bash
python migration/workspace_migrator.py export --profile source_env --output_dir ./my-exports
```

### Validate

Validate migration files before importing:

```bash
python migration/workspace_migrator.py validate --profile target_env
```

### Import

Import with dry-run (preview changes without deploying):

```bash
python migration/workspace_migrator.py import --profile target_env
```

Import with force (deploy changes):

```bash
python migration/workspace_migrator.py import --profile target_env --force yes
```

Import from a custom directory:

```bash
python migration/workspace_migrator.py import --profile target_env --input_dir ./my-exports --force yes
```

## Cross-Environment Tag Migrations

When migrating tags between different environments (e.g., dev → prod), warehouse names typically differ. The migration module provides two ways to map source warehouses to destination warehouses:

### Option 1: CLI Argument

Provide mappings directly in the command:

```bash
python migration/workspace_migrator.py import --entities tags \
  --profile target_env \
  --warehouse_map "Dev Snowflake=Prod Snowflake,Dev BigQuery=Prod BigQuery" \
  --force yes
```

### Option 2: Configuration File

1. **Export** generates a template file (`warehouse_mapping_template.json`):

```json
{
  "_instructions": "Replace <ENTER_DESTINATION_WAREHOUSE> with the actual warehouse name...",
  "warehouse_mapping": {
    "Dev Snowflake": "<ENTER_DESTINATION_WAREHOUSE>",
    "Dev BigQuery": "<ENTER_DESTINATION_WAREHOUSE>"
  }
}
```

2. **Copy and edit** the template to create `warehouse_mapping.json`:

```json
{
  "warehouse_mapping": {
    "Dev Snowflake": "Prod Snowflake",
    "Dev BigQuery": "Prod BigQuery"
  }
}
```

3. **Import** will automatically use the mapping file:

```bash
python migration/workspace_migrator.py import --entities tags --profile target_env --force yes
```

### Mapping Priority

1. CLI `--warehouse_map` argument (highest priority)
2. `warehouse_mapping.json` file in input directory
3. No fallback—unmapped warehouses are skipped with a warning

## Supported Entities

| Entity | Status | CSV Columns |
|--------|--------|-------------|
| `blocklists` | ✅ Implemented | `resource_id`, `target_object_type`, `match_type`, `dataset`, `project`, `effect` |
| `domains` | ✅ Implemented | `domain_name`, `domain_description`, `asset_mcon` |
| `tags` | ✅ Implemented | `warehouse_id`, `warehouse_name`, `full_table_id`, `asset_type`, `tag_key`, `tag_value` |
| `exclusion_windows` | ✅ Implemented | `id`, `resource_uuid`, `scope`, `database`, `dataset`, `full_table_id`, `start_time`, `end_time`, `reason`, `reason_type` |
| `data_products` | ✅ Implemented | `data_product_name`, `data_product_description`, `asset_mcon` |

Entities are imported in dependency order: blocklists → domains → tags → exclusion_windows → data_products.

## Output Files

Exports are saved to `migration/migration-data-exports/` by default (or a custom directory):

```
migration/migration-data-exports/
├── blocklists.csv
├── domains.csv
├── tags.csv
├── exclusion_windows.csv
├── data_products.csv
├── warehouse_mapping_template.json   # Template for tag warehouse mappings
└── migration_manifest.json           # Export metadata
```

The manifest file tracks export metadata:

```json
{
  "export_timestamp": "2025-12-15T16:02:39.226266",
  "source_profile": "default",
  "entities": {
    "blocklists": { "file": "blocklists.csv", "count": 25 },
    "domains": { "file": "domains.csv", "count": 3 },
    "data_products": { "file": "data_products.csv", "count": 5 }
  }
}
```

## Integration with Codebase

The migration module integrates with several parts of the codebase:

### Configuration (`configs/configs.ini`)

Uses profile-based configuration for MC API credentials. Profiles are specified via `--profile`:

```ini
[default]
mcd_id = your_key_id
mcd_token = your_token

[target_env]
mcd_id = target_key_id
mcd_token = target_token
```

### Library Utilities (`lib/`)

- **`lib/util.py`**: Base `Util` class provides MC API client initialization, authentication, and common queries (domains, data products, etc.)
- **`lib/auth/mc_auth.py`**: Handles Monte Carlo authentication
- **`lib/helpers/logs.py`**: Provides `LOGGER` and logging configuration
- **`lib/helpers/sdk_helpers.py`**: Argument parser generation utilities
- **`lib/helpers/warehouse_mapping.py`**: Warehouse mapping utilities for cross-environment tag migrations

### Admin Scripts (`admin/`)

Migrators delegate to admin bulk scripts for core operations:

| Migrator | Uses |
|----------|------|
| `BlocklistMigrator` | `bulk_blocklist_exporter.py`, `bulk_blocklist_importer.py` |
| `DomainMigrator` | `bulk_domain_exporter.py`, `bulk_domain_importerv2.py` |
| `TagMigrator` | `bulk_tag_exporterv2.py`, `bulk_tag_importerv2.py` |
| `ExclusionWindowMigrator` | `bulk_exclusion_window_exporter.py`, `bulk_exclusion_window_importer.py` |
| `DataProductMigrator` | `bulk_data_product_exporter.py`, `bulk_data_product_importer.py` |

### Logging (`logs/`)

All operations are logged to `logs/workspace_migrator-YYYY-MM-DD.log` with detailed output for debugging.

## Extending the Module

To add a new entity migrator:

1. Create a new file (e.g., `my_entity_migrator.py`)
2. Inherit from `BaseMigrator`
3. Implement required properties: `entity_name`, `output_filename`
4. Implement required methods: `export()`, `import_data()`, `validate()`
5. Register in `WorkspaceMigrator.migrators` property
6. Add to `AVAILABLE_ENTITIES` and `IMPLEMENTED_ENTITIES` lists

```python
from migration.base_migrator import BaseMigrator

class MyEntityMigrator(BaseMigrator):
    @property
    def entity_name(self) -> str:
        return "my_entity"

    @property
    def output_filename(self) -> str:
        return "my_entity.csv"

    def export(self, output_file: str = None) -> dict:
        # Implementation
        pass

    def import_data(self, input_file: str = None, dry_run: bool = True) -> dict:
        # Implementation
        pass

    def validate(self, input_file: str = None) -> dict:
        # Implementation
        pass
```

