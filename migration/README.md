# Migration Module

The `migration/` module provides a unified utility for exporting and importing Monte Carlo data observability configurations between environments. It supports migrating **domains**, **data products**, **blocklists**, **tags**, **exclusion windows**, **audiences**, and **monitors**.

## Overview

This module is designed to facilitate environment migrations (e.g. dev → prod or US workspace → EU workspace) by allowing you to:

- **Export** configurations from a source MC environment to CSV/YAML files
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
| `audience_migrator.py` | Handles notification audiences—recipients and channels for alerts (email, Slack, Teams, etc.). |
| `monitor_migrator.py` | Handles monitors—observability rules (metric, custom SQL, table monitors) exported to MaC YAML format. |

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

## Cross-Environment Warehouse Mapping

When migrating **tags** or **monitors** between different environments (e.g., dev → prod), warehouse names typically differ. The migration module provides two ways to map source warehouses to destination warehouses.

### Option 1: CLI Argument

Provide mappings directly in the command:

```bash
python migration/workspace_migrator.py import --entities tags,monitors \
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
python migration/workspace_migrator.py import --entities tags,monitors --profile target_env --force yes
```

### Mapping Priority

1. CLI `--warehouse_map` argument (highest priority)
2. `warehouse_mapping.json` file in input directory
3. No fallback—unmapped warehouses are skipped with a warning

**Note:** The warehouse mapping template is shared across entity types. Both tag and monitor exports will merge their warehouse references into the same template file.

## Monitor Migration

Monitors are exported to **MaC (Monitors as Code) YAML format**, enabling version control and code review workflows.

### Namespace Organization

Imported monitors are organized under a **namespace** (default: `migration`). Namespaces provide:

- **Tracking**: Easily identify which monitors were migrated vs. manually created
- **Isolation**: Monitors from different namespaces don't conflict
- **Rollback**: Delete all monitors in a namespace with a single command

Within a namespace, monitor names must be unique—the system uses `(account, namespace, name)` as the unique identifier.

### Delete by Namespace

To roll back a migration or clean up test imports:

```bash
# Preview what would be deleted (dry-run)
montecarlo --profile target_env monitors delete --namespace migration --dry-run

# Actually delete
montecarlo --profile target_env monitors delete --namespace migration
```

### Convert to UI

Monitors imported via MaC (Monitors as Code) YAML are deployed as "code-managed" and **cannot be edited in the Monte Carlo UI**. Since the migration exports UI monitors (originally created in the UI), users typically expect to continue editing them in the UI after migration.

The `convert-to-ui` functionality converts code-managed monitors to UI-editable monitors.

#### Option 1: Auto-convert during import (Recommended)

Use the `--convert-to-ui` flag to automatically convert monitors after import:

```bash
# Dry-run (preview import + conversion)
python migration/workspace_migrator.py import --profile target_env \
  --entities monitors --convert-to-ui

# Commit (import + convert)
python migration/workspace_migrator.py import --profile target_env \
  --entities monitors --convert-to-ui --force yes
```

#### Option 2: Convert after import

If you prefer to review imported monitors before making them UI-editable:

```bash
# First, import monitors (they will be code-managed)
python migration/workspace_migrator.py import --profile target_env \
  --entities monitors --force yes

# Then, convert to UI (dry-run first)
python migration/workspace_migrator.py convert-to-ui --profile target_env \
  --namespace migration

# Commit conversion
python migration/workspace_migrator.py convert-to-ui --profile target_env \
  --namespace migration --force yes
```

**Note:** After conversion, monitors move from the custom namespace (e.g., `migration`) to the `ui` namespace. This means `delete_by_namespace('migration')` will no longer find them—they are now standard UI monitors.

### Monitor YAML Format

Exported monitors follow the MaC format:

```yaml
montecarlo:
  metric:
    - name: "Daily Row Count"
      warehouse: "Prod Snowflake"
      # ... metric configuration
  custom_sql:
    - name: "Data Quality Check"
      sql: "SELECT COUNT(*) FROM ..."
      # ... custom SQL configuration
  table:
    - name: "Freshness Monitor"
      asset_selection:
        # ... table selection
```

## Audience Migration

Audiences define notification recipients and channels. Each audience can have multiple notification settings (email, Slack, Teams, PagerDuty, etc.).

### CSV Format

```csv
audience_name,notification_type,recipients,recipients_display_names,integration_id
Data Team,EMAIL,user1@example.com;user2@example.com,User One;User Two,
Data Team,SLACK,#data-alerts,,slack-integration-uuid
```

- **recipients**: Semicolon-separated list of recipients (emails, channel names, etc.)
- **recipients_display_names**: Semicolon-separated display names (optional)
- **integration_id**: UUID of the integration for Slack/Teams/PagerDuty (empty for email)

### Import Behavior

- Audiences that already exist in the target environment are **skipped** (not updated)
- Only new audiences are created
- This prevents accidental overwrites of audiences that may have been customized in the target environment

## Supported Entities

| Entity | Status | Format | Columns/Structure |
|--------|--------|--------|-------------------|
| `blocklists` | ✅ Implemented | CSV | `resource_id`, `target_object_type`, `match_type`, `dataset`, `project`, `effect` |
| `domains` | ✅ Implemented | CSV | `domain_name`, `domain_description`, `asset_mcon` |
| `tags` | ✅ Implemented | CSV | `warehouse_id`, `warehouse_name`, `full_table_id`, `asset_type`, `tag_key`, `tag_value` |
| `exclusion_windows` | ✅ Implemented | CSV | `id`, `resource_uuid`, `scope`, `database`, `dataset`, `full_table_id`, `start_time`, `end_time`, `reason`, `reason_type` |
| `data_products` | ✅ Implemented | CSV | `data_product_name`, `data_product_description`, `asset_mcon` |
| `audiences` | ✅ Implemented | CSV | `audience_name`, `notification_type`, `recipients`, `recipients_display_names`, `integration_id` |
| `monitors` | ✅ Implemented | YAML | MaC format with `montecarlo.metric`, `montecarlo.custom_sql`, `montecarlo.table` sections |

Entities are imported in dependency order: blocklists → domains → tags → exclusion_windows → data_products → audiences → monitors.

**Note:** Audiences are imported before monitors because monitors may reference notification audiences.

## Output Files

Exports are saved to `migration/migration-data-exports/` by default (or a custom directory):

```
migration/migration-data-exports/
├── blocklists.csv
├── domains.csv
├── tags.csv
├── exclusion_windows.csv
├── data_products.csv
├── audiences.csv
├── monitors.yaml                     # MaC-formatted monitor definitions
├── warehouse_mapping_template.json   # Template for tag/monitor warehouse mappings
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
- **`lib/helpers/warehouse_mapping.py`**: Warehouse mapping utilities for cross-environment tag and monitor migrations

### Admin Scripts (`admin/`) and Monitors Scripts (`monitors/`)

Migrators delegate to bulk scripts for core operations:

| Migrator | Uses |
|----------|------|
| `BlocklistMigrator` | `admin/bulk_blocklist_exporter.py`, `admin/bulk_blocklist_importer.py` |
| `DomainMigrator` | `admin/bulk_domain_exporter.py`, `admin/bulk_domain_importerv2.py` |
| `TagMigrator` | `admin/bulk_tag_exporterv2.py`, `admin/bulk_tag_importerv2.py` |
| `ExclusionWindowMigrator` | `admin/bulk_exclusion_window_exporter.py`, `admin/bulk_exclusion_window_importer.py` |
| `DataProductMigrator` | `admin/bulk_data_product_exporter.py`, `admin/bulk_data_product_importer.py` |
| `AudienceMigrator` | `admin/bulk_audience_exporter.py`, `admin/bulk_audience_importer.py` |
| `MonitorMigrator` | `monitors/bulk_export_monitors_v2.py`, `monitors/bulk_import_monitors.py` |

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

