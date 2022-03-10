# Monte Carlo Python SDK Examples

These examples use [Pycarlo](https://github.com/monte-carlo-data/python-sdk), Monte Carlo's Python SDK.

## Example list

Very brief descriptions of the examples and the link to the main example/project file are provided here. For more information on an example, look at the source file.

### Insights Reporting
| Example Topic | Discussion |
| ------------- | ---------- |
| [Import Insights to BigQuery](bigquery_insights_importer.py) | Script to import Monte Carlo's insight reports directly to BigQuery. |

### Tagging Assets
| Example Topic | Discussion |
| ------------- | ---------- |
| [Key Asset Tagger](key_asset_tagger.py) | A script that will take all Key Asset Importance Scores and add them as a tag. |

### Lineage
| Example Topic | Discussion |
| ------------- | ---------- |
| [Add External Lineage](lineage.py) | An exmaple class and execution to make adding an external lineage node and a set of edges to that new node. |
| [Reports in Database Schema](reports_by_schema.py) | A script that will provide a list of all Looker reports used by tables in a specific schema. |
| [Get All Tables Upstream from a Report](tables_upstream_from_report.py) | Outputs a csv of any DWH/DL tables upstream from an inputted BI Report. |
| [Get Recent Incidents Upstream from a Report](incidents_upstream_from_report.py) | Prints a boolean (if there is any incidents) if there are upstream incidents and a list of affected upstream tables from an inputted BI Report. |
