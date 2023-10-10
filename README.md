# Monte Carlo Python SDK Examples

These examples use [Pycarlo](https://github.com/monte-carlo-data/python-sdk), Monte Carlo's Python SDK.

## Example list

Very brief descriptions of the examples and the link to the main example/project file are provided here. For more information on an example, look at the source file.

### Insights Reporting
| Example Topic | Discussion |
| ------------- | ---------- |
| [Import Insights to BigQuery](insights/bigquery_insights_importer.py) | Script to import Monte Carlo's insight reports directly to BigQuery. |
| [Import Insights to Databricks](insights/extract_mc_insights_dbx.py) | Script to import Monte Carlo's insight reports directly to Databricks. |

### Tagging Assets
| Example Topic | Discussion |
| ------------- | ---------- |
| [Key Asset Tagger](admin/key_asset_tagger.py) | A script that will take all Key Asset Importance Scores and add them as a tag. |

### Lineage
| Example Topic | Discussion |
| ------------- | ---------- |
| [Add External Lineage](lineage/lineage.py) | An exmaple class and execution to make adding an external lineage node and a set of edges to that new node. |
| [Reports in Database Schema](lineage/reports_by_schema.py) | A script that will provide a list of all Looker reports used by tables in a specific schema. |
| [Get All Tables Upstream from a Report](lineage/tables_upstream_from_report.py) | Outputs a csv of any DWH/DL tables upstream from an inputted BI Report. |
| [Get Recent Incidents Upstream from a Report](lineage/incidents_upstream_from_report.py) | Prints a boolean (if there is any incidents) if there are upstream incidents and a list of affected upstream tables from an inputted BI Report. |
| [Get Downstream Assets from an Asset](lineage/incidents_upstream_from_report.py) | Fetches all downstream assets from a specified asset to a csv. |
| [Get Lineage Graph for All Tables](lineage/lineage_graph_retrieval.py) | Fetches all tables and edges, saves to csv files. |