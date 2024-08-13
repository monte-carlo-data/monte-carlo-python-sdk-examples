<div id="top"></div>
<a href=""></a>

# Monte Carlo Python SDK Examples

These examples use [Pycarlo](https://github.com/monte-carlo-data/python-sdk), Monte Carlo's Python SDK and the Monte Carlo [CLI](https://pypi.org/project/montecarlodata/).

<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li><a href="#utility-setup">Utility Setup</a></li>
    <li><a href="#quick-start">Quick Start</a></li>
    <li><a href="#standalone-scripts">Standalone Scripts</a></li>
  </ol>
</details>

## Utility Setup
Some of the scripts in this repository may be called from the main utility runner or as standalone scripts. 
1. Navigate to a desired directory where the repository will reside
2. Clone or download the git repository
   ```bash
   git clone https://github.com/monte-carlo-data/monte-carlo-python-sdk-examples.git
   ```
3. You can choose from an existing or new virtual environment or use the base python installation as the interpreter. 
In either case, make sure to use python3.12 as the base interpreter
4. Install all python modules:
   ```bash
   python3.12 -m pip install -r requirements.txt
   ```
<p align="right">(<a href="#top">back to top</a>)</p>

## Quick Start

Use the ```--help/-h``` flag for details on the commands/utilities available.

```bash
python3.12 mcdsdksamplerunner.py -h
```

If the Monte Carlo CLI has not been configured before, running any utility will prompt for Monte Carlo credentials to 
generate new tokens. This only applies for accounts not using SSO. 

### Example:

```bash
(venv) python3.12 mcsdksamplerunner.py  monitors bulk-set-freshness-sensitivity -p demo -i /Users/hjarrin/Downloads/freshness_thresholds_auto.csv -w aaaa7777-7777-a7a7-a7a7a-aaaa7777

                    
            ███╗   ███╗ ██████╗ ███╗   ██╗████████╗███████╗     ██████╗ █████╗ ██████╗ ██╗      ██████╗ 
            ████╗ ████║██╔═══██╗████╗  ██║╚══██╔══╝██╔════╝    ██╔════╝██╔══██╗██╔══██╗██║     ██╔═══██╗
            ██╔████╔██║██║   ██║██╔██╗ ██║   ██║   █████╗      ██║     ███████║██████╔╝██║     ██║   ██║
            ██║╚██╔╝██║██║   ██║██║╚██╗██║   ██║   ██╔══╝      ██║     ██╔══██║██╔══██╗██║     ██║   ██║
            ██║ ╚═╝ ██║╚██████╔╝██║ ╚████║   ██║   ███████╗    ╚██████╗██║  ██║██║  ██║███████╗╚██████╔╝
            ╚═╝     ╚═╝ ╚═════╝ ╚═╝  ╚═══╝   ╚═╝   ╚══════╝     ╚═════╝╚═╝  ╚═╝╚═╝  ╚═╝╚══════╝ ╚═════╝ 
                                                                                                         
        
2024-08-13 16:15:28 INFO - running utility using 'demo' profile
2024-08-13 16:15:28 INFO - checking montecarlo cli version...
2024-08-13 16:15:28 INFO - montecarlo cli present
2024-08-13 16:15:28 INFO - validating montecarlo cli connection...
2024-08-13 16:15:29 ERROR - unable to validate token
2024-08-13 16:15:29 INFO - creating new token
MC Username: testuser@testdomain.com
MC Password: 
2024-08-13 16:15:46 INFO - token stored successfully
2024-08-13 16:15:46 INFO - starting input file validation...
2024-08-13 16:15:46 INFO - updating freshness rules...
2024-08-13 16:15:48 INFO - freshness threshold updated successfully for table hxe:dev_schema.offer
2024-08-13 16:15:49 INFO - freshness threshold updated successfully for table hxe:dev_schema.subscription
2024-08-13 16:15:49 INFO - freshness threshold updated successfully for table hxe:dev_schema.zuora_invoice
[COMPLETE] ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100% 0:00:00
```

**Note:** If your account is using SSO, generate the token manually from the UI and store them in ```~/.mcd/profiles.ini
``` or run the ```montecarlo configure``` command by following the onscreen prompts.

<p align="right">(<a href="#top">back to top</a>)</p>

## Standalone Scripts - In Progress

Very brief descriptions of standalone examples and the link to the main example/project file are provided here. For more information on an example, look at the source file.

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
| Example Topic | Discussion                                                                                                                                      |
| ------------- |-------------------------------------------------------------------------------------------------------------------------------------------------|
| [Add External Lineage](lineage/lineage.py) | An example class and execution to make adding an external lineage node and a set of edges to that new node.                                    |
| [Reports in Database Schema](lineage/reports_by_schema.py) | A script that will provide a list of all Looker reports used by tables in a specific schema.                                                    |
| [Get All Tables Upstream from a Report](lineage/tables_upstream_from_report.py) | Outputs a csv of any DWH/DL tables upstream from an inputted BI Report.                                                                         |
| [Get Recent Incidents Upstream from a Report](lineage/incidents_upstream_from_report.py) | Prints a boolean (if there is any incidents) if there are upstream incidents and a list of affected upstream tables from an inputted BI Report. |
| [Get Downstream Assets from an Asset](lineage/incidents_upstream_from_report.py) | Fetches all downstream assets from a specified asset to a csv.                                                                                  |
| [Get Lineage Graph for All Tables](lineage/lineage_graph_retrieval.py) | Fetches all tables and edges, saves to csv files.                                                                                               |