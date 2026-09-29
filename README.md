# DSNY Databricks Hackathon 2026: The Data Masons

## Purpose of this project

Getting from a business question to a trusted dashboard usually takes a lot of back and forth between the person asking and a data engineer. We built an ETL pipeline on Databricks with an AI-assisted way to create gold models. A user asks a question in plain English, such as _"How does the weather impact electricity usage?"_:

1. A Databricks Genie agent drafts the SQL.
2. Claude Code tests the draft and corrects it.
3. The model is built in the user's own sandbox so they can check it.
4. A data engineer reviews the pull request and merges it.
5. A GitHub Action publishes the table to gold.
6. A Tableau workbook is created, ready for visualization.

Every AI-generated model is validated twice, because AI-generated SQL can look right and still be wrong. In our test, Genie's first draft returned no rows because of mismatched zone codes.

## Repository layout

| Path                         | What's in it                                                                                                         |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `models/1_bronze/`           | Materialized views that parse the raw EIA JSON from `landing` and the weather data from api                          |
| `models/2_silver/`           | Cleaned facts and dim models                                                                              |
| `models/3_gold/`             | Shared gold models (reviewed and merged via PR)                                                                      |
| `sandbox/`                   | Personal gold experiments, gitignored. Built by your own sandbox pipeline (`databricks.yml`)                         |
| `scripts/`                   | Ingestion scripts, Databricks connection helper, Tableau datasource swap                                             |
| `database_config/`           | Notebook that creates the catalog's schemas and volumes. It drops them first, so only run it to rebuild from scratch |
| `sample_Tableau_workbook/`   | Template Tableau workbook with the Databricks connection                                                             |
| `gold_tableau_workbooks/`    | Workbooks generated from the template, one per gold model                                                            |
| `docs/`                      | How-tos, starting with [adding a gold model](docs/adding_a_gold_model.md)                                            |
| `databricks.yml`             | Asset bundle for the personal gold sandbox pipeline                                                                  |
| `.github/workflows/`         | `databricks_cicd.yml`: publishes gold on merge to `main` (pulls the workspace Git folder, runs the shared pipeline)  |
| `.claude/skills/gold-model/` | The `/gold-model` Claude Code skill (see [With Claude Code](#adding-a-gold-model))                                   |

## Data

All tables live in the Unity Catalog catalog **`the_data_masons`** on workspace `${DATABRICKS_HOST}`.

| Schema    | Tables                                                                                                                                                                                                                                                                                                     |
| --------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `landing` | `raw_regional_operating_metrics`, `raw_generation_energy_source`, `raw_subregional_demand`: raw EIA responses (`raw_response` as VARIANT)                                                                                                                                                                  |
| `bronze`  | `bronze_NYISO_operation_metrics`, `bronze_NYISO_generation_energy_source`, `bronze_NYISO_subregional_demand`, `bronze_weather_data`                                                                                                                                                                        |
| `silver`  | Facts: `fact_NYISO_operation_metrics`, `fact_NYISO_generation_energy_source`, `fact_NYISO_subregional_demand`, `fact_weather_data`<br>Dims: `dim_nyiso_zone`, `dim_weather_station`, `dim_county`, `dim_balancing_authority`, `dim_subregion`, `dim_combined_zone`, `dim_metric_type`, `dim_energy_source` |
| `gold` | this schema is where AI generated and stored the final gold model |

**Sources**

- **Electricity:** [EIA API v2](https://www.eia.gov/opendata/) RTO endpoints for NYISO: hourly region data, generation by fuel type, and demand by sub-region (NYISO zones A–K).
- **Weather:** hourly ASOS station observations from the Iowa Environmental Mesonet, mapped to NYISO zones through counties.

## Getting started

### Prerequisites

- Access to the Databricks workspace
- Install and Log into Claude Code
- In Databricks, go to your user icon → **Settings → Developer → Access tokens → Generate new token**.
- **Add the server** from the repo folder:
   ```bash
   claude mcp add --transport http genie \
     https://<workspace-host>/api/2.0/mcp/genie/<genie-space-id> \
     --header "Authorization: Bearer <your-token>"
- Check it. Run claude mcp list, or /mcp inside Claude Code: genie should show as connected.
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html), logged in:
  ```bash
  databricks auth login --host ${DATABRICKS_HOST}
  ```
- Python 3.12 (for the local scripts)
- Optional: [GitHub CLI](https://cli.github.com/) (`gh auth login`)
- Tableau Desktop with installed ODBC Databricks driver

### Local Python environment

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows  (for macOS/Linux: source .venv/bin/activate)
pip install -r requirements.txt
```

Create a `.env` file in the repo root (it's gitignored). Workspace-specific values are kept here, not in the repo. Elsewhere in this README, `${NAME}` means "the value of `NAME` from your `.env`". Ask the team for the values.

```ini
# Workspace
DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
DATABRICKS_SERVER_HOST=<workspace>.cloud.databricks.com
DATABRICKS_TOKEN=<personal access token>
DATABRICKS_WAREHOUSE_ID=<SQL warehouse id>
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<SQL warehouse id>
DATABRICKS_CLUSTER_ID=                 # optional; serverless is used when empty
DATABRICKS_USERNAME=<your workspace login>
GOLD_PIPELINE_ID=<the-data-masons-pipeline id>
WORKSPACE_GIT_FOLDER_ID=<workspace Git folder (repo) id>
GENIE_SPACE_ID=<Genie space id>

# Sources
WEATHER_API_KEY=<key>
ZONE_COUNTY_URL = "https://www.weather.gov/source/gis/Shapefiles/County/bp16ap26.dbx"
IEM_STATIONS_URL = "https://mesonet.agron.iastate.edu/geojson/network/NY_ASOS.geojson"
IEM_ASOS_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"
```

Test the connection with `python -m scripts.connect_databricks`.

## Ingestion

| Script                            | Runs                                                                               |
| --------------------------------- | ---------------------------------------------------------------------------------- |
| `scripts/extract_eia_data.py`     | In Databricks (uses `dbutils.secrets`, scope `the-data-masons`, key `eia_api_key`) |
| `scripts/extract_weather_data.py` | Locally via Databricks Connect (`python -m scripts.extract_weather_data`)          |

## The shared pipeline

**`the-data-masons-pipeline`** builds everything in `models/**` from the team's workspace Git folder on `main`. Each file sets its target with `USE SCHEMA bronze|silver|gold;` and defines one `CREATE OR REFRESH MATERIALIZED VIEW`.

**Publishing is automatic.** When a merge to `main` changes `models/3_gold/**`, the GitHub Action [`databricks_cicd.yml`](.github/workflows/databricks_cicd.yml) pulls the workspace Git folder and starts the pipeline.

## Adding a gold model

New gold tables are tried out in a **personal sandbox** first, then promoted by **pull request**. Nobody edits the shared pipeline's source directly.

1. **Write** `sandbox/gold_<subject>.sql`. Don't include a `USE SCHEMA` line, and read only from fully qualified `the_data_masons.silver.*` tables.
2. **Build it in your sandbox.** Your own pipeline writes to `the_data_masons.dev_<you>_gold_sandbox`:
   ```bash
   databricks bundle deploy -t sandbox -p <profile>
   databricks bundle run gold_sandbox -t sandbox -p <profile> --refresh gold_<subject>
   ```
   `bundle deploy` creates the sandbox pipeline and schema if they don't exist, or re-creates them if they were deleted.
3. **Promote.** Copy the file to `models/3_gold/`, add `USE SCHEMA gold;` as the first line, and open a PR against `main`.
4. **Publish.** Merging the PR publishes it: the GitHub Action runs the shared pipeline and the table appears in `the_data_masons.gold`.

Modelling rules:

- Use one grain per table. Put statewide figures in columns, not in `OVERALL` rows.
- Round measures with `ROUND(..., 2)`, and end every model with `current_timestamp() AS gold_processed_timestamp`.
- For weather, average in steps (station-hour → zone-hour → zone-day), the same way as `models/3_gold/gold_daily_weather_by_nyiso_zone.sql`.
- The EIA feed has gaps, so count the hours behind every daily total and flag complete days (23 or more hours, which allows for the spring daylight-saving day).

The full walkthrough, including a no-install route with Genie Code in the workspace, is in [docs/adding_a_gold_model.md](docs/adding_a_gold_model.md).

**With Claude Code:** the `/gold-model` skill (`.claude/skills/gold-model/`) runs the whole flow:

```
/gold-model <what the model should show>     # Genie drafts it → sandbox build
/gold-model tableau <name> --sandbox         # optional: see it in Tableau before the PR
/gold-model promote <name>                   # opens the PR (merging it publishes via GitHub Actions)
/gold-model tableau <name>                   # after merge: workbook pointed at the published table
```

## Tableau

`sample_Tableau_workbook/sample_workbook.twb` connects to the SQL warehouse `${DATABRICKS_WAREHOUSE_ID}` with OAuth, so each person signs in as themselves. To make a copy that points at another gold table, keeping the same connection:

```bash
python scripts/swap_tableau_datasource.py \
  --workbook sample_Tableau_workbook/sample_workbook.twb \
  --table the_data_masons.gold.<model name> \
  -p <profile>
```

This writes `gold_tableau_workbooks/<model name>.twb`. If the script lists missing fields, fix them in Tableau with **Replace References**. Only `.twb` workbooks are supported; save a `.twbx` as `.twb` first.

## License

[MIT](LICENSE)
