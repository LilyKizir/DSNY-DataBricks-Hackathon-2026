# DSNY Databricks Hackathon 2026: The Data Masons

New York electricity data (NYISO, via the EIA API) joined with hourly weather, modelled on Databricks as a medallion lakehouse (landing → bronze → silver → gold) and analysed in Tableau and a Databricks Genie space.

```
EIA API v2 (NYISO) ──► scripts/extract_eia_data.py ──► the_data_masons.landing.raw_*
IEM ASOS weather   ──► scripts/extract_weather_data.py ──► the_data_masons.bronze.bronze_weather_data
                                                              │
                              the-data-masons-pipeline (Lakeflow Declarative Pipelines, models/**)
                                                              │
                                   bronze ──► silver (facts + dims) ──► gold
                                                                          │
                                                   Tableau workbooks · Genie space

new gold model: sandbox/ ──► your sandbox pipeline ──► PR ──► merge to main
                                                               └► GitHub Action ──► the-data-masons-pipeline ──► gold
```
## Repository layout

| Path                       | What's in it                                                                                                             |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `models/1_bronze/`         | Materialized views that parse the raw EIA JSON from `landing`                                                            |
| `models/2_silver/`         | Cleaned, typed facts and dimensions                                                                                      |
| `models/3_gold/`           | Shared gold models (reviewed and merged via PR)                                                                          |
| `sandbox/`                 | Personal gold experiments, gitignored. Built by your own sandbox pipeline (`databricks.yml`)                             |
| `scripts/`                 | Ingestion scripts, Databricks connection helper, Tableau datasource swap                                                 |
| `database_config/`         | Notebook that creates the catalog's schemas and volumes. **It drops them first**, so only run it to rebuild from scratch |
| `sample_Tableau_workbook/` | Template Tableau workbook with the Databricks connection                                                                 |
| `gold_tableau_workbooks/`  | Workbooks generated from the template, one per gold model                                                                |
| `docs/`                    | How-tos, starting with [adding a gold model](docs/adding_a_gold_model.md)                                                |
| `databricks.yml`           | Asset bundle for the personal gold sandbox pipeline                                                                      |
| `.github/workflows/`       | `databricks_cicd.yml`: publishes gold on merge to `main` (pulls the workspace Git folder, runs the shared pipeline)       |
| `.claude/skills/gold-model/` | The `/gold-model` Claude Code skill (see [With Claude Code](#adding-a-gold-model))                                     |

## Data

All tables live in the Unity Catalog catalog **`the_data_masons`** on workspace `https://dbc-34432859-d369.cloud.databricks.com`.

| Schema    | Tables                                                                                                                                                                                                                                                                                                     |
| --------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `landing` | `raw_regional_operating_metrics`, `raw_generation_energy_source`, `raw_subregional_demand`: raw EIA responses (`raw_response` as VARIANT)                                                                                                                                                                  |
| `bronze`  | `bronze_NYISO_operation_metrics`, `bronze_NYISO_generation_energy_source`, `bronze_NYISO_subregional_demand`, `bronze_weather_data`                                                                                                                                                                        |
| `silver`  | Facts: `fact_NYISO_operation_metrics`, `fact_NYISO_generation_energy_source`, `fact_NYISO_subregional_demand`, `fact_weather_data`<br>Dims: `dim_nyiso_zone`, `dim_weather_station`, `dim_county`, `dim_balancing_authority`, `dim_subregion`, `dim_combined_zone`, `dim_metric_type`, `dim_energy_source` |
| `gold`    | `gold_daily_weather_by_nyiso_zone`, `gold_avg_temperature_jan_2026`                                                                                                                                                                                                                                        |

**Sources**

- **Electricity:** [EIA API v2](https://www.eia.gov/opendata/) RTO endpoints for NYISO: hourly region data, generation by fuel type, and demand by sub-region (NYISO zones A–K).
- **Weather:** hourly ASOS station observations from the Iowa Environmental Mesonet, mapped to NYISO zones through counties.

### Data coverage

| Data | Loaded in silver |
|---|---|
| Weather | 2025-01-01 to 2026-09-25, 43 stations, 10 zones (no stations in zone H, Millwood) |
| Demand by zone | Mostly **2026-07-01 to 2026-09-19**: 78 complete days per zone, plus a few partial days. Earlier hours are missing |

### Time and keys

- **Time zones:** timestamps are stored in UTC. Gold models convert to New York local time (`from_utc_timestamp(..., 'America/New_York')`) before bucketing into days or months, because NYISO reports on Eastern time.
- **Hour-ending periods:** EIA's hourly `period` marks the *end* of the hour, so gold models step back one hour before taking the local day.
- **Zone keys:** weather and the demand fact use the zone letter (`A`–`K`), but `dim_subregion` uses EIA codes (`ZONA`–`ZONK`). Join on `RIGHT(sub_ba_code, 1)`.

## Getting started

### Prerequisites

- Access to the Databricks workspace above (ask the team to be added)
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html), logged in:
  ```bash
  databricks auth login --host https://dbc-34432859-d369.cloud.databricks.com
  ```
- Python 3.12 (for the local scripts)
- Optional: [GitHub CLI](https://cli.github.com/) (`gh auth login`), Tableau Desktop

### Local Python environment

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows  (macOS/Linux: source .venv/bin/activate)
pip install -r requirements.txt
```

Create a `.env` file in the repo root (it's gitignored):

```ini
DATABRICKS_SERVER_HOST=dbc-34432859-d369.cloud.databricks.com
DATABRICKS_TOKEN=<personal access token>
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/401941d60f2786b9
DATABRICKS_CLUSTER_ID=                 # optional; serverless is used when empty
WEATHER_API_KEY=<key>
ZONE_COUNTY_URL=<zone-to-county mapping file URL>
IEM_STATIONS_URL=<IEM station list URL>
IEM_ASOS_URL=<IEM ASOS download URL>
```

Test the connection with `python -m scripts.connect_databricks`.

## Ingestion

| Script                            | Runs                                                                               | Writes                                                                                                                                         |
| --------------------------------- | ---------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `scripts/extract_eia_data.py`     | In Databricks (uses `dbutils.secrets`, scope `the-data-masons`, key `eia_api_key`) | `landing.raw_*`                                                                                                                                |
| `scripts/extract_weather_data.py` | Locally via Databricks Connect (`python -m scripts.extract_weather_data`)          | `bronze.bronze_weather_data` (Delta MERGE). The first run backfills from 2025-01-01, later runs pick up from each station's latest observation |

## The shared pipeline

**`the-data-masons-pipeline`** (Lakeflow Declarative Pipelines, serverless) builds everything in `models/**` from the team's workspace Git folder on `main`. Each file sets its target with `USE SCHEMA bronze|silver|gold;` and defines one `CREATE OR REFRESH MATERIALIZED VIEW`.

**Publishing is automatic.** When a merge to `main` changes `models/3_gold/**`, the GitHub Action [`databricks_cicd.yml`](.github/workflows/databricks_cicd.yml) pulls the workspace Git folder and starts the pipeline. To run it by hand (for example after a bronze/silver change, or if the Action fails):

```bash
databricks repos update 2228305554904359 --branch main -p <profile>
databricks pipelines start-update 3f756ff0-30a3-4417-9e2c-97ee920ed727 -p <profile>
```

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

`sample_Tableau_workbook/sample_workbook.twb` connects to the SQL warehouse `401941d60f2786b9` with OAuth, so each person signs in as themselves. To make a copy that points at another gold table, keeping the same connection:

```bash
python scripts/swap_tableau_datasource.py \
  --workbook sample_Tableau_workbook/sample_workbook.twb \
  --table the_data_masons.gold.<model name> \
  -p <profile>
```

This writes `gold_tableau_workbooks/<model name>.twb`. If the script lists missing fields, fix them in Tableau with **Replace References**. Only `.twb` workbooks are supported; save a `.twbx` as `.twb` first.

## Genie

The Genie space **NYISO Energy and Weather Analysis** (id `01f1bab4affc184b90b7087c3cbcd61c`) answers questions in plain English over the silver tables. It's also available to Claude as an MCP server. Treat its SQL as a draft: Genie tends to cut months on UTC and weight zones by station count. For the weather–demand question, it joined the demand fact to `dim_subregion` on mismatched codes (`A` against `ZONA`) and returned 0 rows. The gold models are the reference numbers.

## Known issues

- **Silver doesn't match the repo:** in the live `fact_NYISO_subregional_demand`, `period_timestamp` is still a STRING, although `models/2_silver/` casts it to a timestamp. Gold models use `CAST(period_timestamp AS TIMESTAMP)`, which works either way.
- **Zone codes differ:** the demand fact has zone letters, but `dim_subregion` has `ZONA`–`ZONK` (see [Time and keys](#time-and-keys)).
- **Missing data:** zone H (Millwood) has no weather stations, and demand before July 2026 isn't loaded yet.
- **Manual reruns for bronze/silver:** the GitHub Action only runs for `models/3_gold/**` changes, so run the shared pipeline by hand after bronze or silver changes.

## License

[MIT](LICENSE)
