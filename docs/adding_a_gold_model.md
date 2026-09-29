# Adding a gold model with Genie Code

This is how anyone on the team, technical or not, adds a new gold table without risking the shared pipeline. You do everything in the Databricks workspace; you don't need to install anything.

**In short:** build and check it in your **own Git folder and sandbox schema**, then send it for review with a **pull request**. After merge, the shared pipeline builds it into `the_data_masons.gold`.

```
your Git folder (branch)  ──►  your sandbox pipeline  ──►  the_data_masons.gold_sandbox_<you>   (try it)
        │
        └── PR on GitHub ──► review + merge to main ──► CI pulls Lily's Git folder
                                                          └► the-data-masons-pipeline ──► the_data_masons.gold   (shared)
```

## Rules

1. **Never edit Lily's Git folder** (`/Workspace/Users/lily.kiziriya@…/DSNY-DataBricks-Hackathon-2026`). It's the source of the shared pipeline, so anything saved there goes into the next shared run.
2. **Never commit to `main`.** Always use your own branch and a PR.
3. **Sandbox models live in `sandbox/`, and shared models live in `models/3_gold/`.** Only the move from one to the other goes into a PR.

## One-time setup

1. **Create your own Git folder.** In **Workspace → your home folder → Create → Git folder**, paste `https://github.com/LilyKizir/DSNY-DataBricks-Hackathon-2026` and connect your GitHub account if asked.
2. **Create your sandbox schema.** In a SQL editor, run:
   ```sql
   CREATE SCHEMA IF NOT EXISTS the_data_masons.gold_sandbox_<yourname>;
   ```
3. **Add the team rules for Genie Code.** Paste the block under [Instructions for Genie Code](#instructions-for-genie-code) into your assistant instructions, so Genie Code follows our conventions. (A workspace admin can add them once for everyone instead.)

## Adding a model

### 1. Start a branch
In your Git folder, open the **Git** dialog, create a branch named `gold/<short-name>` (for example `gold/avg-weather-feb-2026`), and switch to it.

### 2. Ask Genie Code to write the model
Open Genie Code in your Git folder and ask in plain words, for example:

> Create a gold model in `sandbox/gold_avg_weather_feb_2026.sql` with the average temperature and wind speed per NYISO zone for February 2026. Follow the team gold-model instructions.

Check that the file:
- is in `sandbox/` and has **no** `USE SCHEMA` line
- starts with `CREATE OR REFRESH MATERIALIZED VIEW gold_...`
- reads only from fully qualified `the_data_masons.silver.*` tables, not from other gold tables

### 3. Check the numbers
Ask Genie Code to "run the SELECT part of this model and show me the results", or copy the part after `AS` into a SQL editor. Look for:
- one row per zone (or whatever grain you asked for), with **no `OVERALL` row mixed in**
- the expected number of days (e.g. 28 for February)
- values that look sensible

### 4. Build it in your sandbox
Create your personal pipeline **once**:
- **New → ETL pipeline** (Lakeflow Declarative Pipelines)
- **Name:** `gold_sandbox_<yourname>`
- **Source code:** your Git folder's `sandbox/` folder only. Never `models/`, because that would try to rebuild tables the shared pipeline owns.
- **Default catalog / schema:** `the_data_masons` / `gold_sandbox_<yourname>`
- **Compute:** serverless

After that, press **Run** whenever you change a sandbox model. Your table shows up at `the_data_masons.gold_sandbox_<yourname>.<model name>`.

### 5. Send it for review
When you're happy with the result:
1. Move the file from `sandbox/` to `models/3_gold/` (same file name).
2. Add this as the **first line**:
   ```sql
   USE SCHEMA gold;
   ```
3. In the **Git** dialog, commit only that file (message: `Add <model name> gold model`) and **push**.
4. Open the PR on GitHub (the Git dialog links to it) against `main`. In the description, say what the table answers and paste a few sample rows.

### 6. After review
A reviewer merges the PR. The GitHub Action (**Deploy and Run Databricks ETL**) then pulls Lily's Git folder and runs `the-data-masons-pipeline`, and the table appears in `the_data_masons.gold`. You can follow it in the repo's **Actions** tab. If it fails, a technical teammate pulls Lily's Git folder and runs the pipeline by hand.

You can then delete your sandbox copy. Rerunning your sandbox pipeline only marks the sandbox table *inactive* (the data stays), so drop it too: `DROP MATERIALIZED VIEW the_data_masons.<your sandbox schema>.<model name>;`

**Removing a gold model** works the same way: open a PR that deletes `models/3_gold/<model name>.sql`. After it's merged and the Action has run, the table in `gold` is only marked inactive, so drop it with `DROP MATERIALIZED VIEW the_data_masons.gold.<model name>;`.

### 7. Open it in Tableau
`scripts/swap_tableau_datasource.py` makes a copy of a workbook that points at your new table. The Databricks connection stays exactly as it is; only the table and its columns change. Run it on your computer (Python and the Databricks CLI, logged in to the workspace):

```bash
python scripts/swap_tableau_datasource.py \
  --workbook sample_Tableau_workbook/sample_workbook.twb \
  --table the_data_masons.gold.<model name> \
  -p <your Databricks CLI profile>
```

This writes `gold_tableau_workbooks/<model name>.twb`. To look at a model before its PR is merged, use your sandbox table instead (`the_data_masons.<your sandbox schema>.<model name>`). If the script lists missing fields, those are fields used on sheets that the new table doesn't have. In Tableau, right-click each one and choose **Replace References**. With Claude Code, `/gold-model tableau <model name>` does all of this once the PR is merged and the table is in gold.

## Instructions for Genie Code

```text
When writing gold models for the DSNY hackathon repo:
- Write new models to sandbox/<name>.sql as Lakeflow Declarative Pipelines SQL:
  CREATE OR REFRESH MATERIALIZED VIEW <name>( CONSTRAINT ... EXPECT (...) ON VIOLATION DROP ROW ) COMMENT "..." AS ...
- Do not add a USE SCHEMA line in sandbox/ files.
- Name models gold_<snake_case_subject>. Check the name isn't already used in sandbox/, models/3_gold/ or the_data_masons.gold.
- Read straight from silver with fully qualified names (the_data_masons.silver.fact_weather_data, the_data_masons.silver.dim_nyiso_zone). Never read from the_data_masons.gold.* tables.
- Weather: copy the logic of models/3_gold/gold_daily_weather_by_nyiso_zone.sql into the model's own CTEs.
  Convert with from_utc_timestamp(observed_at_utc, 'America/New_York') before bucketing into hours, days or months. Never cut months on UTC.
  Average in steps: station-hour -> zone-hour -> zone-day (-> zone-month).
- Some stations map to more than one NYISO zone: never average raw station rows statewide, average the zone values instead.
- Keep one grain per table: put statewide/overall figures in columns (e.g. AVG(x) OVER ()), never as a UNION'd 'OVERALL' row.
- Use CTEs with a short comment on non-obvious logic, ROUND(..., 2) on measures, and end with current_timestamp() AS gold_processed_timestamp.
- Never edit files in models/ unless asked to promote a model, and never edit Lily's Git folder.
```

## Troubleshooting

| Problem | Likely cause and fix |
|---|---|
| Shared pipeline: *table is managed by another pipeline* | Two pipelines are trying to own the same table. Usually a personal pipeline included `models/` or wrote to schema `gold`. Point it at `sandbox/` and your sandbox schema only, and ask a technical teammate to remove the other pipeline. |
| Sandbox pipeline writes to `gold` | The file still has `USE SCHEMA gold;`. Remove it while the file is in `sandbox/`. |
| Pipeline error: *file does not have .py or .sql suffix* | Only `.sql` or `.py` files may be in the pipeline's source folder. |
| Numbers differ slightly from Genie's answer | Genie often cuts months on UTC and weights zones by station count. The model's New York local, zone-weighted numbers are the ones we use. |
