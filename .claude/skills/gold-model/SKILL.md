---
name: gold-model
description: Create a gold materialized view from a natural-language request (via Genie) and build it in the user's personal sandbox on Databricks; promote it to the shared gold layer through a reviewed PR (a GitHub Action publishes it on merge); point a Tableau workbook at a gold model.
disable-model-invocation: true
argument-hint: <what the gold model should show> | promote <gold_model_name> | tableau <gold_model_name> [--sandbox] [<workbook.twb>]
---

# /gold-model

Arguments: **$ARGUMENTS**

Pick the mode from the first word:

- `promote <gold_model_name>`: go to **Promote**.
- `tableau <gold_model_name>`: go to **Tableau**.

- anything else is a request for a new model: go to **Create in sandbox**.

The user typing this command is their go-ahead for everything in the chosen mode, including deploying and running pipelines. Don't stop to ask unless a step fails or the request is ambiguous.

## Step 0 (every mode): resolve the Databricks profile

1. If the environment variable `DATABRICKS_CONFIG_PROFILE` is set (`echo "$DATABRICKS_CONFIG_PROFILE"`), use it.
2. Otherwise run `databricks auth profiles -o json` and keep the profiles whose `host` is `https://dbc-34432859-d369.cloud.databricks.com` and whose `valid` is `true`.
   - Exactly one: use it.
   - Several: ask the user which one (AskUserQuestion), and mention they can skip the question next time with `setx DATABRICKS_CONFIG_PROFILE <name>` on Windows (or `export` in their shell profile).
   - None: stop. If a matching profile exists but is invalid, tell them to run `! databricks auth login -p <that profile>`; otherwise `! databricks auth login --host https://dbc-34432859-d369.cloud.databricks.com`.
3. Confirm it with `databricks current-user me -p <profile> -o json`, and say in one line which user and profile you're running as.

In every command below, `"$PROFILE"` stands for that profile name. Substitute it literally, because shell variables don't persist between commands. Always pass `-p`: a user's default profile may point at another workspace.

## Fixed facts

| Thing                                 | Value                                                                                                                                                                                                                                                                                                                                                                                               |
| ------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Workspace                             | `https://dbc-34432859-d369.cloud.databricks.com` (profile resolved in step 0)                                                                                                                                                                                                                                                                                                                       |
| Sandbox bundle                        | `databricks.yml` at the repo root: target `sandbox`, pipeline key `gold_sandbox`, builds `sandbox/**` into `the_data_masons.dev_<user short name>_gold_sandbox` (dev mode adds the `dev_<user>_` prefix)                                                                                                                                                                                            |
| Shared pipeline                       | `the-data-masons-pipeline`, id `3f756ff0-30a3-4417-9e2c-97ee920ed727`, builds `models/**` from Lily's workspace Git folder                                                                                                                                                                                                                                                                          |
| Publishing                            | GitHub Action `.github/workflows/databricks_cicd.yml`: on every push to `main` that touches `models/3_gold/**` (i.e. a merged promotion PR), it pulls the workspace Git folder and starts the shared pipeline                                                                                                                                                                                       |
| Workspace Git folder                  | repo id `2228305554904359`, branch `main`                                                                                                                                                                                                                                                                                                                                                           |
| SQL warehouse (validation / previews) | `401941d60f2786b9` (Serverless Starter Warehouse)                                                                                                                                                                                                                                                                                                                                                   |
| GitHub repo                           | `LilyKizir/DSNY-DataBricks-Hackathon-2026`, base branch `main`                                                                                                                                                                                                                                                                                                                                      |
| Tableau template workbook             | `sample_Tableau_workbook/sample_workbook.twb` (Databricks connection to this workspace and warehouse, OAuth)                                                                                                                                                                                                                                                                                        |
| Genie space                           | NYISO Energy and Weather Analysis, space id `01f1bab4affc184b90b7087c3cbcd61c`. Find its tools with ToolSearch `query_space_01f1bab4affc184b90b7087c3cbcd61c` (the `mcp__<server>__` prefix depends on what each person named the MCP server). If nothing is found, tell the user to add the Genie MCP server for this space and skip Genie: write the SQL directly from the silver tables instead. |

Run SQL on the warehouse by writing the request body to a JSON file (build it with Python's `json.dumps` so the SQL is escaped correctly), then:

```bash
MSYS_NO_PATHCONV=1 databricks api post /api/2.0/sql/statements -p "$PROFILE" --json @<path to request.json>
# request.json: {"warehouse_id":"401941d60f2786b9","statement":"<sql>","wait_timeout":"50s"}
```

If the state is `PENDING`/`RUNNING`, poll `MSYS_NO_PATHCONV=1 databricks api get /api/2.0/sql/statements/<statement_id> -p "$PROFILE"`.
On Windows Git Bash, `MSYS_NO_PATHCONV=1` is required. Without it, `/api/...` is rewritten to a Windows path and the call fails with `Not Found`. Pass the JSON file as a Windows path (`cygpath -w`).

---

## Create in sandbox

Only the Databricks CLI is needed; no GitHub.

### 1. Ask Genie

Query Genie with the request plus a hint to use the `the_data_masons.silver.*` tables and show the SQL. Poll until `COMPLETED`. Keep Genie's SQL and headline answer.

### 2. Write `sandbox/<name>.sql`

Read the files in `models/3_gold/` and match their style, with these sandbox rules:

- **No `USE SCHEMA` line.** The sandbox pipeline's default schema is the user's sandbox schema. Start with `CREATE OR REFRESH MATERIALIZED VIEW <name>( CONSTRAINT ... ) COMMENT "..." AS ...`.
- Name: `gold_<snake_case_subject>`. It must not collide with a file in `sandbox/` or `models/3_gold/`, or a table in `the_data_masons.gold`.
- **Read straight from silver**, fully qualified (`the_data_masons.silver.fact_weather_data`, `the_data_masons.silver.dim_nyiso_zone`, …), so each model's lineage goes directly to silver. Don't read from other gold tables (`the_data_masons.gold.*`), even when one already has the numbers.
- CTEs with a one-line comment on non-obvious logic, `ROUND(..., 2)` on measures, and a `current_timestamp() AS gold_processed_timestamp` audit column.
- Weather: copy the averaging logic of `models/3_gold/gold_daily_weather_by_nyiso_zone.sql` into the model's own CTEs rather than referencing that table. That means NY local time (`from_utc_timestamp(observed_at_utc, 'America/New_York')`) before bucketing into hours, days or months, then averaging station-hour → zone-hour → zone-day (→ zone-month), and never averaging raw station rows across zones or statewide.
- Treat Genie's SQL as a draft. Fix UTC month boundaries, double counting from stations mapped to several zones, and mixed-grain `UNION ... 'OVERALL'` rows. Note each fix for the report.

### 3. Validate

Run `SELECT * FROM (<body after the top-level AS, no trailing ;>) LIMIT 20` on the warehouse. On `FAILED`, fix and retry (max 3 attempts, then stop and report). Zero rows counts as a failure; check the filters.

### 4. Deploy and run the sandbox

```bash
databricks bundle deploy -t sandbox -p "$PROFILE"
databricks bundle run gold_sandbox -t sandbox -p "$PROFILE" --refresh <name>
```

`databricks.yml` must keep `sync.include: [sandbox/**]` (sandbox files are gitignored and otherwise never uploaded, so the run fails with "Unable access root path …/files/sandbox") and `sync.exclude: [sandbox/.gitkeep]` (pipelines reject non `.sql`/`.py` files with UNSUPPORTED_LIBRARY_FILE_TYPE, and pipeline globs can't filter by extension). Only put `.sql` files in `sandbox/`. If the deploy prints a "recreate"/destructive-action plan, stop and ask the user; never add `--auto-approve` yourself.
`bundle run` waits for the update; run it with `run_in_background` and wait for the notification. On failure, get the errors with `databricks pipelines list-pipeline-events <sandbox pipeline id> -p "$PROFILE" --filter "level='ERROR'" -o json` (find the id with `databricks bundle summary -t sandbox -p ...`). Report the root cause and a fix; don't loop.

### 5. Report

Give the full table name (`the_data_masons.dev_<user>_gold_sandbox.<name>`), a preview of about 10 rows, Genie's headline answer, and any fixes to Genie's SQL. End with: _"Happy with it? Run `/gold-model promote <name>` to send it for review. To look at it in Tableau first, run `/gold-model tableau <name> --sandbox`."_

---

## Promote `<name>`

Opens a PR that adds the model to the shared gold layer. It does **not** merge: a technical reviewer merges, and the GitHub Action then publishes it to `the_data_masons.gold`.

1. Preflight: `sandbox/<name>.sql` exists, `gh auth status` succeeds (if not, tell the user to run `winget install GitHub.cli` then `! gh auth login`), and `models/3_gold/<name>.sql` doesn't already exist on `origin/main`.
2. If the model references any other sandbox table by an unqualified name, stop and say which models must be promoted together.
3. `git fetch origin main`, then work in a throwaway worktree so the user's branch and working tree aren't touched:
   ```bash
   git worktree add "$SCRATCH/w" -b gold/<name> origin/main
   ```
   (`$SCRATCH` = the session scratchpad directory. Keep the folder name short: on Windows the scratchpad path is already long, and a longer worktree path fails with `'$GIT_DIR' too big`. If that happens, run `git worktree prune` and `git branch -D gold/<name>` before retrying.) Write `models/3_gold/<name>.sql` there as the sandbox file with `USE SCHEMA gold;` plus a blank line prepended.
4. Commit, push and open the PR:

   ```bash
   git add models/3_gold/<name>.sql
   git commit -m "Add <name> gold model" -m "<one-line purpose>" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
   git push -u origin gold/<name>
   gh pr create --base main --head gold/<name> --title "Add <name> gold model" --body "<purpose, original request, fixes vs Genie SQL, sample rows from the sandbox table, sandbox table name>

   Merging publishes it: the Deploy and Run Databricks ETL action pulls the workspace Git folder and runs the-data-masons-pipeline.

   🤖 Generated with [Claude Code](https://claude.com/claude-code)"
   ```

5. Clean up: `git worktree remove "$SCRATCH/w"` and `git branch -D gold/<name>`.
6. Report the PR link, and what happens after the merge: the GitHub Action publishes the model to `the_data_masons.gold.<name>`; then `/gold-model tableau <name>` points a workbook at it. Keep `sandbox/<name>.sql` until the table is in gold; after that it can be deleted. Removing a definition doesn't drop its table: the next pipeline run only marks it inactive (`pipelines.metastore.inactive = true`, data still queryable), so also `DROP MATERIALIZED VIEW the_data_masons.dev_<user>_gold_sandbox.<name>`. The same applies to removing a model from shared gold: after the removal PR merges and the Action runs, the `gold` table must be dropped explicitly.

---

## Tableau `<name>` [`--sandbox`] [`<workbook.twb>`]

Makes a copy of a Tableau workbook with its datasource pointed at the gold model. The Databricks connection (server, warehouse, OAuth, catalog) stays exactly as it is; only the table and its column metadata change.

1. Table: `the_data_masons.gold.<name>`. If it isn't there yet (the script says it can't read the table), the PR isn't merged or the GitHub Action hasn't finished: check `gh run list --repo LilyKizir/DSNY-DataBricks-Hackathon-2026 --workflow databricks_cicd.yml --limit 3`. If that run failed, show its log (`gh run view <id> --log-failed`) and the shared pipeline's errors (`databricks pipelines list-pipeline-events 3f756ff0-30a3-4417-9e2c-97ee920ed727 -p "$PROFILE" --filter "level='ERROR'" -o json`), with the root cause. With `--sandbox`, use the user's sandbox schema instead (`databricks bundle summary -t sandbox -p "$PROFILE" -o json` shows it; it looks like `dev_<user>_gold_sandbox`). This lets a model be checked in Tableau before its PR is merged.
2. Workbook: the `.twb` given in the arguments, otherwise the template `sample_Tableau_workbook/sample_workbook.twb`. A `.twbx` isn't supported; ask the user to save it as `.twb` in Tableau (File > Save As).
3. Run:
   ```bash
   python scripts/swap_tableau_datasource.py --workbook <workbook.twb> --table <table> -p "$PROFILE"
   ```
   It writes `gold_tableau_workbooks/<name>.twb` and never overwrites it. If that output already exists, ask before rerunning with `--force`. If the workbook has several Databricks datasources, the script says so; ask which one and pass `--datasource <part of its caption>`.
4. Report the output path and anything the script listed as missing fields. Those are fields used on sheets or in calculations that the new table doesn't have; they show red in Tableau and are fixed with **Replace References** (right-click the field). Remind the user that the workbook signs each person in with their own Databricks account (OAuth) when opened.
