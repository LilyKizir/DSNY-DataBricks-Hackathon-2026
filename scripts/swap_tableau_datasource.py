"""Point a Tableau workbook (.twb) at a different gold table, keeping its Databricks connection.

Only the datasource's table is swapped: the named connection (server, warehouse, OAuth,
catalog) is left byte for byte as it is. Column metadata is rebuilt from the new table's
Unity Catalog schema, so the fields pane matches the new table as soon as the workbook opens.

Usage:
    python scripts/swap_tableau_datasource.py \
        --workbook sample_Tableau_workbook/sample_workbook.twb \
        --table the_data_masons.gold.gold_daily_weather_by_nyiso_zone \
        [--profile <databricks profile>] [--output <path>.twb | --in-place] [--force]
"""
import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import uuid

# Generated workbooks go here, at the repo root, whatever the current directory is
DEFAULT_OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "gold_tableau_workbooks")

# Unity Catalog type_name ->(Tableau local-type, remote-type code, default aggregation)
TYPE_MAP = {
    "STRING": ("string", 129, "Count"),
    "CHAR": ("string", 129, "Count"),
    "VARCHAR": ("string", 129, "Count"),
    "DOUBLE": ("real", 5, "Sum"),
    "FLOAT": ("real", 4, "Sum"),
    "DECIMAL": ("real", 131, "Sum"),
    "LONG": ("integer", 20, "Sum"),
    "INT": ("integer", 3, "Sum"),
    "SHORT": ("integer", 2, "Sum"),
    "BYTE": ("integer", 16, "Sum"),
    "BOOLEAN": ("boolean", 11, "Count"),
    "DATE": ("date", 7, "Year"),
    "TIMESTAMP": ("datetime", 7, "Year"),
    "TIMESTAMP_NTZ": ("datetime", 7, "Year"),
}

# Tableau's default role/type per local-type for the datasource <column> entries
ROLE_MAP = {
    "string": ("dimension", "nominal"),
    "boolean": ("dimension", "nominal"),
    "date": ("dimension", "ordinal"),
    "datetime": ("dimension", "ordinal"),
    "real": ("measure", "quantitative"),
    "integer": ("measure", "quantitative"),
}

RELATION_RE = re.compile(
    r"<relation connection='(?P<conn>[^']+)' name='(?P<name>[^']+)' "
    r"table='\[(?P<catalog>[^\]]+)\]\.\[(?P<schema>[^\]]+)\]\.\[(?P<table>[^\]]+)\]' type='table' />"
)


def xml_attr(value):
    return (value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            .replace("'", "&apos;").replace('"', "&quot;"))


def caption_for(column_name):
    # Same default Tableau uses: snake_case -> Title Case
    return column_name.replace("_", " ").title()


def fetch_columns(table, profile, columns_file):
    if columns_file:
        with open(columns_file, encoding="utf-8") as f:
            info = json.load(f)
    else:
        cmd = ["databricks", "tables", "get", table, "-o", "json"]
        if profile:
            cmd += ["-p", profile]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            sys.exit(f"Could not read {table} from Unity Catalog:\n{result.stderr.strip()}")
        info = json.loads(result.stdout)

    columns = sorted(info.get("columns", []), key=lambda c: c.get("position", 0))
    if not columns:
        sys.exit(f"{table} has no columns (does it exist yet? Gold tables appear only after the shared pipeline runs).")

    usable = []
    for c in columns:
        type_name = (c.get("type_name") or "").upper()
        if type_name not in TYPE_MAP:
            print(f"  skipping column {c['name']}: type {type_name} isn't supported by Tableau")
            continue
        usable.append((c["name"], type_name, c.get("nullable", True)))
    return usable


def build_metadata_records(columns, parent, object_id):
    records = []
    for ordinal, (name, type_name, nullable) in enumerate(columns, start=1):
        local_type, remote_type, aggregation = TYPE_MAP[type_name]
        lines = [
            "          <metadata-record class='column'>",
            f"            <remote-name>{name}</remote-name>",
            f"            <remote-type>{remote_type}</remote-type>",
            f"            <local-name>[{name}]</local-name>",
            f"            <parent-name>[{parent}]</parent-name>",
            f"            <remote-alias>{name}</remote-alias>",
            f"            <ordinal>{ordinal}</ordinal>",
            f"            <local-type>{local_type}</local-type>",
            f"            <aggregation>{aggregation}</aggregation>",
        ]
        if local_type == "string":
            lines.append("            <width>255</width>")
        lines.append(f"            <contains-null>{'true' if nullable else 'false'}</contains-null>")
        if local_type == "string":
            lines.append("            <collation flag='0' name='binary' />")
        lines += [
            f"            <object-id>[{xml_attr(object_id)}]</object-id>",
            "          </metadata-record>",
        ]
        records.append("\n".join(lines))
    return "<metadata-records>\n" + "\n".join(records) + "\n        </metadata-records>"


def build_field_columns(columns):
    lines = []
    for name, type_name, _ in sorted(columns, key=lambda c: c[0]):
        local_type = TYPE_MAP[type_name][0]
        role, kind = ROLE_MAP[local_type]
        lines.append(
            f"      <column caption='{xml_attr(caption_for(name))}' datatype='{local_type}' "
            f"name='[{name}]' role='{role}' type='{kind}' />"
        )
    return lines


def find_datasource(text, wanted):
    """Return (start, end) of the top-level <datasource> block to swap."""
    top_start = text.index("<datasources>")
    top_end = text.index("</datasources>", top_start)
    candidates = []
    for m in re.finditer(r"<datasource\b[^>]*?(?<!/)>.*?</datasource>", text[top_start:top_end], re.S):
        block = m.group(0)
        relations = {r.group("table") for r in RELATION_RE.finditer(block)}
        if not relations:
            continue
        if wanted and wanted not in block[:block.index(">")]:
            continue
        candidates.append((top_start + m.start(), top_start + m.end(), relations))

    if not candidates:
        sys.exit("No Databricks table datasource found" + (f" matching '{wanted}'." if wanted else "."))
    if len(candidates) > 1:
        sys.exit("The workbook has several table datasources; pick one with --datasource <part of its caption or name>.")
    start, end, relations = candidates[0]
    if len(relations) > 1:
        sys.exit(f"The datasource joins several tables ({', '.join(sorted(relations))}); only single-table datasources are swapped.")
    return start, end


def swap(text, datasource, catalog, schema, new_table, columns):
    start, end = find_datasource(text, datasource)
    block = text[start:end]

    relation = RELATION_RE.search(block)
    old_table = relation.group("table")
    old_catalog, old_schema = relation.group("catalog"), relation.group("schema")
    old_caption = re.match(r"<datasource\b[^>]*?caption='([^']*)'", block)
    old_caption = old_caption.group(1) if old_caption else None
    old_object = re.search(r"<object caption='[^']*' id='([^']+)'>", block)
    old_object_id = old_object.group(1) if old_object else None

    fq_new = f"{catalog}.{schema}.{new_table}"
    new_object_id = f"{new_table} ({fq_new})_{uuid.uuid4().hex.upper()}"
    old_field_names = set(re.findall(r"<local-name>\[([^\]]+)\]</local-name>", block))

    # 1. Relations (connection attribute is kept, so the same named connection is reused)
    block = RELATION_RE.sub(
        lambda m: f"<relation connection='{m.group('conn')}' name='{xml_attr(new_table)}' "
                  f"table='[{catalog}].[{schema}].[{new_table}]' type='table' />",
        block,
    )

    # 2. Column metadata rebuilt from the new table
    block = re.sub(r"<metadata-records>.*?</metadata-records>",
                   lambda _: build_metadata_records(columns, new_table, new_object_id), block, count=1, flags=re.S)

    # 3. Object-model ids and captions
    if old_object_id:
        block = block.replace(old_object_id, xml_attr(new_object_id))
    block = block.replace(f"<object caption='{old_table}'", f"<object caption='{xml_attr(new_table)}'")
    block = block.replace(f"<column caption='{old_table}' datatype='table'", f"<column caption='{xml_attr(new_table)}' datatype='table'")

    # 4. Plain field columns: drop the old table's, add the new table's. A field the new table
    #    also has keeps its entry if someone renamed it. Calculated fields have a child
    #    <calculation> element, so the self-closing pattern never matches them.
    new_names = {name for name, _, _ in columns}
    kept = set()

    def drop_old_field(m):
        caption, name = m.group(1), m.group(2)
        if name not in old_field_names:
            return m.group(0)
        if name in new_names and caption != xml_attr(caption_for(name)):
            kept.add(name)
            return m.group(0)
        return ""
    block = re.sub(r"\n\s*<column caption='([^']*)' datatype='[^']*' name='\[([^\]]+)\]' role='[^']*' type='[^']*' />",
                   drop_old_field, block)
    table_column = re.search(r"\n\s*<column caption='[^']*' datatype='table' [^>]*/>", block)
    insert_at = table_column.end() if table_column else block.index("</connection>") + len("</connection>")
    added = build_field_columns([c for c in columns if c[0] not in kept])
    block = block[:insert_at] + "\n" + "\n".join(added) + block[insert_at:]

    # 5. Datasource caption, updated only if it was Tableau's auto caption for the old table
    new_caption = None
    if old_caption and old_table in old_caption:
        new_caption = xml_attr(f"{new_table} ({fq_new}) ({schema})")
        block = block.replace(f"caption='{old_caption}'", f"caption='{new_caption}'", 1)

    text = text[:start] + block + text[end:]
    if new_caption:
        # Worksheets reference the datasource by caption too
        text = text.replace(f"<datasource caption='{old_caption}'", f"<datasource caption='{new_caption}'")

    return text, f"{old_catalog}.{old_schema}.{old_table}", fq_new


def missing_fields(text, columns):
    """Fields used on sheets that the new table doesn't have (they show red in Tableau)."""
    new_names = {name for name, _, _ in columns}
    calculated = set(re.findall(r"<column [^>]*name='\[([^\]]+)\]'[^>]*>\s*<calculation", text))
    parameters = set(re.findall(r"<column [^>]*name='\[([^\]]+)\]'[^>]*param-domain-type", text))
    used = set()
    for deps in re.findall(r"<datasource-dependencies\b.*?</datasource-dependencies>", text, re.S):
        used |= set(re.findall(r"<column [^>]*name='\[([^\]]+)\]'", deps))
    # Fields that calculated fields' formulas refer to
    for formula in re.findall(r"<calculation [^>]*formula='([^']*)'", text):
        used |= set(re.findall(r"\[([^\]\.]+)\](?!\.)", formula))
    return sorted(used - new_names - calculated - parameters - {"Parameters"})


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--workbook", required=True, help="Source .twb")
    parser.add_argument("--table", required=True, help="New table, fully qualified: catalog.schema.table")
    parser.add_argument("-p", "--profile", default=os.environ.get("DATABRICKS_CONFIG_PROFILE"), help="Databricks CLI profile")
    parser.add_argument("--output", help=f"Output .twb (default: {DEFAULT_OUTPUT_DIR}/<table name>.twb)")
    parser.add_argument("--in-place", action="store_true", help="Overwrite the source workbook (a .bak copy is kept)")
    parser.add_argument("--force", action="store_true", help="Overwrite the output file if it exists")
    parser.add_argument("--datasource", help="Part of the datasource caption/name, if the workbook has several")
    parser.add_argument("--columns-file", help="Read columns from this JSON instead of calling the Databricks CLI")
    args = parser.parse_args()

    if not args.workbook.lower().endswith(".twb"):
        sys.exit("Only .twb workbooks are supported (a .twbx is a zip: unzip it and pass the .twb inside).")
    parts = args.table.split(".")
    if len(parts) != 3:
        sys.exit("--table must be fully qualified: catalog.schema.table")
    catalog, schema, new_table = parts

    if args.in_place:
        output = args.workbook
    else:
        output = args.output or os.path.join(DEFAULT_OUTPUT_DIR, f"{new_table}.twb")
        if os.path.exists(output) and not args.force:
            sys.exit(f"{output} already exists; pass --force to overwrite it or --output to pick another name.")
        os.makedirs(os.path.dirname(output) or ".", exist_ok=True)

    print(f"Reading columns of {args.table}")
    columns = fetch_columns(args.table, args.profile, args.columns_file)

    with open(args.workbook, encoding="utf-8", newline="") as f:
        text = f.read()

    new_text, old_fq, new_fq = swap(text, args.datasource, catalog, schema, new_table, columns)

    if args.in_place:
        shutil.copyfile(args.workbook, args.workbook + ".bak")
    with open(output, "w", encoding="utf-8", newline="") as f:
        f.write(new_text)

    print(f"Swapped {old_fq} -> {new_fq} ({len(columns)} columns)")
    print(f"Wrote {output}")
    missing = missing_fields(new_text, columns)
    if missing:
        print("These fields are used on sheets but aren't in the new table (they'll show red; use Replace References in Tableau):")
        for name in missing:
            print(f"  - {name}")


if __name__ == "__main__":
    main()
