import requests
import pandas as pd
import numpy as np
import io
import os
import time
from pathlib import Path
from dotenv import load_dotenv
from scripts.connect_databricks import get_spark
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

load_dotenv()
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")
ZONE_COUNTY_URL = os.getenv("ZONE_COUNTY_URL")
IEM_STATIONS_URL = os.getenv("IEM_STATIONS_URL")
IEM_ASOS_URL = os.getenv("IEM_ASOS_URL")

# Same credentials as connect_databricks.py. DATABRICKS_CLUSTER_ID is optional,
# serverless compute is used when it is not set.
DATABRICKS_SERVER_HOST = os.getenv("DATABRICKS_SERVER_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID")

START_TIMESTAMP = pd.Timestamp("2025-01-01", tz="UTC")

# Pause between IEM requests, it rate limits aggressive clients
REQUEST_PAUSE_SECONDS = 2
MAX_RETRIES = 5

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
ZONE_STATION_PATH = DATA_DIR / "zone_station_map.parquet"

# Delta table holding the hourly observations, one row per zone-county per hour
WEATHER_TABLE = "the_data_masons.bronze.bronze_weather_data"

# One row per zone-county per observation time
ZONE_KEY_COLUMNS = ["state_zone", "fips", "observed_at_utc"]

WEATHER_TABLE_SCHEMA = StructType([
    StructField("state_zone", StringType()),
    StructField("zone_name", StringType()),
    StructField("county", StringType()),
    StructField("fips", StringType()),
    StructField("nyiso_zone", StringType()),
    StructField("nyiso_zone_name", StringType()),
    StructField("station_id", StringType()),
    StructField("station_name", StringType()),
    StructField("distance_km", DoubleType()),
    StructField("observed_at_utc", TimestampType()),
    StructField("temperature", DoubleType()),
    StructField("temperature_unit", StringType()),
    StructField("wind_speed", DoubleType()),
    StructField("wind_speed_unit", StringType()),
    StructField("ingested_at_utc", TimestampType())
])

# NYISO load zone names as they appear in NYISO load/price data
NYISO_ZONE_NAMES = {
    "A": "WEST",
    "B": "GENESE",
    "C": "CENTRL",
    "D": "NORTH",
    "E": "MHK VL",
    "F": "CAPITL",
    "G": "HUD VL",
    "H": "MILLWD",
    "I": "DUNWOD",
    "J": "N.Y.C.",
    "K": "LONGIL"
}

# Approximate county -> NYISO zone
NYISO_COUNTY_ZONES = {
    # A - West
    "36003": "A",  # Allegany
    "36009": "A",  # Cattaraugus
    "36013": "A",  # Chautauqua
    "36029": "A",  # Erie
    "36037": "A",  # Genesee
    "36063": "A",  # Niagara
    "36073": "A",  # Orleans
    "36121": "A",  # Wyoming
    # B - Genesee
    "36051": "B",  # Livingston
    "36055": "B",  # Monroe
    "36069": "B",  # Ontario
    "36117": "B",  # Wayne
    "36123": "B",  # Yates
    # C - Central
    "36007": "C",  # Broome
    "36011": "C",  # Cayuga
    "36015": "C",  # Chemung
    "36017": "C",  # Chenango
    "36023": "C",  # Cortland
    "36053": "C",  # Madison
    "36067": "C",  # Onondaga
    "36075": "C",  # Oswego
    "36097": "C",  # Schuyler
    "36099": "C",  # Seneca
    "36101": "C",  # Steuben
    "36107": "C",  # Tioga
    "36109": "C",  # Tompkins
    # D - North
    "36019": "D",  # Clinton
    "36031": "D",  # Essex
    "36033": "D",  # Franklin
    # E - Mohawk Valley
    "36025": "E",  # Delaware
    "36041": "E",  # Hamilton
    "36043": "E",  # Herkimer
    "36045": "E",  # Jefferson
    "36049": "E",  # Lewis
    "36057": "E",  # Montgomery
    "36065": "E",  # Oneida
    "36077": "E",  # Otsego
    "36089": "E",  # St. Lawrence
    # F - Capital
    "36001": "F",  # Albany
    "36021": "F",  # Columbia
    "36035": "F",  # Fulton
    "36039": "F",  # Greene
    "36083": "F",  # Rensselaer
    "36091": "F",  # Saratoga
    "36093": "F",  # Schenectady
    "36095": "F",  # Schoharie
    "36113": "F",  # Warren
    "36115": "F",  # Washington
    # G - Hudson Valley
    "36027": "G",  # Dutchess
    "36071": "G",  # Orange
    "36079": "G",  # Putnam
    "36087": "G",  # Rockland
    "36105": "G",  # Sullivan
    "36111": "G",  # Ulster
    # I - Dunwoodie (H - Millwood is a small slice of northern Westchester)
    "36119": "I",  # Westchester
    # J - New York City
    "36005": "J",  # Bronx
    "36047": "J",  # Kings
    "36061": "J",  # New York
    "36081": "J",  # Queens
    "36085": "J",  # Richmond
    # K - Long Island
    "36059": "K",  # Nassau
    "36103": "K"   # Suffolk
}

#Get all the zone county
def download_zone_county_file(url: str) -> pd.DataFrame:

    response = requests.get(
        url,
        headers={
            "User-Agent": WEATHER_API_KEY,
            "Accept": "application/geo+json"
        },
        timeout=30
    )

    response.raise_for_status()

    # The file is pipe-delimited text
    df = pd.read_csv(
        io.StringIO(response.text),
        sep="|",
        header=None,
        dtype=str
    )

    df.columns = [
        "state",
        "zone",
        "cwa",
        "zone_name",
        "state_zone",
        "county",
        "fips",
        "time_zone",
        "fe_area",
        "latitude",
        "longitude"
    ]

    # Convert coordinates to numeric
    df["latitude"] = pd.to_numeric(
        df["latitude"],
        errors="coerce"
    )

    df["longitude"] = pd.to_numeric(
        df["longitude"],
        errors="coerce"
    )

    return df


#Filter only New York zone
def get_new_york_zone(df: pd.DataFrame) -> pd.DataFrame:
    new_york_df = df[df["state"] == "NY"].copy()
    new_york_df = new_york_df.dropna(subset=["latitude", "longitude"]).reset_index(drop=True)
    # One row per zone-county pair, a zone can span several counties
    new_york_df = new_york_df.drop_duplicates(subset=["state_zone", "fips"]).reset_index(drop=True)

    print("New York zone-county records found: {}".format(len(new_york_df)))
    print("Unique New York zones found: {}".format(new_york_df["state_zone"].nunique()))
    print("Unique New York counties found: {}".format(new_york_df["fips"].nunique()))

    return new_york_df


# Map counties to NYISO zones
def add_nyiso_zone(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()
    df["nyiso_zone"] = df["fips"].map(NYISO_COUNTY_ZONES)
    df["nyiso_zone_name"] = df["nyiso_zone"].map(NYISO_ZONE_NAMES)

    unmapped = df.loc[df["nyiso_zone"].isna(), ["fips", "county"]].drop_duplicates()
    if not unmapped.empty:
        print("Counties with no NYISO zone: {}".format(unmapped.to_dict("records")))

    return df

# Get New York stations
def get_new_york_stations() -> pd.DataFrame:


    response = requests.get(IEM_STATIONS_URL, timeout=60)
    response.raise_for_status()

    stations_df = pd.DataFrame([
        {
            "station_id": feature["properties"]["sid"],
            "station_name": feature["properties"]["sname"],
            "archive_begin": feature["properties"]["archive_begin"],
            "archive_end": feature["properties"]["archive_end"],
            "station_longitude": feature["geometry"]["coordinates"][0],
            "station_latitude": feature["geometry"]["coordinates"][1]
        }
        for feature in response.json()["features"]
    ])

    # Keep stations that cover the whole backfill window and are still reporting
    stations_df = stations_df[
        (pd.to_datetime(stations_df["archive_begin"], utc=True) <= START_TIMESTAMP) &
        stations_df["archive_end"].isna()
    ].reset_index(drop=True)

    print("Active New York stations found: {}".format(len(stations_df)))

    return stations_df

# Map the zones to the nearest station
def map_zones_to_nearest_station(zones_df: pd.DataFrame, stations_df: pd.DataFrame) -> pd.DataFrame:

    # Haversine distance from every zone to every station
    zone_lat = np.radians(zones_df["latitude"].to_numpy())[:, None]
    zone_lon = np.radians(zones_df["longitude"].to_numpy())[:, None]
    station_lat = np.radians(stations_df["station_latitude"].to_numpy())[None, :]
    station_lon = np.radians(stations_df["station_longitude"].to_numpy())[None, :]

    a = (
        np.sin((station_lat - zone_lat) / 2) ** 2 +
        np.cos(zone_lat) * np.cos(station_lat) * np.sin((station_lon - zone_lon) / 2) ** 2
    )
    distance_km = 2 * 6371 * np.arcsin(np.sqrt(a))

    nearest = distance_km.argmin(axis=1)

    zone_station_df = zones_df[
        ["state_zone", "zone_name", "county", "fips", "nyiso_zone", "nyiso_zone_name", "latitude", "longitude"]
    ].reset_index(drop=True)

    zone_station_df = pd.concat(
        [
            zone_station_df,
            stations_df.iloc[nearest][
                ["station_id", "station_name", "station_latitude", "station_longitude"]
            ].reset_index(drop=True)
        ],
        axis=1
    )

    zone_station_df["distance_km"] = distance_km[np.arange(len(nearest)), nearest].round(1)

    return zone_station_df

#Get the time series observations for a station
def get_station_observations(station_id: str, start_utc: pd.Timestamp, end_utc: pd.Timestamp) -> pd.DataFrame:

    params = {
        "station": station_id,
        "data": ["tmpf", "sped"],  # air temperature (F), wind speed (mph)
        "sts": start_utc.strftime("%Y-%m-%dT%H:%MZ"),
        "ets": end_utc.strftime("%Y-%m-%dT%H:%MZ"),
        "tz": "Etc/UTC",
        "format": "onlycomma",
        "missing": "M",
        "report_type": 3  
    }

    for attempt in range(1, MAX_RETRIES + 1):

        try:
            response = requests.get(IEM_ASOS_URL, params=params, timeout=120)
            rate_limited = (
                response.status_code in (429, 503) or
                response.text.startswith("Too many requests")
            )
            reason = "rate limited"
        except (requests.ConnectionError, requests.Timeout) as e:
            rate_limited = True
            reason = type(e).__name__

        if not rate_limited:
            break

        if attempt == MAX_RETRIES:
            # RequestException so main() skips this station instead of crashing
            raise requests.RequestException(
                "{} after {} attempts".format(reason, MAX_RETRIES)
            )

        wait = 30 * attempt
        print("{} on {}, retrying in {}s...".format(reason, station_id, wait))
        time.sleep(wait)

    response.raise_for_status()

    # No reports in the window
    if not response.text.strip():
        return pd.DataFrame(columns=["station_id", "observed_at_utc", "temperature",
                                     "temperature_unit", "wind_speed", "wind_speed_unit"])

    raw_df = pd.read_csv(
        io.StringIO(response.text),
        na_values="M"
    )

    missing_columns = {"station", "valid", "tmpf", "sped"} - set(raw_df.columns)
    if missing_columns:
        raise requests.RequestException(
            "Unexpected IEM response, missing columns {}: {}".format(
                sorted(missing_columns), response.text[:200]
            )
        )

    observations_df = pd.DataFrame({
        "station_id": raw_df["station"],
        "observed_at_utc": pd.to_datetime(raw_df["valid"], utc=True),
        "temperature": pd.to_numeric(raw_df["tmpf"], errors="coerce"),
        "temperature_unit": "F",
        "wind_speed": pd.to_numeric(raw_df["sped"], errors="coerce"),
        "wind_speed_unit": "mph"
    })

    # Drop reports with neither measurement
    observations_df = observations_df.dropna(
        subset=["temperature", "wind_speed"],
        how="all"
    )

    return observations_df


def save_history(df: pd.DataFrame, path: Path) -> None:

    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    print("Saved {} records to {}".format(len(df), path))

#Get the latest observations from Databricks table
def get_latest_observations(spark: SparkSession, table_name: str = WEATHER_TABLE) -> pd.Series:

    if not spark.catalog.tableExists(table_name):
        print("Table {} not found. Backfilling from {}.".format(table_name, START_TIMESTAMP.date()))
        return pd.Series(dtype="datetime64[ns, UTC]")

    latest_pdf = (
        spark.table(table_name)
        .groupBy("station_id")
        .agg(F.unix_timestamp(F.max("observed_at_utc")).alias("latest_epoch"))
        .toPandas()
    )

    print("Loaded latest observation time for {} stations from {}".format(len(latest_pdf), table_name))

    return (
        pd.to_datetime(latest_pdf["latest_epoch"], unit="s", utc=True)
        .set_axis(latest_pdf["station_id"])
    )

#Append new observations to the Delta table
def append_to_delta(spark: SparkSession, df: pd.DataFrame, table_name: str = WEATHER_TABLE) -> None:

    columns = [field.name for field in WEATHER_TABLE_SCHEMA.fields]
    new_sdf = spark.createDataFrame(
        df[columns].drop_duplicates(subset=ZONE_KEY_COLUMNS),
        schema=WEATHER_TABLE_SCHEMA
    )

    if not spark.catalog.tableExists(table_name):
        new_sdf.write.format("delta").saveAsTable(table_name)
        print("Created {} with {} records".format(table_name, len(df)))
        return

    # Insert-only merge, so a rerun over the same window does not duplicate rows
    (
        DeltaTable.forName(spark, table_name).alias("target")
        .merge(
            new_sdf.alias("source"),
            " AND ".join("target.{0} = source.{0}".format(c) for c in ZONE_KEY_COLUMNS)
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("Merged {} records into {}".format(len(df), table_name))


#Build zone-level observations by mapping stations to zones
def build_zone_observations(observations_df: pd.DataFrame, zone_station_df: pd.DataFrame) -> pd.DataFrame:

    # Attach each station's readings to every zone-county it serves
    zone_observations_df = zone_station_df[
        ["state_zone", "zone_name", "county", "fips", "nyiso_zone", "nyiso_zone_name",
         "station_id", "station_name", "distance_km"]
    ].merge(observations_df, on="station_id", how="inner")

    return zone_observations_df.sort_values(ZONE_KEY_COLUMNS).reset_index(drop=True)


def main():

    end_utc = pd.Timestamp.now(tz="UTC")

    #Get all zone-county data
    df = download_zone_county_file(ZONE_COUNTY_URL)
    #Filter to new York State only
    new_york_df = get_new_york_zone(df)
    #Tag each county with its NYISO load zone
    new_york_df = add_nyiso_zone(new_york_df)

    #Assign each zone to its nearest active weather station
    stations_df = get_new_york_stations()
    zone_station_df = map_zones_to_nearest_station(new_york_df, stations_df)
    save_history(zone_station_df, ZONE_STATION_PATH)

    station_ids = sorted(zone_station_df["station_id"].unique())
    print("Stations needed for New York zones: {}".format(len(station_ids)))

    #Incremental load: each station starts after its latest timestamp in the Delta table
    spark = get_spark()
    #Pull the latest observations from the Delta table
    latest_observations = get_latest_observations(spark)

    new_frames = []

    #For each station, get the latest observations from the delta table
    for station_id in station_ids:

        previous_max = latest_observations.get(station_id)
        start_utc = START_TIMESTAMP if previous_max is None else previous_max

        try:
            observations_df = get_station_observations(station_id, start_utc, end_utc)
        except requests.RequestException as e:
            print("Skipping station {}: {}".format(station_id, e))
            continue
        finally:
            time.sleep(REQUEST_PAUSE_SECONDS)

        if previous_max is not None:
            observations_df = observations_df[observations_df["observed_at_utc"] > previous_max]

        current_max = observations_df["observed_at_utc"].max() if not observations_df.empty else None

        print("{}: previous max {} | current max {} | new records {}".format(
            station_id, previous_max, current_max, len(observations_df)
        ))

        if not observations_df.empty:
            new_frames.append(observations_df)

    if not new_frames:
        print("No new observations to append.")
        return

    #Combine all new observation frames
    new_df = pd.concat(new_frames, ignore_index=True)
    new_df["ingested_at_utc"] = pd.Timestamp.now(tz="UTC")

    #Attach the new station readings to their zone-counties and append to Delta
    zone_observations_df = build_zone_observations(new_df, zone_station_df)
    print("New records to append: {}".format(len(zone_observations_df)))

    #Append the new observations to the Delta table
    append_to_delta(spark, zone_observations_df)


if __name__ == "__main__":
    main()
