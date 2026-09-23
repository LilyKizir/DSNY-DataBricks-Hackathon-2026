import requests
import pandas as pd
import numpy as np
import io
import os
import time
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")
ZONE_COUNTY_URL = os.getenv("ZONE_COUNTY_URL")

HEADERS = {
    "User-Agent": WEATHER_API_KEY,
    "Accept": "application/geo+json"
}

# Measured hourly observations (ASOS airport stations) from the Iowa Environmental Mesonet.
# The NWS API only keeps ~7 days of observations and the NCEI hourly archive
# stopped updating in Aug 2025, so IEM is used for the backfill and daily runs.
IEM_STATIONS_URL = "https://mesonet.agron.iastate.edu/geojson/network/NY_ASOS.geojson"
IEM_ASOS_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"

BACKFILL_START = pd.Timestamp("2025-01-01", tz="UTC")

# Pause between IEM requests, it rate limits aggressive clients
REQUEST_PAUSE_SECONDS = 2
MAX_RETRIES = 5

# Local historical store. Swap load_history/save_history for Delta table
# reads/writes when this moves to Databricks.
DATA_DIR = Path(__file__).resolve().parents[1] / "data"
OBSERVATIONS_PATH = DATA_DIR / "weather_observations_hourly.parquet"
ZONE_STATION_PATH = DATA_DIR / "zone_station_map.parquet"

# One row per station per observation time
KEY_COLUMNS = ["station_id", "observed_at_utc"]

OBSERVATION_COLUMNS = [
    "station_id",
    "observed_at_utc",
    "temperature",
    "temperature_unit",
    "wind_speed",
    "wind_speed_unit",
    "ingested_at_utc"
]


def download_zone_county_file(url: str) -> pd.DataFrame:

    print("Downloading NWS zone-county file...")

    response = requests.get(
        url,
        headers=HEADERS,
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


def get_new_york_zone(df: pd.DataFrame) -> pd.DataFrame:
    new_york_df = df[df["state"] == "NY"].copy()
    new_york_df = new_york_df.dropna(subset=["latitude", "longitude"]).reset_index(drop=True)
    new_york_df = new_york_df.drop_duplicates(subset=["state_zone"]).reset_index(drop=True)

    print("New York zones records found: {}".format(len(new_york_df)))
    print("Unique New York zones found: {}".format(new_york_df["fips"].nunique()))

    return new_york_df


def get_new_york_stations() -> pd.DataFrame:

    print("Downloading New York ASOS station list...")

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
        (pd.to_datetime(stations_df["archive_begin"], utc=True) <= BACKFILL_START) &
        stations_df["archive_end"].isna()
    ].reset_index(drop=True)

    print("Active New York stations found: {}".format(len(stations_df)))

    return stations_df


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
        ["state_zone", "zone_name", "county", "fips", "latitude", "longitude"]
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


def get_station_observations(station_id: str, start_utc: pd.Timestamp, end_utc: pd.Timestamp) -> pd.DataFrame:

    params = {
        "station": station_id,
        "data": ["tmpf", "sped"],  # air temperature (F), wind speed (mph)
        "sts": start_utc.strftime("%Y-%m-%dT%H:%MZ"),
        "ets": end_utc.strftime("%Y-%m-%dT%H:%MZ"),
        "tz": "Etc/UTC",
        "format": "onlycomma",
        "missing": "M",
        "report_type": 3  # routine hourly reports only
    }

    for attempt in range(1, MAX_RETRIES + 1):

        response = requests.get(IEM_ASOS_URL, params=params, timeout=120)

        rate_limited = (
            response.status_code in (429, 503) or
            response.text.startswith("Too many requests")
        )

        if not rate_limited:
            break

        wait = 30 * attempt
        print("Rate limited on {}, retrying in {}s...".format(station_id, wait))
        time.sleep(wait)

    response.raise_for_status()

    raw_df = pd.read_csv(
        io.StringIO(response.text),
        na_values="M"
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


def load_history(path: Path = OBSERVATIONS_PATH) -> pd.DataFrame:

    if not path.exists():
        print("No history found at {}. Backfilling from {}.".format(path, BACKFILL_START.date()))
        return pd.DataFrame(columns=OBSERVATION_COLUMNS)

    history_df = pd.read_parquet(path)
    print("Loaded {} historical records from {}".format(len(history_df), path))

    return history_df


def save_history(df: pd.DataFrame, path: Path) -> None:

    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    print("Saved {} records to {}".format(len(df), path))


def get_station_watermarks(history_df: pd.DataFrame) -> pd.Series:

    # Max observation time already stored, per station
    if history_df.empty:
        return pd.Series(dtype="datetime64[ns, UTC]")

    return history_df.groupby("station_id")["observed_at_utc"].max()


def main():

    end_utc = pd.Timestamp.now(tz="UTC")

    #Get all zone-county data
    df = download_zone_county_file(ZONE_COUNTY_URL)
    #Filter to new York State only
    new_york_df = get_new_york_zone(df)

    #Assign each zone to its nearest active weather station
    stations_df = get_new_york_stations()
    zone_station_df = map_zones_to_nearest_station(new_york_df, stations_df)
    save_history(zone_station_df, ZONE_STATION_PATH)

    station_ids = sorted(zone_station_df["station_id"].unique())
    print("Stations needed for New York zones: {}".format(len(station_ids)))

    #Incremental load: each station starts after its previous max timestamp
    history_df = load_history()
    watermarks = get_station_watermarks(history_df)

    new_frames = []

    for station_id in station_ids:

        previous_max = watermarks.get(station_id)
        start_utc = BACKFILL_START if previous_max is None else previous_max

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

    new_df = pd.concat(new_frames, ignore_index=True)
    new_df["ingested_at_utc"] = pd.Timestamp.now(tz="UTC")

    print("New records to append: {}".format(len(new_df)))

    if history_df.empty:
        updated_df = new_df
    else:
        updated_df = pd.concat([history_df, new_df], ignore_index=True)

    updated_df = (
        updated_df[OBSERVATION_COLUMNS]
        .drop_duplicates(subset=KEY_COLUMNS, keep="first")
        .sort_values(KEY_COLUMNS)
        .reset_index(drop=True)
    )

    save_history(updated_df, OBSERVATIONS_PATH)


if __name__ == "__main__":
    main()
