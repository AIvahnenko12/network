# ==============================================================================
# DATABRICKS PYSPARK LINE WAIT TIMES BUILDER (DEFAULT WAIT TIMES FOCUS)
# ==============================================================================

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
import requests
import json
import urllib.parse

# 1. Retrieve API key securely from Databricks Secrets
try:
  TFL_API_KEY = dbutils.secrets.get(
      scope="tfl-credentials", key="tfl-primary-key"
  )
except Exception:
  TFL_API_KEY = ""

# Benchmark hub stations used to calculate line frequencies (TPH)
LINE_BENCHMARK_STATIONS = {
    "victoria": "940GZZLUOXC",  # Oxford Circus
    "jubilee": "940GZZLUBST",  # Baker Street
    "northern": "940GZZLUBNK",  # Bank
    "central": "940GZZLUOXC",  # Oxford Circus
    "bakerloo": "940GZZLUBST",  # Baker Street
    "piccadilly": "940GZZLUGPK",  # Green Park
    "district": "940GZZLUWMS",  # Westminster
    "circle": "940GZZLUBST",  # Baker Street
    "hammersmith-city": "940GZZLUBST",  # Baker Street
    "metropolitan": "940GZZLUBST",  # Baker Street
    "waterloo-city": "940GZZLUWLO",  # Waterloo
    "dlr": "940GZZDLWHM",  # West Ham
    "elizabeth": "940GZZLUTCR",  # Tottenham Court Road
}


def calculate_wait_from_tph(trains_per_hour: float) -> float:
  """Computes expected passenger wait time using half-headway: Wait = 60 / (2 * TPH)."""
  if trains_per_hour <= 0:
    return 10.0
  return round(60.0 / (2.0 * trains_per_hour), 1)


def fetch_default_wait_from_api(line_id: str, stop_naptan: str) -> float:
  """Queries TfL Timetable API to calculate default average wait time."""
  params = {"app_key": TFL_API_KEY} if TFL_API_KEY else {}
  url = f"https://api.tfl.gov.uk/Line/{line_id}/Timetable/{stop_naptan}"

  try:
    res = requests.get(url, params=params, timeout=5)
    if res.status_code == 200:
      data = res.json()
      routes = data.get("timetable", {}).get("routes", [])
      peak_count = 0
      off_peak_count = 0

      for route in routes:
        for schedule in route.get("schedules", []):
          for entry in schedule.get("knownJourneys", []):
            hour = int(entry.get("hour", 0))
            if hour == 8:
              peak_count += 1
            elif hour == 11:
              off_peak_count += 1

      if peak_count > 0:
        tph_peak = max(2.0, float(peak_count))
        tph_offpeak = max(1.0, float(off_peak_count))
        return calculate_wait_from_tph((tph_peak + tph_offpeak) / 2.0)
  except Exception:
    pass

  return None


def refresh_wait_times():
  params = {"app_key": TFL_API_KEY} if TFL_API_KEY else {}
  modes = ["tube", "dlr", "overground", "elizabeth-line", "national-rail"]
  url = f"https://api.tfl.gov.uk/Line/Mode/{','.join(modes)}"

  rows = []
  res = requests.get(url, params=params)

  if res.status_code == 200:
    lines_data = res.json()

    for item in lines_data:
      line_name = item.get("name", "").strip()
      mode = item.get("modeName", "").strip()
      line_id = item.get("id", "").strip().lower()

      # 1. Try dynamic API Timetable extraction
      naptan = LINE_BENCHMARK_STATIONS.get(line_id)
      default_w = (
          fetch_default_wait_from_api(line_id, naptan) if naptan else None
      )

      # 2. Rule-based fallbacks if API fails or no benchmark station exists
      if default_w is None:
        if mode == "tube":
          default_w = 2.5
        elif mode == "dlr":
          default_w = 2.5
        elif mode == "elizabeth-line":
          default_w = 2.0
        elif mode == "overground":
          default_w = 5.0
        else:  # National Rail
          default_w = 7.5

      rows.append(
          (line_name, mode, default_w)
      )  # Tuple matching schema order

  # Define explicit PySpark schema
  schema = StructType([
      StructField("Line", StringType(), True),
      StructField("Mode", StringType(), True),
      StructField("Default Avg Wait (mins)", DoubleType(), True),
  ])

  # Create PySpark DataFrame from Python list
  spark_df = spark.createDataFrame(rows, schema)
  spark_df = spark_df.orderBy("Mode", "Line")

  # Write to Delta Table
  spark_df.write.mode("overwrite").saveAsTable("line_wait_times")
  print(
      f"Successfully generated and saved {spark_df.count():,} line wait time"
      " profiles to Delta table 'line_wait_times'."
  )
  return spark_df


# Execute function in Databricks
wait_times_df = refresh_wait_times()
display(wait_times_df.limit(10))
