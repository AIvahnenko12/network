# ==============================================================================
# DATABRICKS PYSPARK TRAVEL TIMES BUILDER (NaPTAN -> ICS FALLBACK)
# ==============================================================================

from pyspark.sql import functions as F
import requests
import json
import time
import urllib.parse

# 1. Retrieve API key securely from Databricks Secrets
# (Make sure to configure your secret scope/key accordingly, or replace with direct string if testing)
try:
  TFL_API_KEY = dbutils.secrets.get(
      scope="tfl-credentials", key="tfl-primary-key"
  )
except Exception:
  TFL_API_KEY = ""  # Fallback if running outside Databricks secret scope

RAIL_MODES = ["tube", "dlr", "overground", "elizabeth-line", "national-rail"]
DEFAULT_TRAVEL_TIME = 3.0


def normalize_line(name):
  if not name:
    return ""
  return (
      str(name)
      .lower()
      .replace(" line", "")
      .replace("-", "")
      .replace("_", "")
      .strip()
  )


# 2. Load reference table from Databricks Spark catalog
ref_df = spark.table("station_reference").toPandas()

# Exclude redundant operators
ref_df = ref_df[~ref_df["Line"].isin(["tfl-rail", "london-overground"])].copy()

# Construct Node_ID if not already present
if "Node_ID" not in ref_df.columns:
  ref_df["Node_ID"] = (
      ref_df["Station"].astype(str)
      + "_"
      + ref_df["Line"].astype(str)
      + "_"
      + ref_df["Gate_NLC"].astype(str)
  )

# Build lookup dictionaries: (norm_line, naptan) -> Node_ID and (norm_line, ics) -> Node_ID
naptan_lookup = {}
ics_lookup = {}
valid_nodes = set()

for _, row in ref_df.iterrows():
  node_id = str(row["Node_ID"]).strip()
  norm_line = normalize_line(row["Line"])
  naptan = str(row["NaPTAN"]).strip()
  ics = str(row["ICS"]).strip()

  if pd.notna(node_id) and node_id != "nan":
    valid_nodes.add(node_id)
    if pd.notna(naptan) and naptan != "nan":
      naptan_lookup[(norm_line, naptan)] = node_id
    if pd.notna(ics) and ics != "nan" and ics != "0":
      ics_lookup[(norm_line, ics)] = node_id

print(
    f"Loaded {len(valid_nodes):,} valid nodes with NaPTAN and ICS fallback"
    " mappings."
)

# 3. Query TfL API for routes and travel times
params = {"app_key": TFL_API_KEY} if TFL_API_KEY else {}
lines_url = f"https://api.tfl.gov.uk/Line/Mode/{','.join(RAIL_MODES)}"

res = requests.get(lines_url, params=params)
if res.status_code != 200:
  raise Exception(f"Failed to fetch lines from TfL API: {res.status_code}")

lines = res.json()
records = []

print(f"Querying adjacent stop pairs across {len(lines)} lines...")

for line in lines:
  line_id = line["id"].lower()
  line_name = line["name"]
  norm_line = normalize_line(line_name)

  seq_url = f"https://api.tfl.gov.uk/Line/{line_id}/Route/Sequence/all"
  seq_res = requests.get(seq_url, params=params)
  if seq_res.status_code != 200:
    continue

  seq_data = seq_res.json()

  for branch in seq_data.get("stopPointSequences", []):
    stops = branch.get("stopPoint", [])

    for i in range(len(stops) - 1):
      from_stop, to_stop = stops[i], stops[i + 1]

      # Extract NaPTAN and potential ICS identifiers from TfL API stop payload
      from_id = from_stop.get("id", "")
      to_id = to_stop.get("id", "")
      from_name = from_stop.get("name", "")
      to_name = to_stop.get("name", "")

      # Try NaPTAN first; if missing/unmatched, fallback to ICS if available in stop properties
      from_ics = str(from_stop.get("icsCode", ""))
      to_ics = str(to_stop.get("icsCode", ""))

      from_node_id = naptan_lookup.get(
          (norm_line, from_id),
          ics_lookup.get(
              (norm_line, from_ics), naptan_lookup.get(from_id, from_id)
          ),
      )
      to_node_id = naptan_lookup.get(
          (norm_line, to_id),
          ics_lookup.get((norm_line, to_ics), naptan_lookup.get(to_id, to_id)),
      )

      # Scope check: Both nodes must exist in reference table
      if from_node_id not in valid_nodes or to_node_id not in valid_nodes:
        continue

      duration = None
      status = "Success"

      # Query Journey Planner API for running time
      jp_url = f"https://api.tfl.gov.uk/Journey/JourneyResults/{urllib.parse.quote(from_id)}/to/{urllib.parse.quote(to_id)}"

      try:
        jp_res = requests.get(jp_url, params=params, timeout=4)
        if jp_res.status_code == 200:
          journeys = jp_res.json().get("journeys", [])
          if journeys:
            legs = journeys[0].get("legs", [])
            if legs:
              raw_dur = legs[0].get("duration", journeys[0].get("duration"))
              if raw_dur is not None:
                duration = float(raw_dur)
      except Exception:
        pass

      if duration is None or duration <= 0:
        duration = DEFAULT_TRAVEL_TIME
        status = "Fallback Used"

      # Forward edge
      records.append({
          "Line": line_name,
          "From_Node_ID": from_node_id,
          "From_NaPTAN": from_id,
          "From_Station": from_name,
          "To_Node_ID": to_node_id,
          "To_NaPTAN": to_id,
          "To_Station": to_name,
          "Travel_Time_Mins": duration,
          "Status": status,
      })

      # Symmetric Reverse edge
      records.append({
          "Line": line_name,
          "From_Node_ID": to_node_id,
          "From_NaPTAN": to_id,
          "From_Station": to_name,
          "To_Node_ID": from_node_id,
          "To_NaPTAN": from_id,
          "To_Station": from_name,
          "Travel_Time_Mins": duration,
          "Status": f"{status} (Symmetric)",
      })

      time.sleep(0.01)

# 4. Convert results into a Spark DataFrame and save to Delta table
pdf = pd.DataFrame(records).drop_duplicates(
    subset=["Line", "From_Node_ID", "To_Node_ID"]
)
spark_df = spark.createDataFrame(pdf)

spark_df.write.mode("overwrite").saveAsTable("adjacent_station_travel_times")
print(
    f"Successfully generated and saved {spark_df.count():,} travel time edges"
    " to Delta table 'adjacent_station_travel_times'."
)
