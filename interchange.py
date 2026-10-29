# ==============================================================================
# DATABRICKS PYSPARK BEHIND-THE-BARRIER INTERCHANGE & SYNTHETIC TRIADS BUILDER
# ==============================================================================

from pyspark.sql import functions as F
import requests
import json
import time
import urllib.parse
import pandas as pd

# 1. Retrieve API key securely from Databricks Secrets
try:
  TFL_API_KEY = dbutils.secrets.get(scope="tfl-credentials", key="tfl-primary-key")
except Exception:
  TFL_API_KEY = ""

DEFAULT_INTERCHANGE_TIME = 2.0

# 2. Load station reference table from Databricks Spark catalog
ref_df = spark.table("station_reference").toPandas()

# Exclude redundant operators
ref_df = ref_df[~ref_df["Line"].isin(["tfl-rail", "london-overground"])].copy()

# Ensure Node_ID exists
if "Node_ID" not in ref_df.columns:
  ref_df["Node_ID"] = (
      ref_df["Station"].astype(str) + "_" + 
      ref_df["Line"].astype(str) + "_" + 
      ref_df["Gate_NLC"].astype(str)
  )

# Helper function to parse single or comma-separated Gate_NLC entries
def parse_gate_nlcs(val):
  gate_set = set()
  if pd.notna(val):
    parts = str(val).split(",")
    for p in parts:
      p_clean = p.strip()
      if p_clean.isdigit():
        gate_set.add(int(p_clean))
  return gate_set

# Map each Node_ID to its station attributes and set of Gate_NLC integers
node_meta = {}
for _, row in ref_df.iterrows():
  node_id = str(row["Node_ID"]).strip()
  if pd.notna(node_id) and node_id != "nan":
    node_meta[node_id] = {
        "Station": row["Station"],
        "Line": row["Line"],
        "NaPTAN": str(row["NaPTAN"]).strip(),
        "Gate_NLCs": parse_gate_nlcs(row["Gate_NLC"])
    }

nodes = list(node_meta.keys())
interchanges = []
params = {"app_key": TFL_API_KEY} if TFL_API_KEY else {}

print(f"Evaluating behind-the-barrier interchanges across {len(nodes)} nodes...")

# 3. Compare all unique node pairs sharing at least one common Gate_NLC
for i in range(len(nodes)):
  for j in range(i + 1, len(nodes)):
    node_a = nodes[i]
    node_b = nodes[j]
    
    meta_a = node_meta[node_a]
    meta_b = node_meta[node_b]

    # Must be at the same physical station (sharing a gate NLC)
    common_gates = meta_a["Gate_NLCs"].intersection(meta_b["Gate_NLCs"])

    if common_gates:
      interchange_time = DEFAULT_INTERCHANGE_TIME
      source_type = "Default Rule (2.0 mins)"

      # Optional: Synthetic API Triad calculation for precision where NaPTANs exist
      naptan_a = meta_a["NaPTAN"]
      naptan_b = meta_b["NaPTAN"]

      if naptan_a and naptan_b and naptan_a != "nan" and naptan_b != "nan" and naptan_a != naptan_b:
        jp_url = f"https://api.tfl.gov.uk/Journey/JourneyResults/{urllib.parse.quote(naptan_a)}/to/{urllib.parse.quote(naptan_b)}"
        try:
          jp_res = requests.get(jp_url, params=params, timeout=3)
          if jp_res.status_code == 200:
            journeys = jp_res.json().get("journeys", [])
            if journeys:
              journey = journeys[0]
              total_journey_duration = float(journey.get("duration", 0))
              legs = journey.get("legs", [])
              
              if len(legs) >= 1:
                # Sum leg durations and wait times across legs
                sum_legs = sum(float(l.get("duration", 0)) for l in legs)
                sum_wait = sum(float(l.get("assessment", {}).get("averageWaitMinutes", 2.0)) for l in legs)
                
                # Synthetic Triad Formula: Walk = Journey - (Legs + Wait)
                calculated_walk = total_journey_duration - (sum_legs + sum_wait)
                if 0.5 <= calculated_walk <= 15.0:  # Reasonable walking transfer bounds
                  interchange_time = round(calculated_walk, 1)
                  source_type = "Synthetic API Triad"
        except Exception:
          pass

      # Add bidirectional interchange edges
      for f_node, t_node in [(node_a, node_b), (node_b, node_a)]:
        interchanges.append({
            "From_Node_ID": f_node,
            "To_Node_ID": t_node,
            "Shared_Gate_NLCs": str(list(common_gates)),
            "Interchange_Time_Mins": interchange_time,
            "Interchange_Type": "Behind Barrier",
            "Calculation_Method": source_type
        })

      time.sleep(0.01)  # Rate limit safety pause

# 4. Convert to Spark DataFrame and save to Delta table
pdf_interchanges = pd.DataFrame(interchanges)
spark_interchanges = spark.createDataFrame(pdf_interchanges)

spark_interchanges.write.mode("overwrite").saveAsTable("behind_barrier_interchanges")
print(f"Successfully generated and saved {spark_interchanges.count():,} directional behind-the-barrier interchange edges to Delta table.")
display(spark_interchanges.limit(10))
