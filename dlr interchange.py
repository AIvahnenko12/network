# ==============================================================================
# DATABRICKS PYSPARK DLR INTERCHANGE BUILDER
# ==============================================================================
from pyspark.sql import functions as F

# 1. Load reference table
ref_df = spark.table("station_reference").toPandas()
ref_df = ref_df[~ref_df["Line"].isin(["tfl-rail", "london-overground"])].copy()

# Ensure Node_ID exists
if "Node_ID" not in ref_df.columns:
  ref_df["Node_ID"] = (
      ref_df["Station"].astype(str)
      + "_"
      + ref_df["Line"].astype(str)
      + "_"
      + ref_df["Gate_NLC"].astype(str)
  )

# 2. Identify DLR interchange hubs and connected DLR stations
DLR_INTERCHANGE_HUBS = {"bank", "west ham", "canning town", "stratford", "greenwich"}
SPECIAL_DLR_CONNECTED = {"bow road", "greenwich", "monument", "tower hill"}

interchanges = []
default_dlr_interchange_time = 2.0

# Group nodes by station name (lower) to find cross-line DLR connections at interchange hubs
station_groups = ref_df.groupby(ref_df["Station"].str.lower().str.strip())

for station_name, group in station_groups:
  is_dlr_hub = station_name in DLR_INTERCHANGE_HUBS
  is_dlr_connected = station_name in SPECIAL_DLR_CONNECTED or any(
      group["Line"].str.lower() == "dlr"
  )

  if is_dlr_hub and is_dlr_connected:
    # Get all non-DLR nodes vs DLR nodes at this specific interchange station
    dlr_nodes = group[group["Line"].str.lower() == "dlr"]["Node_ID"].tolist()
    non_dlr_nodes = group[group["Line"].str.lower() != "dlr"]["Node_ID"].tolist()

    # Create directional interchange edges from non-DLR nodes to DLR nodes (and vice versa)
    for non_dlr in non_dlr_nodes:
      for dlr in dlr_nodes:
        if non_dlr != dlr:
          # Forward edge (Non-DLR -> DLR Interchange)
          interchanges.append({
              "From_Node_ID": non_dlr,
              "To_Node_ID": dlr,
              "Interchange_Time_Mins": default_dlr_interchange_time,
              "Interchange_Type": "DLR Hub Transfer",
              "Station": station_name.title(),
          })
          # Reverse edge (DLR -> Non-DLR Interchange)
          interchanges.append({
              "From_Node_ID": dlr,
              "To_Node_ID": non_dlr,
              "Interchange_Time_Mins": default_interchange_time,
              "Interchange_Type": "DLR Hub Transfer",
              "Station": station_name.title(),
          })

# 3. Convert to Spark DataFrame and save to Delta table
pdf_dlr_interchanges = pd.DataFrame(interchanges).drop_duplicates(
    subset=["From_Node_ID", "To_Node_ID"]
)
spark_dlr_interchanges = spark.createDataFrame(pdf_dlr_interchanges)

spark_dlr_interchanges.write.mode("overwrite").saveAsTable(
    "dlr_station_interchanges"
)
print(
    "Successfully generated and saved"
    f" {spark_dlr_interchanges.count():,} DLR interchange edges to Delta table"
    " 'dlr_station_interchanges'."
)
display(spark_dlr_interchanges.limit(10))
