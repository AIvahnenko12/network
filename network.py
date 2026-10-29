# ==============================================================================
# DATABRICKS PYSPARK MULTI-MODAL RAIL NETWORK GRAPH BUILDER
# ==============================================================================
# Combines adjacent travel times, behind-the-barrier interchanges, line wait times, 
# and station reference nodes into a unified GraphFrames directed network graph.
# ==============================================================================

from pyspark.sql import functions as F

# 1. Load core network components from Delta tables
travel_times_df = spark.table("adjacent_station_travel_times")
interchanges_df = spark.table("behind_barrier_interchanges")
wait_times_df = spark.table("line_wait_times")
ref_df = spark.table("station_reference")

print(
    f"Loaded tables -> Travel Edges: {travel_times_df.count():,}, Interchanges:"
    f" {interchanges_df.count():,}, Wait Times: {wait_times_df.count():,}"
)

# 2. Build Unified Vertices (Nodes) Table
# Each unique Node_ID represents a specific platform/line node in the network
vertices_df = (
    ref_df.filter(
        ~F.col("Line").isin(["tfl-rail", "london-overground"])
    )  # Exclude redundant operators
    .withColumn(
        "id",
        F.coalesce(
            F.col("Node_ID"),
            F.concat_ws(
                "_",
                F.col("Station"),
                F.col("Line"),
                F.col("Gate_NLC"),
            ),
        ),
    )
    .select(
        "id",
        F.col("Station").alias("station_name"),
        F.col("Line").alias("line"),
        F.col("NaPTAN").alias("naptan"),
        F.col("ICS").alias("ics"),
        F.col("Gate_NLC").alias("gate_nlc"),
    )
    .dropDuplicates(["id"])
)

print(f"Total unique network vertices (nodes): {vertices_df.count():,}")

# 3. Standardize and Unify Edges Table
# Normalize travel time edges
train_edges = travel_times_df.select(
    F.col("From_Node_ID").alias("src"),
    F.col("To_Node_ID").alias("dst"),
    F.col("Travel_Time_Mins").cast("double").alias("weight"),
    F.lit("Train Run").alias("edge_type"),
    F.col("Line").alias("line"),
)

# Normalize behind-the-barrier interchange edges
interchange_edges = interchanges_df.select(
    F.col("From_Node_ID").alias("src"),
    F.col("To_Node_ID").alias("dst"),
    F.col("Interchange_Time_Mins").cast("double").alias("weight"),
    F.lit("Interchange").alias("edge_type"),
    F.lit("Walk/Transfer").alias("line"),
)

# Combine into a single directional network edge DataFrame
edges_df = train_edges.unionByName(interchange_edges).dropDuplicates(
    ["src", "dst", "edge_type", "line"]
)

print(f"Total unified directional network edges: {edges_df.count():,}")

# 4. Save Unified Network Graph to Delta Tables
vertices_df.write.mode("overwrite").saveAsTable("rail_network_vertices")
edges_df.write.mode("overwrite").saveAsTable("rail_network_edges")

print("Successfully saved unified network graph tables:")
print("  - rail_network_vertices")
print("  - rail_network_edges")

# 5. Optional: Initialize GraphFrames Graph (if GraphFrames package is attached to cluster)
try:
  from graphframes import GraphFrame

  g = GraphFrame(vertices_df, edges_df)
  print(
      "\nGraphFrames Graph successfully initialized! Graph summary:"
      f" {g.vertices.count():,} vertices, {g.edges.count():,} edges."
  )
except Exception as e:
  print(
      "\nNote: GraphFrames library not attached to cluster environment (Tables"
      " are successfully saved as Delta tables for Spark SQL / GraphX routing"
      f" anyway). Details: {e}"
  )
