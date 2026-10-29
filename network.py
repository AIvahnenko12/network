# ==============================================================================
# DATABRICKS PYSPARK NETWORK GRAPH & DELTA TABLE BUILDER (NO GRAPHFRAMES)
# ==============================================================================
# Combines travel times, interchanges, wait times, and station reference nodes 
# into clean Delta tables ready for NetworkX pathfinding and routing analytics.
# ==============================================================================

from pyspark.sql import functions as F

# 1. Load core network components from Delta tables
travel_times_df = spark.table("adjacent_station_travel_times")
interchanges_df = spark.table("behind_barrier_interchanges")
ref_df = spark.table("station_reference")

print(
    f"Loaded tables -> Travel Edges: {travel_times_df.count():,}, Interchanges:"
    f" {interchanges_df.count():,}"
)

# 2. Build Unified Vertices (Nodes) Table with dynamic Node_ID generation
vertices_df = (
    ref_df.filter(~F.col("Line").isin(["tfl-rail", "london-overground"]))
    .withColumn(
        "id",
        F.concat_ws(
            "_",
            F.col("Station"),
            F.col("Line"),
            F.col("Gate_NLC"),
        ),
    )
    .select(
        F.col("id"),
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
train_edges = travel_times_df.select(
    F.col("From_Node_ID").alias("src"),
    F.col("To_Node_ID").alias("dst"),
    F.col("Travel_Time_Mins").cast("double").alias("weight"),
    F.lit("Train Run").alias("edge_type"),
    F.col("Line").alias("line"),
)

interchange_edges = interchanges_df.select(
    F.col("From_Node_ID").alias("src"),
    F.col("To_Node_ID").alias("dst"),
    F.col("Interchange_Time_Mins").cast("double").alias("weight"),
    F.lit("Interchange").alias("edge_type"),
    F.lit("Walk/Transfer").alias("line"),
)

edges_df = train_edges.unionByName(interchange_edges).dropDuplicates(
    ["src", "dst", "edge_type", "line"]
)

print(f"Total unified directional network edges: {edges_df.count():,}")

# 4. Save Unified Network Graph to Delta Tables
vertices_df.write.mode("overwrite").saveAsTable("rail_network_vertices")
edges_df.write.mode("overwrite").saveAsTable("rail_network_edges")

print("Successfully saved unified network tables:")
print("  - rail_network_vertices")
print("  - rail_network_edges")

# 5. Optional: Quick NetworkX conversion for routing / shortest-path audits in Python
import networkx as px_net

# Convert Spark edges to Pandas for lightweight NetworkX graph creation
pdf_edges = edges_df.select("src", "dst", "weight", "edge_type", "line").toPandas()
G = px_net.from_pandas_edgelist(
    pdf_edges,
    source="src",
    target="dst",
    edge_attr=["weight", "edge_type", "line"],
    create_using=px_net.DiGraph(),
)

print(
    f"\nNetworkX DiGraph successfully loaded! Graph summary: {G.number_of_nodes():,}"
    f" nodes, {G.number_of_edges():,} edges."
)
