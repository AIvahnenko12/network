# ==============================================================================
# UNIFIED NETWORK GRAPH BUILDER WITH INTEGRATED WAIT TIMES
# ==============================================================================
import networkx as px_net
from pyspark.sql import functions as F

# 1. Load core tables
travel_times_df = spark.table("adjacent_station_travel_times")
interchanges_df = spark.table("behind_barrier_interchanges")
ref_df = spark.table("station_reference")

# 2. Build Vertices
vertices_df = (
    ref_df.filter(~F.col("Line").isin(["tfl-rail", "london-overground"]))
    .withColumn(
        "id",
        F.concat_ws("_", F.col("Station"), F.col("Line"), F.col("Gate_NLC")),
    )
    .select(
        F.col("id"),
        F.col("Station").alias("station_name"),
        F.col("Line").alias("line"),
    )
    .dropDuplicates(["id"])
)

# 3. Define default mode/line wait lookup dictionary for routing weights
line_wait_lookup = {
    "victoria": 2.5,
    "jubilee": 2.5,
    "northern": 2.5,
    "central": 2.5,
    "bakerloo": 2.5,
    "piccadilly": 2.5,
    "district": 2.5,
    "circle": 2.5,
    "hammersmith-city": 2.5,
    "metropolitan": 2.5,
    "waterloo-city": 2.5,
    "dlr": 2.5,
    "elizabeth": 2.0,
    "overground": 5.0,
}

# 4. Build Edges in Pandas for NetworkX conversion (including wait time penalty on train runs)
pdf_train = travel_times_df.select(
    F.col("From_Node_ID").alias("src"),
    F.col("To_Node_ID").alias("dst"),
    F.col("Travel_Time_Mins").cast("double").alias("run_time"),
    F.col("Line").alias("line"),
).toPandas()

# Add default wait time to train travel time (Total Edge Cost = Run Time + Wait Time)
pdf_train["default_wait"] = pdf_train["line"].apply(
    lambda l: line_wait_lookup.get(str(l).lower().strip(), 3.0)
)
pdf_train["weight"] = pdf_train["run_time"] + pdf_train["default_wait"]
pdf_train["edge_type"] = "Train Run"

pdf_interchange = interchanges_df.select(
    F.col("From_Node_ID").alias("src"),
    F.col("To_Node_ID").alias("dst"),
    F.col("Interchange_Time_Mins").cast("double").alias("weight"),
    F.lit("Walk/Transfer").alias("line"),
    F.lit("Interchange").alias("edge_type"),
).toPandas()

# Combine edges
pdf_edges = pd.concat([pdf_train, pdf_interchange], ignore_index=True)

# 5. Build NetworkX DiGraph
G = px_net.from_pandas_edgelist(
    pdf_edges,
    source="src",
    target="dst",
    edge_attr=["weight", "edge_type", "line"],
    create_using=px_net.DiGraph(),
)

print(
    f"Successfully built clean NetworkX graph: {G.number_of_nodes():,} nodes,"
    f" {G.number_of_edges():,} edges (Wait times factored into edge weights)."
)
