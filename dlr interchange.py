# ==============================================================================
# CORRECTED DLR & NON-DLR BATCH ROUTING SCRIPT
# ==============================================================================

# Stations that count as DLR-eligible (45 core DLR stations + specified major interchange hubs)
DLR_ELIGIBLE_STATIONS = {
    # Hubs / Interchanges specified
    "bank",
    "west ham",
    "canning town",
    "stratford",
    "lewisham",
    "bow road",
    "monument",
    "tower hill",
    "greenwich",
    "woolwich arsenal",
}


def run_batch_dlr_routing(G, df_stations, nlc_to_name, NLC_ALIASES={}):
  """Runs batch routing between DLR-eligible nodes and Non-DLR nodes across the graph."""

  # 1. Classify all graph nodes into DLR vs Non-DLR based on station name or line
  dlr_nodes = set()
  non_dlr_nodes = set()

  for node in G.nodes:
    parts = str(node).split("_")
    # Assuming Node_ID format: Station_Line_Gate_NLC or Station_Line_NLC
    station_name = parts[0].strip().lower()
    line_name = parts[1].strip().lower() if len(parts) > 1 else ""

    is_dlr_line = "dlr" in line_name
    is_designated_hub = station_name in DLR_ELIGIBLE_STATIONS

    if is_dlr_line or is_designated_hub:
      dlr_nodes.add(node)
    else:
      non_dlr_nodes.add(node)

  print(
      f"Node Classification -> DLR Nodes: {len(dlr_nodes):,}, Non-DLR Nodes:"
      f" {len(non_dlr_nodes):,}"
  )

  results = []

  def get_route_interchanges_and_dlr_point(path):
    dlr_interchange_nlc = None
    dlr_interchange_name = None
    all_interchanges = []

    for i in range(len(path) - 1):
      u_node, v_node = path[i], path[i + 1]
      u_parts = u_node.split("_")
      v_parts = v_node.split("_")

      u_line = u_parts[1].lower() if len(u_parts) > 1 else ""
      v_line = v_parts[1].lower() if len(v_parts) > 1 else ""
      u_stat = u_parts[0]
      v_stat = v_parts[0]

      # Detect line transition (Interchange at same station)
      if u_line != v_line and u_stat == v_stat:
        if u_stat not in all_interchanges:
          all_interchanges.append(u_stat.title())

      # Detect DLR transition
      if ("dlr" in u_line and "dlr" not in v_line) or (
          "dlr" not in u_line and "dlr" in v_line
      ):
        trans_stat = u_stat if "dlr" in u_line else v_stat
        dlr_interchange_name = trans_stat.title()

    all_interchanges_str = (
        ", ".join(all_interchanges) if all_interchanges else "Direct / None"
    )
    return dlr_interchange_name, all_interchanges_str

  def add_route_record(direction, s_node, t_node, cost, path):
    total_mins = int(round(cost))
    hours, mins = divmod(total_mins, 60)
    journey_time_str = f"{hours}:{mins:02d}"

    s_parts = s_node.split("_")
    t_parts = t_node.split("_")
    from_station = s_parts[0].title()
    to_station = t_parts[0].title()

    ic_name, all_ics = get_route_interchanges_and_dlr_point(path)

    results.append({
        "from station": from_station,
        "to station": to_station,
        "dlr interchange": ic_name or "N/A",
        "all interchange stations": all_ics,
        "journey time": journey_time_str,
        "Journey_Time_Mins": total_mins,
        "Direction": direction,
    })

  # 2. Iterate across unique station pairs between Non-DLR and DLR nodes
  # Group nodes by station name to avoid redundant intra-station pairings
  from collections import defaultdict

  non_dlr_by_station = defaultdict(list)
  for n in non_dlr_nodes:
    non_dlr_by_station[n.split("_")[0].lower()].append(n)

  dlr_by_station = defaultdict(list)
  for n in dlr_nodes:
    dlr_by_station[n.split("_")[0].lower()].append(n)

  print(
      f"Routing across {len(non_dlr_by_station)} Non-DLR stations and"
      f" {len(dlr_by_station)} DLR stations..."
  )

  # Non-DLR -> DLR
  for s_stat, s_nodes in non_dlr_by_station.items():
    for t_stat, t_nodes in dlr_by_station.items():
      if s_stat == t_stat:
        continue

      best_cost, best_path = float("inf"), None
      for s in s_nodes:
        for t in t_nodes:
          if nx.has_path(G, s, t):
            c = nx.shortest_path_length(G, s, t, weight="weight")
            if c < best_cost:
              best_cost, best_path = c, nx.shortest_path(
                  G, s, t, weight="weight"
              )

      if best_path:
        add_route_record("Non-DLR to DLR", s_nodes[0], t_nodes[0], best_cost, best_path)

  df_out = pd.DataFrame(results).drop_duplicates(
      subset=["from station", "to station"]
  )
  print(
      "Routing Complete: Successfully generated"
      f" {len(df_out):,} route combinations."
  )
  return df_out


# master_routes_df = run_batch_dlr_routing(G, df_stations, nlc_to_name)
