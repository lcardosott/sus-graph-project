#!/usr/bin/env python3
"""Build final public-hospital showcase HTML and static chart assets."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib-codex")

import matplotlib.pyplot as plt
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build final showcase report.")
    parser.add_argument("--analysis-summary", default="data_layer/reports/analysis/summary_2021_public_hospitals.json")
    parser.add_argument("--algorithm-summary", default="algorithm_layer/reports/final_2021_public_hospitals_summary.json")
    parser.add_argument("--sensitivity", default="algorithm_layer/reports/final_2021_public_hospitals_sensitivity_matrix.csv")
    parser.add_argument("--centrality", default="algorithm_layer/reports/final_2021_public_hospitals_centrality.csv")
    parser.add_argument("--stress", default="algorithm_layer/reports/final_2021_public_hospitals_stress_dynamic.csv")
    parser.add_argument("--communities", default="algorithm_layer/reports/final_2021_public_hospitals_communities.csv")
    parser.add_argument("--k-path", default="algorithm_layer/reports/final_2021_public_hospitals_k_path_redundancy.csv")
    parser.add_argument("--regional-overall", default="algorithm_layer/reports/regional_2021_public_hospitals_overall.csv")
    parser.add_argument("--regional-dependency", default="algorithm_layer/reports/regional_2021_public_hospitals_municipality_dependency.csv")
    parser.add_argument("--out-dir", default="viz_layer/reports")
    return parser.parse_args()


def read_json(path: str) -> dict[str, object]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def save_largest_component_chart(sensitivity: pd.DataFrame, out_path: Path) -> None:
    primary = sensitivity[
        (sensitivity["min_residence_count"] == 5.0)
        & (sensitivity["min_transfer_count"] == 2.0)
    ].sort_values("distance_min_km")
    plt.figure(figsize=(7, 4))
    plt.plot(primary["distance_min_km"], primary["largest_component_share"], marker="o", color="#2a7f62")
    plt.xlabel("Minimum distance (km)")
    plt.ylabel("Largest component share")
    plt.ylim(max(0.0, primary["largest_component_share"].min() - 0.02), 1.005)
    plt.title("Public-hospital resilience by distance band")
    plt.tight_layout()
    plt.savefig(out_path, dpi=160)
    plt.close()


def save_stress_chart(stress: pd.DataFrame, out_path: Path) -> None:
    plt.figure(figsize=(7, 4))
    plt.plot(stress["step"], stress["path_increase_ratio"], marker="o", color="#1f77b4")
    plt.xlabel("Suppression step")
    plt.ylabel("Sampled shortest-path increase ratio")
    plt.title("Dynamic suppression stress test")
    plt.axhline(0, color="#999999", linewidth=0.8)
    plt.tight_layout()
    plt.savefig(out_path, dpi=160)
    plt.close()


def save_centrality_chart(centrality: pd.DataFrame, out_path: Path) -> None:
    top = centrality.head(10).copy()
    top["label"] = top["name"].fillna(top["node_id"]).astype(str).str.slice(0, 36)
    plt.figure(figsize=(8, 5))
    plt.barh(top["label"][::-1], top["betweenness"][::-1], color="#4c78a8")
    plt.xlabel("Betweenness")
    plt.title("Top central public hospitals")
    plt.tight_layout()
    plt.savefig(out_path, dpi=160)
    plt.close()


def html_table(frame: pd.DataFrame, columns: list[str], limit: int = 10) -> str:
    return frame[columns].head(limit).to_html(index=False, classes="data-table", border=0, justify="left")


def build_html(
    analysis_summary: dict[str, object],
    algorithm_summary: dict[str, object],
    sensitivity: pd.DataFrame,
    centrality: pd.DataFrame,
    stress: pd.DataFrame,
    communities: pd.DataFrame,
    k_path: pd.DataFrame,
    regional_overall: pd.DataFrame,
    regional_dependency: pd.DataFrame,
    out_dir: Path,
) -> str:
    primary = sensitivity[
        (sensitivity["min_residence_count"] == 5.0)
        & (sensitivity["min_transfer_count"] == 2.0)
    ].sort_values("distance_min_km")
    stress_final = stress.tail(1).iloc[0] if not stress.empty else {}

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>SUS Public-Hospital Graph Results</title>
  <style>
    body {{ margin: 0; font-family: Inter, Segoe UI, sans-serif; color: #1f2428; background: #f7f7f4; }}
    header {{ padding: 28px 36px; background: #ffffff; border-bottom: 1px solid #ddd; }}
    main {{ padding: 24px 36px 48px; }}
    h1 {{ margin: 0 0 8px; font-size: 28px; }}
    h2 {{ margin-top: 30px; font-size: 20px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 12px; }}
    .metric {{ background: #fff; border: 1px solid #ddd; border-radius: 8px; padding: 14px; }}
    .metric b {{ display: block; font-size: 22px; margin-bottom: 4px; }}
    .panel {{ background: #fff; border: 1px solid #ddd; border-radius: 8px; padding: 16px; margin-top: 14px; overflow-x: auto; }}
    img {{ max-width: 100%; border: 1px solid #ddd; border-radius: 8px; background: #fff; }}
    .charts {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 16px; }}
    .data-table {{ border-collapse: collapse; width: 100%; font-size: 13px; }}
    .data-table th, .data-table td {{ padding: 7px 8px; border-bottom: 1px solid #eee; text-align: left; }}
    a {{ color: #2867a6; }}
  </style>
</head>
<body>
  <header>
    <h1>SUS Public-Hospital Graph Results, Brazil 2021</h1>
    <p>Validated national SIH graph filtered to public SUS hospitals, with recurrent long-distance flows and graph-resilience algorithms.</p>
  </header>
  <main>
    <section class="grid">
      <div class="metric"><b>{analysis_summary.get("records_rows")}</b>SIH rows reconciled</div>
      <div class="metric"><b>{analysis_summary.get("public_hospital_nodes_in_graph")}</b>public hospital nodes</div>
      <div class="metric"><b>{algorithm_summary.get("primary_nodes")}</b>primary analysis nodes</div>
      <div class="metric"><b>{algorithm_summary.get("primary_edges")}</b>primary recurrent edges</div>
      <div class="metric"><b>{algorithm_summary.get("communities")}</b>Louvain communities</div>
      <div class="metric"><b>{stress_final.get("path_increase_ratio", "")}</b>final stress path ratio</div>
    </section>

    <h2>Map Demo</h2>
    <div class="panel">
      <p>Open the lightweight public-hospital map and dashboard: <a href="../graph_map_ui.html">graph_map_ui.html</a>. Use guided presets to inspect central hospitals, stress-test removals, regional corridors, and municipal dependencies.</p>
    </div>

    <h2>Resilience Charts</h2>
    <div class="charts">
      <img src="resilience_largest_component_public_hospitals.png" alt="Largest component by distance band" />
      <img src="resilience_stress_path_increase_public_hospitals.png" alt="Stress path increase" />
      <img src="top_central_facilities_public_hospitals.png" alt="Top central facilities" />
    </div>

    <h2>Distance Band Sensitivity</h2>
    <div class="panel">{html_table(primary, ["distance_min_km", "nodes", "edges", "components", "largest_component_share"], 10)}</div>

    <h2>Top Central Public Hospitals</h2>
    <div class="panel">{html_table(centrality, ["rank", "node_id", "name", "municipality_code", "betweenness", "weighted_degree"], 12)}</div>

    <h2>Dynamic Suppression</h2>
    <div class="panel">{html_table(stress, ["step", "removed_node_id", "removed_node_name", "largest_component_share", "sampled_average_shortest_path_km", "path_increase_ratio"], 10)}</div>

    <h2>Community Detection</h2>
    <div class="panel">{html_table(communities.sort_values("nodes", ascending=False), ["community_id", "nodes", "edges", "facility_nodes", "municipality_nodes", "dominant_uf_prefix", "dominant_uf_share"], 12)}</div>

    <h2>Official Health-Region Mismatch</h2>
    <div class="panel">{html_table(regional_overall, ["distance_min_km", "edge_type", "edges", "total_flow", "cross_health_region_flow", "cross_health_region_share", "unknown_health_region_flow", "weighted_mean_distance_km"], 10)}</div>

    <h2>Municipal Dependency Examples</h2>
    <div class="panel">{html_table(regional_dependency, ["distance_min_km", "source_node_id", "source_name", "source_health_region_id", "total_flow", "target_facilities", "top_target_name", "top_target_share", "cross_health_region_share", "weighted_mean_distance_km"], 12)}</div>

    <h2>K-Path Redundancy</h2>
    <div class="panel">{html_table(k_path, ["source_node_id", "target_node_id", "transfer_count", "paths_found", "k1_distance_km", "k2_distance_km", "k2_k1_ratio", "no_alternative"], 12)}</div>

    <h2>Future Capacity Question</h2>
    <div class="panel">
      <p>Capacity is not treated as a final result here because annual SIH totals do not represent simultaneous occupancy. The next defensible version would need beds by competence, occupancy or daily movement, and service-line capacity.</p>
    </div>
  </main>
</body>
</html>
"""


def main() -> int:
    args = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    analysis_summary = read_json(args.analysis_summary)
    algorithm_summary = read_json(args.algorithm_summary)
    sensitivity = pd.read_csv(args.sensitivity)
    centrality = pd.read_csv(args.centrality)
    stress = pd.read_csv(args.stress)
    communities = pd.read_csv(args.communities)
    k_path = pd.read_csv(args.k_path)
    regional_overall = pd.read_csv(args.regional_overall)
    regional_dependency = pd.read_csv(args.regional_dependency)

    save_largest_component_chart(sensitivity, out_dir / "resilience_largest_component_public_hospitals.png")
    save_stress_chart(stress, out_dir / "resilience_stress_path_increase_public_hospitals.png")
    save_centrality_chart(centrality, out_dir / "top_central_facilities_public_hospitals.png")

    html = build_html(analysis_summary, algorithm_summary, sensitivity, centrality, stress, communities, k_path, regional_overall, regional_dependency, out_dir)
    html_path = out_dir / "final_showcase.html"
    html_path.write_text(html, encoding="utf-8")

    md_path = out_dir / "final_results_summary.md"
    md_path.write_text(
        "\n".join(
            [
                "# Public-Hospital Final Results",
                "",
                f"- SIH rows reconciled: {analysis_summary.get('records_rows')}",
                f"- Public hospital nodes in graph: {analysis_summary.get('public_hospital_nodes_in_graph')}",
                f"- Primary analysis graph: {algorithm_summary.get('primary_nodes')} nodes, {algorithm_summary.get('primary_edges')} edges",
                f"- Louvain communities: {algorithm_summary.get('communities')}",
                f"- Dynamic stress steps: {algorithm_summary.get('dynamic_stress_steps')}",
                f"- K-path pairs analyzed: {algorithm_summary.get('k_path_pairs')}",
                "- Regional mismatch reports added for 25 km and 50 km using CNES `REGSAUDE`.",
                "- Capacity analysis suppressed from final claims; annual SIH totals are not simultaneous occupancy.",
            ]
        ),
        encoding="utf-8",
    )
    print(json.dumps({"html": str(html_path), "markdown": str(md_path)}, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
