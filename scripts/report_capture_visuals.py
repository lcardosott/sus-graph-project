#!/usr/bin/env python3
"""Generate report figures from final map layers and algorithm outputs."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from PIL import Image, ImageOps


ROOT = Path(__file__).resolve().parents[1]
FIG = ROOT / "report" / "figures"
TMP_PARTIAL = Path("/tmp/mc859_partial_images")
LAYERS = ROOT / "viz_layer" / "reports" / "final_map_layers.json"


EDGE_COLORS = {
    "residence": "#d56b2d",
    "transfer": "#2f6f9f",
}


def load_layers() -> dict:
    return json.loads(LAYERS.read_text(encoding="utf-8"))


def setup_ax(ax, title: str, subtitle: str | None = None) -> None:
    ax.set_title(title, fontsize=14, weight="bold", loc="left", pad=8)
    if subtitle:
        ax.text(0.0, 1.005, subtitle, transform=ax.transAxes, fontsize=9, color="#555")
    ax.set_xlim(-74.5, -33.0)
    ax.set_ylim(-34.5, 5.8)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.grid(True, color="#e9ecef", linewidth=0.7)
    ax.set_facecolor("#fbfcfd")


def draw_edges(ax, edges: list[dict], limit: int, alpha: float = 0.28) -> None:
    for edge in edges[:limit]:
        color = EDGE_COLORS.get(edge.get("edge_type"), "#777")
        width = min(2.4, max(0.35, 0.22 + float(edge.get("transfer_count", 0)) ** 0.28 / 5.5))
        ax.plot(
            [edge["source_lon"], edge["target_lon"]],
            [edge["source_lat"], edge["target_lat"]],
            color=color,
            linewidth=width,
            alpha=alpha,
            solid_capstyle="round",
            zorder=1,
        )


def draw_edge_nodes(ax, edges: list[dict], limit: int) -> None:
    sources_lon, sources_lat, targets_lon, targets_lat = [], [], [], []
    for edge in edges[:limit]:
        sources_lon.append(edge["source_lon"])
        sources_lat.append(edge["source_lat"])
        targets_lon.append(edge["target_lon"])
        targets_lat.append(edge["target_lat"])
    ax.scatter(sources_lon, sources_lat, s=9, color="#3f6f9f", alpha=0.52, label="Municípios", zorder=2)
    ax.scatter(targets_lon, targets_lat, s=12, color="#243b53", alpha=0.68, label="Hospitais", zorder=3)


def draw_central_nodes(ax, nodes: list[dict], label: str = "Hospitais centrais") -> None:
    lon = [n["lon"] for n in nodes]
    lat = [n["lat"] for n in nodes]
    sizes = [max(42, 170 - 5 * int(n.get("rank", i + 1))) for i, n in enumerate(nodes)]
    ax.scatter(lon, lat, s=sizes, color="#2a7f62", edgecolor="white", linewidth=0.9, alpha=0.92, label=label, zorder=4)
    for node in nodes[:8]:
        ax.annotate(
            str(node.get("rank", "")),
            (node["lon"], node["lat"]),
            xytext=(4, 3),
            textcoords="offset points",
            fontsize=7,
            color="#184936",
            weight="bold",
            zorder=5,
        )


def save_map_preset(data: dict, key: str, out_name: str, title: str, edge_limit: int) -> None:
    preset = data["presets"][key]
    fig, ax = plt.subplots(figsize=(9.2, 7.1), dpi=190)
    setup_ax(
        ax,
        title,
        f"{edge_limit} maiores fluxos filtrados; laranja=residência, azul=transferência",
    )
    draw_edges(ax, preset["edges"], edge_limit)
    draw_edge_nodes(ax, preset["edges"], edge_limit)
    draw_central_nodes(ax, preset["central_nodes"][:12], "Top 12 centralidade")
    ax.legend(loc="lower left", frameon=True, framealpha=0.96, fontsize=8)
    fig.tight_layout()
    fig.savefig(FIG / out_name, bbox_inches="tight")
    plt.close(fig)


def save_central_map(data: dict) -> None:
    preset = data["presets"]["50km"]
    fig, ax = plt.subplots(figsize=(9.2, 7.1), dpi=190)
    setup_ax(ax, "Hospitais centrais na instância de 50 km", "Nós dimensionados pela posição no ranking de intermediação")
    draw_edges(ax, preset["edges"], 480, alpha=0.17)
    draw_edge_nodes(ax, preset["edges"], 480)
    draw_central_nodes(ax, preset["central_nodes"][:20])
    for node in preset["central_nodes"][:5]:
        name = str(node["name"]).title()
        ax.annotate(name[:32], (node["lon"], node["lat"]), xytext=(6, 6), textcoords="offset points", fontsize=7, color="#184936")
    ax.legend(loc="lower left", frameon=True, framealpha=0.96, fontsize=8)
    fig.tight_layout()
    fig.savefig(FIG / "map_central_hospitals.png", bbox_inches="tight")
    plt.close(fig)


def save_dependency_map(data: dict) -> None:
    examples = data["presets"]["dependency"]["examples"]
    fig, ax = plt.subplots(figsize=(9.2, 7.1), dpi=190)
    setup_ax(ax, "Exemplos de dependência municipal", "Municípios em vermelho e hospitais de referência em verde")
    for item in examples:
        src, tgt = item["source"], item["target"]
        ax.plot([src["lon"], tgt["lon"]], [src["lat"], tgt["lat"]], color="#b94747", linewidth=1.8, alpha=0.72, linestyle="--", zorder=1)
    ax.scatter([i["source"]["lon"] for i in examples], [i["source"]["lat"] for i in examples], s=55, color="#b94747", edgecolor="white", linewidth=0.8, label="Município", zorder=3)
    ax.scatter([i["target"]["lon"] for i in examples], [i["target"]["lat"] for i in examples], s=70, color="#2a7f62", edgecolor="white", linewidth=0.8, label="Hospital", zorder=4)
    for item in examples[:6]:
        src = item["source"]
        ax.annotate(src["name"].replace(" - ", "\n")[:28], (src["lon"], src["lat"]), xytext=(5, 4), textcoords="offset points", fontsize=7, color="#6b2424")
    ax.legend(loc="lower left", frameon=True, framealpha=0.96, fontsize=8)
    fig.tight_layout()
    fig.savefig(FIG / "map_dependency_examples.png", bbox_inches="tight")
    plt.close(fig)


def save_k_path_plot() -> None:
    path = ROOT / "algorithm_layer" / "reports" / "final_2021_public_hospitals_50km_k_path_redundancy.csv"
    df = pd.read_csv(path).copy()
    if "k2_k1_ratio" not in df.columns:
        return
    df = df.sort_values("k2_k1_ratio", ascending=False).head(12)
    labels = [str(v).replace("municipality:", "mun. ").replace("facility:", "hosp. ") for v in df["source_node_id"]]
    fig, ax = plt.subplots(figsize=(8.6, 5.2), dpi=190)
    ax.barh(labels, df["k2_k1_ratio"], color="#6f9fca")
    ax.axvline(2.0, color="#b94747", linestyle="--", linewidth=1.1, label="2x o melhor caminho")
    ax.invert_yaxis()
    ax.set_xlabel("Razão d(k2) / d(k1)")
    ax.set_title("Redundância dos k-caminhos no corte de 50 km", loc="left", fontsize=13, weight="bold")
    ax.grid(axis="x", color="#e9ecef")
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(FIG / "k_path_redundancy.png", bbox_inches="tight")
    plt.close(fig)


def save_regional_plot() -> None:
    path = ROOT / "algorithm_layer" / "reports" / "regional_2021_public_hospitals_overall.csv"
    df = pd.read_csv(path)
    df["label"] = df["distance_min_km"].astype(str) + " km / " + df["edge_type"]
    fig, ax = plt.subplots(figsize=(8.5, 4.8), dpi=190)
    ax.bar(df["label"], df["cross_health_region_share"] * 100, color=["#d56b2d", "#2f6f9f", "#d56b2d", "#2f6f9f"])
    ax.set_ylabel("Fluxo fora da região oficial (%)")
    ax.set_title("Fluxos recorrentes que atravessam regiões oficiais de saúde", loc="left", fontsize=13, weight="bold")
    ax.set_ylim(0, max(70, df["cross_health_region_share"].max() * 120))
    ax.grid(axis="y", color="#e9ecef")
    ax.tick_params(axis="x", rotation=15)
    for i, v in enumerate(df["cross_health_region_share"] * 100):
        ax.text(i, v + 1, f"{v:.1f}%", ha="center", fontsize=8)
    fig.tight_layout()
    fig.savefig(FIG / "regional_cross_share.png", bbox_inches="tight")
    plt.close(fig)


def save_partial_images() -> None:
    candidates = {
        "raw_full_map_from_partial.png": TMP_PARTIAL / "partial-006.png",
        "topological_graph_from_partial.png": TMP_PARTIAL / "partial-010.png",
    }
    for name, src in candidates.items():
        if src.exists():
            shutil.copyfile(src, FIG / name)


def add_label(img: Image.Image, label: str) -> Image.Image:
    img = ImageOps.contain(img.convert("RGB"), (1100, 620), method=Image.Resampling.LANCZOS)
    canvas = Image.new("RGB", (1100, 680), "white")
    canvas.paste(img, (0, 60))
    return canvas


def save_raw_vs_guided() -> None:
    raw = FIG / "raw_full_map_from_partial.png"
    guided = FIG / "map_50km.png"
    if not raw.exists() or not guided.exists():
        return
    raw_img = ImageOps.contain(Image.open(raw).convert("RGB"), (1040, 650), method=Image.Resampling.LANCZOS)
    guided_img = ImageOps.contain(Image.open(guided).convert("RGB"), (1040, 650), method=Image.Resampling.LANCZOS)
    fig, axes = plt.subplots(1, 2, figsize=(12.8, 4.8), dpi=190)
    for ax, img, title in [
        (axes[0], raw_img, "Visualização bruta da entrega parcial"),
        (axes[1], guided_img, "Camada final guiada por filtros"),
    ]:
        ax.imshow(img)
        ax.set_title(title, loc="left", fontsize=11, weight="bold")
        ax.axis("off")
    fig.tight_layout()
    fig.savefig(FIG / "visual_raw_vs_guided.png", bbox_inches="tight")
    plt.close(fig)


def main() -> int:
    FIG.mkdir(parents=True, exist_ok=True)
    data = load_layers()
    save_map_preset(data, "25km", "map_25km.png", "Preset 25 km: fluxos recorrentes selecionados", 900)
    save_map_preset(data, "50km", "map_50km.png", "Preset 50 km: corredores mais seletivos", 760)
    save_central_map(data)
    save_dependency_map(data)
    save_k_path_plot()
    save_regional_plot()
    save_partial_images()
    save_raw_vs_guided()
    print("generated report figures in", FIG)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
