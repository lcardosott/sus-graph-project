from __future__ import annotations

import json
import os
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib-mc859")

import matplotlib.pyplot as plt
import numpy as np


ROOT = Path(__file__).resolve().parents[1]
LAYERS = ROOT / "viz_layer" / "reports" / "final_map_layers.json"
OUT = ROOT / "report" / "figures"

ORANGE = "#d56b2d"
BLUE = "#2f6f9f"
GREEN = "#2a7f62"
RED = "#b94747"
GRAY = "#6f7782"
LIGHT = "#eef2f4"


def load_layers() -> dict:
    return json.loads(LAYERS.read_text(encoding="utf-8"))


def setup_axis(ax: plt.Axes, title: str) -> None:
    ax.set_title(title, fontsize=12, weight="bold")
    ax.set_xlim(-74.5, -33.5)
    ax.set_ylim(-34.5, 6.0)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.grid(color=LIGHT, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.spines[["top", "right"]].set_visible(False)


def draw_edges(ax: plt.Axes, edges: list[dict], limit: int) -> None:
    selected = edges[:limit]
    max_flow = max(edge["transfer_count"] for edge in selected) if selected else 1
    for edge in selected:
        color = ORANGE if edge["edge_type"] == "residence" else BLUE
        alpha = 0.18 if edge["edge_type"] == "residence" else 0.42
        lw = 0.25 + 2.6 * np.sqrt(edge["transfer_count"] / max_flow)
        ax.plot(
            [edge["source_lon"], edge["target_lon"]],
            [edge["source_lat"], edge["target_lat"]],
            color=color,
            linewidth=lw,
            alpha=alpha,
            solid_capstyle="round",
            zorder=1,
        )


def draw_central(ax: plt.Axes, nodes: list[dict], labels: int = 4) -> None:
    if not nodes:
        return
    max_b = max(node["betweenness"] for node in nodes)
    for node in nodes:
        size = 55 + 360 * (node["betweenness"] / max_b)
        ax.scatter(node["lon"], node["lat"], s=size, color=GREEN, edgecolor="white", linewidth=0.8, alpha=0.92, zorder=4)
    for node in nodes[:labels]:
        ax.text(
            node["lon"] + 0.55,
            node["lat"] + 0.25,
            f"{node['rank']}. {node['name'].title()}",
            fontsize=7.2,
            color="#1f2428",
            bbox=dict(facecolor="white", edgecolor="none", alpha=0.78, pad=1.5),
            zorder=5,
        )


def save(fig: plt.Figure, name: str) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(OUT / name, bbox_inches="tight", facecolor="white", dpi=220)
    plt.close(fig)


def map_overview_25_50(data: dict) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(10.8, 5.2), sharex=True, sharey=True)
    for ax, key, title in zip(axes, ["25km", "50km"], ["Corte 25 km", "Corte 50 km"]):
        preset = data["presets"][key]
        setup_axis(ax, title)
        draw_edges(ax, preset["edges"], limit=900)
        draw_central(ax, preset["central_nodes"][:12], labels=0)
        ax.text(
            0.03,
            0.04,
            "laranja: residência → hospital\nazul: hospital → hospital\nverde: hospitais centrais",
            transform=ax.transAxes,
            fontsize=8,
            color=GRAY,
            bbox=dict(facecolor="white", edgecolor="#d0d7de", alpha=0.9, pad=4),
        )
    save(fig, "map_overview_25_50.png")


def map_central_hospitals(data: dict) -> None:
    fig, ax = plt.subplots(figsize=(7.2, 6.2))
    preset = data["presets"]["50km"]
    setup_axis(ax, "Hospitais centrais e corredores principais (50 km)")
    draw_edges(ax, preset["edges"], limit=520)
    draw_central(ax, preset["central_nodes"], labels=7)
    save(fig, "map_central_hospitals_50km.png")


def map_dependency(data: dict) -> None:
    fig, ax = plt.subplots(figsize=(7.2, 6.2))
    setup_axis(ax, "Exemplos de dependência municipal")
    examples = data["presets"]["dependency"]["examples"]
    max_flow = max(example["total_flow"] for example in examples)
    for example in examples:
        source = example["source"]
        target = example["target"]
        lw = 0.8 + 3.0 * np.sqrt(example["total_flow"] / max_flow)
        ax.plot(
            [source["lon"], target["lon"]],
            [source["lat"], target["lat"]],
            color=RED,
            linewidth=lw,
            alpha=0.55,
            zorder=1,
        )
        ax.scatter(source["lon"], source["lat"], s=55, color=BLUE, edgecolor="white", linewidth=0.7, zorder=3)
        ax.scatter(target["lon"], target["lat"], s=80, color=GREEN, edgecolor="white", linewidth=0.7, zorder=4)
    for example in examples[:5]:
        source = example["source"]
        target = example["target"]
        ax.text(
            source["lon"] + 0.45,
            source["lat"] + 0.2,
            source["name"],
            fontsize=7.4,
            bbox=dict(facecolor="white", edgecolor="none", alpha=0.82, pad=1.5),
            zorder=5,
        )
        ax.text(
            target["lon"] + 0.45,
            target["lat"] - 0.35,
            target["name"].title(),
            fontsize=7.0,
            color="#15543f",
            bbox=dict(facecolor="white", edgecolor="none", alpha=0.82, pad=1.5),
            zorder=5,
        )
    ax.text(
        0.03,
        0.04,
        "azul: município de origem\nverde: hospital de destino\nvermelho: fluxo concentrado fora da região",
        transform=ax.transAxes,
        fontsize=8,
        color=GRAY,
        bbox=dict(facecolor="white", edgecolor="#d0d7de", alpha=0.9, pad=4),
    )
    save(fig, "map_dependency_examples.png")


def main() -> None:
    data = load_layers()
    map_overview_25_50(data)
    map_central_hospitals(data)
    map_dependency(data)


if __name__ == "__main__":
    main()
