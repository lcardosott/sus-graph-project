from __future__ import annotations

import textwrap
import os
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib-mc859")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "algorithm_layer" / "reports"
ANALYSIS = ROOT / "data_layer" / "reports" / "analysis"
OUT = ROOT / "report" / "figures"

BLUE = "#2f6f9f"
TEAL = "#4c9a8a"
ORANGE = "#d8893c"
RED = "#b9574f"
GRAY = "#6f7782"
LIGHT = "#eef2f4"


def setup() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update(
        {
            "figure.dpi": 140,
            "savefig.dpi": 220,
            "font.family": "DejaVu Sans",
            "axes.edgecolor": "#d0d7de",
            "axes.labelcolor": "#24292f",
            "xtick.color": "#24292f",
            "ytick.color": "#24292f",
            "axes.titleweight": "bold",
            "axes.titlesize": 12,
            "axes.labelsize": 10,
            "xtick.labelsize": 8.5,
            "ytick.labelsize": 9,
        }
    )


def save(fig: plt.Figure, name: str) -> None:
    fig.tight_layout()
    fig.savefig(OUT / name, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def wrap_label(value: str, width: int = 24) -> str:
    return "\n".join(textwrap.wrap(str(value), width=width, break_long_words=False))


def short_name(value: str, limit: int = 28) -> str:
    value = str(value).title()
    replacements = {
        "Hospital ": "Hosp. ",
        "Maternidade ": "Mat. ",
        "Fundacao ": "Fund. ",
        "Fundação ": "Fund. ",
        "Municipal ": "Mun. ",
        "Regional ": "Reg. ",
        "Referencia ": "Ref. ",
        "Referência ": "Ref. ",
    }
    for old, new in replacements.items():
        value = value.replace(old, new)
    return value if len(value) <= limit else value[: limit - 1].rstrip() + "…"


def node_names() -> dict[str, str]:
    nodes = pd.read_csv(ANALYSIS / "node_analysis_2021_public_hospitals.csv", sep=";")
    return dict(zip(nodes["node_id"], nodes["name"]))


def regional_cross_share() -> None:
    df = pd.read_csv(REPORTS / "regional_2021_public_hospitals_overall.csv")
    df["label"] = df.apply(
        lambda r: f"{int(r.distance_min_km)} km\n"
        + ("Residência" if r.edge_type == "residence" else "Transferência"),
        axis=1,
    )
    colors = [BLUE if x == "residence" else ORANGE for x in df["edge_type"]]

    fig, ax = plt.subplots(figsize=(7.1, 4.0))
    bars = ax.bar(df["label"], df["cross_health_region_share"] * 100, color=colors, width=0.62)
    ax.set_title("Fluxos recorrentes fora da região oficial de saúde")
    ax.set_ylabel("Parcela do fluxo (%)")
    ax.set_ylim(0, max(df["cross_health_region_share"] * 100) + 12)
    ax.grid(axis="y", color=LIGHT, linewidth=1)
    ax.set_axisbelow(True)
    for bar, share, flow in zip(bars, df["cross_health_region_share"] * 100, df["cross_health_region_flow"]):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 1.2,
            f"{share:.1f}%\n{flow:,.0f}".replace(",", "."),
            ha="center",
            va="bottom",
            fontsize=8.5,
        )
    ax.spines[["top", "right"]].set_visible(False)
    save(fig, "regional_cross_share.png")


def k_path_redundancy() -> None:
    df = pd.read_csv(REPORTS / "final_2021_public_hospitals_50km_k_path_redundancy.csv")
    names = node_names()
    df = df.sort_values(["no_alternative", "transfer_count"], ascending=[False, False]).head(14).copy()
    df["pair"] = df.apply(
        lambda r: f"{short_name(names.get(r.source_node_id, r.source_node_id), 22)} → "
        f"{short_name(names.get(r.target_node_id, r.target_node_id), 30)}",
        axis=1,
    )
    df["ratio_plot"] = df["k2_k1_ratio"].fillna(0)
    df = df.iloc[::-1]

    fig, ax = plt.subplots(figsize=(7.6, 5.9))
    colors = np.where(df["no_alternative"], RED, TEAL)
    bars = ax.barh(df["pair"].map(lambda x: wrap_label(x, 34)), df["ratio_plot"], color=colors)
    ax.set_title("Redundância dos k-caminhos no corte de 50 km")
    ax.set_xlabel("Razão entre o 2º e o 1º caminho (k2/k1)")
    ax.set_xlim(0, max(3.3, df["ratio_plot"].max() + 0.25))
    ax.grid(axis="x", color=LIGHT, linewidth=1)
    ax.set_axisbelow(True)
    for bar, row in zip(bars, df.itertuples()):
        text = "sem alternativa" if row.no_alternative else f"{row.k2_k1_ratio:.2f}x"
        x = 0.05 if row.no_alternative else row.ratio_plot + 0.04
        ha = "left"
        ax.text(x, bar.get_y() + bar.get_height() / 2, text, va="center", ha=ha, fontsize=8.5)
    ax.spines[["top", "right"]].set_visible(False)
    save(fig, "k_path_redundancy.png")


def sensitivity_matrix_50km() -> None:
    df = pd.read_csv(REPORTS / "final_2021_public_hospitals_50km_sensitivity_matrix.csv")
    pivot_edges = df.pivot(index="min_residence_count", columns="min_transfer_count", values="edges").sort_index()
    pivot_share = (
        df.pivot(index="min_residence_count", columns="min_transfer_count", values="largest_component_share")
        .sort_index()
        .loc[pivot_edges.index, pivot_edges.columns]
    )

    fig, ax = plt.subplots(figsize=(6.2, 4.6))
    matrix = pivot_edges.values / 1000
    im = ax.imshow(matrix, cmap="YlGnBu", aspect="auto")
    ax.set_title("Sensibilidade dos filtros no corte de 50 km")
    ax.set_xlabel("Mínimo de transferências")
    ax.set_ylabel("Mínimo de internações residência → hospital")
    ax.set_xticks(range(len(pivot_edges.columns)), [int(x) for x in pivot_edges.columns])
    ax.set_yticks(range(len(pivot_edges.index)), [int(x) for x in pivot_edges.index])
    for i in range(matrix.shape[0]):
        for j in range(matrix.shape[1]):
            ax.text(
                j,
                i,
                f"{matrix[i, j]:.1f}k\n{pivot_share.values[i, j] * 100:.1f}%",
                ha="center",
                va="center",
                fontsize=8.5,
                color="#10202d" if matrix[i, j] < matrix.max() * 0.72 else "white",
            )
    cbar = fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    cbar.set_label("Arestas filtradas (mil)")
    save(fig, "sensitivity_matrix_50km.png")


def regional_mismatch_top() -> None:
    df = pd.read_csv(REPORTS / "regional_2021_public_hospitals_regional_mismatch.csv")
    df = df[(df["distance_min_km"] == 50.0) & (df["edge_type"] == "residence")].nlargest(10, "flow").copy()
    df["corridor"] = df["source_health_region_id"] + " -> " + df["target_health_region_id"]
    df = df.iloc[::-1]

    fig, ax = plt.subplots(figsize=(7.2, 4.8))
    bars = ax.barh(df["corridor"], df["flow"] / 1000, color=BLUE)
    ax.set_title("Principais corredores entre regiões oficiais (50 km)")
    ax.set_xlabel("Fluxo recorrente de residência (mil internações)")
    ax.grid(axis="x", color=LIGHT, linewidth=1)
    ax.set_axisbelow(True)
    for bar, row in zip(bars, df.itertuples()):
        ax.text(
            bar.get_width() + 0.25,
            bar.get_y() + bar.get_height() / 2,
            f"{row.flow/1000:.1f}k\n{row.share_of_source_region_flow*100:.0f}%",
            va="center",
            fontsize=8.3,
        )
    ax.spines[["top", "right"]].set_visible(False)
    save(fig, "regional_mismatch_top.png")


def communities_summary_50km() -> None:
    df = pd.read_csv(REPORTS / "final_2021_public_hospitals_50km_communities.csv")
    df = df.nlargest(12, "nodes").sort_values("nodes").copy()
    labels = [f"C{int(c)}" for c in df["community_id"]]

    fig, ax = plt.subplots(figsize=(7.2, 4.8))
    ax.barh(labels, df["municipality_nodes"], color=TEAL, label="Municípios")
    ax.barh(labels, df["facility_nodes"], left=df["municipality_nodes"], color=ORANGE, label="Hospitais")
    ax.set_title("Maiores comunidades Louvain no corte de 50 km")
    ax.set_xlabel("Número de nós")
    ax.grid(axis="x", color=LIGHT, linewidth=1)
    ax.set_axisbelow(True)
    ax.legend(frameon=False, loc="lower right")
    for y, row in enumerate(df.itertuples()):
        ax.text(row.nodes + 4, y, f"{int(row.nodes)} nós", va="center", fontsize=8.5, color=GRAY)
    ax.spines[["top", "right"]].set_visible(False)
    save(fig, "communities_summary_50km.png")


def main() -> None:
    setup()
    regional_cross_share()
    k_path_redundancy()
    sensitivity_matrix_50km()
    regional_mismatch_top()
    communities_summary_50km()


if __name__ == "__main__":
    main()
