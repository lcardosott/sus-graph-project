#!/usr/bin/env python3
"""Generate conceptual report figures for the MC859 final report."""

from __future__ import annotations

import os
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib-mc859")

import matplotlib.pyplot as plt
import networkx as nx
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch, Rectangle


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "report" / "figures"

COLORS = {
    "municipio": "#4C78A8",
    "hospital": "#59A14F",
    "transfer": "#E15759",
    "edge": "#5F6368",
    "accent": "#F28E2B",
    "purple": "#B07AA1",
    "blue_light": "#DCEBFA",
    "green_light": "#DDEFD8",
    "red_light": "#F9DEDC",
    "gray_light": "#F4F5F7",
    "text": "#202124",
}


def setup(width: float = 11, height: float = 6.2):
    fig, ax = plt.subplots(figsize=(width, height), dpi=180)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")
    return fig, ax


def box(ax, xy, w, h, text, fc, ec=None, fontsize=12, weight="normal"):
    x, y = xy
    patch = FancyBboxPatch(
        (x, y),
        w,
        h,
        boxstyle="round,pad=0.018,rounding_size=0.018",
        linewidth=1.4,
        edgecolor=ec or "#C7CBD1",
        facecolor=fc,
    )
    ax.add_patch(patch)
    ax.text(
        x + w / 2,
        y + h / 2,
        text,
        ha="center",
        va="center",
        fontsize=fontsize,
        color=COLORS["text"],
        fontweight=weight,
        linespacing=1.15,
    )
    return patch


def arrow(ax, start, end, color=None, lw=2.0, style="-|>", rad=0.0, label=None, label_xy=None):
    patch = FancyArrowPatch(
        start,
        end,
        arrowstyle=style,
        mutation_scale=15,
        linewidth=lw,
        color=color or COLORS["edge"],
        connectionstyle=f"arc3,rad={rad}",
    )
    ax.add_patch(patch)
    if label:
        lx = (start[0] + end[0]) / 2 if label_xy is None else label_xy[0]
        ly = (start[1] + end[1]) / 2 if label_xy is None else label_xy[1]
        ax.text(
            lx,
            ly,
            label,
            ha="center",
            va="center",
            fontsize=10,
            color=COLORS["text"],
            bbox=dict(boxstyle="round,pad=0.25", facecolor="white", edgecolor="none", alpha=0.92),
        )


def title(ax, text, fontsize=14):
    ax.text(0.04, 0.93, text, fontsize=fontsize, fontweight="bold", color=COLORS["text"], va="top")


def save(fig, name):
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / name
    fig.savefig(path, bbox_inches="tight", pad_inches=0.18)
    plt.close(fig)
    return path


def graph_modeling():
    fig, ax = setup(width=11, height=5.8)
    title(ax, "Modelagem do grafo assistencial")

    main_y, main_h = 0.58, 0.15
    box(ax, (0.07, main_y), 0.23, main_h, "Município de\nresidência", COLORS["blue_light"], COLORS["municipio"], weight="bold")
    box(ax, (0.385, main_y), 0.23, main_h, "Hospital público\n(SUS)", COLORS["green_light"], COLORS["hospital"], weight="bold")
    box(ax, (0.70, main_y), 0.23, main_h, "Hospital público\n(SUS)", COLORS["green_light"], COLORS["hospital"], weight="bold")

    y_mid = main_y + main_h / 2
    arrow(ax, (0.30, y_mid), (0.385, y_mid), COLORS["municipio"], label="internações de residentes", label_xy=(0.342, 0.79))
    arrow(ax, (0.615, y_mid), (0.70, y_mid), COLORS["transfer"], label="transferências inferidas", label_xy=(0.657, 0.79))

    box(ax, (0.18, 0.30), 0.25, 0.12, "Peso da aresta\nvolume agregado", "#FFF2CC", "#D6A500", fontsize=10.5)
    box(ax, (0.57, 0.30), 0.25, 0.12, "Custo espacial\ndistância em km", "#E6F2F0", "#4E9A8E", fontsize=10.5)
    arrow(ax, (0.305, 0.43), (0.305, 0.56), "#D6A500", lw=1.6, style="->")
    arrow(ax, (0.695, 0.43), (0.695, 0.56), "#4E9A8E", lw=1.6, style="->")

    legend_y = 0.14
    ax.scatter([0.11, 0.11], [legend_y, legend_y - 0.055], s=[180, 180], c=[COLORS["municipio"], COLORS["hospital"]])
    ax.text(0.15, legend_y, "nó municipal", va="center", fontsize=9.8)
    ax.text(0.15, legend_y - 0.055, "nó hospitalar", va="center", fontsize=9.8)
    ax.plot([0.46, 0.56], [legend_y, legend_y], color=COLORS["municipio"], lw=2.2)
    ax.plot([0.46, 0.56], [legend_y - 0.055, legend_y - 0.055], color=COLORS["transfer"], lw=2.2)
    ax.text(0.59, legend_y, "fluxo residência-hospital", va="center", fontsize=9.8)
    ax.text(0.59, legend_y - 0.055, "fluxo hospital-hospital", va="center", fontsize=9.8)

    return save(fig, "graph_modeling.png")


def transfer_matching_heuristic():
    fig, ax = setup(width=11, height=5.8)
    title(ax, "Heurística de pareamento de transferências")

    timeline_y = 0.55
    ax.plot([0.10, 0.90], [timeline_y, timeline_y], color="#9AA0A6", lw=2.2)
    for x in [0.18, 0.50, 0.82]:
        ax.plot([x, x], [timeline_y - 0.035, timeline_y + 0.035], color="#5F6368", lw=2)

    top_y, top_h = 0.68, 0.12
    box(ax, (0.06, top_y), 0.24, top_h, "Saída com sinal\nde transferência", COLORS["red_light"], COLORS["transfer"], fontsize=10.8, weight="bold")
    box(ax, (0.38, top_y), 0.24, top_h, "Janela temporal\n24-48 horas", "#FFF2CC", "#D6A500", fontsize=10.8, weight="bold")
    box(ax, (0.70, top_y), 0.24, top_h, "Nova internação\nem outro hospital", COLORS["green_light"], COLORS["hospital"], fontsize=10.8, weight="bold")

    arrow(ax, (0.30, top_y + top_h / 2), (0.38, top_y + top_h / 2), "#D6A500")
    arrow(ax, (0.62, top_y + top_h / 2), (0.70, top_y + top_h / 2), "#D6A500")

    filters = [
        ("hospital de\norigem distinto", 0.17),
        ("sexo e idade\ncompatíveis", 0.39),
        ("capítulo CID-10\nconsistente", 0.61),
        ("desempate por\ntempo e CID exato", 0.83),
    ]
    for text, x in filters:
        box(ax, (x - 0.092, 0.23), 0.184, 0.115, text, COLORS["gray_light"], "#C7CBD1", fontsize=9.2)
        arrow(ax, (x, 0.36), (x, timeline_y - 0.02), "#80868B", lw=1.4, style="->")

    ax.text(
        0.5,
        0.08,
        r"Resultado: aresta dirigida $h_i \rightarrow h_j$ ponderada pelo número de transferências recorrentes.",
        ha="center",
        va="center",
        fontsize=11,
        color=COLORS["text"],
    )
    return save(fig, "transfer_matching_heuristic.png")


def directed_projection():
    fig, ax = setup(width=11, height=5.8)
    title(ax, "Do grafo dirigido à projeção não dirigida ponderada")

    # Left: directed graph
    left_nodes = {"M1": (0.12, 0.62), "M2": (0.12, 0.36), "H1": (0.34, 0.67), "H2": (0.34, 0.42), "H3": (0.34, 0.22)}
    for n, (x, y) in left_nodes.items():
        color = COLORS["municipio"] if n.startswith("M") else COLORS["hospital"]
        ax.scatter(x, y, s=520, color=color, edgecolor="white", linewidth=1.5, zorder=3)
        ax.text(x, y, n, color="white", ha="center", va="center", fontweight="bold", fontsize=11, zorder=4)
    for a, b, c in [("M1", "H1", COLORS["municipio"]), ("M1", "H2", COLORS["municipio"]), ("M2", "H2", COLORS["municipio"]), ("H1", "H2", COLORS["transfer"]), ("H2", "H3", COLORS["transfer"])]:
        arrow(ax, left_nodes[a], left_nodes[b], c, lw=1.8)
    ax.text(0.23, 0.13, "grafo dirigido original", ha="center", fontsize=11, fontweight="bold")

    arrow(ax, (0.45, 0.50), (0.56, 0.50), COLORS["edge"], lw=2.4)
    ax.text(0.505, 0.64, "agregação\nponderada", ha="center", va="center", fontsize=10, color=COLORS["text"])
    ax.text(0.505, 0.36, "soma volume\nmenor distância", ha="center", va="center", fontsize=9.3, color="#5F6368")

    # Right: undirected weighted projection
    G = nx.Graph()
    G.add_weighted_edges_from([("M1", "H1", 4), ("M1", "H2", 2), ("M2", "H2", 5), ("H1", "H2", 3), ("H2", "H3", 2)])
    right_pos = {"M1": (0.63, 0.62), "M2": (0.63, 0.36), "H1": (0.84, 0.67), "H2": (0.84, 0.42), "H3": (0.84, 0.22)}
    for u, v, w in G.edges.data("weight"):
        ax.plot([right_pos[u][0], right_pos[v][0]], [right_pos[u][1], right_pos[v][1]], color="#777777", lw=1.2 + w * 0.35, alpha=0.85, zorder=1)
    for n, (x, y) in right_pos.items():
        color = COLORS["municipio"] if n.startswith("M") else COLORS["hospital"]
        ax.scatter(x, y, s=520, color=color, edgecolor="white", linewidth=1.5, zorder=3)
        ax.text(x, y, n, color="white", ha="center", va="center", fontweight="bold", fontsize=11, zorder=4)
    ax.text(0.74, 0.13, "projeção para algoritmos", ha="center", fontsize=11, fontweight="bold")
    ax.text(0.74, 0.07, "centralidade, Louvain, stress test e k-caminhos", ha="center", fontsize=9.8)

    return save(fig, "directed_projection.png")


def stress_test_algorithm():
    fig, ax = setup()
    title(ax, "Stress test por remoção dinâmica")

    steps = [
        ("1. Calcular\nbetweenness", 0.06, COLORS["blue_light"], COLORS["municipio"]),
        ("2. Remover\nhospital central", 0.30, COLORS["red_light"], COLORS["transfer"]),
        ("3. Recalcular\nconectividade", 0.54, "#FFF2CC", "#D6A500"),
        ("4. Medir impacto\ncomponente e rotas", 0.78, COLORS["green_light"], COLORS["hospital"]),
    ]
    for text, x, fc, ec in steps:
        box(ax, (x, 0.65), 0.17, 0.15, text, fc, ec, fontsize=10.0, weight="bold")
    for start, end in [((0.23, 0.725), (0.30, 0.725)), ((0.47, 0.725), (0.54, 0.725)), ((0.71, 0.725), (0.78, 0.725))]:
        arrow(ax, start, end, COLORS["edge"], lw=2)

    pos = {"A": (0.20, 0.35), "B": (0.34, 0.43), "C": (0.34, 0.25), "D": (0.48, 0.35), "E": (0.62, 0.43), "F": (0.62, 0.25)}
    edges = [("A", "B"), ("A", "C"), ("B", "D"), ("C", "D"), ("D", "E"), ("D", "F"), ("E", "F")]
    for u, v in edges:
        ax.plot([pos[u][0], pos[v][0]], [pos[u][1], pos[v][1]], color="#999999", lw=2, zorder=1)
    for n, (x, y) in pos.items():
        central = n == "D"
        ax.scatter(x, y, s=480 if central else 320, color=COLORS["transfer"] if central else COLORS["hospital"], edgecolor="white", lw=1.5, zorder=3)
        ax.text(x, y, n, ha="center", va="center", color="white", fontweight="bold", zorder=4)
    ax.text(0.48, 0.14, "a remoção de um polo pode fragmentar caminhos ou alongar rotas alternativas", ha="center", fontsize=10.5)

    return save(fig, "stress_test_algorithm.png")


def louvain_concept():
    fig, ax = setup()
    title(ax, "Comunidades Louvain e modularidade", fontsize=15)

    G = nx.Graph()
    communities = {
        "A": ["A1", "A2", "A3", "A4"],
        "B": ["B1", "B2", "B3", "B4"],
        "C": ["C1", "C2", "C3"],
    }
    for nodes in communities.values():
        for i, u in enumerate(nodes):
            for v in nodes[i + 1 :]:
                G.add_edge(u, v)
    G.add_edges_from([("A3", "B1"), ("A4", "C2"), ("B4", "C1")])
    pos = {
        "A1": (0.20, 0.62), "A2": (0.32, 0.70), "A3": (0.32, 0.52), "A4": (0.20, 0.44),
        "B1": (0.57, 0.68), "B2": (0.72, 0.66), "B3": (0.72, 0.48), "B4": (0.56, 0.50),
        "C1": (0.40, 0.31), "C2": (0.54, 0.27), "C3": (0.47, 0.43),
    }
    color_by_node = {}
    palette = {"A": "#4C78A8", "B": "#59A14F", "C": "#F28E2B"}
    for c, nodes in communities.items():
        for n in nodes:
            color_by_node[n] = palette[c]

    for u, v in G.edges():
        same = color_by_node[u] == color_by_node[v]
        ax.plot([pos[u][0], pos[v][0]], [pos[u][1], pos[v][1]], color="#777777" if same else "#C44E52", lw=2 if same else 1.5, alpha=0.75 if same else 0.55, zorder=1)
    for n, (x, y) in pos.items():
        ax.scatter(x, y, s=430, color=color_by_node[n], edgecolor="white", lw=1.5, zorder=3)
        ax.text(x, y, n, color="white", ha="center", va="center", fontweight="bold", fontsize=9, zorder=4)

    box(ax, (0.08, 0.07), 0.25, 0.09, "muitas arestas\ndentro do grupo", COLORS["blue_light"], "#4C78A8", fontsize=10)
    box(ax, (0.38, 0.07), 0.25, 0.09, "poucas arestas\nentre grupos", COLORS["red_light"], "#C44E52", fontsize=10)
    box(ax, (0.68, 0.07), 0.23, 0.09, "maximiza\nmodularidade", COLORS["green_light"], "#59A14F", fontsize=10)
    ax.text(0.50, 0.84, "Comunidades indicam blocos funcionais de circulação hospitalar.", ha="center", fontsize=11)

    return save(fig, "louvain_concept.png")


def main():
    paths = [
        graph_modeling(),
        transfer_matching_heuristic(),
        directed_projection(),
        stress_test_algorithm(),
        louvain_concept(),
    ]
    for path in paths:
        print(path.relative_to(ROOT))


if __name__ == "__main__":
    main()
