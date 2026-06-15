# SUS Graph Project

Entrega final da disciplina MC859: análise de fluxos e resiliência da rede hospitalar pública do SUS em 2021 usando Teoria dos Grafos.

## Links rápidos

- **Site / visualização interativa:** https://lcardosott.github.io/sus-graph-project/
- **Mapa interativo direto:** https://lcardosott.github.io/sus-graph-project/viz_layer/graph_map_ui.html
- **Grafo completo antigo:** https://lcardosott.github.io/sus-graph-project/viz_layer/reports/graph_sih_br_2021_all.html
- **Apresentação HTML:** https://lcardosott.github.io/sus-graph-project/presentation.html
- **Relatório final em PDF:** [report/final_report.pdf](report/final_report.pdf)
- **Fonte LaTeX do relatório:** [report/final_report.tex](report/final_report.tex)
- **Roteiro da apresentação:** [presentation_notes.md](presentation_notes.md)
- **Resumo interpretativo dos resultados:** [algorithm_layer/reports/final_analysis_interpretation.md](algorithm_layer/reports/final_analysis_interpretation.md)
- **Documentos de apoio:** [Proposta_MC859.pdf](Proposta_MC859.pdf) e [entrega_parcial.pdf](entrega_parcial.pdf)

Se o GitHub Pages ainda não estiver ativo, abra localmente:

```bash
python3 -m http.server 8000
```

Depois acesse:

```text
http://localhost:8000/viz_layer/graph_map_ui.html
```

## O que foi entregue

O projeto constrói e analisa um grafo nacional de internações do SUS em 2021. Os nós representam municípios de residência e hospitais públicos; as arestas representam fluxos município -> hospital e transferências hospital -> hospital inferidas por heurística conservadora.

Artefatos principais:

- [data_layer](data_layer): seleção, preparação e enriquecimento dos dados.
- [model_layer](model_layer): construção do grafo anual.
- [filter_engine](filter_engine): filtros reutilizáveis de recorrência, tipo e distância.
- [algorithm_layer](algorithm_layer): centralidade, Louvain, stress test, k-caminhos e análises regionais.
- [viz_layer](viz_layer): mapa interativo, camadas leves e figuras.
- [report](report): relatório final, figuras e referências.
- [presentation.html](presentation.html): apresentação HTML estática para GitHub Pages.
- [presentation_notes.md](presentation_notes.md): estratégia, mapa de slides, roteiro e Q&A.

## Visualização final

A visualização principal é [viz_layer/graph_map_ui.html](viz_layer/graph_map_ui.html). Ela usa o arquivo leve [viz_layer/reports/final_map_layers.json](viz_layer/reports/final_map_layers.json), gerado a partir das análises finais de hospitais públicos.

Também foi mantida a versão exploratória antiga com todos os nós em [viz_layer/reports/graph_sih_br_2021_all.html](viz_layer/reports/graph_sih_br_2021_all.html). Ela é útil como referência visual do grafo bruto, mas pode ser pesada em navegadores comuns.

A apresentação final está disponível em [presentation.html](presentation.html). O deck é estático, funciona no GitHub Pages e usa navegação por teclado: setas direita/esquerda mudam os tópicos principais, e setas baixo/cima acessam aprofundamentos.

Presets disponíveis no mapa:

- `25 km`: fluxos recorrentes com distância mínima de 25 km.
- `50 km`: fluxos recorrentes mais longos e seletivos.
- `Hospitais centrais`: hospitais públicos com maior centralidade de intermediação.
- `Dependências`: exemplos municipais com forte dependência de um destino externo.

O HTML é estático e deve funcionar no GitHub Pages quando servido a partir da raiz do repositório.

## Resultados principais

Resumo do recorte final:

- 11.629.005 linhas SIH reconciliadas.
- 5.875 hospitais públicos presentes no grafo.
- 188.035 arestas no recorte público.
- 61.706 arestas recorrentes no corte de 25 km.
- 47.739 arestas recorrentes no corte de 50 km.
- 43 comunidades Louvain na instância de 50 km.

O achado central é que a rede pública hospitalar é conectada no agregado nacional, mas possui dependências locais: fluxos recorrentes atravessam regiões oficiais de saúde, hospitais especializados aparecem como polos estruturais e alguns municípios dependem de poucos destinos externos.

## Reproduzir a análise final

Instale dependências:

```bash
pip install -r requirements.txt
```

Recrie as tabelas analíticas de hospitais públicos a partir dos dados curados locais:

```bash
python data_layer/build_public_hospital_analysis.py \
  --year 2021 \
  --out-dir data_layer/reports/analysis \
  --prefix 2021_public_hospitals
```

Execute os algoritmos finais:

```bash
python algorithm_layer/public_hospital_analysis.py \
  --out-dir algorithm_layer/reports \
  --prefix final_2021_public_hospitals_25km \
  --distance-bands-km 25

python algorithm_layer/public_hospital_analysis.py \
  --out-dir algorithm_layer/reports \
  --prefix final_2021_public_hospitals_50km \
  --distance-bands-km 50

python algorithm_layer/regional_flow_analysis.py
```

Gere as camadas leves e figuras:

```bash
python viz_layer/build_final_map_layers.py
python scripts/report_figures_conceptual.py
python scripts/report_figures_results.py
python scripts/report_figures_maps.py
```

Compile o relatório:

```bash
cd report
pdflatex final_report.tex
bibtex final_report
pdflatex final_report.tex
pdflatex final_report.tex
```

## GitHub Pages

Configuração recomendada no GitHub:

1. Acesse `Settings -> Pages`.
2. Em `Build and deployment`, selecione `Deploy from a branch`.
3. Branch: `gh-pages`.
4. Pasta: `/ (root)`.
5. Salve e aguarde a publicação.

Com essa configuração, a página inicial será [index.html](index.html), com links para o relatório e para o mapa final.

## Observações sobre dados

Dados brutos e intermediários grandes não precisam ser enviados ao GitHub. A entrega versiona o código, os artefatos finais leves, as figuras, o relatório e os arquivos necessários para a visualização. Arquivos Parquet curados, DBFs, caches, tabelas analíticas grandes e grafos completos pesados devem ficar fora do versionamento; eles podem ser recriados localmente pelos scripts.
