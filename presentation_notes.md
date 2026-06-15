# 1. Presentation Strategy

## Core Narrative

O projeto parte de uma tensão simples: o SUS é planejado por regiões e referências, mas o atendimento real aparece nos dados como deslocamentos entre municípios e hospitais. A apresentação deve mostrar que transformar esses deslocamentos em grafo permite enxergar polos estruturais, fluxos inter-regionais, redundância limitada e dependências municipais que não aparecem quando se olha apenas a conectividade nacional.

## Target Audience

Público acadêmico e técnico de MC859, com familiaridade geral em grafos, mas sem conhecimento interno do projeto, das bases do DATASUS ou das decisões de implementação.

## Suggested Duration

12 a 15 minutos no fluxo principal. Os slides verticais servem como aprofundamento técnico e material de Q&A; nem todos precisam ser apresentados se o tempo for curto.

## Main Message

A rede pública hospitalar brasileira de 2021 aparece conectada no agregado, mas essa conectividade convive com dependências locais relevantes. O valor do projeto está em construir uma rede auditável, aplicar algoritmos de grafos e expor essas dependências com resultados e visualizações.

## What Should Be Emphasized

- A pergunta de pesquisa: conectividade agregada não garante alternativas locais.
- A escala da base: 11,6 milhões de linhas SIH, 5.875 hospitais públicos/SUS e 188 mil arestas no escopo público.
- A metodologia: construção de instâncias, filtros de recorrência/distância, projeção ponderada e algoritmos clássicos de grafos.
- Os resultados principais: polos centrais, cruzamento de regiões oficiais, robustez global e redundância local limitada.
- As limitações: acesso realizado não é demanda total, distância geodésica não é tempo real e transferências são inferidas.

## What Should Be Avoided

- Explicar todos os scripts ou todos os arquivos do repositório no fluxo principal.
- Vender o grafo como ferramenta operacional de encaminhamento hospitalar.
- Dizer que cruzar região oficial significa cruzar estado.
- Esconder que muitas coordenadas hospitalares vêm de centróides municipais.
- Dedicar tempo demais a detalhes de implementação que pertencem ao backup.

# 2. Slide Map

| Horizontal Topic | Vertical Depth | Slide Title | Purpose | Presenter | Time |
|---:|---:|---|---|---|---:|
| 1 | 0 | Análise de Fluxos e Resiliência do SUS via Teoria dos Grafos | Identificar projeto, autores e tema. | P1 | 0:40 |
| 1 | 1 | A rede é conectada no agregado... | Abrir com a tese central e escala. | P1 | 0:50 |
| 2 | 0 | Regionalizar atendimento é administrativo; acessar atendimento é fluxo observado | Motivar problema. | P1 | 1:00 |
| 2 | 1 | O problema foi traduzido para quatro leituras operacionais | Mostrar como grafos tornam a pergunta mensurável. | P1 | 0:45 |
| 3 | 0 | Dados públicos de internação, estabelecimentos e território | Apresentar fontes. | P1 | 0:55 |
| 3 | 1 | Brasil 2021, hospitais públicos/SUS | Dar escala e limites do recorte. | P1 | 0:45 |
| 4 | 0 | Pipeline para instâncias filtradas de grafos | Explicar metodologia sem sobrecarregar. | P1 | 1:00 |
| 4 | 1 | Transferências inferidas de forma conservadora | Aprofundar inferência hospital-hospital. | P1/P2 | 0:50 |
| 4 | 2 | Projeção para algoritmos | Justificar grafo não dirigido ponderado. | P2 | 0:50 |
| 5 | 0 | Arquitetura em camadas | Mostrar organização técnica. | P2 | 0:55 |
| 5 | 1 | Resultados auditáveis | Preparar defesa de reprodutibilidade. | P2 | 0:35 |
| 6 | 0 | Cada algoritmo responde a uma pergunta | Conectar algoritmos a perguntas. | P2 | 1:20 |
| 6 | 1 | Stress test | Backup técnico para supressão dinâmica. | P2 | 0:40 |
| 7 | 0 | Três evidências sustentam a conclusão | Abrir resultados. | P3 | 0:45 |
| 7 | 1 | Centralidade destaca hospitais que organizam corredores | Discutir polos estruturais. | P3 | 1:00 |
| 7 | 2 | Fora da região não significa necessariamente outro estado | Explicar REGSAUDE e achado regional. | P3 | 1:05 |
| 7 | 3 | Segunda rota não significa boa alternativa | Explicar k-caminhos e redundância. | P3 | 1:00 |
| 8 | 0 | Interface final guiada | Mostrar visualização como comunicação. | P3 | 0:55 |
| 8 | 1 | Camadas da demonstração | Backup para demo. | P3 | 0:35 |
| 9 | 0 | Limitações | Mostrar maturidade metodológica. | P3 | 1:00 |
| 9 | 1 | Continuidade | Trabalhos futuros realistas. | P3 | 0:45 |
| 10 | 0 | Rede nacional documentada, analisável e visualizável | Concluir contribuição. | P3 | 0:45 |
| 10 | 1 | Mensagem final | Encerrar com frase forte. | P3 | 0:25 |

# 3. Complete HTML Slide Deck

O deck completo foi criado em `presentation.html`. Ele é um arquivo standalone com HTML, CSS e JavaScript internos, pronto para abrir no navegador.

Navegação:

- `ArrowRight`: próximo tópico horizontal.
- `ArrowLeft`: tópico anterior.
- `ArrowDown`: aprofundamento dentro do tópico atual.
- `ArrowUp`: retorno ao slide superior do tópico.

Imagens referenciadas pelo deck:

- `report/figures/directed_projection.png`
- `report/figures/stress_test_algorithm.png`
- `report/figures/map_central_hospitals.png`
- `viz_layer/reports/top_central_facilities_public_hospitals.png`
- `report/figures/regional_cross_share.png`
- `report/figures/k_path_redundancy.png`
- `report/figures/visual_raw_vs_guided.png`
- `report/figures/map_dependency_examples.png`

# 4. Presenter Script

Versão balanceada para 3 apresentadores. Se forem apenas 2 pessoas, junte P1 com metade de P2, e deixe resultados, limitações e conclusão para P2. Se forem 4 pessoas, separe visualização e limitações em um quarto bloco.

## Presenter 1: Abertura, Motivação, Dados e Metodologia

### Slide 1.0: Title

Tempo: 40s.

Abrir dizendo: "Boa tarde. Nosso projeto analisa fluxos hospitalares do SUS usando teoria dos grafos. A ideia central é transformar registros públicos de internação em uma rede, onde municípios e hospitais são nós, e os deslocamentos observados viram arestas."

Nota: não explique ainda todos os algoritmos. Só situe o tema.

### Slide 1.1: Resumo em uma frase

Tempo: 50s.

Falar: "A mensagem principal é que a rede pública hospitalar parece conectada quando observada nacionalmente, mas isso não significa que todos os municípios tenham alternativas locais boas. Trabalhamos com 11,6 milhões de linhas SIH, 5.875 hospitais públicos/SUS e 188 mil arestas no escopo final."

Ênfase: pausar após "conectada no agregado".

### Slide 2.0: Motivação

Tempo: 1min.

Falar: "O SUS tem regionalização e referências administrativas, mas o atendimento real acontece por fluxos. Um paciente mora em um município, é internado em um hospital, e às vezes continua o atendimento em outro. A pergunta é: esses fluxos respeitam a rede regional ou revelam dependências externas?"

Transição: "Para responder isso, precisamos transformar a pergunta em medidas."

### Slide 2.1: Leituras Operacionais

Tempo: 45s.

Falar: "Usamos grafos para medir quatro coisas: conectividade global, polos estruturais, redundância local e desalinhamento com regiões oficiais. Isso evita uma análise puramente descritiva e permite comparar cenários."

Backup: se perguntarem, explique que maior componente mede conectividade global, betweenness mede ponte, e k-caminhos mede alternativas.

### Slide 3.0: Dados

Tempo: 55s.

Falar: "As três bases principais foram SIH/SUS, CNES e IBGE. O SIH traz internações e município de residência; o CNES identifica hospitais, vínculo SUS e região oficial de saúde; o IBGE apoia localização territorial."

Nota: defina CNES e SIH lentamente, porque nem todo avaliador lembra.

### Slide 3.1: Escala

Tempo: 45s.

Falar: "O recorte final é Brasil 2021, hospitais públicos/SUS. Um ponto importante é que só 569 hospitais tinham coordenada CNES direta; para os demais usamos centróide municipal. Isso mantém cobertura nacional, mas limita análise intraurbana."

Ênfase: mostrar maturidade metodológica, não pedir desculpa.

### Slide 4.0: Pipeline

Tempo: 1min.

Falar: "O pipeline seleciona arquivos SIH por ano, mês e UF, enriquece com CNES e IBGE, constrói fluxos e gera instâncias filtradas. Depois aplicamos filtros de recorrência e distância mínima. Os cortes de 25 km e 50 km não são percentuais; são limiares geográficos."

Transição para P2: "A partir dessas instâncias, a parte central do projeto foi escolher como modelar e analisar a rede."

### Slide 4.1: Transferências

Tempo: 50s.

Falar: "As transferências não vêm como uma lista limpa origem-destino em todos os casos. Por isso usamos uma heurística conservadora: sinal administrativo de transferência, nova internação em outro hospital em 24 a 48 horas, sexo e idade compatíveis e continuidade por capítulo CID-10."

Nota: diga "continuidade provável agregada", não "rastreamento individual".

## Presenter 2: Implementação e Algoritmos

### Slide 4.2: Projeção

Tempo: 50s.

Falar: "O grafo original é dirigido porque os fluxos têm origem e destino. Mas para centralidade, comunidades, stress test e k-caminhos usamos uma projeção não dirigida ponderada. Isso evita que as métricas sejam dominadas pela direção administrativa município-hospital e foca na conectividade estrutural."

Apontar para a figura: "Volume é agregado; distância fica como custo."

### Slide 5.0: Arquitetura

Tempo: 55s.

Falar: "A implementação foi organizada em camadas. A data_layer prepara dados e tabelas; a algorithm_layer executa os métodos; a viz_layer transforma resultados em mapas e camadas interativas. Essa separação facilitou validação, reexecução e rastreabilidade."

### Slide 5.1: Código e Reprodutibilidade

Tempo: 35s.

Falar: "A saída não é só o PDF. Cada algoritmo gera CSVs, JSONs e figuras. Isso permite auditar centralidade, comunidades, stress test, k-caminhos e análise regional diretamente no repositório."

### Slide 6.0: Algoritmos

Tempo: 1min20s.

Falar: "Cada algoritmo responde a uma pergunta. Betweenness identifica hospitais que funcionam como pontes. Louvain encontra blocos funcionais. A supressão dinâmica testa robustez ao remover hospitais centrais. K-caminhos avalia se existe alternativa topológica próxima."

Nota: não entre em fórmulas aqui. O objetivo é conectar método e pergunta.

Transição para P3: "Com esses métodos, chegamos a três evidências principais."

### Slide 6.1: Stress Test

Tempo: 40s, usar se houver tempo ou pergunta.

Falar: "O stress test recalcula centralidade a cada passo, remove o hospital mais central e mede maior componente e caminho médio amostrado. A conclusão foi que a rede nacional é globalmente robusta, mas isso não elimina fragilidades municipais."

## Presenter 3: Resultados, Visualização, Limitações e Conclusão

### Slide 7.0: Resultados

Tempo: 45s.

Falar: "Os resultados podem ser organizados em três evidências: hospitais de referência aparecem como polos, muitos fluxos cruzam regiões oficiais e algumas localidades dependem de poucos destinos externos."

### Slide 7.1: Polos Estruturais

Tempo: 1min.

Falar: "A centralidade não mede apenas volume. Ela mede o quanto um hospital aparece em caminhos entre outros nós. Por isso hospitais especializados e regionais aparecem como estruturadores da rede, não apenas como estabelecimentos grandes."

Nota: aponte para mapa e ranking, mas não leia todos os nomes.

### Slide 7.2: Regiões Oficiais

Tempo: 1min05s.

Falar: "Um resultado importante é que 'fora da região' não significa necessariamente outro estado. O campo REGSAUDE distingue regiões dentro da mesma UF. No corte de 50 km, 43,80% do fluxo residência-hospital filtrado cruza região oficial, enquanto cruzamentos de UF são uma parcela bem menor."

Ênfase: este slide responde uma confusão provável dos avaliadores.

### Slide 7.3: Redundância

Tempo: 1min.

Falar: "K-caminhos mostra que uma alternativa pode existir no grafo e ainda ser ruim como substituição. Entre os pares avaliados com segunda alternativa, a razão média entre o segundo e o primeiro caminho foi 3,05; em 27 de 29 casos, a razão passou de 2."

### Slide 8.0: Visualização

Tempo: 55s.

Falar: "A visualização final não tenta mostrar todas as arestas ao mesmo tempo. A experiência bruta ficava pesada e ilegível. A versão final usa camadas e presets para mostrar a mensagem analítica: hospitais centrais, stress test, dependências e corredores."

### Slide 8.1: Camadas da Demonstração

Tempo: 35s.

Falar: "Se houver tempo de demonstração, vale abrir o mapa interativo e mostrar rapidamente presets de 50 km, hospitais centrais e dependência municipal. Não navegar demais; usar a demo para reforçar os resultados."

### Slide 9.0: Limitações

Tempo: 1min.

Falar: "A leitura precisa ser cuidadosa. O SIH mostra internações que aconteceram, não demanda reprimida. Volume anual não é capacidade em uma semana específica. A distância Haversine não é tempo de viagem. E as transferências são inferidas de forma agregada."

Nota: este slide deve soar como rigor, não fraqueza.

### Slide 9.1: Continuidade

Tempo: 45s.

Falar: "Uma continuidade natural seria reduzir o escopo para um estado ou macrorregião, integrar dados locais de leitos, ocupação e especialidade, e trocar distância geodésica por tempo real de deslocamento."

### Slide 10.0: Conclusão

Tempo: 45s.

Falar: "O projeto entregou uma rede nacional documentada, analisável e visualizável. A contribuição principal é mostrar que conectividade nacional pode esconder dependências locais, e que grafos ajudam a tornar essas dependências explícitas."

### Slide 10.1: Mensagem Final

Tempo: 25s.

Fechar dizendo: "Grafos não substituem planejamento em saúde, mas ajudam a enxergar onde a rede observada depende demais de poucos caminhos. Obrigado."

# 5. Backup Q&A Notes

## Technical Questions

**Por que usar projeção não dirigida se os fluxos são dirigidos?**  
Porque a direção é essencial para construir o fluxo, mas algumas métricas de conectividade ficariam dominadas por arestas município-hospital sem retorno. A projeção permite estudar conectividade estrutural aproximada, não rotas clínicas literais.

**Por que 25 km e 50 km?**  
São limiares geográficos para destacar deslocamentos não triviais. O corte de 25 km captura fricções regionais; 50 km é mais seletivo e destaca dependências mais longas.

**K-caminhos representa uma rota real de ambulância?**  
Não. Representa redundância topológica na projeção. É uma aproximação para comparar se há alternativa estrutural próxima.

**Betweenness não favorece hospitais em regiões mais densas?**  
Pode favorecer nós que conectam muitos caminhos. Por isso a interpretação foi qualitativa e estrutural, combinada com mapas, dependência municipal e análise regional.

## Methodology Questions

**Como foram inferidas transferências?**  
Por sinal administrativo, janela temporal de 24-48h, compatibilidade demográfica e continuidade por capítulo CID-10. Arestas indicam continuidade provável agregada.

**Por que o número de coordenadas CNES diretas é baixo?**  
Porque o catálogo usado não forneceu ponto físico confiável para a maioria dos hospitais no recorte. Para manter cobertura nacional, o projeto usou centróides municipais quando necessário.

**O ano de 2021 é representativo?**  
Deve ser lido com cautela por causa da COVID-19. Os resultados descrevem acesso realizado naquele ano, não uma estrutura permanente.

## Result Questions

**A rede é robusta ou vulnerável?**  
As duas coisas em escalas diferentes. Globalmente, a maior componente permanece alta no stress test; localmente, alguns municípios dependem fortemente de poucos destinos.

**Fluxo fora da região significa fora do estado?**  
Não. REGSAUDE é região oficial de saúde e vários cruzamentos acontecem dentro da mesma UF.

**O que significa participação de 100% em dependência municipal?**  
Significa 100% dentro do fluxo filtrado recorrente e distante daquele município, não 100% de todas as internações do município.

## Limitation Questions

**O projeto mede falta de vaga?**  
Não. Mede acesso realizado. Não observa demanda reprimida, tempo de espera ou capacidade simultânea.

**Por que não usar leitos como capacidade?**  
Seria ideal, mas exigiria integração temporal e por especialidade. O projeto não fabricou capacidade a partir de volume anual.

**A visualização mostra todas as arestas?**  
Não no modo final. Ela usa camadas reduzidas para legibilidade e desempenho.

# 6. Final Review Checklist

- Verificar se `presentation.html` abre corretamente no navegador.
- Testar navegação com setas direita, esquerda, baixo e cima.
- Confirmar que todos os caminhos de imagem carregam a partir da raiz do repositório.
- Ensaiar uma versão de 12 minutos sem slides verticais opcionais.
- Ensaiar uma versão de 15 minutos incluindo os principais backups.
- Confirmar que a divisão de fala corresponde ao número real de apresentadores.
- Não ler os bullets; usar os slides como guia visual.
- Evitar dizer que REGSAUDE é estado.
- Reforçar que k-caminhos não é rota clínica operacional.
- Reforçar que o SIH mostra acesso realizado, não demanda reprimida.
- Testar o mapa interativo antes da apresentação, caso ele seja mostrado ao vivo.
- Manter o relatório e o deck consistentes em números: 11,6 milhões de linhas, 5.875 hospitais, 188.035 arestas, 43,80% no cruzamento residência-hospital a 50 km, razão média 3,05 nos k-caminhos.
