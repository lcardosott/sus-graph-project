# Analise Final - Rede SUS 2021 com Hospitais Publicos

## 1. O Que Foi Feito

O projeto partiu dos dados nacionais do SIH/SUS de 2021 ja coletados e tratados. A decisao principal foi nao recolher os dados brutos: usamos os artefatos curados existentes e criamos novas camadas analiticas sobre eles.

Foram preservados backups dos artefatos anteriores e a analise final foi restringida a hospitais publicos/SUS. Para isso, usamos a referencia `cnes_br_2101_public_hospitals.csv` e filtramos a rede para manter:

- municipios como origem de pacientes;
- hospitais publicos como destino de internacoes;
- transferencias entre hospitais publicos;
- estabelecimentos fora do escopo apenas como QA/exclusao, nao como resultado final.

Tambem adicionamos um criterio de recorrencia para reduzir casos isolados:

- fluxo municipio -> hospital entra na analise apenas com pelo menos 5 ocorrencias;
- fluxo hospital -> hospital entra na analise apenas com pelo menos 2 ocorrencias.

Depois disso, rodamos duas versoes principais da rede:

- rede com distancia minima de 25 km;
- rede com distancia minima de 50 km.

A analise de capacidade foi rebaixada para trabalho futuro. Ela nao aparece como conclusao central porque o SIH anual mostra volume no ano, mas nao ocupacao simultanea, entrada e saida diaria, equipe disponivel ou leitos operacionais por momento.

## 2. O Que Foi Analisado

A rede foi interpretada como um grafo de acesso hospitalar:

- nos de municipio representam origem/residencia dos pacientes;
- nos de hospital representam hospitais publicos;
- arestas municipio -> hospital representam deslocamento para internacao;
- arestas hospital -> hospital representam transferencia entre hospitais;
- peso da aresta representa volume recorrente;
- distancia da aresta representa deslocamento geografico aproximado.

Foram avaliadas quatro dimensoes principais.

### 2.1 Estrutura Da Rede

Medimos tamanho da rede, numero de arestas, componentes conectados e comunidades detectadas por Louvain.

### 2.2 Centralidade Hospitalar

Medimos hospitais com alta betweenness centrality. Em termos praticos, esses hospitais aparecem como pontes importantes entre partes da rede. Eles nao sao apenas hospitais grandes; sao hospitais que conectam fluxos de municipios e regioes diferentes.

### 2.3 Stress Test

Removemos iterativamente hospitais centrais e recalculamos a conectividade da rede. A pergunta nao foi "o SUS tem capacidade?", mas sim:

> se hospitais estruturalmente importantes saem da rede, o grafo nacional se fragmenta ou continua conectado?

### 2.4 Regioes Oficiais De Saude

Construimos uma referencia municipio -> regiao de saude a partir do campo `REGSAUDE` dos arquivos CNES/ST de janeiro de 2021.

Essa referencia cobriu:

- 27 arquivos ST estaduais;
- 334.702 linhas de estabelecimentos CNES;
- 5.570 municipios;
- 1.250 municipios com conflito de codigo regional entre estabelecimentos;
- 239 municipios sem regiao identificada.

Esse ponto e importante: o proprio dado regional nao e perfeitamente limpo. Por isso, a analise regional deve ser lida como uma forte aproximacao oficial baseada no CNES, nao como verdade administrativa perfeita.

## 3. Resultados Principais

### 3.1 Escopo Dos Dados

A base final conciliada possui:

- 11.629.005 linhas SIH reconciliadas;
- 11.823 nos no grafo original;
- 203.184 arestas no grafo original;
- 5.875 hospitais publicos na referencia;
- 5.875 hospitais publicos presentes no grafo;
- 188.035 arestas no recorte de hospitais publicos;
- 181.569 fluxos residencia -> hospital;
- 6.466 fluxos hospital -> hospital;
- 377 estabelecimentos nao publicos excluidos do escopo final.

### 3.2 Comparacao 25 km vs 50 km

Com distancia minima de 25 km:

- 8.622 nos;
- 61.706 arestas recorrentes;
- 49 comunidades Louvain;
- maior componente apos stress final: 99,9072% da rede;
- aumento de caminho medio apos cinco remocoes: 0,7839%.

Com distancia minima de 50 km:

- 7.750 nos;
- 47.739 arestas recorrentes;
- 43 comunidades Louvain;
- maior componente apos stress final: 99,4702% da rede;
- aumento de caminho medio apos cinco remocoes: 1,2597%.

Interpretacao imediata: ao subir de 25 km para 50 km, removemos muitos deslocamentos regionais menores e ficamos com uma rede mais "longa", mais seletiva e mais dependente de alguns corredores. Por isso, o impacto relativo no caminho medio fica maior no corte de 50 km.

### 3.3 Hospitais Mais Centrais

No corte de 25 km, os principais hospitais por betweenness foram:

1. FUNDACAO PIO XII BARRETOS
2. HOSPITAL AMARAL CARVALHO JAU
3. HOSPITAL MUNICIPAL ESAU MATOS
4. HOSPITAL ESTADUAL DA CRIANCA
5. HOSPITAL REGIONAL TIBERIO NUNES

No corte de 50 km, os principais foram:

1. FUNDACAO PIO XII BARRETOS
2. HOSPITAL AMARAL CARVALHO JAU
3. HOSPITAL SAO VICENTE DE PAULO
4. HOSPITAL REGIONAL TIBERIO NUNES
5. HOSPITAL MUNICIPAL ESAU MATOS

O resultado e coerente: hospitais especializados ou regionais aparecem como pontos de concentracao de fluxo, especialmente oncologia, alta complexidade e hospitais regionais em areas onde municipios menores dependem de referencia externa.

### 3.4 Stress Test

No corte de 25 km, os hospitais removidos nos cinco passos foram:

1. FUNDACAO PIO XII BARRETOS
2. HOSPITAL AMARAL CARVALHO JAU
3. HOSPITAL DE BASE DE SAO JOSE DO RIO PRETO
4. HOSPITAL DAS CLINICAS FAEPA RIBEIRAO PRETO
5. HOSPITAL DE AMOR JALES

Mesmo apos essas remocoes, o maior componente manteve 99,9072% dos nos.

No corte de 50 km, os removidos foram:

1. FUNDACAO PIO XII BARRETOS
2. HOSPITAL DAS CLINICAS FAEPA RIBEIRAO PRETO
3. HOSPITAL DE BASE DE SAO JOSE DO RIO PRETO
4. HOSPITAL AMARAL CARVALHO JAU
5. HOSPITAL ESTADUAL DA CRIANCA

Mesmo assim, o maior componente manteve 99,4702% dos nos.

Esse resultado nao significa que o SUS esta bem organizado ou que nao ha problema de acesso. Significa apenas que, no nivel agregado nacional, a rede continua conectada. A conectividade global e uma metrica grosseira: ela pode esconder danos locais severos.

A leitura correta e:

> O grafo nacional parece robusto, mas a robustez agregada pode ocultar dependencia local. Um municipio pode perder seu hospital de referencia e sofrer grande impacto, enquanto a media nacional quase nao muda.

### 3.5 Regioes Oficiais E Deslocamento Fora Da Regiao

Para fluxos residencia -> hospital com pelo menos 5 ocorrencias:

No corte de 25 km:

- 60.646 arestas;
- 3.488.440 internacoes/fluxos ponderados;
- 1.117.796 cruzam regiao oficial de saude;
- 32,04% do fluxo cruza regiao oficial;
- distancia media ponderada: 86,96 km.

No corte de 50 km:

- 46.938 arestas;
- 1.942.302 internacoes/fluxos ponderados;
- 850.778 cruzam regiao oficial de saude;
- 43,80% do fluxo cruza regiao oficial;
- distancia media ponderada: 128,20 km.

Para transferencias hospital -> hospital com pelo menos 2 ocorrencias:

No corte de 25 km:

- 1.060 arestas;
- 3.585 transferencias ponderadas;
- 52,11% cruzam regiao oficial;
- distancia media ponderada: 148,14 km.

No corte de 50 km:

- 801 arestas;
- 2.748 transferencias ponderadas;
- 60,99% cruzam regiao oficial;
- distancia media ponderada: 182,08 km.

Essa e uma das evidencias mais fortes do projeto: quando olhamos deslocamentos recorrentes e nao casos isolados, uma parte substancial do atendimento acontece fora da regiao oficial de saude do municipio de residencia. Isso fica ainda mais forte quando aumentamos a distancia minima.

### 3.6 Exemplos De Dependencia Municipal

Alguns municipios aparecem com 100% do fluxo filtrado indo para um unico hospital fora da sua regiao oficial. Exemplos no corte de 50 km:

- Serra Nova Dourada - MT -> HOSPITAL REGIONAL DE AGUA BOA, 96 fluxos, 241,2 km;
- Caiana - MG -> HOSPITAL DO CANCER DE MURIAE, 70 fluxos, 65,4 km;
- Dom Cavati - MG -> HOSPITAL MUNICIPAL ELIANE MARTINS, 58 fluxos, 50,1 km;
- Bujaru - PA -> HOSPITAL REGIONAL PUBLICO DR ABELARDO SANTOS, 47 fluxos, 56,1 km;
- Santana do Cariri - CE -> HOSPITAL MATERNIDADE SAO VICENTE DE PAULO, 41 fluxos, 50,8 km.

Esses casos sao importantes porque mostram o problema que a media nacional nao captura. Para a rede inteira, remover ou alterar um hospital pode parecer pouco. Para um municipio que depende de um unico destino, isso pode significar perda concreta de acesso.

### 3.7 Corredores Regionais Mais Fortes

Alguns corredores regiao -> regiao concentram grande fluxo recorrente. No corte de 25 km, exemplos:

- PE-0002 -> PE-0001: 24.832 fluxos, 87,89% do fluxo da regiao de origem;
- RS-0002 -> RS-0001: 22.908 fluxos, 78,82%;
- GO-0018 -> DF-0006: 22.820 fluxos, 79,38%;
- SP-0201 -> SP-0101: 22.354 fluxos, 75,26%;
- PE-0004 -> PE-0001: 19.561 fluxos, 52,38%.

Isso sugere que algumas regioes oficiais funcionam, na pratica, como areas de encaminhamento para polos externos. A regiao administrativa existe, mas o fluxo real do paciente revela outra organizacao funcional.

## 4. O Que Isso Significa

### 4.1 A Rede E Conectada, Mas Isso Nao Basta

O stress test mostra que a rede nacional nao se fragmenta facilmente. Isso poderia parecer uma boa noticia, mas a interpretacao correta e mais cuidadosa: conectividade global nao mede sofrimento local, tempo de deslocamento, barreira economica, disponibilidade real de vaga ou qualidade da referencia.

Portanto, o resultado nao deve ser apresentado como "o SUS e robusto". Melhor:

> A rede nacional tem alta conectividade estrutural, mas essa conectividade esconde dependencias locais e deslocamentos recorrentes para fora das regioes oficiais.

### 4.2 O Recorte Regional E Mais Perspicaz Que A Media Nacional

A analise por `REGSAUDE` mostra que parte relevante dos pacientes cruza sua regiao oficial para ser internada. Isso indica tensao entre planejamento regional e fluxo real.

Esse e um achado mais forte que simplesmente dizer "existem deslocamentos longos", porque conecta o deslocamento ao desenho institucional do sistema.

### 4.3 A Distancia De 25 km Ajuda A Ver Problemas Locais

O corte de 50 km identifica deslocamentos claramente longos. Ele e bom para mostrar dependencias regionais fortes.

O corte de 25 km e mais sensivel. Ele captura deslocamentos que ainda podem ser significativos para pacientes, especialmente em municipios pequenos, areas rurais, populacoes vulneraveis ou regioes com transporte dificil.

Por isso, os dois cortes devem ser usados juntos:

- 25 km mostra friccao de acesso regional/local;
- 50 km mostra dependencia mais grave e deslocamento longo.

### 4.4 O Projeto Deve Contar Uma Historia De Desalinhamento

A historia mais forte ate agora e:

1. O SUS tem uma rede nacional ampla e conectada.
2. Quando filtramos casos recorrentes, vemos que muitos fluxos nao sao isolados.
3. Uma parcela importante desses fluxos cruza regioes oficiais de saude.
4. Hospitais centrais funcionam como polos reais, muitas vezes acima do desenho regional.
5. A rede agregada parece robusta, mas municipios especificos podem depender de poucos destinos.
6. Portanto, o problema nao e apenas conectividade; e alinhamento entre acesso real, planejamento regional e vulnerabilidade local.

## 5. Limitacoes

As principais limitacoes sao:

- A base e anual, entao nao mede ocupacao simultanea.
- Capacidade real exigiria leitos por competencia, taxa de ocupacao, equipe, especialidade e disponibilidade temporal.
- A distancia e geografica aproximada, nao tempo real de deslocamento.
- `REGSAUDE` vem do CNES/ST e apresentou conflitos em alguns municipios.
- A centralidade usa amostragem para viabilizar processamento em maquina local.
- O grafo usa fluxos observados, nao necessariamente necessidade real reprimida.

## 6. Conclusao Executiva

O projeto avancou de uma visualizacao de fluxos para uma analise estrutural da rede publica hospitalar. A principal evidencia encontrada e que os deslocamentos recorrentes de pacientes frequentemente atravessam regioes oficiais de saude, especialmente quando consideramos distancias acima de 50 km.

O stress test mostra que a rede nacional continua conectada mesmo apos remover hospitais centrais, mas isso nao deve ser interpretado como ausencia de problema. Pelo contrario: a analise municipal mostra que varios municipios dependem fortemente de um unico hospital fora da sua regiao.

Assim, o principal resultado e:

> A rede publica hospitalar brasileira de 2021 apresenta alta conectividade agregada, mas tambem revela dependencias regionais e municipais que indicam desalinhamento entre o desenho oficial das regioes de saude e os fluxos reais de atendimento.

