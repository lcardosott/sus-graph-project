# Roteiro Da Apresentação Final

Arquivo dos slides: `presentation.html`

Formato: vídeo gravado em português, até 10 minutos, com participação equilibrada da dupla.

## Estratégia

A ordem recomendada é:

1. **Interface e tese**: começar mostrando o produto final, porque prende a atenção e deixa claro o que foi entregue.
2. **Como foi desenvolvido**: explicar dados, pipeline, transferências e algoritmos.
3. **Resultados por algoritmo**: explicar o que cada método faz e qual achado ele produziu.
4. **Objetivos, limitações e fechamento**: responder diretamente às perguntas obrigatórias.

Eu não colocaria limitações antes dos resultados. Para o avaliador, primeiro precisamos estabelecer o que o projeto conseguiu mostrar. Depois, no final, mostramos maturidade metodológica explicando o que ainda limita a interpretação e como mitigaríamos.

## Divisão Sugerida Entre A Dupla

**Pessoa 1: slides 1 a 6, de 0:00 a 4:35**

- abertura;
- demonstração curta da UI;
- pergunta central;
- dados e pipeline;
- transferência hospital-hospital.

**Pessoa 2: slides 7 a 15, de 4:35 a 10:00**

- algoritmos;
- resultados;
- objetivos cumpridos;
- limitações;
- conclusão.

Se quiserem equilibrar ainda mais, a Pessoa 1 pode fazer até o slide 7 e a Pessoa 2 começa no slide de Betweenness.

## Roadmap Com Tempo

| Slide | Tempo | Tema | Quem | Objetivo |
|---:|---:|---|---|---|
| 1 | 0:00-0:35 | Abertura | P1 | Identificar projeto e tese geral. |
| 2 | 0:35-1:20 | Interface | P1 | Mostrar rapidamente o que foi entregue. |
| 3 | 1:20-2:00 | Tese e escala | P1 | Fixar mensagem central e números. |
| 4 | 2:00-2:55 | Dados | P1 | Explicar SIH, CNES, IBGE e escopo. |
| 5 | 2:55-3:45 | Pipeline | P1 | Mostrar como registros viram instâncias. |
| 6 | 3:45-4:35 | Transferências | P1 | Explicar inferência hospital-hospital. |
| 7 | 4:35-5:20 | Visão dos algoritmos | P2 | Conectar métodos a perguntas. |
| 8 | 5:20-5:55 | Betweenness | P2 | Explicar polos estruturais. |
| 9 | 5:55-6:30 | Louvain | P2 | Explicar comunidades funcionais. |
| 10 | 6:30-7:05 | Supressão dinâmica | P2 | Explicar robustez global. |
| 11 | 7:05-7:40 | K-caminhos | P2 | Explicar redundância local. |
| 12 | 7:40-8:15 | Regiões oficiais | P2 | Mostrar desalinhamento regional. |
| 13 | 8:15-9:00 | Objetivos | P2 | Responder pergunta obrigatória 1. |
| 14 | 9:00-9:50 | Limitações | P2 | Responder pergunta obrigatória 2. |
| 15 | 9:50-10:00 | Fechamento | P2 | Encerrar com contribuição principal. |

## Roteiro De Fala

### Slide 1. Abertura

Tempo: 35s.

Fala sugerida:

"Olá, somos Lucas Cardoso e Ricardo Andrade Oliveira Bello. Nosso projeto é uma análise de fluxos e resiliência da rede hospitalar pública do SUS usando teoria dos grafos. A ideia foi transformar registros públicos de internação em uma rede: municípios e hospitais são nós, e os deslocamentos observados ou inferidos viram arestas."

Ponto-chave: não explicar todos os algoritmos ainda.

### Slide 2. Interface E Entrega

Tempo: 45s.

Fala sugerida:

"Antes de entrar na metodologia, vale mostrar o produto final. A visualização foi pensada como uma leitura guiada, não como um desenho bruto de todas as 188 mil arestas. O mapa tem presets para 25 km, 50 km, hospitais centrais, stress test e dependências municipais. Isso nos permite apresentar o resultado de forma mais leve e legível."

Se forem demonstrar a UI ao vivo, fazer no máximo 30 segundos:

- abrir a home ou `graph_map_ui.html`;
- mostrar preset `50 km`;
- ativar hospitais centrais;
- mostrar a aba de metodologia ou dashboard;
- voltar ao slide.

Frase de transição:

"Com essa interface em mente, agora vamos explicar o que essa rede representa."

### Slide 3. Tese E Escala

Tempo: 40s.

Fala sugerida:

"A tese central é que a rede pública hospitalar parece conectada quando olhamos o agregado nacional, mas essa conectividade pode esconder dependências locais. Trabalhamos com 11,6 milhões de linhas SIH reconciliadas, 5.875 hospitais públicos/SUS e cerca de 188 mil arestas no grafo final."

Ponto-chave: enfatizar a diferença entre conectividade global e vulnerabilidade local.

### Slide 4. Dados

Tempo: 55s.

Fala sugerida:

"As três bases principais foram SIH/SUS, CNES e IBGE. O SIH/SUS fornece as internações: município de residência, hospital, diagnóstico, procedimento, permanência, valor, óbito e sinais de transferência. O CNES identifica os estabelecimentos, permite filtrar hospitais públicos/SUS e traz regiões oficiais de saúde. O IBGE entra com códigos territoriais e apoio geográfico. O pacote derivado usado no projeto foi publicado no Zenodo com DOI, para permitir auditoria externa."

Ponto-chave: citar o DOI apenas uma vez, sem gastar muito tempo.

### Slide 5. Pipeline

Tempo: 50s.

Fala sugerida:

"O pipeline seleciona os arquivos SIH por ano, mês e UF, enriquece com CNES e IBGE, gera nós e arestas e depois cria instâncias filtradas. Usamos dois filtros principais: recorrência e distância. Fluxos município para hospital entram com pelo menos 5 ocorrências; transferências hospital para hospital entram com pelo menos 2. Depois analisamos cortes de 25 km e 50 km para destacar deslocamentos não triviais."

Ponto-chave: explicar que o filtro evita casos isolados, como uma viagem excepcional.

### Slide 6. Transferências

Tempo: 50s.

Fala sugerida:

"A parte mais delicada foi inferir transferências hospitalares. A base não tem, de forma universal, uma aresta limpa origem-destino, nem identificador individual perfeito. Então usamos uma chave de paciente provável: nascimento, sexo, idade e município de residência. Em pandas, agrupamos as internações por essa chave, ordenamos por data e, quando há sinal de transferência, buscamos apenas dentro do mesmo grupo uma nova internação entre 24 e 48 horas após a saída. O destino é o CNES da próxima internação candidata, com continuidade clínica por capítulo CID-10."

Ponto-chave: dizer explicitamente:

"Isso não é rastreamento individual perfeito; é continuidade provável agregada."

### Slide 7. Algoritmos

Tempo: 45s.

Fala sugerida:

"A partir das instâncias filtradas, cada algoritmo responde a uma pergunta diferente. Betweenness procura hospitais-ponte. Louvain procura comunidades funcionais. A supressão dinâmica testa robustez ao remover polos centrais. K-caminhos mede se a segunda rota é próxima ou apenas uma alternativa formal no grafo."

Complemento técnico:

"Os fluxos são dirigidos na construção, mas as análises estruturais usam uma projeção não dirigida ponderada, porque queremos medir conectividade assistencial aproximada, não simular uma rota clínica literal."

### Slide 8. Algoritmo 1: Betweenness

Tempo: 35s.

Fala sugerida:

"A centralidade de intermediação pergunta quais hospitais aparecem em muitos caminhos mínimos. No nosso caso, isso aponta hospitais que conectam municípios e regiões. O resultado importante é que a métrica não mede apenas volume: ela destacou hospitais especializados, universitários e regionais como polos estruturais."

Ponto-chave: explicar a diferença entre hospital volumoso e hospital-ponte.

### Slide 9. Algoritmo 2: Louvain

Tempo: 35s.

Fala sugerida:

"O Louvain procura comunidades com muitas conexões internas e poucas conexões externas. Ele não usa a região oficial como entrada; a comunidade surge dos próprios fluxos. No corte de 50 km, encontramos 43 comunidades, o que dá uma leitura funcional da rede e permite comparar esses blocos com as regiões de saúde."

Ponto-chave: comunidade Louvain não é região oficial; é agrupamento funcional observado.

### Slide 10. Algoritmo 3: Supressão Dinâmica

Tempo: 35s.

Fala sugerida:

"A supressão dinâmica remove o hospital mais central, recalcula a centralidade e mede a maior componente depois de cada remoção. O resultado foi que, mesmo após cinco remoções no corte de 50 km, a maior componente manteve 99,47% dos nós. Isso sugere robustez global, mas não garante boa alternativa local."

Ponto-chave: robustez global não elimina vulnerabilidade municipal.

### Slide 11. Algoritmo 4: K-Caminhos

Tempo: 35s.

Fala sugerida:

"K-caminhos pergunta se existe uma segunda rota e se ela é realmente próxima. Entre os pares com alternativa, a razão média entre o segundo e o primeiro caminho foi 3,05; em 27 de 29 casos, a segunda rota ficou acima de duas vezes a primeira. Então a rede pode estar conectada, mas a redundância pode ser fraca."

Ponto-chave: alternativa topológica não é automaticamente alternativa assistencial boa.

### Slide 12. Comparação Regional

Tempo: 35s.

Fala sugerida:

"Depois dos algoritmos, comparamos os fluxos com as regiões oficiais de saúde. No corte de 50 km, 43,80% do fluxo residência-hospital filtrado cruza região oficial. Isso não significa necessariamente cruzar estado, porque REGSAUDE divide regiões dentro da mesma UF. O achado é que o fluxo observado nem sempre acompanha o desenho administrativo."

Ponto-chave: evitar confusão entre região de saúde e UF.

### Slide 13. Objetivos Da Proposta

Tempo: 45s.

Pergunta obrigatória:

"Em que medida os objetivos especificados na Proposta de Projeto foram cumpridos ou alcançados e por quê?"

Fala sugerida:

"Os objetivos foram cumpridos em essência. A proposta previa capturar dados públicos de saúde, construir instâncias de grafo e aplicar algoritmos para analisar estrutura e vulnerabilidade. Isso foi entregue: temos coleta e tratamento, grafo nacional, filtros, centralidade, comunidades, stress test, k-caminhos, análise regional, relatório, dataset com DOI e visualização. A principal mudança foi de escala: o escopo final ficou Brasil 2021 e hospitais públicos/SUS, o que aumentou o tamanho da análise e tornou a comparação regional mais rica."

Ponto-chave: tratar mudança de escopo como decisão técnica, não desvio.

### Slide 14. Limitações E Mitigação

Tempo: 50s.

Pergunta obrigatória:

"Quais são as limitações do projeto? Como modificaria fases do projeto para mitigar?"

Fala sugerida:

"As limitações principais são quatro. Primeiro, o SIH mostra acesso realizado, não demanda reprimida. Segundo, volume anual não mede capacidade simultânea, leitos disponíveis ou especialidade. Terceiro, distância Haversine não é tempo real de viagem. Quarto, transferências são inferidas por continuidade provável, não por identificador perfeito. Para mitigar, eu modificaria a captura de dados integrando leitos por competência, ocupação, especialidades e, se disponível, registros de regulação. Na definição das instâncias, usaria tempo de deslocamento por malha viária. E nos algoritmos, faria uma análise mais focalizada por estado ou macrorregião para validar resultados com dados locais."

Ponto-chave: responder dados, instâncias e algoritmos, como pedido.

### Slide 15. Fechamento

Tempo: 10s.

Fala sugerida:

"Em resumo, grafos não substituem planejamento em saúde, mas ajudam a tornar visível onde a rede observada depende demais de poucos caminhos. Essa foi a principal contribuição do projeto: transformar dados públicos em uma rede documentada, analisável e visualizável."

Final:

"Obrigado."

## Checklist Para Gravar

- Abrir `presentation.html` em tela cheia.
- Ensaiar sem demo ao vivo: deve ficar entre 8min30s e 9min30s.
- Se fizer demo da UI, limitar a 30 segundos.
- Não ler todos os bullets; usar os slides como mapa.
- Garantir que a fala sobre objetivos e limitações fique explícita, porque é critério obrigatório.
- Em dupla, manter divisão equilibrada: cerca de 5 minutos para cada pessoa.
- Exportar em `.mp4`, arquivo único, até 250 MB.

## Revisão Crítica De Design

O deck foi ajustado para parecer uma apresentação pública, não um roteiro interno. Foram removidas instruções visíveis como "durante o vídeo", "ato 1/2/3", "vídeo até 10 minutos" e chamadas de resposta obrigatória. Essas informações ficam apenas neste roteiro.

Decisões de espaçamento e layout:

- O rodapé agora usa três colunas fixas: legenda, barra de progresso com largura constante e contador. Assim a barra não muda de tamanho conforme o texto do slide.
- Os slides usam cartões e grids simples para evitar centralização excessiva e reduzir risco de texto se sobrepor a imagens.
- As imagens foram limitadas a poucos slides, com `object-fit: contain`, borda leve e legenda curta. Isso evita cortes estranhos e mantém a tela respirável.
- A apresentação usa fundo claro, contraste alto e poucos acentos de cor. Isso deve gravar melhor em vídeo e comprimir melhor em MP4.
- Os textos foram mantidos curtos nos slides; explicações longas ficam na fala.

Pontos para observar no ensaio:

- Se algum slide parecer cheio, priorizar cortar fala, não aumentar texto.
- O slide de transferência é o mais técnico; falar devagar e não tentar explicar todos os detalhes além da chave provável, janela 24-48h e CNES de destino.
- O slide de limitações tem bastante conteúdo; se passar de tempo, focar em três mitigadores: leitos/ocupação, tempo real de deslocamento e validação regional local.
- Se a gravação for em 720p, usar zoom do navegador em 90% ou 100%; evitar 110% porque pode apertar os slides com tabela.

## Perguntas Técnicas Possíveis

**Por que usar projeção não dirigida?**

Porque a direção é fundamental para construir fluxos, mas algumas métricas de conectividade ficariam dominadas pela orientação município-hospital. A projeção ponderada permite estudar conectividade estrutural aproximada.

**A transferência identifica o paciente real?**

Não perfeitamente. Ela usa uma chave de paciente provável e janela temporal de 24-48h. O resultado é uma continuidade provável agregada.

**Fora da região significa fora do estado?**

Não. Região oficial de saúde é mais granular que UF. Muitos cruzamentos são dentro do mesmo estado.

**A rede é robusta ou vulnerável?**

As duas coisas em escalas diferentes. Globalmente, a maior componente é robusta; localmente, alguns municípios dependem de poucos destinos.

**Por que não usar Google Drive para os dados?**

Porque o dataset foi publicado no Zenodo com DOI, acesso aberto e manifesto, o que é mais adequado para avaliação acadêmica.
