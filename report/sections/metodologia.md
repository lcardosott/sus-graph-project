# Metodologia

O projeto modela fluxos de internacao do SUS como um grafo direcionado e ponderado. Os nos representam municipios de residencia e hospitais publicos; as arestas representam dois fenomenos: deslocamento municipio -> hospital para internacao e transferencia hospital -> hospital inferida por heuristica temporal, demografica e clinica.

A base final usa SIH/SUS 2021, CNES e referencias municipais do IBGE. O recorte final foi nacional, pois a escala brasileira aumenta a robustez estatistica e atende ao requisito de grafo com mais de 10 mil vertices. Para reduzir ruido de casos isolados, foram aplicados filtros de recorrencia: no minimo 5 ocorrencias para fluxos municipio -> hospital e 2 ocorrencias para transferencias hospital -> hospital.

A distancia geografica foi calculada por Haversine a partir das coordenadas dos nos. Como parte das coordenadas de estabelecimentos vem de fallback por centroide municipal, a distancia deve ser interpretada como aproximacao espacial, nao como tempo real de deslocamento.

Os algoritmos aplicados foram: centralidade de intermedicao para detectar hospitais ponte; Louvain para comunidades de fluxo; supressao dinamica de hospitais centrais; k-caminhos mais curtos para redundancia; e fluxo maximo/corte minimo com capacidade empirica proxy, baseada em volume observado.
