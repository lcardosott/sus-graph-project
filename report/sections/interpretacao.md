# Interpretacao

O resultado central nao e que o SUS seja simplesmente robusto. A leitura correta e mais sutil: a rede nacional permanece conectada no agregado, mas essa conectividade global esconde dependencias locais importantes.

Municipios como Serra Nova Dourada (MT), Caiana (MG), Dom Cavati (MG), Bujaru (PA) e Santana do Cariri (CE) aparecem com dependencia forte de um unico destino hospitalar fora da regiao oficial quando aplicamos o corte de 50 km. Para esses casos, uma falha local pode ser muito relevante mesmo que a metrica nacional mude pouco.

Os hospitais centrais encontrados tem coerencia institucional: muitos sao hospitais oncologicos, universitarios, de alta complexidade ou regionais. Isso reforca a hipotese de que o grafo captura polos reais de referencia assistencial.

As principais limitacoes sao: ausencia de capacidade temporal real, uso de distancia geografica e nao tempo de viagem, coordenadas aproximadas por centroide em parte dos CNES, amostragem em metricas caras e conflitos no campo REGSAUDE. Trabalhos futuros devem incorporar leitos operacionais por competencia, ocupacao, especialidades, transporte e validacao qualitativa com gestores regionais.
