# polybet-livro-extrai

Extrai, de arquivos públicos de livro da Polymarket (dl.pendulumflow.com: pmxt v2, ag6 e v3), o livro por minuto dos mercados
de temperatura máxima listados em `catalog/` (dado público). Uma linha por mercado e minuto (05–23 h local), negócios e cobertura por hora.

- `extrai_v2.py`: pmxt v2 (13/04–09/08/2026) + ag6 (10/08–15/08). `extrai_v3.py`: PendulumFlow v3 (18/08 em diante).
- `plano.py SRC H0 H1 HORAS_POR_BLOCO [PREFIXO]` gera a lista de blocos; o workflow `extrai` roda um job por bloco e publica
  `TAG.tar.gz` (min/, neg/, cov/) na Release indicada.
