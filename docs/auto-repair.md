# AUTO-REPAIR: Normalização, Enriquecimento e Telemetria

Este documento descreve as configurações e o comportamento do mecanismo de AUTO-REPAIR do projeto, incluindo normalizações, validações, quarentena e telemetria.

## Perfis de Reparação
- `AUTO_REPAIR_LEVEL` (`none`, `basic`, `aggressive`)
  - `none`: desativa normalizações no hook; somente validações posteriores (FK, constraints).
  - `basic`: normalizações essenciais (CNPJ partes com tamanho, CEP 8 dígitos, UF válida, CNAE principal 7 dígitos, emails básicos, arrays secundários em `{...}`); valores inválidos podem ser nulos.
  - `aggressive`: adiciona heurísticas (dedup/sort em CNAE secundário, email estrito, exemplos de E.164, enriquecimento por CEP e por nome do município) e telemetria detalhada.

## Validações de Consistência
- CPF/CNPJ: valida dígitos verificadores; inválidos viram `NULL` nos campos de pessoas (`socios`) e geram telemetria.
- CNPJ em `estabelecimentos`: linhas com CNPJ completo inválido são quarentenadas e puladas no COPY.

## Quarentena
- Saída: `logs/quarantine/<YYYYMMDD>/<tabela>.jsonl`
- Razões:
  - `critical_fields_null`: identificadores críticos nulos por tabela
  - `fk_violation`: violações de domínio/FK (modo não estrito normaliza para `NULL`)
  - `invalid_cnpj`: CNPJ completo inválido em `estabelecimentos`
- Cada registro inclui linha completa, campos afetados, chunk e timestamp.

## Telemetria de Reparos
- Saída: `logs/auto_repair/<YYYYMMDD>/<tabela>.jsonl` (rotação diária, capping por tamanho)
- Conteúdo por chunk:
  - `level`: perfil ativo
  - `after_nulls` e `null_deltas`: contagem de nulos por coluna e variação
  - `sample_diffs` (aggressive): exemplos `antes→depois` por coluna
  - `invalid_ids` e `invalid_id_examples`: estatísticas de identificadores inválidos
  - `e164_examples` (aggressive): exemplos de canônicos `+55<DDD><Telefone>`
  - `cep_enrichment` (aggressive): métricas e exemplos de inferência/correção

## Enriquecimento por CEP
- Flags:
  - `ENABLE_CEP_ENRICHMENT` (bool): ativa enriquecimento por dataset externo
  - `CEP_MAP_PATH`: CSV com mapeamentos (formatos abaixo)
  - `CEP_CORRECT_UF_ONLY_IF_NULL` (bool): se `true`, corrige `UF` apenas quando nula; se `false`, também corrige divergências
- Formatos aceitos:
  - Exato: colunas `cep`, `uf`, `municipio_codigo`
  - Prefixo: colunas `cep_prefix` (5 dígitos), `uf`, `municipio_codigo`
- Ações (aggressive):
  - Preenche `municipio_codigo` ausente; define `municipio_source` como `cep_map_exact` ou `cep_map_prefix`
  - Preenche/ajusta `uf` conforme `CEP_CORRECT_UF_ONLY_IF_NULL`; define `uf_source`

## Mapeamento por Nome do Município
- Flag: `MUNICIPIO_NAME_MAP_PATH`: CSV com `municipio_nome`, `municipio_codigo` e opcional `uf`
- Ação (aggressive):
  - Infere `municipio_codigo` ausente por match de `municipio_nome` (e `uf` quando presente); define `municipio_source = mun_name_map`

## Backfill de Domínios
- `ENABLE_CONSTRAINTS_BACKFILL` (bool): controla inserts de *“NÃO INFORMADO NA ORIGEM (...)”* em `constraints.sql`
  - `true`: habilita backfill pós-carga
  - `false`: desativa backfill (recomendado quando domínios oficiais estão completos)

## Tipos e Esquema
- `estabelecimentos.cnae_fiscal_secundaria`: `INT[]` para type-safety
- `estabelecimentos.uf_source` e `municipio_source`: rastreiam origem de correções
- `estabelecimentos.municipio_nome`: armazenado para análises e enriquecimentos

## Rotação e Cotas
- Rotação e Cotas
- `TELEMETRY_MAX_BYTES`, `QUARANTINE_MAX_BYTES`: tamanho máximo por arquivo `.jsonl`
- `TELEMETRY_ROTATE_DAILY`, `QUARANTINE_ROTATE_DAILY`: rotação diária de diretórios

## Portas de Qualidade (Quality Gates)
- `ENABLE_QUALITY_GATES`: ativa verificação por chunk
- `GATE_MAX_CHANGED_RATIO`: razão máxima de valores alterados por coluna
- `GATE_MAX_NULL_DELTA_RATIO`: razão máxima de variação de nulos por coluna
- `GATE_LOG_LEVEL`: nível de log (`INFO` ou `WARNING`) quando o gate dispara
- `GATE_MIN_ROWS`: mínimo de linhas no chunk para aplicar gates
- Ao disparar, o chunk é pulado; um registro `quality_gate` é escrito em telemetria e quarentena.

## Sumário e Métricas
- Sumário por tabela: `logs/auto_repair/<YYYYMMDD>/<tabela>_summary.json` com:
  - `rows_total`, `chunks_processed`, `quality_gate_chunks`, `invalid_cnpj_rows_skipped`
  - `changed_totals`, `null_delta_totals`
  - `column_stats` por coluna: `min`, `max`, `nulls` pós-reparo
- Métricas Prometheus (opcional):
  - `ENABLE_METRICS_PROMETHEUS` e `PROMETHEUS_METRICS_PATH` (arquivo de saída `.prom`)
  - Expostos como contadores simples:
    - `cnpj_auto_repair_rows_total{table="..."}`
    - `cnpj_auto_repair_quality_gate_chunks_total{table="..."}`
    - `cnpj_auto_repair_changed_total{table="...",column="..."}`
    - `cnpj_auto_repair_null_delta_total{table="...",column="..."}`
  - Push para Pushgateway:
    - `ENABLE_PROMETHEUS_PUSH`, `PROMETHEUS_PUSH_URL`, `PROMETHEUS_JOB`, `PROMETHEUS_INSTANCE`
  - OpenTelemetry (opcional):
    - `ENABLE_OTLP_PUSH`, `OTLP_ENDPOINT` (POST JSON de sumário)

## Exemplos de Configuração (.env)
```
AUTO_REPAIR_LEVEL=aggressive
STRICT_FK_VALIDATION=false
ENABLE_CEP_ENRICHMENT=true
CEP_MAP_PATH=./data/cep_map.csv
CEP_CORRECT_UF_ONLY_IF_NULL=true
MUNICIPIO_NAME_MAP_PATH=./data/municipios_por_nome.csv
ENABLE_CONSTRAINTS_BACKFILL=false
TELEMETRY_MAX_BYTES=10000000
QUARANTINE_MAX_BYTES=10000000
TELEMETRY_ROTATE_DAILY=true
QUARANTINE_ROTATE_DAILY=true
```

## Boas Práticas
- Prefira domínios oficiais completos e desative backfill quando possível.
- Use `aggressive` apenas quando desejar correções heurísticas e telemetria detalhada.
- Audite a quarentena regularmente e, se necessário, reprocessar linhas após saneamento.
