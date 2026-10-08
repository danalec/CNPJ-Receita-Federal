# CNPJ Dados Abertos — Documentação

## Visão Geral
- Pipeline de `ETL` para baixar, tratar e carregar os dados públicos de CNPJ: https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/
- Foco em **performance** com `COPY FROM STDIN`, tabelas `UNLOGGED` e processamento em _chunks_ via Pandas.
- Pipeline **modular**: retome do ponto de falha sem refazer o restante.
- Nota de integridade: versões da Receita podem ter lacunas (ex.: `2025-11` sem país `150`). Corrija FKs ausentes e aplique/reaplique restrições com `src/constraints.sql`.

## Início Rápido
- Instala dependências: `pip install -r requirements.txt -r requirements-dev.txt`
- Orquestrador completo: `python -m src`
- Etapas específicas:
  - `python -m src.check_update`
  - `python -m src.downloader`
  - `python -m src.extract_files`
  - `python -m src.consolidate_csv`
  - `python -m src.database_loader`

### CLI do orquestrador (`src`)
- `--force`: ignora histórico e roda todas as etapas
- `--resume`: retoma de onde parou, pulando etapas já concluídas
- `--dry-run`: simula a execução sem rodar nada
- `--step {check,download,extract,consolidate,load,constraints}`: executa somente a etapa informada
- `--only` / `--exclude`: restringem as tabelas do passo `load`
- `--run-queries`: executa as queries em `queries/` após a carga
- `--max-workers`, `--rate-limit-per-sec`, `--skip-zip-verify`: sobrescrevem o `.env` nesta execução

### Uso no Windows (PowerShell)
Sem `make`, utilize `./tasks.ps1` na raiz:
- `install` — instala dependências (`pip`, ou `poetry install --with dev` se Poetry estiver disponível)
- `lint` — `ruff check .`
- `check` — `python -m compileall -q src` (byte-compile; **não** é a etapa `check` do pipeline)
- `test` — `pytest -q`
- `pipeline` — `python -m src`
- `etl` — do download às constraints, com o banco via Docker Compose
- `step <nome>` — roda uma etapa (`download`, `load`, `constraints`, …)
- `verify` — lint + compile + mypy + testes
- `ci` — testes, lint, build da imagem e testes de integração

## Pré-requisitos

- `Python` 3.10+
- `PostgreSQL` 14+ com permissões para criar tabelas/índices e a extensão `pg_trgm`
- Espaço em disco para arquivos compactados e CSVs (dezenas de GB)

## Configuração (`.env`)
Copie `.env.example` para `.env`. As variáveis correspondem aos campos de `Settings` em `src/settings.py`.

```ini
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=cnpj
POSTGRES_USER=postgres
POSTGRES_PASSWORD=senha

FILE_ENCODING=latin1
CHUNK_SIZE=200_000
LOG_LEVEL=INFO
```

Variáveis relevantes: `TARGET_DATE` (pasta `YYYY-MM` da Receita), `MAX_WORKERS`,
`RATE_LIMIT_PER_SEC` (bytes/s; `0` desativa), `VERIFY_ZIP_INTEGRITY`,
`IMPERSONATE` (perfil TLS do `curl_cffi`), `PROXIES`, `ENABLE_QUALITY_GATES`,
`STRICT_FK_VALIDATION`, `ENABLE_CONSTRAINTS_BACKFILL`, `USE_UNLOGGED`.
Veja [`.env.example`](../.env.example) e a lista completa em `src/settings.py`.

## Etapas do Pipeline
- Verificação de atualização: `../src/check_update.py`
- Download: `../src/downloader.py`
- Extração: `../src/extract_files.py`
- Consolidação de CSVs: `../src/consolidate_csv.py`
- Carga no PostgreSQL: `../src/database_loader.py`
- Restrições e índices: `../src/constraints.sql`

## Dados e Modelo
- Layout e campos (oficial): [descricao-dados.md](descricao-dados.md)
- Diagrama ER (Mermaid): [diagrama_er.md](diagrama_er.md)
- Metadados Receita Federal: https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf

## Notas de Performance
- Carga via `COPY FROM STDIN`.
- Tabelas `UNLOGGED` durante ingestão; índices ao final.
- Processamento em _chunks_ via Pandas para reduzir uso de memória.
- Arrays `INT[]` em `cnae_fiscal_secundaria` para velocidade (em vez de N:N).

## Testes
- Execute `pytest -q`.

## Troubleshooting

- **BOM / CRLF nos CSVs**: o consolidador remove BOM e normaliza quebras de linha automaticamente (`STRIP_BOM`, `NORMALIZE_LINE_ENDINGS`).
- **FK ausente**: o backfill de `constraints.sql` insere o registro pai como `NAO CONSTA NA ORIGEM`. Reaplique com `--step constraints`.
- **Tabelas sem PK/FK depois da carga**: verifique `logs/cnpj.log` — falhas em `constraints.sql` abortam a etapa.
- **Conexão**: valide `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DATABASE`, `POSTGRES_USER`, `POSTGRES_PASSWORD`.

## Conteúdos Relacionados
- Boas práticas de índices: [boas-praticas-indices.md](boas-praticas-indices.md)
- Guia de Docker: [docker.md](docker.md)
- Emulação de navegador (Downloader): [user-agent.md](user-agent.md)
- Download dos dados (multithread): [download.md](download.md)
- AUTO-REPAIR: Normalização e Telemetria: [auto-repair.md](auto-repair.md)
- Visão geral e início rápido: [README.md](../README.md)

## Renderização do Diagrama ER
- `diagrama_er.md` utiliza Mermaid. Visualize em ferramentas compatíveis (ex.: VS Code com extensão Mermaid).
