# CNPJ Dados Abertos — ETL para PostgreSQL

Baixa, trata e carrega os [dados abertos de CNPJ da Receita Federal](https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/)
em um PostgreSQL.

- **Rápido** — carga via `COPY FROM STDIN`, tabelas `UNLOGGED` durante a ingestão, processamento em chunks.
- **Resiliente** — download assíncrono com `curl_cffi` (emulação de TLS), proxies, retry e circuit breaker.
- **Auditável** — registros rejeitados vão para JSONL em `logs/quarantine/`, com telemetria por chunk.

## Requisitos

- Python 3.10+
- PostgreSQL 14+ (o `constraints.sql` cria a extensão `pg_trgm`)
- ~100 GB de disco

## Começando

```bash
cp .env.example .env          # ajuste as credenciais do banco
pip install -r requirements.txt -r requirements-dev.txt

docker compose up -d db       # opcional: sobe o PostgreSQL
python -m src --force         # pipeline completo
```

## CLI

```bash
python -m src [opções]

  --force                  ignora o histórico e roda todas as etapas
  --resume                 pula etapas já concluídas
  --dry-run                mostra o que seria executado
  --step {check,download,extract,consolidate,load,constraints}
  --only / --exclude       restringem as tabelas do passo load
  --run-queries            executa as queries de queries/ após a carga
  --max-workers N          sobrescreve MAX_WORKERS
  --rate-limit-per-sec N   sobrescreve RATE_LIMIT_PER_SEC
  --skip-zip-verify        pulsa a verificação de integridade dos ZIPs
```

Etapas também rodam isoladas: `python -m src.downloader`, `python -m src.database_loader`, etc.

No Windows, `tasks.ps1` embrulha os comandos comuns:

```powershell
./tasks.ps1 install     ./tasks.ps1 lint      ./tasks.ps1 test
./tasks.ps1 etl         ./tasks.ps1 step load ./tasks.ps1 verify
```

## Como funciona

| Etapa | O que faz |
|---|---|
| `check` | Descobre a pasta `YYYY-MM` mais recente na Receita |
| `download` | Baixa os ZIPs em paralelo, com resume por header `Range` |
| `extract` | Descompacta cada ZIP |
| `consolidate` | Concatena os volumes em um CSV por tabela |
| `load` | Valida, aplica quality gates e carrega via `COPY` |
| `constraints` | PKs, FKs, índices e backfill de registros ausentes |

A base da Receita tem lacunas referenciais. O pipeline trata isso em duas frentes:
`constraints.sql` insere registros pai ausentes como `NAO CONSTA NA ORIGEM` antes de criar as FKs, e
o passo `load` coloca em quarentena as linhas com FK inválida em `logs/quarantine/`.

Se um chunk ultrapassar os limiares de qualidade (`ENABLE_QUALITY_GATES`), ele é registrado em
`logs/telemetry/` e não é carregado.

## Configuração

Tudo via `.env` — ver [`.env.example`](.env.example) e a lista completa em `src/settings.py`.

## Desenvolvimento

```bash
ruff check . && mypy src && pytest -q
```

## Documentação

- [docs/index.md](docs/index.md) — visão geral, configuração e troubleshooting
- [docs/auto-repair.md](docs/auto-repair.md) — normalização e quality gates
- [docs/boas-praticas-indices.md](docs/boas-praticas-indices.md) — índices e performance
- [docs/docker.md](docs/docker.md) — imagem e Docker Compose
- [docs/download.md](docs/download.md) — detalhes do downloader
- [docs/descricao-dados.md](docs/descricao-dados.md) — layout dos dados da Receita

## Licença

MIT — [LICENSE](LICENSE)