# CNPJ Dados Abertos — ETL para PostgreSQL

Baixa, trata e carrega os [dados abertos de CNPJ da Receita Federal](https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/)
em um PostgreSQL.

- **Rápido** — carga via `COPY FROM STDIN`, tabelas `UNLOGGED` durante a ingestão, processamento em chunks.
- **Resiliente** — download assíncrono com `curl_cffi` (emulação de TLS), proxies, retry e circuit breaker.
- **Auditável** — divergências ficam registradas em `logs/quarantine/` e `logs/telemetry/`.

## Requisitos

- Python 3.10+
- PostgreSQL 14+ (o `constraints.sql` cria a extensão `pg_trgm`; o papel precisa ser superusuário
  ou tê-la pré-instalada)
- Espaço em disco para os ZIPs e os CSVs extraídos (dezenas de GB)

> As tabelas ficam `UNLOGGED` **também depois** da carga, a menos que você defina
> `SET_LOGGED_AFTER_COPY=true`. O PostgreSQL trunca tabelas `UNLOGGED` em crash ou restart, então
> nesse cenário o banco é reconstruível a partir dos CSVs, mas não é o seu backup.

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
  --only / --exclude       restringem o passo load por nome de config
                           (ex.: empresas, nao pelo nome da tabela)
  --run-queries            executa as queries de queries/ após o pipeline
  --max-workers N          sobrescreve MAX_WORKERS (a concorrência real é
                           MAX_CONCURRENT_REQUESTS, ou 2× MAX_WORKERS)
  --rate-limit-per-sec N   sobrescreve RATE_LIMIT_PER_SEC
  --skip-zip-verify        pula a verificação de integridade dos ZIPs
```

`--dry-run` e `--run-queries` só valem no pipeline completo: com `--step` eles são ignorados.

Etapas também rodam isoladas: `python -m src.downloader`, `python -m src.database_loader`, etc.
O `downloader` precisa de `TARGET_DATE` preenchido (rode `python -m src --step check` antes);
ele não descobre a data sozinho.

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

A base da Receita tem lacunas referenciais, e o comportamento depende de `STRICT_FK_VALIDATION`:

- `true` (padrão): um único valor de FK inválido **aborta o `load` inteiro** com `ValueError`.
- `false`: o valor vira `NULL`, a linha é registrada em `logs/quarantine/<data>/` com
  `reason: "fk_violation"` e **ainda é carregada**.

`constraints.sql` cria as FKs depois da carga e, antes disso, insere os registros pai ausentes
como `NAO CONSTA NA ORIGEM` (`ENABLE_CONSTRAINTS_BACKFILL`). Ele cobre `paises`, `municipios`,
`naturezas_juridicas` e `cnaes` — **não** cobre `qualificacoes_socios`, que precisa estar
carregada.

Quality gates descartam um chunk inteiro quando a proporção de nulos novos ou de valores alterados
passa dos limiares. As linhas descartadas **são gravadas em `logs/quarantine/<data>/`** com
`reason: "quality_gate"`, então nada se perde: dá para recuperar ou reprocessar com
`ENABLE_QUALITY_GATES=false`. Vale saber que o gate só é avaliado em chunks com pelo menos
`GATE_MIN_ROWS` linhas, e que o critério de "valores alterados" só existe em
`AUTO_REPAIR_LEVEL=aggressive`.

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