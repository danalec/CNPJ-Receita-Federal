# CNPJ Dados Abertos — Pipeline de ETL para PostgreSQL

Ferramenta de ETL (Extract, Transform, Load) de alto desempenho para baixar, tratar e carregar os dados públicos de CNPJ
[(disponibilizados pela Receita Federal do Brasil)](https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/).

- **Alta Performance**: Download **assíncrono** (asyncio/curl_cffi) e carga via `COPY FROM STDIN`.
- **Stealth & Resiliência**: Emulação de TLS de navegador (Chrome/Edge), rotação de proxies, retry inteligente e circuit breaker.
- **Qualidade automática**: Quality gates por chunk (razão de nulos/alterações), quarantine de registros ruins em JSONL, e telemetria.
- **Integridade referencial**: Backfill automático de registros pai ausentes e validação de FKs.
- **Modular**: Execute etapas isoladas ou o pipeline completo.

## Uso rápido

```bash
# Instalar dependências
./tasks.ps1 install

# Verificação completa (lint + mypy + testes)
./tasks.ps1 verify

# Pipeline completo (baixa, extrai, consolida, carrega, constraints)
./tasks.ps1 etl

# Etapa individual
./tasks.ps1 step download
./tasks.ps1 step load

# No Linux/macOS (sem PowerShell)
python -m src --step download
python -m src --step load
```

## Etapas do Pipeline

| Etapa | Comando | Descrição |
|---|---|---|
| Verificar atualizações | `--step check` | Consulta a Receita Federal e detecta nova versão |
| Download | `--step download` | Download assíncrono com curl_cffi (TLS fingerprint) |
| Extração | `--step extract` | Descompactação dos arquivos ZIP |
| Consolidação | `--step consolidate` | Agrupamento de CSVs parciais por tabela |
| Carga | `--step load` | Carga via `COPY FROM STDIN`, quality gates, quarantine |
| Constraints | `--step constraints` | PKs, FKs, índices, backfill de registros ausentes |

Execute o pipeline completo com `--force` (ignora histórico, executa tudo):

```bash
python -m src --force
```

Retome de onde parou com `--resume` (pula etapas já completadas):

```bash
python -m src --resume
```

Carregue apenas tabelas específicas:

```bash
python -m src --step load --only empresas estabelecimentos
python -m src --step load --exclude socios simples
```

## Integridade dos Dados

A base da Receita Federal pode conter inconsistências referenciais (ex.: FK sem registro pai). O pipeline trata isso automaticamente:

1. **Quality gates por chunk**: se a razão de valores alterados ou nulos excede o limiar configurado, o chunk é quarantinado eulogiado em JSONL.
2. **Backfill automático**: registros pai ausentes são inseridos como `"NÃO CONSTA NA ORIGEM"` antes das FKs.
3. **Validação de FKs**: registros com FK inválida são quarantineados.
4. **Constraints ao final**: PKs, FKs e índices são aplicados por `src/constraints.sql` após a carga.

Registros quarantineados ficam em `quarantine/YYYYMMDD/*.jsonl` para auditoria.

## Pré-requisitos

- `Python` 3.10+
- `PostgreSQL` 14+ (via Docker Compose ou local)
- `Docker` (opcional, para o banco)
- Espaço em disco: ~100 GB

## Configuração

Copie `.env.example` para `.env` e ajuste:

```ini
# Banco
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=cnpj
POSTGRES_USER=cnpj
POSTGRES_PASSWORD=cnpj

# Download
MAX_WORKERS=4
DOWNLOAD_CHUNK_SIZE=8192
VERIFY_ZIP_INTEGRITY=true
IMPERSONATE=chrome110    # chrome, chrome110, edge99, safari15_3
# PROXIES=["http://user:pass@host:port", ...]

# Qualidade
ENABLE_QUALITY_GATES=true
GATE_MAX_CHANGED_RATIO=0.3
GATE_MAX_NULL_DELTA_RATIO=0.3
STRICT_FK_VALIDATION=false

# Logging
LOG_LEVEL=INFO
```

## Testes

```bash
pytest -q -m "not integration"          # unitários
pytest -q -m integration                 # integração (requer PostgreSQL)
./tasks.ps1 verify                       # lint + mypy + testes
```

## CI / Docker

```bash
docker compose up -d db    # iniciar PostgreSQL
./tasks.ps1 ci             # executa lint + testes + docker build + integração
```

## Documentação

- [docs/index.md](docs/index.md) — visão geral e notas de versão
- [docs/auto-repair.md](docs/auto-repair.md) — lógica de normalização e quality gates
- [docs/boas-praticas-indices.md](docs/boas-praticas-indices.md) — índices e performance

## Licença

MIT — consulte [LICENSE](LICENSE).
