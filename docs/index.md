# CNPJ Dados Abertos — Documentação

## Visão Geral
- Pipeline de `ETL` para baixar, tratar e carregar os dados públicos de CNPJ: https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/
- Foco em **performance** com `COPY FROM STDIN`, tabelas `UNLOGGED` e processamento em _chunks_ via Pandas.
- Pipeline **modular**: retome do ponto de falha sem refazer o restante.
- Nota de integridade: versões da Receita podem ter lacunas (ex.: `2025-11` sem país `150`). Corrija FKs ausentes e aplique/reaplique restrições com `src/constraints.sql`.

## Início Rápido
- Instala dependências: `poetry install`
- Orquestrador completo: `python -m src`
- Etapas específicas:
  - `python -m src.check_update`
  - `python -m src.downloader`
  - `python -m src.extract_files`
  - `python -m src.consolidate_csv`
  - `python -m src.database_loader`

### CLI do orquestrador (`main.py`)
- `--force`: ignora histórico e roda todas as etapas
- `--step {check,download,extract,consolidate,load,constraints}`: executa somente a etapa informada
- `--no-csv-filter`: desabilita limpeza de linhas malformadas e vazias

### Uso no Windows (PowerShell)
- Sem `make`, utilize `./tasks.ps1` na raiz:
  - `./tasks.ps1 install`
  - `./tasks.ps1 lint`
  - `./tasks.ps1 check`
  - `./tasks.ps1 test`
  - `./tasks.ps1 pipeline`
  - `./tasks.ps1 step load`

## Pré-requisitos

- `Python` 3.10+ e `Poetry`
- `PostgreSQL` 14+ com permissões para criar tabelas/índices
- Espaço em disco para arquivos compactados e CSVs (dezenas de GB)

## Configuração (`.env`)
- Aliases aceitos: `POSTGRES_*` e `PG*`. Opcional: `DATABASE_URL`.
- Variáveis adicionais: `LOG_LEVEL`, `LOG_BACKUP_COUNT`, `CSV_FILTER`, `FILE_ENCODING`, `CHUNK_SIZE`.

```ini
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=cnpj
POSTGRES_USER=postgres
POSTGRES_PASSWORD=senha

CSV_FILTER=true
LOG_LEVEL=INFO
LOG_BACKUP_COUNT=7

# Alternativa única
# DATABASE_URL=postgresql://user:pass@host:5432/db
```

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
- Arrays textuais em `cnae_fiscal_secundaria` para velocidade (em vez de N:N).

## Testes
- Execute `pytest -q`.

## Troubleshooting

- CSV com BOM/linhas malformadas: use `--no-csv-filter`.
- FKs ausentes: corrija e aplique/reaplique com `src/constraints.sql` e `--step constraints`.
- Conexão com banco: valide `DATABASE_URL` ou variáveis `POSTGRES_*`.

## Conteúdos Relacionados
- Boas práticas de índices: [boas-praticas-indices.md](boas-praticas-indices.md)
- Guia de Docker: [docker.md](docker.md)
- Rotação de User-Agent (Downloader): [user-agent.md](user-agent.md)
- Download dos dados (multithread): [download.md](download.md)
- Visão geral e início rápido: [README.md](../README.md)

## Renderização do Diagrama ER
- `diagrama_er.md` utiliza Mermaid. Visualize em ferramentas compatíveis (ex.: VS Code com extensão Mermaid).
