# CNPJ Dados Abertos - Pipeline de `ETL` para PostgreSQL

Ferramenta de `ETL` (Extract, Transform, Load) de alto desempenho para baixar, tratar e carregar os dados públicos de CNPJ
[(disponibilizados pela Receita Federal do Brasil)](https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/).

- Foco em **performance** com `COPY FROM STDIN`, tabelas `UNLOGGED` e processamento em _chunks_ via Pandas.
- Pipeline **modular**: se alguma etapa falhar, corrija e retome diretamente daquele ponto.
- Nota de integridade: eventualmente a Receita publica versões com lacunas (ex.: `2025-11` sem o código de país `150`). Se houver FKs ausentes, corrija no banco e execute `constraints.sql` para aplicar/reaplicar as restrições.

```bash
python main.py --force
# ou execute módulos individuais
python -m src.<modulo>
```

## Como usar (rápido)

1. Instale as dependências e configure o `.env`.
2. Execute `python -m src.check_update` para verificar novas versões.
3. Rode `main.py` para orquestrar automaticamente todo o pipeline, ou execute módulos individualmente:
   - `python -m src.downloader`
   - `python -m src.extract_files`
   - `python -m src.consolidate_csv`
- `python -m src.database_loader`

CLI do orquestrador (`main.py`):

- `--force`: ignora histórico e roda todas as etapas
- `--step {check,download,extract,consolidate,load,constraints}`: executa apenas a etapa informada
- `--no-csv-filter`: desabilita filtros de CSV (linhas malformadas e vazias)

### Uso no Windows (PowerShell)

Sem `make`, utilize `tasks.ps1` na raiz:

```powershell
./tasks.ps1 install
./tasks.ps1 lint
./tasks.ps1 check
./tasks.ps1 test
./tasks.ps1 pipeline
./tasks.ps1 step load
```

## Exemplos por etapa
Exemplos detalhados estão em `docs/`.

## Fluxo de dados
A visão completa do fluxo está em `docs/`.

## Instalação e configuração
```bash
poetry install
```

- `.env` (aliases aceitos): `POSTGRES_*` e `PG*`
- Opcional: `DATABASE_URL` (`postgresql://user:pass@host:port/db`)
- CSV e logs: `CSV_FILTER=true`, `LOG_BACKUP_COUNT=7`, `LOG_LEVEL=INFO`

## Notas de performance
Detalhes e recomendações estão em [docs/index.md](docs/index.md).

## Testes

Execute:

```bash
pytest -q
```

## Documentação

Para conteúdo aprofundado (fluxo, exemplos por módulo, troubleshooting, diagrama ER), consulte [docs/](docs/):
- [Descrição dos dados](docs/descricao-dados.md)
- [Diagrama ER](docs/diagrama_er.md)
- Pasta local (Windows): [c:\Users\danalec\Documents\src\CNPJ-Receita-Federal\docs](file:///c:/Users/danalec/Documents/src/CNPJ-Receita-Federal/docs)

## Contribuição

- Abra issues para relatar inconsistências ou propor melhorias.
- Envie PRs com otimizações de performance, confiabilidade e manutenção.

### Explicação visual das ligações

1. **EMPRESAS (Central)**: É a tabela pai. Ela conecta com:
   - **ESTABELECIMENTOS**: Ligação forte (PK composta). Uma empresa pode ter várias filiais.
   - **SÓCIOS**: Uma empresa tem vários sócios.
   - **SIMPLES**: Uma empresa pode ou não ter registro no Simples (0 ou 1).

2. **ESTABELECIMENTOS**:
   - Conecta com **CNAES** (atividade econômica).
   - Conecta com **MUNICÍPIOS** e **PAÍSES** (geografia).
   - Nota: `cnae_fiscal_secundaria` é um **array** de texto para performance (em vez de tabela associativa N:N), embora represente códigos CNAE.

3. **SOCIOS**:
   - Conecta com **QUALIFICAÇÕES** (diretor, presidente, etc.).

## Contribuição

- Abra issues para relatar inconsistências nos dados ou propor melhorias.
- Envie PRs com otimizações de performance, confiabilidade e manutenção.
