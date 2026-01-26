# CNPJ Dados Abertos — Pipeline de ETL para PostgreSQL

Ferramenta de ETL (Extract, Transform, Load) de alto desempenho para baixar, tratar e carregar os dados públicos de CNPJ
[(disponibilizados pela Receita Federal do Brasil)](https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/).

- Foco em **performance** com `COPY FROM STDIN`, tabelas `UNLOGGED` e processamento em _chunks_ via Pandas.
- Pipeline **modular**: se alguma etapa falhar, corrija e retome diretamente daquele ponto.
- **Integridade**: Tratamento automático de lacunas (ex.: FKs ausentes).

## Integridade dos Dados e Inconsistências na Origem

É comum que a base de dados da Receita Federal apresente inconsistências de integridade referencial (ex: um sócio referenciado que não consta na tabela de sócios).

O Pipeline trata esses casos automaticamente: todas as constraints e chaves estrangeiras são validadas após a inserção para que seja aplicada as constraints e não de erro no processo. Quando um registro pai não é encontrado, o sistema insere um dado fictício (dummy) identificado como "NÃO CONSTA NA ORIGEM". Portanto, não se preocupe: os dados já estão higienizados e inseridos, você não terá erro algum.

Para aplicar as restrições formalmente no banco de dados **CASO SEJA NECESSÁRIO**, execute o arquivo `constraints.sql`. Basta corrigir o dado pontual e executá-lo novamente, sem a necessidade de reiniciar todo o processo de inserção.

```bash
python -m src --force
# ou execute módulos individuais
python -m src.check_update
python -m src.downloader
python -m src.extract_files
python -m src.consolidate_csv
python -m src.database_loader
```

## Uso rápido

1. Instale as dependências e configure o `.env`.
2. Execute `python -m src.check_update` para verificar novas versões.
3. Rode `python -m src` para orquestrar todo o pipeline, ou execute módulos individualmente:
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

## Detalhes das Etapas

### Download dos dados
**Download dos arquivos** em multi-thread, máximo de 4 para evitar abusos de conexões simultâneas.
**Módulo responsável: `downloader.py`**

### Descompactação
Descompacta arquivos baixados que por padrão são divididos em vários arquivos `.zip`. Extrai agrupando o resultado em uma única pasta, normalmente os dados vem com um nome prefixado e as versões, `empresas01.zip`, `empresas02.zip` etc...
**Módulo responsável: `extract_files.py`**

### Consolidação dos arquivos `CSVs`
Agrupa os `CSVs` descompactados em único arquivo, único por categoria, removendo a necessidade de lidar com múltiplos arquivos durante a carga.
**Módulo responsável: `consolidate_csv.py`**

### Carga para o Banco de dados
Utiliza o comando `COPY` do PostgreSQL (via driver `psycopg`) para inserção em massa.

- Cria tabelas como `UNLOGGED` para acelerar a escrita inicial.
- Realiza a limpeza de dados (conversão de datas, formatação de arrays para `CNAEs`, sanitização de decimais etc...)
- Validação das constraints para que não haja erros de integridade.
- Aplicação de Chaves Primárias, Estrangeiras e Índices **após** a carga para maximizar a velocidade.
- Schema Otimizado Separação clara entre SQL de definição (`DDL`) e código Python.

**Módulo responsável: `database_loader.py`**

## Pré-requisitos

- `Python` 3.10+ e `Poetry`
- `PostgreSQL` 14+ acessível e com permissões de criação de tabelas/índices
- Espaço em disco para arquivos compactados e CSVs (dezenas de GB)

## Configuração (`.env`)

Variáveis suportadas (aliases aceitos: `POSTGRES_*` e `PG*`). Opcional: `DATABASE_URL`.
Suportados também: `LOG_LEVEL`, `LOG_BACKUP_COUNT`, `CSV_FILTER`, `FILE_ENCODING`, `CHUNK_SIZE`.

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

## Download dos dados (multithread)

- Módulo responsável: `src/downloader.py`
- Configure a data alvo e paralelismo no `.env`:

```ini
TARGET_DATE=2025-11
MAX_WORKERS=4
DOWNLOAD_CHUNK_SIZE=8192
VERIFY_ZIP_INTEGRITY=true
```

- Execute apenas a etapa de download pelo orquestrador:

```bash
python -m src --step download
```

- Ou execute diretamente o módulo de download:

```bash
python -m src.downloader
```

- Detalhes adicionais em [docs/download.md](docs/download.md).

## Exemplos por etapa
Exemplos detalhados estão em [docs/](docs/index.md).

## Fluxo de dados
A visão completa do fluxo está em [docs/](docs/index.md).

## Instalação e configuração
```bash
poetry install
```

- Configure o `.env` conforme seção acima.

## Notas de performance
Detalhes e recomendações estão em `docs/`. Destaques:
- Carga via `COPY FROM STDIN`.
- Tabelas `UNLOGGED` durante ingestão, com índices aplicados ao final.
- Processamento em _chunks_ via Pandas para reduzir memória.
- Arrays textuais em `cnae_fiscal_secundaria` no lugar de N:N para velocidade.

## Testes
Execute:

```bash
pytest -q
```

## Documentação

Para conteúdo aprofundado (fluxo, exemplos por módulo, troubleshooting, diagrama ER), consulte [docs/](docs/index.md):
- [Descrição dos dados](docs/descricao-dados.md)
- [Diagrama ER](docs/diagrama_er.md)
- [Boas práticas de índices](docs/boas-praticas-indices.md)
- [Guia de Docker](docs/docker.md)
- [Rotação de User-Agent (Downloader)](docs/user-agent.md)


## Troubleshooting

- CSV com BOM/linhas malformadas: use `--no-csv-filter` para desabilitar limpeza.
- FKs ausentes em versões da Receita: corrija as lacunas e aplique/reaplique restrições com `src/constraints.sql` via `--step constraints`.
- Erros de conexão com banco: valide `DATABASE_URL` ou variáveis `POSTGRES_*`.

## Contribuição

- Abra issues para relatar inconsistências ou propor melhorias.
- Envie PRs com otimizações de performance, confiabilidade e manutenção.

## Licença

Este projeto é disponibilizado sob a licença [MIT](LICENSE).
