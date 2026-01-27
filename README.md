# CNPJ Dados Abertos — Pipeline de ETL para PostgreSQL

Ferramenta de ETL (Extract, Transform, Load) de alto desempenho para baixar, tratar e carregar os dados públicos de CNPJ
[(disponibilizados pela Receita Federal do Brasil)](https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/).

- **Alta Performance**: Download **assíncrono** (asyncio/curl_cffi) e carga via `COPY FROM STDIN`.
- **Stealth & Resiliência**: Emulação de TLS de navegador (Chrome/Edge), rotação de proxies, retry inteligente e circuit breaker.
- **Integridade**: Tratamento automático de lacunas (ex.: FKs ausentes).
- **Modular**: Execute etapas isoladas ou o pipeline completo.

## Integridade dos Dados e Inconsistências na Origem

É comum que a base de dados da Receita Federal apresente inconsistências de integridade referencial (ex: um sócio referenciado que não consta na tabela de sócios).

O Pipeline trata esses casos automaticamente: todas as constraints e chaves estrangeiras são validadas após a inserção para que seja aplicada as constraints e não de erro no processo. Quando um registro pai não é encontrado, o sistema insere um dado fictício (dummy) identificado como "NÃO CONSTA NA ORIGEM".

O Pipeline **aplica as restrições automaticamente** ao final da carga. A execução manual do arquivo `constraints.sql` (ou via `--step constraints`) é necessária **apenas** se você precisar reaplicá-las (ex: após correções manuais de dados) ou se a etapa automática tiver sido pulada/falhado.

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
3. Rode `python -m src` para orquestrar todo o pipeline, ou execute módulos individualmente.

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

### Download dos dados (Stealth/Async)
Utiliza **`curl_cffi`** e **`asyncio`** para downloads de alta velocidade que simulam um navegador real (TLS Fingerprinting), evitando bloqueios de WAF. Suporta HTTP/2, rotação de IPs (Proxies) e retomada automática.
**Módulo responsável: `downloader.py`**

### Descompactação
Descompacta arquivos baixados, agrupando volumes (`empresas01.zip`, `empresas02.zip`) em uma estrutura consolidada.
**Módulo responsável: `extract_files.py`**

### Consolidação dos arquivos `CSVs`
Agrupa os `CSVs` descompactados em arquivos únicos por categoria, facilitando a carga.
**Módulo responsável: `consolidate_csv.py`**

### Carga para o Banco de dados
Utiliza o comando `COPY` do PostgreSQL para inserção em massa.
- Tabelas `UNLOGGED` para velocidade.
- Limpeza e sanitização de dados.
- Validação e aplicação de constraints/índices ao final.
**Módulo responsável: `database_loader.py`**

## Pré-requisitos

- `Python` 3.10+ e `Poetry`
- `PostgreSQL` 14+
- Espaço em disco (recomendado 100GB+)

## Configuração (`.env`)

Exemplo de configuração:

```ini
# Banco de Dados
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=cnpj
POSTGRES_USER=postgres
POSTGRES_PASSWORD=senha

# Download & Stealth
TARGET_DATE=2025-11
MAX_WORKERS=4                 # Concorrência base
DOWNLOAD_CHUNK_SIZE=8192
VERIFY_ZIP_INTEGRITY=true
ENABLE_HTTP2=true
STEALTH_MODE=true
IMPERSONATE=chrome110         # chrome, chrome110, edge99, safari15_3
# PROXIES=["http://user:pass@host:port", ...] 

# Geral
CSV_FILTER=true
LOG_LEVEL=INFO
```

## Download dos dados

Execute apenas a etapa de download:

```bash
python -m src --step download
# Ou
python -m src.downloader
```

O downloader gerencia automaticamente:
- **Rate Limiting**: Para não saturar a rede.
- **Circuit Breaker**: Pausa temporária em caso de erros consecutivos.
- **Resume**: Continua de onde parou usando headers `Range`.

## Testes

Execute os testes automatizados:

```bash
pytest -q
```

## Documentação

Para conteúdo aprofundado, consulte [docs/](docs/index.md).

## Licença

Este projeto é disponibilizado sob a licença [MIT](LICENSE).
