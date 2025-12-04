# Documentação do Pipeline CNPJ

## Navegação Rápida
- Visão geral e início rápido: [README.md](../README.md)
- Descrição oficial dos dados: [descricao-dados.md](descricao-dados.md)
- Diagrama entidade–relacionamento (ER): [diagrama_er.md](diagrama_er.md)
- Rotação de User-Agent (Downloader): [user-agent.md](user-agent.md)

## Como Começar
- Instala dependências: `poetry install`
- Pipeline completo: `python main.py`
- Etapa específica: `python main.py --step {check,download,extract,consolidate,load,constraints}`
- Windows sem make: `./tasks.ps1 install`, `./tasks.ps1 pipeline`, `./tasks.ps1 step load`

## Etapas do Pipeline
- Verificação de atualização: ../src/check_update.py
- Download: ../src/downloader.py
- Extração: ../src/extract_files.py
- Consolidação de CSVs: ../src/consolidate_csv.py
- Carga no PostgreSQL: ../src/database_loader.py
- Constraints e índices: ../src/constraints.sql

## Dados e Modelo
- Layout e campos (oficial): [descricao-dados.md](descricao-dados.md)
- Diagrama ER (mermaid): [diagrama_er.md](diagrama_er.md)
- Metadados (Receita Federal): https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf

## Troubleshooting e Performance
- Erros comuns, dicas de performance e consultas exemplo: [README.md](../README.md)
- Verificação de BOM com ripgrep (exemplo): `rg -nU "^\xEF\xBB\xBF" data/extracted_files/**/*.csv` (veja também [README.md](../README.md))

- Boas práticas de índices: [boas-praticas-indices.md](boas-praticas-indices.md)

## Renderização do Diagrama ER
- Arquivo `diagrama_er.md` usa sintaxe Mermaid. Visualize em renderizadores compatíveis (por exemplo, VS Code com extensão Mermaid) ou plataformas que suportem blocos `mermaid`.

## Ferramentas de Desenvolvimento
- Makefile (Linux/macOS): `make lint`, `make check`, `make test`, `make pipeline`, `make step STEP=load`
- PowerShell (Windows): `./tasks.ps1 lint`, `./tasks.ps1 check`, `./tasks.ps1 test`, `./tasks.ps1 pipeline`, `./tasks.ps1 step load`
- Guia de Docker: [docker.md](docker.md)
