[[Voltar ao README]](../README.md) • [[Índice da documentação]](index.md)

# Guia de Docker

Este guia descreve como construir a imagem Docker, executar com Docker Compose e auditar dependências com `pip-audit`.

## Build da imagem

```bash
docker build -t cnpj:latest .
```

- Ajuste o nome/tag conforme sua necessidade (ex.: `cnpj:v1`).
- Utilize `.dockerignore` para reduzir o contexto de build.

## Executar com Docker Compose

```bash
docker compose up --build
```

- Para modo em background: `docker compose up -d --build`.
- Requer um arquivo `docker-compose.yml` com os serviços definidos (ex.: app, db). Substitua nomes conforme sua configuração.

## Auditoria de dependências (pip-audit)

Audite vulnerabilidades nas dependências Python.

### Local (host)

```bash
poetry run pip-audit
```

### Dentro do contêiner

1. Instale a ferramenta na imagem (exemplo genérico):
   - via `pip`: adicionar `pip install pip-audit` na fase de build
   - ou incluir no ambiente de execução conforme seu gerenciador (Poetry, pip)

2. Execute:

```bash
docker compose exec <servico> pip-audit
```

ou

```bash
docker run --rm cnpj:latest pip-audit
```

- Substitua `<servico>` pelo nome do serviço definido no `docker-compose.yml`.
- O comando só funciona se `pip-audit` estiver disponível no contêiner.

## Boas práticas

- Use build multi-stage para reduzir o tamanho da imagem final.
- Fixe versões base (ex.: `python:3.11-slim`) e mantenha atualizações de segurança.
- Limpe caches e artefatos após instalação de dependências.
- Revise variáveis de ambiente e segredos fora da imagem (Compose/Stack, `.env`).

