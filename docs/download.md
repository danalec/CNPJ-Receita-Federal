[[Voltar ao README]](../README.md) • [[Índice da documentação]](index.md)

# Download dos dados (multithread)

Este guia mostra como baixar os arquivos oficiais de CNPJ em múltiplas threads e onde configurar os parâmetros.

## Visão geral
- O módulo responsável pelo download é `src/downloader.py`.
- O download paraleliza requisições usando `ThreadPoolExecutor` e valida a integridade dos `.zip`.
- Os arquivos são salvos em `data/compressed_files`.

## Pré-requisitos
- `.env` configurado e pastas criadas automaticamente pelo projeto.
- Defina a data de destino (ex.: `2025-11`) para montar a URL dos arquivos.

## Configuração (`.env`)
As principais variáveis para o download:

```ini
# Data alvo no formato YYYY-MM
TARGET_DATE=2025-11

# Número de threads para download simultâneo
MAX_WORKERS=4

# Tamanho do chunk em bytes
DOWNLOAD_CHUNK_SIZE=8192

# Limite opcional de taxa por segundo (0 desabilita)
RATE_LIMIT_PER_SEC=0

# Verificação de integridade dos arquivos ZIP
VERIFY_ZIP_INTEGRITY=true
```

Observações:
- `TARGET_DATE` define `download_url` baseado na URL oficial da Receita.
- Ajuste `MAX_WORKERS` de acordo com sua rede e políticas do servidor.

## Executar o download
Duas formas de executar:

1. Orquestrador (apenas etapa de download):
   ```bash
   python -m src --step download
   ```

2. Módulo direto:
   ```bash
   python -m src.downloader
   ```

## Paralelismo e progresso
- O número de threads é controlado por `MAX_WORKERS`.
- O progresso por arquivo é exibido com barra de avanço.
- Downloads são retomados quando possível (suporte a `Range`), evitando repetir do zero.

## Integridade e armazenamento
- Com `VERIFY_ZIP_INTEGRITY=true`, arquivos `.zip` são verificados; se corrompidos, são removidos e o log registra o erro.
- Os arquivos baixados são gravados em `data/compressed_files`.

## Boas práticas
- Evite exagerar em `MAX_WORKERS` para reduzir bloqueios.
- Utilize pausa/taxa (`RATE_LIMIT_PER_SEC`) se notar rejeições do servidor.
- Consulte o guia de [Rotação de User-Agent](user-agent.md) para melhorar a resiliência de requisições.

