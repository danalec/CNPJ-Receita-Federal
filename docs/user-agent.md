[[Voltar ao README]](../README.md) • [[Índice da documentação]](index.md)

# Rotação de User-Agent (Downloader)

Este guia descreve práticas para definir e rotacionar o cabeçalho `User-Agent` durante o download dos arquivos da Receita, reduzindo bloqueios e melhorando compatibilidade.

## Boas práticas
- Defina sempre um `User-Agent` explícito nas requisições HTTP.
- Rotacione entre uma lista de `User-Agent`s realistas (navegadores modernos) para reduzir padrões repetitivos.
- Respeite limites do servidor (intervalos entre requisições, backoff exponencial em erros 429/503).
- Mantenha logs para diagnosticar rejeições e ajustes futuros.

## Exemplo (Python Requests)
```python
import random
import time
import requests

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36",
]

def fetch(url: str, timeout: int = 60) -> requests.Response:
    ua = random.choice(USER_AGENTS)
    headers = {"User-Agent": ua}
    resp = requests.get(url, headers=headers, timeout=timeout)
    if resp.status_code in (429, 503):
        time.sleep(2)
    resp.raise_for_status()
    return resp
```

## Dicas adicionais
- Combine rotação de `User-Agent` com revezamento de proxies apenas se necessário e conforme políticas de uso.
- Evite `User-Agent`s genéricos ou obsoletos; mantenha a lista atualizada.
- Para downloads massivos, insira pausas e verifique cabeçalhos `Retry-After` quando disponíveis.

