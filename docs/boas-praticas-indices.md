[[Voltar ao README]](../README.md) • [[Índice da documentação]](index.md)

# Boas práticas de índices (PostgreSQL)

- Objetivo: acelerar consultas típicas do domínio CNPJ mantendo baixo custo de manutenção.
- Contexto: carga em alto volume via `COPY`, índices criados após ingestão (`constraints.sql`).

## Princípios
- Prefira índices em colunas com alta seletividade (muitos valores distintos) e usadas em filtros/junções.
- Compostos ajudam quando as consultas filtram por mais de uma coluna; a ordem deve seguir o predicado mais seletivo/mais frequente.
- O “prefixo à esquerda” de um índice composto é aproveitado em consultas que usam apenas as primeiras colunas.
- Evite indexar colunas de baixa cardinalidade (ex.: flags/booleans); use índices parciais se necessário.
- Crie índices em colunas de junção (FKs) nas tabelas grandes para acelerar joins com tabelas de domínio.

## Exemplos por tabela
- `empresas`
  - PK: `(cnpj_basico)` já definida.
  - Recomendações: índice em `natureza_juridica_codigo` quando houver filtragem frequente por natureza.
  - Exemplo:
    ```sql
    CREATE INDEX IF NOT EXISTS idx_empresas_natureza ON empresas (natureza_juridica_codigo);
    ```

- `estabelecimentos`
  - PK: `(cnpj_basico, cnpj_ordem, cnpj_dv)`.
  - Recomendações:
    - Índice auxiliar em `cnpj_basico` para acelerar joins com `empresas`.
    - Índices em `municipio_codigo`, `pais_codigo`, `cnae_fiscal_principal_codigo` conforme filtros.
    - GIN para buscas em `cnae_fiscal_secundaria` (array) quando usar `@>`/`ANY`.
    - Índice parcial para registros ativos (`situacao_cadastral = 2`).
  - Exemplos:
    ```sql
    CREATE INDEX IF NOT EXISTS idx_estabelecimentos_cnpj ON estabelecimentos (cnpj_basico);
    CREATE INDEX IF NOT EXISTS idx_estabelecimentos_municipio ON estabelecimentos (municipio_codigo);
    CREATE INDEX IF NOT EXISTS idx_estabelecimentos_pais ON estabelecimentos (pais_codigo);
    CREATE INDEX IF NOT EXISTS idx_estabelecimentos_cnae_sec USING GIN (cnae_fiscal_secundaria);
    CREATE INDEX IF NOT EXISTS idx_estabelecimentos_ativas ON estabelecimentos (cnpj_basico)
      WHERE situacao_cadastral = 2;
    ```

- `socios`
  - Recomendações: índices em `cnpj_basico`, `qualificacao_socio_codigo`, `pais_codigo` conforme filtros.
  - Exemplos (alguns já existem em `constraints.sql`):
    ```sql
    CREATE INDEX IF NOT EXISTS idx_socios_cnpj ON socios (cnpj_basico);
    CREATE INDEX IF NOT EXISTS idx_socios_qualificacao ON socios (qualificacao_socio_codigo);
    CREATE INDEX IF NOT EXISTS idx_socios_pais ON socios (pais_codigo);
    ```

- `simples`
  - PK: `(cnpj_basico)`.
  - Recomendações: índices parciais em colunas de baixa cardinalidade, se consultas concentradas (ex.: `opcao_pelo_mei` = 'S').
  - Exemplo:
    ```sql
    CREATE INDEX IF NOT EXISTS idx_simples_mei ON simples (cnpj_basico)
      WHERE opcao_pelo_mei = 'S';
    ```

## Seletividade e cardinalidade
- Meça seletividade com `pg_stats` e análises de distribuição; evite indexar colunas com poucos valores distintos.
- Use compostos quando filtros são correlacionados (ex.: `cnpj_basico` + `cnpj_ordem`).
- A ordem no índice composto deve refletir: coluna mais seletiva primeiro e compatível com cláusulas `WHERE`/`ORDER BY` predominantes.

## Manutenção
- Pós-carga: execute `ANALYZE` para estatísticas atualizadas; `VACUUM` reduz bloat e melhora desempenho do planner.
- Monitoramento: `pg_stat_user_indexes` para identificar índices pouco usados (`idx_scan` baixo) e revisar.
- Bloat: considere `REINDEX` em índices muito inchados; avalie `fillfactor` para tabelas com muita atualização.

### Consulta de uso de índices
```sql
SELECT relname AS tabela, indexrelname AS indice, idx_scan
FROM pg_stat_user_indexes
WHERE schemaname = 'public'
ORDER BY idx_scan ASC;
```

### Pós-carga: ANALYZE e VACUUM
Execute após `COPY`/ingestão completa:
```sql
-- Atualiza estatísticas e limpa tuples mortos
VACUUM (ANALYZE, VERBOSE) empresas;
VACUUM (ANALYZE, VERBOSE) estabelecimentos;
VACUUM (ANALYZE, VERBOSE) socios;
VACUUM (ANALYZE, VERBOSE) simples;

-- Alternativa: apenas ANALYZE
ANALYZE VERBOSE empresas;
ANALYZE VERBOSE estabelecimentos;
ANALYZE VERBOSE socios;
ANALYZE VERBOSE simples;
```

### Dicas adicionais
- Evite criar índices durante a carga massiva; prefira criar após ingestão para acelerar `COPY`.
- Use `CREATE INDEX CONCURRENTLY` apenas se precisar manter tabelas online com menor bloqueio; para cargas offline, o padrão é mais rápido.
- Revise periodicamente índices sem uso significativo e remova-os para reduzir custo de escrita/manutenção.

