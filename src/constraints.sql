
-- ============================================================================
-- CONSTRAINTS & INDEXES
-- ============================================================================
SET search_path TO rfb;

-- Required by the gin_trgm_ops indexes below (idx_empresas_razao_social,
-- idx_estabelecimentos_nome_fantasia, idx_socios_nome). Without this the
-- CREATE INDEX statements fail with "operator class gin_trgm_ops does not exist".
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ----------------------------------------------------------------------------
-- 1. Primary Keys
-- ----------------------------------------------------------------------------
-- Applied through a guard on pg_constraint rather than DROP + ADD. This file is
-- applied more than once per pipeline (run_loader applies it at the end of
-- `load`, then `--step constraints` applies it again), and rebuilding a primary
-- key over a 100M-row table twice is expensive. It is also crash-safe: a
-- DROP/ADD sequence that fails midway leaves the table with no primary key.
DO $$
DECLARE
    spec   text;
    tbl    text;
    cname  text;
    cdef   text;
BEGIN
    FOREACH spec IN ARRAY ARRAY[
        'paises|paises_pkey|PRIMARY KEY (codigo)',
        'municipios|municipios_pkey|PRIMARY KEY (codigo)',
        'qualificacoes_socios|qualificacoes_socios_pkey|PRIMARY KEY (codigo)',
        'naturezas_juridicas|naturezas_juridicas_pkey|PRIMARY KEY (codigo)',
        'cnaes|cnaes_pkey|PRIMARY KEY (codigo)',
        'empresas|empresas_pkey|PRIMARY KEY (cnpj_basico)',
        'estabelecimentos|estabelecimentos_pkey|PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv)',
        'simples|simples_pkey|PRIMARY KEY (cnpj_basico)'
    ] LOOP
        tbl   := split_part(spec, '|', 1);
        cname := split_part(spec, '|', 2);
        cdef  := split_part(spec, '|', 3);
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = cname) THEN
            EXECUTE format('ALTER TABLE %I ADD CONSTRAINT %I %s', tbl, cname, cdef);
            RAISE NOTICE 'Criada constraint %', cname;
        END IF;
    END LOOP;
END
$$;

-- Socios: no confident primary key, so index only. Queries go by cnpj_basico.
CREATE INDEX IF NOT EXISTS idx_socios_cnpj_basico ON socios (cnpj_basico);

-- ----------------------------------------------------------------------------
-- 2. Indexes for Foreign Keys and Performance
-- ----------------------------------------------------------------------------

-- Empresas
CREATE INDEX IF NOT EXISTS idx_empresas_natureza ON empresas (natureza_juridica_codigo);
CREATE INDEX IF NOT EXISTS idx_empresas_qualificacao ON empresas (qualificacao_responsavel);
CREATE INDEX IF NOT EXISTS idx_empresas_razao_social ON empresas USING gin (razao_social gin_trgm_ops); -- Requires pg_trgm

-- Estabelecimentos
CREATE INDEX IF NOT EXISTS idx_estabelecimentos_cnae_main ON estabelecimentos (cnae_fiscal_principal_codigo);
CREATE INDEX IF NOT EXISTS idx_estabelecimentos_municipio ON establishments (municipio_codigo);
CREATE INDEX IF NOT EXISTS idx_estabelecimentos_uf ON estabelecimentos (uf);
CREATE INDEX IF NOT EXISTS idx_estabelecimentos_nome_fantasia ON estabelecimentos USING gin (nome_fantasia gin_trgm_ops);

-- Socios
CREATE INDEX IF NOT EXISTS idx_socios_cpf_cnpj ON socios (cnpj_cpf_socio);
CREATE INDEX IF NOT EXISTS idx_socios_nome ON socios USING gin (nome_socio_ou_razao_social gin_trgm_ops);

-- ----------------------------------------------------------------------------
-- 3. Backfill Logic (Optional)
-- ----------------------------------------------------------------------------
-- Logic to insert missing parent records to avoid FK violations.
-- Controlled by app.enable_backfill variable.

DO $$
DECLARE
    _enable_backfill text;
BEGIN
    BEGIN
        _enable_backfill := current_setting('app.enable_backfill');
    EXCEPTION WHEN OTHERS THEN
        _enable_backfill := '1'; -- Default to true if not set
    END;

    IF _enable_backfill = '1' THEN
        RAISE NOTICE 'Starting Backfill for Missing FK Targets...';

        -- 3.1 Paises
        INSERT INTO paises (codigo, nome)
        SELECT DISTINCT e.pais_codigo, 'NAO CONSTA NA ORIGEM'
        FROM estabelecimentos e
        WHERE e.pais_codigo IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM paises p WHERE p.codigo = e.pais_codigo)
        ON CONFLICT DO NOTHING;

        -- 3.2 Municipios
        INSERT INTO municipios (codigo, nome)
        SELECT DISTINCT e.municipio_codigo, 'NAO CONSTA NA ORIGEM'
        FROM estabelecimentos e
        WHERE e.municipio_codigo IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM municipios m WHERE m.codigo = e.municipio_codigo)
        ON CONFLICT DO NOTHING;
        
        -- 3.3 Naturezas
        INSERT INTO naturezas_juridicas (codigo, nome)
        SELECT DISTINCT e.natureza_juridica_codigo, 'NAO CONSTA NA ORIGEM'
        FROM empresas e
        WHERE e.natureza_juridica_codigo IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM naturezas_juridicas n WHERE n.codigo = e.natureza_juridica_codigo)
        ON CONFLICT DO NOTHING;
        
        -- 3.4 Cnaes
        INSERT INTO cnaes (codigo, nome)
        SELECT DISTINCT e.cnae_fiscal_principal_codigo, 'NAO CONSTA NA ORIGEM'
        FROM estabelecimentos e
        WHERE e.cnae_fiscal_principal_codigo IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM cnaes c WHERE c.codigo = e.cnae_fiscal_principal_codigo)
        ON CONFLICT DO NOTHING;

        -- 3.5 Empresas (from Estabelecimentos)
        -- This is heavy. Only do if strictly necessary.
        -- If we have an estabelecimento without an empresa, we create a dummy empresa.
        INSERT INTO empresas (cnpj_basico, razao_social)
        SELECT DISTINCT e.cnpj_basico, 'EMPRESA INEXISTENTE NA ORIGEM - GERADA AUTOMATICAMENTE'
        FROM estabelecimentos e
        WHERE NOT EXISTS (SELECT 1 FROM empresas emp WHERE emp.cnpj_basico = e.cnpj_basico)
        ON CONFLICT DO NOTHING;

    ELSE
        RAISE NOTICE 'Skipping Backfill (disabled via app.enable_backfill)';
    END IF;
END
$$;

-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- 4. Foreign Keys
-- ----------------------------------------------------------------------------
-- Same pg_constraint guard as the primary keys: adding a foreign key revalidates
-- every row on both sides, which is not something to do twice per run.
DO $$
DECLARE
    spec   text;
    tbl    text;
    cname  text;
    cdef   text;
BEGIN
    FOREACH spec IN ARRAY ARRAY[
        'empresas|fk_empresas_natureza|FOREIGN KEY (natureza_juridica_codigo) REFERENCES naturezas_juridicas (codigo)',
        'empresas|fk_empresas_qualificacao|FOREIGN KEY (qualificacao_responsavel) REFERENCES qualificacoes_socios (codigo)',
        'estabelecimentos|fk_estabelecimentos_empresa|FOREIGN KEY (cnpj_basico) REFERENCES empresas (cnpj_basico)',
        'estabelecimentos|fk_estabelecimentos_pais|FOREIGN KEY (pais_codigo) REFERENCES paises (codigo)',
        'estabelecimentos|fk_estabelecimentos_municipio|FOREIGN KEY (municipio_codigo) REFERENCES municipios (codigo)',
        'estabelecimentos|fk_estabelecimentos_cnae|FOREIGN KEY (cnae_fiscal_principal_codigo) REFERENCES cnaes (codigo)',
        'socios|fk_socios_empresa|FOREIGN KEY (cnpj_basico) REFERENCES empresas (cnpj_basico)',
        'socios|fk_socios_pais|FOREIGN KEY (pais_codigo) REFERENCES paises (codigo)',
        'socios|fk_socios_qualificacao|FOREIGN KEY (qualificacao_socio_codigo) REFERENCES qualificacoes_socios (codigo)',
        'simples|fk_simples_empresa|FOREIGN KEY (cnpj_basico) REFERENCES empresas (cnpj_basico)'
    ] LOOP
        tbl   := split_part(spec, '|', 1);
        cname := split_part(spec, '|', 2);
        cdef  := split_part(spec, '|', 3);
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = cname) THEN
            EXECUTE format('ALTER TABLE %I ADD CONSTRAINT %I %s', tbl, cname, cdef);
            RAISE NOTICE 'Criada constraint %', cname;
        END IF;
    END LOOP;
END
$$;
