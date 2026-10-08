
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
-- Every constraint and index below is dropped first so this file can be applied
-- more than once: run_loader applies it as the final step of `load`, and
-- `--step constraints` / `tasks.ps1 etl` may apply it again right afterwards.
ALTER TABLE paises DROP CONSTRAINT IF EXISTS paises_pkey;
ALTER TABLE municipios DROP CONSTRAINT IF EXISTS municipios_pkey;
ALTER TABLE qualificacoes_socios DROP CONSTRAINT IF EXISTS qualificacoes_socios_pkey;
ALTER TABLE naturezas_juridicas DROP CONSTRAINT IF EXISTS naturezas_juridicas_pkey;
ALTER TABLE cnaes DROP CONSTRAINT IF EXISTS cnaes_pkey;
ALTER TABLE empresas DROP CONSTRAINT IF EXISTS empresas_pkey;
ALTER TABLE estabelecimentos DROP CONSTRAINT IF EXISTS estabelecimentos_pkey;
ALTER TABLE simples DROP CONSTRAINT IF EXISTS simples_pkey;

ALTER TABLE paises ADD CONSTRAINT paises_pkey PRIMARY KEY (codigo);
ALTER TABLE municipios ADD CONSTRAINT municipios_pkey PRIMARY KEY (codigo);
ALTER TABLE qualificacoes_socios ADD CONSTRAINT qualificacoes_socios_pkey PRIMARY KEY (codigo);
ALTER TABLE naturezas_juridicas ADD CONSTRAINT naturezas_juridicas_pkey PRIMARY KEY (codigo);
ALTER TABLE cnaes ADD CONSTRAINT cnaes_pkey PRIMARY KEY (codigo);

-- Empresas PK
ALTER TABLE empresas ADD CONSTRAINT empresas_pkey PRIMARY KEY (cnpj_basico);

-- Estabelecimentos PK (Composite)
-- Note: Using a composite PK might be heavy.
-- Usually queries are by CNPJ (basico+ordem+dv).
ALTER TABLE estabelecimentos ADD CONSTRAINT estabelecimentos_pkey PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv);

-- Socios PK (Composite)
-- We assume identifiers are unique per company?
-- The documentation doesn't specify a clear PK for socios, but (cnpj_basico, identificador_socio) seems reasonable?
-- Or (cnpj_basico, nome_socio_ou_razao_social)?
-- Let's stick to indexes for now if PK is uncertain, but typically (cnpj_basico, identificador_socio) is a candidate.
-- However, let's just index it for now to avoid issues if data is dirty.
CREATE INDEX IF NOT EXISTS idx_socios_cnpj_basico ON socios (cnpj_basico);

-- Simples PK
ALTER TABLE simples ADD CONSTRAINT simples_pkey PRIMARY KEY (cnpj_basico);

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
-- 4. Foreign Keys
-- ----------------------------------------------------------------------------

ALTER TABLE empresas DROP CONSTRAINT IF EXISTS fk_empresas_natureza;
ALTER TABLE empresas DROP CONSTRAINT IF EXISTS fk_empresas_qualificacao;
ALTER TABLE estabelecimentos DROP CONSTRAINT IF EXISTS fk_estabelecimentos_empresa;
ALTER TABLE estabelecimentos DROP CONSTRAINT IF EXISTS fk_estabelecimentos_pais;
ALTER TABLE estabelecimentos DROP CONSTRAINT IF EXISTS fk_estabelecimentos_municipio;
ALTER TABLE estabelecimentos DROP CONSTRAINT IF EXISTS fk_estabelecimentos_cnae;
ALTER TABLE socios DROP CONSTRAINT IF EXISTS fk_socios_empresa;
ALTER TABLE socios DROP CONSTRAINT IF EXISTS fk_socios_pais;
ALTER TABLE socios DROP CONSTRAINT IF EXISTS fk_socios_qualificacao;
ALTER TABLE simples DROP CONSTRAINT IF EXISTS fk_simples_empresa;

ALTER TABLE empresas 
    ADD CONSTRAINT fk_empresas_natureza 
    FOREIGN KEY (natureza_juridica_codigo) REFERENCES naturezas_juridicas (codigo);

-- Note: qualificacao_responsavel sometimes points to values not in qualificacoes_socios?
-- Assuming it does.
ALTER TABLE empresas 
    ADD CONSTRAINT fk_empresas_qualificacao 
    FOREIGN KEY (qualificacao_responsavel) REFERENCES qualificacoes_socios (codigo);

ALTER TABLE estabelecimentos 
    ADD CONSTRAINT fk_estabelecimentos_empresa 
    FOREIGN KEY (cnpj_basico) REFERENCES empresas (cnpj_basico);

ALTER TABLE estabelecimentos 
    ADD CONSTRAINT fk_estabelecimentos_pais 
    FOREIGN KEY (pais_codigo) REFERENCES paises (codigo);

ALTER TABLE estabelecimentos 
    ADD CONSTRAINT fk_estabelecimentos_municipio 
    FOREIGN KEY (municipio_codigo) REFERENCES municipios (codigo);

ALTER TABLE estabelecimentos 
    ADD CONSTRAINT fk_estabelecimentos_cnae 
    FOREIGN KEY (cnae_fiscal_principal_codigo) REFERENCES cnaes (codigo);

ALTER TABLE socios 
    ADD CONSTRAINT fk_socios_empresa 
    FOREIGN KEY (cnpj_basico) REFERENCES empresas (cnpj_basico);

ALTER TABLE socios 
    ADD CONSTRAINT fk_socios_pais 
    FOREIGN KEY (pais_codigo) REFERENCES paises (codigo);

ALTER TABLE socios 
    ADD CONSTRAINT fk_socios_qualificacao 
    FOREIGN KEY (qualificacao_socio_codigo) REFERENCES qualificacoes_socios (codigo);

ALTER TABLE simples 
    ADD CONSTRAINT fk_simples_empresa 
    FOREIGN KEY (cnpj_basico) REFERENCES empresas (cnpj_basico);

