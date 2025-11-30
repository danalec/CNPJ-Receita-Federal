-- ============================================================================
-- ARQUIVO: constraints.sql (IDEMPOTENTE / RESUMABLE)
-- DESCRIÇÃO: Cria PKs, FKs e Índices apenas se ainda não existirem.
-- ============================================================================

-- ============================================================================
-- 1. CHAVES PRIMÁRIAS (PKs)
-- ============================================================================
-- Usamos blocos anônimos (DO) para verificar se a constraint já existe antes de criar.

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'paises_pkey') THEN
        ALTER TABLE paises ADD CONSTRAINT paises_pkey PRIMARY KEY (codigo);
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'municipios_pkey') THEN
        ALTER TABLE municipios ADD CONSTRAINT municipios_pkey PRIMARY KEY (codigo);
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'qualificacoes_socios_pkey') THEN
        ALTER TABLE qualificacoes_socios ADD CONSTRAINT qualificacoes_socios_pkey PRIMARY KEY (codigo);
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'naturezas_juridicas_pkey') THEN
        ALTER TABLE naturezas_juridicas ADD CONSTRAINT naturezas_juridicas_pkey PRIMARY KEY (codigo);
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'cnaes_pkey') THEN
        ALTER TABLE cnaes ADD CONSTRAINT cnaes_pkey PRIMARY KEY (codigo);
    END IF;
END $$;

-- Tabelas Grandes (PKs)

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'empresas_pkey') THEN
        ALTER TABLE empresas ADD CONSTRAINT empresas_pkey PRIMARY KEY (cnpj_basico);
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'estabelecimentos_pkey') THEN
        ALTER TABLE estabelecimentos ADD CONSTRAINT estabelecimentos_pkey PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv);
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'simples_pkey') THEN
        ALTER TABLE simples ADD CONSTRAINT simples_pkey PRIMARY KEY (cnpj_basico);
    END IF;
END $$;

-- ============================================================================
-- 2. ÍNDICES (Com IF NOT EXISTS)
-- ============================================================================
-- PostgreSQL suporta IF NOT EXISTS nativo para índices.

-- Sócios
CREATE INDEX IF NOT EXISTS idx_socios_cnpj_basico ON socios (cnpj_basico);
CREATE INDEX IF NOT EXISTS idx_socios_nome ON socios (nome_socio_ou_razao_social);
CREATE INDEX IF NOT EXISTS idx_socios_cpf_cnpj ON socios (cnpj_cpf_socio);

-- Estabelecimentos
CREATE INDEX IF NOT EXISTS idx_estabelecimentos_uf ON estabelecimentos (uf);
CREATE INDEX IF NOT EXISTS idx_estabelecimentos_cnae ON estabelecimentos (cnae_fiscal_principal_codigo);

-- Empresas
CREATE INDEX IF NOT EXISTS idx_empresas_razao_social ON empresas (razao_social);

-- ============================================================================
-- 3. CHAVES ESTRANGEIRAS (FKs)
-- ============================================================================
-- Novamente, blocos DO para garantir que não dê erro se já existir.

-- FKs EMPRESAS
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_empresas_natureza') THEN
        ALTER TABLE empresas ADD CONSTRAINT fk_empresas_natureza FOREIGN KEY (natureza_juridica_codigo) REFERENCES naturezas_juridicas (codigo);
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_empresas_qualificacao') THEN
        ALTER TABLE empresas ADD CONSTRAINT fk_empresas_qualificacao FOREIGN KEY (qualificacao_responsavel) REFERENCES qualificacoes_socios (codigo);
    END IF;
END $$;

-- FKs ESTABELECIMENTOS
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_estabelecimentos_empresa') THEN
        ALTER TABLE estabelecimentos ADD CONSTRAINT fk_estabelecimentos_empresa FOREIGN KEY (cnpj_basico) REFERENCES empresas (cnpj_basico);
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_estabelecimentos_pais') THEN
        ALTER TABLE estabelecimentos ADD CONSTRAINT fk_estabelecimentos_pais FOREIGN KEY (pais_codigo) REFERENCES paises (codigo);
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_estabelecimentos_municipio') THEN
        ALTER TABLE estabelecimentos ADD CONSTRAINT fk_estabelecimentos_municipio FOREIGN KEY (municipio_codigo) REFERENCES municipios (codigo);
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_estabelecimentos_cnae') THEN
        ALTER TABLE estabelecimentos ADD CONSTRAINT fk_estabelecimentos_cnae FOREIGN KEY (cnae_fiscal_principal_codigo) REFERENCES cnaes (codigo);
    END IF;
END $$;

-- FKs SIMPLES
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_simples_empresa') THEN
        ALTER TABLE simples ADD CONSTRAINT fk_simples_empresa FOREIGN KEY (cnpj_basico) REFERENCES empresas (cnpj_basico);
    END IF;
END $$;

-- FKs SOCIOS
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_socios_empresa') THEN
        ALTER TABLE socios ADD CONSTRAINT fk_socios_empresa FOREIGN KEY (cnpj_basico) REFERENCES empresas (cnpj_basico);
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_socios_pais') THEN
        ALTER TABLE socios ADD CONSTRAINT fk_socios_pais FOREIGN KEY (pais_codigo) REFERENCES paises (codigo);
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_socios_qualificacao') THEN
        ALTER TABLE socios ADD CONSTRAINT fk_socios_qualificacao FOREIGN KEY (qualificacao_socio_codigo) REFERENCES qualificacoes_socios (codigo);
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_socios_qualif_rep') THEN
        ALTER TABLE socios ADD CONSTRAINT fk_socios_qualif_rep FOREIGN KEY (qualificacao_representante_legal_codigo) REFERENCES qualificacoes_socios (codigo);
    END IF;
END $$;