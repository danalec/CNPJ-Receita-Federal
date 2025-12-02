-- ============================================================================
-- 0. AUTO-REPAIR (CORREÇÃO DE DADOS FALTANTES NAS TABELAS DE DOMÍNIO)
-- ============================================================================

-- 0.1a Corrigir Qualificações (Origem: Empresas)
INSERT INTO qualificacoes_socios (codigo, nome)
SELECT DISTINCT e.qualificacao_responsavel, 'NÃO INFORMADO NA ORIGEM (' || e.qualificacao_responsavel || ')'
FROM empresas e
LEFT JOIN qualificacoes_socios q ON e.qualificacao_responsavel = q.codigo
WHERE q.codigo IS NULL AND e.qualificacao_responsavel IS NOT NULL;

-- 0.1b Corrigir Qualificações (Origem: Sócios)
INSERT INTO qualificacoes_socios (codigo, nome)
SELECT DISTINCT s.qualificacao_socio_codigo, 'NÃO INFORMADO NA ORIGEM (' || s.qualificacao_socio_codigo || ')'
FROM socios s
LEFT JOIN qualificacoes_socios q ON s.qualificacao_socio_codigo = q.codigo
WHERE q.codigo IS NULL AND s.qualificacao_socio_codigo IS NOT NULL;

-- 0.2 Corrigir Naturezas Jurídicas
INSERT INTO naturezas_juridicas (codigo, nome)
SELECT DISTINCT e.natureza_juridica_codigo, 'NÃO INFORMADO NA ORIGEM (' || e.natureza_juridica_codigo || ')'
FROM empresas e
LEFT JOIN naturezas_juridicas n ON e.natureza_juridica_codigo = n.codigo
WHERE n.codigo IS NULL AND e.natureza_juridica_codigo IS NOT NULL;

-- 0.3a Corrigir Países (Origem: Estabelecimentos)
INSERT INTO paises (codigo, nome)
SELECT DISTINCT est.pais_codigo, 'NÃO INFORMADO NA ORIGEM (' || est.pais_codigo || ')'
FROM estabelecimentos est
LEFT JOIN paises p ON est.pais_codigo = p.codigo
WHERE p.codigo IS NULL AND est.pais_codigo IS NOT NULL;

-- 0.3b Corrigir Países (Origem: Sócios)
INSERT INTO paises (codigo, nome)
SELECT DISTINCT s.pais_codigo, 'NÃO INFORMADO NA ORIGEM (' || s.pais_codigo || ')'
FROM socios s
LEFT JOIN paises p ON s.pais_codigo = p.codigo
WHERE p.codigo IS NULL AND s.pais_codigo IS NOT NULL;

-- 0.4 Corrigir Municípios
INSERT INTO municipios (codigo, nome)
SELECT DISTINCT est.municipio_codigo, 'NÃO INFORMADO NA ORIGEM (' || est.municipio_codigo || ')'
FROM estabelecimentos est
LEFT JOIN municipios m ON est.municipio_codigo = m.codigo
WHERE m.codigo IS NULL AND est.municipio_codigo IS NOT NULL;

-- 0.5 Corrigir CNAEs
INSERT INTO cnaes (codigo, nome)
SELECT DISTINCT est.cnae_fiscal_principal_codigo, 'NÃO INFORMADO NA ORIGEM (' || est.cnae_fiscal_principal_codigo || ')'
FROM estabelecimentos est
LEFT JOIN cnaes c ON est.cnae_fiscal_principal_codigo = c.codigo
WHERE c.codigo IS NULL AND est.cnae_fiscal_principal_codigo IS NOT NULL;

-- ============================================================================
-- 1. CHAVES PRIMÁRIAS (PKs)
-- ============================================================================
-- Criamos as PKs AGORA. Isso é crucial para que a limpeza de órfãos (próximo passo)
-- seja rápida, usando índices em vez de varrer a tabela inteira.

-- Domínios
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

-- Tabelas Principais
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
-- 2. LIMPEZA DE ÓRFÃOS (REGISTROS SEM EMPRESA PAI)
-- ============================================================================
-- Remove registros filhos que apontam para CNPJs que não existem na tabela empresas.
-- Isso resolve o erro "Key (cnpj_basico)=(...) is not present in table empresas".

DELETE FROM simples s
WHERE NOT EXISTS (
    SELECT 1 FROM empresas e WHERE e.cnpj_basico = s.cnpj_basico
);

DELETE FROM socios s
WHERE NOT EXISTS (
    SELECT 1 FROM empresas e WHERE e.cnpj_basico = s.cnpj_basico
);

DELETE FROM estabelecimentos est
WHERE NOT EXISTS (
    SELECT 1 FROM empresas e WHERE e.cnpj_basico = est.cnpj_basico
);


-- ============================================================================
-- 3. ÍNDICES SECUNDÁRIOS
-- ============================================================================

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
-- 4. CHAVES ESTRANGEIRAS (FKs)
-- ============================================================================

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