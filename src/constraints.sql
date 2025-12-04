-- ============================================================================
-- ARQUIVO: constraints.sql
-- DESCRIÇÃO: Criação de PKs, FKs e Índices APÓS a carga de dados.
-- ============================================================================

SET search_path TO rfb;

DO $$
BEGIN
  IF current_setting('app.enable_backfill', true) = '1' THEN
    INSERT INTO paises (codigo, nome)
    SELECT DISTINCT est.pais_codigo, 'NÃO INFORMADO NA ORIGEM (' || est.pais_codigo || ')'
    FROM estabelecimentos est
    LEFT JOIN paises p ON est.pais_codigo = p.codigo
    WHERE p.codigo IS NULL AND est.pais_codigo IS NOT NULL;

    INSERT INTO municipios (codigo, nome)
    SELECT DISTINCT est.municipio_codigo, 'NÃO INFORMADO NA ORIGEM (' || est.municipio_codigo || ')'
    FROM estabelecimentos est
    LEFT JOIN municipios m ON est.municipio_codigo = m.codigo
    WHERE m.codigo IS NULL AND est.municipio_codigo IS NOT NULL;

    INSERT INTO qualificacoes_socios (codigo, nome)
    SELECT DISTINCT e.qualificacao_responsavel, 'NÃO INFORMADO NA ORIGEM (' || e.qualificacao_responsavel || ')'
    FROM empresas e
    LEFT JOIN qualificacoes_socios q ON e.qualificacao_responsavel = q.codigo
    WHERE q.codigo IS NULL AND e.qualificacao_responsavel IS NOT NULL;

    INSERT INTO qualificacoes_socios (codigo, nome)
    SELECT DISTINCT s.qualificacao_socio_codigo, 'NÃO INFORMADO NA ORIGEM (' || s.qualificacao_socio_codigo || ')'
    FROM socios s
    LEFT JOIN qualificacoes_socios q ON s.qualificacao_socio_codigo = q.codigo
    WHERE q.codigo IS NULL AND s.qualificacao_socio_codigo IS NOT NULL;

    INSERT INTO naturezas_juridicas (codigo, nome)
    SELECT DISTINCT e.natureza_juridica_codigo, 'NÃO INFORMADO NA ORIGEM (' || e.natureza_juridica_codigo || ')'
    FROM empresas e
    LEFT JOIN naturezas_juridicas n ON e.natureza_juridica_codigo = n.codigo
    WHERE n.codigo IS NULL AND e.natureza_juridica_codigo IS NOT NULL;

    INSERT INTO cnaes (codigo, nome)
    SELECT DISTINCT est.cnae_fiscal_principal_codigo, 'NÃO INFORMADO NA ORIGEM (' || est.cnae_fiscal_principal_codigo || ')'
    FROM estabelecimentos est
    LEFT JOIN cnaes c ON est.cnae_fiscal_principal_codigo = c.codigo
    WHERE c.codigo IS NULL AND est.cnae_fiscal_principal_codigo IS NOT NULL;
  END IF;
END
$$;

ALTER TABLE paises ADD PRIMARY KEY (codigo);
ALTER TABLE municipios ADD PRIMARY KEY (codigo);
ALTER TABLE qualificacoes_socios ADD PRIMARY KEY (codigo);
ALTER TABLE naturezas_juridicas ADD PRIMARY KEY (codigo);
ALTER TABLE cnaes ADD PRIMARY KEY (codigo);

-- 2. CHAVES PRIMÁRIAS E ÍNDICES (Tabelas Grandes)

-- Empresas (PK: CNPJ Básico)
ALTER TABLE empresas ADD PRIMARY KEY (cnpj_basico);

-- Estabelecimentos (PK: Básico + Ordem + DV)
-- Nota: Usamos as 3 partes para garantir unicidade do CNPJ completo
ALTER TABLE estabelecimentos ADD PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv);

-- Simples (PK: CNPJ Básico) - Assumindo 1 registro por empresa no arquivo simples
ALTER TABLE simples ADD PRIMARY KEY (cnpj_basico);

-- Sócios não tem PK simples garantida (mesmo sócio em várias empresas ou identificador repetido).
-- Criamos índices para performance de busca.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_socios_cnpj_basico ON socios (cnpj_basico);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_socios_nome ON socios (nome_socio_ou_razao_social);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_socios_cpf_cnpj ON socios (cnpj_cpf_socio);

-- Índices adicionais úteis para buscas comuns
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estabelecimentos_uf ON estabelecimentos (uf);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estabelecimentos_cnae ON estabelecimentos (cnae_fiscal_principal_codigo);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_empresas_razao_social ON empresas (razao_social);

-- 3. CHAVES ESTRANGEIRAS (FKs)

-- FKs da Tabela EMPRESAS
ALTER TABLE empresas 
    ADD CONSTRAINT fk_empresas_natureza 
    FOREIGN KEY (natureza_juridica_codigo) REFERENCES naturezas_juridicas (codigo);

ALTER TABLE empresas 
    ADD CONSTRAINT fk_empresas_qualificacao 
    FOREIGN KEY (qualificacao_responsavel) REFERENCES qualificacoes_socios (codigo);

-- FKs da Tabela ESTABELECIMENTOS
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

-- FKs da Tabela SIMPLES
ALTER TABLE simples 
    ADD CONSTRAINT fk_simples_empresa 
    FOREIGN KEY (cnpj_basico) REFERENCES empresas (cnpj_basico);

-- FKs da Tabela SOCIOS
ALTER TABLE socios 
    ADD CONSTRAINT fk_socios_empresa 
    FOREIGN KEY (cnpj_basico) REFERENCES empresas (cnpj_basico);

ALTER TABLE socios 
    ADD CONSTRAINT fk_socios_pais 
    FOREIGN KEY (pais_codigo) REFERENCES paises (codigo);

ALTER TABLE socios 
    ADD CONSTRAINT fk_socios_qualificacao 
    FOREIGN KEY (qualificacao_socio_codigo) REFERENCES qualificacoes_socios (codigo);

ALTER TABLE socios 
    ADD CONSTRAINT fk_socios_qualif_rep 
    FOREIGN KEY (qualificacao_representante_legal_codigo) REFERENCES qualificacoes_socios (codigo);
