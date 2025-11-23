-- ============================================================================
-- ARQUIVO: constraints.sql
-- DESCRIÇÃO: Criação de PKs, FKs e Índices APÓS a carga de dados.
-- ============================================================================

-- 1. CHAVES PRIMÁRIAS (Tabelas de Domínio)
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
CREATE INDEX CONCURRENTLY idx_socios_cnpj_basico ON socios (cnpj_basico);
CREATE INDEX CONCURRENTLY idx_socios_nome ON socios (nome_socio_ou_razao_social);
CREATE INDEX CONCURRENTLY idx_socios_cpf_cnpj ON socios (cnpj_cpf_socio);

-- Índices adicionais úteis para buscas comuns
CREATE INDEX CONCURRENTLY idx_estabelecimentos_uf ON estabelecimentos (uf);
CREATE INDEX CONCURRENTLY idx_estabelecimentos_cnae ON estabelecimentos (cnae_fiscal_principal_codigo);
CREATE INDEX CONCURRENTLY idx_empresas_razao_social ON empresas (razao_social);

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
