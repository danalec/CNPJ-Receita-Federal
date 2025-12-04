
DO $$ BEGIN END $$;
ALTER TABLE rfb.paises ADD CONSTRAINT paises_pkey CHECK (true);
ALTER TABLE rfb.empresas ADD CONSTRAINT fk_empresas_natureza CHECK (true);
ALTER TABLE rfb.empresas ADD CONSTRAINT fk_empresas_qualificacao CHECK (true);
ALTER TABLE rfb.estabelecimentos ADD CONSTRAINT fk_estabelecimentos_empresa CHECK (true);
ALTER TABLE rfb.estabelecimentos ADD CONSTRAINT fk_estabelecimentos_pais CHECK (true);
ALTER TABLE rfb.estabelecimentos ADD CONSTRAINT fk_estabelecimentos_municipio CHECK (true);
ALTER TABLE rfb.estabelecimentos ADD CONSTRAINT fk_estabelecimentos_cnae CHECK (true);
    
