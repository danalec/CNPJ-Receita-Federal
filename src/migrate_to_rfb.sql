-- Move existing non-namespaced tables to schema rfb safely
CREATE SCHEMA IF NOT EXISTS rfb;

DO $$
BEGIN
  -- Empresas
  IF to_regclass('public.empresas') IS NOT NULL AND to_regclass('rfb.empresas') IS NULL THEN
    EXECUTE 'ALTER TABLE public.empresas SET SCHEMA rfb';
  END IF;

  -- Estabelecimentos
  IF to_regclass('public.estabelecimentos') IS NOT NULL AND to_regclass('rfb.estabelecimentos') IS NULL THEN
    EXECUTE 'ALTER TABLE public.estabelecimentos SET SCHEMA rfb';
  END IF;

  -- Simples
  IF to_regclass('public.simples') IS NOT NULL AND to_regclass('rfb.simples') IS NULL THEN
    EXECUTE 'ALTER TABLE public.simples SET SCHEMA rfb';
  END IF;

  -- Sócios
  IF to_regclass('public.socios') IS NOT NULL AND to_regclass('rfb.socios') IS NULL THEN
    EXECUTE 'ALTER TABLE public.socios SET SCHEMA rfb';
  END IF;

  -- Países
  IF to_regclass('public.paises') IS NOT NULL AND to_regclass('rfb.paises') IS NULL THEN
    EXECUTE 'ALTER TABLE public.paises SET SCHEMA rfb';
  END IF;

  -- Municípios
  IF to_regclass('public.municipios') IS NOT NULL AND to_regclass('rfb.municipios') IS NULL THEN
    EXECUTE 'ALTER TABLE public.municipios SET SCHEMA rfb';
  END IF;

  -- Qualificações de Sócios
  IF to_regclass('public.qualificacoes_socios') IS NOT NULL AND to_regclass('rfb.qualificacoes_socios') IS NULL THEN
    EXECUTE 'ALTER TABLE public.qualificacoes_socios SET SCHEMA rfb';
  END IF;

  -- Naturezas Jurídicas
  IF to_regclass('public.naturezas_juridicas') IS NOT NULL AND to_regclass('rfb.naturezas_juridicas') IS NULL THEN
    EXECUTE 'ALTER TABLE public.naturezas_juridicas SET SCHEMA rfb';
  END IF;

  -- CNAEs
  IF to_regclass('public.cnaes') IS NOT NULL AND to_regclass('rfb.cnaes') IS NULL THEN
    EXECUTE 'ALTER TABLE public.cnaes SET SCHEMA rfb';
  END IF;
END
$$;
