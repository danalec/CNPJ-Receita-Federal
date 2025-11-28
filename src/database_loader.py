import pandas as pd
import logging
import io
import re
import psycopg2
from psycopg2 import sql
from pathlib import Path
from .settings import settings
from .validation import validate as schema_validate

logger = logging.getLogger(__name__)

# --- Funções de Carga e Limpeza ---


def fast_load_chunk(conn, df, table_name):
    """
    Função de Carga Ultra-Rápida (PostgreSQL COPY).
    Recebe a conexão direta do psycopg2 (conn).
    """
    output = io.StringIO()

    # Prepara o CSV em memória
    df.to_csv(
        output,
        sep=";",
        header=False,
        index=False,
        na_rep="",
        quotechar='"',
        doublequote=True,
    )
    output.seek(0)

    columns = df.columns.tolist()

    # Usa um cursor para executar o COPY
    try:
        with conn.cursor() as cursor:
            ident_cols = [sql.Identifier(c) for c in columns]
            copy_stmt = sql.SQL(
                "COPY {table} ({cols}) FROM STDIN WITH (FORMAT CSV, DELIMITER ';', NULL '', QUOTE '\"', HEADER FALSE)"
            ).format(
                table=sql.Identifier(table_name),
                cols=sql.SQL(", ").join(ident_cols),
            )
            cursor.copy_expert(copy_stmt.as_string(conn), output)

        # O commit é feito no nível superior (loop de processamento)
        # para evitar commit a cada chunk pequeno se desejar,
        # mas aqui faremos commit por chunk para liberar memória do PG.
        conn.commit()

    except Exception as e:
        conn.rollback()
        logger.error(f"Erro no COPY para tabela {table_name}: {e}")
        raise


DOMAIN_CACHE = {}


def _get_domain_set(conn, table, column):
    key = (table, column)
    if key in DOMAIN_CACHE:
        return DOMAIN_CACHE[key]
    with conn.cursor() as cursor:
        cursor.execute(
            sql.SQL("SELECT DISTINCT {col} FROM {tbl}").format(
                col=sql.Identifier(column),
                tbl=sql.Identifier(table),
            )
        )
        s = set(r[0] for r in cursor.fetchall() if r[0] is not None)
    DOMAIN_CACHE[key] = s
    return s


UF_SET = {
    'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RO','RS','RR','SC','SE','SP','TO'
}


def _ensure_domains_loaded(conn, domains):
    with conn.cursor() as cursor:
        for tbl in domains:
            cursor.execute(
                sql.SQL("SELECT COUNT(*) FROM {tbl}").format(tbl=sql.Identifier(tbl))
            )
            cnt = cursor.fetchone()[0]
            if cnt == 0:
                raise RuntimeError(f"Tabela de domínio '{tbl}' ainda não carregada. Ajuste a ordem ou remova filtros --only/--exclude.")


def _validate_fk_set(df, column, valid_set, label):
    vals = set(df[column].dropna().unique()) if column in df.columns else set()
    missing = vals - valid_set
    if missing:
        sample = list(missing)[:10]
        raise ValueError(f"Valores inválidos em '{label}': {sample} (total {len(missing)}). Garanta que a tabela de domínio está carregada e os códigos são válidos.")


def validate_chunk(config_name, chunk_df, conn):
    if config_name == "empresas":
        _ensure_domains_loaded(conn, ["naturezas_juridicas", "qualificacoes_socios"])
        nat = _get_domain_set(conn, "naturezas_juridicas", "codigo")
        qual = _get_domain_set(conn, "qualificacoes_socios", "codigo")
        if "natureza_juridica_codigo" in chunk_df.columns:
            _validate_fk_set(chunk_df, "natureza_juridica_codigo", nat, "natureza_juridica_codigo")
        if "qualificacao_responsavel" in chunk_df.columns:
            _validate_fk_set(chunk_df, "qualificacao_responsavel", qual, "qualificacao_responsavel")
    elif config_name == "estabelecimentos":
        _ensure_domains_loaded(conn, ["paises", "municipios", "cnaes", "empresas"])
        paises = _get_domain_set(conn, "paises", "codigo")
        municipios = _get_domain_set(conn, "municipios", "codigo")
        cnaes = _get_domain_set(conn, "cnaes", "codigo")
        if "pais_codigo" in chunk_df.columns:
            _validate_fk_set(chunk_df, "pais_codigo", paises, "pais_codigo")
        if "municipio_codigo" in chunk_df.columns:
            _validate_fk_set(chunk_df, "municipio_codigo", municipios, "municipio_codigo")
        if "cnae_fiscal_principal_codigo" in chunk_df.columns:
            _validate_fk_set(chunk_df, "cnae_fiscal_principal_codigo", cnaes, "cnae_fiscal_principal_codigo")
        if "uf" in chunk_df.columns:
            _validate_fk_set(chunk_df, "uf", UF_SET, "uf")
        if "cnae_fiscal_secundaria" in chunk_df.columns:
            s = chunk_df["cnae_fiscal_secundaria"].dropna().astype(str)
            bad = ~((s.str.startswith("{") & s.str.endswith("}")) | (s == ""))
            if bad.any():
                raise ValueError("Campo cnae_fiscal_secundaria com formato inválido em algumas linhas. Esperado texto de array PostgreSQL: '{...}'.")
    elif config_name == "socios":
        _ensure_domains_loaded(conn, ["paises", "qualificacoes_socios", "empresas"])
        paises = _get_domain_set(conn, "paises", "codigo")
        qual = _get_domain_set(conn, "qualificacoes_socios", "codigo")
        if "pais_codigo" in chunk_df.columns:
            _validate_fk_set(chunk_df, "pais_codigo", paises, "pais_codigo")
        if "qualificacao_socio_codigo" in chunk_df.columns:
            _validate_fk_set(chunk_df, "qualificacao_socio_codigo", qual, "qualificacao_socio_codigo")
        if "qualificacao_representante_legal_codigo" in chunk_df.columns:
            _validate_fk_set(chunk_df, "qualificacao_representante_legal_codigo", qual, "qualificacao_representante_legal_codigo")


def sanitize_dates(df, date_columns):
    cols = [c for c in date_columns if c in df.columns]
    if cols:
        df[cols] = df[cols].apply(lambda s: pd.to_datetime(s, format="%Y%m%d", errors="coerce"))
    return df


def clean_empresas_chunk(chunk_df):
    if "capital_social" in chunk_df.columns:
        chunk_df["capital_social"] = pd.to_numeric(
            chunk_df["capital_social"].astype(str).str.replace(",", ".", regex=False),
            errors="coerce",
        )
    return chunk_df


def clean_estabelecimentos_chunk(chunk_df):
    date_cols = [
        "data_situacao_cadastral",
        "data_inicio_atividade",
        "data_situacao_especial",
    ]
    chunk_df = sanitize_dates(chunk_df, date_cols)
    col_name = "cnae_fiscal_secundaria"
    if col_name in chunk_df.columns:
        s = chunk_df[col_name].fillna("").astype(str)
        def to_pg_array(x):
            if not x.strip():
                return None
            parts = [p.strip() for p in re.split(r"[;,]", x) if p.strip()]
            return "{" + ",".join(parts) + "}" if parts else None
        chunk_df[col_name] = s.map(to_pg_array)
    return chunk_df


def clean_socios_chunk(chunk_df):
    return sanitize_dates(chunk_df, ["data_entrada_sociedade"])


def clean_simples_chunk(chunk_df):
    return sanitize_dates(
        chunk_df,
        [
            "data_opcao_pelo_simples",
            "data_exclusao_do_simples",
            "data_opcao_pelo_mei",
            "data_exclusao_do_mei",
        ],
    )


ETL_CONFIG = {
    "paises": {"table_name": "paises", "column_names": ["codigo", "nome"]},
    "municipios": {"table_name": "municipios", "column_names": ["codigo", "nome"]},
    "qualificacoes": {
        "table_name": "qualificacoes_socios",
        "column_names": ["codigo", "nome"],
    },
    "naturezas": {
        "table_name": "naturezas_juridicas",
        "column_names": ["codigo", "nome"],
    },
    "cnaes": {"table_name": "cnaes", "column_names": ["codigo", "nome"]},
    "empresas": {
        "table_name": "empresas",
        "column_names": [
            "cnpj_basico",
            "razao_social",
            "natureza_juridica_codigo",
            "qualificacao_responsavel",
            "capital_social",
            "porte_empresa",
            "ente_federativo_responsavel",
        ],
        "dtype_map": {
            "cnpj_basico": str,
            "natureza_juridica_codigo": pd.Int64Dtype(),
            "qualificacao_responsavel": pd.Int64Dtype(),
            "porte_empresa": pd.Int64Dtype(),
        },
        "custom_clean_func": clean_empresas_chunk,
    },
    "estabelecimentos": {
        "table_name": "estabelecimentos",
        "column_names": [
            "cnpj_basico",
            "cnpj_ordem",
            "cnpj_dv",
            "identificador_matriz_filial",
            "nome_fantasia",
            "situacao_cadastral",
            "data_situacao_cadastral",
            "motivo_situacao_cadastral",
            "nome_cidade_exterior",
            "pais_codigo",
            "data_inicio_atividade",
            "cnae_fiscal_principal_codigo",
            "cnae_fiscal_secundaria",
            "tipo_logradouro",
            "logradouro",
            "numero",
            "complemento",
            "bairro",
            "cep",
            "uf",
            "municipio_codigo",
            "ddd_1",
            "telefone_1",
            "ddd_2",
            "telefone_2",
            "ddd_fax",
            "fax",
            "correio_eletronico",
            "situacao_especial",
            "data_situacao_especial",
        ],
        "dtype_map": {
            "cnpj_basico": str,
            "cnpj_ordem": str,
            "cnpj_dv": str,
            "identificador_matriz_filial": pd.Int64Dtype(),
            "situacao_cadastral": pd.Int64Dtype(),
            "motivo_situacao_cadastral": pd.Int64Dtype(),
            "cnae_fiscal_principal_codigo": pd.Int64Dtype(),
            "cep": str,
            "uf": str,
            "pais_codigo": pd.Int64Dtype(),
            "municipio_codigo": pd.Int64Dtype(),
            "ddd_1": str,
            "telefone_1": str,
            "ddd_2": str,
            "telefone_2": str,
            "ddd_fax": str,
            "fax": str,
            "correio_eletronico": str,
            "situacao_especial": str,
            "data_situacao_cadastral": str,
            "data_inicio_atividade": str,
            "data_situacao_especial": str,
            "nome_cidade_exterior": str,
            "numero": str,
            "complemento": str,
        },
        "custom_clean_func": clean_estabelecimentos_chunk,
    },
    "socios": {
        "table_name": "socios",
        "column_names": [
            "cnpj_basico",
            "identificador_socio",
            "nome_socio_ou_razao_social",
            "cnpj_cpf_socio",
            "qualificacao_socio_codigo",
            "data_entrada_sociedade",
            "pais_codigo",
            "representante_legal_cpf",
            "nome_representante_legal",
            "qualificacao_representante_legal_codigo",
            "faixa_etaria",
        ],
        "dtype_map": {
            "cnpj_basico": str,
            "identificador_socio": pd.Int64Dtype(),
            "qualificacao_socio_codigo": pd.Int64Dtype(),
            "cnpj_cpf_socio": str,
            "representante_legal_cpf": str,
            "qualificacao_representante_legal_codigo": pd.Int64Dtype(),
            "faixa_etaria": pd.Int64Dtype(),
            "pais_codigo": pd.Int64Dtype(),
        },
        "custom_clean_func": clean_socios_chunk,
    },
    "simples": {
        "table_name": "simples",
        "column_names": [
            "cnpj_basico",
            "opcao_pelo_simples",
            "data_opcao_pelo_simples",
            "data_exclusao_do_simples",
            "opcao_pelo_mei",
            "data_opcao_pelo_mei",
            "data_exclusao_do_mei",
        ],
        "dtype_map": {"cnpj_basico": str},
        "custom_clean_func": clean_simples_chunk,
    },
}

# --- Processador ---


def process_and_load_file(conn, config_name):
    try:
        etl_config = ETL_CONFIG[config_name]
    except KeyError:
        logger.error(f"Configuração para '{config_name}' não encontrada.")
        return

    table_name = etl_config["table_name"]
    file_path = settings.extracted_dir / config_name / f"{config_name}.csv"

    if not file_path.exists():
        logger.warning(f"Arquivo '{file_path}' não encontrado. Pulando.")
        return

    logger.info(f"--- Processando tabela '{table_name}' (via '{config_name}') ---")

    reader = pd.read_csv(
        file_path,
        delimiter=";",
        encoding=_detect_encoding(file_path, settings.file_encoding),
        header=None,
        names=etl_config["column_names"],
        dtype=etl_config.get("dtype_map", None),
        chunksize=settings.chunk_size,
    )

    total_rows = 0
    for i, chunk in enumerate(reader):
        if "custom_clean_func" in etl_config:
            chunk = etl_config["custom_clean_func"](chunk)
        chunk = schema_validate(config_name, chunk)
        validate_chunk(config_name, chunk, conn)

        # Passa a conexão direta
        fast_load_chunk(conn, chunk, table_name)

        total_rows += len(chunk)
        logger.info(f"  ... Chunk {i + 1} processado. Total: {total_rows} linhas.")

    logger.info(f"--- Tabela '{table_name}' finalizada! ---")


# --- Executor de SQL ---


def execute_sql_file(conn, filename):
    """
    Lê e executa um arquivo SQL usando cursor do psycopg2.
    """
    base_path = Path(__file__).parent
    file_path = base_path / filename

    if not file_path.exists():
        logger.error(f"Arquivo SQL não encontrado: {file_path}")
        return

    logger.info(f"Executando SQL: {filename}")
    with open(file_path, "r", encoding="utf-8") as f:
        sql_content = f.read()
    if filename == "schema.sql" and not settings.use_unlogged:
        sql_content = sql_content.replace("CREATE UNLOGGED TABLE", "CREATE TABLE")

    try:
        if "CONCURRENTLY" in sql_content:
            old_autocommit = conn.autocommit
            try:
                conn.autocommit = True
                statements = [s.strip() for s in sql_content.split(";") if s.strip()]
                for stmt in statements:
                    with conn.cursor() as cursor:
                        cursor.execute(stmt)
                logger.info(f"Sucesso ao executar {filename}")
            finally:
                conn.autocommit = old_autocommit
        else:
            with conn.cursor() as cursor:
                cursor.execute(sql_content)
            conn.commit()
            logger.info(f"Sucesso ao executar {filename}")
    except Exception as e:
        conn.rollback()
        logger.error(f"ERRO ao executar SQL de {filename}: {e}")
        raise


def execute_sql_path(conn, path: Path):
    if not path.exists():
        logger.error(f"Arquivo SQL não encontrado: {path}")
        return
    logger.info(f"Executando SQL: {path.name}")
    sql_content = path.read_text(encoding="utf-8")
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_content)
        conn.commit()
        logger.info(f"Sucesso ao executar {path.name}")
    except Exception as e:
        conn.rollback()
        logger.error(f"ERRO ao executar SQL de {path.name}: {e}")
        raise


def run_queries_in_dir(dir_path: Path):
    try:
        conn = psycopg2.connect(settings.database_uri)
    except Exception as e:
        logger.error(f"Erro ao conectar no banco: {e}")
        return
    try:
        files = sorted([p for p in dir_path.glob("*.sql")])
        for f in files:
            execute_sql_path(conn, f)
        logger.info("Execução de queries finalizada.")
    except Exception as e:
        logger.error(f"Erro durante execução de queries: {e}")
        conn.rollback()
    finally:
        conn.close()


def create_partitioned_estabelecimentos(conn):
    ddl_parent = f"""
    DROP TABLE IF EXISTS estabelecimentos CASCADE;
    CREATE {'' if not settings.use_unlogged else 'UNLOGGED '}TABLE estabelecimentos (
        cnpj_basico CHAR(8),
        cnpj_ordem CHAR(4),
        cnpj_dv CHAR(2),
        identificador_matriz_filial SMALLINT,
        nome_fantasia VARCHAR(255),
        situacao_cadastral SMALLINT,
        data_situacao_cadastral DATE,
        motivo_situacao_cadastral SMALLINT,
        nome_cidade_exterior VARCHAR(100),
        pais_codigo SMALLINT,
        data_inicio_atividade DATE,
        cnae_fiscal_principal_codigo INTEGER,
        cnae_fiscal_secundaria TEXT[],
        tipo_logradouro VARCHAR(50),
        logradouro VARCHAR(255),
        numero VARCHAR(20),
        complemento VARCHAR(255),
        bairro VARCHAR(100),
        cep CHAR(8),
        uf CHAR(2),
        municipio_codigo SMALLINT,
        ddd_1 VARCHAR(4),
        telefone_1 VARCHAR(9),
        ddd_2 VARCHAR(4),
        telefone_2 VARCHAR(9),
        ddd_fax VARCHAR(4),
        fax VARCHAR(9),
        correio_eletronico VARCHAR(255),
        situacao_especial VARCHAR(100),
        data_situacao_especial DATE
    ) PARTITION BY LIST (uf);
    """
    ufs = [
        'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG',
        'PA','PB','PR','PE','PI','RJ','RN','RO','RS','RR','SC','SE','SP','TO'
    ]
    parts = []
    for uf in ufs:
        stmt = sql.SQL(
            "CREATE TABLE {part} PARTITION OF {parent} FOR VALUES IN ({uf})"
        ).format(
            part=sql.Identifier(f"estabelecimentos_{uf}"),
            parent=sql.Identifier("estabelecimentos"),
            uf=sql.Literal(uf),
        )
        parts.append(stmt.as_string(conn))
    parts.append(
        sql.SQL(
            "CREATE TABLE {part} PARTITION OF {parent} DEFAULT"
        ).format(
            part=sql.Identifier("estabelecimentos_default"),
            parent=sql.Identifier("estabelecimentos"),
        ).as_string(conn)
    )
    full_sql = ddl_parent + "\n".join(parts)
    with conn.cursor() as cursor:
        cursor.execute(full_sql)
    conn.commit()


# --- Orquestrador Principal ---


def run_loader(only=None, exclude=None):
    logger.info("Iniciando carga para PostgreSQL (Driver Nativo)...")

    # Conexão direta via psycopg2
    try:
        conn = psycopg2.connect(settings.database_uri)
    except Exception as e:
        logger.error(f"Erro ao conectar no banco: {e}")
        return

    try:
        clear_domain_cache()
        # Cria as tabelas
        execute_sql_file(conn, "schema.sql")
        if settings.partition_estabelecimentos_by == "uf":
            create_partitioned_estabelecimentos(conn)

        processing_order = [
            "paises",
            "municipios",
            "qualificacoes",
            "naturezas",
            "cnaes",
            "empresas",
            "estabelecimentos",
            "simples",
            "socios",
        ]

        if only:
            processing_order = [x for x in processing_order if x in set(only)]
        if exclude:
            processing_order = [x for x in processing_order if x not in set(exclude)]

        # Fluxo principal de processamento dos arquivos CSV para SQL
        for config_name in processing_order:
            process_and_load_file(conn, config_name)

        if settings.set_logged_after_copy:
            logger.info("Tornando tabelas persistentes (LOGGED) após carga...")

            tables = [
                "empresas",
                "estabelecimentos",
                "socios",
                "simples",
                "paises",
                "municipios",
                "qualificacoes_socios",
                "naturezas_juridicas",
                "cnaes",
            ]

            with conn.cursor() as cursor:
                for tbl in tables:
                    cursor.execute(f"ALTER TABLE {tbl} SET LOGGED;")
                if settings.partition_estabelecimentos_by == "uf":
                    cursor.execute("SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent = 'estabelecimentos'::regclass;")
                    parts = [r[0] for r in cursor.fetchall()]
                    for p in parts:
                        cursor.execute(f"ALTER TABLE {p} SET LOGGED;")
            conn.commit()

        logger.info("Aplicando Constraints e Índices...")
        execute_sql_file(conn, "constraints.sql")
        with conn.cursor() as cursor:
            for t in [
                "empresas",
                "estabelecimentos",
                "socios",
                "simples",
                "paises",
                "municipios",
                "qualificacoes_socios",
                "naturezas_juridicas",
                "cnaes",
            ]:
                cursor.execute(f"ANALYZE {t};")
        conn.commit()

        if settings.cluster_after_copy and settings.partition_estabelecimentos_by == "uf":
            with conn.cursor() as cursor:
                cursor.execute("CLUSTER estabelecimentos USING idx_estabelecimentos_uf;")
            conn.commit()
        logger.info("Carga finalizada com sucesso.")

    except Exception as e:
        logger.error(f"Erro crítico durante o processo: {e}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    from .settings import setup_logging

    setup_logging()
    run_loader()
def _detect_encoding(file_path: Path, default: str) -> str:
    try:
        from charset_normalizer import from_path
        res = from_path(str(file_path)).best()
        if res and res.encoding:
            return res.encoding
    except Exception:
        return default
    return default
def clear_domain_cache():
    DOMAIN_CACHE.clear()
