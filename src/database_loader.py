import pandas as pd
import logging
import io
from sqlalchemy import create_engine, text
from .models import Base
from .settings import settings

logger = logging.getLogger(__name__)


def fast_load_chunk(engine, df, table_name):
    """
    Função de Carga Ultra-Rápida (PostgreSQL COPY) com Mapeamento Explícito.
    """

    # 1. Converte o DataFrame para CSV em memória
    output = io.StringIO()

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

    # 2. Pega os nomes das colunas do DataFrame para garantir o mapeamento correto
    # Isso resolve o problema de colunas fora de ordem entre Model e CSV
    columns = df.columns.tolist()
    columns_str = ",".join([f'"{col}"' for col in columns])

    connection = engine.raw_connection()
    cursor = connection.cursor()

    try:
        # 3. Comando COPY com colunas EXPLICITAS
        # COPY tabela (col1, col2, col3...) FROM STDIN ...
        sql = f"""
            COPY {table_name} ({columns_str})
            FROM STDIN 
            WITH (
                FORMAT CSV, 
                DELIMITER ';', 
                NULL '', 
                QUOTE '"', 
                HEADER FALSE
            )
        """
        cursor.copy_expert(sql, output)
        connection.commit()
    except Exception as e:
        connection.rollback()
        logger.error(f"Erro no COPY para tabela {table_name}: {e}")
        raise
    finally:
        cursor.close()
        connection.close()


# --- Hooks de Limpeza ---


def sanitize_dates(df, date_columns):
    """Converte colunas de data, transformando erros (ex: '0') em NaT/NULL."""
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format="%Y%m%d", errors="coerce")
    return df


def clean_empresas_chunk(chunk_df):
    """Aplica limpezas no chunk de empresas."""
    if "capital_social" in chunk_df.columns:
        capital_social_str = (
            chunk_df["capital_social"].astype(str).str.replace(",", ".", regex=False)
        )
        chunk_df["capital_social"] = pd.to_numeric(capital_social_str, errors="coerce")
    return chunk_df


def clean_estabelecimentos_chunk(chunk_df):
    """
    Prepara o chunk de estabelecimentos:
    1. Limpa datas (converte '0' para NULL)
    2. Formata array de CNAEs
    """
    # 1. Limpeza de Datas
    date_cols = [
        "data_situacao_cadastral",
        "data_inicio_atividade",
        "data_situacao_especial",
    ]
    chunk_df = sanitize_dates(chunk_df, date_cols)

    # 2. Limpeza de Array CNAE
    col_name = "cnae_fiscal_secundaria"
    if col_name in chunk_df.columns:
        chunk_df[col_name] = chunk_df[col_name].fillna("")
        mask = chunk_df[col_name] != ""
        # Adiciona chaves {} para formato array do Postgres
        chunk_df.loc[mask, col_name] = "{" + chunk_df.loc[mask, col_name] + "}"
        chunk_df.loc[~mask, col_name] = None

    return chunk_df


def clean_socios_chunk(chunk_df):
    date_cols = ["data_entrada_sociedade"]
    return sanitize_dates(chunk_df, date_cols)


def clean_simples_chunk(chunk_df):
    date_cols = [
        "data_opcao_pelo_simples",
        "data_exclusao_do_simples",
        "data_opcao_pelo_mei",
        "data_exclusao_do_mei",
    ]
    return sanitize_dates(chunk_df, date_cols)


# Estruturação dos dados que serão migrados

ETL_CONFIG = {
    "paises": {
        "table_name": "paises",
        "column_names": ["codigo", "nome"],
    },
    "municipios": {
        "table_name": "municipios",
        "column_names": ["codigo", "nome"],
    },
    "qualificacoes": {
        "table_name": "qualificacoes_socios",
        "column_names": ["codigo", "nome"],
    },
    "naturezas": {
        "table_name": "naturezas_juridicas",
        "column_names": ["codigo", "nome"],
    },
    "cnaes": {
        "table_name": "cnaes",
        "column_names": ["codigo", "nome"],
    },
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


def process_and_load_file(engine, config_name):
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

    logger.info(f"--- Processando tabela '{table_name}' (Modo: PostgreSQL COPY) ---")

    reader = pd.read_csv(
        file_path,
        delimiter=";",
        encoding=settings.file_encoding,
        header=None,
        names=etl_config["column_names"],
        dtype=etl_config.get("dtype_map", None),
        chunksize=settings.chunck_size,
    )

    total_rows = 0
    for i, chunk in enumerate(reader):
        if "custom_clean_func" in etl_config:
            chunk = etl_config["custom_clean_func"](chunk)

        fast_load_chunk(engine, chunk, table_name)

        total_rows += len(chunk)
        logger.info(f"  ... Chunk {i + 1} processado. Total: {total_rows} linhas.")

    logger.info(f"--- Tabela '{table_name}' finalizada! ---")


# --- Orquestrador ---


def run_loader():
    logger.info("Iniciando carga para PostgreSQL...")

    engine = create_engine(settings.database_uri)

    logger.info("Recriando schema...")
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    if settings.set_unlogged_before_copy:
        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE empresas SET UNLOGGED;"))
            conn.execute(text("ALTER TABLE estabelecimentos SET UNLOGGED;"))
            conn.execute(text("ALTER TABLE socios SET UNLOGGED;"))
            conn.execute(text("ALTER TABLE simples SET UNLOGGED;"))
            conn.commit()

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

    for table_config_name in processing_order:
        process_and_load_file(engine, table_config_name)

    logger.info("Carga finalizada.")

    if settings.set_unlogged_before_copy:
        with engine.connect() as conn:
            logger.info("Tornando tabelas persistentes (LOGGED) novamente...")
            conn.execute(text("ALTER TABLE empresas SET LOGGED;"))
            conn.execute(text("ALTER TABLE estabelecimentos SET LOGGED;"))
            conn.execute(text("ALTER TABLE socios SET LOGGED;"))
            conn.execute(text("ALTER TABLE simples SET LOGGED;"))
            conn.commit()


if __name__ == "__main__":
    from .config import setup_logging

    setup_logging()
    run_loader()
