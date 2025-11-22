import pandas as pd
import logging
import io
from sqlalchemy import create_engine
from models import Base
from settings import settings

logger = logging.getLogger(__name__)


def fast_load_chunk(engine, df, table_name):
    """
    Função de Carga Ultra-Rápida (PostgreSQL COPY)
    Carrega um DataFrame para o PostgreSQL usando o comando COPY.
    É de 10x a 50x mais rápido que o to_sql padrão.
    """

    # Converte o DataFrame para CSV em memória (buffer)
    output = io.StringIO()

    # Prepara o CSV: sem index, sem header, separador ';', e NULL representado por string vazia
    df.to_csv(
        output,
        sep=";",
        header=False,
        index=False,
        na_rep="",  # PostgreSQL entende string vazia como NULL se configurado abaixo
        quotechar='"',
        doublequote=True,
    )

    # Volta o ponteiro para o início do arquivo em memória
    output.seek(0)

    # Obtém uma conexão crua (raw) do driver psycopg2
    connection = engine.raw_connection()
    cursor = connection.cursor()

    try:
        # Executa o comando COPY EXPERT
        # COPY table FROM STDIN WITH
        # (FORMAT CSV, DELIMITER ';', NULL '', QUOTE '"')
        sql = f"""
            COPY {table_name} 
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
    Prepara o chunk de estabelecimentos para o PostgreSQL.
    Transforma a lista de CNAEs secundários de '1,2,3' para '{1,2,3}' (formato de array do Postgres).
    """
    col_name = "cnae_fiscal_secundaria"

    if col_name in chunk_df.columns:
        # Preenche nulos com string vazia temporariamente para não quebrar a concatenação
        chunk_df[col_name] = chunk_df[col_name].fillna("")

        # Aplica a formatação de array do Postgres: "{valor,valor}"
        # Apenas onde tem valor (não é vazio)
        mask = chunk_df[col_name] != ""

        # Adiciona as chaves {} ao redor do texto existente
        chunk_df.loc[mask, col_name] = "{" + chunk_df.loc[mask, col_name] + "}"

        # Onde era vazio, voltamos para None (NULL no banco) para não ficar "{}"
        chunk_df.loc[~mask, col_name] = None

    return chunk_df


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
            "municipio_codigo": pd.Int64Dtype(),
        },
        "date_columns": [
            "data_situacao_cadastral",
            "data_inicio_atividade",
            "data_situacao_especial",
        ],
        # Adicionado o hook de limpeza para o array de CNAEs
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
        },
        "date_columns": ["data_entrada_sociedade"],
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
        "date_columns": [
            "data_opcao_pelo_simples",
            "data_exclusao_do_simples",
            "data_opcao_pelo_mei",
            "data_exclusao_do_mei",
        ],
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
        parse_dates=etl_config.get("date_columns", None),
        infer_datetime_format=True,
        chunksize=settings.chunck_size,
    )

    total_rows = 0
    for i, chunk in enumerate(reader):
        if "custom_clean_func" in etl_config:
            chunk = etl_config["custom_clean_func"](chunk)

        # AQUI ESTÁ A MÁGICA: Substituímos to_sql por fast_load_chunk
        fast_load_chunk(engine, chunk, table_name)

        total_rows += len(chunk)
        logger.info(f"  ... Chunk {i + 1} processado. Total: {total_rows} linhas.")

    logger.info(f"--- Tabela '{table_name}' finalizada! ---")


# --- Orquestrador ---


def run_loader():
    logger.info("Iniciando carga para PostgreSQL...")

    engine = create_engine(settings.database_uri)

    # A parte de dropar/criar tabelas é segura com SQLAlchemy e Postgres
    # OBS: Dependendo do tamanho do banco, usar drop_all pode ser perigoso em produção.
    # Para carga inicial, está ótimo.
    logger.info("Recriando schema...")
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    # Ordem de processamento
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


if __name__ == "__main__":
    from settings import setup_logging

    setup_logging()
    run_loader()
