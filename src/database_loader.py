import pandas as pd
import logging
from sqlalchemy import create_engine

# Importa os modelos e o arquivo de configuração GERAL
from . import models
from . import config as base_config

logger = logging.getLogger(__name__)


def clean_empresas_chunk(chunk_df):
    """Aplica limpezas no chunk de empresas USANDO OS NOMES FINAIS das colunas."""
    capital_social_str = chunk_df["capital_social"].str.replace(",", ".", regex=False)
    chunk_df["capital_social"] = pd.to_numeric(capital_social_str, errors="coerce")
    return chunk_df


ETL_CONFIG = {
    # Tabelas de Domínio
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
    # --- Tabelas de Dados Principais ---
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


# --- Processador Genérico de Arquivos ---


def process_and_load_file(engine, config_name):
    """
    Função genérica que lê, transforma e carrega um arquivo CSV sem cabeçalho,
    atribuindo os nomes das colunas diretamente.
    """
    try:
        etl_config = ETL_CONFIG[config_name]
    except KeyError:
        logger.error(f"Configuração para '{config_name}' não encontrada no ETL_CONFIG.")
        return

    table_name = etl_config["table_name"]
    file_path = base_config.EXTRACTED_DIR / config_name / f"{config_name}.csv"

    if not file_path.exists():
        logger.warning(f"Arquivo '{file_path}' não encontrado. Pulando.")
        return

    logger.info(f"--- Iniciando processamento para a tabela '{table_name}' ---")

    reader = pd.read_csv(
        file_path,
        delimiter=";",
        encoding=base_config.FILE_ENCODING,
        header=None,
        names=etl_config["column_names"],
        dtype=etl_config.get("dtype_map", None),
        parse_dates=etl_config.get("date_columns", None),
        infer_datetime_format=True,
        chunksize=base_config.CHUNKSIZE,
    )

    total_rows = 0
    for i, chunk in enumerate(reader):
        if "custom_clean_func" in etl_config:
            chunk = etl_config["custom_clean_func"](chunk)

        chunk.to_sql(table_name, engine, if_exists="append", index=False)

        total_rows += len(chunk)
        logger.info(
            f"  ... Chunk {i + 1} para '{table_name}' processado. "
            f"Total de {total_rows} linhas carregadas."
        )

    logger.info(f"--- Carga para a tabela '{table_name}' finalizada com sucesso! ---")


def run_loader():
    """
    Orquestra todo o processo de carga no banco de dados.
    """
    logger.info("Iniciando a carga de dados para o banco...")

    engine = create_engine(base_config.DATABASE_URI)

    logger.info("Recriando todas as tabelas do banco de dados...")
    models.Base.metadata.drop_all(engine)
    models.Base.metadata.create_all(engine)
    logger.info("Tabelas recriadas com sucesso.")

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

    logger.info("Carga de dados para o banco finalizada.")
