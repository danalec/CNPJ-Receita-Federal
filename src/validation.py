import pandas as pd
from typing import Any, TYPE_CHECKING

pa: Any
Series: Any
if TYPE_CHECKING:
    import pandera as pa
    class BaseSchema(pa.SchemaModel):
        pass
else:
    class BaseSchema:
        @classmethod
        def validate(cls, df, lazy=True):
            return df
try:
    import pandera as pa
    from pandera.typing import Series
    HAVE_SCHEMA_MODEL = hasattr(pa, "SchemaModel")
except Exception:
    HAVE_SCHEMA_MODEL = False
    Series = Any


 

if HAVE_SCHEMA_MODEL:
    class EmpresasSchema(BaseSchema):
        cnpj_basico: Series[str]
        razao_social: Series[str]
        natureza_juridica_codigo: Series[pd.Int64Dtype]
        qualificacao_responsavel: Series[pd.Int64Dtype]
        capital_social: Series[float]
        porte_empresa: Series[pd.Int64Dtype]
        ente_federativo_responsavel: Series[str]


if HAVE_SCHEMA_MODEL:
    class EstabelecimentosSchema(BaseSchema):
        cnpj_basico: Series[str]
        cnpj_ordem: Series[str]
        cnpj_dv: Series[str]
        identificador_matriz_filial: Series[pd.Int64Dtype]
        nome_fantasia: Series[str]
        situacao_cadastral: Series[pd.Int64Dtype]
        data_situacao_cadastral: Series[object]
        motivo_situacao_cadastral: Series[pd.Int64Dtype]
        nome_cidade_exterior: Series[str]
        pais_codigo: Series[pd.Int64Dtype]
        data_inicio_atividade: Series[object]
        cnae_fiscal_principal_codigo: Series[pd.Int64Dtype]
        cnae_fiscal_secundaria: Series[str]
        tipo_logradouro: Series[str]
        logradouro: Series[str]
        numero: Series[str]
        complemento: Series[str]
        bairro: Series[str]
        cep: Series[str]
        uf: Series[str]
        municipio_codigo: Series[pd.Int64Dtype]
        ddd_1: Series[str]
        telefone_1: Series[str]
        ddd_2: Series[str]
        telefone_2: Series[str]
        ddd_fax: Series[str]
        fax: Series[str]
        correio_eletronico: Series[str]
        situacao_especial: Series[str]
        data_situacao_especial: Series[object]


if HAVE_SCHEMA_MODEL:
    class SociosSchema(BaseSchema):
        cnpj_basico: Series[str]
        identificador_socio: Series[pd.Int64Dtype]
        nome_socio_ou_razao_social: Series[str]
        cnpj_cpf_socio: Series[str]
        qualificacao_socio_codigo: Series[pd.Int64Dtype]
        data_entrada_sociedade: Series[object]
        pais_codigo: Series[pd.Int64Dtype]
        representante_legal_cpf: Series[str]
        nome_representante_legal: Series[str]
        qualificacao_representante_legal_codigo: Series[pd.Int64Dtype]
        faixa_etaria: Series[pd.Int64Dtype]


if HAVE_SCHEMA_MODEL:
    class SimplesSchema(BaseSchema):
        cnpj_basico: Series[str]
        opcao_pelo_simples: Series[str]
        data_opcao_pelo_simples: Series[object]
        data_exclusao_do_simples: Series[object]
        opcao_pelo_mei: Series[str]
        data_opcao_pelo_mei: Series[object]
        data_exclusao_do_mei: Series[object]


def validate(config_name: str, df: pd.DataFrame) -> pd.DataFrame:
    if not HAVE_SCHEMA_MODEL:
        return df
    if config_name == "empresas":
        return EmpresasSchema.validate(df, lazy=True)
    if config_name == "estabelecimentos":
        return EstabelecimentosSchema.validate(df, lazy=True)
    if config_name == "socios":
        return SociosSchema.validate(df, lazy=True)
    if config_name == "simples":
        return SimplesSchema.validate(df, lazy=True)
    return df
