import pandas as pd
from typing import TypeVar, Generic

T = TypeVar("T")
class PanderaSeries(Generic[T]):
    pass

class SchemaModel:
    @classmethod
    def validate(cls, df, lazy=True):
        return df


class EmpresasSchema(SchemaModel):
        cnpj_basico: PanderaSeries[str]
        razao_social: PanderaSeries[str]
        natureza_juridica_codigo: PanderaSeries[pd.Int64Dtype]
        qualificacao_responsavel: PanderaSeries[pd.Int64Dtype]
        capital_social: PanderaSeries[float]
        porte_empresa: PanderaSeries[pd.Int64Dtype]
        ente_federativo_responsavel: PanderaSeries[str]


class EstabelecimentosSchema(SchemaModel):
        cnpj_basico: PanderaSeries[str]
        cnpj_ordem: PanderaSeries[str]
        cnpj_dv: PanderaSeries[str]
        identificador_matriz_filial: PanderaSeries[pd.Int64Dtype]
        nome_fantasia: PanderaSeries[str]
        situacao_cadastral: PanderaSeries[pd.Int64Dtype]
        data_situacao_cadastral: PanderaSeries[object]
        motivo_situacao_cadastral: PanderaSeries[pd.Int64Dtype]
        nome_cidade_exterior: PanderaSeries[str]
        pais_codigo: PanderaSeries[pd.Int64Dtype]
        data_inicio_atividade: PanderaSeries[object]
        cnae_fiscal_principal_codigo: PanderaSeries[pd.Int64Dtype]
        cnae_fiscal_secundaria: PanderaSeries[str]
        tipo_logradouro: PanderaSeries[str]
        logradouro: PanderaSeries[str]
        numero: PanderaSeries[str]
        complemento: PanderaSeries[str]
        bairro: PanderaSeries[str]
        cep: PanderaSeries[str]
        uf: PanderaSeries[str]
        municipio_codigo: PanderaSeries[pd.Int64Dtype]
        ddd_1: PanderaSeries[str]
        telefone_1: PanderaSeries[str]
        ddd_2: PanderaSeries[str]
        telefone_2: PanderaSeries[str]
        ddd_fax: PanderaSeries[str]
        fax: PanderaSeries[str]
        correio_eletronico: PanderaSeries[str]
        situacao_especial: PanderaSeries[str]
        data_situacao_especial: PanderaSeries[object]


class SociosSchema(SchemaModel):
        cnpj_basico: PanderaSeries[str]
        identificador_socio: PanderaSeries[pd.Int64Dtype]
        nome_socio_ou_razao_social: PanderaSeries[str]
        cnpj_cpf_socio: PanderaSeries[str]
        qualificacao_socio_codigo: PanderaSeries[pd.Int64Dtype]
        data_entrada_sociedade: PanderaSeries[object]
        pais_codigo: PanderaSeries[pd.Int64Dtype]
        representante_legal_cpf: PanderaSeries[str]
        nome_representante_legal: PanderaSeries[str]
        qualificacao_representante_legal_codigo: PanderaSeries[pd.Int64Dtype]
        faixa_etaria: PanderaSeries[pd.Int64Dtype]


class SimplesSchema(SchemaModel):
        cnpj_basico: PanderaSeries[str]
        opcao_pelo_simples: PanderaSeries[str]
        data_opcao_pelo_simples: PanderaSeries[object]
        data_exclusao_do_simples: PanderaSeries[object]
        opcao_pelo_mei: PanderaSeries[str]
        data_opcao_pelo_mei: PanderaSeries[object]
        data_exclusao_do_mei: PanderaSeries[object]


def validate(config_name: str, df: pd.DataFrame) -> pd.DataFrame:
    return df
