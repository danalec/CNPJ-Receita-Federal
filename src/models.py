import enum
from sqlalchemy import (
    Column,
    Integer,
    String,
    Date,
    Numeric,
    Enum,
    Text,
    ForeignKey,
    ARRAY,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

# --- Definição dos Enums ---
# Usar Enums torna o código mais legível e seguro, evitando valores inválidos.


class PorteEmpresaEnum(enum.Enum):
    NAO_INFORMADO = 1
    MICRO_EMPRESA = 2j
    EMPRESA_DE_PEQUENO_PORTE = 3
    DEMAIS = 5


class IdentificadorMatrizFilialEnum(enum.Enum):
    MATRIZ = 1
    FILIAL = 2


class SituacaoCadastralEnum(enum.Enum):
    NULA = 1
    ATIVA = 2
    SUSPENSA = 3
    INAPTA = 4
    BAIXADA = 8


class OpcaoSimplesMeiEnum(enum.Enum):
    SIM = "S"
    NAO = "N"
    OUTROS = ""  # Representando o "EM BRANCO"


class IdentificadorSocioEnum(enum.Enum):
    PESSOA_JURIDICA = 1
    PESSOA_FISICA = 2
    ESTRANGEIRO = 3


class FaixaEtariaSocioEnum(enum.Enum):
    NAO_SE_APLICA = 0
    ENTRE_0_A_12_ANOS = 1
    ENTRE_13_A_20_ANOS = 2
    ENTRE_21_A_30_ANOS = 3
    ENTRE_31_A_40_ANOS = 4
    ENTRE_41_A_50_ANOS = 5
    ENTRE_51_A_60_ANOS = 6
    ENTRE_61_A_70_ANOS = 7
    ENTRE_71_A_80_ANOS = 8
    MAIORES_DE_80_ANOS = 9


# --- Modelos das Tabelas de Domínio ---


class Pais(Base):
    __tablename__ = "paises"
    codigo = Column(Integer, primary_key=True)
    nome = Column(String(100))


class Municipio(Base):
    __tablename__ = "municipios"
    codigo = Column(Integer, primary_key=True)
    nome = Column(String(150))


class QualificacaoSocio(Base):
    __tablename__ = "qualificacoes_socios"
    codigo = Column(Integer, primary_key=True)
    nome = Column(String(100))


class NaturezaJuridica(Base):
    __tablename__ = "naturezas_juridicas"
    codigo = Column(Integer, primary_key=True)
    nome = Column(String(150))


class Cnae(Base):
    __tablename__ = "cnaes"
    codigo = Column(Integer, primary_key=True)
    nome = Column(Text)


# --- Modelos das Tabelas Principais ---


class Empresa(Base):
    __tablename__ = "empresas"

    cnpj_basico = Column(String(8), primary_key=True)
    razao_social = Column(String(255))
    natureza_juridica_codigo = Column(Integer, ForeignKey("naturezas_juridicas.codigo"))
    qualificacao_responsavel = Column(Integer)
    capital_social = Column(Numeric(18, 2))
    porte_empresa = Column(Enum(PorteEmpresaEnum))
    ente_federativo_responsavel = Column(String(100))

    # Relacionamentos
    natureza_juridica = relationship("NaturezaJuridica")
    estabelecimentos = relationship("Estabelecimento", back_populates="empresa")
    socios = relationship("Socio", back_populates="empresa")


class Estabelecimento(Base):
    __tablename__ = "estabelecimentos"

    cnpj_basico = Column(
        String(8), ForeignKey("empresas.cnpj_basico"), primary_key=True
    )
    cnpj_ordem = Column(String(4), primary_key=True)
    cnpj_dv = Column(String(2), primary_key=True)

    identificador_matriz_filial = Column(Enum(IdentificadorMatrizFilialEnum))
    nome_fantasia = Column(String(255))
    situacao_cadastral = Column(Enum(SituacaoCadastralEnum))
    data_situacao_cadastral = Column(Date)
    motivo_situacao_cadastral = Column(Integer)
    nome_cidade_exterior = Column(String(100))
    pais_codigo = Column(Integer, ForeignKey("paises.codigo"))
    data_inicio_atividade = Column(Date)
    cnae_fiscal_principal_codigo = Column(Integer, ForeignKey("cnaes.codigo"))
    cnae_fiscal_secundaria = Column(ARRAY(String))
    tipo_logradouro = Column(String(50))
    logradouro = Column(String(255))
    numero = Column(String(20))
    complemento = Column(String(255))
    bairro = Column(String(100))
    cep = Column(String(8))
    uf = Column(String(2))
    municipio_codigo = Column(Integer, ForeignKey("municipios.codigo"))
    ddd_1 = Column(String(4))
    telefone_1 = Column(String(9))
    ddd_2 = Column(String(4))
    telefone_2 = Column(String(9))
    ddd_fax = Column(String(4))
    fax = Column(String(9))
    correio_eletronico = Column(String(255))
    situacao_especial = Column(String(100))
    data_situacao_especial = Column(Date)

    # Relacionamentos
    empresa = relationship("Empresa", back_populates="estabelecimentos")
    pais = relationship("Pais")
    municipio = relationship("Municipio")
    cnae_fiscal_principal = relationship("Cnae")


class Simples(Base):
    __tablename__ = "simples"

    cnpj_basico = Column(
        String(8), ForeignKey("empresas.cnpj_basico"), primary_key=True
    )
    opcao_pelo_simples = Column(Enum(OpcaoSimplesMeiEnum))
    data_opcao_pelo_simples = Column(Date)
    data_exclusao_do_simples = Column(Date)
    opcao_pelo_mei = Column(Enum(OpcaoSimplesMeiEnum))
    data_opcao_pelo_mei = Column(Date)
    data_exclusao_do_mei = Column(Date)


class Socio(Base):
    __tablename__ = "socios"

    # Chave primária composta para garantir unicidade
    cnpj_basico = Column(
        String(8), ForeignKey("empresas.cnpj_basico"), primary_key=True
    )
    identificador_socio = Column(Enum(IdentificadorSocioEnum), primary_key=True)
    nome_socio_ou_razao_social = Column(String(255), primary_key=True)

    # IMPORTANTE: Conforme o layout, este campo deve ser descaracterizado na carga.
    # A ocultação dos 3 primeiros e 2 últimos dígitos é uma regra de negócio
    # a ser aplicada ANTES da inserção no banco de dados.
    cnpj_cpf_socio = Column(String(14))

    qualificacao_socio_codigo = Column(
        Integer, ForeignKey("qualificacoes_socios.codigo")
    )
    data_entrada_sociedade = Column(Date)
    pais_codigo = Column(Integer, ForeignKey("paises.codigo"))

    # IMPORTANTE: Mesmo caso do cnpj_cpf_socio.
    representante_legal_cpf = Column(String(11))

    nome_representante_legal = Column(String(255))
    qualificacao_representante_legal_codigo = Column(
        Integer, ForeignKey("qualificacoes_socios.codigo")
    )
    faixa_etaria = Column(Enum(FaixaEtariaSocioEnum))

    # Relacionamentos
    empresa = relationship("Empresa", back_populates="socios")
    pais = relationship("Pais")
    qualificacao_socio = relationship(
        "QualificacaoSocio", foreign_keys=[qualificacao_socio_codigo]
    )
    qualificacao_representante_legal = relationship(
        "QualificacaoSocio", foreign_keys=[qualificacao_representante_legal_codigo]
    )
