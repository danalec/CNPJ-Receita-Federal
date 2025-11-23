from sqlalchemy import (
    Column,
    Integer,
    String,
    Date,
    Numeric,
    Text,
    ForeignKey,
)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.dialects.postgresql import ARRAY

Base = declarative_base()

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
    porte_empresa = Column(Integer)
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

    identificador_matriz_filial = Column(Integer)
    nome_fantasia = Column(String(255))
    situacao_cadastral = Column(Integer)
    data_situacao_cadastral = Column(Date)
    motivo_situacao_cadastral = Column(Integer)
    nome_cidade_exterior = Column(String(100))
    
    """
    A razão do qual dropei as chaves estrangeiras, foi devido a inconsistencias
    encontradas, códigos de países que países ainda não adicionados
    
    Antes: 
        pais_codigo = Column(Integer, ForeignKey("paises.codigo"))
        municipio_codigo = Column(Integer, ForeignKey("municipios.codigo"))
    """
    
    pais_codigo = Column(Integer)
    municipio_codigo = Column(Integer)
    
    data_inicio_atividade = Column(Date)
    cnae_fiscal_principal_codigo = Column(Integer, ForeignKey("cnaes.codigo"))

    # Array de strings para CNAEs secundários
    cnae_fiscal_secundaria = Column(ARRAY(String))

    tipo_logradouro = Column(String(50))
    logradouro = Column(String(255))
    numero = Column(String(20))
    complemento = Column(String(255))
    bairro = Column(String(100))
    cep = Column(String(8))
    uf = Column(String(2))
    
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

    opcao_pelo_simples = Column(String(1))
    data_opcao_pelo_simples = Column(Date)
    data_exclusao_do_simples = Column(Date)
    opcao_pelo_mei = Column(String(1))
    data_opcao_pelo_mei = Column(Date)
    data_exclusao_do_mei = Column(Date)


class Socio(Base):
    __tablename__ = "socios"

    # Chave primária composta para garantir unicidade
    cnpj_basico = Column(
        String(8), ForeignKey("empresas.cnpj_basico"), primary_key=True
    )

    identificador_socio = Column(Integer, primary_key=True)
    nome_socio_ou_razao_social = Column(String(255), primary_key=True)

    cnpj_cpf_socio = Column(String(14))

    qualificacao_socio_codigo = Column(
        Integer, ForeignKey("qualificacoes_socios.codigo")
    )
    data_entrada_sociedade = Column(Date)
    
    # Problema de incosistencias na base de dados
    # explicado na tabela de estabelecimentos
    # pais_codigo = Column(Integer, ForeignKey("paises.codigo"))
    pais_codigo = Column(Integer)

    representante_legal_cpf = Column(String(11))

    nome_representante_legal = Column(String(255))
    qualificacao_representante_legal_codigo = Column(
        Integer, ForeignKey("qualificacoes_socios.codigo")
    )
    # Alterado de Enum para Integer
    faixa_etaria = Column(Integer)

    # Relacionamentos
    empresa = relationship("Empresa", back_populates="socios")
    pais = relationship("Pais")
    qualificacao_socio = relationship(
        "QualificacaoSocio", foreign_keys=[qualificacao_socio_codigo]
    )
    qualificacao_representante_legal = relationship(
        "QualificacaoSocio", foreign_keys=[qualificacao_representante_legal_codigo]
    )
