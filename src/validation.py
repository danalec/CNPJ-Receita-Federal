import pandas as pd
import re
import logging
from typing import TypeVar, Generic
from .settings import settings

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


def validate(config_name: str, df: pd.DataFrame):
    logger = logging.getLogger(__name__)
    level = getattr(settings, "auto_repair_level", "basic")
    if level == "none":
        return df, {"level": level, "after_nulls": {}, "null_deltas": {}, "columns": list(df.columns)}
    before_nulls = {c: int(df[c].isna().sum()) for c in df.columns}
    cols_for_diff = []
    if level == "aggressive":
        if config_name == "estabelecimentos":
            cols_for_diff = [
                "cnpj_basico","cnpj_ordem","cnpj_dv","uf","cep",
                "cnae_fiscal_principal_codigo","cnae_fiscal_secundaria","correio_eletronico"
            ]
        elif config_name == "socios":
            cols_for_diff = ["cnpj_basico","cnpj_cpf_socio","representante_legal_cpf"]
        elif config_name == "empresas":
            cols_for_diff = ["cnpj_basico","razao_social"]
        elif config_name == "simples":
            cols_for_diff = ["cnpj_basico"]
    snapshot = {c: df[c].astype(str).copy() for c in cols_for_diff if c in df.columns}

    def _cpf_valid(s: str) -> bool:
        if not s or len(s) != 11 or s == s[0] * 11:
            return False
        nums = [int(x) for x in s]
        sm1 = sum(nums[i] * (10 - i) for i in range(9))
        d1 = 0 if sm1 % 11 < 2 else 11 - (sm1 % 11)
        if nums[9] != d1:
            return False
        sm2 = sum(nums[i] * (11 - i) for i in range(10))
        d2 = 0 if sm2 % 11 < 2 else 11 - (sm2 % 11)
        return nums[10] == d2

    def _cnpj_valid(s: str) -> bool:
        if not s or len(s) != 14 or s == s[0] * 14:
            return False
        nums = [int(x) for x in s]
        w1 = [5,4,3,2,9,8,7,6,5,4,3,2]
        sm1 = sum(nums[i] * w1[i] for i in range(12))
        d1 = 0 if sm1 % 11 < 2 else 11 - (sm1 % 11)
        if nums[12] != d1:
            return False
        w2 = [6,5,4,3,2,9,8,7,6,5,4,3,2]
        sm2 = sum(nums[i] * w2[i] for i in range(13))
        d2 = 0 if sm2 % 11 < 2 else 11 - (sm2 % 11)
        return nums[13] == d2

    def _digits_only(s: pd.Series) -> pd.Series:
        return s.astype(str).str.replace(r"\D+", "", regex=True)

    def _strip_empty_to_na(s: pd.Series) -> pd.Series:
        t = s.astype(str).str.strip()
        return t.mask(t.eq(""))

    def _ensure_len(s: pd.Series, length: int) -> pd.Series:
        d = _digits_only(s)
        return d.where(d.str.len().eq(length))

    def _normalize_uf(s: pd.Series) -> pd.Series:
        val = s.astype(str).str.strip().str.upper()
        return val.where(val.isin({
            'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RO','RS','RR','SC','SE','SP','TO'
        }))

    def _normalize_email(s: pd.Series) -> pd.Series:
        t = s.astype(str).str.strip().str.lower()
        return t.where(t.str.contains(r"^[^\s@]+@[^\s@]+\.[^\s@]+$", regex=True))

    def _normalize_cnae_code(s: pd.Series) -> pd.Series:
        d = _digits_only(s)
        return pd.to_numeric(d.where(d.str.len().eq(7)), errors="coerce").astype("Int64")

    def _normalize_pg_array_digits(s: pd.Series) -> pd.Series:
        def fix(x: str | None) -> str | None:
            if x is None:
                return None
            xs = str(x).strip()
            if xs == "":
                return None
            # Accept either raw tokens or PostgreSQL array text
            if xs.startswith("{") and xs.endswith("}"):
                body = xs[1:-1]
                tokens = [p.strip() for p in body.split(",")]
            else:
                tokens = [p.strip() for p in re.split(r"[;,]", xs) if p.strip()]
            digits = [re.sub(r"\D+", "", t) for t in tokens]
            clean = [d for d in digits if len(d) == 7]
            return ("{" + ",".join(clean) + "}") if clean else None
        return s.map(fix)

    def _dedup_sort_pg_array(s: pd.Series) -> pd.Series:
        def fix(x: str | None) -> str | None:
            if x is None:
                return None
            xs = str(x).strip()
            if not xs or not xs.startswith("{") or not xs.endswith("}"):
                return None
            body = xs[1:-1]
            toks = [t for t in body.split(",") if t]
            try:
                nums = sorted({int(t) for t in toks})
            except Exception:
                return None
            return ("{" + ",".join(str(n) for n in nums) + "}") if nums else None
        return s.map(fix)

    if config_name == "empresas":
        if "cnpj_basico" in df.columns:
            df["cnpj_basico"] = _ensure_len(df["cnpj_basico"], 8)
        if "razao_social" in df.columns:
            df["razao_social"] = _strip_empty_to_na(df["razao_social"])
        if "ente_federativo_responsavel" in df.columns:
            df["ente_federativo_responsavel"] = _strip_empty_to_na(df["ente_federativo_responsavel"])

    elif config_name == "estabelecimentos":
        if "cnpj_basico" in df.columns:
            df["cnpj_basico"] = _ensure_len(df["cnpj_basico"], 8)
        if "cnpj_ordem" in df.columns:
            df["cnpj_ordem"] = _ensure_len(df["cnpj_ordem"], 4)
        if "cnpj_dv" in df.columns:
            df["cnpj_dv"] = _ensure_len(df["cnpj_dv"], 2)
        for c in ["nome_fantasia","tipo_logradouro","logradouro","complemento","bairro","correio_eletronico","situacao_especial","nome_cidade_exterior"]:
            if c in df.columns:
                df[c] = _strip_empty_to_na(df[c])
        if "correio_eletronico" in df.columns:
            df["correio_eletronico"] = _normalize_email(df["correio_eletronico"])
        if "cep" in df.columns:
            df["cep"] = _ensure_len(df["cep"], 8)
        if "uf" in df.columns:
            df["uf"] = _normalize_uf(df["uf"])
        if "cnae_fiscal_principal_codigo" in df.columns:
            df["cnae_fiscal_principal_codigo"] = _normalize_cnae_code(df["cnae_fiscal_principal_codigo"])
        if "cnae_fiscal_secundaria" in df.columns:
            df["cnae_fiscal_secundaria"] = _normalize_pg_array_digits(df["cnae_fiscal_secundaria"].fillna(""))
        for c in ["ddd_1","telefone_1","ddd_2","telefone_2","ddd_fax","fax","numero"]:
            if c in df.columns:
                df[c] = _digits_only(df[c]).mask(df[c].astype(str).str.strip().eq(""))
        invalid_ids = {}
        invalid_examples = {}
        if {"cnpj_basico","cnpj_ordem","cnpj_dv"}.issubset(df.columns):
            cb = _digits_only(df["cnpj_basico"]).fillna("")
            co = _digits_only(df["cnpj_ordem"]).fillna("")
            dv = _digits_only(df["cnpj_dv"]).fillna("")
            full = (cb + co + dv)
            mask_full = full.str.len().eq(14)
            valid_mask = mask_full & full.map(_cnpj_valid)
            invalid_mask = mask_full & ~valid_mask
            if invalid_mask.any():
                invalid_ids["estabelecimentos_cnpj"] = int(invalid_mask.sum())
                invalid_examples["estabelecimentos_cnpj"] = full.loc[invalid_mask].head(10).tolist()
        if level == "aggressive":
            if "cnae_fiscal_secundaria" in df.columns:
                df["cnae_fiscal_secundaria"] = _dedup_sort_pg_array(df["cnae_fiscal_secundaria"]) 
            for c in ["ddd_1","ddd_2","ddd_fax"]:
                if c in df.columns:
                    d = df[c].astype(str)
                    df[c] = d.where(d.str.len().isin([2,3]))
            for c in ["telefone_1","telefone_2","fax"]:
                if c in df.columns:
                    d = df[c].astype(str)
                    df[c] = d.where(d.str.len().between(8,11))

    elif config_name == "socios":
        if "cnpj_basico" in df.columns:
            df["cnpj_basico"] = _ensure_len(df["cnpj_basico"], 8)
        if "cnpj_cpf_socio" in df.columns:
            d = _digits_only(df["cnpj_cpf_socio"])
            mask_len = d.str.len().isin([11,14])
            def _ok(x: str) -> bool:
                if len(x) == 11:
                    return _cpf_valid(x)
                if len(x) == 14:
                    return _cnpj_valid(x)
                return False
            valid = d.where(mask_len).map(lambda x: _ok(x) if isinstance(x, str) else False)
            df["cnpj_cpf_socio"] = d.where(valid)
        if "representante_legal_cpf" in df.columns:
            cpf = _ensure_len(df["representante_legal_cpf"], 11)
            df["representante_legal_cpf"] = cpf.where(cpf.map(lambda x: _cpf_valid(x) if isinstance(x, str) else False))
        for c in ["nome_socio_ou_razao_social","nome_representante_legal"]:
            if c in df.columns:
                df[c] = _strip_empty_to_na(df[c])

    elif config_name == "simples":
        if "cnpj_basico" in df.columns:
            df["cnpj_basico"] = _ensure_len(df["cnpj_basico"], 8)

    after_nulls = {c: int(df[c].isna().sum()) for c in df.columns}
    deltas = {c: after_nulls[c] - before_nulls.get(c, 0) for c in df.columns}
    repaired = {c: v for c, v in after_nulls.items() if v}
    if repaired:
        logger.info(f"Auto-repair: nullified/cleaned values per column: {repaired}")
    sample_diffs = {}
    if level == "aggressive" and snapshot:
        for c, before in snapshot.items():
            if c in df.columns:
                after = df[c].astype(str)
                changed = before != after
                if changed.any():
                    pairs = (
                        pd.DataFrame({"before": before, "after": after})
                        .loc[changed]
                        .dropna()
                        .head(10)
                        .to_dict(orient="records")
                    )
                    if pairs:
                        sample_diffs[c] = pairs
    telemetry = {
        "level": level,
        "after_nulls": after_nulls,
        "null_deltas": deltas,
        "columns": list(df.columns),
        "sample_diffs": sample_diffs,
    }
    if config_name == "estabelecimentos":
        if "invalid_ids" in locals() and invalid_ids:
            telemetry["invalid_ids"] = invalid_ids
            telemetry["invalid_id_examples"] = invalid_examples
    return df, telemetry
