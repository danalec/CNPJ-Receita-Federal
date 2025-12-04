import pandas as pd
import logging
import io
import re
import json
from datetime import datetime, timezone
import psycopg2
import requests
from psycopg2 import sql
from pathlib import Path
from typing import Dict, Tuple, Set, Union, cast, Any, TypedDict, Callable, Optional
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


DOMAIN_CACHE: Dict[Tuple[str, str], Set[Union[int, str]]] = {}


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
        fetchall = getattr(cursor, "fetchall", None)
        rows = fetchall() if callable(fetchall) else []
        s = set(r[0] for r in rows if r is not None)
    DOMAIN_CACHE[key] = s
    return s


UF_SET = {
    'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RO','RS','RR','SC','SE','SP','TO'
}


def _ensure_domains_loaded(conn, domains, strict):
    with conn.cursor() as cursor:
        for tbl in domains:
            cursor.execute(
                sql.SQL("SELECT COUNT(*) FROM {tbl}").format(tbl=sql.Identifier(tbl))
            )
            cnt = cursor.fetchone()[0]
            if cnt == 0:
                if strict:
                    raise RuntimeError(f"Tabela de domínio '{tbl}' ainda não carregada. Ajuste a ordem ou remova filtros --only/--exclude.")
                else:
                    logger.warning(f"Tabela de domínio '{tbl}' vazia. Validação de FK será relaxada.")


def _validate_fk_set(df, column, valid_set, label, strict):
    if column not in df.columns:
        return None
    mask_valid = df[column].isin(valid_set)
    mask_invalid = (~mask_valid) & df[column].notna()
    if mask_invalid.any():
        if strict:
            sample = list(set(df.loc[mask_invalid, column].astype(object).tolist()))[:10]
            # Return mask to allow caller to quarantine before raising
            raise ValueError(f"Valores inválidos em '{label}': {sample} (total {int(mask_invalid.sum())}). Garanta que a tabela de domínio está carregada e os códigos são válidos.")
        else:
            df.loc[mask_invalid, column] = pd.NA
            logger.warning(f"Valores inválidos em '{label}' foram definidos como nulos. Total {int(mask_invalid.sum())}.")
    return mask_invalid


def validate_chunk(config_name, chunk_df, conn):
    fk_violations = {}
    if config_name == "empresas":
        _ensure_domains_loaded(conn, ["naturezas_juridicas", "qualificacoes_socios"], settings.strict_fk_validation)
        nat = _get_domain_set(conn, "naturezas_juridicas", "codigo")
        qual = _get_domain_set(conn, "qualificacoes_socios", "codigo")
        if "natureza_juridica_codigo" in chunk_df.columns:
            m = _validate_fk_set(chunk_df, "natureza_juridica_codigo", nat, "natureza_juridica_codigo", settings.strict_fk_validation)
            if m is not None and m.any():
                fk_violations["natureza_juridica_codigo"] = m
        if "qualificacao_responsavel" in chunk_df.columns:
            m = _validate_fk_set(chunk_df, "qualificacao_responsavel", qual, "qualificacao_responsavel", settings.strict_fk_validation)
            if m is not None and m.any():
                fk_violations["qualificacao_responsavel"] = m
    elif config_name == "estabelecimentos":
        _ensure_domains_loaded(conn, ["paises", "municipios", "cnaes", "empresas"], settings.strict_fk_validation)
        paises = _get_domain_set(conn, "paises", "codigo")
        municipios = _get_domain_set(conn, "municipios", "codigo")
        cnaes = _get_domain_set(conn, "cnaes", "codigo")
        if "pais_codigo" in chunk_df.columns:
            m = _validate_fk_set(chunk_df, "pais_codigo", paises, "pais_codigo", settings.strict_fk_validation)
            if m is not None and m.any():
                fk_violations["pais_codigo"] = m
        if "municipio_codigo" in chunk_df.columns:
            m = _validate_fk_set(chunk_df, "municipio_codigo", municipios, "municipio_codigo", settings.strict_fk_validation)
            if m is not None and m.any():
                fk_violations["municipio_codigo"] = m
        if "cnae_fiscal_principal_codigo" in chunk_df.columns:
            m = _validate_fk_set(chunk_df, "cnae_fiscal_principal_codigo", cnaes, "cnae_fiscal_principal_codigo", settings.strict_fk_validation)
            if m is not None and m.any():
                fk_violations["cnae_fiscal_principal_codigo"] = m
        if "uf" in chunk_df.columns:
            m = _validate_fk_set(chunk_df, "uf", UF_SET, "uf", settings.strict_fk_validation)
            if m is not None and m.any():
                fk_violations["uf"] = m
        if "cnae_fiscal_secundaria" in chunk_df.columns:
            s = chunk_df["cnae_fiscal_secundaria"].dropna().astype(str)
            bad = ~((s.str.startswith("{") & s.str.endswith("}")) | (s == ""))
            if bad.any():
                if settings.strict_fk_validation:
                    raise ValueError("Campo cnae_fiscal_secundaria com formato inválido em algumas linhas. Esperado texto de array PostgreSQL: '{...}'.")
                else:
                    chunk_df.loc[bad.index[bad], "cnae_fiscal_secundaria"] = None
                    logger.warning("Valores inválidos em cnae_fiscal_secundaria foram definidos como nulos.")
                    fk_violations["cnae_fiscal_secundaria_fmt"] = bad
    elif config_name == "socios":
        _ensure_domains_loaded(conn, ["paises", "qualificacoes_socios", "empresas"], settings.strict_fk_validation)
        paises = _get_domain_set(conn, "paises", "codigo")
        qual = _get_domain_set(conn, "qualificacoes_socios", "codigo")
        if "pais_codigo" in chunk_df.columns:
            m = _validate_fk_set(chunk_df, "pais_codigo", paises, "pais_codigo", settings.strict_fk_validation)
            if m is not None and m.any():
                fk_violations["pais_codigo"] = m
        if "qualificacao_socio_codigo" in chunk_df.columns:
            m = _validate_fk_set(chunk_df, "qualificacao_socio_codigo", qual, "qualificacao_socio_codigo", settings.strict_fk_validation)
            if m is not None and m.any():
                fk_violations["qualificacao_socio_codigo"] = m
        if "qualificacao_representante_legal_codigo" in chunk_df.columns:
            m = _validate_fk_set(chunk_df, "qualificacao_representante_legal_codigo", qual, "qualificacao_representante_legal_codigo", settings.strict_fk_validation)
            if m is not None and m.any():
                fk_violations["qualificacao_representante_legal_codigo"] = m
    return fk_violations


def sanitize_dates(df, date_columns):
    cols = [c for c in date_columns if c in df.columns]
    if cols:
        df[cols] = df[cols].apply(lambda s: pd.to_datetime(s, format="%Y%m%d", errors="coerce"))
    return df


def clean_empresas_chunk(chunk_df):
    if "capital_social" in chunk_df.columns:
        s = chunk_df["capital_social"].astype(str).str.strip()
        s = s.replace({"": None, "None": None, "nan": None}, regex=False)
        s = s.astype(str)
        s = s.str.replace(".", "", regex=False)
        s = s.str.replace(",", ".", regex=False)
        chunk_df["capital_social"] = pd.to_numeric(s, errors="coerce")
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


class ETLTableConfig(TypedDict, total=False):
    table_name: str
    column_names: list[str]
    dtype_map: Dict[str, Any]
    custom_clean_func: Callable[[pd.DataFrame], pd.DataFrame]


ETL_CONFIG: Dict[str, ETLTableConfig] = {
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
                "municipio_nome",
                "uf_source",
                "municipio_source",
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
            "municipio_nome": str,
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
                "uf_source": str,
                "municipio_source": str,
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


def _jsonl_dir(kind: str) -> Path:
    base = cast(Path, settings.auto_repair_dir) if kind == "telemetry" else cast(Path, settings.quarantine_dir)
    rotate = settings.telemetry_rotate_daily if kind == "telemetry" else settings.quarantine_rotate_daily
    if rotate:
        d = datetime.now(timezone.utc).strftime("%Y%m%d")
        return base / d
    return base


def _select_jsonl_file(dir_path: Path, base_name: str, max_bytes: int) -> Path:
    dir_path.mkdir(parents=True, exist_ok=True)
    base = dir_path / f"{base_name}.jsonl"
    if not base.exists() or base.stat().st_size < max_bytes:
        return base
    idx = 1
    while True:
        p = dir_path / f"{base_name}_{idx}.jsonl"
        if not p.exists() or p.stat().st_size < max_bytes:
            return p
        idx += 1


def _write_jsonl(kind: str, config_name: str, record: dict):
    max_bytes = settings.telemetry_max_bytes if kind == "telemetry" else settings.quarantine_max_bytes
    dir_path = _jsonl_dir(kind)
    path = _select_jsonl_file(dir_path, config_name, max_bytes)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _write_json(path: Path, record: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False)


def _critical_fields(config_name: str):
    if config_name == "empresas":
        return ["cnpj_basico"]
    if config_name == "estabelecimentos":
        return ["cnpj_basico", "cnpj_ordem", "cnpj_dv"]
    if config_name == "socios":
        return ["cnpj_basico", "cnpj_cpf_socio"]
    if config_name == "simples":
        return ["cnpj_basico"]
    return []


def process_and_load_file(conn: Any, config_name: str) -> None:
    try:
        etl_config: ETLTableConfig = ETL_CONFIG[config_name]
    except KeyError:
        logger.error(f"Configuração para '{config_name}' não encontrada.")
        return

    table_name = etl_config["table_name"]
    file_path = cast(Path, settings.extracted_dir) / config_name / f"{config_name}.csv"

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
    gate_level_name = getattr(settings, "gate_log_level", "WARNING")
    gate_level = logging.WARNING if str(gate_level_name).upper() == "WARNING" else logging.INFO
    changed_totals: Dict[str, int] = {}
    null_delta_totals: Dict[str, int] = {}
    quality_gate_chunks = 0
    invalid_cnpj_rows = 0
    chunks_processed = 0
    class ColumnStats(TypedDict):
        nulls: int
        min: Optional[str]
        max: Optional[str]

    column_stats: Dict[str, ColumnStats] = {}
    for i, chunk in enumerate(reader):
        chunk_start = datetime.now(timezone.utc)
        clean = etl_config.get("custom_clean_func")
        if clean is not None:
            chunk = clean(chunk)
        chunk, telemetry, masks = schema_validate(config_name, chunk)
        rows_count = int(len(chunk))
        telemetry_record = {
            "table": table_name,
            "config": config_name,
            "chunk": i + 1,
            "rows": rows_count,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "duration_ms": int((datetime.now(timezone.utc) - chunk_start).total_seconds() * 1000),
            **telemetry,
        }
        if getattr(settings, "enable_quality_gates", True) and rows_count >= getattr(settings, "gate_min_rows", 20):
            triggers = {}
            changed = telemetry.get("changed_counts", {}) or {}
            deltas = telemetry.get("null_deltas", {}) or {}
            for c, n in changed.items():
                try:
                    ratio = (int(n) / rows_count)
                except Exception:
                    ratio = 0.0
                if ratio > getattr(settings, "gate_max_changed_ratio", 0.3):
                    triggers[c] = {"type": "changed", "ratio": ratio, "count": int(n)}
            for c, d in deltas.items():
                try:
                    ratio = (abs(int(d)) / rows_count)
                except Exception:
                    ratio = 0.0
                if ratio > getattr(settings, "gate_max_null_delta_ratio", 0.3):
                    if c in triggers:
                        triggers[c]["null_delta_ratio"] = ratio
                        triggers[c]["null_delta"] = int(d)
                    else:
                        triggers[c] = {"type": "null_delta", "ratio": ratio, "delta": int(d)}
            if triggers:
                telemetry_record["quality_gate"] = {"trigger_columns": triggers, "rows": rows_count}
                _write_jsonl("telemetry", config_name, telemetry_record)
                _write_jsonl(
                    "quarantine",
                    config_name,
                    {
                        "table": table_name,
                        "config": config_name,
                        "chunk": i + 1,
                        "reason": "quality_gate",
                        "trigger_columns": triggers,
                        "rows": rows_count,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    },
                )
                quality_gate_chunks += 1
                logger.log(gate_level, f"Chunk {i + 1}: quality gate triggered; skipping chunk.")
                continue
        else:
            _write_jsonl("telemetry", config_name, telemetry_record)

        for c, n in (telemetry.get("changed_counts", {}) or {}).items():
            changed_totals[c] = changed_totals.get(c, 0) + int(n)
        for c, d in (telemetry.get("null_deltas", {}) or {}).items():
            try:
                null_delta_totals[c] = null_delta_totals.get(c, 0) + int(d)
            except Exception:
                pass

        crit = _critical_fields(config_name)
        if crit:
            cols = [c for c in crit if c in chunk.columns]
            if cols:
                mask = chunk[cols].isna().any(axis=1)
                if mask.any():
                    bad = chunk.loc[mask].copy()
                    try:
                        bad = bad.where(pd.notna(bad), None)
                        for rec in bad.to_dict(orient="records"):
                            _write_jsonl(
                                "quarantine",
                                config_name,
                                {
                                    "table": table_name,
                                    "config": config_name,
                                    "chunk": i + 1,
                                    "reason": "critical_fields_null",
                                    "fields": cols,
                                    "row": rec,
                                    "timestamp": datetime.now(timezone.utc).isoformat(),
                                },
                            )
                    except Exception:
                        pass
        if config_name == "estabelecimentos" and isinstance(masks, dict) and masks.get("invalid_cnpj") is not None:
            try:
                bad = chunk.loc[masks["invalid_cnpj"]].copy()
                bad = bad.where(pd.notna(bad), None)
                for rec in bad.to_dict(orient="records"):
                    _write_jsonl(
                        "quarantine",
                        config_name,
                        {
                            "table": table_name,
                            "config": config_name,
                            "chunk": i + 1,
                            "reason": "invalid_cnpj",
                            "fields": ["cnpj_basico","cnpj_ordem","cnpj_dv"],
                            "row": rec,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        },
                    )
            except Exception:
                pass
            if getattr(settings, "skip_invalid_estabelecimentos_cnpj", False):
                skip_count = int(masks["invalid_cnpj"].sum())
                if skip_count:
                    chunk = chunk.loc[~masks["invalid_cnpj"]].copy()
                    logger.info(f"Chunk {i + 1}: skipped {skip_count} estabelecimentos rows due to invalid CNPJ.")
                    invalid_cnpj_rows += skip_count
                    if len(chunk) == 0:
                        logger.info(f"Chunk {i + 1}: no rows left after skip; continuing.")
                        continue
        try:
            fk_info = validate_chunk(config_name, chunk, conn)
        except ValueError as e:
            raise ValueError(f"{str(e)} | telemetry_after={json.dumps(telemetry, ensure_ascii=False)}")
        if isinstance(fk_info, dict) and fk_info:
            for label, mask in fk_info.items():
                try:
                    bad = chunk.loc[mask].copy()
                    bad = bad.where(pd.notna(bad), None)
                    for rec in bad.to_dict(orient="records"):
                        _write_jsonl(
                            "quarantine",
                            config_name,
                            {
                                "table": table_name,
                                "config": config_name,
                                "chunk": i + 1,
                                "reason": "fk_violation",
                                "fields": [label],
                                "row": rec,
                                "timestamp": datetime.utcnow().isoformat(),
                            },
                        )
                except Exception:
                    pass

        # Passa a conexão direta
        fast_load_chunk(conn, chunk, table_name)

        for col in chunk.columns:
            s = chunk[col]
            nulls = int(s.isna().sum())
            cs = column_stats.get(col)
            if cs is None:
                cs = {"nulls": 0, "min": None, "max": None}
                column_stats[col] = cs
            cs["nulls"] = int(cs["nulls"]) + nulls
            vmin = None
            vmax = None
            try:
                vmin = s.dropna().min()
                vmax = s.dropna().max()
            except Exception:
                vmin = None
                vmax = None
            if vmin is not None:
                vmin_str = str(vmin)
                if cs["min"] is None or vmin_str < str(cs["min"]):
                    cs["min"] = vmin_str
            if vmax is not None:
                vmax_str = str(vmax)
                if cs["max"] is None or vmax_str > str(cs["max"]):
                    cs["max"] = vmax_str

        total_rows += len(chunk)
        chunks_processed += 1
        logger.info(f"  ... Chunk {i + 1} processado. Total: {total_rows} linhas.")

    summary = {
        "table": table_name,
        "config": config_name,
        "rows_total": int(total_rows),
        "chunks_processed": int(chunks_processed),
        "quality_gate_chunks": int(quality_gate_chunks),
        "invalid_cnpj_rows_skipped": int(invalid_cnpj_rows),
        "changed_totals": changed_totals,
        "null_delta_totals": null_delta_totals,
        "column_stats": column_stats,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    dir_path = _jsonl_dir("telemetry")
    _write_json(dir_path / f"{config_name}_summary.json", summary)
    if getattr(settings, "enable_metrics_prometheus", False):
        metrics_lines = []
        metrics_lines.append(f"cnpj_auto_repair_rows_total{{table=\"{table_name}\"}} {int(total_rows)}")
        metrics_lines.append(f"cnpj_auto_repair_quality_gate_chunks_total{{table=\"{table_name}\"}} {int(quality_gate_chunks)}")
        for c, n in changed_totals.items():
            metrics_lines.append(f"cnpj_auto_repair_changed_total{{table=\"{table_name}\",column=\"{c}\"}} {int(n)}")
        for c, d in null_delta_totals.items():
            metrics_lines.append(f"cnpj_auto_repair_null_delta_total{{table=\"{table_name}\",column=\"{c}\"}} {int(d)}")
        p = getattr(settings, "prometheus_metrics_path", None)
        out_path = Path(p) if p else (dir_path / "metrics.prom")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "a", encoding="utf-8") as f:
            f.write("\n".join(metrics_lines) + "\n")
        if getattr(settings, "enable_prometheus_push", False) and getattr(settings, "prometheus_push_url", None):
            job = getattr(settings, "prometheus_job", "cnpj_auto_repair")
            inst = getattr(settings, "prometheus_instance", "local")
            url = str(getattr(settings, "prometheus_push_url"))
            push_url = url.rstrip("/") + f"/metrics/job/{job}/instance/{inst}"
            try:
                requests.post(push_url, data="\n".join(metrics_lines) + "\n", headers={"Content-Type": "text/plain"}, timeout=10)
            except Exception:
                pass
    if getattr(settings, "enable_otlp_push", False) and getattr(settings, "otlp_endpoint", None):
        try:
            requests.post(str(getattr(settings, "otlp_endpoint")), json=summary, timeout=10)
        except Exception:
            pass
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
    if filename == "schema.sql" and getattr(settings, "allow_drop", False):
        sql_content = "SET app.allow_drop='1';\n" + sql_content
    if filename == "schema.sql" and not settings.use_unlogged:
        sql_content = sql_content.replace("CREATE UNLOGGED TABLE", "CREATE TABLE")

    try:
        if filename == "schema.sql":
            try:
                with conn.cursor() as cursor:
                    cursor.execute("SET app.allow_drop = %s", ('1' if getattr(settings, 'allow_drop', False) else '0',))
            except Exception:
                pass
        needs_autocommit = ("CONCURRENTLY" in sql_content) or (filename == "constraints.sql")
        if needs_autocommit:
            old_autocommit = conn.autocommit
            try:
                conn.autocommit = True
                statements = []
                if filename == "constraints.sql":
                    with conn.cursor() as cursor:
                        cursor.execute("SET app.enable_backfill = %s", ('1' if getattr(settings, 'enable_constraints_backfill', True) else '0',))
                    import re
                    pattern = re.compile(r"DO\s+\$\$[\s\S]*?\$\$;", re.IGNORECASE)
                    blocks = [m.group(0) for m in pattern.finditer(sql_content)]
                    for g in blocks:
                        statements.append(g)
                    remainder = pattern.sub("", sql_content)
                    statements.extend([s.strip() for s in remainder.split(";") if s.strip()])
                else:
                    statements = [s.strip() for s in sql_content.split(";") if s.strip()]
                with conn.cursor() as cursor:
                    cursor.execute("SET search_path TO rfb;")
                for stmt in statements:
                    with conn.cursor() as cursor:
                        cursor.execute(stmt)
                logger.info(f"Sucesso ao executar {filename}")
            finally:
                conn.autocommit = old_autocommit
        else:
            with conn.cursor() as cursor:
                cursor.execute("SET search_path TO rfb;" + "\n" + sql_content)
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
        cnae_fiscal_secundaria INT[],
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
        data_situacao_especial DATE,
        municipio_nome VARCHAR(150),
        uf_source VARCHAR(20),
        municipio_source VARCHAR(20)
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
        if not getattr(settings, 'skip_constraints', False):
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


def run_constraints():
    """
    Executa apenas o arquivo de constraints e índices.
    """
    logger.info("Aplicando apenas Constraints e Índices...")
    try:
        conn = psycopg2.connect(settings.database_uri)
    except Exception as e:
        logger.error(f"Erro ao conectar no banco: {e}")
        return
    try:
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
        logger.info("Constraints aplicadas com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao aplicar constraints: {e}")
        conn.rollback()
    finally:
        conn.close()
