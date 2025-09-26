# src/processor.py
from __future__ import annotations
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
import json
import re

# =========================
# Utilitários
# =========================

def _read_text_afd(path: Path) -> str:
    raw = path.read_bytes()
    return raw.decode("latin-1", errors="replace")

def _split_lines(s: str) -> List[str]:
    s = s.replace("\r\n", "\n")
    lines = s.split("\n")
    return [ln for ln in lines if ln.strip()]

def _is_digits(s: str) -> bool:
    return s.isdigit()

def _slice(line: str, a: int, b: int) -> str:
    return line[a-1:b]

def _ddmmaaaa_to_iso(d: str) -> Optional[str]:
    if len(d) == 8 and _is_digits(d):
        return f"{d[4:8]}-{d[2:4]}-{d[0:2]}"
    return None

def _hhmm_to_hhmm(h: str) -> Optional[str]:
    if len(h) == 4 and _is_digits(h):
        return f"{h[:2]}:{h[2:4]}"
    return None

def _is_iso_d(s: str) -> bool:
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", s))

def _is_iso_dh(s: str) -> bool:
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:00[+-]\d{4}", s))

# =========================
# CRC-16/IBM (ARC)
# =========================
def crc16_ibm_arc(data: bytes) -> int:
    crc = 0x0000
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc & 0xFFFF

def _crc16_hex4_for_line(line: str, crc_field_pos: Tuple[int, int]) -> str:
    a, b = crc_field_pos
    left = _slice(line, 1, a-1)
    right = _slice(line, b+1, len(line))
    data = (left + right).encode("latin-1", errors="replace")
    return f"{crc16_ibm_arc(data):04X}"

# =========================
# Modelos
# =========================
@dataclass
class Registro1:
    nsr: int
    tipo: int
    id_empregador_tipo: str | None
    id_empregador: str | None
    cno_caepf: str | None
    razao_social: str | None
    numero_fabricacao_processo_ou_inpi: str | None
    data_inicio: str | None
    data_fim: str | None
    data_hora_geracao: str | None
    versao_layout: str | None
    id_fabricante_tipo: str | None
    id_fabricante: str | None
    modelo_rep_c: str | None
    crc16: str | None
    crc_ok: bool | None

@dataclass
class Registro2:
    nsr: int; tipo: int; dh_gravacao: str; cpf_responsavel: str
    id_empregador_tipo: str; id_empregador: str; cno_caepf: str
    razao_social: str; local_prestacao: str; crc16: str; crc_ok: bool

@dataclass
class Registro3:
    nsr: int
    tipo: str  # '3' ou '7' etc.
    dh_marcacao: str
    cpf: str
    crc16: str | None
    crc_ok: bool | None
    formato: str  # 'oficial' ou 'compacto'

@dataclass
class Registro4:
    nsr: int; tipo: int; dh_antes: str; dh_ajustada: str; cpf_responsavel: str; crc16: str; crc_ok: bool

@dataclass
class Registro5:
    nsr: int; tipo: int; dh_gravacao: str; operacao: str; cpf: str; nome: str; demais_dados: str; cpf_responsavel: str; crc16: str; crc_ok: bool

@dataclass
class Registro6:
    nsr: int; tipo: int; dh_gravacao: str; tipo_evento: str

@dataclass
class Registro7:
    nsr: int; tipo: str; dh_marcacao: str; cpf: str; dh_gravacao: str; coletor_id: str; online_offline: str; hash256: str

@dataclass
class Registro9:
    nsr: int; qtd_tipo2: int; qtd_tipo3: int; qtd_tipo4: int; qtd_tipo5: int; qtd_tipo6: int; qtd_tipo7: int; tipo: int

# =========================
# Parsers (oficial)
# =========================
def parse_registro1_oficial(line: str) -> Registro1:
    if len(line) < 302:
        raise ValueError(f"Tipo 1 (oficial) com tamanho {len(line)} < 302")
    nsr = int(_slice(line, 1, 9)); tipo = int(_slice(line, 10, 10))
    id_emp_tipo = _slice(line, 11, 11); id_emp = _slice(line, 12, 25).strip()
    cno = _slice(line, 26, 39).strip(); razao = _slice(line, 40, 189).rstrip()
    num_fab_proc_inpi = _slice(line, 190, 206).strip()
    data_ini = _slice(line, 207, 216); data_fim = _slice(line, 217, 226)
    dh_ger = _slice(line, 227, 250); versao = _slice(line, 251, 253)
    id_fab_tipo = _slice(line, 254, 254); id_fab = _slice(line, 255, 268).strip()
    modelo = _slice(line, 269, 298).rstrip()
    crc = _slice(line, 299, 302).upper()
    crc_calc = _crc16_hex4_for_line(line, (299, 302))
    return Registro1(nsr, tipo, id_emp_tipo, id_emp, cno, razao, num_fab_proc_inpi,
                     data_ini, data_fim, dh_ger, versao, id_fab_tipo, id_fab, modelo,
                     crc, crc == crc_calc)

def parse_registro2(line: str) -> Registro2:
    if len(line) < 331: raise ValueError(f"Tipo 2 tamanho {len(line)} < 331")
    nsr = int(_slice(line, 1, 9)); tipo = int(_slice(line, 10, 10))
    dh = _slice(line, 11, 34); cpf_resp = _slice(line, 35, 48).strip()
    id_emp_tipo = _slice(line, 49, 49); id_emp = _slice(line, 50, 63).strip()
    cno = _slice(line, 64, 77).strip(); razao = _slice(line, 78, 227).rstrip()
    local = _slice(line, 228, 327).rstrip(); crc = _slice(line, 328, 331).upper()
    crc_ok = (crc == _crc16_hex4_for_line(line, (328, 331)))
    return Registro2(nsr, tipo, dh, cpf_resp, id_emp_tipo, id_emp, cno, razao, local, crc, crc_ok)

def parse_registro3_oficial(line: str) -> Registro3:
    if len(line) < 50: raise ValueError(f"Tipo 3 (oficial) tamanho {len(line)} < 50")
    nsr = int(_slice(line, 1, 9)); tipo = _slice(line, 10, 10)
    dh = _slice(line, 11, 34); cpf = _slice(line, 35, 46).strip()
    crc = _slice(line, 47, 50).upper(); crc_ok = (crc == _crc16_hex4_for_line(line, (47, 50)))
    if not _is_iso_dh(dh): raise ValueError(f"Tipo 3 (oficial) DH inválido: {dh!r}")
    return Registro3(nsr, tipo, dh, cpf, crc, crc_ok, formato="oficial")

def parse_registro4(line: str) -> Registro4:
    if len(line) < 73: raise ValueError(f"Tipo 4 tamanho {len(line)} < 73")
    nsr = int(_slice(line, 1, 9)); tipo = int(_slice(line, 10, 10))
    dh_antes = _slice(line, 11, 34); dh_ajustada = _slice(line, 35, 58)
    cpf_resp = _slice(line, 59, 69).strip(); crc = _slice(line, 70, 73).upper()
    crc_ok = (crc == _crc16_hex4_for_line(line, (70, 73)))
    return Registro4(nsr, tipo, dh_antes, dh_ajustada, cpf_resp, crc, crc_ok)

def parse_registro5(line: str) -> Registro5:
    if len(line) < 118: raise ValueError(f"Tipo 5 tamanho {len(line)} < 118")
    nsr = int(_slice(line, 1, 9)); tipo = int(_slice(line, 10, 10))
    dh = _slice(line, 11, 34); oper = _slice(line, 35, 35)
    cpf = _slice(line, 36, 47).strip(); nome = _slice(line, 48, 99).rstrip()
    dados = _slice(line, 100, 103).rstrip(); cpf_resp = _slice(line, 104, 114).strip()
    crc = _slice(line, 115, 118).upper(); crc_ok = (crc == _crc16_hex4_for_line(line, (115, 118)))
    return Registro5(nsr, tipo, dh, oper, cpf, nome, dados, cpf_resp, crc, crc_ok)

def parse_registro6(line: str) -> Registro6:
    if len(line) < 36: raise ValueError(f"Tipo 6 tamanho {len(line)} < 36")
    nsr = int(_slice(line, 1, 9)); tipo = int(_slice(line, 10, 10))
    dh = _slice(line, 11, 34); tipo_evt = _slice(line, 35, 36)
    return Registro6(nsr, tipo, dh, tipo_evt)

def parse_registro7(line: str) -> Registro7:
    if len(line) < 137: raise ValueError(f"Tipo 7 tamanho {len(line)} < 137")
    nsr = int(_slice(line, 1, 9)); tipo = _slice(line, 10, 10)
    dh = _slice(line, 11, 34); cpf = _slice(line, 35, 46).strip()
    dh_grav = _slice(line, 47, 70); coletor = _slice(line, 71, 72); onoff = _slice(line, 73, 73)
    h = _slice(line, 74, 137).strip()
    return Registro7(nsr, tipo, dh, cpf, dh_grav, coletor, onoff, h)

def parse_registro9(line: str) -> Registro9:
    if len(line) < 64: raise ValueError(f"Tipo 9 tamanho {len(line)} < 64")
    nsr = int(_slice(line, 1, 9)); q2 = int(_slice(line, 10, 18)); q3 = int(_slice(line, 19, 27))
    q4 = int(_slice(line, 28, 36)); q5 = int(_slice(line, 37, 45)); q6 = int(_slice(line, 46, 54)); q7 = int(_slice(line, 55, 63))
    tipo9 = int(_slice(line, 64, 64))
    return Registro9(nsr, q2, q3, q4, q5, q6, q7, tipo9)

# =========================
# Parsers (fallback compacto)
# =========================

def parse_registro1_compacto(line: str) -> Registro1:
    """Header compacto: pega as 3 datas + hora nos últimos 28 dígitos."""
    if len(line) < 9+1+28:
        raise ValueError(f"Tipo 1 compacto muito curto: {len(line)}")
    nsr = int(line[:9]); tipo = int(line[9])
    trail = line[-28:]
    if not _is_digits(trail):
        raise ValueError("Tipo 1 compacto sem bloco final de 28 dígitos")
    di, df, dg, hg = trail[0:8], trail[8:16], trail[16:24], trail[24:28]
    di_iso = _ddmmaaaa_to_iso(di); df_iso = _ddmmaaaa_to_iso(df); dg_iso = _ddmmaaaa_to_iso(dg)
    dh_ger = f"{dg_iso}T{_hhmm_to_hhmm(hg)}:00-0300" if dg_iso and _hhmm_to_hhmm(hg) else None
    return Registro1(
        nsr=nsr, tipo=tipo,
        id_empregador_tipo=None, id_empregador=None, cno_caepf=None, razao_social=None,
        numero_fabricacao_processo_ou_inpi=None,
        data_inicio=di_iso, data_fim=df_iso, data_hora_geracao=dh_ger,
        versao_layout=None, id_fabricante_tipo=None, id_fabricante=None, modelo_rep_c=None,
        crc16=None, crc_ok=None
    )

def parse_registro3_compacto(line: str) -> Registro3:
    """Tipo 3 compacto: NSR(9)+3(1)+DDMMAAAA(8)+HHMM(4)+CPF/PIS(12) => >=34"""
    if len(line) < 34:
        raise ValueError(f"Tipo 3 compacto muito curto: {len(line)}")
    nsr = int(line[:9]); tipo = line[9]
    d, h, cpf = line[10:18], line[18:22], line[22:34].strip()
    di = _ddmmaaaa_to_iso(d); hh = _hhmm_to_hhmm(h)
    if not di or not hh:
        raise ValueError(f"Tipo 3 compacto com data/hora inválidas: {d} {h}")
    dh = f"{di}T{hh}:00-0300"
    return Registro3(nsr, tipo, dh, cpf, crc16=None, crc_ok=None, formato="compacto")

# =========================
# Pipeline principal
# =========================
def interpretar_afd(path: Path) -> Dict[str, Any]:
    text = _read_text_afd(path)
    lines = _split_lines(text)
    if not lines:
        raise ValueError("Arquivo vazio")

    registros: List[Tuple[int, str]] = []
    erros: List[str] = []

    for ln in lines:
        if len(ln) < 10 or not _is_digits(ln[:9]):
            erros.append(f"Linha inválida (sem NSR): {ln!r}")
            continue
        registros.append((int(ln[:9]), ln))

    # checagem simples de ordem
    ordem_nsr_ok = [n for n,_ in registros] == sorted(n for n,_ in registros)

    header: Optional[Registro1] = None
    bucket: Dict[str, list] = {k: [] for k in ("2","3","4","5","6","7")}
    trailer: Optional[Registro9] = None
    crc_ok_por_tipo: Dict[str, List[bool]] = {k: [] for k in ("1","2","3","4","5")}

    for _, line in registros:
        tipo = line[9]

        try:
            if tipo == "1":
                # tenta oficial; se falhar, tenta compacto
                try:
                    r = parse_registro1_oficial(line)
                except Exception as e1:
                    try:
                        r = parse_registro1_compacto(line)
                    except Exception as e2:
                        raise ValueError(f"Falha no tipo 1: {e1} | fallback: {e2}")
                header = r
                if r.crc_ok is not None:
                    crc_ok_por_tipo["1"].append(r.crc_ok)

            elif tipo == "2":
                r = parse_registro2(line); bucket["2"].append(r); crc_ok_por_tipo["2"].append(r.crc_ok)

            elif tipo == "3":
                # tenta oficial; se falhar, tenta compacto
                try:
                    r = parse_registro3_oficial(line)
                except Exception as e1:
                    try:
                        r = parse_registro3_compacto(line)
                    except Exception as e2:
                        raise ValueError(f"Falha no tipo 3: {e1} | fallback: {e2}")
                bucket["3"].append(r)
                if r.crc_ok is not None:
                    crc_ok_por_tipo["3"].append(r.crc_ok)

            elif tipo == "4":
                r = parse_registro4(line); bucket["4"].append(r); crc_ok_por_tipo["4"].append(r.crc_ok)

            elif tipo == "5":
                r = parse_registro5(line); bucket["5"].append(r); crc_ok_por_tipo["5"].append(r.crc_ok)

            elif tipo == "6":
                r = parse_registro6(line); bucket["6"].append(r)

            elif tipo == "7":
                r = parse_registro7(line); bucket["7"].append(r)

            elif tipo == "9":
                trailer = parse_registro9(line)

            else:
                erros.append(f"Tipo desconhecido: {tipo}")

        except Exception as e:
            # não para o processamento por causa de uma linha
            erros.append(f"Erro ao parsear NSR {line[:9]} (tipo {tipo}): {e}")

    contagens_ok = True
    if trailer:
        contagens_ok = (
            trailer.qtd_tipo2 == len(bucket["2"]) and
            trailer.qtd_tipo3 == len(bucket["3"]) and
            trailer.qtd_tipo4 == len(bucket["4"]) and
            trailer.qtd_tipo5 == len(bucket["5"]) and
            trailer.qtd_tipo6 == len(bucket["6"]) and
            trailer.qtd_tipo7 == len(bucket["7"]) and
            trailer.tipo == 9
        )

    if not any(bucket.values()) and not header and not trailer:
        # nada parseado: exponha os primeiros erros
        raise ValueError("Nenhum registro foi interpretado. Ex.: " + "; ".join(erros[:3]))

    return {
        "header": asdict(header) if header else None,
        "registros_por_tipo": {k: [asdict(x) for x in v] for k, v in bucket.items()},
        "trailer": asdict(trailer) if trailer else None,
        "validacoes": {
            "ordem_nsr_ok": ordem_nsr_ok,
            "contagens_ok": contagens_ok,
            "crc_ok_por_tipo": {k: (all(v) if v else None) for k, v in crc_ok_por_tipo.items()},
            "erros": erros[:200],  # limites para não explodir
        },
    }

def salvar_json_interpretacao(data: Dict[str, Any], out_path: Path) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return out_path

def exportar_marcacoes_tipo3_csv(data: Dict[str, Any], out_path: Path) -> Path:
    import csv
    regs = data.get("registros_por_tipo", {}).get("3", [])
    cols = ["nsr", "dh_marcacao", "cpf", "crc16", "crc_ok", "formato"]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # BOM UTF-8 para o Excel reconhecer encoding; delimitador ';' para locale pt-BR
    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=cols, delimiter=';', quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        for r in regs:
            w.writerow({
                "nsr": r.get("nsr"),
                "dh_marcacao": r.get("dh_marcacao"),
                "cpf": r.get("cpf"),
                "crc16": r.get("crc16"),
                "crc_ok": r.get("crc_ok"),
                "formato": r.get("formato"),
            })
    return out_path
