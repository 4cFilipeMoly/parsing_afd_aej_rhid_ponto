# src/summarizer.py
from __future__ import annotations
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional
import csv
from datetime import datetime, timedelta

# Quantos pares (entrada/saída) expor no CSV final
N_PARES = 4  # mude para 2 se quiser só entrada1/saida1/entrada2/saida2

def _parse_iso_dh(s: str) -> datetime:
    # Ex.: 2025-07-16T18:22:00-0300
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:00%z")

def _format_dh(dt: Optional[datetime]) -> str:
    return dt.strftime("%Y-%m-%d %H:%M") if dt else ""

def _format_td_hhmm(td: timedelta) -> str:
    total = int(td.total_seconds())
    if total < 0:
        total = 0
    h = total // 3600
    m = (total % 3600) // 60
    return f"{h:02d}:{m:02d}"

def _sum_pairs(times: List[datetime]) -> Tuple[timedelta, List[Tuple[Optional[datetime], Optional[datetime]]]]:
    """Cria pares (E,S) ordenados; pares incompletos não entram no total."""
    pares: List[Tuple[Optional[datetime], Optional[datetime]]] = []
    total = timedelta(0)
    i = 0
    while i < len(times):
        e = times[i]
        s = times[i+1] if i + 1 < len(times) else None
        pares.append((e, s))
        if s and s > e:
            total += (s - e)
        i += 2
    return total, pares

def carregar_marcacoes_csv(path_csv: Path) -> List[Dict[str, Any]]:
    """Lê o CSV de marcações (separador ';', UTF-8 com BOM)."""
    rows: List[Dict[str, Any]] = []
    with path_csv.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f, delimiter=';')
        for row in r:
            rows.append(row)
    return rows

def gerar_jornadas_por_cpf(
    path_csv_marcacoes: Path,
    out_path: Path,
    n_pares: int = N_PARES,
    ordenar_por: str = "data_cpf",  # "data_cpf" (padrão) ou "cpf_data"
) -> Path:
    """
    Gera um CSV com uma linha por CPF+data:
      cpf;data;entrada1;saida1;...;horas_trabalhadas;horas_extras_maior_10h;horas_extras_maior_6h
    - Ordena por data e CPF (ou CPF e data).
    - Pares incompletos não entram no somatório.
    """
    dados = carregar_marcacoes_csv(path_csv_marcacoes)

    # Agrupar por (cpf, data local)
    from collections import defaultdict
    buckets: Dict[Tuple[str, str], List[datetime]] = defaultdict(list)
    for row in dados:
        cpf = (row.get("cpf") or "").strip()
        dh = (row.get("dh_marcacao") or "").strip()
        if not cpf or not dh:
            continue
        try:
            dt = _parse_iso_dh(dh)
        except Exception:
            continue
        # Data local já considerando o TZ do carimbo
        data_local = dt.astimezone(dt.tzinfo).date().isoformat()
        buckets[(cpf, data_local)].append(dt)

    # Colunas de saída
    cols = ["cpf", "data"]
    for i in range(1, n_pares + 1):
        cols += [f"entrada{i}", f"saida{i}"]
    cols += ["horas_trabalhadas", "horas_extras_maior_10h", "horas_extras_maior_6h"]

    # Montar linhas
    linhas: List[Dict[str, Any]] = []
    for (cpf, data_str), times in buckets.items():
        times.sort()
        total, pares = _sum_pairs(times)

        # Calcular extras com dois limiares independentes
        limite_10h = timedelta(hours=10)
        limite_6h  = timedelta(hours=6)
        extra_10h = total - limite_10h if total > limite_10h else timedelta(0)
        extra_6h  = total - limite_6h  if total > limite_6h  else timedelta(0)

        row: Dict[str, Any] = {"cpf": cpf, "data": data_str}
        for i in range(n_pares):
            e = pares[i][0] if i < len(pares) else None
            s = pares[i][1] if i < len(pares) else None
            row[f"entrada{i+1}"] = _format_dh(e) if e else ""
            row[f"saida{i+1}"]   = _format_dh(s) if s else ""

        row["horas_trabalhadas"]      = _format_td_hhmm(total)
        row["horas_extras_maior_10h"] = _format_td_hhmm(extra_10h)
        row["horas_extras_maior_6h"]  = _format_td_hhmm(extra_6h)

        linhas.append(row)

    # Ordenar saída
    if ordenar_por == "cpf_data":
        linhas.sort(key=lambda r: (r["cpf"], r["data"]))
    else:  # padrão: data, depois CPF
        linhas.sort(key=lambda r: (r["data"], r["cpf"]))

    # Salvar CSV final
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, delimiter=';')
        w.writeheader()
        for row in linhas:
            w.writerow(row)

    return out_path
