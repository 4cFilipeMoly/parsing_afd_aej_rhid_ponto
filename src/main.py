# src/main.py
from pathlib import Path
import os
import sys
import io
import json
import zipfile
import requests
from dotenv import load_dotenv
from .processor import interpretar_afd, salvar_json_interpretacao, exportar_marcacoes_tipo3_csv
from .summarizer import gerar_jornadas_por_cpf  # resumo por CPF/dia com duas colunas de extra

# -------------------------
# Configuração (.env)
# -------------------------
load_dotenv()
API_BASE = os.getenv("API_BASE", "").rstrip("/")
API_EMAIL = os.getenv("API_EMAIL")
API_PASSWORD = os.getenv("API_PASSWORD")
API_DOMAIN = os.getenv("API_DOMAIN")
ID_EQUIP = os.getenv("ID_EQUIPAMENTO", "1")
DATA_INI = os.getenv("DATA_INI", "2025-07-01")
DATA_FIM = os.getenv("DATA_FIM", "2025-07-31")

LOGIN_URL = f"{API_BASE}/login"
DOWNLOAD_URL = f"{API_BASE}/report/afd_coletor_marcacao/download"
EXPORT_DIR = Path("export")

# -------------------------
# Utilitários
# -------------------------
def debug_write(name: str, content: bytes | str):
    """Grava conteúdo em export/ para diagnóstico, sempre."""
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    p = EXPORT_DIR / name
    if isinstance(content, str):
        p.write_text(content, encoding="utf-8", errors="ignore")
    else:
        p.write_bytes(content)
    return p

def _try_extract_afd_from_json_bytes(content: bytes) -> tuple[bool, bytes]:
    """
    Tenta encontrar, dentro de um JSON, uma string que seja o AFD (múltiplas linhas
    começando com 9 dígitos + tipo na 10ª coluna). Retorna (encontrou, afd_bytes).
    """
    try:
        obj = json.loads(content.decode("utf-8", errors="ignore"))
    except Exception:
        return (False, b"")

    def walk(o):
        if isinstance(o, str):
            lines = o.replace("\r\n", "\n").split("\n")
            hits = 0
            for ln in lines:
                if len(ln) >= 10 and ln[:9].isdigit() and ln[9] in "12345679":
                    hits += 1
            if hits >= 3:
                # o AFD deve ser ISO-8859-1; salvamos como latin-1
                return o.encode("latin-1", errors="ignore")
        elif isinstance(o, dict):
            for v in o.values():
                found = walk(v)
                if found:
                    return found
        elif isinstance(o, list):
            for v in o:
                found = walk(v)
                if found:
                    return found
        return None

    afd = walk(obj)
    if afd:
        return (True, afd)
    return (False, b"")

# -------------------------
# Autenticação e Download
# -------------------------
def get_token() -> str:
    if not all([API_BASE, API_EMAIL, API_PASSWORD, API_DOMAIN]):
        raise RuntimeError("Configure .env: API_BASE, API_EMAIL, API_PASSWORD, API_DOMAIN")

    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    payload = {"email": API_EMAIL, "password": API_PASSWORD, "domain": API_DOMAIN}

    r = requests.post(LOGIN_URL, json=payload, headers=headers, timeout=60)
    try:
        r.raise_for_status()
    except requests.HTTPError:
        debug_write("last_login_response.txt", r.text)
        raise RuntimeError(
            f"Falha no login ({r.status_code}). Resposta salva em export/last_login_response.txt"
        )

    data = r.json()
    token = (
        data.get("token")
        or data.get("access_token")
        or (data.get("data") or {}).get("token")
        or next((v for v in data.values() if isinstance(v, str) and len(v) > 20), None)
    )
    if not token:
        debug_write("last_login_response.json", json.dumps(data, ensure_ascii=False, indent=2))
        raise RuntimeError("Token não encontrado. Resposta salva em export/last_login_response.json")
    return token

def download_afd(token: str, id_equip: str, data_ini: str, data_fim: str) -> Path:
    """
    Baixa o AFD. Lida com:
    - JSON: tenta extrair texto do AFD de dentro do JSON
    - ZIP: salva e extrai primeiro .txt/.dat
    - TXT/DAT: salva direto
    Sempre salva artefatos de depuração em export/.
    """
    headers = {"Authorization": f"Bearer {token}", "Accept": "*/*"}
    params = {"idEquipamento": str(id_equip), "dataIni": data_ini, "dataFinal": data_fim}

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[DEBUG] cwd={Path.cwd().resolve()} -> export={EXPORT_DIR.resolve()}")

    with requests.get(DOWNLOAD_URL, headers=headers, params=params, timeout=180, stream=True) as r:
        # headers e corpo (bruto/texto) para diagnóstico
        debug_write("last_response_headers.txt", "\n".join(f"{k}: {v}" for k, v in r.headers.items()))
        content = b"".join(r.iter_content(262_144) or b"")
        debug_write("last_response.bin", content)
        try:
            debug_write("last_response.txt", content.decode("latin-1"))
        except Exception:
            pass

        if r.status_code >= 400:
            raise RuntimeError(
                f"Erro no download ({r.status_code}). Veja export/last_response_headers.txt e .bin/.txt"
            )

        dispo = r.headers.get("Content-Disposition", "")
        ctype = (r.headers.get("Content-Type") or "").lower()

        # 1) JSON -> tentar extrair AFD
        if "json" in ctype:
            ok, afd_bytes = _try_extract_afd_from_json_bytes(content)
            if ok:
                out_path = EXPORT_DIR / f"afd_extraido_{id_equip}_{data_ini}_a_{data_fim}.dat"
                out_path.write_bytes(afd_bytes)
                print(f"[DEBUG] AFD extraído de JSON: {out_path}")
                return out_path
            else:
                # salvar o JSON para inspeção
                jp = EXPORT_DIR / f"afd_api_response_{id_equip}_{data_ini}_a_{data_fim}.json"
                jp.write_bytes(content)
                raise RuntimeError(
                    f"A resposta é JSON, mas não encontrei uma string AFD. Salvei {jp.name}. "
                    "Abra-o e me informe as chaves para ajustarmos o extrator."
                )

        # 2) ZIP -> salvar e extrair primeiro .txt/.dat
        is_zip = (
            "zip" in ctype
            or (len(content) >= 2 and content[:2] == b"PK")
            or ("filename=" in dispo and dispo.lower().endswith(".zip"))
        )
        if is_zip:
            zip_path = EXPORT_DIR / (dispo.split("filename=")[-1].strip('"; ') if "filename=" in dispo else "afd_download.zip")
            zip_path.write_bytes(content)
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                names = zf.namelist()
                preferred = [n for n in names if n.lower().endswith((".txt", ".dat"))] or names
                inner = preferred[0]
                extracted_path = EXPORT_DIR / Path(inner).name
                with zf.open(inner) as f:
                    extracted_path.write_bytes(f.read())
            print(f"[DEBUG] ZIP salvo: {zip_path} | Extraído: {extracted_path}")
            return extracted_path

        # 3) TXT/DAT puro
        filename = f"afd_marcacao_{id_equip}_{data_ini}_a_{data_fim}"
        if "filename=" in dispo:
            filename = dispo.split("filename=")[-1].strip('"; ')
        if "." not in Path(filename).name:
            if "text" in ctype or "plain" in ctype:
                filename += ".txt"
            else:
                filename += ".dat"
        out_path = EXPORT_DIR / filename
        out_path.write_bytes(content)
        print(f"[DEBUG] AFD salvo: {out_path}")
        return out_path

# -------------------------
# Orquestração
# -------------------------
def main():
    try:
        print("Autenticando…")
        token = get_token()

        print("Baixando AFD…")
        afd_file = download_afd(token, ID_EQUIP, DATA_INI, DATA_FIM)
        print(f"AFD salvo em: {afd_file.resolve()}")

        print("Interpretando AFD…")
        data = interpretar_afd(afd_file)

        out_json = salvar_json_interpretacao(data, EXPORT_DIR / "interpretacao.json")
        out_csv = exportar_marcacoes_tipo3_csv(data, EXPORT_DIR / "marcacoes_tipo3.csv")

        # ---- Resumo por CPF/dia (pares, total, extras >10h e >6h) ----
        out_resumo = gerar_jornadas_por_cpf(
            EXPORT_DIR / "marcacoes_tipo3.csv",
            EXPORT_DIR / "jornadas_por_cpf.csv",
            n_pares=4,              # mude p/ 2 se quiser menos colunas
            ordenar_por="data_cpf", # garante ordenação por data e CPF
        )

        print("OK!")
        print(f"- JSON: {out_json.resolve()}")
        print(f"- CSV : {out_csv.resolve()}")
        print(f"- Resumo por CPF/dia: {out_resumo.resolve()}")
        print(f"- Validações: {data['validacoes']}")
    except Exception as e:
        print(f"[ERRO] {e}", file=sys.stderr)
        print("Veja também os arquivos em 'export/': last_response.* para diagnóstico.", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
