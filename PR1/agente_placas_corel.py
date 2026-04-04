#!/usr/bin/env python3
"""
Agente autonomo para gerar placas de preco no CorelDRAW a partir de uma planilha em PDF.

Fluxo:
1) Le o PDF e tenta extrair tabelas.
2) Detecta colunas de nome e preco (ou usa as colunas informadas).
3) Abre o CorelDRAW por COM.
4) Gera uma placa por linha usando:
   - template CDR com placeholders, ou
   - layout simples criado do zero.
5) Exporta cada placa para PDF.
"""

from __future__ import annotations

import argparse
import base64
import calendar
import getpass
import hashlib
import html
import hmac
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import threading
import time
import unicodedata
import webbrowser
import secrets
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
import traceback
from urllib.parse import quote, quote_plus
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from pdf_a6_profile import merge_four_plate_pdfs_grid
from placeholder_replacements import replace_placeholders_in_text
from web_generation_progress import GenerationProgressTracker


try:
    import pdfplumber
except ImportError as exc:
    raise SystemExit(
        "Dependencia ausente: pdfplumber. Instale com `pip install -r requirements.txt`."
    ) from exc


@dataclass
class PlateData:
    index: int
    name: str
    price: str
    row: Dict[str, str]
    original_name: str = ""
    cleanup_source: str = "manual"
    quantity: int = 1
    unit_label: str = "KG"
    plate_format: str = "A4"
    format_quantities: Dict[str, int] = field(default_factory=dict)
    duplex_enabled: bool = False
    offer_validity_enabled: bool = False
    offer_validity_day: int = 0
    offer_validity_month: int = 0


@dataclass
class OutputPdfRecord:
    path: Path
    duplex_enabled: bool = False
    plate_format: str = "A4"
    plate_names: List[str] = field(default_factory=list)
    order_index: int = 0


@dataclass(frozen=True)
class LayoutProfile:
    name_max_width_cm: float
    name_base_font_size: float
    name_min_font_size: float
    name_max_font_size: float
    name_split_if_font_below: float
    name_line_spacing_max: float
    name_two_lines_total_height_cm: float
    price_max_width_cm: float
    unit_gap_below_cents_cm: float
    unit_font_size_kg: float
    unit_font_size_unid: float
    offer_validity_bottom_cm: float
    offer_validity_max_width_cm: float
    offer_validity_max_height_cm: float
    offer_validity_font_size: float
    offer_validity_min_font_size: float


@dataclass
class WebProgressSession:
    server: ThreadingHTTPServer
    server_thread: threading.Thread
    tracker: GenerationProgressTracker


class StopRequestedError(RuntimeError):
    pass


_ACTIVE_WEB_PROGRESS_SESSION: Optional[WebProgressSession] = None
_OLLAMA_EXECUTABLE_CACHE: Optional[Path] = None
_OLLAMA_LOOKUP_DONE = False
_OLLAMA_WARNING_EMITTED = False
_OLLAMA_MODELS_CACHE: Optional[List[str]] = None
_OLLAMA_MODELS_LOOKUP_DONE = False
_OLLAMA_MODEL_RESOLUTION_CACHE: Dict[str, str] = {}
_OLLAMA_MODEL_FALLBACK_NOTIFIED: set[str] = set()
_CODE_GUARD_PASSWORD_SHA256_CACHE = ""
_CODE_GUARD_PASSWORD_CACHE_LOADED = False
_CODE_GUARD_PASSWORD_CACHE_ERROR = ""
STATUS_EMOJIS: Dict[str, str] = {
    "PDF": "📄",
    "AI": "🧠",
    "ITENS": "📋",
    "WEB": "🌐",
    "TPL": "🧩",
    "MODEL": "🧩",
    "PLACA": "🏷️",
    "JOBS": "🛠️",
    "FMT": "📐",
    "STOP": "⏹️",
    "CACHE": "💾",
    "COREL": "🎨",
    "PRINT": "🖨️",
    "LOG": "📝",
    "POWER": "⏻",
    "FIM": "✅",
    "CANCEL": "🚫",
    "FORMATO": "📐",
    "PADRAO": "📌",
}


def print_status(tag: str, message: str) -> None:
    safe_tag = re.sub(r"[^A-Z0-9]+", "", str(tag or "").upper())[:8] or "INFO"
    emoji = STATUS_EMOJIS.get(safe_tag, "🔹")
    line = f"{emoji} [{safe_tag}] {message}"
    try:
        print(line)
    except UnicodeEncodeError:
        # Fallback para terminais Windows legados (cp1252) sem suporte a emoji.
        print(f"[{safe_tag}] {message}")


def _get_active_web_progress_tracker() -> Optional[GenerationProgressTracker]:
    session = _ACTIVE_WEB_PROGRESS_SESSION
    if not session:
        return None
    return session.tracker


def _set_active_web_progress_session(session: Optional[WebProgressSession]) -> None:
    global _ACTIVE_WEB_PROGRESS_SESSION
    if _ACTIVE_WEB_PROGRESS_SESSION is not None and _ACTIVE_WEB_PROGRESS_SESSION is not session:
        _shutdown_active_web_progress_session()
    _ACTIVE_WEB_PROGRESS_SESSION = session


def _shutdown_active_web_progress_session() -> None:
    global _ACTIVE_WEB_PROGRESS_SESSION
    session = _ACTIVE_WEB_PROGRESS_SESSION
    if not session:
        return
    _ACTIVE_WEB_PROGRESS_SESSION = None
    try:
        session.server.shutdown()
    except Exception:
        pass
    try:
        session.server.server_close()
    except Exception:
        pass
    try:
        session.server_thread.join(timeout=2.0)
    except Exception:
        pass


class HotkeyStopController:
    def __init__(self, hotkey: str = "f7") -> None:
        self.hotkey = (hotkey or "").strip().lower()
        self._event = threading.Event()
        self._listener = None
        self._keyboard_mod = None

    def _on_press(self, key) -> None:
        key_name = None
        try:
            key_name = getattr(key, "name", None)
        except Exception:
            key_name = None

        if key_name and key_name.lower() == self.hotkey:
            self._event.set()

    def start(self) -> bool:
        if not self.hotkey:
            return False
        try:
            from pynput import keyboard  # type: ignore
        except Exception:
            return False

        self._keyboard_mod = keyboard
        self._listener = keyboard.Listener(on_press=self._on_press)
        self._listener.daemon = True
        self._listener.start()
        return True

    def stop(self) -> None:
        if self._listener:
            try:
                self._listener.stop()
            except Exception:
                pass
            self._listener = None

    def is_stop_requested(self) -> bool:
        return self._event.is_set()


def choose_pdf_via_dialog(initial_dir: Path) -> Optional[Path]:
    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception:
        return None

    root = tk.Tk()
    root.withdraw()
    try:
        root.attributes("-topmost", True)
    except Exception:
        pass

    selected = filedialog.askopenfilename(
        title="Selecione a planilha em PDF",
        initialdir=str(initial_dir),
        filetypes=[("Arquivos PDF", "*.pdf"), ("Todos os arquivos", "*.*")],
    )
    root.destroy()
    if not selected:
        return None
    return Path(selected).expanduser().resolve()


def choose_cdr_via_dialog(initial_dir: Path, title: str = "Selecione o template CDR") -> Optional[Path]:
    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception:
        return None

    root = tk.Tk()
    root.withdraw()
    try:
        root.attributes("-topmost", True)
    except Exception:
        pass

    selected = filedialog.askopenfilename(
        title=title,
        initialdir=str(initial_dir),
        filetypes=[("Arquivos CorelDRAW", "*.cdr"), ("Todos os arquivos", "*.*")],
    )
    root.destroy()
    if not selected:
        return None
    return Path(selected).expanduser().resolve()


def choose_directory_via_dialog(initial_dir: Path, title: str) -> Optional[Path]:
    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception:
        return None

    root = tk.Tk()
    root.withdraw()
    try:
        root.attributes("-topmost", True)
    except Exception:
        pass

    selected = filedialog.askdirectory(
        title=title,
        initialdir=str(initial_dir),
        mustexist=False,
    )
    root.destroy()
    if not selected:
        return None
    return Path(selected).expanduser().resolve()


def _code_guard_state_path() -> Path:
    return Path(__file__).resolve().with_name(CODE_GUARD_STATE_FILE)


def _list_protected_python_files() -> List[Path]:
    root = Path(__file__).resolve().parent
    protected: List[Path] = []
    for candidate in sorted(root.glob("*.py"), key=lambda p: p.name.lower()):
        if candidate.name.startswith("."):
            continue
        try:
            protected.append(candidate.resolve())
        except Exception:
            continue
    return protected


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _build_code_guard_snapshot(files: List[Path], root: Path) -> Dict[str, str]:
    snapshot: Dict[str, str] = {}
    for path in files:
        try:
            rel = path.relative_to(root).as_posix()
        except Exception:
            rel = path.name
        try:
            snapshot[rel] = _sha256_file(path)
        except Exception:
            continue
    return snapshot


def _load_code_guard_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _persist_code_guard_state(path: Path, snapshot: Dict[str, str]) -> None:
    payload = {
        "version": CODE_GUARD_STATE_VERSION,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "files": snapshot,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _detect_changed_code_files(expected: Dict[str, str], current: Dict[str, str]) -> List[str]:
    changed: List[str] = []
    for rel_path, current_hash in current.items():
        if str(expected.get(rel_path) or "") != str(current_hash):
            changed.append(rel_path)
    for rel_path in expected.keys():
        if rel_path not in current:
            changed.append(rel_path)
    return sorted(set(changed))


def _set_python_files_read_only(files: List[Path], read_only: bool) -> int:
    updated = 0
    for path in files:
        try:
            mode = path.stat().st_mode
            new_mode = (mode & ~stat.S_IWRITE) if read_only else (mode | stat.S_IWRITE)
            if new_mode != mode:
                os.chmod(path, new_mode)
            updated += 1
        except Exception:
            continue
    return updated


def _resolve_code_guard_password(explicit_password: str, prompt_if_missing: bool = False) -> str:
    direct = str(explicit_password or "")
    if direct:
        return direct
    env_password = str(os.environ.get(CODE_GUARD_PASSWORD_ENV, "") or "")
    if env_password:
        return env_password
    if prompt_if_missing and sys.stdin and sys.stdin.isatty():
        try:
            return str(getpass.getpass("Senha de protecao do codigo: ") or "")
        except Exception:
            return ""
    return ""


def _extract_code_guard_password_hash(payload: Any) -> str:
    if isinstance(payload, dict):
        hash_candidates = (
            payload.get("senha_hash_sha256"),
            payload.get("code_password_sha256"),
            payload.get("password_sha256"),
            payload.get("sha256"),
            payload.get("hash_sha256"),
            payload.get("senha_hash"),
            payload.get("password_hash"),
        )
        for raw_value in hash_candidates:
            candidate = str(raw_value or "").strip().lower()
            if candidate.startswith("sha256$"):
                candidate = candidate.split("$", 1)[1].strip().lower()
            if re.fullmatch(r"[0-9a-f]{64}", candidate):
                return candidate

        plain_candidates = (
            payload.get("senha"),
            payload.get("password"),
            payload.get("code_password"),
        )
        for raw_value in plain_candidates:
            plain_value = str(raw_value or "")
            if plain_value:
                return hashlib.sha256(plain_value.encode("utf-8")).hexdigest()
        return ""

    if isinstance(payload, list):
        for item in payload:
            candidate = _extract_code_guard_password_hash(item)
            if candidate:
                return candidate
        return ""

    if isinstance(payload, str):
        text_value = payload.strip()
        if not text_value:
            return ""
        if text_value.lower().startswith("sha256$"):
            text_value = text_value.split("$", 1)[1].strip()
        if re.fullmatch(r"[0-9a-fA-F]{64}", text_value):
            return text_value.lower()
        return hashlib.sha256(text_value.encode("utf-8")).hexdigest()

    return ""


def _parse_code_guard_password_hash_from_text(raw_text: str) -> str:
    text = str(raw_text or "")
    if not text.strip():
        return ""
    try:
        parsed = json.loads(text)
    except Exception:
        return _extract_code_guard_password_hash(text)
    return _extract_code_guard_password_hash(parsed)


def _load_code_guard_password_hash_from_remote() -> str:
    api_url = f"{_repo_contents_api_url(CODE_GUARD_REPO_FILE_PATH)}?ref={quote_plus(USERS_REPO_BRANCH)}"
    status, payload, _raw = _github_api_json_request("GET", api_url, token="")
    if status == 200 and isinstance(payload, dict):
        encoded = str(payload.get("content") or "").replace("\n", "").strip()
        encoding = str(payload.get("encoding") or "").strip().lower()
        if encoded and encoding == "base64":
            decoded = base64.b64decode(encoded).decode("utf-8", errors="ignore")
            extracted = _parse_code_guard_password_hash_from_text(decoded)
            if extracted:
                return extracted

    req = Request(
        CODE_GUARD_REMOTE_RAW_URL,
        headers={
            "User-Agent": "PlacasAutA4/1.0",
            "Accept": "application/json",
        },
    )
    with urlopen(req, timeout=15.0) as resp:
        body = resp.read().decode("utf-8", errors="ignore")
    return _parse_code_guard_password_hash_from_text(body)


def _get_code_guard_expected_password_hash() -> str:
    global _CODE_GUARD_PASSWORD_SHA256_CACHE, _CODE_GUARD_PASSWORD_CACHE_LOADED, _CODE_GUARD_PASSWORD_CACHE_ERROR
    if _CODE_GUARD_PASSWORD_CACHE_LOADED:
        return _CODE_GUARD_PASSWORD_SHA256_CACHE

    _CODE_GUARD_PASSWORD_CACHE_LOADED = True
    _CODE_GUARD_PASSWORD_CACHE_ERROR = ""
    try:
        extracted = _load_code_guard_password_hash_from_remote()
    except Exception as exc:
        _CODE_GUARD_PASSWORD_SHA256_CACHE = ""
        _CODE_GUARD_PASSWORD_CACHE_ERROR = (
            f"Nao foi possivel carregar a senha remota em {CODE_GUARD_REMOTE_RAW_URL}: {exc}"
        )
        return ""

    _CODE_GUARD_PASSWORD_SHA256_CACHE = str(extracted or "").strip().lower()
    if not _CODE_GUARD_PASSWORD_SHA256_CACHE:
        _CODE_GUARD_PASSWORD_CACHE_ERROR = (
            f"Senha remota nao configurada em {CODE_GUARD_REMOTE_RAW_URL}. "
            "Preencha o JSON com 'senha_hash_sha256' (hash SHA-256)."
        )
    return _CODE_GUARD_PASSWORD_SHA256_CACHE


def _code_guard_password_load_error() -> str:
    return str(_CODE_GUARD_PASSWORD_CACHE_ERROR or "").strip()


def _is_valid_code_guard_password(password: str) -> bool:
    if not password:
        return False
    expected_hash = _get_code_guard_expected_password_hash()
    if not expected_hash:
        return False
    provided_hash = hashlib.sha256(str(password).encode("utf-8")).hexdigest()
    return hmac.compare_digest(provided_hash, expected_hash)


def _handle_code_guard(args: argparse.Namespace) -> Optional[int]:
    protected_files = _list_protected_python_files()
    if not protected_files:
        return None

    code_root = Path(__file__).resolve().parent
    state_path = _code_guard_state_path()
    explicit_password = str(getattr(args, "code_password", "") or "")
    unlock_only = bool(getattr(args, "unlock_code", False))
    lock_only = bool(getattr(args, "lock_code", False))
    authorize_changes = bool(getattr(args, "authorize_code_change", False))

    if unlock_only:
        password = _resolve_code_guard_password(explicit_password, prompt_if_missing=True)
        if not _is_valid_code_guard_password(password):
            load_error = _code_guard_password_load_error()
            if load_error:
                print(load_error, file=sys.stderr)
            else:
                print("Senha incorreta. O codigo .py continua protegido.", file=sys.stderr)
            return 7
        unlocked_count = _set_python_files_read_only(protected_files, read_only=False)
        print_status("CACHE", f"Protecao removida para edicao em {unlocked_count} arquivo(s) .py.")
        return 0

    current_snapshot = _build_code_guard_snapshot(protected_files, code_root)
    state_payload = _load_code_guard_state(state_path)
    expected_snapshot = state_payload.get("files") if isinstance(state_payload, dict) else {}
    if not isinstance(expected_snapshot, dict):
        expected_snapshot = {}

    if not expected_snapshot:
        _persist_code_guard_state(state_path, current_snapshot)
        expected_snapshot = dict(current_snapshot)

    changed_files = _detect_changed_code_files(expected_snapshot, current_snapshot)
    if changed_files:
        if not authorize_changes:
            print(
                "Alteracao no codigo detectada e bloqueada. "
                "Use --authorize-code-change com a senha para liberar a nova versao.",
                file=sys.stderr,
            )
            for rel_path in changed_files:
                print(f" - {rel_path}", file=sys.stderr)
            return 8
        password = _resolve_code_guard_password(explicit_password, prompt_if_missing=True)
        if not _is_valid_code_guard_password(password):
            load_error = _code_guard_password_load_error()
            if load_error:
                print(load_error, file=sys.stderr)
            else:
                print("Senha incorreta. Alteracoes no codigo nao foram autorizadas.", file=sys.stderr)
            return 7
        _persist_code_guard_state(state_path, current_snapshot)
        print_status("CACHE", f"Alteracao autorizada em {len(changed_files)} arquivo(s) .py.")
    elif authorize_changes:
        password = _resolve_code_guard_password(explicit_password, prompt_if_missing=True)
        if not _is_valid_code_guard_password(password):
            load_error = _code_guard_password_load_error()
            if load_error:
                print(load_error, file=sys.stderr)
            else:
                print("Senha incorreta. Nao foi possivel revalidar a integridade do codigo.", file=sys.stderr)
            return 7
        _persist_code_guard_state(state_path, current_snapshot)
        print_status("CACHE", "Integridade do codigo revalidada com senha.")

    locked_count = _set_python_files_read_only(protected_files, read_only=True)
    if lock_only:
        print_status("CACHE", f"Codigo bloqueado em modo somente leitura ({locked_count} arquivo(s)).")
        return 0

    return None


def normalize_key(value: str) -> str:
    value = (value or "").strip().lower()
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_")


def normalize_user_role(value: str, default: str = "Gerador de Placas") -> str:
    normalized = normalize_key(value).upper()
    if normalized in {"DEV", "DESENVOLVEDOR", "DESENVOLVEDORA", "DEVELOPER"}:
        return ROLE_DEV
    if normalized in {"ADMIN", "ADMINISTRADOR", "ADMINISTRADORA"}:
        return ROLE_ADMIN
    if normalized in {
        "GERADOR_DE_PLACAS",
        "GERADOR_PLACAS",
        "GERADOR",
        "OPERADOR",
        "USUARIO",
        "USUARIO_COMUM",
    }:
        return ROLE_PLATE_GENERATOR
    return default


def get_user_role_permissions(role_value: str) -> Dict[str, bool]:
    normalized_role = normalize_user_role(role_value)
    return dict(ROLE_PERMISSIONS.get(normalized_role, ROLE_PERMISSIONS[ROLE_PLATE_GENERATOR]))


def _users_cache_path() -> Path:
    return Path(__file__).resolve().with_name(USERS_CACHE_FILE)


def _normalize_auth_username(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _normalize_auth_email(value: str) -> str:
    return str(value or "").strip().lower()


def _to_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if value is None:
        return default
    text = str(value).strip().lower()
    if not text:
        return default
    if text in {"1", "true", "sim", "yes", "on"}:
        return True
    if text in {"0", "false", "nao", "não", "no", "off"}:
        return False
    return default


def _sanitize_user_record(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(raw, dict):
        return None
    username = _normalize_auth_username(raw.get("usuario") or raw.get("username") or raw.get("login") or "")
    password_hash = str(raw.get("senha_hash") or raw.get("password_hash") or "").strip()
    if not username or not password_hash:
        return None
    profile = normalize_user_role(str(raw.get("perfil") or raw.get("role") or ""))
    email = _normalize_auth_email(raw.get("email") or "")
    phone = re.sub(r"\D+", "", str(raw.get("telefone") or raw.get("phone") or ""))
    full_name = _normalize_auth_username(raw.get("nome") or raw.get("name") or username)
    return {
        "usuario": username,
        "senha_hash": password_hash,
        "nome": full_name or username,
        "perfil": profile,
        "ativo": _to_bool(raw.get("ativo"), default=True),
        "email": email,
        "telefone": phone,
    }


def _sanitize_users_payload(payload: Any) -> List[Dict[str, Any]]:
    entries = payload
    if isinstance(payload, dict):
        entries = payload.get("usuarios")
    if not isinstance(entries, list):
        return []
    users: List[Dict[str, Any]] = []
    for raw in entries:
        if not isinstance(raw, dict):
            continue
        sanitized = _sanitize_user_record(raw)
        if sanitized:
            users.append(sanitized)
    return users


def _load_users_from_remote() -> List[Dict[str, Any]]:
    api_url = f"{_users_github_api_url()}?ref={quote_plus(USERS_REPO_BRANCH)}"
    status, payload, _raw = _github_api_json_request("GET", api_url, token="")
    if status == 200 and isinstance(payload, dict):
        encoded = str(payload.get("content") or "").replace("\n", "").strip()
        encoding = str(payload.get("encoding") or "").strip().lower()
        if encoded and encoding == "base64":
            decoded = base64.b64decode(encoded).decode("utf-8", errors="ignore")
            if decoded.strip():
                parsed = json.loads(decoded)
                return _sanitize_users_payload(parsed)

    req = Request(
        USERS_REMOTE_RAW_URL,
        headers={
            "User-Agent": "PlacasAutA4/1.0",
            "Accept": "application/json",
        },
    )
    with urlopen(req, timeout=15.0) as resp:
        body = resp.read().decode("utf-8", errors="ignore")
    parsed = json.loads(body)
    return _sanitize_users_payload(parsed)


def _load_users_from_local_cache(cache_path: Path) -> List[Dict[str, Any]]:
    if not cache_path.exists():
        return []
    try:
        raw = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    return _sanitize_users_payload(raw)


def _save_users_to_local_cache(cache_path: Path, users: List[Dict[str, Any]]) -> None:
    payload = [dict(user) for user in users]
    temp_path = cache_path.with_suffix(".tmp")
    try:
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temp_path.replace(cache_path)
    finally:
        try:
            if temp_path.exists():
                temp_path.unlink()
        except Exception:
            pass


def _resolve_users_github_token() -> str:
    for env_name in USERS_GITHUB_TOKEN_ENV_CANDIDATES:
        token = str(os.environ.get(env_name, "") or "").strip()
        if token:
            return token
    return ""


def _repo_contents_api_url(file_path: str) -> str:
    clean = "/".join(str(part or "").strip() for part in str(file_path or "").split("/") if str(part or "").strip())
    encoded = quote(clean, safe="/")
    return f"https://api.github.com/repos/{USERS_REPO_OWNER}/{USERS_REPO_NAME}/contents/{encoded}"


def _users_github_api_url() -> str:
    return _repo_contents_api_url(USERS_REPO_FILE_PATH)


def _github_api_json_request(
    method: str,
    url: str,
    token: str,
    payload: Optional[Dict[str, Any]] = None,
    timeout_seconds: float = 20.0,
) -> tuple[int, Dict[str, Any], str]:
    headers = {
        "User-Agent": "PlacasAutA4/1.0",
        "Accept": "application/vnd.github+json",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    data: Optional[bytes] = None
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json; charset=utf-8"
    req = Request(url, data=data, headers=headers, method=method.upper())
    try:
        with urlopen(req, timeout=max(5.0, float(timeout_seconds))) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
            status = int(getattr(resp, "status", 200) or 200)
    except HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="ignore")
        status = int(getattr(exc, "code", 500) or 500)
    except Exception as exc:
        return 0, {}, str(exc)

    parsed: Dict[str, Any] = {}
    if raw:
        try:
            loaded = json.loads(raw)
            if isinstance(loaded, dict):
                parsed = loaded
        except Exception:
            parsed = {}
    return status, parsed, raw


def _push_users_to_remote_repository(users: List[Dict[str, Any]], actor_name: str = "sistema") -> tuple[bool, str]:
    token = _resolve_users_github_token()
    if not token:
        return False, (
            "Sem token GitHub. Defina a variavel de ambiente "
            "GITHUB_PLACAS_TOKEN (ou PLACA_GITHUB_TOKEN / GITHUB_TOKEN) para sincronizar no repositorio."
        )

    api_url = _users_github_api_url()
    lookup_url = f"{api_url}?ref={quote_plus(USERS_REPO_BRANCH)}"
    status_get, payload_get, raw_get = _github_api_json_request("GET", lookup_url, token=token)
    remote_sha = ""
    if status_get == 200:
        remote_sha = str(payload_get.get("sha") or "")
    elif status_get != 404:
        remote_message = str(payload_get.get("message") or raw_get or "Falha ao consultar arquivo remoto.")
        return False, f"GitHub API retornou {status_get}: {remote_message}"

    content_text = json.dumps(users, ensure_ascii=False, indent=2) + "\n"
    commit_payload: Dict[str, Any] = {
        "message": f"Atualiza usuarios via painel ({sanitize_filename(actor_name, 'sistema')})",
        "content": base64.b64encode(content_text.encode("utf-8")).decode("ascii"),
        "branch": USERS_REPO_BRANCH,
    }
    if remote_sha:
        commit_payload["sha"] = remote_sha

    status_put, payload_put, raw_put = _github_api_json_request("PUT", api_url, token=token, payload=commit_payload)
    if status_put in {200, 201}:
        return True, "Usuarios sincronizados no repositorio remoto."
    remote_error = str(payload_put.get("message") or raw_put or "Falha ao salvar usuarios no repositorio.")
    return False, f"GitHub API retornou {status_put}: {remote_error}"


def _get_plate_audit_target(plate_format: str) -> Dict[str, str]:
    normalized_format = normalize_plate_format(str(plate_format or "A4"), default="A4")
    return dict(PLATE_AUDIT_TARGETS.get(normalized_format) or PLATE_AUDIT_TARGETS["A4"])


def _plate_audit_cache_path(plate_format: str) -> Path:
    target = _get_plate_audit_target(plate_format)
    return Path(__file__).resolve().with_name(str(target["cache_file"]))


def _legacy_plate_audit_minimal_entry(
    raw_entry: Dict[str, Any],
    product_name: str,
    price_value: str,
    offer_validity_until: str,
) -> Dict[str, str]:
    registered_at = re.sub(r"\s+", " ", str(raw_entry.get("registrado_em") or "").strip())
    date_part = ""
    time_part = ""
    if registered_at:
        chunks = registered_at.split(" ", 1)
        date_part = str(chunks[0] or "").strip()
        if len(chunks) > 1:
            time_part = str(chunks[1] or "").strip()
    actor_data = raw_entry.get("usuario") if isinstance(raw_entry.get("usuario"), dict) else {}
    actor_name = re.sub(
        r"\s+",
        " ",
        str(
            actor_data.get("nome")
            or actor_data.get("usuario")
            or raw_entry.get("feito_por")
            or "Desconhecido"
        ).strip(),
    )
    return {
        "data": date_part,
        "horario": time_part,
        "feito_por": actor_name,
        "produto": re.sub(r"\s+", " ", str(product_name or "").strip()),
        "preco": re.sub(r"\s+", " ", str(price_value or "").strip()),
        "validade_oferta_ate": re.sub(r"\s+", " ", str(offer_validity_until or "").strip()),
    }


def _normalize_plate_audit_entries_for_format(entries: List[Dict[str, Any]], plate_format: str) -> List[Dict[str, str]]:
    normalized_format = normalize_plate_format(str(plate_format or "A4"), default="A4")
    normalized_entries: List[Dict[str, str]] = []
    for raw_entry in entries or []:
        if not isinstance(raw_entry, dict):
            continue
        if "produto" in raw_entry and "preco" in raw_entry:
            normalized_entries.append(
                {
                    "data": re.sub(r"\s+", " ", str(raw_entry.get("data") or "").strip()),
                    "horario": re.sub(r"\s+", " ", str(raw_entry.get("horario") or "").strip()),
                    "feito_por": re.sub(r"\s+", " ", str(raw_entry.get("feito_por") or "").strip()),
                    "produto": re.sub(r"\s+", " ", str(raw_entry.get("produto") or "").strip()),
                    "preco": re.sub(r"\s+", " ", str(raw_entry.get("preco") or "").strip()),
                    "validade_oferta_ate": re.sub(
                        r"\s+",
                        " ",
                        str(
                            raw_entry.get("validade_oferta_ate")
                            or raw_entry.get("validade_oferta")
                            or ""
                        ).strip(),
                    ),
                }
            )
            continue

        per_plate = raw_entry.get("placas")
        if not isinstance(per_plate, list):
            continue
        for plate_data in per_plate:
            if not isinstance(plate_data, dict):
                continue
            quantities = plate_data.get("quantidades") if isinstance(plate_data.get("quantidades"), dict) else {}
            qty_for_format = int(quantities.get(normalized_format, 0) or 0)
            if qty_for_format <= 0:
                continue
            minimal_entry = _legacy_plate_audit_minimal_entry(
                raw_entry,
                str(plate_data.get("nome") or plate_data.get("nome_original") or ""),
                str(plate_data.get("preco") or ""),
                "",
            )
            for _ in range(qty_for_format):
                normalized_entries.append(dict(minimal_entry))
    return normalized_entries


def _merge_plate_audit_entries(*groups: List[Dict[str, str]]) -> List[Dict[str, str]]:
    merged: List[Dict[str, str]] = []
    seen_signatures: set[tuple[str, str, str, str, str, str]] = set()
    for group in groups:
        for entry in group or []:
            if not isinstance(entry, dict):
                continue
            normalized_entry = {
                "data": re.sub(r"\s+", " ", str(entry.get("data") or "").strip()),
                "horario": re.sub(r"\s+", " ", str(entry.get("horario") or "").strip()),
                "feito_por": re.sub(r"\s+", " ", str(entry.get("feito_por") or "").strip()),
                "produto": re.sub(r"\s+", " ", str(entry.get("produto") or "").strip()),
                "preco": re.sub(r"\s+", " ", str(entry.get("preco") or "").strip()),
                "validade_oferta_ate": re.sub(r"\s+", " ", str(entry.get("validade_oferta_ate") or "").strip()),
            }
            signature = (
                normalized_entry["data"],
                normalized_entry["horario"],
                normalized_entry["feito_por"],
                normalized_entry["produto"],
                normalized_entry["preco"],
                normalized_entry["validade_oferta_ate"],
            )
            if signature in seen_signatures:
                continue
            seen_signatures.add(signature)
            merged.append(normalized_entry)
    return merged


def _load_plate_audit_from_remote(plate_format: str) -> List[Dict[str, Any]]:
    target = _get_plate_audit_target(plate_format)
    api_url = f"{_repo_contents_api_url(str(target['repo_file_path']))}?ref={quote_plus(USERS_REPO_BRANCH)}"
    status, payload, _raw = _github_api_json_request("GET", api_url, token="")
    if status == 200 and isinstance(payload, dict):
        encoded = str(payload.get("content") or "").replace("\n", "").strip()
        encoding = str(payload.get("encoding") or "").strip().lower()
        if encoded and encoding == "base64":
            decoded = base64.b64decode(encoded).decode("utf-8", errors="ignore")
            if not decoded.strip():
                return []
            parsed = json.loads(decoded)
            if isinstance(parsed, list):
                return [dict(item) for item in parsed if isinstance(item, dict)]
            return []

    req = Request(
        str(target["remote_raw_url"]),
        headers={
            "User-Agent": "PlacasAutA4/1.0",
            "Accept": "application/json",
        },
    )
    try:
        with urlopen(req, timeout=15.0) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
    except HTTPError as http_exc:
        if int(http_exc.code) == 404:
            return []
        raise
    if not body.strip():
        return []
    parsed = json.loads(body)
    if isinstance(parsed, list):
        return [dict(item) for item in parsed if isinstance(item, dict)]
    return []


def _load_plate_audit_from_local_cache(cache_path: Path) -> List[Dict[str, Any]]:
    if not cache_path.exists():
        return []
    try:
        raw = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(raw, list):
        return [dict(item) for item in raw if isinstance(item, dict)]
    return []


def _save_plate_audit_to_local_cache(cache_path: Path, entries: List[Dict[str, Any]]) -> None:
    payload = [dict(entry) for entry in entries if isinstance(entry, dict)]
    temp_path = cache_path.with_suffix(".tmp")
    try:
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temp_path.replace(cache_path)
    finally:
        try:
            if temp_path.exists():
                temp_path.unlink()
        except Exception:
            pass


def load_plate_audit_history(plate_format: str) -> tuple[List[Dict[str, str]], str]:
    normalized_format = normalize_plate_format(str(plate_format or "A4"), default="A4")
    cache_path = _plate_audit_cache_path(normalized_format)
    cached_entries = _normalize_plate_audit_entries_for_format(
        _load_plate_audit_from_local_cache(cache_path),
        normalized_format,
    )
    if normalized_format in {"A5", "A6"}:
        cached_entries = _merge_plate_audit_entries(
            cached_entries,
            _normalize_plate_audit_entries_for_format(
                _load_plate_audit_from_local_cache(_plate_audit_cache_path("A4")),
                normalized_format,
            ),
        )
    remote_loaded = False
    try:
        remote_entries = _normalize_plate_audit_entries_for_format(
            _load_plate_audit_from_remote(normalized_format),
            normalized_format,
        )
        remote_loaded = True
        if normalized_format in {"A5", "A6"}:
            remote_entries = _merge_plate_audit_entries(
                remote_entries,
                _normalize_plate_audit_entries_for_format(_load_plate_audit_from_remote("A4"), normalized_format),
            )
        merged_entries = _merge_plate_audit_entries(remote_entries, cached_entries)
        if merged_entries:
            _save_plate_audit_to_local_cache(cache_path, merged_entries)
            return merged_entries, "remote" if remote_loaded else "cache"
    except Exception:
        pass
    if cached_entries:
        return cached_entries, "cache"
    return [], "empty"


def _push_plate_audit_to_remote(
    plate_format: str,
    entries: List[Dict[str, Any]],
    actor_name: str = "sistema",
) -> tuple[bool, str]:
    token = _resolve_users_github_token()
    if not token:
        return False, (
            "Sem token GitHub. Defina a variavel de ambiente "
            "GITHUB_PLACAS_TOKEN (ou PLACA_GITHUB_TOKEN / GITHUB_TOKEN) para sincronizar no repositorio."
        )

    target = _get_plate_audit_target(plate_format)
    api_url = _repo_contents_api_url(str(target["repo_file_path"]))
    lookup_url = f"{api_url}?ref={quote_plus(USERS_REPO_BRANCH)}"
    status_get, payload_get, raw_get = _github_api_json_request("GET", lookup_url, token=token)
    remote_sha = ""
    if status_get == 200:
        remote_sha = str(payload_get.get("sha") or "")
    elif status_get != 404:
        remote_message = str(payload_get.get("message") or raw_get or "Falha ao consultar arquivo remoto.")
        return False, f"GitHub API retornou {status_get}: {remote_message}"

    content_text = json.dumps(entries, ensure_ascii=False, indent=2) + "\n"
    commit_payload: Dict[str, Any] = {
        "message": (
            f"Auditoria de placas {normalize_plate_format(str(plate_format or 'A4'), default='A4')} "
            f"({sanitize_filename(actor_name, 'sistema')})"
        ),
        "content": base64.b64encode(content_text.encode("utf-8")).decode("ascii"),
        "branch": USERS_REPO_BRANCH,
    }
    if remote_sha:
        commit_payload["sha"] = remote_sha

    status_put, payload_put, raw_put = _github_api_json_request("PUT", api_url, token=token, payload=commit_payload)
    if status_put in {200, 201}:
        return True, (
            "Auditoria de placas sincronizada no repositorio remoto "
            f"({target['repo_file_path']})."
        )
    remote_error = str(payload_put.get("message") or raw_put or "Falha ao salvar auditoria de placas no repositorio.")
    return False, f"GitHub API retornou {status_put}: {remote_error}"


def persist_plate_audit_history(
    plate_format: str,
    entries: List[Dict[str, Any]],
    actor_name: str = "sistema",
) -> Dict[str, Any]:
    result: Dict[str, Any] = {"ok": False, "saved_local": False, "saved_remote": False, "message": ""}
    cache_path = _plate_audit_cache_path(plate_format)
    try:
        _save_plate_audit_to_local_cache(cache_path, entries)
        result["saved_local"] = True
    except Exception as exc:
        result["message"] = f"Falha ao salvar cache local da auditoria: {exc}"
        return result

    synced, remote_message = _push_plate_audit_to_remote(plate_format, entries, actor_name=actor_name)
    result["saved_remote"] = bool(synced)
    result["ok"] = True
    result["message"] = remote_message or (
        "Auditoria salva localmente." if result["saved_local"] else "Falha ao salvar auditoria."
    )
    return result


def load_users_database() -> tuple[List[Dict[str, Any]], str]:
    cache_path = _users_cache_path()
    try:
        remote_users = _load_users_from_remote()
        if remote_users:
            _save_users_to_local_cache(cache_path, remote_users)
            return remote_users, "remote"
    except Exception:
        pass

    cached_users = _load_users_from_local_cache(cache_path)
    if cached_users:
        return cached_users, "cache"
    return [], "empty"


def persist_users_database(users: List[Dict[str, Any]], actor_name: str = "sistema") -> Dict[str, Any]:
    result: Dict[str, Any] = {"ok": False, "saved_local": False, "saved_remote": False, "message": ""}
    cache_path = _users_cache_path()
    try:
        _save_users_to_local_cache(cache_path, users)
        result["saved_local"] = True
    except Exception as exc:
        result["message"] = f"Falha ao salvar cache local de usuarios: {exc}"
        return result

    synced, remote_message = _push_users_to_remote_repository(users, actor_name=actor_name)
    result["saved_remote"] = bool(synced)
    result["ok"] = True
    result["message"] = remote_message or (
        "Usuarios salvos localmente." if result["saved_local"] else "Falha ao salvar usuarios."
    )
    return result


def _b64_urlsafe_sem_padding(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _b64_urlsafe_para_bytes(data: str) -> bytes:
    value = str(data or "").strip()
    if not value:
        raise ValueError("base64 vazio")
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("ascii"))


def hash_user_password(password: str) -> str:
    safe_password = str(password or "")
    if not safe_password:
        raise ValueError("Senha vazia nao e permitida.")
    iterations = int(USER_PASSWORD_PBKDF2_ITERATIONS)
    if iterations < PASSWORD_HASH_MIN_ITERATIONS:
        raise ValueError(f"Iteracoes insuficientes ({iterations}).")
    salt_bytes = os.urandom(16)
    salt = _b64_urlsafe_sem_padding(salt_bytes)
    digest = hashlib.pbkdf2_hmac("sha256", safe_password.encode("utf-8"), salt_bytes, iterations)
    signature = _b64_urlsafe_sem_padding(digest)
    return f"{PASSWORD_HASH_SCHEME}${iterations}${salt}${signature}"


def verify_user_password(password: str, stored_hash: str) -> bool:
    raw_hash = str(stored_hash or "").strip()
    candidate_password = str(password or "")
    if not raw_hash or not candidate_password:
        return False

    if raw_hash.startswith(f"{PASSWORD_HASH_SCHEME}$"):
        parts = raw_hash.split("$", 3)
        if len(parts) != 4:
            return False
        _, raw_iterations, salt_txt, signature_txt = parts
        try:
            iterations = int(raw_iterations)
            if iterations < PASSWORD_HASH_MIN_ITERATIONS:
                return False
            salt = _b64_urlsafe_para_bytes(salt_txt)
            signature_expected = _b64_urlsafe_para_bytes(signature_txt)
            signature_current = hashlib.pbkdf2_hmac(
                "sha256",
                candidate_password.encode("utf-8"),
                salt,
                iterations,
            )
            if hmac.compare_digest(signature_current, signature_expected):
                return True
        except Exception:
            # Compatibilidade com hashes antigos que usavam salt textual direto.
            pass

        try:
            iterations = max(100000, int(raw_iterations))
        except Exception:
            return False
        digest = hashlib.pbkdf2_hmac(
            "sha256",
            candidate_password.encode("utf-8"),
            str(salt_txt).encode("utf-8"),
            iterations,
        )
        expected_clean = str(signature_txt or "").strip()
        expected_no_pad = expected_clean.rstrip("=")
        candidates = {
            base64.b64encode(digest).decode("ascii"),
            base64.b64encode(digest).decode("ascii").rstrip("="),
            base64.urlsafe_b64encode(digest).decode("ascii"),
            base64.urlsafe_b64encode(digest).decode("ascii").rstrip("="),
        }
        for candidate in candidates:
            if hmac.compare_digest(candidate, expected_clean):
                return True
            if hmac.compare_digest(candidate.rstrip("="), expected_no_pad):
                return True
        return False

    if raw_hash.startswith("sha256$"):
        provided_hash = hashlib.sha256(candidate_password.encode("utf-8")).hexdigest()
        return hmac.compare_digest(provided_hash, raw_hash.split("$", 1)[1].strip())

    if raw_hash.startswith("plain$"):
        return hmac.compare_digest(candidate_password, raw_hash.split("$", 1)[1])

    return hmac.compare_digest(candidate_password, raw_hash)


def find_active_user_by_identifier(users: List[Dict[str, Any]], identifier: str) -> Optional[Dict[str, Any]]:
    clean_identifier = str(identifier or "").strip()
    if not clean_identifier:
        return None
    norm_user = normalize_key(clean_identifier)
    norm_email = _normalize_auth_email(clean_identifier)
    norm_phone = re.sub(r"\D+", "", clean_identifier)
    for user in users:
        if not _to_bool(user.get("ativo"), default=True):
            continue
        user_name = normalize_key(str(user.get("usuario") or ""))
        user_email = _normalize_auth_email(user.get("email") or "")
        user_phone = re.sub(r"\D+", "", str(user.get("telefone") or ""))
        if norm_user and user_name and user_name == norm_user:
            return user
        if norm_email and user_email and user_email == norm_email:
            return user
        if norm_phone and user_phone and user_phone == norm_phone:
            return user
    return None


def build_public_user_payload(user: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(user, dict):
        return None
    role = normalize_user_role(str(user.get("perfil") or ""))
    return {
        "usuario": _normalize_auth_username(user.get("usuario") or ""),
        "nome": _normalize_auth_username(user.get("nome") or user.get("usuario") or ""),
        "email": _normalize_auth_email(user.get("email") or ""),
        "telefone": re.sub(r"\D+", "", str(user.get("telefone") or "")),
        "perfil": role,
        "ativo": _to_bool(user.get("ativo"), default=True),
        "sessao_24h": _to_bool(user.get("sessao_24h"), default=False),
        "sessao_expira_em": str(user.get("sessao_expira_em") or "").strip(),
    }


def _password_reset_requests_path() -> Path:
    return Path(__file__).resolve().with_name(PASSWORD_RESET_REQUESTS_FILE)


def _load_password_reset_requests(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(raw, list):
        return [dict(item) for item in raw if isinstance(item, dict)]
    return []


def _persist_password_reset_requests(path: Path, entries: List[Dict[str, Any]]) -> None:
    payload = [dict(entry) for entry in entries if isinstance(entry, dict)]
    temp_path = path.with_suffix(".tmp")
    try:
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temp_path.replace(path)
    finally:
        try:
            if temp_path.exists():
                temp_path.unlink()
        except Exception:
            pass


def register_password_reset_request(identifier: str, resolved_user: Optional[Dict[str, Any]], ip_address: str = "") -> None:
    clean_identifier = re.sub(r"\s+", " ", str(identifier or "").strip())
    if not clean_identifier:
        return
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = {
        "requested_at": now_str,
        "identifier": clean_identifier,
        "ip": str(ip_address or ""),
        "usuario_encontrado": _normalize_auth_username((resolved_user or {}).get("usuario") or ""),
        "email_encontrado": _normalize_auth_email((resolved_user or {}).get("email") or ""),
        "status": "pendente",
    }
    path = _password_reset_requests_path()
    existing = _load_password_reset_requests(path)
    existing.append(entry)
    _persist_password_reset_requests(path, existing)


def _login_session_path() -> Path:
    return Path(__file__).resolve().with_name(LOGIN_SESSION_FILE)


def save_login_session_24h(user: Dict[str, Any]) -> None:
    now_dt = datetime.now().astimezone()
    expire_dt = now_dt + timedelta(hours=LOGIN_SESSION_HOURS)
    payload = {
        "usuario": _normalize_auth_username(user.get("usuario") or ""),
        "nome": _normalize_auth_username(user.get("nome") or user.get("usuario") or ""),
        "perfil": normalize_user_role(str(user.get("perfil") or "")),
        "lembrar_ate": expire_dt.isoformat(timespec="seconds"),
        "registrado_em": now_dt.isoformat(timespec="seconds"),
    }
    _login_session_path().write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def clear_login_session_24h() -> None:
    path = _login_session_path()
    try:
        if path.exists():
            path.unlink()
    except Exception:
        pass


def load_login_session_24h_valid(users: List[Dict[str, Any]]) -> tuple[Optional[Dict[str, Any]], int]:
    path = _login_session_path()
    if not path.exists():
        return None, 0
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        clear_login_session_24h()
        return None, 0
    if not isinstance(raw, dict):
        clear_login_session_24h()
        return None, 0

    saved_user = normalize_key(str(raw.get("usuario") or ""))
    remember_until_txt = str(raw.get("lembrar_ate") or "").strip()
    if not saved_user or not remember_until_txt:
        clear_login_session_24h()
        return None, 0
    try:
        remember_until = datetime.fromisoformat(remember_until_txt)
    except Exception:
        clear_login_session_24h()
        return None, 0

    now_dt = datetime.now().astimezone()
    if remember_until.tzinfo is None:
        remember_until = remember_until.replace(tzinfo=now_dt.tzinfo)
    remaining_seconds = int((remember_until - now_dt).total_seconds())
    if remaining_seconds <= 0:
        clear_login_session_24h()
        return None, 0

    for user in users:
        if not _to_bool(user.get("ativo"), default=True):
            continue
        if normalize_key(str(user.get("usuario") or "")) == saved_user:
            user_data = dict(user)
            user_data["sessao_24h"] = True
            user_data["sessao_expira_em"] = remember_until.isoformat(timespec="seconds")
            return user_data, remaining_seconds

    clear_login_session_24h()
    return None, 0


def _resolve_generation_actor_from_items(items: List[PlateData]) -> Dict[str, str]:
    for item in items or []:
        row = item.row if isinstance(item.row, dict) else {}
        user_value = _normalize_auth_username(row.get("_audit_generated_by_user") or "")
        if not user_value:
            continue
        return {
            "usuario": user_value,
            "nome": _normalize_auth_username(row.get("_audit_generated_by_name") or user_value),
            "email": _normalize_auth_email(row.get("_audit_generated_by_email") or ""),
            "perfil": normalize_user_role(str(row.get("_audit_generated_by_role") or "")),
            "selecionado_em": re.sub(r"\s+", " ", str(row.get("_audit_selected_at") or "").strip()),
        }
    return {
        "usuario": "desconhecido",
        "nome": "Desconhecido",
        "email": "",
        "perfil": ROLE_PLATE_GENERATOR,
        "selecionado_em": "",
    }


def _resolve_plate_generation_offer_validity(item: PlateData, today: Optional[date] = None) -> str:
    if not bool(item.offer_validity_enabled):
        return ""
    ref_date = today or date.today()
    safe_day, safe_month, safe_year = _resolve_offer_validity_date(
        int(item.offer_validity_day or ref_date.day),
        today=ref_date,
        month_value=item.offer_validity_month,
    )
    return f"{safe_day:02d}/{safe_month:02d}/{safe_year:04d}"


def _build_plate_generation_audit_entries(
    items: List[PlateData],
    generated_output_records: List[OutputPdfRecord],
    failed_jobs: List[str],
    output_pdf_dir: Path,
) -> Dict[str, List[Dict[str, str]]]:
    now_dt = datetime.now()
    actor = _resolve_generation_actor_from_items(items)
    actor_name = re.sub(
        r"\s+",
        " ",
        str(actor.get("nome") or actor.get("usuario") or "Desconhecido").strip(),
    )
    created_date = now_dt.strftime("%Y-%m-%d")
    created_time = now_dt.strftime("%H:%M:%S")
    entries_by_format: Dict[str, List[Dict[str, str]]] = {"A4": [], "A5": [], "A6": []}
    for item in items:
        normalized_quantities = normalize_format_quantities(
            item.format_quantities,
            default_format=item.plate_format,
            default_qty=item.quantity,
        )
        product_name = re.sub(r"\s+", " ", str(item.name or item.original_name or "").strip())
        price_value = re.sub(r"\s+", " ", str(item.price or "").strip())
        offer_validity_until = _resolve_plate_generation_offer_validity(item, today=now_dt.date())
        for plate_format in ("A4", "A5", "A6"):
            qty = max(0, int(normalized_quantities.get(plate_format, 0) or 0))
            if qty <= 0:
                continue
            for _ in range(qty):
                entries_by_format[plate_format].append(
                    {
                        "data": created_date,
                        "horario": created_time,
                        "feito_por": actor_name,
                        "produto": product_name,
                        "preco": price_value,
                        "validade_oferta_ate": offer_validity_until,
                    }
                )
    return entries_by_format


def save_plate_generation_audit(
    items: List[PlateData],
    generated_output_records: List[OutputPdfRecord],
    failed_jobs: List[str],
    output_pdf_dir: Path,
) -> Dict[str, Any]:
    entries_by_format = _build_plate_generation_audit_entries(
        items=items,
        generated_output_records=generated_output_records,
        failed_jobs=failed_jobs,
        output_pdf_dir=output_pdf_dir,
    )
    actor_name = str(_resolve_generation_actor_from_items(items).get("usuario") or "sistema")
    persist_details: Dict[str, Dict[str, Any]] = {}
    sources: Dict[str, str] = {}
    overall_ok = True
    overall_saved_local = False
    overall_saved_remote = False
    messages: List[str] = []
    for plate_format in ("A4", "A5", "A6"):
        new_entries = list(entries_by_format.get(plate_format) or [])
        if not new_entries:
            continue
        existing_entries, source = load_plate_audit_history(plate_format)
        sources[plate_format] = source
        merged_entries = _merge_plate_audit_entries(existing_entries, new_entries)
        persist_result = persist_plate_audit_history(
            plate_format,
            merged_entries,
            actor_name=actor_name,
        )
        persist_details[plate_format] = persist_result
        overall_ok = overall_ok and bool(persist_result.get("ok"))
        overall_saved_local = overall_saved_local or bool(persist_result.get("saved_local"))
        overall_saved_remote = overall_saved_remote or bool(persist_result.get("saved_remote"))
        result_message = str(persist_result.get("message") or "").strip()
        if result_message:
            messages.append(f"{plate_format}: {result_message}")
    return {
        "ok": overall_ok,
        "saved_local": overall_saved_local,
        "saved_remote": overall_saved_remote,
        "message": " | ".join(messages),
        "source": sources,
        "entries": entries_by_format,
        "persist_details": persist_details,
    }


def _max_day_in_month(year: int, month: int) -> int:
    safe_year = max(1, int(year))
    safe_month = max(1, min(int(month), 12))
    return int(calendar.monthrange(safe_year, safe_month)[1])


def _max_day_in_current_month(today: Optional[date] = None) -> int:
    ref = today or date.today()
    return _max_day_in_month(ref.year, ref.month)


def _resolve_offer_validity_date(
    day_value: int,
    today: Optional[date] = None,
    month_value: Optional[int] = None,
) -> tuple[int, int, int]:
    ref = today or date.today()
    safe_month = int(month_value or ref.month)
    if safe_month < 1 or safe_month > 12:
        safe_month = ref.month
    max_day = _max_day_in_month(ref.year, safe_month)
    safe_day = max(1, min(int(day_value), max_day))
    return safe_day, safe_month, ref.year


def build_offer_validity_text(
    day_value: int,
    today: Optional[date] = None,
    month_value: Optional[int] = None,
) -> str:
    safe_day, safe_month, safe_year = _resolve_offer_validity_date(
        day_value,
        today=today,
        month_value=month_value,
    )
    return (
        f"Oferta válida até o dia {safe_day:02d}/{safe_month:02d}/{safe_year:04d} "
        "ou enquanto durarem os estoques."
    )


ACCENT_CORRECTIONS: Dict[str, str] = {
    # Correcoes comuns em nomes de produtos extraidos de PDF/OCR.
    "acem": "ac\u00e9m",
    "acai": "a\u00e7a\u00ed",
    "acougue": "a\u00e7ougue",
    "acucares": "a\u00e7\u00facares",
    "agua": "\u00e1gua",
    "aguas": "\u00e1guas",
    "alcool": "\u00e1lcool",
    "alcoolica": "alco\u00f3lica",
    "alcoolicas": "alco\u00f3licas",
    "alcoolico": "alco\u00f3lico",
    "alcoolicos": "alco\u00f3licos",
    "amendoa": "am\u00eandoa",
    "amendoas": "am\u00eandoas",
    "atencao": "aten\u00e7\u00e3o",
    "edicao": "edi\u00e7\u00e3o",
    "edicoes": "edi\u00e7\u00f5es",
    "energetico": "energ\u00e9tico",
    "energeticos": "energ\u00e9ticos",
    "file": "fil\u00e9",
    "suina": "su\u00edna",
    "suino": "su\u00edno",
    "moida": "mo\u00edda",
    "moido": "mo\u00eddo",
    "figado": "f\u00edgado",
    "cha": "ch\u00e1",
    "mocoto": "mocot\u00f3",
    "linguica": "lingui\u00e7a",
    "fragrancias": "fragr\u00e2ncias",
    "grao": "gr\u00e3o",
    "acucar": "a\u00e7\u00facar",
    "oleo": "\u00f3leo",
    "oleos": "\u00f3leos",
    "cafe": "caf\u00e9",
    "cafes": "caf\u00e9s",
    "tres": "tr\u00eas",
    "capsula": "c\u00e1psula",
    "capsulas": "c\u00e1psulas",
    "coracao": "cora\u00e7\u00e3o",
    "coracoes": "cora\u00e7\u00f5es",
    "descricao": "descri\u00e7\u00e3o",
    "desidratacao": "desidrata\u00e7\u00e3o",
    "isotonico": "isot\u00f4nico",
    "isotonicos": "isot\u00f4nicos",
    "liquida": "l\u00edquida",
    "liquidas": "l\u00edquidas",
    "liquido": "l\u00edquido",
    "liquidos": "l\u00edquidos",
    "mediterraneo": "mediterr\u00e2neo",
    "versao": "vers\u00e3o",
    "aluminio": "alum\u00ednio",
    "classico": "cl\u00e1ssico",
    "classicos": "cl\u00e1ssicos",
    "organica": "org\u00e2nica",
    "organicas": "org\u00e2nicas",
    "proteina": "prote\u00edna",
    "proteinas": "prote\u00ednas",
    "po": "p\u00f3",
    "porcao": "por\u00e7\u00e3o",
    "porcoes": "por\u00e7\u00f5es",
    "promocao": "promo\u00e7\u00e3o",
    "promocoes": "promo\u00e7\u00f5es",
    "refeicao": "refei\u00e7\u00e3o",
    "refeicoes": "refei\u00e7\u00f5es",
    "sodio": "s\u00f3dio",
    "tradicao": "tradi\u00e7\u00e3o",
    "unico": "\u00fanico",
    "unicos": "\u00fanicos",
    "instantaneo": "instant\u00e2neo",
    "instantaneos": "instant\u00e2neos",
    "mole": "mol\u00e9",
    "flocao": "floc\u00e3o",
    "pao": "p\u00e3o",
    "pao_de_queijo": "p\u00e3o_de_queijo",
    "feijao": "feij\u00e3o",
    "macarrao": "macarr\u00e3o",
    "molho_de_tomate": "molho_de_tomate",
    "farinha_de_mandioca": "farinha_de_mandioca",
    "fuba": "fub\u00e1",
    "farofa_pronta": "farofa_pronta",
    "mucarela": "mu\u00e7arela",
    "parmesao": "parmes\u00e3o",
    "requeijao": "requeij\u00e3o",
    "presunto": "presunto",
    "camarao": "camar\u00e3o",
    "salmao": "salm\u00e3o",
    "atum": "atum",
    "sardinha": "sardinha",
    "pedaco": "peda\u00e7o",
    "pedacos": "peda\u00e7os",
    "peca": "pe\u00e7a",
    "pecas": "pe\u00e7as",
    "fatiado": "fatiado",
    "fatiada": "fatiada",
    "fatia": "fatia",
    "fatias": "fatias",
    "maca": "ma\u00e7\u00e3",
    "mamao": "mam\u00e3o",
    "melao": "mel\u00e3o",
    "limao": "lim\u00e3o",
    "pessego": "p\u00eassego",
    "goiaba": "goiaba",
    "abobora": "ab\u00f3bora",
    "berinjela": "berinjela",
    "brocolis": "br\u00f3colis",
    "vovo": "vov\u00f3",
    "avo": "av\u00f3",
    "graos": "gr\u00e3os",
    "organico": "org\u00e2nico",
    "organicos": "org\u00e2nicos",
}


DEAD_KEY_CIRCUMFLEX: Dict[str, str] = {
    "a": "\u00e2",
    "e": "\u00ea",
    "i": "\u00ee",
    "o": "\u00f4",
    "u": "\u00fb",
    "A": "\u00c2",
    "E": "\u00ca",
    "I": "\u00ce",
    "O": "\u00d4",
    "U": "\u00db",
}

DEAD_KEY_TILDE: Dict[str, str] = {
    "a": "\u00e3",
    "o": "\u00f5",
    "A": "\u00c3",
    "O": "\u00d5",
}

PRICE_VALUE_PATTERN = r"(?:R\$\s*)?(?:-?\d{1,3}(?:\.\d{3})*(?:,\d{2})|-?\d+(?:,\d{2})|-?\d+(?:\.\d{2}))"
PRICE_FULL_TEXT_RE = re.compile(PRICE_VALUE_PATTERN, re.IGNORECASE)
PRICE_INLINE_RE = re.compile(PRICE_VALUE_PATTERN, re.IGNORECASE)
PRICE_NUMBER_RE = re.compile(
    r"-?\d{1,3}(?:\.\d{3})*(?:,\d{2})|-?\d+(?:,\d{2})|-?\d+(?:\.\d{2})"
)
PRICE_INTEGER_TEXT_RE = re.compile(r"\d+")
PRICE_CENTS_TEXT_RE = re.compile(r"\s*[,.]\d{2}\s*")
PRICE_FUZZY_DECIMAL_RE = re.compile(r"(-?\d{1,3}(?:[.\s]\d{3})*|-?\d+)\s*([,.])\s*([0-9OoBbSsIl]{2})")
PRICE_SPLIT_INTEGER_DECIMAL_RE = re.compile(
    r"(?:R\s*\$\s*)?([0-9OoBbSsIl](?:\s*[0-9OoBbSsIl])*)\s*([,.])\s*([0-9OoBbSsIl]{2})",
    re.IGNORECASE,
)
DATE_XXXX_PLACEHOLDER_RE = re.compile(r"xx\s*[/.]\s*xx\s*[/.]\s*xxxx", re.IGNORECASE)
LEARNING_CACHE_FILE = "aprendizado_placas.json"
LEARNING_CACHE_VERSION = 2
NAME_CLEANUP_CACHE_SECTION = "name_cleanup_cache"
WEB_CONFIG_PAGE_FILE = "web_config_page.html"
USERS_CACHE_FILE = "usuarios.cache.json"
USERS_REMOTE_RAW_URL = "https://raw.githubusercontent.com/PopularAtacarejo/Placas/main/usuarios.json"
USERS_REPO_OWNER = "PopularAtacarejo"
USERS_REPO_NAME = "Placas"
USERS_REPO_BRANCH = "main"
USERS_REPO_FILE_PATH = "usuarios.json"
USERS_GITHUB_TOKEN_ENV_CANDIDATES = ("GITHUB_PLACAS_TOKEN", "PLACA_GITHUB_TOKEN", "GITHUB_TOKEN")
USERS_SESSION_COOKIE_NAME = "placas_session"
USERS_SESSION_MAX_AGE_SECONDS = 12 * 60 * 60
USERS_SESSION_REMEMBER_24H_SECONDS = 24 * 60 * 60
LOGIN_SESSION_FILE = "sessao_login_24h.json"
LOGIN_SESSION_HOURS = 24
PASSWORD_RESET_REQUESTS_FILE = "solicitacoes_reset_senha.json"
PLATE_AUDIT_TARGETS: Dict[str, Dict[str, str]] = {
    "A4": {
        "cache_file": "placas_a4.cache.json",
        "repo_file_path": "Placas A4.json",
        "remote_raw_url": "https://raw.githubusercontent.com/PopularAtacarejo/Placas/main/Placas%20A4.json",
    },
    "A5": {
        "cache_file": "placas_a5.cache.json",
        "repo_file_path": "Placas A5.json",
        "remote_raw_url": "https://raw.githubusercontent.com/PopularAtacarejo/Placas/main/Placas%20A5.json",
    },
    "A6": {
        "cache_file": "placas_a6.cache.json",
        "repo_file_path": "Placas A6.json",
        "remote_raw_url": "https://raw.githubusercontent.com/PopularAtacarejo/Placas/main/Placas%20A6.json",
    },
}
USER_PASSWORD_PBKDF2_ITERATIONS = max(120000, int(os.environ.get("PLACA_PASSWORD_ITERATIONS", "390000") or "390000"))
PASSWORD_HASH_SCHEME = "pbkdf2_sha256"
PASSWORD_HASH_MIN_ITERATIONS = 150000
ROLE_DEV = "Dev"
ROLE_ADMIN = "Admin"
ROLE_PLATE_GENERATOR = "Gerador de Placas"
VALID_USER_ROLES = (ROLE_DEV, ROLE_ADMIN, ROLE_PLATE_GENERATOR)
DEFAULT_OLLAMA_TIMEOUT_SECONDS = 8.0
DEFAULT_OLLAMA_MAX_ITEMS = 0
DEFAULT_WEB_LOOKUP_TIMEOUT_SECONDS = 6.0
_DEFAULT_OLLAMA_MODEL_FALLBACK = "qwen3:1.7b"
DEFAULT_OLLAMA_MODEL = (
    str(os.environ.get("OLLAMA_MODEL", _DEFAULT_OLLAMA_MODEL_FALLBACK)).strip()
    or _DEFAULT_OLLAMA_MODEL_FALLBACK
)
OCR_PRICE_DIGIT_MAP = str.maketrans(
    {
        "O": "0",
        "o": "0",
        "B": "8",
        "b": "8",
        "S": "5",
        "s": "5",
        "I": "1",
        "l": "1",
    }
)
A5_PAIR_GAP_MM = -8.0
A6_QUAD_GAP_X_MM = -9.0
A6_QUAD_GAP_Y_MM = -2.0
MODELOS_PLACA_DIRNAME = "Modelos De Placa"
CODE_GUARD_STATE_FILE = ".code_guard_state.json"
CODE_GUARD_STATE_VERSION = 1
CODE_GUARD_PASSWORD_ENV = "PLACA_CODE_PASSWORD"
CODE_GUARD_REPO_FILE_PATH = "Senha.json"
CODE_GUARD_REMOTE_RAW_URL = "https://raw.githubusercontent.com/PopularAtacarejo/Placas/main/Senha.json"

ROLE_PERMISSIONS: Dict[str, Dict[str, bool]] = {
    ROLE_DEV: {
        "can_manage_users": True,
        "can_configure_plates": True,
        "can_submit_generation": True,
        "can_use_ai_cleanup": True,
        "can_manage_templates": True,
    },
    ROLE_ADMIN: {
        "can_manage_users": False,
        "can_configure_plates": True,
        "can_submit_generation": True,
        "can_use_ai_cleanup": True,
        "can_manage_templates": True,
    },
    ROLE_PLATE_GENERATOR: {
        "can_manage_users": False,
        "can_configure_plates": True,
        "can_submit_generation": True,
        "can_use_ai_cleanup": False,
        "can_manage_templates": True,
    },
}


def _apply_word_case(source_word: str, corrected_word: str) -> str:
    if source_word.isupper():
        return corrected_word.upper()
    if source_word.islower():
        return corrected_word.lower()
    if source_word.istitle():
        return corrected_word[:1].upper() + corrected_word[1:]
    return corrected_word


def _word_has_diacritic(word: str) -> bool:
    normalized = unicodedata.normalize("NFD", str(word or ""))
    return any(unicodedata.combining(ch) for ch in normalized)


def _apply_dead_key_accents(text: str) -> str:
    def _circ_repl(match: re.Match[str]) -> str:
        letter = match.group(1)
        return DEAD_KEY_CIRCUMFLEX.get(letter, letter)

    def _tilde_repl(match: re.Match[str]) -> str:
        letter = match.group(1)
        return DEAD_KEY_TILDE.get(letter, letter)

    out = text
    # Formatos: a^, a ^, ^a, ^ a
    out = re.sub(r"([AaEeIiOoUu])\s*\^", _circ_repl, out)
    out = re.sub(r"\^\s*([AaEeIiOoUu])", _circ_repl, out)
    # Formatos: a~, a ~, ~a, ~ a
    out = re.sub(r"([AaOo])\s*~", _tilde_repl, out)
    out = re.sub(r"~\s*([AaOo])", _tilde_repl, out)
    return out


def _apply_contextual_accent_fixes(text: str) -> str:
    if not text:
        return text

    def _cha_repl(match: re.Match[str]) -> str:
        suffix = str(match.group(1) or "").lower()
        replacement = f"ch\u00e3 de {suffix}"
        source = str(match.group(0) or "")
        if source.isupper():
            return replacement.upper()
        if source.islower():
            return replacement.lower()
        if source.istitle() or (source[:1].isupper() and not source.isupper()):
            return replacement[:1].upper() + replacement[1:]
        return replacement

    return re.sub(r"\bcha\s+de\s+(dentro|fora)\b", _cha_repl, str(text), flags=re.IGNORECASE)


def _normalize_flavor_markers(text: str) -> str:
    if not text:
        return text
    out = str(text)
    lowered = out.lower()

    # Escolhe o sufixo "Div." com base no contexto do produto.
    # Fraldas: variação principal por tamanho.
    # Perfume/Desodorante: variação principal por fragrância.
    diversity_label = "Div. Sabores"
    if re.search(r"\bfrald(?:a|as)\b", lowered):
        diversity_label = "Div. Tamanhos"
    elif re.search(
        r"\b(perfume|perfumes|desodorante|desodorantes|desod(?:\.|$)|colonia|colô?nia|antitranspirante)\b",
        lowered,
        flags=re.IGNORECASE,
    ):
        diversity_label = "Div. Fragrâncias"

    patterns = [
        r"\(\s*diversos\s+sabores[0o]?\s*\)",
        r"\(\s*div\.*\s+sabores\s*\)",
        r"\bdiversos\s+sabores[0o]?\b",
        r"\(\s*diversos[0o]?\s*\)?",
        r"\(\s*sabores\s*\)",
        r"\bdiversos[0o]?\b(?!\s+sabores\b)",
    ]
    for pattern in patterns:
        out = re.sub(pattern, diversity_label, out, flags=re.IGNORECASE)
    # Se ja houver "Div." no nome, padroniza para o sufixo escolhido pelo contexto.
    out = re.sub(
        r"\bDiv\.?\s*(Sabores?|Fragr[aâ]ncias?|Tamanhos?)\b",
        diversity_label,
        out,
        flags=re.IGNORECASE,
    )
    out = re.sub(
        r"\s*-\s*Div\.\s*(Sabores?|Fragr[aâ]ncias?|Tamanhos?)\b",
        f" {diversity_label}",
        out,
        flags=re.IGNORECASE,
    )
    out = re.sub(r"\s+", " ", out).strip()
    return out


def _normalize_spacing_in_text(text: str) -> str:
    if not text:
        return text
    out = str(text)
    out = out.replace("_", " ")
    out = re.sub(r"([A-Za-zÀ-ÿ])(\d)", r"\1 \2", out)
    out = re.sub(r"(\d)([A-Za-zÀ-ÿ])", r"\1 \2", out)
    out = re.sub(r"([a-zà-ÿ])([A-ZÀ-Ý])", r"\1 \2", out)
    out = re.sub(r"([A-Z]{2,})([A-ZÀ-Ý][a-zà-ÿ])", r"\1 \2", out)
    out = re.sub(r"([)\]])([A-Za-zÀ-ÿ])", r"\1 \2", out)
    out = re.sub(r"([A-Za-zÀ-ÿ])([(\[])", r"\1 \2", out)
    out = re.sub(r"\s*/\s*", "/", out)
    # Mantem gramatura/volume juntos: 50 g -> 50g, 250 ml -> 250ml, 1 kg -> 1kg.
    out = re.sub(r"(\d+(?:[.,]\d+)?)\s+(kg|g|gr|mg|ml|l|lt)\b", r"\1\2", out, flags=re.IGNORECASE)
    out = re.sub(r"\s+", " ", out).strip()
    return out


def correct_accents_in_text(text: str) -> str:
    if not text:
        return text

    text = _apply_dead_key_accents(text)
    text = _normalize_spacing_in_text(text)
    text = _apply_contextual_accent_fixes(text)
    word_pattern = re.compile(r"[A-Za-z\u00c0-\u00ff]+")

    def _replace(match: re.Match[str]) -> str:
        word = match.group(0)
        if _word_has_diacritic(word):
            return word
        normalized_word = normalize_key(word).replace("_", "")
        corrected = ACCENT_CORRECTIONS.get(normalized_word)
        if not corrected:
            return word
        return _apply_word_case(word, corrected)

    corrected_text = word_pattern.sub(_replace, text)
    return _normalize_flavor_markers(corrected_text)


def normalize_unit_label(value: str) -> str:
    normalized = normalize_key(value).replace("_", "")
    if normalized in {"kg", "quilo", "quilos", "kilo", "kilos", "kilograma", "kilogramas"}:
        return "KG"
    if normalized in {"pct", "pcto", "pacote", "pacotes"}:
        return "PCT."
    if normalized in {"bdj", "bandeja", "bandejas"}:
        return "BDJ."
    if normalized in {"pack", "multipack", "kit", "combo"}:
        return "PACK."
    if normalized in {"un", "und", "unid", "unidade", "unidades", "unit", "units"}:
        return "UNID"
    return "KG"


UNIT_OPTIONS: tuple[str, ...] = ("KG", "UNID", "PCT.", "BDJ.", "PACK.")


def infer_unit_label(name_value: str, row: Optional[Dict[str, str]] = None, default: str = "KG") -> str:
    row = row or {}
    unit_column = detect_column(
        [row],
        None,
        [
            "unidade",
            "tipo_unidade",
            "tipo_de_unidade",
            "unidade_medida",
            "unidade_de_medida",
            "medida",
            "kg_ou_unid",
        ],
    )
    if unit_column:
        direct_unit_raw = str(row.get(unit_column, "") or "").strip()
        if direct_unit_raw:
            normalized_direct = normalize_key(direct_unit_raw).replace("_", "")
            if normalized_direct in {
                "kg",
                "quilo",
                "quilos",
                "kilo",
                "kilos",
                "kilograma",
                "kilogramas",
                "pct",
                "pcto",
                "pacote",
                "pacotes",
                "bdj",
                "bandeja",
                "bandejas",
                "pack",
                "multipack",
                "kit",
                "combo",
                "un",
                "und",
                "unid",
                "unidade",
                "unidades",
                "unit",
                "units",
            }:
                return normalize_unit_label(direct_unit_raw)

    text = " ".join(
        [
            str(name_value or ""),
            str(row.get("descricao", "") or ""),
            str(row.get("descrição", "") or ""),
            str(row.get("produto", "") or ""),
            str(row.get("nome", "") or ""),
        ]
    ).strip()
    lowered = text.lower()
    normalized = normalize_key(text)
    compact = normalized.replace("_", "")

    if re.search(r"\bovos?\b", lowered):
        return "BDJ."
    if re.search(r"\b(bdj|bandeja)\b", lowered):
        return "BDJ."
    if re.search(r"\b(pct|pacote|pacotes)\b", lowered):
        return "PCT."
    if re.search(r"\b(pack|multipack)\b", lowered):
        return "PACK."

    # Itens com volume/peso embalado tendem a ser vendidos por unidade.
    if re.search(r"\b\d+(?:[.,]\d+)?\s*(ml|g|gr|kg|l|lt)\b", lowered):
        return "UNID"
    if re.search(r"\b\d+(?:[.,]\d+)?(ml|g|gr|kg|l|lt)\b", compact):
        return "UNID"

    name_tokens = normalize_key(name_value).split("_")
    last_token = name_tokens[-1] if name_tokens else ""
    if last_token == "kg":
        return "KG"
    if last_token in {"ml", "g", "gr", "l", "lt"}:
        return "UNID"
    return normalize_unit_label(default)


UNIT_PLACEHOLDER_ALIASES = {
    "UNIDADE",
    "UNIT",
    "MEDIDA",
    "TIPO_UNIDADE",
    "TIPO_DE_UNIDADE",
    "KG_OU_UNID",
    "UN",
    "UND",
    "UNID",
    "UNIDADE_MEDIDA",
    "UNIDADE_DE_MEDIDA",
    "MEDIDA_UNIDADE",
    "UNIT_LABEL",
    "UNIT_TYPE",
    "UNIDADE_DO_PRODUTO",
}


def is_unit_placeholder_token(token: str) -> bool:
    token_up = (token or "").strip().upper()
    if not token_up:
        return False
    if token_up in UNIT_PLACEHOLDER_ALIASES:
        return True
    token_norm = normalize_key(token).upper()
    return token_norm in UNIT_PLACEHOLDER_ALIASES


def normalize_plate_format(value: str, default: str = "A4") -> str:
    a4_aliases = {"A4", "PLACA_A4", "A4_FOLHA_COMPLETA"}
    a5_aliases = {"A5", "PLACA_A5", "A5_DUPLO", "A5_DUO", "A5_2UP", "A5_LADO_A_LADO"}
    a6_aliases = {"A6", "PLACA_A6", "A6_QUAD", "A6_4UP", "A6_2X2", "A6_QUATRO"}

    norm = normalize_key(value).upper()
    if norm in a6_aliases:
        return "A6"
    if norm in a5_aliases:
        return "A5"
    if norm in a4_aliases:
        return "A4"

    default_norm = normalize_key(default).upper()
    if default_norm in a6_aliases:
        return "A6"
    if default_norm in a5_aliases:
        return "A5"
    return "A4"


def normalize_format_quantities(
    raw: Optional[Dict[str, Any]],
    default_format: str = "A4",
    default_qty: int = 1,
) -> Dict[str, int]:
    quantities: Dict[str, int] = {"A4": 0, "A5": 0, "A6": 0}

    if isinstance(raw, dict):
        for key, value in raw.items():
            fmt = normalize_plate_format(str(key), default=default_format)
            try:
                qty = int(value)
            except Exception:
                qty = 0
            if qty > 0:
                quantities[fmt] = quantities.get(fmt, 0) + min(qty, 999)

    if sum(quantities.values()) <= 0:
        fallback_fmt = normalize_plate_format(default_format, default="A4")
        fallback_qty = max(1, min(int(default_qty or 1), 999))
        quantities[fallback_fmt] = fallback_qty

    return quantities


def total_format_copies(format_quantities: Optional[Dict[str, Any]]) -> int:
    if not isinstance(format_quantities, dict):
        return 0
    total = 0
    for value in format_quantities.values():
        try:
            parsed = int(value)
        except Exception:
            parsed = 0
        if parsed > 0:
            total += parsed
    return total


def sanitize_filename(value: str, fallback: str) -> str:
    value = (value or "").replace("\r", " ").replace("\n", " ").strip()
    value = re.sub(r"\s+", " ", value)
    value = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "", value)
    value = value.strip(" .")
    if len(value) > 100:
        value = value[:100].rstrip(" .")
    return value or fallback


def _load_learning_payload_file(cache_path: Path) -> Dict[str, Any]:
    try:
        if not cache_path.exists():
            return {"version": LEARNING_CACHE_VERSION}
        with cache_path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass
    return {"version": LEARNING_CACHE_VERSION}


def _save_learning_payload_file(cache_path: Path, payload: Dict[str, Any]) -> None:
    temp_path = cache_path.with_suffix(".tmp")
    payload = dict(payload or {})
    payload["version"] = LEARNING_CACHE_VERSION
    try:
        with temp_path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        temp_path.replace(cache_path)
    finally:
        try:
            if temp_path.exists():
                temp_path.unlink()
        except Exception:
            pass


def load_web_config_page_html() -> str:
    page_path = Path(__file__).with_name(WEB_CONFIG_PAGE_FILE)
    try:
        return page_path.read_text(encoding="utf-8")
    except Exception as exc:
        raise RuntimeError(f"Falha ao carregar interface web em {page_path}: {exc}") from exc


def _load_name_cleanup_cache(cache_path: Path) -> Dict[str, Dict[str, Any]]:
    payload = _load_learning_payload_file(cache_path)
    raw_cache = payload.get(NAME_CLEANUP_CACHE_SECTION)
    if not isinstance(raw_cache, dict):
        return {}
    sanitized: Dict[str, Dict[str, Any]] = {}
    for key, value in raw_cache.items():
        if isinstance(key, str) and isinstance(value, dict):
            sanitized[key] = value
    return sanitized


def _persist_name_cleanup_cache(cache_path: Path, cache_blob: Dict[str, Dict[str, Any]]) -> None:
    payload = _load_learning_payload_file(cache_path)
    payload[NAME_CLEANUP_CACHE_SECTION] = cache_blob
    _save_learning_payload_file(cache_path, payload)


def remember_product_name_cleanup(
    original_name: str,
    corrected_name: str,
    unit_value: str = "",
    source: str = "manual",
    model_name: str = "local",
    cache_path: Optional[Path] = None,
) -> None:
    raw_original = re.sub(r"\s+", " ", str(original_name or "").strip())
    raw_corrected = re.sub(r"\s+", " ", str(corrected_name or "").strip())
    if not raw_original or not raw_corrected:
        return
    cache_key = normalize_key(raw_original)
    if not cache_key:
        return
    cache_file = cache_path or Path(__file__).with_name(LEARNING_CACHE_FILE)
    cleanup_cache = _load_name_cleanup_cache(cache_file)
    previous = cleanup_cache.get(cache_key, {}) if isinstance(cleanup_cache.get(cache_key), dict) else {}
    hits = int(previous.get("hits", 0) or 0) + 1
    normalized_unit = ""
    if str(unit_value or "").strip():
        normalized_unit = normalize_unit_label(unit_value)
    cleanup_cache[cache_key] = {
        "original": raw_original,
        "corrected": raw_corrected,
        "unit": normalized_unit,
        "confidence": float(previous.get("confidence", 1.0) or 1.0),
        "model": str(model_name or "local"),
        "source": str(source or "manual"),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "hits": hits,
    }
    _persist_name_cleanup_cache(cache_file, cleanup_cache)


def _normalize_cleanup_cache_candidates(*values: str) -> List[str]:
    candidates: List[str] = []
    seen: set[str] = set()
    for raw_value in values:
        clean_value = re.sub(r"\s+", " ", str(raw_value or "").strip())
        if not clean_value:
            continue
        for candidate in (clean_value, correct_accents_in_text(clean_value)):
            cache_key = normalize_key(candidate)
            if not cache_key or cache_key in seen:
                continue
            seen.add(cache_key)
            candidates.append(cache_key)
    return candidates


def _find_name_cleanup_cache_entry(
    cleanup_cache: Dict[str, Dict[str, Any]],
    *values: str,
) -> tuple[str, Optional[Dict[str, Any]]]:
    for cache_key in _normalize_cleanup_cache_candidates(*values):
        cached_entry = cleanup_cache.get(cache_key)
        if isinstance(cached_entry, dict):
            return cache_key, cached_entry
    return "", None


def has_product_name_cleanup_learning(
    product_name: str,
    original_name: str = "",
    cache_path: Optional[Path] = None,
) -> bool:
    cache_file = cache_path or Path(__file__).with_name(LEARNING_CACHE_FILE)
    cleanup_cache = _load_name_cleanup_cache(cache_file)
    _cache_key, cached_entry = _find_name_cleanup_cache_entry(cleanup_cache, original_name, product_name)
    return isinstance(cached_entry, dict)


def _resolve_plate_original_name(item: PlateData) -> str:
    raw_name = str(getattr(item, "original_name", "") or "").strip()
    if raw_name:
        return raw_name
    row = getattr(item, "row", None)
    if isinstance(row, dict):
        raw_name = str(row.get("_plate_original_name") or "").strip()
        if raw_name:
            return raw_name
    return re.sub(r"\s+", " ", str(getattr(item, "name", "") or "").strip())


def persist_plate_selection_learning(items: List[PlateData]) -> None:
    for item in items or []:
        try:
            original_name = _resolve_plate_original_name(item)
            current_name = re.sub(r"\s+", " ", str(getattr(item, "name", "") or "").strip())
            if not original_name or not current_name:
                continue
            remember_product_name_cleanup(
                original_name,
                current_name,
                unit_value=str(getattr(item, "unit_label", "") or "").strip(),
                source="production_start",
                model_name=str(getattr(item, "cleanup_source", "") or "local_ui"),
            )
        except Exception:
            continue


def _find_ollama_executable() -> Optional[Path]:
    global _OLLAMA_EXECUTABLE_CACHE, _OLLAMA_LOOKUP_DONE
    if _OLLAMA_LOOKUP_DONE:
        return _OLLAMA_EXECUTABLE_CACHE
    _OLLAMA_LOOKUP_DONE = True

    from_path = shutil.which("ollama")
    if from_path:
        _OLLAMA_EXECUTABLE_CACHE = Path(from_path)
        return _OLLAMA_EXECUTABLE_CACHE

    candidates: List[Path] = []
    local_app_data = os.environ.get("LOCALAPPDATA", "").strip()
    program_files = os.environ.get("ProgramFiles", "").strip()
    program_files_x86 = os.environ.get("ProgramFiles(x86)", "").strip()
    for base in (local_app_data, program_files, program_files_x86):
        if not base:
            continue
        candidates.extend(
            [
                Path(base) / "Programs" / "Ollama" / "ollama.exe",
                Path(base) / "Ollama" / "ollama.exe",
            ]
        )
    for candidate in candidates:
        try:
            if candidate.exists():
                _OLLAMA_EXECUTABLE_CACHE = candidate
                return _OLLAMA_EXECUTABLE_CACHE
        except Exception:
            continue
    return None


def _list_ollama_models(executable: Path) -> List[str]:
    global _OLLAMA_MODELS_CACHE, _OLLAMA_MODELS_LOOKUP_DONE
    if _OLLAMA_MODELS_LOOKUP_DONE:
        return list(_OLLAMA_MODELS_CACHE or [])
    _OLLAMA_MODELS_LOOKUP_DONE = True
    _OLLAMA_MODELS_CACHE = []
    try:
        proc = subprocess.run(
            [str(executable), "list"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=5,
            check=False,
        )
    except Exception:
        return []
    if int(proc.returncode) != 0:
        return []
    lines = [line.rstrip() for line in (proc.stdout or "").splitlines() if line.strip()]
    if not lines:
        return []
    names: List[str] = []
    for line in lines[1:]:
        parts = line.strip().split()
        if not parts:
            continue
        name = str(parts[0]).strip()
        if name and name not in names:
            names.append(name)
    _OLLAMA_MODELS_CACHE = names
    return list(names)


def _resolve_ollama_model_name(requested_model: str, executable: Path) -> str:
    global _OLLAMA_MODEL_RESOLUTION_CACHE, _OLLAMA_MODEL_FALLBACK_NOTIFIED
    requested = str(requested_model or DEFAULT_OLLAMA_MODEL).strip() or DEFAULT_OLLAMA_MODEL
    cache_key = requested.lower()
    cached = _OLLAMA_MODEL_RESOLUTION_CACHE.get(cache_key)
    if cached:
        return cached

    available = _list_ollama_models(executable)
    if not available:
        _OLLAMA_MODEL_RESOLUTION_CACHE[cache_key] = requested
        return requested

    resolved = ""
    for candidate in available:
        if candidate.lower() == cache_key:
            resolved = candidate
            break
    if not resolved:
        preferred_order = [
            _DEFAULT_OLLAMA_MODEL_FALLBACK,
            "qwen3:4b",
            "qwen3:1.7b",
            "qwen3:0.6b",
        ]
        normalized_available = {item.lower(): item for item in available}
        for preferred in preferred_order:
            match = normalized_available.get(preferred.lower())
            if match:
                resolved = match
                break
    if not resolved:
        for candidate in available:
            if candidate.lower().startswith("qwen3"):
                resolved = candidate
                break
    if not resolved:
        resolved = available[0]

    _OLLAMA_MODEL_RESOLUTION_CACHE[cache_key] = resolved
    if resolved.lower() != cache_key and cache_key not in _OLLAMA_MODEL_FALLBACK_NOTIFIED:
        print_status("AI", f"Modelo Ollama '{requested}' nao encontrado. Usando '{resolved}'.")
        _OLLAMA_MODEL_FALLBACK_NOTIFIED.add(cache_key)
    return resolved


def _extract_json_object_from_text(text: str) -> Optional[Dict[str, Any]]:
    raw = (text or "").strip()
    if not raw:
        return None
    candidates = [raw]
    fenced = re.findall(r"\{[\s\S]*\}", raw)
    candidates.extend(fenced)
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            continue
    return None


def _strip_html_tags(text: str) -> str:
    raw = html.unescape(str(text or ""))
    raw = re.sub(r"<[^>]+>", " ", raw)
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw


def _needs_web_lookup_hint(text: str) -> bool:
    clean = re.sub(r"\s+", " ", str(text or "").strip())
    if not clean:
        return False
    if re.search(r"\b\d+(?:[.,]\d+)?\s*$", clean):
        return True
    if re.search(r"\b(alm|ext|fort|trad|int|desc|org)\b", clean, flags=re.IGNORECASE):
        return True
    if " " not in clean and len(clean) >= 12:
        return True
    return False


def _search_product_web_hints(
    product_name: str,
    timeout_seconds: float = DEFAULT_WEB_LOOKUP_TIMEOUT_SECONDS,
    max_results: int = 3,
) -> List[str]:
    query = f'"{product_name}" produto'
    url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
    try:
        req = Request(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
                )
            },
        )
        with urlopen(req, timeout=max(3.0, float(timeout_seconds))) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
    except Exception:
        return []

    snippets: List[str] = []
    title_matches = re.findall(r'<a[^>]*class="result__a"[^>]*>(.*?)</a>', body, flags=re.IGNORECASE | re.DOTALL)
    snippet_matches = re.findall(
        r'<a[^>]*class="result__snippet"[^>]*>(.*?)</a>|<div[^>]*class="result__snippet"[^>]*>(.*?)</div>',
        body,
        flags=re.IGNORECASE | re.DOTALL,
    )
    for title in title_matches[: max_results * 2]:
        text = _strip_html_tags(title)
        if text and text not in snippets:
            snippets.append(text)
        if len(snippets) >= max_results:
            return snippets[:max_results]
    for match in snippet_matches[: max_results * 3]:
        text = _strip_html_tags(match[0] or match[1] or "")
        if text and text not in snippets:
            snippets.append(text)
        if len(snippets) >= max_results:
            break
    return snippets[:max_results]


def _should_request_name_cleanup(text: str) -> bool:
    clean = re.sub(r"\s+", " ", str(text or "").strip())
    if not clean:
        return False
    compact = normalize_key(clean).replace("_", "")
    if len(compact) <= 4:
        return False
    if re.search(r"[?�]", clean):
        return True
    if re.search(r"[{}\[\]<>]", clean):
        return True
    lowered = clean.lower()
    has_div_marker = bool(
        re.search(r"\bdiv\.?\s*(sabores?|fragr[aâ]ncias?|tamanhos?)\b", lowered, flags=re.IGNORECASE)
    )
    if re.search(r"\bdivers[oa]s?\b", lowered) and not has_div_marker:
        return True
    if re.search(r"\bsabores?0\b", lowered):
        return True
    if re.search(r"\d{2,}\s*[xX]\s*\d{2,}", clean):
        return True
    if " " not in clean and len(compact) >= 12:
        return True
    if re.search(r"[A-Za-z][0-9][A-Za-z]", clean):
        return True
    if re.search(r"\b[a-z]{1,2}\b", clean):
        short_tokens = re.findall(r"\b[a-z]{1,2}\b", clean)
        if len(short_tokens) >= 2:
            return True
    if len(re.findall(r"[A-Za-z]", clean)) >= 8 and clean == clean.lower():
        return True
    return False


def _call_ollama_name_cleanup(
    product_name: str,
    row: Optional[Dict[str, str]] = None,
    model: str = DEFAULT_OLLAMA_MODEL,
    timeout_seconds: float = 18.0,
    web_hints: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    global _OLLAMA_WARNING_EMITTED
    executable = _find_ollama_executable()
    if not executable:
        if not _OLLAMA_WARNING_EMITTED:
            print_status("AI", "Ollama nao encontrado. Usando apenas cache local e heuristicas.")
            _OLLAMA_WARNING_EMITTED = True
        return None
    selected_model = _resolve_ollama_model_name(model, executable)

    row_text = ""
    if isinstance(row, dict) and row:
        pieces: List[str] = []
        for key, value in row.items():
            text_value = str(value or "").strip()
            if not text_value:
                continue
            pieces.append(f"{key}: {text_value}")
        row_text = " | ".join(pieces[:6])

    hints_text = ""
    if web_hints:
        hints_text = " | ".join(str(item or "").strip() for item in web_hints if str(item or "").strip())

    prompt = (
        "Corrija o nome do produto extraido de PDF/OCR para uso em placa de preco. "
        "Responda somente JSON valido, sem markdown, com as chaves "
        'corrected_name, unit, confidence, notes. '
        "Regras: mantenha marca e gramatura reais; nao invente informacoes; "
        'use unit apenas entre KG, UNID, PCT., BDJ., PACK.; '
        'se houver ovo ou ovos, prefira BDJ.; '
        'se o item indicar embalagem como 250ml, 30g, 1kg, prefira UNID; '
        "corrija abreviacoes, OCR, acentos e espacos faltando entre palavras quando fizer sentido comercial; "
        "garanta acentuacao correta em portugues do Brasil no nome final; "
        "se o texto estiver colado, separe as palavras corretamente; "
        "entregue corrected_name ja pronto para exibir na placa; "
        "se as pistas web apontarem gramatura/volume faltando, complete o nome com isso.\n"
        f'Produto: "{product_name}"\n'
        f'Contexto: "{row_text}"\n'
        f'Pistas web: "{hints_text}"'
    )
    try:
        proc = subprocess.run(
            [str(executable), "run", selected_model, prompt],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=max(5, int(timeout_seconds)),
            check=False,
        )
    except Exception:
        return None
    if int(proc.returncode) != 0:
        stderr = (proc.stderr or "").strip()
        if stderr and not _OLLAMA_WARNING_EMITTED:
            print_status("AI", f"Ollama indisponivel no momento: {stderr}")
            _OLLAMA_WARNING_EMITTED = True
        return None
    return _extract_json_object_from_text(proc.stdout)


def cleanup_product_name_with_learning(
    product_name: str,
    row: Optional[Dict[str, str]] = None,
    original_name: str = "",
    use_ollama: bool = False,
    force_ollama: bool = False,
    ollama_model: str = DEFAULT_OLLAMA_MODEL,
    ollama_timeout_seconds: float = DEFAULT_OLLAMA_TIMEOUT_SECONDS,
    allow_web_lookup: bool = True,
    cache_path: Optional[Path] = None,
) -> tuple[str, str, str]:
    base_name = re.sub(r"\s+", " ", str(product_name or "").strip())
    raw_original_name = re.sub(r"\s+", " ", str(original_name or "").strip())
    if not base_name:
        return "", "", "empty"

    cache_file = cache_path or Path(__file__).with_name(LEARNING_CACHE_FILE)
    cleanup_cache = _load_name_cleanup_cache(cache_file)
    cache_key, cached_entry = _find_name_cleanup_cache_entry(cleanup_cache, raw_original_name, base_name)
    if cache_key and isinstance(cached_entry, dict):
        corrected_name = re.sub(r"\s+", " ", str(cached_entry.get("corrected") or base_name).strip())
        cached_unit_raw = str(cached_entry.get("unit") or "").strip()
        cached_unit = normalize_unit_label(cached_unit_raw) if cached_unit_raw else ""
        cached_entry["hits"] = int(cached_entry.get("hits", 0) or 0) + 1
        cleanup_cache[cache_key] = cached_entry
        _persist_name_cleanup_cache(cache_file, cleanup_cache)
        return corrected_name or base_name, cached_unit, "cache"

    corrected_name = correct_accents_in_text(base_name)
    corrected_unit = ""
    persist_original_name = raw_original_name or base_name
    persist_cache_key = normalize_key(persist_original_name)

    if use_ollama and (force_ollama or _should_request_name_cleanup(corrected_name)):
        web_hints: List[str] = []
        if allow_web_lookup and (force_ollama or _needs_web_lookup_hint(corrected_name)):
            web_hints = _search_product_web_hints(corrected_name)
        ai_result = _call_ollama_name_cleanup(
            corrected_name,
            row=row,
            model=ollama_model,
            timeout_seconds=ollama_timeout_seconds,
            web_hints=web_hints,
        )
        if isinstance(ai_result, dict):
            ai_name = re.sub(r"\s+", " ", str(ai_result.get("corrected_name") or "").strip())
            ai_unit_raw = str(ai_result.get("unit") or "").strip()
            try:
                confidence = float(ai_result.get("confidence") or 0.0)
            except Exception:
                confidence = 0.0
            if ai_name and confidence >= 0.55:
                corrected_name = correct_accents_in_text(ai_name)
                corrected_unit = normalize_unit_label(ai_unit_raw) if ai_unit_raw else ""
                if persist_cache_key:
                    cleanup_cache[persist_cache_key] = {
                        "original": persist_original_name,
                        "corrected": corrected_name,
                        "unit": corrected_unit,
                        "confidence": round(confidence, 3),
                        "model": str(ollama_model or DEFAULT_OLLAMA_MODEL),
                        "source": "ollama",
                        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "hits": 1,
                    }
                    _persist_name_cleanup_cache(cache_file, cleanup_cache)
                return corrected_name, corrected_unit, "ollama"

    if persist_cache_key and corrected_name and corrected_name != base_name:
        cleanup_cache[persist_cache_key] = {
            "original": persist_original_name,
            "corrected": corrected_name,
            "unit": corrected_unit,
            "confidence": 1.0,
            "model": "local_rules",
            "source": "heuristic",
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "hits": 1,
        }
        _persist_name_cleanup_cache(cache_file, cleanup_cache)
    return corrected_name, corrected_unit, "heuristic"


def _collect_cdr_templates(paths: List[Path]) -> List[Path]:
    found: List[Path] = []
    seen: set[str] = set()

    def _append(path_obj: Path) -> None:
        try:
            resolved = path_obj.expanduser().resolve()
        except Exception:
            return
        key = str(resolved).lower()
        if key in seen:
            return
        seen.add(key)
        found.append(resolved)

    for base in paths:
        try:
            candidate = base.expanduser().resolve()
        except Exception:
            continue

        if candidate.is_file() and candidate.suffix.lower() == ".cdr":
            _append(candidate)
            continue

        if not candidate.is_dir():
            continue

        try:
            for cdr_file in sorted(candidate.glob("*.cdr")):
                _append(cdr_file)
        except Exception:
            continue

    return found


def _prefer_modelos_placa_dir(base_dir: Path) -> Path:
    try:
        candidate = base_dir.expanduser().resolve() / MODELOS_PLACA_DIRNAME
    except Exception:
        return base_dir
    if candidate.exists() and candidate.is_dir():
        return candidate
    return base_dir


def _find_existing_file(candidates: List[Path]) -> Optional[Path]:
    for candidate in candidates:
        try:
            if candidate.exists():
                return candidate
        except Exception:
            continue
    return None


def _build_external_pdf_print_commands(pdf_path: Path, default_printer: str) -> List[List[str]]:
    env_bases: List[Path] = []
    for env_name in ("ProgramFiles", "ProgramFiles(x86)", "LocalAppData"):
        raw = os.environ.get(env_name, "").strip()
        if raw:
            env_bases.append(Path(raw))
    script_dir = Path(__file__).resolve().parent
    local_bases = [
        script_dir,
        script_dir / "tools",
        script_dir / "bin",
        script_dir / "portable",
    ]

    sumatra = _find_existing_file(
        [base / "SumatraPDF.exe" for base in local_bases]
        + [base / "SumatraPDF" / "SumatraPDF.exe" for base in local_bases]
        + [base / "SumatraPDF-3.5.2-64.exe" for base in local_bases]
        + [base / "SumatraPDF-3.5.2-32.exe" for base in local_bases]
        + [base / "SumatraPDF" / "SumatraPDF-3.5.2-64.exe" for base in local_bases]
        + [base / "SumatraPDF" / "SumatraPDF-3.5.2-32.exe" for base in local_bases]
        + [base / "SumatraPDF" / "SumatraPDF.exe" for base in env_bases]
    )
    acro_reader = _find_existing_file(
        [
            base / "Adobe" / "Acrobat Reader DC" / "Reader" / "AcroRd32.exe"
            for base in env_bases
        ]
        + [base / "Adobe" / "Acrobat Reader" / "Reader" / "AcroRd32.exe" for base in env_bases]
    )
    acrobat = _find_existing_file(
        [base / "Adobe" / "Acrobat DC" / "Acrobat" / "Acrobat.exe" for base in env_bases]
    )
    foxit = _find_existing_file(
        [base / "Foxit Software" / "Foxit PDF Reader" / "FoxitPDFReader.exe" for base in env_bases]
    )
    commands: List[List[str]] = []
    pdf_text = str(pdf_path)
    if sumatra:
        if default_printer:
            commands.append([str(sumatra), "-silent", "-print-to", default_printer, pdf_text])
        commands.append([str(sumatra), "-silent", "-print-to-default", pdf_text])
    if acro_reader and default_printer:
        commands.append([str(acro_reader), "/N", "/T", pdf_text, default_printer])
    if acrobat and default_printer:
        commands.append([str(acrobat), "/N", "/T", pdf_text, default_printer])
    if foxit and default_printer:
        commands.append([str(foxit), "/t", pdf_text, default_printer])
    return commands


def send_pdfs_to_default_printer(
    pdf_files: List[Path],
    delay_seconds: float = 0.7,
) -> tuple[int, List[str], Path]:
    log_path = Path(__file__).resolve().with_name("impressao_automatica.log")
    log_lines: List[str] = []
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_lines.append(f"[{stamp}] Inicio da impressao automatica ({len(pdf_files)} arquivo(s))")

    sent = 0
    errors: List[str] = []

    default_printer = ""
    if os.name == "nt":
        try:
            import win32print  # type: ignore

            default_printer = str(win32print.GetDefaultPrinter() or "")
        except Exception:
            default_printer = ""
    if default_printer:
        log_lines.append(f"Impressora padrao: {default_printer}")

    for pdf_path in pdf_files:
        attempts: List[str] = []
        try:
            if not pdf_path.exists():
                msg = f"{pdf_path.name}: arquivo nao encontrado"
                errors.append(msg)
                log_lines.append(f"[ERRO] {msg}")
                continue
            if os.name != "nt" or not hasattr(os, "startfile"):
                msg = f"{pdf_path.name}: impressao automatica suportada apenas no Windows"
                errors.append(msg)
                log_lines.append(f"[ERRO] {msg}")
                continue

            printed = False

            # Metodo 1: ShellExecute (pywin32) costuma ser mais estavel.
            try:
                import win32api  # type: ignore

                rc = int(win32api.ShellExecute(0, "print", str(pdf_path), None, str(pdf_path.parent), 0))
                if rc <= 32:
                    raise RuntimeError(f"ShellExecute retornou codigo {rc}")
                printed = True
                log_lines.append(f"[OK] {pdf_path.name}: enviado via win32api.ShellExecute(print)")
            except Exception as exc:
                attempts.append(f"win32api print: {exc}")

            # Metodo 1b: ShellExecute com printto para impressora padrao.
            if not printed and default_printer:
                try:
                    import win32api  # type: ignore

                    rc = int(
                        win32api.ShellExecute(
                            0,
                            "printto",
                            str(pdf_path),
                            f'"{default_printer}"',
                            str(pdf_path.parent),
                            0,
                        )
                    )
                    if rc <= 32:
                        raise RuntimeError(f"ShellExecute printto retornou codigo {rc}")
                    printed = True
                    log_lines.append(
                        f"[OK] {pdf_path.name}: enviado via win32api.ShellExecute(printto) -> {default_printer}"
                    )
                except Exception as exc:
                    attempts.append(f"win32api printto: {exc}")

            # Metodo 2: fallback nativo do Windows.
            if not printed:
                try:
                    os.startfile(str(pdf_path), "print")
                    printed = True
                    log_lines.append(f"[OK] {pdf_path.name}: enviado via os.startfile(print)")
                except Exception as exc:
                    attempts.append(f"os.startfile print: {exc}")

            # Metodo 3: apps externos (Sumatra/Adobe/Foxit), sem depender de associacao no Windows.
            if not printed:
                external_commands = _build_external_pdf_print_commands(pdf_path, default_printer)
                if not external_commands:
                    attempts.append("apps externos: nenhum leitor PDF compativel encontrado (Sumatra/Adobe/Foxit)")
                for cmd in external_commands:
                    try:
                        proc = subprocess.run(
                            cmd,
                            capture_output=True,
                            text=True,
                            timeout=45,
                            check=False,
                        )
                        rc = int(proc.returncode)
                        if rc in {0, 1}:
                            printed = True
                            log_lines.append(
                                f"[OK] {pdf_path.name}: enviado via comando externo -> {' '.join(cmd[:2])} (rc={rc})"
                            )
                            break
                        stderr_text = (proc.stderr or "").strip()
                        attempts.append(
                            f"cmd externo rc={rc}: {' '.join(cmd[:2])} {stderr_text[:180]}".strip()
                        )
                    except Exception as exc:
                        attempts.append(f"cmd externo {' '.join(cmd[:2])}: {exc}")

            if printed:
                sent += 1
                if delay_seconds > 0:
                    time.sleep(delay_seconds)
            else:
                detail = " | ".join(attempts) if attempts else "sem detalhe"
                msg = f"{pdf_path.name}: falha ao enviar ({detail})"
                errors.append(msg)
                log_lines.append(f"[ERRO] {msg}")
        except Exception as exc:
            msg = f"{pdf_path.name}: erro inesperado ({exc})"
            errors.append(msg)
            log_lines.append(f"[ERRO] {msg}")

    log_lines.append(f"Resumo: enviados={sent}, erros={len(errors)}")
    try:
        with log_path.open("a", encoding="utf-8") as fh:
            fh.write("\n".join(log_lines))
            fh.write("\n" + ("-" * 80) + "\n")
    except Exception:
        pass

    return sent, errors, log_path


def shutdown_computer_windows() -> None:
    if os.name != "nt":
        raise RuntimeError("Desligamento automatico suportado apenas no Windows.")
    proc = subprocess.run(
        ["shutdown", "/s", "/t", "0"],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    if int(proc.returncode) != 0:
        stderr = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(stderr or f"Comando shutdown retornou codigo {proc.returncode}.")


def is_a5_pair_profile(profile_name: str) -> bool:
    return normalize_plate_format(profile_name, default="A4") == "A5"


def merge_two_plate_pdfs_side_by_side(
    left_pdf: Path,
    right_pdf: Optional[Path],
    output_pdf: Path,
    gap_mm: float = 0.0,
) -> None:
    try:
        from pypdf import PageObject, PdfReader, PdfWriter, Transformation  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "Dependencia ausente: pypdf. Instale com `pip install -r requirements.txt`."
        ) from exc

    left_reader = PdfReader(str(left_pdf))
    if not left_reader.pages:
        raise RuntimeError(f"PDF sem paginas: {left_pdf}")
    left_page = left_reader.pages[0]
    left_w = float(left_page.mediabox.width)
    left_h = float(left_page.mediabox.height)

    right_page = None
    right_w = 0.0
    right_h = 0.0
    if right_pdf:
        right_reader = PdfReader(str(right_pdf))
        if right_reader.pages:
            right_page = right_reader.pages[0]
            right_w = float(right_page.mediabox.width)
            right_h = float(right_page.mediabox.height)

    # Permite gap negativo para aproximar/encostar mais as placas.
    gap_points = float(gap_mm) * 72.0 / 25.4
    target_w = left_w + (gap_points + right_w if right_page is not None else 0.0)
    target_w = max(left_w, target_w)
    target_h = max(left_h, right_h)
    out_page = PageObject.create_blank_page(width=target_w, height=target_h)

    left_y = max(target_h - left_h, 0.0)
    out_page.merge_transformed_page(left_page, Transformation().translate(tx=0.0, ty=left_y))

    if right_page is not None:
        right_x = left_w + gap_points
        right_y = max(target_h - right_h, 0.0)
        out_page.merge_transformed_page(right_page, Transformation().translate(tx=right_x, ty=right_y))

    writer = PdfWriter()
    writer.add_page(out_page)
    output_pdf.parent.mkdir(parents=True, exist_ok=True)
    with output_pdf.open("wb") as fh:
        writer.write(fh)


def merge_pdf_sequence(
    input_pdfs: List[Path],
    output_pdf: Path,
) -> None:
    try:
        from pypdf import PdfReader, PdfWriter  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "Dependencia ausente: pypdf. Instale com `pip install -r requirements.txt`."
        ) from exc

    writer = PdfWriter()
    added_pages = 0
    for pdf_path in input_pdfs:
        reader = PdfReader(str(pdf_path))
        for page in reader.pages:
            writer.add_page(page)
            added_pages += 1
    if added_pages <= 0:
        raise RuntimeError("Nenhuma pagina valida foi encontrada para montar o PDF.")
    output_pdf.parent.mkdir(parents=True, exist_ok=True)
    with output_pdf.open("wb") as fh:
        writer.write(fh)


def _estimate_grouped_output_files(job_entries: List[Dict[str, Any]]) -> int:
    total = 0
    pending_a5_duplex: Optional[bool] = None
    pending_a6_duplex: Optional[bool] = None
    pending_a6_count = 0

    for job in job_entries:
        plate_format = normalize_plate_format(str(job.get("plate_format", "A4")), default="A4")
        duplex_enabled = bool(job.get("duplex_enabled"))
        if plate_format == "A4":
            total += 1
            continue
        if plate_format == "A5":
            if pending_a5_duplex is None:
                pending_a5_duplex = duplex_enabled
            elif pending_a5_duplex == duplex_enabled:
                total += 1
                pending_a5_duplex = None
            else:
                total += 1
                pending_a5_duplex = duplex_enabled
            continue

        if pending_a6_count <= 0:
            pending_a6_duplex = duplex_enabled
            pending_a6_count = 1
            continue
        if pending_a6_duplex == duplex_enabled and pending_a6_count < 4:
            pending_a6_count += 1
            if pending_a6_count == 4:
                total += 1
                pending_a6_count = 0
                pending_a6_duplex = None
        else:
            total += 1
            pending_a6_duplex = duplex_enabled
            pending_a6_count = 1

    if pending_a5_duplex is not None:
        total += 1
    if pending_a6_count > 0:
        total += 1
    return total


def build_print_job_pdfs(
    output_records: List[OutputPdfRecord],
    temp_dir: Path,
    duplex_enabled: bool = False,
) -> List[Path]:
    if not duplex_enabled:
        return [record.path for record in output_records]

    print_jobs: List[Path] = []
    pending_duplex_records: List[OutputPdfRecord] = []
    temp_dir.mkdir(parents=True, exist_ok=True)

    def _flush_pending() -> None:
        if not pending_duplex_records:
            return
        if len(pending_duplex_records) == 1:
            print_jobs.append(pending_duplex_records[0].path)
            pending_duplex_records.clear()
            return
        names: List[str] = []
        for record in pending_duplex_records:
            names.extend(record.plate_names or [record.path.stem])
        raw_name = sanitize_filename(" + ".join(names[:2]) or "frente-verso", "frente-verso")
        merged_path = temp_dir / f"{raw_name}.pdf"
        seq = 2
        while merged_path.exists():
            merged_path = temp_dir / f"{raw_name} ({seq}).pdf"
            seq += 1
        merge_pdf_sequence([record.path for record in pending_duplex_records], merged_path)
        print_jobs.append(merged_path)
        pending_duplex_records.clear()

    for record in output_records:
        if record.duplex_enabled:
            pending_duplex_records.append(record)
            continue
        _flush_pending()
        print_jobs.append(record.path)
    _flush_pending()
    return print_jobs


def _extract_normalized_price_number(value: str) -> str:
    text = (value or "").replace("\u00a0", " ").strip()
    if not text:
        return ""

    # Caso comum do PDF: "R$ 2 9,98" -> "29,98".
    split_match = PRICE_SPLIT_INTEGER_DECIMAL_RE.search(text)
    if split_match:
        integer_part = re.sub(r"\s+", "", split_match.group(1) or "").translate(OCR_PRICE_DIGIT_MAP)
        cents_part = (split_match.group(3) or "").translate(OCR_PRICE_DIGIT_MAP)
        if re.fullmatch(r"-?\d+", integer_part or "") and re.fullmatch(r"\d{2}", cents_part or ""):
            return f"{integer_part},{cents_part}"

    fuzzy_match = PRICE_FUZZY_DECIMAL_RE.search(text)
    if fuzzy_match:
        integer_part = re.sub(r"[.\s]", "", fuzzy_match.group(1) or "").translate(OCR_PRICE_DIGIT_MAP)
        cents_part = (fuzzy_match.group(3) or "").translate(OCR_PRICE_DIGIT_MAP)
        if re.fullmatch(r"-?\d+", integer_part or "") and re.fullmatch(r"\d{2}", cents_part or ""):
            return f"{integer_part},{cents_part}"

    direct_match = PRICE_NUMBER_RE.search(text)
    if direct_match:
        return direct_match.group(0).replace(" ", "")

    return ""


def _extract_price_from_adjacent_integer_cents(integer_value: str, cents_value: str) -> str:
    integer_raw = (integer_value or "").replace("\u00a0", " ").strip()
    cents_raw = (cents_value or "").replace("\u00a0", " ").strip()
    # Aceita apenas inteiro "puro" (sem separador decimal) no primeiro trecho.
    if re.search(r"[,.]", integer_raw):
        return ""
    int_part = integer_raw.translate(OCR_PRICE_DIGIT_MAP)
    cent_part = cents_raw.translate(OCR_PRICE_DIGIT_MAP)
    int_part = re.sub(r"[^\d\-]", "", int_part)
    cent_part = re.sub(r"[^\d]", "", cent_part)
    if not re.fullmatch(r"-?\d{1,6}", int_part or ""):
        return ""
    # Centavos devem ser 1-2 digitos; rejeita textos longos (ex.: datas).
    if len(cent_part) == 1:
        cent_part = f"{cent_part}0"
    if len(cent_part) != 2:
        return ""
    return f"{int_part},{cent_part}"


def _extract_price_from_prefix_one_and_decimal(prefix_value: str, decimal_value: str) -> str:
    """ReconstrÃ³i casos como '2' + '9,98' => '29,98'."""
    prefix = (prefix_value or "").replace("\u00a0", " ").strip().translate(OCR_PRICE_DIGIT_MAP)
    prefix = re.sub(r"[^\d]", "", prefix)
    if not re.fullmatch(r"[1-9]", prefix or ""):
        return ""

    decimal_price = _extract_normalized_price_number(decimal_value)
    if not decimal_price:
        return ""
    m = re.fullmatch(r"(-?\d+)[,.](\d{2})", decimal_price)
    if not m:
        return ""
    integer_part, cents_part = m.group(1), m.group(2)
    if integer_part.startswith("-"):
        return ""
    if len(integer_part) != 1:
        return ""
    return f"{prefix}{integer_part},{cents_part}"


def _is_currency_only_cell(value: str) -> bool:
    return bool(re.fullmatch(r"r\$\s*", (value or "").strip(), flags=re.IGNORECASE))


def _row_has_currency_near_index(entries: List[tuple[str, str]], index: int, distance: int = 2) -> bool:
    if index < 0 or index >= len(entries):
        return False
    start = max(0, index - distance)
    end = min(len(entries) - 1, index + distance)
    for pos in range(start, end + 1):
        if _is_currency_only_cell(entries[pos][1]):
            return True
    return False


def select_header_row_index(rows: List[List[str]]) -> int:
    if not rows:
        return 0

    keywords = {
        "descricao",
        "produto",
        "oferta",
        "preco",
        "codigo",
        "cod",
        "valor",
        "r",
    }
    max_scan = min(len(rows), 6)
    best_idx = 0
    best_score = -1
    for idx in range(max_scan):
        row = rows[idx]
        norm_cells = [normalize_key(c) for c in row if (c or "").strip()]
        if not norm_cells:
            continue

        score = 0
        for cell in norm_cells:
            parts = [p for p in cell.split("_") if p]
            if any(part in keywords for part in parts):
                score += 2
            if len(parts) >= 2:
                score += 1
        if len(norm_cells) >= 3:
            score += 2

        if score > best_score:
            best_score = score
            best_idx = idx
    return best_idx


def parse_tables_from_pdf(pdf_path: Path) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            before_count = len(rows)
            tables = page.extract_tables() or []
            for table in tables:
                if not table:
                    continue

                non_empty_rows = [
                    [("" if c is None else str(c)).strip() for c in row]
                    for row in table
                    if row and any((c or "").strip() for c in row)
                ]
                if not non_empty_rows:
                    continue

                header_idx = select_header_row_index(non_empty_rows)
                header = [normalize_key(c) for c in non_empty_rows[header_idx]]
                header_is_usable = any(header)
                data_start = header_idx + 1 if header_is_usable else 0

                for raw_row in non_empty_rows[data_start:]:
                    row_map: Dict[str, str] = {}
                    for idx, cell in enumerate(raw_row):
                        base_key = header[idx] if idx < len(header) and header[idx] else f"col_{idx+1}"
                        key = base_key
                        if key in row_map:
                            suffix = 2
                            while f"{base_key}_{suffix}" in row_map:
                                suffix += 1
                            key = f"{base_key}_{suffix}"
                        row_map[key] = cell
                    rows.append(row_map)
            # Fallback: quando a tabela nao e detectada corretamente (ex.: coluna "R$" + valor),
            # tenta extrair os precos diretamente do texto da pagina.
            if len(rows) == before_count:
                rows.extend(_extract_price_rows_from_page_text(page))
    return rows


def _extract_price_rows_from_page_text(page) -> List[Dict[str, str]]:
    try:
        text = page.extract_text() or ""
    except Exception:
        text = ""
    text = text.replace("\u00a0", " ")
    if not text.strip():
        return []

    prices: List[str] = []
    # Captura valores apos "R$" mesmo quando o numero vem em outra coluna/linha.
    for currency_match in re.finditer(r"R\s*\$", text, flags=re.IGNORECASE):
        snippet = text[currency_match.end() : currency_match.end() + 32]
        value = _extract_normalized_price_number(snippet)
        if value:
            prices.append(value)

    # Se nao encontrou com "R$", tenta qualquer valor decimal no texto da pagina.
    if not prices:
        for fuzzy_match in PRICE_FUZZY_DECIMAL_RE.finditer(text):
            value = _extract_normalized_price_number(fuzzy_match.group(0))
            if value:
                prices.append(value)

    # Fallback final para regex de preco tradicional.
    if not prices:
        for match in re.finditer(
            r"(?<!\d)(-?\d{1,3}(?:\.\d{3})*(?:,\d{2})|-?\d+(?:,\d{2})|-?\d+(?:\.\d{2}))(?!\d)",
            text,
        ):
            value = _extract_normalized_price_number(match.group(1) or "")
            if value:
                prices.append(value)

    if not prices:
        return []

    extracted_rows: List[Dict[str, str]] = []
    for value in prices:
        extracted_rows.append({"preco": value})
    return extracted_rows


def detect_column(rows: List[Dict[str, str]], requested: Optional[str], candidates: List[str]) -> Optional[str]:
    if not rows:
        return None

    seen_keys: set[str] = set()
    norm_pairs: List[tuple[str, str]] = []
    for row in rows:
        for key in row.keys():
            if not key or key in seen_keys:
                continue
            seen_keys.add(key)
            norm_pairs.append((normalize_key(key), key))

    column_score_cache: Dict[str, tuple[int, int, int, int]] = {}

    def _column_metrics(key: str) -> tuple[int, int, int, int]:
        cached = column_score_cache.get(key)
        if cached is not None:
            return cached
        non_empty = 0
        price_like = 0
        text_like = 0
        currency_only = 0
        for row in rows:
            value = str(row.get(key, "") or "").strip()
            if not value:
                continue
            non_empty += 1
            if _extract_normalized_price_number(value):
                price_like += 1
            if bool(re.search(r"[A-Za-z\u00c0-\u00ff]", value)):
                text_like += 1
            if bool(re.fullmatch(r"r\$\s*", value, flags=re.IGNORECASE)):
                currency_only += 1
        metrics = (non_empty, price_like, text_like, currency_only)
        column_score_cache[key] = metrics
        return metrics

    def _pick_best(candidates: List[str]) -> Optional[str]:
        if not candidates:
            return None
        unique_candidates: List[str] = list(dict.fromkeys(candidates))
        # Prioriza colunas com mais valores de preco reais e penaliza colunas "R$" puras.
        return max(
            unique_candidates,
            key=lambda orig: (
                (_column_metrics(orig)[1] * 8)
                + (_column_metrics(orig)[0] * 2)
                - (_column_metrics(orig)[3] * 3),
                _column_metrics(orig)[2],
                -len(normalize_key(orig)),
            ),
        )

    def _fuzzy_pick(norm_term: str) -> Optional[str]:
        if not norm_term:
            return None
        exact = [orig for nk, orig in norm_pairs if nk == norm_term]
        starts = [orig for nk, orig in norm_pairs if nk.startswith(norm_term)]
        if exact or starts:
            best_exact_or_starts = _pick_best(exact + starts)
            if best_exact_or_starts:
                return best_exact_or_starts
        contains = [orig for nk, orig in norm_pairs if norm_term in nk]
        if contains:
            best_contains = _pick_best(contains)
            if best_contains:
                return best_contains
        token_overlap: List[tuple[int, int, str]] = []
        term_tokens = [t for t in norm_term.split("_") if t]
        if term_tokens:
            for nk, orig in norm_pairs:
                nk_tokens = [t for t in nk.split("_") if t]
                overlap = sum(1 for t in term_tokens if t in nk_tokens)
                if overlap > 0:
                    token_overlap.append((overlap, -len(nk), orig))
            if token_overlap:
                overlap_keys = [item[2] for item in sorted(token_overlap, reverse=True)]
                best_overlap = _pick_best(overlap_keys)
                if best_overlap:
                    return best_overlap
        return None

    if requested:
        req = normalize_key(requested)
        requested_match = _fuzzy_pick(req)
        if requested_match:
            return requested_match

    for cand in candidates:
        cand_norm = normalize_key(cand)
        cand_match = _fuzzy_pick(cand_norm)
        if cand_match:
            return cand_match
    return None


def to_price_text(value: str, prefix: str) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    normalized_candidate = _extract_normalized_price_number(value)
    if normalized_candidate:
        value = normalized_candidate
    value = value.replace("R$", "").replace("$", "").strip()

    match = PRICE_NUMBER_RE.search(value)
    if not match:
        return f"{prefix}{value}".strip()

    num = match.group(0)
    if "," in num:
        normalized = num.replace(".", "").replace(",", ".")
    else:
        normalized = num

    try:
        parsed = float(normalized)
        output = f"{parsed:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"{prefix}{output}".strip()
    except ValueError:
        return f"{prefix}{num}".strip()


def _resolve_row_price_value(row: Dict[str, str], picked_price: Optional[str]) -> str:
    if not row:
        return ""

    entries = [(k, (v or "").strip()) for k, v in row.items()]
    if not entries:
        return ""

    picked_idx: Optional[int] = None
    if picked_price:
        for idx, (key, _value) in enumerate(entries):
            if key == picked_price:
                picked_idx = idx
                break

    # Regra direta: se a propria celula selecionada tiver "R$ <numero>",
    # usa exatamente esse valor (ex.: "R$ 29,98" -> "29,98").
    if picked_idx is not None:
        picked_cell_value = entries[picked_idx][1]
        if re.search(r"r\s*\$\s*-?\d", picked_cell_value, flags=re.IGNORECASE):
            direct_from_currency_cell = _extract_normalized_price_number(picked_cell_value)
            if direct_from_currency_cell:
                return direct_from_currency_cell

    picked_norm = normalize_key(picked_price or "")

    def _score_entry(idx: int, key: str) -> int:
        key_norm = normalize_key(key)
        score = 0
        if picked_norm and key_norm == picked_norm:
            score += 90
        if "oferta" in key_norm:
            score += 70
        if "promo" in key_norm or "promoc" in key_norm:
            score += 55
        if "preco" in key_norm or "valor" in key_norm:
            score += 20
        if picked_idx is not None:
            if idx == picked_idx:
                score += 40
            if abs(idx - picked_idx) == 1:
                score += 35
            elif abs(idx - picked_idx) == 2:
                score += 15
        return score

    def _price_integer_len(price_text: str) -> int:
        m = re.match(r"\s*(-?\d+)[,.]\d{2}\s*$", price_text or "")
        if not m:
            return 0
        return len(m.group(1).lstrip("-"))

    def _base_key(key: str) -> str:
        return re.sub(r"_\d+$", "", normalize_key(key))

    all_candidates: List[tuple[int, int, str]] = []

    def _push_candidate(score: int, idx_ref: int, raw_price: str) -> None:
        normalized = _extract_normalized_price_number(raw_price)
        if normalized:
            all_candidates.append((score, idx_ref, normalized))

    def _choose_best(candidates: List[tuple[int, int, str]]) -> Optional[str]:
        if not candidates:
            return None
        best = max(candidates, key=lambda item: (item[0], _price_integer_len(item[2]), item[1]))
        return best[2]

    currency_indices = [idx for idx, (_key, value) in enumerate(entries) if _is_currency_only_cell(value)]
    oferta_currency_indices = [
        idx for idx in currency_indices if "oferta" in normalize_key(entries[idx][0])
    ]

    # Fase 1: tentativa ancorada perto da coluna OFERTA R$ (mais confiavel para o seu layout).
    anchor_currency_idx: Optional[int] = None
    if picked_idx is not None:
        picked_key_norm = normalize_key(entries[picked_idx][0])
        if _is_currency_only_cell(entries[picked_idx][1]):
            anchor_currency_idx = picked_idx
        else:
            preferred_pool = oferta_currency_indices or currency_indices
            if preferred_pool:
                anchor_currency_idx = min(preferred_pool, key=lambda pos: (abs(pos - picked_idx), pos))

        # Se a coluna escolhida for da oferta, prioriza ancora em coluna de moeda da oferta.
        if "oferta" in picked_key_norm and oferta_currency_indices:
            anchor_currency_idx = min(oferta_currency_indices, key=lambda pos: (abs(pos - picked_idx), pos))
    elif oferta_currency_indices:
        # Sem coluna escolhida, tenta ancora pela moeda da oferta.
        anchor_currency_idx = oferta_currency_indices[0]
    elif currency_indices:
        anchor_currency_idx = currency_indices[0]

    if anchor_currency_idx is not None:
        anchored_candidates: List[tuple[int, int, str]] = []
        for idx in range(anchor_currency_idx + 1, min(len(entries), anchor_currency_idx + 5)):
            key, value = entries[idx]
            direct_price = _extract_normalized_price_number(value)
            if direct_price:
                anchored_candidates.append((_score_entry(idx, key) + 90 - ((idx - anchor_currency_idx) * 6), idx, direct_price))

            for step in (1, 2):
                right_idx = idx + step
                if right_idx >= len(entries):
                    continue
                merged_price = _extract_price_from_prefix_one_and_decimal(entries[idx][1], entries[right_idx][1])
                if not merged_price and step == 1:
                    merged_price = _extract_price_from_adjacent_integer_cents(entries[idx][1], entries[right_idx][1])
                if merged_price:
                    left_key = entries[idx][0]
                    right_key = entries[right_idx][0]
                    key_related = _base_key(left_key) == _base_key(right_key)
                    if key_related or idx == (anchor_currency_idx + 1):
                        anchored_candidates.append(
                            (
                                _score_entry(idx, left_key)
                                + _score_entry(right_idx, right_key)
                                + 150
                                - ((idx - anchor_currency_idx) * 8),
                                right_idx,
                                merged_price,
                            )
                        )

        best_anchored = _choose_best(anchored_candidates)
        if best_anchored:
            return best_anchored

    # Fase 2: fallback geral.
    for idx, (key, value) in enumerate(entries):
        if not value:
            continue
        direct_price = _extract_normalized_price_number(value)
        if direct_price:
            score = _score_entry(idx, key)
            if _row_has_currency_near_index(entries, idx, distance=2):
                score += 25
            _push_candidate(score, idx, direct_price)

    for idx in range(len(entries) - 1):
        merged_price = _extract_price_from_prefix_one_and_decimal(entries[idx][1], entries[idx + 1][1])
        if not merged_price:
            merged_price = _extract_price_from_adjacent_integer_cents(entries[idx][1], entries[idx + 1][1])
        if not merged_price:
            continue
        pair_score = _score_entry(idx, entries[idx][0]) + _score_entry(idx + 1, entries[idx + 1][0]) + 40
        if _row_has_currency_near_index(entries, idx + 1, distance=2):
            pair_score += 20
        _push_candidate(pair_score, idx + 1, merged_price)

    best_general = _choose_best(all_candidates)
    if best_general:
        return best_general

    return ""


def split_price_parts(value: str) -> tuple[str, str]:
    cleaned = (value or "").strip()
    if not cleaned:
        return "0", "00"
    cleaned = cleaned.replace("R$", "").replace("$", "").strip()
    cleaned = cleaned.replace(".", "").replace(",", ".")
    try:
        num = float(cleaned)
        integer = int(abs(num))
        cents = int(round((abs(num) - integer) * 100))
        if cents == 100:
            integer += 1
            cents = 0
        return str(integer), f"{cents:02d}"
    except Exception:
        m = re.search(r"(\d+)[.,](\d{2})", value)
        if m:
            return m.group(1), m.group(2)
        m = re.search(r"(\d+)", value)
        if m:
            return m.group(1), "00"
        return "0", "00"


def build_plate_rows(
    rows: List[Dict[str, str]],
    name_col: Optional[str],
    price_col: Optional[str],
    price_prefix: str,
    default_plate_format: str = "A4",
    use_ollama_cleanup: bool = False,
    ollama_model: str = DEFAULT_OLLAMA_MODEL,
    ollama_timeout_seconds: float = DEFAULT_OLLAMA_TIMEOUT_SECONDS,
    ollama_max_items: int = DEFAULT_OLLAMA_MAX_ITEMS,
    allow_web_lookup: bool = True,
) -> List[PlateData]:
    if not rows:
        return []

    picked_name = detect_column(
        rows,
        name_col,
        ["descricao", "descricao_produto", "nome", "produto", "item", "servico", "name", "product"],
    )
    picked_price = detect_column(
        rows,
        price_col,
        ["oferta_r", "oferta", "preco", "valor", "price", "preco_promocional", "vlr", "venda"],
    )

    plate_rows: List[PlateData] = []
    ollama_used_count = 0
    ollama_limit = max(0, int(ollama_max_items))
    for i, row in enumerate(rows, start=1):
        fallback_name = row.get("col_1", "").strip()
        fallback_price = row.get("col_2", "").strip()

        name_value = (row.get(picked_name, "") if picked_name else fallback_name).strip()
        price_raw = (row.get(picked_price, "") if picked_price else fallback_price).strip()

        # Fallback inteligente para evitar "Item X" quando ha descricao em outra coluna.
        if not name_value:
            best_text = ""
            for cell in row.values():
                cell = (cell or "").strip()
                if not cell:
                    continue
                cell_norm = normalize_key(cell)
                if not re.search(r"[A-Za-z\u00c0-\u00ff]", cell):
                    continue
                if re.search(r"r\$\s*\d", cell.lower()):
                    continue
                if re.fullmatch(r"\d+([.,]\d+)?", cell.replace(" ", "")):
                    continue
                if len(cell_norm) > len(normalize_key(best_text)):
                    best_text = cell
            name_value = best_text.strip()

        normalized_price_raw = _extract_normalized_price_number(price_raw)
        if normalized_price_raw:
            price_raw = normalized_price_raw

        resolved_price = _resolve_row_price_value(row, picked_price)
        if resolved_price:
            price_raw = resolved_price

        if not name_value and not price_raw:
            continue

        if not name_value:
            name_value = f"Item {i}"
        original_name_value = re.sub(r"\s+", " ", str(name_value or "").strip())
        name_value = correct_accents_in_text(name_value)
        has_cleanup_learning = has_product_name_cleanup_learning(
            name_value,
            original_name=original_name_value,
        )
        allow_ollama_for_item = (
            bool(use_ollama_cleanup)
            and not has_cleanup_learning
            and ollama_used_count < ollama_limit
            and _should_request_name_cleanup(name_value)
        )
        if allow_ollama_for_item:
            print_status(
                "AI",
                f"Revisando nome {ollama_used_count + 1}/{ollama_limit}: {sanitize_filename(name_value, f'item-{i}')}",
            )
            ollama_used_count += 1
        name_value, learned_unit, _cleanup_source = cleanup_product_name_with_learning(
            name_value,
            row=row,
            original_name=original_name_value,
            use_ollama=allow_ollama_for_item,
            ollama_model=ollama_model,
            ollama_timeout_seconds=ollama_timeout_seconds,
            allow_web_lookup=allow_web_lookup,
        )
        inferred_unit = learned_unit or infer_unit_label(name_value, row=row, default="KG")
        row_with_meta = dict(row)
        row_with_meta["_plate_original_name"] = original_name_value or name_value
        row_with_meta["_plate_cleanup_source"] = _cleanup_source

        plate_rows.append(
            PlateData(
                index=i,
                name=name_value,
                price=to_price_text(price_raw, price_prefix) if price_raw else "",
                row=row_with_meta,
                original_name=original_name_value or name_value,
                cleanup_source=_cleanup_source,
                unit_label=inferred_unit,
                plate_format=normalize_plate_format(default_plate_format, default="A4"),
                format_quantities=normalize_format_quantities(
                    None,
                    default_format=default_plate_format,
                    default_qty=1,
                ),
            )
        )
    return plate_rows


def configure_plates_via_web(
    items: List[PlateData],
    price_prefix: str = "R$ ",
    default_auto_print: bool = False,
    default_duplex_print: bool = False,
    default_shutdown_after_print: bool = False,
    ollama_cleanup_enabled: bool = False,
    ollama_model: str = DEFAULT_OLLAMA_MODEL,
    ollama_timeout_seconds: float = DEFAULT_OLLAMA_TIMEOUT_SECONDS,
    allow_web_lookup: bool = True,
    template_options: Optional[List[Path]] = None,
    template_defaults: Optional[Dict[str, Optional[Path]]] = None,
    template_selection_holder: Optional[Dict[str, Optional[Path]]] = None,
) -> Optional[tuple[List[PlateData], bool, bool, bool]]:
    if not items:
        return []

    _shutdown_active_web_progress_session()
    progress_tracker = GenerationProgressTracker()

    today = date.today()
    default_month_max_day = _max_day_in_current_month(today)
    initial_rows: List[Dict[str, Any]] = []
    for source_idx, item in enumerate(items):
        initial_month = int(item.offer_validity_month or today.month)
        if initial_month < 1 or initial_month > 12:
            initial_month = today.month
        initial_max_day = _max_day_in_month(today.year, initial_month)
        initial_day = int(item.offer_validity_day or today.day)
        if initial_day < 1 or initial_day > initial_max_day:
            initial_day = min(today.day, initial_max_day)
        format_quantities = normalize_format_quantities(
            item.format_quantities,
            default_format=item.plate_format,
            default_qty=item.quantity,
        )
        initial_rows.append(
            {
                "source_idx": source_idx,
                "index": item.index,
                "enabled": True,
                "name": item.name,
                "original_name": _resolve_plate_original_name(item),
                "price": item.price,
                "qty_a4": int(format_quantities.get("A4", 0)),
                "qty_a5": int(format_quantities.get("A5", 0)),
                "qty_a6": int(format_quantities.get("A6", 0)),
                "unit": normalize_unit_label(item.unit_label),
                "cleanup_source": str(getattr(item, "cleanup_source", "") or "manual"),
                "duplex_enabled": bool(item.duplex_enabled),
                "offer_validity_enabled": bool(item.offer_validity_enabled),
                "offer_validity_day": initial_day,
                "offer_validity_month": initial_month,
            }
        )

    done_event = threading.Event()
    rows_lock = threading.Lock()
    ai_state: Dict[str, Any] = {
        "enabled": bool(ollama_cleanup_enabled),
        "running": False,
        "done": not bool(ollama_cleanup_enabled),
        "processed": 0,
        "total": 0,
        "current": "",
        "phase": "idle",
        "label": "IA inativa" if not bool(ollama_cleanup_enabled) else "Aguardando correcao",
        "detail": "",
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    result_holder: Dict[str, Any] = {
        "items": None,
        "error": None,
        "auto_print": bool(default_auto_print),
        "duplex_print": bool(default_duplex_print),
        "shutdown_after_print": bool(default_shutdown_after_print),
    }

    default_template_paths: Dict[str, str] = {"A4": "", "A5": "", "A6": ""}
    if isinstance(template_defaults, dict):
        for fmt in ("A4", "A5", "A6"):
            raw_path = template_defaults.get(fmt)
            if raw_path:
                default_template_paths[fmt] = str(Path(raw_path).expanduser().resolve())

    options_payload: List[Dict[str, str]] = []
    for candidate in template_options or []:
        try:
            resolved = candidate.expanduser().resolve()
        except Exception:
            continue
        if not resolved.exists() or resolved.suffix.lower() != ".cdr":
            continue
        options_payload.append(
            {
                "path": str(resolved),
                "label": f"{resolved.name} - {resolved.parent}",
            }
        )

    users_lock = threading.Lock()
    sessions_lock = threading.Lock()
    auth_sessions: Dict[str, Dict[str, Any]] = {}
    users_data, users_data_source = load_users_database()
    if not users_data:
        raise RuntimeError(
            "Nenhum usuario ativo encontrado em usuarios.json. "
            "Cadastre ao menos um usuario no repositorio ou no cache local."
        )
    users_data = sorted(users_data, key=lambda entry: normalize_key(str(entry.get("usuario") or "")))
    print_status("WEB", f"Base de usuarios carregada ({users_data_source}): {len(users_data)} registro(s).")

    html_page_template = load_web_config_page_html()

    def _as_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        text = str(value or "").strip().lower()
        return text in {"1", "true", "sim", "yes", "on"}

    def _snapshot_rows() -> List[Dict[str, Any]]:
        with rows_lock:
            return [dict(row) for row in initial_rows]

    def _snapshot_ai_state() -> Dict[str, Any]:
        with rows_lock:
            return dict(ai_state)

    def _auth_payload_for_user(user: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        public_user = build_public_user_payload(user)
        role_value = str(public_user.get("perfil") or "") if public_user else ROLE_PLATE_GENERATOR
        return {
            "logged_in": bool(public_user),
            "user": public_user,
            "permissions": get_user_role_permissions(role_value),
            "roles": list(VALID_USER_ROLES),
        }

    def _render_web_page(auth_user: Optional[Dict[str, Any]]) -> str:
        initial_auth = _auth_payload_for_user(auth_user)
        body_auth_class = "" if initial_auth.get("logged_in") else "auth-locked"
        login_overlay_class = "login-overlay hidden" if initial_auth.get("logged_in") else "login-overlay"
        page_html = html_page_template
        page_html = page_html.replace("__BODY_AUTH_CLASS__", body_auth_class)
        page_html = page_html.replace("__LOGIN_OVERLAY_CLASS__", login_overlay_class)
        page_html = page_html.replace(
            "__INITIAL_AUTH_JSON__",
            json.dumps(initial_auth, ensure_ascii=False),
        )
        return page_html

    def _sanitize_users_for_listing() -> List[Dict[str, Any]]:
        with users_lock:
            return [dict(build_public_user_payload(entry) or {}) for entry in users_data]

    def _reload_users_data_from_remote_if_available() -> tuple[bool, str]:
        try:
            refreshed_users, refreshed_source = load_users_database()
        except Exception:
            return False, "error"
        if not refreshed_users:
            return False, refreshed_source
        refreshed_users = sorted(refreshed_users, key=lambda entry: normalize_key(str(entry.get("usuario") or "")))
        with users_lock:
            users_data.clear()
            users_data.extend(refreshed_users)
        return True, refreshed_source

    def _load_remembered_login_user_if_valid() -> tuple[Optional[Dict[str, Any]], int]:
        with users_lock:
            snapshot_users = [dict(item) for item in users_data]
        remembered_user, remaining_seconds = load_login_session_24h_valid(snapshot_users)
        return remembered_user, remaining_seconds

    def _find_user_by_username(username: str) -> Optional[Dict[str, Any]]:
        user_key = normalize_key(username)
        if not user_key:
            return None
        with users_lock:
            for entry in users_data:
                if normalize_key(str(entry.get("usuario") or "")) == user_key:
                    return dict(entry)
        return None

    def _create_session_for_user(user: Dict[str, Any], max_age_seconds: int = USERS_SESSION_MAX_AGE_SECONDS) -> str:
        session_token = secrets.token_urlsafe(32)
        now_ts = time.time()
        safe_max_age = max(60, int(max_age_seconds or USERS_SESSION_MAX_AGE_SECONDS))
        session_payload = {
            "token": session_token,
            "usuario": _normalize_auth_username(user.get("usuario") or ""),
            "created_at": now_ts,
            "updated_at": now_ts,
            "max_age_seconds": safe_max_age,
        }
        with sessions_lock:
            auth_sessions[session_token] = session_payload
        return session_token

    if ollama_cleanup_enabled:
        review_candidates: List[tuple[int, PlateData, str]] = []
        for source_idx, item in enumerate(items):
            original_name = _resolve_plate_original_name(item)
            current_name = re.sub(r"\s+", " ", str(item.name or "").strip())
            if has_product_name_cleanup_learning(current_name, original_name=original_name):
                continue
            if not (_should_request_name_cleanup(current_name) or _needs_web_lookup_hint(current_name)):
                continue
            review_candidates.append((source_idx, item, original_name))

        with rows_lock:
            ai_state["running"] = bool(review_candidates)
            ai_state["done"] = not bool(review_candidates)
            ai_state["total"] = len(review_candidates)
            ai_state["processed"] = 0
            ai_state["current"] = ""
            ai_state["phase"] = "queue_ready" if review_candidates else "idle"
            ai_state["label"] = "Fila pronta" if review_candidates else "Sem correcao pendente"
            ai_state["detail"] = (
                f"{len(review_candidates)} item(ns) aguardando analise inteligente."
                if review_candidates
                else "Nenhum item precisou de correcao automatica."
            )
            ai_state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        def _background_ai_cleanup() -> None:
            for processed_idx, (source_idx, item, original_name) in enumerate(review_candidates, start=1):
                if done_event.is_set():
                    break
                display_name = re.sub(r"\s+", " ", str(item.name or "").strip())
                with rows_lock:
                    ai_state["running"] = True
                    ai_state["current"] = display_name
                    ai_state["phase"] = "analyzing"
                    ai_state["label"] = "Analisando item"
                    ai_state["detail"] = f"Avaliando o nome '{display_name}' antes da correcao."
                    ai_state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print_status(
                    "AI",
                    f"Corrigindo {processed_idx}/{len(review_candidates)}: {sanitize_filename(display_name, f'item-{source_idx + 1}')}",
                )
                with rows_lock:
                    ai_state["phase"] = "researching" if allow_web_lookup and _needs_web_lookup_hint(display_name) else "correcting"
                    ai_state["label"] = "Pesquisando na web" if ai_state["phase"] == "researching" else "Corrigindo nome"
                    ai_state["detail"] = (
                        f"Buscando pistas para completar o nome '{display_name}'."
                        if ai_state["phase"] == "researching"
                        else f"Aplicando correcao inteligente em '{display_name}'."
                    )
                    ai_state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                corrected_name, learned_unit, cleanup_source = cleanup_product_name_with_learning(
                    display_name,
                    row=item.row,
                    original_name=original_name,
                    use_ollama=True,
                    ollama_model=ollama_model,
                    ollama_timeout_seconds=ollama_timeout_seconds,
                    allow_web_lookup=allow_web_lookup,
                )
                corrected_name = re.sub(r"\s+", " ", str(corrected_name or display_name).strip())
                resolved_unit = learned_unit or infer_unit_label(corrected_name, row=item.row, default=item.unit_label or "KG")
                with rows_lock:
                    if 0 <= source_idx < len(initial_rows):
                        current_row = initial_rows[source_idx]
                        current_row["name"] = corrected_name or current_row.get("name") or display_name
                        current_row["unit"] = normalize_unit_label(resolved_unit)
                        current_row["cleanup_source"] = cleanup_source or current_row.get("cleanup_source") or "heuristic"
                        if not current_row.get("original_name"):
                            current_row["original_name"] = original_name or corrected_name or display_name
                    ai_state["processed"] = processed_idx
                    ai_state["current"] = corrected_name or display_name
                    ai_state["phase"] = "learning"
                    ai_state["label"] = "Salvando aprendizado"
                    ai_state["detail"] = f"Gravando a correcao de '{corrected_name or display_name}' na base local."
                    ai_state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with rows_lock:
                ai_state["running"] = False
                ai_state["done"] = True
                ai_state["current"] = ""
                ai_state["phase"] = "done"
                ai_state["label"] = "Correcao concluida"
                ai_state["detail"] = "A IA terminou as correcoes desta tela."
                ai_state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        threading.Thread(target=_background_ai_cleanup, daemon=True).start()

    class WebConfigHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args) -> None:  # noqa: A003
            return

        def _write_json(
            self,
            payload: Dict[str, Any],
            status: int = 200,
            extra_headers: Optional[Dict[str, str]] = None,
        ) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            if extra_headers:
                for key, value in extra_headers.items():
                    self.send_header(str(key), str(value))
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json_body(self) -> Dict[str, Any]:
            content_length = int(self.headers.get("Content-Length") or "0")
            if content_length <= 0:
                return {}
            raw = self.rfile.read(content_length)
            if not raw:
                return {}
            payload = json.loads(raw.decode("utf-8"))
            if isinstance(payload, dict):
                return payload
            return {}

        def _read_session_token(self) -> str:
            raw_cookie = str(self.headers.get("Cookie") or "")
            if not raw_cookie:
                return ""
            parsed = SimpleCookie()
            try:
                parsed.load(raw_cookie)
            except Exception:
                return ""
            morsel = parsed.get(USERS_SESSION_COOKIE_NAME)
            if not morsel:
                return ""
            return str(morsel.value or "").strip()

        def _build_session_cookie_header(self, token: str, max_age: int = USERS_SESSION_MAX_AGE_SECONDS) -> str:
            safe_max_age = max(0, int(max_age))
            return (
                f"{USERS_SESSION_COOKIE_NAME}={token}; "
                f"Path=/; Max-Age={safe_max_age}; HttpOnly; SameSite=Lax"
            )

        def _clear_session_cookie_header(self) -> str:
            return (
                f"{USERS_SESSION_COOKIE_NAME}=; "
                "Path=/; Max-Age=0; HttpOnly; SameSite=Lax"
            )

        def _get_authenticated_user(self) -> Optional[Dict[str, Any]]:
            token = self._read_session_token()
            if not token:
                return None
            with sessions_lock:
                session = auth_sessions.get(token)
                if not session:
                    return None
                now_ts = time.time()
                updated_at = float(session.get("updated_at") or session.get("created_at") or now_ts)
                max_age = float(session.get("max_age_seconds") or USERS_SESSION_MAX_AGE_SECONDS)
                if (now_ts - updated_at) > max_age:
                    auth_sessions.pop(token, None)
                    return None
                session["updated_at"] = now_ts
                username = _normalize_auth_username(session.get("usuario") or "")
            if not username:
                return None
            user = _find_user_by_username(username)
            if not user or not _to_bool(user.get("ativo"), default=True):
                with sessions_lock:
                    auth_sessions.pop(token, None)
                return None
            return user

        def _require_auth_user(self) -> Optional[Dict[str, Any]]:
            user = self._get_authenticated_user()
            if user:
                return user
            remembered_user, _remaining = _load_remembered_login_user_if_valid()
            if remembered_user:
                return remembered_user
            self._write_json({"ok": False, "error": "Nao autenticado."}, status=401)
            return None

        def _require_permission(self, permission_key: str) -> Optional[Dict[str, Any]]:
            user = self._require_auth_user()
            if not user:
                return None
            permissions = get_user_role_permissions(str(user.get("perfil") or ""))
            if permissions.get(permission_key):
                return user
            self._write_json({"ok": False, "error": "Acesso negado para este perfil."}, status=403)
            return None

        def _parse_request_path(self) -> str:
            raw_path = str(self.path or "")
            return raw_path.split("?", 1)[0]

        def do_GET(self) -> None:  # noqa: N802
            request_path = self._parse_request_path()
            if request_path == "/":
                auth_user = self._get_authenticated_user()
                extra_headers: Dict[str, str] = {}
                if not auth_user:
                    remembered_user, remaining_seconds = _load_remembered_login_user_if_valid()
                    if remembered_user and remaining_seconds > 0:
                        auth_user = remembered_user
                        session_token = _create_session_for_user(remembered_user, max_age_seconds=remaining_seconds)
                        extra_headers["Set-Cookie"] = self._build_session_cookie_header(
                            session_token,
                            max_age=remaining_seconds,
                        )
                body = _render_web_page(auth_user).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                for header_name, header_value in extra_headers.items():
                    self.send_header(header_name, header_value)
                self.end_headers()
                self.wfile.write(body)
                return

            if request_path == "/auth/status":
                auth_user = self._get_authenticated_user()
                extra_headers: Optional[Dict[str, str]] = None
                if not auth_user:
                    remembered_user, remaining_seconds = _load_remembered_login_user_if_valid()
                    if remembered_user and remaining_seconds > 0:
                        auth_user = remembered_user
                        session_token = _create_session_for_user(remembered_user, max_age_seconds=remaining_seconds)
                        extra_headers = {
                            "Set-Cookie": self._build_session_cookie_header(session_token, max_age=remaining_seconds)
                        }
                self._write_json({"ok": True, **_auth_payload_for_user(auth_user)}, extra_headers=extra_headers)
                return

            if request_path == "/auth/users":
                current_user = self._require_permission("can_manage_users")
                if not current_user:
                    return
                self._write_json(
                    {
                        "ok": True,
                        "users": _sanitize_users_for_listing(),
                        "auth": _auth_payload_for_user(current_user),
                    }
                )
                return

            current_user = self._require_auth_user()
            if not current_user:
                return

            if request_path == "/data":
                user_permissions = get_user_role_permissions(str(current_user.get("perfil") or ""))
                if not user_permissions.get("can_configure_plates"):
                    self._write_json({"ok": False, "error": "Seu perfil nao possui acesso ao configurador."}, status=403)
                    return
                self._write_json(
                    {
                        "items": _snapshot_rows(),
                        "month": today.month,
                        "year": today.year,
                        "max_day": default_month_max_day,
                        "default_day": today.day,
                        "default_month": today.month,
                        "auto_print_default": bool(default_auto_print),
                        "duplex_print_default": bool(default_duplex_print),
                        "shutdown_after_print_default": bool(default_shutdown_after_print),
                        "templates": {
                            "defaults": default_template_paths,
                            "available": options_payload,
                        },
                        "ai": _snapshot_ai_state(),
                        "auth": {
                            "user": build_public_user_payload(current_user),
                            "permissions": user_permissions,
                        },
                    }
                )
                return
            if request_path == "/live-config":
                if not get_user_role_permissions(str(current_user.get("perfil") or "")).get("can_configure_plates"):
                    self._write_json({"ok": False, "error": "Seu perfil nao possui acesso ao configurador."}, status=403)
                    return
                self._write_json(
                    {
                        "items": _snapshot_rows(),
                        "ai": _snapshot_ai_state(),
                        "auth": {
                            "user": build_public_user_payload(current_user),
                            "permissions": get_user_role_permissions(str(current_user.get("perfil") or "")),
                        },
                    }
                )
                return
            if request_path == "/progress":
                if not get_user_role_permissions(str(current_user.get("perfil") or "")).get("can_submit_generation"):
                    self._write_json({"ok": False, "error": "Seu perfil nao possui acesso ao progresso da geracao."}, status=403)
                    return
                self._write_json(progress_tracker.snapshot())
                return

            self._write_json({"ok": False, "error": "Rota nao encontrada."}, status=404)

        def do_POST(self) -> None:  # noqa: N802
            request_path = self._parse_request_path()

            if request_path == "/cancel":
                result_holder["items"] = None
                done_event.set()
                self._write_json({"ok": True})
                return

            if request_path in {"/auth/forgot-password", "/forgot-password"}:
                payload = self._read_json_body()
                identifier = re.sub(r"\s+", " ", str(payload.get("identifier") or payload.get("usuario") or "").strip())
                if not identifier:
                    self._write_json({"ok": False, "error": "Informe usuario ou e-mail."}, status=400)
                    return
                with users_lock:
                    matched_user = find_active_user_by_identifier(users_data, identifier)
                client_ip = ""
                try:
                    client_ip = str((self.client_address or ("", 0))[0] or "")
                except Exception:
                    client_ip = ""
                try:
                    register_password_reset_request(identifier, matched_user, ip_address=client_ip)
                except Exception:
                    pass
                self._write_json(
                    {
                        "ok": True,
                        "message": (
                            "Solicitacao recebida. Procure um usuario Dev para redefinir sua senha."
                        ),
                    }
                )
                return

            if request_path in {"/auth/login", "/login"}:
                payload = self._read_json_body()
                identifier = re.sub(r"\s+", " ", str(payload.get("identifier") or payload.get("usuario") or "").strip())
                password = str(payload.get("password") or payload.get("senha") or "")
                remember_24h = _to_bool(payload.get("remember_24h"), default=False)
                if not identifier or not password:
                    self._write_json({"ok": False, "error": "Informe usuario/e-mail e senha."}, status=400)
                    return
                with users_lock:
                    user = find_active_user_by_identifier(users_data, identifier)
                if not user or not verify_user_password(password, str(user.get("senha_hash") or "")):
                    reloaded, reloaded_source = _reload_users_data_from_remote_if_available()
                    if reloaded:
                        with users_lock:
                            user = find_active_user_by_identifier(users_data, identifier)
                        if user and verify_user_password(password, str(user.get("senha_hash") or "")):
                            print_status(
                                "WEB",
                                f"Login confirmado apos recarregar usuarios ({reloaded_source}).",
                            )
                    if not user or not verify_user_password(password, str(user.get("senha_hash") or "")):
                        self._write_json({"ok": False, "error": "Usuario, e-mail, telefone ou senha invalidos."}, status=401)
                        return
                session_max_age = USERS_SESSION_REMEMBER_24H_SECONDS if remember_24h else USERS_SESSION_MAX_AGE_SECONDS
                session_token = _create_session_for_user(user, max_age_seconds=session_max_age)
                user_for_response = dict(user)
                try:
                    if remember_24h:
                        save_login_session_24h(user)
                        remember_until = (datetime.now().astimezone() + timedelta(hours=LOGIN_SESSION_HOURS))
                        user_for_response["sessao_24h"] = True
                        user_for_response["sessao_expira_em"] = remember_until.isoformat(timespec="seconds")
                    else:
                        clear_login_session_24h()
                        user_for_response["sessao_24h"] = False
                        user_for_response["sessao_expira_em"] = ""
                except Exception:
                    pass
                self._write_json(
                    {
                        "ok": True,
                        "nome": _normalize_auth_username(user.get("nome") or user.get("usuario") or ""),
                        "perfil": normalize_user_role(str(user.get("perfil") or "")),
                        "remember_24h": bool(remember_24h),
                        **_auth_payload_for_user(user_for_response),
                    },
                    extra_headers={"Set-Cookie": self._build_session_cookie_header(session_token, max_age=session_max_age)},
                )
                return

            if request_path in {"/auth/logout", "/logout"}:
                token = self._read_session_token()
                if token:
                    with sessions_lock:
                        auth_sessions.pop(token, None)
                clear_login_session_24h()
                self._write_json(
                    {"ok": True, "logged_in": False},
                    extra_headers={"Set-Cookie": self._clear_session_cookie_header()},
                )
                return

            if request_path == "/auth/users":
                current_user = self._require_permission("can_manage_users")
                if not current_user:
                    return
                payload = self._read_json_body()
                username = _normalize_auth_username(payload.get("usuario") or "")
                email = _normalize_auth_email(payload.get("email") or "")
                phone = re.sub(r"\D+", "", str(payload.get("telefone") or payload.get("phone") or ""))
                nome = _normalize_auth_username(payload.get("nome") or username)
                password = str(payload.get("senha") or payload.get("password") or "")
                role = normalize_user_role(str(payload.get("perfil") or payload.get("role") or ROLE_PLATE_GENERATOR))
                if role not in VALID_USER_ROLES:
                    self._write_json({"ok": False, "error": "Perfil invalido."}, status=400)
                    return
                if not username:
                    self._write_json({"ok": False, "error": "Informe o usuario."}, status=400)
                    return
                if len(password) < 6:
                    self._write_json({"ok": False, "error": "A senha deve ter pelo menos 6 caracteres."}, status=400)
                    return
                with users_lock:
                    username_key = normalize_key(username)
                    email_key = _normalize_auth_email(email)
                    phone_key = re.sub(r"\D+", "", str(phone or ""))
                    for existing in users_data:
                        existing_user_key = normalize_key(str(existing.get("usuario") or ""))
                        if existing_user_key and existing_user_key == username_key:
                            self._write_json({"ok": False, "error": "Usuario ja existe."}, status=409)
                            return
                        existing_email_key = _normalize_auth_email(existing.get("email") or "")
                        if email_key and existing_email_key and existing_email_key == email_key:
                            self._write_json({"ok": False, "error": "E-mail ja cadastrado."}, status=409)
                            return
                        existing_phone_key = re.sub(r"\D+", "", str(existing.get("telefone") or ""))
                        if phone_key and existing_phone_key and existing_phone_key == phone_key:
                            self._write_json({"ok": False, "error": "Telefone ja cadastrado."}, status=409)
                            return
                    created_user = {
                        "usuario": username,
                        "senha_hash": hash_user_password(password),
                        "nome": nome or username,
                        "perfil": role,
                        "ativo": True,
                        "email": email,
                        "telefone": phone,
                    }
                    users_data.append(created_user)
                    users_data.sort(key=lambda item: normalize_key(str(item.get("usuario") or "")))
                    persist_result = persist_users_database(users_data, actor_name=str(current_user.get("usuario") or "dev"))
                if not persist_result.get("ok"):
                    self._write_json({"ok": False, "error": str(persist_result.get("message") or "Falha ao salvar usuario.")}, status=500)
                    return
                response_payload = {
                    "ok": True,
                    "message": str(persist_result.get("message") or "Usuario criado."),
                    "saved_remote": bool(persist_result.get("saved_remote")),
                    "saved_local": bool(persist_result.get("saved_local")),
                    "user": build_public_user_payload(created_user),
                    "users": _sanitize_users_for_listing(),
                }
                self._write_json(response_payload, status=201)
                return

            current_user = self._require_auth_user()
            if not current_user:
                return
            current_permissions = get_user_role_permissions(str(current_user.get("perfil") or ""))

            if request_path == "/ai-cleanup":
                if not current_permissions.get("can_use_ai_cleanup"):
                    self._write_json({"ok": False, "error": "Seu perfil nao pode executar correcao com IA."}, status=403)
                    return
                try:
                    payload = self._read_json_body()
                    raw_rows = payload.get("rows")
                    if not isinstance(raw_rows, list) or not raw_rows:
                        self._write_json({"ok": False, "error": "Nenhuma linha enviada para correcao."}, status=400)
                        return
                    with rows_lock:
                        ai_state["enabled"] = bool(ollama_cleanup_enabled)
                        ai_state["running"] = True
                        ai_state["done"] = False
                        ai_state["processed"] = 0
                        ai_state["total"] = len(raw_rows)
                        ai_state["current"] = ""
                        ai_state["phase"] = "analyzing"
                        ai_state["label"] = "Analisando selecao"
                        ai_state["detail"] = "A IA esta revisando os itens selecionados."
                        ai_state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    updates: List[Dict[str, Any]] = []
                    for idx_raw, raw in enumerate(raw_rows, start=1):
                        if not isinstance(raw, dict):
                            continue
                        source_item: Optional[PlateData] = None
                        source_idx_raw = raw.get("source_idx")
                        source_idx: Optional[int] = None
                        try:
                            if source_idx_raw not in (None, ""):
                                source_idx = int(source_idx_raw)
                        except Exception:
                            source_idx = None
                        if source_idx is not None and 0 <= source_idx < len(items):
                            source_item = items[source_idx]
                        row_name = re.sub(r"\s+", " ", str(raw.get("name") or "").strip())
                        if not row_name:
                            continue
                        original_name_value = re.sub(r"\s+", " ", str(raw.get("original_name") or "").strip()) or (
                            _resolve_plate_original_name(source_item) if source_item else row_name
                        )
                        row_context = dict(source_item.row) if source_item else {}
                        row_context.update(
                            {
                                "nome": row_name,
                                "produto": row_name,
                                "descricao": row_name,
                                "preco": str(raw.get("price") or "").strip(),
                            }
                        )
                        with rows_lock:
                            ai_state["current"] = row_name
                            ai_state["phase"] = "researching" if allow_web_lookup else "correcting"
                            ai_state["label"] = "Pesquisando na web" if allow_web_lookup else "Corrigindo nome"
                            ai_state["detail"] = (
                                f"Buscando o nome correto para '{row_name}'."
                                if allow_web_lookup
                                else f"Corrigindo '{row_name}'."
                            )
                            ai_state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        corrected_name, learned_unit, cleanup_source = cleanup_product_name_with_learning(
                            row_name,
                            row=row_context,
                            original_name=original_name_value,
                            use_ollama=bool(ollama_cleanup_enabled),
                            force_ollama=True,
                            ollama_model=ollama_model,
                            ollama_timeout_seconds=ollama_timeout_seconds,
                            allow_web_lookup=allow_web_lookup,
                        )
                        resolved_unit = learned_unit or infer_unit_label(
                            corrected_name,
                            row=row_context,
                            default=str(raw.get("unit") or "KG"),
                        )
                        if source_idx is not None:
                            with rows_lock:
                                if 0 <= source_idx < len(initial_rows):
                                    initial_rows[source_idx]["name"] = corrected_name
                                    initial_rows[source_idx]["unit"] = normalize_unit_label(resolved_unit)
                                    initial_rows[source_idx]["cleanup_source"] = cleanup_source or "ollama"
                                    if not initial_rows[source_idx].get("original_name"):
                                        initial_rows[source_idx]["original_name"] = original_name_value
                        updates.append(
                            {
                                "client_idx": raw.get("client_idx"),
                                "source_idx": source_idx,
                                "index": raw.get("index"),
                                "name": corrected_name,
                                "original_name": original_name_value,
                                "unit": normalize_unit_label(resolved_unit),
                                "cleanup_source": cleanup_source or "ollama",
                            }
                        )
                        with rows_lock:
                            ai_state["processed"] = idx_raw
                            ai_state["phase"] = "learning"
                            ai_state["label"] = "Salvando aprendizado"
                            ai_state["detail"] = f"Gravando '{corrected_name}' na base local."
                            ai_state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    with rows_lock:
                        ai_state["running"] = False
                        ai_state["done"] = True
                        ai_state["current"] = ""
                        ai_state["phase"] = "done"
                        ai_state["label"] = "Correcao concluida"
                        ai_state["detail"] = "As correcoes selecionadas foram finalizadas."
                        ai_state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    self._write_json({"ok": True, "updates": updates})
                    return
                except Exception as exc:
                    with rows_lock:
                        ai_state["running"] = False
                        ai_state["done"] = True
                        ai_state["phase"] = "error"
                        ai_state["label"] = "Falha na correcao"
                        ai_state["detail"] = str(exc)
                        ai_state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    self._write_json({"ok": False, "error": f"Falha ao corrigir com IA: {exc}"}, status=500)
                    return

            if request_path != "/submit":
                self._write_json({"ok": False, "error": "Rota nao encontrada."}, status=404)
                return

            try:
                if not current_permissions.get("can_submit_generation"):
                    self._write_json({"ok": False, "error": "Seu perfil nao pode iniciar geracao."}, status=403)
                    return
                payload = self._read_json_body()
                raw_rows = payload.get("rows")
                if not isinstance(raw_rows, list):
                    self._write_json({"ok": False, "error": "Payload invalido."}, status=400)
                    return
                raw_templates = payload.get("templates")
                parsed_templates: Dict[str, Optional[Path]] = {"A4": None, "A5": None, "A6": None}
                if isinstance(raw_templates, dict):
                    if not current_permissions.get("can_manage_templates"):
                        for fmt in ("A4", "A5", "A6"):
                            requested_template = str(raw_templates.get(fmt) or "").strip()
                            default_template = str(default_template_paths.get(fmt) or "").strip()
                            if requested_template != default_template:
                                self._write_json(
                                    {"ok": False, "error": "Seu perfil nao pode alterar modelos (templates)."},
                                    status=403,
                                )
                                return
                    for fmt in ("A4", "A5", "A6"):
                        raw_value = str(raw_templates.get(fmt) or "").strip()
                        if not raw_value:
                            continue
                        candidate = Path(raw_value).expanduser().resolve()
                        if candidate.suffix.lower() != ".cdr" or not candidate.exists():
                            self._write_json(
                                {"ok": False, "error": f"Modelo {fmt} invalido ou inexistente: {candidate}"},
                                status=400,
                            )
                            return
                        parsed_templates[fmt] = candidate

                selected: List[PlateData] = []
                for raw in raw_rows:
                    if not isinstance(raw, dict):
                        continue
                    if not _as_bool(raw.get("enabled")):
                        continue

                    source_item: Optional[PlateData] = None
                    source_idx_raw = raw.get("source_idx")
                    source_idx: Optional[int] = None
                    try:
                        if source_idx_raw not in (None, ""):
                            source_idx = int(source_idx_raw)
                    except Exception:
                        source_idx = None
                    if source_idx is not None and 0 <= source_idx < len(items):
                        source_item = items[source_idx]

                    try:
                        qty_a4 = int(raw.get("qty_a4") or 0)
                        qty_a5 = int(raw.get("qty_a5") or 0)
                        qty_a6 = int(raw.get("qty_a6") or 0)
                    except Exception:
                        item_label = source_item.name if source_item else str(raw.get("name") or "item manual")
                        self._write_json(
                            {"ok": False, "error": f"Quantidade invalida para '{item_label}'."},
                            status=400,
                        )
                        return
                    if (
                        qty_a4 < 0
                        or qty_a4 > 999
                        or qty_a5 < 0
                        or qty_a5 > 999
                        or qty_a6 < 0
                        or qty_a6 > 999
                    ):
                        item_label = source_item.name if source_item else str(raw.get("name") or "item manual")
                        self._write_json(
                            {"ok": False, "error": f"Quantidade invalida para '{item_label}'."},
                            status=400,
                        )
                        return
                    total_qty = qty_a4 + qty_a5 + qty_a6
                    if total_qty <= 0:
                        item_label = source_item.name if source_item else str(raw.get("name") or "item manual")
                        self._write_json(
                            {"ok": False, "error": f"Informe ao menos 1 copia para '{item_label}'."},
                            status=400,
                        )
                        return

                    raw_index = int(raw.get("index") or (len(selected) + 1))
                    name_value = re.sub(r"\s+", " ", str(raw.get("name") or "").strip())
                    if not name_value:
                        self._write_json(
                            {"ok": False, "error": f"Nome invalido na linha {raw_index}."},
                            status=400,
                        )
                        return
                    name_value = correct_accents_in_text(name_value)
                    original_name_value = re.sub(r"\s+", " ", str(raw.get("original_name") or "").strip()) or (
                        _resolve_plate_original_name(source_item) if source_item else name_value
                    )

                    price_raw = re.sub(r"\s+", " ", str(raw.get("price") or "").strip())
                    if not price_raw:
                        self._write_json(
                            {"ok": False, "error": f"Preco invalido para '{name_value}'."},
                            status=400,
                        )
                        return
                    price_value = to_price_text(price_raw, price_prefix)

                    format_quantities = {"A4": qty_a4, "A5": qty_a5, "A6": qty_a6}
                    preferred_format = "A4" if qty_a4 > 0 else ("A5" if qty_a5 > 0 else "A6")
                    plate_format = normalize_plate_format(preferred_format, default="A4")
                    unit = normalize_unit_label(str(raw.get("unit") or "KG"))
                    duplex_row_enabled = _as_bool(raw.get("duplex_enabled"))
                    validity_enabled = _as_bool(raw.get("offer_validity_enabled"))
                    validity_month = int(raw.get("offer_validity_month") or today.month)
                    if validity_month < 1 or validity_month > 12:
                        self._write_json(
                            {
                                "ok": False,
                                "error": (
                                    f"Mes invalido para '{name_value}'. "
                                    "Use 1 a 12."
                                ),
                            },
                            status=400,
                        )
                        return
                    max_day_for_month = _max_day_in_month(today.year, validity_month)
                    validity_day = int(raw.get("offer_validity_day") or today.day)
                    if validity_day < 1 or validity_day > max_day_for_month:
                        self._write_json(
                            {
                                "ok": False,
                                "error": (
                                    f"Dia invalido para '{name_value}'. "
                                    f"Use 1 a {max_day_for_month} para {validity_month:02d}/{today.year:04d}."
                                ),
                            },
                            status=400,
                        )
                        return

                    updated_row = dict(source_item.row) if source_item else {}
                    updated_row["unidade"] = unit
                    updated_row["formato_placa"] = plate_format
                    updated_row["qtd_a4"] = str(qty_a4)
                    updated_row["qtd_a5"] = str(qty_a5)
                    updated_row["qtd_a6"] = str(qty_a6)
                    updated_row["preco"] = price_value
                    updated_row["nome"] = name_value
                    updated_row["frente_verso"] = "1" if duplex_row_enabled else "0"
                    updated_row["produto"] = name_value
                    updated_row["descricao"] = name_value
                    updated_row["descrição"] = name_value
                    updated_row["descricao_produto"] = name_value
                    updated_row["nome_produto"] = name_value
                    updated_row["descricao_do_produto"] = name_value
                    updated_row["oferta_validade_ativa"] = "1" if validity_enabled else "0"
                    updated_row["oferta_validade_dia"] = f"{validity_day:02d}"
                    updated_row["oferta_validade_mes"] = f"{validity_month:02d}"
                    updated_row["oferta_validade_ano"] = f"{today.year:04d}"
                    updated_row["oferta_validade_texto"] = (
                        build_offer_validity_text(validity_day, today=today, month_value=validity_month)
                        if validity_enabled
                        else ""
                    )
                    auth_user_payload = build_public_user_payload(current_user) or {}
                    updated_row["_audit_generated_by_user"] = str(auth_user_payload.get("usuario") or "")
                    updated_row["_audit_generated_by_name"] = str(auth_user_payload.get("nome") or "")
                    updated_row["_audit_generated_by_email"] = str(auth_user_payload.get("email") or "")
                    updated_row["_audit_generated_by_role"] = str(auth_user_payload.get("perfil") or "")
                    updated_row["_audit_selected_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    updated_row["_plate_original_name"] = original_name_value
                    updated_row["_plate_cleanup_source"] = str(raw.get("cleanup_source") or "confirmed_web")
                    remember_product_name_cleanup(
                        original_name_value,
                        name_value,
                        unit_value=unit,
                        source="confirmed_web_manual" if source_item is None else "confirmed_web",
                        model_name="local_ui",
                    )

                    selected.append(
                        PlateData(
                            index=source_item.index if source_item else raw_index,
                            name=name_value,
                            price=price_value,
                            row=updated_row,
                            original_name=original_name_value,
                            cleanup_source=str(raw.get("cleanup_source") or "confirmed_web"),
                            quantity=total_qty,
                            unit_label=unit,
                            plate_format=plate_format,
                            format_quantities=format_quantities,
                            duplex_enabled=duplex_row_enabled,
                            offer_validity_enabled=validity_enabled,
                            offer_validity_day=validity_day,
                            offer_validity_month=validity_month,
                        )
                    )

                if not selected:
                    self._write_json(
                        {"ok": False, "error": "Selecione ao menos uma placa para continuar."},
                        status=400,
                    )
                    return

                result_holder["items"] = selected
                result_holder["auto_print"] = _as_bool(payload.get("auto_print"))
                result_holder["duplex_print"] = _as_bool(payload.get("duplex_print"))
                result_holder["shutdown_after_print"] = _as_bool(payload.get("shutdown_after_print"))
                result_holder["template_overrides"] = parsed_templates
                if isinstance(template_selection_holder, dict):
                    template_selection_holder.clear()
                    template_selection_holder.update(parsed_templates)
                predicted_copies = sum(
                    total_format_copies(
                        normalize_format_quantities(
                            it.format_quantities,
                            default_format=it.plate_format,
                            default_qty=it.quantity,
                        )
                    )
                    for it in selected
                )
                progress_tracker.mark_config_submitted(len(selected), predicted_copies)
                done_event.set()
                self._write_json({"ok": True})
            except Exception as exc:
                result_holder["error"] = str(exc)
                done_event.set()
                self._write_json({"ok": False, "error": f"Falha ao processar envio: {exc}"}, status=500)

    server = ThreadingHTTPServer(("127.0.0.1", 0), WebConfigHandler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    url = f"http://127.0.0.1:{server.server_port}/"
    print_status("WEB", f"Abrindo configuracao: {url}")
    try:
        webbrowser.open(url, new=2)
    except Exception:
        pass

    done_event.wait()
    if result_holder.get("error"):
        try:
            server.shutdown()
        except Exception:
            pass
        try:
            server.server_close()
        except Exception:
            pass
        try:
            server_thread.join(timeout=2.0)
        except Exception:
            pass
        raise RuntimeError(str(result_holder["error"]))

    selected_items = result_holder.get("items")
    if selected_items is None:
        try:
            progress_tracker.finish("cancelled")
        except Exception:
            pass
        try:
            server.shutdown()
        except Exception:
            pass
        try:
            server.server_close()
        except Exception:
            pass
        try:
            server_thread.join(timeout=2.0)
        except Exception:
            pass
        return None

    _set_active_web_progress_session(
        WebProgressSession(
            server=server,
            server_thread=server_thread,
            tracker=progress_tracker,
        )
    )
    return (
        selected_items,
        bool(result_holder.get("auto_print")),
        bool(result_holder.get("duplex_print")),
        bool(result_holder.get("shutdown_after_print")),
    )


def configure_plates_via_dialog(
    items: List[PlateData],
    price_prefix: str = "R$ ",
    default_auto_print: bool = False,
    default_duplex_print: bool = False,
    default_shutdown_after_print: bool = False,
) -> Optional[tuple[List[PlateData], bool, bool, bool]]:
    if not items:
        return []

    try:
        import tkinter as tk
        from tkinter import messagebox, ttk
    except Exception:
        print("Interface grafica indisponivel; usando todas as placas com quantidade 1 e unidade padrao detectada.")
        return items, bool(default_auto_print), bool(default_duplex_print), bool(default_shutdown_after_print)

    configured_items: Optional[List[PlateData]] = None
    configured_auto_print = bool(default_auto_print)
    configured_duplex_print = bool(default_duplex_print)
    configured_shutdown_after_print = bool(default_shutdown_after_print)
    today = date.today()
    month_options = tuple(f"{m:02d}" for m in range(1, 13))

    root = tk.Tk()
    root.title("Configuracao das placas")
    screen_w = int(root.winfo_screenwidth() or 1600)
    screen_h = int(root.winfo_screenheight() or 900)
    win_w = min(max(1180, int(screen_w * 0.92)), 1860)
    win_h = min(max(680, int(screen_h * 0.85)), 1020)
    pos_x = max((screen_w - win_w) // 2, 0)
    pos_y = max((screen_h - win_h) // 2 - 20, 0)
    root.geometry(f"{win_w}x{win_h}+{pos_x}+{pos_y}")
    root.minsize(1100, 620)

    try:
        root.attributes("-topmost", True)
    except Exception:
        pass

    try:
        style = ttk.Style(root)
        for theme_name in ("vista", "xpnative", "clam"):
            if theme_name in style.theme_names():
                style.theme_use(theme_name)
                break
        style.configure("HeaderTitle.TLabel", font=("Segoe UI Semibold", 12))
        style.configure("HeaderHint.TLabel", foreground="#4f5963")
        style.configure("Status.TLabel", padding=(8, 6))
    except Exception:
        pass

    header = ttk.Frame(root, padding=(12, 10, 12, 6))
    header.pack(fill="x")
    ttk.Label(header, text="Configuracao das placas", style="HeaderTitle.TLabel").pack(anchor="w")
    ttk.Label(
        header,
        text=(
            "Revise os itens antes de iniciar. Use os comandos em lote para acelerar, "
            "edite os campos por linha e ajuste validade quando necessario."
        ),
        style="HeaderHint.TLabel",
        justify="left",
        wraplength=max(900, win_w - 80),
    ).pack(anchor="w", pady=(2, 0))

    tools = ttk.Frame(root, padding=(12, 2, 12, 6))
    tools.pack(fill="x")
    tools.grid_columnconfigure(3, weight=1)

    select_card = ttk.LabelFrame(tools, text="Selecao")
    select_card.grid(row=0, column=0, sticky="nw", padx=(0, 8))
    batch_card = ttk.LabelFrame(tools, text="Formato e unidade")
    batch_card.grid(row=0, column=1, sticky="nw", padx=(0, 8))
    validity_card = ttk.LabelFrame(tools, text="Validade")
    validity_card.grid(row=0, column=2, sticky="nw", padx=(0, 8))
    filter_card = ttk.LabelFrame(tools, text="Busca")
    filter_card.grid(row=0, column=3, sticky="nwe")
    filter_card.grid_columnconfigure(1, weight=1)

    search_var = tk.StringVar(value="")
    show_selected_only_var = tk.BooleanVar(value=False)
    bulk_month_var = tk.StringVar(value=f"{today.month:02d}")
    bulk_day_var = tk.StringVar(value=f"{today.day:02d}")
    auto_print_var = tk.BooleanVar(value=bool(default_auto_print))
    duplex_print_var = tk.BooleanVar(value=bool(default_duplex_print))
    shutdown_after_print_var = tk.BooleanVar(value=bool(default_shutdown_after_print))
    status_var = tk.StringVar(value="")

    list_host = ttk.Frame(root, padding=(12, 0, 12, 6))
    list_host.pack(fill="both", expand=True)
    list_host.grid_rowconfigure(0, weight=1)
    list_host.grid_columnconfigure(0, weight=1)

    canvas = tk.Canvas(list_host, highlightthickness=0, borderwidth=0)
    scrollbar_y = ttk.Scrollbar(list_host, orient="vertical", command=canvas.yview)
    scroll_frame = ttk.Frame(canvas)
    canvas.configure(yscrollcommand=scrollbar_y.set)
    canvas.grid(row=0, column=0, sticky="nsew")
    scrollbar_y.grid(row=0, column=1, sticky="ns")

    canvas_window = canvas.create_window((0, 0), window=scroll_frame, anchor="nw")

    def _sync_canvas_width(event) -> None:
        min_width = 980
        target_width = max(int(event.width), min_width)
        canvas.itemconfigure(canvas_window, width=target_width)

    def _sync_scroll_region(_event=None) -> None:
        canvas.configure(scrollregion=canvas.bbox("all"))

    canvas.bind("<Configure>", _sync_canvas_width)
    scroll_frame.bind("<Configure>", _sync_scroll_region)

    def _on_mousewheel(event) -> None:
        try:
            delta = int(-1 * (event.delta / 120))
        except Exception:
            delta = -1
        canvas.yview_scroll(delta, "units")

    canvas.bind_all("<MouseWheel>", _on_mousewheel)

    headers = ["Gerar", "#", "Produto", "Preco", "Qtd A4", "Qtd A5", "Qtd A6", "Unidade", "Fr/Vs", "Validade", "Dia", "Mes"]
    for col, label in enumerate(headers):
        header_lbl = ttk.Label(scroll_frame, text=label)
        header_lbl.grid(row=0, column=col, sticky="w", padx=6, pady=(3, 8))

    row_controls: List[Dict[str, object]] = []

    def _safe_int(value: object, default: int = 0) -> int:
        try:
            return int(str(value).strip())
        except Exception:
            return default

    def _set_widget_state(widget: object, enabled: bool) -> None:
        try:
            if isinstance(widget, ttk.Combobox):
                widget.configure(state="readonly" if enabled else "disabled")
            elif isinstance(widget, ttk.Entry):
                widget.state(["!disabled"] if enabled else ["disabled"])
            elif isinstance(widget, ttk.Checkbutton):
                widget.state(["!disabled"] if enabled else ["disabled"])
            elif isinstance(widget, tk.Spinbox):
                widget.configure(state="normal" if enabled else "disabled")
            else:
                widget.configure(state="normal" if enabled else "disabled")
        except Exception:
            pass

    def _apply_row_state(controls: Dict[str, object]) -> None:
        row_enabled = bool(controls["enabled"].get())
        _set_widget_state(controls["name_widget"], row_enabled)
        _set_widget_state(controls["price_widget"], row_enabled)
        _set_widget_state(controls["qty_a4_widget"], row_enabled)
        _set_widget_state(controls["qty_a5_widget"], row_enabled)
        _set_widget_state(controls["qty_a6_widget"], row_enabled)
        _set_widget_state(controls["unit_widget"], row_enabled)
        _set_widget_state(controls["duplex_widget"], row_enabled)
        _set_widget_state(controls["offer_validity_widget"], row_enabled)
        day_enabled = row_enabled and bool(controls["offer_validity_enabled"].get())
        _set_widget_state(controls["offer_validity_day_widget"], day_enabled)
        _set_widget_state(controls["offer_validity_month_widget"], day_enabled)
        try:
            month_value = int(str(controls["offer_validity_month"].get()).strip())
        except Exception:
            month_value = today.month
        if month_value < 1 or month_value > 12:
            month_value = today.month
            controls["offer_validity_month"].set(f"{month_value:02d}")
        max_day_for_month = _max_day_in_month(today.year, month_value)
        try:
            controls["offer_validity_day_widget"].configure(to=max_day_for_month)
        except Exception:
            pass
        try:
            day_value = int(str(controls["offer_validity_day"].get()).strip())
        except Exception:
            day_value = 1
        if day_value < 1:
            day_value = 1
        if day_value > max_day_for_month:
            day_value = max_day_for_month
        controls["offer_validity_day"].set(f"{day_value:02d}")

    def _refresh_status() -> None:
        total_rows = len(row_controls)
        selected_rows = [c for c in row_controls if bool(c["enabled"].get())]
        selected_count = len(selected_rows)
        a4_count = sum(max(0, _safe_int(c["qty_a4"].get(), 0)) for c in selected_rows)
        a5_count = sum(max(0, _safe_int(c["qty_a5"].get(), 0)) for c in selected_rows)
        a6_count = sum(max(0, _safe_int(c["qty_a6"].get(), 0)) for c in selected_rows)
        total_copies = a4_count + a5_count + a6_count
        duplex_count = sum(1 for c in selected_rows if bool(c["duplex_enabled"].get()))
        validity_count = sum(1 for c in selected_rows if bool(c["offer_validity_enabled"].get()))
        visible_count = sum(1 for c in row_controls if bool(c.get("visible", True)))
        status_var.set(
            f"Selecionadas: {selected_count}/{total_rows} | Copias: {total_copies} | "
            f"A4: {a4_count} | A5: {a5_count} | A6: {a6_count} | "
            f"Fr/Verso: {duplex_count} | Com validade: {validity_count} | Visiveis: {visible_count}"
        )

    def _apply_filters() -> None:
        query = normalize_key(str(search_var.get())).replace("_", "")
        only_selected = bool(show_selected_only_var.get())
        for controls in row_controls:
            name_norm = normalize_key(str(controls["name"].get())).replace("_", "")
            visible = True
            if query and query not in name_norm:
                visible = False
            if only_selected and not bool(controls["enabled"].get()):
                visible = False
            controls["visible"] = visible
            for widget in controls["widgets"]:
                if visible:
                    widget.grid()
                else:
                    widget.grid_remove()
        _sync_scroll_region()
        _refresh_status()

    def _set_all_enabled(value: bool) -> None:
        for controls in row_controls:
            controls["enabled"].set(value)
            _apply_row_state(controls)
        _apply_filters()

    def _invert_selection() -> None:
        for controls in row_controls:
            controls["enabled"].set(not bool(controls["enabled"].get()))
            _apply_row_state(controls)
        _apply_filters()

    def _set_units(unit_value: str, only_selected: bool) -> None:
        normalized = normalize_unit_label(unit_value)
        for controls in row_controls:
            if only_selected and not controls["enabled"].get():
                continue
            controls["unit"].set(normalized)
        _refresh_status()

    def _set_duplex(enabled: bool, only_selected: bool) -> None:
        for controls in row_controls:
            if only_selected and not controls["enabled"].get():
                continue
            controls["duplex_enabled"].set(enabled)
        _refresh_status()

    def _set_formats(format_value: str, only_selected: bool) -> None:
        normalized = normalize_plate_format(format_value, default="A4")
        for controls in row_controls:
            if only_selected and not controls["enabled"].get():
                continue
            if normalized == "A4":
                controls["qty_a4"].set(str(max(1, _safe_int(controls["qty_a4"].get(), 1))))
                controls["qty_a5"].set("0")
                controls["qty_a6"].set("0")
            elif normalized == "A5":
                controls["qty_a4"].set("0")
                controls["qty_a5"].set(str(max(1, _safe_int(controls["qty_a5"].get(), 1))))
                controls["qty_a6"].set("0")
            else:
                controls["qty_a4"].set("0")
                controls["qty_a5"].set("0")
                controls["qty_a6"].set(str(max(1, _safe_int(controls["qty_a6"].get(), 1))))
        _refresh_status()

    def _set_offer_validity(enabled: bool, only_selected: bool) -> None:
        for controls in row_controls:
            if only_selected and not controls["enabled"].get():
                continue
            controls["offer_validity_enabled"].set(enabled)
            _apply_row_state(controls)
        _refresh_status()

    def _get_bulk_month_or_show_error() -> Optional[int]:
        raw = str(bulk_month_var.get()).strip()
        try:
            parsed = int(raw)
        except Exception:
            messagebox.showerror("Mes invalido", "Informe um mes valido para a data de oferta.")
            return None
        if parsed < 1 or parsed > 12:
            messagebox.showerror("Mes invalido", "Use mes entre 1 e 12.")
            return None
        return parsed

    def _get_bulk_day_or_show_error(month_value: int) -> Optional[int]:
        max_day_for_month = _max_day_in_month(today.year, month_value)
        raw = str(bulk_day_var.get()).strip()
        try:
            parsed = int(raw)
        except Exception:
            messagebox.showerror("Dia invalido", "Informe um dia valido para a data de oferta.")
            return None
        if parsed < 1 or parsed > max_day_for_month:
            messagebox.showerror(
                "Dia invalido",
                f"Use dia entre 1 e {max_day_for_month} para {month_value:02d}/{today.year:04d}.",
            )
            return None
        return parsed

    def _set_offer_date(only_selected: bool) -> None:
        parsed_month = _get_bulk_month_or_show_error()
        if parsed_month is None:
            return
        parsed_day = _get_bulk_day_or_show_error(parsed_month)
        if parsed_day is None:
            return
        day_text = f"{parsed_day:02d}"
        month_text = f"{parsed_month:02d}"
        for controls in row_controls:
            if only_selected and not controls["enabled"].get():
                continue
            controls["offer_validity_day"].set(day_text)
            controls["offer_validity_month"].set(month_text)
            _apply_row_state(controls)
        _refresh_status()

    for row_idx, item in enumerate(items, start=1):
        enabled_var = tk.BooleanVar(value=True)
        name_var = tk.StringVar(value=item.name)
        price_var = tk.StringVar(value=item.price)
        format_quantities = normalize_format_quantities(
            item.format_quantities,
            default_format=item.plate_format,
            default_qty=item.quantity,
        )
        qty_a4_var = tk.StringVar(value=str(int(format_quantities.get("A4", 0))))
        qty_a5_var = tk.StringVar(value=str(int(format_quantities.get("A5", 0))))
        qty_a6_var = tk.StringVar(value=str(int(format_quantities.get("A6", 0))))
        unit_var = tk.StringVar(value=normalize_unit_label(item.unit_label))
        duplex_var = tk.BooleanVar(value=bool(item.duplex_enabled))
        validity_var = tk.BooleanVar(value=bool(item.offer_validity_enabled))
        initial_month = int(item.offer_validity_month or today.month)
        if initial_month < 1 or initial_month > 12:
            initial_month = today.month
        max_day_for_initial_month = _max_day_in_month(today.year, initial_month)
        initial_day = int(item.offer_validity_day or today.day)
        if initial_day < 1 or initial_day > max_day_for_initial_month:
            initial_day = min(today.day, max_day_for_initial_month)
        validity_day_var = tk.StringVar(value=f"{initial_day:02d}")
        validity_month_var = tk.StringVar(value=f"{initial_month:02d}")

        enabled_chk = ttk.Checkbutton(scroll_frame, variable=enabled_var)
        enabled_chk.grid(row=row_idx, column=0, sticky="w", padx=6, pady=2)

        idx_label = ttk.Label(scroll_frame, text=str(item.index))
        idx_label.grid(row=row_idx, column=1, sticky="w", padx=6, pady=2)

        name_input = ttk.Entry(scroll_frame, textvariable=name_var)
        name_input.grid(row=row_idx, column=2, sticky="we", padx=6, pady=2)

        price_input = ttk.Entry(scroll_frame, width=12, textvariable=price_var)
        price_input.grid(row=row_idx, column=3, sticky="w", padx=6, pady=2)

        qty_a4_input = tk.Spinbox(scroll_frame, from_=0, to=999, width=5, textvariable=qty_a4_var)
        qty_a4_input.grid(row=row_idx, column=4, sticky="w", padx=6, pady=2)

        qty_a5_input = tk.Spinbox(scroll_frame, from_=0, to=999, width=5, textvariable=qty_a5_var)
        qty_a5_input.grid(row=row_idx, column=5, sticky="w", padx=6, pady=2)

        qty_a6_input = tk.Spinbox(scroll_frame, from_=0, to=999, width=5, textvariable=qty_a6_var)
        qty_a6_input.grid(row=row_idx, column=6, sticky="w", padx=6, pady=2)

        unit_combo = ttk.Combobox(scroll_frame, width=8, textvariable=unit_var, state="readonly")
        unit_combo["values"] = UNIT_OPTIONS
        unit_combo.grid(row=row_idx, column=7, sticky="w", padx=6, pady=2)

        duplex_chk = ttk.Checkbutton(scroll_frame, variable=duplex_var)
        duplex_chk.grid(row=row_idx, column=8, sticky="w", padx=6, pady=2)

        validity_chk = ttk.Checkbutton(scroll_frame, variable=validity_var)
        validity_chk.grid(row=row_idx, column=9, sticky="w", padx=6, pady=2)

        validity_day_input = tk.Spinbox(
            scroll_frame,
            from_=1,
            to=max_day_for_initial_month,
            width=4,
            format="%02.0f",
            textvariable=validity_day_var,
        )
        validity_day_input.grid(row=row_idx, column=10, sticky="w", padx=6, pady=2)
        validity_month_input = ttk.Combobox(
            scroll_frame,
            width=4,
            values=month_options,
            textvariable=validity_month_var,
            state="readonly",
        )
        validity_month_input.grid(row=row_idx, column=11, sticky="w", padx=6, pady=2)

        controls: Dict[str, object] = {
            "enabled": enabled_var,
            "name": name_var,
            "price": price_var,
            "qty_a4": qty_a4_var,
            "qty_a5": qty_a5_var,
            "qty_a6": qty_a6_var,
            "unit": unit_var,
            "duplex_enabled": duplex_var,
            "offer_validity_enabled": validity_var,
            "offer_validity_day": validity_day_var,
            "offer_validity_month": validity_month_var,
            "name_widget": name_input,
            "price_widget": price_input,
            "qty_a4_widget": qty_a4_input,
            "qty_a5_widget": qty_a5_input,
            "qty_a6_widget": qty_a6_input,
            "unit_widget": unit_combo,
            "duplex_widget": duplex_chk,
            "offer_validity_widget": validity_chk,
            "offer_validity_day_widget": validity_day_input,
            "offer_validity_month_widget": validity_month_input,
            "widgets": [
                enabled_chk,
                idx_label,
                name_input,
                price_input,
                qty_a4_input,
                qty_a5_input,
                qty_a6_input,
                unit_combo,
                duplex_chk,
                validity_chk,
                validity_day_input,
                validity_month_input,
            ],
            "visible": True,
        }

        enabled_chk.configure(command=lambda c=controls: (_apply_row_state(c), _apply_filters()))
        duplex_chk.configure(command=lambda: _refresh_status())
        validity_chk.configure(command=lambda c=controls: (_apply_row_state(c), _refresh_status()))
        name_var.trace_add("write", lambda *_args: _apply_filters())
        qty_a4_var.trace_add("write", lambda *_args: _refresh_status())
        qty_a5_var.trace_add("write", lambda *_args: _refresh_status())
        qty_a6_var.trace_add("write", lambda *_args: _refresh_status())
        validity_month_var.trace_add("write", lambda *_args, c=controls: (_apply_row_state(c), _refresh_status()))

        row_controls.append(controls)
        _apply_row_state(controls)

    scroll_frame.grid_columnconfigure(2, weight=1)
    _sync_scroll_region()

    ttk.Button(select_card, text="Marcar todas", command=lambda: _set_all_enabled(True)).grid(
        row=0, column=0, padx=6, pady=6, sticky="w"
    )
    ttk.Button(select_card, text="Desmarcar todas", command=lambda: _set_all_enabled(False)).grid(
        row=0, column=1, padx=6, pady=6, sticky="w"
    )
    ttk.Button(select_card, text="Inverter", command=_invert_selection).grid(
        row=0, column=2, padx=6, pady=6, sticky="w"
    )

    ttk.Button(batch_card, text="KG selecionadas", command=lambda: _set_units("KG", True)).grid(
        row=0, column=0, padx=6, pady=(6, 4), sticky="w"
    )
    ttk.Button(batch_card, text="UNID selecionadas", command=lambda: _set_units("UNID", True)).grid(
        row=0, column=1, padx=6, pady=(6, 4), sticky="w"
    )
    ttk.Button(batch_card, text="PCT. selecionadas", command=lambda: _set_units("PCT.", True)).grid(
        row=0, column=2, padx=6, pady=(6, 4), sticky="w"
    )
    ttk.Button(batch_card, text="BDJ. selecionadas", command=lambda: _set_units("BDJ.", True)).grid(
        row=0, column=3, padx=6, pady=(6, 4), sticky="w"
    )
    ttk.Button(batch_card, text="PACK. selecionadas", command=lambda: _set_units("PACK.", True)).grid(
        row=0, column=4, padx=6, pady=(6, 4), sticky="w"
    )
    ttk.Button(batch_card, text="A4 selecionadas", command=lambda: _set_formats("A4", True)).grid(
        row=1, column=0, padx=6, pady=(0, 6), sticky="w"
    )
    ttk.Button(batch_card, text="A5 selecionadas", command=lambda: _set_formats("A5", True)).grid(
        row=1, column=1, padx=6, pady=(0, 6), sticky="w"
    )
    ttk.Button(batch_card, text="A6 selecionadas", command=lambda: _set_formats("A6", True)).grid(
        row=1, column=2, padx=6, pady=(0, 6), sticky="w"
    )
    ttk.Button(batch_card, text="Fr/Verso ON", command=lambda: _set_duplex(True, True)).grid(
        row=1, column=3, padx=6, pady=(0, 6), sticky="w"
    )
    ttk.Button(batch_card, text="Fr/Verso OFF", command=lambda: _set_duplex(False, True)).grid(
        row=1, column=4, padx=6, pady=(0, 6), sticky="w"
    )
    ttk.Button(batch_card, text="KG todas", command=lambda: _set_units("KG", False)).grid(
        row=2, column=0, padx=6, pady=(0, 6), sticky="w"
    )
    ttk.Button(batch_card, text="UNID todas", command=lambda: _set_units("UNID", False)).grid(
        row=2, column=1, padx=6, pady=(0, 6), sticky="w"
    )
    ttk.Button(batch_card, text="PCT. todas", command=lambda: _set_units("PCT.", False)).grid(
        row=2, column=2, padx=6, pady=(0, 6), sticky="w"
    )
    ttk.Button(batch_card, text="BDJ. todas", command=lambda: _set_units("BDJ.", False)).grid(
        row=2, column=3, padx=6, pady=(0, 6), sticky="w"
    )
    ttk.Button(batch_card, text="PACK. todas", command=lambda: _set_units("PACK.", False)).grid(
        row=2, column=4, padx=6, pady=(0, 6), sticky="w"
    )
    ttk.Button(batch_card, text="A4 todas", command=lambda: _set_formats("A4", False)).grid(
        row=3, column=0, padx=6, pady=(0, 6), sticky="w"
    )
    ttk.Button(batch_card, text="A5 todas", command=lambda: _set_formats("A5", False)).grid(
        row=3, column=1, padx=6, pady=(0, 6), sticky="w"
    )
    ttk.Button(batch_card, text="A6 todas", command=lambda: _set_formats("A6", False)).grid(
        row=3, column=2, padx=6, pady=(0, 6), sticky="w"
    )

    ttk.Label(validity_card, text=f"Mes ({today.year:04d})").grid(
        row=0, column=0, padx=(6, 4), pady=6, sticky="w"
    )
    ttk.Combobox(
        validity_card,
        width=5,
        values=month_options,
        textvariable=bulk_month_var,
        state="readonly",
    ).grid(row=0, column=1, padx=(0, 6), pady=6, sticky="w")
    ttk.Label(validity_card, text="Dia").grid(
        row=0, column=2, padx=(6, 4), pady=6, sticky="w"
    )
    bulk_day_input = tk.Spinbox(
        validity_card,
        from_=1,
        to=_max_day_in_month(today.year, today.month),
        width=4,
        format="%02.0f",
        textvariable=bulk_day_var,
    )
    bulk_day_input.grid(row=0, column=3, padx=(0, 6), pady=6, sticky="w")
    ttk.Button(validity_card, text="Data selecionadas", command=lambda: _set_offer_date(True)).grid(
        row=0, column=4, padx=6, pady=6, sticky="w"
    )
    ttk.Button(validity_card, text="Data todas", command=lambda: _set_offer_date(False)).grid(
        row=0, column=5, padx=6, pady=6, sticky="w"
    )
    ttk.Button(validity_card, text="Ativar selecionadas", command=lambda: _set_offer_validity(True, True)).grid(
        row=1, column=4, padx=6, pady=(0, 6), sticky="w"
    )
    ttk.Button(validity_card, text="Remover selecionadas", command=lambda: _set_offer_validity(False, True)).grid(
        row=1, column=5, padx=6, pady=(0, 6), sticky="w"
    )

    ttk.Label(filter_card, text="Produto:").grid(row=0, column=0, padx=(6, 4), pady=(6, 4), sticky="w")
    search_entry = ttk.Entry(filter_card, textvariable=search_var)
    search_entry.grid(row=0, column=1, padx=(0, 6), pady=(6, 4), sticky="we")
    ttk.Button(filter_card, text="Limpar", command=lambda: search_var.set("")).grid(
        row=0, column=2, padx=(0, 6), pady=(6, 4), sticky="w"
    )
    ttk.Checkbutton(
        filter_card,
        text="Somente selecionadas",
        variable=show_selected_only_var,
    ).grid(row=1, column=1, padx=(0, 6), pady=(0, 6), sticky="w")

    search_var.trace_add("write", lambda *_args: _apply_filters())
    show_selected_only_var.trace_add("write", lambda *_args: _apply_filters())

    def _sync_bulk_day_limit() -> None:
        month_value = _safe_int(str(bulk_month_var.get()).strip(), today.month)
        if month_value < 1 or month_value > 12:
            month_value = today.month
            bulk_month_var.set(f"{month_value:02d}")
        max_day_for_month = _max_day_in_month(today.year, month_value)
        try:
            bulk_day_input.configure(to=max_day_for_month)
        except Exception:
            pass
        day_value = _safe_int(str(bulk_day_var.get()).strip(), today.day)
        day_value = max(1, min(day_value, max_day_for_month))
        bulk_day_var.set(f"{day_value:02d}")

    bulk_month_var.trace_add("write", lambda *_args: _sync_bulk_day_limit())
    _sync_bulk_day_limit()
    search_entry.bind("<Return>", lambda _event: _apply_filters())

    status_frame = ttk.Frame(root, padding=(12, 0, 12, 6))
    status_frame.pack(fill="x")
    ttk.Label(status_frame, textvariable=status_var, style="Status.TLabel", anchor="w", relief="groove").pack(fill="x")

    actions = ttk.Frame(root, padding=(12, 2, 12, 12))
    actions.pack(fill="x")

    def on_cancel() -> None:
        nonlocal configured_items
        configured_items = None
        try:
            canvas.unbind_all("<MouseWheel>")
        except Exception:
            pass
        root.destroy()

    def on_ok() -> None:
        nonlocal configured_items, configured_auto_print, configured_duplex_print, configured_shutdown_after_print
        selected: List[PlateData] = []
        for idx, item in enumerate(items):
            controls = row_controls[idx]
            if not controls["enabled"].get():
                continue

            qty_a4 = _safe_int(controls["qty_a4"].get(), 0)
            qty_a5 = _safe_int(controls["qty_a5"].get(), 0)
            qty_a6 = _safe_int(controls["qty_a6"].get(), 0)
            if (
                qty_a4 < 0
                or qty_a4 > 999
                or qty_a5 < 0
                or qty_a5 > 999
                or qty_a6 < 0
                or qty_a6 > 999
            ):
                messagebox.showerror("Quantidade invalida", f"Use quantidade entre 0 e 999 para '{item.name}'.")
                return
            total_qty = qty_a4 + qty_a5 + qty_a6
            if total_qty <= 0:
                messagebox.showerror(
                    "Quantidade invalida",
                    f"Informe ao menos 1 copia em A4, A5 ou A6 para '{item.name}'.",
                )
                return

            name_value = re.sub(r"\s+", " ", str(controls["name"].get()).strip())
            if not name_value:
                messagebox.showerror("Nome invalido", f"Informe o nome do produto da linha {item.index}.")
                return
            name_value = correct_accents_in_text(name_value)

            price_raw = re.sub(r"\s+", " ", str(controls["price"].get()).strip())
            if not price_raw:
                messagebox.showerror("Preco invalido", f"Informe o preco do produto '{name_value}'.")
                return
            price_value = to_price_text(price_raw, price_prefix)

            format_quantities = {"A4": qty_a4, "A5": qty_a5, "A6": qty_a6}
            preferred_format = "A4" if qty_a4 > 0 else ("A5" if qty_a5 > 0 else "A6")
            plate_format = normalize_plate_format(preferred_format, default="A4")
            unit = normalize_unit_label(str(controls["unit"].get()))
            duplex_row_enabled = bool(controls["duplex_enabled"].get())
            validity_enabled = bool(controls["offer_validity_enabled"].get())
            validity_month = today.month
            validity_month_raw = str(controls["offer_validity_month"].get()).strip()
            if validity_month_raw:
                try:
                    validity_month = int(validity_month_raw)
                except Exception:
                    messagebox.showerror("Mes invalido", f"Mes de validade invalido para '{name_value}'.")
                    return
            if validity_month < 1 or validity_month > 12:
                messagebox.showerror(
                    "Mes invalido",
                    f"Use mes entre 1 e 12 para '{name_value}'.",
                )
                return
            max_day_for_month = _max_day_in_month(today.year, validity_month)
            validity_day = today.day
            validity_day_raw = str(controls["offer_validity_day"].get()).strip()
            if validity_day_raw:
                try:
                    validity_day = int(validity_day_raw)
                except Exception:
                    messagebox.showerror("Dia invalido", f"Dia de validade invalido para '{name_value}'.")
                    return
            if validity_day < 1 or validity_day > max_day_for_month:
                messagebox.showerror(
                    "Dia invalido",
                    (
                        f"Use dia entre 1 e {max_day_for_month} para "
                        f"'{name_value}' em {validity_month:02d}/{today.year:04d}."
                    ),
                )
                return

            updated_row = dict(item.row)
            updated_row["unidade"] = unit
            updated_row["formato_placa"] = plate_format
            updated_row["qtd_a4"] = str(qty_a4)
            updated_row["qtd_a5"] = str(qty_a5)
            updated_row["qtd_a6"] = str(qty_a6)
            updated_row["preco"] = price_value
            updated_row["nome"] = name_value
            updated_row["frente_verso"] = "1" if duplex_row_enabled else "0"
            updated_row["produto"] = name_value
            updated_row["descricao"] = name_value
            updated_row["descrição"] = name_value
            updated_row["descricao_produto"] = name_value
            updated_row["nome_produto"] = name_value
            updated_row["descricao_do_produto"] = name_value
            updated_row["oferta_validade_ativa"] = "1" if validity_enabled else "0"
            updated_row["oferta_validade_dia"] = f"{validity_day:02d}"
            updated_row["oferta_validade_mes"] = f"{validity_month:02d}"
            updated_row["oferta_validade_ano"] = f"{today.year:04d}"
            updated_row["oferta_validade_texto"] = (
                build_offer_validity_text(validity_day, today=today, month_value=validity_month)
                if validity_enabled
                else ""
            )
            remember_product_name_cleanup(
                _resolve_plate_original_name(item),
                name_value,
                unit_value=unit,
                source="confirmed_tk",
                model_name="local_ui",
            )

            selected.append(
                PlateData(
                    index=item.index,
                    name=name_value,
                    price=price_value,
                    row=updated_row,
                    original_name=_resolve_plate_original_name(item),
                    cleanup_source="confirmed_tk",
                    quantity=total_qty,
                    unit_label=unit,
                    plate_format=plate_format,
                    format_quantities=format_quantities,
                    duplex_enabled=duplex_row_enabled,
                    offer_validity_enabled=validity_enabled,
                    offer_validity_day=validity_day,
                    offer_validity_month=validity_month,
                )
            )

        if not selected:
            messagebox.showerror("Nenhuma placa selecionada", "Selecione ao menos uma placa para continuar.")
            return

        configured_items = selected
        configured_auto_print = bool(auto_print_var.get())
        configured_duplex_print = bool(duplex_print_var.get())
        configured_shutdown_after_print = bool(shutdown_after_print_var.get())
        try:
            canvas.unbind_all("<MouseWheel>")
        except Exception:
            pass
        root.destroy()

    ttk.Checkbutton(actions, text="Imprimir ao concluir", variable=auto_print_var).pack(side="left")
    ttk.Checkbutton(actions, text="Preparar frente e verso", variable=duplex_print_var).pack(side="left", padx=(10, 0))
    ttk.Checkbutton(actions, text="Desligar ao finalizar", variable=shutdown_after_print_var).pack(side="left", padx=(10, 0))
    ttk.Button(actions, text="Cancelar", command=on_cancel).pack(side="right")
    ttk.Button(actions, text="OK - Iniciar geracao", command=on_ok).pack(side="right", padx=(0, 8))

    _apply_filters()
    search_entry.focus_set()
    root.protocol("WM_DELETE_WINDOW", on_cancel)
    root.mainloop()
    if configured_items is None:
        return None
    return configured_items, configured_auto_print, configured_duplex_print, configured_shutdown_after_print

class CorelDrawAgent:
    PROFILES: Dict[str, LayoutProfile] = {
        "A4_FOLHA_COMPLETA": LayoutProfile(
            name_max_width_cm=17.5,
            name_base_font_size=70.0,
            name_min_font_size=45.0,
            name_max_font_size=120.0,
            name_split_if_font_below=70.0,
            name_line_spacing_max=70.0,
            name_two_lines_total_height_cm=4.5,
            price_max_width_cm=17.5,
            unit_gap_below_cents_cm=0.35,
            unit_font_size_kg=90.0,
            unit_font_size_unid=67.0,
            offer_validity_bottom_cm=1.35,
            offer_validity_max_width_cm=17.5,
            offer_validity_max_height_cm=0.8,
            offer_validity_font_size=18.0,
            offer_validity_min_font_size=8.0,
        ),
    }

    def __init__(self, visible: bool, profile_name: str = "Placa A4") -> None:
        self.visible = visible
        self.app = None
        self._fast_mode_enabled = False
        self._learning_cache_path = Path(__file__).with_name(LEARNING_CACHE_FILE)
        self._name_fit_cache: Dict[str, Dict[str, Any]] = {}
        self._learning_cache_dirty = False
        self._learning_cache_updates = 0
        requested_profile = normalize_key(profile_name or "Placa A4").upper()
        profile_aliases = {
            "PLACA_A4": "A4_FOLHA_COMPLETA",
            "A4_FOLHA_COMPLETA": "A4_FOLHA_COMPLETA",
        }
        self.profile_name = profile_aliases.get(requested_profile, requested_profile)
        self.profile = self.PROFILES.get(self.profile_name, self.PROFILES["A4_FOLHA_COMPLETA"])
        self._load_learning_cache()

    def _load_learning_cache(self) -> None:
        self._name_fit_cache = {}
        try:
            payload = _load_learning_payload_file(self._learning_cache_path)
            cache_blob = payload.get("name_fit_cache")
            if not isinstance(cache_blob, dict):
                return
            sanitized: Dict[str, Dict[str, Any]] = {}
            for key, value in cache_blob.items():
                if isinstance(key, str) and isinstance(value, dict):
                    sanitized[key] = value
            self._name_fit_cache = sanitized
        except Exception:
            self._name_fit_cache = {}

    def persist_learning_cache(self, force: bool = False) -> None:
        if not force and not self._learning_cache_dirty:
            return
        try:
            payload = _load_learning_payload_file(self._learning_cache_path)
            payload["name_fit_cache"] = self._name_fit_cache
            _save_learning_payload_file(self._learning_cache_path, payload)
            self._learning_cache_dirty = False
        except Exception:
            pass

    def _mark_learning_cache_updated(self) -> None:
        self._learning_cache_dirty = True
        self._learning_cache_updates += 1
        if self._learning_cache_updates % 10 == 0:
            self.persist_learning_cache()

    def learned_name_count(self) -> int:
        return len(self._name_fit_cache)

    @staticmethod
    def _safe_get_story_prop(shape, prop_name: str, default: Optional[float] = None) -> Optional[float]:
        try:
            return float(getattr(shape.Text.Story, prop_name))
        except Exception:
            return default

    @staticmethod
    def _safe_set_story_prop(shape, prop_name: str, value: float) -> bool:
        try:
            setattr(shape.Text.Story, prop_name, value)
            return True
        except Exception:
            return False

    @staticmethod
    def _safe_get_shape_story_text(shape, default: str = "") -> str:
        for getter in (
            lambda: str(shape.Text.Story),
            lambda: str(shape.Text.Story.Text),
            lambda: str(shape.Text.Text),
            lambda: str(shape.Text.Story.Characters.All.Text),
        ):
            try:
                text = getter()
                if text is None:
                    continue
                return str(text)
            except Exception:
                pass
        return default

    @staticmethod
    def _safe_set_shape_story_text(shape, value: str) -> bool:
        text_value = str(value)
        setters: List[Callable[[], None]] = [
            lambda: setattr(shape.Text, "Story", text_value),
            lambda: setattr(shape.Text.Story, "Text", text_value),
            lambda: setattr(shape.Text, "Text", text_value),
            lambda: setattr(shape.Text.Story.Characters.All, "Text", text_value),
        ]

        def _replace_via_delete_insert() -> None:
            story_obj = shape.Text.Story
            try:
                story_obj.Delete()
            except Exception:
                pass
            story_obj.InsertAfter(text_value)

        setters.append(_replace_via_delete_insert)

        for setter in setters:
            try:
                setter()
                return True
            except Exception:
                pass
        return False

    def _build_name_fit_cache_key(self, text: str, max_w: float, max_h: float) -> str:
        normalized_text = normalize_key(text or "")
        if not normalized_text:
            return ""
        width_bucket = round(float(max_w), 2)
        height_bucket = round(float(max_h), 2)
        return f"{self.profile_name}|{width_bucket}|{height_bucket}|{normalized_text}"

    def _set_app_fast_mode(self, enabled: bool) -> None:
        if not self.app:
            return
        # Quando a janela esta visivel, evita modo rapido para permitir
        # acompanhar a edicao em tempo real no CorelDRAW.
        effective_enabled = bool(enabled) and (not bool(self.visible))
        if self._fast_mode_enabled == effective_enabled:
            if not effective_enabled:
                try:
                    self.app.Refresh()
                except Exception:
                    pass
            return
        try:
            self.app.Optimization = effective_enabled
        except Exception:
            pass
        try:
            self.app.EventsEnabled = not effective_enabled
        except Exception:
            pass
        if not effective_enabled:
            try:
                self.app.Refresh()
            except Exception:
                pass
        try:
            self.app.ActiveWindow.Refresh()
        except Exception:
            pass
        self._fast_mode_enabled = effective_enabled

    def open(self) -> None:
        try:
            import win32com.client  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "Dependencia ausente: pywin32. Instale com `pip install -r requirements.txt`."
            ) from exc

        last_error: Optional[Exception] = None
        prog_ids = [
            "CorelDRAW.Application",
            "CorelDRAW.Application.26",
            "CorelDRAW.Application.25",
            "CorelDRAW.Application.24",
        ]
        for prog_id in prog_ids:
            try:
                self.app = win32com.client.Dispatch(prog_id)
                break
            except Exception as exc:  # noqa: BLE001
                last_error = exc

        if not self.app:
            raise RuntimeError(
                f"Nao foi possivel abrir CorelDRAW via COM. Erro: {last_error!r}"
            )

        try:
            self.app.Visible = self.visible
        except Exception:
            pass

    def close(self) -> None:
        self.persist_learning_cache(force=True)
        if not self.app:
            return
        self._set_app_fast_mode(False)
        try:
            self.app.Quit()
        except Exception:
            pass
        self.app = None

    @staticmethod
    def _check_stop(stop_requested: Optional[Callable[[], bool]]) -> None:
        if stop_requested and stop_requested():
            raise StopRequestedError("Processo interrompido por F7.")

    @staticmethod
    def _is_offer_validity_caption(story: str, story_norm: Optional[str] = None) -> bool:
        normalized = story_norm or normalize_key(story)
        compact = normalized.replace("_", "")
        if "ofertavalidaateodia" in compact:
            return True
        if "duraremosestoques" in compact and "oferta" in compact:
            return True
        if ("xx_xx_xxxx" in normalized or "xx/xx/xxxx" in (story or "").lower()) and "oferta" in compact:
            return True
        return False

    @staticmethod
    def _is_offer_validity_date_placeholder_only(story: str) -> bool:
        text = (story or "").strip()
        if not text:
            return False
        return bool(re.fullmatch(r"\s*xx\s*[/.]\s*xx\s*[/.]\s*xxxx\s*", text, flags=re.IGNORECASE))

    @staticmethod
    def _is_likely_product_placeholder(story: str) -> bool:
        s = (story or "").strip()
        if not s:
            return False
        if not re.search(r"[A-Za-z\u00c0-\u00ff]", s):
            return False

        normalized = normalize_key(s)
        compact = normalized.replace("_", "")
        static_labels = {
            "preco",
            "pre_o",
            "r",
            "r_",
            "r_s",
            "kg",
            "kg_",
            "un",
            "und",
            "unid",
            "unidade",
            "pct",
            "pct_",
            "pacote",
            "bdj",
            "bandeja",
            "pack",
            "ml",
            "lt",
            "l",
            "g",
            "gr",
            "imperdivel",
            "imperd_vel",
            "por_apenas",
            "por_apena",
        }
        if normalized in static_labels:
            return False
        if compact in {
            "preco",
            "preo",
            "r",
            "rs",
            "kg",
            "un",
            "und",
            "unid",
            "unidade",
            "pct",
            "pacote",
            "bdj",
            "bandeja",
            "pack",
            "ml",
            "lt",
            "l",
            "g",
            "gr",
            "imperdivel",
            "imperdvel",
            "porapenas",
        }:
            return False
        if compact.startswith("imperd"):
            return False
        if compact.startswith("prec"):
            return False
        if "ofertavalidaateodia" in compact:
            return False
        if "duraremosestoques" in compact:
            return False

        if re.fullmatch(r"\s*[,.\d]+\s*", s):
            return False
        if re.search(r"r\$\s*\d", s.lower()):
            return False
        if len(normalized) < 4:
            return False
        return True

    def _apply_font_if_possible(self, shape, font_name: Optional[str]) -> None:
        if not font_name:
            return

        # A API COM do CorelDRAW varia por versao; tentamos caminhos comuns.
        try:
            shape.Text.Story.Font = font_name
            return
        except Exception:
            pass
        try:
            shape.Text.SetFont(font_name)
            return
        except Exception:
            pass
        try:
            shape.Text.Story.Characters.All.Font = font_name
        except Exception:
            pass

    @staticmethod
    def _safe_shape_size(shape) -> tuple[float, float]:
        try:
            return float(shape.SizeWidth), float(shape.SizeHeight)
        except Exception:
            return 0.0, 0.0

    @staticmethod
    def _safe_get_prop(shape, prop_name: str, default: float = 0.0) -> float:
        try:
            return float(getattr(shape, prop_name))
        except Exception:
            return default

    @staticmethod
    def _safe_set_prop(shape, prop_name: str, value: float) -> bool:
        try:
            setattr(shape, prop_name, value)
            return True
        except Exception:
            return False

    def _safe_page_bounds(self, shape) -> tuple[float, float, float, float]:
        # left, right, bottom, top
        try:
            page = shape.ParentPage
            left = self._safe_get_prop(page, "LeftX", 0.0)
            right = self._safe_get_prop(page, "RightX", 0.0)
            bottom = self._safe_get_prop(page, "BottomY", 0.0)
            top = self._safe_get_prop(page, "TopY", 0.0)
            if right > left and top > bottom:
                return left, right, bottom, top
        except Exception:
            pass
        if self.app:
            try:
                page = self.app.ActiveDocument.ActivePage
                left = self._safe_get_prop(page, "LeftX", 0.0)
                right = self._safe_get_prop(page, "RightX", 0.0)
                bottom = self._safe_get_prop(page, "BottomY", 0.0)
                top = self._safe_get_prop(page, "TopY", 0.0)
                if right > left and top > bottom:
                    return left, right, bottom, top
            except Exception:
                pass
        w = self._safe_page_width(shape)
        return 0.0, w if w > 0 else 0.0, 0.0, self._safe_get_prop(shape, "TopY", 0.0)

    @staticmethod
    def _is_unit_label_text(story: str) -> bool:
        norm = normalize_key(story).replace("_", "")
        return norm in {
            "kg",
            "g",
            "gr",
            "un",
            "und",
            "unid",
            "unidade",
            "pct",
            "pacote",
            "bdj",
            "bandeja",
            "pack",
            "ml",
            "l",
            "lt",
        }

    @staticmethod
    def _cm_to_doc_units_by_bounds(cm_value: float, page_left: float, page_right: float) -> float:
        page_w = max(page_right - page_left, 0.0)
        # A4 = 21 cm de largura.
        if page_w > 0:
            return (cm_value / 21.0) * page_w
        return cm_value

    def _fit_price_shape_to_bounds(self, shape, left: float, right: float, top: float, bottom: float) -> None:
        width = self._safe_get_prop(shape, "SizeWidth")
        height = self._safe_get_prop(shape, "SizeHeight")
        if width <= 0 or height <= 0:
            return

        max_w = max(right - left, 0.1)
        max_h = max(top - bottom, 0.1)
        min_size = 12.0

        # Reduz fonte para caber na caixa com menos iteracoes.
        self._fit_font_within_shape(shape, max_w, max_h, min_size, max_steps=24)

        # Garante posicionamento final dentro da caixa.
        cur_left = self._safe_get_prop(shape, "LeftX")
        cur_right = self._safe_get_prop(shape, "RightX")
        cur_top = self._safe_get_prop(shape, "TopY")
        cur_bottom = self._safe_get_prop(shape, "BottomY")
        if cur_right > right:
            self._safe_set_prop(shape, "LeftX", cur_left - (cur_right - right))
        if cur_left < left:
            self._safe_set_prop(shape, "LeftX", left)
        if cur_top > top:
            self._safe_set_prop(shape, "TopY", top)
        if cur_bottom < bottom:
            self._safe_set_prop(shape, "TopY", self._safe_get_prop(shape, "TopY") + (bottom - cur_bottom))

    @staticmethod
    def _safe_page_width(shape) -> float:
        try:
            page = shape.ParentPage
            return float(page.SizeWidth)
        except Exception:
            return 0.0

    @staticmethod
    def _safe_get_font_size(shape) -> float:
        for accessor in (
            lambda: float(shape.Text.Story.Size),
            lambda: float(shape.Text.Size),
            lambda: float(shape.Text.Story.Characters.All.Size),
        ):
            try:
                return accessor()
            except Exception:
                pass
        return 18.0

    @staticmethod
    def _safe_set_font_size(shape, size_value: float) -> None:
        for setter in (
            lambda: setattr(shape.Text.Story, "Size", size_value),
            lambda: setattr(shape.Text, "Size", size_value),
            lambda: setattr(shape.Text.Story.Characters.All, "Size", size_value),
        ):
            try:
                setter()
                return
            except Exception:
                pass

    def _fit_font_within_shape(
        self,
        shape,
        max_w: float,
        max_h: float,
        min_size: float,
        max_steps: int = 24,
    ) -> float:
        current_size = max(self._safe_get_font_size(shape), min_size)

        def fits() -> bool:
            cur_w, cur_h = self._safe_shape_size(shape)
            return cur_w <= max_w and cur_h <= max_h

        if fits():
            return current_size

        # Chute inicial proporcional para evitar varias reducoes lineares.
        cur_w, cur_h = self._safe_shape_size(shape)
        if cur_w > 0 and cur_h > 0:
            factor_w = (max_w / cur_w) if cur_w > 0 else 1.0
            factor_h = (max_h / cur_h) if cur_h > 0 else 1.0
            factor = min(factor_w, factor_h)
            if factor < 1.0:
                current_size = max(min_size, current_size * max(factor * 0.98, 0.1))
                self._safe_set_font_size(shape, current_size)
                if fits():
                    return current_size

        # Busca binaria: encontra o maior tamanho que cabe com poucas iteracoes.
        low = float(min_size)
        self._safe_set_font_size(shape, low)
        if not fits():
            return low

        high = float(max(current_size, low))
        best = low
        steps = max(1, min(int(max_steps), 10))
        for _ in range(steps):
            if (high - low) <= 0.1:
                break
            mid = (low + high) / 2.0
            self._safe_set_font_size(shape, mid)
            if fits():
                best = mid
                low = mid
            else:
                high = mid

        self._safe_set_font_size(shape, best)
        return best

    @staticmethod
    def _best_two_line_split(text: str) -> str:
        words = [w for w in text.split() if w]
        if len(words) < 2:
            return text

        best = text
        best_score = 10**9
        for cut in range(1, len(words)):
            line1 = " ".join(words[:cut])
            line2 = " ".join(words[cut:])
            score = max(len(line1), len(line2)) * 10 + abs(len(line1) - len(line2))
            if score < best_score:
                best_score = score
                # Corel artistic text respeita quebra melhor com CR.
                best = f"{line1}\r{line2}"
        return best

    @staticmethod
    def _safe_shape_bounds(shape) -> tuple[float, float, float, float]:
        left = CorelDrawAgent._safe_get_prop(shape, "LeftX")
        right = CorelDrawAgent._safe_get_prop(shape, "RightX")
        bottom = CorelDrawAgent._safe_get_prop(shape, "BottomY")
        top = CorelDrawAgent._safe_get_prop(shape, "TopY")
        if right < left:
            left, right = right, left
        if top < bottom:
            bottom, top = top, bottom
        return left, right, bottom, top

    @staticmethod
    def _rects_overlap(
        rect_a: tuple[float, float, float, float],
        rect_b: tuple[float, float, float, float],
        padding: float = 0.0,
    ) -> bool:
        left_a, right_a, bottom_a, top_a = rect_a
        left_b, right_b, bottom_b, top_b = rect_b
        return not (
            right_a <= (left_b + padding)
            or right_b <= (left_a + padding)
            or top_a <= (bottom_b + padding)
            or top_b <= (bottom_a + padding)
        )

    def _shape_overlaps_bounds(
        self,
        shape,
        blockers: List[tuple[float, float, float, float]],
        padding: float = 0.0,
    ) -> bool:
        rect = self._safe_shape_bounds(shape)
        for blocker in blockers:
            if self._rects_overlap(rect, blocker, padding=padding):
                return True
        return False

    def _iter_text_shapes(self, shapes):
        try:
            count = int(getattr(shapes, "Count", 0))
        except Exception:
            count = 0
        for idx in range(1, count + 1):
            try:
                shape = shapes.Item(idx)
            except Exception:
                continue
            try:
                story = self._safe_get_shape_story_text(shape, "")
                yield shape, story
            except Exception:
                pass
            try:
                nested = shape.Shapes
                nested_count = int(getattr(nested, "Count", 0))
                if nested_count > 0:
                    yield from self._iter_text_shapes(nested)
            except Exception:
                pass

    def _is_non_product_label(self, story: str) -> bool:
        text = (story or "").strip()
        if not text:
            return True
        if not re.search(r"[A-Za-z\u00c0-\u00ff]", text):
            return True
        if PRICE_INLINE_RE.search(text):
            return True
        norm = normalize_key(text).replace("_", "")
        if not norm:
            return True
        if self._is_offer_validity_caption(text, norm):
            return True
        blocked_exact = {
            "oferta",
            "imperdivel",
            "porapenas",
            "preco",
            "rs",
            "r",
            "kg",
            "un",
            "und",
            "unid",
            "unidade",
            "medida",
        }
        if norm in blocked_exact:
            return True
        if norm.startswith("oferta") or norm.startswith("imperd") or norm.startswith("prec"):
            return True
        if len(norm) < 4:
            return True
        return False

    @staticmethod
    def _is_product_placeholder_label(story: str) -> bool:
        normalized = normalize_key(story)
        return normalized in {
            "nome",
            "produto",
            "nome_produto",
            "nome_do_produto",
            "descricao",
            "descricao_produto",
            "descricao_do_produto",
            "item",
            "item_nome",
            "product",
            "product_name",
        }

    @staticmethod
    def _name_matches_expected(expected_name: str, candidate_story: str) -> bool:
        expected_norm = normalize_key(expected_name).replace("_", "")
        candidate_norm = normalize_key(candidate_story).replace("_", "")
        if not expected_norm or not candidate_norm:
            return False
        if candidate_norm == expected_norm:
            return True
        if len(expected_norm) >= 6 and expected_norm in candidate_norm:
            return True
        if len(candidate_norm) >= 6 and candidate_norm in expected_norm:
            return True
        return False

    def _is_name_applied_on_page(
        self,
        page,
        expected_name: str,
        page_context: Optional[Dict[str, list]] = None,
    ) -> bool:
        expected = re.sub(r"\s+", " ", (expected_name or "").strip())
        if not expected:
            return True

        name_bucket = list((page_context or {}).get("name_text", []))
        seen_any_candidate = False
        for candidate_shape in name_bucket:
            candidate_story = self._safe_get_shape_story_text(candidate_shape, "")
            candidate_story = re.sub(r"\s+", " ", candidate_story or "").strip()
            if not candidate_story:
                continue
            seen_any_candidate = True
            if self._name_matches_expected(expected, candidate_story):
                return True
            if (
                re.search(r"[A-Za-z\u00c0-\u00ff]", candidate_story)
                and "{" not in candidate_story
                and "}" not in candidate_story
                and not self._is_product_placeholder_label(candidate_story)
            ):
                # Aceita como aplicado para evitar falso positivo quando o Corel
                # altera pequenas partes do texto (quebra de linha/espacamento).
                return True

        if seen_any_candidate:
            return False

        # Fallback: procura em todos os textos da pagina.
        for _shape, story in self._iter_text_shapes(page.Shapes):
            candidate_story = re.sub(r"\s+", " ", story or "").strip()
            if self._name_matches_expected(expected, candidate_story):
                return True
        return False

    def _force_replace_product_name_on_page(
        self,
        page,
        target_name: str,
        font_name: Optional[str],
        layout_context: Optional[Dict[str, list]] = None,
    ) -> bool:
        name_text = re.sub(r"\s+", " ", (target_name or "").strip())
        if not name_text:
            return False

        bounds = layout_context.get("__page_bounds__") if layout_context else None
        if bounds and len(bounds) == 4:
            page_left, page_right, page_bottom, page_top = bounds
        else:
            try:
                page_left = float(page.LeftX)
                page_right = float(page.RightX)
                page_bottom = float(page.BottomY)
                page_top = float(page.TopY)
            except Exception:
                page_left, page_right, page_bottom, page_top = 0.0, 1.0, 0.0, 1.0
        page_height = max(page_top - page_bottom, 0.1)

        candidates: List[tuple[float, Any, str]] = []
        for shape, story in self._iter_text_shapes(page.Shapes):
            if self._is_non_product_label(story):
                continue
            left, right, bottom, top = self._safe_shape_bounds(shape)
            width = max(right - left, 0.01)
            height = max(top - bottom, 0.01)
            area = width * height
            center_y = (top + bottom) / 2.0
            rel_y = (center_y - page_bottom) / page_height
            position_bonus = max(0.1, 1.0 - abs(rel_y - 0.45))
            score = area * (1.0 + position_bonus)
            candidates.append((score, shape, story))

        if not candidates:
            return False

        candidates.sort(key=lambda entry: entry[0], reverse=True)
        _, best_shape, _ = candidates[0]
        applied = False
        try:
            self._fit_product_text(best_shape, name_text, font_name)
            applied = True
        except Exception:
            applied = False
        if not applied:
            try:
                if self._safe_set_shape_story_text(best_shape, name_text):
                    self._apply_font_if_possible(best_shape, font_name)
                    applied = True
                else:
                    applied = False
            except Exception:
                applied = False
        if applied and layout_context is not None:
            bucket = layout_context.setdefault("name_text", [])
            if best_shape not in bucket:
                bucket.append(best_shape)
        return applied

    def _fit_product_text(self, shape, new_text: str, font_name: Optional[str]) -> None:
        new_text = re.sub(r"\s+", " ", (new_text or "").strip())
        if not new_text:
            return

        anchor_top = self._safe_get_prop(shape, "TopY")
        page_left, page_right, _, _ = self._safe_page_bounds(shape)
        shape_left = self._safe_get_prop(shape, "LeftX")
        original_w, original_h = self._safe_shape_size(shape)
        left_margin = 0.05
        right_margin = 0.05
        target_left_bound = page_left + left_margin
        target_right_bound = page_right - right_margin
        max_w_profile = self._cm_to_doc_units_by_bounds(self.profile.name_max_width_cm, page_left, page_right)
        max_w_right_bound = max(target_right_bound - max(shape_left, target_left_bound), 0.2)
        max_w_page_bound = max(target_right_bound - target_left_bound, 0.2)
        max_w = min(max_w_profile, max_w_right_bound) if max_w_profile > 0 else max_w_right_bound
        max_w = min(max_w, max_w_page_bound)
        if max_w <= 0:
            max_w = max(original_w, 1.0)
        max_h_limit = self._cm_to_doc_units_by_bounds(
            self.profile.name_two_lines_total_height_cm, page_left, page_right
        )
        max_h_limit = max(max_h_limit, original_h)
        min_size = max(self.profile.name_min_font_size, 1.0)
        max_size = max(self.profile.name_max_font_size, min_size)
        cache_key = self._build_name_fit_cache_key(new_text, max_w, max_h_limit)

        def clamp_and_center_horizontally() -> None:
            shape_w = max(self._safe_get_prop(shape, "SizeWidth"), 0.1)
            available_w = max(target_right_bound - target_left_bound, 0.2)
            centered_left = target_left_bound + max((available_w - shape_w) / 2.0, 0.0)
            max_left = max(target_left_bound, target_right_bound - shape_w)
            self._safe_set_prop(shape, "LeftX", min(max(centered_left, target_left_bound), max_left))

        def finalize_positioning() -> None:
            for _ in range(24):
                cur_w_check, cur_h_check = self._safe_shape_size(shape)
                if cur_w_check <= max_w and cur_h_check <= max_h_limit:
                    break
                current_size = self._safe_get_font_size(shape)
                next_size = max(min_size, current_size - 0.5)
                if next_size >= current_size:
                    break
                self._safe_set_font_size(shape, next_size)
            clamp_and_center_horizontally()
            self._safe_set_prop(shape, "TopY", anchor_top)

        def fits_current_shape() -> bool:
            cur_w_check, cur_h_check = self._safe_shape_size(shape)
            return cur_w_check <= max_w and cur_h_check <= max_h_limit

        # Reaproveita aprendizado anterior do mesmo nome para acelerar repeticoes.
        cached_fit = self._name_fit_cache.get(cache_key) if cache_key else None
        if isinstance(cached_fit, dict):
            try:
                cached_size = float(cached_fit.get("size", max_size))
            except Exception:
                cached_size = max_size
            cached_size = max(min_size, min(max_size, cached_size))
            cached_line_spacing = cached_fit.get("line_spacing")
            cached_char_spacing = cached_fit.get("char_spacing")

            # Usa sempre o texto atual (ja corrigido), reaproveitando apenas metrica.
            if not self._safe_set_shape_story_text(shape, new_text):
                raise RuntimeError("Nao foi possivel atualizar o texto do nome no shape.")
            self._apply_font_if_possible(shape, font_name)
            self._safe_set_font_size(shape, cached_size)
            if cached_line_spacing is not None:
                try:
                    self._safe_set_story_prop(shape, "LineSpacing", float(cached_line_spacing))
                except Exception:
                    pass
            if cached_char_spacing is not None:
                try:
                    self._safe_set_story_prop(shape, "CharSpacing", float(cached_char_spacing))
                except Exception:
                    pass
            for para_zero in ("ParagraphSpacingBefore", "ParagraphSpacingAfter"):
                try:
                    self._safe_set_story_prop(shape, para_zero, 0.0)
                except Exception:
                    pass

            if fits_current_shape():
                finalize_positioning()
                return

        # Primeiro tenta em 1 linha no range configurado no perfil.
        if not self._safe_set_shape_story_text(shape, new_text):
            raise RuntimeError("Nao foi possivel atualizar o texto do nome no shape.")
        self._apply_font_if_possible(shape, font_name)
        self._safe_set_story_prop(shape, "CharSpacing", 0.0)
        self._safe_set_font_size(shape, max_size)
        self._fit_font_within_shape(shape, max_w, max_h_limit, min_size, max_steps=36)
        best_one_line_size = self._safe_get_font_size(shape)
        cur_w, cur_h = self._safe_shape_size(shape)

        # Se nao couber em 1 linha dentro de 17,5 x 4,5 cm, quebra em 2 linhas.
        should_split = " " in new_text and (
            best_one_line_size <= self.profile.name_split_if_font_below
            or cur_w > max_w
            or cur_h > max_h_limit
        )
        if cur_w > max_w or cur_h > max_h_limit:
            should_split = True
        if should_split:
            if not self._safe_set_shape_story_text(shape, self._best_two_line_split(new_text)):
                raise RuntimeError("Nao foi possivel aplicar quebra de linha no nome.")
            self._apply_font_if_possible(shape, font_name)

            # Mantem espacamento entre linhas em 70.0.
            try:
                shape.Text.Story.LineSpacing = float(self.profile.name_line_spacing_max)
            except Exception:
                pass
            for para_zero in ("ParagraphSpacingBefore", "ParagraphSpacingAfter"):
                try:
                    setattr(shape.Text.Story, para_zero, 0)
                except Exception:
                    pass

            self._safe_set_font_size(shape, max_size)
            self._fit_font_within_shape(shape, max_w, max_h_limit, min_size, max_steps=40)

        # Ajuste final de espaco entre caracteres quando faltar pouco.
        cur_w, cur_h = self._safe_shape_size(shape)
        if cur_w > max_w or cur_h > max_h_limit:
            for spacing in (-0.5, -1.0, -1.5, -2.0):
                try:
                    shape.Text.Story.CharSpacing = spacing
                except Exception:
                    break
                cur_w, cur_h = self._safe_shape_size(shape)
                if cur_w <= max_w and cur_h <= max_h_limit:
                    break

        # Mantem o topo original para nao "descer" quando quebrar em 2 linhas.
        finalize_positioning()

        # Aprende o melhor ajuste encontrado para acelerar proximas placas do mesmo produto.
        if cache_key:
            learned_story = self._safe_get_shape_story_text(shape, "")
            learned_size = round(float(self._safe_get_font_size(shape)), 2)
            learned_line_spacing = self._safe_get_story_prop(shape, "LineSpacing", None)
            learned_char_spacing = self._safe_get_story_prop(shape, "CharSpacing", None)
            previous_hits = 0
            if isinstance(cached_fit, dict):
                try:
                    previous_hits = int(cached_fit.get("hits", 0))
                except Exception:
                    previous_hits = 0
            learned_entry: Dict[str, Any] = {
                "story": learned_story,
                "size": learned_size,
                "hits": previous_hits + 1,
            }
            if learned_line_spacing is not None:
                learned_entry["line_spacing"] = round(float(learned_line_spacing), 2)
            if learned_char_spacing is not None:
                learned_entry["char_spacing"] = round(float(learned_char_spacing), 2)
            if self._name_fit_cache.get(cache_key) != learned_entry:
                self._name_fit_cache[cache_key] = learned_entry
                self._mark_learning_cache_updated()

    def _align_price_parts(self, layout_context: Optional[Dict[str, list]]) -> None:
        if not layout_context:
            return
        int_shapes = layout_context.get("price_integer", [])
        cents_shapes = layout_context.get("price_cents", [])
        full_shapes = layout_context.get("price_full", [])
        unit_shapes = layout_context.get("unit_label", [])
        target_unit_label = str(layout_context.get("__unit_label__", "KG")).strip() or "KG"
        name_shapes = layout_context.get("name_text", [])
        name_blockers = [
            self._safe_shape_bounds(shape)
            for shape in name_shapes
            if self._safe_get_prop(shape, "SizeWidth") > 0 and self._safe_get_prop(shape, "SizeHeight") > 0
        ]

        # Caso template use um unico texto para preco completo.
        if full_shapes and (not int_shapes or not cents_shapes):
            full_shape = max(full_shapes, key=lambda s: self._safe_get_prop(s, "SizeWidth"))
            page_left, page_right, page_bottom, page_top = self._safe_page_bounds(full_shape)
            left_margin = 0.05
            right_margin = 0.05
            top_margin = 0.05
            bottom_margin = 0.05
            target_left = page_left + left_margin
            target_right = page_right - right_margin
            center_x = (target_left + target_right) / 2.0
            max_price_w = self._cm_to_doc_units_by_bounds(self.profile.price_max_width_cm, page_left, page_right)
            max_price_w = max(min(max_price_w, target_right - target_left), 0.2)

            self._fit_price_shape_to_bounds(
                full_shape,
                max(target_left, center_x - (max_price_w / 2.0)),
                min(target_right, center_x + (max_price_w / 2.0)),
                page_top - top_margin,
                page_bottom + bottom_margin,
            )

            def center_full_price() -> None:
                shape_w = max(self._safe_get_prop(full_shape, "SizeWidth"), 0.1)
                new_left = center_x - (shape_w / 2.0)
                max_left = max(target_left, target_right - shape_w)
                new_left = min(max(new_left, target_left), max_left)
                self._safe_set_prop(full_shape, "LeftX", new_left)

            def full_price_fits() -> bool:
                center_full_price()
                left, right, bottom, top = self._safe_shape_bounds(full_shape)
                fits_bounds = (
                    left >= target_left
                    and right <= target_right
                    and (right - left) <= max_price_w
                    and top <= (page_top - top_margin)
                    and bottom >= (page_bottom + bottom_margin)
                )
                if not fits_bounds:
                    return False
                return not self._shape_overlaps_bounds(full_shape, name_blockers, padding=0.01)

            min_size = 12.0
            current_size = max(self._safe_get_font_size(full_shape), min_size)
            self._safe_set_font_size(full_shape, current_size)
            for _ in range(28):
                if full_price_fits():
                    break
                current_size -= 0.5
                if current_size <= min_size:
                    current_size = min_size
                    self._safe_set_font_size(full_shape, current_size)
                    break
                self._safe_set_font_size(full_shape, current_size)

            best_size = self._safe_get_font_size(full_shape)
            if full_price_fits():
                for _ in range(10):
                    candidate = best_size + 0.5
                    self._safe_set_font_size(full_shape, candidate)
                    if full_price_fits():
                        best_size = candidate
                    else:
                        break
                self._safe_set_font_size(full_shape, best_size)
                center_full_price()
            # Garantia final: nunca ultrapassar a largura maxima configurada.
            for _ in range(20):
                left, right, _, _ = self._safe_shape_bounds(full_shape)
                cur_w = right - left
                if cur_w <= max_price_w:
                    break
                ratio = max_price_w / max(cur_w, 0.1)
                current_size = max(self._safe_get_font_size(full_shape) * ratio * 0.98, 12.0)
                self._safe_set_font_size(full_shape, current_size)
                center_full_price()
            return

        if not int_shapes or not cents_shapes:
            return

        # Usa o maior numero inteiro como referencia principal do preco.
        int_shape = max(int_shapes, key=lambda s: self._safe_get_prop(s, "SizeWidth"))
        cents_shape = max(cents_shapes, key=lambda s: self._safe_get_prop(s, "SizeHeight"))

        bounds = layout_context.get("__page_bounds__") if layout_context else None
        if bounds and len(bounds) == 4:
            page_left, page_right, page_bottom, page_top = bounds
        else:
            page_left, page_right, page_bottom, page_top = self._safe_page_bounds(int_shape)
        left_margin = 0.05
        right_margin = 0.05
        top_margin = 0.05
        bottom_margin = 0.05

        target_left_bound = page_left + left_margin
        target_right_bound = page_right - right_margin
        anchor_top = self._safe_get_prop(int_shape, "TopY")
        anchor_top = min(anchor_top, page_top - top_margin)
        max_group_w = self._cm_to_doc_units_by_bounds(self.profile.price_max_width_cm, page_left, page_right)
        max_group_w = min(max_group_w, target_right_bound - target_left_bound)
        max_group_w = max(max_group_w, 0.2)
        min_gap = 0.02
        center_x = (target_left_bound + target_right_bound) / 2.0

        def place_group(top_y: float) -> tuple[tuple[float, float, float, float], tuple[float, float, float, float]]:
            int_w = max(self._safe_get_prop(int_shape, "SizeWidth"), 0.1)
            int_h = max(self._safe_get_prop(int_shape, "SizeHeight"), 0.1)
            cents_w = max(self._safe_get_prop(cents_shape, "SizeWidth"), 0.1)
            gap = max(int_h * 0.03, 0.06)
            group_w = int_w + gap + cents_w

            int_left = center_x - (group_w / 2.0)
            max_left = max(target_left_bound, target_right_bound - group_w)
            int_left = min(max(int_left, target_left_bound), max_left)
            self._safe_set_prop(int_shape, "LeftX", int_left)
            self._safe_set_prop(int_shape, "TopY", top_y)
            int_right_now = self._safe_get_prop(int_shape, "RightX")

            self._safe_set_prop(cents_shape, "LeftX", int_right_now + gap)
            self._safe_set_prop(cents_shape, "TopY", top_y)

            int_rect = self._safe_shape_bounds(int_shape)
            cents_rect = self._safe_shape_bounds(cents_shape)
            group_left = min(int_rect[0], cents_rect[0])
            group_right = max(int_rect[1], cents_rect[1])
            if group_left < target_left_bound:
                shift = target_left_bound - group_left
                self._safe_set_prop(int_shape, "LeftX", self._safe_get_prop(int_shape, "LeftX") + shift)
                self._safe_set_prop(cents_shape, "LeftX", self._safe_get_prop(cents_shape, "LeftX") + shift)
            elif group_right > target_right_bound:
                shift = target_right_bound - group_right
                self._safe_set_prop(int_shape, "LeftX", self._safe_get_prop(int_shape, "LeftX") + shift)
                self._safe_set_prop(cents_shape, "LeftX", self._safe_get_prop(cents_shape, "LeftX") + shift)
            return self._safe_shape_bounds(int_shape), self._safe_shape_bounds(cents_shape)

        def group_status(top_y: float) -> tuple[bool, bool]:
            int_rect, cents_rect = place_group(top_y)
            group_left = min(int_rect[0], cents_rect[0])
            group_right = max(int_rect[1], cents_rect[1])
            group_top = max(int_rect[3], cents_rect[3])
            group_bottom = min(int_rect[2], cents_rect[2])
            no_overlap_between_parts = cents_rect[0] >= (int_rect[1] + min_gap)
            fits_h = (
                group_left >= target_left_bound
                and group_right <= target_right_bound
                and (group_right - group_left) <= max_group_w
            )
            fits_v = group_top <= (page_top - top_margin) and group_bottom >= (page_bottom + bottom_margin)
            in_bounds = no_overlap_between_parts and fits_h and fits_v
            clear_of_names = (
                not self._shape_overlaps_bounds(int_shape, name_blockers, padding=0.01)
                and not self._shape_overlaps_bounds(cents_shape, name_blockers, padding=0.01)
            )
            return in_bounds, clear_of_names

        def find_clear_top(start_top: float) -> tuple[float, bool]:
            top_now = min(start_top, page_top - top_margin)
            step = max((page_top - page_bottom) / 260.0, 0.03)
            for _ in range(40):
                in_bounds, clear_of_names = group_status(top_now)
                if in_bounds and clear_of_names:
                    return top_now, True
                current_bottom = min(self._safe_get_prop(int_shape, "BottomY"), self._safe_get_prop(cents_shape, "BottomY"))
                if current_bottom <= (page_bottom + bottom_margin):
                    break
                top_now -= step
            return min(start_top, page_top - top_margin), False

        min_int_size = 12.0
        min_cents_size = 10.0
        int_size = max(self._safe_get_font_size(int_shape), min_int_size)
        cents_size = max(self._safe_get_font_size(cents_shape), min_cents_size)
        self._safe_set_font_size(int_shape, int_size)
        self._safe_set_font_size(cents_shape, cents_size)

        in_bounds, _ = group_status(anchor_top)
        while not in_bounds and (int_size > min_int_size or cents_size > min_cents_size):
            prev_int_size, prev_cents_size = int_size, cents_size
            int_size = max(min_int_size, int_size - 0.5)
            cents_size = max(min_cents_size, cents_size - 0.5)
            self._safe_set_font_size(int_shape, int_size)
            self._safe_set_font_size(cents_shape, cents_size)
            in_bounds, _ = group_status(anchor_top)
            if int_size == prev_int_size and cents_size == prev_cents_size:
                break

        top_choice, clear_choice = find_clear_top(anchor_top)
        while not clear_choice and (int_size > min_int_size or cents_size > min_cents_size):
            prev_int_size, prev_cents_size = int_size, cents_size
            int_size = max(min_int_size, int_size - 0.5)
            cents_size = max(min_cents_size, cents_size - 0.5)
            self._safe_set_font_size(int_shape, int_size)
            self._safe_set_font_size(cents_shape, cents_size)
            top_choice, clear_choice = find_clear_top(anchor_top)
            if int_size == prev_int_size and cents_size == prev_cents_size:
                break

        best_int_size = self._safe_get_font_size(int_shape)
        best_cents_size = self._safe_get_font_size(cents_shape)
        best_top = top_choice
        if clear_choice:
            for _ in range(10):
                candidate_int = best_int_size + 0.5
                candidate_cents = best_cents_size + 0.5
                self._safe_set_font_size(int_shape, candidate_int)
                self._safe_set_font_size(cents_shape, candidate_cents)
                candidate_top, candidate_clear = find_clear_top(anchor_top)
                if candidate_clear:
                    best_int_size = candidate_int
                    best_cents_size = candidate_cents
                    best_top = candidate_top
                else:
                    break

        self._safe_set_font_size(int_shape, best_int_size)
        self._safe_set_font_size(cents_shape, best_cents_size)
        place_group(best_top)

        # Garantia final: bloco inteiro+centavos respeita a largura maxima.
        for _ in range(20):
            int_rect = self._safe_shape_bounds(int_shape)
            cents_rect = self._safe_shape_bounds(cents_shape)
            group_w = max(int_rect[1], cents_rect[1]) - min(int_rect[0], cents_rect[0])
            if group_w <= max_group_w:
                break
            ratio = max_group_w / max(group_w, 0.1)
            self._safe_set_font_size(int_shape, max(self._safe_get_font_size(int_shape) * ratio * 0.98, 12.0))
            self._safe_set_font_size(cents_shape, max(self._safe_get_font_size(cents_shape) * ratio * 0.98, 10.0))
            place_group(best_top)

        # Posiciona a unidade (KG, UN, etc.) abaixo dos centavos.
        if not unit_shapes:
            try:
                page = int_shape.ParentPage
                fallback_unit = self._create_artistic_text_on_page(
                    page,
                    self._safe_get_prop(cents_shape, "LeftX"),
                    self._safe_get_prop(cents_shape, "BottomY"),
                    target_unit_label,
                )
                if fallback_unit is not None:
                    unit_shapes = [fallback_unit]
                    layout_context["unit_label"] = unit_shapes
            except Exception:
                pass
        if unit_shapes:
            cents_left = self._safe_get_prop(cents_shape, "LeftX")
            cents_right = self._safe_get_prop(cents_shape, "RightX")
            cents_top = self._safe_get_prop(cents_shape, "TopY")
            cents_bottom = self._safe_get_prop(cents_shape, "BottomY")
            unit_shape = min(
                unit_shapes,
                key=lambda s: abs(self._safe_get_prop(s, "LeftX") - cents_left)
                + abs(self._safe_get_prop(s, "TopY") - cents_bottom),
            )
            self._safe_set_shape_story_text(unit_shape, target_unit_label)
            # Evita "UNID" duplicado: limpa outros labels curtos de unidade.
            for other in unit_shapes:
                if other is unit_shape:
                    continue
                try:
                    other_story = self._safe_get_shape_story_text(other, "")
                    if self._is_unit_label_text(other_story):
                        self._safe_set_shape_story_text(other, "")
                except Exception:
                    pass
            # Ajusta a unidade para o tamanho padrao por tipo.
            normalized_unit = normalize_unit_label(target_unit_label)
            if normalized_unit == "UNID":
                unit_font_target = self.profile.unit_font_size_unid
            elif normalized_unit == "PACK.":
                unit_font_target = min(
                    self.profile.unit_font_size_unid * 0.9,
                    self.profile.unit_font_size_kg * 0.62,
                )
            else:
                unit_font_target = self.profile.unit_font_size_kg
            self._safe_set_font_size(unit_shape, max(8.0, float(unit_font_target)))
            unit_gap = self._cm_to_doc_units_by_bounds(self.profile.unit_gap_below_cents_cm, page_left, page_right)
            price_blockers = [
                self._safe_shape_bounds(int_shape),
                self._safe_shape_bounds(cents_shape),
            ]

            def _clamp_unit_inside_page() -> None:
                if self._safe_get_prop(unit_shape, "LeftX") < target_left_bound:
                    self._safe_set_prop(unit_shape, "LeftX", target_left_bound)
                if self._safe_get_prop(unit_shape, "RightX") > target_right_bound:
                    self._safe_set_prop(
                        unit_shape,
                        "LeftX",
                        target_right_bound - self._safe_get_prop(unit_shape, "SizeWidth"),
                    )
                if self._safe_get_prop(unit_shape, "BottomY") < (page_bottom + bottom_margin):
                    self._safe_set_prop(
                        unit_shape,
                        "TopY",
                        self._safe_get_prop(unit_shape, "TopY")
                        + ((page_bottom + bottom_margin) - self._safe_get_prop(unit_shape, "BottomY")),
                    )
                if self._safe_get_prop(unit_shape, "TopY") > (page_top - top_margin):
                    self._safe_set_prop(unit_shape, "TopY", page_top - top_margin)

            def _unit_is_clear() -> bool:
                return not self._shape_overlaps_bounds(unit_shape, price_blockers, padding=0.01)

            def _place_unit(left_x: float, top_y: float) -> bool:
                self._safe_set_prop(unit_shape, "LeftX", left_x)
                self._safe_set_prop(unit_shape, "TopY", top_y)
                _clamp_unit_inside_page()
                return _unit_is_clear()

            def _try_place_unit_current_size() -> bool:
                # Posicao base: centralizada abaixo dos centavos.
                cents_center = (cents_left + cents_right) / 2.0
                unit_w = max(self._safe_get_prop(unit_shape, "SizeWidth"), 0.1)
                base_left = cents_center - (unit_w / 2.0)
                base_top = cents_bottom - unit_gap
                clear_now = _place_unit(base_left, base_top)

                # Se houver sobreposicao com o preco, tenta posicoes alternativas.
                if not clear_now:
                    lateral_gap = max(unit_gap * 0.7, 0.05)
                    right_left = cents_right + lateral_gap
                    clear_now = _place_unit(right_left, base_top)
                    if not clear_now:
                        cur_w = max(self._safe_get_prop(unit_shape, "SizeWidth"), 0.1)
                        left_left = cents_left - lateral_gap - cur_w
                        clear_now = _place_unit(left_left, base_top)
                    if not clear_now:
                        step_down = max(
                            unit_gap * 0.6,
                            max(self._safe_get_prop(unit_shape, "SizeHeight"), 0.1) * 0.18,
                            0.03,
                        )
                        for _ in range(18):
                            next_top = self._safe_get_prop(unit_shape, "TopY") - step_down
                            clear_now = _place_unit(self._safe_get_prop(unit_shape, "LeftX"), next_top)
                            if clear_now:
                                break
                            if self._safe_get_prop(unit_shape, "BottomY") <= (page_bottom + bottom_margin + 0.001):
                                break

                # Garante ficar visualmente abaixo dos centavos.
                if self._safe_get_prop(unit_shape, "TopY") >= (cents_top - 0.01):
                    extra_gap = max(unit_gap, self._safe_get_prop(unit_shape, "SizeHeight") * 0.15)
                    clear_now = _place_unit(self._safe_get_prop(unit_shape, "LeftX"), cents_bottom - extra_gap)
                return clear_now and _unit_is_clear()

            clear = _try_place_unit_current_size()
            min_unit_font = 8.0
            if normalized_unit == "PACK.":
                min_unit_font = 32.0
            if not clear:
                for _ in range(12):
                    current_size = float(self._safe_get_font_size(unit_shape))
                    if current_size <= min_unit_font + 0.1:
                        break
                    next_size = max(min_unit_font, current_size * 0.9)
                    if next_size >= current_size - 0.01:
                        break
                    self._safe_set_font_size(unit_shape, next_size)
                    clear = _try_place_unit_current_size()
                    if clear:
                        break

            # Mantem a unidade acima dos numeros grandes para evitar ocultacao.
            for method_name in ("OrderToFront", "BringToFront"):
                try:
                    method = getattr(unit_shape, method_name, None)
                    if callable(method):
                        method()
                        break
                except Exception:
                    pass

    def _create_artistic_text_on_page(self, page, x: float, y: float, text: str):
        # Tenta criar texto no layer ativo da pagina; fallback para documento ativo.
        try:
            layer = page.ActiveLayer
            return layer.CreateArtisticText(x, y, text)
        except Exception:
            pass
        try:
            if self.app:
                layer = self.app.ActiveDocument.ActiveLayer
                return layer.CreateArtisticText(x, y, text)
        except Exception:
            pass
        return None

    def _upsert_offer_validity_caption(
        self,
        page,
        layout_context: Optional[Dict[str, list]],
        font_name: Optional[str],
    ) -> None:
        if not layout_context:
            return
        offer_text = str(layout_context.get("__offer_validity_text__", "")).strip()
        offer_shapes = list(layout_context.get("offer_validity", []))

        # Sem validade: limpa textos ja detectados.
        if not offer_text:
            for shape in offer_shapes:
                self._safe_set_shape_story_text(shape, "")
            return

        bounds = layout_context.get("__page_bounds__")
        if bounds and len(bounds) == 4:
            page_left, page_right, page_bottom, page_top = bounds
        else:
            try:
                page_left = float(page.LeftX)
                page_right = float(page.RightX)
                page_bottom = float(page.BottomY)
                page_top = float(page.TopY)
            except Exception:
                return

        target_shape = None
        if offer_shapes:
            target_shape = max(offer_shapes, key=lambda s: self._safe_get_prop(s, "SizeWidth"))
        else:
            initial_x = page_left + 0.2
            initial_top = page_bottom + self._cm_to_doc_units_by_bounds(1.0, page_left, page_right)
            target_shape = self._create_artistic_text_on_page(page, initial_x, initial_top, offer_text)
            if not target_shape:
                return
            offer_bucket = layout_context.setdefault("offer_validity", [])
            if target_shape not in offer_bucket:
                offer_bucket.append(target_shape)

        if not self._safe_set_shape_story_text(target_shape, offer_text):
            return

        self._apply_font_if_possible(target_shape, font_name)
        self._safe_set_story_prop(target_shape, "ParagraphSpacingBefore", 0.0)
        self._safe_set_story_prop(target_shape, "ParagraphSpacingAfter", 0.0)
        self._safe_set_story_prop(target_shape, "LineSpacing", 100.0)
        self._safe_set_story_prop(target_shape, "CharSpacing", 0.0)

        self._safe_set_font_size(target_shape, max(self.profile.offer_validity_font_size, self.profile.offer_validity_min_font_size))

        max_w = self._cm_to_doc_units_by_bounds(self.profile.offer_validity_max_width_cm, page_left, page_right)
        max_h = self._cm_to_doc_units_by_bounds(self.profile.offer_validity_max_height_cm, page_left, page_right)
        max_w = max(0.2, min(max_w, page_right - page_left - 0.1))
        max_h = max(0.05, max_h)
        min_size = max(1.0, self.profile.offer_validity_min_font_size)
        self._fit_font_within_shape(
            target_shape,
            max_w=max_w,
            max_h=max_h,
            min_size=min_size,
            max_steps=20,
        )

        def _center_offer_shape() -> None:
            shape_w = self._safe_get_prop(target_shape, "SizeWidth")
            centered_left = ((page_left + page_right) / 2.0) - (shape_w / 2.0)
            max_left = max(page_left + 0.05, page_right - shape_w - 0.05)
            self._safe_set_prop(target_shape, "LeftX", min(max(page_left + 0.05, centered_left), max_left))

        top_cap = page_top - 0.05
        bottom_margin = self._cm_to_doc_units_by_bounds(0.08, page_left, page_right)
        blocker_padding = self._cm_to_doc_units_by_bounds(0.10, page_left, page_right)
        top_from_bottom = self._cm_to_doc_units_by_bounds(self.profile.offer_validity_bottom_cm, page_left, page_right)

        offer_blockers = [
            self._safe_shape_bounds(shape)
            for bucket_name in ("price_full", "price_integer", "price_cents", "unit_label")
            for shape in layout_context.get(bucket_name, [])
            if self._safe_get_prop(shape, "SizeWidth") > 0 and self._safe_get_prop(shape, "SizeHeight") > 0
        ]

        def _place_offer(top_y: float) -> None:
            shape_h = max(self._safe_get_prop(target_shape, "SizeHeight"), 0.05)
            min_top = page_bottom + bottom_margin + shape_h
            self._safe_set_prop(target_shape, "TopY", min(top_cap, max(min_top, top_y)))
            _center_offer_shape()

        def _fits_below_blockers() -> bool:
            return not self._shape_overlaps_bounds(target_shape, offer_blockers, padding=blocker_padding)

        base_shape_h = max(self._safe_get_prop(target_shape, "SizeHeight"), 0.05)
        base_top = page_bottom + top_from_bottom + base_shape_h
        _place_offer(base_top)

        if offer_blockers and not _fits_below_blockers():
            current_size = max(self._safe_get_font_size(target_shape), min_size)
            for _ in range(24):
                offer_rect = self._safe_shape_bounds(target_shape)
                overlapping = [
                    blocker
                    for blocker in offer_blockers
                    if self._rects_overlap(offer_rect, blocker, padding=blocker_padding)
                ]
                if not overlapping:
                    break

                shape_h = max(self._safe_get_prop(target_shape, "SizeHeight"), 0.05)
                min_top = page_bottom + bottom_margin + shape_h
                allowed_top = min(blocker[2] - blocker_padding for blocker in overlapping)
                if allowed_top >= min_top:
                    _place_offer(allowed_top)
                    if _fits_below_blockers():
                        break

                next_size = max(min_size, current_size - 0.5)
                if next_size >= current_size:
                    break
                current_size = next_size
                self._safe_set_font_size(target_shape, current_size)
                self._fit_font_within_shape(
                    target_shape,
                    max_w=max_w,
                    max_h=max_h,
                    min_size=min_size,
                    max_steps=12,
                )
                _place_offer(base_top)

    @staticmethod
    def _classify_replacement_token(token: str) -> tuple[str, str]:
        token_up = (token or "").upper()
        if token_up in {
            "NOME",
            "PRODUTO",
            "NOME_PRODUTO",
            "NOME_DO_PRODUTO",
            "DESCRICAO",
            "DESCRICAO_PRODUTO",
            "DESCRICAO_DO_PRODUTO",
            "ITEM",
            "ITEM_NOME",
            "PRODUCT",
            "PRODUCT_NAME",
        }:
            return "name", "name_text"
        if token_up in {"PRECO_INTEIRO"}:
            return "price", "price_integer"
        if token_up in {"PRECO_CENTAVOS"}:
            return "price", "price_cents"
        if is_unit_placeholder_token(token):
            return "price", "unit_label"
        if token_up.startswith("PRECO") or token_up == "PRICE":
            return "price", "price_full"
        if token_up in {"OFERTA_VALIDADE", "OFERTA_VALIDADE_TEXTO", "TEXTO_VALIDADE_OFERTA", "DATA_VALIDADE"}:
            return "offer", "offer_validity"
        return "", ""

    @staticmethod
    def _pick_price_for_story(story: str, replacements: Dict[str, str]) -> str:
        price_full = str(replacements.get("PRECO", "")).strip()
        price_no_prefix = str(replacements.get("PRECO_SEM_PREFIXO", price_full)).strip()
        if re.search(r"r\$\s*", story or "", flags=re.IGNORECASE):
            if re.search(r"r\$\s*", price_full, flags=re.IGNORECASE):
                return price_full
            return f"R$ {price_no_prefix}".strip()
        return price_no_prefix or price_full

    def _replace_tokens_in_shapes(
        self,
        shapes,
        replacements: Dict[str, str],
        font_name: Optional[str],
        stop_requested: Optional[Callable[[], bool]] = None,
        layout_context: Optional[Dict[str, list]] = None,
    ) -> None:
        try:
            count = int(getattr(shapes, "Count", 0))
        except Exception:
            count = 0
        for idx in range(1, count + 1):
            self._check_stop(stop_requested)
            try:
                shape = shapes.Item(idx)
            except Exception:
                continue

            try:
                story = self._safe_get_shape_story_text(shape, "")
                stripped_story = story.strip()
                new_story = story
                replacement_kind = ""
                replacement_subkind = ""

                if layout_context is not None and stripped_story and len(stripped_story) <= 12:
                    if self._is_unit_label_text(stripped_story):
                        unit_bucket = layout_context.setdefault("unit_label", [])
                        if shape not in unit_bucket:
                            unit_bucket.append(shape)

                placeholder_hints: List[tuple[str, str]] = []
                def _on_placeholder_replaced(token: str) -> None:
                    hint_kind, hint_subkind = self._classify_replacement_token(token)
                    if hint_subkind:
                        placeholder_hints.append((hint_kind, hint_subkind))

                if "{" in story and "}" in story:
                    new_story = replace_placeholders_in_text(
                        story,
                        replacements,
                        normalize_key,
                        on_token_replaced=_on_placeholder_replaced,
                    )
                    if placeholder_hints:
                        replacement_kind, replacement_subkind = placeholder_hints[0]
                        if replacement_subkind == "offer_validity":
                            offer_enabled_text = str(replacements.get("OFERTA_VALIDADE_TEXTO", "")).strip()
                            if not offer_enabled_text and (
                                self._is_offer_validity_caption(story) or self._is_offer_validity_caption(new_story)
                            ):
                                new_story = ""

                # Compatibilidade com templates sem placeholders.
                # Se o texto fixo do modelo for detectado, substitui pelos campos principais.
                if new_story == story:
                    has_letters = bool(re.search(r"[A-Za-z\u00c0-\u00ff]", story))
                    story_norm = normalize_key(story) if has_letters else ""
                    if DATE_XXXX_PLACEHOLDER_RE.search(story):
                        offer_text_full = str(replacements.get("OFERTA_VALIDADE_TEXTO", "")).strip()
                        offer_date = str(replacements.get("DATA_VALIDADE", "")).strip()
                        if self._is_offer_validity_caption(story, story_norm if has_letters else None):
                            new_story = offer_text_full if offer_text_full else ""
                        elif self._is_offer_validity_date_placeholder_only(story):
                            new_story = offer_date if offer_text_full else ""
                        elif offer_text_full:
                            new_story = DATE_XXXX_PLACEHOLDER_RE.sub(offer_date, story)
                        else:
                            new_story = story
                        if new_story != story:
                            replacement_kind = "offer"
                            replacement_subkind = "offer_validity"
                    if new_story == story and (
                        story_norm in {"unidade", "unit", "medida", "tipo_unidade", "kg_ou_unid"}
                        or is_unit_placeholder_token(story_norm)
                    ):
                        new_story = str(replacements.get("UNIDADE", story))
                        replacement_kind = "price"
                        replacement_subkind = "unit_label"
                    elif stripped_story and len(stripped_story) <= 12 and self._is_unit_label_text(stripped_story):
                        # Para texto fixo de unidade (ex.: KG/UNID), o ajuste final
                        # e deduplicacao sao feitos em _align_price_parts().
                        replacement_subkind = "unit_label"
                    elif has_letters and self._is_offer_validity_caption(story, story_norm):
                        new_story = str(replacements.get("OFERTA_VALIDADE_TEXTO", ""))
                        replacement_kind = "offer"
                        replacement_subkind = "offer_validity"
                    elif story_norm in {
                        "nome_do_produto",
                        "nome_produto",
                        "produto",
                        "descricao",
                        "descricao_produto",
                        "descricao_do_produto",
                        "item",
                        "product_name",
                    }:
                        new_story = str(replacements.get("NOME", story))
                        replacement_kind = "name"
                        replacement_subkind = "name_text"
                    elif has_letters and self._is_likely_product_placeholder(story):
                        new_story = str(replacements.get("NOME", story))
                        replacement_kind = "name"
                        replacement_subkind = "name_text"
                    elif PRICE_FULL_TEXT_RE.fullmatch(stripped_story):
                        preco = self._pick_price_for_story(stripped_story, replacements) or str(
                            replacements.get("PRECO_SEM_PREFIXO", replacements.get("PRECO", story))
                        )
                        new_story = preco
                        replacement_kind = "price"
                        replacement_subkind = "price_full"
                    elif PRICE_INLINE_RE.search(stripped_story):
                        # Substitui apenas o trecho de preco quando o texto mistura unidade/sufixos.
                        preco = self._pick_price_for_story(stripped_story, replacements)
                        if preco:
                            new_story = PRICE_INLINE_RE.sub(preco, story, count=1)
                            replacement_kind = "price"
                            replacement_subkind = "price_full"
                    elif PRICE_INTEGER_TEXT_RE.fullmatch(stripped_story):
                        new_story = str(replacements.get("PRECO_INTEIRO", story))
                        replacement_kind = "price"
                        replacement_subkind = "price_integer"
                    elif PRICE_CENTS_TEXT_RE.fullmatch(story):
                        sep = "," if "," in story else "."
                        leading = story[: len(story) - len(story.lstrip())]
                        trailing = story[len(story.rstrip()) :]
                        new_story = f"{leading}{sep}{replacements.get('PRECO_CENTAVOS', '00')}{trailing}"
                        replacement_kind = "price"
                        replacement_subkind = "price_cents"

                if new_story != story:
                    if replacement_kind == "name":
                        applied = False
                        try:
                            self._fit_product_text(shape, new_story, font_name)
                            applied = True
                        except Exception:
                            applied = False
                        # Fallback: garante gravacao do nome mesmo se o ajuste avancado falhar.
                        if not applied:
                            try:
                                if self._safe_set_shape_story_text(shape, new_story):
                                    self._apply_font_if_possible(shape, font_name)
                            except Exception:
                                pass
                    else:
                        if self._safe_set_shape_story_text(shape, new_story):
                            self._apply_font_if_possible(shape, font_name)
                if layout_context is not None and replacement_subkind:
                    bucket = layout_context.setdefault(replacement_subkind, [])
                    if shape not in bucket:
                        bucket.append(shape)
            except Exception:
                pass

            try:
                nested = shape.Shapes
                nested_count = int(getattr(nested, "Count", 0))
                if nested_count > 0:
                    self._replace_tokens_in_shapes(
                        nested,
                        replacements,
                        font_name,
                        stop_requested,
                        layout_context,
                    )
            except Exception:
                pass

    def _publish_pdf(self, doc, output_pdf: Path) -> None:
        # Forca exportacao completa do documento (todas as paginas/conteudo).
        if self.app:
            settings = None
            try:
                settings = self.app.PDFVBASettings
            except Exception:
                settings = None
            if settings is not None:
                try:
                    settings.PublishRange = 0  # all pages
                except Exception:
                    pass
                try:
                    settings.PageRange = ""
                except Exception:
                    pass
                try:
                    settings.SelectionOnly = False
                except Exception:
                    pass
                try:
                    settings.EmbedFonts = True
                except Exception:
                    pass

        output_pdf.parent.mkdir(parents=True, exist_ok=True)
        doc.PublishToPDF(str(output_pdf))

    def _clear_document_selection(self, doc) -> None:
        # Em alguns cenarios o Corel mantem o ultimo texto criado/alterado selecionado.
        # Se "SelectionOnly" estiver ativo na UI/versao, isso pode gerar PDF parcial.
        try:
            doc.ClearSelection()
            return
        except Exception:
            pass
        try:
            active_page = doc.ActivePage
            active_page.ClearSelection()
            return
        except Exception:
            pass
        if self.app:
            try:
                self.app.ActiveDocument.ClearSelection()
                return
            except Exception:
                pass
            try:
                active_page = self.app.ActiveDocument.ActivePage
                active_page.ClearSelection()
            except Exception:
                pass

    @staticmethod
    def _close_doc_without_saving(doc) -> None:
        try:
            doc.Close(False)
            return
        except Exception:
            pass
        try:
            doc.Close(0)
            return
        except Exception:
            pass
        try:
            doc.Close()
        except Exception:
            pass

    def create_from_template(
        self,
        template_path: Path,
        replacements: Dict[str, str],
        output_pdf: Path,
        output_cdr: Optional[Path] = None,
        font_name: Optional[str] = None,
        stop_requested: Optional[Callable[[], bool]] = None,
    ) -> None:
        if not self.app:
            raise RuntimeError("CorelDRAW nao foi inicializado.")

        self._set_app_fast_mode(True)
        doc = None
        try:
            doc = self.app.OpenDocument(str(template_path))
            pages = doc.Pages
            page_count = int(getattr(pages, "Count", 0))
            warned_missing_name_text = False
            for idx in range(1, page_count + 1):
                self._check_stop(stop_requested)
                page = pages.Item(idx)
                try:
                    page.Activate()
                except Exception:
                    pass
                page_context: Dict[str, list] = {}
                try:
                    page_context["__page_bounds__"] = (
                        float(page.LeftX),
                        float(page.RightX),
                        float(page.BottomY),
                        float(page.TopY),
                    )
                except Exception:
                    pass
                page_context["__unit_label__"] = str(replacements.get("UNIDADE", "KG"))
                page_context["__offer_validity_text__"] = str(replacements.get("OFERTA_VALIDADE_TEXTO", ""))
                self._replace_tokens_in_shapes(
                    page.Shapes,
                    replacements,
                    font_name,
                    stop_requested,
                    page_context,
                )
                if not warned_missing_name_text:
                    expected_name = str(replacements.get("NOME", "")).strip()
                    name_applied = self._is_name_applied_on_page(page, expected_name, page_context)
                    if expected_name and not name_applied:
                        forced_ok = self._force_replace_product_name_on_page(
                            page,
                            expected_name,
                            font_name,
                            page_context,
                        )
                        if not forced_ok:
                            print(
                                "[WARN] Nao foi encontrado texto editavel de nome no template "
                                f"(pagina {idx}). Verifique se o nome esta em curvas."
                            )
                            warned_missing_name_text = True
                try:
                    self._align_price_parts(page_context)
                except Exception:
                    # Falhas de alinhamento nao devem impedir exportacao da placa.
                    pass
                try:
                    self._upsert_offer_validity_caption(page, page_context, font_name)
                except Exception:
                    pass

            self._clear_document_selection(doc)
            if output_cdr:
                output_cdr.parent.mkdir(parents=True, exist_ok=True)
                doc.SaveAs(str(output_cdr))
            self._check_stop(stop_requested)
            self._publish_pdf(doc, output_pdf)
        finally:
            if doc is not None:
                self._close_doc_without_saving(doc)
            self._set_app_fast_mode(False)

    def create_simple(
        self,
        name: str,
        price: str,
        output_pdf: Path,
        width_mm: float,
        height_mm: float,
        font_name: Optional[str] = None,
        stop_requested: Optional[Callable[[], bool]] = None,
    ) -> None:
        if not self.app:
            raise RuntimeError("CorelDRAW nao foi inicializado.")

        self._set_app_fast_mode(True)
        doc = None
        try:
            doc = self.app.CreateDocument()
            try:
                # 3 costuma ser milimetros em varias versoes do CorelDRAW.
                doc.Unit = 3
                doc.ActivePage.SetSize(width_mm, height_mm)
            except Exception:
                pass

            layer = doc.ActiveLayer
            y_name = max(height_mm - 18, 10)
            y_price = max(height_mm / 2, 8)

            name_shape = layer.CreateArtisticText(10, y_name, name)
            price_shape = layer.CreateArtisticText(10, y_price, price)
            try:
                name_shape.Text.Story.Size = self.profile.name_base_font_size
                price_shape.Text.Story.Size = 64
            except Exception:
                pass
            self._fit_product_text(name_shape, name, font_name)
            self._apply_font_if_possible(price_shape, font_name)

            self._check_stop(stop_requested)
            self._clear_document_selection(doc)
            self._publish_pdf(doc, output_pdf)
        finally:
            if doc is not None:
                self._close_doc_without_saving(doc)
            self._set_app_fast_mode(False)


def build_replacements(item: PlateData) -> Dict[str, str]:
    preco_sem_prefixo = item.price
    if preco_sem_prefixo.startswith("R$"):
        preco_sem_prefixo = preco_sem_prefixo[2:].strip()
    preco_inteiro, preco_centavos = split_price_parts(preco_sem_prefixo)
    unit_value = normalize_unit_label(item.unit_label)
    normalized_quantities = normalize_format_quantities(
        item.format_quantities,
        default_format=item.plate_format,
        default_qty=item.quantity,
    )
    total_qty = max(1, total_format_copies(normalized_quantities))
    plate_format = normalize_plate_format(item.plate_format, default="A4")
    offer_text = ""
    offer_date = ""
    offer_day = ""
    offer_month = ""
    offer_year = ""
    if item.offer_validity_enabled:
        today = date.today()
        safe_day, safe_month, safe_year = _resolve_offer_validity_date(
            int(item.offer_validity_day or today.day),
            today=today,
            month_value=item.offer_validity_month,
        )
        offer_text = build_offer_validity_text(safe_day, today=today, month_value=safe_month)
        offer_date = f"{safe_day:02d}/{safe_month:02d}/{safe_year:04d}"
        offer_day = f"{safe_day:02d}"
        offer_month = f"{safe_month:02d}"
        offer_year = f"{safe_year:04d}"
    replacements = {
        "INDEX": str(item.index),
        "NOME": item.name,
        "PRODUTO": item.name,
        "NOME_PRODUTO": item.name,
        "NOME_DO_PRODUTO": item.name,
        "DESCRICAO": item.name,
        "DESCRICAO_PRODUTO": item.name,
        "DESCRICAO_DO_PRODUTO": item.name,
        "ITEM": item.name,
        "ITEM_NOME": item.name,
        "PRODUCT": item.name,
        "PRODUCT_NAME": item.name,
        "PRECO": item.price,
        "PRECO_SEM_PREFIXO": preco_sem_prefixo,
        "PRECO_INTEIRO": preco_inteiro,
        "PRECO_CENTAVOS": preco_centavos,
        "PRICE": item.price,
        "UNIDADE": unit_value,
        "FORMATO_PLACA": plate_format,
        "UNIT": unit_value,
        "UN": unit_value,
        "UND": unit_value,
        "UNID": unit_value,
        "MEDIDA": unit_value,
        "TIPO_UNIDADE": unit_value,
        "TIPO_DE_UNIDADE": unit_value,
        "KG_OU_UNID": unit_value,
        "UNIDADE_MEDIDA": unit_value,
        "UNIDADE_DE_MEDIDA": unit_value,
        "MEDIDA_UNIDADE": unit_value,
        "UNIT_LABEL": unit_value,
        "UNIT_TYPE": unit_value,
        "UNIDADE_DO_PRODUTO": unit_value,
        "GRAMATURA": "",
        "PESO": "",
        "GRAM": "",
        "QTD": str(total_qty),
        "QUANTIDADE": str(total_qty),
        "QTD_A4": str(int(normalized_quantities.get("A4", 0))),
        "QTD_A5": str(int(normalized_quantities.get("A5", 0))),
        "QTD_A6": str(int(normalized_quantities.get("A6", 0))),
        "OFERTA_VALIDADE_TEXTO": offer_text,
        "TEXTO_VALIDADE_OFERTA": offer_text,
        "OFERTA_VALIDADE": offer_text,
        "DATA_VALIDADE": offer_date,
        "DIA_VALIDADE": offer_day,
        "MES_VALIDADE": offer_month,
        "ANO_VALIDADE": offer_year,
        "OFERTA_VALIDADE_DIA": offer_day,
        "OFERTA_VALIDADE_MES": offer_month,
        "OFERTA_VALIDADE_ANO": offer_year,
    }
    for key, value in item.row.items():
        normalized_key = normalize_key(key).upper()
        if not normalized_key:
            continue
        if normalized_key not in replacements:
            replacements[normalized_key] = value
    return replacements


def parse_args() -> argparse.Namespace:
    default_template_cdr = r"C:\Users\marke\OneDrive\Desktop\PlacasAut\A4.cdr"
    parser = argparse.ArgumentParser(
        description="Gera placas de preco no CorelDRAW a partir de um PDF com tabela."
    )
    parser.add_argument("--input-pdf", help="Caminho do PDF com a planilha/tabela.")
    parser.add_argument(
        "--select-input",
        action="store_true",
        help="Abre uma janela para selecionar o PDF da planilha.",
    )
    parser.add_argument("--output-dir", default="saida_placas", help="Pasta de saida.")
    parser.add_argument(
        "--pdf-output-dir",
        help="Pasta final dos PDFs. Se informado, substitui --output-dir/--pdf-subdir para os PDFs.",
    )
    parser.add_argument(
        "--pdf-subdir",
        default="pdf",
        help="Nome da subpasta onde os PDFs serao salvos dentro de --output-dir.",
    )
    parser.add_argument(
        "--select-pdf-output-dir",
        action="store_true",
        help="Abre uma janela para selecionar a pasta onde os PDFs serao salvos.",
    )
    parser.add_argument(
        "--template-cdr",
        default=default_template_cdr,
        help=(
            f"Template .cdr com placeholders, ex: {{NOME}}/{{{{NOME}}}} e "
            f"{{PRECO}}/{{{{PRECO}}}}. Padrao: {default_template_cdr}"
        ),
    )
    parser.add_argument(
        "--template-cdr-a4",
        help="Template .cdr especifico para A4 (sobrescreve deteccao automatica para A4).",
    )
    parser.add_argument(
        "--template-cdr-a5",
        help="Template .cdr especifico para A5 (sobrescreve deteccao automatica para A5).",
    )
    parser.add_argument(
        "--template-cdr-a6",
        help="Template .cdr especifico para A6 (sobrescreve deteccao automatica para A6).",
    )
    parser.add_argument(
        "--select-template-cdr",
        action="store_true",
        help="Abre uma janela para selecionar o template .cdr.",
    )
    parser.add_argument(
        "--select-template-cdrs",
        action="store_true",
        help="Abre 3 janelas para selecionar templates CDR especificos de A4, A5 e A6.",
    )
    parser.add_argument(
        "--select-template-cdr-a4",
        action="store_true",
        help="Abre uma janela para selecionar o template CDR especifico de A4.",
    )
    parser.add_argument(
        "--select-template-cdr-a5",
        action="store_true",
        help="Abre uma janela para selecionar o template CDR especifico de A5.",
    )
    parser.add_argument(
        "--select-template-cdr-a6",
        action="store_true",
        help="Abre uma janela para selecionar o template CDR especifico de A6.",
    )
    parser.add_argument("--name-col", help="Nome da coluna para descricao/produto.")
    parser.add_argument("--price-col", help="Nome da coluna para preco.")
    parser.add_argument("--price-prefix", default="R$ ", help="Prefixo para o preco.")
    parser.add_argument(
        "--use-ollama-cleanup",
        action="store_true",
        help="Usa Ollama local para sugerir correcao de nomes de produtos antes da configuracao.",
    )
    parser.add_argument(
        "--ollama-model",
        default=DEFAULT_OLLAMA_MODEL,
        help=f"Modelo do Ollama usado na limpeza de nomes. Padrao: {DEFAULT_OLLAMA_MODEL}",
    )
    parser.add_argument(
        "--ollama-timeout",
        type=float,
        default=DEFAULT_OLLAMA_TIMEOUT_SECONDS,
        help="Timeout em segundos para cada correcao de nome via Ollama.",
    )
    parser.add_argument(
        "--ollama-max-items",
        type=int,
        default=DEFAULT_OLLAMA_MAX_ITEMS,
        help=f"Limite de itens por execucao enviados ao Ollama antes da tela abrir. Padrao: {DEFAULT_OLLAMA_MAX_ITEMS}",
    )
    parser.add_argument("--font-name", default="AhkioW00-Bold", help="Fonte aplicada nos textos variaveis.")
    parser.add_argument(
        "--profile",
        default="Placa A4",
        help="Perfil de layout. Exemplo: Placa A4. A5 gera 2 por arquivo e A6 gera 4 por arquivo (2x2).",
    )
    parser.add_argument("--stop-hotkey", default="f7", help="Tecla global para interromper (padrao: f7).")
    parser.add_argument(
        "--skip-config-window",
        action="store_true",
        help="Nao abre a janela de configuracao; gera todas as placas com quantidade 1 e unidade detectada automaticamente.",
    )
    parser.add_argument(
        "--config-ui",
        choices=("web", "tk"),
        default="web",
        help="Interface para configuracao: web (HTML/CSS no navegador) ou tk (janela Tkinter).",
    )
    parser.add_argument(
        "--auto-print",
        action="store_true",
        help="Envia automaticamente os PDFs finais para a impressora padrao ao concluir.",
    )
    parser.add_argument(
        "--duplex-print",
        action="store_true",
        help="Agrupa PDFs marcados para impressao frente e verso antes de enviar para a impressora.",
    )
    parser.add_argument(
        "--shutdown-after-print",
        action="store_true",
        help="Desliga o computador apos enviar todos os jobs de impressao sem falhas.",
    )
    parser.add_argument(
        "--unlock-code",
        action="store_true",
        help="Desbloqueia os arquivos .py para edicao (exige senha).",
    )
    parser.add_argument(
        "--lock-code",
        action="store_true",
        help="Bloqueia os arquivos .py em modo somente leitura.",
    )
    parser.add_argument(
        "--authorize-code-change",
        action="store_true",
        help="Autoriza alteracoes detectadas no codigo e atualiza a integridade (exige senha).",
    )
    parser.add_argument(
        "--code-password",
        help=(
            "Senha da protecao de codigo. Tambem pode ser informada pela variavel "
            f"de ambiente {CODE_GUARD_PASSWORD_ENV}."
        ),
    )
    parser.add_argument("--save-cdr", action="store_true", help="Tambem salva o .cdr de cada placa.")
    parser.add_argument("--close-corel", action="store_true", help="Fecha CorelDRAW ao final.")
    parser.add_argument("--hidden-corel", action="store_true", help="Executa CorelDRAW em modo invisivel.")
    parser.add_argument("--page-width-mm", type=float, default=210.0, help="Largura no layout simples.")
    parser.add_argument("--page-height-mm", type=float, default=148.0, help="Altura no layout simples.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    guard_exit_code = _handle_code_guard(args)
    if guard_exit_code is not None:
        return int(guard_exit_code)

    output_dir = Path(args.output_dir).expanduser().resolve()
    if args.select_pdf_output_dir:
        initial_pdf_output_dir = (
            Path(args.pdf_output_dir).expanduser().resolve()
            if args.pdf_output_dir
            else (output_dir / sanitize_filename(str(args.pdf_subdir), "pdf"))
        )
        selected_pdf_output_dir = choose_directory_via_dialog(
            initial_pdf_output_dir,
            "Selecione a pasta onde os PDFs serao salvos",
        )
        if not selected_pdf_output_dir:
            print("Nenhuma pasta de PDF foi selecionada.", file=sys.stderr)
            return 2
        args.pdf_output_dir = str(selected_pdf_output_dir)

    input_pdf: Optional[Path] = None
    if args.input_pdf:
        input_pdf = Path(args.input_pdf).expanduser().resolve()
    if args.select_input or not input_pdf:
        selected = choose_pdf_via_dialog(Path.cwd())
        if not selected:
            print("Nenhum PDF foi selecionado.", file=sys.stderr)
            return 2
        input_pdf = selected

    if not input_pdf.exists():
        print(f"Arquivo nao encontrado: {input_pdf}", file=sys.stderr)
        return 2

    template_cdr: Optional[Path] = None
    if args.select_template_cdr:
        template_initial_dir = Path(args.template_cdr).expanduser().resolve().parent if args.template_cdr else Path.cwd()
        selected_template = choose_cdr_via_dialog(_prefer_modelos_placa_dir(template_initial_dir))
        if not selected_template:
            print("Nenhum template CDR foi selecionado.", file=sys.stderr)
            return 2
        template_cdr = selected_template
    elif args.template_cdr:
        template_cdr = Path(args.template_cdr).expanduser().resolve()

    template_cdr_a4_override = (
        Path(args.template_cdr_a4).expanduser().resolve() if args.template_cdr_a4 else None
    )
    template_cdr_a5_override = (
        Path(args.template_cdr_a5).expanduser().resolve() if args.template_cdr_a5 else None
    )
    template_cdr_a6_override = (
        Path(args.template_cdr_a6).expanduser().resolve() if args.template_cdr_a6 else None
    )
    if args.select_template_cdrs:
        initial_dir = template_cdr.parent if template_cdr and template_cdr.parent.exists() else Path.cwd()
        initial_dir = _prefer_modelos_placa_dir(initial_dir)
        selected_a4 = choose_cdr_via_dialog(initial_dir, "Selecione o template CDR para A4")
        if selected_a4:
            template_cdr_a4_override = selected_a4
            initial_dir = selected_a4.parent
        selected_a5 = choose_cdr_via_dialog(initial_dir, "Selecione o template CDR para A5")
        if selected_a5:
            template_cdr_a5_override = selected_a5
            initial_dir = selected_a5.parent
        selected_a6 = choose_cdr_via_dialog(initial_dir, "Selecione o template CDR para A6")
        if selected_a6:
            template_cdr_a6_override = selected_a6

    if args.select_template_cdr_a4:
        initial_dir = (
            template_cdr_a4_override.parent
            if template_cdr_a4_override and template_cdr_a4_override.parent.exists()
            else (template_cdr.parent if template_cdr and template_cdr.parent.exists() else Path.cwd())
        )
        selected_a4 = choose_cdr_via_dialog(_prefer_modelos_placa_dir(initial_dir), "Selecione o template CDR para A4")
        if not selected_a4:
            print("Nenhum template CDR A4 foi selecionado.", file=sys.stderr)
            return 2
        template_cdr_a4_override = selected_a4

    if args.select_template_cdr_a5:
        initial_dir = (
            template_cdr_a5_override.parent
            if template_cdr_a5_override and template_cdr_a5_override.parent.exists()
            else (template_cdr.parent if template_cdr and template_cdr.parent.exists() else Path.cwd())
        )
        selected_a5 = choose_cdr_via_dialog(_prefer_modelos_placa_dir(initial_dir), "Selecione o template CDR para A5")
        if not selected_a5:
            print("Nenhum template CDR A5 foi selecionado.", file=sys.stderr)
            return 2
        template_cdr_a5_override = selected_a5

    if args.select_template_cdr_a6:
        initial_dir = (
            template_cdr_a6_override.parent
            if template_cdr_a6_override and template_cdr_a6_override.parent.exists()
            else (template_cdr.parent if template_cdr and template_cdr.parent.exists() else Path.cwd())
        )
        selected_a6 = choose_cdr_via_dialog(_prefer_modelos_placa_dir(initial_dir), "Selecione o template CDR para A6")
        if not selected_a6:
            print("Nenhum template CDR A6 foi selecionado.", file=sys.stderr)
            return 2
        template_cdr_a6_override = selected_a6

    for fmt, template_path in (
        ("A4", template_cdr_a4_override),
        ("A5", template_cdr_a5_override),
        ("A6", template_cdr_a6_override),
    ):
        if template_path and not template_path.exists():
            print(f"Template CDR {fmt} nao encontrado: {template_path}", file=sys.stderr)
            return 2

    if template_cdr and not template_cdr.exists():
        print(f"Template CDR nao encontrado: {template_cdr}", file=sys.stderr)
        auto_candidates: List[Path] = []
        script_dir = Path(__file__).resolve().parent
        search_roots = [Path.cwd(), script_dir]
        if input_pdf:
            search_roots.append(input_pdf.parent)
        if template_cdr and template_cdr.parent:
            search_roots.append(template_cdr.parent)

        for root in search_roots:
            auto_candidates.extend(
                [
                    root / "A4.cdr",
                    root / "A5.cdr",
                    root / "A6.cdr",
                    root / MODELOS_PLACA_DIRNAME / "A4.cdr",
                    root / MODELOS_PLACA_DIRNAME / "A5.cdr",
                    root / MODELOS_PLACA_DIRNAME / "A6.cdr",
                ]
            )
        resolved_template = next((cand for cand in auto_candidates if cand.exists()), None)
        if resolved_template:
            template_cdr = resolved_template.resolve()
            print_status("TPL", f"Template detectado automaticamente: {template_cdr}")
        elif any((template_cdr_a4_override, template_cdr_a5_override, template_cdr_a6_override)):
            print_status("TPL", "Template base ausente; usando apenas templates por formato.")
            template_cdr = None
        else:
            fallback_dir = template_cdr.parent if template_cdr.parent.exists() else Path.cwd()
            selected_template = choose_cdr_via_dialog(_prefer_modelos_placa_dir(fallback_dir))
            if not selected_template:
                return 2
            template_cdr = selected_template

    print_status("PDF", f"Lendo dados do arquivo: {input_pdf}")
    try:
        rows = parse_tables_from_pdf(input_pdf)
    except Exception as exc:
        print(f"Falha ao ler o PDF selecionado: {exc}", file=sys.stderr)
        return 3
    if not rows:
        print("Nenhuma tabela foi encontrada no PDF.", file=sys.stderr)
        return 3

    if args.use_ollama_cleanup:
        print_status(
            "AI",
            f"Limpeza inteligente de nomes ativa com Ollama ({args.ollama_model}). "
            f"Limite antes da tela: {max(0, int(args.ollama_max_items))} item(ns).",
        )

    items = build_plate_rows(
        rows,
        args.name_col,
        args.price_col,
        args.price_prefix,
        default_plate_format=args.profile,
        use_ollama_cleanup=bool(args.use_ollama_cleanup),
        ollama_model=str(args.ollama_model or DEFAULT_OLLAMA_MODEL),
        ollama_timeout_seconds=float(args.ollama_timeout or DEFAULT_OLLAMA_TIMEOUT_SECONDS),
        ollama_max_items=int(args.ollama_max_items or DEFAULT_OLLAMA_MAX_ITEMS),
    )
    if not items:
        print("Nenhuma linha valida para gerar placas.", file=sys.stderr)
        return 4

    template_panel_options = _collect_cdr_templates(
        [
            Path.cwd(),
            Path.cwd() / MODELOS_PLACA_DIRNAME,
            Path(__file__).resolve().parent,
            Path(__file__).resolve().parent / MODELOS_PLACA_DIRNAME,
            input_pdf.parent,
            input_pdf.parent / MODELOS_PLACA_DIRNAME,
            template_cdr.parent if template_cdr else Path.cwd(),
            (template_cdr.parent / MODELOS_PLACA_DIRNAME) if template_cdr else (Path.cwd() / MODELOS_PLACA_DIRNAME),
            template_cdr_a4_override.parent if template_cdr_a4_override else Path.cwd(),
            (template_cdr_a4_override.parent / MODELOS_PLACA_DIRNAME)
            if template_cdr_a4_override
            else (Path.cwd() / MODELOS_PLACA_DIRNAME),
            template_cdr_a5_override.parent if template_cdr_a5_override else Path.cwd(),
            (template_cdr_a5_override.parent / MODELOS_PLACA_DIRNAME)
            if template_cdr_a5_override
            else (Path.cwd() / MODELOS_PLACA_DIRNAME),
            template_cdr_a6_override.parent if template_cdr_a6_override else Path.cwd(),
            (template_cdr_a6_override.parent / MODELOS_PLACA_DIRNAME)
            if template_cdr_a6_override
            else (Path.cwd() / MODELOS_PLACA_DIRNAME),
            template_cdr if template_cdr else Path.cwd(),
            template_cdr_a4_override if template_cdr_a4_override else Path.cwd(),
            template_cdr_a5_override if template_cdr_a5_override else Path.cwd(),
            template_cdr_a6_override if template_cdr_a6_override else Path.cwd(),
        ]
    )
    template_panel_defaults: Dict[str, Optional[Path]] = {
        "A4": template_cdr_a4_override,
        "A5": template_cdr_a5_override,
        "A6": template_cdr_a6_override,
    }
    template_panel_selection: Dict[str, Optional[Path]] = {}

    print_status("ITENS", f"Linhas detectadas para gerar placas: {len(items)}")
    auto_print_enabled = bool(args.auto_print)
    duplex_print_enabled = bool(args.duplex_print)
    shutdown_after_print_enabled = bool(args.shutdown_after_print)
    if not args.skip_config_window:
        configured: Optional[tuple[List[PlateData], bool, bool, bool]] = None
        if args.config_ui == "web":
            try:
                configured = configure_plates_via_web(
                    items,
                    price_prefix=args.price_prefix,
                    default_auto_print=auto_print_enabled,
                    default_duplex_print=duplex_print_enabled,
                    default_shutdown_after_print=shutdown_after_print_enabled,
                    ollama_cleanup_enabled=bool(args.use_ollama_cleanup),
                    ollama_model=str(args.ollama_model or DEFAULT_OLLAMA_MODEL),
                    ollama_timeout_seconds=float(args.ollama_timeout or DEFAULT_OLLAMA_TIMEOUT_SECONDS),
                    allow_web_lookup=True,
                    template_options=template_panel_options,
                    template_defaults=template_panel_defaults,
                    template_selection_holder=template_panel_selection,
                )
            except Exception as exc:
                print_status("WEB", f"Falha na interface web ({exc}). Usando Tkinter...")
                configured = configure_plates_via_dialog(
                    items,
                    price_prefix=args.price_prefix,
                    default_auto_print=auto_print_enabled,
                    default_duplex_print=duplex_print_enabled,
                    default_shutdown_after_print=shutdown_after_print_enabled,
                )
        else:
            configured = configure_plates_via_dialog(
                items,
                price_prefix=args.price_prefix,
                default_auto_print=auto_print_enabled,
                default_duplex_print=duplex_print_enabled,
                default_shutdown_after_print=shutdown_after_print_enabled,
            )
        if configured is None:
            print_status("CANCEL", "Geracao cancelada pelo usuario.")
            return 0
        items, auto_print_enabled, duplex_print_enabled, shutdown_after_print_enabled = configured
        if template_panel_selection:
            selected_a4 = template_panel_selection.get("A4")
            selected_a5 = template_panel_selection.get("A5")
            selected_a6 = template_panel_selection.get("A6")
            template_cdr_a4_override = selected_a4 if isinstance(selected_a4, Path) else None
            template_cdr_a5_override = selected_a5 if isinstance(selected_a5, Path) else None
            template_cdr_a6_override = selected_a6 if isinstance(selected_a6, Path) else None
            print_status(
                "MODEL",
                "Modelos selecionados no painel: "
                f"A4={template_cdr_a4_override or 'automatico'}, "
                f"A5={template_cdr_a5_override or 'automatico'}, "
                f"A6={template_cdr_a6_override or 'automatico'}",
            )
        if not items:
            print("Nenhuma placa selecionada para gerar.", file=sys.stderr)
            return 5

    persist_plate_selection_learning(items)
    print_status("CACHE", f"Correcoes da selecao gravadas no aprendizado: {len(items)} item(ns).")

    selected_count = len(items)
    job_entries: List[Dict[str, Any]] = []
    used_single_names: Dict[str, int] = {}
    for item in items:
        base_replacements = build_replacements(item)
        per_format_qty = normalize_format_quantities(
            item.format_quantities,
            default_format=item.plate_format,
            default_qty=item.quantity,
        )
        for plate_format in ("A4", "A5", "A6"):
            qty = int(per_format_qty.get(plate_format, 0))
            if qty <= 0:
                continue
            for copy_index in range(1, qty + 1):
                base_name = sanitize_filename(item.name, f"placa-{item.index:03d}")
                seq = used_single_names.get(base_name, 0) + 1
                used_single_names[base_name] = seq
                single_name = base_name if seq == 1 else f"{base_name} ({seq})"
                replacements = dict(base_replacements)
                replacements["FORMATO_PLACA"] = plate_format
                replacements["COPIA"] = str(copy_index)
                replacements["COPIA_TOTAL"] = str(qty)
                job_entries.append(
                    {
                        "item": item,
                        "single_name": single_name,
                        "replacements": replacements,
                        "plate_format": plate_format,
                        "duplex_enabled": bool(item.duplex_enabled),
                    }
                )

    total_jobs = len(job_entries)
    total_a5_jobs = sum(1 for job in job_entries if str(job.get("plate_format", "A4")) == "A5")
    total_a6_jobs = sum(1 for job in job_entries if str(job.get("plate_format", "A4")) == "A6")
    total_a4_jobs = total_jobs - total_a5_jobs - total_a6_jobs
    total_output_files = _estimate_grouped_output_files(job_entries)
    progress_tracker = _get_active_web_progress_tracker()
    if progress_tracker:
        progress_tracker.set_phase("preparing", "Preparando geracao", "Organizando filas e formatos das placas.")
        progress_tracker.start(total_jobs)
    print_status("PLACA", f"Placas selecionadas: {selected_count}")
    print_status("JOBS", f"Total de geracoes: {total_jobs}")
    print_status(
        "FMT",
        "Formatos selecionados: "
        f"A4={total_a4_jobs} placa(s), A5={total_a5_jobs} placa(s), A6={total_a6_jobs} placa(s). "
        f"Arquivos finais previstos: {total_output_files}"
    )

    if args.pdf_output_dir:
        output_pdf_dir = Path(args.pdf_output_dir).expanduser().resolve()
    else:
        pdf_subdir = sanitize_filename(str(args.pdf_subdir), "pdf")
        output_pdf_dir = output_dir / pdf_subdir
    output_cdr_dir = output_dir / "cdr"
    output_pdf_dir.mkdir(parents=True, exist_ok=True)
    if args.save_cdr:
        output_cdr_dir.mkdir(parents=True, exist_ok=True)

    stop_controller = HotkeyStopController(args.stop_hotkey)
    hotkey_enabled = stop_controller.start()
    if hotkey_enabled:
        print_status("STOP", f"Hotkey de parada ativa: {args.stop_hotkey.upper()} (aperte para interromper).")
    else:
        print_status("STOP", "Hotkey de parada indisponivel. Instale 'pynput' para usar F7.")

    agent = CorelDrawAgent(visible=not args.hidden_corel, profile_name=args.profile)
    print_status("CACHE", f"Aprendizado ativo: {agent.learned_name_count()} nomes no cache.")
    generated_output_records: List[OutputPdfRecord] = []
    failed_jobs: List[str] = []
    try:
        print_status("COREL", "Abrindo CorelDRAW...")
        if progress_tracker:
            progress_tracker.set_phase("opening_corel", "Abrindo CorelDRAW", "Inicializando o CorelDRAW para gerar as placas.")
        agent.open()

        template_cdr_a4: Optional[Path] = template_cdr_a4_override or template_cdr
        template_cdr_a5: Optional[Path] = template_cdr_a5_override or template_cdr
        template_cdr_a6: Optional[Path] = template_cdr_a6_override or template_cdr
        if template_cdr:
            parent = template_cdr.parent
            name = template_cdr.name

            def _swap_template_suffix(template_name: str, target_format: str) -> str:
                swapped = re.sub(r"(?i)a[456]", target_format, template_name)
                return swapped if swapped != template_name else template_name

            name_a4 = _swap_template_suffix(name, "A4")
            name_a5 = _swap_template_suffix(name, "A5")
            name_a6 = _swap_template_suffix(name, "A6")
            a4_candidates: List[Path] = []
            a5_candidates: List[Path] = []
            a6_candidates: List[Path] = []
            if name_a4 != name:
                a4_candidates.append(parent / name_a4)
            a4_candidates.extend([parent / "A4.cdr", template_cdr])
            if name_a5 != name:
                a5_candidates.append(parent / name_a5)
            a5_candidates.extend([parent / "A5.cdr", template_cdr])
            if name_a6 != name:
                a6_candidates.append(parent / name_a6)
            a6_candidates.extend([parent / "A6.cdr", template_cdr])

            if not template_cdr_a4_override:
                for cand in a4_candidates:
                    if cand and cand.exists():
                        template_cdr_a4 = cand
                        break
            if not template_cdr_a5_override:
                for cand in a5_candidates:
                    if cand and cand.exists():
                        template_cdr_a5 = cand
                        break
            if not template_cdr_a6_override:
                for cand in a6_candidates:
                    if cand and cand.exists():
                        template_cdr_a6 = cand
                        break

        tmp_dir_a5: Optional[Path] = None
        tmp_dir_a6: Optional[Path] = None
        has_any_a5 = any(str(job.get("plate_format", "A4")) == "A5" for job in job_entries)
        has_any_a6 = any(str(job.get("plate_format", "A4")) == "A6" for job in job_entries)
        if has_any_a5:
            tmp_dir_a5 = output_pdf_dir / "_tmp_duo_a5"
            if tmp_dir_a5.exists():
                shutil.rmtree(tmp_dir_a5, ignore_errors=True)
            tmp_dir_a5.mkdir(parents=True, exist_ok=True)
        if has_any_a6:
            tmp_dir_a6 = output_pdf_dir / "_tmp_quad_a6"
            if tmp_dir_a6.exists():
                shutil.rmtree(tmp_dir_a6, ignore_errors=True)
            tmp_dir_a6.mkdir(parents=True, exist_ok=True)

        used_output_names: Dict[str, int] = {}

        def _next_output_name(raw_name: str, fallback: str) -> str:
            base_name = sanitize_filename(raw_name, fallback)
            seq = used_output_names.get(base_name, 0) + 1
            used_output_names[base_name] = seq
            return base_name if seq == 1 else f"{base_name} ({seq})"

        def _group_output_name(names: List[str], fallback: str) -> str:
            clean = [n for n in names if n]
            if not clean:
                return fallback
            if len(clean) == 1:
                return clean[0]
            if len(clean) == 2:
                return f"{clean[0]} + {clean[1]}"
            return f"{clean[0]} + {len(clean) - 1} placas"

        temp_records_a5: List[OutputPdfRecord] = []
        temp_records_a6: List[OutputPdfRecord] = []
        output_number = 0
        try:
            for job_number, job in enumerate(job_entries, start=1):
                if stop_controller.is_stop_requested():
                    raise StopRequestedError("Processo interrompido por F7.")

                item = job["item"]
                single_name = str(job["single_name"])
                replacements = dict(job["replacements"])
                plate_format = normalize_plate_format(str(job.get("plate_format", "A4")), default=args.profile)
                duplex_row_enabled = bool(job.get("duplex_enabled"))
                out_cdr = output_cdr_dir / f"{single_name}.cdr" if args.save_cdr else None
                if plate_format == "A5":
                    template_for_job = template_cdr_a5
                elif plate_format == "A6":
                    template_for_job = template_cdr_a6
                else:
                    template_for_job = template_cdr_a4
                if progress_tracker:
                    progress_tracker.set_current(job_number, total_jobs, single_name, plate_format)
                progress_output_name = ""
                try:
                    if plate_format == "A5":
                        if tmp_dir_a5 is None:
                            raise RuntimeError("Diretorio temporario A5 nao inicializado.")
                        temp_pdf = tmp_dir_a5 / f"{job_number:04d}_{sanitize_filename(single_name, f'placa-{job_number:03d}')}.pdf"
                        if template_for_job:
                            agent.create_from_template(
                                template_for_job,
                                replacements,
                                temp_pdf,
                                out_cdr,
                                font_name=args.font_name,
                                stop_requested=stop_controller.is_stop_requested,
                            )
                        else:
                            agent.create_simple(
                                item.name,
                                item.price,
                                temp_pdf,
                                width_mm=args.page_width_mm,
                                height_mm=args.page_height_mm,
                                font_name=args.font_name,
                                stop_requested=stop_controller.is_stop_requested,
                            )
                        print(f"[OK] placa {job_number}/{total_jobs} ({plate_format}) -> {temp_pdf.name}")
                        temp_records_a5.append(
                            OutputPdfRecord(
                                path=temp_pdf,
                                duplex_enabled=duplex_row_enabled,
                                plate_format="A5",
                                plate_names=[single_name],
                                order_index=job_number,
                            )
                        )
                        progress_output_name = temp_pdf.name
                    elif plate_format == "A6":
                        if tmp_dir_a6 is None:
                            raise RuntimeError("Diretorio temporario A6 nao inicializado.")
                        temp_pdf = tmp_dir_a6 / f"{job_number:04d}_{sanitize_filename(single_name, f'placa-{job_number:03d}')}.pdf"
                        if template_for_job:
                            agent.create_from_template(
                                template_for_job,
                                replacements,
                                temp_pdf,
                                out_cdr,
                                font_name=args.font_name,
                                stop_requested=stop_controller.is_stop_requested,
                            )
                        else:
                            agent.create_simple(
                                item.name,
                                item.price,
                                temp_pdf,
                                width_mm=args.page_width_mm,
                                height_mm=args.page_height_mm,
                                font_name=args.font_name,
                                stop_requested=stop_controller.is_stop_requested,
                            )
                        print(f"[OK] placa {job_number}/{total_jobs} ({plate_format}) -> {temp_pdf.name}")
                        temp_records_a6.append(
                            OutputPdfRecord(
                                path=temp_pdf,
                                duplex_enabled=duplex_row_enabled,
                                plate_format="A6",
                                plate_names=[single_name],
                                order_index=job_number,
                            )
                        )
                        progress_output_name = temp_pdf.name
                    else:
                        output_name = _next_output_name(single_name, f"placa-{job_number:03d}")
                        out_pdf = output_pdf_dir / f"{output_name}.pdf"
                        if template_for_job:
                            agent.create_from_template(
                                template_for_job,
                                replacements,
                                out_pdf,
                                out_cdr,
                                font_name=args.font_name,
                                stop_requested=stop_controller.is_stop_requested,
                            )
                        else:
                            agent.create_simple(
                                item.name,
                                item.price,
                                out_pdf,
                                width_mm=args.page_width_mm,
                                height_mm=args.page_height_mm,
                                font_name=args.font_name,
                                stop_requested=stop_controller.is_stop_requested,
                            )
                        output_number += 1
                        print(f"[OK] arquivo {output_number}/{total_output_files} -> {out_pdf.name}")
                        generated_output_records.append(
                            OutputPdfRecord(
                                path=out_pdf,
                                duplex_enabled=duplex_row_enabled,
                                plate_format="A4",
                                plate_names=[single_name],
                                order_index=job_number,
                            )
                        )
                        progress_output_name = out_pdf.name
                    if progress_tracker:
                        progress_tracker.mark_success(progress_output_name)
                except StopRequestedError:
                    raise
                except Exception as job_exc:
                    failed_message = f"{single_name} ({plate_format}): {job_exc}"
                    failed_jobs.append(failed_message)
                    print(f"[ERRO] placa {job_number}/{total_jobs} -> {failed_message}")
                    if progress_tracker:
                        progress_tracker.mark_failure(single_name, str(job_exc), job_index=job_number)
                    continue

            def _flush_a5_records(records: List[OutputPdfRecord]) -> None:
                nonlocal output_number
                if progress_tracker and records:
                    progress_tracker.set_phase("grouping", "Montando arquivos finais", "Agrupando placas A5 em arquivos finais.")
                idx = 0
                while idx < len(records):
                    left_record = records[idx]
                    right_record = None
                    if idx + 1 < len(records) and records[idx + 1].duplex_enabled == left_record.duplex_enabled:
                        right_record = records[idx + 1]
                    try:
                        names = list(left_record.plate_names)
                        if right_record is not None:
                            names.extend(right_record.plate_names)
                        raw_name = _group_output_name(names, f"a5-duo-{output_number + 1:03d}")
                        output_name = _next_output_name(raw_name, f"a5-duo-{output_number + 1:03d}")
                        out_pdf = output_pdf_dir / f"{output_name}.pdf"
                        merge_two_plate_pdfs_side_by_side(
                            left_record.path,
                            right_record.path if right_record else None,
                            out_pdf,
                            gap_mm=A5_PAIR_GAP_MM,
                        )
                        output_number += 1
                        print(f"[OK] arquivo {output_number}/{total_output_files} -> {out_pdf.name}")
                        generated_output_records.append(
                            OutputPdfRecord(
                                path=out_pdf,
                                duplex_enabled=left_record.duplex_enabled,
                                plate_format="A5",
                                plate_names=names,
                                order_index=left_record.order_index,
                            )
                        )
                    except Exception as pending_exc:
                        failed_name = _group_output_name(
                            list(left_record.plate_names)
                            + (list(right_record.plate_names) if right_record is not None else []),
                            "A5",
                        )
                        failed_message = f"{failed_name} (A5): {pending_exc}"
                        failed_jobs.append(failed_message)
                        print(f"[ERRO] merge final A5 -> {failed_message}")
                        if progress_tracker:
                            progress_tracker.mark_failure(failed_name, str(pending_exc))
                    idx += 2 if right_record is not None else 1

            def _flush_a6_records(records: List[OutputPdfRecord]) -> None:
                nonlocal output_number
                if progress_tracker and records:
                    progress_tracker.set_phase("grouping", "Montando arquivos finais", "Agrupando placas A6 em arquivos finais.")
                idx = 0
                while idx < len(records):
                    group = [records[idx]]
                    next_idx = idx + 1
                    while (
                        next_idx < len(records)
                        and len(group) < 4
                        and records[next_idx].duplex_enabled == group[0].duplex_enabled
                    ):
                        group.append(records[next_idx])
                        next_idx += 1
                    try:
                        group_names: List[str] = []
                        group_pdfs: List[Path] = []
                        for record in group:
                            group_names.extend(record.plate_names)
                            group_pdfs.append(record.path)
                        raw_name = _group_output_name(group_names, f"a6-quad-{output_number + 1:03d}")
                        output_name = _next_output_name(raw_name, f"a6-quad-{output_number + 1:03d}")
                        out_pdf = output_pdf_dir / f"{output_name}.pdf"
                        merge_four_plate_pdfs_grid(
                            group_pdfs,
                            out_pdf,
                            gap_x_mm=A6_QUAD_GAP_X_MM,
                            gap_y_mm=A6_QUAD_GAP_Y_MM,
                        )
                        output_number += 1
                        print(f"[OK] arquivo {output_number}/{total_output_files} -> {out_pdf.name}")
                        generated_output_records.append(
                            OutputPdfRecord(
                                path=out_pdf,
                                duplex_enabled=group[0].duplex_enabled,
                                plate_format="A6",
                                plate_names=group_names,
                                order_index=group[0].order_index,
                            )
                        )
                    except Exception as pending_exc:
                        failed_name = _group_output_name(
                            [name for record in group for name in record.plate_names],
                            "A6",
                        )
                        failed_message = f"{failed_name} (A6): {pending_exc}"
                        failed_jobs.append(failed_message)
                        print(f"[ERRO] merge final A6 -> {failed_message}")
                        if progress_tracker:
                            progress_tracker.mark_failure(failed_name, str(pending_exc))
                    idx = next_idx

            _flush_a5_records(temp_records_a5)
            _flush_a6_records(temp_records_a6)
            if failed_jobs:
                print(f"[WARN] Placas com erro: {len(failed_jobs)}")
                for failure in failed_jobs:
                    print(f"       - {failure}")
            if progress_tracker:
                progress_tracker.finish("finished_with_errors" if failed_jobs else "finished")
        finally:
            if tmp_dir_a5 is not None:
                shutil.rmtree(tmp_dir_a5, ignore_errors=True)
            if tmp_dir_a6 is not None:
                shutil.rmtree(tmp_dir_a6, ignore_errors=True)

    except StopRequestedError as exc:
        if progress_tracker:
            progress_tracker.finish("stopped")
        print(str(exc))
        _shutdown_active_web_progress_session()
        return 130
    finally:
        stop_controller.stop()
        agent.persist_learning_cache(force=True)
        if args.close_corel:
            print_status("COREL", "Fechando CorelDRAW...")
            agent.close()

    try:
        audit_result = save_plate_generation_audit(
            items=items,
            generated_output_records=generated_output_records,
            failed_jobs=failed_jobs,
            output_pdf_dir=output_pdf_dir,
        )
        if audit_result.get("saved_remote"):
            saved_formats = ", ".join(
                plate_format
                for plate_format in ("A4", "A5", "A6")
                if plate_format in dict(audit_result.get("persist_details") or {})
            )
            print_status(
                "CACHE",
                "Auditoria de placas salva no repositorio remoto"
                + (f" ({saved_formats})." if saved_formats else "."),
            )
        elif audit_result.get("saved_local"):
            print_status(
                "CACHE",
                "Auditoria de placas salva localmente; sem token GitHub para sincronizar no remoto.",
            )
        else:
            print_status("CACHE", f"Falha ao salvar auditoria de placas: {audit_result.get('message')}")
    except Exception as audit_exc:
        print_status("CACHE", f"Falha ao registrar auditoria de placas: {audit_exc}")

    if auto_print_enabled:
        ordered_output_records = sorted(generated_output_records, key=lambda record: (record.order_index, record.path.name))
        if ordered_output_records:
            print_job_temp_dir = output_pdf_dir / "_tmp_print_jobs"
            if print_job_temp_dir.exists():
                shutil.rmtree(print_job_temp_dir, ignore_errors=True)
            try:
                if progress_tracker:
                    progress_tracker.set_phase("print_prepare", "Preparando impressao", "Montando os jobs finais para envio a impressora.")
                print_jobs = build_print_job_pdfs(
                    ordered_output_records,
                    print_job_temp_dir,
                    duplex_enabled=duplex_print_enabled,
                )
                duplex_selected = sum(1 for record in ordered_output_records if record.duplex_enabled)
                print_status(
                    "PRINT",
                    f"Enviando {len(print_jobs)} job(s) PDF para impressao..."
                    + (f" ({duplex_selected} arquivo(s) marcados frente/verso)" if duplex_print_enabled else ""),
                )
                if progress_tracker:
                    progress_tracker.set_phase(
                        "printing",
                        "Enviando para impressora",
                        f"Mandando {len(print_jobs)} job(s) para a fila de impressao.",
                    )
                sent_count, print_errors, print_log_path = send_pdfs_to_default_printer(print_jobs)
                print_status("PRINT", f"Impressao: {sent_count}/{len(print_jobs)} job(s) enviados.")
                print_status("LOG", f"Log de impressao: {print_log_path}")
                if progress_tracker:
                    if print_errors:
                        progress_tracker.set_phase(
                            "printing_errors",
                            "Impressao com falhas",
                            f"{len(print_errors)} job(s) tiveram falha no envio para impressao.",
                        )
                    else:
                        progress_tracker.set_phase(
                            "printing_done",
                            "Impressao enviada",
                            f"{sent_count}/{len(print_jobs)} job(s) enviados para a fila de impressao.",
                        )
                if print_errors:
                    print("[WARN] Falhas ao enviar para impressao:")
                    for msg in print_errors:
                        print(f"       - {msg}")
                elif shutdown_after_print_enabled:
                    print_status("POWER", "Todos os jobs foram enviados. Desligando o computador...")
                    shutdown_computer_windows()
            finally:
                if print_job_temp_dir.exists():
                    shutil.rmtree(print_job_temp_dir, ignore_errors=True)
        else:
            print_status("PRINT", "Impressao automatica ativa, mas nenhum PDF final foi gerado.")
            if progress_tracker:
                progress_tracker.set_phase("print_skip", "Impressao sem arquivos", "Nenhum PDF final foi gerado para enviar.")

    if progress_tracker:
        current_phase = str(progress_tracker.snapshot().get("phase") or "")
        if current_phase not in {"stopped", "cancelled"}:
            progress_tracker.finish("finished_with_errors" if current_phase in {"finished_with_errors", "printing_errors"} else "finished")

    print_status("FIM", f"Concluido. PDFs gerados em: {output_pdf_dir}")
    _shutdown_active_web_progress_session()
    return 0


def _write_crash_log(exc: Exception) -> Path:
    log_path = Path(__file__).resolve().with_name("erro_execucao_placas.log")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    details = [
        f"[{timestamp}] Erro nao tratado",
        f"Tipo: {type(exc).__name__}",
        f"Mensagem: {exc}",
        f"Python: {sys.version}",
        f"CWD: {Path.cwd()}",
        "",
        traceback.format_exc(),
        "",
        "=" * 80,
        "",
    ]
    try:
        with log_path.open("a", encoding="utf-8") as fh:
            fh.write("\n".join(details))
    except Exception:
        pass
    return log_path


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as exc:
        log_path = _write_crash_log(exc)
        print(f"[ERRO] Falha inesperada: {exc}", file=sys.stderr)
        print(f"[ERRO] Detalhes em: {log_path}", file=sys.stderr)
        try:
            import tkinter as tk
            from tkinter import messagebox

            root = tk.Tk()
            root.withdraw()
            messagebox.showerror(
                "Erro na execucao",
                f"Ocorreu um erro inesperado.\n\nDetalhes salvos em:\n{log_path}",
            )
            root.destroy()
        except Exception:
            pass
        raise SystemExit(1)
