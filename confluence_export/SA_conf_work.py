#!/usr/bin/env python3
"""
Экспорт страниц Confluence, созданных конкретным пользователем.

Запуск:
  python SA_conf_work.py
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
import requests
from requests.auth import HTTPBasicAuth

warnings.filterwarnings("ignore")

try:
    import pdfkit
except ImportError:  # optional until PDF export
    pdfkit = None


# --- КОНФИГУРАЦИЯ (можно просто править здесь и запускать без аргументов) ---
CONFLUENCE_URL = "https://confluence.moscow.alfaintra.net"
USERNAME = ""  # пусто = Bearer PAT; если Basic не проходит — укажите r"MOSCOW\U_M2XNX"
API_TOKEN = ""  # <<< вставьте сюда ваш PAT (как в старом скрипте)
OUTPUT_DIR = "./confluence_pdfs_Zabaryanskiy"
CACHE_FILE = "./confluence_pages_cache_Zabaryanskiy.json"
DOWNLOAD_WORKERS = 5
WKHTMLTOPDF_PATH = r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe"
# Если CQL по creator пустой — автоматически обойти пространства
AUTO_FALLBACK_SCAN = True

TARGET_NAMES = [
    r"MOSCOW\U_M2XNX",
    "U_M2XNX",
    "Забарянский Юрий Геннадьевич",
    "Забарянский Юрий",
    "Забарянский Ю.Г.",
    "Забарянский",
    "Zabaryanskiy",
    "Y. Zabaryanskiy",
    "YZabaryanskiy",
    "YZabaryanskiy@alfabank.ru",
]
# ---------------------------------------------------------------------------


print_lock = Lock()


def safe_print(*args: Any, **kwargs: Any) -> None:
    with print_lock:
        print(*args, **kwargs)


def build_session(
    base_url: str,
    username: str,
    token: str,
    verify_ssl: bool,
) -> requests.Session:
    session = requests.Session()
    session.verify = verify_ssl
    session.headers.update({"Accept": "application/json"})

    # Confluence Server/DC: чаще Basic (user + PAT/password).
    # Cloud / некоторые инсталляции: Bearer PAT.
    if username:
        session.auth = HTTPBasicAuth(username, token)
    else:
        session.headers["Authorization"] = f"Bearer {token}"

    # Быстрая проверка доступности
    ping = session.get(f"{base_url.rstrip('/')}/rest/api/space", params={"limit": 1}, timeout=30)
    if ping.status_code not in (200, 401, 403):
        raise RuntimeError(f"Confluence недоступен: HTTP {ping.status_code} {ping.text[:200]}")
    if ping.status_code in (401, 403):
        raise RuntimeError(
            f"Ошибка авторизации: HTTP {ping.status_code}. "
            "Проверьте USERNAME/API_TOKEN (для Server/DC обычно нужен логин + PAT)."
        )
    safe_print("✅ Сессия создана, API доступен.")
    return session


def load_cache(cache_file: str, expected_user: str) -> Optional[List[Dict[str, Any]]]:
    if not os.path.exists(cache_file):
        return None
    try:
        with open(cache_file, "r", encoding="utf-8") as f:
            cache = json.load(f)
        if cache.get("user") != expected_user:
            safe_print(f"⚠️ Кеш относится к другому пользователю ({cache.get('user')}), игнорируем.")
            return None
        pages = cache.get("pages", [])
        safe_print(f"📂 Кеш: {cache_file} ({len(pages)} страниц)")
        return pages
    except Exception as e:
        safe_print(f"⚠️ Ошибка чтения кеша: {e}")
        return None


def save_cache(cache_file: str, user_label: str, pages: List[Dict[str, Any]]) -> None:
    try:
        payload = {
            "user": user_label,
            "date": time.strftime("%Y-%m-%d %H:%M:%S"),
            "total": len(pages),
            "pages": pages,
        }
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        safe_print(f"💾 Кеш сохранён: {cache_file} ({len(pages)} страниц)")
    except Exception as e:
        safe_print(f"⚠️ Ошибка сохранения кеша: {e}")


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def expand_user_hints(hints: Iterable[str]) -> List[str]:
    """
    Разворачивает доменные логины AD/Windows:
      MOSCOW\\U_M2XNX → MOSCOW\\U_M2XNX, U_M2XNX, MOSCOW/U_M2XNX, ...
    """
    ordered: List[str] = []
    seen: Set[str] = set()

    def add(value: str) -> None:
        v = (value or "").strip()
        if not v or v in seen:
            return
        seen.add(v)
        ordered.append(v)

    for raw in hints:
        if not raw or not str(raw).strip():
            continue
        hint = str(raw).strip()
        add(hint)

        # DOMAIN\sam / DOMAIN/sam
        for sep in ("\\", "/"):
            if sep in hint and not hint.lower().startswith("http"):
                parts = hint.split(sep)
                if len(parts) >= 2 and parts[-1]:
                    domain = parts[0]
                    sam = parts[-1]
                    add(sam)
                    add(f"{domain}\\{sam}")
                    add(f"{domain}/{sam}")
                    add(sam.upper())
                    add(sam.lower())
                    add(f"{domain.upper()}\\{sam.upper()}")
                    add(f"{domain.lower()}\\{sam.lower()}")

        if "@" in hint:
            add(hint.split("@", 1)[0])

    return ordered


def _cql_quote(value: str) -> str:
    """Экранирование строки для CQL (важно для MOSCOW\\USER)."""
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _username_variants(identity: Dict[str, str]) -> List[str]:
    """Все разумные формы username для CQL creator/contributor."""
    raw: List[str] = []
    for key in ("username", "userKey", "accountId"):
        if identity.get(key):
            raw.append(identity[key])
    return expand_user_hints(raw)


def _user_identity(user: Dict[str, Any]) -> Dict[str, str]:
    """Нормализованные идентификаторы пользователя Confluence Server/DC/Cloud."""
    return {
        "username": user.get("username") or user.get("userName") or "",
        "userKey": user.get("userKey") or user.get("key") or "",
        "accountId": user.get("accountId") or "",
        "displayName": user.get("displayName") or user.get("fullName") or "",
        "email": user.get("email") or user.get("emailAddress") or "",
    }


def _cql_user_value(identity: Dict[str, str]) -> Optional[str]:
    """Значение для CQL creator=/contributor= — username, иначе accountId/userKey."""
    for key in ("username", "accountId", "userKey"):
        if identity.get(key):
            return identity[key]
    return None


def resolve_user(
    session: requests.Session,
    base_url: str,
    hints: Iterable[str],
) -> Optional[Dict[str, str]]:
    """
    Найти пользователя по логину / email / displayName.
    Пробуем несколько REST-эндпоинтов Server/DC.
    """
    base = base_url.rstrip("/")
    hints_list = expand_user_hints(hints)
    seen: Set[str] = set()
    candidates: List[Dict[str, Any]] = []

    def add_users(users: Iterable[Dict[str, Any]]) -> None:
        for u in users:
            if not isinstance(u, dict):
                continue
            ident = _user_identity(u)
            key = ident["username"] or ident["userKey"] or ident["accountId"] or ident["displayName"]
            if not key or key in seen:
                continue
            seen.add(key)
            candidates.append(u)

    for hint in hints_list:
        hint = hint.strip()
        safe_print(f"👤 Резолв пользователя: '{hint}'")

        endpoints = [
            (f"{base}/rest/api/user", {"username": hint}),
            (f"{base}/rest/api/user", {"key": hint}),
            (f"{base}/rest/api/user/search", {"username": hint, "limit": 25}),
            (f"{base}/rest/api/user/search", {"searchString": hint, "limit": 25}),
        ]

        # Прямой lookup
        for url, params in endpoints[:2]:
            try:
                r = session.get(url, params=params, timeout=30)
                if r.status_code == 200:
                    data = r.json()
                    if isinstance(data, dict) and (data.get("username") or data.get("userKey") or data.get("accountId")):
                        add_users([data])
            except Exception as e:
                safe_print(f"   ⚠️ {url}: {e}")

        # Search
        for url, params in endpoints[2:]:
            try:
                r = session.get(url, params=params, timeout=30)
                if r.status_code == 200:
                    data = r.json()
                    if isinstance(data, list):
                        add_users(data)
                    elif isinstance(data, dict):
                        add_users(data.get("results") or data.get("users") or [])
            except Exception as e:
                safe_print(f"   ⚠️ {url}: {e}")

        # CQL user search (Server 7+)
        for cql in (
            f'type=user AND user.fullname~"{hint}"',
            f'type=user AND user.fullname="{hint}"',
            f'type=user AND user.fullname~"{hint.split("@")[0]}"' if "@" in hint else None,
        ):
            if not cql:
                continue
            try:
                r = session.get(
                    f"{base}/rest/api/search",
                    params={"cql": cql, "limit": 25},
                    timeout=30,
                )
                if r.status_code != 200:
                    continue
                for item in r.json().get("results", []):
                    user_obj = item.get("user") or item
                    if isinstance(user_obj, dict):
                        add_users([user_obj])
            except Exception as e:
                safe_print(f"   ⚠️ CQL user search: {e}")

    if not candidates:
        safe_print("❌ Пользователь не найден ни по одному варианту имени.")
        return None

    # Ранжируем: точное совпадение username/email/displayName важнее
    ranked: List[Tuple[int, Dict[str, str]]] = []
    norm_hints = {_norm(h) for h in hints_list}
    for u in candidates:
        ident = _user_identity(u)
        score = 0
        for field in ("username", "email", "displayName", "userKey", "accountId"):
            val = _norm(ident.get(field, ""))
            if not val:
                continue
            if val in norm_hints:
                score += 100
            elif any(h in val or val in h for h in norm_hints if len(h) >= 3):
                score += 10
        ranked.append((score, ident))

    ranked.sort(key=lambda x: x[0], reverse=True)
    best_score, best = ranked[0]
    safe_print(
        f"✅ Пользователь: displayName='{best['displayName']}', "
        f"username='{best['username']}', userKey='{best['userKey']}', "
        f"email='{best['email']}' (score={best_score})"
    )
    if best_score == 0:
        safe_print("⚠️ Совпадение слабое — проверьте, что это нужный человек.")
        safe_print("   Кандидаты:")
        for score, ident in ranked[:10]:
            safe_print(
                f"   - {ident['displayName']} | {ident['username']} | "
                f"{ident['email']} | key={ident['userKey']}"
            )
    return best


def _page_from_search_result(result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    /rest/api/search возвращает обёртку:
      { content: { id, title, space, history, version, ... }, title, ... }
    Иногда (редко) поля лежат на верхнем уровне.
    """
    content = result.get("content") if isinstance(result.get("content"), dict) else result
    if not isinstance(content, dict):
        return None
    page_id = content.get("id") or result.get("id")
    if not page_id:
        return None
    if content.get("type") and content.get("type") != "page":
        return None

    space = content.get("space") or {}
    history = content.get("history") or {}
    created_by = history.get("createdBy") or {}
    version = content.get("version") or {}
    version_by = version.get("by") or {}

    return {
        "id": str(page_id),
        "title": content.get("title") or result.get("title") or f"page_{page_id}",
        "space": (space.get("key") if isinstance(space, dict) else None) or "unknown",
        "author": created_by.get("displayName")
        or version_by.get("displayName")
        or "Неизвестен",
        "creatorUsername": created_by.get("username") or "",
        "url": (content.get("_links") or {}).get("webui") or (result.get("url") or ""),
    }


def search_cql_pages(
    session: requests.Session,
    base_url: str,
    cql: str,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    search_url = f"{base_url.rstrip('/')}/rest/api/search"
    start = 0
    pages: List[Dict[str, Any]] = []
    safe_print(f"🔍 CQL: {cql}")

    while True:
        params = {
            "cql": cql,
            "limit": limit,
            "start": start,
            "expand": "content.space,content.history,content.history.createdBy,content.version",
        }
        try:
            r = session.get(search_url, params=params, timeout=60)
            if r.status_code == 400:
                safe_print(f"   ⚠️ CQL 400: {r.text[:300]}")
                break
            if r.status_code != 200:
                safe_print(f"   ❌ HTTP {r.status_code}: {r.text[:300]}")
                break

            data = r.json()
            results = data.get("results") or []
            for item in results:
                page = _page_from_search_result(item)
                if page:
                    pages.append(page)

            safe_print(f"   📄 batch={len(results)}, total_so_far={len(pages)}")

            size = data.get("size", len(results))
            next_link = (data.get("_links") or {}).get("next")
            if not next_link and size < limit:
                break
            start += limit
            # защита от бесконечного цикла, если API всегда возвращает size==limit
            if not results:
                break
            if start > 100000:
                safe_print("   ⚠️ Достигнут лимит пагинации (100k), остановка")
                break
        except Exception as e:
            safe_print(f"   ⚠️ Ошибка поиска: {e}")
            break

    return pages


def search_pages_by_user(
    session: requests.Session,
    base_url: str,
    identity: Dict[str, str],
) -> List[Dict[str, Any]]:
    """Основной поиск: creator, затем contributor (все варианты DOMAIN\\user)."""
    variants = _username_variants(identity)
    if not variants:
        return []

    queries: List[str] = []
    seen_q: Set[str] = set()
    for user_val in variants:
        for field in ("creator", "contributor"):
            q = f"type=page AND {field} = {_cql_quote(user_val)}"
            if q not in seen_q:
                seen_q.add(q)
                queries.append(q)

    all_pages: Dict[str, Dict[str, Any]] = {}
    for q in queries:
        for page in search_cql_pages(session, base_url, q):
            all_pages[page["id"]] = page
        # как только creator что-то нашёл — contributor всё равно добьём,
        # но не останавливаемся раньше: разные формы логина могут дать разный набор

    safe_print(f"📊 Уникальных страниц по CQL: {len(all_pages)}")
    return list(all_pages.values())


def list_spaces(session: requests.Session, base_url: str) -> List[Dict[str, Any]]:
    url = f"{base_url.rstrip('/')}/rest/api/space"
    start = 0
    limit = 50
    spaces: List[Dict[str, Any]] = []
    while True:
        r = session.get(url, params={"limit": limit, "start": start}, timeout=60)
        r.raise_for_status()
        data = r.json()
        batch = data.get("results") or []
        spaces.extend(batch)
        if len(batch) < limit:
            break
        start += limit
    return spaces


def scan_space_for_creator(
    session: requests.Session,
    base_url: str,
    space_key: str,
    identity: Dict[str, str],
    title_hints: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fallback: страницы пространства + фильтр history.createdBy.
    Медленно, но работает, когда CQL creator недоступен/ломается.
    """
    url = f"{base_url.rstrip('/')}/rest/api/content"
    start = 0
    limit = 50
    matched: List[Dict[str, Any]] = []
    usernames = {
        _norm(v)
        for v in _username_variants(identity)
    }
    usernames.discard("")
    # также displayName / email из identity
    for extra in (identity.get("displayName", ""), identity.get("email", "")):
        if extra:
            usernames.add(_norm(extra))
    hint_norms = {_norm(h) for h in (title_hints or []) if h}

    while True:
        params = {
            "spaceKey": space_key,
            "type": "page",
            "status": "current",
            "limit": limit,
            "start": start,
            "expand": "history,history.createdBy,space,version",
        }
        try:
            r = session.get(url, params=params, timeout=60)
            if r.status_code != 200:
                safe_print(f"   ❌ space {space_key}: HTTP {r.status_code}")
                break
            data = r.json()
            results = data.get("results") or []
            for content in results:
                created_by = ((content.get("history") or {}).get("createdBy") or {})
                cand = {
                    _norm(v)
                    for v in expand_user_hints(
                        [
                            created_by.get("username", ""),
                            created_by.get("userKey", ""),
                            created_by.get("accountId", ""),
                            created_by.get("displayName", ""),
                            created_by.get("email", ""),
                        ]
                    )
                }
                cand.discard("")
                if not (cand & usernames):
                    # слабый fallback по displayName hints
                    dn = _norm(created_by.get("displayName", ""))
                    if not any(h and h in dn for h in hint_norms if len(h) >= 5):
                        continue

                matched.append(
                    {
                        "id": str(content.get("id")),
                        "title": content.get("title") or f"page_{content.get('id')}",
                        "space": space_key,
                        "author": created_by.get("displayName") or "Неизвестен",
                        "creatorUsername": created_by.get("username") or "",
                        "url": (content.get("_links") or {}).get("webui") or "",
                    }
                )

            if len(results) < limit:
                break
            start += limit
        except Exception as e:
            safe_print(f"   ⚠️ space {space_key}: {e}")
            break

    return matched


def fallback_scan_spaces(
    session: requests.Session,
    base_url: str,
    identity: Dict[str, str],
    space_keys: Optional[List[str]],
    name_hints: List[str],
    max_spaces: int,
    workers: int,
) -> List[Dict[str, Any]]:
    if space_keys:
        spaces = [{"key": k} for k in space_keys]
    else:
        safe_print("📚 Получаем список пространств...")
        spaces = list_spaces(session, base_url)
        safe_print(f"📚 Пространств доступно: {len(spaces)}")
        if max_spaces > 0:
            spaces = spaces[:max_spaces]

    safe_print(f"🚀 Fallback-обход {len(spaces)} пространств ({workers} потоков)...")
    all_pages: Dict[str, Dict[str, Any]] = {}

    def worker(space: Dict[str, Any]) -> List[Dict[str, Any]]:
        key = space.get("key") or ""
        if not key:
            return []
        found = scan_space_for_creator(session, base_url, key, identity, name_hints)
        if found:
            safe_print(f"   ✅ [{key}] найдено {len(found)}")
        return found

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(worker, s) for s in spaces]
        for fut in as_completed(futures):
            try:
                for page in fut.result():
                    all_pages[page["id"]] = page
            except Exception as e:
                safe_print(f"❌ Ошибка обхода пространства: {e}")

    return list(all_pages.values())


def get_page_html(session: requests.Session, base_url: str, page_id: str) -> Optional[str]:
    url = f"{base_url.rstrip('/')}/rest/api/content/{page_id}"
    try:
        r = session.get(url, params={"expand": "body.storage,version,space"}, timeout=60)
        r.raise_for_status()
        return r.json().get("body", {}).get("storage", {}).get("value", "")
    except Exception as e:
        safe_print(f"   ❌ HTML {page_id}: {e}")
        return None


def get_page_export_pdf(
    session: requests.Session,
    base_url: str,
    page_id: str,
    output_path: str,
) -> bool:
    """
    Предпочтительный способ для Server/DC: встроенный PDF export.
    /spaces/flyingpdf/pdfpageexport.action?pageId=ID
    """
    urls = [
        f"{base_url.rstrip('/')}/spaces/flyingpdf/pdfpageexport.action?pageId={page_id}",
        f"{base_url.rstrip('/')}/exportword?pageId={page_id}",  # не pdf, запасной не используем
    ]
    try:
        r = session.get(urls[0], timeout=120, allow_redirects=True)
        ctype = (r.headers.get("Content-Type") or "").lower()
        if r.status_code == 200 and ("pdf" in ctype or r.content[:4] == b"%PDF"):
            with open(output_path, "wb") as f:
                f.write(r.content)
            return True
        safe_print(f"   ⚠️ native PDF export недоступен ({r.status_code}, {ctype[:40]})")
        return False
    except Exception as e:
        safe_print(f"   ⚠️ native PDF export: {e}")
        return False


def convert_html_to_pdf(
    html_content: str,
    output_path: str,
    wkhtmltopdf_path: Optional[str],
) -> bool:
    if pdfkit is None:
        safe_print("   ❌ pdfkit не установлен (pip install pdfkit) и native PDF не сработал")
        return False
    try:
        styled_html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<style>
body {{ font-family: Arial, sans-serif; margin: 40px; }}
h1, h2, h3 {{ color: #205081; }}
table {{ border-collapse: collapse; width: 100%; }}
th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
th {{ background-color: #f2f2f2; }}
img {{ max-width: 100%; }}
</style>
</head>
<body>{html_content}</body>
</html>"""
        options = {
            "encoding": "UTF-8",
            "quiet": "",
            "enable-local-file-access": "",
            "margin-top": "10mm",
            "margin-bottom": "10mm",
            "margin-left": "10mm",
            "margin-right": "10mm",
        }
        if wkhtmltopdf_path and os.path.exists(wkhtmltopdf_path):
            config = pdfkit.configuration(wkhtmltopdf=wkhtmltopdf_path)
            pdfkit.from_string(styled_html, output_path, options=options, configuration=config)
        else:
            pdfkit.from_string(styled_html, output_path, options=options)
        return True
    except Exception as e:
        safe_print(f"   ❌ Конвертация PDF: {e}")
        return False


def safe_filename(title: str, page_id: str) -> str:
    cleaned = "".join(c for c in title if c.isalnum() or c in (" ", "-", "_", ".")).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    if not cleaned:
        cleaned = f"page_{page_id}"
    # ограничим длину имени файла
    if len(cleaned) > 120:
        cleaned = cleaned[:120].rstrip()
    return cleaned


def export_page_worker(
    session: requests.Session,
    base_url: str,
    page_data: Dict[str, Any],
    output_dir: str,
    wkhtmltopdf_path: Optional[str],
    prefer_native_pdf: bool,
) -> bool:
    page_id = page_data.get("id")
    page_title = page_data.get("title", f"page_{page_id}")
    space_key = page_data.get("space") or "unknown"
    if not page_id:
        return False

    space_dir = os.path.join(output_dir, space_key)
    os.makedirs(space_dir, exist_ok=True)
    out = os.path.join(space_dir, f"{safe_filename(page_title, page_id)}.pdf")

    if os.path.exists(out) and os.path.getsize(out) > 0:
        safe_print(f"⏩ [{page_id}] уже есть: {os.path.basename(out)}")
        return True

    safe_print(f"📥 [{page_id}] {page_title}")
    if prefer_native_pdf and get_page_export_pdf(session, base_url, page_id, out):
        safe_print(f"   ✅ [{page_id}] native PDF")
        return True

    html = get_page_html(session, base_url, page_id)
    if not html:
        return False
    ok = convert_html_to_pdf(html, out, wkhtmltopdf_path)
    safe_print(f"   {'✅' if ok else '❌'} [{page_id}] html→pdf")
    return ok


def export_pages_parallel(
    session: requests.Session,
    base_url: str,
    pages: List[Dict[str, Any]],
    output_dir: str,
    workers: int,
    wkhtmltopdf_path: Optional[str],
    prefer_native_pdf: bool,
) -> Tuple[int, int]:
    if not pages:
        return 0, 0
    ok_n = 0
    fail_n = 0
    safe_print(f"\n🚀 Экспорт PDF: {len(pages)} стр., потоков={workers}")
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {
            pool.submit(
                export_page_worker,
                session,
                base_url,
                page,
                output_dir,
                wkhtmltopdf_path,
                prefer_native_pdf,
            ): page
            for page in pages
        }
        for fut in as_completed(futs):
            try:
                if fut.result():
                    ok_n += 1
                else:
                    fail_n += 1
            except Exception as e:
                safe_print(f"❌ export error: {e}")
                fail_n += 1
    return ok_n, fail_n


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Выгрузка всех Confluence-страниц, созданных пользователем",
    )
    p.add_argument("--url", default=os.getenv("CONFLUENCE_URL", CONFLUENCE_URL))
    p.add_argument("--username", default=os.getenv("CONFLUENCE_USERNAME", USERNAME))
    p.add_argument(
        "--token",
        default=os.getenv("CONFLUENCE_TOKEN") or os.getenv("API_TOKEN") or API_TOKEN,
    )
    p.add_argument("--user", action="append", dest="user_hints", default=None)
    p.add_argument("--output", default=os.getenv("OUTPUT_DIR", OUTPUT_DIR))
    p.add_argument("--cache", default=os.getenv("CACHE_FILE", CACHE_FILE))
    p.add_argument("--workers", type=int, default=DOWNLOAD_WORKERS)
    p.add_argument("--wkhtmltopdf", default=WKHTMLTOPDF_PATH)
    p.add_argument("--ssl-verify", action="store_true")
    p.add_argument("--force-rescan", action="store_true")
    p.add_argument(
        "--fallback-scan",
        action="store_true",
        default=AUTO_FALLBACK_SCAN,
        help="Если CQL пустой — обойти пространства (включено по умолчанию)",
    )
    p.add_argument("--no-fallback-scan", action="store_true")
    p.add_argument("--spaces", default="")
    p.add_argument("--max-spaces", type=int, default=0)
    p.add_argument("--scan-workers", type=int, default=4)
    p.add_argument("--list-only", action="store_true")
    p.add_argument("--no-native-pdf", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    safe_print("🚀 Экспорт страниц Confluence по автору...")
    if not args.token:
        safe_print("❌ Не задан токен.")
        safe_print('   Откройте SA_conf_work.py и вставьте PAT в строку: API_TOKEN = "..."')
        return 2

    hints = expand_user_hints(args.user_hints or TARGET_NAMES)
    verify_ssl = bool(args.ssl_verify)
    do_fallback = bool(args.fallback_scan) and not bool(args.no_fallback_scan)

    os.makedirs(args.output, exist_ok=True)

    session = build_session(args.url, args.username, args.token, verify_ssl=verify_ssl)

    cache_user_label = next(
        (h for h in hints if "\\" in h or h.upper().startswith("U_")),
        hints[0],
    )
    pages = None if args.force_rescan else load_cache(args.cache, cache_user_label)

    if pages is None:
        identity = resolve_user(session, args.url, hints)
        if not identity:
            # Даже без резолва пробуем искать напрямую по доменному логину
            safe_print("⚠️ Пользователь не резолвился через /user API — ищем по логину напрямую")
            identity = {
                "username": r"MOSCOW\U_M2XNX",
                "userKey": "",
                "accountId": "",
                "displayName": "Забарянский Юрий Геннадьевич",
                "email": "YZabaryanskiy@alfabank.ru",
            }

        pages = search_pages_by_user(session, args.url, identity)

        space_keys = [s.strip() for s in args.spaces.split(",") if s.strip()]
        if (not pages and do_fallback) or (do_fallback and space_keys):
            if not pages:
                safe_print("\n⚠️ CQL ничего не дал — запускаем fallback-обход пространств...")
            scanned = fallback_scan_spaces(
                session,
                args.url,
                identity,
                space_keys or None,
                hints,
                args.max_spaces,
                args.scan_workers,
            )
            by_id = {p["id"]: p for p in pages}
            for p in scanned:
                by_id[p["id"]] = p
            pages = list(by_id.values())

        if pages:
            save_cache(args.cache, cache_user_label, pages)
        else:
            safe_print("\n❌ Страницы не найдены.")
            safe_print("Проверьте TARGET_NAMES / API_TOKEN в начале скрипта.")
            safe_print("Или ограничьте обход: python SA_conf_work.py --spaces KEY1,KEY2")
            return 1

    # индекс найденного
    index_path = os.path.join(args.output, "pages_index.json")
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(pages, f, ensure_ascii=False, indent=2)
    safe_print(f"📝 Индекс: {index_path} ({len(pages)} страниц)")

    if args.list_only:
        safe_print("⏹ Режим --list-only: PDF не скачиваем.")
        return 0

    exported, failed = export_pages_parallel(
        session,
        args.url,
        pages,
        args.output,
        args.workers,
        args.wkhtmltopdf,
        prefer_native_pdf=not args.no_native_pdf,
    )
    safe_print(f"\n📊 ИТОГИ: ok={exported}, fail={failed}, dir={args.output}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    t0 = time.time()
    code = main()
    if code is not None:
        safe_print(f"\n⏱️ {time.time() - t0:.1f}s")
    sys.exit(code or 0)
