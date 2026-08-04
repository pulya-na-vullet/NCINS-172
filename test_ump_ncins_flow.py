#!/usr/bin/env python3
"""
NCINS / UMP E2E helper для NIB.

Важно: токен и API должны быть из одной системы.
  - corporate JWT  → corp-gateway (corp-ncins-*-api)     ✅ уже работало у тебя
  - UMP Keycloak JWT → ump-application-facade           ❌ нельзя слать в corp-gateway
    (иначе 401 Jwt issuer is not configured)

Два режима:
  1) --auth corporate  (по умолчанию) curl лида → corp-gateway /v1/applications
  2) --auth ump        Keycloak UMP nib-corp-ncins → application-facade /applications
                       DEV/QA: + POST /applications/list; TEST: create only

GET /applications/{id} у NIB нет в правах → 403. Читаем через list.

Примеры:
  python ump.py --env dev --auth corporate --channel nib
  python ump.py --env dev --auth ump --channel nib
  python ump.py --env dev --auth ump --base-url http://ump-application-facade.umpdevwk8sm1.moscow.alfaintra.net
  python ump.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import datetime, timezone
from typing import Any

import requests
import urllib3

# self-signed / corporate CA на keycloak UMP
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ENVIRONMENTS: dict[str, dict[str, Any]] = {
    "dev": {
        "operate": "http://operate.umpqak8sm1.moscow.alfaintra.net/",
        "kafka": (
            "https://akhq.umpqak8sm1.moscow.alfaintra.net/ui/ump-kafka-cluster/"
            "topic/ump.process.to.system/data?sort=Newest&partition=All"
        ),
        "corporate": {
            "token_url": (
                "http://corp-gateway-dev.moscow.alfaintra.net/mks-gateway/public/auth/"
                "realms/corporate/protocol/openid-connect/token"
            ),
            "client_id": "nib-corp-ncinsurance",
            "client_secret": "nib_corp_ncinsurance",
            "base_url": (
                "http://corp-gateway-dev.moscow.alfaintra.net/"
                "corp-ncins-gateway/secure/corp-ncins-corp-ncins-api"
            ),
            # corp-ncins proxy обычно с /v1
            "paths": {
                "create": "/v1/applications",
                "list": "/v1/applications/list",
                "get": "/v1/applications/{id}",
            },
            "can_list": True,
            "verify_ssl": True,
        },
        "ump": {
            # НИБ Keycloak UMP / DEV — токен ТОЛЬКО для application-facade, не для corp-gateway
            "token_url": (
                "https://keycloak.umpdevwk8sm1.moscow.alfaintra.net/"
                "realms/ump/protocol/openid-connect/token"
            ),
            "client_id": "nib-corp-ncins",
            "client_secret": "OlcnSVnz3UiORtl4XfJZ3NRRZlqw7QPY",
            "base_url": "http://ump-application-facade.umpdevwk8sm1.moscow.alfaintra.net",
            # если host другой — передай --base-url (из Postman / Confluence)
            "base_url_candidates": [
                "http://ump-application-facade.umpdevwk8sm1.moscow.alfaintra.net",
                "https://ump-application-facade.umpdevwk8sm1.moscow.alfaintra.net",
                "http://umpdevwk8sm1.moscow.alfaintra.net/ump-application-facade",
                "https://umpdevwk8sm1.moscow.alfaintra.net/ump-application-facade",
            ],
            # контракт facade: /applications (без /v1)
            "paths": {
                "create": "/applications",
                "list": "/applications/list",
                "get": "/applications/{id}",
            },
            "can_list": True,
            "verify_ssl": False,
        },
        "sfa_base_url": "https://dev.ufrulkint-api.moscow.alfaintra.net/ufr-eos-ul-ncins-core-api",
    },
    "qa": {
        "operate": "http://operate.umpqak8sm1.moscow.alfaintra.net/",
        "kafka": (
            "https://akhq.umpqak8sm1.moscow.alfaintra.net/ui/ump-kafka-cluster/"
            "topic/ump.process.to.system/data?sort=Newest&partition=All"
        ),
        "corporate": {
            "token_url": (
                "http://corp-gateway-test.moscow.alfaintra.net/mks-gateway/public/auth/"
                "realms/corporate/protocol/openid-connect/token"
            ),
            "client_id": "nib-corp-ncinsurance",
            "client_secret": "nib_corp_ncinsurance",
            "base_url": (
                "http://corp-gateway-test.moscow.alfaintra.net/"
                "corp-ncins-gateway/secure/corp-ncins-corp-ncins-api"
            ),
            "paths": {
                "create": "/v1/applications",
                "list": "/v1/applications/list",
                "get": "/v1/applications/{id}",
            },
            "can_list": True,
            "verify_ssl": True,
        },
        "ump": {
            "token_url": (
                "https://keycloak.umpqak8sm1.moscow.alfaintra.net/"
                "realms/ump/protocol/openid-connect/token"
            ),
            "client_id": "nib-corp-ncins",
            "client_secret": "DRcjLK7ZeFSy4P0A7fPuZrD1ppXccxd0",
            "base_url": "http://ump-application-facade.umpqak8sm1.moscow.alfaintra.net",
            "base_url_candidates": [
                "http://ump-application-facade.umpqak8sm1.moscow.alfaintra.net",
                "https://ump-application-facade.umpqak8sm1.moscow.alfaintra.net",
                "http://umpqak8sm1.moscow.alfaintra.net/ump-application-facade",
                "https://umpqak8sm1.moscow.alfaintra.net/ump-application-facade",
            ],
            "paths": {
                "create": "/applications",
                "list": "/applications/list",
                "get": "/applications/{id}",
            },
            "can_list": True,
            "verify_ssl": False,
        },
        "sfa_base_url": "https://int.ufrulkint-api.moscow.alfaintra.net/ufr-eos-ul-ncins-core-api",
    },
    "test": {
        "operate": "http://operate.umpqak8sm1.moscow.alfaintra.net/",
        "kafka": (
            "https://akhq.umptech.moscow.alfaintra.net/ui/ump-kafka-cluster/"
            "topic/ump.process.to.system/data?sort=Newest&partition=All"
        ),
        "corporate": {
            # curl от лида
            "token_url": (
                "http://corp-gateway-test.moscow.alfaintra.net/mks-gateway/public/auth/"
                "realms/corporate/protocol/openid-connect/token"
            ),
            "client_id": "nib-corp-ncinsurance",
            "client_secret": "nib_corp_ncinsurance",
            "base_url": (
                "http://corp-gateway-test.moscow.alfaintra.net/"
                "corp-ncins-gateway/secure/corp-ncins-corp-ncins-api"
            ),
            "paths": {
                "create": "/v1/applications",
                "list": "/v1/applications/list",
                "get": "/v1/applications/{id}",
            },
            "can_list": True,
            "verify_ssl": True,
        },
        "ump": {
            # TEST: только POST /applications
            "token_url": (
                "https://idp-api-test.alfaintra.net/auth/realms/ump/"
                "protocol/openid-connect/token"
            ),
            "client_id": "nib-corp-ncins",
            "client_secret": "wcpWehuLXKRWwMYE17EXvg9ShCQ7Rovc",
            "base_url": "https://ump.alfabank.ru/ump-application-facade",
            "base_url_candidates": [
                "https://ump.alfabank.ru/ump-application-facade",
            ],
            "paths": {
                "create": "/applications",
                "list": "/applications/list",
                "get": "/applications/{id}",
            },
            "can_list": False,
            "verify_ssl": False,
        },
        "sfa_base_url": "https://int.ufrulkint-api.moscow.alfaintra.net/ufr-eos-ul-ncins-core-api",
    },
}


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def build_create_payload(channel: str) -> dict[str, Any]:
    """Полный payload, чтобы prepare смог собрать documents[] для AlfaCapture."""
    ext_id = str(uuid.uuid4())
    system_code = "NIB" if channel == "nib" else "SFA"
    doc_link = str(uuid.uuid4())
    dates = "2026-06-25T11:47:13.57Z"

    return {
        "externalApplicationIds": [
            {
                "extAppId": ext_id,
                "systemCode": system_code,
                "extAppCreateDate": now_iso(),
            }
        ],
        "participants": [
            {
                "pin": "AAAXYX",
                "type": "LEGAL",
                "fullName": "ООО Звезда",
                "inn": "7826688577",
                "ogrn": "1027810281740",
                "citizenship": "Россия",
                "citizenshipCode": "RU",
                "taxCountryCode": "RU",
                "contacts": [
                    {"type": "EMAIL", "value": "romashka@rambler.ru"},
                    {"type": "PHONE", "value": "79183221488"},
                ],
                "addresses": [
                    {
                        "type": "FACT",
                        "address": {"fullAddress": "Г. Пушкино ул. Колотушкина д. 14/88"},
                    }
                ],
            }
        ],
        "products": [
            {
                "code": "NON_CREDIT_INSURANCE",
                "type": "PRODUCT",
                "salesChannel": {"code": system_code},
                "productProperties": {
                    "code": "NON_CREDIT_INSURANCE",
                    "programId": 1073741824,
                    "signDate": dates,
                    "beginDate": dates,
                    "endDate": dates,
                    "duration": 12,
                    "paymentType": "payment_account",
                    "contractNumber": f"Z6922/888/PY{datetime.now().strftime('%H%M%S')}/6",
                    "insuranceSum": 20000.0,
                    "insurancePremium": 20000.0,
                    "agreementLink": doc_link,
                    "policyLink": doc_link,
                    "contractLink": doc_link,
                    "debitAccount": "123523464567347",
                    "insuranceObjects": [{"paymentAccount": "123523464567347"}],
                },
            }
        ],
    }


def get_token(
    token_url: str,
    client_id: str,
    client_secret: str,
    verify_ssl: bool = True,
    timeout: int = 60,
) -> str:
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    resp = requests.post(
        token_url,
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=timeout,
        verify=verify_ssl,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"TOKEN {resp.status_code}: {resp.text[:1000]}")
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError(f"В ответе token нет access_token: {resp.text[:500]}")
    return token


def default_headers(token: str, channel: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "A-userId": "123456",
        "A-customerId": "U_M2XNX",
        "A-clientType": "UL",
        "A-channelId": "NIB" if channel == "nib" else "SFA",
    }


def create_application(
    base_url: str,
    create_path: str,
    headers: dict[str, str],
    payload: dict[str, Any],
    verify_ssl: bool = True,
    timeout: int = 120,
) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}{create_path}"
    params = {"finalVersion": "true", "fullCreate": "true"}
    resp = requests.post(
        url, headers=headers, params=params, json=payload, timeout=timeout, verify=verify_ssl
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"CREATE {resp.status_code}: {resp.text[:2000]}")
    return resp.json()


def list_applications(
    base_url: str,
    list_path: str,
    headers: dict[str, str],
    body: dict[str, Any],
    *,
    verify_ssl: bool = True,
    timeout: int = 60,
) -> tuple[dict[str, Any] | None, str]:
    """POST /applications/list."""
    url = f"{base_url.rstrip('/')}{list_path}"
    resp = requests.post(
        url,
        headers=headers,
        params={"limit": 10, "offset": 0},
        json=body,
        timeout=timeout,
        verify=verify_ssl,
    )
    detail = f"{resp.status_code} {url} body={json.dumps(body, ensure_ascii=False)}"
    if resp.status_code >= 400:
        return None, f"{detail} -> {resp.text[:400]}"
    return resp.json(), f"{detail} OK"


def corp_list_bodies(app_id: str, number: str | None) -> list[dict[str, Any]]:
    """
    corp-ncins DTO = GetApplicationsUmpPostRequestDto — НЕ facade {filter:{...}}.
    Пробуем типичные варианты, пока не найдём рабочий / не пришлёшь schema из Postman.
    """
    bodies: list[dict[str, Any]] = []
    if app_id:
        bodies.extend(
            [
                {"ids": [app_id]},
                {"applicationIds": [app_id]},
                {"id": app_id},
                {"applicationId": app_id},
            ]
        )
    if number:
        bodies.extend(
            [
                {"numbers": [number]},
                {"number": number},
                {"applicationNumber": number},
                {"applicationNumbers": [number]},
            ]
        )
    bodies.append({})
    return bodies


def ump_list_bodies(number: str | None) -> list[dict[str, Any]]:
    """Контракт ump-application-facade: filter.applicationFilter."""
    bodies: list[dict[str, Any]] = []
    if number:
        bodies.append({"filter": {"applicationFilter": {"number": number}}})
    bodies.append(
        {"filter": {"applicationFilter": {"pin": "AAAXYX", "inn": "7826688577"}}}
    )
    return bodies


def pick_from_list_response(data: dict[str, Any], app_id: str) -> dict[str, Any] | None:
    items = data.get("list") or data.get("applications") or data.get("content") or []
    if isinstance(data, list):
        items = data
    if not isinstance(items, list):
        return None
    for item in items:
        if isinstance(item, dict) and item.get("id") == app_id:
            return item
    if items and isinstance(items[0], dict):
        return items[0]
    return None


def try_read_application(
    auth_cfg: dict[str, Any],
    headers: dict[str, str],
    *,
    auth_mode: str,
    app_id: str,
    number: str | None,
) -> tuple[dict[str, Any] | None, list[str]]:
    """
    NIB: GET /applications/{id} не в правах → 403.
    Читаем через POST /applications/list (схема тела зависит от auth).
    """
    attempts: list[str] = []
    base = auth_cfg["base_url"]
    paths = auth_cfg["paths"]
    verify = auth_cfg.get("verify_ssl", True)

    if not auth_cfg.get("can_list", True):
        attempts.append(
            "list недоступен для этого стенда/клиента (на TEST у nib-corp-ncins только POST /applications)"
        )
        return None, attempts

    list_path = paths["list"]
    bodies = (
        corp_list_bodies(app_id, number)
        if auth_mode == "corporate"
        else ump_list_bodies(number)
    )

    for body in bodies:
        data, msg = list_applications(
            base, list_path, headers, body, verify_ssl=verify
        )
        attempts.append(f"LIST {msg}")
        if data is None:
            # невалидное поле — пробуем следующий body
            continue
        found = pick_from_list_response(data, app_id)
        if found:
            return found, attempts

    # GET только для диагностики — ожидаем 403, не считаем ошибкой скрипта
    get_path = paths.get("get", "/v1/applications/{id}").format(id=app_id)
    get_url = f"{base.rstrip('/')}{get_path}"
    try:
        resp = requests.get(get_url, headers=headers, timeout=30, verify=verify)
        attempts.append(
            f"GET {resp.status_code} {get_url} "
            f"(ожидаемо 403 у NIB) -> {resp.text[:160]}"
        )
    except requests.RequestException as exc:
        attempts.append(f"GET network error: {exc}")

    return None, attempts


def extract_channel_codes(app: dict[str, Any]) -> list[str]:
    return [c.get("code") for c in app.get("channels") or [] if c.get("code")]


def summarize_app(app: dict[str, Any]) -> dict[str, Any]:
    products_summary = []
    for p in app.get("products") or []:
        props = p.get("productProperties")
        products_summary.append(
            {
                "id": p.get("id"),
                "code": p.get("code"),
                "status": (p.get("status") or {}).get("code"),
                "hasProductProperties": bool(props),
                "productPropertiesKeys": sorted(props.keys()) if isinstance(props, dict) else [],
            }
        )

    participants_summary = []
    for part in app.get("participants") or []:
        contacts = part.get("contacts") or []
        emails = [c.get("value") for c in contacts if c.get("type") == "EMAIL"]
        participants_summary.append(
            {
                "pin": part.get("pin"),
                "hasContacts": bool(contacts),
                "emails": emails,
            }
        )

    return {
        "id": app.get("id"),
        "number": app.get("number"),
        "statusCode": app.get("statusCode"),
        "finalVersion": app.get("finalVersion"),
        "channels": extract_channel_codes(app),
        "participants": participants_summary,
        "products": products_summary,
        "note": (
            "list возвращает урезанный ApplicationMain — "
            "productProperties/contacts могут отсутствовать даже если они есть в UMP"
        ),
    }


def warn_incomplete_create(app: dict[str, Any]) -> None:
    summary = summarize_app(app)
    problems: list[str] = []

    if summary.get("statusCode") == "DRAFT":
        problems.append(
            "statusCode=DRAFT (в create-ответе так бывает даже при finalVersion=true; "
            "смотри процессы в Operate)"
        )

    for p in summary.get("products") or []:
        if not p.get("hasProductProperties"):
            problems.append(
                "в ответе нет products[].productProperties — "
                "list/create могут не отдавать их; проверь prepare в Operate"
            )

    for part in summary.get("participants") or []:
        if not part.get("emails"):
            problems.append(
                "в ответе нет EMAIL — list часто без contacts; "
                "если prepare падает на email, данные реально не сохранились"
            )

    if not problems:
        print("\nOK: в ответе есть полезные поля для проверки.")
        return

    print("\n=== WARN по create/list ответу ===")
    for item in problems:
        print(f"- {item}")


def print_json(title: str, data: Any) -> None:
    print(f"\n=== {title} ===")
    print(json.dumps(data, ensure_ascii=False, indent=2))


def print_operate_checklist(app_id: str, channel: str, env_cfg: dict[str, Any]) -> None:
    expected_channel = "nib-corp-ncins" if channel == "nib" else "sfa-ncins"
    print(
        f"""
=== Что смотреть в Operate ===
Operate: {env_cfg['operate']}
Kafka:   {env_cfg['kafka']}

businessKey / application id: {app_id}
Ожидаемый channels[].code: {expected_channel}

Порядок процессов:
1) ump-app-reg-pa
2) ump-main-ma-ncins-pa
3) ump-prepare-documents-ncins-pa  -> docsResult / acDocuments
4) ump-generate-and-save-document-pa  <-- NCINS-143
5) ump-signing-documents-ncins-pa
6) ump-payment-ncins-pa
7) ump-finalisation-ncins-pa
"""
    )


def print_auth_help() -> None:
    print(
        """
=== Авторизация NIB (не смешивай токен и API) ===
corporate JWT  →  corp-gateway /v1/applications
  --auth corporate  client_id=nib-corp-ncinsurance  (curl от лида)
  Это рабочий путь для создания заявки через NIB proxy.

UMP Keycloak JWT  →  ump-application-facade /applications
  --auth ump  client_id=nib-corp-ncins
  DEV/QA: POST /applications + POST /applications/list
  TEST: только POST /applications
  НЕ слать UMP-токен в corp-gateway → 401 Jwt issuer is not configured

GET /applications/{id} у NIB нет → 403 RBAC (ожидаемо). Читай через list.
"""
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="NCINS UMP create + list + Camunda helpers")
    parser.add_argument("--env", choices=["dev", "qa", "test"], default="dev")
    parser.add_argument(
        "--auth",
        choices=["ump", "corporate"],
        default="corporate",
        help="corporate = curl лида → corp-gateway (по умолчанию); ump = Keycloak UMP → facade",
    )
    parser.add_argument("--channel", choices=["nib", "sfa"], default="nib")
    parser.add_argument("--client-id", default="", help="Override client_id")
    parser.add_argument("--client-secret", default="", help="Override client_secret")
    parser.add_argument("--token-url", default="")
    parser.add_argument("--base-url", default="", help="Override API base URL")
    parser.add_argument("--token", default="", help="Готовый Bearer token")
    parser.add_argument("--skip-create", action="store_true")
    parser.add_argument(
        "--skip-get",
        action="store_true",
        help="Не читать заявку (ни list, ни get)",
    )
    parser.add_argument("--app-id", default="", help="UUID заявки, если --skip-create")
    parser.add_argument("--app-number", default="", help="Номер заявки для list, напр. UMP26080447600")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    env_cfg = ENVIRONMENTS[args.env]
    auth_cfg = dict(env_cfg[args.auth])
    if args.base_url:
        auth_cfg["base_url"] = args.base_url
    if args.token_url:
        auth_cfg["token_url"] = args.token_url
    if args.client_id:
        auth_cfg["client_id"] = args.client_id
    if args.client_secret:
        auth_cfg["client_secret"] = args.client_secret

    if args.channel == "sfa" and not args.base_url:
        auth_cfg["base_url"] = env_cfg["sfa_base_url"]

    print_auth_help()
    print(
        f"env={args.env} auth={args.auth} "
        f"client_id={auth_cfg['client_id']} base={auth_cfg['base_url']}"
    )

    payload = build_create_payload(args.channel)
    print_json("CREATE payload", payload)

    prepare_start = {
        "businessKey": args.app_id or "<application-id>",
        "productCode": "NON_CREDIT_INSURANCE",
    }
    generate_start = {
        "serviceId": args.app_id or "<application-id>",
        "serviceCode": "APPLICATION",
        "productCode": "NON_CREDIT_INSURANCE",
        "documents": [
            {
                "documentType": "CONTRACT_ACCOUNT_BLOCK",
                "reportData": (
                    "PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiPz48ZGF0YXNvdXJjZT4"
                    "PGNvbnRyYWN0TnVtYmVyPnRlc3Q8L2NvbnRyYWN0TnVtYmVyPjwvZGF0YXNvdXJjZT4="
                ),
                "isWriteInEa": False,
            }
        ],
    }
    print_json("Start JSON для ump-prepare-documents-ncins-pa", prepare_start)
    print_json("Start JSON для ump-generate-and-save-document-pa", generate_start)

    if args.dry_run:
        print("\nDry-run: запросы в API не отправлялись.")
        return 0

    verify = auth_cfg.get("verify_ssl", True)
    if args.token:
        token = args.token
    else:
        print(f"\nПолучаю token ({args.auth}): {auth_cfg['token_url']}")
        print(f"client_id={auth_cfg['client_id']}")
        token = get_token(
            auth_cfg["token_url"],
            auth_cfg["client_id"],
            auth_cfg["client_secret"],
            verify_ssl=verify,
        )

    headers = default_headers(token, args.channel)
    app: dict[str, Any]
    app_number = args.app_number or None

    if args.skip_create:
        if not args.app_id:
            print("--skip-create требует --app-id", file=sys.stderr)
            return 2
        app_id = args.app_id
        print(f"\nПропускаю create, app_id={app_id}")
        app = {"id": app_id, "number": app_number}
    else:
        create_path = auth_cfg["paths"]["create"]
        bases: list[str] = []
        if args.base_url:
            bases = [args.base_url]
        else:
            bases = [auth_cfg["base_url"]]
            for cand in auth_cfg.get("base_url_candidates") or []:
                if cand not in bases:
                    bases.append(cand)

        created: dict[str, Any] | None = None
        last_error: Exception | None = None
        for base in bases:
            auth_cfg["base_url"] = base
            print(
                f"\nCREATE {base}{create_path}?finalVersion=true&fullCreate=true"
            )
            try:
                created = create_application(
                    base,
                    create_path,
                    headers,
                    payload,
                    verify_ssl=verify,
                )
                break
            except RuntimeError as exc:
                last_error = exc
                err = str(exc)
                print(f"  -> {exc}")
                if "Jwt issuer is not configured" in err:
                    print(
                        "  Подсказка: UMP JWT нельзя слать в corp-gateway. "
                        "Для corp-gateway используй: --auth corporate\n"
                        "  Для UMP JWT нужен host ump-application-facade "
                        "(или правильный --base-url из Postman)."
                    )
                continue
            except requests.RequestException as exc:
                last_error = RuntimeError(f"network: {exc}")
                print(f"  -> network error: {exc}")
                continue

        if created is None:
            raise RuntimeError(
                f"CREATE не удался на всех base_url. Последняя ошибка: {last_error}\n"
                f"Рабочий вариант: python ump.py --env {args.env} --auth corporate --channel nib"
            )

        print_json("CREATE response", created)
        app_id = created.get("id")
        app_number = created.get("number") or app_number
        if not app_id:
            print("В ответе create нет id", file=sys.stderr)
            return 1
        app = created
        warn_incomplete_create(created)

    if not args.skip_get:
        print(
            f"\nЧитаю заявку через POST /applications/list "
            f"(GET /{{id}} у NIB нет в правах)"
        )
        fetched, attempts = try_read_application(
            auth_cfg,
            headers,
            auth_mode=args.auth,
            app_id=app_id,
            number=app_number,
        )
        for line in attempts:
            print(f"  try: {line}")
        if fetched is None:
            print(
                "\nlist/get не дали полную заявку. Это не блокер: "
                "бери application id из CREATE и смотри Operate."
            )
        else:
            app = fetched
            print_json("LIST/read (short checks)", summarize_app(app))
            warn_incomplete_create(app)
    else:
        print("\n--skip-get: чтение заявки пропущено")

    prepare_start["businessKey"] = app_id
    generate_start["serviceId"] = app_id
    print_json("Start JSON prepare (готово)", prepare_start)
    print_json("Start JSON generate (готово)", generate_start)

    channels = extract_channel_codes(app if isinstance(app, dict) else {})
    expected = "nib-corp-ncins" if args.channel == "nib" else "sfa-ncins"
    if channels:
        if expected in channels:
            print(f"OK channel: {expected}")
        else:
            print(f"WARN channel: ожидали {expected}, получили {channels}")

    print_operate_checklist(app_id, args.channel, env_cfg)
    print(
        f"\nГотово. application id = {app_id}"
        + (f", number = {app_number}" if app_number else "")
        + f"\nOperate businessKey={app_id}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print(f"\nERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
