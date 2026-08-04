#!/usr/bin/env python3
"""
NCINS / UMP E2E helper для NIB.

Два способа авторизации:
  1) --auth ump        Keycloak UMP, client_id=nib-corp-ncins
                       доступы: POST /applications, POST /applications/list
                       (на TEST list может быть закрыт)
  2) --auth corporate  Keycloak corporate (curl от лида),
                       client_id=nib-corp-ncinsurance → corp-gateway

GET /applications/{id} у NIB НЕТ в правах → будет 403 RBAC.
Читаем заявку через POST /applications/list (фильтр по number / pin / inn).

Примеры:
  python test_ump_ncins_flow.py --env dev --auth ump --channel nib
  python test_ump_ncins_flow.py --env qa --auth ump --channel nib
  python test_ump_ncins_flow.py --env test --auth corporate --channel nib
  python test_ump_ncins_flow.py --env dev --auth ump --skip-get
  python test_ump_ncins_flow.py --dry-run
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
            # НИБ пользователи Keycloak в UMP / DEV
            "token_url": (
                "https://keycloak.umpdevwk8sm1.moscow.alfaintra.net/"
                "realms/ump/protocol/openid-connect/token"
            ),
            "client_id": "nib-corp-ncins",
            "client_secret": "OlcnSVnz3UiORtl4XfJZ3NRRZlqw7QPY",
            # тот же corp-proxy; при необходимости --base-url на facade
            "base_url": (
                "http://corp-gateway-dev.moscow.alfaintra.net/"
                "corp-ncins-gateway/secure/corp-ncins-corp-ncins-api"
            ),
            "paths": {
                "create": "/v1/applications",
                "list": "/v1/applications/list",
                "get": "/v1/applications/{id}",
                # fallback на контракт facade без /v1
                "create_alt": "/applications",
                "list_alt": "/applications/list",
            },
            "can_list": True,
            "verify_ssl": False,  # curl -k
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
            # НИБ пользователи Keycloak в UMP / QA
            "token_url": (
                "https://keycloak.umpqak8sm1.moscow.alfaintra.net/"
                "realms/ump/protocol/openid-connect/token"
            ),
            "client_id": "nib-corp-ncins",
            "client_secret": "DRcjLK7ZeFSy4P0A7fPuZrD1ppXccxd0",
            "base_url": (
                "http://corp-gateway-test.moscow.alfaintra.net/"
                "corp-ncins-gateway/secure/corp-ncins-corp-ncins-api"
            ),
            "paths": {
                "create": "/v1/applications",
                "list": "/v1/applications/list",
                "get": "/v1/applications/{id}",
                "create_alt": "/applications",
                "list_alt": "/applications/list",
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
            # НИБ пользователи Keycloak в UMP / TEST — только POST /applications
            "token_url": (
                "https://idp-api-test.alfaintra.net/auth/realms/ump/"
                "protocol/openid-connect/token"
            ),
            "client_id": "nib-corp-ncins",
            "client_secret": "wcpWehuLXKRWwMYE17EXvg9ShCQ7Rovc",
            "base_url": (
                "http://corp-gateway-test.moscow.alfaintra.net/"
                "corp-ncins-gateway/secure/corp-ncins-corp-ncins-api"
            ),
            "paths": {
                "create": "/v1/applications",
                "list": "/v1/applications/list",
                "get": "/v1/applications/{id}",
                "create_alt": "/applications",
                "list_alt": "/applications/list",
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
    *,
    number: str | None = None,
    pin: str | None = None,
    inn: str | None = None,
    verify_ssl: bool = True,
    timeout: int = 60,
) -> tuple[dict[str, Any] | None, str]:
    """POST /applications/list — единственный разрешённый read для NIB на DEV/QA."""
    url = f"{base_url.rstrip('/')}{list_path}"
    application_filter: dict[str, Any] = {}
    if number:
        application_filter["number"] = number
    if pin:
        application_filter["pin"] = pin
    if inn:
        application_filter["inn"] = inn
    body = {"filter": {"applicationFilter": application_filter}} if application_filter else {}

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
        return None, f"{detail} -> {resp.text[:500]}"
    return resp.json(), f"{detail} OK"


def try_read_application(
    auth_cfg: dict[str, Any],
    headers: dict[str, str],
    *,
    app_id: str,
    number: str | None,
) -> tuple[dict[str, Any] | None, list[str]]:
    """
    NIB: GET /applications/{id} не в правах → 403.
    Читаем через POST /applications/list.
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

    list_paths = [paths["list"]]
    if paths.get("list_alt"):
        list_paths.append(paths["list_alt"])

    # сначала по number (из create), потом по pin+inn
    filters: list[dict[str, str | None]] = []
    if number:
        filters.append({"number": number, "pin": None, "inn": None})
    filters.append({"number": None, "pin": "AAAXYX", "inn": "7826688577"})

    for list_path in list_paths:
        for flt in filters:
            data, msg = list_applications(
                base,
                list_path,
                headers,
                number=flt["number"],
                pin=flt["pin"],
                inn=flt["inn"],
                verify_ssl=verify,
            )
            attempts.append(f"LIST {msg}")
            if data is None:
                continue
            items = data.get("list") or []
            # предпочитаем точное совпадение по id
            for item in items:
                if item.get("id") == app_id:
                    return item, attempts
            if items:
                return items[0], attempts

    # GET только для диагностики — ожидаем 403
    get_path = paths.get("get", "/v1/applications/{id}").format(id=app_id)
    get_url = f"{base.rstrip('/')}{get_path}"
    try:
        resp = requests.get(get_url, headers=headers, timeout=30, verify=verify)
        attempts.append(
            f"GET {resp.status_code} {get_url} "
            f"(ожидаемо 403: у NIB нет GET /applications/{{id}}) -> {resp.text[:200]}"
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
=== Авторизация NIB ===
UMP Keycloak (рекомендуется для UMP API):
  DEV  client_id=nib-corp-ncins  -> POST /applications, POST /applications/list
  QA   client_id=nib-corp-ncins  -> POST /applications, POST /applications/list
  TEST client_id=nib-corp-ncins  -> POST /applications  (list НЕТ)

Corporate (curl от лида) — для corp-gateway:
  client_id=nib-corp-ncinsurance / client_secret=nib_corp_ncinsurance

GET /applications/{id} у NIB нет в правах → 403 RBAC — это ожидаемо.
"""
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="NCINS UMP create + list + Camunda helpers")
    parser.add_argument("--env", choices=["dev", "qa", "test"], default="dev")
    parser.add_argument(
        "--auth",
        choices=["ump", "corporate"],
        default="ump",
        help="ump = Keycloak UMP nib-corp-ncins; corporate = curl от лида",
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
        print(
            f"\nCREATE {auth_cfg['base_url']}{create_path}"
            f"?finalVersion=true&fullCreate=true"
        )
        try:
            created = create_application(
                auth_cfg["base_url"],
                create_path,
                headers,
                payload,
                verify_ssl=verify,
            )
        except RuntimeError as exc:
            # fallback на /applications без /v1 (контракт facade)
            alt = auth_cfg["paths"].get("create_alt")
            if not alt:
                raise
            print(f"CREATE /v1 не прошёл ({exc}); пробую {alt}")
            created = create_application(
                auth_cfg["base_url"],
                alt,
                headers,
                payload,
                verify_ssl=verify,
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
