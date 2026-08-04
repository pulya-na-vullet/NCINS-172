#!/usr/bin/env python3
"""
NCINS / UMP E2E helper:
1) получить token
2) создать мультизаявку с полным payload (чтобы не было documents=null)
3) прочитать заявку
4) напечатать JSON для ручного Start процессов в Camunda

Примеры:
  python test_ump_ncins_flow.py --env dev --channel nib
  python test_ump_ncins_flow.py --env test --channel sfa
  python test_ump_ncins_flow.py --env test --channel nib --skip-create --app-id <uuid>
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import datetime, timezone
from typing import Any

import requests

# --- стенды из комментариев к NCINS-143 ---
ENVIRONMENTS: dict[str, dict[str, str]] = {
    "dev": {
        "nib_base_url": (
            "http://corp-gateway-dev.moscow.alfaintra.net/"
            "corp-ncins-acc-gateway/secure/corp-ncins-acc-corp-ncins-acc-api"
        ),
        # если create идёт через corp-ncins API / proxy — поправь URL под свой Postman
        "create_base_url": (
            "http://corp-gateway-dev.moscow.alfaintra.net/"
            "corp-ncins-gateway/secure/corp-ncins-corp-ncins-api"
        ),
        "sfa_base_url": "https://dev.ufrulkint-api.moscow.alfaintra.net/ufr-eos-ul-ncins-core-api",
        "operate": "http://operate.umpqak8sm1.moscow.alfaintra.net/",
        "kafka": (
            "https://akhq.umpqak8sm1.moscow.alfaintra.net/ui/ump-kafka-cluster/"
            "topic/ump.process.to.system/data?sort=Newest&partition=All"
        ),
        "token_url": (
            "http://corp-gateway-dev.moscow.alfaintra.net/mks-gateway/public/auth/"
            "realms/corporate/protocol/openid-connect/token"
        ),
    },
    "test": {
        "nib_base_url": (
            "http://corp-gateway-test.moscow.alfaintra.net/"
            "corp-ncins-acc-gateway/secure/corp-ncins-acc-corp-ncins-acc-api"
        ),
        "create_base_url": (
            "http://corp-gateway-test.moscow.alfaintra.net/"
            "corp-ncins-gateway/secure/corp-ncins-corp-ncins-api"
        ),
        "sfa_base_url": "https://int.ufrulkint-api.moscow.alfaintra.net/ufr-eos-ul-ncins-core-api",
        "operate": "http://operate.umpqak8sm1.moscow.alfaintra.net/",  # уточни INT operate при необходимости
        "kafka": (
            "https://akhq.umptech.moscow.alfaintra.net/ui/ump-kafka-cluster/"
            "topic/ump.process.to.system/data?sort=Newest&partition=All"
        ),
        "token_url": (
            "http://corp-gateway-test.moscow.alfaintra.net/mks-gateway/public/auth/"
            "realms/corporate/protocol/openid-connect/token"
        ),
    },
}


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def build_create_payload(channel: str) -> dict[str, Any]:
    """Полный payload, чтобы prepare смог собрать documents[] для AlfaCapture."""
    ext_id = str(uuid.uuid4())
    system_code = "NIB" if channel == "nib" else "SFA"
    sales_channel = system_code

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
                "contacts": [
                    {"type": "EMAIL", "value": "romashka@rambler.ru"},
                    {"type": "PHONE", "value": "+79183221488"},
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
                "salesChannel": {"code": sales_channel},
                "productProperties": {
                    "code": "NON_CREDIT_INSURANCE",
                    "programId": 1073741824,
                    "signDate": "2026-06-25T11:47:13.570Z",
                    "beginDate": "2026-06-25T11:47:13.570Z",
                    "endDate": "2026-06-25T11:47:13.570Z",
                    "duration": 12,
                    "paymentType": "payment_account",
                    "contractNumber": f"Z6922/888/PY{datetime.now().strftime('%H%M%S')}/6",
                    "insuranceSum": 20000.0,
                    "insurancePremium": 20000.0,
                    "agreementLink": str(uuid.uuid4()),
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
    timeout: int = 60,
) -> str:
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    resp = requests.post(token_url, data=data, timeout=timeout)
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError(f"В ответе token нет access_token: {resp.text[:500]}")
    return token


def default_headers(token: str, channel: str) -> dict[str, str]:
    # заголовки как в Postman NIB; для SFA при необходимости поправь
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
    headers: dict[str, str],
    payload: dict[str, Any],
    timeout: int = 120,
) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}/v1/applications"
    params = {"finalVersion": "true", "fullCreate": "true"}
    resp = requests.post(url, headers=headers, params=params, json=payload, timeout=timeout)
    if resp.status_code >= 400:
        raise RuntimeError(f"CREATE {resp.status_code}: {resp.text[:2000]}")
    return resp.json()


def get_application(
    base_url: str,
    headers: dict[str, str],
    app_id: str,
    timeout: int = 60,
) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}/v1/applications/{app_id}"
    resp = requests.get(url, headers=headers, timeout=timeout)
    if resp.status_code >= 400:
        raise RuntimeError(f"GET {resp.status_code}: {resp.text[:2000]}")
    return resp.json()


def extract_channel_codes(app: dict[str, Any]) -> list[str]:
    return [c.get("code") for c in app.get("channels") or [] if c.get("code")]


def print_json(title: str, data: Any) -> None:
    print(f"\n=== {title} ===")
    print(json.dumps(data, ensure_ascii=False, indent=2))


def print_operate_checklist(app_id: str, channel: str, env_cfg: dict[str, str]) -> None:
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
   имя: Регистрация заявки
   check: дедуп = Нет / isDeduplicatedTech=false

2) ump-main-ma-ncins-pa
   имя: Управление мультизаявкой по некредитному страхованию

3) ump-prepare-documents-ncins-pa
   имя: Процесс подготовки документов
   variables:
     docsResult=SUCCESS
     acRequestId=...
     acDocuments=[{{"acId":"...","type":"CONTRACT_ACCOUNT_BLOCK"}}]

4) ump-generate-and-save-document-pa
   имя: Формирование и сохранение документа в AlfaCapture
   (child от prepare)  <-- NCINS-143

5) ump-signing-documents-ncins-pa
6) ump-payment-ncins-pa
7) ump-finalisation-ncins-pa  -> POST /v1/ins-contracts
   затем 3x Сохранить документы в ЭА
"""
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="NCINS UMP create + helpers for Camunda checks")
    parser.add_argument("--env", choices=["dev", "test"], default="dev")
    parser.add_argument("--channel", choices=["nib", "sfa"], default="nib")
    parser.add_argument("--client-id", default="nib-corp-ncinsurance")
    parser.add_argument("--client-secret", default="nib_corp_ncinsurance")
    parser.add_argument("--token-url", default="")
    parser.add_argument("--base-url", default="", help="Override create API base URL")
    parser.add_argument("--token", default="", help="Готовый Bearer token (тогда token-url не нужен)")
    parser.add_argument("--skip-create", action="store_true")
    parser.add_argument("--app-id", default="", help="UUID заявки, если --skip-create")
    parser.add_argument("--dry-run", action="store_true", help="Только напечатать payload/start JSON")
    args = parser.parse_args()

    env_cfg = ENVIRONMENTS[args.env]
    base_url = args.base_url or env_cfg["create_base_url"]
    if args.channel == "sfa" and not args.base_url:
        base_url = env_cfg["sfa_base_url"]

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
                # минимальный xml в base64; в реальном флоу его собирает prepare
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

    if args.token:
        token = args.token
    else:
        token_url = args.token_url or env_cfg["token_url"]
        print(f"\nПолучаю token: {token_url}")
        token = get_token(token_url, args.client_id, args.client_secret)

    headers = default_headers(token, args.channel)

    if args.skip_create:
        if not args.app_id:
            print("--skip-create требует --app-id", file=sys.stderr)
            return 2
        app_id = args.app_id
        print(f"\nПропускаю create, app_id={app_id}")
    else:
        print(f"\nCREATE {base_url}/v1/applications?finalVersion=true&fullCreate=true")
        created = create_application(base_url, headers, payload)
        print_json("CREATE response", created)
        app_id = created.get("id")
        if not app_id:
            print("В ответе create нет id", file=sys.stderr)
            return 1

    print(f"\nGET application {app_id}")
    app = get_application(base_url, headers, app_id)
    print_json("GET response (short checks)", {
        "id": app.get("id"),
        "statusCode": app.get("statusCode"),
        "finalVersion": app.get("finalVersion"),
        "channels": extract_channel_codes(app),
        "products": [
            {
                "id": p.get("id"),
                "code": p.get("code"),
                "status": (p.get("status") or {}).get("code"),
                "hasProductProperties": bool(p.get("productProperties")),
            }
            for p in (app.get("products") or [])
        ],
    })

    # обновим start JSON реальным id
    prepare_start["businessKey"] = app_id
    generate_start["serviceId"] = app_id
    print_json("Start JSON prepare (готово)", prepare_start)
    print_json("Start JSON generate (готово)", generate_start)

    channels = extract_channel_codes(app)
    expected = "nib-corp-ncins" if args.channel == "nib" else "sfa-ncins"
    if expected in channels:
        print(f"OK channel: {expected}")
    else:
        print(f"WARN channel: ожидали {expected}, получили {channels}")

    print_operate_checklist(app_id, args.channel, env_cfg)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print(f"\nERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
