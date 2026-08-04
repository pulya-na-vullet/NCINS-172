#!/usr/bin/env python3
"""
NCINS-143: автопроверка флоу UMP (НИБ).

ДВА РАЗНЫХ ТОКЕНА — НЕ СМЕШИВАТЬ:

  A) Keycloak UMP (прислал лид Яковлев)  →  ump-application-facade
     client_id = nib-corp-ncins
     POST /applications  (+ /applications/list на DEV/QA)

  B) Corporate Keycloak (прислал разработчик)  →  corp-gateway НИБ
     client_id = nib-corp-ncinsurance
     POST /v1/applications

Если UMP-токен слать в corp-gateway → 401 Jwt issuer is not configured.

Примеры (для NCINS-143 / НИБ create — corporate):
  python ump.py --env dev --auth corporate --channel nib

  # токен лида (UMP) — только если знаешь рабочий --base-url facade из Postman
  python ump.py --env dev --auth ump --channel nib --base-url https://.../ump-application-facade

  python ump.py --env dev --auth ump --token-only
  python ump.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------------------------------------------------------------------------
# Конфиги стендов
# ---------------------------------------------------------------------------

ENVIRONMENTS: dict[str, dict[str, Any]] = {
    "dev": {
        # corp-gateway-dev процессы обычно на umpdev; в задаче также указан umpqa — проверь оба
        "operate": "http://operate.umpdevwk8sm1.moscow.alfaintra.net/",
        "operate_alt": "http://operate.umpqak8sm1.moscow.alfaintra.net/",
        "kafka": (
            "https://akhq.umpdevwk8sm1.moscow.alfaintra.net/ui/ump-kafka-cluster/"
            "topic/ump.process.to.system/data?sort=Newest&partition=All"
        ),
        "kafka_alt": (
            "https://akhq.umpqak8sm1.moscow.alfaintra.net/ui/ump-kafka-cluster/"
            "topic/ump.process.to.system/data?sort=Newest&partition=All"
        ),
        # B) токен разработчика → corp-gateway
        "corporate": {
            "label": "Corporate Keycloak (curl разработчика) → corp-gateway",
            "token_url": (
                "http://corp-gateway-dev.moscow.alfaintra.net/mks-gateway/public/auth/"
                "realms/corporate/protocol/openid-connect/token"
            ),
            "client_id": "nib-corp-ncinsurance",
            "client_secret": "nib_corp_ncinsurance",
            # проверено 2026-08-04: corp-ncins-gateway OK; *-acc-* даёт 403 RBAC
            "base_url": (
                "http://corp-gateway-dev.moscow.alfaintra.net/"
                "corp-ncins-gateway/secure/corp-ncins-corp-ncins-api"
            ),
            "base_url_candidates": [
                (
                    "http://corp-gateway-dev.moscow.alfaintra.net/"
                    "corp-ncins-gateway/secure/corp-ncins-corp-ncins-api"
                ),
                (
                    "http://corp-gateway-dev.moscow.alfaintra.net/"
                    "corp-ncins-acc-gateway/secure/corp-ncins-acc-corp-ncins-acc-api"
                ),
            ],
            "paths": {
                "create": "/v1/applications",
                "list": "/v1/applications/list",
                "get": "/v1/applications/{id}",
                "contracts": "/v1/contracts",
            },
            # GET /v1/contracts — Учёт (acc). Гурин: обязательно после завершения процессов UMP
            "contract_base_url_candidates": [
                (
                    "http://corp-gateway-dev.moscow.alfaintra.net/"
                    "corp-ncins-acc-gateway/secure/corp-ncins-acc-corp-ncins-acc-api"
                ),
                (
                    "http://corp-gateway-dev.moscow.alfaintra.net/"
                    "corp-ncins-gateway/secure/corp-ncins-corp-ncins-api"
                ),
            ],
            "can_list": True,
            "verify_ssl": True,
        },
        # A) токен лида → UMP facade (host снаружи часто 404 — нужен URL из Postman)
        "ump": {
            "label": "Keycloak UMP (curl лида) → ump-application-facade",
            "token_url": (
                "https://keycloak.umpdevwk8sm1.moscow.alfaintra.net/"
                "realms/ump/protocol/openid-connect/token"
            ),
            "client_id": "nib-corp-ncins",
            "client_secret": "OlcnSVnz3UiORtl4XfJZ3NRRZlqw7QPY",
            # OpenAPI default + k8s-style hosts; при 404 передай --base-url из Postman
            "base_url": "https://ump.alfabank.ru/ump-application-facade",
            "base_url_candidates": [
                "https://ump.alfabank.ru/ump-application-facade",
                "http://ump-application-facade.umpdevwk8sm1.moscow.alfaintra.net",
                "https://ump-application-facade.umpdevwk8sm1.moscow.alfaintra.net",
                "http://ump-application-facade.umpqak8sm1.moscow.alfaintra.net",
                "https://ump-application-facade.umpqak8sm1.moscow.alfaintra.net",
                "http://umpdevwk8sm1.moscow.alfaintra.net/ump-application-facade",
                "https://umpdevwk8sm1.moscow.alfaintra.net/ump-application-facade",
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
        "sfa_base_url": "https://dev.ufrulkint-api.moscow.alfaintra.net/ufr-eos-ul-ncins-core-api",
    },
    "qa": {
        "operate": "http://operate.umpqak8sm1.moscow.alfaintra.net/",
        "kafka": (
            "https://akhq.umpqak8sm1.moscow.alfaintra.net/ui/ump-kafka-cluster/"
            "topic/ump.process.to.system/data?sort=Newest&partition=All"
        ),
        "corporate": {
            "label": "Corporate Keycloak → corp-gateway (QA/INT)",
            "token_url": (
                "http://corp-gateway-test.moscow.alfaintra.net/mks-gateway/public/auth/"
                "realms/corporate/protocol/openid-connect/token"
            ),
            "client_id": "nib-corp-ncinsurance",
            "client_secret": "nib_corp_ncinsurance",
            "base_url": (
                "http://corp-gateway-test.moscow.alfaintra.net/"
                "corp-ncins-acc-gateway/secure/corp-ncins-acc-corp-ncins-acc-api"
            ),
            "base_url_candidates": [
                (
                    "http://corp-gateway-test.moscow.alfaintra.net/"
                    "corp-ncins-acc-gateway/secure/corp-ncins-acc-corp-ncins-acc-api"
                ),
                (
                    "http://corp-gateway-test.moscow.alfaintra.net/"
                    "corp-ncins-gateway/secure/corp-ncins-corp-ncins-api"
                ),
            ],
            "paths": {
                "create": "/v1/applications",
                "list": "/v1/applications/list",
                "get": "/v1/applications/{id}",
                "contracts": "/v1/contracts",
            },
            "contract_base_url_candidates": [
                (
                    "http://corp-gateway-test.moscow.alfaintra.net/"
                    "corp-ncins-acc-gateway/secure/corp-ncins-acc-corp-ncins-acc-api"
                ),
                (
                    "http://corp-gateway-test.moscow.alfaintra.net/"
                    "corp-ncins-gateway/secure/corp-ncins-corp-ncins-api"
                ),
            ],
            "can_list": True,
            "verify_ssl": True,
        },
        "ump": {
            "label": "Keycloak UMP QA → ump-application-facade",
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
            "label": "Corporate Keycloak (curl разработчика) → corp-gateway TEST",
            "token_url": (
                "http://corp-gateway-test.moscow.alfaintra.net/mks-gateway/public/auth/"
                "realms/corporate/protocol/openid-connect/token"
            ),
            "client_id": "nib-corp-ncinsurance",
            "client_secret": "nib_corp_ncinsurance",
            "base_url": (
                "http://corp-gateway-test.moscow.alfaintra.net/"
                "corp-ncins-acc-gateway/secure/corp-ncins-acc-corp-ncins-acc-api"
            ),
            "base_url_candidates": [
                (
                    "http://corp-gateway-test.moscow.alfaintra.net/"
                    "corp-ncins-acc-gateway/secure/corp-ncins-acc-corp-ncins-acc-api"
                ),
                (
                    "http://corp-gateway-test.moscow.alfaintra.net/"
                    "corp-ncins-gateway/secure/corp-ncins-corp-ncins-api"
                ),
            ],
            "paths": {
                "create": "/v1/applications",
                "list": "/v1/applications/list",
                "get": "/v1/applications/{id}",
                "contracts": "/v1/contracts",
            },
            "contract_base_url_candidates": [
                (
                    "http://corp-gateway-test.moscow.alfaintra.net/"
                    "corp-ncins-acc-gateway/secure/corp-ncins-acc-corp-ncins-acc-api"
                ),
                (
                    "http://corp-gateway-test.moscow.alfaintra.net/"
                    "corp-ncins-gateway/secure/corp-ncins-corp-ncins-api"
                ),
            ],
            "can_list": True,
            "verify_ssl": True,
        },
        "ump": {
            "label": "Keycloak UMP TEST → facade (только POST /applications)",
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


# ---------------------------------------------------------------------------
# Чеклист PASS / FAIL / MANUAL
# ---------------------------------------------------------------------------


@dataclass
class CheckResult:
    name: str
    status: str  # PASS | FAIL | WARN | MANUAL | SKIP
    detail: str = ""


@dataclass
class Checklist:
    items: list[CheckResult] = field(default_factory=list)

    def add(self, name: str, status: str, detail: str = "") -> None:
        self.items.append(CheckResult(name, status, detail))
        mark = {"PASS": "OK", "FAIL": "FAIL", "WARN": "WARN", "MANUAL": "???", "SKIP": "--"}
        prefix = mark.get(status, status)
        line = f"[{prefix}] {name}"
        if detail:
            line += f" — {detail}"
        print(line)

    def print_summary(self) -> int:
        print("\n========== ИТОГ ЧЕКЛИСТА ==========")
        counts = {"PASS": 0, "FAIL": 0, "WARN": 0, "MANUAL": 0, "SKIP": 0}
        for item in self.items:
            counts[item.status] = counts.get(item.status, 0) + 1
            print(f"  {item.status:6} | {item.name}")
            if item.detail:
                print(f"         {item.detail}")
        print(
            f"\nPASS={counts['PASS']} FAIL={counts['FAIL']} "
            f"WARN={counts['WARN']} MANUAL={counts['MANUAL']} SKIP={counts['SKIP']}"
        )
        return 1 if counts["FAIL"] else 0


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def build_create_payload(channel: str) -> dict[str, Any]:
    """Payload мультизаявки (как в инструкции; уникальный extAppId каждый раз)."""
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
    resp = requests.post(
        token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=timeout,
        verify=verify_ssl,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"TOKEN {resp.status_code}: {resp.text[:1000]}")
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError(f"Нет access_token в ответе: {resp.text[:500]}")
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
    resp = requests.post(
        url,
        headers=headers,
        params={"finalVersion": "true", "fullCreate": "true"},
        json=payload,
        timeout=timeout,
        verify=verify_ssl,
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
    GetApplicationsUmpPostRequestDto (corp-ncins): рабочее поле — number.
    Остальные ключи (ids/applicationIds/...) дают 400 Unrecognized field.
    app_id оставлен в сигнатуре для совместимости вызова.
    """
    _ = app_id
    bodies: list[dict[str, Any]] = []
    if number:
        bodies.append({"number": number})
    return bodies


def try_get_contracts(
    auth_cfg: dict[str, Any],
    headers: dict[str, str],
    *,
    app_id: str,
    number: str | None,
    inn: str = "7826688577",
) -> tuple[dict[str, Any] | list[Any] | None, list[str]]:
    """
    GET /v1/contracts — проверка, что после финализации UMP договор появился в Учёте.
    (Гурин @PAGurin: обязательно тестануть, даже если не было в тексте задачи.)

    UMP сам пишет договор через POST /v1/ins-contracts на финализации;
    GET /v1/contracts — проверка снаружи, что контракт отдаётся.
    """
    attempts: list[str] = []
    verify = auth_cfg.get("verify_ssl", True)
    path = auth_cfg.get("paths", {}).get("contracts", "/v1/contracts")

    bases: list[str] = []
    for b in auth_cfg.get("contract_base_url_candidates") or []:
        if b not in bases:
            bases.append(b)
    # fallback: тот же base, где create
    if auth_cfg.get("base_url") and auth_cfg["base_url"] not in bases:
        bases.append(auth_cfg["base_url"])

    param_variants: list[dict[str, str]] = []
    if app_id:
        param_variants.extend(
            [
                {"applicationId": app_id},
                {"applicationIds": app_id},
                {"id": app_id},
            ]
        )
    if number:
        param_variants.extend(
            [
                {"number": number},
                {"applicationNumber": number},
                {"contractNumber": number},
            ]
        )
    if inn:
        param_variants.append({"inn": inn})
    param_variants.append({})

    for base in bases:
        url = f"{base.rstrip('/')}{path}"
        for params in param_variants:
            try:
                resp = requests.get(
                    url, headers=headers, params=params, timeout=60, verify=verify
                )
            except requests.RequestException as exc:
                attempts.append(f"GET network {url} params={params} -> {exc}")
                continue
            detail = f"GET {resp.status_code} {url} params={params}"
            if resp.status_code >= 400:
                attempts.append(f"{detail} -> {resp.text[:300]}")
                continue
            try:
                data = resp.json()
            except ValueError:
                attempts.append(f"{detail} -> non-json: {resp.text[:200]}")
                continue
            attempts.append(f"{detail} OK")
            return data, attempts

    return None, attempts


def contract_looks_present(data: Any, app_id: str) -> tuple[bool, str]:
    """Грубая эвристика: ответ не пустой / есть id / есть ссылка на applicationId."""
    if data is None:
        return False, "пустой ответ"
    if isinstance(data, list):
        if not data:
            return False, "пустой список"
        return True, f"список из {len(data)} элемент(ов)"
    if isinstance(data, dict):
        # типичные обёртки
        for key in ("list", "contracts", "content", "items", "data"):
            val = data.get(key)
            if isinstance(val, list):
                if not val:
                    return False, f"{key}=[]"
                return True, f"{key} len={len(val)}"
        if data.get("id") or data.get("contractId") or data.get("contractNumber"):
            return True, "есть id/contractNumber в объекте"
        # иногда возвращают faults
        if data.get("failures") and not any(
            data.get(k) for k in ("id", "contractId", "list", "contracts")
        ):
            return False, f"failures={data.get('failures')}"
        # не пустой dict — считаем WARN-уровнем успеха (разберём глазами)
        if data:
            app_hit = app_id and app_id in json.dumps(data, ensure_ascii=False)
            return True, ("applicationId найден в JSON" if app_hit else "непустой JSON-объект")
    return False, f"неожиданный тип {type(data)}"


def ump_list_bodies(number: str | None) -> list[dict[str, Any]]:
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
    attempts: list[str] = []
    base = auth_cfg["base_url"]
    paths = auth_cfg["paths"]
    verify = auth_cfg.get("verify_ssl", True)

    if not auth_cfg.get("can_list", True):
        attempts.append(
            "list недоступен (на TEST у nib-corp-ncins только POST /applications)"
        )
        return None, attempts

    bodies = (
        corp_list_bodies(app_id, number)
        if auth_mode == "corporate"
        else ump_list_bodies(number)
    )

    for body in bodies:
        data, msg = list_applications(
            base, paths["list"], headers, body, verify_ssl=verify
        )
        attempts.append(f"LIST {msg}")
        if data is None:
            continue
        found = pick_from_list_response(data, app_id)
        if found:
            return found, attempts

    get_path = paths.get("get", "/v1/applications/{id}").format(id=app_id)
    get_url = f"{base.rstrip('/')}{get_path}"
    try:
        resp = requests.get(get_url, headers=headers, timeout=30, verify=verify)
        attempts.append(
            f"GET {resp.status_code} {get_url} "
            f"(у NIB часто 403 — это нормально) -> {resp.text[:160]}"
        )
    except requests.RequestException as exc:
        attempts.append(f"GET network error: {exc}")

    return None, attempts


def extract_channel_codes(app: dict[str, Any]) -> list[str]:
    return [c.get("code") for c in app.get("channels") or [] if c.get("code")]


def print_json(title: str, data: Any) -> None:
    print(f"\n=== {title} ===")
    print(json.dumps(data, ensure_ascii=False, indent=2))


def build_payment_finished_payload(app_id: str) -> dict[str, Any]:
    return {
        "messageName": "paymentFinished",
        "correlationKey": f"{app_id}.NON_CREDIT_INSURANCE",
        "variables": {
            "id": app_id,
            "outputVariables": {},
        },
    }


def write_kafka_payment_finished(
    app_id: str,
    env_cfg: dict[str, Any],
    *,
    out_dir: str = ".",
) -> tuple[dict[str, Any], str]:
    """
    Сформировать JSON paymentFinished и сохранить в файл.
    Процесс оплаты ждёт его в Kafka (ump.process.to.system), таймаут PT5M.
    """
    payload = build_payment_finished_payload(app_id)
    out_path = f"{out_dir.rstrip('/')}/paymentFinished_{app_id}.json"
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
        fh.write("\n")

    print(
        f"""
========== KAFKA: paymentFinished (сформировано тестом) ==========
Топик: ump.process.to.system
UI:    {env_cfg.get('kafka')}
{('UI alt: ' + env_cfg['kafka_alt']) if env_cfg.get('kafka_alt') else ''}
Файл:  {out_path}

ВАЖНО: отправь в Produce за ~5 минут (PT5M), иначе оплата Canceled.

JSON:
"""
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    print()
    return payload, out_path


def print_manual_operate_steps(app_id: str, channel: str, env_cfg: dict[str, Any]) -> None:
    expected = "nib-corp-ncins" if channel == "nib" else "sfa-ncins"
    operate_alt = env_cfg.get("operate_alt")
    kafka_alt = env_cfg.get("kafka_alt")
    alt_block = ""
    if operate_alt:
        alt_block += f"Operate (alt): {operate_alt}\n"
    if kafka_alt:
        alt_block += f"Kafka (alt):   {kafka_alt}\n"
    print(
        f"""
========== РУЧНЫЕ ПРОВЕРКИ В OPERATE (скрипт сам это не увидит) ==========
Operate: {env_cfg['operate']}
Kafka:   {env_cfg['kafka']}
{alt_block}businessKey / application id: {app_id}
Ожидаемый channels[].code: {expected}

1) Найди процесс по businessKey = {app_id}
   (если нет на первом Operate — открой alt из задачи)

2) «Регистрация заявки» (ump-app-reg-pa)
   → дедупликация должна ИГНОРИРОВАТЬСЯ (повторный create не должен стопорить флоу)

3) «Подготовка документов» (ump-prepare-documents-ncins-pa)
   → в Variables есть массив acDocuments
   → Input: productUpdateType / AFTER_PREPARE_DOCS на шаге обновления продукта
   → Call Activity ump-generate-and-save-document-pa отрабатывает (NCINS-143)

4) «Подписание» (ump-signing-documents-ncins-pa)
   → Input с AFTER_SIGNING / Kafka SigningDocs

5) «Процесс оплаты» — стоп на receive paymentFinished
   → в Kafka ump.process.to.system отправь paymentFinished (скрипт печатает JSON)
   → уложись в ~5 минут, иначе Canceled

6) «Финализация» → POST /v1/ins-contracts (создание договора в Учёте)
   → потом GET /v1/contracts
"""
    )


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="NCINS-143: токен → create → channel → чеклист Operate"
    )
    parser.add_argument("--env", choices=["dev", "qa", "test"], default="dev")
    parser.add_argument(
        "--auth",
        choices=["ump", "corporate"],
        default="corporate",
        help=(
            "corporate = токен разработчика → corp-gateway (создание НИБ по NCINS-143); "
            "ump = токен лида → facade (нужен рабочий --base-url)"
        ),
    )
    parser.add_argument("--channel", choices=["nib", "sfa"], default="nib")
    parser.add_argument("--client-id", default="")
    parser.add_argument("--client-secret", default="")
    parser.add_argument("--token-url", default="")
    parser.add_argument("--base-url", default="", help="Override API base URL")
    parser.add_argument("--token", default="", help="Готовый Bearer token")
    parser.add_argument(
        "--token-only",
        action="store_true",
        help="Только получить токен и выйти (проверка auth)",
    )
    parser.add_argument("--skip-create", action="store_true")
    parser.add_argument("--skip-get", action="store_true")
    parser.add_argument(
        "--check-contracts",
        action="store_true",
        help="Явно включить GET /v1/contracts (и так включено по умолчанию)",
    )
    parser.add_argument(
        "--no-check-contracts",
        action="store_true",
        help="Не вызывать GET /v1/contracts",
    )
    parser.add_argument(
        "--contracts-only",
        action="store_true",
        help="Только токен + GET /v1/contracts (нужен --app-id, опционально --app-number)",
    )
    parser.add_argument("--app-id", default="")
    parser.add_argument("--app-number", default="")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    args.check_contracts = not args.no_check_contracts
    if args.contracts_only:
        args.skip_create = True
        args.skip_get = True
        args.check_contracts = True
        if not args.app_id:
            print("--contracts-only требует --app-id", file=sys.stderr)
            return 2

    cl = Checklist()
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
    if args.channel == "sfa" and not args.base_url and args.auth == "corporate":
        # SFA идёт через свой gateway, не через NIB corp
        auth_cfg["base_url"] = env_cfg["sfa_base_url"]
        auth_cfg["base_url_candidates"] = [env_cfg["sfa_base_url"]]

    expected_channel = "nib-corp-ncins" if args.channel == "nib" else "sfa-ncins"

    print(
        f"""
========== NCINS-143 smoke / e2e helper ==========
Стенд:     {args.env}
Auth:      {args.auth} — {auth_cfg.get('label', '')}
client_id: {auth_cfg['client_id']}
token_url: {auth_cfg['token_url']}
API base:  {auth_cfg['base_url']}
Канал:     {args.channel} → ждём channels[].code = {expected_channel}
"""
    )

    payload = build_create_payload(args.channel)
    if args.dry_run:
        print_json("CREATE payload (dry-run)", payload)
        print("Dry-run: запросы не отправлялись.")
        cl.add("dry-run", "SKIP", "запросы не отправлялись")
        return cl.print_summary()

    verify = auth_cfg.get("verify_ssl", True)

    # --- 1. Токен ---
    print("\n--- ШАГ 1. Получить токен ---")
    try:
        if args.token:
            token = args.token
            cl.add("Получение токена", "PASS", "использован --token")
        else:
            print(f"POST {auth_cfg['token_url']}")
            print(f"client_id={auth_cfg['client_id']}")
            token = get_token(
                auth_cfg["token_url"],
                auth_cfg["client_id"],
                auth_cfg["client_secret"],
                verify_ssl=verify,
            )
            cl.add(
                "Получение токена",
                "PASS",
                f"access_token длина={len(token)}, client_id={auth_cfg['client_id']}",
            )
            print(f"access_token (первые 40 символов): {token[:40]}...")
    except Exception as exc:  # noqa: BLE001
        cl.add("Получение токена", "FAIL", str(exc))
        return cl.print_summary()

    if args.token_only:
        print("\n--token-only: дальше не идём.")
        cl.add("CREATE заявки", "SKIP", "--token-only")
        cl.add("channels", "SKIP", "--token-only")
        return cl.print_summary()

    headers = default_headers(token, args.channel)
    app: dict[str, Any]
    app_id: str
    app_number = args.app_number or None

    # --- 2. CREATE ---
    print("\n--- ШАГ 2. Создать мультизаявку ---")
    if args.skip_create:
        if not args.app_id:
            cl.add("CREATE заявки", "FAIL", "--skip-create требует --app-id")
            return cl.print_summary()
        app_id = args.app_id
        app = {"id": app_id, "number": app_number}
        cl.add("CREATE заявки", "SKIP", f"используем --app-id={app_id}")
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
            print(f"\nCREATE {base}{create_path}?finalVersion=true&fullCreate=true")
            try:
                created = create_application(
                    base, create_path, headers, payload, verify_ssl=verify
                )
                break
            except RuntimeError as exc:
                last_error = exc
                err = str(exc)
                print(f"  -> {exc}")
                if "Jwt issuer is not configured" in err:
                    print(
                        "  !!! UMP JWT нельзя слать в corp-gateway.\n"
                        "      Для create НИБ:  --auth corporate\n"
                        "      Для facade UMP: --auth ump --base-url <URL из Postman>"
                    )
                    cl.add(
                        "Совпадение токен↔API",
                        "FAIL",
                        "UMP JWT в corp-gateway (Jwt issuer is not configured)",
                    )
                if "404" in err and args.auth == "ump":
                    print(
                        "  !!! 404 на facade: токен лида ОК, но host facade снаружи не тот.\n"
                        "      1) Для NCINS-143 создавай заявку так:\n"
                        "         python ump.py --env dev --auth corporate --channel nib\n"
                        "      2) Если нужен именно facade — спроси у лида/из Postman\n"
                        "         base URL и передай: --auth ump --base-url <URL>"
                    )
                continue
            except requests.RequestException as exc:
                last_error = RuntimeError(f"network: {exc}")
                print(f"  -> network error: {exc}")
                continue

        if created is None:
            cl.add("CREATE заявки", "FAIL", str(last_error))
            hint = (
                "сейчас --auth ump → 404 на facade. Запусти: "
                "--auth corporate --channel nib"
                if args.auth == "ump"
                else f"попробуй другой --base-url; auth={args.auth}"
            )
            cl.add("Подсказка", "WARN", hint)
            return cl.print_summary()

        print_json("CREATE response", created)
        app_id = created.get("id") or ""
        app_number = created.get("number") or app_number
        if not app_id:
            cl.add("CREATE заявки", "FAIL", "в ответе нет id")
            return cl.print_summary()
        app = created
        cl.add(
            "CREATE заявки",
            "PASS",
            f"id={app_id}" + (f", number={app_number}" if app_number else ""),
        )
        cl.add("Совпадение токен↔API", "PASS", f"auth={args.auth}, base={auth_cfg['base_url']}")
        # Сразу после create — JSON paymentFinished с реальным applicationId
        _, pf_path = write_kafka_payment_finished(app_id, env_cfg)
        cl.add("Сформирован paymentFinished JSON", "PASS", pf_path)

    # --- 3. channels ---
    print("\n--- ШАГ 3. Проверить channels ---")
    channels = extract_channel_codes(app)
    if channels:
        if expected_channel in channels:
            cl.add("channels в create-ответе", "PASS", f"{channels}")
        else:
            cl.add(
                "channels в create-ответе",
                "FAIL",
                f"ожидали {expected_channel}, получили {channels}",
            )
    else:
        cl.add(
            "channels в create-ответе",
            "WARN",
            "в create-ответе нет channels — проверим после list / в Operate",
        )

    # --- 4. LIST / read ---
    print("\n--- ШАГ 4. Прочитать заявку (list) ---")
    if args.skip_get:
        cl.add("Чтение заявки (list)", "SKIP", "--skip-get")
    else:
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
            cl.add(
                "Чтение заявки (list)",
                "WARN",
                "list/get не дали заявку — смотри Operate по id из CREATE",
            )
        else:
            app = fetched
            print_json("LIST item", app)
            channels = extract_channel_codes(app)
            cl.add("Чтение заявки (list)", "PASS", f"id={app.get('id')}, channels={channels}")
            if channels:
                if expected_channel in channels:
                    cl.add("channels после list", "PASS", f"{channels}")
                else:
                    cl.add(
                        "channels после list",
                        "FAIL",
                        f"ожидали {expected_channel}, получили {channels}",
                    )

    # --- 5. GET /v1/contracts (обязательно по словам Гурина) ---
    print("\n--- ШАГ 5. GET /v1/contracts (договор после финализации UMP) ---")
    print(
        "UMP на финализации делает POST /v1/ins-contracts.\n"
        "Мы проверяем снаружи: GET /v1/contracts отдаёт договор.\n"
        "Сначала в Operate должен быть Completed «Процесс финализации заявки»."
    )
    if not args.check_contracts:
        cl.add("GET /v1/contracts", "SKIP", "--no-check-contracts")
    elif args.auth != "corporate":
        cl.add(
            "GET /v1/contracts",
            "WARN",
            "для contracts нужен --auth corporate (corp-gateway Учёт); сейчас auth=ump",
        )
    else:
        # для contracts предпочтительно ходить в acc-gateway кандидаты
        contract_cfg = dict(auth_cfg)
        data, attempts = try_get_contracts(
            contract_cfg,
            headers,
            app_id=app_id,
            number=app_number,
        )
        for line in attempts:
            print(f"  try: {line}")
        if data is None:
            cl.add(
                "GET /v1/contracts",
                "FAIL",
                "не удалось получить договор — дождись финализации в Operate "
                "или уточни у Гурина точный path/query",
            )
            print(
                "\nПодсказка curl (подставь TOKEN):\n"
                f"  curl -s -H \"Authorization: Bearer $TOKEN\" \\\n"
                f"    -H \"A-userId: 123456\" -H \"A-customerId: U_M2XNX\" \\\n"
                f"    -H \"A-clientType: UL\" -H \"A-channelId: NIB\" \\\n"
                f"    \"http://corp-gateway-dev.moscow.alfaintra.net/"
                f"corp-ncins-acc-gateway/secure/corp-ncins-acc-corp-ncins-acc-api"
                f"/v1/contracts?applicationId={app_id}\"\n"
            )
        else:
            print_json("GET /v1/contracts response", data)
            ok, detail = contract_looks_present(data, app_id)
            cl.add(
                "GET /v1/contracts",
                "PASS" if ok else "WARN",
                detail,
            )

    if args.contracts_only:
        print(
            f"\n--contracts-only. application id = {app_id}, number = {app_number or '(нет)'}"
        )
        return cl.print_summary()

    # --- 6. Kafka paymentFinished (если не напечатали сразу после create) ---
    if args.skip_create:
        print_kafka_payment_finished(app_id, env_cfg)
    cl.add(
        "Kafka: отправить paymentFinished",
        "MANUAL",
        f"correlationKey={app_id}.NON_CREDIT_INSURANCE (успеть ~5 мин)",
    )

    # --- 7. Ручные проверки NCINS-143 ---
    print_manual_operate_steps(app_id, args.channel, env_cfg)
    cl.add(
        "Operate: дедупликация на регистрации игнорируется",
        "MANUAL",
        "ump-app-reg-pa",
    )
    cl.add(
        "Operate: acDocuments в prepare-documents",
        "MANUAL",
        "ump-prepare-documents-ncins-pa → Variables.acDocuments",
    )
    cl.add(
        "Operate: AFTER_PREPARE_DOCS / AFTER_SIGNING в mappings",
        "MANUAL",
        "prepare + signing service-task-update-product",
    )
    cl.add(
        "Operate: финализация → POST /v1/ins-contracts",
        "MANUAL",
        "ump-finalisation-ncins-pa шаг «Создать договор» Completed",
    )
    cl.add(
        "GET /v1/contracts после финализации",
        "MANUAL",
        f"python ump.py --env {args.env} --auth corporate --contracts-only --app-id {app_id}",
    )

    print(
        f"\nГотово.\n"
        f"  application id = {app_id}\n"
        f"  number         = {app_number or '(нет)'}\n"
        f"  Operate        = {env_cfg['operate']}\n"
        f"  businessKey    = {app_id}\n"
        f"\nПовторно проверить договор:\n"
        f"  python ump.py --env {args.env} --auth corporate --contracts-only "
        f"--app-id {app_id}"
        + (f" --app-number {app_number}" if app_number else "")
        + "\n"
    )
    return cl.print_summary()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print(f"\nERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
