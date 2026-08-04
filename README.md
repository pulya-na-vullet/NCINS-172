# NCINS-143 — проверка флоу UMP

Скрипт: `test_ump_ncins_flow.py` (у тебя может быть `ump.py`).

## Рабочая команда

```bash
# создать заявку + channels + list + GET /v1/contracts
python ump.py --env dev --auth corporate --channel nib

# только договор по уже созданной заявке (Гурин: обязательно)
python ump.py --env dev --auth corporate --contracts-only \
  --app-id 8e14dc1f-7106-4bc3-acc6-6de0789d7c19 \
  --app-number UMP26080447676
```

## Контракты (важно)

1. UMP на **финализации** сам делает `POST /v1/ins-contracts` (создать договор в Учёте).
2. Тестер проверяет снаружи: **`GET /v1/contracts`** — договор отдаётся.
3. Гурин (@PAGurin): «Да, прям обязательно».

Перед GET в Operate должен быть Completed **«Процесс финализации заявки»**  
(`ump-finalisation-ncins-pa`). Если оплаты Canceled — финализация могла не стартовать.

## Рабочий create API

`corp-ncins-gateway/.../corp-ncins-corp-ncins-api` (`*-acc-*` на create applications → 403).

Для contracts скрипт сначала пробует **acc-gateway**.
