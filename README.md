# NCINS-143 — проверка флоу UMP (генерация/сохранение документов)

Скрипт: `test_ump_ncins_flow.py` (у тебя может лежать как `ump.py`).

## Что у тебя только что произошло

1. `--auth ump` → токен лида **взялся нормально** (Шаг 1 OK).
2. CREATE на `ump-application-facade.*` → **404 nginx**.

Значит: **токен правильный, а внешний URL facade на DEV мы угадали неверно** (сервис с твоей машины по этим host’ам не открыт / другой path).

По комментариям NCINS-143 создание мультизаявки в **НИБе** идёт через **corp-gateway**, не напрямую в facade.

## Что запускать сейчас

```bash
pip install -r requirements.txt

# рабочий путь для НИБ (токен разработчика → corp-gateway)
python ump.py --env dev --auth corporate --channel nib
```

Ожидай: CREATE 200/201, в ответе `id` / `number`, `channels` с `nib-corp-ncins`, дальше смотри Operate.

## Два токена (не смешивать)

| Кто | `--auth` | Куда |
|-----|----------|------|
| Разработчик (`nib-corp-ncinsurance`) | `corporate` | corp-gateway `/v1/applications` ← **create НИБ** |
| Лид Яковлев (`nib-corp-ncins`) | `ump` | ump-application-facade `/applications` ← нужен **точный `--base-url` из Postman** |

UMP-токен в corp-gateway → `401 Jwt issuer is not configured`.  
UMP-токен на неверный host facade → `404` (твой случай).

Если лид даст Postman URL facade:

```bash
python ump.py --env dev --auth ump --channel nib --base-url "https://ПРАВИЛЬНЫЙ_ХОСТ/ump-application-facade"
```

## Прочее

```bash
python ump.py --env dev --auth ump --token-only   # только проверить токен
python ump.py --dry-run
```

Ручной чеклист Operate (дедуп, `acDocuments`, mappings, Kafka) скрипт печатает после успешного CREATE.
