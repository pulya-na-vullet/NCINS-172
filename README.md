# NCINS-143 — проверка флоу UMP (генерация/сохранение документов)

Скрипт `test_ump_ncins_flow.py` делает авточасть и печатает чеклист ручных проверок в Operate.

## Важно: два токена

| Кто прислал | `--auth` | Куда ходить |
|-------------|----------|-------------|
| **Лид** (Яковлев) — Keycloak UMP, `nib-corp-ncins` | `ump` | `ump-application-facade` `/applications` |
| **Разработчик** — corporate, `nib-corp-ncinsurance` | `corporate` | `corp-gateway` `/v1/applications` |

Нельзя слать UMP-токен в corp-gateway → `401 Jwt issuer is not configured`.

По умолчанию скрипт использует **`--auth ump`** (как сказал лид).

## Установка

```bash
pip install -r requirements.txt
```

## Как гонять (DEV)

```bash
# 1) Только проверить, что токен лида берётся
python test_ump_ncins_flow.py --env dev --auth ump --token-only

# 2) Полный автопрогон НИБ (токен → create → channels → list)
python test_ump_ncins_flow.py --env dev --auth ump --channel nib

# 3) Через corp-gateway (токен разработчика)
python test_ump_ncins_flow.py --env dev --auth corporate --channel nib

# 4) Уже есть id заявки — только list + чеклист Operate
python test_ump_ncins_flow.py --env dev --auth ump --skip-create --app-id <UUID> --app-number UMP...
```

Стенды: `--env dev|qa|test`.

## Что скрипт проверяет сам

1. Получение токена (PASS/FAIL)
2. CREATE мультизаявки
3. `channels[].code` = `nib-corp-ncins` (НИБ) или `sfa-ncins` (СФА)
4. Чтение через `POST /applications/list` (если доступно)

## Что смотреть руками в Operate (скрипт напечатает напоминание)

1. Регистрация — дедуп игнорируется
2. Prepare documents — есть `acDocuments`
3. Mappings: `AFTER_PREPARE_DOCS`, `AFTER_SIGNING`
4. Флоу до `/v1/ins-contracts` / generate-and-save
5. Kafka `ump.process.to.system`

Operate DEV/QA: http://operate.umpqak8sm1.moscow.alfaintra.net/
