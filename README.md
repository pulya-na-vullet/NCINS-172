# NCINS-143 — проверка флоу UMP

Скрипт: `test_ump_ncins_flow.py` (у тебя может быть `ump.py`).

## Рабочая команда (проверено)

```bash
python ump.py --env dev --auth corporate --channel nib
```

Рабочий API:

`http://corp-gateway-dev.moscow.alfaintra.net/corp-ncins-gateway/secure/corp-ncins-corp-ncins-api`

- `corp-ncins-acc-gateway` → 403 RBAC (не тот API для этого клиента)
- `--auth ump` → токен OK, но facade host снаружи 404 без `--base-url` из Postman

List: тело `{"number": "UMP..."}` (поле `number`).

## Пример успешного прогона

- CREATE OK, `channels = nib-corp-ncins`
- status после list: `IN_PROGRESS`
- application id / businessKey, например: `8e14dc1f-7106-4bc3-acc6-6de0789d7c19`
- number: `UMP26080447676`

## Дальше руками в Operate

1. http://operate.umpdevwk8sm1.moscow.alfaintra.net/  
   (если пусто — http://operate.umpqak8sm1.moscow.alfaintra.net/)
2. Поиск по businessKey = application id
3. Проверки NCINS-143: дедуп на регистрации, `acDocuments`, `AFTER_PREPARE_DOCS` / `AFTER_SIGNING`, generate-and-save, Kafka

## Два токена

| `--auth` | Клиент | Куда |
|----------|--------|------|
| `corporate` | `nib-corp-ncinsurance` | corp-gateway `/v1/applications` |
| `ump` | `nib-corp-ncins` | facade `/applications` + `--base-url` |
