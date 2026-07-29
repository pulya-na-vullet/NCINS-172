# Экспорт страниц Confluence по автору

Скрипт находит **все страницы, созданные пользователем**, и сохраняет их в PDF.

## Почему старый скрипт не находил страницы

1. CQL `creator.fullname="ФИО"` на Confluence Server/DC обычно **не работает**. Нужен логин: `creator = "username"`.
2. Ответ `/rest/api/search` кладёт страницу в `result.content`, а не в корень `result` — из‑за этого `id`/`title` часто пустые.
3. Сначала нужно **резолвить пользователя** (`/rest/api/user`, `/rest/api/user/search`) и уже потом искать по его `username` / `userKey`.

## Установка

```bash
cd confluence_export
pip install -r requirements.txt
# для html→pdf (если native PDF export недоступен):
# установить wkhtmltopdf: https://wkhtmltopdf.org/downloads.html
```

## Запуск

**Не храните PAT в файле.** Передайте токен через env или CLI.

```bash
# Windows PowerShell
$env:CONFLUENCE_TOKEN = "ваш_pat"
$env:CONFLUENCE_USERNAME = "ваш_логин"   # для Server/DC почти всегда нужен

# 1) Сначала только список (без PDF) — проверить, что автор резолвится
python export_user_pages.py --list-only --user Zabaryanskiy --user "Забарянский"

# 2) Если знаете точный логин из профиля Confluence — лучше так
python export_user_pages.py --user YZabaryanskiy

# 3) Если CQL пустой — медленный, но надёжный обход пространств
python export_user_pages.py --user YZabaryanskiy --fallback-scan

# 4) Обход только известных space key
python export_user_pages.py --user YZabaryanskiy --fallback-scan --spaces SPACE1,SPACE2
```

По умолчанию имена из вашего скрипта уже зашиты как подсказки (`Забарянский…`, `YZabaryanskiy@alfabank.ru` и т.д.).

## Что делает скрипт

1. Резолвит пользователя по логину / ФИО / email.
2. Ищет `type=page AND creator = "<username>"`, затем `contributor`.
3. Пишет кеш `confluence_pages_cache_Zabaryanskiy.json` и `pages_index.json`.
4. Экспортирует PDF:
   - сначала native Confluence PDF (`/spaces/flyingpdf/...`);
   - если не вышло — HTML через REST + `pdfkit`/`wkhtmltopdf`.

## Безопасность

PAT из чата лучше **сразу отозвать и выпустить новый** — он был в открытом виде в коде.
