# Экспорт страниц Confluence по автору

## Запуск

1. Откройте `SA_conf_work.py`
2. В начале файла вставьте PAT в `API_TOKEN = "..."` (тот же, что был в старом скрипте)
3. Запустите:

```bash
pip install requests pdfkit
python SA_conf_work.py
```

Без аргументов: ищет страницы `MOSCOW\U_M2XNX` / Забарянский и сохраняет PDF в `./confluence_pdfs_Zabaryanskiy`.

Только список:

```bash
python SA_conf_work.py --list-only
```

Если Bearer не проходит, в начале файла укажите:

```python
USERNAME = r"MOSCOW\U_M2XNX"
```

**Не коммитьте PAT в git** — GitHub push protection его блокирует.
