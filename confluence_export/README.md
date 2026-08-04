# Экспорт и анализ документов СА

## 1) Выгрузка PDF из Confluence

```bash
pip install -r requirements.txt
python SA_conf_work.py
```

PDF появятся в `./confluence_pdfs_Zabaryanskiy`.

## 2) Анализ PDF через YandexGPT

1. В [Yandex Cloud](https://console.yandex.cloud/) создайте API-ключ с доступом к Foundation Models.
2. В `analyze_sa_pdfs_yandex.py` укажите:

```python
PDF_DIR = r"C:\Users\Дмитрий Григорьев\Desktop\Alfa Bank\Работа СА Юры\confluence_pdfs_Zabaryanskiy"
YANDEX_FOLDER_ID = "b1g...."
YANDEX_API_KEY = "AQVN...."
```

3. Запуск:

```bash
pip install pypdf requests
python analyze_sa_pdfs_yandex.py
```

Пробный прогон на 3 файла:

```bash
python analyze_sa_pdfs_yandex.py --limit 3
```

Результаты в `./sa_yandex_analysis/`:
- `summary_table.csv` — сводная таблица (Excel откроет)
- `summary_table.md` — таблица с +/− и оценками 1–10
- `executive_summary.md` — краткий итог для руководителя
- `analysis_details.json` — полный JSON

По каждому документу:
- `score_work` — качество работы СА
- `score_doc` — качество документа для команды
- `score_overall` — итог
- плюсы / минусы / вердикт
