#!/usr/bin/env python3
"""
Анализ PDF-документов СА (Юрий Забарянский) через YandexGPT.

Берёт все PDF из папки выгрузки Confluence, отправляет текст в Yandex API
и для каждого документа даёт:
  - плюсы / минусы
  - оценку качества работы СА (1–10)
  - оценку качества документа для команды (1–10)
  - итоговую оценку (1–10)

На выходе: CSV + Markdown-сводная таблица + JSON с деталями.

Запуск:
  python analyze_sa_pdfs_yandex.py

Нужны ключи Yandex Cloud (Folder ID + API Key сервиса с ролью
ai.languageModels.user / yc.ai.languageModels.execute):
  https://console.yandex.cloud/
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

import requests

warnings.filterwarnings("ignore")

try:
    from pypdf import PdfReader
except ImportError:
    try:
        from PyPDF2 import PdfReader  # type: ignore
    except ImportError:
        PdfReader = None  # type: ignore


# --- КОНФИГУРАЦИЯ ---
# PDF из выгрузки Confluence (по умолчанию рядом со скриптом)
PDF_DIR = r"./confluence_pdfs_Zabaryanskiy"
OUTPUT_DIR = r"./sa_yandex_analysis"

# Yandex Cloud Foundation Models / YandexGPT
# Вставьте локально (в git не коммитить):
YANDEX_FOLDER_ID = ""  # каталог (b1g...)
YANDEX_API_KEY = ""    # API-ключ сервисного аккаунта
# Альтернатива API-ключу: IAM-токен (тогда API_KEY оставьте пустым)
YANDEX_IAM_TOKEN = ""

# Модель: yandexgpt-lite | yandexgpt | yandexgpt-32k
YANDEX_MODEL = "yandexgpt"
YANDEX_TEMPERATURE = 0.2
YANDEX_MAX_TOKENS = 2000

# Сколько символов текста PDF отправлять в модель (обрезка длинных доков)
MAX_CHARS_PER_DOC = 12000
# Параллельные запросы к API (1–3; выше — риск 429)
API_WORKERS = 2
REQUEST_TIMEOUT = 180
RETRY_COUNT = 3
RETRY_SLEEP_SEC = 5

ANALYST_NAME = "Забарянский Юрий Геннадьевич"
# -----------------------------

print_lock = Lock()


def safe_print(*args: Any, **kwargs: Any) -> None:
    with print_lock:
        print(*args, **kwargs)


SYSTEM_PROMPT = f"""Ты — ведущий методолог / руководитель практики системного анализа в банке.
Твоя задача — оценить документы системного аналитика {ANALYST_NAME}.

Критерии оценки КАЧЕСТВА РАБОТЫ СА (score_work, 1–10):
- полнота проработки требований и границ решения;
- ясность акторов, сценариев, бизнес-правил;
- проработка интеграций, данных, НФТ, рисков/ограничений;
- трассируемость (откуда требование, зачем фича);
- практическая пригодность для разработки и тестирования.

Критерии оценки КАЧЕСТВА ДОКУМЕНТА ДЛЯ КОМАНДЫ (score_doc, 1–10):
- структура, читаемость, единообразие;
- наличие целей, контекста, ASSUMPTIONS/OUT OF SCOPE;
- диаграммы/таблицы/примеры там, где нужны;
- однозначность формулировок, отсутствие «воды»;
- можно ли по документу стартовать разработку без устных уточнений.

Правила ответа:
1) Отвечай СТРОГО валидным JSON без markdown-обёртки и без комментариев.
2) Оценки — целые числа от 1 до 10.
3) plus/minus — короткие конкретные пункты (3–6 штук), по делу.
4) Не выдумывай содержимое, которого нет в тексте. Если текста мало — снижай оценки и пиши об этом в minus/verdict.
5) score_overall — итог с учётом и работы СА, и качества документа (обычно среднее с лёгким уклоном к полезности для команды).

Схема JSON:
{{
  "doc_title": "краткое название",
  "plus": ["...", "..."],
  "minus": ["...", "..."],
  "score_work": 7,
  "score_doc": 6,
  "score_overall": 6,
  "verdict": "2–4 предложения: вывод для руководителя команды"
}}
"""


@dataclass
class DocAnalysis:
    file: str
    relative_path: str
    space: str
    pages_pdf: int
    chars: int
    plus: List[str] = field(default_factory=list)
    minus: List[str] = field(default_factory=list)
    score_work: Optional[int] = None
    score_doc: Optional[int] = None
    score_overall: Optional[int] = None
    verdict: str = ""
    error: str = ""
    raw_model: str = ""


def extract_pdf_text(path: Path, max_chars: int) -> Tuple[str, int]:
    if PdfReader is None:
        raise RuntimeError("Установите pypdf: pip install pypdf")
    reader = PdfReader(str(path))
    parts: List[str] = []
    for page in reader.pages:
        try:
            parts.append(page.extract_text() or "")
        except Exception:
            continue
    text = re.sub(r"[ \t]+", " ", "\n".join(parts))
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text[:max_chars], len(reader.pages)


def iter_pdfs(root: Path) -> List[Path]:
    files = sorted(p for p in root.rglob("*.pdf") if p.is_file() and p.stat().st_size > 0)
    return files


def yandex_headers(api_key: str, iam_token: str, folder_id: str) -> Dict[str, str]:
    headers = {
        "Content-Type": "application/json",
        "x-folder-id": folder_id,
    }
    if api_key:
        headers["Authorization"] = f"Api-Key {api_key}"
    elif iam_token:
        headers["Authorization"] = f"Bearer {iam_token}"
    else:
        raise RuntimeError("Не задан YANDEX_API_KEY или YANDEX_IAM_TOKEN")
    return headers


def call_yandex_gpt(
    *,
    folder_id: str,
    api_key: str,
    iam_token: str,
    model: str,
    system_text: str,
    user_text: str,
    temperature: float,
    max_tokens: int,
) -> str:
    url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
    payload = {
        "modelUri": f"gpt://{folder_id}/{model}",
        "completionOptions": {
            "stream": False,
            "temperature": temperature,
            "maxTokens": str(max_tokens),
        },
        "messages": [
            {"role": "system", "text": system_text},
            {"role": "user", "text": user_text},
        ],
    }
    headers = yandex_headers(api_key, iam_token, folder_id)

    last_err = ""
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
            if r.status_code in (429, 500, 502, 503, 504):
                last_err = f"HTTP {r.status_code}: {r.text[:300]}"
                time.sleep(RETRY_SLEEP_SEC * attempt)
                continue
            if r.status_code != 200:
                raise RuntimeError(f"YandexGPT HTTP {r.status_code}: {r.text[:500]}")
            data = r.json()
            alts = (((data.get("result") or {}).get("alternatives")) or [])
            if not alts:
                raise RuntimeError(f"Пустой ответ модели: {json.dumps(data, ensure_ascii=False)[:500]}")
            return (alts[0].get("message") or {}).get("text") or ""
        except RuntimeError:
            raise
        except Exception as e:
            last_err = str(e)
            time.sleep(RETRY_SLEEP_SEC * attempt)
    raise RuntimeError(f"YandexGPT не ответил после ретраев: {last_err}")


def extract_json_object(text: str) -> Dict[str, Any]:
    text = (text or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        m = re.search(r"\{[\s\S]*\}", text)
        if not m:
            raise
        return json.loads(m.group(0))


def clamp_score(value: Any) -> Optional[int]:
    try:
        n = int(round(float(value)))
    except (TypeError, ValueError):
        return None
    return max(1, min(10, n))


def analyze_one(
    pdf_path: Path,
    root: Path,
    *,
    folder_id: str,
    api_key: str,
    iam_token: str,
    model: str,
    max_chars: int,
) -> DocAnalysis:
    rel = str(pdf_path.relative_to(root))
    space = pdf_path.parent.name if pdf_path.parent != root else "root"
    result = DocAnalysis(file=pdf_path.name, relative_path=rel, space=space, pages_pdf=0, chars=0)

    try:
        text, pages = extract_pdf_text(pdf_path, max_chars)
        result.pages_pdf = pages
        result.chars = len(text)
        if len(text.strip()) < 80:
            result.error = "Слишком мало текста (возможно скан/картинки без OCR)"
            result.score_work = 2
            result.score_doc = 2
            result.score_overall = 2
            result.minus = ["Не удалось извлечь достаточно текста из PDF"]
            result.verdict = "Документ не проанализирован по содержанию: нужен OCR или другой формат."
            return result

        user_prompt = (
            f"Файл: {rel}\n"
            f"Пространство Confluence: {space}\n"
            f"Страниц PDF: {pages}\n\n"
            f"Текст документа (может быть обрезан):\n"
            f"----- BEGIN -----\n{text}\n----- END -----\n"
        )
        raw = call_yandex_gpt(
            folder_id=folder_id,
            api_key=api_key,
            iam_token=iam_token,
            model=model,
            system_text=SYSTEM_PROMPT,
            user_text=user_prompt,
            temperature=YANDEX_TEMPERATURE,
            max_tokens=YANDEX_MAX_TOKENS,
        )
        result.raw_model = raw
        data = extract_json_object(raw)
        result.plus = [str(x) for x in (data.get("plus") or [])][:8]
        result.minus = [str(x) for x in (data.get("minus") or [])][:8]
        result.score_work = clamp_score(data.get("score_work"))
        result.score_doc = clamp_score(data.get("score_doc"))
        result.score_overall = clamp_score(data.get("score_overall"))
        result.verdict = str(data.get("verdict") or "").strip()
        if result.score_overall is None and result.score_work and result.score_doc:
            result.score_overall = clamp_score((result.score_work + result.score_doc) / 2)
    except Exception as e:
        result.error = str(e)
        safe_print(f"   ❌ {rel}: {e}")
    return result


def avg(nums: List[Optional[int]]) -> Optional[float]:
    vals = [n for n in nums if isinstance(n, int)]
    if not vals:
        return None
    return round(sum(vals) / len(vals), 2)


def write_outputs(rows: List[DocAnalysis], out_dir: Path) -> Dict[str, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    details = out_dir / "analysis_details.json"
    csv_path = out_dir / "summary_table.csv"
    md_path = out_dir / "summary_table.md"

    with details.open("w", encoding="utf-8") as f:
        json.dump([asdict(r) for r in rows], f, ensure_ascii=False, indent=2)

    fieldnames = [
        "space",
        "file",
        "relative_path",
        "pages_pdf",
        "score_work",
        "score_doc",
        "score_overall",
        "plus",
        "minus",
        "verdict",
        "error",
    ]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        w.writeheader()
        for r in rows:
            w.writerow(
                {
                    "space": r.space,
                    "file": r.file,
                    "relative_path": r.relative_path,
                    "pages_pdf": r.pages_pdf,
                    "score_work": r.score_work if r.score_work is not None else "",
                    "score_doc": r.score_doc if r.score_doc is not None else "",
                    "score_overall": r.score_overall if r.score_overall is not None else "",
                    "plus": " | ".join(r.plus),
                    "minus": " | ".join(r.minus),
                    "verdict": r.verdict,
                    "error": r.error,
                }
            )

    ranked = sorted(
        rows,
        key=lambda x: (x.score_overall is None, -(x.score_overall or 0), x.relative_path),
    )
    lines = [
        f"# Сводная оценка документов СА: {ANALYST_NAME}",
        "",
        f"Документов: **{len(rows)}**",
        f"Средняя оценка работы СА: **{avg([r.score_work for r in rows]) or '—'}**",
        f"Средняя оценка качества документа: **{avg([r.score_doc for r in rows]) or '—'}**",
        f"Средняя итоговая: **{avg([r.score_overall for r in rows]) or '—'}**",
        "",
        "| # | Space | Документ | Работа СА | Качество док. | Итог | + | − | Вердикт |",
        "|---|-------|----------|-----------|---------------|------|---|---|---------|",
    ]
    for i, r in enumerate(ranked, 1):
        plus = "<br>".join(f"+ {p}" for p in r.plus) or "—"
        minus = "<br>".join(f"− {m}" for m in r.minus) or ("— " + r.error if r.error else "—")
        verdict = (r.verdict or r.error or "—").replace("|", "\\|")
        lines.append(
            f"| {i} | {r.space} | {r.file} | {r.score_work or '—'} | {r.score_doc or '—'} | "
            f"{r.score_overall or '—'} | {plus} | {minus} | {verdict} |"
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    # короткий executive summary
    exec_path = out_dir / "executive_summary.md"
    weak = [r for r in ranked if (r.score_overall or 0) <= 5]
    strong = [r for r in ranked if (r.score_overall or 0) >= 8]
    exec_lines = [
        f"# Executive summary: {ANALYST_NAME}",
        "",
        f"- Всего документов: {len(rows)}",
        f"- Средние: work={avg([r.score_work for r in rows])}, doc={avg([r.score_doc for r in rows])}, overall={avg([r.score_overall for r in rows])}",
        f"- Сильные (≥8): {len(strong)}",
        f"- Слабые (≤5): {len(weak)}",
        "",
        "## Топ сильных",
    ]
    for r in strong[:10]:
        exec_lines.append(f"- [{r.score_overall}] {r.relative_path}: {r.verdict}")
    exec_lines.append("")
    exec_lines.append("## Приоритет на доработку")
    for r in weak[:15]:
        exec_lines.append(f"- [{r.score_overall or 'err'}] {r.relative_path}: {'; '.join(r.minus[:2]) or r.error}")
    exec_path.write_text("\n".join(exec_lines) + "\n", encoding="utf-8")

    return {"json": details, "csv": csv_path, "md": md_path, "exec": exec_path}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Анализ PDF СА через YandexGPT")
    p.add_argument("--pdf-dir", default=os.getenv("PDF_DIR", PDF_DIR))
    p.add_argument("--out", default=os.getenv("OUTPUT_DIR", OUTPUT_DIR))
    p.add_argument("--folder-id", default=os.getenv("YANDEX_FOLDER_ID", YANDEX_FOLDER_ID))
    p.add_argument("--api-key", default=os.getenv("YANDEX_API_KEY", YANDEX_API_KEY))
    p.add_argument("--iam-token", default=os.getenv("YANDEX_IAM_TOKEN", YANDEX_IAM_TOKEN))
    p.add_argument("--model", default=os.getenv("YANDEX_MODEL", YANDEX_MODEL))
    p.add_argument("--max-chars", type=int, default=MAX_CHARS_PER_DOC)
    p.add_argument("--workers", type=int, default=API_WORKERS)
    p.add_argument("--limit", type=int, default=0, help="Обработать только N первых PDF (0=все)")
    p.add_argument("--space", default="", help="Только один space (имя подпапки)")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    safe_print("🚀 Анализ PDF СА через YandexGPT...")

    pdf_root = Path(args.pdf_dir).expanduser()
    if not pdf_root.is_absolute():
        # ищем относительно cwd и рядом со скриптом
        candidates = [
            Path.cwd() / pdf_root,
            Path(__file__).resolve().parent / pdf_root,
            Path(r"C:\Users\Дмитрий Григорьев\Desktop\Alfa Bank\Работа СА Юры") / "confluence_pdfs_Zabaryanskiy",
        ]
        for c in candidates:
            if c.exists():
                pdf_root = c
                break

    if not pdf_root.exists():
        safe_print(f"❌ Папка PDF не найдена: {args.pdf_dir}")
        safe_print("   Укажите --pdf-dir или PDF_DIR в начале скрипта.")
        return 2

    if not args.folder_id or not (args.api_key or args.iam_token):
        safe_print("❌ Нужны YANDEX_FOLDER_ID и YANDEX_API_KEY (или YANDEX_IAM_TOKEN).")
        safe_print("   Пропишите в начале analyze_sa_pdfs_yandex.py или через env.")
        return 2

    if PdfReader is None:
        safe_print("❌ Нет библиотеки pypdf. Установите: pip install pypdf")
        return 2

    pdfs = iter_pdfs(pdf_root)
    if args.space:
        pdfs = [p for p in pdfs if p.parent.name == args.space]
    if args.limit and args.limit > 0:
        pdfs = pdfs[: args.limit]

    if not pdfs:
        safe_print(f"❌ PDF не найдены в {pdf_root}")
        return 1

    safe_print(f"📁 PDF: {pdf_root}")
    safe_print(f"📄 К анализу: {len(pdfs)} файлов, модель={args.model}")

    rows: List[DocAnalysis] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futs = {
            pool.submit(
                analyze_one,
                pdf,
                pdf_root,
                folder_id=args.folder_id,
                api_key=args.api_key,
                iam_token=args.iam_token,
                model=args.model,
                max_chars=args.max_chars,
            ): pdf
            for pdf in pdfs
        }
        done = 0
        for fut in as_completed(futs):
            pdf = futs[fut]
            try:
                row = fut.result()
            except Exception as e:
                row = DocAnalysis(
                    file=pdf.name,
                    relative_path=str(pdf),
                    space=pdf.parent.name,
                    pages_pdf=0,
                    chars=0,
                    error=str(e),
                )
            rows.append(row)
            done += 1
            mark = row.score_overall if row.score_overall is not None else "err"
            safe_print(f"[{done}/{len(pdfs)}] {mark}/10  {row.relative_path}")

    out_dir = Path(args.out)
    if not out_dir.is_absolute():
        out_dir = Path.cwd() / out_dir
    paths = write_outputs(rows, out_dir)

    safe_print("\n📊 СВОДКА")
    safe_print(f"   work={avg([r.score_work for r in rows])}  "
               f"doc={avg([r.score_doc for r in rows])}  "
               f"overall={avg([r.score_overall for r in rows])}")
    safe_print(f"   CSV:  {paths['csv']}")
    safe_print(f"   MD:   {paths['md']}")
    safe_print(f"   EXEC: {paths['exec']}")
    safe_print(f"   JSON: {paths['json']}")
    return 0


if __name__ == "__main__":
    t0 = time.time()
    code = main()
    safe_print(f"\n⏱️ {time.time() - t0:.1f}s")
    sys.exit(code)
