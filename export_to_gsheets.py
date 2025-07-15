#!/usr/bin/env python3
"""
Экспорт результатов анализа чатов в Google Sheets.
"""

import json
from pathlib import Path
from typing import Any
import gspread
from oauth2client.service_account import ServiceAccountCredentials

SPREADSHEET_ID = "1_cyDMKgax9SRi__VehFUr16p_XSBmO4gEC2Ut04FXY8"
SHEET_NAME = "Чаты"
OUTPUT_DIR = Path("output")


def build_rows_from_results(folder: Path) -> list[list[Any]]:
    rows = []
    for file in folder.glob("*_analysis.json"):
        try:
            data = json.loads(file.read_text(encoding="utf-8"))
            result = data.get("result", data)  # fallback если нет поля .result

            row = [
                Path(data.get("source_file", file.name)).stem,
                data.get("created_at"),
                result.get("has_order"),
                result.get("total_sum"),
                result.get("complaint"),
                result.get("summary"),
                data.get("model", ""),
                data.get("chat_hash", ""),
            ]
            rows.append(row)
        except Exception as e:
            print(f"❌ Ошибка чтения {file.name}: {e}")
    return rows


def upload_to_google_sheets(rows: list[list[Any]], sheet_name: str, spreadsheet_id: str):
    print(f"📡 Подключение к Google Sheets...")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)

    sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    sheet.clear()

    headers = ["chat_name", "created_at", "has_order", "total_sum", "complaint", "summary", "model", "chat_hash"]
    all_data = [headers] + rows

    # чтобы убрать предупреждение DeprecationWarning
    sheet.update(values=all_data, range_name="A1")
    print(f"✅ Загружено строк: {len(rows)}")


def main():
    rows = build_rows_from_results(OUTPUT_DIR)
    if not rows:
        print("⚠️ Нет данных для выгрузки.")
        return
    upload_to_google_sheets(rows, SHEET_NAME, SPREADSHEET_ID)


if __name__ == "__main__":
    main()
